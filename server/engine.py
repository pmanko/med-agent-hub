"""One stage engine for streaming and blocking med-agent-hub requests."""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

import httpx

from . import team as stages
from . import temporal
from .config import (
    EXPERT_DRY_MULTIPLIER,
    ORCHESTRATOR_DRY_MULTIPLIER,
    SYNTH_DRY_MULTIPLIER,
    SYNTH_REPEAT_PENALTY,
)
from .context_sources import (
    ContextBudget,
    ContextRequest,
    ContextSourceError,
    ContextView,
    EvidenceLedger,
    EvidenceRecord,
    HistoryView,
    IncludedRecord,
    InsufficientContextError,
    RouterTokenCounter,
    SourceRegistry,
    TokenCounter,
    fit_message_history,
    is_chart_message,
    select_context,
)
from .levels_loader import Profile, resolve_temporal_policy
from .prompt_loader import load_prompt


@dataclass(frozen=True)
class ExecutionRequest:
    profile: Profile
    messages: Sequence[Mapping[str, Any]]
    response_format: Optional[Dict[str, Any]] = None
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    context: Optional[Mapping[str, Any]] = None
    patient: Optional[str] = None
    model_label: Optional[str] = None
    is_disconnected: Optional[Callable[[], Awaitable[bool]]] = None
    source_registry: Optional[SourceRegistry] = None
    token_counter: Optional[TokenCounter] = None


def _chart_answer_response_format() -> Dict[str, Any]:
    """Hub-owned structured-output contract for product Answer generation."""
    cell = {
        "type": "object",
        "properties": {
            "text": {"type": "string"},
            "refs": {"type": "array", "items": {"type": "integer"}},
        },
        "required": ["text", "refs"],
        "additionalProperties": False,
    }
    column = {
        "type": "object",
        "properties": {"key": {"type": "string"}, "label": {"type": "string"}},
        "required": ["key", "label"],
        "additionalProperties": False,
    }
    row = {
        "type": "object",
        "properties": {"cells": {"type": "object", "additionalProperties": cell}},
        "required": ["cells"],
        "additionalProperties": False,
    }
    block = {
        "type": "object",
        "properties": {
            "kind": {"type": "string", "enum": ["table"]},
            "title": {"type": "string"},
            "columns": {"type": "array", "items": column},
            "rows": {"type": "array", "items": row},
        },
        "required": ["kind", "title", "columns", "rows"],
        "additionalProperties": False,
    }
    schema = {
        "type": "object",
        "properties": {
            "answer": {"type": "string"},
            "citations": {"type": "array", "items": {"type": "integer"}},
            "blocks": {"type": "array", "items": block},
        },
        "required": ["answer", "citations", "blocks"],
        "additionalProperties": False,
    }
    return {
        "type": "json_schema",
        "json_schema": {"name": "chart_answer", "strict": True, "schema": schema},
    }


def _answer_response_format(request: ExecutionRequest) -> Optional[Dict[str, Any]]:
    """Product profiles own their contract; low-level legs remain caller-controlled."""
    if request.profile.output_mode == "product":
        return _chart_answer_response_format()
    return request.response_format


@dataclass
class _State:
    messages: List[Dict[str, Any]]
    ledger: EvidenceLedger = field(default_factory=lambda: EvidenceLedger(()))
    view: Optional[ContextView] = None
    chart: str = ""
    mappings: List[Dict[str, Any]] = field(default_factory=list)
    gathered: str = ""
    derived_context: str = ""
    token_counter: Optional[TokenCounter] = None
    context_budget: Optional[ContextBudget] = None
    temporal_facts: Optional[Dict[str, Any]] = None
    reference_date: Optional[str] = None
    steps: List[Dict[str, Any]] = field(default_factory=list)
    answer_text: str = ""
    citations: List[int] = field(default_factory=list)
    table_issues: List[Dict[str, Any]] = field(default_factory=list)
    citation_issues: List[Dict[str, Any]] = field(default_factory=list)
    blocks: List[Any] = field(default_factory=list)
    answer_conf: Dict[str, Any] = field(
        default_factory=lambda: {"level": "green", "note": ""}
    )
    answer_validation: Optional[Dict[str, Any]] = None
    answer_gate: Optional[Dict[str, Any]] = None
    original_answer: Optional[str] = None
    review_draft: Optional[str] = None
    review_draft_citations: List[int] = field(default_factory=list)
    review_draft_blocks: List[Any] = field(default_factory=list)
    review_edited: bool = False
    references: List[Dict[str, Any]] = field(default_factory=list)
    claims: List[str] = field(default_factory=list)
    indepth_conf: Dict[str, Any] = field(
        default_factory=lambda: {"level": "green", "note": ""}
    )
    indepth_gate: Optional[Dict[str, Any]] = None
    indepth_error: str = ""
    indepth_error_code: Optional[str] = None
    indepth_mandatory_source_ids: List[str] = field(default_factory=list)
    drug_context: Any = None
    raw_review_content: Optional[str] = None
    history: Optional[HistoryView] = None


def _prompt(profile: Profile, role: str, fallback: str) -> str:
    return load_prompt(str(profile.prompts.get(role) or fallback))


def _temporal_anchor(request: ExecutionRequest) -> Optional[str]:
    """Resolve temporal "today" without making historical charts current by default."""
    configured = str(request.profile.policies.get("anchor") or "").strip()
    if configured:
        return configured
    environment = str(os.environ.get("HUB_ANCHOR") or "").strip()
    if environment:
        return environment
    if request.profile.output_mode == "product":
        return "wall_clock"
    return None


def _context_summary(state: _State) -> Dict[str, Any]:
    view = state.view
    return {
        "schema_version": "context_view.v1",
        "sources": list(state.ledger.source_names),
        "ledger_records": len(state.ledger.records),
        "selection_mode": view.mode if view else "none",
        "included_ids": list(view.included_ids) if view else [],
        "included": [
            {"source_id": item.stable_id, "reason": item.reason}
            for item in (view.included if view else ())
        ],
        "excluded": [
            {"source_id": item.stable_id, "reason": item.reason}
            for item in (view.excluded if view else ())
        ],
        "input_tokens": view.input_tokens if view else None,
        "input_limit": view.input_limit if view else None,
        "history": (
            {
                "dropped_turns": list(state.history.dropped_turns),
                "stripped_citation_tokens": state.history.stripped_citation_tokens,
                "fixed_input_tokens": state.history.fixed_input_tokens,
            }
            if state.history
            else None
        ),
        "derived_context_chars": len(state.derived_context),
    }


def _merge_reference_grounding(
    existing: Dict[str, Any], incoming: Mapping[str, Any]
) -> None:
    """Merge usage and claim-level verdicts when Answer and In-Depth cite one record."""
    usages = list(existing.get("usage") or [])
    for usage in incoming.get("usage") or []:
        if usage not in usages:
            usages.append(usage)
    existing["usage"] = usages

    checks = list(existing.get("groundingChecks") or [])
    for check in incoming.get("groundingChecks") or []:
        if check not in checks:
            checks.append(check)
    if checks:
        existing["groundingChecks"] = checks

    groups = {
        index
        for reference in (existing, incoming)
        for index in (reference.get("groundingGroup") or [])
        if isinstance(index, int)
    }
    for check in checks:
        groups.update(
            index
            for index in check.get("source_indices") or []
            if isinstance(index, int)
        )
    if len(groups) > 1:
        existing["groundingScope"] = "source_set"
        existing["groundingGroup"] = sorted(groups)
    elif groups or any(
        reference.get("groundingScope") == "record"
        for reference in (existing, incoming)
    ):
        existing["groundingScope"] = "record"
        existing.pop("groundingGroup", None)

    statuses = {check.get("status") for check in checks}
    if not statuses:
        statuses = {
            existing.get("groundingStatus"),
            incoming.get("groundingStatus"),
        }
    if statuses & {"unchecked", "checking", None}:
        grounded, aggregate = None, "unchecked"
    elif "mixed" in statuses or {"verified", "unsupported"} <= statuses:
        grounded, aggregate = None, "mixed"
    elif "unsupported" in statuses:
        grounded, aggregate = False, "unsupported"
    else:
        grounded, aggregate = True, "verified"
    existing["grounded"] = grounded
    existing["groundingStatus"] = aggregate


def _ledger_after_drug_injection(
    ledger: EvidenceLedger,
    chart: str,
    mappings: Sequence[Mapping[str, Any]],
) -> EvidenceLedger:
    existing_by_uuid = {
        record.resource_uuid: record
        for record in ledger.records
        if record.resource_uuid
    }
    existing_by_id = {record.stable_id: record for record in ledger.records}
    records: list[EvidenceRecord] = []
    for index, mapping in enumerate(mappings, 1):
        uuid = mapping.get("resourceUuid")
        existing = existing_by_id.get(str(mapping.get("sourceId") or ""))
        if existing is None:
            existing = existing_by_uuid.get(uuid)
        if existing is not None:
            records.append(
                EvidenceRecord(
                    stable_id=existing.stable_id,
                    source=existing.source,
                    source_priority=existing.source_priority,
                    resource_type=existing.resource_type,
                    resource_uuid=existing.resource_uuid,
                    date=mapping.get("date"),
                    text=str(mapping.get("text") or existing.text),
                    mandatory=existing.mandatory,
                    metadata=existing.metadata,
                    raw=existing.raw,
                )
            )
            continue
        resource_type = str(mapping.get("resourceType") or "DrugReference")
        records.append(
            EvidenceRecord(
                stable_id=str(mapping.get("sourceId") or f"drug-reference:{index}"),
                source="drug-safety",
                source_priority=100,
                resource_type=resource_type,
                resource_uuid=uuid,
                date=mapping.get("date"),
                text=str(mapping.get("text") or ""),
                mandatory=resource_type.lower() == "drugreference",
            )
        )
    return EvidenceLedger(tuple(records), original_text=chart, preamble=ledger.preamble)


def _fixed_context_text(
    request: ExecutionRequest,
    messages: Sequence[Mapping[str, Any]],
    temporal_block: str,
) -> str:
    parts: list[str] = []
    for message in messages:
        content = message.get("content")
        if not isinstance(content, str):
            content = json.dumps(content)
        if is_chart_message(message):
            continue
        parts.append(f"{message.get('role', 'user')}: {content}")
    for role, fallback in (
        ("answer", "synthesis-answer"),
        ("review", "validation-rewrite"),
        ("indepth", "synthesis-indepth"),
        ("orchestrator", "orchestrator"),
        ("expert", "medical_expert"),
    ):
        if role in request.profile.models:
            if role == "review":
                stem = str(request.profile.prompts.get(role) or fallback)
                parts.append(load_prompt(stem + "-answer"))
                parts.append(load_prompt(stem + "-indepth"))
            else:
                parts.append(_prompt(request.profile, role, fallback))
    if temporal_block:
        parts.append(temporal_block)
    return "\n\n".join(parts)


def _replace_chart_message(
    messages: List[Dict[str, Any]], chart: str
) -> List[Dict[str, Any]]:
    out = [
        message
        for message in messages
        if not (
            message.get("role") == "user"
            and isinstance(message.get("content"), str)
            and is_chart_message(message)
        )
    ]
    if chart:
        at = 1 if out and out[0].get("role") == "system" else 0
        out.insert(
            at,
            {
                "role": "user",
                "content": "Patient records (most recent first):\n" + chart,
            },
        )
    return out


async def _count_answer_input(
    request: ExecutionRequest,
    state: _State,
    chart: str,
    *,
    messages: Optional[Sequence[Mapping[str, Any]]] = None,
) -> int:
    counter = state.token_counter
    if counter is None:
        raise ContextSourceError(
            "tokenization_unavailable",
            "The profile requires an exact token counter.",
            source="llama-router",
        )
    base_messages = [dict(message) for message in (messages or state.messages)]
    actual_messages = _replace_chart_message(base_messages, chart)
    user = _prompt(request.profile, "answer", "synthesis-answer")
    if state.gathered:
        user += "\n\n" + state.gathered
    actual_messages.append({"role": "user", "content": user})
    count_chat = getattr(counter, "count_chat", None)
    if callable(count_chat):
        payload: Dict[str, Any] = {"messages": actual_messages}
        response_format = _answer_response_format(request)
        if response_format is not None:
            payload["response_format"] = response_format
        return await count_chat(request.profile.models["answer"], payload)
    # Unit-test counters can implement the simpler text protocol. Production
    # profiles instantiate RouterTokenCounter and therefore always take the exact
    # chat-template path above.
    rendered = "\n".join(
        f"{message.get('role', 'user')}: {message.get('content', '')}"
        for message in actual_messages
    )
    return await counter.count(request.profile.models["answer"], rendered)


async def _select_answer_context(
    request: ExecutionRequest,
    state: _State,
) -> None:
    if state.token_counter is None or state.context_budget is None:
        raise ContextSourceError(
            "tokenization_unavailable",
            "The profile requires exact context selection.",
            source="llama-router",
        )
    state.view = await select_context(
        state.ledger,
        question=stages._latest_user_text(state.messages),
        model=request.profile.models["answer"],
        budget=state.context_budget,
        counter=state.token_counter,
        input_measure=lambda chart: _count_answer_input(request, state, chart),
    )
    state.chart = state.view.render()
    state.messages = _replace_chart_message(state.messages, state.chart)


async def _count_indepth_input(
    request: ExecutionRequest,
    state: _State,
    chart: str,
    prior_answer: str,
    extra_msgs: Optional[List[Dict[str, Any]]] = None,
) -> int:
    counter = state.token_counter
    if counter is None:
        raise ContextSourceError(
            "tokenization_unavailable",
            "The profile requires an exact token counter.",
            source="llama-router",
        )
    messages = _replace_chart_message(state.messages, chart)
    messages = stages._indepth_messages(
        messages,
        _prompt(request.profile, "indepth", "synthesis-indepth"),
        state.gathered,
        prior_answer,
        extra_msgs=extra_msgs,
    )
    count_chat = getattr(counter, "count_chat", None)
    if callable(count_chat):
        return await count_chat(
            request.profile.models["indepth"],
            {"messages": messages, "response_format": stages._INDEPTH_RF},
        )
    rendered = "\n".join(
        f"{message.get('role', 'user')}: {message.get('content', '')}"
        for message in messages
    )
    return await counter.count(request.profile.models["indepth"], rendered)


async def _select_indepth_context(
    request: ExecutionRequest,
    state: _State,
    prior_answer: str,
    *,
    extra_msgs: Optional[List[Dict[str, Any]]] = None,
    stage: str = "indepth",
) -> Tuple[ContextView, List[Dict[str, Any]], str]:
    if state.token_counter is None or state.context_budget is None:
        raise ContextSourceError(
            "tokenization_unavailable",
            "The profile requires exact context selection.",
            source="llama-router",
        )
    view = await select_context(
        state.ledger,
        question=stages._latest_user_text(state.messages),
        model=request.profile.models["indepth"],
        budget=state.context_budget,
        counter=state.token_counter,
        input_measure=lambda chart: _count_indepth_input(
            request, state, chart, prior_answer, extra_msgs
        ),
    )
    chart = view.render()
    messages = _replace_chart_message(state.messages, chart)
    state.steps.append(
        {
            "role": "context_selection",
            "stage": stage,
            "mode": view.mode,
            "input_tokens": view.input_tokens,
            "input_limit": view.input_limit,
            "included": [
                {"stable_id": item.stable_id, "reason": item.reason}
                for item in view.included
            ],
            "excluded": [
                {"stable_id": item.stable_id, "reason": item.reason}
                for item in view.excluded
            ],
        }
    )
    return view, messages, chart


async def _select_answer_review_context(
    request: ExecutionRequest,
    state: _State,
    answer_text: str,
    blocks: List[Any],
    *,
    stage: str = "answer_review",
) -> str:
    """Select evidence against the reviewer's exact prompt, schema, and output reserve."""
    counter = state.token_counter
    budget = state.context_budget
    if counter is None or budget is None:
        raise ContextSourceError(
            "tokenization_unavailable",
            "The profile requires exact context selection.",
            source="llama-router",
        )
    reviewer_model = request.profile.models["review"]
    review_text = answer_text
    block_text, _block_refs = stages._block_temporal_text_and_refs(blocks)
    if block_text:
        review_text += "\n\n=== STRUCTURED BLOCK TEXT ===\n" + block_text

    async def measure(chart: str) -> int:
        messages = stages._answer_validation_messages(
            chart=chart,
            gathered=state.gathered,
            answer_text=review_text,
            validation_prompt=str(
                request.profile.prompts.get("review") or "validation-rewrite"
            ),
        )
        count_chat = getattr(counter, "count_chat", None)
        if callable(count_chat):
            return await count_chat(
                reviewer_model,
                {
                    "messages": messages,
                    "response_format": stages._REWRITE_VERDICT_RF,
                },
            )
        rendered = "\n".join(
            f"{message.get('role', 'user')}: {message.get('content', '')}"
            for message in messages
        )
        return await counter.count(reviewer_model, rendered)

    view = await select_context(
        state.ledger,
        question=stages._latest_user_text(state.messages),
        model=reviewer_model,
        budget=budget,
        counter=counter,
        input_measure=measure,
    )
    state.steps.append(
        {
            "role": "context_selection",
            "stage": stage,
            "mode": view.mode,
            "input_tokens": view.input_tokens,
            "input_limit": view.input_limit,
            "included": [
                {"stable_id": item.stable_id, "reason": item.reason}
                for item in view.included
            ],
            "excluded": [
                {"stable_id": item.stable_id, "reason": item.reason}
                for item in view.excluded
            ],
        }
    )
    return view.render()


async def _select_indepth_review_context(
    request: ExecutionRequest,
    state: _State,
    prior_answer: str,
    claims: List[str],
) -> str:
    counter = state.token_counter
    budget = state.context_budget
    if counter is None or budget is None:
        raise ContextSourceError(
            "tokenization_unavailable",
            "The profile requires exact context selection.",
            source="llama-router",
        )
    reviewer_model = request.profile.models["review"]

    async def measure(chart: str) -> int:
        messages = stages._indepth_validation_messages(
            chart=chart,
            gathered=state.gathered,
            answer_text=prior_answer,
            claims=claims,
            validation_prompt=str(
                request.profile.prompts.get("review") or "validation-rewrite"
            ),
        )
        count_chat = getattr(counter, "count_chat", None)
        if callable(count_chat):
            return await count_chat(
                reviewer_model,
                {
                    "messages": messages,
                    "response_format": stages._INDEPTH_VERDICT_RF,
                },
            )
        rendered = "\n".join(
            f"{message.get('role', 'user')}: {message.get('content', '')}"
            for message in messages
        )
        return await counter.count(reviewer_model, rendered)

    view = await select_context(
        state.ledger,
        question=stages._latest_user_text(state.messages),
        model=reviewer_model,
        budget=budget,
        counter=counter,
        input_measure=measure,
    )
    state.steps.append(
        {
            "role": "context_selection",
            "stage": "indepth_review",
            "mode": view.mode,
            "input_tokens": view.input_tokens,
            "input_limit": view.input_limit,
            "included": [
                {"stable_id": item.stable_id, "reason": item.reason}
                for item in view.included
            ],
            "excluded": [
                {"stable_id": item.stable_id, "reason": item.reason}
                for item in view.excluded
            ],
        }
    )
    return view.render()


async def _prepare_context(request: ExecutionRequest, state: _State) -> None:
    registry = request.source_registry or SourceRegistry.default()
    context = request.context or {}
    requested_sources = context.get("sources") or ()
    if isinstance(requested_sources, str):
        requested_sources = (requested_sources,)
    elif not isinstance(requested_sources, (list, tuple)):
        raise ContextSourceError(
            "context_source_unavailable",
            "context.sources must be a list of configured source names.",
            source="request",
        )
    try:
        ledger = await registry.build_ledger(
            ContextRequest(
                patient=request.patient,
                messages=state.messages,
                source=str(context.get("source")) if context.get("source") else None,
                sources=tuple(str(item) for item in requested_sources),
                supplemental_sources=(
                    ("knowledge-base",) if "gather" in request.profile.stages else ()
                ),
                question=stages._latest_user_text(state.messages),
            )
        )
    except ContextSourceError:
        if request.patient or context.get("source") or requested_sources:
            raise
        ledger = EvidenceLedger(())

    state.ledger = ledger
    full_chart = ledger.render()
    mappings = ledger.mappings()
    raw_records = ledger.raw_records()
    anchor = _temporal_anchor(request)
    temporal_enabled, _mode = resolve_temporal_policy(request.profile, request.context)
    resolved_reference_date = None
    if temporal_enabled or request.profile.policies.get("drug_safety"):
        resolved_reference_date = temporal.resolve_anchor(
            anchor,
            full_chart,
            timezone_name=os.environ.get("HUB_TIMEZONE"),
        )
    if request.profile.policies.get("drug_safety") and full_chart:
        full_chart, mappings, state.drug_context = stages._prepare_drug_safety(
            full_chart,
            mappings,
            raw_records,
            stages._latest_user_text(state.messages),
            resolved_reference_date,
            True,
        )
        ledger = _ledger_after_drug_injection(ledger, full_chart, mappings)
        state.ledger = ledger

    temporal_block = ""
    if temporal_enabled:
        state.reference_date = resolved_reference_date
        state.temporal_facts = temporal.build_temporal_facts(
            full_chart,
            state.reference_date,
            anchor_mode=anchor or "latest_record",
        )
        temporal_block = temporal.render_temporal_facts(
            state.temporal_facts,
            profile=str(request.profile.policies.get("temporal_render") or "full"),
        )
    state.gathered = temporal_block

    if request.profile.exact_tokenizer:
        counter = request.token_counter or RouterTokenCounter()
        budget = ContextBudget(
            context_window=request.profile.context_window,
            reserved_output_tokens=request.profile.reserved_output_tokens,
        )
        state.token_counter = counter
        state.context_budget = budget
        mandatory = tuple(record for record in ledger.records if record.mandatory)
        mandatory_text = ledger.preamble + EvidenceLedger(mandatory).render()
        state.history = await fit_message_history(
            state.messages,
            model=request.profile.models["answer"],
            budget=budget,
            counter=counter,
            fixed_renderer=lambda messages: _fixed_context_text(
                request, messages, temporal_block
            ),
            mandatory_text=mandatory_text,
            mandatory_ids=tuple(record.stable_id for record in mandatory),
            input_measure=lambda messages: _count_answer_input(
                request, state, mandatory_text, messages=messages
            ),
        )
        state.messages = [dict(message) for message in state.history.messages]
        await _select_answer_context(request, state)
    else:
        state.view = ContextView(
            records=ledger.records,
            record_indices=tuple(range(1, len(ledger.records) + 1)),
            mode="full",
            included=tuple(
                IncludedRecord(record.stable_id, "full_context")
                for record in ledger.records
            ),
            excluded=(),
            input_tokens=0,
            input_limit=0,
            original_text=ledger.render(),
            preamble=ledger.preamble,
        )

    state.chart = state.view.render()
    # Resolution uses the complete ledger because temporal facts may legitimately cite a
    # record omitted from the reduced prose chart. Selected chart lines retain these indices.
    state.mappings = state.ledger.mappings()
    if (
        request.patient
        or state.view.mode == "selected"
        or state.ledger.source_names != ("inline",)
    ):
        state.messages = _replace_chart_message(state.messages, state.chart)
    state.steps.append({"role": "context", **_context_summary(state)})


def _sampling(request: ExecutionRequest) -> Dict[str, Any]:
    knobs = request.profile.knobs
    answer_default = max(request.temperature or 0.0, stages._SYNTH_MIN_TEMPERATURE)
    return {
        "orchestrator_temperature": stages._knob(
            knobs, "orchestrator", "temperature", request.temperature
        ),
        "orchestrator_repeat_penalty": stages._knob(
            knobs, "orchestrator", "repeat_penalty", None
        ),
        "orchestrator_dry": stages._knob(
            knobs, "orchestrator", "dry", ORCHESTRATOR_DRY_MULTIPLIER
        ),
        "expert_temperature": stages._knob(knobs, "expert", "temperature", 0.1),
        "expert_repeat_penalty": stages._knob(knobs, "expert", "repeat_penalty", None),
        "expert_dry": stages._knob(knobs, "expert", "dry", EXPERT_DRY_MULTIPLIER),
        "answer_temperature": stages._knob(
            knobs, "answer", "temperature", answer_default
        ),
        "answer_repeat_penalty": stages._knob(
            knobs, "answer", "repeat_penalty", SYNTH_REPEAT_PENALTY
        ),
        "answer_dry": stages._knob(knobs, "answer", "dry", SYNTH_DRY_MULTIPLIER),
        "review_temperature": stages._knob(knobs, "review", "temperature", 0.0),
        "review_repeat_penalty": stages._knob(knobs, "review", "repeat_penalty", None),
        "review_dry": stages._knob(knobs, "review", "dry", None),
    }


async def _disconnected(request: ExecutionRequest) -> bool:
    try:
        return bool(request.is_disconnected and await request.is_disconnected())
    except Exception:
        return False


def _stream_payload(
    state: _State,
    request: ExecutionRequest,
    *,
    in_depth: Optional[Dict[str, Any]] = None,
) -> str:
    payload: Dict[str, Any] = {
        "answer": state.answer_text,
        "references": state.references,
        "blocks": state.blocks,
    }
    if request.model_label or request.profile.id:
        payload["model"] = request.model_label or request.profile.id
    if state.answer_conf:
        payload["confidence"] = {"answer": state.answer_conf}
    if state.answer_validation is not None:
        payload["answerValidation"] = state.answer_validation
    if state.answer_gate is not None:
        payload["temporalGate"] = state.answer_gate
    if in_depth is not None:
        payload["inDepth"] = in_depth
    warnings = stages._compute_safety_warnings(
        state.drug_context,
        state.answer_text,
        stages._latest_user_text(state.messages),
        bool(request.profile.policies.get("drug_safety")),
    )
    if warnings:
        payload["safetyWarnings"] = warnings
    payload["context"] = _context_summary(state)
    return json.dumps(payload)


def _raw_result(request: ExecutionRequest, state: _State) -> str:
    mode = request.profile.output_mode
    if mode == "review" and state.raw_review_content is not None:
        return state.raw_review_content
    if mode == "indepth":
        body = (
            "**In Depth**\n" + "\n".join("- " + claim for claim in state.claims)
            if state.claims
            else ""
        )
        return json.dumps({"answer": body, "citations": [], "blocks": []})
    if mode == "bare":
        payload = {
            "answer": state.answer_text,
            "citations": state.citations,
            "blocks": state.blocks,
        }
        warnings = stages._compute_safety_warnings(
            state.drug_context,
            state.answer_text,
            stages._latest_user_text(state.messages),
            bool(request.profile.policies.get("drug_safety")),
        )
        if warnings:
            payload["safetyWarnings"] = warnings
        return json.dumps(payload)
    if mode == "combined":
        payload = json.loads(
            stages._assemble_envelope(
                state.answer_text,
                state.citations,
                state.blocks,
                state.claims,
                state.answer_conf,
                state.indepth_conf,
            )
        )
        warnings = stages._compute_safety_warnings(
            state.drug_context,
            state.answer_text,
            stages._latest_user_text(state.messages),
            bool(request.profile.policies.get("drug_safety")),
        )
        if warnings:
            payload["safetyWarnings"] = warnings
        return json.dumps(payload)
    return stages._fallback_envelope(
        "I could not produce a complete answer for this turn. Please try again."
    )


async def _execute_stages(
    request: ExecutionRequest,
    budget_policy: Optional[stages.ChatBudgetPolicy] = None,
) -> AsyncIterator[Tuple[str, str]]:
    """Execute the profile stage list and emit public phase events as stages complete."""
    state = _State(messages=[dict(message) for message in request.messages])
    sampling = _sampling(request)
    answer_response_format = _answer_response_format(request)
    temporal_enabled, temporal_mode = resolve_temporal_policy(
        request.profile, request.context
    )
    gate_count = 0
    stage_occurrences: Dict[str, int] = {}
    active_stage: Optional[str] = None
    active_stage_started = 0.0
    active_stage_occurrence = 0
    active_stage_recorded = True
    product = request.profile.output_mode == "product"
    if budget_policy is not None:
        state.token_counter = budget_policy.counter

    def record_stage_timing(status: str = "completed") -> None:
        nonlocal active_stage_recorded
        if active_stage is None or active_stage_recorded:
            return
        state.steps.append(
            {
                "role": "stage_timing",
                "stage": active_stage,
                "occurrence": active_stage_occurrence,
                "duration_ms": round(
                    (time.perf_counter() - active_stage_started) * 1000
                ),
                "status": status,
            }
        )
        active_stage_recorded = True

    def write_execution_trace(*, answer_text: Optional[str] = None) -> None:
        if budget_policy is not None and not any(
            step.get("role") == "exact_request_budgets" for step in state.steps
        ):
            state.steps.append(
                {
                    "role": "exact_request_budgets",
                    "measurements": list(budget_policy.measurements),
                }
            )
        stages._write_trace(
            request.profile.id,
            state.messages,
            orchestrator=request.profile.models.get("orchestrator"),
            expert=request.profile.models.get("expert"),
            synthesizer=request.profile.models.get(
                "answer", request.profile.models.get("indepth")
            ),
            validator=request.profile.models.get("review"),
            steps=state.steps,
            answer_confidence=state.answer_conf,
            indepth_confidence=state.indepth_conf,
            answer_text=state.answer_text if answer_text is None else answer_text,
            in_depth_claims=state.claims,
            reference_date=state.reference_date,
            temporal_facts=state.temporal_facts,
            temporal_gate=state.answer_gate,
            original_answer_text=state.original_answer,
            answer_validation=state.answer_validation,
            context_summary=_context_summary(state),
            indepth_temporal_gate=state.indepth_gate,
            final_references=state.references,
            sampling={
                "answer_temperature": sampling["answer_temperature"],
                "synth_temperature": sampling["answer_temperature"],
                "synth_temperature_floor": sampling["answer_temperature"],
                "synth_temperature_source": (
                    "level_knob"
                    if "temperature" in request.profile.knobs.get("answer", {})
                    else "request_or_default"
                ),
            },
        )

    try:
        async with httpx.AsyncClient() as client:
            for stage in request.profile.stages:
                stage_occurrences[stage] = stage_occurrences.get(stage, 0) + 1
                active_stage = stage
                active_stage_started = time.perf_counter()
                active_stage_occurrence = stage_occurrences[stage]
                active_stage_recorded = False

                if stage == "context":
                    await _prepare_context(request, state)
                    record_stage_timing()
                    continue

                if stage == "gather":
                    expert_notes, gather_steps = await stages._gather_evidence(
                        client,
                        has_expert="expert" in request.profile.models,
                        orchestrator_model=request.profile.models["orchestrator"],
                        orchestrator_system=_prompt(
                            request.profile, "orchestrator", "orchestrator"
                        ),
                        expert_model=request.profile.models.get("expert"),
                        expert_system=(
                            _prompt(request.profile, "expert", "medical_expert")
                            if "expert" in request.profile.models
                            else ""
                        ),
                        messages=state.messages,
                        chart=state.chart,
                        max_tokens=request.max_tokens,
                        orch_temp=sampling["orchestrator_temperature"],
                        orch_rp=sampling["orchestrator_repeat_penalty"],
                        orch_dry=sampling["orchestrator_dry"],
                        exp_temp=sampling["expert_temperature"],
                        exp_rp=sampling["expert_repeat_penalty"],
                        exp_dry=sampling["expert_dry"],
                    )
                    state.steps.extend(gather_steps)
                    gathered = stages._gathered_evidence(expert_notes)
                    state.derived_context = gathered
                    if gathered:
                        state.gathered = (
                            state.gathered + "\n\n" + gathered
                            if state.gathered
                            else gathered
                        )
                    if request.profile.exact_tokenizer:
                        await _select_answer_context(request, state)
                        state.steps.append(
                            {"role": "context_reselection", **_context_summary(state)}
                        )
                    record_stage_timing()
                    continue

                if stage == "answer":
                    (
                        state.answer_text,
                        state.citations,
                        state.blocks,
                    ) = await stages._synthesize_answer(
                        client,
                        request.profile.models["answer"],
                        state.messages,
                        _prompt(request.profile, "answer", "synthesis-answer"),
                        state.gathered,
                        response_format=answer_response_format,
                        temperature=sampling["answer_temperature"],
                        max_tokens=request.max_tokens,
                        repeat_penalty=sampling["answer_repeat_penalty"],
                        dry=sampling["answer_dry"],
                    )
                    state.steps.append(
                        {
                            "role": "answer_synth",
                            "model": request.profile.models["answer"],
                            "output": state.answer_text,
                            "citations": state.citations,
                        }
                    )
                    # Substance checking is part of the deterministic answer gate, not
                    # the optional LLM review stage. This must run for every answer path.
                    (
                        state.answer_text,
                        state.citations,
                        state.blocks,
                        state.answer_conf,
                    ) = await stages._ensure_substantive_answer(
                        client,
                        synth_model=request.profile.models["answer"],
                        base_messages=state.messages,
                        answer_instruction=_prompt(
                            request.profile, "answer", "synthesis-answer"
                        ),
                        gathered=state.gathered,
                        response_format=answer_response_format,
                        answer_text=state.answer_text,
                        citations=state.citations,
                        blocks=state.blocks,
                        synth_temperature=sampling["answer_temperature"],
                        synth_repeat_penalty=sampling["answer_repeat_penalty"],
                        synth_dry=sampling["answer_dry"],
                        max_tokens=request.max_tokens,
                        max_loops=int(request.profile.policies.get("review_loops", 1)),
                        steps=state.steps,
                    )
                    record_stage_timing()
                    continue

                if stage == "gate":
                    gate_count += 1
                    before_gate_answer = state.answer_text
                    before_gate_citations = list(state.citations)
                    block_issues: List[Dict[str, Any]] = []
                    if (
                        request.profile.output_mode == "review"
                        and not state.answer_text
                    ):
                        record_stage_timing()
                        continue
                    if request.profile.output_mode == "product":
                        before_blocks = list(state.blocks)
                        state.blocks, block_issues = stages._normalize_product_blocks(
                            state.blocks
                        )
                        if block_issues:
                            state.table_issues = block_issues
                        if state.blocks != before_blocks or block_issues:
                            state.steps.append(
                                {
                                    "role": "table_contract",
                                    "status": (
                                        "fail" if block_issues else "canonicalized"
                                    ),
                                    "before_blocks": len(before_blocks),
                                    "after_blocks": len(state.blocks),
                                    "n_issues": len(block_issues),
                                }
                            )
                        if gate_count > 1 and state.blocks != before_blocks:
                            state.review_edited = True
                    if gate_count == 1:
                        (
                            state.answer_text,
                            state.citations,
                            state.blocks,
                            state.answer_gate,
                            state.original_answer,
                        ) = stages._apply_temporal_gate(
                            question=stages._latest_user_text(state.messages),
                            answer_text=state.answer_text,
                            citations=state.citations,
                            blocks=state.blocks,
                            temporal_facts=(
                                state.temporal_facts if temporal_enabled else None
                            ),
                            temporal_gate_mode=temporal_mode,
                            steps=state.steps,
                        )
                        state.answer_conf = stages._merge_temporal_gate_conf(
                            state.answer_conf, state.answer_gate
                        )
                    elif state.review_edited:
                        (
                            state.answer_text,
                            state.citations,
                            state.blocks,
                            state.answer_conf,
                            state.answer_gate,
                            state.original_answer,
                        ) = stages._regate_after_rewrite(
                            question=stages._latest_user_text(state.messages),
                            answer_text=state.answer_text,
                            citations=state.citations,
                            blocks=state.blocks,
                            temporal_facts=(
                                state.temporal_facts if temporal_enabled else None
                            ),
                            temporal_gate_mode=temporal_mode,
                            steps=state.steps,
                            answer_conf=state.answer_conf,
                            prior_original_answer=state.original_answer,
                        )
                    else:
                        state.answer_conf = stages._merge_temporal_gate_conf(
                            state.answer_conf, state.answer_gate
                        )
                    if request.profile.output_mode == "product":
                        before_contract_answer = state.answer_text
                        before_contract_citations = list(state.citations)
                        (
                            state.answer_text,
                            state.citations,
                            citation_issues,
                        ) = stages._enforce_product_citation_contract(
                            state.answer_text, state.citations, state.blocks
                        )
                        state.citation_issues = state.table_issues + citation_issues
                        contract_changed = (
                            state.answer_text != before_contract_answer
                            or state.citations != before_contract_citations
                        )
                        if gate_count > 1 and (
                            state.answer_text != before_gate_answer
                            or state.citations != before_gate_citations
                        ):
                            state.review_edited = True
                        if contract_changed or state.citation_issues:
                            state.steps.append(
                                {
                                    "role": "citation_contract",
                                    "status": (
                                        "fail" if state.citation_issues else "canonicalized"
                                    ),
                                    "before": before_contract_citations,
                                    "after": list(state.citations),
                                }
                            )
                    record_stage_timing()
                    continue

                if stage == "resolve_refs":
                    state.references = stages._resolve_references(
                        state.citations,
                        state.mappings,
                        answer=state.answer_text,
                        blocks=state.blocks,
                        grounding_status="checking" if state.mappings else None,
                    )
                    has_review = "review" in request.profile.stages
                    has_final_grounding = "ground_verdicts" in request.profile.stages
                    gate_issues = [
                        check
                        for check in (state.answer_gate or {}).get("checks", [])
                        if check.get("status") in {"warn", "fail"}
                    ]
                    gate_issues.extend(state.citation_issues)
                    unresolved = [
                        reference
                        for reference in state.references
                        if reference.get("resolutionStatus") == "unresolved"
                    ]
                    gate_issues.extend(
                        {
                            "id": "citation_resolution",
                            "status": "fail",
                            "severity": "block",
                            "reason": f"Citation [{reference.get('index')}] does not resolve to the current evidence ledger.",
                            "source_indices": [reference.get("index")],
                        }
                        for reference in unresolved
                    )
                    fast_status = (
                        "checking" if has_review or has_final_grounding else "checked"
                    )
                    if (
                        state.answer_conf.get("level") == "red"
                        or (state.answer_gate or {}).get("applied") == "fallback"
                        or unresolved
                    ) and not (has_review or has_final_grounding):
                        fast_status = "needs_review"
                    fast_validation = stages._answer_validation_wire(
                        fast_status,
                        summary=state.answer_conf.get("note", ""),
                        issues=gate_issues,
                    )
                    prior_validation = state.answer_validation
                    state.answer_validation = fast_validation
                    record_stage_timing()
                    yield (
                        "answer_done",
                        _stream_payload(
                            state,
                            request,
                            in_depth={"status": "pending", "answer": ""},
                        ),
                    )
                    if has_review:
                        state.answer_validation = prior_validation
                    if await _disconnected(request):
                        return
                    continue

                if stage == "review":
                    reviews_current_answer = "answer" in request.profile.stages
                    payload_override = None
                    if reviews_current_answer:
                        state.review_draft = state.answer_text
                        state.review_draft_citations = list(state.citations)
                        state.review_draft_blocks = list(state.blocks)
                        payload_override = {
                            "schema_version": "answer_to_review.v1",
                            "original_question": stages._latest_user_text(
                                state.messages
                            ),
                            "answer": state.answer_text,
                            "citations": list(state.citations),
                            "blocks": list(state.blocks),
                        }
                    review_chart_selector = None
                    if (
                        request.profile.output_mode == "product"
                        and request.profile.exact_tokenizer
                    ):
                        async def review_chart_selector(
                            answer_text: str,
                            blocks: List[Any],
                            stage: str,
                        ) -> str:
                            return await _select_answer_review_context(
                                request,
                                state,
                                answer_text,
                                blocks,
                                stage=stage,
                            )
                    (
                        state.raw_review_content,
                        state.answer_conf,
                        state.answer_text,
                        state.answer_validation,
                        reviewed_gate,
                        state.original_answer,
                    ) = await stages._review_existing_answer(
                        client,
                        messages=state.messages,
                        gathered=state.gathered,
                        chart=state.chart,
                        temporal_facts=(
                            state.temporal_facts if temporal_enabled else None
                        ),
                        temporal_gate_mode=temporal_mode,
                        reviewer_model=request.profile.models["review"],
                        reviewer_prompt=str(
                            request.profile.prompts.get("review")
                            or "validation-rewrite"
                        ),
                        validator_temperature=sampling["review_temperature"],
                        validator_repeat_penalty=sampling["review_repeat_penalty"],
                        validator_dry=sampling["review_dry"],
                        max_tokens=request.max_tokens,
                        steps=state.steps,
                        payload_override=payload_override,
                        review_chart_selector=review_chart_selector,
                    )
                    if reviewed_gate is not None:
                        state.answer_gate = reviewed_gate
                    reviewed = json.loads(state.raw_review_content or "{}")
                    state.citations = [
                        value
                        for value in reviewed.get("citations") or []
                        if isinstance(value, int)
                    ]
                    state.blocks = (
                        list(reviewed.get("blocks") or [])
                        if isinstance(reviewed.get("blocks"), list)
                        else []
                    )
                    if reviews_current_answer:
                        state.review_edited = (
                            state.answer_text.strip() != state.review_draft.strip()
                            or state.citations != state.review_draft_citations
                            or state.blocks != state.review_draft_blocks
                        )
                    record_stage_timing()
                    continue

                if stage == "final_resolve_refs":
                    state.references = stages._resolve_references(
                        state.citations,
                        state.mappings,
                        answer=state.answer_text,
                        blocks=state.blocks,
                        grounding_status="checking" if state.mappings else None,
                    )
                    if "review" in request.profile.stages:
                        prior_validation = state.answer_validation or {}
                        prior_status = prior_validation.get("status")
                        unresolved_final = any(
                            reference.get("resolutionStatus") == "unresolved"
                            for reference in state.references
                        )
                        if (
                            state.answer_conf.get("level") == "red"
                            or unresolved_final
                            or state.citation_issues
                            or prior_status == "needs_review"
                        ):
                            status = "needs_review"
                        elif prior_status == "unavailable":
                            status = "unavailable"
                        elif state.review_edited:
                            status = "edited"
                        else:
                            status = "checked"
                        state.answer_validation = stages._answer_validation_wire(
                            status,
                            summary=(
                                prior_validation.get("summary")
                                or state.answer_conf.get("note", "")
                            ),
                            issues=list(prior_validation.get("issues") or [])
                            + list(state.citation_issues)
                            + [
                                check
                                for check in (state.answer_gate or {}).get("checks", [])
                                if check.get("status") in {"warn", "fail"}
                            ]
                            + [
                                {
                                    "id": "citation_resolution",
                                    "status": "fail",
                                    "severity": "block",
                                    "reason": f"Citation [{reference.get('index')}] does not resolve to the current evidence ledger.",
                                    "source_indices": [reference.get("index")],
                                }
                                for reference in state.references
                                if reference.get("resolutionStatus") == "unresolved"
                            ],
                            original_answer=(
                                prior_validation.get("originalAnswer")
                                or (state.review_draft if state.review_edited else None)
                            ),
                        )
                    record_stage_timing()
                    continue

                if stage == "ground_verdicts":
                    if state.mappings:
                        deterministic_checks = [
                            check
                            for check in (state.answer_gate or {}).get("checks", [])
                            if check.get("status") == "pass"
                            and check.get("source_indices")
                        ]
                        if deterministic_checks:
                            state.references = await stages._ground_references(
                                client,
                                request.profile.models["grounding"],
                                state.answer_text,
                                state.references,
                                state.mappings,
                                deterministic_checks=deterministic_checks,
                            )
                        else:
                            state.references = await stages._ground_references(
                                client,
                                request.profile.models["grounding"],
                                state.answer_text,
                                state.references,
                                state.mappings,
                            )
                    unsupported = [
                        reference
                        for reference in state.references
                        if reference.get("groundingStatus")
                        in {"unsupported", "mixed"}
                    ]
                    unchecked = [
                        reference
                        for reference in state.references
                        if reference.get("resolutionStatus") == "resolved"
                        and reference.get("groundingStatus") == "unchecked"
                    ]
                    if unsupported:
                        prior = state.answer_validation or {}
                        state.answer_conf = {
                            "level": "red",
                            "note": "One or more cited source sets did not support the associated claim.",
                        }
                        state.answer_validation = stages._answer_validation_wire(
                            "needs_review",
                            summary="One or more cited sources do not support the associated claim.",
                            issues=list(prior.get("issues") or [])
                            + stages._grounding_failure_issues(unsupported),
                            original_answer=prior.get("originalAnswer"),
                        )
                    elif unchecked:
                        prior = state.answer_validation or {}
                        if state.answer_conf.get("level") == "green":
                            state.answer_conf = {
                                "level": "yellow",
                                "note": "Citation support checking was unavailable for one or more sources.",
                            }
                        if prior.get("status") != "needs_review":
                            state.answer_validation = stages._answer_validation_wire(
                                "unavailable",
                                summary="Citation support checking was unavailable for one or more sources.",
                                issues=list(prior.get("issues") or [])
                                + [
                                    {
                                        "id": "citation_grounding_unavailable",
                                        "status": "warn",
                                        "severity": "warn",
                                        "reason": "Citation support could not be checked.",
                                        "source_indices": [reference.get("index")],
                                    }
                                    for reference in unchecked
                                ],
                                original_answer=prior.get("originalAnswer"),
                            )
                    elif "review" not in request.profile.stages:
                        prior = state.answer_validation or {}
                        unresolved = any(
                            reference.get("resolutionStatus") == "unresolved"
                            for reference in state.references
                        )
                        status = (
                            "needs_review"
                            if unresolved
                            or state.citation_issues
                            or state.answer_conf.get("level") == "red"
                            else "checked"
                        )
                        state.answer_validation = stages._answer_validation_wire(
                            status,
                            summary=prior.get("summary")
                            or state.answer_conf.get("note", ""),
                            issues=list(prior.get("issues") or [])
                            + list(state.citation_issues),
                            original_answer=prior.get("originalAnswer"),
                        )
                    record_stage_timing()
                    if "review" in request.profile.stages:
                        yield (
                            "answer_validation",
                            _stream_payload(
                                state,
                                request,
                                in_depth={"status": "pending", "answer": ""},
                            ),
                        )
                        if await _disconnected(request):
                            return
                    continue

                if stage == "indepth":
                    if product and not stages._is_substantive_answer(state.answer_text):
                        state.indepth_error = (
                            "In-Depth was withheld because the final Answer was not substantive."
                        )
                        state.steps.append(
                            {
                                "role": "indepth_withheld",
                                "reason": "non-substantive-answer",
                            }
                        )
                        record_stage_timing()
                        continue
                    answer_status = str(
                        (state.answer_validation or {}).get("status") or ""
                    )
                    if product and answer_status not in {"checked", "edited"}:
                        state.indepth_error = (
                            "In-Depth was withheld because the final Answer needs review."
                            if answer_status == "needs_review"
                            else "In-Depth was withheld because the final Answer check is unavailable."
                        )
                        state.steps.append(
                            {
                                "role": "indepth_withheld",
                                "reason": "answer-validation-status",
                                "answer_validation_status": answer_status,
                            }
                        )
                        record_stage_timing()
                        continue
                    if product:
                        yield (
                            "indepth_pending",
                            _stream_payload(
                                state,
                                request,
                                in_depth={"status": "pending", "answer": ""},
                            ),
                        )
                    prior_answer = (
                        stages._latest_assistant_text(state.messages)
                        if request.profile.output_mode == "indepth"
                        else state.answer_text
                    )
                    try:
                        indepth_messages = state.messages
                        indepth_chart = state.chart
                        if product and request.profile.exact_tokenizer:
                            (
                                _indepth_view,
                                indepth_messages,
                                indepth_chart,
                            ) = await _select_indepth_context(
                                request, state, prior_answer
                            )
                        if (
                            "review" in request.profile.models
                            and request.profile.output_mode != "indepth"
                        ):
                            async def fit_review(claims: List[str]) -> str:
                                return await _select_indepth_review_context(
                                    request, state, prior_answer, claims
                                )

                            async def fit_retry(
                                extra_msgs: List[Dict[str, Any]],
                            ) -> List[Dict[str, Any]]:
                                _view, messages, _chart = (
                                    await _select_indepth_context(
                                        request,
                                        state,
                                        prior_answer,
                                        extra_msgs=extra_msgs,
                                        stage="indepth_retry",
                                    )
                                )
                                return messages

                            (
                                state.claims,
                                state.indepth_conf,
                            ) = await stages._gen_indepth(
                                client,
                                request.profile.models["indepth"],
                                indepth_messages,
                                _prompt(
                                    request.profile, "indepth", "synthesis-indepth"
                                ),
                                state.gathered,
                                prior_answer,
                                validator_model=request.profile.models["review"],
                                validator_prompt=str(
                                    request.profile.prompts.get("review")
                                    or "validation-rewrite"
                                ),
                                chart=indepth_chart,
                                synth_temperature=sampling["answer_temperature"],
                                synth_repeat_penalty=sampling["answer_repeat_penalty"],
                                synth_dry=sampling["answer_dry"],
                                validator_temperature=sampling["review_temperature"],
                                validator_repeat_penalty=sampling[
                                    "review_repeat_penalty"
                                ],
                                validator_dry=sampling["review_dry"],
                                max_tokens=request.max_tokens,
                                max_loops=int(
                                    request.profile.policies.get("review_loops", 1)
                                ),
                                steps=state.steps,
                                canonicalize_citations=(
                                    product and temporal_mode == "enforce"
                                ),
                                review_context_fitter=(
                                    fit_review
                                    if product and request.profile.exact_tokenizer
                                    else None
                                ),
                                retry_context_fitter=(
                                    fit_retry
                                    if product and request.profile.exact_tokenizer
                                    else None
                                ),
                            )
                        else:
                            state.claims = await stages._synthesize_indepth(
                                client,
                                request.profile.models["indepth"],
                                indepth_messages,
                                _prompt(
                                    request.profile, "indepth", "synthesis-indepth"
                                ),
                                state.gathered,
                                prior_answer,
                                temperature=sampling["answer_temperature"],
                                max_tokens=request.max_tokens,
                                repeat_penalty=sampling["answer_repeat_penalty"],
                                dry=sampling["answer_dry"],
                            )
                            state.steps.append(
                                {
                                    "role": "indepth",
                                    "model": request.profile.models["indepth"],
                                    "claims": list(state.claims),
                                }
                            )
                        if state.indepth_conf.get("status") == "unavailable":
                            state.indepth_error = (
                                "In-Depth was withheld because review was unavailable."
                            )
                    except InsufficientContextError as exc:
                        state.indepth_error = str(exc)
                        state.indepth_error_code = exc.code
                        state.indepth_mandatory_source_ids = list(
                            exc.mandatory_ids
                        )
                        state.steps.append(
                            {
                                "role": "indepth_withheld",
                                "reason": exc.code,
                                "mandatory_source_ids": list(
                                    exc.mandatory_ids
                                ),
                            }
                        )
                    except Exception as exc:
                        state.indepth_error = str(exc)
                    record_stage_timing()
                    continue

                if stage == "indepth_gate":
                    state.indepth_gate = temporal.gate_indepth_claims(
                        stages._latest_user_text(state.messages),
                        state.claims,
                        state.temporal_facts,
                        mode=temporal_mode,
                    )
                    if state.indepth_conf.get("status") == "unavailable":
                        state.indepth_gate["review_status"] = "unavailable"
                    elif state.indepth_conf.get("status") == "edited":
                        state.indepth_gate["review_status"] = "edited"
                        state.indepth_gate["review_removed"] = int(
                            state.indepth_conf.get("removed") or 0
                        )
                        state.indepth_gate["review_issues"] = str(
                            state.indepth_conf.get("issues") or ""
                        )
                        state.indepth_gate["review_attempts"] = int(
                            state.indepth_conf.get("review_attempts") or 1
                        )
                        if state.indepth_gate.get("status") == "checked":
                            state.indepth_gate["status"] = "edited"
                    state.claims = list(state.indepth_gate["claims"])
                    citation_checks: list[dict[str, Any]] = []
                    candidates: list[tuple[int, str, list[dict[str, Any]]]] = []
                    for claim_index, claim in enumerate(state.claims, 1):
                        claim_citations = stages._extract_citations(claim)
                        if not claim_citations:
                            citation_checks.append(
                                {
                                    "claim_index": claim_index,
                                    "claim": claim,
                                    "status": "fail",
                                    "reason": "In-Depth claim has no source citation.",
                                    "source_indices": [],
                                }
                            )
                            continue
                        claim_references = stages._resolve_references(
                            claim_citations,
                            state.mappings,
                            answer=claim,
                            grounding_status="unchecked",
                            answer_usage_location="indepth",
                        )
                        unresolved = [
                            reference
                            for reference in claim_references
                            if reference.get("resolutionStatus") == "unresolved"
                        ]
                        if unresolved:
                            citation_checks.append(
                                {
                                    "claim_index": claim_index,
                                    "claim": claim,
                                    "status": "fail",
                                    "reason": "In-Depth claim cites a source outside the current evidence ledger.",
                                    "source_indices": [
                                        reference.get("index")
                                        for reference in unresolved
                                    ],
                                }
                            )
                            continue
                        candidates.append((claim_index, claim, claim_references))

                    flattened = [
                        reference
                        for _claim_index, _claim, references in candidates
                        for reference in references
                    ]
                    if flattened and state.mappings:
                        flattened = await stages._ground_references(
                            client,
                            request.profile.models["grounding"],
                            "\n".join(claim for _index, claim, _refs in candidates),
                            flattened,
                            state.mappings,
                        )

                    accepted_claims: list[str] = []
                    indepth_references: list[dict[str, Any]] = []
                    offset = 0
                    for claim_index, claim, references in candidates:
                        grounded_references = flattened[
                            offset : offset + len(references)
                        ]
                        offset += len(references)
                        unsupported = [
                            reference
                            for reference in grounded_references
                            if reference.get("groundingStatus")
                            in {"unsupported", "mixed"}
                        ]
                        if unsupported:
                            citation_checks.append(
                                {
                                    "claim_index": claim_index,
                                    "claim": claim,
                                    "status": "fail",
                                    "reason": "In-Depth claim is not supported by its cited source.",
                                    "source_indices": [
                                        reference.get("index")
                                        for reference in unsupported
                                    ],
                                }
                            )
                            continue
                        unchecked = [
                            reference
                            for reference in grounded_references
                            if reference.get("groundingStatus") == "unchecked"
                        ]
                        if unchecked:
                            citation_checks.append(
                                {
                                    "claim_index": claim_index,
                                    "claim": claim,
                                    "status": "fail",
                                    "reason": "In-Depth citation support could not be checked within the grounding limits.",
                                    "source_indices": [
                                        reference.get("index")
                                        for reference in unchecked
                                    ],
                                }
                            )
                            continue
                        accepted_claims.append(claim)
                        indepth_references.extend(grounded_references)

                    state.claims = accepted_claims
                    state.indepth_gate["claims"] = accepted_claims
                    if citation_checks:
                        state.indepth_gate["citation_checks"] = citation_checks
                        state.indepth_gate["status"] = (
                            "edited" if accepted_claims else "needs_review"
                        )
                    for reference in indepth_references:
                        existing = next(
                            (
                                item
                                for item in state.references
                                if item.get("index") == reference.get("index")
                            ),
                            None,
                        )
                        if existing is None:
                            state.references.append(reference)
                            continue
                        _merge_reference_grounding(existing, reference)
                    if (
                        state.indepth_gate["status"] == "needs_review"
                        and not state.indepth_error
                    ):
                        state.indepth_error = (
                            "In-Depth was withheld because evidence checks rejected every claim."
                        )
                    if product:
                        record_stage_timing()
                        if state.indepth_error:
                            indepth = {
                                "status": "needs_review",
                                "answer": "",
                                "error": state.indepth_error,
                                "validation": state.indepth_gate,
                            }
                            if state.indepth_error_code:
                                indepth["errorCode"] = state.indepth_error_code
                                indepth["mandatorySourceIds"] = list(
                                    state.indepth_mandatory_source_ids
                                )
                            payload = json.loads(
                                _stream_payload(state, request, in_depth=indepth)
                            )
                            yield (
                                "indepth_error",
                                json.dumps(payload),
                            )
                        else:
                            indepth = {
                                "status": "complete",
                                "answer": "\n".join(
                                    "- " + claim for claim in state.claims
                                ),
                                "error": "",
                                "validation": state.indepth_gate,
                            }
                            payload = json.loads(
                                _stream_payload(state, request, in_depth=indepth)
                            )
                            yield (
                                "indepth_done",
                                json.dumps(payload),
                            )
                    else:
                        record_stage_timing()
                    continue

        if product:
            indepth_status = "needs_review" if state.indepth_error else "complete"
            indepth = {
                "status": indepth_status,
                "answer": (
                    ""
                    if state.indepth_error
                    else "\n".join("- " + claim for claim in state.claims)
                ),
                "error": state.indepth_error or "",
                "validation": state.indepth_gate,
            }
            if state.indepth_error_code:
                indepth["errorCode"] = state.indepth_error_code
                indepth["mandatorySourceIds"] = list(
                    state.indepth_mandatory_source_ids
                )
            yield "done", _stream_payload(state, request, in_depth=indepth)
        else:
            yield "result", _raw_result(request, state)

        write_execution_trace()
    except asyncio.CancelledError:
        record_stage_timing("cancelled")
        write_execution_trace()
        raise
    except ContextSourceError:
        record_stage_timing("failed")
        write_execution_trace()
        raise
    except Exception as exc:
        record_stage_timing("failed")
        fallback = stages._fallback_envelope(
            "I could not produce a complete answer for this turn. Please try again."
        )
        if product:
            payload = json.loads(fallback)
            fallback_gate = temporal.run_temporal_gate(
                stages._latest_user_text(state.messages),
                str(payload.get("answer") or ""),
                [],
                state.temporal_facts if temporal_enabled else None,
                temporal_mode,
            )
            fallback_gate["applied"] = "fallback"
            state.answer_gate = fallback_gate
            state.answer_conf = {
                "level": "red",
                "note": "The hub could not safely complete the configured stages.",
            }
            state.steps.append(
                {
                    "role": "pipeline_error",
                    "error": type(exc).__name__,
                    "temporal_gate": fallback_gate,
                }
            )
            payload["temporalGate"] = fallback_gate
            payload["answerValidation"] = stages._answer_validation_wire(
                "needs_review",
                summary="The hub could not safely complete the configured stages.",
            )
            payload["inDepth"] = {
                "status": "failed",
                "answer": "",
                "error": "In-Depth was not generated.",
            }
            write_execution_trace(answer_text=str(payload.get("answer") or ""))
            yield "done", json.dumps(payload)
        else:
            yield "result", fallback


def _chat_budget_policy(
    request: ExecutionRequest,
) -> Optional[stages.ChatBudgetPolicy]:
    if not request.profile.exact_tokenizer:
        return None
    return stages.ChatBudgetPolicy(
        counter=request.token_counter or RouterTokenCounter(),
        context_window=request.profile.context_window,
        reserved_output_tokens=request.profile.reserved_output_tokens,
    )


class StageEngine:
    """Single owner of profile event execution and blocking event drain."""

    async def events(self, request: ExecutionRequest) -> AsyncIterator[Tuple[str, str]]:
        cancelled = False
        budget_policy = _chat_budget_policy(request)
        events = _execute_stages(request, budget_policy).__aiter__()
        try:
            while True:
                budget_token = (
                    stages.activate_chat_budget(budget_policy)
                    if budget_policy is not None
                    else None
                )
                try:
                    event = await events.__anext__()
                except StopAsyncIteration:
                    return
                finally:
                    if budget_token is not None:
                        stages.reset_chat_budget(budget_token)
                yield event
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            await events.aclose()
            if cancelled:
                stages._write_cancellation_trace(
                    request.profile.id,
                    [dict(message) for message in request.messages],
                    router_lock_released=not stages._ROUTER_LOCK.locked(),
                )

    async def drain(self, request: ExecutionRequest) -> str:
        result: Optional[str] = None
        async for name, data in self.events(request):
            if name in {"result", "done"}:
                result = data
        if result is None:
            return stages._fallback_envelope(
                "I could not produce a complete answer for this turn. Please try again."
            )
        if request.profile.output_mode != "product":
            return result
        final_payload = json.loads(result or "{}")
        references = final_payload.get("references") or []
        answer_citations = [
            reference.get("index")
            for reference in references
            if isinstance(reference, dict)
            and isinstance(reference.get("index"), int)
            and any(
                isinstance(usage, dict)
                and usage.get("location") in {"answer", "block"}
                for usage in (reference.get("usage") or [])
            )
        ]
        output: Dict[str, Any] = {
            "answer": final_payload.get("answer") or "",
            "citations": answer_citations,
            "references": references,
            "blocks": final_payload.get("blocks") or [],
        }
        for key in (
            "confidence",
            "answerValidation",
            "inDepth",
            "model",
            "safetyWarnings",
            "context",
            "temporalGate",
        ):
            if key in final_payload:
                output[key] = final_payload[key]
        return json.dumps(output)


_ENGINE = StageEngine()


async def execute_profile(
    request: ExecutionRequest,
) -> AsyncIterator[Tuple[str, str]]:
    """Public streaming adapter over the shared stage engine."""
    events = _ENGINE.events(request)
    try:
        async for event in events:
            yield event
    finally:
        await events.aclose()


async def drain_profile(request: ExecutionRequest) -> str:
    """Public blocking adapter over the shared stage engine."""
    return await _ENGINE.drain(request)
