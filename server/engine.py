"""One stage engine for streaming and blocking med-agent-hub requests."""

from __future__ import annotations

import asyncio
import copy
import dataclasses
import hashlib
import json
import os
import re
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
    ContextSelector,
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
    context_selector: Optional[ContextSelector] = None


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
    """Complete profiles own their contract; low-level legs remain caller-controlled."""
    if (
        request.profile.output_mode == "product"
        or request.profile.policies.get("answer_contract") == "chart_answer"
    ):
        return _chart_answer_response_format()
    return request.response_format


def _confidence_policy(profile: Profile) -> str:
    """What the confidence sections carry (``policies.confidence``, or ``review.confidence``):
    ``levels`` (a green, yellow or red grade) or ``findings`` (no grade; the list of what the
    checks found)."""
    review = profile.policies.get("review") or {}
    return str(profile.policies.get("confidence") or review.get("confidence") or "levels")


_ISSUE_CHECKS = (
    ("agreement", "agreement"),
    ("citation_grounding", "citation"),
    ("citation_not_addressed", "citation"),
    ("citation_grounding_unavailable", "citation"),
    ("review_doubt", "review"),
    ("citation_", "record"),
)


def _answer_findings(state: "_State") -> List[Dict[str, Any]]:
    """What the checks found on the Answer, one entry per issue the validation carries."""
    findings: List[Dict[str, Any]] = []
    for issue in (state.answer_validation or {}).get("issues") or []:
        if not isinstance(issue, dict):
            continue
        issue_id = str(issue.get("id") or "")
        if issue.get("wrong") is not None and not issue_id:
            check = "review"
        else:
            check = next(
                (name for prefix, name in _ISSUE_CHECKS if issue_id.startswith(prefix)), "date"
            )
        says = str(issue.get("reason") or "").strip()
        if check == "review":
            wrong, chart = str(issue.get("wrong") or ""), str(issue.get("chart") or "")
            says = (f"questioned \"{wrong}\" against {chart}" if chart else wrong) or says
        findings.append(
            {
                "check": check,
                "where": "answer",
                "status": str(issue.get("status") or "fail"),
                "says": says,
            }
        )
    return findings


def _confidence_section(
    profile: Profile, section: Mapping[str, Any], findings: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """The shipped confidence section: the grade under ``levels``, no grade under ``findings``."""
    out: Dict[str, Any] = dict(section or {})
    if _confidence_policy(profile) == "findings" and out:
        out["level"] = None
    if out or findings:
        out["findings"] = findings
    return out


def _shadow_scorer(profile: Profile) -> bool:
    """Whether the profile records the entailment scorer's verdict beside each citation check."""
    review = profile.policies.get("review") or {}
    return str(review.get("shadow") or "") == "scorer"


def _grounding_policy(profile: Profile) -> str:
    """How the profile checks that cited records support their sentences (``review.grounding``)."""
    if "ground_verdicts" not in profile.stages:
        return "off"
    review = profile.policies.get("review") or {}
    return str(review.get("grounding") or "separate")


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
    initial_answer_text: Optional[str] = None
    initial_answer_citations: List[int] = field(default_factory=list)
    initial_answer_blocks: List[Any] = field(default_factory=list)
    original_answer_references: List[Dict[str, Any]] = field(default_factory=list)
    answer_check_edited: bool = False
    answer_emitted: bool = False
    stable_answer_snapshot: Optional[Dict[str, Any]] = None
    review_draft: Optional[str] = None
    review_draft_citations: List[int] = field(default_factory=list)
    review_draft_blocks: List[Any] = field(default_factory=list)
    review_edited: bool = False
    review_citation_verdicts: Optional[Dict[int, bool]] = None
    references: List[Dict[str, Any]] = field(default_factory=list)
    claims: List[str] = field(default_factory=list)
    indepth_review_draft: str = ""
    indepth_review_references: List[Dict[str, Any]] = field(default_factory=list)
    indepth_conf: Dict[str, Any] = field(
        default_factory=lambda: {"level": "green", "note": ""}
    )
    indepth_gate: Optional[Dict[str, Any]] = None
    # 1-based In-Depth claim index -> what each check marked on it; shown beside the claim.
    indepth_marks: Dict[int, List[str]] = field(default_factory=dict)
    indepth_findings: List[Dict[str, Any]] = field(default_factory=list)
    agreement: Optional[Dict[str, Any]] = None
    # Neutral tags: what a claim is, not a call to check it (uncited general guidance).
    indepth_tags: Dict[int, str] = field(default_factory=dict)
    indepth_error: str = ""
    indepth_error_code: Optional[str] = None
    indepth_required_source_ids: List[str] = field(default_factory=list)
    drug_context: Any = None
    raw_review_content: Optional[str] = None
    history: Optional[HistoryView] = None


def _conversation_history_summary(
    messages: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Describe prior conversational context without storing clinical plaintext."""
    conversational = [
        {
            "role": str(message.get("role") or ""),
            "content": message.get("content"),
        }
        for message in messages
        if message.get("role") in {"user", "assistant"}
    ]
    current_user_index = next(
        (
            index
            for index in range(len(conversational) - 1, -1, -1)
            if conversational[index]["role"] == "user"
        ),
        len(conversational),
    )
    prior = conversational[:current_user_index]
    encoded = json.dumps(
        prior, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    return {
        "prior_message_count": len(prior),
        "prior_turn_count": sum(item["role"] == "user" for item in prior),
        "prior_roles": [item["role"] for item in prior],
        "prior_messages_sha256": hashlib.sha256(encoded).hexdigest(),
    }


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
        "source_metadata": {
            name: dict(metadata)
            for name, metadata in state.ledger.source_metadata.items()
        },
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
    supports = {check.get("support") for check in checks if check.get("support")}
    if not supports:
        supports = {stages._support_of(existing), stages._support_of(incoming)}
    if statuses & {"unchecked", "checking", None} or stages.UNCHECKED in supports:
        support = stages.UNCHECKED
    elif stages.CONTRADICTED in supports or "unsupported" in statuses:
        support = stages.CONTRADICTED
    elif stages.NOT_ADDRESSED in supports:
        support = stages.NOT_ADDRESSED
    else:
        support = stages.SUPPORTED
    existing["support"] = support
    existing["grounded"] = {stages.SUPPORTED: True, stages.CONTRADICTED: False}.get(support)
    existing["groundingStatus"] = stages._GROUNDING_STATUS_FOR_SUPPORT[support]


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
            # Keep every other field, QueryStore's slice tier and rank included: a dropped
            # one silently sends the selector back to its local fallback ranking.
            records.append(
                dataclasses.replace(
                    existing,
                    date=mapping.get("date"),
                    text=str(mapping.get("text") or existing.text),
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
                metadata={
                    "review_state": mapping.get("reviewState"),
                    "package": mapping.get("package"),
                },
            )
        )
    return EvidenceLedger(
        tuple(records),
        original_text=chart,
        preamble=ledger.preamble,
        source_metadata=ledger.source_metadata,
    )


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
        ("review", "validation-label"),
        ("indepth", "synthesis-indepth"),
        ("orchestrator", "orchestrator"),
        ("expert", "medical_expert"),
    ):
        if role in request.profile.models:
            if role == "review":
                stem = str(request.profile.prompts.get(role) or fallback)
                parts.append(load_prompt(stem + "-answer"))
                folded = _grounding_policy(request.profile) == "folded"
                if folded:
                    parts.append(load_prompt(stages._CITATION_SUPPORT_PROMPT))
                parts.append(load_prompt(stem + "-indepth"))
                if folded:
                    parts.append(load_prompt(stages._INDEPTH_CITATION_SUPPORT_PROMPT))
            else:
                parts.append(_prompt(request.profile, role, fallback))
    if temporal_block:
        parts.append(temporal_block)
    return "\n\n".join(parts)


def requester_line(context: Optional[Mapping[str, Any]]) -> str:
    """One line naming who is asking, from the account context ChartSearchAI captured.

    Only the assigned roles and the session location's name reach the model; the account's
    identifiers, effective roles and locale stay in ChartSearchAI's audit record.
    """
    account = (context or {}).get("account_context")
    if not isinstance(account, Mapping) or account.get("source") != "openmrs_session":
        return ""
    roles = [str(role).strip() for role in account.get("assigned_roles") or () if str(role).strip()]
    location = account.get("session_location")
    place = str(location.get("name") or "").strip() if isinstance(location, Mapping) else ""
    if not roles and not place:
        return ""
    return "Requester: " + (", ".join(roles) or "unknown role") + (f" at {place}" if place else "")


def _replace_chart_message(
    messages: List[Dict[str, Any]], chart: str, requester: str = "", gathered: str = ""
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
                "content": stages.records_message(chart, gathered)["content"]
                + ("\n\n" + requester if requester else ""),
            },
        )
    return out


async def _reuse_answer_chart(
    state: _State, measure: Callable[[str], Awaitable[int]], stage: str
) -> bool:
    """Whether a later stage can keep the answer's records, so the router reuses their prefix."""
    if not state.chart or state.context_budget is None:
        return False
    tokens = await measure(state.chart)
    if tokens > state.context_budget.input_limit:
        return False
    state.steps.append(
        {
            "role": "context_selection",
            "stage": stage,
            "mode": "answer_records",
            "input_tokens": tokens,
            "input_limit": state.context_budget.input_limit,
        }
    )
    return True


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
    actual_messages = _replace_chart_message(base_messages, chart, requester_line(request.context), state.gathered)
    user = stages.with_gathered(
        _prompt(request.profile, "answer", "synthesis-answer"), state.gathered, actual_messages
    )
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
    selector = request.context_selector or select_context
    state.view = await selector(
        state.ledger,
        question=stages._latest_user_text(state.messages),
        model=request.profile.models["answer"],
        budget=state.context_budget,
        counter=state.token_counter,
        input_measure=lambda chart: _count_answer_input(request, state, chart),
    )
    state.chart = state.view.render()
    state.messages = _replace_chart_message(state.messages, state.chart, requester_line(request.context), state.gathered)


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
    messages = _replace_chart_message(state.messages, chart, requester_line(request.context), state.gathered)
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
    def measure(chart: str) -> Awaitable[int]:
        return _count_indepth_input(request, state, chart, prior_answer, extra_msgs)

    if state.view is not None and await _reuse_answer_chart(state, measure, stage):
        messages = _replace_chart_message(
            state.messages, state.chart, requester_line(request.context), state.gathered
        )
        return state.view, messages, state.chart
    selector = request.context_selector or select_context
    view = await selector(
        state.ledger,
        question=stages._latest_user_text(state.messages),
        model=request.profile.models["indepth"],
        budget=state.context_budget,
        counter=state.token_counter,
        input_measure=measure,
    )
    chart = view.render()
    messages = _replace_chart_message(state.messages, chart, requester_line(request.context), state.gathered)
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
    folded = _grounding_policy(request.profile) == "folded"
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
                request.profile.prompts.get("review") or "validation-label"
            ),
            citation_support=folded,
        )
        count_chat = getattr(counter, "count_chat", None)
        if callable(count_chat):
            return await count_chat(
                reviewer_model,
                {
                    "messages": messages,
                    "response_format": (
                        stages._LABEL_WITH_CITATIONS_VERDICT_RF
                        if folded
                        else stages._LABEL_VERDICT_RF
                    ),
                },
            )
        rendered = "\n".join(
            f"{message.get('role', 'user')}: {message.get('content', '')}"
            for message in messages
        )
        return await counter.count(reviewer_model, rendered)

    if await _reuse_answer_chart(state, measure, stage):
        return state.chart
    selector = request.context_selector or select_context
    view = await selector(
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
    folded = _grounding_policy(request.profile) == "folded"

    async def measure(chart: str) -> int:
        messages = stages._indepth_validation_messages(
            chart=chart,
            gathered=state.gathered,
            answer_text=prior_answer,
            claims=claims,
            validation_prompt=str(
                request.profile.prompts.get("review") or "validation-label"
            ),
            citation_support=folded,
        )
        count_chat = getattr(counter, "count_chat", None)
        if callable(count_chat):
            return await count_chat(
                reviewer_model,
                {
                    "messages": messages,
                    "response_format": (
                        stages._INDEPTH_WITH_CITATIONS_VERDICT_RF
                        if folded
                        else stages._INDEPTH_VERDICT_RF
                    ),
                },
            )
        rendered = "\n".join(
            f"{message.get('role', 'user')}: {message.get('content', '')}"
            for message in messages
        )
        return await counter.count(reviewer_model, rendered)

    if await _reuse_answer_chart(state, measure, "indepth_review"):
        return state.chart
    selector = request.context_selector or select_context
    view = await selector(
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
    context_started = time.perf_counter()
    timing: Dict[str, int] = {}
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
                supplemental_sources=request.profile.supplemental_sources,
                question=stages._latest_user_text(state.messages),
            )
        )
    except ContextSourceError:
        if request.patient or context.get("source") or requested_sources:
            raise
        ledger = EvidenceLedger(())
    timing["source_fetch_ms"] = round((time.perf_counter() - context_started) * 1000)

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
        drug_started = time.perf_counter()
        full_chart, mappings, state.drug_context = stages._prepare_drug_safety(
            full_chart,
            mappings,
            raw_records,
            stages._latest_user_text(state.messages),
            resolved_reference_date,
            True,
            exposure_complete=any(
                bool(metadata.get("patient_ledger_complete"))
                for metadata in ledger.source_metadata.values()
            ),
        )
        ledger = _ledger_after_drug_injection(ledger, full_chart, mappings)
        state.ledger = ledger
        timing["drug_safety_ms"] = round((time.perf_counter() - drug_started) * 1000)
    else:
        timing["drug_safety_ms"] = 0

    temporal_block = ""
    if temporal_enabled:
        temporal_started = time.perf_counter()
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
        timing["temporal_facts_ms"] = round(
            (time.perf_counter() - temporal_started) * 1000
        )
    else:
        timing["temporal_facts_ms"] = 0
    state.gathered = temporal_block

    if request.profile.exact_tokenizer:
        # StageEngine installs one request-scoped counter before context preparation.
        # Reusing it lets the final dispatch consume the exact count already measured
        # for the identical prompt instead of tokenizing that prompt again.
        counter = state.token_counter or request.token_counter or RouterTokenCounter()
        budget = ContextBudget(
            context_window=request.profile.context_window,
            reserved_output_tokens=request.profile.reserved_output_tokens,
        )
        state.token_counter = counter
        state.context_budget = budget
        mandatory = tuple(record for record in ledger.records if record.mandatory)
        mandatory_text = ledger.preamble + EvidenceLedger(mandatory).render()
        history_started = time.perf_counter()
        state.history = await fit_message_history(
            state.messages,
            model=request.profile.models["answer"],
            budget=budget,
            counter=counter,
            fixed_renderer=lambda messages: _fixed_context_text(
                request, messages, temporal_block
            ),
            mandatory_text=mandatory_text,
            required_ids=tuple(record.stable_id for record in mandatory),
            input_measure=lambda messages: _count_answer_input(
                request, state, mandatory_text, messages=messages
            ),
            defer_when_no_droppable=True,
        )
        timing["history_fit_ms"] = round((time.perf_counter() - history_started) * 1000)
        state.messages = [dict(message) for message in state.history.messages]
        selection_started = time.perf_counter()
        await _select_answer_context(request, state)
        timing["selection_ms"] = round((time.perf_counter() - selection_started) * 1000)
    else:
        timing["history_fit_ms"] = 0
        timing["selection_ms"] = 0
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
        state.messages = _replace_chart_message(state.messages, state.chart, requester_line(request.context), state.gathered)
    requester = requester_line(request.context)
    if requester:
        state.steps.append({"role": "requester", "line": requester})
    timing["total_ms"] = round((time.perf_counter() - context_started) * 1000)
    state.steps.append({"role": "context_preparation_timing", **timing})
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
        # Checks copy dates and values from the chart; the repetition penalty garbles them.
        "review_dry": stages._knob(knobs, "review", "dry", 0.0),
    }


async def _disconnected(request: ExecutionRequest) -> bool:
    try:
        return bool(request.is_disconnected and await request.is_disconnected())
    except Exception:
        return False


def _initial_answer_changed(state: _State) -> bool:
    initial = state.initial_answer_text
    _initial_block_text, initial_block_refs = stages._block_temporal_text_and_refs(
        state.initial_answer_blocks
    )
    _current_block_text, current_block_refs = stages._block_temporal_text_and_refs(
        state.blocks
    )
    initial_sources = stages._citation_indices(
        list(state.initial_answer_citations) + initial_block_refs,
        initial or "",
    )
    current_sources = stages._citation_indices(
        list(state.citations) + current_block_refs,
        state.answer_text,
    )
    # Citation order is not a change: resolving references can reorder the same records.
    return (state.answer_check_edited or state.review_edited) and initial is not None and (
        initial.strip() != state.answer_text.strip()
        or set(initial_sources) != set(current_sources)
        or state.initial_answer_blocks != state.blocks
    )


def _snapshot_stable_answer(state: _State) -> Dict[str, Any]:
    """Capture the latest coherent Answer state already shown to the client."""
    return copy.deepcopy(
        {
            "answer_text": state.answer_text,
            "citations": state.citations,
            "blocks": state.blocks,
            "references": state.references,
            "answer_conf": state.answer_conf,
            "answer_validation": state.answer_validation,
            "answer_gate": state.answer_gate,
            "citation_issues": state.citation_issues,
            "table_issues": state.table_issues,
            "original_answer_references": state.original_answer_references,
            "answer_check_edited": state.answer_check_edited,
            "review_edited": state.review_edited,
        }
    )


def _restore_stable_answer(state: _State) -> None:
    """Restore the last stable Answer after a later stage fails partway through."""
    snapshot = state.stable_answer_snapshot
    if not snapshot:
        return
    for name, value in snapshot.items():
        setattr(state, name, copy.deepcopy(value))


def _mark_answer_low_confidence(state: _State, note: str) -> None:
    """Keep lifecycle and confidence aligned for deterministic blocking issues."""
    existing = str(state.answer_conf.get("note") or "").strip()
    notes = [existing] if existing else []
    if note and note not in notes:
        notes.append(note)
    state.answer_conf = {"level": "red", "note": " ".join(notes)}


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
        payload["confidence"] = {
            "answer": _confidence_section(request.profile, state.answer_conf, _answer_findings(state))
        }
        if (
            in_depth is not None
            and in_depth.get("status") != "pending"
            and state.indepth_conf
        ):
            payload["confidence"]["in_depth"] = _confidence_section(
                request.profile, state.indepth_conf, list(state.indepth_findings)
            )
    if state.answer_validation is not None:
        validation = dict(state.answer_validation)
        initial = state.initial_answer_text
        initial_changed = _initial_answer_changed(state)
        if initial_changed:
            validation["originalAnswer"] = initial
            validation["originalReferences"] = list(
                state.original_answer_references
            )
            validation["originalBlocks"] = copy.deepcopy(
                state.initial_answer_blocks
            )
        elif initial is not None:
            validation.pop("originalAnswer", None)
            validation.pop("originalReferences", None)
            validation.pop("originalBlocks", None)
        payload["answerValidation"] = validation
    if state.answer_gate is not None:
        payload["temporalGate"] = state.answer_gate
    if state.agreement is not None:
        payload["agreement"] = state.agreement
    if in_depth is not None:
        payload["inDepth"] = in_depth
    safety_check = stages._compute_safety_check(
        state.drug_context,
        state.answer_text,
        stages._latest_user_text(state.messages),
        bool(request.profile.policies.get("drug_safety")),
    )
    payload["safetyStatus"] = safety_check.status
    payload["safetyCheck"] = safety_check.to_dict()
    if safety_check.warnings:
        payload["safetyWarnings"] = [
            warning.to_dict() for warning in safety_check.warnings
        ]
    payload["context"] = _context_summary(state)
    return json.dumps(payload)


def _latest_indepth_draft_claims(state: _State) -> List[str]:
    """Return the latest model draft that review or gates rejected or changed."""
    synthesis_roles = {"indepth", "indepth_synth", "indepth_resynth"}
    rejected: List[List[str]] = []
    fallback: List[List[str]] = []
    for index, step in enumerate(state.steps):
        if step.get("role") not in synthesis_roles:
            continue
        raw_claims = step.get("original_claims") or step.get("claims") or []
        if not isinstance(raw_claims, list):
            continue
        claims = [str(claim) for claim in raw_claims if str(claim).strip()]
        if not claims:
            continue
        fallback.append(claims)
        for later in state.steps[index + 1 :]:
            if later.get("role") in synthesis_roles:
                break
            if later.get("role") != "indepth_validator":
                continue
            if later.get("status") == "unavailable":
                rejected.append(claims)
            break
    if rejected:
        return rejected[-1]
    if fallback:
        return fallback[-1]
    return [str(claim) for claim in state.claims if str(claim).strip()]


def _capture_indepth_review_artifact(state: _State) -> None:
    claims = _latest_indepth_draft_claims(state)
    if not claims:
        return
    state.indepth_review_draft = "\n".join("- " + claim for claim in claims)
    state.indepth_review_references = stages._resolve_references(
        stages._extract_citations(state.indepth_review_draft),
        state.mappings,
        answer=state.indepth_review_draft,
        grounding_status="unchecked",
        answer_usage_location="indepth_review_draft",
    )


def _indepth_validation_summary(
    validation: Mapping[str, Any], error: str = ""
) -> str:
    status = str(validation.get("status") or "unavailable")
    lead = {
        "checked": "In-Depth claims were checked against chart, citation, and temporal rules.",
        "edited": (
            "In-Depth was updated after checks; a date was corrected from the chart's "
            "dated records."
        ),
        "needs_review": (
            "Some In-Depth points are marked for checking; they are shown, not removed."
        ),
        "unavailable": "The In-Depth checks could not be completed.",
        "no_claims": "No In-Depth claims were written; nothing beyond the answer was fully supported.",
    }.get(status, "The In-Depth check finished with an unknown status.")
    reasons: list[str] = []

    def add_reason(value: Any) -> None:
        cleaned = " ".join(str(value or "").split())
        if cleaned and cleaned not in reasons:
            reasons.append(cleaned)

    add_reason(validation.get("review_issues"))
    for check in validation.get("citation_checks") or []:
        if isinstance(check, Mapping) and check.get("status") in {"fail", "warn"}:
            add_reason(check.get("reason"))
    for claim_check in validation.get("checks") or []:
        if not isinstance(claim_check, Mapping):
            continue
        claim_gate = claim_check.get("gate")
        if not isinstance(claim_gate, Mapping):
            continue
        for check in claim_gate.get("checks") or []:
            if isinstance(check, Mapping) and check.get("status") == "fail":
                add_reason(check.get("reason"))
    if not reasons and error:
        add_reason(error)
    return " ".join([lead, *reasons[:2]])


def _indepth_mark_note(marked: int, total: int, review: Mapping[str, Any]) -> str:
    """Clinician-facing In-Depth confidence note: how many points are marked, and the review's words."""
    parts: list[str] = []
    if marked:
        parts.append(
            f"{marked} of {total} point{'s' if total != 1 else ''} "
            f"{'is' if marked == 1 else 'are'} marked for checking"
        )
    issues = " ".join(str(review.get("issues") or "").split())
    if review.get("status") == "flagged" and issues:
        parts.append("review note: " + issues.rstrip("."))
    if review.get("status") == "unreviewed":
        parts.append("In-Depth review was unavailable, so these points were not reviewed")
    if not parts:
        return ""
    note = "; ".join(parts)
    return note[0].upper() + note[1:] + ". Verify marked points against the chart."


def _date_check_label(entry: Mapping[str, Any]) -> str:
    """A short inline reason for a claim the date check failed, in the chart's terms."""
    for check in (entry.get("gate") or {}).get("checks") or []:
        if check.get("status") != "fail":
            continue
        reason = str(check.get("reason") or "")
        found = re.match(r"(.+?) has fewer than two dated points", reason)
        if check.get("id") == "single_point_trend" and found:
            return f"the chart has one dated {found.group(1)} value, so no trend"
        found = re.search(r"binds value ([\d.]+) to (\d{4}-\d{2}-\d{2})", reason)
        if check.get("id") == "date_value_binding" and found:
            value = found.group(1).removesuffix(".0")
            return f"the chart does not give {value} on {found.group(2)}"
        found = re.search(r"emits '([^']+)'", reason)
        if check.get("id") == "date_format" and found:
            return f"{found.group(1)} is not a date in the chart"
        found = re.search(r"last clinical encounter (\d{4}-\d{2}-\d{2})", reason)
        if check.get("id") == "last_visit" and found:
            return f"the last visit in the chart is {found.group(1)}"
        if check.get("id") == "trend_direction":
            return "the chart's values move the other way"
    return "the date check found a conflict"


def _marked_claim(claim: str, labels: Sequence[str], tag: str = "") -> str:
    if labels:
        return claim + " *(Check: " + "; ".join(labels) + ".)*"
    if tag:
        return claim + " *(" + tag[0].upper() + tag[1:] + ".)*"
    return claim


def _in_depth_payload(state: _State) -> Dict[str, Any]:
    plain_answer = "\n".join("- " + claim for claim in state.claims)
    final_answer = (
        ""
        if state.indepth_error
        else "\n".join(
            "- "
            + _marked_claim(
                claim,
                state.indepth_marks.get(index, ()),
                state.indepth_tags.get(index, ""),
            )
            for index, claim in enumerate(state.claims, 1)
        )
    )
    validation = dict(state.indepth_gate or {})
    if state.indepth_error and not validation.get("status"):
        validation["status"] = "needs_review"
    if validation and not validation.get("summary"):
        validation["summary"] = _indepth_validation_summary(
            validation, state.indepth_error or ""
        )
    payload: Dict[str, Any] = {
        "status": "needs_review" if state.indepth_error else "complete",
        "answer": final_answer,
        "error": state.indepth_error or "",
        "validation": validation or None,
    }
    if state.indepth_error_code:
        payload["errorCode"] = state.indepth_error_code
        payload["requiredSourceIds"] = list(state.indepth_required_source_ids)
    if state.indepth_review_draft and state.indepth_review_draft.strip() != (
        "" if state.indepth_error else plain_answer.strip()
    ):
        payload["reviewDraft"] = state.indepth_review_draft
        payload["reviewReferences"] = list(state.indepth_review_references)
    return payload


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
        safety_check = stages._compute_safety_check(
            state.drug_context,
            state.answer_text,
            stages._latest_user_text(state.messages),
            bool(request.profile.policies.get("drug_safety")),
        )
        payload["safetyStatus"] = safety_check.status
        payload["safetyCheck"] = safety_check.to_dict()
        if safety_check.warnings:
            payload["safetyWarnings"] = [
                warning.to_dict() for warning in safety_check.warnings
            ]
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
        safety_check = stages._compute_safety_check(
            state.drug_context,
            state.answer_text,
            stages._latest_user_text(state.messages),
            bool(request.profile.policies.get("drug_safety")),
        )
        payload["safetyStatus"] = safety_check.status
        payload["safetyCheck"] = safety_check.to_dict()
        if safety_check.warnings:
            payload["safetyWarnings"] = [
                warning.to_dict() for warning in safety_check.warnings
            ]
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
    state.steps.append(
        {"role": "conversation_history", **_conversation_history_summary(request.messages)}
    )
    sampling = _sampling(request)
    answer_response_format = _answer_response_format(request)
    grounding = _grounding_policy(request.profile)
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
            stats = getattr(budget_policy.counter, "stats", None)
            state.steps.append(
                {
                    "role": "exact_request_budgets",
                    "measurements": list(budget_policy.measurements),
                    "token_counter": stats() if callable(stats) else None,
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
            original_answer_text=(
                state.initial_answer_text
                if _initial_answer_changed(state)
                else state.original_answer
            ),
            answer_validation=state.answer_validation,
            context_summary=_context_summary(state),
            request_context=request.context,
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
                        # The orchestrator reads the records alone, as before gathered
                        # evidence joined the shared context message.
                        messages=_replace_chart_message(
                            state.messages, state.chart, requester_line(request.context)
                        ),
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
                    state.initial_answer_text = state.answer_text
                    state.initial_answer_citations = list(state.citations)
                    state.initial_answer_blocks = copy.deepcopy(state.blocks)
                    state.steps.append(
                        {
                            "role": "answer_synth",
                            "model": request.profile.models["answer"],
                            "output": state.answer_text,
                            "citations": state.citations,
                            "blocks": copy.deepcopy(state.blocks),
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
                    state.answer_check_edited = (
                        state.answer_text != state.initial_answer_text
                        or state.citations != state.initial_answer_citations
                        or state.blocks != state.initial_answer_blocks
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
                        if block_issues or gate_count == 1 or state.review_edited:
                            # Preserve table failures when review changes nothing.
                            # untouched. Once a rewrite removes that block, only current
                            # envelope issues remain lifecycle blockers.
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
                        if gate_count == 1 and (
                            state.blocks != before_blocks or block_issues
                        ):
                            state.answer_check_edited = True
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
                        state.answer_check_edited = state.answer_check_edited or (
                            state.answer_gate.get("applied") in {"patch", "fallback"}
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
                        state.answer_check_edited = state.answer_check_edited or (
                            state.answer_gate.get("applied") in {"patch", "fallback"}
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
                        if gate_count == 1 and contract_changed:
                            state.answer_check_edited = True
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
                        if state.citation_issues:
                            _mark_answer_low_confidence(
                                state,
                                "The answer has a blocking citation-format or citation-scope issue.",
                            )
                    record_stage_timing()
                    continue

                if stage == "resolve_refs":
                    if state.initial_answer_text is not None:
                        state.original_answer_references = stages._resolve_references(
                            state.initial_answer_citations,
                            state.mappings,
                            answer=state.initial_answer_text,
                            blocks=state.initial_answer_blocks,
                            grounding_status="unchecked",
                            answer_usage_location="answer_review_draft",
                        )
                    state.references = stages._resolve_references(
                        state.citations,
                        state.mappings,
                        answer=state.answer_text,
                        blocks=state.blocks,
                        grounding_status=(
                            ("unchecked" if grounding == "off" else "checking")
                            if state.mappings
                            else None
                        ),
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
                    if unresolved:
                        _mark_answer_low_confidence(
                            state,
                            "One or more citations do not resolve to the current evidence ledger.",
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
                    state.answer_emitted = True
                    state.stable_answer_snapshot = _snapshot_stable_answer(state)
                    yield (
                        "answer_done",
                        _stream_payload(
                            state,
                            request,
                            in_depth=(
                                {"status": "pending", "answer": ""}
                                if "indepth" in request.profile.stages
                                else {"status": "complete", "answer": ""}
                            ),
                        ),
                    )
                    if has_review:
                        state.answer_validation = prior_validation
                    if await _disconnected(request):
                        return
                    continue

                if stage == "review":
                    if grounding == "folded":
                        state.review_citation_verdicts = {}
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
                        reviewed_original_answer,
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
                            or "validation-label"
                        ),
                        validator_temperature=sampling["review_temperature"],
                        validator_repeat_penalty=sampling["review_repeat_penalty"],
                        validator_dry=sampling["review_dry"],
                        max_tokens=request.max_tokens,
                        steps=state.steps,
                        payload_override=payload_override,
                        review_chart_selector=review_chart_selector,
                        citation_verdicts=state.review_citation_verdicts,
                    )
                    if reviewed_gate is not None:
                        state.answer_gate = reviewed_gate
                    if state.original_answer is None:
                        state.original_answer = reviewed_original_answer
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
                        grounding_status=(
                            ("unchecked" if grounding == "off" else "checking")
                            if state.mappings
                            else None
                        ),
                    )
                    if "review" in request.profile.stages:
                        prior_validation = state.answer_validation or {}
                        prior_status = prior_validation.get("status")
                        unresolved_final = any(
                            reference.get("resolutionStatus") == "unresolved"
                            for reference in state.references
                        )
                        if unresolved_final:
                            _mark_answer_low_confidence(
                                state,
                                "One or more citations do not resolve to the current evidence ledger.",
                            )
                        if state.citation_issues:
                            _mark_answer_low_confidence(
                                state,
                                "The answer has a blocking citation-format or citation-scope issue.",
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
                        elif state.review_edited or _initial_answer_changed(state):
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
                    elif "ground_verdicts" not in request.profile.stages:
                        # No review and no citation check: the deterministic checks alone set
                        # the status (a gate patch is an edit, a failed gate or an unresolved
                        # record needs review).
                        prior = state.answer_validation or {}
                        unresolved_final = any(
                            reference.get("resolutionStatus") == "unresolved"
                            for reference in state.references
                        )
                        if unresolved_final:
                            _mark_answer_low_confidence(
                                state,
                                "One or more citations do not resolve to the current evidence ledger.",
                            )
                        if state.citation_issues:
                            _mark_answer_low_confidence(
                                state,
                                "The answer has a blocking citation-format or citation-scope issue.",
                            )
                        status = (
                            "needs_review"
                            if unresolved_final
                            or state.citation_issues
                            or state.answer_conf.get("level") == "red"
                            else "edited"
                            if _initial_answer_changed(state)
                            else "checked"
                        )
                        state.answer_validation = stages._answer_validation_wire(
                            status,
                            summary=prior.get("summary") or state.answer_conf.get("note", ""),
                            issues=list(prior.get("issues") or []) + list(state.citation_issues),
                            original_answer=prior.get("originalAnswer"),
                        )
                    record_stage_timing()
                    continue

                if stage == "agree":
                    # A second writer from another family answers the same question on its own;
                    # the answer shown is the first writer's, never a merged text. Each sentence
                    # of it is then checked against the second answer as a source: a fact the
                    # second writer states differently is a disagreement the clinician sees.
                    second_model = request.profile.models["second"]
                    second_text, _second_citations, _second_blocks = await stages._synthesize_answer(
                        client,
                        second_model,
                        state.messages,
                        _prompt(request.profile, "answer", "synthesis-answer"),
                        state.gathered,
                        response_format=answer_response_format,
                        temperature=sampling["answer_temperature"],
                        max_tokens=request.max_tokens,
                        repeat_penalty=sampling["answer_repeat_penalty"],
                        dry=0.0,
                    )
                    claims = stages._answer_claims(state.answer_text, state.blocks)
                    verdicts = (
                        await stages._bounded_entailment_verdicts(
                            client,
                            second_model,
                            [(second_text, claim) for claim in claims],
                        )
                        if claims and second_text.strip()
                        else []
                    )
                    state.agreement = stages._agreement_record(
                        second_model, second_text, claims, verdicts
                    )
                    state.steps.append(
                        {
                            "role": "second_writer",
                            "model": second_model,
                            "output": second_text,
                            "agreement": state.agreement["status"],
                            "claims": len(claims),
                        }
                    )
                    issues = stages._agreement_issues(state.agreement)
                    if issues:
                        prior = state.answer_validation or {}
                        differs = [issue for issue in issues if issue["status"] == "fail"]
                        if differs:
                            _mark_answer_low_confidence(
                                state,
                                "The two writers differ on " + "; ".join(
                                    issue["claim"] for issue in differs
                                ) + ".",
                            )
                        state.answer_validation = stages._answer_validation_wire(
                            "needs_review" if differs else str(prior.get("status") or "checked"),
                            summary=(
                                "A second writer from another model family stated something different."
                                if differs
                                else str(prior.get("summary") or "")
                            ),
                            issues=list(prior.get("issues") or []) + issues,
                            original_answer=prior.get("originalAnswer"),
                        )
                    record_stage_timing()
                    continue

                if stage == "ground_verdicts":
                    if grounding == "off":
                        # The profile checks no citation support: citations stay unchecked and no
                        # label claims a check that was never meant to run.
                        state.steps.append({"role": "grounding", "policy": "off"})
                    elif state.mappings:
                        deterministic_checks = [
                            check
                            for check in (state.answer_gate or {}).get("checks", [])
                            if check.get("status") == "pass"
                            and check.get("source_indices")
                        ]
                        folded = grounding == "folded"
                        grounding_options: Dict[str, Any] = {}
                        if _shadow_scorer(request.profile):
                            grounding_options["shadow_scorer"] = True
                        if deterministic_checks:
                            grounding_options["deterministic_checks"] = deterministic_checks
                        # A folded review that judged no cited record (the 9B returned an empty
                        # list in 5 of 7 verify turns on 28 October) falls back to the separate
                        # check with the review model, so citations are never left unchecked
                        # by an omission.
                        cited_now = {
                            reference.get("index")
                            for reference in state.references
                            if reference.get("resolutionStatus") == "resolved"
                        }
                        fold_missed = folded and bool(cited_now) and not (
                            cited_now & set((state.review_citation_verdicts or {}).keys())
                        )
                        if folded and not fold_missed:
                            grounding_options["index_verdicts"] = (
                                state.review_citation_verdicts or {}
                            )
                        state.references = await stages._ground_references(
                            client,
                            None
                            if folded and not fold_missed
                            else request.profile.models.get("grounding")
                            or request.profile.models["review"],
                            state.answer_text,
                            state.references,
                            state.mappings,
                            **grounding_options,
                        )
                        state.steps.append(
                            {
                                "role": "grounding",
                                "policy": "folded-fallback" if fold_missed else grounding,
                                "model": (
                                    request.profile.models["review"]
                                    if folded
                                    else request.profile.models["grounding"]
                                ),
                            }
                        )
                    contradicted = [
                        reference
                        for reference in state.references
                        if stages._support_of(reference) == stages.CONTRADICTED
                    ]
                    not_addressed = [
                        reference
                        for reference in state.references
                        if stages._support_of(reference) == stages.NOT_ADDRESSED
                    ]
                    unchecked = [
                        reference
                        for reference in state.references
                        if grounding != "off"
                        and reference.get("resolutionStatus") == "resolved"
                        and stages._support_of(reference) == stages.UNCHECKED
                    ]
                    if not_addressed:
                        # A record that does not hold a stated fact is incomplete citing, a
                        # note for the reader, never a mark against the answer.
                        prior = state.answer_validation or {}
                        state.answer_validation = stages._answer_validation_wire(
                            str(prior.get("status") or "checked"),
                            summary=str(prior.get("summary") or ""),
                            issues=list(prior.get("issues") or [])
                            + stages._grounding_failure_issues(not_addressed),
                            original_answer=prior.get("originalAnswer"),
                        )
                    if contradicted:
                        prior = state.answer_validation or {}
                        state.answer_conf = {
                            "level": "red",
                            "note": stages._contradiction_note(contradicted),
                        }
                        state.answer_validation = stages._answer_validation_wire(
                            "needs_review",
                            summary="A cited record states something different from the answer.",
                            issues=list(prior.get("issues") or [])
                            + stages._grounding_failure_issues(contradicted),
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
                        if unresolved:
                            _mark_answer_low_confidence(
                                state,
                                "One or more citations do not resolve to the current evidence ledger.",
                            )
                        if state.citation_issues:
                            _mark_answer_low_confidence(
                                state,
                                "The answer has a blocking citation-format or citation-scope issue.",
                            )
                        status = (
                            "needs_review"
                            if unresolved
                            or state.citation_issues
                            or state.answer_conf.get("level") == "red"
                            else (
                                "edited"
                                if _initial_answer_changed(state)
                                else "checked"
                            )
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
                        state.stable_answer_snapshot = _snapshot_stable_answer(state)
                        yield (
                            "answer_validation",
                            _stream_payload(
                                state,
                                request,
                                in_depth=(
                                    {"status": "pending", "answer": ""}
                                    if "indepth" in request.profile.stages
                                    else {"status": "complete", "answer": ""}
                                ),
                            ),
                        )
                        if await _disconnected(request):
                            return
                    continue

                if stage == "indepth":
                    if product:
                        state.stable_answer_snapshot = _snapshot_stable_answer(state)
                        yield (
                            "indepth_pending",
                            _stream_payload(
                                state,
                                request,
                                in_depth={"status": "pending", "answer": ""},
                            ),
                        )
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
                    # In-depth is withheld only on preemption (disconnect / new turn), never
                    # because the answer needs review: it runs on the answer as-is, and the
                    # answer's own validation status is surfaced separately on the envelope.
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
                                    or "validation-label"
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
                                steps=state.steps,
                                canonicalize_citations=(
                                    product and temporal_mode == "enforce"
                                ),
                                review_context_fitter=(
                                    fit_review
                                    if product and request.profile.exact_tokenizer
                                    else None
                                ),
                                **(
                                    {"citation_support": True}
                                    if grounding == "folded"
                                    else {}
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
                        _capture_indepth_review_artifact(state)
                    except InsufficientContextError as exc:
                        _capture_indepth_review_artifact(state)
                        state.indepth_error = str(exc)
                        state.indepth_error_code = exc.code
                        state.indepth_required_source_ids = list(
                            exc.required_ids
                        )
                        state.steps.append(
                            {
                                "role": "indepth_withheld",
                                "reason": exc.code,
                                "required_source_ids": list(
                                    exc.required_ids
                                ),
                            }
                        )
                    except Exception as exc:
                        _capture_indepth_review_artifact(state)
                        state.indepth_error = str(exc)
                    record_stage_timing()
                    continue

                if stage == "indepth_gate":
                    # A synthesis that wrote no claims answered as its prompt asks when nothing more is
                    # supported. Every written claim is shown; the checks below mark it, never remove it.
                    wrote_no_claims = not state.claims and not state.indepth_error
                    review = dict(state.indepth_conf or {})
                    state.indepth_gate = temporal.gate_indepth_claims(
                        state.claims,
                        state.temporal_facts,
                        mode=temporal_mode,
                    )
                    if wrote_no_claims:
                        state.indepth_gate["status"] = "no_claims"
                        state.indepth_conf = {}
                    state.claims = list(state.indepth_gate["claims"])
                    marks: Dict[int, List[Tuple[str, str]]] = {}
                    state.indepth_findings = []

                    def mark(claim_index: int, level: str, label: str, check: str) -> None:
                        if (level, label) not in marks.setdefault(claim_index, []):
                            marks[claim_index].append((level, label))
                            state.indepth_findings.append(
                                {"check": check, "where": claim_index, "says": label}
                            )

                    gate_entries = {
                        entry.get("claim_index"): entry
                        for entry in state.indepth_gate.get("checks") or []
                    }
                    for claim_index in state.indepth_gate.get("flagged") or []:
                        mark(
                            claim_index,
                            "red",
                            _date_check_label(gate_entries.get(claim_index) or {}),
                            "date",
                        )
                    if review.get("status") == "flagged":
                        state.indepth_gate["review_status"] = "flagged"
                        state.indepth_gate["review_flagged"] = list(
                            review.get("flagged") or []
                        )
                        state.indepth_gate["review_issues"] = str(
                            review.get("issues") or ""
                        )
                        for claim_index in review.get("flagged") or []:
                            mark(claim_index, "yellow", "flagged on review", "review")
                    elif review.get("status") == "unreviewed":
                        state.indepth_gate["review_status"] = "unavailable"
                    elif "review" in request.profile.models:
                        state.indepth_gate["review_status"] = "clean"
                    citation_checks: list[dict[str, Any]] = []
                    candidates: list[tuple[int, str, list[dict[str, Any]]]] = []
                    for claim_index, claim in enumerate(state.claims, 1):
                        claim_citations = stages._extract_citations(claim)
                        if not claim_citations:
                            # General guidance cites no record. It is tagged, not marked for review.
                            state.indepth_tags[claim_index] = "general guidance, not from the chart"
                            citation_checks.append(
                                {
                                    "claim_index": claim_index,
                                    "claim": claim,
                                    "status": "info",
                                    "reason": "In-Depth claim cites no record; it is general guidance.",
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
                            mark(claim_index, "red", "it cites a record not in this chart", "record")
                            continue
                        candidates.append((claim_index, claim, claim_references))

                    flattened = [
                        reference
                        for _claim_index, _claim, references in candidates
                        for reference in references
                    ]
                    if (
                        grounding == "folded"
                        and flattened
                        and state.mappings
                        and review.get("citation_unsupported") is not None
                    ):
                        # The In-Depth review judged each claim's citations in its own call.
                        unsupported_claims = review.get("citation_unsupported")
                        grounded: list[dict[str, Any]] = []
                        for claim_index, claim, references in candidates:
                            verdicts = (
                                {
                                    reference.get("index"): claim_index
                                    not in unsupported_claims
                                    for reference in references
                                }
                                if unsupported_claims is not None
                                else {}
                            )
                            grounded.extend(
                                await stages._ground_references(
                                    client,
                                    None,
                                    claim,
                                    references,
                                    state.mappings,
                                    index_verdicts=verdicts,
                                )
                            )
                        flattened = grounded
                    elif grounding in {"separate", "folded"} and flattened and state.mappings:
                        # Separate, or a folded In-Depth review that returned no citation list.
                        flattened = await stages._ground_references(
                            client,
                            request.profile.models.get("grounding")
                            or request.profile.models["review"],
                            "\n".join(claim for _index, claim, _refs in candidates),
                            flattened,
                            state.mappings,
                            shadow_scorer=_shadow_scorer(request.profile),
                        )

                    indepth_references: list[dict[str, Any]] = []
                    offset = 0
                    for claim_index, claim, references in candidates:
                        grounded_references = flattened[
                            offset : offset + len(references)
                        ]
                        offset += len(references)
                        indepth_references.extend(grounded_references)
                        contradicted = [
                            reference
                            for reference in grounded_references
                            if stages._support_of(reference) == stages.CONTRADICTED
                        ]
                        if contradicted:
                            chart_value = "; ".join(
                                dict.fromkeys(
                                    check["chart_value"]
                                    for reference in contradicted
                                    for check in reference.get("groundingChecks") or []
                                    if check.get("chart_value")
                                    and stages._check_support(check) == stages.CONTRADICTED
                                )
                            )
                            cited = ", ".join(
                                f"[{reference.get('index')}]" for reference in contradicted
                            )
                            citation_checks.append(
                                {
                                    "claim_index": claim_index,
                                    "claim": claim,
                                    "status": "fail",
                                    "reason": (
                                        f"{cited} states something different: {chart_value}."
                                        if chart_value
                                        else f"{cited} states something different from this claim."
                                    ),
                                    "source_indices": [
                                        reference.get("index") for reference in contradicted
                                    ],
                                }
                            )
                            mark(
                                claim_index,
                                "red",
                                f"{cited} states {chart_value}"
                                if chart_value
                                else f"{cited} states something different",
                                "citation",
                            )
                            continue
                        not_addressed = [
                            reference
                            for reference in grounded_references
                            if stages._support_of(reference) == stages.NOT_ADDRESSED
                        ]
                        if not_addressed:
                            # The record holds part of the claim, not all of it: a note, not a mark.
                            not_found = "; ".join(
                                dict.fromkeys(
                                    check["not_found"]
                                    for reference in not_addressed
                                    for check in reference.get("groundingChecks") or []
                                    if check.get("not_found")
                                )
                            )
                            cited = ", ".join(
                                f"[{reference.get('index')}]" for reference in not_addressed
                            )
                            citation_checks.append(
                                {
                                    "claim_index": claim_index,
                                    "claim": claim,
                                    "status": "not_addressed",
                                    "reason": (
                                        f"{cited} does not hold: {not_found}."
                                        if not_found
                                        else f"{cited} does not hold part of this claim."
                                    ),
                                    "source_indices": [
                                        reference.get("index") for reference in not_addressed
                                    ],
                                }
                            )
                            continue
                        unchecked = [
                            reference
                            for reference in grounded_references
                            if grounding != "off"
                            and stages._support_of(reference) == stages.UNCHECKED
                        ]
                        if unchecked:
                            citation_checks.append(
                                {
                                    "claim_index": claim_index,
                                    "claim": claim,
                                    "status": "warn",
                                    "reason": "In-Depth citation support could not be checked within the grounding limits.",
                                    "source_indices": [
                                        reference.get("index")
                                        for reference in unchecked
                                    ],
                                }
                            )
                            mark(claim_index, "yellow", "its citation was not checked", "citation")
                            continue
                        if grounding != "off" and grounded_references:
                            citation_checks.append(
                                {
                                    "claim_index": claim_index,
                                    "claim": claim,
                                    "status": "pass",
                                    "reason": "Its cited records state what the claim attributes to them.",
                                    "source_indices": [
                                        reference.get("index") for reference in grounded_references
                                    ],
                                }
                            )

                    state.indepth_marks = {
                        claim_index: [label for _level, label in labels]
                        for claim_index, labels in sorted(marks.items())
                    }
                    if citation_checks:
                        state.indepth_gate["citation_checks"] = citation_checks
                    if marks:
                        state.indepth_gate["status"] = "needs_review"
                        state.indepth_gate["marked"] = sorted(marks)
                    levels = {level for labels in marks.values() for level, _label in labels}
                    if state.claims and not state.indepth_error:
                        # The level grades the In-Depth as a whole: one marked point among
                        # several is a point to check, not a low-confidence section.
                        level = (
                            "red"
                            if "red" in levels and len(marks) >= len(state.claims)
                            else "yellow"
                            if levels or review.get("status") == "unreviewed"
                            else str(review.get("level") or "green")
                        )
                        state.indepth_conf = {
                            "level": level,
                            "note": _indepth_mark_note(
                                len(marks), len(state.claims), review
                            ),
                        }
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
                    if state.indepth_error:
                        state.indepth_conf = {
                            **state.indepth_conf,
                            "level": "red",
                            "note": state.indepth_error,
                        }
                    if product:
                        record_stage_timing()
                        indepth = _in_depth_payload(state)
                        if state.indepth_error:
                            payload = json.loads(
                                _stream_payload(state, request, in_depth=indepth)
                            )
                            yield (
                                "indepth_error",
                                json.dumps(payload),
                            )
                        else:
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
            yield "done", _stream_payload(
                state, request, in_depth=_in_depth_payload(state)
            )
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
        state.steps.append(
            {
                "role": "pipeline_error",
                "error": type(exc).__name__,
            }
        )
        if product and state.answer_emitted and (
            state.stable_answer_snapshot or state.answer_text.strip()
        ):
            _restore_stable_answer(state)
            note = "The hub could not safely complete the remaining checks."
            state.answer_conf = {"level": "red", "note": note}
            prior_validation = state.answer_validation or {}
            state.answer_validation = stages._answer_validation_wire(
                "needs_review",
                summary=note,
                issues=list(prior_validation.get("issues") or []),
                original_answer=prior_validation.get("originalAnswer"),
            )
            state.indepth_error = "In-Depth was not generated."
            state.indepth_conf = {"level": "red", "note": state.indepth_error}
            payload = _stream_payload(
                state, request, in_depth=_in_depth_payload(state)
            )
            write_execution_trace(answer_text=state.answer_text)
            yield "done", payload
            return
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
            state.steps[-1]["temporal_gate"] = fallback_gate
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
            safety_check = stages._compute_safety_check(
                state.drug_context,
                str(payload.get("answer") or ""),
                stages._latest_user_text(state.messages),
                bool(request.profile.policies.get("drug_safety")),
            )
            payload["safetyStatus"] = safety_check.status
            payload["safetyCheck"] = safety_check.to_dict()
            if safety_check.warnings:
                payload["safetyWarnings"] = [
                    warning.to_dict() for warning in safety_check.warnings
                ]
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
        router_slot_evidence = stages.RouterSlotEvidence()
        events = _execute_stages(request, budget_policy).__aiter__()
        try:
            while True:
                router_slot_token = stages.activate_router_slot_evidence(
                    router_slot_evidence
                )
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
                    stages.reset_router_slot_evidence(router_slot_token)
                yield event
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            router_slot_token = stages.activate_router_slot_evidence(
                router_slot_evidence
            )
            try:
                await events.aclose()
            finally:
                if cancelled:
                    stages._write_cancellation_trace(
                        request.profile.id,
                        [dict(message) for message in request.messages],
                        router_lock_released=router_slot_evidence.fully_released,
                        router_slot_evidence=router_slot_evidence.snapshot(),
                    )
                stages.reset_router_slot_evidence(router_slot_token)
                if (
                    request.token_counter is None
                    and budget_policy is not None
                    and isinstance(budget_policy.counter, RouterTokenCounter)
                ):
                    await budget_policy.counter.aclose()

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
            "safetyStatus",
            "safetyWarnings",
            "safetyCheck",
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
