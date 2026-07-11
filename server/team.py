"""Reusable clinical answer, review, grounding, and optional gather stages."""

import asyncio
import json
import logging
import os
import re
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple

import httpx

from . import drug_safety, kb, temporal
from .config import EXPERT_DRY_MULTIPLIER, llm_config
from .context_sources import (
    ChatTokenCounter,
    ContextSourceError,
    InsufficientContextError,
)
from .prompt_loader import load_prompt

logger = logging.getLogger(__name__)

# Small-model tool-calling degrades over long chains; keep the loop short.
MAX_TOOL_ITERATIONS = 3

# The orchestrator, medical_expert, and synthesis system prompts are read from
# files per request (server/prompt_loader.load_prompt) under server/prompts/, so a
# prompt edit changes behaviour with no rebuild. A missing file fails loud — the
# files are the single source of truth.

# Prefix that marks a real (non-abstain) kb_search observation.
_KB_BLOCK_HEADER = "Knowledge-base reference snippets"


def _tool_definitions(
    has_expert: bool = True, allow_kb_search: bool = True
) -> List[Dict[str, Any]]:
    """Tool definitions for sources not already supplied by the context ledger."""
    tools: List[Dict[str, Any]] = [
        {
            "type": "function",
            "function": {
                "name": "kb_search",
                "description": (
                    "Search the clinical knowledge base of openly-licensed reference "
                    "guidance (WHO IMCI danger signs, essential medicines, standard "
                    "dosing and thresholds, antiretroviral guidance) for facts that are "
                    "NOT in the patient's chart. Call this FIRST for any claim about a "
                    "guideline, a drug or dose, a threshold, a danger sign, an "
                    "immunization schedule, a normal/reference range, or whether a "
                    "treatment is current or recommended. Example: the question asks "
                    "whether a patient's regimen is still recommended -> "
                    'kb_search({"query": "WHO first-line ART; stavudine d4T '
                    'phase-out"}). Returns reference snippets with provenance — never '
                    "patient data; cite the source inline as prose, never as an integer."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "The clinical topic, drug, or guideline term to look up.",
                        }
                    },
                    "required": ["query"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "medical_expert",
                "description": (
                    "Consult a clinical expert to interpret THIS patient's chart against "
                    "the question. Call this AFTER kb_search when guideline/dosing/"
                    "threshold facts matter: the expert AUTOMATICALLY receives the "
                    "snippets kb_search returned this turn, so you do NOT copy any facts "
                    "into your question — just ask what you want interpreted. Use for "
                    "clinical judgment and interpretation, not for plain chart lookup you "
                    "can answer yourself. Example: after retrieving the guidance -> "
                    'medical_expert({"query": "Given the chart\'s regimen, is it still '
                    'WHO-recommended, and what is the concern if not?"}).'
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "A focused clinical question for the expert about this chart.",
                        }
                    },
                    "required": ["query"],
                },
            },
        },
    ]
    if not has_expert:
        tools = [t for t in tools if t["function"]["name"] != "medical_expert"]
    if not allow_kb_search:
        tools = [t for t in tools if t["function"]["name"] != "kb_search"]
    return tools


def _chart_context(messages: List[Dict[str, Any]]) -> str:
    """The chart snapshot is chartsearchai's first user message (after system)."""
    for m in messages:
        if m.get("role") == "user":
            content = m.get("content")
            return content if isinstance(content, str) else json.dumps(content)
    return ""


def _prepare_drug_safety(
    chart_text: str,
    mappings: List[Dict[str, Any]],
    records: List[Dict[str, Any]],
    question: str,
    anchor: Optional[str],
    enabled: bool,
) -> Tuple[str, List[Dict[str, Any]], Optional["drug_safety.PatientClinicalContext"]]:
    """Build patient drug context and inject deterministic drug-reference records.
    patient's clinical context from the RAW querystore records (reference_date resolved the same
    way temporal grounding resolves "now" — a demo dataset has no real wall-clock activity), then
    injects matching drug-reference records into the chart. No-op (unchanged inputs, None context)
    when disabled or there is no chart to inject into."""
    if not enabled or not chart_text:
        return chart_text, mappings, None
    reference_date = temporal.resolve_anchor(
        anchor or os.environ.get("HUB_ANCHOR"), chart_text
    )
    dataset = drug_safety.load_dataset()
    patient_context = drug_safety.build_patient_context(
        records, reference_date, dataset
    )
    new_text, new_mappings = drug_safety.inject_drug_references(
        chart_text,
        mappings,
        question,
        patient_context.age_years,
        dataset,
        active_order_atc_codes=patient_context.active_drug_atc_codes,
    )
    return new_text, new_mappings, patient_context


def _compute_safety_warnings(
    patient_context: Optional["drug_safety.PatientClinicalContext"],
    answer_text: str,
    question: str,
    enabled: bool,
) -> List[Dict[str, str]]:
    """Post-answer drug-safety check (deterministic, no LLM). Returns [] when disabled, there is no
    patient context (no patient ref, or querystore retrieval failed), or nothing is flagged.
    """
    if not enabled or patient_context is None:
        return []
    warnings = drug_safety.validate_answer(
        answer_text, question, patient_context, drug_safety.load_dataset()
    )
    return [w.to_dict() for w in warnings]


_INLINE_CITATION_RE = re.compile(r"\[(\d+)\]")


def _citation_indices(citations: List[int], answer: Optional[str] = None) -> List[int]:
    """Union structured citations with inline ``[N]`` markers, preserving first-seen order."""
    out: List[int] = []
    seen: set[int] = set()
    for raw in citations or []:
        try:
            idx = int(raw)
        except (TypeError, ValueError):
            continue
        if idx not in seen:
            out.append(idx)
            seen.add(idx)
    if answer:
        for match in _INLINE_CITATION_RE.finditer(answer):
            idx = int(match.group(1))
            if idx not in seen:
                out.append(idx)
                seen.add(idx)
    return out


def _resolve_references(
    citations: List[int],
    mappings: List[Dict[str, Any]],
    *,
    answer: Optional[str] = None,
    blocks: Optional[List[Any]] = None,
    grounding_status: Optional[str] = None,
    answer_usage_location: str = "answer",
) -> List[Dict[str, Any]]:
    """Resolve 1-based ``[N]`` citation indices into rich reference objects using chart mappings.

    ``grounding_status`` is a UI lifecycle hint, not a verdict. Product staged profiles use
    ``checking`` for immediately resolved references; final grounding verdicts are attached by
    ``_ground_references`` only after optional answer review has produced the final answer.
    """
    by_index = {
        m.get("index"): m for m in (mappings or []) if isinstance(m.get("index"), int)
    }
    block_refs = _block_temporal_text_and_refs(blocks or [])[1]
    refs: List[Dict[str, Any]] = []
    for c in _citation_indices(list(citations or []) + block_refs, answer):
        m = by_index.get(c)
        if not m:
            refs.append(
                {
                    "index": c,
                    "resolutionStatus": "unresolved",
                    "groundingStatus": "unchecked",
                    "grounded": None,
                    "usage": _reference_usages(
                        answer or "", blocks or [], c, citations, answer_usage_location
                    ),
                }
            )
            continue
        ref = {
            "index": c,
            "sourceId": m.get("sourceId"),
            "source": m.get("source"),
            "resourceType": m.get("resourceType"),
            "resourceUuid": m.get("resourceUuid"),
            "date": m.get("date"),
            "title": m.get("title") or "",
            "sourceText": m.get("text") or "",
            "resolutionStatus": "resolved",
            "usage": _reference_usages(
                answer or "", blocks or [], c, citations, answer_usage_location
            ),
        }
        if grounding_status:
            ref["groundingStatus"] = grounding_status
            if grounding_status == "checking":
                ref["grounded"] = None
        refs.append(ref)
    return refs


def _reference_usages(
    answer: str,
    blocks: List[Any],
    index: int,
    structured_citations: List[int],
    answer_location: str = "answer",
) -> List[Dict[str, Any]]:
    usages: List[Dict[str, Any]] = [
        {"location": answer_location, "text": fragment}
        for fragment in _claim_fragments_for_index(answer, index)
    ]
    if index in (structured_citations or []) and not usages:
        usages.append({"location": answer_location, "text": answer})

    def walk(value: Any, path: str) -> None:
        if isinstance(value, dict):
            refs = value.get("refs")
            if isinstance(refs, list) and index in refs:
                text = value.get("text")
                usages.append(
                    {
                        "location": "block",
                        "path": path,
                        "text": str(text) if text is not None else "",
                    }
                )
            for key, child in value.items():
                if key != "refs":
                    walk(child, f"{path}.{key}" if path else str(key))
        elif isinstance(value, list):
            for position, child in enumerate(value):
                walk(child, f"{path}[{position}]")

    walk(blocks or [], "blocks")
    return usages


def _claim_fragments_for_index(answer: str, index: int) -> List[str]:
    """Return answer fragments whose text explicitly cites ``[index]``."""
    if not answer:
        return []
    marker = f"[{index}]"
    # Sentence-ish split first; if the model writes dense clauses, the whole cited sentence is still
    # a conservative claim scope for the lightweight hub grounding pass.
    pieces = re.split(r"(?<=[.!?])\s+|\n+", answer)
    fragments = [p for p in pieces if marker in p]
    if not fragments and marker in answer:
        fragments = [answer]
    return fragments


_ENTAILMENT_MAX_PAIRS = 16  # beyond this, references keep an "unchecked" verdict rather than an
                            # unbounded batch call — mirrors the Java verifier's cap.
_ENTAILMENT_MAX_SOURCES_PER_CLAIM = 8
_ENTAILMENT_SOURCE_CHARS = 3000
_ENTAILMENT_MAX_SOURCE_SET_CHARS = 12000

_ENTAILMENT_SYSTEM_PROMPT = (
    "You are a strict clinical fact-checker. For each PAIR, decide whether the SOURCE record "
    "supports the STATEMENT. Answer NO if: the statement is about a different person or describes "
    "family/social history rather than the patient's own record; the statement is negated, denied, "
    "ruled out, or described as only suspected/possible when the source does not confirm it; the "
    "source does not state the specific fact claimed; or you are not fully certain from the source "
    "text alone. Never use outside medical knowledge — judge only what the SOURCE text says. Return "
    'exactly one verdict per pair, in the same order, as JSON: {"verdicts": ["YES"|"NO", ...]}.'
)

_ENTAILMENT_RF = {
    "type": "json_schema",
    "json_schema": {
        "name": "entailment_verdicts",
        "schema": {
            "type": "object",
            "properties": {
                "verdicts": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["YES", "NO"]},
                },
            },
            "required": ["verdicts"],
        },
    },
}


async def _entailment_verdicts(
    client: httpx.AsyncClient,
    model: str,
    pairs: List[Tuple[str, str]],
) -> List[Optional[bool]]:
    """Batched LLM entailment check for citation grounding — mirrors the validator call shape
    (fresh one-shot message list, dedicated json_schema response_format, one call for the whole
    batch since the hub's single-slot router makes per-pair fan-out no faster). ``pairs`` are
    ``(source_text, statement)``. FAIL-OPEN to ``None`` (unchecked) for every pair on any call or
    parse failure, or if the model returns fewer verdicts than pairs — a flaky grounding check must
    never fabricate a verdict."""
    if not pairs:
        return []
    body = "\n\n".join(
        f"PAIR {i + 1}:\nSOURCE: {source or '(none)'}\nSTATEMENT: {statement or '(none)'}"
        for i, (source, statement) in enumerate(pairs)
    )
    user = _ENTAILMENT_SYSTEM_PROMPT + "\n\n" + body
    try:
        msg = await _chat(
            client,
            model,
            [{"role": "user", "content": user}],
            response_format=_ENTAILMENT_RF,
        )
        obj = json.loads(_message_text(msg))
        verdicts = obj.get("verdicts") if isinstance(obj, dict) else None
        if not isinstance(verdicts, list):
            raise ValueError("entailment response missing a 'verdicts' array")
    except Exception:
        logger.warning(
            "entailment grounding[%s] call failed/unparseable -> all %d pair(s) unchecked",
            model,
            len(pairs),
        )
        return [None] * len(pairs)
    out: List[Optional[bool]] = []
    for i in range(len(pairs)):
        v = verdicts[i] if i < len(verdicts) else None
        if isinstance(v, str) and v.strip().upper() == "YES":
            out.append(True)
        elif isinstance(v, str) and v.strip().upper() == "NO":
            out.append(False)
        else:
            out.append(None)
    return out


async def _ground_references(
    client: httpx.AsyncClient,
    model: str,
    answer: str,
    references: List[Dict[str, Any]],
    mappings: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Ground final claims against their cited source sets in one bounded entailment call.

    A sentence may rely on multiple citations collectively (for example, two dated weights support
    a change claim). Grouping identical usage text prevents each source from being incorrectly asked
    to support the entire multi-source sentence by itself.
    """
    text_by_index = {
        m.get("index"): " ".join(str(p) for p in (m.get("date"), m.get("text")) if p)
        for m in (mappings or [])
        if isinstance(m.get("index"), int)
    }

    groups: List[Dict[str, Any]] = []
    group_by_key: Dict[Tuple[str, str, str], int] = {}
    groups_by_reference: List[List[int]] = [[] for _ in (references or [])]
    for ref_position, ref in enumerate(references or []):
        idx = ref.get("index")
        idx = idx if isinstance(idx, int) else -1
        source = str(text_by_index.get(idx) or "").strip()
        usages = [
            usage
            for usage in (ref.get("usage") or [])
            if isinstance(usage, dict)
        ]
        if not usages:
            usages = [
                {"location": "answer", "text": fragment}
                for fragment in _claim_fragments_for_index(answer or "", idx)
            ]
        for usage in usages:
            claim = _INLINE_CITATION_RE.sub("", str(usage.get("text") or "")).strip()
            if not claim or not source:
                continue
            key = (
                str(usage.get("location") or "answer"),
                str(usage.get("path") or ""),
                claim,
            )
            group_position = group_by_key.get(key)
            if group_position is None:
                group_position = len(groups)
                group_by_key[key] = group_position
                groups.append(
                    {
                        "claim": claim,
                        "location": key[0],
                        "path": key[1],
                        "sources": [],
                        "references": [],
                    }
                )
            group = groups[group_position]
            if (
                ref_position not in group["references"]
                and len(group["references"]) < _ENTAILMENT_MAX_SOURCES_PER_CLAIM
            ):
                remaining = _ENTAILMENT_MAX_SOURCE_SET_CHARS - sum(
                    len(item) for item in group["sources"]
                )
                if remaining <= 0:
                    continue
                source_item = f"[{idx}] {source}"[: min(remaining, _ENTAILMENT_SOURCE_CHARS)]
                group["references"].append(ref_position)
                group["sources"].append(source_item)
            if (
                ref_position in group["references"]
                and group_position not in groups_by_reference[ref_position]
            ):
                groups_by_reference[ref_position].append(group_position)

    checkable_positions = list(range(min(len(groups), _ENTAILMENT_MAX_PAIRS)))
    pairs = [
        ("\n".join(groups[i]["sources"]), groups[i]["claim"])
        for i in checkable_positions
    ]
    verdicts = await _entailment_verdicts(client, model, pairs) if pairs else []
    verdict_by_position = dict(zip(checkable_positions, verdicts))

    grounded_refs: List[Dict[str, Any]] = []
    for i, ref in enumerate(references or []):
        out = dict(ref)
        if out.get("resolutionStatus") == "unresolved":
            grounded_refs.append(out)
            continue
        grounding_checks = []
        for group_position in groups_by_reference[i]:
            group = groups[group_position]
            group_verdict = verdict_by_position.get(group_position)
            grounding_checks.append(
                {
                    "status": (
                        "verified"
                        if group_verdict is True
                        else "unsupported"
                        if group_verdict is False
                        else "unchecked"
                    ),
                    "claim": group["claim"],
                    "location": group["location"],
                    "path": group["path"],
                    "source_indices": sorted(
                        {
                            references[position].get("index")
                            for position in group["references"]
                            if isinstance(references[position].get("index"), int)
                        }
                    ),
                }
            )
        check_statuses = {check["status"] for check in grounding_checks}
        if not grounding_checks or "unchecked" in check_statuses:
            verdict = None
            aggregate_status = "unchecked"
        elif check_statuses == {"unsupported"}:
            verdict = False
            aggregate_status = "unsupported"
        elif check_statuses == {"verified"}:
            verdict = True
            aggregate_status = "verified"
        else:
            verdict = None
            aggregate_status = "mixed"
        out["grounded"] = verdict
        out["groundingStatus"] = aggregate_status
        out["groundingChecks"] = grounding_checks
        source_set = sorted(
            {
                references[position].get("index")
                for group_position in groups_by_reference[i]
                for position in groups[group_position]["references"]
                if isinstance(references[position].get("index"), int)
            }
        )
        if len(source_set) > 1:
            out["groundingScope"] = "source_set"
            out["groundingGroup"] = source_set
        elif source_set:
            out["groundingScope"] = "record"
        grounded_refs.append(out)
    return grounded_refs


def _latest_user_text(messages: List[Dict[str, Any]]) -> str:
    """The current turn's question is chartsearchai's LAST user message."""
    for m in reversed(messages):
        if m.get("role") == "user":
            content = m.get("content")
            return content if isinstance(content, str) else json.dumps(content)
    return ""


def _latest_assistant_text(messages: List[Dict[str, Any]]) -> str:
    """The prior answer to elaborate (two-call in-depth-only mode) is the LAST assistant message."""
    for m in reversed(messages):
        if m.get("role") == "assistant":
            content = m.get("content")
            return content if isinstance(content, str) else json.dumps(content)
    return ""


# Serialize ALL calls to the LLM backend hub-wide. The llama.cpp router (build 9430) has an
# unfixed TOCTOU race in models-max eviction (ggml-org/llama.cpp#20137, closed "not planned"):
# concurrent requests bypass the load gate, so a request can land on a model the router is
# evicting -> it force-kills that child after DEFAULT_STOP_TIMEOUT (hardcoded 10s) -> the call
# fails with "Failed to read connection" (also #18063 on stream:false). The issue's own
# recommendation is to QUEUE requests instead of letting them bypass; since the router won't,
# we serialize client-side: exactly one model request in flight at a time, so the router only
# ever loads/evicts ONE model with no concurrent request to race -> clean sequential loading
# (the single-GPU host serves one model at a time regardless, so this costs no real throughput).
_ROUTER_LOCK = asyncio.Lock()


@dataclass
class ChatBudgetPolicy:
    counter: ChatTokenCounter
    context_window: int
    reserved_output_tokens: int
    measurements: List[Dict[str, int | str]] = field(default_factory=list)


_CHAT_BUDGET: ContextVar[Optional[ChatBudgetPolicy]] = ContextVar(
    "med_agent_hub_chat_budget", default=None
)


def activate_chat_budget(policy: ChatBudgetPolicy) -> Token:
    return _CHAT_BUDGET.set(policy)


def reset_chat_budget(token: Token) -> None:
    _CHAT_BUDGET.reset(token)


async def _chat(
    client: httpx.AsyncClient,
    model: str,
    messages: List[Dict[str, Any]],
    *,
    tools: Optional[List[Dict[str, Any]]] = None,
    response_format: Optional[Dict[str, Any]] = None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    repeat_penalty: Optional[float] = None,
    dry_multiplier: Optional[float] = None,
) -> Dict[str, Any]:
    """One OpenAI-compatible backend call. Returns the first choice's message."""
    payload: Dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature
        if temperature is not None
        else llm_config.temperature,
    }
    if max_tokens is not None:
        payload["max_tokens"] = max_tokens
    if tools is not None:
        payload["tools"] = tools
        payload["tool_choice"] = "auto"
    if response_format is not None:
        payload["response_format"] = response_format
    if repeat_penalty is not None:
        payload["repeat_penalty"] = repeat_penalty
    if dry_multiplier is not None:
        payload["dry_multiplier"] = dry_multiplier

    budget = _CHAT_BUDGET.get()
    if budget is not None:
        requested_output = (
            max_tokens
            if isinstance(max_tokens, int) and max_tokens > 0
            else budget.reserved_output_tokens
        )
        output_tokens = min(requested_output, budget.reserved_output_tokens)
        payload["max_tokens"] = output_tokens
        count_chat = getattr(budget.counter, "count_chat", None)
        if not callable(count_chat):
            raise ContextSourceError(
                "tokenization_unavailable",
                "The product profile requires an exact chat-template token counter.",
                source="llama-router",
            )
        input_tokens = await count_chat(model, payload)
        budget.measurements.append(
            {
                "model": model,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "context_window": budget.context_window,
            }
        )
        if input_tokens + output_tokens > budget.context_window:
            raise InsufficientContextError(
                f"The exact {model!r} request requires {input_tokens} input plus "
                f"{output_tokens} output tokens, exceeding its "
                f"{budget.context_window}-token context window.",
                mandatory_ids=(),
            )

    headers = {"Content-Type": "application/json"}
    if llm_config.api_key:
        headers["Authorization"] = f"Bearer {llm_config.api_key}"

    url = f"{llm_config.base_url.rstrip('/')}/v1/chat/completions"
    logger.info(
        "team _chat: model=%s tools=%s response_format=%s",
        model,
        bool(tools),
        bool(response_format),
    )
    # Hold the lock for the WHOLE request (load + generate) so the router never sees a second
    # request while it is loading/evicting a model. Timeout covers a cold big-model load + a long
    # thinking generation. The lock makes loads strictly sequential — no eviction-vs-serve race.
    async with _ROUTER_LOCK:
        resp = await client.post(url, json=payload, headers=headers, timeout=600.0)
    if resp.status_code >= 400:
        # Surface the backend's reason (context overflow, bad schema, model-load failure) — bare
        # status codes are not actionable.
        logger.error(
            "router %s for model=%s tools=%s response_format=%s: %s",
            resp.status_code,
            model,
            bool(tools),
            bool(response_format),
            resp.text[:800],
        )
        resp.raise_for_status()
    return resp.json()["choices"][0]["message"]


def _message_text(msg: Dict[str, Any]) -> str:
    """Read assistant text from standard or reasoning-content backend responses."""
    return (msg.get("content") or "").strip() or (
        msg.get("reasoning_content") or ""
    ).strip()


async def _run_medical_expert(
    client: httpx.AsyncClient,
    query: str,
    chart_context: str,
    expert_system: str,
    kb_context: str = "",
    model: Optional[str] = None,
    temperature: float = 0.1,
    repeat_penalty: Optional[float] = None,
    dry_multiplier: float = EXPERT_DRY_MULTIPLIER,
) -> str:
    """Typed clinical-expert tool: a single MedGemma call, free text (no schema).
    The KB block (when any was retrieved) is placed FIRST in the user message so the
    decisive reference guidance is not lost in the middle of a long chart."""
    if kb_context:
        user = (
            "Reference guidance (NOT chart data; for dosing/threshold/guideline facts "
            "use only these or say they were not found):\n"
            f"{kb_context}\n\n"
            f"Patient chart:\n{chart_context}\n\n"
            f"Question: {query}"
        )
    else:
        user = f"Patient chart:\n{chart_context}\n\nQuestion: {query}"
    messages = [
        {"role": "system", "content": expert_system},
        {"role": "user", "content": user},
    ]
    try:
        msg = await _chat(
            client,
            model or llm_config.med_model,
            messages,
            temperature=temperature,
            max_tokens=800,
            repeat_penalty=repeat_penalty,
            dry_multiplier=dry_multiplier,
        )
        return _message_text(msg) or "(no expert response)"
    except Exception as e:  # tool failure must not abort the turn
        logger.warning("medical_expert tool failed: %s", e)
        return "(medical expert unavailable for this turn)"


def _run_kb_search(query: str) -> str:
    """Typed knowledge-base tool: BM25 over the openly-licensed clinical seed.
    Formats hits as labelled reference snippets; abstains (empty) on no match."""
    try:
        hits = kb.search(query)
    except Exception as e:  # tool failure must not abort the turn
        logger.warning("kb_search tool failed: %s", e)
        return "(knowledge base unavailable for this turn)"
    if not hits:
        return "(no relevant knowledge-base entries — do not invent guidance)"
    lines = [
        f"{_KB_BLOCK_HEADER} (NOT chart data; cite the source inline as prose, never "
        "as an integer citation):"
    ]
    for h in hits:
        src = ", ".join(p for p in (h.get("source"), h.get("version")) if p)
        lines.append(f"- {h['text']} [{src}]")
    return "\n".join(lines)


def _gathered_evidence(kb_context: str, expert_notes: List[str]) -> str:
    """Collapse the accumulated KB snippets (first) and clinical-expert notes into a
    single 'Gathered evidence' block for the synthesis turn. Empty when no tool
    produced usable output."""
    parts: List[str] = []
    if kb_context:
        parts.append(kb_context)
    notes = [
        n for n in expert_notes if n and not n.startswith("(medical expert unavailable")
    ]
    if notes:
        parts.append("Clinical expert notes:\n" + "\n\n".join(notes))
    if not parts:
        return ""
    return "Gathered evidence:\n\n" + "\n\n".join(parts)


# The single fallback-answer string. Defined ONCE so the hub and harness/validate/runner.py::_row_is_good
# reference the same sentinel — the hub must never SHIP what the runner would re-run on --resume.
FALLBACK_ANSWER = (
    "I could not produce a complete answer for this turn. Please try again."
)


def _fallback_envelope(answer: str = FALLBACK_ANSWER) -> str:
    """A minimal, always-schema-valid chart_answer envelope."""
    return json.dumps({"answer": answer, "citations": [], "blocks": []})


def _is_substantive_answer(text: Optional[str]) -> bool:
    """Deterministic 'is this a real answer?' check — mirrors harness runner._row_is_good's text predicate
    (non-empty after strip · has an alphanumeric char · not the fallback message). The LLM validator fails
    open on a "." answer (no concrete error to name), so this cheap check, not the validator, is what
    guarantees an empty/punctuation-only/fallback answer never ships green."""
    ans = (text or "").strip()
    if not ans:
        return False
    if not any(ch.isalnum() for ch in ans):
        return False
    return "could not produce a complete answer" not in ans


def _normalize_envelope(raw: str) -> str:
    """Post-process the synthesizer envelope JSON: (1) repair the section line breaks small
    models mangle — a literal backslash-n OR runs of backslashes ("**Answer**\\\\\\:") — into
    real newlines, and (2) reconcile inline [N] chart-record markers into
    `citations` so the count is not lost when the model cites in prose but leaves the array
    empty. Returns `raw` unchanged if it is not parseable JSON."""
    try:
        env = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return raw
    if not isinstance(env, dict):
        return raw
    ans = env.get("answer")
    if isinstance(ans, str):
        # Small synths mis-escape the section line breaks as RUNS of backslashes
        # ("**Answer**\\\\\\: text" / "**Answer**\\\\<newline>This"); collapse a run (+ an
        # optional trailing colon) to one newline, then the single literal \n, then tidy.
        ans = re.sub(r"\\{2,}\s*:?\s*", "\n", ans)
        ans = ans.replace("\\n", "\n")
        ans = re.sub(r"\n{3,}", "\n\n", ans).strip()
        env["answer"] = ans
        inline = sorted({int(m) for m in re.findall(r"\[(\d+)\]", ans)})
        if inline:
            existing = [c for c in (env.get("citations") or []) if isinstance(c, int)]
            env["citations"] = sorted(set(existing) | set(inline))
    return json.dumps(env)


# Synthesis anti-degeneration: a small synthesizer can fall into token-level
# repetition loops ("AIDS AIDS AIDS...") on a long evidence prompt. A modest
# temperature floor + repeat_penalty on the synthesis call breaks the loop; the
# orchestrator's tool loop keeps the request temperature so tool-calling stays
# deterministic. The configured llama.cpp backend honors repeat_penalty and DRY.
_SYNTH_MIN_TEMPERATURE = 0.5

# The Answer and the In Depth are DISTINCT from generation onward: two synthesis calls,
# two validators. The In-Depth synthesis returns a list of claim strings; the In-Depth
# validator returns the 1-based claim numbers to drop; the Answer validator returns a
# strict pass/fail verdict with the reason. The Answer and In-Depth bodies are combined
# into one markdown body only at the chartsearchai handoff.
_INDEPTH_RF = {
    "type": "json_schema",
    "json_schema": {
        "name": "in_depth",
        "schema": {
            "type": "object",
            "properties": {
                "claims": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["claims"],
        },
    },
}


# Rewrite-validator verdict (the "suggest the fix" mode): the validator LOCALIZES each chart
# contradiction (wrong phrase + grounding record + replacement) AND returns the surgically-corrected
# answer, so the refine loop ADOPTS the validator's fix instead of asking the writer to regenerate. The
# research basis: actionable, localized feedback beats a binary verdict, and a surgical edit avoids the
# over-correction that a from-scratch regenerate inflicts on the already-correct parts of a strong answer.
_REWRITE_VERDICT_RF = {
    "type": "json_schema",
    "json_schema": {
        "name": "rewrite_verdict",
        "schema": {
            "type": "object",
            "properties": {
                "answer_ok": {"type": "boolean"},
                "errors": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "wrong": {"type": "string"},
                            "chart": {"type": "string"},
                            "fix": {"type": "string"},
                        },
                    },
                },
                "corrected_answer": {"type": "string"},
            },
            "required": ["answer_ok"],
        },
    },
}

# A validator_prompt with this stem selects the rewrite path (loads <stem>-answer.txt + the rewrite
# schema + the adopt-the-fix loop); the default "validation" keeps the regenerate path unchanged.
_REWRITE_VALIDATOR_PROMPT = "validation-rewrite"


_INDEPTH_VERDICT_RF = {
    "type": "json_schema",
    "json_schema": {
        "name": "indepth_verdict",
        "schema": {
            "type": "object",
            "properties": {
                "drop": {"type": "array", "items": {"type": "integer"}},
                "issues": {"type": "string"},
            },
            "required": ["drop"],
        },
    },
}


def _knob(knobs: Optional[Dict[str, Any]], role: str, key: str, default: Any) -> Any:
    """Resolve one per-role sampling knob from a profile's `knobs` block, falling back to
    the global default when the role or key is unset. knobs = {role: {key: value}}."""
    role_knobs = (knobs or {}).get(role)
    if isinstance(role_knobs, Mapping) and role_knobs.get(key) is not None:
        return role_knobs[key]
    return default


# Confidence level -> tag label (high=green, medium=yellow, low=red). The hub emits the structured
# {level, note}; clients (dashboard/report, and chat once its schema is updated) render the tag.
_CONF_LABEL = {
    "green": "High confidence",
    "yellow": "Medium confidence",
    "red": "Low confidence",
}


def _answer_body(answer_text: str, claims: List[str]) -> str:
    """Combine the direct Answer and the In-Depth claims into one CLEAN markdown body (no confidence
    text baked in — confidence is structured metadata a client renders as a tag). The Answer leads
    under a **Answer** header; non-empty claims follow as a **In Depth** bullet list."""
    body = "**Answer**\n" + (answer_text or "").strip()
    if claims:
        body += "\n\n**In Depth**\n" + "\n".join("- " + c for c in claims)
    return body


def _assemble_envelope(
    answer_text: str,
    citations: List[int],
    blocks: List[Any],
    claims: List[str],
    answer_conf: Optional[Dict[str, Any]] = None,
    indepth_conf: Optional[Dict[str, Any]] = None,
    safety_warnings: Optional[List[Dict[str, str]]] = None,
) -> str:
    """Serialize the chartsearchai {answer, citations, blocks} envelope, where `answer` is the CLEAN
    combined Answer + In-Depth markdown body. Carries a `confidence` block (per-section {level, note})
    as structured metadata a client renders as a TAG — chartsearchai drops it today; the harness
    reads confidence from the reasoning trace, the dashboard/report render the tag."""
    env: Dict[str, Any] = {
        "answer": _answer_body(answer_text, claims),
        "citations": citations or [],
        "blocks": blocks or [],
    }
    if answer_conf or indepth_conf:
        env["confidence"] = {
            "answer": answer_conf or {"level": "green", "note": ""},
            "in_depth": indepth_conf or {"level": "green", "note": ""},
        }
    if safety_warnings:
        env["safetyWarnings"] = safety_warnings
    return json.dumps(env)


_ANSWER_VALIDATION_LABELS = {
    "validating": "Checking answer",
    "checked": "Checked",
    "edited": "Updated after check",
    "needs_review": "Needs review",
    "unavailable": "Check unavailable",
}


def _answer_validation_wire(
    status: str,
    *,
    summary: str = "",
    issues: Optional[List[Any]] = None,
    original_answer: Optional[str] = None,
) -> Dict[str, Any]:
    wire: Dict[str, Any] = {
        "status": status,
        "label": _ANSWER_VALIDATION_LABELS.get(
            status, status.replace("_", " ").title()
        ),
        "summary": summary or "",
        "issues": issues or [],
        "completedAt": datetime.now(timezone.utc).isoformat(),
    }
    if original_answer is not None:
        wire["originalAnswer"] = original_answer
    return wire


def _assemble_answer_envelope(
    answer_text: str,
    citations: List[int],
    blocks: List[Any],
    answer_conf: Optional[Dict[str, Any]] = None,
    answer_validation: Optional[Dict[str, Any]] = None,
    safety_warnings: Optional[List[Dict[str, str]]] = None,
) -> str:
    """Serialize the staged UX Answer leg without an In-Depth placeholder in the markdown body.

    The caller attaches In-Depth as structured/pending UI state, so the answer body remains the
    direct clinical answer rather than pretending the background section already exists.
    """
    env: Dict[str, Any] = {
        "answer": answer_text or "",
        "citations": citations or [],
        "blocks": blocks or [],
    }
    if answer_conf:
        env["confidence"] = {"answer": answer_conf}
    if answer_validation:
        env["answerValidation"] = answer_validation
    if safety_warnings:
        env["safetyWarnings"] = safety_warnings
    return json.dumps(env)


def _answer_fields(normalized_json_str: str) -> Tuple[str, List[int], List[Any]]:
    """Pull (answer_text, citations, blocks) out of a normalized envelope JSON string. Tolerant:
    returns ("", [], []) on any junk / non-object / missing fields."""
    try:
        env = json.loads(normalized_json_str)
    except (json.JSONDecodeError, TypeError):
        return "", [], []
    if not isinstance(env, dict):
        return "", [], []
    ans = env.get("answer")
    answer_text = ans.strip() if isinstance(ans, str) else ""
    citations = [c for c in (env.get("citations") or []) if isinstance(c, int)]
    blocks = env.get("blocks") if isinstance(env.get("blocks"), list) else []
    return answer_text, citations, blocks


def _review_payload_from_messages(messages: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Extract the answer_to_review.v1 JSON payload from the latest user message.

    ChartSearchAI sends this as prose plus a fenced JSON object. Be deliberately tolerant so a
    caller can also send the raw JSON object/string in tests or future integrations.
    """
    raw = _latest_user_text(messages)
    if not raw:
        return {}
    candidates: List[str] = []
    stripped = raw.strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        candidates.append(stripped)
    fence = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", raw, re.S)
    if fence:
        candidates.append(fence.group(1))
    first, last = raw.find("{"), raw.rfind("}")
    if first >= 0 and last > first:
        candidates.append(raw[first:last + 1])
    for candidate in candidates:
        try:
            obj = json.loads(candidate)
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(obj, dict):
            payload = (
                obj.get("answer_to_review")
                if isinstance(obj.get("answer_to_review"), dict)
                else obj
            )
            if (
                payload.get("schema_version") == "answer_to_review.v1"
                or "answer" in payload
            ):
                return payload
    return {}


def _block_temporal_text_and_refs(blocks: List[Any]) -> Tuple[str, List[int]]:
    """Flatten table/block cell text so deterministic gates see model-generated dates in
    structured output, not only dates in the prose answer."""
    texts: List[str] = []
    refs: List[int] = []

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if key == "refs" and isinstance(child, list):
                    refs.extend(i for i in child if isinstance(i, int))
                else:
                    walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)
        elif isinstance(value, str):
            texts.append(value)

    walk(blocks or [])
    return "\n".join(texts), sorted(set(refs))


def _gate_failure_note(gate: Optional[Dict[str, Any]]) -> str:
    checks = (gate or {}).get("checks") or []
    for c in checks:
        if c.get("status") == "fail":
            return c.get("reason") or "a deterministic temporal check failed."
    for c in checks:
        if c.get("status") == "warn":
            return c.get("reason") or "a deterministic temporal check warned."
    return ""


def _merge_temporal_gate_conf(
    conf: Optional[Dict[str, Any]], gate: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """Fold deterministic temporal-gate status into the structured Answer confidence."""
    base = dict(conf or {"level": "green", "note": ""})
    if (
        not gate
        or gate.get("mode") == "off"
        or gate.get("status") not in {"warn", "fail"}
    ):
        return base
    applied = gate.get("applied")
    target_level = "yellow"
    if gate.get("status") == "fail" and applied not in {"patch"}:
        target_level = "red"
    order = {"green": 0, "yellow": 1, "red": 2}
    if order.get(target_level, 0) > order.get(base.get("level", "green"), 0):
        base["level"] = target_level
    reason = _gate_failure_note(gate)
    if applied == "patch":
        note = "Deterministic temporal gate corrected the answer before validation"
    elif applied == "fallback":
        note = "Deterministic temporal gate blocked the draft answer"
    elif gate.get("mode") == "warn":
        note = "Deterministic temporal gate warning"
    else:
        note = "Deterministic temporal gate"
    if reason:
        note += ": " + reason
    note += "."
    existing = (base.get("note") or "").strip()
    base["note"] = (existing + " " + note).strip() if existing else note
    return base


def _apply_temporal_gate(
    *,
    question: str,
    answer_text: str,
    citations: List[int],
    blocks: List[Any],
    temporal_facts: Optional[Dict[str, Any]],
    temporal_gate_mode: str,
    steps: List[Dict[str, Any]],
) -> Tuple[str, List[int], List[Any], Dict[str, Any], Optional[str]]:
    """Run the deterministic temporal gate and optionally replace the answer in enforce mode."""
    block_text, block_refs = _block_temporal_text_and_refs(blocks)
    gate_answer_text = answer_text + (("\n" + block_text) if block_text else "")
    gate_citations = sorted(set(citations or []) | set(block_refs))
    gate = temporal.run_temporal_gate(
        question, gate_answer_text, gate_citations, temporal_facts, temporal_gate_mode
    )
    original_answer = None
    gate["applied"] = "none"
    if gate.get("mode") == "enforce" and gate.get("status") == "fail":
        original_answer = answer_text
        patch = (gate.get("patch_answer") or "").strip()
        patch_citations = [
            c for c in (gate.get("patch_citations") or []) if isinstance(c, int)
        ]
        if patch:
            answer_text, citations, blocks = patch, patch_citations, []
            gate["applied"] = "patch"
        else:
            answer_text, citations, blocks = (
                (
                "I cannot safely answer this temporal question because deterministic temporal "
                "validation found a contradiction in the draft answer. Please verify against the chart."
                ),
                [],
                [],
            )
            gate["applied"] = "fallback"
    steps.append(
        {
        "role": "temporal_gate",
        "mode": gate.get("mode"),
        "status": gate.get("status"),
        "applied": gate.get("applied"),
        "n_checks": len(gate.get("checks") or []),
        }
    )
    return answer_text, citations, blocks, gate, original_answer


def _regate_after_rewrite(
    *,
    question: str,
    answer_text: str,
    citations: List[int],
    blocks: List[Any],
    temporal_facts: Optional[Dict[str, Any]],
    temporal_gate_mode: str,
    steps: List[Dict[str, Any]],
    answer_conf: Dict[str, Any],
    prior_original_answer: Optional[str],
) -> Tuple[str, List[int], List[Any], Dict[str, Any], Dict[str, Any], Optional[str]]:
    """Re-run the deterministic temporal gate on text a validator or LLM reviewer just rewrote.

    The initial gate only sees the first draft; a validator/reviewer rewrite that runs AFTER it
    (rewrite-mode answer validation, or the async answer-review leg's corrected_answer) can
    reintroduce a date/temporal contradiction the first check never had a chance to catch. Call
    this on every answer-mutating step's output before it ships, so the same enforcement the draft
    got also applies to whatever text actually goes out.

    Returns (answer_text, citations, blocks, answer_conf, temporal_gate, original_answer_text).
    """
    answer_text, citations, blocks, gate, patched_from = _apply_temporal_gate(
        question=question,
        answer_text=answer_text,
        citations=citations,
        blocks=blocks,
        temporal_facts=temporal_facts,
        temporal_gate_mode=temporal_gate_mode,
        steps=steps,
    )
    answer_conf = _merge_temporal_gate_conf(answer_conf, gate)
    return (
        answer_text,
        citations,
        blocks,
        answer_conf,
        gate,
        (patched_from or prior_original_answer),
    )


async def _synthesize_answer(
    client: httpx.AsyncClient,
    synth_model: str,
    base_messages: List[Dict[str, Any]],
    answer_instruction: str,
    gathered: str,
    *,
    response_format: Optional[Dict[str, Any]],
    temperature: float,
    max_tokens: Optional[int],
    repeat_penalty: Optional[float],
    dry: Optional[float],
    extra_msgs: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[str, List[int], List[Any]]:
    """Answer synthesis bound to chartsearchai's response_format. Returns the (answer_text,
    citations, blocks) parsed from the envelope. FAIL-OPEN: returns ("", [], []) on any error.
    """
    user = answer_instruction + ("\n\n" + gathered if gathered else "")
    messages = (
        list(base_messages) + [{"role": "user", "content": user}] + (extra_msgs or [])
    )
    try:
        msg = await _chat(
            client,
            synth_model,
            messages,
            response_format=response_format,
            temperature=temperature,
            max_tokens=max_tokens,
            repeat_penalty=repeat_penalty,
            dry_multiplier=dry,
        )
        return _answer_fields(_normalize_envelope(_message_text(msg)))
    except ContextSourceError:
        raise
    except Exception as e:
        logger.warning("answer synthesis failed: %s", e)
        return "", [], []


async def _synthesize_indepth(
    client: httpx.AsyncClient,
    synth_model: str,
    base_messages: List[Dict[str, Any]],
    indepth_instruction: str,
    gathered: str,
    answer_text: str,
    *,
    temperature: float,
    max_tokens: Optional[int],
    repeat_penalty: Optional[float],
    dry: Optional[float],
    extra_msgs: Optional[List[Dict[str, Any]]] = None,
) -> List[str]:
    """In-Depth synthesis: elaborate the already-produced direct answer into a list of claim
    strings (one addressable claim each). `extra_msgs` carries the prior draft + validator
    feedback on a re-synthesis pass. FAIL-OPEN: returns [] on any error."""
    user = (
        indepth_instruction
        + "\n\n=== DIRECT ANSWER (elaborate THIS; do not restate it) ===\n"
        + answer_text
        + ("\n\n=== GATHERED KB / EVIDENCE ===\n" + gathered if gathered else "")
    )
    messages = (
        list(base_messages) + [{"role": "user", "content": user}] + (extra_msgs or [])
    )
    try:
        msg = await _chat(
            client,
            synth_model,
            messages,
            response_format=_INDEPTH_RF,
            temperature=temperature,
            max_tokens=max_tokens,
            repeat_penalty=repeat_penalty,
            dry_multiplier=dry,
        )
        obj = json.loads(_message_text(msg))
    except ContextSourceError:
        raise
    except (Exception,):  # parse OR call failure -> no elaboration
        logger.warning("in-depth synthesis failed -> no elaboration")
        return []
    if not isinstance(obj, dict):
        return []
    return [
        c.strip() for c in (obj.get("claims") or []) if isinstance(c, str) and c.strip()
    ]


async def _validate_answer_rewrite(
    client: httpx.AsyncClient,
    validator_model: str,
    *,
    chart: str,
    gathered: str,
    answer_text: str,
    max_tokens: Optional[int],
    temperature: float,
    repeat_penalty: Optional[float],
    dry: Optional[float],
    validation_prompt: str = _REWRITE_VALIDATOR_PROMPT,
) -> Dict[str, Any]:
    """Rewrite-mode audit: localize each chart contradiction AND return the corrected answer. Returns
    {answer_ok, errors:[{wrong,chart,fix}], corrected_answer}. FAIL-OPEN: {answer_ok: True, errors: []}
    on any parse failure so a flaky validator never blocks the run."""
    instruction = load_prompt(validation_prompt + "-answer")
    audit_user = (
        instruction
        + "\n\n=== PATIENT CHART (ground truth) ===\n"
        + (chart or "(none)")
        + "\n\n=== GATHERED KB / EVIDENCE (the guidance the team retrieved) ===\n"
        + (gathered or "(none)")
        + "\n\n=== DRAFT ANSWER ===\n"
        + answer_text
    )
    msg = await _chat(
        client,
        validator_model,
        [{"role": "user", "content": audit_user}],
        response_format=_REWRITE_VERDICT_RF,
        temperature=temperature,
        max_tokens=max_tokens,
        repeat_penalty=repeat_penalty,
        dry_multiplier=dry,
    )
    raw = _message_text(msg)
    try:
        verdict = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning(
            "rewrite-validator[%s] verdict UNPARSEABLE -> FAIL-OPEN (pass); raw=%r",
            validator_model,
            raw[:240],
        )
        return {"answer_ok": True, "errors": []}
    if not isinstance(verdict, dict):
        logger.warning(
            "rewrite-validator[%s] verdict not an object -> FAIL-OPEN; raw=%r",
            validator_model,
            raw[:240],
        )
        return {"answer_ok": True, "errors": []}
    errors = [e for e in (verdict.get("errors") or []) if isinstance(e, dict)]
    logger.info(
        "rewrite-validator[%s] answer_ok=%s n_errors=%d",
        validator_model,
        verdict.get("answer_ok"),
        len(errors),
    )
    return {
        "answer_ok": verdict.get("answer_ok", True),
        "errors": errors,
        "corrected_answer": (verdict.get("corrected_answer") or "").strip(),
    }


async def _validate_indepth_verdict(
    client: httpx.AsyncClient,
    validator_model: str,
    *,
    chart: str,
    gathered: str,
    answer_text: str,
    claims: List[str],
    max_tokens: Optional[int],
    temperature: float,
    repeat_penalty: Optional[float],
    dry: Optional[float],
    validation_prompt: str = "validation",
) -> Dict[str, Any]:
    """Audit the In-Depth claims claim-by-claim. Returns {drop: [1-based claim numbers, clamped to
    1..len(claims)], issues: str}. FAIL-OPEN: returns {drop: [], issues: ""} on any parse failure.
    """
    instruction = load_prompt(validation_prompt + "-indepth")
    numbered = "\n".join(f"{i}. {c}" for i, c in enumerate(claims, start=1))
    audit_user = (
        instruction
        + "\n\n=== PATIENT CHART (ground truth) ===\n"
        + (chart or "(none)")
        + "\n\n=== GATHERED KB / EVIDENCE (the guidance the team retrieved) ===\n"
        + (gathered or "(none)")
        + "\n\n=== DIRECT ANSWER (context) ===\n"
        + answer_text
        + "\n\n=== IN-DEPTH CLAIMS (numbered; return the numbers to DROP) ===\n"
        + numbered
    )
    msg = await _chat(
        client,
        validator_model,
        [{"role": "user", "content": audit_user}],
        response_format=_INDEPTH_VERDICT_RF,
        temperature=temperature,
        max_tokens=max_tokens,
        repeat_penalty=repeat_penalty,
        dry_multiplier=dry,
    )
    raw = _message_text(msg)
    try:
        verdict = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning(
            "indepth-validator[%s] verdict UNPARSEABLE -> FAIL-OPEN (keep all); raw=%r",
            validator_model,
            raw[:240],
        )
        return {"drop": [], "issues": ""}
    if not isinstance(verdict, dict):
        logger.warning(
            "indepth-validator[%s] verdict not an object -> FAIL-OPEN; raw=%r",
            validator_model,
            raw[:240],
        )
        return {"drop": [], "issues": ""}
    drop = [
        d
        for d in (verdict.get("drop") or [])
        if isinstance(d, int) and 1 <= d <= len(claims)
    ]
    logger.info(
        "indepth-validator[%s] drop=%s/%d claims issues=%r",
        validator_model,
        drop,
        len(claims),
        (verdict.get("issues") or "")[:120],
    )
    return {"drop": drop, "issues": verdict.get("issues", "")}


def _answer_note(level: str, first_issue: str, last_issue: str) -> str:
    """Clinician-facing confidence note for the Answer, composed deterministically from the
    validator verdicts captured during the re-synth cycle (no extra LLM call)."""
    if level == "yellow":
        base = "An initial draft was flagged on clinical review and corrected on a second pass"
        return (
            base
            + ((" (first issue: " + first_issue + ")") if first_issue else "")
            + "."
        )
    if level == "red":
        return (
            "Clinical review still flagged this after a revision: "
            + (
                last_issue
                or first_issue
                or "the answer could not be confirmed against the chart."
            )
            + " Verify against the chart before acting."
        )
    return ""


async def _review_existing_answer(
    client: httpx.AsyncClient,
    *,
    messages: List[Dict[str, Any]],
    gathered: str,
    chart: str,
    temporal_facts: Optional[Dict[str, Any]],
    temporal_gate_mode: str,
    reviewer_model: str,
    reviewer_prompt: str,
    validator_temperature: float,
    validator_repeat_penalty: Optional[float],
    validator_dry: Optional[float],
    max_tokens: Optional[int],
    steps: List[Dict[str, Any]],
    payload_override: Optional[Dict[str, Any]] = None,
) -> Tuple[
    str, Dict[str, Any], str, Dict[str, Any], Optional[Dict[str, Any]], Optional[str]
]:
    """Review an already-visible Answer and return an updated chartsearchai envelope.

    This is the async staged validation leg. It is deliberately conservative: deterministic gates can
    patch high-confidence temporal/date failures; the LLM reviewer can rewrite prose when it offers a
    clean correction; otherwise the original answer remains visible with needs_review metadata.
    """
    payload = payload_override or _review_payload_from_messages(messages)
    original_answer = str(
        payload.get("answer") or _latest_assistant_text(messages) or ""
    )
    answer_text = original_answer
    citations = [
        c
        for c in (payload.get("citations") or _extract_citations(answer_text))
        if isinstance(c, int)
    ]
    blocks = payload.get("blocks") if isinstance(payload.get("blocks"), list) else []
    question = str(
        payload.get("original_question")
        or payload.get("question")
        or _latest_user_text(messages)
        or ""
    )
    issues: List[Any] = []
    status = "checked"
    summary = "Answer checked against chart and deterministic temporal/date rules."
    answer_conf = {"level": "green", "note": ""}
    temporal_gate_result: Optional[Dict[str, Any]] = None

    if not payload:
        validation = _answer_validation_wire(
            "unavailable",
            summary="Answer check could not run because the review payload was missing.",
            issues=[{"reason": "missing answer_to_review.v1 payload"}],
        )
        answer_conf = {
            "level": "yellow",
            "note": "Answer check unavailable: missing review payload.",
        }
        content = _assemble_answer_envelope(
            answer_text, citations, blocks, answer_conf, validation
        )
        return content, answer_conf, answer_text, validation, None, None

    if not _is_substantive_answer(answer_text):
        status = "needs_review"
        summary = "The answer check found a non-substantive answer and could not safely repair it."
        issues.append(
            {
                "id": "substance_gate",
                "reason": "The answer is empty, punctuation-only, or fallback text.",
            }
        )
        answer_conf = {
            "level": "red",
            "note": "Answer check found no usable answer. Verify against the chart.",
        }
    else:
        block_text, block_refs = _block_temporal_text_and_refs(blocks)
        gate_answer_text = answer_text + (("\n" + block_text) if block_text else "")
        gate_citations = sorted(set(citations or []) | set(block_refs))
        temporal_gate_result = temporal.run_temporal_gate(
            question,
            gate_answer_text,
            gate_citations,
            temporal_facts,
            temporal_gate_mode,
        )
        temporal_gate_result["applied"] = "none"
        if temporal_gate_result.get("status") == "fail":
            gate_issues = [
                c
                for c in (temporal_gate_result.get("checks") or [])
                if c.get("status") == "fail"
            ]
            issues.extend(gate_issues)
            patch = (temporal_gate_result.get("patch_answer") or "").strip()
            patch_citations = [
                c
                for c in (temporal_gate_result.get("patch_citations") or [])
                if isinstance(c, int)
            ]
            if temporal_gate_result.get("mode") == "enforce" and patch:
                answer_text, citations, blocks = patch, patch_citations, []
                temporal_gate_result["applied"] = "patch"
                status = "edited"
                summary = (
                    "The answer was updated after deterministic temporal/date checks."
                )
                answer_conf = {
                    "level": "yellow",
                    "note": "Deterministic answer check corrected a temporal/date issue.",
                }
            else:
                status = "needs_review"
                summary = (
                    "The answer check found a temporal/date issue that needs review."
                )
                answer_conf = {
                    "level": "red",
                    "note": "Deterministic answer check found a temporal/date issue. Verify against the chart.",
                }
        elif temporal_gate_result.get("status") == "warn":
            issues.extend(
                [
                    c
                    for c in (temporal_gate_result.get("checks") or [])
                    if c.get("status") == "warn"
                ]
            )

    if status in {"checked", "edited"} and _is_substantive_answer(answer_text):
        review_text = answer_text
        block_text, _block_refs = _block_temporal_text_and_refs(blocks)
        if block_text:
            review_text += "\n\n=== STRUCTURED BLOCK TEXT ===\n" + block_text
        try:
            verdict = await _validate_answer_rewrite(
                client,
                reviewer_model,
                chart=chart,
                gathered=gathered,
                answer_text=review_text,
                max_tokens=max_tokens,
                temperature=validator_temperature,
                repeat_penalty=validator_repeat_penalty,
                dry=validator_dry,
                validation_prompt=reviewer_prompt or _REWRITE_VALIDATOR_PROMPT,
            )
        except Exception as e:
            logger.warning("answer-review validator call failed: %s", e)
            validation = _answer_validation_wire(
                "unavailable",
                summary="Answer check unavailable; the review model call failed.",
                issues=[{"reason": str(e)}],
            )
            answer_conf = {
                "level": "yellow",
                "note": "Answer check unavailable because the review model failed.",
            }
            content = _assemble_answer_envelope(
                answer_text, citations, blocks, answer_conf, validation
            )
            steps.append(
                {
                    "role": "answer_review",
                    "model": reviewer_model,
                    "status": "unavailable",
                }
            )
            return (
                content,
                answer_conf,
                answer_text,
                validation,
                temporal_gate_result,
                original_answer,
            )

        errs = (verdict or {}).get("errors") or []
        steps.append(
            {
                "role": "answer_review",
                "mode": "rewrite",
                "model": reviewer_model,
                "answer_ok": (verdict or {}).get("answer_ok", True),
                "n_errors": len(errs),
                "errors": errs,
            }
        )
        if verdict and not verdict.get("answer_ok", True) and errs:
            issues.extend(errs)
            corrected = (verdict.get("corrected_answer") or "").strip()
            if blocks:
                status = "needs_review"
                summary = "The answer check found an issue in prose or table content that needs review."
                answer_conf = {
                    "level": "red",
                    "note": _answer_note("red", _rw_issue(verdict), ""),
                }
            elif (
                corrected
                and corrected != answer_text
                and _is_substantive_answer(corrected)
            ):
                recheck = await _validate_answer_rewrite(
                    client,
                    reviewer_model,
                    chart=chart,
                    gathered=gathered,
                    answer_text=corrected,
                    max_tokens=max_tokens,
                    temperature=validator_temperature,
                    repeat_penalty=validator_repeat_penalty,
                    dry=validator_dry,
                    validation_prompt=reviewer_prompt or _REWRITE_VALIDATOR_PROMPT,
                )
                steps.append(
                    {
                        "role": "answer_review",
                        "mode": "rewrite",
                        "model": reviewer_model,
                        "attempt": 1,
                        "answer_ok": recheck.get("answer_ok", True),
                              "n_errors": len(recheck.get("errors") or []),
                        "errors": recheck.get("errors") or [],
                    }
                )
                if recheck.get("answer_ok", True) or not recheck.get("errors"):
                    answer_text = corrected
                    citations = _extract_citations(corrected) or citations
                    status = "edited"
                    summary = "The answer was updated after chart check."
                    answer_conf = {
                        "level": "yellow",
                        "note": _answer_note("yellow", _rw_issue(verdict), ""),
                    }
                    # The reviewer's corrected_answer must pass the same deterministic temporal/date
                    # enforcement the original draft got, or a reviewer-introduced date error would
                    # ship unchecked. Re-run the gate on the rewrite; discard its own before/after
                    # tracking (`_`) since `original_answer` here means the pre-review answer, not
                    # whatever the gate patched from.
                    (
                        answer_text,
                        citations,
                        blocks,
                        answer_conf,
                        temporal_gate_result,
                        _,
                    ) = _regate_after_rewrite(
                        question=question,
                        answer_text=answer_text,
                        citations=citations,
                        blocks=blocks,
                        temporal_facts=temporal_facts,
                        temporal_gate_mode=temporal_gate_mode,
                        steps=steps,
                        answer_conf=answer_conf,
                        prior_original_answer=None,
                    )
                    if (
                        temporal_gate_result.get("status") == "fail"
                        and temporal_gate_result.get("applied") != "patch"
                    ):
                        status = "needs_review"
                        summary = (
                            "The answer check found a temporal/date issue introduced during "
                            "review that needs review."
                        )
                elif len(recheck.get("errors") or []) < len(errs):
                    answer_text = corrected
                    citations = _extract_citations(corrected) or citations
                    status = "needs_review"
                    summary = "The answer was improved after chart check but still needs review."
                    answer_conf = {
                        "level": "red",
                        "note": _answer_note(
                            "red", _rw_issue(verdict), _rw_issue(recheck)
                        ),
                    }
                else:
                    status = "needs_review"
                    summary = "The answer check found an issue that could not be safely repaired."
                    answer_conf = {
                        "level": "red",
                        "note": _answer_note(
                            "red", _rw_issue(verdict), _rw_issue(recheck)
                        ),
                    }
            else:
                status = "needs_review"
                summary = (
                    "The answer check found an issue that could not be safely repaired."
                )
                answer_conf = {
                    "level": "red",
                    "note": _answer_note("red", _rw_issue(verdict), ""),
                }

    original_for_wire = (
        original_answer
        if status == "edited" and original_answer != answer_text
        else None
    )
    validation = _answer_validation_wire(
        status,
        summary=summary,
        issues=issues,
        original_answer=original_for_wire,
    )
    content = _assemble_answer_envelope(
        answer_text, citations, blocks, answer_conf, validation
    )
    steps.append(
        {
            "role": "answer_review_result",
            "model": reviewer_model,
            "status": status,
            "n_issues": len(issues),
        }
    )
    return (
        content,
        answer_conf,
        answer_text,
        validation,
        temporal_gate_result,
        original_for_wire,
    )


def _indepth_note(level: str, n_dropped: int, issues: str) -> str:
    """Clinician-facing confidence note for the In-Depth, from the validator verdict."""
    if level == "yellow":
        return "Supporting context was flagged on review and regenerated."
    if level == "red":
        base = "Some supporting context could not be reliably grounded"
        if n_dropped:
            base += " (" + str(n_dropped) + " point(s) removed)"
        return base + ((": " + issues) if issues else "") + "."
    return ""


def _indepth_feedback(verdict: Dict[str, Any], claims: List[str]) -> str:
    """In-Depth re-synthesis guidance from the validator verdict (the flagged claims + the note)."""
    issues = (verdict.get("issues") or "").strip()
    drop = verdict.get("drop") or []
    flagged = "; ".join(claims[i - 1] for i in drop if 1 <= i <= len(claims))
    parts = ["Some In-Depth points were flagged on clinical review."]
    if flagged:
        parts.append("Flagged points: " + flagged)
    if issues:
        parts.append("Reviewer note: " + issues)
    parts.append(
        "Rewrite the In-Depth as a fresh list of claims: drop or correct the flagged points, "
                 "keep only well-grounded WHO/guideline guidance applied to this patient, and never "
        "invent a source, dose, or value."
    )
    return "\n".join(parts)


async def _gen_indepth(
    client: httpx.AsyncClient,
    synth_model: str,
    base_messages: List[Dict[str, Any]],
    indepth_instruction: str,
    gathered: str,
    answer_text: str,
    *,
    validator_model: Optional[str],
    validator_prompt: Optional[str],
    chart: str,
    synth_temperature: float,
    synth_repeat_penalty: Optional[float],
    synth_dry: Optional[float],
    validator_temperature: float,
    validator_repeat_penalty: Optional[float],
    validator_dry: Optional[float],
    max_tokens: Optional[int],
    max_loops: int,
    steps: List[Dict[str, Any]],
) -> Tuple[List[str], Dict[str, Any]]:
    """IN-DEPTH path with the same confidence cycle as the Answer: synthesize the KB-informed claim
    list, audit it; if flagged, RE-SYNTHESIZE (with feedback) and re-audit BEFORE stripping. Returns
    (surviving_claims, confidence) where confidence.level is green (clean first pass) / yellow
    (flagged then cleared on re-synth) / red (still flagged -> the survivors are kept, the rest
    block/stripped). Records every call into `steps`."""
    green = {"level": "green", "note": ""}

    async def _audit(cl: List[str], attempt: int) -> Dict[str, Any]:
        try:
            verdict = await _validate_indepth_verdict(
                client,
                validator_model,
                chart=chart,
                gathered=gathered,
                answer_text=answer_text,
                claims=cl,
                max_tokens=max_tokens,
                temperature=validator_temperature,
                repeat_penalty=validator_repeat_penalty,
                dry=validator_dry,
                validation_prompt=validator_prompt or "validation",
            )
        except Exception as e:
            logger.warning("indepth-validator call failed: %s", e)
            verdict = {"drop": [], "issues": ""}
        steps.append(
            {
                "role": "indepth_validator",
                "model": validator_model,
                "attempt": attempt,
                "drop": verdict.get("drop") or [],
                "issues": verdict.get("issues", ""),
                "claims_in": len(cl),
            }
        )
        return verdict

    claims = await _synthesize_indepth(
        client,
        synth_model,
        base_messages,
        indepth_instruction,
        gathered,
        answer_text,
        temperature=synth_temperature,
        max_tokens=max_tokens,
        repeat_penalty=synth_repeat_penalty,
        dry=synth_dry,
    )
    steps.append(
        {"role": "indepth_synth", "model": synth_model, "claims": list(claims)}
    )
    if not (validator_model and claims):
        return claims, green

    v = await _audit(claims, 0)
    if not (v.get("drop") or []):
        return claims, green

    # flagged -> re-synthesize the In-Depth (feedback) and re-audit BEFORE stripping.
    for _ in range(max(0, max_loops)):
        logger.info("indepth-validator: claims flagged -> re-synthesizing")
        revised = await _synthesize_indepth(
            client,
            synth_model,
            base_messages,
            indepth_instruction,
            gathered,
            answer_text,
            temperature=synth_temperature,
            max_tokens=max_tokens,
            repeat_penalty=synth_repeat_penalty,
            dry=synth_dry,
            extra_msgs=[
                {"role": "assistant", "content": json.dumps({"claims": claims})},
                {"role": "user", "content": _indepth_feedback(v, claims)},
            ],
        )
        steps.append(
            {"role": "indepth_resynth", "model": synth_model, "claims": list(revised)}
        )
        if not revised:
            break
        claims = revised
        v = await _audit(claims, 1)
        if not (v.get("drop") or []):
            return claims, {"level": "yellow", "note": _indepth_note("yellow", 0, "")}

    # still flagged after re-synth -> block/strip the remaining flagged claims (red).
    drop = v.get("drop") or []
    kept = [c for i, c in enumerate(claims, start=1) if i not in set(drop)]
    logger.info("indepth-validator: still flagged after re-synth -> strip %s", drop)
    return kept, {
        "level": "red",
        "note": _indepth_note("red", len(drop), v.get("issues", "")),
    }


def _extract_citations(text: str) -> List[int]:
    """The 1-based [N] citation indices a corrected answer cites, in order, deduped — so an adopted
    rewrite carries its own citations rather than the superseded draft's."""
    return sorted({int(m) for m in re.findall(r"\[(\d+)\]", text or "")})


def _rw_issue(verdict: Optional[Dict[str, Any]]) -> str:
    """A clinician-facing issue string from a rewrite verdict's first localized error (the chart-correct
    fact), for the confidence caveat."""
    errs = (verdict or {}).get("errors") or []
    if not errs:
        return ""
    e = errs[0]
    return (e.get("chart") or e.get("fix") or "").strip()


async def _validate_and_refine_answer(
    client: httpx.AsyncClient,
    *,
    synth_model: str,
    base_messages: List[Dict[str, Any]],
    answer_instruction: str,
    gathered: str,
    response_format: Optional[Dict[str, Any]],
    answer_text: str,
    citations: List[int],
    blocks: List[Dict[str, Any]],
    validator_model: Optional[str],
    validator_prompt: Optional[str],
    chart: str,
    synth_temperature: float,
    synth_repeat_penalty: Optional[float],
    synth_dry: Optional[float],
    validator_temperature: float,
    validator_repeat_penalty: Optional[float],
    validator_dry: Optional[float],
    max_tokens: Optional[int],
    max_loops: int,
    steps: List[Dict[str, Any]],
) -> Tuple[str, List[int], List[Dict[str, Any]], Dict[str, Any]]:
    """Audit the draft Answer against the chart and, on a genuine flag, re-synthesize up to max_loops.
    This composable post-synthesis step is shared by every profile that declares review.
    With no validator model, it performs only the always-on substance check.
    Returns (answer_text, citations, blocks, answer_conf{level: green|yellow|red, note}).
    """
    answer_conf = {"level": "green", "note": ""}

    # --- Deterministic substance gate (always; NO model). A non-substantive draft (empty /
    # punctuation-only / the fallback string) must NEVER ship green — the LLM validator fails open on a
    # "." answer. Re-synthesize up to max_loops; if it stays non-substantive, ship the fallback envelope
    # with RED confidence. Mirrors harness runner._row_is_good so the hub never ships what resume re-runs.
    if not _is_substantive_answer(answer_text):
        for i in range(max(1, max_loops)):
            attempt_text, attempt_cit, attempt_blk = await _synthesize_answer(
                client,
                synth_model,
                base_messages,
                answer_instruction,
                gathered,
                response_format=response_format,
                temperature=synth_temperature,
                max_tokens=max_tokens,
                repeat_penalty=synth_repeat_penalty,
                dry=synth_dry,
            )
            steps.append(
                {
                    "role": "answer_resynth",
                    "model": synth_model,
                    "reason": "non-substantive",
                    "attempt": i + 1,
                    "output": attempt_text,
                }
            )
            if _is_substantive_answer(attempt_text):
                answer_text, citations, blocks = attempt_text, attempt_cit, attempt_blk
                answer_conf = {
                    "level": "yellow",
                    "note": _answer_note(
                        "yellow", "the first draft was not a usable answer", ""
                    ),
                }
                break
        else:
            steps.append(
                {"role": "substance_gate", "result": "fallback", "model": synth_model}
            )
            return (
                FALLBACK_ANSWER,
                [],
                [],
                {
                    "level": "red",
                    "note": "The team could not produce a usable answer this turn. Verify against the chart.",
                },
            )

    if not validator_model:
        return answer_text, citations, blocks, answer_conf

    # --- Single answer validator: REWRITE mode. The validator localizes each chart contradiction AND
    # returns the surgically-corrected answer; we ADOPT that fix (re-extracting its [N] citations),
    # re-audit, and keep the BEST (fewest-errors) version — never regressing below the draft. (The old
    # regenerate path — re-synthesizing the whole answer from a one-line critique, which could degrade a
    # strong answer — was removed; rewrite won the A/B.)
    async def _audit_rw(draft: str, attempt: int) -> Optional[Dict[str, Any]]:
        try:
            verdict = await _validate_answer_rewrite(
                client,
                validator_model,
                chart=chart,
                gathered=gathered,
                answer_text=draft,
                max_tokens=max_tokens,
                temperature=validator_temperature,
                repeat_penalty=validator_repeat_penalty,
                dry=validator_dry,
                validation_prompt=validator_prompt or _REWRITE_VALIDATOR_PROMPT,
            )
        except Exception as e:
            logger.warning("rewrite-validator call failed: %s", e)
            verdict = None  # fail-open
        errs = (verdict or {}).get("errors") or []
        steps.append(
            {
                "role": "answer_validator",
                "mode": "rewrite",
                "model": validator_model,
                "attempt": attempt,
                "answer_ok": (verdict or {}).get("answer_ok", True),
                "n_errors": len(errs),
                "errors": errs,
            }
        )
        return verdict

    v = await _audit_rw(answer_text, 0)
    # fail-open or a clean pass -> keep current conf (green, or yellow if the substance gate re-synthesized).
    if v is None or v.get("answer_ok", True) or not v.get("errors"):
        return answer_text, citations, blocks, answer_conf
    first_issue = _rw_issue(v)
    best_text, best_cit, best_n = answer_text, citations, len(v.get("errors") or [])
    cleared = False
    for i in range(max(1, max_loops)):
        corrected = (v.get("corrected_answer") or "").strip()
        if not corrected or corrected == best_text:
            break  # flagged but no usable rewrite offered -> keep the best so far
        if not _is_substantive_answer(corrected):
            steps.append(
                {
                    "role": "substance_gate",
                    "result": "rejected_review_rewrite",
                    "model": validator_model,
                }
            )
            break
        cand_cit = _extract_citations(corrected) or best_cit
        v = await _audit_rw(corrected, i + 1)
        n = len((v or {}).get("errors") or []) if v else 0
        if v is None or v.get("answer_ok", True) or n == 0:
            best_text, best_cit, cleared = corrected, cand_cit, True
            break
        if n < best_n:  # strictly fewer errors -> adopt, then try to fix the rest
            best_text, best_cit, best_n = corrected, cand_cit, n
        else:
            break  # not better -> stop; keep the previous best (never regress)
    if cleared:
        answer_conf = {
            "level": "yellow",
            "note": _answer_note("yellow", first_issue, ""),
        }
    else:
        answer_conf = {
            "level": "red",
            "note": _answer_note("red", first_issue, _rw_issue(v)),
        }
    return best_text, best_cit, blocks, answer_conf


# Per-turn reasoning trace: the hub appends one structured line per turn to a writable mount so the
# live dashboard can render the full LLM flow (orchestrator -> kb/expert -> answer synth -> answer
# validator(+resynth) -> in-depth synth -> in-depth validator) + per-section confidence. The dashboard
# correlates a trace line to a results.jsonl cell by level_id + the ts falling in the cell's
# started_at..ended_at window (the runner is strictly sequential).
_TRACE_DIR = os.environ.get("TEAM_TRACE_DIR", "/app/trace")


def _write_trace(
    level_id: Optional[str],
    messages: List[Dict[str, Any]],
    *,
    orchestrator: Optional[str],
    expert: Optional[str],
    synthesizer: Optional[str],
    validator: Optional[str],
    steps: List[Dict[str, Any]],
    answer_confidence: Dict[str, Any],
    indepth_confidence: Dict[str, Any],
    answer_text: str = "",
                 in_depth_claims: Optional[List[str]] = None,
                 reference_date: Optional[str] = None,
                 temporal_facts: Optional[Dict[str, Any]] = None,
                 temporal_gate: Optional[Dict[str, Any]] = None,
                 original_answer_text: Optional[str] = None,
                 answer_validation: Optional[Dict[str, Any]] = None,
    sampling: Optional[Dict[str, Any]] = None,
    context_summary: Optional[Dict[str, Any]] = None,
    indepth_temporal_gate: Optional[Dict[str, Any]] = None,
    final_references: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """Append one per-turn reasoning-trace line — the structured package a client renders (the
    SHIPPED answer + in-depth claims + per-section confidence + the ordered call steps). Best-effort:
    never raises (a trace-write failure must never break a turn)."""
    try:
        question = ""
        for m in reversed(messages):
            if m.get("role") == "user":
                c = m.get("content")
                question = c if isinstance(c, str) else json.dumps(c)
                break
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level_id": level_id,
            "question": question[:2000],
            "reference_date": reference_date,
            "models": {
                "orchestrator": orchestrator,
                "expert": expert,
                "synthesizer": synthesizer,
                "validator": validator,
            },
            "sampling": sampling or {},
            "answer_text": answer_text,
            "in_depth_claims": in_depth_claims or [],
            "answer_confidence": answer_confidence,
            "indepth_confidence": indepth_confidence,
            "temporal_facts_schema_version": (
                temporal_facts.get("schema_version")
                if isinstance(temporal_facts, dict)
                else None
            ),
            "temporal_facts_summary": temporal.compact_temporal_facts_summary(
                temporal_facts
            ),
            "temporal_gate": temporal_gate,
            "indepth_temporal_gate": indepth_temporal_gate,
            "original_answer_text": original_answer_text,
            "answer_validation": answer_validation,
            "context": context_summary,
            "final_references": final_references or [],
            "steps": steps,
        }
        os.makedirs(_TRACE_DIR, exist_ok=True)
        with open(os.path.join(_TRACE_DIR, "trace.jsonl"), "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str) + "\n")
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("trace write failed (non-fatal): %s", e)


async def _gather_evidence(
    client: httpx.AsyncClient,
    *,
    has_expert: bool,
    orchestrator_model: str,
    orchestrator_system: str,
    expert_model: Optional[str],
    expert_system: str,
    messages: List[Dict[str, Any]],
    chart: str,
    max_tokens: Optional[int],
    orch_temp: Optional[float] = None,
    orch_rp: Optional[float] = None,
    orch_dry: Optional[float] = None,
    exp_temp: Optional[float] = None,
    exp_rp: Optional[float] = None,
    exp_dry: Optional[float] = None,
    allow_kb_search: bool = True,
) -> Tuple[str, List[str], List[Dict[str, Any]]]:
    """Gather team/expert/KB context for a compiled ``gather`` stage.

    The tool loop uses a small orchestrator to call ``medical_expert``/``kb_search`` and is
    followed by a deterministic KB fallback when the model does not request one.
    Returns ``(kb_context, expert_notes, orch_steps)``.
    """
    kb_context = ""
    expert_notes: List[str] = []
    orch_steps: List[Dict[str, Any]] = []

    # The tool loop runs under the orchestrator's OWN system prompt — not chartsearchai's envelope
    # prompt, which biases a small model toward answering immediately. The caller's `messages` is
    # left untouched for the synthesis prefix.
    loop_messages: List[Dict[str, Any]] = [
        {"role": "system", "content": orchestrator_system}
    ] + [m for m in messages if m.get("role") != "system"]
    tools = _tool_definitions(has_expert, allow_kb_search)

    try:
        for _ in range(MAX_TOOL_ITERATIONS if tools else 0):
            msg = await _chat(
                client,
                orchestrator_model,
                loop_messages,
                tools=tools,
                temperature=orch_temp,
                max_tokens=max_tokens,
                repeat_penalty=orch_rp,
                dry_multiplier=orch_dry,  # DRY default OFF for tool-calling
            )
            tool_calls = msg.get("tool_calls")
            orch_steps.append(
                {
                    "role": "orchestrator",
                    "model": orchestrator_model,
                    "tool_calls": [
                        tc.get("function", {}).get("name") for tc in (tool_calls or [])
                    ],
                }
            )
            if not tool_calls:
                break  # orchestrator has gathered enough; proceed to synthesis
            loop_messages.append(msg)
            seen: set = set()  # dedupe identical calls within this message
            for tc in tool_calls:
                name = tc.get("function", {}).get("name")
                try:
                    args = json.loads(tc["function"]["arguments"] or "{}")
                except (json.JSONDecodeError, KeyError, TypeError):
                    args = {}
                dedup_key = (name, json.dumps(args, sort_keys=True))
                if dedup_key in seen:
                    observation = "(duplicate tool call ignored)"
                else:
                    seen.add(dedup_key)
                    if name == "medical_expert":
                        observation = await _run_medical_expert(
                            client,
                            args.get("query", ""),
                            chart,
                            expert_system,
                            kb_context=kb_context,
                            model=expert_model,
                            temperature=exp_temp,
                            repeat_penalty=exp_rp,
                            dry_multiplier=exp_dry,
                        )
                        expert_notes.append(observation)
                        orch_steps.append(
                            {
                                "role": "medical_expert",
                                "model": expert_model,
                                "query": args.get("query", ""),
                                "note": observation[:400],
                            }
                        )
                    elif name == "kb_search":
                        observation = _run_kb_search(args.get("query", ""))
                        hit = observation.startswith(_KB_BLOCK_HEADER)
                        if hit:
                            kb_context = (
                                kb_context + "\n\n" + observation
                                if kb_context
                                else observation
                            )
                        orch_steps.append(
                            {
                                "role": "kb_search",
                                "query": args.get("query", ""),
                                "hit": hit,
                                "chars": len(observation),
                            }
                            )
                    else:
                        observation = f"(unknown tool: {name})"
                loop_messages.append(
                    {
                    "role": "tool",
                    "tool_call_id": tc.get("id"),
                    "content": observation,
                    }
                )
    except ContextSourceError:
        raise
    except Exception as e:
        logger.warning("orchestrator tool loop failed, proceeding to synthesis: %s", e)

    # KB-retrieval fallback: small orchestrators often skip kb_search (esp. on follow-up turns),
    # leaving the In-Depth with no guidance to ground. If nothing was gathered, do one deterministic
    # kb_search on the question so the synthesis still has reference context.
    if allow_kb_search and not kb_context:
        q = _latest_user_text(messages)
        if q:
            obs = _run_kb_search(q)
            hit = obs.startswith(_KB_BLOCK_HEADER)
            if hit:
                kb_context = obs
            orch_steps.append(
                {
                    "role": "kb_search",
                    "query": q[:160],
                    "hit": hit,
                    "chars": len(obs),
                    "fallback": True,
    }
            )

    return kb_context, expert_notes, orch_steps
