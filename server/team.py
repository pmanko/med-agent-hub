"""
In-process "Med Agent Team": a ReAct-style orchestrator over typed tools,
called directly by the OpenAI-compat bridge.

Flow (prompt-driven, not a hardcoded pipeline): the orchestrator (Gemma-4-E4B)
runs a short tool loop under its OWN system prompt — it is *strongly suggested*
to call `kb_search` first for guideline/dose/threshold questions, then
`medical_expert` (MedGemma) which reasons GIVEN the retrieved KB context, then
stop. Generation is then TWO distinct calls: an Answer synthesis (bound to
chartsearchai's response_format) and a separate In-Depth synthesis that elaborates
that answer. Each has its own validator, and the two are combined into a single
markdown body ("**Answer**\n...\n\n**In Depth**\n- ...") only at the chartsearchai
handoff, which still receives the strict {answer, citations, blocks} envelope.

Design notes (see specs/artifacts/planning/react-team-orchestration-design.md):
- The tool loop runs on its own message list under the orchestrator prompt, NOT
  under chartsearchai's envelope system prompt (which biases a small model). The
  original `messages` is kept untouched for the synthesis prefix so LM Studio's
  [system, user(chart)] prompt cache stays warm.
- KB results are threaded into the medical_expert call IN CODE (the prompt no
  longer carries that dependency) so the clinical model reasons WITH the guidance.
- Tool-selection turns run PLAIN (tools, no response_format); only the final
  synthesis is schema-constrained. Mixing the two in one call is unreliable sub-7B.
- Duplicate tool calls in one assistant message are deduped (Gemma+LM Studio bug
  #1756 emits identical duplicates regardless of parallel_tool_calls).
- Every path returns a schema-valid envelope: a failed/slow tool is skipped and
  the turn still synthesizes; a hard failure returns a minimal fallback envelope.
"""

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

from . import kb
from .config import (
    llm_config, SYNTH_REPEAT_PENALTY,
    ORCHESTRATOR_DRY_MULTIPLIER, EXPERT_DRY_MULTIPLIER, SYNTH_DRY_MULTIPLIER,
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


# Team levels (per-tier orchestrator/expert/synthesizer models + prompts) are
# declared in server/levels.yaml and resolved by server/levels_loader (see
# openai_compat._content_for). run_team takes the resolved per-role models, prompt
# names, and the has_expert toggle as kwargs — no presets baked into code.


def _tool_definitions(has_expert: bool = True) -> List[Dict[str, Any]]:
    """OpenAI tool definitions the orchestrator may call. kb_search is always
    offered; medical_expert only when the level has an expert (so a level with
    expert: null runs with no expert tool). Descriptions carry the trigger
    keywords + ordering so a small model sequences kb_search then expert."""
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
                    "kb_search({\"query\": \"WHO first-line ART; stavudine d4T "
                    "phase-out\"}). Returns reference snippets with provenance — never "
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
                    "medical_expert({\"query\": \"Given the chart's regimen, is it still "
                    "WHO-recommended, and what is the concern if not?\"})."
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
    return tools


def _chart_context(messages: List[Dict[str, Any]]) -> str:
    """The chart snapshot is chartsearchai's first user message (after system)."""
    for m in messages:
        if m.get("role") == "user":
            content = m.get("content")
            return content if isinstance(content, str) else json.dumps(content)
    return ""


def _latest_user_text(messages: List[Dict[str, Any]]) -> str:
    """The current turn's question is chartsearchai's LAST user message."""
    for m in reversed(messages):
        if m.get("role") == "user":
            content = m.get("content")
            return content if isinstance(content, str) else json.dumps(content)
    return ""


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
    """One LM Studio (OpenAI-compat) chat call. Returns the first choice's message."""
    payload: Dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature if temperature is not None else llm_config.temperature,
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

    headers = {"Content-Type": "application/json"}
    if llm_config.api_key:
        headers["Authorization"] = f"Bearer {llm_config.api_key}"

    url = f"{llm_config.base_url.rstrip('/')}/v1/chat/completions"
    logger.info("team _chat: model=%s tools=%s response_format=%s",
                model, bool(tools), bool(response_format))
    resp = await client.post(url, json=payload, headers=headers, timeout=180.0)
    if resp.status_code >= 400:
        # Surface LM Studio's reason (context overflow, bad schema, etc.) — bare
        # status codes are not actionable.
        logger.error("LM Studio %s for model=%s tools=%s response_format=%s: %s",
                     resp.status_code, model, bool(tools), bool(response_format),
                     resp.text[:800])
        resp.raise_for_status()
    return resp.json()["choices"][0]["message"]


def _message_text(msg: Dict[str, Any]) -> str:
    """Assistant text. Reasoning models served via LM Studio MLX (Qwen 3.x) emit the
    answer / structured envelope in `reasoning_content` and leave `content` empty —
    fall back to it so a real answer is not lost as an empty-content fallback. Verified:
    qwen3.6-35b-a3b under a JSON response_format returns the envelope in reasoning_content;
    enable_thinking=false / /no_think are not honored by LM Studio MLX."""
    return (msg.get("content") or "").strip() or (msg.get("reasoning_content") or "").strip()


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
        msg = await _chat(client, model or llm_config.med_model, messages, temperature=temperature,
                          max_tokens=800, repeat_penalty=repeat_penalty, dry_multiplier=dry_multiplier)
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
    notes = [n for n in expert_notes if n and not n.startswith("(medical expert unavailable")]
    if notes:
        parts.append("Clinical expert notes:\n" + "\n\n".join(notes))
    if not parts:
        return ""
    return "Gathered evidence:\n\n" + "\n\n".join(parts)


def _fallback_envelope(answer: str) -> str:
    """A minimal, always-schema-valid chart_answer envelope."""
    return json.dumps({"answer": answer, "citations": [], "blocks": []})


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
# deterministic. NOTE: LM Studio's MLX OpenAI engine SILENTLY DROPS frequency_penalty
# (and DRY) — only `repeat_penalty` (config.SYNTH_REPEAT_PENALTY) is honored, so that
# is the lever we send. (frequency_penalty here was previously a confirmed no-op.)
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


_ANSWER_VERDICT_RF = {
    "type": "json_schema",
    "json_schema": {
        "name": "answer_verdict",
        "schema": {
            "type": "object",
            "properties": {
                "answer_ok": {"type": "boolean"},
                "answer_issues": {"type": "string"},
            },
            "required": ["answer_ok"],
        },
    },
}


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
    """Resolve one per-role sampling knob from a level's `knobs` block, falling back to
    the global default when the role or key is unset. knobs = {role: {key: value}}."""
    role_knobs = (knobs or {}).get(role)
    if isinstance(role_knobs, dict) and role_knobs.get(key) is not None:
        return role_knobs[key]
    return default


# Confidence level -> tag label (high=green, medium=yellow, low=red). The hub emits the structured
# {level, note}; clients (dashboard/report, and chat once its schema is updated) render the tag.
_CONF_LABEL = {"green": "High confidence", "yellow": "Medium confidence", "red": "Low confidence"}


def _answer_body(answer_text: str, claims: List[str]) -> str:
    """Combine the direct Answer and the In-Depth claims into one CLEAN markdown body (no confidence
    text baked in — confidence is structured metadata a client renders as a tag). The Answer leads
    under a **Answer** header; non-empty claims follow as a **In Depth** bullet list."""
    body = "**Answer**\n" + (answer_text or "").strip()
    if claims:
        body += "\n\n**In Depth**\n" + "\n".join("- " + c for c in claims)
    return body


def _assemble_envelope(
    answer_text: str, citations: List[int], blocks: List[Any], claims: List[str],
    answer_conf: Optional[Dict[str, Any]] = None, indepth_conf: Optional[Dict[str, Any]] = None,
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
        env["confidence"] = {"answer": answer_conf or {"level": "green", "note": ""},
                             "in_depth": indepth_conf or {"level": "green", "note": ""}}
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


def _answer_feedback(verdict: Dict[str, Any]) -> str:
    """Answer-focused re-synthesis guidance from the Answer verdict. The In Depth is generated
    separately, so this only steers the direct Answer."""
    parts = ["Your goal is an accurate direct answer, grounded only in the patient chart."]
    ai = (verdict.get("answer_issues") or "").strip()
    if ai:
        parts.append("A reviewer found this problem with the answer: " + ai)
    parts.append(
        "Rewrite the direct answer to be correct using only chart-supported facts; where the chart "
        "lacks the information, say 'not documented'.")
    return "\n".join(parts)


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
    citations, blocks) parsed from the envelope. FAIL-OPEN: returns ("", [], []) on any error."""
    user = answer_instruction + ("\n\n" + gathered if gathered else "")
    messages = list(base_messages) + [{"role": "user", "content": user}] + (extra_msgs or [])
    try:
        msg = await _chat(
            client, synth_model, messages,
            response_format=response_format, temperature=temperature,
            max_tokens=max_tokens, repeat_penalty=repeat_penalty, dry_multiplier=dry)
        return _answer_fields(_normalize_envelope(_message_text(msg)))
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
        + "\n\n=== DIRECT ANSWER (elaborate THIS; do not restate it) ===\n" + answer_text
        + ("\n\n=== GATHERED KB / EVIDENCE ===\n" + gathered if gathered else "")
    )
    messages = list(base_messages) + [{"role": "user", "content": user}] + (extra_msgs or [])
    try:
        msg = await _chat(
            client, synth_model, messages,
            response_format=_INDEPTH_RF, temperature=temperature,
            max_tokens=max_tokens, repeat_penalty=repeat_penalty, dry_multiplier=dry)
        obj = json.loads(_message_text(msg))
    except (Exception,):  # parse OR call failure -> no elaboration
        logger.warning("in-depth synthesis failed -> no elaboration")
        return []
    if not isinstance(obj, dict):
        return []
    return [c.strip() for c in (obj.get("claims") or []) if isinstance(c, str) and c.strip()]


async def _validate_answer(
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
    validation_prompt: str = "validation",
) -> Dict[str, Any]:
    """Audit the direct Answer for chart-accuracy. Returns {answer_ok, answer_issues}. FAIL-OPEN:
    {answer_ok: True} on any parse failure so a flaky validator never blocks the run."""
    instruction = load_prompt(validation_prompt + "-answer")
    audit_user = (
        instruction
        + "\n\n=== PATIENT CHART (ground truth) ===\n" + (chart or "(none)")
        + "\n\n=== GATHERED KB / EVIDENCE (the guidance the team retrieved) ===\n" + (gathered or "(none)")
        + "\n\n=== DRAFT ANSWER ===\n" + answer_text
    )
    msg = await _chat(
        client, validator_model, [{"role": "user", "content": audit_user}],
        response_format=_ANSWER_VERDICT_RF, temperature=temperature, max_tokens=max_tokens,
        repeat_penalty=repeat_penalty, dry_multiplier=dry)
    raw = _message_text(msg)
    try:
        verdict = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning("answer-validator[%s] verdict UNPARSEABLE -> FAIL-OPEN (pass); raw=%r",
                       validator_model, raw[:240])
        return {"answer_ok": True}
    if not isinstance(verdict, dict):
        logger.warning("answer-validator[%s] verdict not an object -> FAIL-OPEN; raw=%r",
                       validator_model, raw[:240])
        return {"answer_ok": True}
    logger.info("answer-validator[%s] answer_ok=%s answer_issues=%r",
                validator_model, verdict.get("answer_ok"), (verdict.get("answer_issues") or "")[:160])
    return verdict


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
    1..len(claims)], issues: str}. FAIL-OPEN: returns {drop: [], issues: ""} on any parse failure."""
    instruction = load_prompt(validation_prompt + "-indepth")
    numbered = "\n".join(f"{i}. {c}" for i, c in enumerate(claims, start=1))
    audit_user = (
        instruction
        + "\n\n=== PATIENT CHART (ground truth) ===\n" + (chart or "(none)")
        + "\n\n=== GATHERED KB / EVIDENCE (the guidance the team retrieved) ===\n" + (gathered or "(none)")
        + "\n\n=== DIRECT ANSWER (context) ===\n" + answer_text
        + "\n\n=== IN-DEPTH CLAIMS (numbered; return the numbers to DROP) ===\n" + numbered
    )
    msg = await _chat(
        client, validator_model, [{"role": "user", "content": audit_user}],
        response_format=_INDEPTH_VERDICT_RF, temperature=temperature, max_tokens=max_tokens,
        repeat_penalty=repeat_penalty, dry_multiplier=dry)
    raw = _message_text(msg)
    try:
        verdict = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning("indepth-validator[%s] verdict UNPARSEABLE -> FAIL-OPEN (keep all); raw=%r",
                       validator_model, raw[:240])
        return {"drop": [], "issues": ""}
    if not isinstance(verdict, dict):
        logger.warning("indepth-validator[%s] verdict not an object -> FAIL-OPEN; raw=%r",
                       validator_model, raw[:240])
        return {"drop": [], "issues": ""}
    drop = [d for d in (verdict.get("drop") or []) if isinstance(d, int) and 1 <= d <= len(claims)]
    logger.info("indepth-validator[%s] drop=%s/%d claims issues=%r",
                validator_model, drop, len(claims), (verdict.get("issues") or "")[:120])
    return {"drop": drop, "issues": verdict.get("issues", "")}


def _answer_note(level: str, first_issue: str, last_issue: str) -> str:
    """Clinician-facing confidence note for the Answer, composed deterministically from the
    validator verdicts captured during the re-synth cycle (no extra LLM call)."""
    if level == "yellow":
        base = "An initial draft was flagged on clinical review and corrected on a second pass"
        return base + ((" (first issue: " + first_issue + ")") if first_issue else "") + "."
    if level == "red":
        return ("Clinical review still flagged this after a revision: "
                + (last_issue or first_issue or "the answer could not be confirmed against the chart.")
                + " Verify against the chart before acting.")
    return ""


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
    parts.append("Rewrite the In-Depth as a fresh list of claims: drop or correct the flagged points, "
                 "keep only well-grounded WHO/guideline guidance applied to this patient, and never "
                 "invent a source, dose, or value.")
    return "\n".join(parts)


async def _gen_indepth(
    client: httpx.AsyncClient, synth_model: str, base_messages: List[Dict[str, Any]],
    indepth_instruction: str, gathered: str, answer_text: str, *,
    validator_model: Optional[str], validator_prompt: Optional[str], chart: str,
    synth_temperature: float, synth_repeat_penalty: Optional[float], synth_dry: Optional[float],
    validator_temperature: float, validator_repeat_penalty: Optional[float], validator_dry: Optional[float],
    max_tokens: Optional[int], max_loops: int, steps: List[Dict[str, Any]],
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
                client, validator_model, chart=chart, gathered=gathered,
                answer_text=answer_text, claims=cl, max_tokens=max_tokens,
                temperature=validator_temperature, repeat_penalty=validator_repeat_penalty,
                dry=validator_dry, validation_prompt=validator_prompt or "validation")
        except Exception as e:
            logger.warning("indepth-validator call failed: %s", e)
            verdict = {"drop": [], "issues": ""}
        steps.append({"role": "indepth_validator", "model": validator_model, "attempt": attempt,
                      "drop": verdict.get("drop") or [], "issues": verdict.get("issues", ""),
                      "claims_in": len(cl)})
        return verdict

    claims = await _synthesize_indepth(
        client, synth_model, base_messages, indepth_instruction, gathered, answer_text,
        temperature=synth_temperature, max_tokens=max_tokens,
        repeat_penalty=synth_repeat_penalty, dry=synth_dry)
    steps.append({"role": "indepth_synth", "model": synth_model, "claims": list(claims)})
    if not (validator_model and claims):
        return claims, green

    v = await _audit(claims, 0)
    if not (v.get("drop") or []):
        return claims, green

    # flagged -> re-synthesize the In-Depth (feedback) and re-audit BEFORE stripping.
    for _ in range(max(0, max_loops)):
        logger.info("indepth-validator: claims flagged -> re-synthesizing")
        revised = await _synthesize_indepth(
            client, synth_model, base_messages, indepth_instruction, gathered, answer_text,
            temperature=synth_temperature, max_tokens=max_tokens,
            repeat_penalty=synth_repeat_penalty, dry=synth_dry,
            extra_msgs=[{"role": "assistant", "content": json.dumps({"claims": claims})},
                        {"role": "user", "content": _indepth_feedback(v, claims)}])
        steps.append({"role": "indepth_resynth", "model": synth_model, "claims": list(revised)})
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
    return kept, {"level": "red", "note": _indepth_note("red", len(drop), v.get("issues", ""))}


async def _synthesize_and_validate(
    client: httpx.AsyncClient,
    *,
    base_messages: List[Dict[str, Any]],
    gathered: str,
    chart: str,
    synth_model: str,
    answer_instruction: str,
    indepth_instruction: str,
    response_format: Optional[Dict[str, Any]],
    validator_model: Optional[str],
    validator_prompt: Optional[str] = None,
    synth_temperature: float,
    synth_repeat_penalty: Optional[float],
    synth_dry: Optional[float],
    validator_temperature: float,
    validator_repeat_penalty: Optional[float],
    validator_dry: Optional[float],
    max_tokens: Optional[int],
    max_loops: int,
) -> Tuple[str, List[Dict[str, Any]], Dict[str, Any], Dict[str, Any], str, List[str]]:
    """Two-call generation + two independent validators, combined into one envelope only here.
    Returns (envelope_json, trace_steps, answer_confidence, indepth_confidence, answer_text,
    in_depth_claims) — the last two are the SHIPPED content (clean), so the trace package can carry
    the structured pieces a client renders. Each confidence is
    {level: green|yellow|red, note: <clinician caveat>} from that section's re-synth cycle:
      green  = passed clinical review on the first pass;
      yellow = flagged, re-synthesized, then cleared;
      red    = flagged, re-synthesized, still flagged.
    We ALWAYS present the answer — a red section ships with its criticism (no abstain); the renderer
    collapses a red Answer by default. A reasonless Answer flag (answer_ok=False, empty
    answer_issues) is treated as PASS so noise never lowers confidence. FAIL-OPEN: a validator
    outage ships the draft at green."""
    steps: List[Dict[str, Any]] = []
    _idkw = dict(
        validator_model=validator_model, validator_prompt=validator_prompt, chart=chart,
        synth_temperature=synth_temperature, synth_repeat_penalty=synth_repeat_penalty, synth_dry=synth_dry,
        validator_temperature=validator_temperature, validator_repeat_penalty=validator_repeat_penalty,
        validator_dry=validator_dry, max_tokens=max_tokens, max_loops=max_loops, steps=steps)
    green = {"level": "green", "note": ""}

    answer_text, citations, blocks = await _synthesize_answer(
        client, synth_model, base_messages, answer_instruction, gathered,
        response_format=response_format, temperature=synth_temperature,
        max_tokens=max_tokens, repeat_penalty=synth_repeat_penalty, dry=synth_dry)
    steps.append({"role": "answer_synth", "model": synth_model, "output": answer_text, "citations": citations})
    if not answer_text:
        conf = {"level": "red", "note": "The team could not produce an answer this turn."}
        return (_fallback_envelope(
            "I could not produce a complete answer for this turn. Please try again."),
            steps, conf, dict(green), "", [])

    # --- ANSWER path (confidence: green clean / yellow fixed-on-retry / red still-flagged) ---
    answer_conf = dict(green)
    if validator_model:
        async def _audit(draft: str, attempt: int) -> Optional[Dict[str, Any]]:
            try:
                verdict = await _validate_answer(
                    client, validator_model, chart=chart, gathered=gathered, answer_text=draft,
                    max_tokens=max_tokens, temperature=validator_temperature,
                    repeat_penalty=validator_repeat_penalty, dry=validator_dry,
                    validation_prompt=validator_prompt or "validation")
            except Exception as e:
                logger.warning("answer-validator call failed: %s", e)
                verdict = None  # fail-open
            steps.append({"role": "answer_validator", "model": validator_model, "attempt": attempt,
                          "answer_ok": (verdict or {}).get("answer_ok", True),
                          "answer_issues": (verdict or {}).get("answer_issues", "")})
            return verdict

        def _passed(verdict: Optional[Dict[str, Any]]) -> bool:
            # Pass when: no verdict (fail-open), answer_ok True, OR a reasonless False flag.
            if verdict is None or verdict.get("answer_ok", True):
                return True
            return not (verdict.get("answer_issues") or "").strip()

        v = await _audit(answer_text, 0)
        if not _passed(v):
            first_issue = (v.get("answer_issues") or "").strip()
            fixed = False
            for i in range(max(0, max_loops)):
                logger.info("answer-validator: Answer flagged -> re-synthesizing")
                attempt_text, attempt_cit, attempt_blk = await _synthesize_answer(
                    client, synth_model, base_messages, answer_instruction, gathered,
                    response_format=response_format, temperature=synth_temperature,
                    max_tokens=max_tokens, repeat_penalty=synth_repeat_penalty, dry=synth_dry,
                    extra_msgs=[
                        {"role": "assistant",
                         "content": _assemble_envelope(answer_text, citations, blocks, [])},
                        {"role": "user", "content": _answer_feedback(v)},
                    ])
                steps.append({"role": "answer_resynth", "model": synth_model, "output": attempt_text})
                if not attempt_text:
                    break  # re-synth produced nothing; keep the last answer, mark red
                answer_text, citations, blocks = attempt_text, attempt_cit, attempt_blk
                v = await _audit(answer_text, i + 1)
                if _passed(v):
                    logger.info("answer-validator: revision fixed the Answer -> yellow")
                    fixed = True
                    break
            if fixed:
                answer_conf = {"level": "yellow", "note": _answer_note("yellow", first_issue, "")}
            else:
                # RED: keep the (last) flagged answer + its criticism — never hide it (renderer collapses).
                last_issue = (v.get("answer_issues") or "").strip()
                logger.info("answer-validator: Answer still flagged -> red (present + caveat)")
                answer_conf = {"level": "red", "note": _answer_note("red", first_issue, last_issue)}

    # --- IN-DEPTH path (same green/yellow/red cycle) ---------------------
    claims, indepth_conf = await _gen_indepth(
        client, synth_model, base_messages, indepth_instruction, gathered, answer_text, **_idkw)
    return (_assemble_envelope(answer_text, citations, blocks, claims, answer_conf, indepth_conf),
            steps, answer_conf, indepth_conf, answer_text, claims)


# Per-turn reasoning trace: the hub appends one structured line per turn to a writable mount so the
# live dashboard can render the full LLM flow (orchestrator -> kb/expert -> answer synth -> answer
# validator(+resynth) -> in-depth synth -> in-depth validator) + per-section confidence. The dashboard
# correlates a trace line to a results.jsonl cell by level_id + the ts falling in the cell's
# started_at..ended_at window (the runner is strictly sequential).
_TRACE_DIR = os.environ.get("TEAM_TRACE_DIR", "/app/trace")


def _write_trace(level_id: Optional[str], messages: List[Dict[str, Any]], *, orchestrator: str,
                 expert: str, synthesizer: str, validator: Optional[str],
                 steps: List[Dict[str, Any]], answer_confidence: Dict[str, Any],
                 indepth_confidence: Dict[str, Any], answer_text: str = "",
                 in_depth_claims: Optional[List[str]] = None) -> None:
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
            "models": {"orchestrator": orchestrator, "expert": expert,
                       "synthesizer": synthesizer, "validator": validator},
            "answer_text": answer_text,
            "in_depth_claims": in_depth_claims or [],
            "answer_confidence": answer_confidence,
            "indepth_confidence": indepth_confidence,
            "steps": steps,
        }
        os.makedirs(_TRACE_DIR, exist_ok=True)
        with open(os.path.join(_TRACE_DIR, "trace.jsonl"), "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str) + "\n")
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("trace write failed (non-fatal): %s", e)


async def run_team(
    messages: List[Dict[str, Any]],
    *,
    response_format: Optional[Dict[str, Any]] = None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    orchestrator_model: Optional[str] = None,
    synthesizer_model: Optional[str] = None,
    expert_model: Optional[str] = None,
    orchestrator_prompt: Optional[str] = None,
    synthesizer_prompt: Optional[str] = None,
    expert_prompt: Optional[str] = None,
    has_expert: bool = True,
    validator_model: Optional[str] = None,
    validator_prompt: Optional[str] = None,
    validator_max_loops: int = 1,
    knobs: Optional[Dict[str, Any]] = None,
    level_id: Optional[str] = None,
) -> str:
    """
    Run the team for one chartsearchai turn.

    `messages` is the full chartsearchai array ([system, user(chart), ...turns,
    user(question)]); `response_format` is the chart_answer envelope schema.
    Returns the envelope as a JSON string (the bridge puts it in
    choices[0].message.content). Always returns a schema-valid envelope.
    """
    model = orchestrator_model or llm_config.orchestrator_model
    synth_model = synthesizer_model or llm_config.synthesizer_model
    expert_model = expert_model or llm_config.med_model
    chart = _chart_context(messages)

    # Per-role sampling knobs: a level may override any role's temperature / repeat_penalty
    # / dry; unset falls back to today's global default for that role.
    orch_temp = _knob(knobs, "orchestrator", "temperature", temperature)
    orch_rp = _knob(knobs, "orchestrator", "repeat_penalty", None)
    orch_dry = _knob(knobs, "orchestrator", "dry", ORCHESTRATOR_DRY_MULTIPLIER)
    exp_temp = _knob(knobs, "expert", "temperature", 0.1)
    exp_rp = _knob(knobs, "expert", "repeat_penalty", None)
    exp_dry = _knob(knobs, "expert", "dry", EXPERT_DRY_MULTIPLIER)
    synth_temp = _knob(knobs, "synthesizer", "temperature", max(temperature or 0.0, _SYNTH_MIN_TEMPERATURE))
    synth_rp = _knob(knobs, "synthesizer", "repeat_penalty", SYNTH_REPEAT_PENALTY)
    synth_dry = _knob(knobs, "synthesizer", "dry", SYNTH_DRY_MULTIPLIER)
    val_temp = _knob(knobs, "validator", "temperature", 0.0)
    val_rp = _knob(knobs, "validator", "repeat_penalty", None)
    val_dry = _knob(knobs, "validator", "dry", None)

    # Read the role prompts ONCE per request; thread the expert text into the
    # per-iteration expert call so the tool loop does not re-read it from disk
    # each turn.
    orchestrator_system = load_prompt(orchestrator_prompt or "orchestrator")
    expert_system = load_prompt(expert_prompt or "medical_expert")
    # Two-call synthesis: the level's synthesis_prompt / validator_prompt are BASE names; each
    # role's answer/in-depth prompt is "<base>-answer" / "<base>-indepth" (default base
    # "synthesis" / "validation"). A level can swap the whole set by overriding the base.
    _synth_base = synthesizer_prompt or "synthesis"
    answer_instruction = load_prompt(_synth_base + "-answer")
    indepth_instruction = load_prompt(_synth_base + "-indepth")

    # The tool loop runs under the orchestrator's OWN system prompt — not
    # chartsearchai's envelope prompt, which biases a small model toward answering
    # immediately. The original `messages` is left untouched for the synthesis prefix.
    loop_messages: List[Dict[str, Any]] = [
        {"role": "system", "content": orchestrator_system}
    ] + [m for m in messages if m.get("role") != "system"]

    kb_context = ""          # accumulated KB snippets, threaded into the expert + synthesis
    expert_notes: List[str] = []  # accumulated clinical-expert observations
    orch_steps: List[Dict[str, Any]] = []  # reasoning-trace steps for the orchestrator tool loop

    async with httpx.AsyncClient() as client:
        # --- tool loop (plain: tools, no response_format) -------------------
        try:
            for _ in range(MAX_TOOL_ITERATIONS):
                msg = await _chat(
                    client, model, loop_messages,
                    tools=_tool_definitions(has_expert), temperature=orch_temp, max_tokens=max_tokens,
                    repeat_penalty=orch_rp, dry_multiplier=orch_dry,  # DRY default OFF for tool-calling
                )
                tool_calls = msg.get("tool_calls")
                orch_steps.append({"role": "orchestrator", "model": model,
                                   "tool_calls": [tc.get("function", {}).get("name") for tc in (tool_calls or [])]})
                if not tool_calls:
                    break  # orchestrator has gathered enough; proceed to synthesis
                loop_messages.append(msg)
                seen: set = set()  # dedupe identical calls within this message (bug #1756)
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
                                client, args.get("query", ""), chart, expert_system,
                                kb_context=kb_context, model=expert_model,
                                temperature=exp_temp, repeat_penalty=exp_rp, dry_multiplier=exp_dry)
                            expert_notes.append(observation)
                            orch_steps.append({"role": "medical_expert", "model": expert_model,
                                               "query": args.get("query", ""), "note": observation[:400]})
                        elif name == "kb_search":
                            observation = _run_kb_search(args.get("query", ""))
                            hit = observation.startswith(_KB_BLOCK_HEADER)
                            if hit:
                                kb_context = (
                                    kb_context + "\n\n" + observation if kb_context else observation
                                )
                            orch_steps.append({"role": "kb_search", "query": args.get("query", ""),
                                               "hit": hit, "chars": len(observation)})
                        else:
                            observation = f"(unknown tool: {name})"
                    loop_messages.append({
                        "role": "tool",
                        "tool_call_id": tc.get("id"),
                        "content": observation,
                    })
        except Exception as e:
            logger.warning("orchestrator tool loop failed, proceeding to synthesis: %s", e)

        # KB-retrieval fallback: small orchestrators often skip kb_search (esp. on follow-up
        # turns), leaving the In-Depth with no guidance to ground. If nothing was gathered, do one
        # deterministic kb_search on the question so the synthesis still has reference context.
        if not kb_context:
            q = _latest_user_text(messages)
            if q:
                obs = _run_kb_search(q)
                hit = obs.startswith(_KB_BLOCK_HEADER)
                if hit:
                    kb_context = obs
                orch_steps.append({"role": "kb_search", "query": q[:160], "hit": hit,
                                   "chars": len(obs), "fallback": True})

        # --- generation: two distinct calls (answer + in-depth), each validated,
        # combined into one envelope. The base prefix is the ORIGINAL chartsearchai
        # array (byte-identical for LM Studio's cache); the gathered evidence rides on
        # the last user turn so decisive evidence sits at the end (lost-in-the-middle).
        gathered = _gathered_evidence(kb_context, expert_notes)
        try:
            content, synth_steps, answer_conf, indepth_conf, answer_text, claims = \
                await _synthesize_and_validate(
                    client,
                    base_messages=list(messages), gathered=gathered, chart=chart,
                    synth_model=synth_model,
                    answer_instruction=answer_instruction, indepth_instruction=indepth_instruction,
                    response_format=response_format, validator_model=validator_model,
                    validator_prompt=validator_prompt,
                    synth_temperature=synth_temp, synth_repeat_penalty=synth_rp, synth_dry=synth_dry,
                    validator_temperature=val_temp, validator_repeat_penalty=val_rp, validator_dry=val_dry,
                    max_tokens=max_tokens, max_loops=validator_max_loops)
            _write_trace(level_id, messages, orchestrator=model, expert=expert_model,
                         synthesizer=synth_model, validator=validator_model,
                         steps=orch_steps + synth_steps,
                         answer_confidence=answer_conf, indepth_confidence=indepth_conf,
                         answer_text=answer_text, in_depth_claims=claims)
            return content
        except Exception as e:
            logger.error("synthesis failed: %s", e, exc_info=True)

    _write_trace(level_id, messages, orchestrator=model, expert=expert_model,
                 synthesizer=synth_model, validator=validator_model, steps=orch_steps,
                 answer_confidence={"level": "red", "note": "The team could not produce an answer this turn."},
                 indepth_confidence={"level": "green", "note": ""})
    return _fallback_envelope(
        "I could not produce a complete answer for this turn. Please try again.")
