"""
In-process "Med Agent Team": a ReAct-style orchestrator over typed tools,
called directly by the OpenAI-compat bridge.

Flow (prompt-driven, not a hardcoded pipeline): the orchestrator (Gemma-4-E4B)
runs a short tool loop under its OWN system prompt — it is *strongly suggested*
to call `kb_search` first for guideline/dose/threshold questions, then
`medical_expert` (MedGemma) which reasons GIVEN the retrieved KB context, then
stop. A final synthesis call — bound to chartsearchai's response_format — composes
the strict {answer, citations, blocks} envelope from the gathered evidence.

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
import re
from typing import Any, Dict, List, Optional

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

# The validator returns a structured verdict. The answer and the context are judged
# as SEPARATE criteria (distinct issue fields) so the feedback localizes where the
# error entered — bad gathered evidence vs bad synthesis.
_VALIDATOR_RF = {
    "type": "json_schema",
    "json_schema": {
        "name": "validator_verdict",
        "schema": {
            "type": "object",
            "properties": {
                "ok": {"type": "boolean"},
                "answer_issues": {"type": "string"},
                "context_issues": {"type": "string"},
            },
            "required": ["ok"],
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


def _validator_feedback(verdict: Dict[str, Any]) -> str:
    """Turn a flagged verdict into specific re-synthesis instructions."""
    parts = ["A reviewer audited your draft against the patient chart and found issues "
             "that MUST be fixed before the answer is returned:"]
    ai = (verdict.get("answer_issues") or "").strip()
    ci = (verdict.get("context_issues") or "").strip()
    if ai:
        parts.append("ANSWER problems: " + ai)
    if ci:
        parts.append("CONTEXT/evidence problems (do not carry these into the answer): " + ci)
    parts.append(
        "Rewrite the answer using ONLY facts supported by the patient chart. Do not assert "
        "a trend from fewer than two dated points, do not claim a time window the data does "
        "not cover, keep every date matched to its real value, and answer 'not documented' "
        "when the chart lacks the information.")
    return "\n".join(parts)


async def _run_validator(
    client: httpx.AsyncClient,
    validator_model: str,
    validator_prompt: Optional[str],
    *,
    chart: str,
    gathered: str,
    answer: str,
    max_tokens: Optional[int],
    temperature: float = 0.0,
    repeat_penalty: Optional[float] = None,
    dry_multiplier: Optional[float] = None,
) -> Dict[str, Any]:
    """One audit pass: judge the gathered CONTEXT and the draft ANSWER separately
    against the chart. Returns {ok, answer_issues, context_issues}; fails OPEN (ok=True)
    on any malformed verdict so a flaky validator never blocks the run."""
    instruction = load_prompt(validator_prompt or "validation")
    try:
        obj = json.loads(answer)
        answer_text = obj.get("answer", answer) if isinstance(obj, dict) else answer
    except (json.JSONDecodeError, TypeError):
        answer_text = answer
    audit_user = (
        instruction
        + "\n\n=== PATIENT CHART (ground truth) ===\n" + (chart or "(none)")
        + "\n\n=== GATHERED CONTEXT (evidence the team collected) ===\n" + (gathered or "(none)")
        + "\n\n=== DRAFT ANSWER (to audit) ===\n" + answer_text
    )
    msg = await _chat(
        client, validator_model, [{"role": "user", "content": audit_user}],
        response_format=_VALIDATOR_RF, temperature=temperature, max_tokens=max_tokens,
        repeat_penalty=repeat_penalty, dry_multiplier=dry_multiplier)
    try:
        verdict = json.loads(_message_text(msg))
    except (json.JSONDecodeError, TypeError):
        return {"ok": True}
    return verdict if isinstance(verdict, dict) else {"ok": True}


async def _audit_and_revise(
    client: httpx.AsyncClient,
    content: str,
    *,
    validator_model: str,
    validator_prompt: Optional[str],
    chart: str,
    gathered: str,
    synth_model: str,
    synth_messages: List[Dict[str, Any]],
    response_format: Optional[Dict[str, Any]],
    synth_temperature: float,
    synth_repeat_penalty: Optional[float],
    synth_dry: Optional[float],
    validator_temperature: float,
    validator_repeat_penalty: Optional[float],
    validator_dry: Optional[float],
    max_tokens: Optional[int],
    max_loops: int,
) -> str:
    """Post-synthesis audit round: validate the draft, and on a flag append specific
    feedback + re-synthesize, up to max_loops cycles. Returns the (possibly revised)
    envelope. Never raises — any validator/re-synth failure keeps the current draft."""
    for _ in range(max(0, max_loops)):
        try:
            verdict = await _run_validator(
                client, validator_model, validator_prompt,
                chart=chart, gathered=gathered, answer=content, max_tokens=max_tokens,
                temperature=validator_temperature, repeat_penalty=validator_repeat_penalty,
                dry_multiplier=validator_dry)
        except Exception as e:
            logger.warning("validator call failed, keeping draft: %s", e)
            return content
        if verdict.get("ok", True):
            return content
        revise_messages = synth_messages + [
            {"role": "assistant", "content": content},
            {"role": "user", "content": _validator_feedback(verdict)},
        ]
        try:
            msg = await _chat(
                client, synth_model, revise_messages,
                response_format=response_format, temperature=synth_temperature,
                max_tokens=max_tokens, repeat_penalty=synth_repeat_penalty,
                dry_multiplier=synth_dry)
            revised = _normalize_envelope(_message_text(msg))
            if revised:
                content = revised
        except Exception as e:
            logger.warning("re-synthesis after validator flag failed, keeping draft: %s", e)
            return content
    return content


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
    synthesis_instruction = load_prompt(synthesizer_prompt or "synthesis")

    # The tool loop runs under the orchestrator's OWN system prompt — not
    # chartsearchai's envelope prompt, which biases a small model toward answering
    # immediately. The original `messages` is left untouched for the synthesis prefix.
    loop_messages: List[Dict[str, Any]] = [
        {"role": "system", "content": orchestrator_system}
    ] + [m for m in messages if m.get("role") != "system"]

    kb_context = ""          # accumulated KB snippets, threaded into the expert + synthesis
    expert_notes: List[str] = []  # accumulated clinical-expert observations

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
                        elif name == "kb_search":
                            observation = _run_kb_search(args.get("query", ""))
                            if observation.startswith(_KB_BLOCK_HEADER):
                                kb_context = (
                                    kb_context + "\n\n" + observation if kb_context else observation
                                )
                        else:
                            observation = f"(unknown tool: {name})"
                    loop_messages.append({
                        "role": "tool",
                        "tool_call_id": tc.get("id"),
                        "content": observation,
                    })
        except Exception as e:
            logger.warning("orchestrator tool loop failed, proceeding to synthesis: %s", e)

        # --- final synthesis (constrained to the envelope) ------------------
        # Rebuild from the ORIGINAL prefix (byte-identical for LM Studio's cache);
        # append the gathered evidence + instruction as the last user turn, so the
        # decisive evidence sits at the end of the array (lost-in-the-middle).
        gathered = _gathered_evidence(kb_context, expert_notes)
        synth_user = synthesis_instruction + "\n\n" + gathered if gathered else synthesis_instruction
        synth_messages = list(messages) + [{"role": "user", "content": synth_user}]
        try:
            msg = await _chat(
                client, synth_model, synth_messages,
                response_format=response_format, temperature=synth_temp,
                max_tokens=max_tokens, repeat_penalty=synth_rp,
                dry_multiplier=synth_dry,
            )
            content = _normalize_envelope(_message_text(msg))
            if content:
                if validator_model:
                    content = await _audit_and_revise(
                        client, content,
                        validator_model=validator_model, validator_prompt=validator_prompt,
                        chart=chart, gathered=gathered,
                        synth_model=synth_model, synth_messages=synth_messages,
                        response_format=response_format,
                        synth_temperature=synth_temp, synth_repeat_penalty=synth_rp, synth_dry=synth_dry,
                        validator_temperature=val_temp, validator_repeat_penalty=val_rp, validator_dry=val_dry,
                        max_tokens=max_tokens, max_loops=validator_max_loops)
                return content
            logger.warning("synthesis returned empty content; using fallback envelope")
        except Exception as e:
            logger.error("synthesis call failed: %s", e, exc_info=True)

    return _fallback_envelope(
        "I could not produce a complete answer for this turn. Please try again.")
