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
from .config import llm_config
from .prompt_loader import load_prompt

logger = logging.getLogger(__name__)

# Small-model tool-calling degrades over long chains; keep the loop short.
MAX_TOOL_ITERATIONS = 3

# The orchestrator, medical_expert, and synthesis system prompts are now read
# from files per request (server/prompt_loader.load_prompt), selected by
# PROMPT_VARIANT, so a prompt edit changes behaviour with no rebuild and an A/B
# can run two variants side by side. The byte-identical baked fallbacks live in
# prompt_loader so a missing file never fails the turn.

# Prefix that marks a real (non-abstain) kb_search observation.
_KB_BLOCK_HEADER = "Knowledge-base reference snippets"


# Per-request team-model presets: the OpenAI `model` id selects which model runs
# each role, so ONE med-agent-hub serves any of these configs per request (no
# reboot). Advertised via /v1/models so chartsearchai's exact-match served-model
# validation accepts the id. An empty dict means run_team's llm_config defaults.
TEAM_PRESETS: Dict[str, Dict[str, str]] = {
    # all-small: e4b orchestrator + e4b synthesizer + medgemma-1.5-4b expert
    # (the llm_config defaults) — fits anywhere.
    "med-agent-team": {},
    # mid ("low power"): a4b synthesizer (quality) + the small medgemma-1.5 expert
    # + e4b orchestrator — the big synth without the heavy 27b expert.
    "med-agent-team-a4b": {"synthesizer_model": "google/gemma-4-26b-a4b"},
    # big: gemma-4-26b-a4b synthesizer (MoE, ~4B-active, so it co-fits the 27b
    # where the 31b dense did not) + medgemma-27b expert; e4b stays the fast
    # orchestrator for the tool loop.
    "med-agent-team-a4b-27b": {
        "synthesizer_model": "google/gemma-4-26b-a4b",
        "expert_model": "medgemma-27b-text-it-mlx",
    },
    # CLEAN (non-gemma-4) Qwen synthesizers — the fix for the gemma-4 repetition
    # collapse (#622): Qwen has no collapse + strong JSON. e4b stays orchestrator
    # (short tool-calls, below the collapse threshold), medgemma the clinical expert.
    "med-agent-team-qwen": {  # standard: Qwen3.6-35B-A3B synth + medgemma-27b expert
        "synthesizer_model": "qwen3.6-35b-a3b-mlx",
        "expert_model": "medgemma-27b-text-it-mlx",
    },
    "med-agent-team-qwen-low": {  # low-resource: Qwen3-14B synth + medgemma-1.5 expert (default)
        "synthesizer_model": "qwen3-14b-mlx",
    },
}


def team_config_for(model_id: str) -> Dict[str, str]:
    """Map an advertised team model id to run_team model overrides. Unknown ids
    (which chartsearchai validation would have rejected) -> defaults (empty)."""
    return dict(TEAM_PRESETS.get(model_id, {}))


def _tool_definitions() -> List[Dict[str, Any]]:
    """OpenAI tool definitions the orchestrator may call. Descriptions carry the
    trigger keywords + ordering so a small model sequences kb_search then expert."""
    return [
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
    frequency_penalty: Optional[float] = None,
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
    if frequency_penalty is not None:
        payload["frequency_penalty"] = frequency_penalty

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
        msg = await _chat(client, model or llm_config.med_model, messages, temperature=0.1, max_tokens=800)
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
    """Post-process the synthesizer envelope JSON: (1) normalize a literal backslash-n in
    `answer` to a real newline — small models (e.g. qwen3-14b) copy the prompt's JSON \\n
    escaping verbatim and garble — and (2) reconcile inline [N] chart-record markers into
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
        if "\\n" in ans:
            env["answer"] = ans = ans.replace("\\n", "\n")
        inline = sorted({int(m) for m in re.findall(r"\[(\d+)\]", ans)})
        if inline:
            existing = [c for c in (env.get("citations") or []) if isinstance(c, int)]
            env["citations"] = sorted(set(existing) | set(inline))
    return json.dumps(env)


# Synthesis anti-degeneration: a small synthesizer (e.g. the gemma-4 26b-a4b MoE)
# can fall into token-level repetition loops ("AIDS AIDS AIDS...") on a long
# evidence prompt under greedy decoding. A modest temperature floor + frequency
# penalty on the synthesis call breaks the loop; the orchestrator's tool loop keeps
# the request temperature so its tool-calling stays deterministic.
_SYNTH_MIN_TEMPERATURE = 0.5
_SYNTH_FREQUENCY_PENALTY = 0.8


async def run_team(
    messages: List[Dict[str, Any]],
    *,
    response_format: Optional[Dict[str, Any]] = None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    orchestrator_model: Optional[str] = None,
    synthesizer_model: Optional[str] = None,
    expert_model: Optional[str] = None,
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

    # Read the active prompt variant ONCE per request (PROMPT_VARIANT is static
    # per instance); thread the expert text into the per-iteration expert call so
    # the tool loop does not re-read it from disk each turn.
    orchestrator_system = load_prompt("orchestrator")
    expert_system = load_prompt("medical_expert")
    synthesis_instruction = load_prompt("synthesis")

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
                    tools=_tool_definitions(), temperature=temperature, max_tokens=max_tokens,
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
                                kb_context=kb_context, model=expert_model)
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
            synth_temperature = max(temperature or 0.0, _SYNTH_MIN_TEMPERATURE)
            msg = await _chat(
                client, synth_model, synth_messages,
                response_format=response_format, temperature=synth_temperature,
                max_tokens=max_tokens, frequency_penalty=_SYNTH_FREQUENCY_PENALTY,
            )
            content = _normalize_envelope(_message_text(msg))
            if content:
                return content
            logger.warning("synthesis returned empty content; using fallback envelope")
        except Exception as e:
            logger.error("synthesis call failed: %s", e, exc_info=True)

    return _fallback_envelope(
        "I could not produce a complete answer for this turn. Please try again.")
