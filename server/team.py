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
- The tool loop runs on its own message list under ORCHESTRATOR_SYSTEM, NOT under
  chartsearchai's envelope system prompt (which biases a small model). The
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
from typing import Any, Dict, List, Optional

import httpx

from . import kb
from .config import llm_config

logger = logging.getLogger(__name__)

# Small-model tool-calling degrades over long chains; keep the loop short.
MAX_TOOL_ITERATIONS = 3

# Loop-phase system prompt for the orchestrator. Tool-focused, envelope-free, and
# strongly suggests the kb_search -> medical_expert -> stop flow without hardcoding
# it (the code still works if the model skips a tool).
ORCHESTRATOR_SYSTEM = (
    "You are the coordinator of a small clinical team answering a clinician's "
    "question about ONE patient's chart. You do not write the final answer yourself "
    "— you decide which teammates to consult, then stop.\n\n"
    "You have two tools:\n"
    "- kb_search: look up EXTERNAL reference guidance (WHO / essential-medicines "
    "guidelines, dosing, thresholds, danger signs, normal ranges) that is NOT in the "
    "patient's chart.\n"
    "- medical_expert: have a clinical expert interpret THIS patient's chart against "
    "the question. The expert automatically receives whatever kb_search returned this "
    "turn — you do NOT need to copy any facts into your question to it.\n\n"
    "How to work:\n"
    "- Before each tool call, state in ONE sentence what you still need and why.\n"
    "- DEFAULT pattern: if the question involves a guideline, a drug/dose, a threshold, "
    "a danger sign, an immunization schedule, a normal/reference range, or whether "
    "something is current/recommended, FIRST call kb_search to pull the relevant "
    "reference facts, THEN call medical_expert to interpret the chart against them, "
    "THEN stop.\n"
    "- SKIP the tools only when the chart plainly and fully answers the question with "
    "no outside guidance needed (e.g. \"what medications is she on?\", \"how many did "
    "you list?\"). Then just say you are done.\n"
    "- Call at most one tool per step. Once you have what you need, stop — do not keep "
    "calling tools.\n\n"
    "Worked example (a question like the demo's):\n"
    "Question: \"Is this patient's antiretroviral regimen still recommended?\"\n"
    "Step 1 — Thought: I need the current WHO guidance on this regimen, which is not in "
    "the chart.\n"
    "         Action: kb_search({\"query\": \"WHO recommended first-line ART; stavudine "
    "d4T phase-out\"})\n"
    "         Observation: reference snippets on preferred dolutegravir-based regimens "
    "and the stavudine phase-out are returned.\n"
    "Step 2 — Thought: now interpret the chart's regimen against that guidance.\n"
    "         Action: medical_expert({\"query\": \"Given the chart's ART regimen, is it "
    "still WHO-recommended, and if not what is the concern?\"})\n"
    "         Observation: the expert reasons over the chart plus the retrieved "
    "guidance.\n"
    "Step 3 — Thought: I have enough. Done.\n\n"
    "Do not invent guideline facts, doses, or chart values. If kb_search returns "
    "nothing relevant, say so rather than guessing."
)

# Clinical-agent (medical_expert) system prompt. The expert now reasons GIVEN any
# KB reference snippets the code threads into its user message (KB block first).
MEDICAL_EXPERT_SYSTEM = (
    "You are a clinical reasoning assistant. You are given a patient chart excerpt, "
    "optionally some knowledge-base reference snippets (external guideline/dosing/"
    "threshold facts, NOT chart data), and a focused question. Reason using the chart "
    "together with any provided reference snippets, and give concise, clinically-"
    "grounded reasoning.\n\n"
    "State only what the chart supports plus accepted medical knowledge and the "
    "provided reference snippets. For any guideline, dose, threshold, danger sign, "
    "schedule, or \"is this current / recommended\" claim, rely on the provided "
    "reference snippets; if neither the chart nor the snippets support an answer, say "
    "so explicitly. Do not invent values, doses, or thresholds. When you use a "
    "reference snippet, name its source in prose (e.g. \"per WHO IMCI\"). Plain text, "
    "no preamble."
)

# Final synthesis instruction. Carves the KB/expert facts out of the integer
# citation channel (chart records only) while allowing them as inline prose.
SYNTHESIS_INSTRUCTION = (
    "You are now writing the final answer as the chart-answer JSON object "
    "{answer, citations, blocks}. Use the patient chart above AND the gathered "
    "evidence block below (knowledge-base reference snippets and the clinical "
    "expert's notes).\n\n"
    "CITATIONS — read carefully:\n"
    "- The integer indices in `citations` and the `[N]` markers are RECORD INDICES "
    "from the numbered patient chart ONLY. A claim gets an integer ONLY if you can "
    "point to the numbered chart record that states it.\n"
    "- The chart-only rule (\"use only the records; never add information not in the "
    "records\") still governs PATIENT facts. Labeled knowledge-base reference snippets "
    "are an ALLOWED EXCEPTION: you MAY state them as general medical guidance, but "
    "attribute them inline in prose (e.g. \"per WHO IMCI\", \"per WHO HIV guidelines\") "
    "and NEVER give them a bracket number or put them in `citations`.\n"
    "- Knowledge-base facts, the medical expert's notes, and your own medical "
    "knowledge are ALL NOT chart records — attribute them inline in prose and NEVER "
    "place them in `citations`. Only a claim verifiable against a numbered chart "
    "record gets an integer.\n"
    "- Never invent a source name, URL, guideline title, dose, threshold, or "
    "chart-record index. If no chart record and no reference snippet supports the "
    "answer, say the information is not available rather than guessing.\n\n"
    "Worked example (mixed): \"She is on a stavudine (d4T)-based regimen [4]. Per WHO "
    "HIV guidelines, stavudine is no longer recommended because of frequent, often "
    "irreversible toxicity, and tenofovir or zidovudine are the preferred nucleoside "
    "backbones.\" -> citations: [4]  (the chart record for the regimen is cited; the "
    "WHO guidance is attributed inline with NO integer).\n\n"
    "Emit a `table` block when the answer lists/enumerates multiple items; otherwise "
    "leave `blocks` empty. Keep the answer concise."
)

# Prefix that marks a real (non-abstain) kb_search observation.
_KB_BLOCK_HEADER = "Knowledge-base reference snippets"


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


async def _run_medical_expert(
    client: httpx.AsyncClient,
    query: str,
    chart_context: str,
    kb_context: str = "",
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
        {"role": "system", "content": MEDICAL_EXPERT_SYSTEM},
        {"role": "user", "content": user},
    ]
    try:
        msg = await _chat(client, llm_config.med_model, messages, temperature=0.1, max_tokens=800)
        return (msg.get("content") or "").strip() or "(no expert response)"
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


async def run_team(
    messages: List[Dict[str, Any]],
    *,
    response_format: Optional[Dict[str, Any]] = None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    orchestrator_model: Optional[str] = None,
) -> str:
    """
    Run the team for one chartsearchai turn.

    `messages` is the full chartsearchai array ([system, user(chart), ...turns,
    user(question)]); `response_format` is the chart_answer envelope schema.
    Returns the envelope as a JSON string (the bridge puts it in
    choices[0].message.content). Always returns a schema-valid envelope.
    """
    model = orchestrator_model or llm_config.orchestrator_model
    chart = _chart_context(messages)

    # The tool loop runs under the orchestrator's OWN system prompt — not
    # chartsearchai's envelope prompt, which biases a small model toward answering
    # immediately. The original `messages` is left untouched for the synthesis prefix.
    loop_messages: List[Dict[str, Any]] = [
        {"role": "system", "content": ORCHESTRATOR_SYSTEM}
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
                                client, args.get("query", ""), chart, kb_context=kb_context)
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
        synth_user = SYNTHESIS_INSTRUCTION + "\n\n" + gathered if gathered else SYNTHESIS_INSTRUCTION
        synth_messages = list(messages) + [{"role": "user", "content": synth_user}]
        try:
            msg = await _chat(
                client, model, synth_messages,
                response_format=response_format, temperature=temperature, max_tokens=max_tokens,
            )
            content = (msg.get("content") or "").strip()
            if content:
                return content
            logger.warning("synthesis returned empty content; using fallback envelope")
        except Exception as e:
            logger.error("synthesis call failed: %s", e, exc_info=True)

    return _fallback_envelope(
        "I could not produce a complete answer for this turn. Please try again.")
