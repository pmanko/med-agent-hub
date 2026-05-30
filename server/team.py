"""
In-process "Med Agent Team": a ReAct-style orchestrator over typed tools,
called directly by the OpenAI-compat bridge.

This replaces the A2A executor / multi-process subagent topology for the
in-house team. The orchestrator (one local model, dual role) runs a short
tool-calling loop over typed tools — for now a single `medical_expert` call —
then makes ONE final synthesis call bound to chartsearchai's response_format,
producing the strict {answer, citations, blocks} envelope.

Design notes:
- Tool-selection turns run PLAIN (tools, no response_format); only the final
  synthesis call is constrained to the envelope. Mixing tool-calling and
  constrained JSON in one turn is unreliable on small (4-8B) local models.
- chartsearchai pins `[system, user(chart)]` byte-identical across turns for
  LM Studio's prompt cache; we preserve that prefix and append the orchestrator's
  tool turns at the end, after the question.
- Every path returns a schema-valid envelope: a failed/slow tool is skipped and
  the turn still synthesizes; a hard failure returns a minimal fallback envelope.
"""

import json
import logging
from typing import Any, Dict, List, Optional

import httpx

from .config import llm_config

logger = logging.getLogger(__name__)

# Small-model tool-calling degrades over long chains; keep the loop short.
MAX_TOOL_ITERATIONS = 3

MEDICAL_EXPERT_SYSTEM = (
    "You are a clinical reasoning assistant. Given a patient chart excerpt and a "
    "focused question, give concise, clinically-grounded reasoning. State only what "
    "the chart supports plus accepted medical knowledge; if the chart lacks the "
    "information, say so. Do not invent values. Plain text, no preamble."
)

# Light, provisional guidance appended after the question (keeps the
# [system, user(chart)] prefix byte-identical for cache reuse). The path it
# suggests is a hypothesis to be measured, not a hard pipeline.
SYNTHESIS_INSTRUCTION = (
    "Using the patient chart above and any expert notes gathered, answer the "
    "question as the chart-answer JSON object. Cite chart records by their integer "
    "index in `citations` and `[N]` markers; cite ONLY claims the chart supports. "
    "Emit a table block when the answer is naturally tabular; otherwise leave "
    "`blocks` empty."
)


def _tool_definitions() -> List[Dict[str, Any]]:
    """OpenAI tool definitions the orchestrator may call. One tool for v1."""
    return [
        {
            "type": "function",
            "function": {
                "name": "medical_expert",
                "description": (
                    "Consult a medical expert model for clinical reasoning about the "
                    "patient's chart and the question. Use when clinical interpretation "
                    "beyond plain chart lookup would help."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "A focused clinical question for the expert.",
                        }
                    },
                    "required": ["query"],
                },
            },
        }
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


async def _run_medical_expert(client: httpx.AsyncClient, query: str, chart_context: str) -> str:
    """Typed medical-expert tool: a single medgemma call, free text (no schema)."""
    messages = [
        {"role": "system", "content": MEDICAL_EXPERT_SYSTEM},
        {"role": "user", "content": f"Patient chart:\n{chart_context}\n\nQuestion: {query}"},
    ]
    try:
        msg = await _chat(client, llm_config.med_model, messages, temperature=0.1, max_tokens=800)
        return (msg.get("content") or "").strip() or "(no expert response)"
    except Exception as e:  # tool failure must not abort the turn
        logger.warning("medical_expert tool failed: %s", e)
        return "(medical expert unavailable for this turn)"


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
    working: List[Dict[str, Any]] = list(messages)

    async with httpx.AsyncClient() as client:
        # --- tool loop (plain: tools, no response_format) -------------------
        try:
            for _ in range(MAX_TOOL_ITERATIONS):
                msg = await _chat(
                    client, model, working,
                    tools=_tool_definitions(), temperature=temperature, max_tokens=max_tokens,
                )
                tool_calls = msg.get("tool_calls")
                if not tool_calls:
                    break  # orchestrator has gathered enough; proceed to synthesis
                working.append(msg)
                for tc in tool_calls:
                    name = tc.get("function", {}).get("name")
                    try:
                        args = json.loads(tc["function"]["arguments"] or "{}")
                    except (json.JSONDecodeError, KeyError):
                        args = {}
                    if name == "medical_expert":
                        observation = await _run_medical_expert(
                            client, args.get("query", ""), chart)
                    else:
                        observation = f"(unknown tool: {name})"
                    working.append({
                        "role": "tool",
                        "tool_call_id": tc.get("id"),
                        "content": observation,
                    })
        except Exception as e:
            logger.warning("orchestrator tool loop failed, proceeding to synthesis: %s", e)

        # --- final synthesis (constrained to the envelope) ------------------
        synth_messages = working + [{"role": "user", "content": SYNTHESIS_INSTRUCTION}]
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
