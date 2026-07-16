"""Render querystore records into the numbered chart text the hub's LLM prompts expect.

The hub owns this serializer because it owns context retrieval and prompt construction. ChartSearchAI
relays patient/profile requests and does not build a second chart snapshot. Validation fixtures use this
same renderer so judges see the exact numbered evidence ledger supplied to the model.

Input is the raw querystore REST representation: each record is a dict with ``resourceType``,
``resourceUuid``, ``date`` (ISO ``yyyy-MM-dd``), ``text`` (the labelled per-record projection), and
``metadata`` (carrying ``obs_group_uuid`` / ``obs_group_concept_name`` for group members). The
``embedding`` is never present (querystore excludes it from the REST surface).
"""
from __future__ import annotations

import re
from typing import Any

# OpenMRS renders whole-number obs values with a trailing ".0" ("988.0"); trim it (value-lossless),
# but never a ".0" inside a code/version (ICD-10 "E11.0", "1.0.0") — same guard chartsearchai uses.
_TRAILING_ZERO = re.compile(r"(?<![\w.])(\d+)\.0(?![\w.])")

_OBS_GROUP_UUID = "obs_group_uuid"
_OBS_GROUP_NAME = "obs_group_concept_name"


def render_chart(records: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    """Render querystore records to ``(chart_text, mappings)``.

    Each well-formed record becomes one line ``[N] (date) text (part of: <panel>)`` (the date and group
    label are omitted when absent). ``mappings[k]`` = ``{index, resourceType, resourceUuid, date, text}``
    for the grounding / citation layer (its ``text`` carries the full rendered body). Complete source
    adapters validate stable identities before calling this renderer; malformed inline/debug records
    remain omitted so numbering stays dense.
    """
    lines: list[str] = []
    mappings: list[dict[str, Any]] = []
    index = 0
    for rec in records:
        if not rec or not rec.get("resourceType") or not rec.get("resourceUuid"):
            continue
        index += 1
        date = rec.get("date")
        body = _trim_zero(rec.get("text") or "")
        group = _group_label(rec.get("metadata") or {})
        date_prefix = f"({date}) " if date else ""
        rendered = f"{date_prefix}{body}{group}"
        lines.append(f"[{index}] {rendered}")
        mappings.append({
            "index": index,
            "resourceType": rec.get("resourceType"),
            "resourceUuid": rec.get("resourceUuid"),
            "date": date,
            "text": rendered,
        })
    return ("\n".join(lines) + "\n" if lines else ""), mappings


def _trim_zero(text: str) -> str:
    return _TRAILING_ZERO.sub(r"\1", text)


def _group_label(metadata: dict[str, Any]) -> str:
    if not metadata.get(_OBS_GROUP_UUID):
        return ""
    name = str(metadata.get(_OBS_GROUP_NAME) or "").strip()
    return f" (part of: {name})" if name else ""
