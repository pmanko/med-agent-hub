"""Render querystore records into the numbered chart text the hub's LLM prompts expect.

The hub owns this serializer because it owns context retrieval and prompt construction. ChartSearchAI
relays patient/profile requests and does not build a second chart snapshot. Validation fixtures use this
same renderer so judges see the exact numbered evidence ledger supplied to the model.

Input is the raw querystore REST representation: each record is a dict with ``resourceType``,
``resourceUuid``, ``date`` (ISO ``yyyy-MM-dd``, a deterministic sort/filter date — not necessarily a
clinical fact), ``clinicalDate`` (the temporally safe event date, or absent), ``dateKind``
(``clinical_event`` | ``administrative`` | ``unknown``), ``text`` (the labelled per-record projection),
and ``metadata`` (carrying ``obs_group_uuid`` / ``obs_group_concept_name`` for group members). The
``embedding`` is never present (querystore excludes it from the REST surface).
"""
from __future__ import annotations

import re
from typing import Any, Optional

# OpenMRS renders whole-number obs values with a trailing ".0" ("988.0"); trim it (value-lossless),
# but never a ".0" inside a code/version (ICD-10 "E11.0", "1.0.0") — same guard chartsearchai uses.
_TRAILING_ZERO = re.compile(r"(?<![\w.])(\d+)\.0(?![\w.])")

_OBS_GROUP_UUID = "obs_group_uuid"
_OBS_GROUP_NAME = "obs_group_concept_name"
_CLINICAL_EVENT = "clinical_event"


def render_chart(records: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    """Render querystore records to ``(chart_text, mappings)``.

    Each well-formed record becomes one line ``[N] (date) text (part of: <panel>) [dateKind]``
    (the group label is omitted when absent; the trailing ``[dateKind]`` marker is omitted for
    genuine clinical events). ``mappings[k]`` = ``{index, resourceType, resourceUuid, date, text}``
    for the grounding / citation layer (its ``text`` carries the full rendered body, marker
    included). Complete source adapters validate stable identities before calling this renderer;
    malformed inline/debug records remain omitted so numbering stays dense.

    A record always shows a date, even when ``dateKind`` is not ``clinical_event`` — never an
    empty date prefix. temporal.py's parsers carry a dateless record's date forward from whichever
    record precedes it (run-length compression for same-date obs groups); omitting the date here
    would make an administrative record silently inherit an unrelated neighbor's date instead of
    being excluded from clinical-date reasoning. The trailing marker is how that exclusion happens.
    """
    lines: list[str] = []
    mappings: list[dict[str, Any]] = []
    index = 0
    for rec in records:
        if not rec or not rec.get("resourceType") or not rec.get("resourceUuid"):
            continue
        index += 1
        date, is_clinical = _display_date(rec)
        body = _trim_zero(rec.get("text") or "")
        group = _group_label(rec.get("metadata") or {})
        date_prefix = f"({date}) " if date else ""
        # Nothing to qualify when there is no date to show at all.
        marker = "" if is_clinical or not date else f" [{rec.get('dateKind') or 'unknown'}]"
        # The [dateKind] marker is a temporal-safety hint the answer model reads on the chart
        # line; it must not enter the grounding source (mappings[k].text). The entailment
        # grounding layer compares a claim against this text, and a trailing marker makes it
        # reject otherwise-supported claims.
        grounding_text = f"{date_prefix}{body}{group}"
        lines.append(f"[{index}] {grounding_text}{marker}")
        mappings.append({
            "index": index,
            "resourceType": rec.get("resourceType"),
            "resourceUuid": rec.get("resourceUuid"),
            "date": date,
            "text": grounding_text,
        })
    return ("\n".join(lines) + "\n" if lines else ""), mappings


def _display_date(rec: dict[str, Any]) -> tuple[Optional[str], bool]:
    """The date shown on this record's chart line, and whether it is a genuine clinical event.

    ``clinicalDate`` is trustworthy whenever present, independent of ``dateKind`` — querystore can
    report a real clinical fact (e.g. a Condition's onset) alongside ``dateKind: administrative``
    because ``dateKind`` describes ``date`` (the sort/audit field), not ``clinicalDate``. Only when
    ``clinicalDate`` is absent does ``dateKind`` decide whether ``date`` may stand in for it: sources
    that never populate ``dateKind`` (inline charts, static knowledge) default to treating their
    ``date`` as clinical, preserving today's behavior for non-querystore records.
    """
    clinical_date = rec.get("clinicalDate")
    if clinical_date:
        return clinical_date, True
    date_kind = rec.get("dateKind")
    if date_kind is None or date_kind == _CLINICAL_EVENT:
        return rec.get("date"), True
    return rec.get("date"), False


def _trim_zero(text: str) -> str:
    return _TRAILING_ZERO.sub(r"\1", text)


def _group_label(metadata: dict[str, Any]) -> str:
    if not metadata.get(_OBS_GROUP_UUID):
        return ""
    name = str(metadata.get(_OBS_GROUP_NAME) or "").strip()
    return f" (part of: {name})" if name else ""
