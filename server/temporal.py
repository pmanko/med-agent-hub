"""Deterministic temporal grounding for the synthesis evidence (P0 anchor + P1 series).

The synth fabricates dates/trends because it eyeballs the chart. Here we parse the serialized
chart text server-side and emit a compact block the synth REPORTS from instead of deriving:
  - a reference-date ANCHOR line (the simulated "now") so "recent"/"most recent" are defined;
  - per-concept numeric SERIES sorted oldest->newest, with most-recent value+date, range, the
    data window, and a trend direction ONLY when >=2 points exist (the no-trend-from-one-point guard).
All relative to the resolved anchor. Pure functions; no LLM, no I/O. Grounded in the real chart
format (chartsearchai PatientChartSerializer): `[N] (YYYY-MM-DD) <Class> — <concept>: <value> <unit>`.
"""
from __future__ import annotations

import calendar
import datetime as _dt
import json
import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

# A record line is `[N] <rest>`; the date `(YYYY-MM-DD)` is present only on the FIRST line of a
# same-date run (the serializer run-length-compresses it — e.g. Ellcky keeps 8/276 dates), so it is
# parsed as an OPTIONAL prefix and carried forward to the dateless follow-ons.
_REC_RE = re.compile(r"^\[(\d+)\]\s*(.*)$")
_DATE_PREFIX_RE = re.compile(r"^\((\d{4}-\d{2}-\d{2})\)\s*(.*)$")
_DATE_RE = re.compile(r"\((\d{4}-\d{2}-\d{2})\)")
_ISO_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_ISO_TOKEN_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
_UNICODE_HYPHEN = "\u2010\u2011\u2012\u2013\u2014\u2015\u2212\ufe58\ufe63\uff0d"
_NON_ASCII_ISO_TOKEN_RE = re.compile(
    rf"\b\d{{4}}[{_UNICODE_HYPHEN}]\d{{2}}[{_UNICODE_HYPHEN}]\d{{2}}\b"
)
_TRUNCATED_ISO_TOKEN_RE = re.compile(r"\b\d{4}-\d{1,2}\b(?!-)")
_DOUBLE_SEPARATOR_DATE_RE = re.compile(
    r"\b\d{4}\s*[-_/]\s*\d{1,2}\s*[-_/]{2,}\s*\d{1,4}\b"
)
_BRACKETED_DATE_RE = re.compile(r"\b\d{4}\s*[-_/]\s*\d{1,2}\s*[-_/]\s*\[[^\]\s]{1,8}\]")
_DATE_LIKE_RE = re.compile(
    r"(?<![\w])[\d\u0660-\u0669\u0966-\u096f]{1,4}\s*[-_/]\s*"
    r"[\d\u0660-\u0669\u0966-\u096f]{1,2}\s*[-_/]\s*"
    r"[\d\u0660-\u0669\u0966-\u096f]{1,4}(?![\w])"
)
_DATE_ID_TOKEN_RE = re.compile(r"\bD\d{4}_\d{2}_\d{2}\b")
_LEADING_NUM_RE = re.compile(r"^([+-]?\d+(?:\.\d+)?)\s*(.*)$")
_NUMBER_TOKEN_RE = re.compile(r"(?<![\[\d.-])([+-]?\d+(?:\.\d+)?)(?![\]\d.-])")
_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_UPCOMING_RE = re.compile(
    r"\b(upcoming|future|next|scheduled|appointment|follow-?up|return visit)\b", re.I
)
_FUTURE_EVENT_PATTERN = r"(?:appointment|visit|follow-?up|return[- ]visit)"
_FUTURE_DATE_PREFIX_RE = re.compile(
    rf"(?:\b(?:(?:next|upcoming|future)\s+)+{_FUTURE_EVENT_PATTERN}(?:\s+date)?\b"
    rf"(?:\s+(?:is|will be|on|for))?|"
    rf"\b{_FUTURE_EVENT_PATTERN}(?:\s+date)?\b\s+is\s+scheduled\s+for|"
    rf"(?:\bhas\s+(?:a\s+)?|^\s*(?:(?:the|a)\s+)?)"
    rf"scheduled\s+{_FUTURE_EVENT_PATTERN}(?:\s+date)?\b"
    rf"(?:\s+(?:is|will be|on|for))?)\s*$",
    re.I,
)
_FUTURE_DATE_SUFFIX_RE = re.compile(
    rf"^\s*(?:(?:is|will be)\s+)?(?:the\s+)?"
    rf"(?:(?:next|upcoming|future)\s+)+{_FUTURE_EVENT_PATTERN}\b",
    re.I,
)
_NO_UPCOMING_RE = re.compile(
    r"\b(no|none|not|without|does not|doesn't|isn't|not documented|no documented)"
    r"\b.{0,90}\b(upcoming|future|next|appointment|follow-?up|return visit)\b",
    re.I,
)
_CITATION_SEQUENCE_RE = re.compile(r"(?:\s*,?\s*\[\d+\])+", re.I)
_SAFE_NO_UPCOMING_CLAIM_RE = re.compile(
    r"(?:(?:the )?(?:record|chart) (?:does not|doesn't) (?:show|document) any|no) "
    r"(?:upcoming|future) (?:appointments?|visits?)"
    r"(?: (?:are )?(?:documented|shown))?"
    r"(?:; all listed (?:return[- ]visit|appointment|follow-?up) dates are "
    r"(?:in the )?(?:past|historical))?[.!]?",
    re.I,
)
_LAST_VISIT_RE = re.compile(
    r"\b(last|most recent|latest)\b.{0,30}\b(visit|encounter)\b", re.I
)
_VISIT_ASSERTION_NEGATION_RE = re.compile(
    r"^\s*(?:(?:is|was|remains)\s+)?(?:not documented|not available|unknown|none)\b",
    re.I,
)
_VISIT_DATE_RELATION_NEGATION_RE = re.compile(
    r"^\s*(?:\[\d+\]\s*)?(?:(?:is|was)\s+not(?:\s+(?:on|at|dated|the\s+date\s+of\s+the))?|"
    r"did\s+not\s+occur(?:\s+on)?)\s*$",
    re.I,
)
_TREND_RE = re.compile(
    r"\b(trend|changed|change|increas(?:e|ed|ing)|decreas(?:e|ed|ing)|declin(?:e|ed|ing)|"
    r"improv(?:e|ed|ing)|worsen(?:ed|ing)?|gain(?:ed|ing)?|growing|growth|lost|losing|rising|falling)\b",
    re.I,
)
_UP_WORD_RE = re.compile(
    r"\b(increas(?:e|ed|ing)|gain(?:ed|ing)?|growing|rose|rising|up|higher|improv(?:e|ed|ing))\b",
    re.I,
)
_DOWN_WORD_RE = re.compile(
    r"\b(decreas(?:e|ed|ing)|declin(?:e|ed|ing)|lost|losing|fell|falling|down|lower|worsen(?:ed|ing)?)\b",
    re.I,
)
_NEGATED_UP_RE = re.compile(
    r"\b(not|no|isn't|is not|wasn't|was not)\b.{0,20}\b(gain(?:ed|ing)?|growing|increas(?:e|ed|ing))\b",
    re.I,
)
_WINDOW_RE = re.compile(
    r"\b(past|last)\s+(year|12\s+months?|6\s+months?|six\s+months?)\b", re.I
)
_CONCEPT_STOP_TOKENS = {
    "and",
    "the",
    "for",
    "with",
    "from",
    "into",
    "past",
    "last",
    "year",
    "years",
    "month",
    "months",
    "date",
    "dated",
    "time",
    "times",
    "number",
    "count",
    "counts",
    "value",
    "values",
    "measurement",
    "measurements",
    "current",
    "recent",
    "most",
    "latest",
}

# Record classes whose date is NOT a clinical visit: an administrative enrollment (a Program) carries
# its enrollment date, which can post-date the last actual visit (Aloice's TB Program "enrolled"
# 2026-05-20 when the last clinical visit was 2026-01-07). These are labeled, never reported as a visit.
_ADMIN_CLASSES = {"Program"}
_ENCOUNTER_CLASSES = {"encounter", "visit"}


def _parse_iso_date(value: Optional[str]) -> Optional[_dt.date]:
    if not value or not _ISO_RE.fullmatch(str(value)):
        return None
    try:
        return _dt.date.fromisoformat(str(value))
    except ValueError:
        return None


def _date_id(value: Optional[str]) -> Optional[str]:
    return (
        f"D{value.replace('-', '_')}" if value and _parse_iso_date(str(value)) else None
    )


def _shift_months(date: _dt.date, months: int) -> _dt.date:
    month_index = date.year * 12 + (date.month - 1) + months
    year, month0 = divmod(month_index, 12)
    month = month0 + 1
    day = min(date.day, calendar.monthrange(year, month)[1])
    return _dt.date(year, month, day)


def _add_date_role(
    roles_by_date: Dict[str, set], value: Optional[str], role: str
) -> None:
    if value and _parse_iso_date(str(value)):
        roles_by_date.setdefault(str(value), set()).add(role)


def _date_ledger_entry(
    value: str, roles: set, reference_date: Optional[str]
) -> Dict[str, Any]:
    date = _parse_iso_date(value)
    ref = _parse_iso_date(reference_date)
    if not date:
        return {"date_id": _date_id(value), "iso": value}
    return {
        "date_id": _date_id(value),
        "iso": value,
        "year": date.year,
        "month": date.month,
        "month_name": calendar.month_name[date.month],
        "day": date.day,
        "relation_to_reference": _relation_to_anchor(value, reference_date),
        "days_from_reference": (date - ref).days if ref else None,
    }


def _date_ledger(
    roles_by_date: Dict[str, set], reference_date: Optional[str]
) -> List[Dict[str, Any]]:
    return [
        _date_ledger_entry(value, roles, reference_date)
        for value, roles in sorted(roles_by_date.items(), reverse=True)
    ]


def _record_class(body: str) -> str:
    """The leading class token of a record body: 'Finding — Weight: 41' -> 'Finding';
    'Program: TB Program...' -> 'Program'; 'Drug order: Lamivudine' -> 'Drug order'."""
    head = body.split(" — ", 1)[0]
    head = head.split(":", 1)[0]
    return head.strip()


def parse_events(chart: str) -> List[Dict[str, str]]:
    """One {date, cls, body} per record line, carrying the run-leader's date forward to dateless
    follow-ons (run-length compression). cls types the event for the timeline (Finding / Test /
    Assessment / Drug order / Program / ...)."""
    out: List[Dict[str, str]] = []
    last_date: Optional[str] = None
    for line in (chart or "").splitlines():
        m = _REC_RE.match(line.strip())
        if not m:
            continue
        body = m.group(2)
        dm = _DATE_PREFIX_RE.match(body)
        if dm:
            last_date = dm.group(1)
            body = dm.group(2)
        if last_date is None:
            continue
        out.append(
            {
                "index": int(m.group(1)),
                "date": last_date,
                "cls": _record_class(body),
                "body": body,
            }
        )
    return out


def resolve_anchor(
    anchor: Optional[str], chart: str, *, timezone_name: Optional[str] = None
) -> Optional[str]:
    """Resolve the reference 'now' (ISO date).
       - None / 'latest_record' -> the max date of a CLINICAL record (administrative enrollments are
         excluded — a Program can post-date the last visit and must not define 'now');
       - 'wall_clock' -> today's date (real clock);
       - an explicit 'YYYY-MM-DD' -> itself.
    Returns None when latest_record is requested but the chart has no dates."""
    mode = (anchor or "latest_record").strip()
    if mode == "wall_clock":
        if timezone_name:
            return _dt.datetime.now(ZoneInfo(timezone_name)).date().isoformat()
        return _dt.date.today().isoformat()
    if _ISO_RE.fullmatch(mode):
        return mode
    clinical = [
        e["date"] for e in parse_events(chart) if e["cls"] not in _ADMIN_CLASSES
    ]
    if clinical:
        return max(clinical)
    dates = _DATE_RE.findall(chart or "")
    return max(dates) if dates else None


def _clean_concept(raw: str) -> str:
    """'Weight (kg), WT)' -> 'Weight'; 'CD4 count' -> 'CD4 count'; keeps 'CD4%'."""
    c = raw.strip()
    cut = c.find(" (")  # drop unit/synonym noise after the first ' ('
    if cut > 0:
        c = c[:cut]
    return c.rstrip(" ),").strip()


def parse_dated_observations(chart: str) -> List[Dict[str, Any]]:
    """Parse `[N] [(date)] <Class> — <concept>: <value> <unit>` lines into numeric observations:
    {index, date, concept, value, unit, raw}. The `(date)` appears only on the FIRST line of a
    same-date run (run-length compression) and is carried forward to the dateless follow-ons.
    Only rows whose value begins with a number (true numeric series) are kept — drug orders /
    Yes-No assessments are skipped. De-duplicated per (concept, date), keeping the first.
    """
    out: List[Dict[str, Any]] = []
    seen: set = set()
    last_date: Optional[str] = None
    for line in (chart or "").splitlines():
        m = _REC_RE.match(line.strip())
        if not m:
            continue
        index, rest = int(m.group(1)), m.group(2)
        dm = _DATE_PREFIX_RE.match(rest)
        if (
            dm
        ):  # run leader carries the date; same-date follow-ons drop it -> carry forward
            last_date = dm.group(1)
            rest = dm.group(2)
        date = last_date
        if date is None:
            continue  # record(s) before any dated line — no date to attribute
        if " — " in rest:
            rest = rest.split(" — ", 1)[1]
        if ":" not in rest:
            continue
        head, _, tail = rest.partition(":")
        value_text = tail.strip()
        if _ISO_RE.match(value_text):
            continue  # a date-valued obs (e.g. "Return visit date: 2006-05-18"), not a measurement
        nm = _LEADING_NUM_RE.match(value_text)
        if not nm:
            continue  # not a numeric obs
        concept = _clean_concept(head)
        if not concept:
            continue
        key = (concept, date)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "index": index,
                "date": date,
                "concept": concept,
                "value": float(nm.group(1)),
                "unit": nm.group(2).strip(),
                "raw": line.strip(),
            }
        )
    return out


def parse_dated_date_observations(chart: str) -> List[Dict[str, Any]]:
    """Parse date-valued observations such as `Return visit date: 2006-05-18`.

    These are not numeric series, but they are exactly the records models need for appointment
    and follow-up questions. The row's record date is the source date; `value_date` is the
    date documented by the observation."""
    out: List[Dict[str, Any]] = []
    seen: set = set()
    last_date: Optional[str] = None
    for line in (chart or "").splitlines():
        m = _REC_RE.match(line.strip())
        if not m:
            continue
        index, rest = int(m.group(1)), m.group(2)
        dm = _DATE_PREFIX_RE.match(rest)
        if dm:
            last_date = dm.group(1)
            rest = dm.group(2)
        source_date = last_date
        if source_date is None:
            continue
        cls = _record_class(rest)
        if " — " in rest:
            rest = rest.split(" — ", 1)[1]
        if ":" not in rest:
            continue
        head, _, tail = rest.partition(":")
        value_text = tail.strip()
        dm_value = _ISO_TOKEN_RE.search(value_text)
        if not dm_value:
            continue
        concept = _clean_concept(head)
        if not concept:
            continue
        value_date = dm_value.group(0)
        key = (concept, source_date, value_date, index)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "index": index,
                "date": source_date,
                "source_date": source_date,
                "value_date": value_date,
                "concept": concept,
                "cls": cls,
                "raw": line.strip(),
            }
        )
    return out


def _relation_to_anchor(date: str, anchor: Optional[str]) -> str:
    if not anchor:
        return "unknown"
    if date < anchor:
        return "past"
    if date > anchor:
        return "future"
    return "today"


def _point(o: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "index": o.get("index"),
        "date": o.get("date"),
        "effective_date_id": _date_id(o.get("date")),
        "value": o.get("value"),
        "unit": o.get("unit") or "",
    }


def _series_direction(first: float, last: float) -> str:
    if last > first:
        return "up"
    if last < first:
        return "down"
    return "flat"


def _summarize_events(
    events: List[Dict[str, Any]], date: str, *, include_summaries: bool = True
) -> Dict[str, Any]:
    same = [e for e in events if e.get("date") == date]
    out = {
        "date": date,
        "date_id": _date_id(date),
        "record_count": len(same),
        "indices": [e.get("index") for e in same if e.get("index") is not None],
        "classes": sorted({e.get("cls") for e in same if e.get("cls")}),
    }
    if include_summaries:
        out["summaries"] = [e.get("body") for e in same[:8] if e.get("body")]
    return out


def _clinical_encounter_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return only records that explicitly represent an encounter or visit."""
    return [
        event
        for event in events
        if str(event.get("cls") or "").strip().lower() in _ENCOUNTER_CLASSES
    ]


def build_temporal_facts(
    chart: str, anchor: Optional[str], *, anchor_mode: Optional[str] = None
) -> Dict[str, Any]:
    """Build the deterministic JSON sidecar the model can parse for temporal inference."""
    reference_date = (
        resolve_anchor(anchor, chart)
        if (anchor and not _ISO_RE.fullmatch(anchor))
        else anchor
    )
    if reference_date is None:
        reference_date = resolve_anchor(None, chart)
    mode = anchor_mode or anchor or "latest_record"

    events = parse_events(chart)
    encounter_events = _clinical_encounter_events(events)
    encounter_dates = sorted({e["date"] for e in encounter_events}, reverse=True)
    clinical_events = [e for e in events if e["cls"] not in _ADMIN_CLASSES]
    clinical_dates = sorted(
        {e["date"] for e in clinical_events}, reverse=True
    )
    admin_dates = sorted(
        {e["date"] for e in events if e["cls"] in _ADMIN_CLASSES}, reverse=True
    )
    date_roles: Dict[str, set] = {}
    _add_date_role(date_roles, reference_date, "reference_date")
    ref_date = _parse_iso_date(reference_date)
    if ref_date:
        _add_date_role(
            date_roles,
            _shift_months(ref_date, -6).isoformat(),
            "past_6_months_window_start",
        )
        _add_date_role(
            date_roles,
            _shift_months(ref_date, -12).isoformat(),
            "past_12_months_window_start",
        )
    for e in events:
        _add_date_role(
            date_roles,
            e.get("date"),
            "admin_record_date"
            if e.get("cls") in _ADMIN_CLASSES
            else "clinical_record_date",
        )

    by_concept: Dict[str, List[Dict[str, Any]]] = {}
    observations = parse_dated_observations(chart)
    for o in observations:
        _add_date_role(date_roles, o.get("date"), "numeric_measurement_date")
        by_concept.setdefault(o["concept"], []).append(o)

    numeric_series: List[Dict[str, Any]] = []
    for concept, points in sorted(by_concept.items()):
        series = sorted(points, key=lambda o: o["date"])
        most_recent = series[-1]
        values = [o["value"] for o in series]
        trend_supported = len(series) >= 2
        direction = (
            _series_direction(series[0]["value"], most_recent["value"])
            if trend_supported
            else "none"
        )
        numeric_series.append(
            {
                "concept": concept,
                "points": [_point(o) for o in series],
                "n_points": len(series),
                "most_recent": _point(most_recent),
                "window": {
                    "start": series[0]["date"],
                    "start_date_id": _date_id(series[0]["date"]),
                    "end": most_recent["date"],
                    "end_date_id": _date_id(most_recent["date"]),
                },
                "range": {"min": min(values), "max": max(values)},
                "trend_supported": trend_supported,
                "direction": direction,
                "delta": (most_recent["value"] - series[0]["value"])
                if trend_supported
                else None,
            }
        )

    date_obs = parse_dated_date_observations(chart)
    for o in date_obs:
        _add_date_role(date_roles, o.get("source_date"), "date_observation_source_date")
        _add_date_role(date_roles, o.get("value_date"), "date_observation_value_date")
    return_visit_dates = [
        {
            "index": o.get("index"),
            "date": o.get("date"),
            "date_id": _date_id(o.get("date")),
            "source_date": o.get("source_date"),
            "source_date_id": _date_id(o.get("source_date")),
            "value_date": o.get("value_date"),
            "value_date_id": _date_id(o.get("value_date")),
            "concept": o.get("concept"),
            "cls": o.get("cls"),
            "relation_to_reference": _relation_to_anchor(
                o["value_date"], reference_date
            ),
        }
        for o in date_obs
        if "return visit" in o.get("concept", "").lower()
    ]

    appointment_terms = (
        "appointment",
        "return visit",
        "follow-up",
        "follow up",
        "scheduled visit",
    )
    all_candidates = [
        {
            "index": o.get("index"),
            "source_date": o.get("source_date"),
            "source_date_id": _date_id(o.get("source_date")),
            "date": o.get("value_date"),
            "date_id": _date_id(o.get("value_date")),
            "concept": o.get("concept"),
            "source_type": "date_observation",
            "status": "candidate",
            "relation_to_reference": _relation_to_anchor(
                o.get("value_date", ""), reference_date
            ),
        }
        for o in date_obs
        if any(term in o.get("concept", "").lower() for term in appointment_terms)
    ]
    candidates_by_relation = {
        rel: [c for c in all_candidates if c["relation_to_reference"] == rel]
        for rel in ("past", "today", "future", "unknown")
    }
    candidates_by_relation["all"] = all_candidates
    ledger = _date_ledger(date_roles, reference_date)

    return {
        "date_output_contract": {
            "copy_dates_verbatim_from_date_ledger": True,
            "date_id_format": "DYYYY_MM_DD",
            "do_not_reformat_dates": True,
        },
        "date_ledger": ledger,
        "anchor_mode": mode,
        "reference_date": reference_date,
        "reference_date_id": _date_id(reference_date),
        "last_clinical_encounter": (
            _summarize_events(encounter_events, encounter_dates[0])
            if encounter_dates
            else None
        ),
        "latest_clinical_activity": (
            _summarize_events(clinical_events, clinical_dates[0])
            if clinical_dates
            else None
        ),
        "clinical_dates": [
            _summarize_events(events, d, include_summaries=False)
            for d in clinical_dates
        ],
        "admin_dates": [
            _summarize_events(
                [e for e in events if e["cls"] in _ADMIN_CLASSES],
                d,
                include_summaries=False,
            )
            for d in admin_dates
        ],
        "numeric_series": numeric_series,
        "return_visit_dates": return_visit_dates,
        "appointment_candidates": candidates_by_relation,
    }


def _compact_event_summary_for_prompt(
    summary: Optional[Dict[str, Any]], *, include_classes: bool = False
) -> Optional[Dict[str, Any]]:
    """Keep date/order/citation affordances while avoiding repeated class lists in the prompt."""
    if not isinstance(summary, dict):
        return summary
    out: Dict[str, Any] = {
        "date": summary.get("date"),
        "date_id": summary.get("date_id"),
        "record_count": summary.get("record_count"),
    }
    indices = summary.get("indices")
    if indices:
        out["indices"] = indices
    if include_classes:
        classes = summary.get("classes") or []
        if classes:
            out["class_count"] = len(classes)
            out["classes_sample"] = classes[:12]
    summaries = summary.get("summaries")
    if summaries:
        out["summaries"] = summaries[:8]
    return out


def compact_temporal_facts_for_prompt(facts: Dict[str, Any]) -> Dict[str, Any]:
    """Return the model-visible temporal sidecar.

    The full ``temporal_facts`` object stays in memory for deterministic gate checks and trace/audit
    metadata. The prompt copy removes repeated per-date class mirrors and duplicate appointment buckets
    that can push otherwise-small charts over a 12B context window.
    """
    if not facts:
        return {}
    appt = facts.get("appointment_candidates") or {}
    past_appts = appt.get("past") or []
    prompt_facts: Dict[str, Any] = {
        "render_profile": "compact",
        "date_output_contract": facts.get("date_output_contract"),
        "date_ledger": facts.get("date_ledger") or [],
        "anchor_mode": facts.get("anchor_mode"),
        "reference_date": facts.get("reference_date"),
        "reference_date_id": facts.get("reference_date_id"),
        "last_clinical_encounter": _compact_event_summary_for_prompt(
            facts.get("last_clinical_encounter"), include_classes=True
        ),
        "latest_clinical_activity": _compact_event_summary_for_prompt(
            facts.get("latest_clinical_activity"), include_classes=True
        ),
        "clinical_dates": [
            _compact_event_summary_for_prompt(d, include_classes=False)
            for d in (facts.get("clinical_dates") or [])
        ],
        "admin_dates": [
            _compact_event_summary_for_prompt(d, include_classes=False)
            for d in (facts.get("admin_dates") or [])
        ],
        "numeric_series": facts.get("numeric_series") or [],
        "return_visit_dates": facts.get("return_visit_dates") or [],
        "appointment_candidates": {
            "counts": {
                k: len(appt.get(k) or [])
                for k in ("past", "today", "future", "unknown")
            },
            "future": appt.get("future") or [],
            "today": appt.get("today") or [],
            "latest_past": past_appts[:5],
        },
    }
    return prompt_facts


def render_temporal_facts(facts: Dict[str, Any], profile: str = "full") -> str:
    """Render the JSON sidecar as model-visible evidence.

    ``profile`` is a configured temporal-render policy, never an implicit default change:
    ``"full"`` (the default, and the only behavior every research/batch arm has ever seen) ships
    the ``facts`` dict verbatim; ``"compact"`` is an explicit per-level opt-in for context-window
    pressure on small product profiles and drops repeated per-date class mirrors.
    """
    if not facts:
        return ""
    if profile == "compact":
        prompt_facts = compact_temporal_facts_for_prompt(facts)
        marker = "compact deterministic JSON from the chart"
    else:
        prompt_facts = facts
        marker = "deterministic JSON from the chart"
    return (
        f"Structured temporal facts ({marker}). "
        "For any date you write, copy an `iso` value from `date_ledger`; do not reformat "
        "or reconstruct dates from memory.\n"
        "```json\n" + json.dumps(prompt_facts, separators=(",", ":")) + "\n```"
    )


def _compact_facts_summary(facts: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not facts:
        return None
    appt = facts.get("appointment_candidates") or {}
    return {
        "reference_date": facts.get("reference_date"),
        "reference_date_id": facts.get("reference_date_id"),
        "date_ledger_count": len(facts.get("date_ledger") or []),
        "last_clinical_encounter": facts.get("last_clinical_encounter"),
        "latest_clinical_activity": facts.get("latest_clinical_activity"),
        "appointment_candidate_counts": {
            k: len(appt.get(k) or []) for k in ("past", "today", "future")
        },
        "numeric_series": [
            {
                "concept": s.get("concept"),
                "n_points": s.get("n_points"),
                "direction": s.get("direction"),
                "window": s.get("window"),
            }
            for s in (facts.get("numeric_series") or [])
        ],
    }


def compact_temporal_facts_summary(
    facts: Optional[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Small trace/report summary for the otherwise-full prompt sidecar."""
    return _compact_facts_summary(facts)


def _gate_result(
    mode: str,
    status: str,
    checks: List[Dict[str, Any]],
    patch_answer: Optional[str] = None,
    patch_citations: Optional[List[int]] = None,
) -> Dict[str, Any]:
    return {
        "schema_version": "temporal_gate.v1",
        "mode": mode,
        "status": status,
        "checks": checks,
        "patch_answer": patch_answer,
        "patch_citations": patch_citations or [],
    }


def _add_check(
    checks: List[Dict[str, Any]],
    check_id: str,
    status: str,
    severity: str,
    claim: str,
    reason: str,
    source_indices: Optional[List[int]] = None,
) -> None:
    checks.append(
        {
            "id": check_id,
            "status": status,
            "severity": severity,
            "claim": claim,
            "reason": reason,
            "source_indices": source_indices or [],
        }
    )


def _final_gate_status(checks: List[Dict[str, Any]]) -> str:
    if not checks:
        return "not_applicable"
    if any(c.get("status") == "fail" for c in checks):
        return "fail"
    if any(c.get("status") == "warn" for c in checks):
        return "warn"
    return "pass"


def _fact_dates(cands: List[Dict[str, Any]]) -> List[str]:
    return sorted({c.get("date") for c in cands if c.get("date")})


def _asserted_last_visit_dates(question: str, answer: str) -> List[str]:
    """Return dates governed by a positive last-visit assertion.

    Bind each date to the nearest ``last visit/encounter`` phrase on either side within the same
    assertion. This avoids a broad disclaimer elsewhere masking a contradictory assertion, without
    having to enumerate conjunctions such as "but", "although", or "yet".
    """
    question_asks_last = bool(_LAST_VISIT_RE.search(question or ""))
    answer = answer or ""
    named_visits = [
        match
        for match in _LAST_VISIT_RE.finditer(answer)
        if not re.search(r"\breturn[- ]visit\b", match.group(0), re.I)
    ]
    date_matches = list(_ISO_TOKEN_RE.finditer(answer))
    asserted: List[str] = []
    for date_match in date_matches:
        candidates = []
        for visit_match in named_visits:
            between_start = min(date_match.end(), visit_match.end())
            between_end = max(date_match.start(), visit_match.start())
            between = answer[between_start:between_end] if between_end > between_start else ""
            distance = min(
                abs(date_match.start() - visit_match.end()),
                abs(visit_match.start() - date_match.end()),
            )
            if distance <= 120 and not re.search(r"[.!?]\s+", between):
                candidates.append((distance, visit_match, between))
        if candidates:
            _distance, nearest, between = min(candidates, key=lambda item: item[0])
            immediate_prefix = answer[max(0, nearest.start() - 30) : nearest.start()]
            directly_negated = bool(
                re.search(
                    r"\b(?:no|without)\s+(?:explicit\s+)?(?:the\s+)?$|"
                    r"\bnot\s+(?:the\s+)?$",
                    immediate_prefix,
                    re.I,
                )
            )
            assertion_tail = answer[
                nearest.end() : min(len(answer), max(date_match.end(), nearest.end() + 50))
            ]
            if (
                not directly_negated
                and not _VISIT_DATE_RELATION_NEGATION_RE.match(between)
                and not _VISIT_ASSERTION_NEGATION_RE.match(assertion_tail)
            ):
                asserted.append(date_match.group(0))
            continue
        if (
            question_asks_last
            and not named_visits
            and len(date_matches) == 1
            and len(answer.split()) <= 15
            and not re.search(
                r"\b(?:return[- ]visit|appointment|follow-?up)\b", answer, re.I
            )
        ):
            local = answer[max(0, date_match.start() - 100) : date_match.end()]
            if not re.search(r"\bclinical activity\b", local, re.I) and not re.search(
                r"\b(?:no|without)\b.{0,50}\b(?:visit|encounter)\b", answer, re.I
            ):
                asserted.append(date_match.group(0))
    return sorted(set(asserted))


def _date_has_future_framing(sentence: str, date_match: re.Match) -> bool:
    prefix = sentence[max(0, date_match.start() - 120) : date_match.start()]
    suffix = sentence[date_match.end() : min(len(sentence), date_match.end() + 80)]
    prefix_match = _FUTURE_DATE_PREFIX_RE.search(prefix)
    if prefix_match:
        clause_start = max(
            (
                boundary.end()
                for boundary in re.finditer(
                    r"[.;!?]|\b(?:but|however|yet)\b", prefix[: prefix_match.end()], re.I
                )
            ),
            default=0,
        )
        clause = prefix[clause_start : prefix_match.end()]
        if not _NO_UPCOMING_RE.search(clause):
            return True
    return bool(_FUTURE_DATE_SUFFIX_RE.search(suffix))


def _no_upcoming_claim_scope(answer: str) -> str:
    for sentence in _SENTENCE_RE.split(answer or ""):
        match = _NO_UPCOMING_RE.search(sentence)
        if not match:
            continue
        normalized = _CITATION_SEQUENCE_RE.sub("", sentence.strip())
        normalized = re.sub(r"\s+([.;!?])", r"\1", " ".join(normalized.split()))
        if _SAFE_NO_UPCOMING_CLAIM_RE.fullmatch(normalized):
            return sentence.strip()[:240]
        return match.group(0)[:240]
    return (answer or "")[:240]


def _latest_candidate(cands: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    return max(
        (c for c in cands if c.get("date")), key=lambda c: c["date"], default=None
    )


def _allowed_iso_dates(facts: Dict[str, Any]) -> set:
    contract = facts.get("date_output_contract") or {}
    allowed = {
        str(d)
        for d in (contract.get("allowed_iso_dates") or [])
        if _parse_iso_date(str(d))
    }
    if allowed:
        return allowed
    return {
        str(d.get("iso"))
        for d in (facts.get("date_ledger") or [])
        if isinstance(d, dict) and _parse_iso_date(str(d.get("iso")))
    }


def _date_output_failures(answer: str, facts: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Find dates the model should not have emitted: malformed date-like strings and
    valid ISO dates absent from the model-visible date ledger."""
    allowed = _allowed_iso_dates(facts)
    out: List[Dict[str, Any]] = []
    seen: set = set()

    def _add(kind: str, raw: str, reason: str) -> None:
        key = (kind, raw)
        if key in seen:
            return
        seen.add(key)
        out.append({"kind": kind, "date": raw, "reason": reason})

    for raw in _DATE_ID_TOKEN_RE.findall(answer or ""):
        _add(
            "date_id_exposed",
            raw,
            "a date_id meant for internal matching, not a user-facing date",
        )
    extra_patterns = [
        (
            _NON_ASCII_ISO_TOKEN_RE,
            "malformed",
            "uses non-ASCII hyphens; copy an exact YYYY-MM-DD from date_ledger",
        ),
        (
            _DOUBLE_SEPARATOR_DATE_RE,
            "malformed",
            "contains repeated date separators, not an exact YYYY-MM-DD string copied from date_ledger",
        ),
        (
            _BRACKETED_DATE_RE,
            "malformed",
            "contains bracketed characters inside a date, not an exact YYYY-MM-DD string copied from date_ledger",
        ),
        (
            _TRUNCATED_ISO_TOKEN_RE,
            "malformed",
            "is truncated; copy a complete YYYY-MM-DD string from date_ledger",
        ),
    ]
    for pattern, kind, reason in extra_patterns:
        for m in pattern.finditer(answer or ""):
            raw = m.group(0)
            if _ISO_RE.fullmatch(raw):
                continue
            _add(kind, raw, reason)
    for m in _DATE_LIKE_RE.finditer(answer or ""):
        raw = m.group(0)
        if _ISO_RE.fullmatch(raw):
            continue
        _add("malformed", raw, "not an exact YYYY-MM-DD string copied from date_ledger")
    for raw in _ISO_TOKEN_RE.findall(answer or ""):
        if _parse_iso_date(raw) and raw not in allowed:
            _add("not_in_ledger", raw, "valid ISO shape but not present in date_ledger")
    return out


def _series_terms(item: Dict[str, Any]) -> Tuple[List[str], set[str]]:
    concept = str(item.get("concept") or "").lower()
    phrase_tokens = [
        token for token in re.split(r"[^a-z0-9%]+", concept) if len(token) >= 2
    ]
    aliases = {
        token
        for token in phrase_tokens
        if len(token) >= 3 and token not in _CONCEPT_STOP_TOKENS
    }
    if "haemoglobin" in concept:
        aliases.update({"hemoglobin", "hgb"})
    if "weight" in concept:
        aliases.update({"wt", "growth", "gaining"})
    if "height" in concept:
        aliases.update({"growth", "growing"})
    if "cd4" in concept:
        aliases.add("cd4")
    return phrase_tokens, aliases


def _selected_series(
    question: str, answer: str, facts: Dict[str, Any]
) -> List[Dict[str, Any]]:
    series = facts.get("numeric_series") or []

    def scored(text: str) -> List[Tuple[int, set[str], Dict[str, Any]]]:
        word_list = [
            token
            for token in re.split(r"[^a-z0-9%]+", (text or "").lower())
            if token
        ]
        words = set(word_list)
        candidates: List[Tuple[int, set[str], Dict[str, Any]]] = []
        for item in series:
            phrase_tokens, aliases = _series_terms(item)
            matched = words & aliases
            phrase_match = bool(phrase_tokens) and any(
                word_list[start : start + len(phrase_tokens)] == phrase_tokens
                for start in range(len(word_list) - len(phrase_tokens) + 1)
            )
            value = (100 if phrase_match else 0) + len(matched)
            if value > 0:
                candidates.append((value, matched, item))
        return candidates

    candidates = scored(answer)
    if not candidates:
        candidates = scored(question)
    selected: List[Dict[str, Any]] = []
    for score, matched, item in candidates:
        dominated = any(
            score < 100 and matched <= other_matched and score < other_score
            for other_score, other_matched, _other_item in candidates
        )
        if not dominated:
            selected.append(item)
    return selected


def _series_mention_spans(answer: str, item: Dict[str, Any]) -> List[Tuple[int, int]]:
    phrase_tokens, aliases = _series_terms(item)
    if phrase_tokens:
        phrase_pattern = r"(?<![a-z0-9%])" + r"\s+".join(
            re.escape(token) for token in phrase_tokens
        ) + r"(?![a-z0-9%])"
        phrase_spans = [
            match.span() for match in re.finditer(phrase_pattern, answer, re.I)
        ]
        if phrase_spans:
            return phrase_spans
    spans: set[Tuple[int, int]] = set()
    for term in sorted(aliases, key=len, reverse=True):
        pattern = r"(?<![a-z0-9%])" + re.escape(term) + r"(?![a-z0-9%])"
        spans.update(match.span() for match in re.finditer(pattern, answer, re.I))
    clustered: List[Tuple[int, int]] = []
    for start, end in sorted(spans):
        if not clustered or start - clustered[-1][1] > 40:
            clustered.append((start, end))
        else:
            clustered[-1] = (clustered[-1][0], max(clustered[-1][1], end))
    return clustered


def _series_mention_positions(answer: str, item: Dict[str, Any]) -> List[int]:
    return [start for start, _end in _series_mention_spans(answer, item)]


def _answer_direction_for_series(
    answer: str, item: Dict[str, Any], selected: List[Dict[str, Any]]
) -> Optional[str]:
    mentions = [
        (index, span)
        for index, candidate in enumerate(selected)
        for span in _series_mention_spans(answer, candidate)
    ]
    target_index = next(
        (index for index, candidate in enumerate(selected) if candidate is item), None
    )
    if target_index is None or not any(index == target_index for index, _span in mentions):
        return _answer_direction(answer) if len(selected) == 1 else None

    direction_spans: List[Tuple[str, Tuple[int, int]]] = [
        ("down", match.span()) for match in _DOWN_WORD_RE.finditer(answer)
    ]
    for match in _UP_WORD_RE.finditer(answer):
        local = answer[max(0, match.start() - 25) : match.end()]
        direction_spans.append(
            ("down" if _NEGATED_UP_RE.search(local) else "up", match.span())
        )

    directions: set[str] = set()
    for direction, (direction_start, direction_end) in direction_spans:
        distances = []
        for index, (mention_start, mention_end) in mentions:
            if direction_start >= mention_end:
                distance = direction_start - mention_end
            elif mention_start >= direction_end:
                distance = mention_start - direction_end
            else:
                distance = 0
            distances.append((distance, index))
        nearest = min((distance for distance, _index in distances), default=None)
        assigned = {
            index for distance, index in distances if nearest is not None and distance == nearest
        }

        preceding = sorted(
            (
                (mention_start, mention_end, index)
                for index, (mention_start, mention_end) in mentions
                if mention_end <= direction_start
            ),
            key=lambda value: value[1],
        )
        if preceding and re.fullmatch(
            r"\s*(?:both\s+)?", answer[preceding[-1][1] : direction_start], re.I
        ):
            group = [preceding[-1]]
            for candidate in reversed(preceding[:-1]):
                connector = answer[candidate[1] : group[-1][0]]
                if not re.fullmatch(
                    r"\s*(?:(?:,|/|&)(?:\s*(?:and|or))?|(?:and|or))\s*",
                    connector,
                    re.I,
                ):
                    break
                group.append(candidate)
            assigned.update(index for _start, _end, index in group)

        following = sorted(
            (
                (mention_start, mention_end, index)
                for index, (mention_start, mention_end) in mentions
                if mention_start >= direction_end
            ),
            key=lambda value: value[0],
        )
        if following and re.fullmatch(
            r"\s*", answer[direction_end : following[0][0]]
        ):
            group = [following[0]]
            for candidate in following[1:]:
                connector = answer[group[-1][1] : candidate[0]]
                if not re.fullmatch(
                    r"\s*(?:(?:,|/|&)(?:\s*(?:and|or))?|(?:and|or))\s*",
                    connector,
                    re.I,
                ):
                    break
                group.append(candidate)
            assigned.update(index for _start, _end, index in group)

        if target_index in assigned:
            directions.add(direction)
    return next(iter(directions)) if len(directions) == 1 else None


def _answer_direction(answer: str) -> Optional[str]:
    if _NEGATED_UP_RE.search(answer):
        return "down"
    has_down = bool(_DOWN_WORD_RE.search(answer))
    has_up = bool(_UP_WORD_RE.search(answer))
    if has_down and not has_up:
        return "down"
    if has_up and not has_down:
        return "up"
    return None


def _format_value(v: Any) -> str:
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


def _series_patch(series: Dict[str, Any]) -> tuple[str, List[int]]:
    pts = series.get("points") or []
    if not pts:
        return (
            "The chart does not contain dated measurements for this temporal trend.",
            [],
        )
    first, last = pts[0], pts[-1]
    unit = (" " + str(last.get("unit"))) if last.get("unit") else ""
    if len(pts) == 1:
        return (
            f"The chart shows one dated {series.get('concept')} measurement: "
            f"{_format_value(last.get('value'))}{unit} on {last.get('date')} [{last.get('index')}]. "
            "A trend cannot be determined from one point.",
            [last.get("index")] if isinstance(last.get("index"), int) else [],
        )
    direction = {"up": "increased", "down": "decreased", "flat": "was unchanged"}.get(
        series.get("direction"), "changed"
    )
    return (
        f"The documented {series.get('concept')} series {direction} from "
        f"{_format_value(first.get('value'))}{unit} on {first.get('date')} [{first.get('index')}] "
        f"to {_format_value(last.get('value'))}{unit} on {last.get('date')} [{last.get('index')}].",
        [i for i in (first.get("index"), last.get("index")) if isinstance(i, int)],
    )


def _has_exact_unit_suffix(suffix: str, unit: str) -> bool:
    normalized_suffix = unicodedata.normalize("NFKC", suffix).translate(
        str.maketrans({"⁄": "/", "∕": "/", "−": "-"})
    )
    normalized_unit = unicodedata.normalize("NFKC", unit).translate(
        str.maketrans({"⁄": "/", "∕": "/", "−": "-"})
    )
    match = re.match(re.escape(normalized_unit) + r"(?![a-z0-9])", normalized_suffix, re.I)
    if not match:
        return False
    continuation = normalized_suffix[match.end() :]
    return not re.match(
        r"\s*(?:/|per\b|(?:[·*]\s*)?m\s*(?:\^\s*)?(?:\{\s*)?"
        r"(?:-\s*)?[23](?:\s*\})?)",
        continuation,
        re.I,
    )


def _series_value_candidates(
    text: str, series: Dict[str, Any]
) -> List[tuple[re.Match, float]]:
    """Return numeric claims that belong to this series, excluding nearby other measures."""
    # The generic tokenizer excludes hyphen-adjacent digits to avoid treating ISO dates as
    # measurements. Dates have already been blanked by the caller, so normalize a remaining
    # numeric range separator without changing string offsets.
    text = re.sub(r"(?<=\d)-(?=\d)", "–", text)
    values: List[tuple[re.Match, float]] = []
    all_numbers: List[tuple[re.Match, float]] = []
    for match in _NUMBER_TOKEN_RE.finditer(text):
        try:
            all_numbers.append((match, float(match.group())))
        except ValueError:
            continue

    units = {
        str(point.get("unit") or "").strip().lower()
        for point in (series.get("points") or [])
        if str(point.get("unit") or "").strip()
    }
    concept_tokens = {
        token
        for token in re.split(
            r"[^a-z0-9%]+", str(series.get("concept") or "").lower()
        )
        if len(token) >= 2 and token not in _CONCEPT_STOP_TOKENS
    }
    concept = str(series.get("concept") or "").lower()
    if "haemoglobin" in concept:
        concept_tokens.update({"hemoglobin", "hgb"})
    if "weight" in concept:
        concept_tokens.update({"weight", "wt"})
    if "height" in concept:
        concept_tokens.update({"height", "ht"})
    if "cd4" in concept:
        concept_tokens.add("cd4")

    explicit: set[int] = set()
    for index, (match, _value) in enumerate(all_numbers):
        suffix = text[match.end() : match.end() + 24].lstrip().lower()
        if any(_has_exact_unit_suffix(suffix, unit) for unit in units):
            explicit.add(index)

    # In ranges such as "53 to 52 kg", the trailing unit applies to both values.
    for index in tuple(explicit):
        if index == 0:
            continue
        previous = all_numbers[index - 1][0]
        current = all_numbers[index][0]
        between = text[previous.end() : current.start()]
        if re.fullmatch(r"\s*(?:to|through|[-–—])\s*", between, re.I):
            explicit.add(index - 1)

    for index, item in enumerate(all_numbers):
        match, _value = item
        if index in explicit:
            values.append(item)
            continue
        prefix = text[max(0, match.start() - 40) : match.start()].lower()
        words = re.findall(r"[a-z][a-z0-9%]*", prefix)
        if words and any(word in concept_tokens for word in words[-3:]):
            values.append(item)
    return values


def _claim_local_series_values(
    text: str,
    series: Dict[str, Any],
    selected_series: List[Dict[str, Any]],
) -> List[tuple[re.Match, float]]:
    values = _series_value_candidates(text, series)
    if len(selected_series) <= 1:
        return values

    target_index = next(
        (index for index, candidate in enumerate(selected_series) if candidate is series),
        None,
    )
    if target_index is None:
        return []
    if re.search(r"\brespectively\b", text, re.I):
        mention_starts: List[tuple[int, int]] = []
        for index, candidate in enumerate(selected_series):
            spans = _series_mention_spans(text, candidate)
            if not spans:
                mention_starts = []
                break
            mention_starts.append((min(start for start, _end in spans), index))
        unit_sets = [
            {
                unicodedata.normalize("NFKC", str(point.get("unit") or "").strip())
                for point in candidate.get("points") or []
                if str(point.get("unit") or "").strip()
            }
            for candidate in selected_series
        ]
        common_units = set.intersection(*unit_sets) if unit_sets else set()
        ordered_values: List[tuple[re.Match, float]] = []
        for match in _NUMBER_TOKEN_RE.finditer(text):
            try:
                ordered_values.append((match, float(match.group())))
            except ValueError:
                continue
        last_value_end = ordered_values[-1][0].end() if ordered_values else 0
        shared_unit_suffix = text[last_value_end : last_value_end + 40].lstrip()
        if (
            len(mention_starts) == len(selected_series)
            and len({start for start, _index in mention_starts})
            == len(selected_series)
            and len(ordered_values) == len(selected_series)
            and common_units
            and any(
                _has_exact_unit_suffix(shared_unit_suffix, unit)
                for unit in common_units
            )
        ):
            mention_order = [index for _start, index in sorted(mention_starts)]
            return [ordered_values[mention_order.index(target_index)]]
    if not values:
        return []
    mentions = [
        (index, span)
        for index, candidate in enumerate(selected_series)
        for span in _series_mention_spans(text, candidate)
    ]
    if not any(index == target_index for index, _span in mentions):
        return []

    assigned: List[tuple[re.Match, float]] = []
    for value_match, value in values:
        distances: List[tuple[int, int]] = []
        for index, (start, end) in mentions:
            if value_match.start() >= end:
                distance = value_match.start() - end
            elif start >= value_match.end():
                distance = start - value_match.end()
            else:
                distance = 0
            distances.append((distance, index))
        nearest = min((distance for distance, _index in distances), default=None)
        nearest_series = {
            index
            for distance, index in distances
            if nearest is not None and distance == nearest
        }
        if nearest_series == {target_index}:
            assigned.append((value_match, value))
    return assigned


def _date_value_failures(
    answer: str,
    series: Dict[str, Any],
    selected_series: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    points = series.get("points") or []
    by_date: Dict[str, set] = {}
    value_dates: Dict[float, set] = {}
    for p in points:
        try:
            val = float(p.get("value"))
        except (TypeError, ValueError):
            continue
        by_date.setdefault(str(p.get("date")), set()).add(val)
        value_dates.setdefault(val, set()).add(str(p.get("date")))
    out = []
    for sentence in _SENTENCE_RE.split(answer or ""):
        clean_sentence = re.sub(r"\[\d+\]", "", sentence)
        date_matches = list(_ISO_TOKEN_RE.finditer(clean_sentence))
        if not date_matches:
            continue
        value_sentence = list(clean_sentence)
        for date_match in date_matches:
            value_sentence[date_match.start() : date_match.end()] = " " * len(
                date_match.group()
            )
        value_text = "".join(value_sentence)
        values = _claim_local_series_values(
            value_text, series, selected_series or [series]
        )
        if not values:
            continue
        all_expected_values = sorted(value_dates)
        for _match, val in values:
            if val not in value_dates:
                failure = {
                    "date": date_matches[0].group() if len(date_matches) == 1 else None,
                    "value": val,
                    "expected_values": all_expected_values,
                }
                if failure not in out:
                    out.append(failure)
        separators = list(
            re.finditer(
                r"\b(?:to|and|versus|vs\.?)\b|[;,]", clean_sentence, re.I
            )
        )
        if re.search(r"\brespectively\b", clean_sentence, re.I) and len(
            date_matches
        ) == len(values):
            bindings = list(zip(date_matches, values))
        elif len(date_matches) == 1:
            bindings = [(date_matches[0], value) for value in values]
        else:
            bindings = []
            for date_match in date_matches:
                segment_start = max(
                    (
                        match.end()
                        for match in separators
                        if match.end() <= date_match.start()
                    ),
                    default=0,
                )
                segment_end = min(
                    (
                        match.start()
                        for match in separators
                        if match.start() >= date_match.end()
                    ),
                    default=len(clean_sentence),
                )
                local_values = [
                    item
                    for item in values
                    if segment_start <= item[0].start()
                    and item[0].end() <= segment_end
                ]
                nearest = min(
                    local_values or values,
                    key=lambda item: min(
                        abs(date_match.start() - item[0].end()),
                        abs(item[0].start() - date_match.end()),
                    ),
                )
                bindings.append((date_match, nearest))
        for date_match, (_nearest_match, val) in bindings:
            date = date_match.group()
            failure = None
            if val not in value_dates:
                continue
            if date in by_date and val not in by_date[date]:
                failure = {
                    "date": date,
                    "value": val,
                    "expected_values": sorted(by_date[date]),
                }
            elif val in value_dates and date not in value_dates[val]:
                failure = {
                    "date": date,
                    "value": val,
                    "expected_dates": sorted(value_dates[val]),
                }
            if failure and failure not in out:
                out.append(failure)
    return out


def run_temporal_gate(
    question: str,
    answer: str,
    citations: Optional[List[int]],
    temporal_facts: Optional[Dict[str, Any]],
    mode: str = "off",
) -> Dict[str, Any]:
    """High-precision runtime gate for temporal claims.

    The gate is deliberately conservative: it blocks only deterministic contradictions in the
    structured temporal facts and otherwise emits warnings/no-op metadata."""
    normalized_mode = (mode or "off").strip().lower()
    if normalized_mode not in {"off", "warn", "enforce"}:
        normalized_mode = "off"
    if normalized_mode == "off":
        return _gate_result(normalized_mode, "not_applicable", [])
    if not temporal_facts:
        return _gate_result(normalized_mode, "not_applicable", [])

    checks: List[Dict[str, Any]] = []
    q = question or ""
    a = answer or ""
    qa = q + "\n" + a
    ref = temporal_facts.get("reference_date")
    appt = temporal_facts.get("appointment_candidates") or {}
    future = appt.get("future") or []
    past = appt.get("past") or []
    all_candidates = appt.get("all") or []
    patch_answer: Optional[str] = None
    patch_citations: List[int] = []

    for failure in _date_output_failures(a, temporal_facts)[:5]:
        _add_check(
            checks,
            "date_format",
            "fail",
            "block",
            str(failure.get("date"))[:240],
            f"The answer emits {failure.get('date')!r}, which is {failure.get('reason')}.",
            [c for c in (citations or []) if isinstance(c, int)],
        )

    if _UPCOMING_RE.search(qa):
        no_upcoming = bool(_NO_UPCOMING_RE.search(a))
        if future and no_upcoming:
            newest = _latest_candidate(future)
            _add_check(
                checks,
                "upcoming_date",
                "fail",
                "block",
                a[:240],
                "The answer says no upcoming appointment, but temporal_facts has future appointment candidates.",
                [c.get("index") for c in future if isinstance(c.get("index"), int)],
            )
            if newest:
                patch_answer = (
                    f"The chart documents a future return-visit/appointment candidate on "
                    f"{newest.get('date')} [{newest.get('index')}]."
                )
                patch_citations = (
                    [newest["index"]] if isinstance(newest.get("index"), int) else []
                )
        if not future and no_upcoming:
            _add_check(
                checks,
                "upcoming_date",
                "pass",
                "warn",
                _no_upcoming_claim_scope(a),
                "No future appointment candidates are present after the reference date.",
                sorted(
                    {
                        candidate.get("index")
                        for candidate in all_candidates
                        if isinstance(candidate.get("index"), int)
                    }
                ),
            )
        for sentence in _SENTENCE_RE.split(a):
            for date_match in _ISO_TOKEN_RE.finditer(sentence):
                if not _date_has_future_framing(sentence, date_match):
                    continue
                date = date_match.group()
                if ref and date <= ref:
                    indices = [
                        c.get("index") for c in all_candidates if c.get("date") == date
                    ]
                    _add_check(
                        checks,
                        "upcoming_date",
                        "fail",
                        "block",
                        sentence[:240],
                        f"The answer frames {date} as upcoming/future, but reference_date is {ref}.",
                        [i for i in indices if isinstance(i, int)],
                    )
                    latest_past = _latest_candidate(past)
                    if not patch_answer and latest_past:
                        patch_answer = (
                            f"No upcoming appointment is documented after {ref}. The latest return-visit "
                            f"date found is {latest_past.get('date')}, which is before the reference date "
                            f"[{latest_past.get('index')}]."
                        )
                        patch_citations = (
                            [latest_past["index"]]
                            if isinstance(latest_past.get("index"), int)
                            else []
                        )
        if (
            all_candidates
            and re.search(r"\bappointment\b", a, re.I)
            and not re.search(r"return visit date", a, re.I)
        ):
            _add_check(
                checks,
                "upcoming_date",
                "warn",
                "warn",
                a[:240],
                "The chart has appointment-like date observations, not a formal Appointment resource/status.",
                [
                    c.get("index")
                    for c in all_candidates
                    if isinstance(c.get("index"), int)
                ],
            )

    last = temporal_facts.get("last_clinical_encounter") or {}
    latest_activity = temporal_facts.get("latest_clinical_activity") or {}
    last_date = last.get("date")
    if _LAST_VISIT_RE.search(qa):
        asserted_visit_dates = _asserted_last_visit_dates(q, a)
        if last_date and any(date != last_date for date in asserted_visit_dates):
            indices = [i for i in (last.get("indices") or []) if isinstance(i, int)]
            _add_check(
                checks,
                "last_visit",
                "fail",
                "block",
                a[:240],
                f"The answer gives a last-visit date other than the deterministic last clinical encounter {last_date}.",
                indices,
            )
            if not patch_answer:
                cite = "".join(f"[{i}]" for i in indices[:3])
                patch_answer = (
                    f"The most recent clinical visit/encounter documented in the chart is {last_date}"
                    + (f" {cite}." if cite else ".")
                )
                patch_citations = indices[:3]
        elif not last_date and asserted_visit_dates:
            activity_date = latest_activity.get("date")
            activity_indices = [
                i
                for i in (latest_activity.get("indices") or [])
                if isinstance(i, int)
            ]
            _add_check(
                checks,
                "last_visit",
                "fail",
                "block",
                a[:240],
                "The answer asserts a last-visit date, but the evidence ledger has no explicit visit/encounter record.",
                activity_indices[:1],
            )
            if not patch_answer:
                cite = f" [{activity_indices[0]}]" if activity_indices else ""
                activity_clause = (
                    f" The latest dated clinical activity is {activity_date}{cite}."
                    if activity_date
                    else ""
                )
                patch_answer = (
                    "No explicit visit/encounter record is documented in the available chart."
                    + activity_clause
                )
                patch_citations = activity_indices[:1]

    selected = _selected_series(q, a, temporal_facts)
    if _TREND_RE.search(qa) and selected:
        for s in selected:
            answer_dir = _answer_direction_for_series(a, s, selected)
            indices = [
                p.get("index")
                for p in (s.get("points") or [])
                if isinstance(p.get("index"), int)
            ]
            if not s.get("trend_supported"):
                _add_check(
                    checks,
                    "single_point_trend",
                    "fail",
                    "block",
                    a[:240],
                    f"{s.get('concept')} has fewer than two dated points, so no trend is supported.",
                    indices,
                )
                if not patch_answer:
                    patch_answer, patch_citations = _series_patch(s)
            elif (
                answer_dir
                and s.get("direction") in {"up", "down"}
                and answer_dir != s.get("direction")
            ):
                _add_check(
                    checks,
                    "trend_direction",
                    "fail",
                    "block",
                    a[:240],
                    f"The answer direction is {answer_dir}, but the computed {s.get('concept')} direction is {s.get('direction')}.",
                    indices,
                )
                if not patch_answer:
                    patch_answer, patch_citations = _series_patch(s)
            if _WINDOW_RE.search(qa):
                window = s.get("window") or {}
                _add_check(
                    checks,
                    "window_scope",
                    "warn",
                    "warn",
                    a[:240],
                    f"Answer/query names a strict window, but the documented {s.get('concept')} window is {window.get('start')}..{window.get('end')}.",
                    indices,
                )

    if any(c.get("id") == "date_format" and c.get("status") == "fail" for c in checks):
        if not patch_answer and len(selected) == 1:
            patch_answer, patch_citations = _series_patch(selected[0])

    for s in selected:
        for failure in _date_value_failures(a, s, selected)[:3]:
            bound_date = failure.get("date")
            claim = (
                f"value {failure['value']} to {bound_date}"
                if bound_date
                else f"value {failure['value']}"
            )
            _add_check(
                checks,
                "date_value_binding",
                "fail",
                "block",
                a[:240],
                f"The answer binds {claim}, but temporal_facts bind it to {failure.get('expected_dates') or failure.get('expected_values')}.",
                [
                    p.get("index")
                    for p in (s.get("points") or [])
                    if isinstance(p.get("index"), int)
                ],
            )
            if not patch_answer:
                patch_answer, patch_citations = _series_patch(s)

    return _gate_result(
        normalized_mode,
        _final_gate_status(checks),
        checks,
        patch_answer,
        patch_citations,
    )


_DANGLING_CITATION_RE = re.compile(r"\s*\[\s*$")
_VALID_CITATION_TOKEN_RE = re.compile(
    r"(?<![\w\[])\[\d+(?:\s*,\s*\d+)*\](?![\w\]])"
)
_GROUPED_CITATION_RE = re.compile(
    r"(?<![\w\[])\[((?:\d+\s*,\s*)+\d+)\](?![\w\]])"
)


def _canonicalize_grouped_citations(claim: str) -> Tuple[str, List[int]]:
    source_indices: List[int] = []

    def replacement(match: re.Match) -> str:
        indices: List[int] = []
        for value in re.findall(r"\d+", match.group(1)):
            index = int(value)
            if index not in indices:
                indices.append(index)
            if index not in source_indices:
                source_indices.append(index)
        return "".join(f"[{index}]" for index in indices)

    return _GROUPED_CITATION_RE.sub(replacement, claim or ""), source_indices


def canonicalize_indepth_citations(claim: str) -> Tuple[str, List[int]]:
    """Canonicalize only complete, standalone numeric citation groups."""
    return _canonicalize_grouped_citations(claim)


def _has_malformed_citation_syntax(claim: str) -> bool:
    remainder = _VALID_CITATION_TOKEN_RE.sub("", claim or "")
    return "[" in remainder or "]" in remainder


def _repair_dangling_indepth_citation(
    claim: str, temporal_facts: Optional[Dict[str, Any]]
) -> Optional[Tuple[str, int, str]]:
    """Repair one truncated trailing citation only when evidence binding is unique."""
    if not claim or not _DANGLING_CITATION_RE.search(claim):
        return None
    existing = [int(value) for value in re.findall(r"\[(\d+)]", claim)]
    if existing:
        return None
    cleaned = _DANGLING_CITATION_RE.sub("", claim).rstrip()
    if not temporal_facts:
        return None

    dates = set(_ISO_TOKEN_RE.findall(claim))
    if not dates:
        return None
    without_dates = list(claim)
    for match in _ISO_TOKEN_RE.finditer(claim):
        without_dates[match.start() : match.end()] = " " * len(match.group())
    value_text = "".join(without_dates)

    matching_indices: set[int] = set()
    for series in _selected_series("", claim, temporal_facts):
        claimed_values = _series_value_candidates(value_text, series)
        if not claimed_values:
            continue
        for point in series.get("points") or []:
            index = point.get("index")
            try:
                point_value = float(point.get("value"))
            except (TypeError, ValueError):
                continue
            point_unit = unicodedata.normalize(
                "NFKC", str(point.get("unit") or "").strip()
            )
            value_matches = False
            for value_match, claimed_value in claimed_values:
                if point_value != claimed_value:
                    continue
                if point_unit:
                    suffix = unicodedata.normalize(
                        "NFKC", value_text[value_match.end() : value_match.end() + 40]
                    )
                    if not _has_exact_unit_suffix(suffix.lstrip(), point_unit):
                        continue
                value_matches = True
                break
            if (
                isinstance(index, int)
                and str(point.get("date")) in dates
                and value_matches
            ):
                matching_indices.add(index)
    if len(matching_indices) != 1:
        return None

    source_index = next(iter(matching_indices))
    punctuation = cleaned[-1] if cleaned[-1:] in {".", "!", "?"} else ""
    stem = cleaned[:-1].rstrip() if punctuation else cleaned
    return (
        f"{stem} [{source_index}]{punctuation}",
        source_index,
        "A truncated trailing citation uniquely matched one dated numeric record.",
    )


def gate_indepth_claims(
    question: str,
    claims: List[str],
    temporal_facts: Optional[Dict[str, Any]],
    *,
    mode: str = "enforce",
) -> Dict[str, Any]:
    """Apply the deterministic temporal gate to every displayed In-Depth claim."""
    normalized_mode = (mode or "enforce").strip().lower()
    if normalized_mode not in {"off", "warn", "enforce"}:
        normalized_mode = "enforce"
    kept: List[str] = []
    removed: List[int] = []
    checks: List[Dict[str, Any]] = []
    edited = False
    if not claims:
        return {
            "schema_version": "indepth_temporal_gate.v1",
            "mode": normalized_mode,
            "status": "needs_review",
            "claims": [],
            "removed": [],
            "checks": [],
        }
    for index, claim in enumerate(claims or [], 1):
        original_claim = claim
        canonicalized_indices: List[int] = []
        if normalized_mode == "enforce":
            claim, canonicalized_indices = _canonicalize_grouped_citations(claim or "")
            if canonicalized_indices:
                edited = True
        citation_repair = (
            _repair_dangling_indepth_citation(claim or "", temporal_facts)
            if normalized_mode == "enforce"
            else None
        )
        if citation_repair:
            claim, source_index, repair_reason = citation_repair
            edited = True
        citations = [int(value) for value in re.findall(r"\[(\d+)]", claim or "")]
        gate = run_temporal_gate(
            question, claim or "", citations, temporal_facts, normalized_mode
        )
        if normalized_mode != "off" and _has_malformed_citation_syntax(claim or ""):
            gate = dict(gate)
            gate_checks = list(gate.get("checks") or [])
            _add_check(
                gate_checks,
                "citation_format",
                "fail",
                "block",
                original_claim[:240],
                "The claim contains malformed, attached, nested, or incomplete evidence citation syntax.",
                citations,
            )
            gate.update(
                {
                    "status": "fail",
                    "checks": gate_checks,
                    "patch_answer": None,
                    "patch_citations": [],
                }
            )
        check = {"claim_index": index, "claim": original_claim, "gate": gate}
        if citation_repair:
            check["evaluated_claim"] = claim
            check["citation_repair"] = {
                "status": "repaired",
                "source_index": source_index,
                "reason": repair_reason,
            }
        if canonicalized_indices:
            check["evaluated_claim"] = claim
            check["citation_canonicalization"] = {
                "status": "canonicalized",
                "source_indices": canonicalized_indices,
            }
        checks.append(check)
        if gate.get("status") != "fail" or normalized_mode != "enforce":
            kept.append(claim)
            continue
        patch = str(gate.get("patch_answer") or "").strip()
        patch_citations = {
            value for value in gate.get("patch_citations") or [] if isinstance(value, int)
        }
        original_citations = set(citations)
        if patch and patch_citations and patch_citations <= original_citations:
            kept.append(patch)
        else:
            removed.append(index)
        edited = True

    if edited and not kept:
        status = "needs_review"
    elif edited:
        status = "edited"
    elif any(item["gate"].get("status") == "fail" for item in checks):
        status = "needs_review"
    else:
        status = "checked"
    return {
        "schema_version": "indepth_temporal_gate.v1",
        "mode": normalized_mode,
        "status": status,
        "claims": kept,
        "removed": removed,
        "checks": checks,
    }


def build_temporal_block(chart: str, anchor: Optional[str]) -> str:
    """The injected evidence block: the anchor line + per-concept series. '' when there's neither
    an anchor nor any series. The synth/validator read 'most recent' and trends FROM here.
    """
    obs = parse_dated_observations(chart)
    if not anchor and not obs:
        return ""
    lines: List[str] = []
    if anchor:
        lines.append(
            f'Current date: {anchor}. Interpret "current", "recent", and "most recent" '
            f"relative to THIS date; the patient's records may predate it. Use the dated series "
            f"below verbatim — do not infer dates, values, or trends not shown."
        )
    # Typed event timeline: "last visit"/"most recent" must resolve to a clinical visit/encounter,
    # NOT the chart's max date (which can be an administrative Program enrollment that post-dates the
    # last visit). Type each record by class; report the most-recent CLINICAL date + the visit dates,
    # and list administrative records separately so they are never mistaken for a visit.
    events = parse_events(chart)
    visit_dates = sorted({e["date"] for e in _clinical_encounter_events(events)}, reverse=True)
    if visit_dates:
        lines.append(
            f'Most recent clinical visit/encounter: {visit_dates[0]}. Answer "last visit" / '
            f'"most recent visit" from THIS date — the administrative records below are NOT visits.'
        )
        shown = visit_dates[:12]
        more = f" (+{len(visit_dates) - 12} earlier)" if len(visit_dates) > 12 else ""
        lines.append(
            "Clinical visit/encounter dates (newest first): "
            + ", ".join(shown)
            + more
            + "."
        )
    elif events:
        clinical_dates = sorted(
            {e["date"] for e in events if e["cls"] not in _ADMIN_CLASSES}, reverse=True
        )
        if clinical_dates:
            lines.append(
                "No explicit visit/encounter record is present. Latest dated clinical activity: "
                f"{clinical_dates[0]}. Do NOT report this activity date as a visit date."
            )
    seen_admin: set = set()
    for e in sorted(
        (e for e in events if e["cls"] in _ADMIN_CLASSES),
        key=lambda e: e["date"],
        reverse=True,
    ):
        summary = e["body"].split(". ")[0].strip()
        if (e["date"], summary) in seen_admin:
            continue
        seen_admin.add((e["date"], summary))
        lines.append(f"Administrative record on {e['date']} (NOT a visit): {summary}.")
    by_concept: Dict[str, List[Dict[str, Any]]] = {}
    for o in obs:
        by_concept.setdefault(o["concept"], []).append(o)
    if by_concept:
        if lines:
            lines.append("")
        lines.append("Computed numeric series (deterministic, oldest→newest):")
    for concept, series in by_concept.items():
        series = sorted(series, key=lambda o: o["date"])
        last = series[-1]
        unit = (" " + last["unit"]) if last["unit"] else ""
        if len(series) < 2:
            lines.append(
                f"- {concept}: {last['value']}{unit} ({last['date']}) — single measurement."
            )
            continue
        first = series[0]
        vals = [o["value"] for o in series]
        direction = (
            "↑"
            if last["value"] > first["value"]
            else ("↓" if last["value"] < first["value"] else "→")
        )
        lines.append(
            f"- {concept}: most recent {last['value']}{unit} ({last['date']}); "
            f"{len(series)} values {first['value']}→{last['value']} {direction} "
            f"over {first['date']}…{last['date']}; range {min(vals)}–{max(vals)}."
        )
    return "\n".join(lines)
