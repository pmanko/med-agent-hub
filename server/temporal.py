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

import json
import calendar
import datetime as _dt
import re
from typing import Any, Dict, List, Optional

# A record line is `[N] <rest>`; the date `(YYYY-MM-DD)` is present only on the FIRST line of a
# same-date run (the serializer run-length-compresses it — e.g. Ellcky keeps 8/276 dates), so it is
# parsed as an OPTIONAL prefix and carried forward to the dateless follow-ons.
_REC_RE = re.compile(r"^\[(\d+)\]\s*(.*)$")
_DATE_PREFIX_RE = re.compile(r"^\((\d{4}-\d{2}-\d{2})\)\s*(.*)$")
_DATE_RE = re.compile(r"\((\d{4}-\d{2}-\d{2})\)")
_ISO_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_ISO_TOKEN_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
_UNICODE_HYPHEN = "\u2010\u2011\u2012\u2013\u2014\u2015\u2212\ufe58\ufe63\uff0d"
_NON_ASCII_ISO_TOKEN_RE = re.compile(rf"\b\d{{4}}[{_UNICODE_HYPHEN}]\d{{2}}[{_UNICODE_HYPHEN}]\d{{2}}\b")
_TRUNCATED_ISO_TOKEN_RE = re.compile(r"\b\d{4}-\d{1,2}\b(?!-)")
_DOUBLE_SEPARATOR_DATE_RE = re.compile(r"\b\d{4}\s*[-_/]\s*\d{1,2}\s*[-_/]{2,}\s*\d{1,4}\b")
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
_UPCOMING_RE = re.compile(r"\b(upcoming|future|next|scheduled|appointment|follow-?up|return visit)\b", re.I)
_NO_UPCOMING_RE = re.compile(
    r"\b(no|none|not|without|does not|doesn't|isn't|not documented|no documented)"
    r"\b.{0,90}\b(upcoming|future|next|appointment|follow-?up|return visit)\b",
    re.I,
)
_LAST_VISIT_RE = re.compile(r"\b(last|most recent|latest)\b.{0,30}\b(visit|encounter)\b", re.I)
_TREND_RE = re.compile(
    r"\b(trend|changed|change|increas(?:e|ed|ing)|decreas(?:e|ed|ing)|declin(?:e|ed|ing)|"
    r"improv(?:e|ed|ing)|worsen(?:ed|ing)?|gain(?:ed|ing)?|growing|growth|lost|losing|rising|falling)\b",
    re.I,
)
_UP_WORD_RE = re.compile(r"\b(increas(?:e|ed|ing)|gain(?:ed|ing)?|growing|rose|rising|up|higher|improv(?:e|ed|ing))\b", re.I)
_DOWN_WORD_RE = re.compile(r"\b(decreas(?:e|ed|ing)|declin(?:e|ed|ing)|lost|losing|fell|falling|down|lower|worsen(?:ed|ing)?)\b", re.I)
_NEGATED_UP_RE = re.compile(r"\b(not|no|isn't|is not|wasn't|was not)\b.{0,20}\b(gain(?:ed|ing)?|growing|increas(?:e|ed|ing))\b", re.I)
_WINDOW_RE = re.compile(r"\b(past|last)\s+(year|12\s+months?|6\s+months?|six\s+months?)\b", re.I)
_CONCEPT_STOP_TOKENS = {
    "and", "the", "for", "with", "from", "into", "past", "last", "year", "years",
    "month", "months", "date", "dated", "time", "times", "number", "value", "values",
    "measurement", "measurements", "current", "recent", "most", "latest",
}

# Record classes whose date is NOT a clinical visit: an administrative enrollment (a Program) carries
# its enrollment date, which can post-date the last actual visit (Aloice's TB Program "enrolled"
# 2026-05-20 when the last clinical visit was 2026-01-07). These are labeled, never reported as a visit.
_ADMIN_CLASSES = {"Program"}


def _parse_iso_date(value: Optional[str]) -> Optional[_dt.date]:
    if not value or not _ISO_RE.fullmatch(str(value)):
        return None
    try:
        return _dt.date.fromisoformat(str(value))
    except ValueError:
        return None


def _date_id(value: Optional[str]) -> Optional[str]:
    return f"D{value.replace('-', '_')}" if value and _parse_iso_date(str(value)) else None


def _shift_months(date: _dt.date, months: int) -> _dt.date:
    month_index = date.year * 12 + (date.month - 1) + months
    year, month0 = divmod(month_index, 12)
    month = month0 + 1
    day = min(date.day, calendar.monthrange(year, month)[1])
    return _dt.date(year, month, day)


def _add_date_role(roles_by_date: Dict[str, set], value: Optional[str], role: str) -> None:
    if value and _parse_iso_date(str(value)):
        roles_by_date.setdefault(str(value), set()).add(role)


def _date_ledger_entry(value: str, roles: set, reference_date: Optional[str]) -> Dict[str, Any]:
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


def _date_ledger(roles_by_date: Dict[str, set], reference_date: Optional[str]) -> List[Dict[str, Any]]:
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
        out.append({"index": int(m.group(1)), "date": last_date, "cls": _record_class(body), "body": body})
    return out


def resolve_anchor(anchor: Optional[str], chart: str) -> Optional[str]:
    """Resolve the reference 'now' (ISO date).
       - None / 'latest_record' -> the max date of a CLINICAL record (administrative enrollments are
         excluded — a Program can post-date the last visit and must not define 'now');
       - 'wall_clock' -> today's date (real clock);
       - an explicit 'YYYY-MM-DD' -> itself.
    Returns None when latest_record is requested but the chart has no dates."""
    mode = (anchor or "latest_record").strip()
    if mode == "wall_clock":
        return _dt.date.today().isoformat()
    if _ISO_RE.fullmatch(mode):
        return mode
    clinical = [e["date"] for e in parse_events(chart) if e["cls"] not in _ADMIN_CLASSES]
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
       Yes-No assessments are skipped. De-duplicated per (concept, date), keeping the first."""
    out: List[Dict[str, Any]] = []
    seen: set = set()
    last_date: Optional[str] = None
    for line in (chart or "").splitlines():
        m = _REC_RE.match(line.strip())
        if not m:
            continue
        index, rest = int(m.group(1)), m.group(2)
        dm = _DATE_PREFIX_RE.match(rest)
        if dm:  # run leader carries the date; same-date follow-ons drop it -> carry forward
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
        out.append({"index": index, "date": date, "concept": concept,
                    "value": float(nm.group(1)), "unit": nm.group(2).strip(), "raw": line.strip()})
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
        out.append({
            "index": index,
            "date": source_date,
            "source_date": source_date,
            "value_date": value_date,
            "concept": concept,
            "cls": cls,
            "raw": line.strip(),
        })
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


def _summarize_events(events: List[Dict[str, Any]], date: str, *, include_summaries: bool = True) -> Dict[str, Any]:
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


def build_temporal_facts(
    chart: str, anchor: Optional[str], *, anchor_mode: Optional[str] = None
) -> Dict[str, Any]:
    """Build the deterministic JSON sidecar the model can parse for temporal inference."""
    reference_date = resolve_anchor(anchor, chart) if (anchor and not _ISO_RE.fullmatch(anchor)) else anchor
    if reference_date is None:
        reference_date = resolve_anchor(None, chart)
    mode = anchor_mode or anchor or "latest_record"

    events = parse_events(chart)
    clinical_dates = sorted({e["date"] for e in events if e["cls"] not in _ADMIN_CLASSES}, reverse=True)
    admin_dates = sorted({e["date"] for e in events if e["cls"] in _ADMIN_CLASSES}, reverse=True)
    date_roles: Dict[str, set] = {}
    _add_date_role(date_roles, reference_date, "reference_date")
    ref_date = _parse_iso_date(reference_date)
    if ref_date:
        _add_date_role(date_roles, _shift_months(ref_date, -6).isoformat(), "past_6_months_window_start")
        _add_date_role(date_roles, _shift_months(ref_date, -12).isoformat(), "past_12_months_window_start")
    for e in events:
        _add_date_role(
            date_roles,
            e.get("date"),
            "admin_record_date" if e.get("cls") in _ADMIN_CLASSES else "clinical_record_date",
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
            if trend_supported else "none"
        )
        numeric_series.append({
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
            "delta": (most_recent["value"] - series[0]["value"]) if trend_supported else None,
        })

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
            "relation_to_reference": _relation_to_anchor(o["value_date"], reference_date),
        }
        for o in date_obs
        if "return visit" in o.get("concept", "").lower()
    ]

    appointment_terms = ("appointment", "return visit", "follow-up", "follow up", "scheduled visit")
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
            "relation_to_reference": _relation_to_anchor(o.get("value_date", ""), reference_date),
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
        "schema_version": "temporal_facts.v1.1",
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
            _summarize_events(events, clinical_dates[0]) if clinical_dates else None
        ),
        "clinical_dates": [_summarize_events(events, d, include_summaries=False) for d in clinical_dates],
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
        "schema_version": facts.get("schema_version"),
        "render_profile": "prompt_compact.v1",
        "date_output_contract": facts.get("date_output_contract"),
        "date_ledger": facts.get("date_ledger") or [],
        "anchor_mode": facts.get("anchor_mode"),
        "reference_date": facts.get("reference_date"),
        "reference_date_id": facts.get("reference_date_id"),
        "last_clinical_encounter": _compact_event_summary_for_prompt(
            facts.get("last_clinical_encounter"), include_classes=True
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
            "counts": {k: len(appt.get(k) or []) for k in ("past", "today", "future", "unknown")},
            "future": appt.get("future") or [],
            "today": appt.get("today") or [],
            "latest_past": past_appts[:5],
        },
    }
    return prompt_facts


def render_temporal_facts(facts: Dict[str, Any]) -> str:
    """Render the compact JSON sidecar as model-visible evidence."""
    if not facts:
        return ""
    prompt_facts = compact_temporal_facts_for_prompt(facts)
    return (
        "Structured temporal facts (temporal_facts.v1.1; compact deterministic JSON from the chart). "
        "For any date you write, copy an `iso` value from `date_ledger`; do not reformat "
        "or reconstruct dates from memory.\n"
        "```json\n"
        + json.dumps(prompt_facts, separators=(",", ":"))
        + "\n```"
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


def compact_temporal_facts_summary(facts: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Small trace/report summary for the otherwise-full prompt sidecar."""
    return _compact_facts_summary(facts)


def _gate_result(mode: str, status: str, checks: List[Dict[str, Any]],
                 patch_answer: Optional[str] = None,
                 patch_citations: Optional[List[int]] = None) -> Dict[str, Any]:
    return {
        "schema_version": "temporal_gate.v1",
        "mode": mode,
        "status": status,
        "checks": checks,
        "patch_answer": patch_answer,
        "patch_citations": patch_citations or [],
    }


def _add_check(checks: List[Dict[str, Any]], check_id: str, status: str, severity: str,
               claim: str, reason: str, source_indices: Optional[List[int]] = None) -> None:
    checks.append({
        "id": check_id,
        "status": status,
        "severity": severity,
        "claim": claim,
        "reason": reason,
        "source_indices": source_indices or [],
    })


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


def _latest_candidate(cands: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    return max((c for c in cands if c.get("date")), key=lambda c: c["date"], default=None)


def _allowed_iso_dates(facts: Dict[str, Any]) -> set:
    contract = facts.get("date_output_contract") or {}
    allowed = {
        str(d) for d in (contract.get("allowed_iso_dates") or [])
        if _parse_iso_date(str(d))
    }
    if allowed:
        return allowed
    return {
        str(d.get("iso")) for d in (facts.get("date_ledger") or [])
        if isinstance(d, dict) and _parse_iso_date(str(d.get("iso")))
    }


def _date_output_failures(answer: str, facts: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Find dates the model should not have emitted: malformed date-like strings and
    valid ISO dates absent from the model-visible date ledger."""
    allowed = _allowed_iso_dates(facts)
    if not allowed:
        return []
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
            "date_id_exposed", raw,
            "a date_id meant for internal matching, not a user-facing date",
        )
    extra_patterns = [
        (_NON_ASCII_ISO_TOKEN_RE, "malformed", "uses non-ASCII hyphens; copy an exact YYYY-MM-DD from date_ledger"),
        (_DOUBLE_SEPARATOR_DATE_RE, "malformed", "contains repeated date separators, not an exact YYYY-MM-DD string copied from date_ledger"),
        (_BRACKETED_DATE_RE, "malformed", "contains bracketed characters inside a date, not an exact YYYY-MM-DD string copied from date_ledger"),
        (_TRUNCATED_ISO_TOKEN_RE, "malformed", "is truncated; copy a complete YYYY-MM-DD string from date_ledger"),
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


def _selected_series(question: str, answer: str, facts: Dict[str, Any]) -> List[Dict[str, Any]]:
    hay = (question + " " + answer).lower()
    selected = []
    for s in facts.get("numeric_series") or []:
        concept = str(s.get("concept") or "")
        low = concept.lower()
        tokens = [
            t for t in re.split(r"[^a-z0-9%]+", low)
            if len(t) >= 3 and t not in _CONCEPT_STOP_TOKENS
        ]
        aliases = set(tokens)
        if "haemoglobin" in low:
            aliases.update({"hemoglobin", "hgb"})
        if "weight" in low:
            aliases.update({"wt", "growth", "gaining"})
        if "height" in low:
            aliases.update({"growth", "growing"})
        if "cd4" in low:
            aliases.add("cd4")
        if any(a in hay for a in aliases):
            selected.append(s)
    return selected


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
        return "The chart does not contain dated measurements for this temporal trend.", []
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


def _date_value_failures(answer: str, series: Dict[str, Any]) -> List[Dict[str, Any]]:
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
        dates = _ISO_TOKEN_RE.findall(sentence)
        if not dates:
            continue
        if len(dates) != 1:
            continue
        nums = []
        clean_sentence = re.sub(r"\[\d+\]", "", sentence)
        for n in _NUMBER_TOKEN_RE.findall(clean_sentence):
            try:
                nums.append(float(n))
            except ValueError:
                pass
        for date in dates:
            for val in nums:
                if val in value_dates and date not in value_dates[val]:
                    out.append({"date": date, "value": val, "expected_dates": sorted(value_dates[val])})
                elif date in by_date and val not in by_date[date] and val in value_dates:
                    out.append({"date": date, "value": val, "expected_values": sorted(by_date[date])})
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
            checks, "date_format", "fail", "block", str(failure.get("date"))[:240],
            f"The answer emits {failure.get('date')!r}, which is {failure.get('reason')}.",
            [c for c in (citations or []) if isinstance(c, int)],
        )

    if _UPCOMING_RE.search(qa):
        no_upcoming = bool(_NO_UPCOMING_RE.search(a))
        if future and no_upcoming:
            newest = _latest_candidate(future)
            _add_check(
                checks, "upcoming_date", "fail", "block", a[:240],
                "The answer says no upcoming appointment, but temporal_facts has future appointment candidates.",
                [c.get("index") for c in future if isinstance(c.get("index"), int)],
            )
            if newest:
                patch_answer = (
                    f"The chart documents a future return-visit/appointment candidate on "
                    f"{newest.get('date')} [{newest.get('index')}]."
                )
                patch_citations = [newest["index"]] if isinstance(newest.get("index"), int) else []
        if not future and no_upcoming:
            _add_check(
                checks, "upcoming_date", "pass", "warn", a[:240],
                "No future appointment candidates are present after the reference date.",
                [],
            )
        for sentence in _SENTENCE_RE.split(a):
            if not _UPCOMING_RE.search(sentence) or _NO_UPCOMING_RE.search(sentence):
                continue
            for date in _ISO_TOKEN_RE.findall(sentence):
                if ref and date <= ref:
                    indices = [c.get("index") for c in all_candidates if c.get("date") == date]
                    _add_check(
                        checks, "upcoming_date", "fail", "block", sentence[:240],
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
                            [latest_past["index"]] if isinstance(latest_past.get("index"), int) else []
                        )
        if all_candidates and re.search(r"\bappointment\b", a, re.I) and not re.search(r"return visit date", a, re.I):
            _add_check(
                checks, "upcoming_date", "warn", "warn", a[:240],
                "The chart has appointment-like date observations, not a formal Appointment resource/status.",
                [c.get("index") for c in all_candidates if isinstance(c.get("index"), int)],
            )

    last = temporal_facts.get("last_clinical_encounter") or {}
    last_date = last.get("date")
    if last_date and _LAST_VISIT_RE.search(qa):
        answer_dates = _fact_dates([{"date": d} for d in _ISO_TOKEN_RE.findall(a)])
        if answer_dates and last_date not in answer_dates:
            indices = [i for i in (last.get("indices") or []) if isinstance(i, int)]
            _add_check(
                checks, "last_visit", "fail", "block", a[:240],
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

    selected = _selected_series(q, a, temporal_facts)
    if _TREND_RE.search(qa) and selected:
        answer_dir = _answer_direction(a)
        for s in selected:
            indices = [
                p.get("index") for p in (s.get("points") or []) if isinstance(p.get("index"), int)
            ]
            if not s.get("trend_supported"):
                _add_check(
                    checks, "single_point_trend", "fail", "block", a[:240],
                    f"{s.get('concept')} has fewer than two dated points, so no trend is supported.",
                    indices,
                )
                if not patch_answer:
                    patch_answer, patch_citations = _series_patch(s)
            elif answer_dir and s.get("direction") in {"up", "down"} and answer_dir != s.get("direction"):
                _add_check(
                    checks, "trend_direction", "fail", "block", a[:240],
                    f"The answer direction is {answer_dir}, but the computed {s.get('concept')} direction is {s.get('direction')}.",
                    indices,
                )
                if not patch_answer:
                    patch_answer, patch_citations = _series_patch(s)
            if _WINDOW_RE.search(qa):
                window = s.get("window") or {}
                _add_check(
                    checks, "window_scope", "warn", "warn", a[:240],
                    f"Answer/query names a strict window, but the documented {s.get('concept')} window is {window.get('start')}..{window.get('end')}.",
                    indices,
                )

    if any(c.get("id") == "date_format" and c.get("status") == "fail" for c in checks):
        if not patch_answer and len(selected) == 1:
            patch_answer, patch_citations = _series_patch(selected[0])

    for s in selected:
        for failure in _date_value_failures(a, s)[:3]:
            _add_check(
                checks, "date_value_binding", "fail", "block", a[:240],
                f"The answer binds value {failure['value']} to {failure['date']}, but temporal_facts bind it to {failure.get('expected_dates') or failure.get('expected_values')}.",
                [p.get("index") for p in (s.get("points") or []) if isinstance(p.get("index"), int)],
            )
            if not patch_answer:
                patch_answer, patch_citations = _series_patch(s)

    return _gate_result(normalized_mode, _final_gate_status(checks), checks, patch_answer, patch_citations)


def build_temporal_block(chart: str, anchor: Optional[str]) -> str:
    """The injected evidence block: the anchor line + per-concept series. '' when there's neither
       an anchor nor any series. The synth/validator read 'most recent' and trends FROM here."""
    obs = parse_dated_observations(chart)
    if not anchor and not obs:
        return ""
    lines: List[str] = []
    if anchor:
        lines.append(
            f"Current date: {anchor}. Interpret \"current\", \"recent\", and \"most recent\" "
            f"relative to THIS date; the patient's records may predate it. Use the dated series "
            f"below verbatim — do not infer dates, values, or trends not shown."
        )
    # Typed event timeline: "last visit"/"most recent" must resolve to a clinical visit/encounter,
    # NOT the chart's max date (which can be an administrative Program enrollment that post-dates the
    # last visit). Type each record by class; report the most-recent CLINICAL date + the visit dates,
    # and list administrative records separately so they are never mistaken for a visit.
    events = parse_events(chart)
    visit_dates = sorted({e["date"] for e in events if e["cls"] not in _ADMIN_CLASSES}, reverse=True)
    if visit_dates:
        lines.append(
            f"Most recent clinical visit/encounter: {visit_dates[0]}. Answer \"last visit\" / "
            f"\"most recent visit\" from THIS date — the administrative records below are NOT visits."
        )
        shown = visit_dates[:12]
        more = f" (+{len(visit_dates) - 12} earlier)" if len(visit_dates) > 12 else ""
        lines.append("Clinical visit/encounter dates (newest first): " + ", ".join(shown) + more + ".")
    seen_admin: set = set()
    for e in sorted((e for e in events if e["cls"] in _ADMIN_CLASSES), key=lambda e: e["date"], reverse=True):
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
            lines.append(f"- {concept}: {last['value']}{unit} ({last['date']}) — single measurement.")
            continue
        first = series[0]
        vals = [o["value"] for o in series]
        direction = "↑" if last["value"] > first["value"] else ("↓" if last["value"] < first["value"] else "→")
        lines.append(
            f"- {concept}: most recent {last['value']}{unit} ({last['date']}); "
            f"{len(series)} values {first['value']}→{last['value']} {direction} "
            f"over {first['date']}…{last['date']}; range {min(vals)}–{max(vals)}."
        )
    return "\n".join(lines)
