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
_LEADING_NUM_RE = re.compile(r"^([+-]?\d+(?:\.\d+)?)\s*(.*)$")

# Record classes whose date is NOT a clinical visit: an administrative enrollment (a Program) carries
# its enrollment date, which can post-date the last actual visit (Aloice's TB Program "enrolled"
# 2026-05-20 when the last clinical visit was 2026-01-07). These are labeled, never reported as a visit.
_ADMIN_CLASSES = {"Program"}


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
        out.append({"date": last_date, "cls": _record_class(body), "body": body})
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
