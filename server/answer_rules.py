"""Deterministic answer rules for the faults the judges find (umbrella spec ANSWER-RULES-2026-10).

Each rule reads only the chart's records (``temporal_facts["records"]``), the temporal facts and
the reference date. A rule either edits the answer with chart-derived text (drops a row, line or
sentence whose cited records fall outside the asked window, corrects a drug citation, appends a
sentence built from a series) and reports an ``info`` finding, or marks a contradiction it can
prove with a ``fail`` finding that names the chart's value. No rule rewrites a clinical statement.

Every rule is general or reads its parameters from the chart: no drug, disease or measurement
list is built in. The series rules take the chart's own measuring rhythm and the words that pick
out one measurement among the chart's series; the drug rule takes the chart's drug orders.

Findings have the gate-check shape (``id``, ``status``, ``severity``, ``claim``, ``reason``,
``source_indices``) and ids ``rule_<name>``. Claim-level marks (``claim_marks``) serve the Answer's
sentences and each In-Depth point; the edits (``apply_answer_rules``) serve the Answer only.
"""

from __future__ import annotations

import copy
import datetime as _dt
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from . import temporal

_MARKER_RE = re.compile(r"\[(\d+)\]")
_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9\"'(])|\n+")
_LINE_ITEM_RE = re.compile(r"^\s*(?:[-*•]|\d+[.)])\s+")
_NUMBER_WORDS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8,
    "nine": 9, "ten": 10, "eleven": 11, "twelve": 12, "eighteen": 18, "few": 3, "several": 3,
}
_SPAN_RE = re.compile(
    r"\b(?:past|last|previous)\s+(?:(?P<n>\d+|one|two|three|four|five|six|seven|eight|nine|ten|"
    r"eleven|twelve|eighteen|few|several)\s+)?(?P<unit>day|week|month|year)s?\b",
    re.I,
)

_DRUG_STOP_TOKENS = {"and", "with", "tablet", "tablets", "oral", "plus", "fixed", "dose", "vaccine", "vaccination"}
# An answer that already says nothing falls inside the asked window, or places a record before it.
_WINDOW_STATED_RE = re.compile(
    r"\bno (?:records?|data|measurements?|entries|orders?|visits?|encounters?|changes?)\b.{0,80}"
    r"\b(?:within|in|during|since)\b|\bmore than\b.{0,40}\b(?:before|ago|prior)\b|"
    r"\b(?:older than|outside the|before the (?:asked|requested))\b",
    re.I,
)
_DEMOGRAPHIC_WORDS_RE = re.compile(
    r"\b(?:name|named|female|male|sex|gender|born|birth|age|aged|years? old|address|lives|"
    r"identifier|identification|ID)\b",
    re.I,
)
_DIAGNOSIS_DENIAL_RE = re.compile(
    r"\b(?:does not|doesn't|do not|did not|not)\s+(?:explicitly\s+|clearly\s+)?(?:state|list|"
    r"document|record|mention|specify|include|name|identify)\w*\s+(?:a|any|the|an)?\s*"
    r"(?:primary\s+|specific\s+|main\s+|explicit\s+|clear\s+)?diagnos|"
    r"\bno (?:documented |recorded |explicit |primary |specific |stated )?diagnos[ie]s\b",
    re.I,
)
_STEADY_RE = re.compile(r"\b(?:steady|steadily|consistent(?:ly)?|continuous(?:ly)?)\b", re.I)
_GAP_STATED_RE = re.compile(
    r"\bno (?:later|more recent|newer|subsequent|further)\b|\bmonths? (?:before|ago|old|earlier)\b|"
    r"\bnot (?:been )?(?:re-?)?(?:measured|recorded|documented) since\b|\bout of date\b|\boutdated\b",
    re.I,
)


def _finding(rule: str, status: str, claim: str, reason: str, indices: Iterable[int]) -> Dict[str, Any]:
    return {
        "id": f"rule_{rule}",
        "status": status,
        "severity": "edit" if status == "info" else "block",
        "claim": (claim or "")[:240],
        "reason": reason,
        "source_indices": sorted({i for i in indices if isinstance(i, int)}),
    }


def _records(facts: Optional[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    return {
        r["index"]: r
        for r in (facts or {}).get("records") or []
        if isinstance(r, dict) and isinstance(r.get("index"), int)
    }


def _date(value: Any) -> Optional[_dt.date]:
    return temporal._parse_iso_date(value) if value else None


def _cites(text: str) -> List[int]:
    return [int(m) for m in _MARKER_RE.findall(text or "")]


def _span_start(match: re.Match, reference: _dt.date) -> _dt.date:
    raw = (match.group("n") or "one").lower()
    n = int(raw) if raw.isdigit() else _NUMBER_WORDS.get(raw, 1)
    unit = match.group("unit").lower()
    if unit == "day":
        return reference - _dt.timedelta(days=n)
    if unit == "week":
        return reference - _dt.timedelta(weeks=n)
    return temporal._shift_months(reference, -n * (12 if unit == "year" else 1))


def _span_words(match: re.Match) -> str:
    return " ".join(match.group(0).split()).lower()


def asked_window(question: str, facts: Optional[Dict[str, Any]]) -> Optional[Tuple[_dt.date, str]]:
    """The first date the question's "past N months/year/weeks" window admits, and its words."""
    reference = _date((facts or {}).get("reference_date"))
    match = _SPAN_RE.search(question or "")
    if not reference or not match:
        return None
    return _span_start(match, reference), _span_words(match)


def _drug_name(record: Dict[str, Any]) -> Optional[str]:
    if record.get("cls") != "Drug order":
        return None
    body = str(record.get("body") or "")
    name = body.split(":", 1)[1] if ":" in body else body
    return name.split(". ", 1)[0].strip(" .") or None


def _drug_tokens(name: str) -> set:
    """The words that name a drug order: its long words and, for a name of several words, their
    initials ("Diphtheria tetanus and pertussis vaccination" is also "dtp", "Hemophilus influenza
    B vaccine" also "hib"), so an answer's abbreviation still names the order."""
    words = [w for w in re.split(r"[^a-z]+", (name or "").lower()) if w and w not in _DRUG_STOP_TOKENS]
    tokens = {w for w in words if len(w) >= 5}
    if len(words) >= 2:
        tokens.add("".join(w[0] for w in words))
    return tokens


def _words(text: str) -> set:
    return set(re.split(r"[^a-z]+", (text or "").lower()))


def _clusters(text: str) -> List[Tuple[int, int, str, List[Tuple[int, int, int]]]]:
    """Citation groups with the item text before each: (text_start, group_end, item_text,
    [(index, marker_start, marker_end), ...]). Adjacent markers form one group."""
    out: List[Tuple[int, int, str, List[Tuple[int, int, int]]]] = []
    previous_end = 0
    group: List[Tuple[int, int, int]] = []
    group_text_start = 0
    for match in _MARKER_RE.finditer(text or ""):
        if group and not re.search(r"\w", text[group[-1][2] : match.start()]):
            group.append((int(match.group(1)), match.start(), match.end()))
            continue
        if group:
            out.append((group_text_start, group[-1][2], text[group_text_start : group[0][1]], group))
            previous_end = group[-1][2]
        group = [(int(match.group(1)), match.start(), match.end())]
        group_text_start = previous_end
    if group:
        out.append((group_text_start, group[-1][2], text[group_text_start : group[0][1]], group))
    return out


# --- claim-level marks (Answer sentences and In-Depth points) -------------------------------------


def _relative_time_marks(claim: str, records: Dict[int, Dict[str, Any]], reference: Optional[_dt.date]) -> List[Dict[str, Any]]:
    if not reference:
        return []
    cited = [i for i in _cites(claim) if i in records]
    dates = sorted(d for d in (_date(records[i].get("date")) for i in cited) if d)
    if not dates:
        return []
    bodies = " ".join(str(records[i].get("body") or "") for i in cited).lower()
    for match in _SPAN_RE.finditer(claim):
        # "the past week on 2026-01-17" anchors the span to a stated date, not the reference date.
        near = claim[max(0, match.start() - 25) : match.end() + 40]
        if re.search(r"\d{4}-\d{2}-\d{2}|\bas of\b", near):
            continue
        # "previous months" names no span; "adherence in the past week [11]" quotes the record's
        # own question ("Antiretroviral adherence in past week: Most").
        if not match.group("n") and match.group(0).lower().endswith("s"):
            continue
        if f"{match.group(0).split()[0].lower()} {match.group('unit').lower()}" in bodies or (
            f"past {match.group('unit').lower()}" in bodies or f"last {match.group('unit').lower()}" in bodies
        ) and not match.group("n"):
            continue
        start = _span_start(match, reference)
        if dates[-1] < start:
            span = _span_words(match)
            return [
                _finding(
                    "relative_time",
                    "fail",
                    claim,
                    f"the cited records are from {dates[0].isoformat()}"
                    + (f" to {dates[-1].isoformat()}" if dates[-1] != dates[0] else "")
                    + f", not the {span} before {reference.isoformat()}",
                    cited,
                )
            ]
    return []


def _drug_citation_marks(claim: str, records: Dict[int, Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    names = {i: _drug_name(r) for i, r in records.items()}
    chart_tokens = set().union(*(_drug_tokens(n) for n in names.values() if n)) if names else set()
    for _start, _end, item, group in _clusters(claim):
        named = _words(item) & chart_tokens
        if not named:
            continue
        for index, _ms, _me in group:
            name = names.get(index)
            if name and not (_drug_tokens(name) & named):
                out.append(
                    _finding(
                        "drug_citation",
                        "fail",
                        claim,
                        f"[{index}] is the order for {name}, not {', '.join(sorted(named))}",
                        [index],
                    )
                )
    return out


def _demographics_marks(claim: str, records: Dict[int, Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for _start, _end, item, group in _clusters(claim):
        indices = [i for i, _s, _e in group]
        if indices and all(records.get(i, {}).get("cls") == "Patient" for i in indices):
            if not _DEMOGRAPHIC_WORDS_RE.search(item):
                out.append(
                    _finding(
                        "demographics_citation",
                        "fail",
                        claim,
                        f"[{indices[0]}] is the patient demographics record and states no clinical fact",
                        indices,
                    )
                )
    return out


def _diagnosis_marks(claim: str, records: Dict[int, Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not _DIAGNOSIS_DENIAL_RE.search(claim):
        return []
    named = [
        r
        for r in records.values()
        if (r.get("cls") == "Program" and "status: active" in str(r.get("body")).lower())
        or "diagnos" in str(r.get("cls") or "").lower()
        or "problem" in str(r.get("cls") or "").lower()
    ]
    if not named:
        return []
    first = min(named, key=lambda r: r["index"])
    condition = str(first.get("body") or "").split(":", 1)[-1].split(". ", 1)[0].strip()
    return [
        _finding(
            "diagnosis_presence",
            "fail",
            claim,
            f"[{first['index']}] records {condition}",
            [r["index"] for r in named],
        )
    ]


def claim_marks(claim: str, facts: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Contradictions one claim makes with the chart: relative time, drug citation, demographics
    citation and diagnosis presence."""
    records = _records(facts)
    if not claim or not records:
        return []
    reference = _date((facts or {}).get("reference_date"))
    return (
        _relative_time_marks(claim, records, reference)
        + _drug_citation_marks(claim, records)
        + _demographics_marks(claim, records)
        + _diagnosis_marks(claim, records)
    )


# --- answer edits ---------------------------------------------------------------------------------


def _all_before(indices: Sequence[int], records: Dict[int, Dict[str, Any]], start: _dt.date) -> bool:
    dates = [_date(records[i].get("date")) for i in indices if i in records]
    return bool(dates) and all(d and d < start for d in dates)


def _row_refs(row: Any) -> List[int]:
    refs: List[int] = []

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if key == "refs" and isinstance(child, list):
                    refs.extend(i for i in child if isinstance(i, int))
                else:
                    walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(row)
    return refs


def _window_edits(
    question: str, text: str, blocks: List[Any], records: Dict[int, Dict[str, Any]], facts: Dict[str, Any]
) -> Tuple[str, List[Any], List[Dict[str, Any]]]:
    window = asked_window(question, facts)
    if not window:
        return text, blocks, []
    start, words = window
    dropped: List[int] = []
    findings: List[Dict[str, Any]] = []
    cited_anywhere = [i for i in _cites(text) + _row_refs(blocks) if i in records]
    if cited_anywhere and _all_before(cited_anywhere, records, start):
        # Nothing the answer cites falls inside the window: leaving it out would leave no answer.
        # Does the chart hold records of the kinds the answer cites inside the window (up to the
        # reference date)? Then the answer missed them; otherwise the chart has nothing newer.
        reference = _date(facts.get("reference_date")) or start
        kinds = {records[i].get("cls") for i in cited_anywhere}
        in_window = sorted(
            (
                r
                for r in records.values()
                if r.get("cls") in kinds
                and r.get("clinical", True)
                and start <= (_date(r.get("date")) or _dt.date.min) <= reference
            ),
            key=lambda r: (str(r.get("date") or ""), -r["index"]),
        )
        if in_window:
            latest = in_window[-1]
            findings.append(
                _finding(
                    "window",
                    "fail",
                    text,
                    f"the chart has {len(in_window)} {latest.get('cls', 'record').lower()} record"
                    f"{'s' if len(in_window) != 1 else ''} of the kinds the answer cites dated within the "
                    f"{words} (latest {latest['date']} [{latest['index']}]); the answer cites none of them",
                    [r["index"] for r in in_window[-5:]],
                )
            )
            return text, blocks, findings
        if _WINDOW_STATED_RE.search(text):
            return text, blocks, []
        newest = max(cited_anywhere, key=lambda i: (str(records[i].get("date") or ""), -i))
        lead = (
            f"The chart has no records of these kinds dated within the {words} (from "
            f"{start.isoformat()}); the records below are older, the latest from {records[newest]['date']}."
        )
        findings.append(
            _finding("window", "info", question, f"the chart has no clinical record dated on or after {start.isoformat()}", cited_anywhere)
        )
        return lead + " " + text, blocks, findings

    new_blocks = copy.deepcopy(blocks)
    for block in new_blocks if isinstance(new_blocks, list) else []:
        rows = block.get("rows") if isinstance(block, dict) else None
        if not isinstance(rows, list):
            continue
        kept = []
        for row in rows:
            refs = _row_refs(row)
            if _all_before(refs, records, start):
                dropped.extend(refs)
            else:
                kept.append(row)
        block["rows"] = kept

    pieces: List[str] = []
    for line in text.split("\n"):
        if not line.strip():
            pieces.append(line)
            continue
        if _LINE_ITEM_RE.match(line) and _all_before(_cites(line), records, start):
            dropped.extend(_cites(line))
            continue
        sentences = re.split(r"(?<=[.!?])\s+", line)
        kept_sentences = []
        for sentence in sentences:
            cited = _cites(sentence)
            if cited and _all_before(cited, records, start) and not _WINDOW_STATED_RE.search(sentence):
                dropped.extend(cited)
                continue
            # Within a sentence with in-window records, a citation group keeps only its in-window
            # records; a group with none left keeps its item text and loses its citations.
            for _gs, _ge, _item, group in reversed(_clusters(sentence)):
                indices = [i for i, _s, _e in group]
                late = [i for i in indices if not _all_before([i], records, start)]
                if late and len(late) < len(indices):
                    dropped.extend(i for i in indices if i not in late)
                    sentence = (
                        sentence[: group[0][1]]
                        + "".join(f"[{i}]" for i in late)
                        + sentence[group[-1][2] :]
                    )
            kept_sentences.append(sentence)
        joined = " ".join(s for s in kept_sentences if s.strip())
        if joined:
            pieces.append(joined)
    new_text = "\n".join(pieces).strip("\n")

    if dropped:
        n = len(set(dropped))
        note = (
            f"Records dated before {start.isoformat()}, outside the {words}, are left out "
            f"({n} record{'s' if n != 1 else ''})."
        )
        new_text = new_text.rstrip() + (" " if new_text.strip() else "") + note
        findings.append(
            _finding("window", "info", question, f"left out {n} cited records dated before {start.isoformat()}", dropped)
        )
    return new_text, new_blocks, findings


def _drug_citation_edits(text: str, records: Dict[int, Dict[str, Any]], answer_cites: set) -> Tuple[str, List[Dict[str, Any]]]:
    names = {i: _drug_name(r) for i, r in records.items()}
    chart_tokens = set().union(*(_drug_tokens(n) for n in names.values() if n)) if names else set()
    findings: List[Dict[str, Any]] = []
    for _start, _end, item, group in reversed(_clusters(text)):
        named = _words(item) & chart_tokens
        if not named:
            continue
        indices = [i for i, _s, _e in group]
        wrong = [i for i in indices if names.get(i) and not (_drug_tokens(names[i]) & named)]
        if not wrong:
            continue
        right = [i for i in indices if i not in wrong]
        if not right:
            candidates = [i for i, n in names.items() if n and _drug_tokens(n) & named]
            if not candidates:
                continue
            preferred = [i for i in candidates if i in answer_cites] or candidates
            right = [max(preferred, key=lambda i: (str(records[i].get("date") or ""), -i))]
        text = text[: group[0][1]] + "".join(f"[{i}]" for i in right) + text[group[-1][2] :]
        findings.append(
            _finding(
                "drug_citation",
                "info",
                item.strip(),
                "cited "
                + ", ".join(f"[{i}] ({names[i]})" for i in wrong)
                + f" for {', '.join(sorted(named))}; replaced by "
                + ", ".join(f"[{i}]" for i in right),
                wrong + right,
            )
        )
    return text, findings


def _names_series(text: str, series: Dict[str, Any], shared: set) -> bool:
    """Whether ``text`` names the measurement by a word that picks it out in this chart: "white",
    "oxygen", "weight", "growing". A word several measurements share ("blood" in white blood cells,
    blood pressure and blood oxygen) names none of them."""
    _phrase, aliases = temporal._series_terms(series)
    return bool((aliases - shared) & set(re.split(r"[^a-z0-9%]+", (text or "").lower())))


def _shared_series_words(facts: Dict[str, Any]) -> set:
    counts: Dict[str, int] = {}
    for series in facts.get("numeric_series") or []:
        for token in temporal._series_terms(series)[1]:
            counts[token] = counts.get(token, 0) + 1
    return {token for token, n in counts.items() if n >= 3}


def _series_in_answer(question: str, answer: str, facts: Dict[str, Any]) -> List[Dict[str, Any]]:
    """The series the question asks about that the answer also mentions; none when the question
    is not about a measurement (an orders list that mentions a CD4 panel gets no series rule)."""
    shared = _shared_series_words(facts)
    return [
        series
        for series in temporal._selected_series(question, "", facts)
        if _names_series(question, series, shared) and _names_series(answer, series, shared)
    ]


def _overdue(series: Dict[str, Any], reference: _dt.date) -> bool:
    """Whether the gap from the series' last reading to the reference date is longer than any
    interval between its own readings: the chart's measuring rhythm, not a fixed cutoff, says
    when a value is old. A series with one reading has no rhythm and is never called overdue."""
    dates = sorted(d for d in (_date(p.get("date")) for p in series.get("points") or []) if d)
    if len(dates) < 2:
        return False
    longest = max((b - a).days for a, b in zip(dates, dates[1:]))
    return (reference - dates[-1]).days > max(longest, 1)


def _points_in_window(series: Dict[str, Any], start: Optional[_dt.date]) -> List[Dict[str, Any]]:
    points = [p for p in series.get("points") or [] if _date(p.get("date"))]
    return [p for p in points if not start or _date(p["date"]) >= start]


def _reading(point: Dict[str, Any]) -> str:
    unit = f" {point['unit']}" if point.get("unit") else ""
    return f"{temporal._format_value(point.get('value'))}{unit} ({point.get('date')}) [{point.get('index')}]"


def _series_edits(question: str, text: str, facts: Dict[str, Any]) -> Tuple[str, List[int], List[Dict[str, Any]]]:
    """R3 stale latest value and R4 readings and shape, for the series the answer is about."""
    reference = _date(facts.get("reference_date"))
    series_list = _series_in_answer(question, text, facts) if reference else []
    if not series_list:
        return text, [], []
    window = asked_window(question, facts)
    start = window[0] if window else None
    trend_question = bool(temporal._TREND_RE.search(question or ""))
    appended: List[str] = []
    added: List[int] = []
    findings: List[Dict[str, Any]] = []
    stale_by_date: Dict[str, List[Dict[str, Any]]] = {}
    for series in series_list:
        concept = str(series.get("concept") or "").split(" (")[0]
        points = _points_in_window(series, start)
        if trend_question and len(points) >= 3:
            values = {round(float(p["value"]), 3) for p in points if isinstance(p.get("value"), (int, float))}
            stated = {
                round(float(m.group()), 3)
                for m in re.finditer(r"(?<![\d.\[])\d+(?:\.\d+)?(?![\d\]])", re.sub(r"\d{4}-\d{2}-\d{2}", " ", text))
            }
            ordered = [float(p["value"]) for p in points]
            steps = [b - a for a, b in zip(ordered, ordered[1:]) if b != a]
            changes_direction = any(x * y < 0 for x, y in zip(steps, steps[1:]))
            if changes_direction and _STEADY_RE.search(text):
                findings.append(
                    _finding(
                        "series_shape",
                        "fail",
                        text,
                        f"the {concept.lower()} readings go up and down: "
                        + ", ".join(temporal._format_value(v) for v in ordered),
                        [p["index"] for p in points],
                    )
                )
            if len(values & stated) <= 2:
                shown = points if len(points) <= 8 else [points[0], min(points, key=lambda p: p["value"]), max(points, key=lambda p: p["value"]), points[-1]]
                lead = (
                    f"Recorded {concept.lower()} readings: "
                    if len(points) <= 8
                    else f"{len(points)} {concept.lower()} readings; first, lowest, highest and last: "
                )
                appended.append(lead + ", ".join(_reading(p) for p in shown) + ".")
                added.extend(p["index"] for p in shown)
                findings.append(
                    _finding("series", "info", text, f"added {len(shown)} {concept.lower()} readings", [p["index"] for p in shown])
                )
        last = series.get("most_recent") or {}
        last_date = _date(last.get("date"))
        if last_date and not _GAP_STATED_RE.search(text) and _overdue(series, reference):
            stale_by_date.setdefault(last["date"], []).append(series)
    for date, group in sorted(stale_by_date.items()):
        last_date = _date(date)
        months = round((reference - last_date).days / 30.44 * 2) / 2
        concepts = " and ".join(str(s.get("concept") or "").split(" (")[0].lower() for s in group)
        indices = [s["most_recent"]["index"] for s in group]
        appended.append(
            f"The latest {concepts} in the chart {'is' if len(group) == 1 else 'are'} from {date} "
            + "".join(f"[{i}]" for i in indices)
            + f", about {temporal._format_value(months)} months before {reference.isoformat()}."
        )
        added.extend(indices)
        findings.append(
            _finding("stale", "info", text, f"the latest {concepts} is from {date}, {temporal._format_value(months)} months before the reference date", indices)
        )
    if appended:
        text = text.rstrip() + " " + " ".join(appended)
    return text, added, findings


def apply_answer_rules(
    question: str,
    answer_text: str,
    citations: List[int],
    blocks: List[Any],
    facts: Optional[Dict[str, Any]],
    *,
    edit: bool = True,
) -> Tuple[str, List[int], List[Any], List[Dict[str, Any]]]:
    """Run every rule on the Answer. Returns the (possibly edited) answer, its citations, its
    blocks and the findings. With ``edit`` False the findings are reported and nothing changes."""
    records = _records(facts)
    if not records or not (answer_text or blocks):
        return answer_text, citations, blocks, []
    text, new_blocks, findings = _window_edits(question, answer_text or "", blocks or [], records, facts or {})
    text, drug_findings = _drug_citation_edits(text, records, set(citations or []) | set(_cites(answer_text)))
    findings += drug_findings
    text, added, series_findings = _series_edits(question, text, facts or {})
    findings += series_findings
    for sentence in _SENTENCE_RE.split(text):
        findings += [
            f
            for f in claim_marks(sentence, facts)
            if f["id"] not in {"rule_drug_citation"}
        ]
    if not edit:
        return answer_text, citations, blocks, [f for f in findings if f["status"] == "fail"]
    kept = set(_cites(text))
    new_citations = [c for c in (citations or []) if c in kept or c in set(_row_refs(new_blocks))]
    new_citations += [c for c in sorted(kept) if c not in new_citations]
    new_citations += [c for c in added if c not in new_citations]
    return text, new_citations, new_blocks, findings
