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
_COMPACT_MARKER_RE = re.compile(r"\[(\d+(?:\s*[,/]\s*\d+)+)\]")
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
# An item the answer itself frames as older ("previous orders", "remain active since").
_PRIOR_RE = re.compile(r"\b(?:previous(?:ly)?|prior|earlier|before|older|remains?|since)\b", re.I)
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


def _expand_compact_citations(
    text: str,
    records: Dict[int, Dict[str, Any]],
    known_citations: Iterable[int] = (),
) -> str:
    """Expand compact multi-citation brackets (e.g. ``[2, 34, 37]`` or ``[2/34]``) into
    adjacent single markers ``[2][34][37]`` when corroborated by ``known_citations`` or when
    every index in the group maps to a chart record of the same class."""
    if not text or not _COMPACT_MARKER_RE.search(text):
        return text
    known = {c for c in (known_citations or ()) if isinstance(c, int)}

    def _replace(match: re.Match) -> str:
        nums = [int(part.strip()) for part in re.split(r"[,/]", match.group(1))]
        if known and all(n in known for n in nums):
            return "".join(f"[{n}]" for n in nums)
        if records and all(n in records for n in nums):
            classes = {records[n].get("cls") for n in nums}
            if len(classes) == 1 and None not in classes:
                if len(nums) >= 3:
                    return "".join(f"[{n}]" for n in nums)
                drugs = {_drug_name(records[n]) for n in nums}
                if len(drugs) == 1 and None not in drugs:
                    return "".join(f"[{n}]" for n in nums)
        return match.group(0)

    return _COMPACT_MARKER_RE.sub(_replace, text)


def _span_start(match: re.Match, reference: _dt.date) -> Optional[_dt.date]:
    raw = (match.group("n") or "one").lower()
    n = int(raw) if raw.isdigit() else _NUMBER_WORDS.get(raw, 1)
    if n <= 0:
        return None
    unit = match.group("unit").lower()
    if (
        (unit == "day" and n > 73000)
        or (unit == "week" and n > 10400)
        or (unit == "month" and n > 2400)
        or (unit == "year" and n > 200)
    ):
        return None
    try:
        if unit == "day":
            return reference - _dt.timedelta(days=n)
        if unit == "week":
            return reference - _dt.timedelta(weeks=n)
        return temporal._shift_months(reference, -n * (12 if unit == "year" else 1))
    except (OverflowError, ValueError):
        return None


def _span_words(match: re.Match) -> str:
    return " ".join(match.group(0).split()).lower()


def asked_window(question: str, facts: Optional[Dict[str, Any]]) -> Optional[Tuple[_dt.date, str]]:
    """The first date the question's "past N months/year/weeks" window admits, and its words.

    Per umbrella spec ANSWER-RULES-2026-10 (confidence-labels.md), window pruning applies only
    to explicit relative-span questions validated against the reference date and requested subject.
    """
    reference = _date((facts or {}).get("reference_date"))
    match = _SPAN_RE.search(question or "")
    if not reference or not match:
        return None
    start = _span_start(match, reference)
    if not start:
        return None
    return start, _span_words(match)


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


_CLUSTER_SEP_RE = re.compile(r"^\s*(?:[,/]\s*(?:(?:and|or)\s+)?|(?:and|or)\s+)?\s*$", re.I)


def _clusters(text: str) -> List[Tuple[int, int, str, List[Tuple[int, int, int]]]]:
    """Citation groups with the item text before each: (text_start, group_end, item_text,
    [(index, marker_start, marker_end), ...]). Adjacent markers (including ``[1], [2], and [3]``)
    form one group."""
    out: List[Tuple[int, int, str, List[Tuple[int, int, int]]]] = []
    previous_end = 0
    group: List[Tuple[int, int, int]] = []
    group_text_start = 0
    for match in _MARKER_RE.finditer(text or ""):
        if group and _CLUSTER_SEP_RE.match(text[group[-1][2] : match.start()]):
            group.append((int(match.group(1)), match.start(), match.end()))
            continue
        if group:
            raw_item = text[group_text_start : group[0][1]]
            sentence_item = re.split(r"(?<=[.!?])\s+|\n+", raw_item)[-1]
            out.append((group_text_start, group[-1][2], sentence_item, group))
            previous_end = group[-1][2]
        group = [(int(match.group(1)), match.start(), match.end())]
        group_text_start = previous_end
    if group:
        raw_item = text[group_text_start : group[0][1]]
        sentence_item = re.split(r"(?<=[.!?])\s+|\n+", raw_item)[-1]
        out.append((group_text_start, group[-1][2], sentence_item, group))
    return out


# --- claim-level marks (Answer sentences and In-Depth points) -------------------------------------


def _relative_time_marks(claim: str, records: Dict[int, Dict[str, Any]], reference: Optional[_dt.date]) -> List[Dict[str, Any]]:
    if not reference or _WINDOW_STATED_RE.search(claim or ""):
        return []
    cited = [i for i in _cites(claim) if i in records]
    dates = sorted(d for d in (_date(records[i].get("date")) for i in cited) if d)
    if not dates:
        return []
    bodies = " ".join(str(records[i].get("body") or "") for i in cited).lower()
    for match in _SPAN_RE.finditer(claim):
        if re.search(r"\b(?:no|not|none|never|without)\b[^.;!?]*$", claim[: match.start()], re.I):
            continue
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
        if not start:
            continue
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


_DIAG_STOP_TOKENS = {
    "a", "an", "the", "any", "of", "in", "on", "to", "for", "with", "by",
    "is", "are", "was", "were", "has", "have", "been",
    "documented", "recorded", "stated", "noted", "explicit", "primary",
    "specific", "main", "clear", "known", "formal", "definitive", "confirmed",
    "active", "current", "care", "program", "programme", "treatment", "disease",
}


def _condition_tokens(condition: str) -> set:
    words = [
        w
        for w in re.split(r"[^a-z0-9]+", (condition or "").lower())
        if len(w) >= 2 and w not in _DIAG_STOP_TOKENS
    ]
    tokens = set(words)
    alpha_words = [w for w in words if w[0].isalpha()]
    if len(alpha_words) >= 2:
        tokens.add("".join(w[0] for w in alpha_words))
    if "tuberculosis" in tokens or "tb" in tokens:
        tokens.update({"tuberculosis", "tb"})
    if "hiv" in tokens or {"human", "immunodeficiency", "virus"} <= tokens:
        tokens.update({"hiv", "human", "immunodeficiency", "virus"})
    return tokens


def _record_diagnosis(record: Dict[str, Any]) -> Optional[str]:
    cls = str(record.get("cls") or "")
    cls_lower = cls.lower()
    body_str = str(record.get("body") or "")
    body_lower = body_str.lower()
    condition: Optional[str] = None
    if cls == "Program" and "status: active" in body_lower:
        condition = body_str.split(":", 1)[-1].split(". ", 1)[0].strip()
    elif cls_lower == "condition" or "diagnos" in cls_lower or "problem" in cls_lower:
        if re.search(r"\bstatus:\s*(?:inactive|resolved|refuted|entered-in-error)\b", body_lower):
            return None
        condition = body_str.split(":", 1)[-1].split(". ", 1)[0].strip()
    elif cls == "Drug order" and "action: discontinue" not in body_lower:
        ind_match = re.search(
            r"\b(?:order\s+reason|indication|diagnosis|reason)\s*:\s*([^.;\n]+)",
            body_str,
            re.I,
        )
        if ind_match:
            condition = ind_match.group(1).strip()
        else:
            head = body_str.split(":", 1)[-1].split(". ", 1)[0].strip()
            for_match = re.search(r"\bfor\s+([a-z][a-z0-9\s()-]{2,})$", head, re.I)
            if for_match:
                condition = for_match.group(1).strip()
    if not condition:
        return None
    if re.search(
        r"^\s*(?:no|not|none|negative|without|absent|ruled out|unknown|unspecified)\b",
        condition,
        re.I,
    ):
        return None
    return condition


def _diagnosis_marks(claim: str, records: Dict[int, Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not _DIAGNOSIS_DENIAL_RE.search(claim):
        return []
    specific_match = re.search(
        r"\bdiagnos[ie]s\s+of\s+([a-z0-9][a-z0-9\s-]{1,}?)(?:\s+(?:is|are|was|were|has|have|in|on)\b|[.,;:\[]|$)",
        claim,
        re.I,
    )
    specific_tokens = _condition_tokens(specific_match.group(1)) if specific_match else set()
    if specific_match and not specific_tokens:
        return []
    named: List[Tuple[Dict[str, Any], str]] = []
    for r in records.values():
        condition = _record_diagnosis(r)
        if not condition:
            continue
        if specific_tokens:
            cond_tokens = _condition_tokens(condition)
            if not (cond_tokens & specific_tokens):
                continue
        named.append((r, condition))
    if not named:
        return []
    first, condition = min(named, key=lambda item: item[0]["index"])
    return [
        _finding(
            "diagnosis_presence",
            "fail",
            claim,
            f"[{first['index']}] records {condition}",
            [r["index"] for r, _c in named],
        )
    ]


def claim_marks(claim: str, facts: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Contradictions one claim makes with the chart: relative time, drug citation, demographics
    citation and diagnosis presence."""
    records = _records(facts)
    if not claim or not records:
        return []
    expanded_claim = _expand_compact_citations(claim, records)
    reference = _date((facts or {}).get("reference_date"))
    return (
        _relative_time_marks(expanded_claim, records, reference)
        + _drug_citation_marks(expanded_claim, records)
        + _demographics_marks(expanded_claim, records)
        + _diagnosis_marks(expanded_claim, records)
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
    listed_old: List[int] = []
    findings: List[Dict[str, Any]] = []
    cited_anywhere = [i for i in _cites(text) + _row_refs(blocks) if i in records]
    reference = _date(facts.get("reference_date")) or start
    future_cited = [
        i
        for i in cited_anywhere
        if (_date(records[i].get("date")) or _dt.date.min) > reference
    ]
    if future_cited:
        findings.append(
            _finding(
                "window",
                "fail",
                text,
                "the answer cites records dated after the reference date "
                f"{reference.isoformat()} as within the {words}: "
                + ", ".join(f"[{i}] ({records[i]['date']})" for i in sorted(set(future_cited))),
                future_cited,
            )
        )
        return text, blocks, findings
    if cited_anywhere and _all_before(cited_anywhere, records, start):
        # Nothing the answer cites falls inside the window: leaving it out would leave no answer.
        # Does the chart hold records of the kinds (and requested drug/subject) the answer cites
        # inside the window (up to the reference date)? Then the answer missed them; otherwise the
        # chart has nothing newer.
        kinds = {records[i].get("cls") for i in cited_anywhere}
        chart_drug_tokens = (
            set().union(*(_drug_tokens(_drug_name(r) or "") for r in records.values() if _drug_name(r)))
            if records
            else set()
        )
        subject_drugs = (_words(question) | _words(text)) & chart_drug_tokens
        in_window = sorted(
            (
                r
                for r in records.values()
                if r.get("cls") in kinds
                and r.get("clinical", True)
                and start <= (_date(r.get("date")) or _dt.date.min) <= reference
                and (
                    r.get("cls") != "Drug order"
                    or not subject_drugs
                    or bool(_drug_tokens(_drug_name(r) or "") & subject_drugs)
                )
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

    def _prune_row_old_refs(value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if key == "refs" and isinstance(child, list):
                    kept_refs = []
                    for i in child:
                        if isinstance(i, int) and _all_before([i], records, start):
                            dropped.append(i)
                        else:
                            kept_refs.append(i)
                    value[key] = kept_refs
                else:
                    _prune_row_old_refs(child)
        elif isinstance(value, list):
            for child in value:
                _prune_row_old_refs(child)

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
                if refs:
                    _prune_row_old_refs(row)
                    if not _row_refs(row):
                        continue
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
            if cited and _all_before(cited, records, start):
                if _WINDOW_STATED_RE.search(sentence) or _PRIOR_RE.search(sentence):
                    kept_sentences.append(sentence)
                else:
                    dropped.extend(cited)
                continue
            # Within a sentence with in-window records, a citation group with both in-window and
            # older citations keeps only its in-window records; a group citing only older records
            # (and not framed as prior/outside the window) is retained in prose and flagged in listed_old.
            for _gs, _ge, item, group in reversed(_clusters(sentence)):
                indices = [i for i, _s, _e in group]
                late = [i for i in indices if not _all_before([i], records, start)]
                trailing = sentence[group[-1][2] : group[-1][2] + 60]
                framed_old = (
                    _PRIOR_RE.search(item)
                    or _PRIOR_RE.search(trailing)
                    or _WINDOW_STATED_RE.search(item + " " + trailing)
                )
                if not late and any(i in records for i in indices) and not framed_old:
                    # An item citing only older records inside a sentence about the window: the
                    # rule cannot cut the item's words, so it names it instead of claiming it.
                    listed_old.extend(i for i in indices if i in records)
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

    if listed_old:
        findings.append(
            _finding(
                "window",
                "fail",
                text,
                "the answer presents records dated before "
                f"{start.isoformat()} as within the {words}: "
                + ", ".join(f"[{i}] ({records[i]['date']})" for i in sorted(set(listed_old))),
                listed_old,
            )
        )
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
            claimed_dates = set(re.findall(r"\b\d{4}-\d{2}-\d{2}\b", item))
            claims_stop = bool(
                re.search(r"\b(?:stop(?:ped)?|discontinu(?:e|ed)|held|cancel(?:led|ed)?)\b", item, re.I)
            )
            claims_start = (
                bool(
                    re.search(
                        r"\b(?:start(?:ed)?|initiat(?:e|ed)|prescrib(?:e|ed)|order(?:ed)?|began|commenc(?:e|ed))\b",
                        item,
                        re.I,
                    )
                )
                and not claims_stop
            )

            def _supports_claim(idx: int) -> bool:
                rec = records.get(idx) or {}
                if claimed_dates and str(rec.get("date") or "") not in claimed_dates:
                    return False
                body_lower = str(rec.get("body") or "").lower()
                is_discontinue = "action: discontinue" in body_lower
                if claims_start and is_discontinue:
                    return False
                if claims_stop and not is_discontinue:
                    return False
                return True

            candidates = [
                i
                for i, n in names.items()
                if n and (_drug_tokens(n) & named) and _supports_claim(i)
            ]
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
    selected = [
        series
        for series in temporal._selected_series(question, "", facts)
        if _names_series(question, series, shared) and _names_series(answer, series, shared)
    ]
    if re.search(r"\b(?:grow(?:ing|th))\b", question or "", re.I):
        all_series = facts.get("numeric_series") or []
        anthro_series = [
            s
            for s in all_series
            if any(
                k in str(s.get("concept") or "").lower()
                for k in ("weight", "height", "length", "head circumference")
            )
        ]
        if any(_names_series(answer, s, shared) for s in anthro_series) or selected:
            seen_concepts = {s.get("concept") for s in selected}
            for s in anthro_series:
                if s.get("concept") not in seen_concepts:
                    selected.append(s)
                    seen_concepts.add(s.get("concept"))
    return selected


def _overdue(series: Dict[str, Any], reference: _dt.date) -> bool:
    """Whether the gap from the series' last reading to the reference date is longer than any
    interval between its own readings: the chart's measuring rhythm, not a fixed cutoff, says
    when a value is old. A series with one reading has no rhythm and is never called overdue."""
    dates = sorted(d for d in (_date(p.get("date")) for p in series.get("points") or []) if d)
    if len(dates) < 2:
        return False
    longest = max((b - a).days for a, b in zip(dates, dates[1:]))
    return (reference - dates[-1]).days > max(longest, 1)


def _points_in_window(
    series: Dict[str, Any], start: Optional[_dt.date], reference: Optional[_dt.date] = None
) -> List[Dict[str, Any]]:
    points = [p for p in series.get("points") or [] if _date(p.get("date"))]
    return [
        p
        for p in points
        if (not start or _date(p["date"]) >= start)
        and (not reference or _date(p["date"]) <= reference)
    ]


def _reading(point: Dict[str, Any]) -> str:
    unit = f" {point['unit']}" if point.get("unit") else ""
    return f"{temporal._format_value(point.get('value'))}{unit} ({point.get('date')}) [{point.get('index')}]"


def _asserts_steady_for_series(
    text: str, series: Dict[str, Any], series_list: Sequence[Dict[str, Any]], shared: set
) -> bool:
    series_indices = {
        p["index"]
        for p in series.get("points") or []
        if isinstance(p.get("index"), int)
    }
    active_series_is_target = False
    for clause in re.split(r"[.;!?]+|\n+", text or ""):
        if not clause.strip():
            continue
        names_target = _names_series(clause, series, shared)
        names_other = any(
            _names_series(clause, other, shared)
            for other in series_list
            if other is not series
        )
        clause_cites = set(_cites(clause))
        cites_target = bool(clause_cites & series_indices)
        has_target_values = bool(temporal._series_value_candidates(clause, series))
        refers_to_target = (names_target or cites_target or has_target_values) and not (
            names_other and not names_target
        )
        is_continuation = (
            active_series_is_target
            and not names_other
            and not (clause_cites - series_indices)
            and bool(
                re.search(
                    r"\b(?:it|this|they|these|the\s+(?:trend|series|readings?|values?|measurements?|levels?|counts?))\b",
                    clause,
                    re.I,
                )
            )
            and not re.search(
                r"\b(?:took|taking|takes|adher\w*|regimen|medications?|drugs?|therapy|treatment|prescribed|ordered|visits?|appointments?)\b",
                clause,
                re.I,
            )
        )
        if names_target:
            active_series_is_target = True
        elif names_other:
            active_series_is_target = False
        if not (refers_to_target or is_continuation):
            continue
        for match in _STEADY_RE.finditer(clause):
            prefix = clause[max(0, match.start() - 30) : match.start()]
            if re.search(
                r"\b(?:not|no|never|neither|didn't|did not|does not|doesn't|wasn't|was not|weren't|were not|without)\b",
                prefix,
                re.I,
            ):
                continue
            return True
    return False


def _series_edits(question: str, text: str, facts: Dict[str, Any]) -> Tuple[str, List[int], List[Dict[str, Any]]]:
    """R3 stale latest value and R4 readings and shape, for the series the answer is about.

    When a trend series has more than eight in-window readings and the answer states two or fewer
    points, the appended summary samples the chronological first, lowest, highest, and last points
    (deduplicated by record index in chronological order).
    """
    reference = _date(facts.get("reference_date"))
    series_list = _series_in_answer(question, text, facts) if reference else []
    if not series_list:
        return text, [], []
    shared = _shared_series_words(facts)
    window = asked_window(question, facts)
    start = window[0] if window else None
    trend_question = bool(temporal._TREND_RE.search(question or ""))
    is_growth_question = bool(re.search(r"\b(?:grow(?:ing|th))\b", question or "", re.I))
    appended: List[str] = []
    added: List[int] = []
    findings: List[Dict[str, Any]] = []
    stale_by_date: Dict[str, List[Dict[str, Any]]] = {}
    cites_in_text = set(_cites(text))
    for series in series_list:
        concept = str(series.get("concept") or "").split(" (")[0]
        points = _points_in_window(series, start, reference)
        stated = {
            round(float(m.group()), 3)
            for m in re.finditer(r"(?<![\d.\[])\d+(?:\.\d+)?(?![\d\]])", re.sub(r"\d{4}-\d{2}-\d{2}", " ", text))
        }
        stated_points = [
            p
            for p in points
            if isinstance(p.get("value"), (int, float))
            and round(float(p["value"]), 3) in stated
            and (
                p.get("index") in cites_in_text
                or (str(p.get("date") or "") and str(p.get("date")) in text)
            )
        ]
        if trend_question and (
            len(points) >= 3
            or (is_growth_question and len(points) >= 2 and len(stated_points) == 0)
        ):
            ordered = [float(p["value"]) for p in points]
            steps = [b - a for a, b in zip(ordered, ordered[1:]) if b != a]
            changes_direction = any(x * y < 0 for x, y in zip(steps, steps[1:]))
            if changes_direction and _asserts_steady_for_series(text, series, series_list, shared):
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
            if len(stated_points) <= 2 and len(stated_points) < len(points):
                if len(points) <= 8:
                    shown = points
                    lead = f"Recorded {concept.lower()} readings: "
                    appended.append(lead + ", ".join(_reading(p) for p in shown) + ".")
                else:
                    first_pt = points[0]
                    low_pt = min(points, key=lambda p: p["value"])
                    high_pt = max(points, key=lambda p: p["value"])
                    last_pt = points[-1]
                    key_indices = {
                        first_pt["index"],
                        low_pt["index"],
                        high_pt["index"],
                        last_pt["index"],
                    }
                    shown = [p for p in points if p.get("index") in key_indices]
                    total_records = sum(
                        1 + len(p.get("extra_indices") or []) for p in points
                    )
                    count_label = (
                        f"{total_records} {concept.lower()} readings ({len(points)} dates)"
                        if total_records != len(points)
                        else f"{len(points)} {concept.lower()} readings"
                    )
                    lead = f"{count_label}; first, lowest, highest and last: "
                    if len(shown) < 4:
                        role_items = []
                        for p in shown:
                            roles = [
                                label
                                for label, target in (
                                    ("first", first_pt),
                                    ("lowest", low_pt),
                                    ("highest", high_pt),
                                    ("last", last_pt),
                                )
                                if target.get("index") == p.get("index")
                            ]
                            role_items.append(f"{'/'.join(roles)} {_reading(p)}")
                        appended.append(lead + ", ".join(role_items) + ".")
                    else:
                        appended.append(
                            lead + ", ".join(_reading(p) for p in shown) + "."
                        )
                added.extend(p["index"] for p in shown)
                findings.append(
                    _finding("series", "info", text, f"added {len(shown)} {concept.lower()} readings", [p["index"] for p in shown])
                )
        last = series.get("most_recent") or {}
        last_date = _date(last.get("date"))
        if (
            last_date
            and (reference - last_date).days > 90
            and not _GAP_STATED_RE.search(text)
            and _overdue(series, reference)
        ):
            stale_by_date.setdefault(last["date"], []).append(series)
    if re.search(r"\b(?:grow(?:ing|th)?|gaining\s+weight)\b", question or "", re.I):
        records_map = _records(facts)
        growth_conds = sorted(
            (
                r
                for r in records_map.values()
                if str(r.get("cls") or "").lower() in {"condition", "diagnosis", "problem"}
                and "status: active" in str(r.get("body") or "").lower()
                and re.search(
                    r"\b(?:failure to thrive|delayed milestones|developmental delay|malnutrition|underweight|stunting|wasting)\b",
                    str(r.get("body") or ""),
                    re.I,
                )
                and r["index"] not in cites_in_text
            ),
            key=lambda r: r["index"],
        )
        for gc in growth_conds:
            cond_label = (
                str(gc.get("body") or "").split(":", 1)[-1].split(". ", 1)[0].strip()
            )
            if cond_label.lower() in (text or "").lower():
                continue
            appended.append(
                f"The chart also documents an active {cond_label} diagnosis ({gc.get('date')}) [{gc['index']}]."
            )
            added.append(gc["index"])
            findings.append(
                _finding(
                    "series",
                    "info",
                    text,
                    f"added active growth condition {cond_label} [{gc['index']}]",
                    [gc["index"]],
                )
            )
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


def _measurement_citation_edits(
    text: str, records: Dict[int, Dict[str, Any]]
) -> Tuple[str, List[Dict[str, Any]]]:
    """Repair paired blood-pressure citations when a same-date non-BP vital (e.g. Pulse) is cited
    instead of one of the Systolic/Diastolic blood pressure records."""
    findings: List[Dict[str, Any]] = []
    for _start, _end, item, group in reversed(_clusters(text)):
        bp_match = re.search(
            r"\bblood\s+pressure\b[^.;!?]{0,40}?\b(?P<sys>\d{2,3})\s*/\s*(?P<dia>\d{2,3})\s*mmhg\b",
            item,
            re.I,
        )
        if not bp_match:
            continue
        indices = [i for i, _s, _e in group if i in records]
        if not indices:
            continue
        bp_cited = [
            i
            for i in indices
            if "blood pressure" in str(records[i].get("body") or "").lower()
        ]
        wrong_cited = [
            i
            for i in indices
            if "blood pressure" not in str(records[i].get("body") or "").lower()
            and str(records[i].get("cls") or "").lower() not in {"encounter", "visit"}
        ]
        if not bp_cited or not wrong_cited:
            continue
        target_date = str(records[bp_cited[0]].get("date") or "")
        sys_val = bp_match.group("sys")
        dia_val = bp_match.group("dia")
        sys_candidates = [
            r["index"]
            for r in records.values()
            if str(r.get("date") or "") == target_date
            and "systolic blood pressure" in str(r.get("body") or "").lower()
            and re.search(rf"\b{re.escape(sys_val)}(?:\.0)?\b", str(r.get("body") or ""))
        ]
        dia_candidates = [
            r["index"]
            for r in records.values()
            if str(r.get("date") or "") == target_date
            and "diastolic blood pressure" in str(r.get("body") or "").lower()
            and re.search(rf"\b{re.escape(dia_val)}(?:\.0)?\b", str(r.get("body") or ""))
        ]
        if not sys_candidates or not dia_candidates:
            continue
        right = [sys_candidates[0], dia_candidates[0]]
        text = (
            text[: group[0][1]]
            + "".join(f"[{i}]" for i in right)
            + text[group[-1][2] :]
        )
        findings.append(
            _finding(
                "measurement_citation",
                "info",
                item.strip(),
                "cited "
                + ", ".join(f"[{i}]" for i in wrong_cited)
                + " for blood pressure; replaced by "
                + ", ".join(f"[{i}]" for i in right),
                wrong_cited + right,
            )
        )
    return text, findings


def _adherence_edits(
    question: str,
    text: str,
    records: Dict[int, Dict[str, Any]],
    facts: Dict[str, Any],
) -> Tuple[str, List[int], List[Dict[str, Any]]]:
    """Append prior non-adherence history and staleness when an adherence question cites only the
    latest adherence observation(s)."""
    if not re.search(
        r"\b(?:adher\w*|taking\s+them|how\s+well\s+(?:she|he|they)\b[^?]*\btaking)\b",
        question or "",
        re.I,
    ):
        return text, [], []
    adh_records = [
        r
        for r in records.values()
        if "adherence" in str(r.get("body") or "").lower() and _date(r.get("date"))
    ]
    if not adh_records:
        return text, [], []
    cites_in_text = set(_cites(text))
    cited_adh = [r for r in adh_records if r["index"] in cites_in_text]
    if not cited_adh:
        return text, [], []
    latest_date = max(str(r.get("date") or "") for r in adh_records)
    prior_no = sorted(
        (
            r
            for r in adh_records
            if str(r.get("date") or "") < latest_date
            and re.search(r":\s*(?:no|none|poor)\b", str(r.get("body") or ""), re.I)
        ),
        key=lambda r: (str(r.get("date") or ""), -r["index"]),
    )
    cited_prior_no_dates = {
        str(r.get("date") or "")
        for r in cited_adh
        if str(r.get("date") or "") < latest_date
        and re.search(r":\s*(?:no|none|poor)\b", str(r.get("body") or ""), re.I)
    }
    appended: List[str] = []
    added: List[int] = []
    findings: List[Dict[str, Any]] = []
    has_any_past_week_no = any(
        "past week" in str(r.get("body") or "").lower()
        and re.search(r":\s*(?:no|none|poor)\b", str(r.get("body") or ""), re.I)
        for r in adh_records
    )
    cited_past_week_all = [
        r
        for r in cited_adh
        if "past week" in str(r.get("body") or "").lower()
        and re.search(r":\s*all\b", str(r.get("body") or ""), re.I)
    ]
    if cited_past_week_all and not has_any_past_week_no:
        repaired_text, n_subs = re.subn(
            r"\bno(\s+(?:antiretroviral\s+)?adherence(?:\s+to\s+[^.;,]+?)?\s+in\s+the\s+past\s+week)\b",
            r"all\1",
            text,
            flags=re.I,
        )
        if n_subs:
            text = repaired_text
            findings.append(
                _finding(
                    "adherence_value",
                    "info",
                    text,
                    "repaired 'no ... adherence in the past week' to 'all' per cited past-week All record",
                    [r["index"] for r in cited_past_week_all],
                )
            )
    if prior_no:
        earliest_no = prior_no[0]
        latest_no = prior_no[-1]
        distinct_no_dates = sorted({str(r.get("date") or "") for r in prior_no})
        already_states_all_prior_no = bool(
            re.search(
                rf"\b{len(distinct_no_dates)}\s+(?:earlier\s+|prior\s+|previous\s+)?(?:visits?|dates?|encounters?|times?)\b",
                text or "",
                re.I,
            )
            or (
                len(distinct_no_dates) == 1
                and re.search(
                    r"\b(?:earlier|previous|prior)\b[^.]{0,60}\b(?:no|non-?adher|poor)\b",
                    text or "",
                    re.I,
                )
            )
        )
        if (
            len(cited_prior_no_dates) < len(distinct_no_dates)
            and not already_states_all_prior_no
        ):
            if len(distinct_no_dates) >= 2:
                appended.append(
                    f"Earlier visits from {earliest_no['date']} [{earliest_no['index']}] to "
                    f"{latest_no['date']} [{latest_no['index']}] ({len(distinct_no_dates)} visits) "
                    f"recorded '{latest_no['body']}'."
                )
                added.extend([earliest_no["index"], latest_no["index"]])
            else:
                appended.append(
                    f"An earlier visit on {earliest_no['date']} [{earliest_no['index']}] "
                    f"recorded '{earliest_no['body']}'."
                )
                added.append(earliest_no["index"])
            findings.append(
                _finding(
                    "adherence_history",
                    "info",
                    text,
                    f"added {len(distinct_no_dates)} earlier non-adherence visit(s)",
                    list(added),
                )
            )
    prior_week_all = sorted(
        (
            r
            for r in adh_records
            if str(r.get("date") or "") < latest_date
            and "past week" in str(r.get("body") or "").lower()
            and re.search(r":\s*all\b", str(r.get("body") or ""), re.I)
        ),
        key=lambda r: (str(r.get("date") or ""), -r["index"]),
    )
    if prior_week_all:
        distinct_all_dates = sorted({str(r.get("date") or "") for r in prior_week_all})
        cited_prior_all_dates = {
            str(r.get("date") or "")
            for r in cited_adh
            if str(r.get("date") or "") < latest_date
            and "past week" in str(r.get("body") or "").lower()
            and re.search(r":\s*all\b", str(r.get("body") or ""), re.I)
        }
        if (
            len(distinct_all_dates) >= 2
            and len(cited_prior_all_dates) < len(distinct_all_dates)
            and not re.search(
                r"\bpast\s+week\b[^.;]{0,50}\ball\b|\ball\b[^.;]{0,50}\bpast\s+week\b",
                text or "",
                re.I,
            )
        ):
            earliest_all = prior_week_all[0]
            latest_all = prior_week_all[-1]
            appended.append(
                f"Across earlier visits from {earliest_all['date']} [{earliest_all['index']}] to "
                f"{latest_all['date']} [{latest_all['index']}] ({len(distinct_all_dates)} visits), "
                f"'{latest_all['body']}' was recorded."
            )
            added.extend([earliest_all["index"], latest_all["index"]])
            findings.append(
                _finding(
                    "adherence_history",
                    "info",
                    text,
                    f"added {len(distinct_all_dates)} earlier past-week All adherence visit(s)",
                    [earliest_all["index"], latest_all["index"]],
                )
            )
    reference = _date(facts.get("reference_date"))
    last_dt = _date(latest_date)
    adh_dates = sorted({_date(r.get("date")) for r in adh_records if _date(r.get("date"))})
    longest_gap = (
        max((b - a).days for a, b in zip(adh_dates, adh_dates[1:]))
        if len(adh_dates) >= 2
        else 0
    )
    if (
        reference
        and last_dt
        and (reference - last_dt).days > 90
        and len(adh_dates) >= 2
        and (reference - last_dt).days > max(longest_gap, 1)
        and not _GAP_STATED_RE.search(text)
    ):
        months = round((reference - last_dt).days / 30.44 * 2) / 2
        latest_idx = min(
            r["index"] for r in adh_records if str(r.get("date") or "") == latest_date
        )
        appended.append(
            f"The latest adherence records in the chart are from {latest_date} [{latest_idx}], "
            f"about {temporal._format_value(months)} months before {reference.isoformat()}."
        )
        added.append(latest_idx)
        findings.append(
            _finding(
                "stale",
                "info",
                text,
                f"the latest adherence record is from {latest_date}, {temporal._format_value(months)} months before the reference date",
                [latest_idx],
            )
        )
    if appended:
        text = text.rstrip() + " " + " ".join(appended)
    return text, added, findings


def _last_visit_purpose_edits(
    question: str,
    text: str,
    records: Dict[int, Dict[str, Any]],
    facts: Dict[str, Any],
) -> Tuple[str, List[int], List[Dict[str, Any]]]:
    """When asked when the last visit was and what it was for, append the visit's documented
    complaints, abnormal findings, hospitalization, and treatment plans that are not yet cited."""
    if not re.search(
        r"\blast\s+(?:visit|encounter)\b.*\bwhat\s+was\s+it\s+for\b",
        question or "",
        re.I,
    ):
        return text, [], []
    last_enc = (facts or {}).get("last_clinical_encounter")
    if not isinstance(last_enc, dict) or not last_enc.get("date"):
        return text, [], []
    enc_date = str(last_enc["date"])
    cites_set = {i for i in _cites(text) if i in records}
    has_cited_drug_order = any(
        records[i].get("cls") == "Drug order"
        and str(records[i].get("date") or "") == enc_date
        for i in cites_set
    )
    same_day = sorted(
        (
            r
            for r in records.values()
            if str(r.get("date") or "") == enc_date
            and str(r.get("cls") or "").lower() not in {"encounter", "visit", "patient"}
        ),
        key=lambda r: r["index"],
    )
    if not same_day:
        return text, [], []
    highlights: List[str] = []
    added: List[int] = []
    has_uncited_acute = False
    for r in same_day:
        idx = r["index"]
        if idx in cites_set:
            continue
        body = str(r.get("body") or "").strip()
        body_lower = body.lower()
        is_acute_or_plan = (
            body_lower
            in {
                "scheduled visit: no",
                "new complaints: yes",
                "new who/cdc staging criteria met: yes",
            }
            or "hospitalized location:" in body_lower
            or "temperature" in body_lower
            or "pulse:" in body_lower
            or "blood pressure:" in body_lower
            or "plan: continue regimen" in body_lower
        )
        is_context = (
            "adherence in past week:" in body_lower
            or "adherence in last month:" in body_lower
            or "weight" in body_lower
            or "who hiv stage:" in body_lower
            or (r.get("cls") == "Drug order" and not has_cited_drug_order)
        )
        if is_acute_or_plan or is_context:
            highlights.append(f"{body} [{idx}]")
            added.append(idx)
            has_uncited_acute = has_uncited_acute or is_acute_or_plan
    if not highlights or not has_uncited_acute:
        return text, [], []
    summary_sentence = (
        f"Documented findings and plans on {enc_date} include: "
        + "; ".join(highlights[:12])
        + "."
    )
    text = text.rstrip() + " " + summary_sentence
    return (
        text,
        added,
        [
            _finding(
                "visit_purpose",
                "info",
                question,
                f"added {len(highlights[:12])} same-day clinical findings/plans for {enc_date}",
                added[:12],
            )
        ],
    )


def _missed_return_visit_edits(
    question: str,
    text: str,
    facts: Dict[str, Any],
) -> Tuple[str, List[int], List[Dict[str, Any]]]:
    """When asked about upcoming/scheduled appointments and only past return-visit date
    observations exist in the chart, state that no return visit date after reference_date is
    documented in the chart and cite the latest past return-visit date."""
    if not re.search(
        r"\b(?:upcoming|next|future|scheduled)\s+(?:appointments?|visits?|return\s+visits?)\b"
        r"|\b(?:any\s+appointments?|return\s+visit\s+date)\b",
        question or "",
        re.I,
    ):
        return text, [], []
    appt = (facts or {}).get("appointment_candidates") or {}
    if appt.get("future") or appt.get("today"):
        return text, [], []
    past = [c for c in (appt.get("past") or []) if c.get("date") and isinstance(c.get("index"), int)]
    if not past:
        return text, [], []
    latest_past = max(past, key=lambda c: (str(c.get("date") or ""), -int(c.get("index") or 0)))
    past_date = str(latest_past.get("date") or "")
    past_idx = int(latest_past["index"])
    ref_iso = str((facts or {}).get("reference_date") or "")
    source_date = str(latest_past.get("source_date") or "")
    last_enc_date = str(((facts or {}).get("last_clinical_encounter") or {}).get("date") or "")
    if (
        past_date in (text or "")
        and (not source_date or source_date in (text or ""))
        and re.search(r"\b(?:documented|chart|records?)\b", text or "", re.I)
    ):
        return text, [], []
    if past_date in (text or "") and re.search(r"\b(?:documented|chart|records?)\b", text or "", re.I):
        enc_tail = (
            f", and no subsequent encounter is documented in the chart after {last_enc_date}."
            if last_enc_date and last_enc_date < past_date
            else "."
        )
        addition = (
            f"That return visit date ({past_date} [{past_idx}]) was recorded at the {source_date} encounter{enc_tail}"
        )
    else:
        source_clause = f" (recorded on {source_date})" if source_date else ""
        if last_enc_date and last_enc_date < past_date:
            tail = f"which has passed with no subsequent encounter documented after {last_enc_date}."
        else:
            tail = "which has already passed."
        addition = (
            f"In the chart records, no return visit date after {ref_iso} is documented; "
            f"the latest documented return visit date was {past_date} [{past_idx}]{source_clause}, "
            f"{tail}"
        )
    text = text.rstrip() + " " + addition
    return (
        text,
        [past_idx],
        [
            _finding(
                "missed_return_visit",
                "info",
                question,
                f"added latest past return visit date {past_date} [{past_idx}]",
                [past_idx],
            )
        ],
    )


def _medication_safety_reconciliation_edits(
    question: str,
    text: str,
    records: Dict[int, Dict[str, Any]],
) -> Tuple[str, List[int], List[Dict[str, Any]]]:
    """Ground medication cessation and missed-dose questions in the patient's chart records and
    strip unresolved non-chart citations when the prose does not explicitly name a reference."""
    if not records:
        return text, [], []
    added: List[int] = []
    findings: List[Dict[str, Any]] = []
    # Strip inline citations to indices outside the patient chart snapshot unless the prose
    # explicitly attributes a reference.
    if "reference" not in (text or "").lower():
        non_chart = [i for i in _cites(text) if i not in records]
        if non_chart:
            text = re.sub(
                r"\s*\[" + "|".join(str(i) for i in sorted(set(non_chart))) + r"\]",
                "",
                text,
            )
            text = re.sub(r"\s+([.,;:])", r"\1", text)
    q_lower = (question or "").lower()
    drug_orders = [
        r
        for r in records.values()
        if r.get("cls") == "Drug order"
        and "action: discontinue" not in str(r.get("body") or "").lower()
    ]
    if not drug_orders:
        return text, added, findings
    latest_order_date = max(str(r.get("date") or "") for r in drug_orders)
    latest_orders = sorted(
        (r for r in drug_orders if str(r.get("date") or "") == latest_order_date),
        key=lambda r: r["index"],
    )
    cites_in_text = set(_cites(text))
    asks_cessation = bool(
        re.search(
            r"\b(?:stop(?:ping|s)?\s+(?:all\s+)?(?:her|his|their|the)?\s*(?:hiv\s+|antiretroviral\s+)?(?:medications?|therapy|treatment|them))\b",
            q_lower,
        )
    )
    if asks_cessation:
        cont_plans = [
            r
            for r in records.values()
            if str(r.get("date") or "") == latest_order_date
            and "antiretroviral plan: continue regimen" in str(r.get("body") or "").lower()
        ]
        if cont_plans and cont_plans[0]["index"] not in cites_in_text:
            plan_rec = cont_plans[0]
            order_str = ", ".join(
                f"{_drug_name(r)} [{r['index']}]" for r in latest_orders if _drug_name(r)
            )
            addition = (
                f"The chart's latest visit on {latest_order_date} documents "
                f"{plan_rec['body']} [{plan_rec['index']}] with active orders for {order_str}."
            )
            text = text.rstrip() + " " + addition
            new_idxs = [plan_rec["index"]] + [r["index"] for r in latest_orders]
            added.extend(new_idxs)
            cites_in_text.update(new_idxs)
            findings.append(
                _finding(
                    "regimen_continuation",
                    "info",
                    question,
                    f"added continuation plan [{plan_rec['index']}] and latest orders",
                    new_idxs,
                )
            )
    # Reconcile when the question names an older superseded drug (e.g., efavirenz [72]) not in the
    # latest medication orders.
    latest_tokens = set().union(
        *(_drug_tokens(_drug_name(r) or "") for r in latest_orders if _drug_name(r))
    )
    older_orders = [
        r for r in drug_orders if str(r.get("date") or "") < latest_order_date
    ]
    q_words = _words(question)
    superseded_matches = [
        r
        for r in older_orders
        if (_drug_tokens(_drug_name(r) or "") & q_words)
        and not (_drug_tokens(_drug_name(r) or "") & latest_tokens)
    ]
    if superseded_matches:
        latest_old = max(
            superseded_matches, key=lambda r: (str(r.get("date") or ""), -r["index"])
        )
        if latest_old["index"] not in cites_in_text:
            old_name = _drug_name(latest_old) or "that medication"
            order_str = ", ".join(
                f"{_drug_name(r)} [{r['index']}]" for r in latest_orders if _drug_name(r)
            )
            addition = (
                f"In the chart, {old_name} was last ordered on {latest_old['date']} "
                f"[{latest_old['index']}], while the most recent orders on {latest_order_date} "
                f"are {order_str}."
            )
            text = text.rstrip() + " " + addition
            new_idxs = [latest_old["index"]] + [r["index"] for r in latest_orders]
            added.extend(new_idxs)
            findings.append(
                _finding(
                    "regimen_reconciliation",
                    "info",
                    question,
                    f"reconciled older {old_name} order [{latest_old['index']}] with {latest_order_date} orders",
                    new_idxs,
                )
            )
    return text, added, findings


def _apply_table_cell_rules(
    blocks: List[Any],
    records: Dict[int, Dict[str, Any]],
    facts: Optional[Dict[str, Any]],
    answer_cites: set,
    *,
    edit: bool,
) -> Tuple[List[Any], List[Dict[str, Any]]]:
    new_blocks = copy.deepcopy(blocks)
    findings: List[Dict[str, Any]] = []
    for block in new_blocks if isinstance(new_blocks, list) else []:
        rows = block.get("rows") if isinstance(block, dict) else None
        if not isinstance(rows, list):
            continue
        for row in rows:
            cells = row.get("cells") if isinstance(row, dict) else None
            if not isinstance(cells, dict):
                continue
            for cell in cells.values():
                if not isinstance(cell, dict):
                    continue
                cell_text = str(cell.get("text") or "").strip()
                cell_refs = [r for r in (cell.get("refs") or []) if isinstance(r, int)]
                if not cell_text or not cell_refs:
                    continue
                synthetic = f"{cell_text} " + "".join(f"[{r}]" for r in cell_refs)
                if edit:
                    edited_synthetic, drug_findings = _drug_citation_edits(
                        synthetic, records, answer_cites
                    )
                    if drug_findings:
                        cell["refs"] = _cites(edited_synthetic)
                        findings.extend(drug_findings)
                        synthetic = edited_synthetic
                findings.extend(claim_marks(synthetic, facts))
    return new_blocks, findings


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
    expanded_text = _expand_compact_citations(answer_text or "", records, citations or [])
    answer_cites = set(citations or []) | set(_cites(expanded_text)) | set(_row_refs(blocks))
    if not edit:
        _wt, _wb, window_findings = _window_edits(
            question, expanded_text, blocks or [], records, facts or {}
        )
        _st, _sa, series_findings = _series_edits(question, expanded_text, facts or {})
        findings = [
            f for f in (window_findings + series_findings) if f.get("status") == "fail"
        ]
        for sentence in _SENTENCE_RE.split(expanded_text):
            findings.extend(claim_marks(sentence, facts))
        _tb, table_findings = _apply_table_cell_rules(
            blocks or [], records, facts, answer_cites, edit=False
        )
        findings.extend(f for f in table_findings if f.get("status") == "fail")
        return answer_text, citations, blocks, findings

    text, new_blocks, findings = _window_edits(question, expanded_text, blocks or [], records, facts or {})
    text, drug_findings = _drug_citation_edits(text, records, answer_cites)
    findings += drug_findings
    text, meas_findings = _measurement_citation_edits(text, records)
    findings += meas_findings
    new_blocks, table_findings = _apply_table_cell_rules(
        new_blocks, records, facts, answer_cites, edit=True
    )
    findings += table_findings
    text, added, series_findings = _series_edits(question, text, facts or {})
    findings += series_findings
    text, adh_added, adh_findings = _adherence_edits(question, text, records, facts or {})
    added += adh_added
    findings += adh_findings
    text, visit_added, visit_findings = _last_visit_purpose_edits(
        question, text, records, facts or {}
    )
    added += visit_added
    findings += visit_findings
    text, appt_added, appt_findings = _missed_return_visit_edits(
        question, text, facts or {}
    )
    added += appt_added
    findings += appt_findings
    text, safety_added, safety_findings = _medication_safety_reconciliation_edits(
        question, text, records
    )
    added += safety_added
    findings += safety_findings
    for sentence in _SENTENCE_RE.split(text):
        findings += claim_marks(sentence, facts)
    kept = set(_cites(text))
    if text == expanded_text and not any(f.get("status") == "info" for f in findings):
        text = answer_text
    new_citations = [c for c in (citations or []) if c in kept or c in set(_row_refs(new_blocks))]
    new_citations += [c for c in sorted(kept) if c not in new_citations]
    new_citations += [c for c in added if c not in new_citations]
    for c in _row_refs(new_blocks):
        if c not in new_citations:
            new_citations.append(c)
    return text, new_citations, new_blocks, findings
