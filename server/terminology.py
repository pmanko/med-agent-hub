"""Deterministic OpenMRS terminology normalization and chart clinical-signal extraction."""

from __future__ import annotations

import json
import re
from typing import Any, Iterable, Mapping, Sequence, Tuple

CIEL_UUID_RE = re.compile(r"^(\d+)A{20,}$", re.IGNORECASE)
_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")
_SKIP_CONCEPTS = frozenset(
    {
        "yes",
        "no",
        "none",
        "unknown",
        "true",
        "false",
        "scheduled visit",
        "pay category",
        "return visit date",
        "next appointment date",
        "location",
        "other non-coded",
    }
)
_ABNORMAL_INTERPRETATIONS = frozenset(
    {"H", "HH", "L", "LL", "A", "AA", "CRIT", "HIGH", "LOW", "ABNORMAL", "CRITICAL", "POSITIVE"}
)
_PREFERRED_CODE_PREFIXES = ("CIEL:", "WHO-ATC:", "ATC:", "SNOMED-CT:", "LOINC:", "ICD-10", "RxNORM:", "ANCDAK:")


def derive_ciel_code(uuid_value: Any) -> str | None:
    """Derive ``CIEL:<id>`` from canonical CIEL UUIDs (e.g. ``123AAA...``)."""
    raw = str(uuid_value or "").strip()
    if not raw:
        return None
    match = CIEL_UUID_RE.fullmatch(raw)
    if match:
        return f"CIEL:{match.group(1)}"
    return None


def extract_record_codes(metadata: Mapping[str, Any] | None) -> list[str]:
    """Collect normalized terminology codes from QueryStore record metadata."""
    if not isinstance(metadata, Mapping):
        return []
    codes: list[str] = []
    seen: set[str] = set()

    def add(code: str | None) -> None:
        cleaned = str(code or "").strip()
        if not cleaned or cleaned in seen:
            return
        seen.add(cleaned)
        codes.append(cleaned)

    for raw in metadata.get("mapping_codes") or ():
        add(str(raw))
    for uuid_field in ("concept_uuid", "value_coded_uuid", "allergen_uuid", "program_uuid"):
        add(derive_ciel_code(metadata.get(uuid_field)))
    return codes


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _first_sentence(text: str, limit: int = 160) -> str:
    cleaned = _clean_text(text)
    if not cleaned:
        return ""
    if len(cleaned) <= limit:
        return cleaned
    cut = cleaned[:limit].rsplit(" ", 1)[0].rstrip(" ,;:")
    return f"{cut}..." if cut else cleaned[:limit]


def _order_is_active(metadata: Mapping[str, Any], text: str) -> bool:
    action = str(metadata.get("action") or "").strip().upper()
    if action == "DISCONTINUE":
        return False
    if metadata.get("date_stopped"):
        return False
    lower = text.lower()
    if "no longer in force" in lower or ". stopped:" in lower:
        return False
    return True


def _condition_is_active(metadata: Mapping[str, Any], text: str) -> bool:
    status = str(metadata.get("clinical_status") or "").strip().upper()
    if status in {"INACTIVE", "RESOLVED", "REMISSION"}:
        return False
    if metadata.get("end_date"):
        return False
    lower = text.lower()
    return "status: inactive" not in lower and "status: resolved" not in lower


def _prioritized_codes(codes: Sequence[str], *, limit: int = 5) -> list[str]:
    def key(code: str) -> Tuple[int, str]:
        for idx, prefix in enumerate(_PREFERRED_CODE_PREFIXES):
            if code.upper().startswith(prefix.upper()):
                return (idx, code)
        return (len(_PREFERRED_CODE_PREFIXES), code)

    return sorted(set(codes), key=key)[:limit]


def _record_fields(record: Any) -> Tuple[str, str, Mapping[str, Any]]:
    if isinstance(record, Mapping):
        rtype = str(record.get("resourceType") or "")
        text = str(record.get("text") or "")
        raw_meta = record.get("metadata")
        meta = raw_meta if isinstance(raw_meta, Mapping) else {}
        return rtype, text, meta
    rtype = str(getattr(record, "resource_type", "") or "")
    text = str(getattr(record, "text", "") or "")
    raw_meta = getattr(record, "metadata", None)
    meta = raw_meta if isinstance(raw_meta, Mapping) else {}
    return rtype, text, meta


def extract_chart_clinical_signals(
    records: Sequence[Any],
) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
    """Extract prioritized (codes, terms) from primary patient records for WHO KB lookup."""
    prioritized_codes: list[str] = []
    seen_codes: set[str] = set()
    prioritized_terms: list[str] = []
    seen_terms: set[str] = set()

    def add_code(code: str) -> None:
        c = code.strip()
        if c and c not in seen_codes:
            seen_codes.add(c)
            prioritized_codes.append(c)

    def add_term(term: str) -> None:
        t = _clean_text(term)
        key = t.casefold()
        if t and key not in _SKIP_CONCEPTS and key not in seen_terms:
            seen_terms.add(key)
            prioritized_terms.append(t)

    bucketed: list[Tuple[int, str, str, Mapping[str, Any]]] = []
    for record in records:
        rtype, text, meta = _record_fields(record)
        norm_type = re.sub(r"[^a-z]", "", rtype.lower())
        if norm_type in {
            "knowledgereference",
            "drugreference",
            "safetyfinding",
            "drugclassnote",
            "patient",
            "visit",
            "encounter",
        }:
            continue
        if norm_type in {"condition", "diagnosis"}:
            prio = 0 if _condition_is_active(meta, text) else 3
        elif norm_type in {"drugorder", "medicationdispense"}:
            prio = 0 if _order_is_active(meta, text) else 2
        elif norm_type == "program":
            prio = 1 if meta.get("active") is not False else 3
        elif norm_type == "allergyintolerance" or norm_type == "allergy":
            prio = 1
        elif norm_type == "obs":
            interp = str(meta.get("interpretation") or "").strip().upper()
            cname = str(meta.get("concept_name") or "").lower()
            if interp in _ABNORMAL_INTERPRETATIONS:
                prio = 1
            elif any(
                kw in cname
                for kw in ("cd4", "viral load", "who stage", "antiretroviral", "regimen", "tb", "tuberculosis")
            ):
                prio = 2
            else:
                prio = 4
        else:
            prio = 4
        bucketed.append((prio, norm_type, text, meta))

    bucketed.sort(key=lambda item: item[0])
    for prio, norm_type, text, meta in bucketed:
        for code in extract_record_codes(meta):
            add_code(code)
        if norm_type in {"drugorder", "medicationdispense"}:
            name = _clean_text(meta.get("drug_name") or meta.get("concept_name"))
            if name:
                add_term(name)
        elif norm_type == "program":
            add_term(str(meta.get("program_name") or ""))
        elif norm_type in {"allergy", "allergyintolerance"}:
            add_term(str(meta.get("allergen_name") or meta.get("concept_name") or ""))
        elif prio <= 2:
            cname = _clean_text(meta.get("concept_name"))
            vcoded = _clean_text(meta.get("value_coded_name"))
            if vcoded and vcoded.casefold() not in _SKIP_CONCEPTS:
                add_term(f"{cname} {vcoded}".strip() if cname else vcoded)
            elif cname:
                add_term(cname)
        elif not meta:
            # Plain-text chart lines fallback (e.g., inline tests).
            stripped = re.sub(r"^\(\d{4}-\d{2}-\d{2}\)\s*", "", text).strip()
            for prefix in ("Condition:", "Drug order:", "Program:", "Allergy:", "Diagnosis:"):
                if stripped.startswith(prefix):
                    head = stripped[len(prefix) :].split(".", 1)[0].strip()
                    add_term(head)
                    break

    return tuple(prioritized_codes[:40]), tuple(prioritized_terms[:16])


def _collect_glossary_candidates(records: Sequence[Any]) -> list[dict[str, Any]]:
    by_concept: dict[str, dict[str, Any]] = {}
    for record in records:
        rtype, text, meta = _record_fields(record)
        norm_type = re.sub(r"[^a-z]", "", rtype.lower())
        if norm_type in {
            "knowledgereference",
            "drugreference",
            "safetyfinding",
            "drugclassnote",
            "patient",
            "visit",
            "encounter",
        }:
            continue
        if not meta:
            continue

        label = _clean_text(
            meta.get("drug_name")
            or meta.get("program_name")
            or meta.get("allergen_name")
            or meta.get("concept_name")
        )
        if not label or label.casefold() in _SKIP_CONCEPTS:
            continue

        codes = _prioritized_codes(extract_record_codes(meta), limit=5)
        synonyms = [
            _clean_text(s)
            for s in (meta.get("synonyms") or ())
            if _clean_text(s) and _clean_text(s).casefold() != label.casefold()
        ]
        mapping_names = [
            _clean_text(s)
            for s in (meta.get("mapping_names") or ())
            if _clean_text(s) and _clean_text(s).casefold() != label.casefold()
        ]
        aliases = list(dict.fromkeys([*synonyms, *mapping_names]))[:4]
        definition = _first_sentence(str(meta.get("description") or ""), limit=140)
        units = _clean_text(meta.get("units"))
        interp = _clean_text(meta.get("interpretation")).upper()
        concept_class = _clean_text(meta.get("concept_class"))
        current_state = _clean_text(meta.get("current_state"))

        # Only include concepts that actually add terminology/reference context beyond bare label.
        if not (codes or aliases or definition or units or interp in _ABNORMAL_INTERPRETATIONS or current_state):
            continue

        dedup_key = _clean_text(meta.get("concept_uuid") or meta.get("program_uuid") or label).casefold()
        if norm_type in {"condition", "diagnosis"}:
            score = 50
        elif norm_type in {"drugorder", "medicationdispense", "allergy", "allergyintolerance"}:
            score = 45
        elif norm_type == "program":
            score = 42
        elif interp in _ABNORMAL_INTERPRETATIONS:
            score = 38
        elif concept_class.lower() in {"test", "diagnosis", "drug", "finding"}:
            score = 25
        else:
            score = 12

        if aliases:
            score += 4
        if definition:
            score += 3
        if codes:
            score += 2

        existing = by_concept.get(dedup_key)
        if existing is None or score > existing["_score"]:
            entry: dict[str, Any] = {
                "_score": score,
                "_key": dedup_key,
                "concept": label,
            }
            if concept_class:
                entry["class"] = concept_class
            elif norm_type == "program":
                entry["class"] = "Program"
            if codes:
                entry["codes"] = codes
            if aliases:
                entry["aliases"] = aliases
            if units:
                entry["units"] = units
            if interp in _ABNORMAL_INTERPRETATIONS:
                entry["abnormal_flag"] = interp
            if current_state:
                entry["program_state"] = current_state
            if definition:
                entry["definition"] = definition
            by_concept[dedup_key] = entry

    candidates = list(by_concept.values())
    candidates.sort(key=lambda item: (-item["_score"], item["concept"].casefold(), item["_key"]))
    return candidates


def build_terminology_context(
    records: Sequence[Any],
    *,
    max_entries: int = 12,
) -> str:
    """Build a patient-invariant deduplicated ``terminology_context`` JSON block.

    Because this depends solely on the patient's chart ledger (never the turn question), it
    can lead ``state.gathered`` in ``msg[0]`` without busting cross-turn KV prefix caching.
    """
    candidates = _collect_glossary_candidates(records)
    if not candidates or max_entries <= 0:
        return ""
    selected = []
    for item in candidates[:max_entries]:
        cleaned = {k: v for k, v in item.items() if not k.startswith("_")}
        selected.append(cleaned)
    return "Terminology context:\n" + json.dumps(
        {"terminology_context": selected},
        ensure_ascii=False,
        separators=(",", ":"),
    )


def build_turn_terminology_hints(
    records: Sequence[Any],
    question: str,
    *,
    already_included_concepts: Iterable[str] = (),
    max_entries: int = 4,
) -> str:
    """Build compact question-specific alias/mapping bridges absent from the patient-invariant glossary."""
    q_terms = {
        t.casefold()
        for t in _TOKEN_RE.findall(question or "")
        if len(t) >= 3
    }
    if not q_terms or max_entries <= 0:
        return ""
    seen = {c.casefold() for c in already_included_concepts}
    matched: list[dict[str, Any]] = []
    for item in _collect_glossary_candidates(records):
        if item["concept"].casefold() in seen:
            continue
        hay_tokens = {
            t.casefold()
            for field in (item["concept"], *item.get("aliases", ()), item.get("definition", ""))
            for t in _TOKEN_RE.findall(field)
        }
        overlap = len(q_terms & hay_tokens)
        if overlap <= 0:
            continue
        matched.append((overlap, item))
    if not matched:
        return ""
    matched.sort(key=lambda pair: (-pair[0], pair[1]["concept"].casefold()))
    selected = [
        {k: v for k, v in item.items() if not k.startswith("_")}
        for _score, item in matched[:max_entries]
    ]
    return "Query terminology bridge:\n" + json.dumps(
        {"query_terminology_bridge": selected},
        ensure_ascii=False,
        separators=(",", ":"),
    )
