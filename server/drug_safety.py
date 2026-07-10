"""Deterministic drug-reference injection + post-answer drug-safety validation — a
straight port of chartsearchai's Java `reference` package (DrugReference, DrugReferenceService,
DrugReferenceInjector, DrugSafetyValidator, PatientClinicalContext). No LLM call; every check is
regex/set-membership over the answer text and the patient's already-retrieved chart records.

Two matching modes, preserved exactly from the Java (mixing them up produces silent false
negatives): alias matching (question/answer text, dataset lookups) is WHOLE-WORD; patient-token
matching (allergy/condition/active-drug tokens) is substring `in`.
"""

import json
import os
import re
import threading
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

_DATASET_PATH = os.environ.get(
    "DRUG_SAFETY_DATASET_PATH",
    os.path.join(os.path.dirname(__file__), "drug_data", "drug-reference.json"))

# Which dataset FORMAT the configured path holds: "json" (curated rules, bundled default) or "atc"
# (a WHO ATC classification export the operator supplies). One-or-the-other, deployment-wide — the
# same source-format selection the ported Java drug-reference layer offered (ADR Decision 24).
_SOURCE_FORMAT = os.environ.get("DRUG_SAFETY_SOURCE_FORMAT", "json").strip().lower()

_CROSS_REACTIVITY_PATH = os.environ.get(
    "DRUG_SAFETY_CROSS_REACTIVITY_PATH",
    os.path.join(os.path.dirname(__file__), "drug_data", "cross-reactivity-groups.json"),
)

DEFAULT_WEIGHT_CONCEPT_UUID = "5089AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
DEFAULT_WEIGHT_MAX_AGE_DAYS = 90
_DISABLED_SENTINEL = "none"
_KILOGRAM_UNITS = {"kg", "kilogram", "kilograms"}

_ATC_SUBGROUP_PREFIX_LENGTH = 5

# A level-5 ATC substance code is 7 chars: one letter, two digits, two letters, two digits
# (e.g. M01AE01). Guards against a non-ATC/malformed file turning any 7-char token into a drug.
_ATC_LEVEL5 = re.compile(r"[A-Z]\d{2}[A-Z]{2}\d{2}")
# Parent-group code lengths to try for a substance's drug_class, longest first: level 4, 3, 2.
_ATC_PARENT_LENGTHS = (5, 4, 3)

_DOSE_MG = re.compile(r"(\d+(?:\.\d+)?)\s*mg\b")
_EVERY_N_HOURS = re.compile(r"(?:every\s+(\d+)\s*(?:hours|hrs|hr|h)\b|q(\d+)h\b|(\d+)\s*hourly\b)")
_FREQ_QID = re.compile(r"\b(?:four times|qid|qds)\b")
_FREQ_TID = re.compile(r"\b(?:three times|thrice|tid|tds)\b")
_FREQ_BID = re.compile(r"\b(?:twice|two times|bid|bd)\b")
_FREQ_OD = re.compile(r"\b(?:once daily|once a day|once|od|daily)\b")
_LIMIT_CUE = re.compile(
    r"(?:maximum|max|up to|no more than|not exceed|do not exceed|exceeds?|ceiling|limit|less than|under)\b\W*$")
_LIMIT_CUE_LOOKBACK = 24
_CLAUSE_DELIMITER = re.compile(r"[;!?\n]+|\.(?!\d)")
_MAX_ALIAS_TO_DOSE_DISTANCE = 120

TYPE_OVERDOSE = "overdose"
TYPE_INTERACTION = "interaction"
TYPE_CONTRAINDICATION = "contraindication"

RESOURCE_TYPE_DRUG_REFERENCE = "drug_reference"


def _format_number(value: float) -> str:
    if value == int(value):
        return str(int(value))
    return str(value)


def _clean_text(value: Any) -> Optional[str]:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _clean_text_list(values: Any) -> List[str]:
    return [cleaned for value in (values or []) if (cleaned := _clean_text(value))]


@dataclass
class AgeBand:
    min_years: int
    max_years: int
    mg_per_kg_min: float = 0.0
    mg_per_kg_max: float = 0.0
    max_daily_dose_mg: float = 0.0


@dataclass
class Interaction:
    token: Optional[str] = None
    atc: Optional[str] = None
    note: Optional[str] = None


@dataclass
class Contraindication:
    type: str = ""
    token: str = ""
    note: Optional[str] = None


@dataclass
class DrugReferenceEntry:
    id: str
    name: str
    drug_class: Optional[str] = None
    aliases: List[str] = field(default_factory=list)
    atc_codes: List[str] = field(default_factory=list)
    age_bands: List[AgeBand] = field(default_factory=list)
    interactions: List[Interaction] = field(default_factory=list)
    contraindications: List[Contraindication] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    source: Optional[str] = None

    def normalized_atc_codes(self) -> Set[str]:
        return {c.strip().upper() for c in self.atc_codes if c and c.strip()}

    def atc_subgroups(self) -> Set[str]:
        return {c[:_ATC_SUBGROUP_PREFIX_LENGTH] for c in self.normalized_atc_codes()
                if len(c) >= _ATC_SUBGROUP_PREFIX_LENGTH}

    def band_for_age(self, age_years: Optional[int]) -> Optional[AgeBand]:
        if age_years is None:
            return None
        for band in self.age_bands:
            if band.min_years <= age_years <= band.max_years:
                return band
        return None

    def matches_text(self, lower_text: Optional[str]) -> bool:
        if not lower_text:
            return False
        for alias in self.aliases:
            if not alias:
                continue
            a = alias.lower()
            start = 0
            while True:
                idx = lower_text.find(a, start)
                if idx < 0:
                    break
                end = idx + len(a)
                left_ok = idx == 0 or not lower_text[idx - 1].isalnum()
                right_ok = end >= len(lower_text) or not lower_text[end].isalnum()
                if left_ok and right_ok:
                    return True
                start = idx + 1
        return False


@dataclass
class CrossReactivityGroup:
    """Curated family spanning ATC branches that the ATC hierarchy cannot connect."""

    name: str
    atc_prefixes: List[str] = field(default_factory=list)
    note: Optional[str] = None

    def normalized_prefixes(self) -> Set[str]:
        return {prefix.strip().upper() for prefix in self.atc_prefixes
                if isinstance(prefix, str) and prefix.strip()}

    def contains_code(self, code: Optional[str]) -> bool:
        if not isinstance(code, str) or not code.strip():
            return False
        normalized = code.strip().upper()
        return any(normalized.startswith(prefix) for prefix in self.normalized_prefixes())

    def contains_entry(self, entry: DrugReferenceEntry) -> bool:
        return any(self.contains_code(code) for code in entry.normalized_atc_codes())


def _entry_from_dict(d: Dict[str, Any]) -> DrugReferenceEntry:
    age_bands = []
    for band in d.get("ageBands") or []:
        if not isinstance(band, dict) or "minYears" not in band or "maxYears" not in band:
            continue
        try:
            age_bands.append(AgeBand(
                min_years=band["minYears"], max_years=band["maxYears"],
                mg_per_kg_min=band.get("mgPerKgMin", 0.0),
                mg_per_kg_max=band.get("mgPerKgMax", 0.0),
                max_daily_dose_mg=band.get("maxDailyDoseMg", 0.0),
            ))
        except (TypeError, ValueError):
            continue
    return DrugReferenceEntry(
        id=_clean_text(d.get("id")) or "",
        name=_clean_text(d.get("name")) or "",
        drug_class=_clean_text(d.get("drugClass")),
        aliases=_clean_text_list(d.get("aliases")),
        atc_codes=_clean_text_list(d.get("atcCodes")),
        age_bands=age_bands,
        interactions=[Interaction(token=i.get("token"), atc=i.get("atc"), note=i.get("note"))
                      for i in (d.get("interactions") or []) if isinstance(i, dict)],
        contraindications=[Contraindication(type=c.get("type", ""), token=c.get("token", ""), note=c.get("note"))
                            for c in (d.get("contraindications") or []) if isinstance(c, dict)],
        warnings=_clean_text_list(d.get("warnings")),
        source=_clean_text(d.get("source")),
    )


class DrugReferenceDataset:
    """Loaded + indexed drug-reference entries. Mirrors DrugReferenceService's query surface."""

    def __init__(self, entries: List[DrugReferenceEntry],
                 cross_reactivity_groups: Optional[List[CrossReactivityGroup]] = None):
        self.entries = entries
        self.cross_reactivity_groups = list(cross_reactivity_groups or [])

    def find_by_query(self, text: Optional[str]) -> List[DrugReferenceEntry]:
        if not text or not text.strip():
            return []
        lower = text.lower()
        return [e for e in self.entries if e.matches_text(lower)]

    def find_by_active_orders(self, context: "PatientClinicalContext") -> List[DrugReferenceEntry]:
        if not context.active_drug_atc_codes:
            return []
        out = []
        for e in self.entries:
            if e.normalized_atc_codes() & context.active_drug_atc_codes:
                out.append(e)
        return out

    def lookup_by_token(self, token: Optional[str]) -> Optional[DrugReferenceEntry]:
        if not token or not token.strip():
            return None
        lower = token.lower()
        for e in self.entries:
            if e.matches_text(lower):
                return e
        return None

    def display_name_for_atc_code(self, upper_code: str) -> str:
        for e in self.entries:
            if upper_code in e.normalized_atc_codes():
                return e.name
        return upper_code

    def groups_for(self, entry: DrugReferenceEntry) -> List[CrossReactivityGroup]:
        return [group for group in self.cross_reactivity_groups if group.contains_entry(entry)]

    def shared_group(self, first: DrugReferenceEntry,
                     second: DrugReferenceEntry) -> Optional[CrossReactivityGroup]:
        return next((group for group in self.groups_for(first) if group.contains_entry(second)), None)

    def shared_group_for_code(self, entry: DrugReferenceEntry,
                              code: str) -> Optional[CrossReactivityGroup]:
        return next((group for group in self.groups_for(entry) if group.contains_code(code)), None)


_lock = threading.Lock()
_dataset: Optional[DrugReferenceDataset] = None


def _load_entries(path: str) -> List[DrugReferenceEntry]:
    if not path or not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return []
    entries: List[DrugReferenceEntry] = []
    for raw_entry in raw.get("entries", []) if isinstance(raw, dict) else []:
        if (not isinstance(raw_entry, dict) or not _clean_text(raw_entry.get("id"))
                or not _clean_text(raw_entry.get("name"))):
            continue
        try:
            entries.append(_entry_from_dict(raw_entry))
        except (KeyError, TypeError, ValueError):
            continue
    return entries


def _load_cross_reactivity_groups(path: Optional[str]) -> List[CrossReactivityGroup]:
    if not path or path.strip().lower() == _DISABLED_SENTINEL or not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return []
    groups: List[CrossReactivityGroup] = []
    for item in raw.get("groups", []) if isinstance(raw, dict) else []:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        group = CrossReactivityGroup(
            name=name.strip() if isinstance(name, str) else "",
            atc_prefixes=list(item.get("atcPrefixes") or []),
            note=item.get("note"),
        )
        if group.name and group.normalized_prefixes():
            groups.append(group)
    return groups


def _load_atc_entries(path: str) -> List[DrugReferenceEntry]:
    """Parse a WHO ATC classification export into classification entries — the Python port of Java
    AtcDrugReferenceSource. Each non-blank, non-``#``-comment line is ``<atcCode><whitespace><name>``
    for ALL levels; one entry is emitted per level-5 substance (a 7-char valid ATC code), carrying
    its name, code, a lowercase alias for matching, and a ``drug_class`` derived from the nearest
    parent group PRESENT IN THE SAME DATASET (level 4 -> 3 -> 2). ATC is a classification, not a
    rulebook, so entries carry no dosing/interaction/contraindication rules — safety comes from
    ATC-class reasoning. Fail-safe: a missing/unreadable dataset degrades to [] (never raises), so
    the drug-reference feature stays an additive net that cannot break the answer path."""
    if not path or not os.path.exists(path):
        return []
    # code -> name for ALL levels, preserving file order so substances emit in dataset order and a
    # substance's class can be resolved from its parent-group names. Codes are upper-cased because
    # ATC/RxNorm crosswalk exports are not all upper case and the rest of the pipeline compares upper.
    names: Dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                trimmed = line.strip()
                if not trimmed or trimmed.startswith("#"):
                    continue
                parts = re.split(r"\s+", trimmed, maxsplit=1)
                if len(parts) < 2:
                    continue
                code = parts[0].strip().upper()
                name = parts[1].strip()
                if code and name:
                    names[code] = name
    except OSError:
        return []

    def _nearest_group(code: str) -> Optional[str]:
        for length in _ATC_PARENT_LENGTHS:
            if len(code) > length:
                parent = names.get(code[:length])
                if parent is not None:
                    return parent
        return None

    out: List[DrugReferenceEntry] = []
    for code, name in names.items():
        if len(code) == 7 and _ATC_LEVEL5.fullmatch(code):
            out.append(DrugReferenceEntry(
                id=code, name=name, drug_class=_nearest_group(code),
                aliases=[name.lower()], atc_codes=[code], source="atc"))
    return out


def _load_source(path: str, source_format: str) -> List[DrugReferenceEntry]:
    if source_format == "atc":
        return _load_atc_entries(path)
    return _load_entries(path)


def load_dataset(path: Optional[str] = None, source_format: Optional[str] = None,
                 cross_reactivity_path: Optional[str] = None) -> DrugReferenceDataset:
    """Lazy singleton for the default path+format; an explicit path always loads fresh (test seam).
    ``source_format`` selects the adapter (``json``|``atc``); defaults to the DRUG_SAFETY_SOURCE_FORMAT env."""
    fmt = (source_format or _SOURCE_FORMAT or "json").strip().lower()
    groups_path = _CROSS_REACTIVITY_PATH if cross_reactivity_path is None else cross_reactivity_path
    global _dataset
    if path is not None:
        return DrugReferenceDataset(
            _load_source(path, fmt), _load_cross_reactivity_groups(groups_path)
        )
    if _dataset is not None:
        return _dataset
    with _lock:
        if _dataset is None:
            _dataset = DrugReferenceDataset(
                _load_source(_DATASET_PATH, fmt), _load_cross_reactivity_groups(groups_path)
            )
    return _dataset


@dataclass
class PatientClinicalContext:
    age_years: Optional[int]
    weight_kg: Optional[float] = None
    active_drug_names: Set[str] = field(default_factory=set)
    active_drug_atc_codes: Set[str] = field(default_factory=set)
    allergy_tokens: Set[str] = field(default_factory=set)
    condition_tokens: Set[str] = field(default_factory=set)

    def __post_init__(self):
        self.active_drug_names = {s.strip().lower() for s in self.active_drug_names if s and s.strip()}
        self.active_drug_atc_codes = {s.strip().upper() for s in self.active_drug_atc_codes if s and s.strip()}
        self.allergy_tokens = {s.strip().lower() for s in self.allergy_tokens if s and s.strip()}
        self.condition_tokens = {s.strip().lower() for s in self.condition_tokens if s and s.strip()}

    def has_active_drug(self, name_token: Optional[str], atc_code: Optional[str]) -> bool:
        if name_token and name_token.strip():
            n = name_token.strip().lower()
            if any(n in drug for drug in self.active_drug_names):
                return True
        if atc_code and atc_code.strip() and atc_code.strip().upper() in self.active_drug_atc_codes:
            return True
        return False

    def has_allergy_token(self, token: str) -> bool:
        return _contains_token(self.allergy_tokens, token)

    def has_condition_token(self, token: str) -> bool:
        return _contains_token(self.condition_tokens, token)


def _contains_token(haystack: Set[str], token: Optional[str]) -> bool:
    if not token or not token.strip():
        return False
    t = token.strip().lower()
    return any(t in value for value in haystack)


@dataclass
class SafetyWarning:
    type: str
    drug: str
    detail: str

    def to_dict(self) -> Dict[str, str]:
        return {"type": self.type, "drug": self.drug, "detail": self.detail}


# ---------------------------------------------------------------------------
# Patient context construction from raw querystore records
# ---------------------------------------------------------------------------

def build_patient_context(records: List[Dict[str, Any]], reference_date: Optional[str],
                           dataset: Optional[DrugReferenceDataset] = None, *,
                           weight_concept_uuid: Optional[str] = None,
                           weight_max_age_days: Optional[int] = None) -> PatientClinicalContext:
    """Builds a PatientClinicalContext from raw querystore records (resourceType/metadata),
    mirroring PatientClinicalContextBuilder. querystore does not expose ATC codes on drug_order
    metadata (confirmed against the Java serializer, 2026-07-05), so active-drug ATC codes are
    resolved via a dataset alias lookup on the order's drug name — a stated simplification, fine
    for the curated demo dataset; a mis-resolution here means a missed interaction/duplicate-
    therapy check, never a false positive, since an unresolved name just contributes no ATC code.
    """
    dataset = dataset or load_dataset()
    age_years: Optional[int] = None
    drug_names: Set[str] = set()
    atc_codes: Set[str] = set()
    allergy_tokens: Set[str] = set()
    condition_tokens: Set[str] = set()
    weights: List[Tuple[date, float]] = []
    configured_weight_concept = (
        weight_concept_uuid
        if weight_concept_uuid is not None
        else os.environ.get("DRUG_SAFETY_WEIGHT_CONCEPT_UUID", DEFAULT_WEIGHT_CONCEPT_UUID)
    ).strip()
    weight_enabled = configured_weight_concept.lower() != _DISABLED_SENTINEL
    if weight_max_age_days is None:
        try:
            weight_max_age_days = int(os.environ.get(
                "DRUG_SAFETY_WEIGHT_MAX_AGE_DAYS", str(DEFAULT_WEIGHT_MAX_AGE_DAYS)
            ))
        except (TypeError, ValueError):
            weight_max_age_days = DEFAULT_WEIGHT_MAX_AGE_DAYS
    if weight_max_age_days <= 0:
        weight_max_age_days = DEFAULT_WEIGHT_MAX_AGE_DAYS
    try:
        anchor_date = date.fromisoformat((reference_date or "")[:10])
    except ValueError:
        anchor_date = None

    for rec in records or []:
        rtype = rec.get("resourceType")
        meta = rec.get("metadata") or {}
        if rtype == "patient":
            if age_years is None and meta.get("age_years") is not None:
                age_years = meta.get("age_years")
        elif rtype == "drug_order":
            if not _order_is_active(meta, reference_date):
                continue
            name = meta.get("drug_name") or meta.get("concept_name")
            if name:
                drug_names.add(name)
                entry = dataset.lookup_by_token(name)
                if entry:
                    atc_codes |= entry.normalized_atc_codes()
        elif rtype == "allergy":
            for key in ("allergen_name", "allergen_non_coded"):
                if meta.get(key):
                    allergy_tokens.add(meta[key])
        elif rtype == "condition":
            for key in ("concept_name", "non_coded"):
                if meta.get(key):
                    condition_tokens.add(meta[key])

        elif rtype == "obs" and weight_enabled and meta.get("concept_uuid") == configured_weight_concept:
            units = _clean_text(meta.get("units"))
            if units is None or units.casefold() not in _KILOGRAM_UNITS:
                continue
            try:
                value = float(meta.get("value_numeric"))
                observed = date.fromisoformat(str(rec.get("date") or "")[:10])
            except (TypeError, ValueError):
                continue
            if value <= 0 or anchor_date is None or observed > anchor_date:
                continue
            if observed >= anchor_date - timedelta(days=weight_max_age_days):
                weights.append((observed, value))

    weight_kg = max(weights, key=lambda item: item[0])[1] if weights else None
    return PatientClinicalContext(age_years=age_years, weight_kg=weight_kg, active_drug_names=drug_names,
                                   active_drug_atc_codes=atc_codes, allergy_tokens=allergy_tokens,
                                   condition_tokens=condition_tokens)


def _order_is_active(meta: Dict[str, Any], reference_date: Optional[str]) -> bool:
    if meta.get("date_stopped"):
        return False
    expire = meta.get("auto_expire_date")
    if expire and reference_date and expire < reference_date:
        return False
    return True


# ---------------------------------------------------------------------------
# Injection (Part 1)
# ---------------------------------------------------------------------------

def _related_to_any(order: DrugReferenceEntry, question_drugs: List[DrugReferenceEntry],
                    dataset: DrugReferenceDataset) -> bool:
    order_subgroups = order.atc_subgroups()
    return any(
        bool(order_subgroups & question_drug.atc_subgroups())
        or dataset.shared_group(order, question_drug) is not None
        for question_drug in question_drugs
    )


def _render_entry(ref: DrugReferenceEntry, age: Optional[int]) -> str:
    parts = [f"Drug reference — {ref.name}"]
    paren_bits = []
    if ref.drug_class:
        paren_bits.append(ref.drug_class)
    atc_codes = _clean_text_list(ref.atc_codes)
    if atc_codes:
        paren_bits.append("ATC " + ", ".join(atc_codes))
    if paren_bits:
        parts.append(f" ({'; '.join(paren_bits)})")
    parts.append(".")

    band = ref.band_for_age(age)
    if band is not None:
        parts.append(f" Dosing for ages {band.min_years}-{band.max_years}: "
                      f"{_format_number(band.mg_per_kg_min)}-{_format_number(band.mg_per_kg_max)} mg/kg per dose")
        if band.max_daily_dose_mg > 0:
            parts.append(f", maximum {_format_number(band.max_daily_dose_mg)} mg/day")
        else:
            parts.append(" (no pediatric daily maximum published for this age — consult a dosing reference)")
        parts.append(".")

    if ref.contraindications:
        notes = [_clean_text(c.note) or _clean_text(c.token) for c in ref.contraindications if c is not None]
        notes = [note for note in notes if note]
        if notes:
            parts.append(" Contraindicated with: " + "; ".join(notes) + ".")

    if ref.interactions:
        notes = []
        for i in ref.interactions:
            if i is None:
                continue
            label = _clean_text(i.token) or _clean_text(i.atc)
            note = _clean_text(i.note)
            rendered = f"{label} ({note})" if label and note else label or note
            if rendered:
                notes.append(rendered)
        if notes:
            parts.append(" Interactions: " + "; ".join(notes) + ".")

    if ref.warnings:
        parts.append(" Warnings: " + "; ".join(ref.warnings) + ".")

    if ref.source:
        parts.append(f" Source: {ref.source}.")
    return "".join(parts)


def _inject_drug_references(
        chart_text: str, mappings: List[Dict[str, Any]], question: Optional[str], age: Optional[int],
        dataset: DrugReferenceDataset, *, active_order_atc_codes: Optional[Set[str]] = None,
        inject_from_query: bool = True, inject_from_orders: bool = True,
) -> Tuple[str, List[Dict[str, Any]]]:
    """Appends matching drug-reference entries to the chart as additional numbered, citable
    records — question-driven (alias hit) and order-driven (ATC match, scoped to entries related
    to a question-named drug). Returns (chart_text, mappings) unchanged when nothing matches.
    """
    question_drugs = dataset.find_by_query(question)
    by_id: Dict[str, DrugReferenceEntry] = {}

    if inject_from_query:
        for ref in question_drugs:
            by_id[ref.id] = ref

    if inject_from_orders and active_order_atc_codes:
        context = PatientClinicalContext(age_years=None, active_drug_atc_codes=active_order_atc_codes)
        for ref in dataset.find_by_active_orders(context):
            if _related_to_any(ref, question_drugs, dataset):
                by_id[ref.id] = ref

    matched = list(by_id.values())
    if not matched:
        return chart_text, mappings

    out_mappings = list(mappings)
    text = chart_text
    index = len(out_mappings) + 1
    for ref in matched:
        rendered = _render_entry(ref, age)
        out_mappings.append({"index": index, "resourceType": RESOURCE_TYPE_DRUG_REFERENCE,
                              "resourceUuid": ref.id, "date": None, "text": rendered})
        text = text + f"[{index}] {rendered}\n"
        index += 1
    return text, out_mappings


def inject_drug_references(
        chart_text: str, mappings: List[Dict[str, Any]], question: Optional[str], age: Optional[int],
        dataset: DrugReferenceDataset, *, active_order_atc_codes: Optional[Set[str]] = None,
        inject_from_query: bool = True, inject_from_orders: bool = True,
) -> Tuple[str, List[Dict[str, Any]]]:
    """Fail-safe public injection boundary: reference-data failures never break an answer."""
    try:
        return _inject_drug_references(
            chart_text, mappings, question, age, dataset,
            active_order_atc_codes=active_order_atc_codes,
            inject_from_query=inject_from_query,
            inject_from_orders=inject_from_orders,
        )
    except (AttributeError, KeyError, TypeError, ValueError, RuntimeError):
        return chart_text, mappings


# ---------------------------------------------------------------------------
# Validation (Part 2)
# ---------------------------------------------------------------------------

def frequency_per_day(window: str) -> int:
    m = _EVERY_N_HOURS.search(window)
    if m:
        n = m.group(1) or m.group(2) or m.group(3)
        try:
            h = int(n)
            if h > 0:
                return round(24.0 / h)
        except ValueError:
            pass
    if _FREQ_QID.search(window):
        return 4
    if _FREQ_TID.search(window):
        return 3
    if _FREQ_BID.search(window):
        return 2
    if _FREQ_OD.search(window):
        return 1
    return 0


def _preceded_by_limit_cue(clause: str, dose_pos: int) -> bool:
    start = max(0, dose_pos - _LIMIT_CUE_LOOKBACK)
    return bool(_LIMIT_CUE.search(clause[start:dose_pos]))


def _nearest_alias_distance(text: str, pos: int, ref: DrugReferenceEntry) -> float:
    best = float("inf")
    for alias in ref.aliases:
        if not alias:
            continue
        a = alias.lower()
        start = 0
        while True:
            idx = text.find(a, start)
            if idx < 0:
                break
            end = idx + len(a)
            dist = (idx - pos) if pos < idx else ((pos - end) if pos > end else 0)
            if dist < best:
                best = dist
            start = idx + 1
    return best


def _alias_owns_dose(clause: str, dose_pos: int, ref: DrugReferenceEntry,
                      all_entries: List[DrugReferenceEntry]) -> bool:
    mine = _nearest_alias_distance(clause, dose_pos, ref)
    if mine == float("inf") or mine > _MAX_ALIAS_TO_DOSE_DISTANCE:
        return False
    for other in all_entries:
        if other is not ref and _nearest_alias_distance(clause, dose_pos, other) < mine:
            return False
    return True


def _parse_stated_dose_mg(lower_answer: str, ref: DrugReferenceEntry,
                          all_entries: List[DrugReferenceEntry]) -> Tuple[Optional[float], Optional[float]]:
    max_per_dose: Optional[float] = None
    max_daily: Optional[float] = None
    for clause in _CLAUSE_DELIMITER.split(lower_answer):
        if not ref.matches_text(clause):
            continue
        for m in _DOSE_MG.finditer(clause):
            dose_pos = m.start()
            if _preceded_by_limit_cue(clause, dose_pos) or not _alias_owns_dose(clause, dose_pos, ref, all_entries):
                continue
            try:
                per_dose = float(m.group(1))
            except ValueError:
                continue
            freq = frequency_per_day(clause)
            daily = per_dose * (freq if freq > 0 else 1)
            if max_per_dose is None or per_dose > max_per_dose:
                max_per_dose = per_dose
            if max_daily is None or daily > max_daily:
                max_daily = daily
    return max_per_dose, max_daily


def _add_contraindications(warnings: List[SafetyWarning], ref: DrugReferenceEntry,
                            context: PatientClinicalContext) -> None:
    for c in ref.contraindications:
        if c is None or not _clean_text(c.type) or not _clean_text(c.token):
            continue
        hit = False
        against = None
        if c.type.lower() == "allergy" and context.has_allergy_token(c.token):
            hit, against = True, "active allergy"
        elif c.type.lower() == "condition" and context.has_condition_token(c.token):
            hit, against = True, "active condition"
        if hit:
            note = _clean_text(c.note) or c.token
            warnings.append(SafetyWarning(TYPE_CONTRAINDICATION, ref.name,
                                           f"contraindicated by {against}: {note}"))


def _add_interactions(warnings: List[SafetyWarning], ref: DrugReferenceEntry,
                       context: PatientClinicalContext) -> None:
    for i in ref.interactions:
        if i is None:
            continue
        if context.has_active_drug(i.token, i.atc):
            label = _clean_text(i.token) or _clean_text(i.atc)
            if not label:
                continue
            detail = f"interacts with active order {label}"
            if _clean_text(i.note):
                detail += f" — {i.note.strip()}"
            warnings.append(SafetyWarning(TYPE_INTERACTION, ref.name, detail))


def _add_class_contraindications(warnings: List[SafetyWarning], ref: DrugReferenceEntry,
                                  context: PatientClinicalContext, dataset: DrugReferenceDataset) -> None:
    ref_classes = ref.atc_subgroups()
    if not ref_classes:
        return
    seen_allergens: Set[str] = set()
    for allergy_token in context.allergy_tokens:
        allergen = dataset.lookup_by_token(allergy_token)
        if allergen is None or allergen.id in seen_allergens:
            continue
        seen_allergens.add(allergen.id)
        if allergen is ref or allergen.id == ref.id:
            warnings.append(SafetyWarning(TYPE_CONTRAINDICATION, ref.name,
                                           f"the patient has a recorded allergy to {ref.name}"))
            continue
        shared = next((cls for cls in allergen.atc_subgroups() if cls in ref_classes), None)
        if shared:
            warnings.append(SafetyWarning(
                TYPE_CONTRAINDICATION, ref.name,
                f"same ATC class ({shared}) as the patient's allergy to {allergen.name} — possible cross-reactivity"))
            continue
        group = dataset.shared_group(ref, allergen)
        if group is not None:
            warnings.append(SafetyWarning(
                TYPE_CONTRAINDICATION, ref.name,
                f"same cross-reactivity group ({group.name}) as the patient's allergy to "
                f"{allergen.name} — possible cross-reactivity"))


def _add_class_interactions(warnings: List[SafetyWarning], ref: DrugReferenceEntry,
                             context: PatientClinicalContext, dataset: DrugReferenceDataset) -> None:
    ref_classes = ref.atc_subgroups()
    if not ref_classes:
        return
    ref_codes = ref.normalized_atc_codes()
    for order_code in context.active_drug_atc_codes:
        if order_code in ref_codes:
            continue
        order_class = order_code[:_ATC_SUBGROUP_PREFIX_LENGTH]
        if len(order_code) >= _ATC_SUBGROUP_PREFIX_LENGTH and order_class in ref_classes:
            warnings.append(SafetyWarning(
                TYPE_INTERACTION, ref.name,
                f"same ATC class ({order_class}) as active order {dataset.display_name_for_atc_code(order_code)}"
                " — possible duplicate therapy"))
            continue
        group = dataset.shared_group_for_code(ref, order_code)
        if group is not None:
            warnings.append(SafetyWarning(
                TYPE_INTERACTION, ref.name,
                f"same cross-reactivity group ({group.name}) as active order "
                f"{dataset.display_name_for_atc_code(order_code)} — possible duplicate therapy"))


def _add_overdose(warnings: List[SafetyWarning], ref: DrugReferenceEntry, context: PatientClinicalContext,
                   lower_answer: str, all_entries: List[DrugReferenceEntry]) -> None:
    age = context.age_years if context else None
    band = ref.band_for_age(age)
    if band is None:
        return
    per_dose_mg, daily_mg = _parse_stated_dose_mg(lower_answer, ref, all_entries)
    if (band.max_daily_dose_mg > 0 and daily_mg is not None
            and daily_mg > band.max_daily_dose_mg):
        warnings.append(SafetyWarning(
            TYPE_OVERDOSE, ref.name,
            f"stated dose ~{_format_number(daily_mg)} mg/day exceeds the "
            f"{_format_number(band.max_daily_dose_mg)} mg/day maximum for ages "
            f"{band.min_years}-{band.max_years}"))
        return
    if (band.mg_per_kg_max > 0 and context.weight_kg is not None and per_dose_mg is not None
            and per_dose_mg > band.mg_per_kg_max * context.weight_kg):
        warnings.append(SafetyWarning(
            TYPE_OVERDOSE, ref.name,
            f"stated dose {_format_number(per_dose_mg)} mg exceeds the "
            f"{_format_number(band.mg_per_kg_max)} mg/kg per-dose maximum for the patient's "
            f"{_format_number(context.weight_kg)} kg weight"))


def _validate_answer(answer: Optional[str], question: Optional[str], context: PatientClinicalContext,
                     dataset: DrugReferenceDataset, *, warn_dose: bool = True, warn_interactions: bool = True,
                     warn_contraindications: bool = True) -> List[SafetyWarning]:
    """Pure validation — the drugs checked are the union of what the QUESTION resolves to and what
    the ANSWER names, via the same find_by_query the injector uses (so question/answer/injector
    matching never drifts). Returns [] when nothing is flagged — never None.
    """
    warnings: List[SafetyWarning] = []
    lower_answer = (answer or "").lower()
    all_entries = dataset.entries

    in_play: Dict[str, DrugReferenceEntry] = {}
    for ref in dataset.find_by_query(question):
        in_play[ref.id] = ref
    for ref in dataset.find_by_query(answer):
        in_play[ref.id] = ref

    for ref in in_play.values():
        if warn_contraindications:
            _add_contraindications(warnings, ref, context)
            _add_class_contraindications(warnings, ref, context, dataset)
        if warn_interactions:
            _add_interactions(warnings, ref, context)
            _add_class_interactions(warnings, ref, context, dataset)
        if warn_dose:
            _add_overdose(warnings, ref, context, lower_answer, all_entries)

    return warnings


def validate_answer(answer: Optional[str], question: Optional[str], context: PatientClinicalContext,
                    dataset: DrugReferenceDataset, *, warn_dose: bool = True, warn_interactions: bool = True,
                    warn_contraindications: bool = True) -> List[SafetyWarning]:
    """Fail-safe safety boundary: incomplete reference data cannot break the answer path."""
    try:
        return _validate_answer(
            answer, question, context, dataset,
            warn_dose=warn_dose,
            warn_interactions=warn_interactions,
            warn_contraindications=warn_contraindications,
        )
    except (AttributeError, KeyError, TypeError, ValueError, RuntimeError):
        return []
