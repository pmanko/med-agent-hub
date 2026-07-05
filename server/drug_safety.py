"""Deterministic drug-reference injection + post-answer drug-safety validation — a
straight port of chartsearchai's Java `reference` package (DrugReference, DrugReferenceService,
DrugReferenceInjector, DrugSafetyValidator, PatientClinicalContext). No LLM call; every check is
regex/set-membership over the answer text and the patient's already-retrieved chart records.

Two matching modes, preserved exactly from the Java (mixing them up produces silent false
negatives): alias matching (question/answer text, dataset lookups) is WHOLE-WORD; patient-token
matching (allergy/condition/active-drug tokens) is substring `in`.
"""

import os
import re
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

_DATASET_PATH = os.environ.get(
    "DRUG_SAFETY_DATASET_PATH",
    os.path.join(os.path.dirname(__file__), "drug_data", "drug-reference.json"))

_ATC_SUBGROUP_PREFIX_LENGTH = 5

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


def _entry_from_dict(d: Dict[str, Any]) -> DrugReferenceEntry:
    return DrugReferenceEntry(
        id=d.get("id"),
        name=d.get("name"),
        drug_class=d.get("drugClass"),
        aliases=list(d.get("aliases") or []),
        atc_codes=list(d.get("atcCodes") or []),
        age_bands=[AgeBand(min_years=b["minYears"], max_years=b["maxYears"],
                            mg_per_kg_min=b.get("mgPerKgMin", 0.0), mg_per_kg_max=b.get("mgPerKgMax", 0.0),
                            max_daily_dose_mg=b.get("maxDailyDoseMg", 0.0))
                   for b in (d.get("ageBands") or [])],
        interactions=[Interaction(token=i.get("token"), atc=i.get("atc"), note=i.get("note"))
                      for i in (d.get("interactions") or [])],
        contraindications=[Contraindication(type=c.get("type", ""), token=c.get("token", ""), note=c.get("note"))
                            for c in (d.get("contraindications") or [])],
        source=d.get("source"),
    )


class DrugReferenceDataset:
    """Loaded + indexed drug-reference entries. Mirrors DrugReferenceService's query surface."""

    def __init__(self, entries: List[DrugReferenceEntry]):
        self.entries = entries

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


_lock = threading.Lock()
_dataset: Optional[DrugReferenceDataset] = None


def _load_entries(path: str) -> List[DrugReferenceEntry]:
    import json
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return [_entry_from_dict(e) for e in raw.get("entries", [])]


def load_dataset(path: Optional[str] = None) -> DrugReferenceDataset:
    """Lazy singleton for the default path; an explicit path always loads fresh (test seam)."""
    global _dataset
    if path is not None:
        return DrugReferenceDataset(_load_entries(path))
    if _dataset is not None:
        return _dataset
    with _lock:
        if _dataset is None:
            _dataset = DrugReferenceDataset(_load_entries(_DATASET_PATH))
    return _dataset


@dataclass
class PatientClinicalContext:
    age_years: Optional[int]
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
                           dataset: Optional[DrugReferenceDataset] = None) -> PatientClinicalContext:
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

    return PatientClinicalContext(age_years=age_years, active_drug_names=drug_names,
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

def _related_to_any(order: DrugReferenceEntry, question_drugs: List[DrugReferenceEntry]) -> bool:
    order_subgroups = order.atc_subgroups()
    if not order_subgroups:
        return False
    return any(order_subgroups & q.atc_subgroups() for q in question_drugs)


def _render_entry(ref: DrugReferenceEntry, age: Optional[int]) -> str:
    parts = [f"Drug reference — {ref.name}"]
    paren_bits = []
    if ref.drug_class:
        paren_bits.append(ref.drug_class)
    if ref.atc_codes:
        paren_bits.append("ATC " + ", ".join(ref.atc_codes))
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
        notes = [c.note if c.note else c.token for c in ref.contraindications]
        parts.append(" Contraindicated with: " + "; ".join(notes) + ".")

    if ref.interactions:
        notes = []
        for i in ref.interactions:
            label = i.token if i.token else i.atc
            notes.append(f"{label} ({i.note})" if i.note else label)
        parts.append(" Interactions: " + "; ".join(notes) + ".")

    if ref.source:
        parts.append(f" Source: {ref.source}.")
    return "".join(parts)


def inject_drug_references(
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
            if _related_to_any(ref, question_drugs):
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


def _parse_daily_dose_mg(lower_answer: str, ref: DrugReferenceEntry,
                          all_entries: List[DrugReferenceEntry]) -> Optional[float]:
    best: Optional[float] = None
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
            if best is None or daily > best:
                best = daily
    return best


def _add_contraindications(warnings: List[SafetyWarning], ref: DrugReferenceEntry,
                            context: PatientClinicalContext) -> None:
    for c in ref.contraindications:
        hit = False
        against = None
        if c.type.lower() == "allergy" and context.has_allergy_token(c.token):
            hit, against = True, "active allergy"
        elif c.type.lower() == "condition" and context.has_condition_token(c.token):
            hit, against = True, "active condition"
        if hit:
            note = c.note if c.note else c.token
            warnings.append(SafetyWarning(TYPE_CONTRAINDICATION, ref.name,
                                           f"contraindicated by {against}: {note}"))


def _add_interactions(warnings: List[SafetyWarning], ref: DrugReferenceEntry,
                       context: PatientClinicalContext) -> None:
    for i in ref.interactions:
        if context.has_active_drug(i.token, i.atc):
            detail = f"interacts with active order {i.token if i.token else i.atc}"
            if i.note:
                detail += f" — {i.note}"
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


def _add_class_interactions(warnings: List[SafetyWarning], ref: DrugReferenceEntry,
                             context: PatientClinicalContext, dataset: DrugReferenceDataset) -> None:
    ref_classes = ref.atc_subgroups()
    if not ref_classes:
        return
    ref_codes = ref.normalized_atc_codes()
    for order_code in context.active_drug_atc_codes:
        if len(order_code) < _ATC_SUBGROUP_PREFIX_LENGTH:
            continue
        order_class = order_code[:_ATC_SUBGROUP_PREFIX_LENGTH]
        if order_class not in ref_classes or order_code in ref_codes:
            continue
        warnings.append(SafetyWarning(
            TYPE_INTERACTION, ref.name,
            f"same ATC class ({order_class}) as active order {dataset.display_name_for_atc_code(order_code)}"
            " — possible duplicate therapy"))


def _add_overdose(warnings: List[SafetyWarning], ref: DrugReferenceEntry, context: PatientClinicalContext,
                   lower_answer: str, all_entries: List[DrugReferenceEntry]) -> None:
    age = context.age_years if context else None
    band = ref.band_for_age(age)
    if band is None or band.max_daily_dose_mg <= 0:
        return
    daily_mg = _parse_daily_dose_mg(lower_answer, ref, all_entries)
    if daily_mg is not None and daily_mg > band.max_daily_dose_mg:
        warnings.append(SafetyWarning(
            TYPE_OVERDOSE, ref.name,
            f"stated dose ~{_format_number(daily_mg)} mg/day exceeds the "
            f"{_format_number(band.max_daily_dose_mg)} mg/day maximum for ages "
            f"{band.min_years}-{band.max_years}"))


def validate_answer(answer: Optional[str], question: Optional[str], context: PatientClinicalContext,
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
