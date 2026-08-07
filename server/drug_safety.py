"""Deterministic drug-reference injection + post-answer drug-safety validation — a
straight port of chartsearchai's Java `reference` package (DrugReference, DrugReferenceService,
DrugReferenceInjector, DrugSafetyValidator, PatientClinicalContext). No LLM call; every check is
regex/set-membership over the answer text and the patient's already-retrieved chart records.

Two matching modes, preserved exactly from the Java (mixing them up produces silent false
negatives): alias matching (question/answer text, dataset lookups) is WHOLE-WORD; patient-token
matching (allergy/condition/active-drug tokens) is substring `in`.
"""

import json
import math
import os
import re
import threading
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

_DATASET_PATH = os.environ.get(
    "DRUG_SAFETY_DATASET_PATH",
    os.path.join(os.path.dirname(__file__), "drug_data", "drug-reference.json"))

# Which dataset FORMAT the configured path holds: "json" (package-shaped rules) or "atc"
# (a WHO ATC classification export the operator supplies). One-or-the-other, deployment-wide — the
# same source-format selection the ported Java drug-reference layer offered (ADR Decision 24).
_SOURCE_FORMAT = os.environ.get("DRUG_SAFETY_SOURCE_FORMAT", "json").strip().lower()

_CROSS_REACTIVITY_PATH = os.environ.get(
    "DRUG_SAFETY_CROSS_REACTIVITY_PATH",
    os.path.join(os.path.dirname(__file__), "drug_data", "cross-reactivity-groups.json"),
)

DEFAULT_WEIGHT_CONCEPT_UUID = "5089AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
DEFAULT_WEIGHT_MAX_AGE_DAYS = 90
DEFAULT_MIN_INTERACTION_SEVERITY = "minor"
_DISABLED_SENTINEL = "none"
_KILOGRAM_UNITS = {"kg", "kilogram", "kilograms"}
_INTERACTION_SEVERITY_RANKS = {
    "unknown": 0,
    "minor": 1,
    "moderate": 2,
    "major": 3,
}
_MIN_INT32 = -(2**31)
_MAX_INT32 = 2**31 - 1

_ATC_SUBGROUP_PREFIX_LENGTH = 5

# A level-5 ATC substance code is 7 chars: one letter, two digits, two letters, two digits
# (e.g. M01AE01). Guards against a non-ATC/malformed file turning any 7-char token into a drug.
_ATC_LEVEL5 = re.compile(r"[A-Z]\d{2}[A-Z]{2}\d{2}")
# A reviewed cross-reactivity package may target ATC levels 2-5, but never the
# one-letter anatomical level: that would turn one bad value into a very broad
# clinical match.
_ATC_GROUP_PREFIX = re.compile(r"[A-Z]\d{2}(?:[A-Z](?:[A-Z](?:\d{2})?)?)?")
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

# Honest drug-safety check states (dual-provider-conformance.v1 drug_safety_status): an empty
# warning list must never be presented as "checked" when the check could not actually run.
STATUS_CHECKED = "checked"
STATUS_LIMITED = "limited"
STATUS_UNAVAILABLE = "unavailable"

_DRUG_SAFETY_QUESTION = re.compile(
    r"\b(?:safe(?:ty)?|contraindicat(?:ed|ion)?|interact(?:ion|ions)?|"
    r"dose|dosing|overdose|allergic|allergy)\b",
    re.IGNORECASE,
)
_QUESTION_DRUG_SPANS = (
    re.compile(
        r"\b(?:is|are)\s+(.{1,120}?)\s+(?:safe|contraindicated)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:interactions?|contraindications?)\s+(?:between|with|for)\s+"
        r"(.{1,120}?)(?:[?.!]|$)",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:take|use|combine)\s+(.{1,120}?)(?:\s+(?:safely|together)|[?.!]|$)",
        re.IGNORECASE,
    ),
)
_DRUG_MENTION_SEPARATOR = re.compile(r"\s*(?:,|\band\b|\bwith\b|\bplus\b|/)\s*", re.IGNORECASE)
_ANSWER_DRUG_ACTION = re.compile(
    r"\b(?:start|take|prescribe|recommend|consider|give|switch\s+to)\s+"
    r"(?P<candidate>(?:(?:oral|intravenous|iv|topical|inhaled)\s+)?"
    r"[A-Za-z][A-Za-z0-9-]*(?:\s+[A-Za-z][A-Za-z0-9-]*){0,2}?)"
    r"(?=\s+(?:for|to|because|as|if|when|with|at|once|twice|daily|every)\b|[.,;!?]|$)",
    re.IGNORECASE,
)
_DRUG_LIKE_SUFFIX = re.compile(
    r"(?:cillin|cycline|floxacin|azole|mab|nib|pril|sartan|olol|statin|prazole|"
    r"triptan|caine|vir|mycin|parin|formin|profen)$",
    re.IGNORECASE,
)
_NON_DRUG_MENTION_WORDS = frozenset(
    {
        "any",
        "clinical",
        "close",
        "drug",
        "drugs",
        "exercise",
        "further",
        "medication",
        "medications",
        "monitoring",
        "my",
        "patient",
        "review",
        "that",
        "the",
        "there",
        "this",
    }
)
_ROUTE_PREFIX = re.compile(r"^(?:oral|intravenous|iv|topical|inhaled)\s+", re.IGNORECASE)
_QUESTION_TRAILING_CONTEXT = re.compile(
    r"\s+(?:together|for\s+(?:this|the)\s+patient|in\s+this\s+patient)$",
    re.IGNORECASE,
)

REVIEW_PROPOSED = "proposed"
REVIEW_EVIDENCE_CURATED = "evidence_curated"
REVIEW_CLINICALLY_APPROVED = "clinically_approved"
REVIEW_RETIRED = "retired"
_REVIEW_STATES = {
    REVIEW_PROPOSED,
    REVIEW_EVIDENCE_CURATED,
    REVIEW_CLINICALLY_APPROVED,
    REVIEW_RETIRED,
}


def _format_number(value: float) -> str:
    if value == int(value):
        return str(int(value))
    return str(value)


def _clean_text(value: Any) -> Optional[str]:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _clean_text_list(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []
    return [cleaned for value in (values or []) if (cleaned := _clean_text(value))]


def _finite_number(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("clinical value must be a JSON number")
    try:
        parsed = float(value)
    except OverflowError as exc:
        raise ValueError(
            "clinical value is outside the supported numeric range"
        ) from exc
    if not math.isfinite(parsed):
        raise ValueError("clinical value must be finite")
    return parsed


def _whole_year(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("age-band bounds must be integral JSON numbers")
    if value < _MIN_INT32 or value > _MAX_INT32:
        raise ValueError("age-band bounds must fit a signed 32-bit integer")
    return value


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
    severity: Optional[str] = None


def _interaction_from_dict(value: Any) -> Optional[Interaction]:
    if not isinstance(value, dict):
        return None
    token = _clean_text(value.get("token"))
    atc = _clean_text(value.get("atc"))
    if not (token or atc) or (atc and not _ATC_LEVEL5.fullmatch(atc.upper())):
        return None
    severity = None
    if "severity" in value and value.get("severity") is not None:
        severity = _clean_text(value.get("severity"))
        if severity is None or severity.lower() not in _INTERACTION_SEVERITY_RANKS:
            return None
    return Interaction(
        token=token,
        atc=atc,
        note=_clean_text(value.get("note")),
        severity=severity,
    )


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
    """Candidate family spanning ATC branches that the ATC hierarchy cannot connect."""

    name: str
    atc_prefixes: List[str] = field(default_factory=list)
    note: Optional[str] = None

    def normalized_prefixes(self) -> Set[str]:
        normalized = {
            prefix.strip().upper()
            for prefix in self.atc_prefixes
            if isinstance(prefix, str) and prefix.strip()
        }
        return {prefix for prefix in normalized if _ATC_GROUP_PREFIX.fullmatch(prefix)}

    def contains_code(self, code: Optional[str]) -> bool:
        if not isinstance(code, str) or not code.strip():
            return False
        normalized = code.strip().upper()
        return any(normalized.startswith(prefix) for prefix in self.normalized_prefixes())

    def contains_entry(self, entry: DrugReferenceEntry) -> bool:
        return any(self.contains_code(code) for code in entry.normalized_atc_codes())


@dataclass
class _CrossReactivityLoad:
    groups: List[CrossReactivityGroup]
    package: Dict[str, Any]
    issues: List[str]


def _entry_from_dict(d: Dict[str, Any]) -> DrugReferenceEntry:
    age_bands = []
    for band in d.get("ageBands") or []:
        if not isinstance(band, dict) or "minYears" not in band or "maxYears" not in band:
            continue
        try:
            parsed = AgeBand(
                min_years=_whole_year(band["minYears"]),
                max_years=_whole_year(band["maxYears"]),
                mg_per_kg_min=_finite_number(band.get("mgPerKgMin", 0.0)),
                mg_per_kg_max=_finite_number(band.get("mgPerKgMax", 0.0)),
                max_daily_dose_mg=_finite_number(
                    band.get("maxDailyDoseMg", 0.0)
                ),
            )
        except (TypeError, ValueError):
            continue
        if (
            parsed.min_years < 0
            or parsed.max_years < parsed.min_years
            or parsed.mg_per_kg_min < 0
            or parsed.mg_per_kg_max < 0
            or parsed.max_daily_dose_mg < 0
        ):
            continue
        age_bands.append(parsed)
    return DrugReferenceEntry(
        id=_clean_text(d.get("id")) or "",
        name=_clean_text(d.get("name")) or "",
        drug_class=_clean_text(d.get("drugClass")),
        aliases=_clean_text_list(d.get("aliases")),
        atc_codes=[code for code in _clean_text_list(d.get("atcCodes"))
                   if _ATC_LEVEL5.fullmatch(code.upper())],
        age_bands=age_bands,
        interactions=[
            parsed
            for item in (d.get("interactions") or [])
            if (parsed := _interaction_from_dict(item)) is not None
        ],
        contraindications=[
            Contraindication(type=rule_type, token=token,
                             note=_clean_text(item.get("note")))
            for item in (d.get("contraindications") or [])
            if isinstance(item, dict)
            for rule_type, token in [[
                (_clean_text(item.get("type")) or "").lower(),
                _clean_text(item.get("token")),
            ]]
            if rule_type in {"allergy", "condition"} and token
        ],
        warnings=_clean_text_list(d.get("warnings")),
        source=_clean_text(d.get("source")),
    )


class DrugReferenceDataset:
    """Loaded + indexed drug-reference entries. Mirrors DrugReferenceService's query surface."""

    def __init__(self, entries: List[DrugReferenceEntry],
                 cross_reactivity_groups: Optional[List[CrossReactivityGroup]] = None, *,
                 package_id: str = "unidentified-drug-reference",
                 source_format: str = "memory",
                 source_version: Optional[str] = None,
                 provenance: Optional[Dict[str, Any]] = None,
                 review_state: str = REVIEW_PROPOSED,
                 cross_reactivity_review_state: str = REVIEW_PROPOSED,
                 source_issues: Optional[List[str]] = None,
                 cross_reactivity_package: Optional[Dict[str, Any]] = None,
                 cross_reactivity_issues: Optional[List[str]] = None):
        self.entries = entries
        self.cross_reactivity_groups = list(cross_reactivity_groups or [])
        self.package_id = _clean_text(package_id) or "unidentified-drug-reference"
        self.source_format = _clean_text(source_format) or "unknown"
        self.source_version = _clean_text(source_version)
        self.provenance = dict(provenance or {})
        normalized_review = _clean_text(review_state) or REVIEW_PROPOSED
        self.review_state = (
            normalized_review if normalized_review in _REVIEW_STATES else REVIEW_PROPOSED
        )
        normalized_cross_review = (
            _clean_text(cross_reactivity_review_state) or REVIEW_PROPOSED
        )
        normalized_cross_review = (
            normalized_cross_review
            if normalized_cross_review in _REVIEW_STATES
            else REVIEW_PROPOSED
        )
        self.source_issues = list(dict.fromkeys(source_issues or []))
        normalized_cross_issues = list(
            dict.fromkeys(cross_reactivity_issues or [])
        )
        default_cross_package = {
            "id": "in-memory-cross-reactivity",
            "source_format": "memory",
            "version": None,
            "provenance": {},
            "review_state": normalized_cross_review,
            "issues": list(normalized_cross_issues),
        }
        self.cross_reactivity_package = {
            **default_cross_package,
            **dict(cross_reactivity_package or {}),
            "review_state": normalized_cross_review,
            "issues": list(normalized_cross_issues),
        }

    @property
    def cross_reactivity_review_state(self) -> str:
        value = _clean_text(self.cross_reactivity_package.get("review_state"))
        return value if value in _REVIEW_STATES else REVIEW_PROPOSED

    @cross_reactivity_review_state.setter
    def cross_reactivity_review_state(self, value: str) -> None:
        normalized = _clean_text(value) or REVIEW_PROPOSED
        self.cross_reactivity_package["review_state"] = (
            normalized if normalized in _REVIEW_STATES else REVIEW_PROPOSED
        )

    @property
    def cross_reactivity_issues(self) -> List[str]:
        raw = self.cross_reactivity_package.get("issues")
        return list(raw) if isinstance(raw, list) else []

    @cross_reactivity_issues.setter
    def cross_reactivity_issues(self, value: List[str]) -> None:
        self.cross_reactivity_package["issues"] = list(dict.fromkeys(value or []))

    def package_metadata(self) -> Dict[str, Any]:
        return {
            "id": self.package_id,
            "source_format": self.source_format,
            "version": self.source_version,
            "provenance": dict(self.provenance),
            "review_state": self.review_state,
            "issues": list(self.source_issues),
            "cross_reactivity_review_state": self.cross_reactivity_review_state,
            "cross_reactivity": dict(self.cross_reactivity_package),
        }

    def primary_rules_usable(self) -> bool:
        return self.review_state == REVIEW_CLINICALLY_APPROVED and not self.source_issues

    def relationship_rules_usable(self) -> bool:
        return (self.cross_reactivity_review_state == REVIEW_CLINICALLY_APPROVED
                and not self.cross_reactivity_issues)

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
        if not self.relationship_rules_usable():
            return []
        return [group for group in self.cross_reactivity_groups if group.contains_entry(entry)]

    def shared_group(self, first: DrugReferenceEntry,
                     second: DrugReferenceEntry) -> Optional[CrossReactivityGroup]:
        return next((group for group in self.groups_for(first) if group.contains_entry(second)), None)

    def shared_group_for_code(self, entry: DrugReferenceEntry,
                              code: str) -> Optional[CrossReactivityGroup]:
        return next((group for group in self.groups_for(entry) if group.contains_code(code)), None)


_lock = threading.Lock()
_dataset: Optional[DrugReferenceDataset] = None


def _load_json_document(path: str) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    return raw if isinstance(raw, dict) else {}


def _append_issue(issues: List[str], issue: str) -> None:
    if issue not in issues:
        issues.append(issue)


def _entry_has_rejected_content(raw: Dict[str, Any], parsed: DrugReferenceEntry) -> bool:
    for key, accepted in (
        ("aliases", parsed.aliases),
        ("atcCodes", parsed.atc_codes),
        ("warnings", parsed.warnings),
    ):
        value = raw.get(key, [])
        if key in raw and (not isinstance(value, list) or len(accepted) != len(value)):
            return True
    if any(not _ATC_LEVEL5.fullmatch(code.strip().upper()) for code in parsed.atc_codes):
        return True

    for key in ("drugClass", "source"):
        if key in raw and raw.get(key) is not None and not isinstance(raw.get(key), str):
            return True

    age_bands = raw.get("ageBands", [])
    if "ageBands" in raw and (
        not isinstance(age_bands, list) or len(parsed.age_bands) != len(age_bands)
    ):
        return True

    interactions = raw.get("interactions", [])
    if "interactions" in raw and not isinstance(interactions, list):
        return True
    if isinstance(interactions, list) and len(parsed.interactions) != len(interactions):
        return True
    for interaction in interactions if isinstance(interactions, list) else []:
        if not isinstance(interaction, dict) or not (
            _clean_text(interaction.get("token")) or _clean_text(interaction.get("atc"))
        ):
            return True
        atc = _clean_text(interaction.get("atc"))
        if atc and not _ATC_LEVEL5.fullmatch(atc.upper()):
            return True
        if "severity" in interaction and interaction.get("severity") is not None:
            severity = _clean_text(interaction.get("severity"))
            if severity is None or severity.lower() not in _INTERACTION_SEVERITY_RANKS:
                return True

    contraindications = raw.get("contraindications", [])
    if "contraindications" in raw and not isinstance(contraindications, list):
        return True
    for contraindication in contraindications if isinstance(contraindications, list) else []:
        if (
            not isinstance(contraindication, dict)
            or (_clean_text(contraindication.get("type")) or "").lower()
            not in {"allergy", "condition"}
            or not _clean_text(contraindication.get("token"))
        ):
            return True
    return False


def _entries_from_document(
    raw: Dict[str, Any], issues: Optional[List[str]] = None
) -> List[DrugReferenceEntry]:
    diagnostics = issues if issues is not None else []
    entries: List[DrugReferenceEntry] = []
    raw_entries = raw.get("entries")
    if not isinstance(raw_entries, list):
        _append_issue(diagnostics, "source_data_invalid")
        return entries
    for raw_entry in raw_entries:
        if (not isinstance(raw_entry, dict) or not _clean_text(raw_entry.get("id"))
                or not _clean_text(raw_entry.get("name"))):
            _append_issue(diagnostics, "source_data_partially_invalid")
            continue
        try:
            parsed = _entry_from_dict(raw_entry)
        except (KeyError, TypeError, ValueError):
            _append_issue(diagnostics, "source_data_partially_invalid")
            continue
        if _entry_has_rejected_content(raw_entry, parsed):
            _append_issue(diagnostics, "source_data_partially_invalid")
        entries.append(parsed)
    return entries


def _load_cross_reactivity_package(
    path: Optional[str],
) -> _CrossReactivityLoad:
    basename = os.path.basename(path) if path else "unavailable"
    package = {
        "id": f"configured-cross-reactivity:{basename}",
        "source_format": "json" if path else "unavailable",
        "version": None,
        "provenance": {"dataset": basename} if path else {},
        "review_state": REVIEW_PROPOSED,
        "issues": [],
    }
    issues: List[str] = []
    if not path:
        _append_issue(issues, "cross_reactivity_source_unavailable")
        package["source_format"] = "unavailable"
        package["issues"] = list(issues)
        return _CrossReactivityLoad([], package, issues)
    if path.strip().lower() == _DISABLED_SENTINEL:
        _append_issue(issues, "cross_reactivity_source_unavailable")
        package["source_format"] = "disabled"
        package["issues"] = list(issues)
        return _CrossReactivityLoad([], package, issues)
    if not os.path.exists(path):
        _append_issue(issues, "cross_reactivity_source_unavailable")
        package["issues"] = list(issues)
        return _CrossReactivityLoad([], package, issues)
    try:
        with open(path, "r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        _append_issue(issues, "cross_reactivity_data_invalid")
        package["issues"] = list(issues)
        return _CrossReactivityLoad([], package, issues)
    if not isinstance(raw, dict):
        _append_issue(issues, "cross_reactivity_data_invalid")
        package["issues"] = list(issues)
        return _CrossReactivityLoad([], package, issues)
    review_state = (
        _clean_text(raw.get("reviewState"))
    ) or REVIEW_PROPOSED
    if not all(
        _clean_text(raw.get(field)) for field in ("packageId", "version", "source")
    ):
        _append_issue(issues, "cross_reactivity_package_identity_incomplete")
    if review_state not in _REVIEW_STATES:
        _append_issue(issues, "cross_reactivity_data_partially_invalid")
        review_state = REVIEW_PROPOSED
    package.update({
        "id": _clean_text(raw.get("packageId")) or package["id"],
        "version": _clean_text(raw.get("version")),
        "provenance": {
            "dataset": basename,
            **({"source": raw.get("source")} if _clean_text(raw.get("source")) else {}),
        },
        "review_state": review_state,
    })
    groups: List[CrossReactivityGroup] = []
    raw_groups = raw.get("groups")
    if not isinstance(raw_groups, list):
        _append_issue(issues, "cross_reactivity_data_invalid")
        raw_groups = []
    for item in raw_groups:
        if not isinstance(item, dict):
            _append_issue(issues, "cross_reactivity_data_partially_invalid")
            continue
        raw_prefixes = item.get("atcPrefixes")
        prefixes_valid = (
            isinstance(raw_prefixes, list)
            and bool(raw_prefixes)
            and all(
                isinstance(prefix, str)
                and bool(prefix.strip())
                and bool(_ATC_GROUP_PREFIX.fullmatch(prefix.strip().upper()))
                for prefix in raw_prefixes
            )
        )
        name = item.get("name")
        group = CrossReactivityGroup(
            name=name.strip() if isinstance(name, str) else "",
            atc_prefixes=_clean_text_list(item.get("atcPrefixes")),
            note=_clean_text(item.get("note")),
        )
        if group.name and prefixes_valid:
            groups.append(group)
        else:
            _append_issue(issues, "cross_reactivity_data_partially_invalid")
    package["issues"] = list(issues)
    return _CrossReactivityLoad(groups, package, issues)


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


def _load_source_dataset(
    path: str,
    source_format: str,
    cross_reactivity: _CrossReactivityLoad,
) -> DrugReferenceDataset:
    basename = os.path.basename(path) if path else "unavailable"
    if source_format == "atc":
        return DrugReferenceDataset(
            _load_atc_entries(path),
            cross_reactivity.groups,
            package_id=f"configured-atc:{basename}",
            source_format="atc",
            provenance={"dataset": basename},
            # ATC supplies classification, not reviewed clinical decision rules.
            review_state=REVIEW_PROPOSED,
            cross_reactivity_review_state=cross_reactivity.package["review_state"],
            source_issues=[] if path and os.path.exists(path) else ["source_unavailable"],
            cross_reactivity_package=cross_reactivity.package,
            cross_reactivity_issues=cross_reactivity.issues,
        )

    source_issues: List[str] = []
    raw = _load_json_document(path)
    if not path or not os.path.exists(path):
        _append_issue(source_issues, "source_unavailable")
    elif not raw:
        _append_issue(source_issues, "source_data_invalid")
    if raw and not all(
        _clean_text(raw.get(field)) for field in ("packageId", "version", "source")
    ):
        _append_issue(source_issues, "source_package_identity_incomplete")
    review_state = _clean_text(raw.get("reviewState")) or REVIEW_PROPOSED
    if review_state not in _REVIEW_STATES:
        _append_issue(source_issues, "source_data_partially_invalid")
        review_state = REVIEW_PROPOSED
    return DrugReferenceDataset(
        _entries_from_document(raw, source_issues),
        cross_reactivity.groups,
        package_id=_clean_text(raw.get("packageId")) or f"configured-json:{basename}",
        source_format="json",
        source_version=_clean_text(raw.get("version")),
        provenance={
            "dataset": basename,
            **(
                {"source": raw.get("source")}
                if _clean_text(raw.get("source"))
                else {}
            ),
        },
        review_state=review_state,
        cross_reactivity_review_state=cross_reactivity.package["review_state"],
        source_issues=source_issues,
        cross_reactivity_package=cross_reactivity.package,
        cross_reactivity_issues=cross_reactivity.issues,
    )


def load_dataset(path: Optional[str] = None, source_format: Optional[str] = None,
                 cross_reactivity_path: Optional[str] = None) -> DrugReferenceDataset:
    """Lazy singleton for the default path+format; an explicit path always loads fresh (test seam).
    ``source_format`` selects the adapter (``json``|``atc``); defaults to the DRUG_SAFETY_SOURCE_FORMAT env."""
    fmt = (source_format or _SOURCE_FORMAT or "json").strip().lower()
    groups_path = _CROSS_REACTIVITY_PATH if cross_reactivity_path is None else cross_reactivity_path
    cross_reactivity = _load_cross_reactivity_package(groups_path)
    global _dataset
    if path is not None:
        return _load_source_dataset(path, fmt, cross_reactivity)
    if _dataset is not None:
        return _dataset
    with _lock:
        if _dataset is None:
            _dataset = _load_source_dataset(
                _DATASET_PATH, fmt, cross_reactivity
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
    mapping_complete: bool = True
    exposure_complete: bool = True
    active_order_count: int = 0
    mapped_active_order_count: int = 0

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
                           exposure_complete: bool = False,
                           weight_concept_uuid: Optional[str] = None,
                           weight_max_age_days: Optional[int] = None) -> PatientClinicalContext:
    """Builds a PatientClinicalContext from raw querystore records (resourceType/metadata),
    mirroring PatientClinicalContextBuilder. querystore does not expose ATC codes on drug_order
    metadata (confirmed against the Java serializer, 2026-07-05), so active-drug ATC codes are
    resolved via a dataset alias lookup on the order's drug name — a stated simplification, fine
    for a package-shaped dataset; a mis-resolution here means a missed interaction/duplicate-
    therapy check, never a false positive, since an unresolved name just contributes no ATC code.
    """
    dataset = dataset or load_dataset()
    age_years: Optional[int] = None
    drug_names: Set[str] = set()
    atc_codes: Set[str] = set()
    allergy_tokens: Set[str] = set()
    condition_tokens: Set[str] = set()
    weights: List[Tuple[date, float]] = []
    active_order_count = 0
    mapped_active_order_count = 0
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
            active_order_count += 1
            name = meta.get("drug_name") or meta.get("concept_name")
            if name:
                drug_names.add(name)
                entry = dataset.lookup_by_token(name)
                if entry:
                    mapped_active_order_count += 1
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
                                   condition_tokens=condition_tokens,
                                   mapping_complete=mapped_active_order_count == active_order_count,
                                   exposure_complete=bool(exposure_complete),
                                   active_order_count=active_order_count,
                                   mapped_active_order_count=mapped_active_order_count)


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


def _render_entry(
    ref: DrugReferenceEntry, age: Optional[int], rules_usable: bool
) -> str:
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

    if not rules_usable:
        parts.append(
            " Informational research classification only; this source package is not "
            "clinically approved for deterministic dosing, interaction, contraindication, "
            "duplicate-therapy, or cross-reactivity decisions."
        )
        return "".join(parts)

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
        severity_floor = _configured_interaction_severity_floor()
        for i in ref.interactions:
            if i is None or not _clears_interaction_severity_floor(i, severity_floor):
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
        rendered = _render_entry(ref, age, dataset.primary_rules_usable())
        out_mappings.append({
            "index": index,
            "resourceType": RESOURCE_TYPE_DRUG_REFERENCE,
            "resourceUuid": ref.id,
            "date": None,
            "text": rendered,
            "reviewState": dataset.review_state,
            "package": dataset.package_metadata(),
        })
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


def _configured_interaction_severity_floor() -> int:
    configured = os.environ.get(
        "DRUG_SAFETY_MIN_INTERACTION_SEVERITY",
        DEFAULT_MIN_INTERACTION_SEVERITY,
    ).strip().lower()
    return _INTERACTION_SEVERITY_RANKS.get(
        configured, _INTERACTION_SEVERITY_RANKS[DEFAULT_MIN_INTERACTION_SEVERITY]
    )


def _clears_interaction_severity_floor(
    interaction: Interaction, severity_floor: int
) -> bool:
    if interaction.severity is None:
        return True
    severity = (_clean_text(interaction.severity) or "").lower()
    rank = _INTERACTION_SEVERITY_RANKS.get(severity)
    return rank is not None and rank >= severity_floor


def _add_interactions(warnings: List[SafetyWarning], ref: DrugReferenceEntry,
                       context: PatientClinicalContext) -> None:
    severity_floor = _configured_interaction_severity_floor()
    for i in ref.interactions:
        if i is None or not _clears_interaction_severity_floor(i, severity_floor):
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
        if not dataset.relationship_rules_usable() or not ref_classes:
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
    if not dataset.relationship_rules_usable():
        return
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


@dataclass
class SafetyCheckResult:
    """Canonical, provenance-bearing result for one deterministic safety pass."""
    status: str
    warnings: List[SafetyWarning]
    package: Dict[str, Any] = field(default_factory=dict)
    coverage: Dict[str, Any] = field(default_factory=dict)
    identity_confidence: str = "unavailable"
    issues: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": "drug_safety.v1",
            "status": self.status,
            "warnings": [warning.to_dict() for warning in self.warnings],
            "package": dict(self.package),
            "coverage": dict(self.coverage),
            "identity_confidence": self.identity_confidence,
            "issues": list(self.issues),
        }


def build_safety_coverage(
    context: Optional[PatientClinicalContext], *, execution_complete: bool
) -> Dict[str, Any]:
    return {
        "mapping_complete": bool(context and context.mapping_complete),
        "exposure_complete": bool(context and context.exposure_complete),
        "execution_complete": execution_complete,
        "active_order_count": context.active_order_count if context else 0,
        "mapped_active_order_count": context.mapped_active_order_count if context else 0,
    }


def _unresolved_named_drugs(
    question: Optional[str], answer: Optional[str], dataset: DrugReferenceDataset
) -> List[str]:
    unresolved: List[str] = []
    candidates: List[str] = []
    if question and _DRUG_SAFETY_QUESTION.search(question):
        for pattern in _QUESTION_DRUG_SPANS:
            for match in pattern.finditer(question):
                span = _QUESTION_TRAILING_CONTEXT.sub("", match.group(1).strip())
                candidates.extend(_DRUG_MENTION_SEPARATOR.split(span))
    candidates.extend(
        match.group("candidate") for match in _ANSWER_DRUG_ACTION.finditer(answer or "")
    )
    for raw_candidate in candidates:
        candidate = _ROUTE_PREFIX.sub("", raw_candidate.strip(" \t\r\n.,;:!?()[]{}")).strip()
        if not candidate:
            continue
        normalized = candidate.casefold()
        words = normalized.split()
        if not words or all(word in _NON_DRUG_MENTION_WORDS for word in words):
            continue
        if dataset.lookup_by_token(normalized) is not None:
            continue
        plausible = any(_DRUG_LIKE_SUFFIX.search(word) for word in words)
        plausible = plausible or (
            candidate[0].isupper()
            and len(words) <= 3
            and not any(word in _NON_DRUG_MENTION_WORDS for word in words)
        )
        if plausible:
            unresolved.append(normalized)
    return list(dict.fromkeys(unresolved))


def _package_metadata(dataset: Any) -> Dict[str, Any]:
    if dataset is None:
        return {}
    try:
        metadata = dataset.package_metadata()
    except (AttributeError, TypeError, ValueError, RuntimeError):
        return {}
    return dict(metadata) if isinstance(metadata, dict) else {}


def check_answer_safety(answer: Optional[str], question: Optional[str],
                        context: Optional[PatientClinicalContext],
                        dataset: Optional[DrugReferenceDataset], *, warn_dose: bool = True,
                        warn_interactions: bool = True,
                        warn_contraindications: bool = True) -> SafetyCheckResult:
    """Run the approved deterministic checks and disclose why a pass was incomplete.

    Only a ``clinically_approved`` package may emit product warnings. Proposed, evidence-curated,
    retired, missing, and malformed sources remain visible through status/package/issues but cannot
    masquerade as reviewed clinical decision support.
    """
    package = _package_metadata(dataset)
    entries = getattr(dataset, "entries", None) if dataset is not None else None
    issues: List[str] = []
    for issue in getattr(dataset, "source_issues", []):
        _append_issue(issues, issue)
    for issue in getattr(dataset, "cross_reactivity_issues", []):
        _append_issue(issues, issue)
    if dataset is None or not entries or context is None:
        if not entries:
            _append_issue(issues, "source_unavailable")
        if context is None:
            _append_issue(issues, "patient_context_unavailable")
        return SafetyCheckResult(
            status=STATUS_UNAVAILABLE,
            warnings=[],
            package=package,
            coverage=build_safety_coverage(context, execution_complete=False),
            identity_confidence="unavailable",
            issues=issues,
        )
    if not context.mapping_complete:
        _append_issue(issues, "mapping_incomplete")
    if not context.exposure_complete:
        _append_issue(issues, "exposure_incomplete")
    review_state = getattr(dataset, "review_state", REVIEW_PROPOSED)
    if review_state == REVIEW_RETIRED:
        _append_issue(issues, "source_retired")
    elif review_state != REVIEW_CLINICALLY_APPROVED:
        _append_issue(issues, "source_not_clinically_approved")
    cross_review_state = getattr(
        dataset, "cross_reactivity_review_state", REVIEW_PROPOSED
    )
    if cross_review_state == REVIEW_RETIRED:
        _append_issue(issues, "cross_reactivity_source_retired")
    elif (
        cross_review_state != REVIEW_CLINICALLY_APPROVED
        and "cross_reactivity_source_unavailable" not in issues
        and "cross_reactivity_data_invalid" not in issues
    ):
        _append_issue(issues, "cross_reactivity_not_clinically_approved")
    if not (warn_dose and warn_interactions and warn_contraindications):
        _append_issue(issues, "check_scope_limited")
    try:
        unresolved_named_drugs = _unresolved_named_drugs(question, answer, dataset)
    except (AttributeError, TypeError, ValueError, RuntimeError):
        unresolved_named_drugs = ["resolution_failed"]
    for drug in unresolved_named_drugs:
        _append_issue(issues, f"named_drug_unresolved:{drug}")

    # Unapproved source material can be represented as research context, but it cannot produce
    # deterministic warnings or CDS cards. The policy check itself completed successfully.
    if not dataset.primary_rules_usable():
        return SafetyCheckResult(
            status=(
                STATUS_UNAVAILABLE
                if review_state == REVIEW_RETIRED
                else STATUS_LIMITED
            ),
            warnings=[],
            package=package,
            coverage=build_safety_coverage(context, execution_complete=True),
            identity_confidence=(
                "high"
                if context.mapping_complete and context.exposure_complete
                else "limited"
            ),
            issues=issues,
        )
    try:
        warnings = _validate_answer(
            answer, question, context, dataset,
            warn_dose=warn_dose,
            warn_interactions=warn_interactions,
            warn_contraindications=warn_contraindications,
        )
    except (AttributeError, KeyError, TypeError, ValueError, RuntimeError):
        return SafetyCheckResult(
            status=STATUS_UNAVAILABLE,
            warnings=[],
            package=package,
            coverage=build_safety_coverage(context, execution_complete=False),
            identity_confidence="unavailable",
            issues=[*issues, "execution_failed"],
        )
    return SafetyCheckResult(
        status=STATUS_LIMITED if issues else STATUS_CHECKED,
        warnings=warnings,
        package=package,
        coverage=build_safety_coverage(context, execution_complete=True),
        identity_confidence=(
            "high" if context.mapping_complete and context.exposure_complete else "limited"
        ),
        issues=issues,
    )


def validate_answer(answer: Optional[str], question: Optional[str], context: PatientClinicalContext,
                    dataset: DrugReferenceDataset, *, warn_dose: bool = True, warn_interactions: bool = True,
                    warn_contraindications: bool = True) -> List[SafetyWarning]:
    """Fail-safe safety boundary: incomplete reference data cannot break the answer path."""
    return check_answer_safety(
        answer, question, context, dataset,
        warn_dose=warn_dose,
        warn_interactions=warn_interactions,
        warn_contraindications=warn_contraindications,
    ).warnings
