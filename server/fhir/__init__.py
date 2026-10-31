"""FHIR R4 / US Core / IPS chart condensation and ContextSource subpackage."""

from __future__ import annotations

from .condenser import (
    CondensedChartResult,
    build_tier_items,
    condense_bundle_to_budget,
    estimate_tokens_local,
    items_to_querystore_records,
)
from .extractors import ClinicalItem, ExtractedChart, NumericPoint, extract_bundle
from .kb_lookup import (
    audit_cohort_code_coverage,
    extract_chart_kb_codes,
    get_acip_rules,
    get_clinical_exclusivity_and_privacy,
    get_rxnorm_lookup,
    resolve_condition_code,
    resolve_immunization_code,
    resolve_lab_code,
    resolve_medication_code,
)
from .normalizers import (
    annotate_immunization_anomalies,
    classify_sensitivity,
    classify_vaccine_group,
    detect_condition_conflicts,
    detect_medication_conflicts,
    is_clinic_room_equipment,
    normalize_drug_ingredient,
)
from .source import FhirBundleContextSource, ResilientTokenCounter
from .wahs_analyzer import (
    analyze_data_quality_and_conflicts,
    analyze_duplicates_and_bloat,
    analyze_patterns_and_summaries,
    answer_wahs_question,
)
from .wahs_tools import (
    get_lab_history,
    read_clinical_note,
    search_chart_entries,
)

__all__ = [
    "ClinicalItem",
    "CondensedChartResult",
    "ExtractedChart",
    "FhirBundleContextSource",
    "NumericPoint",
    "ResilientTokenCounter",
    "analyze_data_quality_and_conflicts",
    "analyze_duplicates_and_bloat",
    "analyze_patterns_and_summaries",
    "annotate_immunization_anomalies",
    "answer_wahs_question",
    "audit_cohort_code_coverage",
    "build_tier_items",
    "classify_sensitivity",
    "classify_vaccine_group",
    "condense_bundle_to_budget",
    "detect_condition_conflicts",
    "detect_medication_conflicts",
    "estimate_tokens_local",
    "extract_bundle",
    "extract_chart_kb_codes",
    "get_acip_rules",
    "get_clinical_exclusivity_and_privacy",
    "get_lab_history",
    "get_rxnorm_lookup",
    "is_clinic_room_equipment",
    "items_to_querystore_records",
    "normalize_drug_ingredient",
    "read_clinical_note",
    "resolve_condition_code",
    "resolve_immunization_code",
    "resolve_lab_code",
    "resolve_medication_code",
    "search_chart_entries",
]
