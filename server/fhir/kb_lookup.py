"""Terminology lookup compatibility re-exports delegating to `wahs_core.kb_lookup`."""

from __future__ import annotations

from wahs_core.kb_lookup import (
    audit_cohort_code_coverage,
    extract_chart_kb_codes,
    get_acip_rules,
    get_clinical_exclusivity_and_privacy,
    get_clinical_graph_rules,
    get_rxnorm_lookup,
    reset_kb_lookup_cache,
    resolve_condition_code,
    resolve_immunization_code,
    resolve_lab_code,
    resolve_medication_code,
)

__all__ = [
    "audit_cohort_code_coverage",
    "extract_chart_kb_codes",
    "get_acip_rules",
    "get_clinical_exclusivity_and_privacy",
    "get_clinical_graph_rules",
    "get_rxnorm_lookup",
    "reset_kb_lookup_cache",
    "resolve_condition_code",
    "resolve_immunization_code",
    "resolve_lab_code",
    "resolve_medication_code",
]
