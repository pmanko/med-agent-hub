"""FHIR R4 extractor compatibility adapter over `wahs_core.extractors`.

Delegates canonical clinical extraction (`extract_bundle`, dataclasses, helper
parsers, and resource type sets) to `wahs_core` while preserving
`extract_bundle_lossless` and downstream compatibility re-exports for Hub
workbench modules (`fhir_browser`, `wahs_skills`, `wahs_analyzer`).
"""

from __future__ import annotations

from typing import Any, Mapping

from wahs_core.extractors import (
    LOOKUP_ONLY_RESOURCE_TYPES,
    NON_CLINICAL_RESOURCE_TYPES,
    ClinicalItem,
    ExtractedChart,
    NumericPoint,
    _categories,
    _codeable_status,
    _coding_pair,
    _format_obs_value,
    _iso_date,
    _iso_timestamp,
    _strip_urn,
    extract_bundle,
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


def extract_bundle_lossless(
    bundle: Mapping[str, Any],
    *,
    enable_privacy_classification: bool = False,
    filter_room_equipment_devices: bool = False,
) -> ExtractedChart:
    """Non-destructive W12/W15 FHIR Bundle extractor delegating to `wahs_core.extract_bundle`."""
    return extract_bundle(
        bundle,
        include_conflict_annotations=False,
        enable_privacy_classification=enable_privacy_classification,
        preserve_intraday_timestamps=True,
        preserve_all_note_sections=True,
        filter_room_equipment_devices=filter_room_equipment_devices,
    )


# Imported after helper re-exports so `fhir_browser` resolves them without circularity.
from .fhir_browser import build_fhir_browser_index  # noqa: E402

__all__ = [
    "ClinicalItem",
    "ExtractedChart",
    "LOOKUP_ONLY_RESOURCE_TYPES",
    "NON_CLINICAL_RESOURCE_TYPES",
    "NumericPoint",
    "annotate_immunization_anomalies",
    "build_fhir_browser_index",
    "classify_sensitivity",
    "classify_vaccine_group",
    "detect_condition_conflicts",
    "detect_medication_conflicts",
    "extract_bundle",
    "extract_bundle_lossless",
    "is_clinic_room_equipment",
    "normalize_drug_ingredient",
]
