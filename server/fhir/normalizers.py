"""Clinical normalizer compatibility re-exports delegating to `wahs_core.normalizers`."""

from __future__ import annotations

from wahs_core.normalizers import (
    annotate_immunization_anomalies,
    classify_sensitivity,
    classify_sensitivity_with_source,
    classify_vaccine_group,
    detect_condition_conflicts,
    detect_medication_conflicts,
    is_clinic_room_equipment,
    is_non_medical_admin_code,
    normalize_drug_ingredient,
)

__all__ = [
    "annotate_immunization_anomalies",
    "classify_sensitivity",
    "classify_sensitivity_with_source",
    "classify_vaccine_group",
    "detect_condition_conflicts",
    "detect_medication_conflicts",
    "is_clinic_room_equipment",
    "is_non_medical_admin_code",
    "normalize_drug_ingredient",
]
