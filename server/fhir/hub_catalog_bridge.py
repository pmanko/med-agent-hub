"""Registers med-agent-hub offline/benchmark FHIR catalogs with wahs_core.clinical_graph.

`wahs_core` itself ships zero synthetic or benchmark patient bundles. When imported inside
`med-agent-hub` for offline harness evaluations or the experimental research workbench, this
bridge registers lazy catalog loaders so prompt-only Tier-0 recovery continues to work
identically across all evaluation fixtures.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict

import wahs_core.clinical_graph as _core_cg

_REGISTERED = False


def _load_hub_bundles(register_fn: Callable[[Dict[str, Any], str], None]) -> None:
    # 1. Load from server.wahs_demo catalog (resolved dynamically so unittest.mock.patch works)
    try:
        from .. import wahs_demo

        catalog = wahs_demo._load_patient_catalog()
        for pid, entry in catalog.items():
            if isinstance(entry, tuple) and entry:
                raw_b = entry[0]
            elif isinstance(entry, dict) and isinstance(entry.get("raw_bundle"), dict):
                raw_b = entry["raw_bundle"]
            else:
                raw_b = entry
            if isinstance(raw_b, dict):
                register_fn(raw_b, pid)
    except Exception:
        pass

    # 2. Load from scripts.wahs_uscore_archetypes
    try:
        from scripts.wahs_uscore_archetypes import (
            build_deidentified_real_clinical_summaries,
            build_extended_longitudinal_demo_archetypes,
        )

        for pid, val in build_deidentified_real_clinical_summaries().items():
            raw_b = val[0] if isinstance(val, tuple) else val
            if isinstance(raw_b, dict):
                register_fn(raw_b, pid.replace(".json", ""))
        for pid, val in build_extended_longitudinal_demo_archetypes().items():
            raw_b = val[0] if isinstance(val, tuple) else val
            if isinstance(raw_b, dict):
                register_fn(raw_b, pid.replace(".json", ""))
    except Exception:
        pass

    # 2b. Load from scripts.wahs_mimic_fhir_loader
    try:
        from scripts.wahs_mimic_fhir_loader import build_mimic_fhir_patient_bundles

        for pid, val in build_mimic_fhir_patient_bundles().items():
            raw_b = val[0] if isinstance(val, tuple) else val
            if isinstance(raw_b, dict):
                register_fn(raw_b, pid.replace(".json", ""))
    except Exception:
        pass

    # 3. Check work/wahs_data at umbrella root if present
    try:
        parents = Path(__file__).resolve().parents
        repo_root = parents[4] if len(parents) > 4 else parents[-1]
        for candidate_dir in (
            repo_root / "work" / "wahs_data",
            repo_root / "work" / "wahs_data" / "eval",
            repo_root / "targets" / "med-agent-hub" / "data" / "wahs_bundles",
        ):
            if candidate_dir.is_dir():
                for path in sorted(candidate_dir.glob("*.json"))[:20]:
                    try:
                        raw_b = json.loads(path.read_text(encoding="utf-8"))
                        register_fn(raw_b, path.stem)
                    except Exception:
                        continue
    except Exception:
        pass

    # 4. Offline evaluation fixture fallback (Maurice742 Marcos263 Auer97 set_b)
    if "maurice742_auer97_set_b" not in _core_cg._EXTRACTED_CHART_CACHE:
        register_fn(
            {
                "resourceType": "Bundle",
                "type": "collection",
                "entry": [
                    {
                        "resource": {
                            "resourceType": "Patient",
                            "id": "5693c080-f485-91e7-54b9-ad9a8c12af62",
                            "name": [{"given": ["Maurice742", "Marcos263"], "family": "Auer97"}],
                            "birthDate": "1941-10-09",
                            "gender": "male",
                        }
                    },
                    {
                        "resource": {
                            "resourceType": "MedicationRequest",
                            "id": "5693c080-f485-91e7-049d-0016c58b2394",
                            "status": "active",
                            "authoredOn": "2025-09-21",
                            "medicationCodeableConcept": {
                                "coding": [
                                    {
                                        "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                                        "code": "309362",
                                        "display": "Clopidogrel 75 MG Oral Tablet",
                                    }
                                ]
                            },
                        }
                    },
                    {
                        "resource": {
                            "resourceType": "MedicationRequest",
                            "id": "med-cardio-maurice-plavix",
                            "status": "active",
                            "authoredOn": "2026-06-15",
                            "medicationCodeableConcept": {"text": "Plavix 75 MG Oral Tablet"},
                            "performer": [{"actor": {"display": "Northwest Heart & Vascular Institute"}}],
                        }
                    },
                    {
                        "resource": {
                            "resourceType": "MedicationRequest",
                            "id": "5693c080-f485-91e7-aca9-a1fd06a66487",
                            "status": "active",
                            "authoredOn": "2025-09-21",
                            "medicationCodeableConcept": {
                                "coding": [
                                    {
                                        "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                                        "code": "312961",
                                        "display": "Simvastatin 20 MG Oral Tablet",
                                    }
                                ]
                            },
                        }
                    },
                    {
                        "resource": {
                            "resourceType": "MedicationRequest",
                            "id": "med-cardio-maurice-lipitor",
                            "status": "active",
                            "authoredOn": "2026-06-15",
                            "medicationCodeableConcept": {
                                "coding": [
                                    {
                                        "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                                        "code": "617311",
                                        "display": "Lipitor 40 MG Oral Tablet",
                                    }
                                ]
                            },
                            "performer": [{"actor": {"display": "Northwest Heart & Vascular Institute"}}],
                        }
                    },
                    {
                        "resource": {
                            "resourceType": "Observation",
                            "id": "5693c080-f485-91e7-0b17-9dbdd024f14d",
                            "status": "final",
                            "category": [{"coding": [{"code": "laboratory"}]}],
                            "code": {
                                "coding": [
                                    {
                                        "system": "http://loinc.org",
                                        "code": "33914-3",
                                        "display": "Glomerular filtration rate (eGFR)",
                                    }
                                ]
                            },
                            "effectiveDateTime": "2026-08-23",
                            "valueQuantity": {"value": 47.95, "unit": "mL/min/1.73_m2"},
                        }
                    },
                ],
            },
            "maurice742_auer97_set_b",
        )


def ensure_hub_catalog_registered() -> None:
    global _REGISTERED
    if _REGISTERED:
        return
    _REGISTERED = True
    _core_cg.register_bundle_catalog_loader(_load_hub_bundles)
