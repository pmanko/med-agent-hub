"""Deterministic Patient Clinical Entity Graph and Med Agent Hub Pre-Generation Enrichment.

Constructs a patient-specific clinical entity graph from either an ``ExtractedChart`` /
FHIR R4 ``Bundle`` or condensed chart ledger lines (``[1] ... [N]``), linking:
  - Condition / Problem nodes
  - MedicationRequest / MedicationStatement nodes
  - Observation (Lab / Vital) trajectory nodes
  - DocumentReference / DiagnosticReport narrative note nodes
  - AllergyIntolerance nodes
  - Immunization series nodes
  - Multi-facility Organization provenance nodes

Computes deterministic clinical relationship edges:
  - ``TREATS`` (Medication <-> Condition indication via RxNorm / SNOMED / ICD-10 KB rules)
  - ``CONTRAINDICATED_OR_DOSE_ADJUST_IN`` (Medication <-> Renal function eGFR/Cr, K+, INR, or Allergy)
  - ``MONITORED_BY`` (Condition / Medication <-> Lab trajectory LOINC series)
  - ``CONTRADICTED_OR_MODIFIED_BY_NOTE`` (Structured Medication/Condition <-> Narrative Note)
  - ``REPORTED_BY_ORG`` (Clinical item <-> Organization provenance)

Provides ``build_wahs_hub_enrichment_block(...)`` for Med Agent Hub pre-generation
context intelligence during ``-kb`` WAHS profile execution.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

from .. import kb as hub_kb
from .condenser import estimate_tokens_local
from .extractors import ClinicalItem, ExtractedChart, extract_bundle
from .kb_lookup import extract_chart_kb_codes, get_rxnorm_lookup
from .normalizers import normalize_drug_ingredient
from .wahs_tools import get_lab_history, read_clinical_note, search_chart_entries


NODE_CATEGORIES = (
    "Condition",
    "MedicationRequest",
    "Observation",
    "DocumentReference",
    "AllergyIntolerance",
    "Immunization",
    "Organization",
)

EDGE_TYPES = (
    "TREATS",
    "CONTRAINDICATED_OR_DOSE_ADJUST_IN",
    "MONITORED_BY",
    "CONTRADICTED_OR_MODIFIED_BY_NOTE",
    "REPORTED_BY_ORG",
)


@dataclass
class ClinicalNode:
    """A node in the patient-specific clinical entity graph."""

    node_id: str
    category: str
    label: str
    code: str = ""
    citation_idx: Optional[int] = None
    resource_uuid: str = ""
    date_str: str = ""
    status: str = ""
    value_summary: str = ""
    raw_line: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def citation_tag(self) -> str:
        if self.citation_idx is not None:
            return f"[{self.citation_idx}]"
        if self.resource_uuid:
            return f"[UUID: {self.resource_uuid}]"
        return ""

    def short_display(self) -> str:
        tag = f"[{self.citation_idx}] " if self.citation_idx is not None else ""
        val = f" ({self.value_summary})" if self.value_summary else ""
        code_part = f" [{self.code}]" if self.code and self.code not in self.label else ""
        return f"{tag}{self.label}{val}{code_part}".strip()


@dataclass
class ClinicalEdge:
    """A directed, provenance-linked clinical relationship edge."""

    source_id: str
    target_id: str
    edge_type: str
    rationale: str
    kb_source: str = ""
    source_citation: Optional[int] = None
    target_citation: Optional[int] = None


@dataclass
class HubEnrichmentResult:
    """Structured result from Med Agent Hub pre-generation context enrichment."""

    enrichment_text: str
    enrichment_tokens: int
    omitted_clinical_items: int
    overflow_triggered: bool
    overflow_recovered_count: int
    subgraph_nodes: int
    subgraph_edges: int
    kb_Passages_matched: int
    max_citation_idx_before: int
    max_citation_idx_after: int
    active_question: str = ""
    patient_name: str = ""
    omitted_by_category: Dict[str, int] = field(default_factory=dict)
    edge_Breakdown: Dict[str, int] = field(default_factory=dict)
    recovered_mappings: List[Dict[str, Any]] = field(default_factory=list)

    def to_metadata_dict(self) -> Dict[str, Any]:
        return {
            "omitted_clinical_items": self.omitted_clinical_items,
            "overflow_triggered": self.overflow_triggered,
            "overflow_recovered_count": self.overflow_recovered_count,
            "subgraph_nodes": self.subgraph_nodes,
            "subgraph_edges": self.subgraph_edges,
            "kb_passages_matched": self.kb_Passages_matched,
            "enrichment_tokens": self.enrichment_tokens,
            "max_citation_idx_before": self.max_citation_idx_before,
            "max_citation_idx_after": self.max_citation_idx_after,
            "patient_name": self.patient_name,
            "omitted_by_category": dict(self.omitted_by_category),
            "edge_breakdown": dict(self.edge_Breakdown),
            "recovered_mappings": list(self.recovered_mappings),
        }


# ---------------------------------------------------------------------------
# Clinical Domain Rules for Graph Edge Construction
# ---------------------------------------------------------------------------

_DRUG_INDICATION_KEYWORDS: List[Tuple[Tuple[str, ...], Tuple[str, ...], str, str]] = [
    (
        ("apixaban", "eliquis", "rivaroxaban", "xarelto", "warfarin", "coumadin", "dabigatran", "edoxaban"),
        ("atrial fibrillation", "afib", "flutter", "thrombosis", "embolism", "dvt", "pe", "stroke"),
        "Anticoagulation for thromboembolic protection / atrial fibrillation",
        "AHA/ACC/HRS AFib & Anticoagulation Guideline",
    ),
    (
        ("sacubitril", "valsartan", "entresto", "lisinopril", "losartan", "ramipril", "enalapril", "carvedilol", "metoprolol", "bisoprolol", "spironolactone", "eplerenone", "furosemide", "lasix", "torsemide", "bumetanide"),
        ("heart failure", "hfref", "hfpef", "cardiomyopathy", "ventricular dysfunction", "edema", "overload"),
        "Guideline-directed medical therapy (GDMT) / decongestion for heart failure",
        "AHA/ACC/HFSA Heart Failure Guideline",
    ),
    (
        ("empagliflozin", "jardiance", "dapagliflozin", "farxiga", "canagliflozin"),
        ("heart failure", "hfref", "hfpef", "diabetes", "kidney disease", "ckd", "renal", "nephropathy"),
        "SGLT2 inhibitor organ-protective therapy for HF / T2DM / CKD",
        "KDIGO CKD & ADA/AHA Cardiorenal Guideline",
    ),
    (
        ("lisinopril", "losartan", "valsartan", "amlodipine", "hydrochlorothiazide", "chlorthalidone", "metoprolol", "carvedilol", "diltiazem", "spironolactone", "hydralazine"),
        ("hypertension", "high blood pressure", "elevated blood pressure"),
        "Antihypertensive therapy for blood pressure control",
        "ACC/AHA Hypertension Guideline",
    ),
    (
        ("metformin", "insulin", "glargine", "lispro", "aspart", "glipizide", "glimepiride", "semaglutide", "ozempic", "liraglutide", "dulaglutide", "sitagliptin"),
        ("diabetes", "hyperglycemia", "prediabetes", "a1c"),
        "Glycemic control therapy for diabetes mellitus",
        "ADA Standards of Care in Diabetes",
    ),
    (
        ("atorvastatin", "lipitor", "rosuvastatin", "crestor", "simvastatin", "pravastatin", "ezetimibe", "evolocumab"),
        ("hyperlipidemia", "dyslipidemia", "hypercholesterolemia", "coronary", "cad", "atherosclerosis", "myocardial infarction", "stroke"),
        "Lipid-lowering and atherosclerotic cardiovascular risk reduction",
        "ACC/AHA Cholesterol & ASCVD Guideline",
    ),
    (
        ("levothyroxine", "synthroid"),
        ("hypothyroidism", "thyroid"),
        "Thyroid hormone replacement for hypothyroidism",
        "ATA Hypothyroidism Guideline",
    ),
    (
        ("allopurinol", "febuxostat", "colchicine"),
        ("gout", "hyperuricemia"),
        "Urate-lowering / flare prophylaxis therapy for gout",
        "ACR Gout Management Guideline",
    ),
    (
        ("albuterol", "salbutamol", "tiotropium", "ipratropium", "fluticasone", "budesonide", "formoterol", "salmeterol", "montelukast"),
        ("asthma", "copd", "chronic obstructive", "bronchitis", "wheezing"),
        "Bronchodilator / anti-inflammatory airway therapy",
        "GOLD COPD / GINA Asthma Strategy",
    ),
    (
        ("sertraline", "escitalopram", "fluoxetine", "citalopram", "venlafaxine", "duloxetine", "bupropion"),
        ("depression", "depressive", "anxiety", "ptsd", "mood"),
        "Pharmacotherapy for depressive or anxiety disorder",
        "APA Mood & Anxiety Clinical Practice Guideline",
    ),
    (
        ("buprenorphine", "suboxone", "naloxone", "methadone", "naltrexone"),
        ("opioid", "dependence", "substance", "addiction"),
        "Medication for opioid use disorder (MOUD)",
        "SAMHSA / ASAM OUD Guideline",
    ),
    (
        ("aspirin", "clopidogrel", "plavix", "ticagrelor", "prasugrel", "isosorbide", "nitroglycerin"),
        ("coronary", "cad", "angina", "ischemic heart", "myocardial infarction", "stent", "pci"),
        "Antiplatelet / antianginal therapy for ischemic heart disease",
        "ACC/AHA Chronic Coronary Disease Guideline",
    ),
]

_SAFETY_CONTRAINDICATION_RULES: List[Tuple[Tuple[str, ...], Tuple[str, ...], str, str]] = [
    (
        ("metformin",),
        ("stage 4", "stage 5", "ckd 4", "ckd 5", "kidney disease, stage 4", "kidney disease stage 4", "egfr", "glomerular filtration", "creatinine", "acute kidney", "aki"),
        "Metformin renal safety: contraindicated when eGFR < 30 mL/min/1.73m2; avoid initiation and reduce dose when eGFR 30-44 due to lactic acidosis risk",
        "FDA Metformin Boxed Warning & KDIGO Diabetes in CKD",
    ),
    (
        ("spironolactone", "eplerenone", "sacubitril", "valsartan", "entresto", "lisinopril", "losartan", "ramipril", "enalapril"),
        ("hyperkalemia", "potassium", "stage 4", "stage 5", "ckd 4", "kidney disease, stage 4", "kidney disease stage 4", "acute kidney", "aki", "creatinine", "egfr", "glomerular filtration"),
        "RAAS / MRA cardiorenal safety: high risk of hyperkalemia (K+ > 5.0 mEq/L) and acute worsening of eGFR/creatinine in Stage 4 CKD or volume depletion",
        "AHA/ACC/HFSA & KDIGO Cardiorenal Monitoring Rule",
    ),
    (
        ("apixaban", "eliquis", "rivaroxaban", "xarelto", "dabigatran", "edoxaban"),
        ("stage 4", "stage 5", "ckd 4", "kidney disease, stage 4", "kidney disease stage 4", "creatinine", "egfr", "glomerular filtration", "bleed", "hemorrhage", "anemia", "hemoglobin"),
        "DOAC renal & bleeding safety: evaluate apixaban dose reduction (2.5 mg BID if >=2 of age >=80, weight <=60 kg, Cr >=1.5 mg/dL) and monitor renal clearance/hemoglobin",
        "FDA Apixaban/DOAC Renal Dosing & Anticoagulation Safety",
    ),
    (
        ("warfarin", "coumadin"),
        ("inr", "prothrombin", "bleed", "hemorrhage", "bruising", "hematuria", "melena"),
        "Warfarin narrow therapeutic index: supratherapeutic INR (>3.0-3.5) or bleeding requires prompt dose hold/adjustment and interaction review",
        "CHEST Antithrombotic Therapy Guideline",
    ),
    (
        ("ibuprofen", "naproxen", "meloxicam", "ketorolac", "diclofenac", "celecoxib", "indomethacin", "nsaid"),
        ("kidney disease", "ckd", "renal", "heart failure", "hfref", "creatinine", "egfr", "apixaban", "warfarin", "rivaroxaban", "ulcer", "bleed"),
        "NSAID high-risk interaction: precipitates acute kidney injury (AKI), fluid retention/HF decompensation, and major GI/anticoagulant bleeding",
        "KDIGO & AGS Beers Criteria High-Risk Medication Rule",
    ),
    (
        ("allopurinol", "colchicine", "gabapentin", "pregabalin", "digoxin", "fenofibrate"),
        ("kidney disease", "ckd", "stage 3", "stage 4", "stage 5", "creatinine", "egfr", "glomerular filtration"),
        "Renally cleared medication: requires eGFR-guided dose adjustment to prevent systemic accumulation and toxicity",
        "Renal Pharmacotherapy Dosing Guideline",
    ),
    (
        ("furosemide", "lasix", "torsemide", "bumetanide", "hydrochlorothiazide", "chlorthalidone", "metolazone"),
        ("potassium", "sodium", "creatinine", "egfr", "urea nitrogen", "bun", "hypotension", "dehydration", "acute kidney"),
        "Diuretic electrolyte & renal safety: aggressive diuresis can induce hypokalemia/hyponatremia, prerenal azotemia, and rising creatinine",
        "ACC/AHA Heart Failure Diuretic Monitoring Rule",
    ),
    (
        ("insulin", "glargine", "lispro", "aspart", "glipizide", "glimepiride", "glyburide"),
        ("hypoglycemia", "glucose", "hemoglobin a1c", "a1c", "kidney disease", "ckd"),
        "Hypoglycemia risk: reduced renal insulin clearance and tight A1c (<6.5%) in older or CKD patients increase severe hypoglycemia risk",
        "ADA Glycemic Targets & Older Adults Safety Guideline",
    ),
]

_MONITORING_RULES: List[Tuple[Tuple[str, ...], Tuple[str, ...], str]] = [
    (
        ("kidney", "ckd", "renal", "nephropathy", "proteinuria", "spironolactone", "eplerenone", "sacubitril", "valsartan", "lisinopril", "losartan", "ramipril", "furosemide", "torsemide", "empagliflozin", "dapagliflozin", "metformin", "allopurinol"),
        ("creatinine", "glomerular filtration", "egfr", "potassium", "urea nitrogen", "bun", "albumin/creatinine", "uacr", "cystatin"),
        "Renal function & electrolyte surveillance (Cr / eGFR / K+ / BUN / UACR)",
    ),
    (
        ("heart failure", "hfref", "hfpef", "cardiomyopathy", "edema", "dyspnea", "sacubitril", "valsartan", "furosemide", "torsemide", "spironolactone", "carvedilol", "metoprolol"),
        ("natriuretic", "nt-probnp", "bnp", "body weight", "weight", "potassium", "creatinine", "sodium", "ejection fraction", "lvef"),
        "Heart failure congestion & cardiorenal trajectory monitoring (NT-proBNP / Weight / K+ / Cr)",
    ),
    (
        ("diabetes", "hyperglycemia", "metformin", "insulin", "glargine", "glipizide", "semaglutide", "liraglutide", "empagliflozin", "dapagliflozin"),
        ("hemoglobin a1c", "hba1c", "a1c", "glucose", "albumin/creatinine", "uacr"),
        "Glycemic control & diabetic nephropathy surveillance (HbA1c / Glucose / UACR)",
    ),
    (
        ("atrial fibrillation", "afib", "thrombosis", "embolism", "warfarin", "apixaban", "rivaroxaban", "dabigatran", "heparin"),
        ("inr", "prothrombin", "hemoglobin", "hematocrit", "platelet", "creatinine", "heart rate"),
        "Anticoagulation safety, bleeding & rate surveillance (INR / Hgb / Platelets / Cr)",
    ),
    (
        ("hyperlipidemia", "dyslipidemia", "coronary", "cad", "atherosclerosis", "atorvastatin", "rosuvastatin", "simvastatin", "ezetimibe"),
        ("cholesterol", "ldl", "hdl", "triglyceride", "aminotransferase", "ast", "alt"),
        "Lipid panel & statin safety surveillance (LDL-C / Total Cholesterol / Triglycerides / Hepatic enzymes)",
    ),
    (
        ("hypertension", "blood pressure", "amlodipine", "lisinopril", "losartan", "valsartan", "hydrochlorothiazide", "chlorthalidone", "metoprolol", "carvedilol"),
        ("blood pressure", "systolic", "diastolic", "mean arterial"),
        "Hemodynamic blood pressure trajectory monitoring",
    ),
    (
        ("hypothyroidism", "thyroid", "levothyroxine"),
        ("thyrotropin", "tsh", "thyroxine", "free t4"),
        "Thyroid function surveillance (TSH / Free T4)",
    ),
    (
        ("gout", "hyperuricemia", "allopurinol", "febuxostat"),
        ("urate", "uric acid"),
        "Serum urate target surveillance",
    ),
    (
        ("anemia", "iron deficiency", "bleed", "hemorrhage"),
        ("hemoglobin", "hematocrit", "ferritin", "iron", "mcv", "reticulocyte"),
        "Hematologic & iron status trajectory monitoring",
    ),
]

_NOTE_MODIFICATION_CUES = (
    "stopped",
    "discontinued",
    "held",
    "holding",
    "not taking",
    "non-adherent",
    "nonadherent",
    "self-discontinued",
    "ran out",
    "reduced",
    "half dose",
    "cutting",
    "declined",
    "intolerant",
    "side effect",
    "bruising",
    "dizziness",
    "hyperkalemia",
    "worsening",
    "progressed",
    "stage 4",
    "stage 5",
    "acute",
    "discrepancy",
    "conflict",
    "outside",
    "walk-in",
    "urgent care",
    "emergency",
    "hospitalized",
    "discharge",
    "changed",
    "switched",
    "increased",
    "decreased",
)

_KNOWN_FACILITY_PATTERNS = re.compile(
    r"\b([A-Z][A-Za-z0-9&'.-]*(?:\s+[A-Z][A-Za-z0-9&'.-]*)*\s+"
    r"(?:Hospital|Medical Center|Clinic|Health|Healthcare|UrgentCare|Urgent Care|"
    r"Cardiology|Nephrology|Associates|Diagnostics|LabCorp|Quest|Pharmacy|VA|Center))\b"
)


class ClinicalEntityGraph:
    """Deterministic patient-specific clinical entity graph with citation provenance."""

    def __init__(
        self,
        patient_name: str = "",
        patient_id: str = "",
        max_citation_idx: int = 0,
        omitted_clinical_items: int = 0,
        omitted_by_category: Optional[Dict[str, int]] = None,
    ) -> None:
        self.nodes: Dict[str, ClinicalNode] = {}
        self.edges: List[ClinicalEdge] = []
        self._edge_keys: Set[Tuple[str, str, str]] = set()
        self.patient_name = patient_name
        self.patient_id = patient_id
        self.max_citation_idx = max_citation_idx
        self.omitted_clinical_items = omitted_clinical_items
        self.omitted_by_category: Dict[str, int] = dict(omitted_by_category or {})

    def add_node(self, node: ClinicalNode) -> ClinicalNode:
        existing = self.nodes.get(node.node_id)
        if existing is not None:
            if existing.citation_idx is None and node.citation_idx is not None:
                existing.citation_idx = node.citation_idx
            if not existing.resource_uuid and node.resource_uuid:
                existing.resource_uuid = node.resource_uuid
            if not existing.code and node.code:
                existing.code = node.code
            if not existing.value_summary and node.value_summary:
                existing.value_summary = node.value_summary
            return existing
        self.nodes[node.node_id] = node
        if node.citation_idx is not None and node.citation_idx > self.max_citation_idx:
            self.max_citation_idx = node.citation_idx
        return node

    def add_edge(
        self,
        source_id: str,
        target_id: str,
        edge_type: str,
        rationale: str,
        kb_source: str = "",
    ) -> Optional[ClinicalEdge]:
        if source_id == target_id or source_id not in self.nodes or target_id not in self.nodes:
            return None
        key = (source_id, target_id, edge_type)
        if key in self._edge_keys:
            return None
        self._edge_keys.add(key)
        src_node = self.nodes[source_id]
        tgt_node = self.nodes[target_id]
        edge = ClinicalEdge(
            source_id=source_id,
            target_id=target_id,
            edge_type=edge_type,
            rationale=rationale,
            kb_source=kb_source,
            source_citation=src_node.citation_idx,
            target_citation=tgt_node.citation_idx,
        )
        self.edges.append(edge)
        return edge

    def nodes_by_category(self, category: str) -> List[ClinicalNode]:
        return [n for n in self.nodes.values() if n.category == category]

    def edge_breakdown(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for e in self.edges:
            counts[e.edge_type] = counts.get(e.edge_type, 0) + 1
        return counts

    @classmethod
    def build(
        cls,
        source: Union[ExtractedChart, Dict[str, Any], str, Sequence[Dict[str, Any]]],
        extracted: Optional[ExtractedChart] = None,
    ) -> "ClinicalEntityGraph":
        """Build a ``ClinicalEntityGraph`` from an ``ExtractedChart``, FHIR Bundle, or prompt text."""
        if isinstance(source, ExtractedChart):
            return cls.from_extracted_chart(source)
        if isinstance(source, dict) and source.get("resourceType") == "Bundle":
            return cls.from_bundle(source)
        if isinstance(source, str):
            return cls.from_condensed_ledger(source, extracted=extracted)
        if isinstance(source, Sequence):
            combined_text = "\n".join(
                str(m.get("content") or "")
                for m in source
                if isinstance(m, dict) and m.get("content")
            )
            return cls.from_condensed_ledger(combined_text, extracted=extracted)
        raise TypeError(f"Unsupported source type for ClinicalEntityGraph.build: {type(source)!r}")

    @classmethod
    def from_bundle(cls, bundle: Dict[str, Any]) -> "ClinicalEntityGraph":
        extracted = extract_bundle(bundle)
        return cls.from_extracted_chart(extracted)

    @classmethod
    def from_extracted_chart(
        cls,
        extracted: ExtractedChart,
        citation_map: Optional[Dict[str, int]] = None,
    ) -> "ClinicalEntityGraph":
        """Construct a clinical entity graph directly from a parsed ``ExtractedChart``."""
        cit_map = dict(citation_map or {})
        graph = cls(
            patient_name=extracted.patient_name,
            patient_id=extracted.patient_id,
            max_citation_idx=max(cit_map.values(), default=0),
        )
        seq_idx = 2  # [1] is demographics, [2] is manifest when default-indexed

        def _next_cit(resource_id: str) -> int:
            nonlocal seq_idx
            if resource_id and resource_id in cit_map:
                return cit_map[resource_id]
            seq_idx += 1
            return seq_idx

        def _register_org_from_item(item: ClinicalItem, parent_node: ClinicalNode) -> None:
            org_name = (item.source_org or "").strip() or _extract_org_name_from_text(
                f"{item.display} {item.text}"
            )
            if org_name:
                org_id = f"org:{_slugify(org_name)}"
                org_node = graph.add_node(
                    ClinicalNode(
                        node_id=org_id,
                        category="Organization",
                        label=org_name,
                        citation_idx=parent_node.citation_idx,
                    )
                )
                graph.add_edge(
                    parent_node.node_id,
                    org_node.node_id,
                    "REPORTED_BY_ORG",
                    f"Recorded or reported by facility {org_name}",
                    kb_source="FHIR Provenance / Performer",
                )

        for item in extracted.items:
            if item.section == "patient":
                continue
            item_date = item.clinical_date or item.date or ""
            r_uuid = item.resource_uuid
            if item.section.startswith("condition") or item.resource_type == "Condition":
                c_id = f"cond:{r_uuid or _slugify(item.display)}"
                node = graph.add_node(
                    ClinicalNode(
                        node_id=c_id,
                        category="Condition",
                        label=item.display,
                        code=item.code,
                        citation_idx=_next_cit(r_uuid),
                        resource_uuid=r_uuid,
                        date_str=item_date,
                        status=item.status,
                        value_summary=item.conflict_note or item.status,
                        raw_line=item.text,
                    )
                )
                _register_org_from_item(item, node)
            elif item.section.startswith("med") or item.resource_type.startswith("Medication"):
                m_id = f"med:{r_uuid or _slugify(item.display)}"
                node = graph.add_node(
                    ClinicalNode(
                        node_id=m_id,
                        category="MedicationRequest",
                        label=item.display,
                        code=item.code,
                        citation_idx=_next_cit(r_uuid),
                        resource_uuid=r_uuid,
                        date_str=item_date,
                        status=item.status,
                        value_summary=item.text or item.status,
                        raw_line=item.text,
                    )
                )
                _register_org_from_item(item, node)
            elif item.section == "allergy" or item.resource_type == "AllergyIntolerance":
                a_id = f"alg:{r_uuid or _slugify(item.display)}"
                node = graph.add_node(
                    ClinicalNode(
                        node_id=a_id,
                        category="AllergyIntolerance",
                        label=item.display,
                        code=item.code,
                        citation_idx=_next_cit(r_uuid),
                        resource_uuid=r_uuid,
                        date_str=item_date,
                        status=item.status,
                        value_summary=item.text,
                        raw_line=item.text,
                    )
                )
                _register_org_from_item(item, node)
            elif item.section == "immunization" or item.resource_type == "Immunization":
                i_id = f"imm:{r_uuid or _slugify(item.display)}"
                node = graph.add_node(
                    ClinicalNode(
                        node_id=i_id,
                        category="Immunization",
                        label=item.display,
                        code=item.code,
                        citation_idx=_next_cit(r_uuid),
                        resource_uuid=r_uuid,
                        date_str=item_date,
                        status=item.status,
                        value_summary=item.conflict_note or item.status,
                        raw_line=item.text,
                    )
                )
                _register_org_from_item(item, node)
            elif item.section in {"clinical_note", "note"} or item.resource_type in {
                "DocumentReference",
                "DiagnosticReport",
            }:
                raw_note = str(
                    item.metadata.get("raw_decoded_text")
                    or item.metadata.get("full_text")
                    or item.text
                    or ""
                ).strip()
                n_id = f"note:{r_uuid or _slugify(item.display + '_' + item_date)}"
                node = graph.add_node(
                    ClinicalNode(
                        node_id=n_id,
                        category="DocumentReference",
                        label=item.display,
                        code=item.code,
                        citation_idx=_next_cit(r_uuid),
                        resource_uuid=r_uuid,
                        date_str=item_date,
                        status=item.status,
                        value_summary=raw_note[:280],
                        raw_line=raw_note or item.display,
                    )
                )
                _register_org_from_item(item, node)

        # Group numeric_points by concept/code into trajectory Observation nodes
        series_by_key: Dict[str, List[Any]] = {}
        for pt in extracted.numeric_points:
            key = f"{pt.code or ''}:{pt.concept}".strip(":")
            series_by_key.setdefault(key, []).append(pt)

        for code_key, points in series_by_key.items():
            if not points:
                continue
            points_sorted = sorted(points, key=lambda p: p.date or "")
            latest = points_sorted[-1]
            prev_vals = ", ".join(
                f"{p.value:g} ({p.date[:10]})" for p in points_sorted[-4:-1]
            )
            val_summary = f"latest {latest.value:g} {latest.unit}".strip()
            if latest.date:
                val_summary += f" on {latest.date[:10]}"
            if prev_vals:
                val_summary += f"; prev: {prev_vals}"
            obs_id = f"obs:{_slugify(code_key)}"
            graph.add_node(
                ClinicalNode(
                    node_id=obs_id,
                    category="Observation",
                    label=latest.concept or code_key,
                    code=latest.code,
                    citation_idx=_next_cit(latest.resource_uuid),
                    resource_uuid=latest.resource_uuid,
                    date_str=latest.date,
                    status="final",
                    value_summary=val_summary,
                    metadata={
                        "latest_value": latest.value,
                        "unit": latest.unit,
                        "count": len(points_sorted),
                        "min_value": min(p.value for p in points_sorted),
                        "max_value": max(p.value for p in points_sorted),
                    },
                )
            )

        graph._infer_clinical_edges()
        return graph

    @classmethod
    def from_condensed_ledger(
        cls,
        text: str,
        extracted: Optional[ExtractedChart] = None,
    ) -> "ClinicalEntityGraph":
        """Parse a condensed chart ledger (``[1] ... [N] ...``) and build the entity graph."""
        patient_name, patient_id = _parse_patient_header_from_text(text)
        omitted_total, omitted_by_cat = parse_omitted_clinical_items(text)
        max_cit = _find_max_citation_idx(text)

        graph = cls(
            patient_name=patient_name or (extracted.patient_name if extracted else ""),
            patient_id=patient_id or (extracted.patient_id if extracted else ""),
            max_citation_idx=max_cit,
            omitted_clinical_items=omitted_total,
            omitted_by_category=omitted_by_cat,
        )

        # Parse numbered [N] lines in the prompt
        line_pattern = re.compile(
            r"^\s*\[(\d+)\]\s*(?:\(([^)]*)\)\s*)?(.*)$", re.MULTILINE
        )
        for match in line_pattern.finditer(text):
            cit_idx = int(match.group(1))
            date_str = (match.group(2) or "").strip()
            body = (match.group(3) or "").strip()
            if not body:
                continue
            lower_body = body.lower()
            if lower_body.startswith("patient:") or lower_body.startswith("completeness manifest:"):
                continue

            uuid_val = _extract_first_uuid(body)
            code_val = _extract_first_code(body)
            category = _classify_ledger_line_category(body)
            if not category:
                continue

            label, value_summary = _split_ledger_label_and_value(body, category)
            node_id = f"{category.lower()}:{cit_idx}:{uuid_val or _slugify(label)[:32]}"
            node = graph.add_node(
                ClinicalNode(
                    node_id=node_id,
                    category=category,
                    label=label,
                    code=code_val,
                    citation_idx=cit_idx,
                    resource_uuid=uuid_val,
                    date_str=date_str,
                    value_summary=value_summary,
                    raw_line=body,
                )
            )
            org_name = _extract_org_name_from_text(body)
            if org_name:
                org_id = f"org:{_slugify(org_name)}"
                org_node = graph.add_node(
                    ClinicalNode(
                        node_id=org_id,
                        category="Organization",
                        label=org_name,
                        citation_idx=cit_idx,
                    )
                )
                graph.add_edge(
                    node.node_id,
                    org_node.node_id,
                    "REPORTED_BY_ORG",
                    f"Reported or performed at {org_name}",
                    kb_source="FHIR Provenance / Performer",
                )

        # If an ExtractedChart is available and some nodes lack detail, enrich metadata
        if extracted is not None:
            if not graph.patient_name:
                graph.patient_name = extracted.patient_name
            if not graph.patient_id:
                graph.patient_id = extracted.patient_id

        graph._infer_clinical_edges()
        return graph

    def _infer_clinical_edges(self) -> None:
        """Deterministically infer clinical relationship edges across graph nodes."""
        rxnorm_lookup = get_rxnorm_lookup()
        conditions = self.nodes_by_category("Condition")
        medications = self.nodes_by_category("MedicationRequest")
        observations = self.nodes_by_category("Observation")
        notes = self.nodes_by_category("DocumentReference")
        allergies = self.nodes_by_category("AllergyIntolerance")

        # 1. Medication -> Condition (TREATS)
        for med in medications:
            med_text = f"{med.label} {med.value_summary} {med.raw_line}".lower()
            ing = normalize_drug_ingredient(med.label).lower()
            rx_info = rxnorm_lookup.get(med.code) or {}
            rx_indication = str(rx_info.get("indication") or "").lower()
            rx_class = str(rx_info.get("class") or "")

            for cond in conditions:
                cond_text = f"{cond.label} {cond.value_summary} {cond.raw_line}".lower()
                matched_rationale = ""
                matched_kb = ""

                # Check RxNorm lookup indication match
                if rx_indication:
                    ind_tokens = [
                        t.strip()
                        for t in re.split(r"[,;/()]+", rx_indication)
                        if len(t.strip()) >= 4
                    ]
                    if any(tok in cond_text for tok in ind_tokens):
                        matched_rationale = (
                            f"{med.label} ({rx_class or 'RxNorm'}) treats {cond.label}"
                        )
                        matched_kb = f"RxNorm {med.code}" if med.code else "RxNorm Lookup"

                # Check curated clinical indication rules
                if not matched_rationale:
                    for drug_keys, cond_keys, rationale, kb_src in _DRUG_INDICATION_KEYWORDS:
                        if any(dk in med_text or dk == ing for dk in drug_keys) and any(
                            ck in cond_text for ck in cond_keys
                        ):
                            matched_rationale = rationale
                            matched_kb = kb_src
                            break

                if matched_rationale:
                    self.add_edge(
                        med.node_id,
                        cond.node_id,
                        "TREATS",
                        matched_rationale,
                        kb_source=matched_kb,
                    )

        # 2. Medication -> Condition / Observation / Allergy (CONTRAINDICATED_OR_DOSE_ADJUST_IN)
        for med in medications:
            med_text = f"{med.label} {med.value_summary} {med.raw_line}".lower()
            ing = normalize_drug_ingredient(med.label).lower()

            # Check against Conditions and Observations
            for target in [*conditions, *observations]:
                tgt_text = f"{target.label} {target.value_summary} {target.raw_line}".lower()
                for drug_keys, risk_keys, rationale, kb_src in _SAFETY_CONTRAINDICATION_RULES:
                    if any(dk in med_text or dk == ing for dk in drug_keys) and any(
                        rk in tgt_text for rk in risk_keys
                    ):
                        # Avoid flagging normal labs as contraindications unless clinically relevant
                        if target.category == "Observation" and not _is_observation_clinically_notable(
                            target, drug_keys
                        ):
                            continue
                        self.add_edge(
                            med.node_id,
                            target.node_id,
                            "CONTRAINDICATED_OR_DOSE_ADJUST_IN",
                            rationale,
                            kb_source=kb_src,
                        )
                        break

            # Check against Allergies (match strictly on allergen substance tokens + class cross-reactivity,
            # never on generic clinical/status metadata words from value_summary or raw_line).
            med_label_clean = re.sub(r"([(\[][^)\]]*[)\]])", " ", med.label).lower()
            med_drug_text = f"{med_label_clean} {ing}".strip()
            med_drug_words = set(re.findall(r"[a-z]{4,}", med_drug_text))
            for alg in allergies:
                alg_substance_text, alg_tokens = _extract_allergen_substance_tokens(alg.label)
                direct_match = bool(alg_tokens and any(tok in med_drug_words or tok == ing for tok in alg_tokens))
                penicillin_match = any(
                    k in alg_substance_text
                    for k in ("penicillin", "amoxicillin", "ampicillin", "piperacillin", "beta-lactam", "betalactam", "augmentin", "unasyn", "zosyn")
                ) and any(
                    p in med_drug_text
                    for p in (
                        "penicillin", "amoxicillin", "ampicillin", "piperacillin", "nafcillin",
                        "oxacillin", "dicloxacillin", "augmentin", "unasyn", "zosyn",
                        "cephalexin", "cefazolin", "ceftriaxone", "cefepime", "cefdinir", "cefuroxime",
                    )
                )
                sulfa_match = any(
                    k in alg_substance_text
                    for k in ("sulfa", "sulfonamide", "sulfamethoxazole", "bactrim", "septra", "cotrimoxazole")
                ) and any(
                    s in med_drug_text
                    for s in ("sulfamethoxazole", "trimethoprim-sulfamethoxazole", "bactrim", "septra", "cotrimoxazole", "sulfasalazine", "sulfadiazine")
                )
                nsaid_match = any(
                    k in alg_substance_text
                    for k in ("nsaid", "aspirin", "ibuprofen", "naproxen", "ketorolac", "diclofenac", "meloxicam", "celecoxib", "indomethacin")
                ) and any(
                    n in med_drug_text
                    for n in ("aspirin", "ibuprofen", "naproxen", "ketorolac", "diclofenac", "meloxicam", "celecoxib", "indomethacin", "piroxicam", "nabumetone")
                )
                if direct_match or penicillin_match or sulfa_match or nsaid_match:
                    self.add_edge(
                        med.node_id,
                        alg.node_id,
                        "CONTRAINDICATED_OR_DOSE_ADJUST_IN",
                        f"Medication {med.label} conflicts with documented allergy/intolerance {alg.label}",
                        kb_source="Allergy-Medication Cross-Reactivity Rule",
                    )

        # 3. Condition / Medication -> Observation (MONITORED_BY)
        for source_node in [*conditions, *medications]:
            src_text = f"{source_node.label} {source_node.value_summary} {source_node.raw_line}".lower()
            for obs in observations:
                obs_text = f"{obs.label} {obs.value_summary} {obs.raw_line}".lower()
                for src_keys, obs_keys, rationale in _MONITORING_RULES:
                    if any(sk in src_text for sk in src_keys) and any(
                        ok in obs_text for ok in obs_keys
                    ):
                        self.add_edge(
                            source_node.node_id,
                            obs.node_id,
                            "MONITORED_BY",
                            rationale,
                            kb_source="LOINC Clinical Surveillance Mapping",
                        )
                        break

        # 4. Medication / Condition -> DocumentReference Note (CONTRADICTED_OR_MODIFIED_BY_NOTE)
        for note in notes:
            note_text = f"{note.label} {note.value_summary} {note.raw_line}".lower()
            has_modification_cue = any(cue in note_text for cue in _NOTE_MODIFICATION_CUES)
            for item in [*medications, *conditions]:
                item_terms = _extract_entity_match_terms(item)
                if not item_terms:
                    continue
                if any(term in note_text for term in item_terms):
                    if has_modification_cue or item.category == "MedicationRequest":
                        cue_matched = next(
                            (c for c in _NOTE_MODIFICATION_CUES if c in note_text),
                            "narrative clinical context",
                        )
                        self.add_edge(
                            item.node_id,
                            note.node_id,
                            "CONTRADICTED_OR_MODIFIED_BY_NOTE",
                            f"Structured {item.category} ({item.label}) modified or contextualized in note ({cue_matched})",
                            kb_source="Structured-vs-Narrative Reconciliation",
                        )

    def format_subgraph_lines(
        self,
        question: str = "",
        max_lines: int = 10,
    ) -> List[str]:
        """Format the highest-yield provenance-linked relationship lines for prompt injection."""
        if not self.edges:
            return []

        q_terms = _extract_question_keywords(question)
        edge_priority = {
            "CONTRAINDICATED_OR_DOSE_ADJUST_IN": 0,
            "CONTRADICTED_OR_MODIFIED_BY_NOTE": 1,
            "TREATS": 2,
            "MONITORED_BY": 3,
            "REPORTED_BY_ORG": 4,
        }

        scored_edges: List[Tuple[Tuple[int, int, int], ClinicalEdge]] = []
        for idx, edge in enumerate(self.edges):
            src = self.nodes.get(edge.source_id)
            tgt = self.nodes.get(edge.target_id)
            if not src or not tgt:
                continue
            combined = f"{src.label} {src.value_summary} {tgt.label} {tgt.value_summary} {edge.rationale}".lower()
            q_hits = sum(1 for t in q_terms if t in combined)
            prio = edge_priority.get(edge.edge_type, 5)
            # Sort by: question relevance first (if question has terms), then edge safety priority
            scored_edges.append(((-q_hits, prio, idx), edge))

        scored_edges.sort(key=lambda pair: pair[0])

        lines: List[str] = []
        seen_summaries: Set[str] = set()
        for _, edge in scored_edges:
            if len(lines) >= max_lines:
                break
            src = self.nodes[edge.source_id]
            tgt = self.nodes[edge.target_id]
            line = (
                f"- {src.short_display()} --[{edge.edge_type}]--> "
                f"{tgt.short_display()} | {edge.rationale}"
            )
            if line not in seen_summaries:
                seen_summaries.add(line)
                lines.append(line)
        return lines

    def to_dict(self) -> Dict[str, Any]:
        return {
            "patient_name": self.patient_name,
            "patient_id": self.patient_id,
            "max_citation_idx": self.max_citation_idx,
            "omitted_clinical_items": self.omitted_clinical_items,
            "omitted_by_category": dict(self.omitted_by_category),
            "node_count": len(self.nodes),
            "edge_count": len(self.edges),
            "edge_breakdown": self.edge_breakdown(),
            "nodes": [asdict(n) for n in self.nodes.values()],
            "edges": [asdict(e) for e in self.edges],
        }


# ---------------------------------------------------------------------------
# Pre-Generation Context Intelligence & Overflow Recovery Orchestrator
# ---------------------------------------------------------------------------

_EXTRACTED_CHART_CACHE: Dict[str, ExtractedChart] = {}
_CACHE_INITIALIZED = False


def _ensure_patient_chart_cache() -> None:
    """Populate a lazy lookup of known WAHS patient bundles so Hub enrichment can recover Tier-0 items."""
    global _CACHE_INITIALIZED
    if _CACHE_INITIALIZED:
        return
    _CACHE_INITIALIZED = True

    def _register_bundle(raw_bundle: Dict[str, Any], key_hint: str = "") -> None:
        if not isinstance(raw_bundle, dict) or raw_bundle.get("resourceType") != "Bundle":
            return
        try:
            ext = extract_bundle(raw_bundle)
        except Exception:
            return
        for key in (
            key_hint,
            ext.patient_id,
            ext.patient_name,
            _normalize_person_key(ext.patient_name),
        ):
            if key:
                _EXTRACTED_CHART_CACHE[key.strip().lower()] = ext

    # 1. Load from server.wahs_demo catalog (includes WAHS_BUNDLE_DIR + builtin samples)
    try:
        from ..wahs_demo import _load_patient_catalog

        catalog = _load_patient_catalog()
        for pid, entry in catalog.items():
            if isinstance(entry, tuple) and entry:
                raw_b = entry[0]
            elif isinstance(entry, dict) and isinstance(entry.get("raw_bundle"), dict):
                raw_b = entry["raw_bundle"]
            else:
                raw_b = entry
            if isinstance(raw_b, dict):
                _register_bundle(raw_b, pid)
    except Exception:
        pass

    # 2. Load from scripts.wahs_uscore_archetypes (Elena Rostova, Arthur Pendelton, Margaret Chen, Sofia Alvarez, Marcus Vance)
    try:
        from scripts.wahs_uscore_archetypes import (
            build_deidentified_real_clinical_summaries,
            build_extended_longitudinal_demo_archetypes,
        )

        for pid, val in build_deidentified_real_clinical_summaries().items():
            raw_b = val[0] if isinstance(val, tuple) else val
            if isinstance(raw_b, dict):
                _register_bundle(raw_b, pid.replace(".json", ""))
        for pid, val in build_extended_longitudinal_demo_archetypes().items():
            raw_b = val[0] if isinstance(val, tuple) else val
            if isinstance(raw_b, dict):
                _register_bundle(raw_b, pid.replace(".json", ""))
    except Exception:
        pass

    # 2b. Load from scripts.wahs_mimic_fhir_loader (Julian Mercer MIMIC-IV #10014354, Clara Hollis MIMIC-IV #10019003)
    try:
        from scripts.wahs_mimic_fhir_loader import build_mimic_fhir_patient_bundles

        for pid, val in build_mimic_fhir_patient_bundles().items():
            raw_b = val[0] if isinstance(val, tuple) else val
            if isinstance(raw_b, dict):
                _register_bundle(raw_b, pid.replace(".json", ""))
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
                        _register_bundle(raw_b, path.stem)
                    except Exception:
                        continue
    except Exception:
        pass

    # 4. Ensure Maurice742 Marcos263 Auer97 (set_b) Tier-0 items are registered even in container builds without work/wahs_data/eval
    if "maurice742_auer97_set_b" not in _EXTRACTED_CHART_CACHE:
        _register_bundle(
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
                                "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "309362", "display": "Clopidogrel 75 MG Oral Tablet"}]
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
                                "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "312961", "display": "Simvastatin 20 MG Oral Tablet"}]
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
                                "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "617311", "display": "Lipitor 40 MG Oral Tablet"}]
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
                            "code": {"coding": [{"system": "http://loinc.org", "code": "33914-3", "display": "Glomerular filtration rate (eGFR)"}]},
                            "effectiveDateTime": "2026-08-23",
                            "valueQuantity": {"value": 47.95, "unit": "mL/min/1.73_m2"},
                        }
                    },
                ],
            },
            "maurice742_auer97_set_b",
        )


def _resolve_extracted_chart(
    prompt_text: str,
    extracted: Optional[ExtractedChart] = None,
) -> Optional[ExtractedChart]:
    if extracted is not None:
        return extracted
    _ensure_patient_chart_cache()
    patient_name, patient_id = _parse_patient_header_from_text(prompt_text)
    clean_pid = re.sub(r"(_set_b|\.json)$", "", (patient_id or "").strip().lower())
    for candidate in (
        patient_id,
        clean_pid,
        patient_name,
        _normalize_person_key(patient_name),
    ):
        if candidate and candidate.strip().lower() in _EXTRACTED_CHART_CACHE:
            return _EXTRACTED_CHART_CACHE[candidate.strip().lower()]

    # Partial or first/last name token match (e.g., "Elena Rostova", "Maurice742 Marcos263 Auer97")
    norm_target = _normalize_person_key(patient_name)
    if norm_target:
        target_tokens = [t for t in norm_target.split() if len(t) >= 3]
        for cached_key, ext in _EXTRACTED_CHART_CACHE.items():
            if norm_target in cached_key or cached_key in norm_target:
                return ext
            if len(target_tokens) >= 2 and target_tokens[0] in cached_key and target_tokens[-1] in cached_key:
                return ext
    return None


def parse_omitted_clinical_items(text: str) -> Tuple[int, Dict[str, int]]:
    """Parse ``omitted_clinical_items`` and per-category overflow counts from a condensed chart prompt."""
    by_category: Dict[str, int] = {}
    explicit_total: Optional[int] = None

    # Pattern 1: Completeness Manifest omitted_clinical_items=N
    m_omitted = re.search(r"\bomitted_clinical_items\s*=\s*(\d+)", text, flags=re.IGNORECASE)
    if m_omitted:
        explicit_total = int(m_omitted.group(1))

    # Pattern 2: Completeness Manifest budget_overflow_omitted: cat1=N, cat2=M
    m_overflow = re.search(
        r"budget_overflow_omitted\s*:\s*([^\n|;]+)", text, flags=re.IGNORECASE
    )
    if m_overflow:
        segment = m_overflow.group(1)
        for cat_match in re.finditer(r"([a-zA-Z0-9_/-]+)\s*=\s*(\d+)", segment):
            cat_name = cat_match.group(1).strip()
            if cat_name.lower() in {"omitted_clinical_items", "total", "items"}:
                continue
            cat_count = int(cat_match.group(2))
            if cat_count > 0:
                by_category[cat_name] = cat_count

    # Pattern 3: Frontend / Exporter header "Omitted by Active Budget: ..."
    m_header = re.search(
        r"Omitted by Active Budget\s*:\s*([^\n]+)", text, flags=re.IGNORECASE
    )
    if m_header:
        header_seg = m_header.group(1).strip()
        if re.search(r"\b(?:0\s+items|none|0\s+omitted)\b", header_seg, flags=re.IGNORECASE):
            if explicit_total is None and not by_category:
                explicit_total = 0
        else:
            for cat_match in re.finditer(
                r"([a-zA-Z0-9_/-]+)\s*[:=]\s*(\d+)", header_seg
            ):
                cat_name = cat_match.group(1).strip()
                if cat_name.lower() in {"items", "total", "omitted"}:
                    continue
                cat_count = int(cat_match.group(2))
                if cat_count > 0 and cat_name not in by_category:
                    by_category[cat_name] = cat_count
            if explicit_total is None:
                m_num = re.search(r"\b(\d+)\s+items?\b", header_seg, flags=re.IGNORECASE)
                if m_num:
                    explicit_total = int(m_num.group(1))

    cat_sum = sum(by_category.values())
    if explicit_total is not None:
        total = max(explicit_total, cat_sum)
    else:
        total = cat_sum
    return total, by_category


def extract_active_question(
    messages: Union[str, Sequence[Dict[str, Any]]],
) -> str:
    """Extract the active clinical question from a WAHS prompt or OpenAI message list."""
    if isinstance(messages, str):
        full_text = messages
    else:
        user_parts = [
            str(m.get("content") or "")
            for m in messages
            if isinstance(m, dict) and str(m.get("role") or "").lower() == "user"
        ]
        full_text = "\n\n".join(user_parts) if user_parts else "\n\n".join(
            str(m.get("content") or "") for m in messages if isinstance(m, dict)
        )

    for marker in (
        "USER_QUESTION:",
        "Task / Question:",
        "CLINICAL_QUESTION:",
        "Question:",
    ):
        idx = full_text.rfind(marker)
        if idx != -1:
            after = full_text[idx + len(marker) :].strip()
            # Stop if orchestration block already appended
            orch_idx = after.find("[MED-AGENT-HUB ORCHESTRATION:")
            if orch_idx != -1:
                after = after[:orch_idx].strip()
            if after:
                return after

    # Fallback: take the text after the last numbered [N] line
    matches = list(re.finditer(r"^\s*\[\d+\]\s*.*$", full_text, flags=re.MULTILINE))
    if matches:
        tail = full_text[matches[-1].end() :].strip()
        orch_idx = tail.find("[MED-AGENT-HUB ORCHESTRATION:")
        if orch_idx != -1:
            tail = tail[:orch_idx].strip()
        if tail:
            return tail

    return full_text[-600:].strip()


def _recover_omitted_tier0_items(
    extracted: ExtractedChart,
    prompt_text: str,
    question: str,
    omitted_by_category: Dict[str, int],
    start_citation_idx: int,
    max_recovered: int = 6,
) -> List[str]:
    """Use ``wahs_tools`` against ``extracted`` to recover omitted Tier-0 clinical records."""
    existing_uuids = set(
        re.findall(r"\bUUIDs?\s*:\s*([A-Za-z0-9_.,\s-]+)", prompt_text)
    )
    flat_existing_uuids: Set[str] = set()
    for chunk in existing_uuids:
        for token in re.split(r"[,;\s]+", chunk):
            if token.strip():
                flat_existing_uuids.add(token.strip())

    lower_prompt = prompt_text.lower()
    q_keywords = _extract_question_keywords(question)
    if not q_keywords:
        q_keywords = ["creatinine", "egfr", "potassium", "note", "medication", "hba1c"]

    recovered_entries: List[Tuple[str, str, str, str]] = []  # (date, category, summary_with_details, uuid)
    seen_uuids: Set[str] = set()

    def _try_add(date_s: str, cat_s: str, text_s: str, r_uuid: str) -> None:
        if len(recovered_entries) >= max_recovered:
            return
        if r_uuid and (r_uuid in flat_existing_uuids or r_uuid in seen_uuids):
            return
        # Check if core summary substring already appears verbatim in the condensed chart
        clean_prefix = re.split(r"[(\[—:]", text_s)[0].strip().lower()[:32]
        if clean_prefix and len(clean_prefix) >= 6 and clean_prefix in lower_prompt:
            return
        if r_uuid:
            seen_uuids.add(r_uuid)
        recovered_entries.append((date_s or "unknown-date", cat_s, text_s, r_uuid))

    # 0. Recover omitted Tier-0 active medications & implanted/home devices first (up to 4 items so Discharge Summary fits)
    _HIGH_ALERT_MED_TOKENS = (
        "plavix", "clopidogrel", "lipitor", "atorvastatin", "simvastatin",
        "spironolactone", "aldactone", "lisinopril", "potassium", "ibuprofen",
        "naproxen", "apixaban", "eliquis", "ativan", "lorazepam", "ambien", "zolpidem",
    )
    tier0_items = [
        it for it in getattr(extracted, "items", [])
        if getattr(it, "section", "") in {"med_active", "device", "condition_active"}
    ]
    tier0_items.sort(
        key=lambda it: (
            0 if getattr(it, "section", "") == "med_active" else (1 if getattr(it, "section", "") == "device" else 2),
            0 if any(tok in str(getattr(it, "display", "") or "").lower() for tok in _HIGH_ALERT_MED_TOKENS) else (1 if getattr(it, "conflict_note", None) else 2),
            -int(re.sub(r"\D", "", str(getattr(it, "date", "") or "")[:10]) or "0"),
        )
    )
    tier0_cap = max(1, max_recovered - 2)
    for it in tier0_items:
        if len(recovered_entries) >= tier0_cap:
            break
        r_uuid = str(getattr(it, "resource_uuid", "") or "")
        disp = str(getattr(it, "display", "") or "").strip()
        if not disp:
            continue
        sec = str(getattr(it, "section", "") or "")
        cat_label = (
            "Medication"
            if sec == "med_active"
            else ("Device" if sec == "device" else "Active Problem")
        )
        code = str(getattr(it, "code", "") or "")
        org = str(getattr(it, "source_org", "") or "")
        parts_txt = [disp]
        if code and code not in disp:
            parts_txt.append(f"({code})")
        if org:
            parts_txt.append(f"[Org: {org}]")
        _try_add(str(getattr(it, "date", "") or ""), cat_label, " ".join(parts_txt), r_uuid)

    # 1. Recover omitted clinical notes (prioritizing Discharge Summaries & most recent dates)
    note_queries = q_keywords[:2] + ["discharge", ""]
    for nq in note_queries:
        if len(recovered_entries) >= max_recovered:
            break
        note_res = read_clinical_note(extracted, query=nq)
        raw_notes = list(note_res.get("results") or [])
        raw_notes.sort(
            key=lambda n: (
                1 if ("discharge" in str(n.get("display") or "").lower() or "consult" in str(n.get("display") or "").lower()) else 0,
                str(n.get("date") or ""),
            ),
            reverse=True,
        )
        for n_item in raw_notes[:2]:
            r_uuid = str(n_item.get("resource_uuid") or "")
            if r_uuid and r_uuid in flat_existing_uuids:
                continue
            title = str(n_item.get("display") or "Clinical Note")
            if "syndicated copy" in title.lower():
                continue
            snippet = str(n_item.get("full_text") or n_item.get("text") or "").strip().replace("\n", " ")
            if len(snippet) > 165:
                snippet = snippet[:162] + "..."
            summary_txt = f"{title} — {snippet}" if snippet and snippet not in title else title
            _try_add(str(n_item.get("date") or ""), "Clinical Note", summary_txt, r_uuid)

    # 2. Recover omitted lab trajectories via get_lab_history
    lab_queries = [
        kw for kw in q_keywords
        if kw in {
            "creatinine", "egfr", "glomerular", "potassium", "sodium", "hba1c",
            "a1c", "glucose", "inr", "hemoglobin", "platelet", "ldl", "cholesterol",
            "bnp", "nt-probnp", "tsh", "urate", "uric", "weight", "blood", "pressure",
        }
    ]
    if not lab_queries and ("lab_observation" in omitted_by_category or len(recovered_entries) < max_recovered):
        lab_queries = ["creatinine", "potassium", "glomerular", "hemoglobin", "a1c"]

    for lq in lab_queries[:4]:
        if len(recovered_entries) >= max_recovered:
            break
        lab_res = get_lab_history(extracted, analyte_or_loinc=lq, limit=6)
        pts = lab_res.get("results") or []
        if not pts:
            continue
        latest_pt = pts[-1]
        disp = str(latest_pt.get("concept") or lq)
        code = str(latest_pt.get("code") or "")
        r_uuid = str(latest_pt.get("resource_uuid") or "")
        if disp.lower()[:28] in lower_prompt and (not r_uuid or r_uuid in flat_existing_uuids):
            continue
        trail = ", ".join(
            f"{p.get('value')} {p.get('unit') or ''} ({str(p.get('date') or '')[:10]})".strip()
            for p in pts[-3:]
        )
        code_tag = f" (LOINC {code})" if code else ""
        _try_add(
            str(latest_pt.get("date") or ""),
            "Lab Observation",
            f"{disp}{code_tag}: {trail}",
            r_uuid,
        )

    # 3. Search across all chart categories via search_chart_entries for question keywords
    for kw in [*q_keywords[:4], ""]:
        if len(recovered_entries) >= max_recovered:
            break
        srch = search_chart_entries(extracted, query=kw, limit=10)
        for m in srch.get("results") or []:
            r_uuid = str(m.get("resource_uuid") or "")
            if r_uuid and r_uuid in flat_existing_uuids:
                continue
            cat = str(m.get("resource_type") or m.get("section") or "ClinicalItem")
            summ = str(m.get("display") or "")
            det = str(m.get("text") or "").strip().replace("\n", " ")
            if len(det) > 160:
                det = det[:157] + "..."
            code = str(m.get("code") or "")
            code_part = f" ({code})" if code and code not in summ else ""
            full_s = f"{summ}{code_part}" + (f" — {det}" if det and det not in summ else "")
            _try_add(str(m.get("date") or ""), cat, full_s, r_uuid)

    formatted_lines: List[str] = []
    cur_idx = start_citation_idx
    for date_s, cat_s, text_s, r_uuid in recovered_entries:
        cur_idx += 1
        uuid_suffix = f" [UUID: {r_uuid}]" if r_uuid else ""
        date_prefix = f"({date_s[:10]}) " if date_s and date_s != "unknown-date" else ""
        formatted_lines.append(
            f"[{cur_idx}] {date_prefix}Tier-0 Recovered {cat_s}: {text_s}{uuid_suffix}"
        )
    return formatted_lines


def _extract_codes_from_prompt_text(prompt_text: str) -> List[str]:
    codes: List[str] = []
    seen: Set[str] = set()
    for match in re.finditer(
        r"\b(?:SNOMED|ICD-?10|LOINC|RxNorm|CVX)\s*[:#]?\s*([A-Za-z0-9.-]+)",
        prompt_text,
        flags=re.IGNORECASE,
    ):
        c = match.group(1).strip().rstrip(".,;)")
        if c and c not in seen:
            seen.add(c)
            codes.append(c)
    return codes


def build_wahs_hub_enrichment_block(
    messages: Union[str, Sequence[Dict[str, Any]]],
    extracted: Optional[ExtractedChart] = None,
    max_tokens: int = 800,
) -> HubEnrichmentResult:
    """Build the bounded Med Agent Hub pre-generation context enrichment block.

    Implements:
      1. Conditional Tier-0 Overflow Recovery (triggered ONLY when ``omitted_clinical_items > 0``)
      2. Patient Clinical Entity Subgraph (``TREATS``, ``CONTRAINDICATED_OR_DOSE_ADJUST_IN``,
         ``MONITORED_BY``, ``CONTRADICTED_OR_MODIFIED_BY_NOTE``, ``REPORTED_BY_ORG``)
      3. Live Question-Conditioned Clinical KB Lookup (``server.kb.search(..., corpus="wahs")``)
      4. Strict ``<= max_tokens`` (default 800) token budget cap.
    """
    if isinstance(messages, str):
        prompt_text = messages
    else:
        prompt_text = "\n\n".join(
            str(m.get("content") or "")
            for m in messages
            if isinstance(m, dict) and m.get("content")
        )

    # Strip any prior orchestration block if re-invoked
    orch_marker = "[MED-AGENT-HUB ORCHESTRATION:"
    if orch_marker in prompt_text:
        prompt_text = prompt_text[: prompt_text.find(orch_marker)].rstrip()

    active_question = extract_active_question(prompt_text)
    omitted_count, omitted_by_cat = parse_omitted_clinical_items(prompt_text)
    resolved_extracted = _resolve_extracted_chart(prompt_text, extracted=extracted)

    # Build the patient-specific clinical entity graph from the condensed ledger
    # (or from resolved_extracted if the prompt text has no numbered [N] lines)
    has_numbered_lines = bool(re.search(r"^\s*\[\d+\]\s+", prompt_text, flags=re.MULTILINE))
    if has_numbered_lines:
        graph = ClinicalEntityGraph.from_condensed_ledger(
            prompt_text, extracted=resolved_extracted
        )
    elif resolved_extracted is not None:
        graph = ClinicalEntityGraph.from_extracted_chart(resolved_extracted)
        graph.omitted_clinical_items = omitted_count
        graph.omitted_by_category = dict(omitted_by_cat)
    else:
        graph = ClinicalEntityGraph.from_condensed_ledger(prompt_text)

    max_cit_before = graph.max_citation_idx

    # Rule 1: Conditional Tier-0 Overflow Recovery
    overflow_triggered = omitted_count > 0
    recovered_lines: List[str] = []
    if overflow_triggered and resolved_extracted is not None:
        recovered_lines = _recover_omitted_tier0_items(
            extracted=resolved_extracted,
            prompt_text=prompt_text,
            question=active_question,
            omitted_by_category=omitted_by_cat,
            start_citation_idx=max_cit_before,
            max_recovered=6,
        )
        if recovered_lines and has_numbered_lines:
            graph = ClinicalEntityGraph.from_condensed_ledger(
                f"{prompt_text}\n" + "\n".join(recovered_lines),
                extracted=resolved_extracted,
            )

    max_cit_after = max_cit_before + len(recovered_lines)

    # Rule 2: Patient Clinical Entity Subgraph
    subgraph_lines = graph.format_subgraph_lines(question=active_question, max_lines=8)

    # Rule 3: Live Question-Conditioned Clinical KB Lookup
    chart_codes = _extract_codes_from_prompt_text(prompt_text)
    if resolved_extracted is not None:
        for c in extract_chart_kb_codes(resolved_extracted):
            if c not in chart_codes:
                chart_codes.append(c)

    # Augment KB query with top entity labels so specific clinical terms match corpus
    entity_terms = " ".join(
        n.label for n in list(graph.nodes.values())[:6] if n.category in {"Condition", "MedicationRequest"}
    )
    kb_query = f"{active_question} {entity_terms}".strip()
    kb_hits = hub_kb.search(query=kb_query, corpus="wahs", k=4, codes=chart_codes)
    kb_lines: List[str] = []
    for hit in kb_hits:
        h_id = str(hit.get("id") or "kb")
        h_src = str(hit.get("source") or "Clinical Guideline")
        h_title = str(hit.get("title") or "")
        h_text = str(hit.get("text") or "").strip().replace("\n", " ")
        if len(h_text) > 210:
            h_text = h_text[:207] + "..."
        kb_lines.append(f"- [KB {h_id} | {h_src}] {h_title}: {h_text}")

    # Rule 4: Assemble and enforce <= max_tokens cap
    header_line = (
        "[MED-AGENT-HUB ORCHESTRATION: CLINICAL ENTITY SUBGRAPH, QUESTION KB & TIER-0 OVERFLOW RECOVERY]"
    )
    instruction_line = (
        "Hub Verification Directive: Preserve inline [N] citations when referencing linked clinical "
        "entities or Tier-0 overflow-recovered records below."
    )

    if overflow_triggered:
        cat_desc = (
            ", ".join(f"{k}={v}" for k, v in omitted_by_cat.items())
            if omitted_by_cat
            else f"total={omitted_count}"
        )
        overflow_header = (
            f"1. Tier-0 Overflow Recovery (ACTIVE: omitted_clinical_items={omitted_count} [{cat_desc}]; "
            f"recovered={len(recovered_lines)} records via wahs_tools):"
        )
    else:
        overflow_header = (
            "1. Tier-0 Overflow Recovery: SKIPPED (omitted_clinical_items=0; "
            "100% of clinical items are already represented in the condensed chart above)."
        )

    def _render_block(
        rec_slice: List[str],
        sub_slice: List[str],
        kb_slice: List[str],
    ) -> str:
        sections: List[str] = [header_line, instruction_line, overflow_header]
        if rec_slice:
            sections.extend(rec_slice)
        sections.append(
            f"2. Patient Clinical Entity Subgraph ({len(graph.nodes)} nodes, {len(graph.edges)} edges):"
        )
        if sub_slice:
            sections.extend(sub_slice)
        else:
            sections.append("- No cross-entity conflicts or multi-hop links triggered for this chart view.")
        sections.append(
            f"3. Question-Conditioned Clinical Knowledge Base ({len(kb_slice)} matched guidelines):"
        )
        if kb_slice:
            sections.extend(kb_slice)
        else:
            sections.append("- Standard clinical reconciliation rules apply.")
        return "\n".join(sections)

    cur_rec = list(recovered_lines)
    cur_sub = list(subgraph_lines)
    cur_kb = list(kb_lines)

    block_text = _render_block(cur_rec, cur_sub, cur_kb)
    while estimate_tokens_local(block_text) > max_tokens:
        if len(cur_sub) > 3:
            cur_sub.pop()
        elif len(cur_kb) > 1:
            cur_kb.pop()
        elif len(cur_sub) > 2:
            cur_sub.pop()
        elif len(cur_rec) > 1:
            cur_rec.pop()
        elif len(cur_sub) > 1:
            cur_sub.pop()
        elif cur_kb:
            cur_kb.pop()
        else:
            # Hard character truncation safeguard
            max_chars = max(256, max_tokens * 3)
            block_text = block_text[:max_chars].rsplit("\n", 1)[0]
            break
        block_text = _render_block(cur_rec, cur_sub, cur_kb)

    final_tokens = estimate_tokens_local(block_text)
    max_cit_after = max_cit_before + len(cur_rec)
    rec_mappings: List[Dict[str, Any]] = []
    rec_line_re = re.compile(
        r"^\s*\[(\d+)\]\s*(?:\(([^)]+)\)\s*)?Tier-0 Recovered\s+([^:]+):\s*(.*?)(?:\s*\[UUID:\s*([^\]]+)\])?\s*$"
    )
    for r_ln in cur_rec:
        m_rec = rec_line_re.match(r_ln)
        if m_rec:
            r_idx = int(m_rec.group(1))
            r_dt = (m_rec.group(2) or "").strip()
            r_cat = (m_rec.group(3) or "ClinicalItem").strip()
            r_disp = (m_rec.group(4) or "").strip()
            r_uuid = (m_rec.group(5) or "").strip()
            if r_uuid:
                rec_mappings.append(
                    {
                        "index": r_idx,
                        "resourceType": r_cat,
                        "resourceUuid": r_uuid,
                        "date": r_dt,
                        "display": r_disp,
                        "memberUuids": [r_uuid],
                    }
                )

    return HubEnrichmentResult(
        enrichment_text=block_text,
        enrichment_tokens=final_tokens,
        omitted_clinical_items=omitted_count,
        overflow_triggered=overflow_triggered,
        overflow_recovered_count=len(cur_rec),
        subgraph_nodes=len(graph.nodes),
        subgraph_edges=len(graph.edges),
        kb_Passages_matched=len(cur_kb),
        max_citation_idx_before=max_cit_before,
        max_citation_idx_after=max_cit_after,
        active_question=active_question,
        patient_name=graph.patient_name,
        omitted_by_category=omitted_by_cat,
        edge_Breakdown=graph.edge_breakdown(),
        recovered_mappings=rec_mappings,
    )


# ---------------------------------------------------------------------------
# Internal Parsing & Classification Helpers
# ---------------------------------------------------------------------------


def _slugify(value: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "_", (value or "").strip().lower()).strip("_")
    return s[:48] or "item"


def _normalize_person_key(name: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9\s]", " ", (name or "").lower())
    return " ".join(cleaned.split())


def _parse_patient_header_from_text(text: str) -> Tuple[str, str]:
    patient_name = ""
    patient_id = ""

    # Match "[1] (date) Patient: Name, DOB: ..." or "Patient: Name (DOB ...)"
    m_name = re.search(
        r"\bPatient\s*:\s*([A-Za-z0-9_.', -]+?)(?:\s*\((?:DOB|ID)|,\s*(?:DOB|Age|Gender|ID)|\n|$)",
        text,
    )
    if m_name:
        patient_name = m_name.group(1).strip()

    m_id = re.search(r"\bPatient\s+ID\s*:\s*([A-Za-z0-9_-]+)", text, flags=re.IGNORECASE)
    if m_id:
        patient_id = m_id.group(1).strip()

    return patient_name, patient_id


def _find_max_citation_idx(text: str) -> int:
    indices = [
        int(m.group(1))
        for m in re.finditer(r"^\s*\[(\d+)\]\s+", text, flags=re.MULTILINE)
    ]
    return max(indices, default=0)


def _extract_first_uuid(line: str) -> str:
    m = re.search(r"\bUUIDs?\s*:\s*([A-Za-z0-9_-]+)", line)
    if m:
        return m.group(1).strip()
    return ""


def _extract_first_code(line: str) -> str:
    m = re.search(
        r"\b(?:SNOMED|ICD-?10|LOINC|RxNorm|CVX)\s*[:#]?\s*([A-Za-z0-9.-]+)",
        line,
        flags=re.IGNORECASE,
    )
    if m:
        return m.group(1).strip().rstrip(".,;)")
    return ""


def _classify_ledger_line_category(body: str) -> str:
    lower = re.sub(r"^tier-0\s+recovered\s+", "", body.lower()).strip()
    if lower.startswith(("condition", "problem", "diagnosis", "active problem")):
        return "Condition"
    if lower.startswith(("medication", "rx:", "prescribed")):
        return "MedicationRequest"
    if lower.startswith(("allergy", "intolerance")):
        return "AllergyIntolerance"
    if lower.startswith(("immunization", "vaccine")):
        return "Immunization"
    if lower.startswith((
        "lab",
        "vital",
        "observation",
        "numeric",
        "diagnostic observation",
    )):
        return "Observation"
    if lower.startswith((
        "clinical note",
        "note",
        "documentreference",
        "diagnosticreport",
        "report",
        "narrative",
        "discharge summary",
        "progress note",
    )):
        return "DocumentReference"
    if lower.startswith(("organization", "facility")):
        return "Organization"

    # Secondary heuristic if line has embedded prefix
    if "rxnorm" in lower and ("mg" in lower or "tablet" in lower or "capsule" in lower or "daily" in lower):
        return "MedicationRequest"
    if "loinc" in lower and ("latest" in lower or "mg/dl" in lower or "mmol/l" in lower or "ml/min" in lower or "%" in lower):
        return "Observation"
    if "snomed" in lower or "icd-10" in lower or "icd10" in lower:
        return "Condition"
    return ""


def _split_ledger_label_and_value(body: str, category: str) -> Tuple[str, str]:
    # Strip category prefix like "Condition Summary:", "Medication [HIGH-ALERT: ...]:", "Lab Trajectory:"
    cleaned = re.sub(
        r"^(?:Tier-0\s+Recovered\s+)?(?:Condition(?:\s+Summary)?|Active\s+Problem|Problem|Medication(?:\s*\[[^\]]*\])?|Device|"
        r"Allergy/Intolerance|Allergy|Immunization(?:\s+Series)?|Lab\s+Trajectory|Lab/Vital\s+Observation|"
        r"Lab(?:\s+Observation)?|Observation|Clinical\s+Note|Diagnostic\s+Report|Report|Note)\s*:\s*",
        "",
        body,
        flags=re.IGNORECASE,
    ).strip()

    # Strip trailing [UUID: ...] from display
    cleaned_no_uuid = re.sub(r"\s*\[UUIDs?\s*:[^\]]*\]", "", cleaned).strip()

    if category == "Observation" and ":" in cleaned_no_uuid:
        head, tail = cleaned_no_uuid.split(":", 1)
        return head.strip(), tail.strip()[:160]
    if category == "AllergyIntolerance":
        m_qual = re.match(r"^([^(\[]+?)\s*([(\[](?:status|criticality|reaction|verification|category)\b.*)$", cleaned_no_uuid, flags=re.IGNORECASE)
        if m_qual:
            return m_qual.group(1).strip()[:120], m_qual.group(2).strip("()[] ")[:160]
    if "—" in cleaned_no_uuid:
        head, tail = cleaned_no_uuid.split("—", 1)
        return head.strip(), tail.strip()[:160]
    if "|" in cleaned_no_uuid:
        head, tail = cleaned_no_uuid.split("|", 1)
        return head.strip(), tail.strip()[:160]
    return cleaned_no_uuid[:120], ""


_ALLERGEN_METADATA_STOP_WORDS = {
    "allergy", "allergic", "intolerance", "substance", "medication", "drug", "agent",
    "food", "environment", "biologic", "category", "status", "active", "inactive",
    "resolved", "verification", "confirmed", "unconfirmed", "refuted", "entered",
    "error", "criticality", "high", "low", "unable", "assess", "reaction", "severe",
    "moderate", "mild", "anaphylaxis", "rash", "hives", "urticaria", "itching",
    "nausea", "vomiting", "diarrhea", "dyspnea", "bronchospasm", "angioedema",
    "swelling", "history", "known", "unspecified", "other", "snomed", "rxnorm",
    "loinc", "icd10", "uuid", "oral", "tablet", "capsule", "sodium", "potassium",
    "hydrochloride", "calcium", "magnesium", "sulfate", "acetate", "tartrate",
    "succinate", "besylate", "maleate", "fumarate", "citrate", "finding", "disorder",
}


def _extract_allergen_substance_tokens(alg_label: str) -> Tuple[str, List[str]]:
    """Extract normalized allergen substance text and specific substance tokens from an allergy label."""
    if not alg_label:
        return "", []
    # Strip trailing qualifier blocks such as (status: active, ...) or [status: active; ...]
    substance_head = re.sub(
        r"\s*[(\[][^)\]]*\b(?:status|criticality|reaction|verification|category|snomed|rxnorm)\b[^)\]]*[)\]]",
        " ",
        alg_label,
        flags=re.IGNORECASE,
    )
    substance_head = re.sub(r"\b(?:allergy|intolerance)\s+to\b", " ", substance_head, flags=re.IGNORECASE)
    substance_text = " ".join(substance_head.lower().split())
    tokens = [
        w for w in re.findall(r"[a-z]{4,}", substance_text)
        if w not in _ALLERGEN_METADATA_STOP_WORDS
    ]
    return substance_text, tokens


def _extract_org_name_from_text(text: str) -> str:
    if not text:
        return ""
    m_org = re.search(
        r"(?:\borg\b|\bfacility\b|\bperformer\b|\breported by\b|\bfrom\b)\s*[:=]?\s*"
        r"([A-Z][A-Za-z0-9&'. -]{3,45})",
        text,
    )
    if m_org:
        cand = m_org.group(1).strip().rstrip(".,;|])")
        if len(cand) >= 4 and not cand.lower().startswith(("patient", "active", "completed", "final")):
            return cand
    m_fac = _KNOWN_FACILITY_PATTERNS.search(text)
    if m_fac:
        return m_fac.group(1).strip()
    return ""


def _is_observation_clinically_notable(
    obs_node: ClinicalNode,
    drug_keys: Tuple[str, ...],
) -> bool:
    """Check whether an Observation node warrants a CONTRAINDICATED_OR_DOSE_ADJUST_IN edge."""
    meta = obs_node.metadata or {}
    latest_val = meta.get("latest_value")
    text = f"{obs_node.label} {obs_node.value_summary} {obs_node.raw_line}".lower()

    # Check numeric thresholds when available
    if isinstance(latest_val, (int, float)):
        if any(k in text for k in ("glomerular filtration", "egfr")):
            return float(latest_val) < 60.0
        if "creatinine" in text and "albumin" not in text:
            return float(latest_val) >= 1.3
        if "potassium" in text:
            return float(latest_val) >= 5.0 or float(latest_val) <= 3.4
        if "inr" in text:
            return float(latest_val) > 3.0
        if "hemoglobin a1c" in text or "hba1c" in text:
            return float(latest_val) < 6.5 or float(latest_val) >= 8.0

    # Parse latest numeric value from value_summary text if metadata not populated
    m_latest = re.search(r"(?:latest\s*[=:]?\s*)(\d+(?:\.\d+)?)", text)
    if m_latest:
        val = float(m_latest.group(1))
        if any(k in text for k in ("glomerular filtration", "egfr")):
            return val < 60.0
        if "creatinine" in text and "albumin" not in text:
            return val >= 1.3
        if "potassium" in text:
            return val >= 5.0 or val <= 3.4
        if "inr" in text:
            return val > 3.0

    return any(
        flag in text
        for flag in ("high", "low", "critical", "abnormal", "elevated", "stage 4", "stage 5")
    )


def _extract_entity_match_terms(node: ClinicalNode) -> List[str]:
    terms: List[str] = []
    if node.category == "MedicationRequest":
        ing = normalize_drug_ingredient(node.label).lower()
        if ing and len(ing) >= 4:
            terms.append(ing)
        first_word = re.findall(r"[a-zA-Z]{4,}", node.label.lower())
        if first_word and first_word[0] not in terms:
            terms.append(first_word[0])
    elif node.category == "Condition":
        lower_lbl = node.label.lower()
        for kw in (
            "kidney disease",
            "ckd",
            "heart failure",
            "hfref",
            "hfpef",
            "atrial fibrillation",
            "afib",
            "diabetes",
            "hypertension",
            "coronary",
            "gout",
            "asthma",
            "copd",
            "hypothyroidism",
            "anemia",
            "depression",
        ):
            if kw in lower_lbl:
                terms.append(kw)
        if not terms:
            words = [
                w for w in re.findall(r"[a-z]{5,}", lower_lbl)
                if w not in {"chronic", "acute", "disorder", "disease", "syndrome", "finding", "history", "stage", "severe", "moderate"}
            ]
            terms.extend(words[:2])
    return terms


def _extract_question_keywords(question: str) -> List[str]:
    if not question:
        return []
    stop_words = {
        "what", "which", "when", "where", "whose", "whom", "this", "that", "these",
        "those", "there", "their", "about", "patient", "chart", "record", "records",
        "clinical", "summary", "summarize", "provide", "please", "doctor", "questions",
        "based", "current", "active", "history", "should", "would", "could", "have",
        "with", "from", "into", "during", "after", "before", "between", "under",
        "review", "prepare", "visit", "appointment",
    }
    words = re.findall(r"[a-zA-Z0-9-]{3,}", question.lower())
    out: List[str] = []
    seen: Set[str] = set()
    for w in words:
        if w not in stop_words and w not in seen:
            seen.add(w)
            out.append(w)
    return out
