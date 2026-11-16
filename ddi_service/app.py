"""Standalone clinical DDI & medication-safety microservice backed by DDInter 2.0 + RxNorm + CIEL."""

from __future__ import annotations

import hashlib
import json
import os
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from fastapi import FastAPI
from pydantic import BaseModel, Field

DEFAULT_KB_PATH = Path(__file__).resolve().parent / "data" / "ddi-knowledge-base.json"

AMPATH_LEGACY_CIEL_TO_DRUGS: Dict[str, Tuple[str, ...]] = {
    "625": ("stavudine",),
    "628": ("lamivudine",),
    "630": ("lamivudine", "zidovudine"),
    "631": ("nevirapine",),
    "633": ("efavirenz",),
    "656": ("isoniazid",),
    "794": ("lopinavir", "ritonavir"),
    "916": ("sulfamethoxazole", "trimethoprim"),
    "1131": ("rifampicin", "isoniazid", "pyrazinamide", "ethambutol"),
}

ALIAS_OVERRIDES: Dict[str, Tuple[str, ...]] = {
    "rifampin": ("rifampicin",),
    "acetaminophen": ("paracetamol",),
    "co-trimoxazole": ("sulfamethoxazole", "trimethoprim"),
    "cotrimoxazole": ("sulfamethoxazole", "trimethoprim"),
    "tmp-smx": ("sulfamethoxazole", "trimethoprim"),
    "bactrim": ("sulfamethoxazole", "trimethoprim"),
    "septra": ("sulfamethoxazole", "trimethoprim"),
    "combivir": ("lamivudine", "zidovudine"),
    "kaletra": ("lopinavir", "ritonavir"),
    "tenofovir": ("tenofovir disoproxil",),
    "tdf": ("tenofovir disoproxil",),
    "taf": ("tenofovir alafenamide",),
    "d4t": ("stavudine",),
    "3tc": ("lamivudine",),
    "azt": ("zidovudine",),
    "zdv": ("zidovudine",),
    "nvp": ("nevirapine",),
    "efv": ("efavirenz",),
    "dtg": ("dolutegravir",),
    "tld": ("tenofovir disoproxil", "lamivudine", "dolutegravir"),
    "rhze": ("rifampicin", "isoniazid", "pyrazinamide", "ethambutol"),
    "inh": ("isoniazid",),
}

CLINICAL_SHORT_ALIASES: frozenset[str] = frozenset(
    {"d4t", "3tc", "azt", "zdv", "nvp", "efv", "dtg", "tdf", "taf", "tld", "rhze", "inh"}
)

AMBIGUOUS_SINGLE_WORDS: frozenset[str] = frozenset(
    {
        "acid",
        "aid",
        "aim",
        "air",
        "all",
        "arm",
        "art",
        "ask",
        "base",
        "bed",
        "bit",
        "box",
        "can",
        "cap",
        "care",
        "case",
        "coal",
        "cold",
        "cut",
        "day",
        "diet",
        "dose",
        "drug",
        "dry",
        "due",
        "ear",
        "end",
        "eye",
        "fast",
        "fat",
        "fit",
        "fix",
        "flu",
        "for",
        "gas",
        "gel",
        "get",
        "gold",
        "gum",
        "had",
        "has",
        "heat",
        "her",
        "high",
        "him",
        "his",
        "hot",
        "ice",
        "ill",
        "iron",
        "key",
        "lab",
        "lead",
        "leg",
        "less",
        "lime",
        "line",
        "lip",
        "low",
        "man",
        "map",
        "may",
        "men",
        "met",
        "mint",
        "mix",
        "more",
        "most",
        "nap",
        "net",
        "new",
        "nil",
        "none",
        "nor",
        "not",
        "note",
        "now",
        "oil",
        "old",
        "one",
        "only",
        "oral",
        "out",
        "own",
        "pain",
        "pair",
        "pan",
        "part",
        "past",
        "patch",
        "per",
        "pill",
        "plan",
        "plus",
        "poor",
        "port",
        "post",
        "put",
        "rate",
        "raw",
        "red",
        "rest",
        "risk",
        "rod",
        "row",
        "run",
        "safe",
        "salt",
        "same",
        "scan",
        "see",
        "set",
        "she",
        "shot",
        "show",
        "side",
        "sign",
        "six",
        "skin",
        "slow",
        "some",
        "sort",
        "step",
        "stop",
        "sub",
        "such",
        "sum",
        "sun",
        "tab",
        "take",
        "tea",
        "ten",
        "test",
        "than",
        "that",
        "the",
        "them",
        "then",
        "they",
        "this",
        "time",
        "tip",
        "too",
        "top",
        "true",
        "two",
        "urea",
        "use",
        "very",
        "via",
        "wait",
        "war",
        "was",
        "water",
        "way",
        "weak",
        "week",
        "well",
        "were",
        "what",
        "when",
        "who",
        "why",
        "will",
        "with",
        "word",
        "work",
        "year",
        "your",
        "zero",
        "zinc",
    }
)

ALLERGY_CLASS_SYNONYMS: Dict[str, Tuple[str, ...]] = {
    "sulfa": ("sulfamethoxazole", "sulfadiazine", "sulfasalazine", "dapsone"),
    "sulfonamide": ("sulfamethoxazole", "sulfadiazine", "sulfasalazine", "dapsone"),
    "penicillin": ("amoxicillin", "ampicillin", "penicillin", "piperacillin", "flucloxacillin"),
    "beta-lactam": ("amoxicillin", "ampicillin", "penicillin", "ceftriaxone", "cefuroxime"),
    "nsaid": ("ibuprofen", "diclofenac", "naproxen", "indomethacin", "meloxicam", "aspirin"),
    "aspirin": ("aspirin", "acetylsalicylic"),
}

_CIEL_CODE_RE = re.compile(r"^(?:CIEL:)?(\d+)(?:A{20,})?$", re.IGNORECASE)
_TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9\-]*")
_DOSE_SUFFIX_RE = re.compile(
    r"\b\d+(?:\.\d+)?\s*(?:mg|mcg|ug|g|kg|ml|l|iu|units?|tablets?|tabs?|caps?|capsules?|solution|suspension|syrup|cream|ointment)\b.*$",
    re.IGNORECASE,
)
_SEVERITY_RANK = {"major": 0, "moderate": 1, "minor": 2, "unknown": 3}
_ACTIONABLE_SEVERITIES = frozenset({"major", "moderate"})


def _normalize_ciel(raw: Any) -> Optional[str]:
    text = str(raw or "").strip()
    if not text:
        return None
    m = _CIEL_CODE_RE.fullmatch(text)
    if m:
        return m.group(1)
    return None


@dataclass
class DdiKnowledgeIndex:
    package: Dict[str, Any]
    schema_version: str
    drugs_by_id: Dict[str, Dict[str, Any]]
    by_ciel_code: Dict[str, List[Dict[str, Any]]]
    by_ciel_uuid: Dict[str, List[Dict[str, Any]]]
    by_alias: Dict[str, List[Dict[str, Any]]]
    interactions: Dict[Tuple[str, str], Tuple[str, str]]
    mechanisms: Dict[str, str]
    disease_interactions: Dict[str, List[Tuple[str, str, str]]]
    disease_notes: Dict[str, str]
    drug_count: int
    interaction_count: int
    searchable_aliases: frozenset[str] = field(default_factory=frozenset)

    @classmethod
    def load(cls, path: Path) -> "DdiKnowledgeIndex":
        raw_bytes = path.read_bytes()
        sha256 = hashlib.sha256(raw_bytes).hexdigest()
        data = json.loads(raw_bytes.decode("utf-8"))
        meta = data.get("metadata") or {}
        src = meta.get("source") or {}
        schema_version = str(meta.get("schema_version") or "1.5")

        drugs_by_id: Dict[str, Dict[str, Any]] = {}
        by_ciel_code: Dict[str, List[Dict[str, Any]]] = {}
        by_ciel_uuid: Dict[str, List[Dict[str, Any]]] = {}
        by_alias: Dict[str, List[Dict[str, Any]]] = {}

        def add_unique(bucket: Dict[str, List[Dict[str, Any]]], key: str, drug: Dict[str, Any]) -> None:
            k = key.strip().casefold()
            if not k:
                return
            existing = bucket.setdefault(k, [])
            if all(d["id"] != drug["id"] for d in existing):
                existing.append(drug)

        for drug in data.get("drugs") or ():
            did = str(drug.get("id") or "").strip()
            if not did:
                continue
            drugs_by_id[did] = drug
            for raw_alias in (
                [drug.get("name"), drug.get("rxnorm_name")]
                + list(drug.get("brand_names") or ())
                + list(drug.get("international_names") or ())
                + list(drug.get("abbreviations") or ())
            ):
                if isinstance(raw_alias, str) and raw_alias.strip():
                    add_unique(by_alias, raw_alias, drug)

            for ciel in drug.get("ciel") or ():
                if not isinstance(ciel, Mapping):
                    continue
                code = _normalize_ciel(ciel.get("code"))
                if code:
                    add_unique(by_ciel_code, code, drug)
                cuuid = str(ciel.get("uuid") or "").strip().upper()
                if cuuid:
                    by_ciel_uuid.setdefault(cuuid, [])
                    if all(d["id"] != did for d in by_ciel_uuid[cuuid]):
                        by_ciel_uuid[cuuid].append(drug)
                cname = str(ciel.get("name") or "").strip()
                if cname and "/" not in cname:
                    add_unique(by_alias, cname, drug)

        for alias, targets in ALIAS_OVERRIDES.items():
            for target_name in targets:
                for matched in by_alias.get(target_name.casefold(), ()):
                    add_unique(by_alias, alias, matched)

        interactions: Dict[Tuple[str, str], Tuple[str, str]] = {}
        raw_interactions = data.get("interactions") or ()
        for item in raw_interactions:
            if not isinstance(item, (list, tuple)) or len(item) < 4:
                continue
            a, b, sev, mid = str(item[0]), str(item[1]), str(item[2]), str(item[3])
            pair = (a, b) if a <= b else (b, a)
            interactions[pair] = (sev, mid)

        mechanisms: Dict[str, str] = {}
        for mid, mobj in (data.get("mechanisms") or {}).items():
            if isinstance(mobj, Mapping) and mobj.get("text"):
                mechanisms[str(mid)] = str(mobj["text"]).strip()

        disease_interactions: Dict[str, List[Tuple[str, str, str]]] = {}
        for item in data.get("disease_interactions") or ():
            if not isinstance(item, (list, tuple)) or len(item) < 4:
                continue
            did, cond, sev, nid = str(item[0]), str(item[1]), str(item[2]), str(item[3])
            disease_interactions.setdefault(did, []).append((cond, sev, nid))

        disease_notes: Dict[str, str] = {}
        for nid, nobj in (data.get("disease_notes") or {}).items():
            if isinstance(nobj, Mapping) and nobj.get("text"):
                disease_notes[str(nid)] = str(nobj["text"]).strip()

        searchable = frozenset(
            k
            for k in by_alias
            if (len(k) >= 4 or k in CLINICAL_SHORT_ALIASES) and k not in AMBIGUOUS_SINGLE_WORDS
        )

        package = {
            "name": "openmrs-ddi-knowledge-base",
            "version": schema_version,
            "sha256": sha256,
            "provenance": {
                "source": str(src.get("name") or "DDInter 2.0"),
                "dataset": "openmrs-ddi-knowledge-base",
                "origin": f"{src.get('normalization') or 'RxNorm'} / {src.get('bridge') or 'CIEL'}",
            },
        }
        return cls(
            package=package,
            schema_version=schema_version,
            drugs_by_id=drugs_by_id,
            by_ciel_code=by_ciel_code,
            by_ciel_uuid=by_ciel_uuid,
            by_alias=by_alias,
            interactions=interactions,
            mechanisms=mechanisms,
            disease_interactions=disease_interactions,
            disease_notes=disease_notes,
            drug_count=len(drugs_by_id),
            interaction_count=len(interactions),
            searchable_aliases=searchable,
        )

    def resolve_names_or_codes(self, tokens: Sequence[str]) -> List[Dict[str, Any]]:
        seen_ids: Set[str] = set()
        resolved: List[Dict[str, Any]] = []

        def add_drugs(drugs: Sequence[Dict[str, Any]]) -> None:
            for d in drugs:
                if d["id"] not in seen_ids:
                    seen_ids.add(d["id"])
                    resolved.append(d)

        for raw in tokens:
            s = str(raw or "").strip()
            if not s:
                continue
            code = _normalize_ciel(s)
            if code:
                if code in AMPATH_LEGACY_CIEL_TO_DRUGS:
                    for name in AMPATH_LEGACY_CIEL_TO_DRUGS[code]:
                        add_drugs(self.by_alias.get(name, ()))
                if code in self.by_ciel_code:
                    add_drugs(self.by_ciel_code[code])
                continue
            cuuid = s.upper()
            if cuuid in self.by_ciel_uuid:
                add_drugs(self.by_ciel_uuid[cuuid])
                continue
            parts = [p.strip() for p in re.split(r"[/+,;]|\band\b", s, flags=re.I) if p.strip()]
            for part in (s, *parts):
                cleaned = _DOSE_SUFFIX_RE.sub("", part).strip(" .,:;()[]{}")
                for variant in (part, cleaned):
                    key = variant.casefold()
                    if key in self.by_alias:
                        add_drugs(self.by_alias[key])
        return resolved

    def resolve_record_drugs(self, metadata: Mapping[str, Any]) -> List[Dict[str, Any]]:
        tokens: List[str] = []
        for raw_code in metadata.get("mapping_codes") or ():
            code = _normalize_ciel(raw_code)
            if code:
                tokens.append(f"CIEL:{code}")
        for uuid_key in ("concept_uuid", "drug_uuid"):
            raw_u = metadata.get(uuid_key)
            if raw_u:
                tokens.append(str(raw_u))
        drugs = self.resolve_names_or_codes(tokens)
        if drugs:
            return drugs
        name_tokens: List[str] = []
        for name_key in ("drug_name", "concept_name"):
            raw_n = metadata.get(name_key)
            if raw_n:
                name_tokens.append(str(raw_n))
        return self.resolve_names_or_codes(name_tokens)

    def scan_text_drugs(self, text: str) -> List[Dict[str, Any]]:
        if not text:
            return []
        words = [w.casefold() for w in _TOKEN_RE.findall(text)]
        if not words:
            return []
        seen_ids: Set[str] = set()
        found: List[Dict[str, Any]] = []

        def collect(alias_key: str) -> None:
            if alias_key not in self.searchable_aliases:
                return
            for drug in self.by_alias.get(alias_key, ()):
                if drug["id"] not in seen_ids:
                    seen_ids.add(drug["id"])
                    found.append(drug)

        n = len(words)
        for i in range(n):
            collect(words[i])
            if i + 1 < n:
                collect(f"{words[i]} {words[i + 1]}")
            if i + 2 < n:
                collect(f"{words[i]} {words[i + 1]} {words[i + 2]}")
        return found

    def lookup_interaction(self, drug_a: Dict[str, Any], drug_b: Dict[str, Any]) -> Optional[Tuple[str, str]]:
        if drug_a["id"] == drug_b["id"]:
            return None
        pair = (drug_a["id"], drug_b["id"]) if drug_a["id"] <= drug_b["id"] else (drug_b["id"], drug_a["id"])
        hit = self.interactions.get(pair)
        if not hit:
            return None
        sev, mid = hit
        if sev.strip().casefold() not in _ACTIONABLE_SEVERITIES:
            return None
        mech = self.mechanisms.get(mid, "")
        return sev, mech


_index: Optional[DdiKnowledgeIndex] = None


def get_index() -> DdiKnowledgeIndex:
    global _index
    if _index is None:
        kb_path = Path(os.environ.get("DDI_KB_PATH") or DEFAULT_KB_PATH)
        _index = DdiKnowledgeIndex.load(kb_path)
    return _index


@asynccontextmanager
async def lifespan(_app: FastAPI):
    get_index()
    yield


app = FastAPI(title="OpenMRS DDI Service", version="1.0.0", lifespan=lifespan)


def _order_is_active(meta: Mapping[str, Any], text: str, reference_date: Optional[str]) -> bool:
    action = str(meta.get("action") or "").strip().upper()
    if action == "DISCONTINUE":
        return False
    stopped = str(meta.get("date_stopped") or "").strip()
    if stopped:
        if reference_date:
            try:
                if date.fromisoformat(stopped[:10]) <= date.fromisoformat(reference_date[:10]):
                    return False
            except ValueError:
                return False
        else:
            return False
    lower = text.lower()
    if "no longer in force" in lower or ". stopped:" in lower:
        return False
    return True


def _drug_matches_allergy(drug: Mapping[str, Any], allergy_tokens: Sequence[str]) -> Optional[str]:
    drug_names = {
        str(drug.get("name") or "").casefold(),
        str(drug.get("rxnorm_name") or "").casefold(),
    }
    drug_names.update(str(a).casefold() for a in (drug.get("brand_names") or ()))
    for token in allergy_tokens:
        low = str(token or "").strip().casefold()
        if not low:
            continue
        if any(low in dn or dn in low for dn in drug_names if dn):
            return token
        for class_kw, members in ALLERGY_CLASS_SYNONYMS.items():
            if class_kw in low and any(m in dn for m in members for dn in drug_names if dn):
                return token
    return None


def _identity_confidence(active_count: int, mapped_count: int, exposure_complete: bool) -> str:
    if not exposure_complete:
        return "limited" if active_count else "unavailable"
    if active_count == 0 or mapped_count == active_count:
        return "high"
    return "partial" if mapped_count > 0 else "low"


def _compute_ddis(
    idx: DdiKnowledgeIndex,
    *,
    active_ids: Sequence[str],
    order_sources: Mapping[str, Sequence[int]],
    extra_ids: Sequence[str] = (),
    allergy_tokens: Sequence[str] = (),
) -> List[Dict[str, Any]]:
    warnings: List[Dict[str, Any]] = []
    seen_keys: Set[Tuple[str, ...]] = set()

    def same_single_combination_order(a_id: str, b_id: str) -> bool:
        sa = set(order_sources.get(a_id) or ())
        sb = set(order_sources.get(b_id) or ())
        return bool(sa) and sa == sb and len(sa) == 1

    # 1. Allergy contraindications for both active and newly checked drugs
    for did in list(dict.fromkeys([*active_ids, *extra_ids])):
        drug = idx.drugs_by_id.get(did)
        if not drug:
            continue
        matched_allergy = _drug_matches_allergy(drug, allergy_tokens)
        if matched_allergy:
            key = ("contraindication", did)
            if key not in seen_keys:
                seen_keys.add(key)
                warnings.append(
                    {
                        "type": "contraindication",
                        "drug": str(drug["name"]),
                        "severity": "Major",
                        "detail": (
                            f"{drug['name']} conflicts with documented allergy '{matched_allergy}'."
                        ),
                    }
                )

    # 2. Pairwise DDIs across distinct active orders and against extra/delta drugs
    candidate_ids = list(dict.fromkeys([*active_ids, *extra_ids]))
    extra_set = set(extra_ids)
    active_set = set(active_ids)
    hits: List[Tuple[int, str, str, str, str, str]] = []

    for i, a_id in enumerate(candidate_ids):
        drug_a = idx.drugs_by_id.get(a_id)
        if not drug_a:
            continue
        for b_id in candidate_ids[i + 1 :]:
            drug_b = idx.drugs_by_id.get(b_id)
            if not drug_b:
                continue
            if a_id in active_set and b_id in active_set and same_single_combination_order(a_id, b_id):
                continue
            # Also skip if both come ONLY from fixed-dose combinations in extra_ids via a single compound? No,
            # cross-checking active vs active or active vs delta or proposed pair is intended.
            res = idx.lookup_interaction(drug_a, drug_b)
            if not res:
                continue
            sev, mech = res
            pair = tuple(sorted((a_id, b_id)))
            key = ("interaction", pair[0], pair[1])
            if key in seen_keys:
                continue
            seen_keys.add(key)
            label_a, label_b = str(drug_a["name"]), str(drug_b["name"])
            is_delta = (a_id in extra_set) or (b_id in extra_set)
            sev_rank = _SEVERITY_RANK.get(sev.casefold(), 9)
            detail = (
                f"{label_a} + {label_b} ({sev} interaction): {mech}"
                if mech
                else f"{label_a} + {label_b} ({sev} interaction)."
            )
            hits.append((0 if is_delta else 1, sev_rank, f"{label_a} + {label_b}", sev, detail, f"{pair[0]}:{pair[1]}"))

    hits.sort(key=lambda h: (h[0], h[1], h[2]))
    for _is_delta_ord, _sev_rank, pair_label, sev, detail, pid in hits:
        warnings.append(
            {
                "type": "interaction",
                "drug": pair_label,
                "severity": sev,
                "detail": detail,
                "pair_id": pid,
            }
        )
    return warnings


class ResolveRequest(BaseModel):
    tokens: List[str] = Field(default_factory=list)


class InteractionsRequest(BaseModel):
    drugs: List[str] = Field(default_factory=list)


class PrepareRequest(BaseModel):
    records: List[Dict[str, Any]] = Field(default_factory=list)
    reference_date: Optional[str] = None
    question: str = ""
    exposure_complete: bool = False


class CheckRequest(BaseModel):
    patient_context: Dict[str, Any] = Field(default_factory=dict)
    answer_text: str = ""
    question: str = ""


@app.get("/health")
def health() -> Dict[str, Any]:
    idx = get_index()
    return {
        "status": "ok",
        "drugs": idx.drug_count,
        "interactions": idx.interaction_count,
        "schema_version": idx.schema_version,
    }


@app.post("/v1/resolve")
def resolve(req: ResolveRequest) -> Dict[str, Any]:
    idx = get_index()
    resolved = idx.resolve_names_or_codes(req.tokens)
    return {
        "drugs": [
            {
                "id": d["id"],
                "name": d["name"],
                "rxcui": d.get("rxcui"),
                "atc": d.get("atc") or [],
            }
            for d in resolved
        ]
    }


@app.post("/v1/interactions")
def interactions(req: InteractionsRequest) -> Dict[str, Any]:
    idx = get_index()
    resolved = idx.resolve_names_or_codes(req.drugs)
    ids = [d["id"] for d in resolved]
    findings = _compute_ddis(idx, active_ids=ids, order_sources={})
    return {"resolved": [d["name"] for d in resolved], "interactions": findings}


@app.post("/v1/prepare")
def prepare(req: PrepareRequest) -> Dict[str, Any]:
    idx = get_index()
    age_years: Optional[int] = None
    active_order_count = 0
    mapped_active_order_count = 0
    active_ids: List[str] = []
    seen_active_ids: Set[str] = set()
    order_sources: Dict[str, List[int]] = {}
    atc_codes: Set[str] = set()
    allergy_tokens: List[str] = []
    condition_tokens: List[str] = []
    order_idx = 0

    for rec in req.records:
        rtype = str(rec.get("resourceType") or "").strip().lower()
        meta = rec.get("metadata") or {}
        text = str(rec.get("text") or "")
        if rtype == "patient":
            if age_years is None and meta.get("age_years") is not None:
                age_years = meta.get("age_years")
        elif rtype == "drug_order":
            if not _order_is_active(meta, text, req.reference_date):
                continue
            order_idx += 1
            active_order_count += 1
            matched = idx.resolve_record_drugs(meta)
            if matched:
                mapped_active_order_count += 1
            for drug in matched:
                did = drug["id"]
                if did not in seen_active_ids:
                    seen_active_ids.add(did)
                    active_ids.append(did)
                order_sources.setdefault(did, []).append(order_idx)
                for code in drug.get("atc") or ():
                    if code:
                        atc_codes.add(str(code).upper())
        elif rtype == "allergy":
            for key in ("allergen_name", "allergen_non_coded", "concept_name"):
                val = str(meta.get(key) or "").strip()
                if val and val not in allergy_tokens:
                    allergy_tokens.append(val)
        elif rtype == "condition":
            for key in ("concept_name", "non_coded"):
                val = str(meta.get(key) or "").strip()
                if val and val not in condition_tokens:
                    condition_tokens.append(val)

    question_drugs = idx.scan_text_drugs(req.question)
    extra_ids = [d["id"] for d in question_drugs if d["id"] not in seen_active_ids]
    findings = _compute_ddis(
        idx,
        active_ids=active_ids,
        order_sources=order_sources,
        extra_ids=extra_ids,
        allergy_tokens=allergy_tokens,
    )

    active_names = [idx.drugs_by_id[did]["name"] for did in active_ids]
    facts_lines: List[str] = []
    if active_names or findings:
        facts_lines.append("Medication safety context (DDInter 2.0 / RxNorm / CIEL):")
        if active_names:
            facts_lines.append(f"- Resolved active ingredients: {', '.join(active_names)}")
        if extra_ids:
            q_names = [idx.drugs_by_id[did]["name"] for did in extra_ids]
            facts_lines.append(f"- Question-referenced drugs: {', '.join(q_names)}")
        for item in findings[:5]:
            facts_lines.append(
                f"- {item.get('severity', 'Safety')} {item['type']}: {item['detail']}"
            )
    medication_safety_facts = "\n".join(facts_lines)

    drug_references: List[Dict[str, Any]] = []
    for item in findings[:3]:
        pid = item.get("pair_id") or item["drug"].lower().replace(" ", "-")
        drug_references.append(
            {
                "sourceId": f"ddi-reference:{pid}",
                "drug": item["drug"],
                "title": f"{item['type'].capitalize()}: {item['drug']} ({item.get('severity', 'Clinical alert')})",
                "text": item["detail"],
                "source": "DDInter 2.0 (via openmrs-ddi-knowledge-base)",
                "provenance": dict(idx.package.get("provenance") or {}),
            }
        )

    mapping_complete = active_order_count == 0 or mapped_active_order_count == active_order_count
    patient_context = {
        "age_years": age_years,
        "active_drugs": active_names,
        "active_drug_ids": active_ids,
        "active_order_sources": order_sources,
        "active_drug_atc_codes": sorted(atc_codes),
        "allergy_tokens": allergy_tokens,
        "condition_tokens": condition_tokens,
        "mapping_complete": mapping_complete,
        "exposure_complete": bool(req.exposure_complete),
        "active_order_count": active_order_count,
        "mapped_active_order_count": mapped_active_order_count,
        "identity_confidence": _identity_confidence(
            active_order_count, mapped_active_order_count, bool(req.exposure_complete)
        ),
    }

    return {
        "patient_context": patient_context,
        "drug_references": drug_references,
        "proactive_warnings": [
            {"type": w["type"], "drug": w["drug"], "detail": w["detail"]} for w in findings[:5]
        ],
        "medication_safety_facts": medication_safety_facts,
        "package": idx.package,
    }


@app.post("/v1/check")
def check(req: CheckRequest) -> Dict[str, Any]:
    idx = get_index()
    ctx = req.patient_context or {}
    active_ids = [str(x) for x in (ctx.get("active_drug_ids") or ()) if str(x) in idx.drugs_by_id]
    active_set = set(active_ids)
    order_sources = ctx.get("active_order_sources") or {}
    allergy_tokens = list(ctx.get("allergy_tokens") or ())
    active_order_count = int(ctx.get("active_order_count") or 0)
    mapped_active_order_count = int(ctx.get("mapped_active_order_count") or 0)
    mapping_complete = bool(ctx.get("mapping_complete", active_order_count == mapped_active_order_count))
    exposure_complete = bool(ctx.get("exposure_complete"))
    identity_confidence = str(
        ctx.get("identity_confidence")
        or _identity_confidence(active_order_count, mapped_active_order_count, exposure_complete)
    )

    mentioned = idx.scan_text_drugs(f"{req.question}\n{req.answer_text}")
    extra_ids = [d["id"] for d in mentioned if d["id"] not in active_set]
    findings = _compute_ddis(
        idx,
        active_ids=active_ids,
        order_sources=order_sources,
        extra_ids=extra_ids,
        allergy_tokens=allergy_tokens,
    )

    issues: List[str] = []
    if mapped_active_order_count < active_order_count:
        issues.append("unmapped_active_medication")

    return {
        "schema_version": "drug_safety.v1",
        "status": "limited" if issues else "checked",
        "warnings": [
            {"type": w["type"], "drug": w["drug"], "detail": w["detail"]} for w in findings[:8]
        ],
        "package": idx.package,
        "coverage": {
            "mapping_complete": mapping_complete,
            "exposure_complete": exposure_complete,
            "execution_complete": True,
            "active_order_count": active_order_count,
            "mapped_active_order_count": mapped_active_order_count,
        },
        "identity_confidence": identity_confidence,
        "issues": issues,
    }
