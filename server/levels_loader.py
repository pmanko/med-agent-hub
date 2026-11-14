"""Validated hub profiles and low-level stage legs.

Configured profiles declare topology, ordered stages, role models, prompts, and
policies directly. Dynamic low-level ids compile to the same immutable shape but
are intentionally absent from product discovery.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional, Tuple

import rfc8785
import yaml

from .prompt_loader import load_prompt

_PATH = Path(__file__).parent / "levels.yaml"
_PROMPTS = Path(__file__).parent / "prompts"
_TEMPORAL_GATE_MODES = {"off", "warn", "enforce"}
_CLINICAL_TOPOLOGIES = {"single", "team", "leg"}
_WORKFLOWS = {"clinical_answer", "catalyst_query", "wahs"}
_WAHS_TOPOLOGIES = {"single", "xcheck"}
_WAHS_ALLOWED_STAGES = {"context", "answer", "review", "gate"}
_WAHS_ALLOWED_ROLES = {"answer", "review"}
_WAHS_PROVIDERS = {"local", "openrouter"}
_OUTPUT_MODES = {"bare", "combined", "product", "review", "indepth"}
_ANSWER_CONTRACTS = {"caller", "chart_answer"}
# What happens with a model reviewer's verdict. Only labelling is built; applying the reviewer's fix
# or sending the writer back to redo the answer stay reserved until a judged run earns them.
_REVIEW_ACTIONS = {"label"}
_RESERVED_REVIEW_ACTIONS = {"apply", "redo"}
# How cited records are checked against their sentences: a separate entailment call, a verdict
# folded into the review, or not at all.
_GROUNDING_POLICIES = {"separate", "folded", "off"}
# What the confidence sections carry: a graded level, or only the checks' findings.
_CONFIDENCE_POLICIES = {"levels", "findings"}
_ALLOWED_STAGES = {
    "context",
    "gather",
    "answer",
    "gate",
    "resolve_refs",
    "review",
    "final_resolve_refs",
    "agree",
    "ground_verdicts",
    "indepth",
    "indepth_gate",
}
_QUERY_REQUIRED_ROLE = "query_generate"
_QUERY_OPTIONAL_ROLE = "query_review"
_QUERY_ALLOWED_ROLES = {_QUERY_REQUIRED_ROLE, _QUERY_OPTIONAL_ROLE}
_QUERY_ALLOWED_STAGES = {
    "context",
    "query_generate",
    "query_lint",
    "query_review",
    "query_finalize",
}

StagePlan = Tuple[str, ...]


class ModelNotFoundError(KeyError):
    code = "model_not_found"

    def __init__(self, model_id: str, configured: List[str]) -> None:
        self.model_id = model_id
        self.configured = tuple(configured)
        super().__init__(
            f"model {model_id!r} is not a configured profile or valid low-level leg"
        )


@dataclass(frozen=True)
class Profile:
    id: str
    label: str
    topology: str
    stages: StagePlan
    models: Mapping[str, str]
    prompts: Mapping[str, str]
    policies: Mapping[str, Any]
    workflow: str = "clinical_answer"
    supplemental_sources: Tuple[str, ...] = ()
    knobs: Mapping[str, Any] = field(default_factory=dict)
    visibility: str = "experimental"
    default: bool = False
    selection_priority: int = 1000
    context_window: int = 0
    reserved_output_tokens: int = 0
    exact_tokenizer: bool = False
    low_level_leg: bool = False
    output_contracts: Tuple[str, ...] = ()

    @property
    def staged(self) -> bool:
        return self.output_mode == "product"

    @property
    def validation(self) -> bool:
        return "review" in self.stages

    @property
    def output_mode(self) -> str:
        return str(self.policies.get("output", "bare"))


def _split_dynamic_prompt_profile(
    profile_id: str, prefix: str
) -> tuple[str, str | None, str | None, float | None]:
    rest = profile_id[len(prefix) :]
    writer_prompt, *options = rest.split("~")
    gate: str | None = None
    temperature: float | None = None
    for option in options:
        if not option:
            raise ModelNotFoundError(profile_id, profile_ids())
        if option in _TEMPORAL_GATE_MODES and gate is None:
            gate = option
            continue
        if option.startswith("temp") and option[4:] and temperature is None:
            try:
                temperature = float(option[4:])
            except ValueError as exc:
                raise ModelNotFoundError(profile_id, profile_ids()) from exc
            if temperature < 0:
                raise ModelNotFoundError(profile_id, profile_ids())
            continue
        raise ModelNotFoundError(profile_id, profile_ids())
    writer, separator, prompt = writer_prompt.partition("@")
    if not writer or (separator and not prompt):
        raise ModelNotFoundError(profile_id, profile_ids())
    return writer, prompt or None, gate, temperature


def _dynamic_profile(profile_id: str) -> Optional[Profile]:
    definitions = (
        (
            "answer-review:",
            "review",
            ("context", "review"),
            "review",
            "validation-label",
            "enforce",
        ),
        (
            "indepth-only:",
            "indepth",
            ("context", "indepth"),
            "indepth",
            "synthesis-indepth",
            "off",
        ),
        (
            "answer:",
            "answer",
            ("context", "answer", "gate"),
            "bare",
            "synthesis-chartsearchai",
            "off",
        ),
    )
    for prefix, role, stages, output, default_prompt, default_gate in definitions:
        if not profile_id.startswith(prefix):
            continue
        writer, prompt, gate, temperature = _split_dynamic_prompt_profile(
            profile_id, prefix
        )
        models = {role: writer}
        prompts = {role: prompt or default_prompt}
        knobs: Dict[str, Any] = {}
        if temperature is not None:
            knobs[role] = {"temperature": temperature}
        profile = Profile(
            id=profile_id,
            label=profile_id,
            topology="leg",
            stages=stages,
            models=models,
            prompts=prompts,
            policies={
                "temporal_gate": gate or default_gate,
                "temporal_render": "full",
                "output": output,
                "drug_safety": False,
                **(
                    {"review": {"action": "label", "grounding": "off"}}
                    if "review" in stages
                    else {}
                ),
            },
            knobs=knobs,
            visibility="internal",
            low_level_leg=True,
        )
        return compile_profile(profile)
    return None


_DOCUMENT_CACHE: Dict[Tuple[str, int, int], Dict[str, Any]] = {}


def _load_document() -> Dict[str, Any]:
    try:
        st = _PATH.stat()
        cache_key = (str(_PATH), st.st_mtime_ns, st.st_size)
        cached = _DOCUMENT_CACHE.get(cache_key)
        if cached is not None:
            return cached
        document = yaml.safe_load(_PATH.read_text(encoding="utf-8")) or {}
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"profiles file not found at {_PATH}") from exc
    if not isinstance(document, dict):
        raise ValueError(f"{_PATH} must contain an object")
    _DOCUMENT_CACHE.clear()
    _DOCUMENT_CACHE[cache_key] = document
    return document


def _load_raw() -> Dict[str, dict]:
    document = _load_document()
    profiles = document.get("profiles")
    if not isinstance(profiles, dict) or not profiles:
        raise ValueError(f"{_PATH} must contain a non-empty top-level profiles mapping")
    return profiles


def catalyst_query_profile_ids() -> List[str]:
    return [
        profile_id
        for profile_id, spec in _load_raw().items()
        if str((spec or {}).get("workflow") or "clinical_answer") == "catalyst_query"
    ]


def get_catalyst_query_profile(profile_id: str) -> Profile:
    raw = _load_raw()
    if profile_id not in catalyst_query_profile_ids():
        raise ModelNotFoundError(profile_id, catalyst_query_profile_ids())
    return _from_spec(profile_id, raw[profile_id] or {})


def validate_catalyst_query_profiles() -> Tuple[Profile, ...]:
    """Validate every configured Catalyst query profile at Hub startup."""
    return tuple(
        get_catalyst_query_profile(profile_id)
        for profile_id in catalyst_query_profile_ids()
    )


def wahs_profile_ids() -> List[str]:
    raw = _load_raw()
    ids = [
        profile_id
        for profile_id, spec in raw.items()
        if str((spec or {}).get("workflow") or "clinical_answer") == "wahs"
    ]
    defaults = [
        profile_id for profile_id in ids if bool((raw[profile_id] or {}).get("default"))
    ]
    if len(defaults) > 1:
        raise ValueError(
            f"levels.yaml may define at most one default wahs profile; found {defaults}"
        )
    return ids


def is_wahs_profile_id(profile_id: str) -> bool:
    if profile_id in wahs_profile_ids():
        return True
    if profile_id.endswith("-direct-kb"):
        base_kb = profile_id[: -len("-direct-kb")] + "-kb"
        if base_kb in wahs_profile_ids():
            return True
    if profile_id.startswith(("wahs-kb:", "wahs-raw:", "wahs-direct-kb:")):
        _, _, model_id = profile_id.partition(":")
        return bool(model_id.strip())
    return False


OPENROUTER_MODEL_MAP: Dict[str, str] = {
    "gemma-e2b": "google/gemma-3-4b-it",
    "gemma-e4b": "google/gemma-3-4b-it",
    "gemma-e4b-q8": "google/gemma-3-4b-it",
    "gemma-4-12b": "google/gemma-3-12b-it",
    "gemma-4-12b-q4": "google/gemma-3-12b-it",
    "gemma-26b": "google/gemma-3-27b-it",
    "gemma-31b": "google/gemma-3-27b-it",
    "medgemma-1.5-4b-q8": "google/gemma-3-27b-it",
    "medgemma-27b": "google/gemma-3-27b-it",
    "qwen3-4b": "qwen/qwen-2.5-72b-instruct",
    "qwen3.5-9b": "qwen/qwen-2.5-72b-instruct",
    "qwen2.5-14b": "qwen/qwen-2.5-72b-instruct",
    "qwen3-14b": "qwen/qwen-2.5-72b-instruct",
    "qwen3.6-35b": "qwen/qwen-2.5-72b-instruct",
    "llama-3.2-3b": "meta-llama/llama-3.2-3b-instruct",
    "llama-3.1-8b": "meta-llama/llama-3.3-70b-instruct",
    "hermes3-8b": "nousresearch/hermes-3-llama-3.1-8b",
    "mistral-nemo-12b": "mistralai/mistral-nemo",
    "granite-3.3-8b": "ibm-granite/granite-3.3-8b-instruct",
    "lfm2-8b-a1b": "liquid/lfm-7b",
}


WAHS_RECOMMENDED_LIMIT_URL = "https://reports.openclinai.org/2026-11-03-wahs-huge-context-and-hub-graph-eval/index.html"

WAHS_MODEL_CONTEXT_SPECS: Dict[str, Tuple[int, int]] = {
    "gemma-26b": (262144, 32768),
    "gemma-31b": (262144, 32768),
    "qwen3.6-35b": (262144, 32768),
    "gemma-4-12b": (262144, 24576),
    "gemma-4-12b-q4": (262144, 24576),
    "qwen3-14b": (262144, 24576),
    "medgemma-27b": (131072, 24576),
    "qwen2.5-14b": (131072, 24576),
    "mistral-nemo-12b": (131072, 24576),
    "gemma-e4b": (131072, 16384),
    "gemma-e4b-q8": (131072, 16384),
    "qwen3.5-9b": (131072, 16384),
    "llama-3.2-3b": (131072, 12288),
    "llama-3.1-8b": (131072, 16384),
    "hermes3-8b": (131072, 16384),
    "granite-3.3-8b": (131072, 16384),
    "lfm2-8b-a1b": (65536, 16384),
    "gemma-e2b": (131072, 12288),
    "medgemma-1.5-4b-q8": (131072, 12288),
    "qwen3-4b": (131072, 12288),
    "google/gemini-2.5-flash": (1048576, 65536),
    "google/gemma-3-27b-it": (131072, 32768),
    "google/gemma-3-12b-it": (131072, 24576),
    "qwen/qwen-2.5-72b-instruct": (131072, 32768),
    "meta-llama/llama-3.3-70b-instruct": (131072, 32768),
    "mistralai/mistral-nemo": (131072, 24576),
}


def resolve_wahs_model_context_spec(model_id: str, fallback_window: int = 0) -> Tuple[int, int]:
    clean = str(model_id or "").strip()
    if clean in WAHS_MODEL_CONTEXT_SPECS:
        return WAHS_MODEL_CONTEXT_SPECS[clean]
    return (131072, fallback_window or 24576)


def parse_model_map(raw: str) -> Dict[str, str]:
    """Parse ``LLM_MODEL_MAP`` as a JSON object or comma-separated ``src=dst`` pairs."""
    text = str(raw or "").strip()
    if not text:
        return {}
    if text.startswith("{"):
        parsed = json.loads(text)
        if not isinstance(parsed, Mapping):
            raise ValueError("LLM_MODEL_MAP JSON must be an object mapping model ids")
        return {
            str(k).strip(): str(v).strip()
            for k, v in parsed.items()
            if str(k).strip() and str(v).strip()
        }
    mapping: Dict[str, str] = {}
    for item in text.split(","):
        part = item.strip()
        if not part or "=" not in part:
            continue
        key, value = part.split("=", 1)
        if key.strip() and value.strip():
            mapping[key.strip()] = value.strip()
    return mapping


def resolve_openrouter_model(model_id: str) -> str:
    clean = str(model_id or "").strip()
    if "/" in clean:
        return clean
    return OPENROUTER_MODEL_MAP.get(clean, clean)


def resolve_backend_model(
    model_id: str,
    *,
    provider: str = "",
    model_map_raw: str = "",
) -> str:
    """Resolve a profile model alias to the configured upstream backend model id."""
    clean = str(model_id or "").strip()
    explicit = parse_model_map(model_map_raw)
    if clean in explicit:
        return explicit[clean]
    if (provider or "").strip().lower() == "openrouter":
        return resolve_openrouter_model(clean)
    return clean


def _infer_model_family(model_id: str) -> str:
    low = model_id.lower()
    if "medgemma" in low:
        return "medgemma"
    if "gemini" in low:
        return "gemini"
    if "gemma-3" in low or "gemma3" in low:
        return "gemma-3"
    if "gemma" in low:
        return "gemma-4"
    if "qwen" in low:
        return "qwen"
    if "llama" in low or "hermes" in low:
        return "llama"
    if "mistral" in low or "nemo" in low:
        return "mistral"
    if "granite" in low:
        return "granite"
    if "lfm" in low or "liquid" in low:
        return "lfm2"
    if "claude" in low:
        return "claude"
    return low.split("-", 1)[0].split("/", 1)[0]


def get_wahs_profile(profile_id: str) -> Profile:
    raw = _load_raw()
    if profile_id in wahs_profile_ids():
        return _from_spec(profile_id, raw[profile_id] or {})
    if profile_id.endswith("-direct-kb"):
        base_kb = profile_id[: -len("-direct-kb")] + "-kb"
        if base_kb in wahs_profile_ids():
            spec = json.loads(json.dumps(raw[base_kb] or {}))
            spec["label"] = f"{spec.get('label', base_kb)} (Direct KB Only — No Hub Graph)"
            pol = dict(spec.get("policies") or {})
            pol["include_kb_findings"] = True
            pol["include_hub_enrichment"] = False
            pol["overflow_lookup_tools"] = False
            spec["policies"] = pol
            return _from_spec(profile_id, spec)
    if profile_id.startswith(("wahs-kb:", "wahs-raw:", "wahs-direct-kb:")):
        prefix, _, raw_model = profile_id.partition(":")
        model_id = raw_model.strip()
        if not model_id:
            raise ModelNotFoundError(profile_id, wahs_profile_ids())
        include_kb = prefix in {"wahs-kb", "wahs-direct-kb"}
        include_hub_enrichment = prefix == "wahs-kb"
        provider = "openrouter" if "/" in model_id else "local"
        family = _infer_model_family(model_id)
        if prefix == "wahs-kb":
            mode_label = "+ 4-Layer Clinical KB + Med Agent Hub"
        elif prefix == "wahs-direct-kb":
            mode_label = "Direct Model + 4-Layer Clinical KB"
        else:
            mode_label = "Raw Condensed Chart Only"
        return compile_profile(
            Profile(
                id=profile_id,
                label=f"{model_id} — {mode_label} (Dynamic)",
                topology="single",
                stages=("context", "answer", "gate"),
                models={"answer": model_id},
                prompts={
                    "answer": "wahs-kb-guided" if include_kb else "wahs-raw-unhinted"
                },
                policies={
                    "provider": provider,
                    "model_family": family,
                    "include_kb_findings": include_kb,
                    "include_hub_enrichment": include_hub_enrichment,
                    "overflow_lookup_tools": include_hub_enrichment,
                    "citation_grounding": "enforce",
                },
                workflow="wahs",
                knobs={"answer": {"temperature": 0.1, "dry": 0.0, "maxTokens": 4096}},
                visibility="evaluation",
                context_window=32768 if provider == "openrouter" else 24576,
                reserved_output_tokens=2048,
                exact_tokenizer=(provider == "local"),
            )
        )
    raise ModelNotFoundError(profile_id, wahs_profile_ids())


def validate_wahs_profiles() -> Tuple[Profile, ...]:
    """Validate every configured WAHS profile at Hub startup."""
    return tuple(get_wahs_profile(profile_id) for profile_id in wahs_profile_ids())


def wahs_profile_metadata(
    profile: Profile,
    *,
    backend_models: set[str] | None,
    openrouter_configured: bool = False,
    effective_default: bool = False,
) -> Dict[str, Any]:
    from . import model_provisioner

    provider = str(profile.policies.get("provider") or "local").lower()
    model_provisioned = True
    auto_downloadable = False
    if provider == "openrouter":
        available = bool(openrouter_configured)
        unavailable_reasons = (
            [] if available else ["openrouter_api_key_not_configured"]
        )
    else:
        unavailable_reasons = (
            ["model_backend_unreachable"]
            if backend_models is None
            else [
                f"model_not_advertised:{model}"
                for model in sorted(set(profile.models.values()) - backend_models)
            ]
        )
        for model in sorted(set(profile.models.values())):
            present = model_provisioner.is_model_file_present(model)
            pinned = model_provisioner.is_model_pinned(model)
            if present is False:
                model_provisioned = False
                if pinned:
                    auto_downloadable = True
                else:
                    unavailable_reasons.append(f"model_weights_missing:{model}")
        if openrouter_configured and unavailable_reasons:
            provider = "openrouter"
            unavailable_reasons = []
        available = not unavailable_reasons

    system_prompts: Dict[str, str] = {}
    for role, p_name in sorted(profile.prompts.items()):
        system_prompts[str(role)] = load_prompt(str(p_name)).strip()

    ans_model = str(profile.models.get("answer") or "")
    family = str(
        profile.policies.get("model_family") or _infer_model_family(ans_model)
    )
    ctx_length, rec_limit = resolve_wahs_model_context_spec(
        ans_model, fallback_window=profile.context_window
    )
    return {
        "id": profile.id,
        "label": profile.label,
        "workflow": profile.workflow,
        "topology": profile.topology,
        "provider": provider,
        "openrouter_model": resolve_openrouter_model(ans_model),
        "model_family": family,
        "parameter_size": str(profile.policies.get("parameter_size") or ""),
        "quantization": str(profile.policies.get("quantization") or ""),
        "available": available,
        "model_provisioned": model_provisioned,
        "auto_downloadable": auto_downloadable,
        "default": bool(effective_default or profile.default),
        "selection_priority": profile.selection_priority,
        "visibility": profile.visibility,
        "stages": list(profile.stages),
        "required_models": sorted(set(profile.models.values())),
        "role_models": _jsonable(profile.models),
        "role_knobs": _jsonable(profile.knobs),
        "policies": _jsonable(profile.policies),
        "prompts": _jsonable(profile.prompts),
        "system_prompts": system_prompts,
        "profile_configuration_digest": _profile_configuration_digest(profile),
        "role_prompt_digests": _role_prompt_digests(profile),
        "context_window": profile.context_window or rec_limit,
        "context_length": ctx_length,
        "recommended_limit_tokens": rec_limit,
        "recommended_limit_url": WAHS_RECOMMENDED_LIMIT_URL,
        "exact_tokenizer": profile.exact_tokenizer,
        "unavailable_reasons": unavailable_reasons,
    }


def catalyst_query_profile_evidence(profile: Profile) -> Dict[str, Any]:
    """Credential-free evidence for the exact Hub profile execution contract."""
    model_classes = profile.policies.get("model_classes") or {}

    def role_evidence(public_role: str, role: str) -> Dict[str, Any]:
        prompt_name = str(profile.prompts[role])
        prompt_text = load_prompt(prompt_name)
        return {
            "role": public_role,
            "providerId": "med-agent-hub",
            "modelClass": str(
                model_classes.get(role) or str(profile.models[role]).split("-", 1)[0]
            ),
            "modelId": profile.models[role],
            "config": _jsonable(profile.knobs[role]),
            "systemPrompt": {
                "promptId": prompt_name,
                "version": "1",
                "promptRef": f"med-agent-hub:server/prompts/{prompt_name}.txt",
                "promptDigest": hashlib.sha256(prompt_text.encode("utf-8")).hexdigest(),
                "text": prompt_text,
            },
        }

    evidence: Dict[str, Any] = {
        "profileId": profile.id,
        "profileName": profile.label,
        "writer": role_evidence("writer", _QUERY_REQUIRED_ROLE),
    }
    if _QUERY_OPTIONAL_ROLE in profile.models:
        evidence["reviewer"] = role_evidence("reviewer", _QUERY_OPTIONAL_ROLE)
    compact = _jsonable(evidence)
    compact["writer"]["systemPrompt"].pop("text")
    if "reviewer" in compact:
        compact["reviewer"]["systemPrompt"].pop("text")
    evidence["profileDigest"] = hashlib.sha256(rfc8785.dumps(compact)).hexdigest()
    return evidence


def catalyst_query_profile_metadata(
    profile: Profile, *, backend_models: set[str] | None
) -> Dict[str, Any]:
    unavailable_reasons = (
        ["model_backend_unreachable"]
        if backend_models is None
        else [
            f"model_not_advertised:{model}"
            for model in sorted(set(profile.models.values()) - backend_models)
        ]
    )
    available = not unavailable_reasons
    return {
        "id": profile.id,
        "label": profile.label,
        "workflow": profile.workflow,
        "topology": profile.topology,
        "available": available,
        "required_models": sorted(set(profile.models.values())),
        "role_models": _jsonable(profile.models),
        "role_knobs": _jsonable(profile.knobs),
        "policies": _jsonable(profile.policies),
        "stages": list(profile.stages),
        "unavailable_reasons": unavailable_reasons,
        "capabilities": {"staged": False, "validation": True, "modelRouter": available},
        "outputContracts": list(profile.output_contracts),
        "revisionCapable": True,
        "profileEvidence": catalyst_query_profile_evidence(profile),
    }


def _from_spec(profile_id: str, spec: Mapping[str, Any]) -> Profile:
    context = spec.get("context") or {}
    supplemental_sources = context.get("supplemental_sources") or ()
    if isinstance(supplemental_sources, str):
        supplemental_sources = (supplemental_sources,)
    elif not isinstance(supplemental_sources, (list, tuple)):
        raise ValueError(
            f"profile {profile_id!r} context.supplemental_sources must be a list"
        )
    profile = Profile(
        id=profile_id,
        label=str(spec.get("label") or "").strip(),
        topology=str(spec.get("topology") or "").strip().lower(),
        stages=tuple(spec.get("stages") or ()),
        models=dict(spec.get("models") or {}),
        prompts=dict(spec.get("prompts") or {}),
        policies=dict(spec.get("policies") or {}),
        workflow=str(spec.get("workflow") or "clinical_answer").strip().lower(),
        supplemental_sources=tuple(supplemental_sources),
        knobs=dict(spec.get("knobs") or {}),
        visibility=str(spec.get("visibility") or "experimental"),
        default=bool(spec.get("default", False)),
        selection_priority=int(spec.get("selection_priority", 1000)),
        context_window=int(context.get("window") or 0),
        reserved_output_tokens=int(context.get("reserved_output_tokens") or 0),
        exact_tokenizer=bool(context.get("exact_tokenizer", False)),
        output_contracts=tuple(spec.get("outputContracts") or ()),
    )
    return compile_profile(profile)


def _compile_catalyst_query_profile(profile: Profile) -> Profile:
    if profile.topology != "caller":
        raise ValueError(
            f"Catalyst query profile {profile.id!r} topology must be caller"
        )
    if not profile.stages or profile.stages[0] != "context":
        raise ValueError(
            f"Catalyst query profile {profile.id!r} must start with context"
        )
    unknown = [stage for stage in profile.stages if stage not in _QUERY_ALLOWED_STAGES]
    if unknown:
        raise ValueError(
            f"Catalyst query profile {profile.id!r} has unknown stages {unknown}"
        )
    expected_stages = (
        "context",
        "query_generate",
        "query_lint",
        *((("query_review",)) if _QUERY_OPTIONAL_ROLE in profile.models else ()),
        "query_finalize",
    )
    if profile.stages != expected_stages:
        raise ValueError(
            f"Catalyst query profile {profile.id!r} stages must be {expected_stages}"
        )
    if (
        _QUERY_REQUIRED_ROLE not in profile.models
        or set(profile.models) - _QUERY_ALLOWED_ROLES
    ):
        raise ValueError(
            f"Catalyst query profile {profile.id!r} must define only query_generate and optional query_review"
        )
    has_review = _QUERY_OPTIONAL_ROLE in profile.models
    if has_review != ("query_review" in profile.stages):
        raise ValueError(
            f"Catalyst query profile {profile.id!r} reviewer role and stage must agree"
        )
    for role, model in profile.models.items():
        if not isinstance(model, str) or not model.strip():
            raise ValueError(
                f"Catalyst query profile {profile.id!r} has invalid {role} model"
            )
        prompt = profile.prompts.get(role)
        if not isinstance(prompt, str) or not prompt.strip():
            raise ValueError(
                f"Catalyst query profile {profile.id!r} has no {role} prompt"
            )
        load_prompt(prompt)
        role_knobs = profile.knobs.get(role)
        if not isinstance(role_knobs, Mapping) or any(
            key not in role_knobs for key in ("temperature", "dry", "maxTokens")
        ):
            raise ValueError(
                f"Catalyst query profile {profile.id!r} needs temperature, dry, and maxTokens for {role}"
            )
    if profile.default:
        raise ValueError(
            f"Catalyst query profile {profile.id!r} cannot replace the clinical default"
        )
    if "catalyst.query.v1" not in profile.output_contracts:
        raise ValueError(
            f"Catalyst query profile {profile.id!r} must declare catalyst.query.v1"
        )
    return Profile(
        id=profile.id,
        label=profile.label,
        topology=profile.topology,
        stages=tuple(profile.stages),
        models=_freeze_mapping(profile.models),
        prompts=_freeze_mapping(profile.prompts),
        policies=_freeze_mapping(profile.policies),
        workflow=profile.workflow,
        supplemental_sources=(),
        knobs=_freeze_mapping(profile.knobs),
        visibility=profile.visibility,
        default=False,
        selection_priority=profile.selection_priority,
        output_contracts=tuple(profile.output_contracts),
    )


def _review_policy(profile: Profile) -> Optional[str]:
    """Validate the profile's ``review`` policy block and return its grounding choice.

    A profile that reviews or grounds its answer declares ``review: {action, grounding}``.
    """
    confidence = str(profile.policies.get("confidence") or "levels")
    if confidence not in _CONFIDENCE_POLICIES:
        raise ValueError(f"profile {profile.id!r} has invalid confidence policy {confidence!r}")
    checks = {"review", "ground_verdicts"} & set(profile.stages)
    review = profile.policies.get("review")
    if not checks:
        if review is not None:
            raise ValueError(
                f"profile {profile.id!r} declares a review policy but neither reviews nor grounds"
            )
        return None
    if not isinstance(review, Mapping):
        raise ValueError(
            f"profile {profile.id!r} must declare policies.review with action and grounding"
        )
    unknown = sorted(set(review) - {"action", "grounding", "confidence"})
    if unknown:
        raise ValueError(f"profile {profile.id!r} review policy has unknown keys {unknown}")
    action = str(review.get("action") or "")
    if action in _RESERVED_REVIEW_ACTIONS:
        raise ValueError(
            f"profile {profile.id!r} review action {action!r} is reserved and not built; use label"
        )
    if action not in _REVIEW_ACTIONS:
        raise ValueError(f"profile {profile.id!r} has invalid review action {action!r}")
    confidence = str(review.get("confidence") or "levels")
    if confidence not in _CONFIDENCE_POLICIES:
        raise ValueError(
            f"profile {profile.id!r} has invalid review confidence {confidence!r}"
        )
    grounding = str(review.get("grounding") or "")
    if grounding not in _GROUNDING_POLICIES:
        raise ValueError(
            f"profile {profile.id!r} has invalid review grounding {grounding!r}"
        )
    if grounding != "off" and "ground_verdicts" not in profile.stages:
        raise ValueError(
            f"profile {profile.id!r} grounding {grounding} requires the ground_verdicts stage"
        )
    if grounding == "folded" and "review" not in profile.stages:
        raise ValueError(
            f"profile {profile.id!r} grounding folded requires the review stage"
        )
    return grounding


def _compile_wahs_profile(profile: Profile) -> Profile:
    if profile.topology not in _WAHS_TOPOLOGIES:
        raise ValueError(
            f"WAHS profile {profile.id!r} topology must be one of {_WAHS_TOPOLOGIES}"
        )
    if not profile.stages or profile.stages[0] != "context":
        raise ValueError(f"WAHS profile {profile.id!r} must start with context")
    unknown = [stage for stage in profile.stages if stage not in _WAHS_ALLOWED_STAGES]
    if unknown:
        raise ValueError(f"WAHS profile {profile.id!r} has unknown stages {unknown}")
    if "answer" not in profile.models or set(profile.models) - _WAHS_ALLOWED_ROLES:
        raise ValueError(
            f"WAHS profile {profile.id!r} must define answer and optional review roles"
        )
    has_review = "review" in profile.models
    if has_review != ("review" in profile.stages):
        raise ValueError(
            f"WAHS profile {profile.id!r} review role and stage must agree"
        )
    provider = str(profile.policies.get("provider") or "local").lower()
    if provider not in _WAHS_PROVIDERS:
        raise ValueError(
            f"WAHS profile {profile.id!r} has invalid provider {provider!r}"
        )
    for role, model in profile.models.items():
        if not isinstance(model, str) or not model.strip():
            raise ValueError(f"WAHS profile {profile.id!r} has invalid {role} model")
        prompt = profile.prompts.get(role)
        if not isinstance(prompt, str) or not prompt.strip():
            raise ValueError(f"WAHS profile {profile.id!r} has no {role} prompt")
        load_prompt(prompt)
    return Profile(
        id=profile.id,
        label=profile.label,
        topology=profile.topology,
        stages=tuple(profile.stages),
        models=_freeze_mapping(profile.models),
        prompts=_freeze_mapping(profile.prompts),
        policies=_freeze_mapping(profile.policies),
        workflow=profile.workflow,
        supplemental_sources=tuple(profile.supplemental_sources),
        knobs=_freeze_mapping(profile.knobs),
        visibility=profile.visibility,
        default=profile.default,
        selection_priority=profile.selection_priority,
        context_window=profile.context_window,
        reserved_output_tokens=profile.reserved_output_tokens,
        exact_tokenizer=profile.exact_tokenizer,
        output_contracts=tuple(profile.output_contracts),
    )


def compile_profile(profile: Profile) -> Profile:
    if not profile.id or not profile.label:
        raise ValueError("profile id and label are required")
    if profile.workflow not in _WORKFLOWS:
        raise ValueError(
            f"profile {profile.id!r} has invalid workflow {profile.workflow!r}"
        )
    if profile.workflow == "catalyst_query":
        return _compile_catalyst_query_profile(profile)
    if profile.workflow == "wahs":
        return _compile_wahs_profile(profile)
    if profile.topology not in _CLINICAL_TOPOLOGIES:
        raise ValueError(
            f"profile {profile.id!r} has invalid topology {profile.topology!r}"
        )
    supplemental_sources = tuple(
        str(source).strip() for source in profile.supplemental_sources
    )
    if any(not source for source in supplemental_sources):
        raise ValueError(
            f"profile {profile.id!r} has an empty supplemental context source"
        )
    if len(set(supplemental_sources)) != len(supplemental_sources):
        raise ValueError(
            f"profile {profile.id!r} repeats a supplemental context source"
        )
    if not profile.stages or profile.stages[0] != "context":
        raise ValueError(f"profile {profile.id!r} must start with context")
    unknown = [stage for stage in profile.stages if stage not in _ALLOWED_STAGES]
    if unknown:
        raise ValueError(f"profile {profile.id!r} has unknown stages {unknown}")
    for stage in _ALLOWED_STAGES - {"gate"}:
        if profile.stages.count(stage) > 1:
            raise ValueError(f"profile {profile.id!r} repeats stage {stage!r}")
    if profile.topology == "team" and "gather" not in profile.stages:
        raise ValueError(f"team profile {profile.id!r} must include gather")
    if profile.topology == "single" and "orchestrator" in profile.models:
        raise ValueError(
            f"single profile {profile.id!r} cannot declare an orchestrator"
        )
    if "gather" in profile.stages and "orchestrator" not in profile.models:
        raise ValueError(
            f"profile {profile.id!r} gather requires an orchestrator model"
        )
    grounding = _review_policy(profile)
    for stage, role in (
        ("answer", "answer"),
        ("review", "review"),
        ("agree", "second"),
        ("ground_verdicts" if grounding == "separate" else "", "grounding"),
        ("indepth_gate" if grounding == "separate" else "", "grounding"),
        ("indepth", "indepth"),
    ):
        if stage in profile.stages and role not in profile.models:
            raise ValueError(
                f"profile {profile.id!r} stage {stage} requires model role {role}"
            )
    if profile.output_mode not in _OUTPUT_MODES:
        raise ValueError(
            f"profile {profile.id!r} has invalid output mode {profile.output_mode!r}"
        )
    answer_contract = str(
        profile.policies.get(
            "answer_contract",
            "chart_answer" if profile.output_mode == "product" else "caller",
        )
    )
    if answer_contract not in _ANSWER_CONTRACTS:
        raise ValueError(
            f"profile {profile.id!r} has invalid answer contract {answer_contract!r}"
        )
    if profile.output_mode == "product" and answer_contract != "chart_answer":
        raise ValueError(
            f"product profile {profile.id!r} must use the chart_answer contract"
        )

    stages = profile.stages
    if "answer" in stages:
        answer = stages.index("answer")
        if "gather" in stages and stages.index("gather") > answer:
            raise ValueError(f"profile {profile.id!r} gather must run before answer")
        if answer + 1 >= len(stages) or stages[answer + 1] != "gate":
            raise ValueError(f"profile {profile.id!r} answer must be followed by gate")
    if (
        "review" in stages
        and profile.output_mode != "review"
        and "gate" not in stages[stages.index("review") + 1 :]
    ):
        raise ValueError(f"profile {profile.id!r} review must be followed by gate")
    if (
        "review" in stages
        and profile.output_mode != "review"
        and stages[stages.index("review") + 1] != "gate"
    ):
        raise ValueError(
            f"profile {profile.id!r} review must be immediately followed by gate"
        )
    if "resolve_refs" in stages:
        resolve = stages.index("resolve_refs")
        if "answer" not in stages or resolve < stages.index("answer"):
            raise ValueError(f"profile {profile.id!r} resolve_refs must follow answer")
        if "review" in stages and resolve > stages.index("review"):
            raise ValueError(f"profile {profile.id!r} resolve_refs must precede review")
    if "ground_verdicts" in stages:
        ground = stages.index("ground_verdicts")
        if (
            "final_resolve_refs" not in stages
            or stages.index("final_resolve_refs") > ground
        ):
            raise ValueError(
                f"profile {profile.id!r} ground_verdicts requires prior final_resolve_refs"
            )
        if "review" in stages and stages.index("review") > ground:
            raise ValueError(
                f"profile {profile.id!r} ground_verdicts must run after review"
            )
    if "final_resolve_refs" in stages:
        final_resolve = stages.index("final_resolve_refs")
        if "review" in stages and final_resolve < stages.index("review"):
            raise ValueError(
                f"profile {profile.id!r} final_resolve_refs must run after review"
            )
        if "gate" in stages and final_resolve < max(
            index for index, stage in enumerate(stages) if stage == "gate"
        ):
            raise ValueError(
                f"profile {profile.id!r} final_resolve_refs must run after the final gate"
            )
    if "indepth_gate" in stages and (
        "indepth" not in stages
        or stages.index("indepth_gate") < stages.index("indepth")
    ):
        raise ValueError(f"profile {profile.id!r} indepth_gate must follow indepth")
    if "agree" in stages:
        agree = stages.index("agree")
        if (
            "final_resolve_refs" not in stages
            or stages.index("final_resolve_refs") > agree
        ):
            raise ValueError(
                f"profile {profile.id!r} agree must run after final_resolve_refs"
            )
        if "indepth" in stages and agree > stages.index("indepth"):
            raise ValueError(
                f"profile {profile.id!r} agree must run before indepth"
            )
    if profile.output_mode == "product":
        # In-Depth is optional for a product profile (a fast checked answer), but never
        # without its gate; every answer check stays mandatory.
        required = {
            "answer",
            "gate",
            "resolve_refs",
            "final_resolve_refs",
        }
        # A profile that checks citations (a review policy) grounds them after the final
        # citation resolution; one without any citation check carries no grounding stage.
        if profile.policies.get("review") is not None:
            required.add("ground_verdicts")
        if "indepth" in stages:
            required.add("indepth_gate")
        missing = sorted(required - set(stages))
        if missing:
            raise ValueError(f"product profile {profile.id!r} lacks stages {missing}")
        if (
            "ground_verdicts" in stages
            and stages.index("final_resolve_refs") >= stages.index("ground_verdicts")
        ) or (
            "indepth" in stages
            and not (
                (
                    stages.index("ground_verdicts")
                    if "ground_verdicts" in stages
                    else stages.index("final_resolve_refs")
                )
                < stages.index("indepth")
                < stages.index("indepth_gate")
            )
        ):
            raise ValueError(
                f"product profile {profile.id!r} must ground before gated In-Depth"
            )
    temporal_mode = str(profile.policies.get("temporal_gate", "off")).lower()
    if temporal_mode not in _TEMPORAL_GATE_MODES:
        raise ValueError(
            f"profile {profile.id!r} has invalid temporal gate {temporal_mode!r}"
        )
    if profile.output_mode == "product":
        if temporal_mode != "enforce":
            raise ValueError(
                f"product-envelope profile {profile.id!r} must enforce temporal checks"
            )
        if (
            not profile.exact_tokenizer
            or profile.context_window <= profile.reserved_output_tokens
        ):
            raise ValueError(
                f"product-envelope profile {profile.id!r} requires an exact context budget"
            )
    return Profile(
        id=profile.id,
        label=profile.label,
        topology=profile.topology,
        stages=tuple(profile.stages),
        models=_freeze_mapping(profile.models),
        prompts=_freeze_mapping(profile.prompts),
        policies=_freeze_mapping(profile.policies),
        workflow=profile.workflow,
        supplemental_sources=supplemental_sources,
        knobs=_freeze_mapping(profile.knobs),
        visibility=profile.visibility,
        default=profile.default,
        selection_priority=profile.selection_priority,
        context_window=profile.context_window,
        reserved_output_tokens=profile.reserved_output_tokens,
        exact_tokenizer=profile.exact_tokenizer,
        low_level_leg=profile.low_level_leg,
        output_contracts=tuple(profile.output_contracts),
    )


def _freeze_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return _freeze_mapping(value)
    if isinstance(value, list):
        return tuple(_freeze_value(item) for item in value)
    return value


def _freeze_mapping(value: Mapping[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType({key: _freeze_value(item) for key, item in value.items()})


def known_model_aliases(model_map_raw: str = "") -> List[str]:
    """Return all configured backing-model aliases known to med-agent-hub."""
    explicit = parse_model_map(model_map_raw)
    return sorted(
        set(OPENROUTER_MODEL_MAP)
        | set(WAHS_MODEL_CONTEXT_SPECS)
        | set(explicit)
    )


EXECUTION_MODES: Dict[str, Dict[str, Any]] = {
    "quick": {
        "topology": "single",
        "stages": ("context", "answer", "gate", "resolve_refs", "final_resolve_refs"),
        "output": "product",
        "supports_indepth": False,
        "default_indepth": False,
        "visibility": "product",
        "description": "Single-pass cited answer with deterministic temporal, citation, and safety checks.",
    },
    "fast_checked": {
        "topology": "single",
        "stages": (
            "context",
            "answer",
            "gate",
            "resolve_refs",
            "review",
            "gate",
            "final_resolve_refs",
            "ground_verdicts",
        ),
        "output": "product",
        "supports_indepth": False,
        "default_indepth": False,
        "visibility": "product",
        "description": "Reviewed and grounded cited answer without an In-Depth section.",
    },
    "standard": {
        "topology": "single",
        "stages": (
            "context",
            "answer",
            "gate",
            "resolve_refs",
            "final_resolve_refs",
            "indepth",
            "indepth_gate",
        ),
        "output": "product",
        "supports_indepth": True,
        "default_indepth": True,
        "visibility": "product",
        "description": "Cited answer and In-Depth section with deterministic temporal, citation, and safety checks.",
    },
    "verified": {
        "topology": "single",
        "stages": (
            "context",
            "answer",
            "gate",
            "resolve_refs",
            "review",
            "gate",
            "final_resolve_refs",
            "ground_verdicts",
            "indepth",
            "indepth_gate",
        ),
        "output": "product",
        "supports_indepth": True,
        "default_indepth": True,
        "visibility": "product",
        "description": "Cited answer, verification review with citation grounding, and In-Depth section.",
    },
    "agree": {
        "topology": "single",
        "stages": (
            "context",
            "answer",
            "gate",
            "resolve_refs",
            "final_resolve_refs",
            "agree",
            "indepth",
            "indepth_gate",
        ),
        "output": "product",
        "supports_indepth": True,
        "default_indepth": True,
        "visibility": "product",
        "description": "Dual-writer agreement check across clinical statements plus In-Depth section.",
    },
    "team": {
        "topology": "team",
        "stages": (
            "context",
            "gather",
            "answer",
            "gate",
            "resolve_refs",
            "review",
            "gate",
            "final_resolve_refs",
            "ground_verdicts",
            "indepth",
            "indepth_gate",
        ),
        "output": "product",
        "supports_indepth": True,
        "default_indepth": True,
        "visibility": "product",
        "description": "Coordinator evidence gathering paired with synthesis, verification review, and In-Depth section.",
    },
    "bare_eval": {
        "topology": "single",
        "stages": ("context", "answer", "gate"),
        "output": "bare",
        "supports_indepth": False,
        "default_indepth": False,
        "visibility": "evaluation",
        "description": "Single-call evaluation mode emitting raw answer text without the product envelope.",
    },
}

_EXECUTION_MODE_ALIASES: Dict[str, str] = {
    "fast": "fast_checked",
    "bare": "standard",
    "checked": "verified",
    "verify": "verified",
    "cross_checked": "verified",
    "eval": "bare_eval",
}

_EXECUTION_SPEC_KEYS = {
    "mode",
    "verifier_model",
    "second_model",
    "orchestrator_model",
    "expert_model",
    "specialist_model",
    "grounding_model",
    "indepth_model",
    "indepth",
    "checks",
    "knobs",
    "context_window",
    "reserved_output_tokens",
    "exact_tokenizer",
    "supplemental_sources",
    "preset_id",
    "roles",
}

_EXECUTION_CHECK_KEYS = {
    "temporal",
    "grounding",
    "confidence",
    "drug_safety",
    "review_action",
    "review_prompt",
    "answer_contract",
}


def _execution_required_roles(stages: Tuple[str, ...]) -> List[str]:
    roles = ["answer"]
    if "gather" in stages:
        roles.extend(["orchestrator", "expert"])
    if "review" in stages:
        roles.append("review")
    if "ground_verdicts" in stages:
        roles.append("grounding")
    if "agree" in stages:
        roles.append("second")
    if "indepth" in stages:
        roles.append("indepth")
    return roles


def execution_modes_metadata() -> List[Dict[str, Any]]:
    """Describe generic orthogonal execution modes supported for backing models."""
    return [
        {
            "id": mode_id,
            "topology": spec["topology"],
            "output_mode": spec["output"],
            "stages": list(spec["stages"]),
            "required_roles": _execution_required_roles(spec["stages"]),
            "supports_indepth_toggle": bool(spec["supports_indepth"]),
            "description": spec["description"],
        }
        for mode_id, spec in EXECUTION_MODES.items()
    ]


def _verify_profile_prompts(profile: Profile) -> None:
    roles: Dict[str, str] = {
        str(role): str(configured)
        for role, configured in profile.prompts.items()
    }
    if "second" in profile.models and "second" not in roles:
        roles["second"] = str(profile.prompts.get("answer") or "synthesis-answer")
    for role, prompt in sorted(roles.items()):
        if not prompt:
            continue
        names = _prompt_assets(profile, role, prompt)
        missing = [
            name for name in names if not (_PROMPTS / f"{name}.txt").is_file()
        ]
        if missing:
            raise ValueError(
                f"profile {profile.id!r} references missing prompts {missing}"
            )


def _clean_role_model(value: Any, *, field_name: str, default: str) -> str:
    if value is None:
        return default
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"execution.{field_name} must be a non-empty string")
    return value.strip()


def compile_execution_spec(
    model_id: str, execution: Optional[Mapping[str, Any]] = None
) -> Profile:
    """Compile an orthogonal backing-model + execution-mode specification into a ``Profile``."""
    model = str(model_id or "").strip()
    if not model:
        raise ValueError("model id is required")
    if execution is None:
        spec: Dict[str, Any] = {}
    elif not isinstance(execution, Mapping):
        raise ValueError("execution must be a JSON object")
    else:
        spec = dict(execution)

    unknown = sorted(set(spec) - _EXECUTION_SPEC_KEYS)
    if unknown:
        raise ValueError(f"execution has unknown keys {unknown}")

    raw_mode = str(spec.get("mode") or "standard").strip().lower()
    canonical_mode = _EXECUTION_MODE_ALIASES.get(raw_mode, raw_mode)
    if canonical_mode not in EXECUTION_MODES:
        raise ValueError(
            f"unknown execution mode {raw_mode!r}; supported: {sorted(EXECUTION_MODES)}"
        )
    mode_def = EXECUTION_MODES[canonical_mode]

    checks_raw = spec.get("checks")
    if checks_raw is None:
        checks: Dict[str, Any] = {}
    elif not isinstance(checks_raw, Mapping):
        raise ValueError("execution.checks must be a JSON object")
    else:
        checks = dict(checks_raw)
    unknown_checks = sorted(set(checks) - _EXECUTION_CHECK_KEYS)
    if unknown_checks:
        raise ValueError(f"execution.checks has unknown keys {unknown_checks}")

    if "indepth" in spec and spec["indepth"] is not None:
        if not isinstance(spec["indepth"], bool):
            raise ValueError("execution.indepth must be a boolean")
        indepth_enabled = spec["indepth"]
        if (
            not mode_def["supports_indepth"]
            and indepth_enabled != mode_def["default_indepth"]
        ):
            raise ValueError(
                f"execution mode {canonical_mode!r} does not support indepth={indepth_enabled}"
            )
    else:
        indepth_enabled = bool(mode_def["default_indepth"])

    stages = tuple(
        stage
        for stage in mode_def["stages"]
        if indepth_enabled or stage not in {"indepth", "indepth_gate"}
    )

    roles_raw = spec.get("roles")
    if roles_raw is None:
        roles_override: Dict[str, str] = {}
    elif not isinstance(roles_raw, Mapping):
        raise ValueError("execution.roles must be a JSON object")
    else:
        roles_override = {
            str(key).strip(): _clean_role_model(
                val, field_name=f"roles.{key}", default=""
            )
            for key, val in roles_raw.items()
        }

    answer_model = roles_override.get("answer") or model
    models: Dict[str, str] = {"answer": answer_model}
    prompts: Dict[str, str] = {"answer": "synthesis-answer"}

    has_review = "review" in stages or "ground_verdicts" in stages
    raw_grounding = checks.get("grounding")
    if not has_review:
        if raw_grounding is not None and str(raw_grounding).strip().lower() != "off":
            raise ValueError(
                f"execution mode {canonical_mode!r} does not run review/grounding; use 'verified' or 'fast_checked'"
            )
        grounding = "off"
    else:
        grounding = str(raw_grounding or "separate").strip().lower()

    if "review" in stages:
        verifier_model = _clean_role_model(
            spec.get("verifier_model") or roles_override.get("review"),
            field_name="verifier_model",
            default=answer_model,
        )
        models["review"] = verifier_model
        review_prompt = checks.get("review_prompt")
        if review_prompt is not None:
            prompts["review"] = _clean_role_model(
                review_prompt, field_name="checks.review_prompt", default=""
            )
        elif (
            grounding == "folded"
            or raw_mode in {"verify", "cross_checked"}
            or verifier_model != answer_model
        ):
            prompts["review"] = "validation-verify"
        else:
            prompts["review"] = "validation-label"

    if "ground_verdicts" in stages:
        models["grounding"] = _clean_role_model(
            spec.get("grounding_model") or roles_override.get("grounding"),
            field_name="grounding_model",
            default=answer_model,
        )

    if "agree" in stages:
        models["second"] = _clean_role_model(
            spec.get("second_model") or roles_override.get("second"),
            field_name="second_model",
            default=answer_model,
        )

    if "gather" in stages:
        models["orchestrator"] = _clean_role_model(
            spec.get("orchestrator_model") or roles_override.get("orchestrator"),
            field_name="orchestrator_model",
            default=answer_model,
        )
        prompts["orchestrator"] = "orchestrator"
        expert_override = (
            spec.get("expert_model")
            or spec.get("specialist_model")
            or roles_override.get("expert")
            or roles_override.get("specialist")
        )
        if expert_override is not None or canonical_mode == "team":
            models["expert"] = _clean_role_model(
                expert_override,
                field_name="expert_model",
                default=answer_model,
            )
            prompts["expert"] = "medical_expert"

    if "indepth" in stages:
        models["indepth"] = _clean_role_model(
            spec.get("indepth_model") or roles_override.get("indepth"),
            field_name="indepth_model",
            default=answer_model,
        )
        prompts["indepth"] = "synthesis-indepth"

    output_mode = str(mode_def["output"])
    if output_mode == "product":
        temporal_gate = str(checks.get("temporal") or "enforce").strip().lower()
        confidence = str(checks.get("confidence") or "levels").strip().lower()
        drug_safety = checks.get("drug_safety", True)
        if not isinstance(drug_safety, bool):
            raise ValueError("execution.checks.drug_safety must be a boolean")
        policies: Dict[str, Any] = {
            "output": "product",
            "answer_contract": str(
                checks.get("answer_contract") or "chart_answer"
            ).strip().lower(),
            "temporal_gate": temporal_gate,
            "temporal_render": "full",
            "drug_safety": drug_safety,
            "confidence": confidence,
        }
        if has_review:
            review_action = str(
                checks.get("review_action") or "label"
            ).strip().lower()
            policies["review"] = {
                "action": review_action,
                "grounding": grounding,
                "confidence": confidence,
            }
    else:
        temporal_gate = str(checks.get("temporal") or "off").strip().lower()
        policies = {
            "output": output_mode,
            "answer_contract": str(
                checks.get("answer_contract") or "chart_answer"
            ).strip().lower(),
            "temporal_gate": temporal_gate,
            "temporal_render": "full",
        }
        if "confidence" in checks:
            policies["confidence"] = str(checks["confidence"]).strip().lower()
        if "drug_safety" in checks:
            if not isinstance(checks["drug_safety"], bool):
                raise ValueError("execution.checks.drug_safety must be a boolean")
            policies["drug_safety"] = checks["drug_safety"]

    knobs: Dict[str, Dict[str, Any]] = {"answer": {"temperature": 0}}
    if has_review and grounding == "folded":
        knobs["review"] = {"temperature": 0}
    knobs_raw = spec.get("knobs")
    if knobs_raw is not None:
        if not isinstance(knobs_raw, Mapping):
            raise ValueError("execution.knobs must be a JSON object")
        for role, role_knobs in knobs_raw.items():
            if not isinstance(role_knobs, Mapping):
                raise ValueError(f"execution.knobs.{role} must be a JSON object")
            merged = dict(knobs.get(str(role), {}))
            merged.update(dict(role_knobs))
            knobs[str(role)] = merged

    context_window = int(spec.get("context_window") or 24576)
    reserved_output_tokens = int(spec.get("reserved_output_tokens") or 4096)
    exact_tokenizer = bool(spec.get("exact_tokenizer", True))
    supplemental_raw = spec.get("supplemental_sources")
    if supplemental_raw is None:
        supplemental_sources: Tuple[str, ...] = ("knowledge-base",)
    elif isinstance(supplemental_raw, str):
        supplemental_sources = (supplemental_raw,)
    elif isinstance(supplemental_raw, (list, tuple)):
        supplemental_sources = tuple(str(s) for s in supplemental_raw)
    else:
        raise ValueError("execution.supplemental_sources must be a list of strings")

    preset_id = spec.get("preset_id")
    profile_id = (
        str(preset_id).strip()
        if isinstance(preset_id, str) and preset_id.strip()
        else f"{answer_model}:{canonical_mode}"
    )

    compiled = compile_profile(
        Profile(
            id=profile_id,
            label=profile_id,
            topology=str(mode_def["topology"]),
            stages=stages,
            models=models,
            prompts=prompts,
            policies=policies,
            workflow="clinical_answer",
            supplemental_sources=supplemental_sources,
            knobs=knobs,
            visibility=str(mode_def["visibility"]),
            default=False,
            selection_priority=100,
            context_window=context_window,
            reserved_output_tokens=reserved_output_tokens,
            exact_tokenizer=exact_tokenizer,
        )
    )
    _verify_profile_prompts(compiled)
    return compiled


def profile_ids() -> List[str]:
    raw = _load_raw()
    ids = [
        profile_id
        for profile_id, spec in raw.items()
        if str((spec or {}).get("workflow") or "clinical_answer") == "clinical_answer"
    ]
    if ids:
        defaults = [
            profile_id for profile_id in ids if bool((raw[profile_id] or {}).get("default"))
        ]
        if len(defaults) != 1:
            raise ValueError(
                f"levels.yaml must define exactly one default profile; found {defaults}"
            )
    return ids


def validate_profiles() -> Tuple[Profile, ...]:
    """Compile every configured profile and canonical execution mode and verify prompts."""
    profiles = list(get_profile(profile_id) for profile_id in profile_ids())
    for mode_id in EXECUTION_MODES:
        profiles.append(compile_execution_spec("gemma-4-12b", {"mode": mode_id}))
    profiles.append(
        compile_execution_spec(
            "gemma-4-12b",
            {"mode": "verified", "checks": {"grounding": "folded"}},
        )
    )
    for profile in profiles:
        _verify_profile_prompts(profile)
    return tuple(profiles)


def get_profile(profile_id: str) -> Profile:
    dynamic = _dynamic_profile(profile_id)
    if dynamic is not None:
        return dynamic
    configured_ids = profile_ids()
    if profile_id in configured_ids:
        raw = _load_raw()
        return _from_spec(profile_id, raw[profile_id] or {})
    if ":" in profile_id:
        base_model, _, suffix = profile_id.rpartition(":")
        mode_key = suffix.strip().lower()
        canonical_mode = _EXECUTION_MODE_ALIASES.get(mode_key, mode_key)
        if base_model.strip() and canonical_mode in EXECUTION_MODES:
            return compile_execution_spec(base_model.strip(), {"mode": mode_key})
    raise ModelNotFoundError(profile_id, configured_ids)


def resolve_temporal_policy(
    profile: Profile, request_context: Optional[Mapping[str, Any]]
) -> tuple[bool, str]:
    if profile.output_mode == "product":
        return True, "enforce"
    context = request_context or {}
    enabled = bool(context.get("temporal", True))
    mode = str(
        context.get("temporal_gate", profile.policies.get("temporal_gate", "off"))
    ).lower()
    if mode not in _TEMPORAL_GATE_MODES:
        mode = str(profile.policies.get("temporal_gate", "off"))
    return enabled, mode


def _jsonable(value: Any) -> Any:
    """Return immutable profile configuration as canonical JSON values."""
    if isinstance(value, Mapping):
        return {
            str(key): _jsonable(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _sha256(value: str) -> str:
    return f"sha256:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _profile_configuration_digest(profile: Profile) -> str:
    configuration = {
        "id": profile.id,
        "label": profile.label,
        "workflow": profile.workflow,
        "topology": profile.topology,
        "stages": profile.stages,
        "models": profile.models,
        "prompts": profile.prompts,
        "policies": profile.policies,
        "staged": profile.staged,
        "validation": profile.validation,
        "knobs": profile.knobs,
        "visibility": profile.visibility,
        "default": profile.default,
        "selection_priority": profile.selection_priority,
        "supplemental_sources": profile.supplemental_sources,
        "context_window": profile.context_window,
        "reserved_output_tokens": profile.reserved_output_tokens,
        "exact_tokenizer": profile.exact_tokenizer,
        "low_level_leg": profile.low_level_leg,
        "output_contracts": profile.output_contracts,
    }
    canonical = json.dumps(
        _jsonable(configuration),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return _sha256(canonical)


def _prompt_assets(profile: Profile, role: str, configured: str) -> Tuple[str, ...]:
    if role != "review" or profile.workflow == "wahs":
        return (configured,)
    review_policy = profile.policies.get("review") or {}
    folded = review_policy.get("grounding") == "folded"
    assets = [configured + "-answer"]
    if folded:
        assets.append("review-citation-support")
    if "indepth" in profile.stages:
        assets.append(configured + "-indepth")
        if folded:
            assets.append("review-indepth-citation-support")
    return tuple(assets)


def _role_prompt_digests(profile: Profile) -> Dict[str, Any]:
    digests: Dict[str, Any] = {}
    prompt_roles: Dict[str, str] = {
        str(role): str(configured_value)
        for role, configured_value in profile.prompts.items()
    }
    if "second" in profile.models and "second" not in prompt_roles:
        prompt_roles["second"] = str(
            profile.prompts.get("answer") or "synthesis-answer"
        )
    for role, configured in sorted(prompt_roles.items()):
        system_prompts = {}
        for name in _prompt_assets(profile, role, configured):
            content = (
                (_PROMPTS / f"{name}.txt").read_text(encoding="utf-8").rstrip("\n")
            )
            system_prompts[name] = _sha256(content)
        digests[str(role)] = {
            "configured_prompt": configured,
            "system_prompt_sha256": system_prompts,
        }
    return digests


def profile_metadata(
    profile: Profile,
    *,
    available: bool,
    unavailable_reasons: Tuple[str, ...] = (),
    effective_default: Optional[bool] = None,
) -> Dict[str, Any]:
    metadata = {
        "id": profile.id,
        "label": profile.label,
        "workflow": profile.workflow,
        "staged": profile.staged,
        "validation": profile.validation,
        "temporal_enforcement": str(profile.policies.get("temporal_gate", "off")),
        "available": bool(available),
        "default": (
            profile.default if effective_default is None else bool(effective_default)
        ),
        "selection_priority": profile.selection_priority,
        "topology": profile.topology,
        "visibility": profile.visibility,
        "stages": list(profile.stages),
        "required_models": sorted(set(profile.models.values())),
        "role_models": dict(profile.models),
        "role_knobs": _jsonable(profile.knobs),
        "profile_configuration_digest": _profile_configuration_digest(profile),
        "role_prompt_digests": _role_prompt_digests(profile),
        "context_window": profile.context_window or None,
        "exact_tokenizer": profile.exact_tokenizer,
        "unavailable_reasons": list(unavailable_reasons),
    }
    if profile.output_contracts:
        metadata["outputContracts"] = list(profile.output_contracts)
    model_classes = profile.policies.get("model_classes")
    if isinstance(model_classes, Mapping):
        metadata["role_model_classes"] = _jsonable(model_classes)
    return metadata
