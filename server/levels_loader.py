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

import yaml

from .prompt_loader import load_prompt

_PATH = Path(__file__).parent / "levels.yaml"
_PROMPTS = Path(__file__).parent / "prompts"
_TEMPORAL_GATE_MODES = {"off", "warn", "enforce"}
_CLINICAL_TOPOLOGIES = {"single", "team", "leg"}
_WORKFLOWS = {"clinical_answer", "catalyst_query"}
_OUTPUT_MODES = {"bare", "combined", "product", "review", "indepth"}
_ANSWER_CONTRACTS = {"caller", "chart_answer"}
_ALLOWED_STAGES = {
    "context",
    "gather",
    "answer",
    "gate",
    "resolve_refs",
    "review",
    "final_resolve_refs",
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
            "validation-rewrite",
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
            },
            knobs=knobs,
            visibility="internal",
            low_level_leg=True,
        )
        return compile_profile(profile)
    return None


def _load_document() -> Dict[str, Any]:
    try:
        document = yaml.safe_load(_PATH.read_text(encoding="utf-8")) or {}
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"profiles file not found at {_PATH}") from exc
    if not isinstance(document, dict):
        raise ValueError(f"{_PATH} must contain an object")
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
    encoded = json.dumps(compact, sort_keys=True, separators=(",", ":"))
    evidence["profileDigest"] = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
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


def compile_profile(profile: Profile) -> Profile:
    if not profile.id or not profile.label:
        raise ValueError("profile id and label are required")
    if profile.workflow not in _WORKFLOWS:
        raise ValueError(
            f"profile {profile.id!r} has invalid workflow {profile.workflow!r}"
        )
    if profile.workflow == "catalyst_query":
        return _compile_catalyst_query_profile(profile)
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
    for stage, role in (
        ("answer", "answer"),
        ("review", "review"),
        ("ground_verdicts", "grounding"),
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
        if "review" in stages and stages.index("final_resolve_refs") < stages.index(
            "review"
        ):
            raise ValueError(
                f"profile {profile.id!r} final_resolve_refs must run after review"
            )
        if stages.index("final_resolve_refs") < max(
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
    if profile.output_mode == "product":
        required = {
            "answer",
            "gate",
            "resolve_refs",
            "final_resolve_refs",
            "ground_verdicts",
            "indepth",
            "indepth_gate",
        }
        missing = sorted(required - set(stages))
        if missing:
            raise ValueError(f"product profile {profile.id!r} lacks stages {missing}")
        if not (
            stages.index("final_resolve_refs")
            < stages.index("ground_verdicts")
            < stages.index("indepth")
            < stages.index("indepth_gate")
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


def profile_ids() -> List[str]:
    raw = _load_raw()
    ids = [
        profile_id
        for profile_id, spec in raw.items()
        if str((spec or {}).get("workflow") or "clinical_answer") == "clinical_answer"
    ]
    defaults = [
        profile_id for profile_id in ids if bool((raw[profile_id] or {}).get("default"))
    ]
    if len(defaults) != 1:
        raise ValueError(
            f"levels.yaml must define exactly one default profile; found {defaults}"
        )
    return ids


def validate_profiles() -> Tuple[Profile, ...]:
    """Compile every configured profile and verify its prompt files at startup."""
    profiles = tuple(get_profile(profile_id) for profile_id in profile_ids())
    for profile in profiles:
        for role in profile.models:
            prompt = profile.prompts.get(role)
            if not prompt:
                continue
            names = [str(prompt)]
            if role == "review":
                names = [str(prompt) + "-answer"]
                if "indepth" in profile.stages:
                    names.append(str(prompt) + "-indepth")
            missing = [
                name for name in names if not (_PROMPTS / f"{name}.txt").is_file()
            ]
            if missing:
                raise ValueError(
                    f"profile {profile.id!r} references missing prompts {missing}"
                )
    return profiles


def get_profile(profile_id: str) -> Profile:
    dynamic = _dynamic_profile(profile_id)
    if dynamic is not None:
        return dynamic
    raw = _load_raw()
    if profile_id not in profile_ids():
        raise ModelNotFoundError(profile_id, profile_ids())
    return _from_spec(profile_id, raw[profile_id] or {})


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
    if role != "review":
        return (configured,)
    assets = [configured + "-answer"]
    if "indepth" in profile.stages:
        assets.append(configured + "-indepth")
    return tuple(assets)


def _role_prompt_digests(profile: Profile) -> Dict[str, Any]:
    digests: Dict[str, Any] = {}
    for role, configured_value in sorted(profile.prompts.items()):
        configured = str(configured_value)
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
