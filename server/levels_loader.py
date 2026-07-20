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

_PATH = Path(__file__).parent / "levels.yaml"
_PROMPTS = Path(__file__).parent / "prompts"
_TEMPORAL_GATE_MODES = {"off", "warn", "enforce"}
_TOPOLOGIES = {"single", "team", "leg"}
_OUTPUT_MODES = {"bare", "combined", "product", "query", "review", "indepth"}
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
    "query_generate",
    "query_lint",
    "query_review",
    "query_finalize",
}


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
    stages: Tuple[str, ...]
    models: Mapping[str, str]
    prompts: Mapping[str, str]
    policies: Mapping[str, Any]
    capabilities: Mapping[str, bool]
    knobs: Mapping[str, Any] = field(default_factory=dict)
    visibility: str = "experimental"
    default: bool = False
    context_window: int = 0
    reserved_output_tokens: int = 0
    exact_tokenizer: bool = False
    low_level_leg: bool = False
    output_contracts: Tuple[str, ...] = ()

    @property
    def staged(self) -> bool:
        return bool(self.capabilities.get("staged"))

    @property
    def output_mode(self) -> str:
        return str(self.policies.get("output", "bare"))


@dataclass(frozen=True)
class StagePlan:
    id: str
    stages: Tuple[str, ...]
    topology: str
    low_level_leg: bool = False


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
            capabilities={"staged": False, "validation": role == "review"},
            knobs=knobs,
            visibility="internal",
            low_level_leg=True,
        )
        return compile_profile(profile)
    return None


def _load_raw() -> Dict[str, dict]:
    try:
        document = yaml.safe_load(_PATH.read_text(encoding="utf-8")) or {}
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"profiles file not found at {_PATH}") from exc
    profiles = document.get("profiles")
    if not isinstance(profiles, dict) or not profiles:
        raise ValueError(f"{_PATH} must contain a non-empty top-level profiles mapping")
    return profiles


def _from_spec(profile_id: str, spec: Mapping[str, Any]) -> Profile:
    context = spec.get("context") or {}
    profile = Profile(
        id=profile_id,
        label=str(spec.get("label") or "").strip(),
        topology=str(spec.get("topology") or "").strip().lower(),
        stages=tuple(spec.get("stages") or ()),
        models=dict(spec.get("models") or {}),
        prompts=dict(spec.get("prompts") or {}),
        policies=dict(spec.get("policies") or {}),
        capabilities=dict(spec.get("capabilities") or {}),
        knobs=dict(spec.get("knobs") or {}),
        visibility=str(spec.get("visibility") or "experimental"),
        default=bool(spec.get("default", False)),
        context_window=int(context.get("window") or 0),
        reserved_output_tokens=int(context.get("reserved_output_tokens") or 0),
        exact_tokenizer=bool(context.get("exact_tokenizer", False)),
        output_contracts=tuple(spec.get("outputContracts") or ()),
    )
    return compile_profile(profile)


def compile_profile(profile: Profile) -> Profile:
    if not profile.id or not profile.label:
        raise ValueError("profile id and label are required")
    if profile.topology not in _TOPOLOGIES:
        raise ValueError(
            f"profile {profile.id!r} has invalid topology {profile.topology!r}"
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
        ("query_generate", "query_generate"),
        ("query_review", "query_review"),
    ):
        if stage in profile.stages and role not in profile.models:
            raise ValueError(
                f"profile {profile.id!r} stage {stage} requires model role {role}"
            )
    if profile.output_mode not in _OUTPUT_MODES:
        raise ValueError(
            f"profile {profile.id!r} has invalid output mode {profile.output_mode!r}"
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
    query_stages = {"query_generate", "query_lint", "query_review", "query_finalize"}
    if profile.output_mode == "query":
        expected = (
            "context",
            "query_generate",
            "query_lint",
            "query_review",
            "query_finalize",
        )
        if profile.stages != expected:
            raise ValueError(
                f"query profile {profile.id!r} must use ordered stages {expected}"
            )
        if profile.topology != "single":
            raise ValueError(f"query profile {profile.id!r} must use single topology")
        if profile.output_contracts != ("catalyst.query.v1",):
            raise ValueError(
                f"query profile {profile.id!r} must advertise only catalyst.query.v1"
            )
        if profile.staged:
            raise ValueError(f"query profile {profile.id!r} cannot stream")
        for role in ("query_generate", "query_review"):
            for knob in ("temperature", "dry"):
                value = (profile.knobs.get(role) or {}).get(knob)
                if value != 0:
                    raise ValueError(
                        f"query profile {profile.id!r} role {role!r} "
                        f"must use {knob} 0"
                    )
        generation_attempts = profile.policies.get("generation_attempts", 2)
        if (
            not isinstance(generation_attempts, int)
            or isinstance(generation_attempts, bool)
            or not 1 <= generation_attempts <= 3
        ):
            raise ValueError(
                f"query profile {profile.id!r} generation_attempts must be 1..3"
            )
        if profile.policies.get("collaborative_review") is True:
            model_classes = profile.policies.get("model_classes")
            if not isinstance(model_classes, Mapping):
                raise ValueError(
                    f"collaborative query profile {profile.id!r} requires model_classes"
                )
            writer_class = str(model_classes.get("query_generate") or "")
            reviewer_class = str(model_classes.get("query_review") or "")
            if not writer_class or not reviewer_class or writer_class == reviewer_class:
                raise ValueError(
                    f"collaborative query profile {profile.id!r} requires different "
                    "writer and reviewer model classes"
                )
    elif query_stages.intersection(profile.stages):
        raise ValueError(
            f"non-query profile {profile.id!r} cannot declare query stages"
        )
    elif profile.output_contracts:
        raise ValueError(
            f"non-query profile {profile.id!r} cannot advertise query output contracts"
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
        capabilities=_freeze_mapping(profile.capabilities),
        knobs=_freeze_mapping(profile.knobs),
        visibility=profile.visibility,
        default=profile.default,
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
    ids = list(raw)
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
    if profile_id not in raw:
        raise ModelNotFoundError(profile_id, list(raw))
    return _from_spec(profile_id, raw[profile_id] or {})


def get_stage_plan(profile_id: str) -> StagePlan:
    profile = get_profile(profile_id)
    return StagePlan(
        profile.id, profile.stages, profile.topology, profile.low_level_leg
    )


def resolve_temporal_policy(
    profile: Profile, request_context: Optional[Mapping[str, Any]]
) -> tuple[bool, str]:
    if profile.visibility == "product":
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
        "topology": profile.topology,
        "stages": profile.stages,
        "models": profile.models,
        "prompts": profile.prompts,
        "policies": profile.policies,
        "capabilities": profile.capabilities,
        "knobs": profile.knobs,
        "visibility": profile.visibility,
        "default": profile.default,
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
) -> Dict[str, Any]:
    metadata = {
        "id": profile.id,
        "label": profile.label,
        "staged": profile.staged,
        "validation": bool(profile.capabilities.get("validation")),
        "temporal_enforcement": str(profile.policies.get("temporal_gate", "off")),
        "available": bool(available),
        "default": profile.default,
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
    if profile.output_mode == "query":
        metadata["revisionCapable"] = bool(
            profile.policies.get("collaborative_review") is True
            and isinstance(model_classes, Mapping)
            and model_classes.get("query_generate") != model_classes.get("query_review")
        )
    return metadata
