"""Validated hub profiles and low-level stage legs.

Configured profiles declare topology, ordered stages, role models, prompts, and
policies directly. Dynamic low-level ids compile to the same immutable shape but
are intentionally absent from product discovery.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional, Tuple

import yaml

_PATH = Path(__file__).parent / "levels.yaml"
_PROMPTS = Path(__file__).parent / "prompts"
_TEMPORAL_GATE_MODES = {"off", "warn", "enforce"}
_TOPOLOGIES = {"single", "team", "leg"}
_OUTPUT_MODES = {"bare", "combined", "product", "review", "indepth"}
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
    knobs: Mapping[str, Any] = field(default_factory=dict)
    visibility: str = "experimental"
    default: bool = False
    selection_priority: int = 1000
    context_window: int = 0
    reserved_output_tokens: int = 0
    exact_tokenizer: bool = False
    low_level_leg: bool = False

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
        knobs=dict(spec.get("knobs") or {}),
        visibility=str(spec.get("visibility") or "experimental"),
        default=bool(spec.get("default", False)),
        selection_priority=int(spec.get("selection_priority", 1000)),
        context_window=int(context.get("window") or 0),
        reserved_output_tokens=int(context.get("reserved_output_tokens") or 0),
        exact_tokenizer=bool(context.get("exact_tokenizer", False)),
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
        knobs=_freeze_mapping(profile.knobs),
        visibility=profile.visibility,
        default=profile.default,
        selection_priority=profile.selection_priority,
        context_window=profile.context_window,
        reserved_output_tokens=profile.reserved_output_tokens,
        exact_tokenizer=profile.exact_tokenizer,
        low_level_leg=profile.low_level_leg,
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


def profile_metadata(
    profile: Profile,
    *,
    available: bool,
    unavailable_reasons: Tuple[str, ...] = (),
    effective_default: Optional[bool] = None,
) -> Dict[str, Any]:
    return {
        "id": profile.id,
        "label": profile.label,
        "staged": profile.staged,
        "validation": profile.validation,
        "temporal_enforcement": str(profile.policies.get("temporal_gate", "off")),
        "available": bool(available),
        "default": profile.default if effective_default is None else bool(effective_default),
        "selection_priority": profile.selection_priority,
        "topology": profile.topology,
        "visibility": profile.visibility,
        "stages": list(profile.stages),
        "required_models": sorted(set(profile.models.values())),
        "context_window": profile.context_window or None,
        "exact_tokenizer": profile.exact_tokenizer,
        "unavailable_reasons": list(unavailable_reasons),
    }
