"""
File-backed team levels — the advertised /v1/models groupings.

Each level in ``server/levels.yaml`` fixes the per-role models (orchestrator,
expert, synthesizer) and optional per-level prompt names. Read PER REQUEST (like
``prompt_loader``) so editing the bind-mounted file changes behaviour with no
rebuild; the file is the single source of truth. ``expert: null`` (or omitting it)
runs the level with NO medical expert — the orchestrator is offered no
``medical_expert`` tool and the expert role is skipped.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

_PATH = Path(__file__).parent / "levels.yaml"


@dataclass(frozen=True)
class Level:
    """One advertised team tier."""

    id: str
    orchestrator: str
    synthesizer: str
    expert: Optional[str] = None  # None -> no medical_expert tool / role
    orchestrator_prompt: str = "orchestrator"
    expert_prompt: str = "medical_expert"
    synthesis_prompt: str = "synthesis"
    validator: Optional[str] = None  # None -> no post-synthesis audit round
    validator_prompt: str = "validation"
    validator_max_loops: int = 1
    # Two-call synthesis (Answer + In-Depth, default) vs a single chartsearchai-style call.
    # two_call: false -> the "parity" shape: ONE synthesis call using synthesis_prompt as a
    # whole prompt (not split into -answer/-indepth), validator skipped, output is the bare
    # chartsearchai {answer, citations, blocks} envelope (no In-Depth body, no confidence).
    two_call: bool = True
    # Reference-date anchor (the simulated "now" for recency/series). None -> fall back to the
    # HUB_ANCHOR env (run-wide) then "latest_record" (the max date in the chart). Modes:
    # "latest_record" | an explicit ISO date "YYYY-MM-DD" | "wall_clock".
    anchor: Optional[str] = None
    # Optional per-role sampling knobs: {role: {temperature, repeat_penalty, dry}}.
    # A role's entry overrides the global default for that role only; unset -> default.
    # Roles: orchestrator / expert / synthesizer / validator.
    knobs: Dict[str, Any] = field(default_factory=dict)

    @property
    def has_expert(self) -> bool:
        return bool(self.expert)

    @property
    def has_validator(self) -> bool:
        return bool(self.validator)


def _load_raw() -> Dict[str, dict]:
    try:
        data = yaml.safe_load(_PATH.read_text(encoding="utf-8")) or {}
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"levels file not found at {_PATH}") from exc
    levels = data.get("levels")
    if not isinstance(levels, dict) or not levels:
        raise ValueError(f"{_PATH} must contain a non-empty top-level `levels:` mapping")
    return levels


def level_ids() -> List[str]:
    """The advertised /v1/models ids, in file order. Read fresh each call."""
    return list(_load_raw().keys())


def get_level(level_id: str) -> Level:
    """Resolve one level. Fail loud on an unknown id or a missing required field."""
    raw = _load_raw()
    if level_id not in raw:
        raise KeyError(f"unknown level {level_id!r}; levels.yaml defines {list(raw)}")
    spec = raw[level_id] or {}
    try:
        return Level(
            id=level_id,
            orchestrator=spec["orchestrator"],
            synthesizer=spec["synthesizer"],
            expert=spec.get("expert"),
            orchestrator_prompt=spec.get("orchestrator_prompt", "orchestrator"),
            expert_prompt=spec.get("expert_prompt", "medical_expert"),
            synthesis_prompt=spec.get("synthesis_prompt", "synthesis"),
            validator=spec.get("validator"),
            validator_prompt=spec.get("validator_prompt", "validation"),
            validator_max_loops=spec.get("validator_max_loops", 1),
            two_call=spec.get("two_call", True),
            anchor=spec.get("anchor"),
            knobs=spec.get("knobs") or {},
        )
    except KeyError as exc:
        raise KeyError(f"level {level_id!r} missing required field {exc}") from exc
