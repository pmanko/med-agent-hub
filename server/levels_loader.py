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
_TEMPORAL_GATE_MODES = {"off", "warn", "enforce"}


def _split_dynamic_prompt_level(
    level_id: str, prefix: str
) -> tuple[str, str | None, str | None, float | None]:
    """Parse dynamic prompt-level ids.

    Supported forms:
      answer:<writer>
      answer:<writer>@<prompt>
      answer:<writer>@<prompt>~<temporal_gate>
      answer:<writer>@<prompt>~<temporal_gate>~temp0
      answer:<writer>@<prompt>~<temporal_gate>~temp0.5
      answer-review:<reviewer>
      indepth-only:<writer>
      indepth-only:<writer>@<prompt>

    ``@`` and ``~`` are deliberately not valid in our router model ids or prompt
    stems, which keeps this parser tiny and makes backend configs easy to read.
    """
    rest = level_id[len(prefix):]
    writer_prompt, *options = rest.split("~")
    gate: str | None = None
    temp_floor: float | None = None
    for opt in options:
        if not opt:
            raise KeyError(f"dynamic level {level_id!r} has an empty option suffix")
        if opt in _TEMPORAL_GATE_MODES:
            if gate is not None:
                raise KeyError(f"dynamic level {level_id!r} repeats temporal gate suffixes")
            gate = opt
            continue
        if opt.startswith("temp") and opt[4:]:
            if temp_floor is not None:
                raise KeyError(f"dynamic level {level_id!r} repeats temperature suffixes")
            try:
                temp_floor = float(opt[4:])
            except ValueError as exc:
                raise KeyError(
                    f"dynamic level {level_id!r} has invalid temperature suffix {opt!r}"
                ) from exc
            if temp_floor < 0:
                raise KeyError(
                    f"dynamic level {level_id!r} has invalid negative temperature suffix {opt!r}"
                )
            continue
        raise KeyError(
            f"dynamic level {level_id!r} has invalid option {opt!r}; expected temporal gate "
            f"{sorted(_TEMPORAL_GATE_MODES)} or temp<number>")
    writer, at, prompt = writer_prompt.partition("@")
    if not writer:
        raise KeyError(f"dynamic level {level_id!r} is missing a writer model id")
    return writer, (prompt if at and prompt else None), gate, temp_floor


def _dynamic_knobs(temp_floor: float | None) -> Dict[str, Any]:
    if temp_floor is None:
        return {}
    return {"synthesizer": {"temperature": temp_floor}}


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
    # two_call:false + indepth_shared:true -> the parity Answer PLUS one shared single-pass In-Depth
    # (the synthesis-indepth prompt, no validator). Lets a single-model-style arm emit an In-Depth
    # section so it is judged on the background dimension too. Default off -> existing levels unchanged.
    indepth_shared: bool = False
    # indepth_only:true -> the two-call architecture's IN-DEPTH leg: skip answer synthesis entirely
    # and produce only the In-Depth, elaborating the prior answer carried in the message history.
    indepth_only: bool = False
    # answer_only:true -> staged UX leg: run the normal answer path for a level, including its
    # validator and temporal gate, but skip the In-Depth leg so the caller can ship the answer
    # immediately and attach In-Depth later.
    answer_only: bool = False
    # answer_review:true -> staged UX validation leg: review an already-visible direct answer and
    # return a chartsearchai envelope plus answerValidation metadata, without adding In-Depth.
    answer_review: bool = False
    # solo:true (P1) -> SINGLE scaffolding: one model, no orchestrator/team. run_team skips the tool
    # loop; the writer answers from the deterministic context. False -> team. Orthogonal to context.
    solo: bool = False
    # Reference-date anchor (the simulated "now" for recency/series). None -> fall back to the
    # HUB_ANCHOR env (run-wide) then "latest_record" (the max date in the chart). Modes:
    # "latest_record" | an explicit ISO date "YYYY-MM-DD" | "wall_clock".
    anchor: Optional[str] = None
    # Deterministic temporal gate mode. off preserves existing behavior; warn records gate failures;
    # enforce replaces high-confidence temporal contradictions before the optional LLM validator.
    temporal_gate: str = "off"
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
        # Staged Answer leg: "answer-only:<level-id>" mirrors an existing level but stops after the
        # direct answer (+ validator/gates). This preserves the team tier semantics better than
        # mapping the UI's "AI Team High" choice to a raw solo writer.
        if level_id.startswith("answer-only:") and level_id.split(":", 1)[1]:
            _base = get_level(level_id.split(":", 1)[1])
            return Level(
                id=level_id,
                orchestrator=_base.orchestrator,
                synthesizer=_base.synthesizer,
                expert=_base.expert,
                orchestrator_prompt=_base.orchestrator_prompt,
                expert_prompt=_base.expert_prompt,
                synthesis_prompt=_base.synthesis_prompt,
                validator=_base.validator,
                validator_prompt=_base.validator_prompt,
                validator_max_loops=_base.validator_max_loops,
                two_call=_base.two_call,
                indepth_shared=False,
                indepth_only=False,
                answer_only=True,
                answer_review=False,
                solo=_base.solo,
                anchor=_base.anchor,
                temporal_gate=_base.temporal_gate,
                knobs=_base.knobs,
            )
        # Generic In-Depth leg: "indepth-only:<writer-model>" resolves dynamically to a single
        # shared-prompt In-Depth pass on ANY router model — no hand-authored level needed — so the
        # two-call In-Depth is available for parity across every arm/run. The orchestrator only
        # carries the prior answer in session history; the writer (synthesizer) does the In-Depth.
        if level_id.startswith("indepth-only:") and level_id.split(":", 1)[1]:
            _target = level_id.split(":", 1)[1]
            if _target in raw:
                _base = get_level(_target)
                _prompt = _base.synthesis_prompt + "-indepth" if _base.two_call else "synthesis-indepth"
                return Level(
                    id=level_id,
                    orchestrator=_base.synthesizer,
                    synthesizer=_base.synthesizer,
                    expert=None,
                    synthesis_prompt=_prompt,
                    two_call=False,
                    indepth_only=True,
                    answer_review=False,
                    solo=True,
                    anchor=_base.anchor,
                    temporal_gate=_base.temporal_gate,
                    knobs=_base.knobs,
                )
            _w, _prompt, _gate, _temp = _split_dynamic_prompt_level(level_id, "indepth-only:")
            return Level(
                id=level_id,
                orchestrator=_w,
                synthesizer=_w,
                expert=None,
                synthesis_prompt=_prompt or "synthesis-indepth",
                two_call=False,
                indepth_only=True,
                answer_review=False,
                solo=True,  # P1: single-model In-Depth leg — no orchestrator/team
                temporal_gate=_gate or "off",
                knobs=_dynamic_knobs(_temp),
            )
        # Generic Answer leg: "answer:<writer>" mirrors indepth-only — a single CONTEXTUAL answer
        # through the hub (the parity lane: one answer call with the full gathered evidence incl the
        # temporal block, no In-Depth, no validator), so a two-call arm routes BOTH legs through the
        # hub with symmetric context. The orchestrator gathers; the writer (synthesizer) answers.
        if level_id.startswith("answer:") and level_id.split(":", 1)[1]:
            _w, _prompt, _gate, _temp = _split_dynamic_prompt_level(level_id, "answer:")
            return Level(
                id=level_id,
                orchestrator=_w,
                synthesizer=_w,
                expert=None,
                synthesis_prompt=_prompt or "synthesis-chartsearchai",
                two_call=False,
                solo=True,  # P1: single-model Answer leg — no orchestrator/team (this is the fix)
                temporal_gate=_gate or "off",
                knobs=_dynamic_knobs(_temp),
            )
        # Staged Answer validation leg. The reviewer is a normal router model, but the hub runs a
        # special path keyed by the answer_to_review.v1 payload rather than synthesizing a fresh answer.
        if level_id.startswith("answer-review:") and level_id.split(":", 1)[1]:
            _w, _prompt, _gate, _temp = _split_dynamic_prompt_level(level_id, "answer-review:")
            return Level(
                id=level_id,
                orchestrator=_w,
                synthesizer=_w,
                expert=None,
                synthesis_prompt=_prompt or "validation-rewrite",
                two_call=False,
                indepth_shared=False,
                indepth_only=False,
                answer_only=False,
                answer_review=True,
                solo=True,
                temporal_gate=_gate or "enforce",
                knobs=_dynamic_knobs(_temp),
            )
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
            indepth_shared=spec.get("indepth_shared", False),
            indepth_only=spec.get("indepth_only", False),
            answer_only=spec.get("answer_only", False),
            answer_review=spec.get("answer_review", False),
            solo=spec.get("solo", False),
            anchor=spec.get("anchor"),
            temporal_gate=str(spec.get("temporal_gate", "off")).lower(),
            knobs=spec.get("knobs") or {},
        )
    except KeyError as exc:
        raise KeyError(f"level {level_id!r} missing required field {exc}") from exc
