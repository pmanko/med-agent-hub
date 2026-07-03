"""Team levels config: the OpenAI `model` id selects a level (server/levels.yaml),
which fixes the per-role models (orchestrator / synthesizer / expert) + prompts, so
ONE med-agent-hub serves any tier per request — no reboot. `expert: null` drops the
medical_expert tool + role. Mocks the LM Studio boundary; exercises the real
levels_loader + run_team. Run: pytest tests/test_team_config.py
"""

import asyncio
import json

import pytest

from server import team, levels_loader


def test_level_ids_advertises_the_configured_levels():
    # med-agent-hub publishes the keys in levels.yaml: the core tiers are always
    # advertised, with no duplicates (extra experiment lanes may be added over time).
    ids = levels_loader.level_ids()
    assert len(ids) == len(set(ids))  # no duplicate ids
    for tier in ("med-agent-team-low", "med-agent-team-med", "med-agent-team-high"):
        assert tier in ids, ids


def test_get_level_resolves_models_and_prompts():
    low = levels_loader.get_level("med-agent-team-low")
    assert low.orchestrator and low.synthesizer        # required roles present
    # synthesis_prompt is the BASE name; two-call resolves <base>-answer / <base>-indepth.
    assert low.synthesis_prompt == "synthesis"         # tier-agnostic two-call prompts (default base)
    high = levels_loader.get_level("med-agent-team-high")
    assert high.expert and high.expert != low.expert   # high steps up to a bigger expert
    assert high.synthesis_prompt == "synthesis"        # default prompt name


def test_unknown_level_fails_loud():
    with pytest.raises(KeyError):
        levels_loader.get_level("med-agent-team-bogus")


def test_expert_toggle_drops_the_medical_expert_tool():
    # The whole point of the toggle: a level with no expert is offered no
    # medical_expert tool.
    names_with = [t["function"]["name"] for t in team._tool_definitions(has_expert=True)]
    names_without = [t["function"]["name"] for t in team._tool_definitions(has_expert=False)]
    assert "kb_search" in names_with and "medical_expert" in names_with
    assert "kb_search" in names_without and "medical_expert" not in names_without


def test_level_with_null_expert_reports_no_expert():
    # A Level with expert unset reports has_expert False (drives the tool toggle).
    assert levels_loader.Level(id="x", orchestrator="o", synthesizer="s", expert=None).has_expert is False
    assert levels_loader.Level(id="y", orchestrator="o", synthesizer="s", expert="medgemma").has_expert is True


def test_indepth_shared_level_resolves():
    # The single-model In-Depth-parity lane: parity shape (two_call false) + shared In-Depth ON,
    # no expert, the 12B writer -> a single model that ALSO emits an In-Depth section.
    lv = levels_loader.get_level("single-12b-indepth")
    assert lv.two_call is False
    assert lv.indepth_shared is True
    assert lv.has_expert is False
    assert lv.synthesizer == "gemma-4-12b"


def test_existing_levels_default_indepth_shared_false():
    # indepth_shared defaults OFF so every pre-existing level (validated teams, bare parity)
    # is unchanged.
    for tier in ("med-agent-team-med-validated", "med-agent-team-parity", "med-agent-team-high"):
        assert levels_loader.get_level(tier).indepth_shared is False, tier


def test_generic_indepth_only_resolves_any_writer():
    # "indepth-only:<writer>" resolves dynamically to a single-pass In-Depth leg for ANY router
    # model, with no hand-authored level — so the two-call In-Depth is available for parity across
    # every arm/run (the writer does the shared-prompt In-Depth; orchestrator just carries history).
    for writer in ("mistral-nemo-12b-q8", "granite-3.3-8b", "qwen3.6-35b"):
        lv = levels_loader.get_level(f"indepth-only:{writer}")
        assert lv.synthesizer == writer
        assert lv.indepth_only is True
        assert lv.two_call is False
        assert lv.has_expert is False
        assert lv.solo is True  # P1: single scaffolding


def test_generic_indepth_only_accepts_prompt_variant():
    lv = levels_loader.get_level("indepth-only:gemma-4-12b@synthesis-indepth")
    assert lv.synthesizer == "gemma-4-12b"
    assert lv.indepth_only is True
    assert lv.two_call is False
    assert lv.synthesis_prompt == "synthesis-indepth"
    assert lv.solo is True


def test_generic_answer_resolves_any_writer():
    # "answer:<writer>" mirrors indepth-only: a single CONTEXTUAL Answer leg through the hub (the
    # parity lane — one answer call with full gathered incl the temporal block, no In-Depth, no
    # validator), so a two-call arm routes BOTH legs through the hub with symmetric context (not raw).
    for writer in ("gemma-4-12b", "qwen2.5-14b"):
        lv = levels_loader.get_level(f"answer:{writer}")
        assert lv.synthesizer == writer
        assert lv.two_call is False
        assert lv.indepth_only is False and lv.indepth_shared is False
        assert lv.has_expert is False
        assert lv.solo is True  # P1: single scaffolding (no orchestrator/team) — the fix
        assert lv.synthesis_prompt == "synthesis-chartsearchai"  # bare answer (no In-Depth)


def test_generic_answer_accepts_prompt_variant_and_temporal_gate():
    lv = levels_loader.get_level("answer:gemma-e4b-q8@synthesis-date-output-contract~warn")
    assert lv.synthesizer == "gemma-e4b-q8"
    assert lv.synthesis_prompt == "synthesis-date-output-contract"
    assert lv.temporal_gate == "warn"
    assert lv.two_call is False
    assert lv.solo is True
    assert lv.has_expert is False


def test_generic_answer_accepts_temperature_suffix():
    lv = levels_loader.get_level("answer:gemma-4-12b@synthesis-date-output-contract~enforce~temp0")
    assert lv.synthesizer == "gemma-4-12b"
    assert lv.synthesis_prompt == "synthesis-date-output-contract"
    assert lv.temporal_gate == "enforce"
    assert lv.knobs == {"synthesizer": {"temperature": 0.0}}

    warm = levels_loader.get_level("answer:gemma-4-12b@synthesis-date-output-contract~enforce~temp0.5")
    assert warm.knobs["synthesizer"]["temperature"] == 0.5


def test_generic_answer_review_resolves_default_reviewer_lane():
    lv = levels_loader.get_level("answer-review:qwen2.5-14b")
    assert lv.synthesizer == "qwen2.5-14b"
    assert lv.answer_review is True
    assert lv.two_call is False
    assert lv.solo is True
    assert lv.temporal_gate == "enforce"
    assert lv.synthesis_prompt == "validation-rewrite"


def test_generic_answer_rejects_unknown_temporal_gate():
    with pytest.raises(KeyError):
        levels_loader.get_level("answer:gemma-e4b-q8@synthesis-chartsearchai~maybe")


def test_generic_answer_rejects_unknown_temperature_suffix():
    with pytest.raises(KeyError):
        levels_loader.get_level("answer:gemma-e4b-q8@synthesis-chartsearchai~enforce~temperature0")


def test_temporal_gate_dynamic_answer_levels_use_run_anchor_and_modes():
    off = levels_loader.get_level("answer:gemma-4-12b@synthesis-chartsearchai~off")
    warn = levels_loader.get_level("answer:gemma-4-12b@synthesis-chartsearchai~warn")
    enforce = levels_loader.get_level("answer:gemma-26b@synthesis-chartsearchai~enforce")
    assert off.temporal_gate == "off"
    assert warn.temporal_gate == "warn"
    assert enforce.temporal_gate == "enforce"
    assert {off.anchor, warn.anchor, enforce.anchor} == {None}
    assert off.solo is True and warn.solo is True and enforce.solo is True


def test_wide_date_team_levels_use_contract_prompt_and_warn_gate():
    low = levels_loader.get_level("med-agent-team-12b-date-warn")
    assert low.synthesizer == "gemma-4-12b"
    assert low.validator == "qwen2.5-14b"
    assert low.synthesis_prompt == "synthesis-date-output-contract"
    assert low.temporal_gate == "warn"
    assert low.two_call is False

    high = levels_loader.get_level("med-agent-team-high-date-warn")
    assert high.orchestrator == "gemma-31b"
    assert high.expert == "medgemma-27b"
    assert high.synthesizer == "qwen3.6-35b-q6"
    assert high.validator == "gemma-31b"
    assert high.synthesis_prompt == "synthesis-date-output-contract"
    assert high.temporal_gate == "warn"


def test_unknown_non_indepth_level_still_fails_loud():
    # The dynamic path is gated to the "indepth-only:" prefix — anything else still raises.
    import pytest
    with pytest.raises(KeyError):
        levels_loader.get_level("totally-bogus-level")


def test_advertised_models_includes_dynamic_indepth_legs(monkeypatch):
    # chartsearchai exact-match-validates the requested model against /v1/models, so the dynamic
    # indepth-only:<writer> legs MUST be advertised for every router model — else they 400 mid-run
    # (the regression this guards). Mock the router's /v1/models so the test is hermetic.
    import httpx
    from server import openai_compat

    class _Resp:
        def json(self):
            return {"data": [{"id": "mistral-nemo-12b-q8"}, {"id": "qwen3.6-35b"}]}

    monkeypatch.setattr(httpx, "get", lambda *a, **k: _Resp())
    monkeypatch.setattr(openai_compat, "prompt_names", lambda: [
        "synthesis-chartsearchai",
        "synthesis-date-output-contract",
        "synthesis-indepth",
    ])
    ids = openai_compat._advertised_models()
    assert "indepth-only:mistral-nemo-12b-q8" in ids
    assert "indepth-only:qwen3.6-35b" in ids
    assert "answer:mistral-nemo-12b-q8" in ids   # the Answer leg advertised too (both legs hub-served)
    assert "answer:qwen3.6-35b" in ids
    assert "answer:mistral-nemo-12b-q8@synthesis-date-output-contract~warn" in ids
    assert "answer:qwen3.6-35b@synthesis-date-output-contract~enforce" in ids
    assert "answer:qwen3.6-35b@synthesis-date-output-contract~enforce~temp0" in ids
    assert "answer:qwen3.6-35b@synthesis-date-output-contract~enforce~temp0.5" in ids
    assert "indepth-only:qwen3.6-35b@synthesis-indepth" in ids
    assert "med-agent-team-med" in ids  # static levels still advertised alongside


def _stateful_fake(calls):
    """Orchestrator calls medical_expert on its first turn, then stops; synthesis
    is the response_format turn. Lets us see which model each role uses."""
    state = {"n": 0}

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, repeat_penalty=None, dry_multiplier=None, **kwargs):
        calls.append((model, bool(tools), response_format is not None))
        state["n"] += 1
        if response_format is not None:                      # synthesis turn
            return {"content": json.dumps({"answer": "ok", "citations": [], "blocks": []})}
        if state["n"] == 1:                                  # 1st orchestrator turn -> call expert
            return {"content": "", "tool_calls": [
                {"id": "c1", "function": {"name": "medical_expert",
                                          "arguments": json.dumps({"query": "interpret"})}}]}
        return {"content": "", "tool_calls": None}           # later orchestrator turn -> stop
    return fake_chat


def test_run_team_routes_expert_model(monkeypatch):
    """The expert (medical_expert) call uses expert_model — the 3rd per-call role."""
    calls = []
    monkeypatch.setattr(team, "_chat", _stateful_fake(calls))
    asyncio.run(team.run_team(
        [{"role": "system", "content": "s"}, {"role": "user", "content": "chart"},
         {"role": "user", "content": "q"}],
        response_format={"type": "json_schema", "json_schema": {}},
        orchestrator_model="ORCH", synthesizer_model="SYNTH", expert_model="EXPERT",
    ))
    orch = [m for (m, t, rf) in calls if t]                    # tool turns = orchestrator
    expert = [m for (m, t, rf) in calls if not t and not rf]   # no-tools, no-rf = expert
    synth = [m for (m, t, rf) in calls if not t and rf]        # no-tools, rf = synthesis
    assert orch and all(m == "ORCH" for m in orch), calls
    assert expert == ["EXPERT"], calls
    # Two-call synthesis (Answer + In-Depth) — both on the synthesizer model.
    assert synth and all(m == "SYNTH" for m in synth), calls


def test_validator_runs_on_the_parity_path(monkeypatch):
    # The answer-validator is a COMPOSABLE post-synthesis step (validator is orthogonal to two_call):
    # it must run on the two_call=False parity path too, not only two_call=True. Assert the validator
    # MODEL is actually invoked when a two_call=False team sets a validator. RED before the decoupling
    # (parity skipped the validator); GREEN after.
    calls = []

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, repeat_penalty=None, dry_multiplier=None, **kwargs):
        calls.append(model)
        if model == "VALIDATOR":                                   # answer-validator audit turn
            return {"content": json.dumps({"answer_ok": True, "answer_issues": ""})}
        if response_format is not None:                            # synthesis turn
            return {"content": json.dumps({"answer": "ok", "citations": [], "blocks": []})}
        return {"content": "", "tool_calls": None}                 # orchestrator turn -> stop

    monkeypatch.setattr(team, "_chat", fake_chat)
    asyncio.run(team.run_team(
        [{"role": "system", "content": "s"}, {"role": "user", "content": "chart"},
         {"role": "user", "content": "q"}],
        response_format={"type": "json_schema", "json_schema": {}},
        orchestrator_model="ORCH", synthesizer_model="SYNTH", expert_model=None, has_expert=False,
        synthesizer_prompt="synthesis-chartsearchai", two_call=False, validator_model="VALIDATOR",
    ))
    assert "VALIDATOR" in calls, f"validator must run on the two_call=False parity path; called: {calls}"
