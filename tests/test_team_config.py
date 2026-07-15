"""Configured profile and low-level leg contracts."""

import pytest

from server import levels_loader, team


def test_profile_ids_are_unique_and_include_supported_topologies():
    ids = levels_loader.profile_ids()
    assert len(ids) == len(set(ids))
    assert {
        "med-agent-team-med",
        "single-e2b-checked",
        "single-e4b-checked",
        "team-med-checked",
    } <= set(ids)


def test_configured_team_profile_resolves_explicit_roles_and_stages():
    profile = levels_loader.get_profile("med-agent-team-med")
    assert profile.topology == "team"
    assert profile.models["orchestrator"] == "gemma-e4b-q8"
    assert profile.models["answer"] == "qwen2.5-14b"
    assert profile.stages == ("context", "gather", "answer", "gate", "indepth")


def test_unknown_profile_fails_with_structured_model_not_found():
    with pytest.raises(levels_loader.ModelNotFoundError) as caught:
        levels_loader.get_profile("totally-bogus-level")
    assert caught.value.code == "model_not_found"


def test_expert_role_controls_tool_availability():
    names_with = [tool["function"]["name"] for tool in team._tool_definitions(True)]
    names_without = [tool["function"]["name"] for tool in team._tool_definitions(False)]
    assert names_with == ["medical_expert"]
    assert names_without == []


@pytest.mark.parametrize("writer", ["mistral-nemo-12b-q8", "qwen3.6-35b"])
def test_indepth_leg_compiles_for_any_explicit_writer(writer):
    profile = levels_loader.get_profile(f"indepth-only:{writer}@synthesis-indepth")
    assert profile.topology == "leg"
    assert profile.models == {"indepth": writer}
    assert profile.stages == ("context", "indepth")
    assert profile.output_mode == "indepth"


def test_answer_leg_compiles_prompt_gate_and_temperature():
    profile = levels_loader.get_profile(
        "answer:gemma-4-12b@synthesis-date-output-contract~enforce~temp0"
    )
    assert profile.models == {"answer": "gemma-4-12b"}
    assert profile.prompts == {"answer": "synthesis-date-output-contract"}
    assert profile.policies["temporal_gate"] == "enforce"
    assert profile.knobs == {"answer": {"temperature": 0.0}}
    assert profile.stages == ("context", "answer", "gate")


def test_review_leg_keeps_its_existing_wire_contract():
    profile = levels_loader.get_profile("answer-review:qwen2.5-14b")
    assert profile.models == {"review": "qwen2.5-14b"}
    assert profile.prompts == {"review": "validation-rewrite"}
    assert profile.output_mode == "review"
    assert profile.stages == ("context", "review")


def test_invalid_dynamic_options_fail_loud():
    with pytest.raises(levels_loader.ModelNotFoundError):
        levels_loader.get_profile("answer:gemma-e4b@synthesis-answer~maybe")
    with pytest.raises(levels_loader.ModelNotFoundError):
        levels_loader.get_profile(
            "answer:gemma-e4b@synthesis-answer~enforce~temperature0"
        )


def test_product_single_profile_has_no_fake_orchestrator_and_correct_order():
    profile = levels_loader.get_profile("single-e4b-checked")
    assert profile.topology == "single"
    assert "orchestrator" not in profile.models
    assert profile.default is True
    assert profile.policies["temporal_gate"] == "enforce"
    assert profile.exact_tokenizer is True
    assert profile.stages == (
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
    )


def test_grounding_is_after_review_and_final_reference_resolution():
    # Required order: review -> final_resolve_refs -> ground_verdicts.
    profile = levels_loader.get_profile("single-12b-checked")
    assert profile.stages.index("review") < profile.stages.index("final_resolve_refs")
    assert profile.stages.index("final_resolve_refs") < profile.stages.index(
        "ground_verdicts"
    )


def test_product_discovery_excludes_low_level_legs():
    assert all(
        not profile_id.startswith(("answer:", "answer-review:", "indepth-only:"))
        for profile_id in levels_loader.profile_ids()
    )
