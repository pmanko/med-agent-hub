from __future__ import annotations

from dataclasses import replace

import pytest

from server.levels_loader import (
    ModelNotFoundError,
    compile_profile,
    get_profile,
    profile_ids,
    profile_metadata,
    resolve_temporal_policy,
    validate_profiles,
)


def test_configured_catalog_contains_only_current_product_eval_and_debug_profiles():
    assert set(profile_ids()) == {
        "single-e2b-checked",
        "single-e4b-checked",
        "single-12b-checked",
        "single-a4b-checked",
        "team-med-checked",
        "debug-team-high-checked",
        "eval-e4b-answer-only",
        "eval-e4b-temporal-enforce",
        "eval-12b-answer-only",
        "eval-12b-temporal-enforce",
    }


def test_default_product_profile_is_human_readable_single_e4b():
    profile = get_profile("single-e4b-checked")

    assert profile.label == "Fast checked answer (E4B)"
    assert profile.default is True
    assert profile.topology == "single"
    assert "orchestrator" not in profile.models
    assert profile.models["answer"] == "gemma-e4b"
    assert profile.supplemental_sources == ("knowledge-base",)
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


def test_experimental_e2b_profile_changes_only_the_fast_answer_writer():
    profile = get_profile("single-e2b-checked")

    assert profile.label == "Experimental fast answer (E2B, E4B check)"
    assert profile.default is False
    assert profile.topology == "single"
    assert "orchestrator" not in profile.models
    assert profile.models == {
        "answer": "gemma-e2b",
        "review": "gemma-e4b",
        "grounding": "gemma-e4b",
        "indepth": "gemma-e4b",
    }
    assert profile.stages == get_profile("single-e4b-checked").stages
    assert profile.policies == get_profile("single-e4b-checked").policies
    assert profile.context_window == 24576
    assert profile.reserved_output_tokens == 4096
    assert profile.exact_tokenizer is True
    assert profile.supplemental_sources == ("knowledge-base",)
    assert profile.knobs["answer"]["temperature"] == 0


def test_product_profiles_temporal_cannot_weaken_enforce():
    profile = get_profile("single-e4b-checked")

    assert profile.context_window > profile.reserved_output_tokens > 0
    assert profile.exact_tokenizer is True
    assert profile.policies["temporal_gate"] == "enforce"
    assert profile.policies["temporal_render"] == "full"
    assert resolve_temporal_policy(
        profile, {"temporal": False, "temporal_gate": "off"}
    ) == (True, "enforce")


@pytest.mark.parametrize(
    ("profile_id", "model", "temporal_gate"),
    [
        ("eval-e4b-answer-only", "gemma-e4b", "off"),
        ("eval-e4b-temporal-enforce", "gemma-e4b", "enforce"),
        ("eval-12b-answer-only", "gemma-4-12b", "off"),
        ("eval-12b-temporal-enforce", "gemma-4-12b", "enforce"),
    ],
)
def test_answer_path_evaluation_profiles_have_matched_exact_context(
    profile_id: str, model: str, temporal_gate: str
):
    profile = get_profile(profile_id)

    assert profile.visibility == "evaluation"
    assert profile.default is False
    assert profile.topology == "single"
    assert profile.stages == ("context", "answer", "gate")
    assert profile.models == {"answer": model}
    assert profile.prompts == {"answer": "synthesis-answer"}
    assert profile.policies["output"] == "bare"
    assert profile.policies["answer_contract"] == "chart_answer"
    assert profile.policies["temporal_gate"] == temporal_gate
    assert profile.policies["temporal_render"] == "full"
    assert profile.context_window == 24576
    assert profile.reserved_output_tokens == 4096
    assert profile.exact_tokenizer is True
    assert profile.supplemental_sources == ("knowledge-base",)
    assert profile.knobs["answer"]["temperature"] == 0


def test_non_advertised_product_envelope_temporal_cannot_weaken_enforce():
    profile = get_profile("debug-team-high-checked")

    assert profile.visibility != "product"
    assert profile.output_mode == "product"
    assert resolve_temporal_policy(
        profile, {"temporal": False, "temporal_gate": "off"}
    ) == (True, "enforce")


def test_product_envelope_requires_exact_budget_even_when_not_advertised():
    profile = get_profile("single-e4b-checked")
    experimental = replace(
        profile,
        visibility="experimental",
        exact_tokenizer=False,
        context_window=0,
        reserved_output_tokens=0,
    )

    with pytest.raises(ValueError, match="product-envelope.*exact context budget"):
        compile_profile(experimental)


def test_low_level_answer_leg_remains_minimal_and_experimental():
    profile = get_profile(
        "answer:gemma-4-12b@synthesis-date-output-contract~warn~temp0"
    )

    assert profile.low_level_leg is True
    assert profile.stages == ("context", "answer", "gate")
    assert "answer_contract" not in profile.policies
    assert profile.policies["temporal_gate"] == "warn"
    assert profile.knobs["answer"]["temperature"] == 0.0
    assert resolve_temporal_policy(
        profile, {"temporal": False, "temporal_gate": "off"}
    ) == (False, "off")


def test_invalid_answer_contract_is_rejected_at_compile_time():
    profile = get_profile("eval-e4b-answer-only")
    bad = replace(profile, policies={**profile.policies, "answer_contract": "typo"})

    with pytest.raises(ValueError, match="invalid answer contract"):
        compile_profile(bad)


def test_invalid_grounding_order_is_rejected_at_compile_time():
    profile = get_profile("single-e4b-checked")
    bad = replace(
        profile,
        stages=(
            "context",
            "answer",
            "gate",
            "resolve_refs",
            "final_resolve_refs",
            "ground_verdicts",
            "review",
            "gate",
            "indepth",
            "indepth_gate",
        ),
    )

    with pytest.raises(ValueError, match="ground_verdicts.*review"):
        compile_profile(bad)


def test_final_reference_resolution_before_review_is_rejected_at_compile_time():
    profile = get_profile("single-e4b-checked")
    bad = replace(
        profile,
        stages=(
            "context",
            "answer",
            "gate",
            "resolve_refs",
            "final_resolve_refs",
            "review",
            "gate",
            "ground_verdicts",
            "indepth",
            "indepth_gate",
        ),
    )

    with pytest.raises(ValueError, match="final_resolve_refs.*review"):
        compile_profile(bad)


def test_answer_without_immediate_gate_is_rejected_at_compile_time():
    profile = get_profile("single-e4b-checked")
    bad = replace(
        profile,
        stages=(
            "context",
            "answer",
            "resolve_refs",
            "gate",
            "review",
            "gate",
            "final_resolve_refs",
            "ground_verdicts",
            "indepth",
            "indepth_gate",
        ),
    )

    with pytest.raises(ValueError, match="answer must be followed by gate"):
        compile_profile(bad)


def test_unknown_model_is_a_structured_error_not_backend_passthrough():
    with pytest.raises(ModelNotFoundError) as caught:
        get_profile("definitely-not-configured")

    assert caught.value.code == "model_not_found"
    assert "definitely-not-configured" in str(caught.value)


def test_discovery_metadata_is_authoritative_and_dynamic_legs_are_not_advertised():
    ids = profile_ids()
    assert "single-e4b-checked" in ids
    assert all(not model_id.startswith("answer:") for model_id in ids)

    metadata = profile_metadata(get_profile("single-e4b-checked"), available=True)
    assert metadata == {
        "id": "single-e4b-checked",
        "label": "Fast checked answer (E4B)",
        "staged": True,
        "validation": True,
        "temporal_enforcement": "enforce",
        "available": True,
        "default": True,
        "selection_priority": 10,
        "topology": "single",
        "visibility": "product",
        "stages": list(get_profile("single-e4b-checked").stages),
        "required_models": ["gemma-e4b"],
        "context_window": 24576,
        "exact_tokenizer": True,
        "unavailable_reasons": [],
    }


def test_stage_plan_derives_discovery_capabilities_without_manual_flags():
    profile = get_profile("single-e4b-checked")
    metadata = profile_metadata(profile, available=True)

    assert metadata["staged"] is True
    assert metadata["validation"] is True


def test_unconsumed_legacy_staged_profiles_are_removed():
    assert "med-agent-team-staged-12b" not in profile_ids()
    assert "med-agent-team-staged-12b-validated" not in profile_ids()


def test_only_one_configured_profile_is_default():
    defaults = [
        profile_id for profile_id in profile_ids() if get_profile(profile_id).default
    ]
    assert defaults == ["single-e4b-checked"]


def test_all_configured_profiles_and_prompts_validate_at_startup():
    profiles = validate_profiles()
    assert len(profiles) == len(profile_ids())


def test_compiled_profile_configuration_is_immutable():
    profile = get_profile("single-e4b-checked")
    with pytest.raises(TypeError):
        profile.models["answer"] = "different"
    with pytest.raises(TypeError):
        profile.knobs["answer"]["temperature"] = 0.5
