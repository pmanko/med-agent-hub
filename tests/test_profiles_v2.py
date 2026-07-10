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
)


def test_default_product_profile_is_human_readable_single_e4b():
    profile = get_profile("single-e4b-checked")

    assert profile.label == "Fast checked answer (E4B)"
    assert profile.default is True
    assert profile.topology == "single"
    assert "orchestrator" not in profile.models
    assert profile.models["answer"] == "gemma-e4b"
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


def test_product_profile_requires_exact_context_budget_and_enforce_gate():
    profile = get_profile("single-e4b-checked")

    assert profile.context_window > profile.reserved_output_tokens > 0
    assert profile.exact_tokenizer is True
    assert profile.policies["temporal_gate"] == "enforce"
    assert resolve_temporal_policy(
        profile, {"temporal": False, "temporal_gate": "off"}
    ) == (True, "enforce")


def test_low_level_answer_leg_remains_minimal_and_experimental():
    profile = get_profile(
        "answer:gemma-4-12b@synthesis-date-output-contract~warn~temp0"
    )

    assert profile.low_level_leg is True
    assert profile.stages == ("context", "answer", "gate")
    assert profile.policies["temporal_gate"] == "warn"
    assert profile.knobs["answer"]["temperature"] == 0.0
    assert resolve_temporal_policy(
        profile, {"temporal": False, "temporal_gate": "off"}
    ) == (False, "off")


def test_invalid_grounding_order_is_rejected_at_compile_time():
    profile = get_profile("single-e4b-checked")
    bad = replace(
        profile,
        stages=(
            "context",
            "answer",
            "gate",
            "resolve_refs",
            "ground_verdicts",
            "review",
            "indepth",
            "indepth_gate",
        ),
    )

    with pytest.raises(ValueError, match="ground_verdicts.*review"):
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
        "topology": "single",
        "visibility": "product",
    }


def test_only_one_configured_profile_is_default():
    defaults = [profile_id for profile_id in profile_ids() if get_profile(profile_id).default]
    assert defaults == ["single-e4b-checked"]
