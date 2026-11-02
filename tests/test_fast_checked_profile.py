"""A product profile may skip In-Depth; it keeps every answer check and never promises an In-Depth."""

from dataclasses import replace

import pytest

import server.team as team
from server.levels_loader import compile_profile, get_profile
from tests.factories import make_profile
from tests.test_staged_stream import _collect, _stub_common

ANSWER_CHECKS = ("answer", "gate", "resolve_refs", "review", "final_resolve_refs", "ground_verdicts")


def test_the_fast_checked_profile_keeps_every_answer_check_and_drops_only_in_depth():
    checked = get_profile("single-12b-checked")
    fast = get_profile("single-12b-fast")

    assert all(stage in fast.stages for stage in ANSWER_CHECKS)
    assert [s for s in checked.stages if s not in ("indepth", "indepth_gate")] == list(fast.stages)
    assert {role: fast.models[role] for role in fast.models} == {
        role: checked.models[role] for role in ("answer", "review", "grounding")
    }
    assert not checked.default and not fast.default


def test_in_depth_without_its_gate_is_rejected():
    fast = get_profile("single-12b-fast")

    with pytest.raises(ValueError, match="indepth"):
        compile_profile(replace(fast, stages=fast.stages + ("indepth",)))


@pytest.mark.parametrize("reviewed", [False, True], ids=["unreviewed", "reviewed"])
def test_a_profile_without_in_depth_streams_no_pending_in_depth(monkeypatch, reviewed):
    _stub_common(monkeypatch)

    async def fake_answer(*_a, **_k):
        return ("Ans [1].", [1], [])

    async def fake_substantive(_client, **k):
        return (k["answer_text"], k["citations"], k["blocks"], {"level": "green", "note": ""})

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_ensure_substantive_answer", fake_substantive)
    profile = make_profile(
        topology="single",
        stages=["context", "answer", "gate", "resolve_refs"]
        + (["review", "gate"] if reviewed else [])
        + ["final_resolve_refs", "ground_verdicts"],
        models={"answer": "M", "grounding": "M", **({"review": "M"} if reviewed else {})},
        prompts={"answer": "synthesis-answer"},
        output="product",
        supplemental_sources=("knowledge-base",),
    )

    events = _collect(profile)

    names = [name for name, _ in events]
    assert not [name for name in names if name.startswith("indepth")]
    in_depth = [data["inDepth"] for _, data in events if isinstance(data.get("inDepth"), dict)]
    assert in_depth and all(item["status"] == "complete" and not item["answer"] for item in in_depth)


def test_the_answer_profile_is_a_product_profile_without_review_or_in_depth():
    checked = get_profile("single-12b-checked")
    answer = get_profile("single-12b-answer")

    assert answer.visibility == "product" and answer.output_mode == "product"
    assert list(answer.stages) == [
        "context", "answer", "gate", "resolve_refs", "final_resolve_refs", "ground_verdicts"
    ]
    assert answer.models == {role: checked.models[role] for role in ("answer", "grounding")}
    assert not answer.default
