"""Phased staged streaming (run_team_stream): the hub-owned answer -> (optional validation) -> in-depth
flow that the chartsearchai controller relays. Stage helpers are stubbed so these assert the
generator's CONTRACT — event sequence, conditional validation, and hub-side reference resolution —
not the LLM. A separate test pins the cancellation invariant (a cancelled _chat frees the router lock)."""

import asyncio
import json

import server.team as team

_MAPPINGS = [
    {"index": 1, "resourceType": "obs", "resourceUuid": "u1", "date": "2025-01-01", "text": "a"},
    {"index": 2, "resourceType": "order", "resourceUuid": "u2", "date": "2025-02-02", "text": "b"},
]


def _stub_common(monkeypatch):
    async def fake_retrieve(_patient):
        return "[1] obs\n[2] order\n", _MAPPINGS

    def fake_gate(**k):
        return k["answer_text"], k["citations"], k["blocks"], {"mode": "off", "status": "ok", "applied": "none"}, None

    async def fake_indepth(*_a, **_k):
        return (["claim one", "claim two"], {"level": "green", "note": ""})

    monkeypatch.setattr(team, "_retrieve_chart", fake_retrieve)
    monkeypatch.setattr(team, "_apply_temporal_gate", fake_gate)
    monkeypatch.setattr(team, "_gen_indepth", fake_indepth)
    monkeypatch.setattr(team, "_merge_temporal_gate_conf", lambda conf, _gate: conf)
    monkeypatch.setattr(team, "_write_trace", lambda *_a, **_k: None)


def _collect(**kwargs):
    async def _run():
        out = []
        async for name, data in team.run_team_stream(
            [{"role": "user", "content": "q?"}], patient="p", context={"temporal": False},
            model_label="lvl", **kwargs
        ):
            out.append((name, json.loads(data)))
        return out

    return asyncio.run(_run())


def test_staged_stream_with_validator_emits_full_phase_sequence(monkeypatch):
    _stub_common(monkeypatch)

    async def fake_answer(*_a, **_k):
        return ("Ans [1].", [1], [])

    async def fake_validate(_client, **_k):
        # validator rewrites the answer to cite record 2 -> status must be "edited" + carry originalAnswer
        return ("Ans fixed [2].", [2], [], {"level": "yellow", "note": "fixed a claim"})

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_validate_and_refine_answer", fake_validate)

    events = _collect(synth_model="M", validator_model="V")
    names = [n for n, _ in events]
    assert names == ["answer_done", "answer_validation", "indepth_pending", "indepth_done", "done"]

    ev = dict(events)
    # answer_done: fast answer, marked "validating", references RESOLVED by the hub (not indices)
    assert ev["answer_done"]["answerValidation"]["status"] == "validating"
    assert ev["answer_done"]["references"] == [
        {"index": 1, "resourceType": "obs", "resourceUuid": "u1", "date": "2025-01-01"}
    ]
    # answer_validation: the correction is surfaced (edited + original), refs re-resolved for the new citation
    assert ev["answer_validation"]["answerValidation"]["status"] == "edited"
    assert ev["answer_validation"]["answerValidation"]["originalAnswer"] == "Ans [1]."
    assert ev["answer_validation"]["references"] == [
        {"index": 2, "resourceType": "order", "resourceUuid": "u2", "date": "2025-02-02"}
    ]
    # done: in-depth complete
    assert ev["done"]["inDepth"]["status"] == "complete"
    assert "claim one" in ev["done"]["inDepth"]["answer"]
    assert ev["done"]["model"] == "lvl"


def test_staged_stream_without_validator_skips_validation_phase(monkeypatch):
    _stub_common(monkeypatch)

    async def fake_answer(*_a, **_k):
        return ("Ans [1].", [1], [])

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    # No validator configured -> _validate_and_refine_answer must NEVER be called.
    monkeypatch.setattr(team, "_validate_and_refine_answer",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("validator ran with no validator")))

    events = _collect(synth_model="M", validator_model=None)
    names = [n for n, _ in events]
    assert names == ["answer_done", "indepth_pending", "indepth_done", "done"]
    # No validation coming -> answer_done carries NO answerValidation (frontend settles immediately).
    assert "answerValidation" not in dict(events)["answer_done"]
    assert dict(events)["done"]["inDepth"]["status"] == "complete"


def test_chat_cancel_releases_router_lock():
    """The load-bearing preempt invariant: a cancelled _chat releases _ROUTER_LOCK so the next
    request (the preempting question) gets the single slot immediately."""

    async def _run():
        started = asyncio.Event()

        class Hanging:
            async def post(self, *_a, **_k):
                started.set()
                await asyncio.sleep(3600)  # hang until cancelled

        task = asyncio.create_task(team._chat(Hanging(), "m", [{"role": "user", "content": "x"}]))
        await started.wait()
        assert team._ROUTER_LOCK.locked()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert not team._ROUTER_LOCK.locked(), "router lock not released on cancel -> preempt can't free the slot"

    asyncio.run(_run())
