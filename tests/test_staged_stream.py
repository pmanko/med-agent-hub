"""Phased staged streaming (run_team_stream): the hub-owned answer -> (optional validation) -> in-depth
flow that the chartsearchai controller relays. Stage helpers are stubbed so these assert the
generator's CONTRACT — event sequence, conditional validation, and hub-side reference resolution —
not the LLM. A separate test pins the cancellation invariant (a cancelled _chat frees the router lock)."""

import asyncio
import json

import server.openai_compat as openai_compat
import server.team as team

_MAPPINGS = [
    {"index": 1, "resourceType": "obs", "resourceUuid": "u1", "date": "2025-01-01", "text": "observed data"},
    {"index": 2, "resourceType": "order", "resourceUuid": "u2", "date": "2025-02-02", "text": "different order"},
]


def _stub_common(monkeypatch):
    async def fake_retrieve(_patient):
        return "[1] obs\n[2] order\n", _MAPPINGS, []

    def fake_gate(**k):
        return k["answer_text"], k["citations"], k["blocks"], {"mode": "off", "status": "ok", "applied": "none"}, None

    async def fake_indepth(*_a, **_k):
        return (["claim one", "claim two"], {"level": "green", "note": ""})

    async def fake_ground(_client, _model, _answer, references, _mappings):
        # Deterministic stand-in for the real entailment call: these sequencing/wiring tests care
        # that grounding runs exactly once, after review, on the FINAL references — not what a real
        # LLM would verdict. The entailment call itself has its own dedicated tests below.
        out = []
        for ref in references:
            r = dict(ref)
            r["grounded"] = True
            r["groundingStatus"] = "verified"
            out.append(r)
        return out

    monkeypatch.setattr(team, "_retrieve_chart", fake_retrieve)
    monkeypatch.setattr(team, "_apply_temporal_gate", fake_gate)
    monkeypatch.setattr(team, "_gen_indepth", fake_indepth)
    monkeypatch.setattr(team, "_merge_temporal_gate_conf", lambda conf, _gate: conf)
    monkeypatch.setattr(team, "_write_trace", lambda *_a, **_k: None)
    monkeypatch.setattr(team, "_ground_references", fake_ground)


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
        {
            "index": 1, "resourceType": "obs", "resourceUuid": "u1", "date": "2025-01-01",
            "sourceText": "observed data", "groundingStatus": "checking", "grounded": None,
        }
    ]
    # answer_validation: the correction is surfaced (edited + original), refs re-resolved for the new citation
    assert ev["answer_validation"]["answerValidation"]["status"] == "edited"
    assert ev["answer_validation"]["answerValidation"]["originalAnswer"] == "Ans [1]."
    assert ev["answer_validation"]["references"] == [
        {
            "index": 2, "resourceType": "order", "resourceUuid": "u2", "date": "2025-02-02",
            "sourceText": "different order", "groundingStatus": "checking", "grounded": None,
        }
    ]
    # done: in-depth complete
    assert ev["done"]["inDepth"]["status"] == "complete"
    assert "claim one" in ev["done"]["inDepth"]["answer"]
    assert ev["done"]["model"] == "lvl"
    assert ev["done"]["references"] == [
        {
            "index": 2, "resourceType": "order", "resourceUuid": "u2", "date": "2025-02-02",
            "sourceText": "different order", "groundingStatus": "verified", "grounded": True,
        }
    ]


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
    assert dict(events)["answer_done"]["references"][0]["groundingStatus"] == "checking"
    assert dict(events)["done"]["references"][0]["groundingStatus"] == "verified"
    assert dict(events)["done"]["inDepth"]["status"] == "complete"


def test_stage_drain_returns_final_post_review_envelope(monkeypatch):
    _stub_common(monkeypatch)

    async def fake_answer(*_a, **_k):
        return ("Ans [1].", [1], [])

    async def fake_validate(_client, **_k):
        return ("Ans fixed [2].", [2], [], {"level": "yellow", "note": "fixed a claim"})

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_validate_and_refine_answer", fake_validate)

    async def _run():
        return await team.run_team_stage_drain(
            messages=[{"role": "user", "content": "q?"}], patient="p", context={"temporal": False},
            model_label="single-12b-checked", synth_model="M", validator_model="V",
        )

    env = json.loads(asyncio.run(_run()))
    assert env["answer"] == "Ans fixed [2]."
    assert env["citations"] == [2]
    assert env["answerValidation"]["status"] == "edited"
    assert env["inDepth"]["status"] == "complete"
    assert env["references"][0]["index"] == 2
    assert env["references"][0]["groundingStatus"] == "verified"


def _fake_chat_returning_verdicts(verdicts_by_call):
    """Stub for team._chat that returns the next queued verdicts list as an entailment response."""
    calls = []

    async def fake_chat(_client, model, messages, **kwargs):
        calls.append({"model": model, "messages": messages, **kwargs})
        verdicts = verdicts_by_call[len(calls) - 1]
        return {"content": json.dumps({"verdicts": verdicts})}

    return fake_chat, calls


def test_entailment_verdicts_maps_yes_no_positionally(monkeypatch):
    fake_chat, calls = _fake_chat_returning_verdicts([["YES", "NO"]])
    monkeypatch.setattr(team, "_chat", fake_chat)

    async def _run():
        return await team._entailment_verdicts(
            client=None, model="M",
            pairs=[("source A", "claim A"), ("source B", "claim B")],
        )

    verdicts = asyncio.run(_run())
    assert verdicts == [True, False]
    assert len(calls) == 1  # ONE batched call for both pairs, not one per pair
    assert calls[0]["response_format"]["json_schema"]["name"] == "entailment_verdicts"


def test_entailment_verdicts_fails_open_to_unchecked_on_call_error(monkeypatch):
    async def broken_chat(*_a, **_k):
        raise RuntimeError("router unreachable")

    monkeypatch.setattr(team, "_chat", broken_chat)

    async def _run():
        return await team._entailment_verdicts(client=None, model="M", pairs=[("s", "c")])

    assert asyncio.run(_run()) == [None]


def test_entailment_verdicts_fails_open_on_malformed_response(monkeypatch):
    async def malformed_chat(*_a, **_k):
        return {"content": "not json"}

    monkeypatch.setattr(team, "_chat", malformed_chat)

    async def _run():
        return await team._entailment_verdicts(client=None, model="M", pairs=[("s", "c")])

    assert asyncio.run(_run()) == [None]


def test_ground_references_verified_for_an_entailed_paraphrase_despite_low_word_overlap(monkeypatch):
    # The old lexical heuristic required ~45% token overlap; a clean paraphrase like this would have
    # scored well below that and come back unsupported/unchecked. Entailment must verify it anyway.
    fake_chat, calls = _fake_chat_returning_verdicts([["YES"]])
    monkeypatch.setattr(team, "_chat", fake_chat)
    refs = [{"index": 4, "resourceType": "encounter", "resourceUuid": "enc-4", "date": "2026-01-26"}]
    mappings = [{
        "index": 4, "resourceType": "encounter", "resourceUuid": "enc-4", "date": "2026-01-26",
        "text": "Pt seen today for a routine checkup; no acute concerns raised.",
    }]

    async def _run():
        return await team._ground_references(
            client=None, model="M",
            answer="The patient had a well visit on 2026-01-26 [4].",
            references=refs, mappings=mappings,
        )

    grounded = asyncio.run(_run())
    assert grounded[0]["groundingStatus"] == "verified"
    assert grounded[0]["grounded"] is True
    assert len(calls) == 1


def test_ground_references_unsupported_for_high_overlap_but_negated_statement(monkeypatch):
    # High lexical overlap (shares "diabetes", "family history") but describes a RELATIVE, not the
    # patient — the failure mode the token-overlap heuristic could not catch.
    fake_chat, calls = _fake_chat_returning_verdicts([["NO"]])
    monkeypatch.setattr(team, "_chat", fake_chat)
    refs = [{"index": 7, "resourceType": "obs", "resourceUuid": "obs-7", "date": "2026-01-01"}]
    mappings = [{
        "index": 7, "resourceType": "obs", "resourceUuid": "obs-7", "date": "2026-01-01",
        "text": "Family history of diabetes (mother). Patient's own glucose panel is normal.",
    }]

    async def _run():
        return await team._ground_references(
            client=None, model="M",
            answer="The patient has a diagnosis of diabetes [7].",
            references=refs, mappings=mappings,
        )

    grounded = asyncio.run(_run())
    assert grounded[0]["groundingStatus"] == "unsupported"
    assert grounded[0]["grounded"] is False
    assert len(calls) == 1


def test_ground_references_caps_pairs_and_unchecks_the_rest(monkeypatch):
    n = team._ENTAILMENT_MAX_PAIRS + 3
    fake_chat, calls = _fake_chat_returning_verdicts([["YES"] * team._ENTAILMENT_MAX_PAIRS])
    monkeypatch.setattr(team, "_chat", fake_chat)
    refs = [
        {"index": i, "resourceType": "obs", "resourceUuid": f"u{i}", "date": "2026-01-01"}
        for i in range(1, n + 1)
    ]
    mappings = [
        {"index": i, "resourceType": "obs", "resourceUuid": f"u{i}", "date": "2026-01-01",
         "text": f"finding number {i}"}
        for i in range(1, n + 1)
    ]
    answer = " ".join(f"Claim about finding {i} [{i}]." for i in range(1, n + 1))

    async def _run():
        return await team._ground_references(
            client=None, model="M", answer=answer, references=refs, mappings=mappings)

    grounded = asyncio.run(_run())
    assert len(calls) == 1  # still exactly one batched call, capped, never unbounded
    verified = [r for r in grounded if r["groundingStatus"] == "verified"]
    unchecked = [r for r in grounded if r["groundingStatus"] == "unchecked"]
    assert len(verified) == team._ENTAILMENT_MAX_PAIRS
    assert len(unchecked) == 3


def test_named_sse_emits_heartbeats_while_a_leg_stalls():
    # Gate 6: without heartbeats, an intermediary/browser sees a dead-looking connection during a
    # long leg and there is nothing for an abort to interrupt until the NEXT event finally arrives.
    async def slow_gen():
        await asyncio.sleep(0.05)
        yield ("answer_done", '{"answer":"hi"}')

    async def _run():
        return [chunk async for chunk in openai_compat._named_sse(slow_gen(), interval_s=0.01)]

    chunks = asyncio.run(_run())
    heartbeats = [c for c in chunks if c == ": hb\n\n"]
    assert len(heartbeats) >= 2, f"expected repeated heartbeats while stalled, got {chunks}"
    assert chunks[-1] == 'event: answer_done\ndata: {"answer":"hi"}\n\n'


def test_named_sse_cancel_mid_heartbeat_still_frees_router_lock():
    # The heartbeat wait must not weaken the existing cancel-frees-the-lock invariant: cancelling
    # while parked on a heartbeat still has to unwind whatever the hub is doing underneath.
    async def _run():
        started = asyncio.Event()

        class Hanging:
            async def post(self, *_a, **_k):
                started.set()
                await asyncio.sleep(3600)

        async def hanging_gen():
            await team._chat(Hanging(), "m", [{"role": "user", "content": "x"}])
            yield ("done", "{}")  # unreachable; _chat never returns

        stream = openai_compat._named_sse(hanging_gen(), interval_s=0.01)
        agen = stream.__aiter__()
        first = asyncio.ensure_future(agen.__anext__())
        await started.wait()
        assert team._ROUTER_LOCK.locked()
        first.cancel()
        try:
            await first
        except asyncio.CancelledError:
            pass
        await agen.aclose()
        assert not team._ROUTER_LOCK.locked(), "cancelling mid-heartbeat must still free the router lock"

    asyncio.run(_run())


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


def test_run_team_stream_client_disconnect_mid_indepth_frees_router_lock(monkeypatch):
    """Gate 6, at the layer that owns it: a client disconnect WHILE THE IN-DEPTH LEG IS GENERATING
    inside run_team_stream must unwind the in-flight _chat, free the single _ROUTER_LOCK, and let the
    next request (the preempting question) acquire the slot. This is the timing invariant the e2e
    preempt spec deliberately does NOT assert (real model latency swamps the signal); a fake _chat
    makes it deterministic here. Stronger than test_chat_cancel_releases_router_lock: it drives the
    WHOLE staged generator to the in-depth phase, not just _chat in isolation."""
    _stub_common(monkeypatch)

    async def fake_synth(*_a, **_k):
        # Bypass the answer leg's real _chat so the ONLY router-lock holder under test is in-depth.
        return ("Answer.", [], [])

    monkeypatch.setattr(team, "_synthesize_answer", fake_synth)

    indepth_running = asyncio.Event()

    class HangingChat:
        async def post(self, *_a, **_k):
            indepth_running.set()
            await asyncio.sleep(3600)  # hang until cancelled

    async def hanging_indepth(*_a, **_k):
        # The in-depth leg does REAL router work: acquire the single slot via _chat, then hang —
        # exactly the state a mid-in-depth disconnect must be able to interrupt.
        await team._chat(HangingChat(), "indepth-model", [{"role": "user", "content": "deep dive"}])
        return (["claim"], {"level": "green", "note": ""})

    monkeypatch.setattr(team, "_gen_indepth", hanging_indepth)

    async def _run():
        agen = team.run_team_stream(
            [{"role": "user", "content": "q?"}], patient="p", context={"temporal": False},
            model_label="lvl", synth_model="m", validator_model=None,
        ).__aiter__()

        # Drive the generator to the in-depth phase. validator=None → no answer_validation event;
        # sequence is answer_done -> indepth_pending -> (in-depth runs) -> indepth_done.
        seen = []
        while True:
            name, _ = await agen.__anext__()
            seen.append(name)
            if name == "indepth_pending":
                break
        assert seen == ["answer_done", "indepth_pending"], seen

        # The next step runs the (hanging) in-depth leg, which grabs the single router slot.
        step = asyncio.ensure_future(agen.__anext__())
        await indepth_running.wait()
        assert team._ROUTER_LOCK.locked(), "the in-depth leg must hold the router slot while generating"

        # Client disconnects → the driving task is cancelled mid-in-depth.
        step.cancel()
        try:
            await step
        except asyncio.CancelledError:
            pass
        await agen.aclose()

        assert not team._ROUTER_LOCK.locked(), "a disconnect mid-in-depth must free the router slot"

        # The preempting request now acquires the single slot immediately (would hang if still held).
        await asyncio.wait_for(team._ROUTER_LOCK.acquire(), timeout=1.0)
        team._ROUTER_LOCK.release()

    asyncio.run(_run())


def test_run_team_stream_team_scaffolding_gathers_via_the_tool_loop(monkeypatch):
    # Gate 13: a team-scaffolded staged profile (solo=False) must gather the SAME way run_team
    # does — the orchestrator tool loop consulting medical_expert/kb_search — not skip straight to
    # synthesis the way every solo single-writer staged profile does.
    _stub_common(monkeypatch)
    calls = []

    async def fake_chat(_client, model, _messages, *, tools=None, response_format=None, **_kwargs):
        calls.append(model)
        if response_format is not None:
            return {"content": json.dumps({"answer": "Ans.", "citations": [], "blocks": []})}
        orchestrator_turns = sum(1 for m in calls if m == "orch-model")
        if tools is not None and orchestrator_turns == 1:
            return {
                "role": "assistant", "content": None,
                "tool_calls": [{"id": "t1", "function": {
                    "name": "medical_expert", "arguments": json.dumps({"query": "interpret"})}}],
            }
        return {"content": "ok", "tool_calls": None}

    monkeypatch.setattr(team, "_chat", fake_chat)

    events = _collect(
        synth_model="M", validator_model=None,
        solo=False, orchestrator_model="orch-model", expert_model="expert-model",
    )
    names = [n for n, _ in events]
    assert names == ["answer_done", "indepth_pending", "indepth_done", "done"]
    assert "orch-model" in calls, "team scaffolding must run the orchestrator tool loop"
    assert "expert-model" in calls, "the tool-called medical expert must actually be consulted"


def test_run_team_stream_solo_profile_never_runs_the_tool_loop(monkeypatch):
    # The default (solo=True, every existing staged profile) must stay byte-identical: no
    # orchestrator call at all, gather is a pure no-op.
    _stub_common(monkeypatch)
    calls = []

    async def fake_chat(_client, model, _messages, **_kwargs):
        calls.append(model)
        return {"content": "unused"}

    monkeypatch.setattr(team, "_chat", fake_chat)

    async def fake_answer(*_a, **_k):
        return ("Ans.", [], [])

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)

    _collect(synth_model="M", validator_model=None)  # solo defaults to True
    assert calls == [], f"solo staged profile must not touch the orchestrator/expert models, got {calls}"


def test_run_team_stream_derives_gather_from_the_stage_plan_not_a_trusted_flag(monkeypatch):
    # Gate 3: the engine must EXECUTE stage_plan_for_level, not just carry it as descriptive test
    # metadata. Pass level_id="med-agent-team-med-validated" (a real team/staged config) but the
    # WRONG solo=True kwarg — if the runtime actually consults the plan, it must still gather.
    _stub_common(monkeypatch)
    calls = []

    async def fake_chat(_client, model, _messages, *, tools=None, response_format=None, **_kwargs):
        calls.append(model)
        if response_format is not None:
            return {"content": json.dumps({"answer": "Ans.", "citations": [], "blocks": []})}
        return {"content": "ok", "tool_calls": None}

    monkeypatch.setattr(team, "_chat", fake_chat)

    _collect(
        synth_model="qwen2.5-14b", validator_model=None,
        solo=True,  # deliberately wrong — the stage plan (not this kwarg) must win
        orchestrator_model="gemma-e4b-q8",
        level_id="med-agent-team-med-validated",
    )
    assert "gemma-e4b-q8" in calls, (
        "the orchestrator was never called — run_team_stream trusted the caller's solo=True kwarg "
        "instead of executing stage_plan_for_level(level_id), which says this level has a gather stage"
    )
