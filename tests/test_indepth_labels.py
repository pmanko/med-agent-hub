"""The labels a clinician sees on an In-Depth: why a claim is marked, and how marked it is."""

from __future__ import annotations

import asyncio
import json

import server.engine as engine
import server.team as team


def _attribution_reply(entries):
    async def fake_chat(_client, _model, _messages, **kwargs):
        assert kwargs["response_format"]["json_schema"]["name"] == "attribution_verdicts"
        assert kwargs["dry_multiplier"] == 0.0
        return {"content": json.dumps({"pairs": entries})}

    return fake_chat


def test_the_verdict_comes_from_the_fact_list_not_the_checkers_word():
    found = [{"fact": "Nevirapine", "status": "found"}]
    not_found = [{"fact": "fever on 2025-12-31", "status": "found"},
                 {"fact": "productive cough on 2025-10-22", "status": "not_found"}]
    differs = [{"fact": "41 kg on 2026-01-07", "status": "differs", "source_value": "52 kg"}]

    assert team._support_from_facts(found)["support"] == "supported"
    assert team._support_from_facts(not_found) == {
        "support": "not_addressed",
        "facts": not_found,
        "not_found": "productive cough on 2025-10-22",
        "chart_value": None,
    }
    verdict = team._support_from_facts(differs)
    assert verdict["support"] == "contradicted"
    assert verdict["chart_value"] == "41 kg on 2026-01-07: the record states 52 kg"
    assert team._support_from_facts([])["support"] == "unchecked"


def test_citation_check_returns_one_fact_derived_verdict_per_pair(monkeypatch):
    monkeypatch.setattr(
        team,
        "_chat",
        _attribution_reply(
            [
                {"facts": [{"fact": "Nevirapine", "status": "found"}]},
                {"facts": [{"fact": "cough on 2025-10-22", "status": "not_found"}]},
            ]
        ),
    )

    verdicts = asyncio.run(
        team._entailment_verdicts(
            None,
            "G",
            [("[1] Drug order: Nevirapine", "On nevirapine."), ("[33] Fever", "Fever and cough.")],
        )
    )

    assert [v["support"] for v in verdicts] == ["supported", "not_addressed"]
    assert verdicts[1]["not_found"] == "cough on 2025-10-22"


def test_a_contradicted_indepth_citation_carries_what_the_record_states(monkeypatch):
    monkeypatch.setattr(
        team,
        "_chat",
        _attribution_reply(
            [{"facts": [{"fact": "weight 44 kg", "status": "differs", "source_value": "41 kg"}]}]
        ),
    )
    claim = "The weight was 44 kg on 2026-01-07 [1]."
    mappings = [{"index": 1, "date": "2026-01-07", "text": "Weight (kg): 41 kg"}]
    references = team._resolve_references(
        [1], mappings, answer=claim, grounding_status="unchecked", answer_usage_location="indepth"
    )

    grounded = asyncio.run(team._ground_references(None, "G", claim, references, mappings))

    assert grounded[0]["support"] == "contradicted"
    assert grounded[0]["groundingStatus"] == "unsupported"
    assert grounded[0]["groundingChecks"][0]["chart_value"] == "weight 44 kg: the record states 41 kg"


def test_a_fact_the_record_does_not_hold_is_not_a_failure(monkeypatch):
    monkeypatch.setattr(
        team,
        "_chat",
        _attribution_reply([{"facts": [{"fact": "52 kg on 2025-10-22", "status": "not_found"},
                                       {"fact": "41 kg on 2026-01-07", "status": "found"}]}]),
    )
    claim = "Weight fell from 52 kg on 2025-10-22 to 41 kg on 2026-01-07 [21]."
    mappings = [{"index": 21, "date": "2026-01-07", "text": "Weight (kg): 41 kg"}]
    references = team._resolve_references([21], mappings, answer=claim, grounding_status="unchecked")

    grounded = asyncio.run(team._ground_references(None, "G", claim, references, mappings))

    assert grounded[0]["support"] == "not_addressed"
    assert grounded[0]["grounded"] is None
    issues = team._grounding_failure_issues(grounded)
    assert [issue["status"] for issue in issues] == ["info"]
    assert "52 kg on 2025-10-22" in issues[0]["reason"]


def test_a_date_check_mark_says_what_the_chart_has():
    def entry(check_id, reason):
        return {"gate": {"checks": [{"id": check_id, "status": "fail", "reason": reason}]}}

    labels = [
        engine._date_check_label(entry("single_point_trend", "CD4 count has fewer than two dated points, so no trend is supported.")),
        engine._date_check_label(entry("date_value_binding", "The answer binds value 41.0 to 2025-10-22, but temporal_facts bind it to [52.0].")),
        engine._date_check_label(entry("date_format", "The answer emits '2026-01_26', which is not an exact YYYY-MM-DD string copied from date_ledger.")),
        engine._date_check_label(entry("last_visit", "The answer gives a last-visit date other than the deterministic last clinical encounter 2026-01-07.")),
        engine._date_check_label({}),
    ]

    assert "CD4 count" in labels[0]
    assert "41" in labels[1] and "2025-10-22" in labels[1] and "41.0" not in labels[1]
    assert "2026-01_26" in labels[2]
    assert "2026-01-07" in labels[3]
    assert labels[4]
    assert len(set(labels)) == len(labels)


def test_reordered_citations_alone_do_not_make_an_answer_edited():
    state = engine._State(messages=[])
    state.initial_answer_text = "Weight fell from 52 kg [287] to 41 kg [21]."
    state.answer_text = state.initial_answer_text
    state.initial_answer_citations = [21, 287]
    state.citations = [287, 21]
    state.answer_check_edited = True

    assert engine._initial_answer_changed(state) is False

    state.citations = [287, 22]
    assert engine._initial_answer_changed(state) is True


def test_a_reviewer_flag_counts_only_when_it_quotes_the_chart():
    chart = "[1] (2026-01-26) Drug order: Nevirapine. Action: NEW\n[20] Current WHO HIV stage: WHO stage 3 adult"

    assert team._quoted_in_chart({"chart": "WHO stage 1 adult on 2025-12-17 [53]"}, chart) is False
    assert team._quoted_in_chart({"chart": "Current WHO HIV stage: WHO stage 3 adult"}, chart) is True
    assert team._quoted_in_chart({"chart": "the nevirapine order [1]"}, chart) is True
    assert team._quoted_in_chart({"chart": "No record supports a safety claim"}, chart) is False


def test_stream_displays_missing_fact_and_source_number_in_indepth_answer(monkeypatch):
    from dataclasses import replace
    from server.levels_loader import get_profile
    from tests.factories import patient_source_registry

    chart = "[1] (2026-05-20) Program — Tuberculosis treatment program\n"
    claim = "A productive cough was reported on 2025-10-22 [1]."

    async def fake_answer(*_args, **_kwargs):
        return "Enrolled in the tuberculosis treatment program [1].", [1], []

    async def fake_substantive(*_args, **kwargs):
        return (
            kwargs["answer_text"],
            kwargs["citations"],
            kwargs["blocks"],
            {"level": "green", "note": ""},
        )

    async def fake_indepth(*_args, **_kwargs):
        return [claim], {"level": "green", "note": ""}

    async def fake_chat(_client, _model, _messages, **kwargs):
        rf_name = ((kwargs.get("response_format") or {}).get("json_schema") or {}).get("name")
        if rf_name == "attribution_verdicts":
            return {
                "content": json.dumps(
                    {
                        "pairs": [
                            {
                                "facts": [
                                    {"fact": "productive cough on 2025-10-22", "status": "not_found"}
                                ]
                            }
                        ]
                    }
                )
            }
        return {"content": json.dumps({"verdicts": ["YES"]})}

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_ensure_substantive_answer", fake_substantive)
    monkeypatch.setattr(team, "_gen_indepth", fake_indepth)
    monkeypatch.setattr(team, "_chat", fake_chat)
    monkeypatch.setattr(team, "_write_trace", lambda *_a, **_k: None)

    class _Counter:
        async def count(self, *_args):
            return 1

        async def count_chat(self, *_args):
            return 1

    registry = patient_source_registry(
        chart,
        [
            {
                "resourceType": "obs",
                "resourceUuid": "tb-prog",
                "date": "2026-05-20",
                "text": "(2026-05-20) Program: Tuberculosis treatment program",
            }
        ],
    )
    profile = get_profile("single-12b-checked")
    profile = replace(
        profile,
        stages=tuple(stage for stage in profile.stages if stage != "review"),
        policies={**profile.policies, "anchor": "2026-06-20", "drug_safety": False},
    )
    request = engine.ExecutionRequest(
        profile=profile,
        messages=[{"role": "user", "content": "What programs is the patient in?"}],
        patient="synthetic",
        source_registry=registry,
        token_counter=_Counter(),
    )

    payload = json.loads(asyncio.run(engine.drain_profile(request)))
    assert (
        payload["inDepth"]["validation"]["citation_checks"][0]["reason"]
        == "[1] does not hold: productive cough on 2025-10-22."
    )
    assert {
        "check": "citation",
        "where": 1,
        "status": "info",
        "says": "[1] does not hold: productive cough on 2025-10-22.",
    } in payload["confidence"]["in_depth"]["findings"]


def test_indepth_citation_support_probe_fixture_exercises_separate_and_folded_contracts(
    monkeypatch,
):
    from pathlib import Path

    fixture_path = (
        Path(__file__).resolve().parent / "fixtures" / "indepth-citation-support-probes.json"
    )
    fixture = json.loads(fixture_path.read_text())
    probes = fixture["probes"]
    assert len(probes) == 10

    monkeypatch.setattr(
        team,
        "_chat",
        _attribution_reply([{"facts": probe["facts"]} for probe in probes]),
    )
    pairs = [(probe["source"], probe["statement"]) for probe in probes]
    verdicts = asyncio.run(team._entailment_verdicts(None, "gemma-4-12b", pairs))
    assert [v["support"] for v in verdicts] == [probe["expected"] for probe in probes]
    assert sum(v["support"] == "supported" for v in verdicts) == 5
    assert sum(v["support"] == "not_addressed" for v in verdicts) == 4
    assert sum(v["support"] == "contradicted" for v in verdicts) == 1

    claims = [
        f"{probe['statement']} " + "".join(f"[{idx}]" for idx in probe["source_indices"])
        for probe in probes
    ]
    folded_messages = team._indepth_validation_messages(
        chart="\n".join(probe["source"] for probe in probes),
        gathered="",
        answer_text="Summary answer.",
        claims=claims,
        validation_prompt="validation-label",
        citation_support=True,
    )
    assert "=== CITATION SUPPORT ===" in folded_messages[-1]["content"]
    unsupported_indices = [
        index for index, probe in enumerate(probes, 1) if probe["expected"] == "contradicted"
    ]
    not_addressed_indices = [
        index for index, probe in enumerate(probes, 1) if probe["expected"] == "not_addressed"
    ]

    async def fake_folded_chat(_client, _model, _messages, **_kwargs):
        return {
            "content": json.dumps(
                {
                    "flag": [],
                    "issues": "",
                    "unsupported": unsupported_indices,
                    "not_addressed": not_addressed_indices,
                }
            )
        }

    monkeypatch.setattr(team, "_chat", fake_folded_chat)
    folded_verdict = asyncio.run(
        team._validate_indepth_verdict(
            None,
            "gemma-4-12b",
            chart="\n".join(probe["source"] for probe in probes),
            gathered="",
            answer_text="Summary answer.",
            claims=claims,
            max_tokens=256,
            temperature=0.0,
            repeat_penalty=None,
            dry=None,
            citation_support=True,
        )
    )
    assert folded_verdict["unsupported"] == [8]
    assert folded_verdict["not_addressed"] == [5, 6, 7, 10]
