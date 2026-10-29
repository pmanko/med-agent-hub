"""The labels a clinician sees on an In-Depth: why a claim is marked, and how marked it is."""

from __future__ import annotations

import asyncio
import json

import server.engine as engine
import server.team as team


def _attribution_reply(entries):
    async def fake_chat(_client, _model, _messages, **kwargs):
        assert kwargs["response_format"]["json_schema"]["name"] == "attribution_verdicts"
        return {"content": json.dumps({"pairs": entries})}

    return fake_chat


def test_attribution_check_reports_the_facts_its_record_does_not_state(monkeypatch):
    monkeypatch.setattr(
        team,
        "_chat",
        _attribution_reply(
            [
                {"facts": [{"fact": "Nevirapine", "found": True}], "verdict": "YES"},
                {
                    "facts": [
                        {"fact": "fever on 2025-12-31", "found": True},
                        {"fact": "productive cough on 2025-10-22", "found": False},
                    ],
                    "verdict": "NO",
                },
            ]
        ),
    )
    reasons: list = []

    verdicts = asyncio.run(
        team._entailment_verdicts(
            None,
            "G",
            [("[1] Drug order: Nevirapine", "On nevirapine."), ("[33] Fever", "Fever and cough.")],
            attribution_only=True,
            reasons=reasons,
        )
    )

    assert verdicts == [True, False]
    assert reasons == [None, "productive cough on 2025-10-22"]


def test_an_unsupported_indepth_citation_carries_what_was_not_found(monkeypatch):
    monkeypatch.setattr(
        team,
        "_chat",
        _attribution_reply(
            [
                {
                    "facts": [{"fact": "productive cough on 2025-10-22", "found": False}],
                    "verdict": "NO",
                }
            ]
        ),
    )
    claim = "A productive cough was reported on 2025-10-22 [1]."
    mappings = [{"index": 1, "date": "2026-05-20", "text": "Program: Tuberculosis treatment program"}]
    references = team._resolve_references(
        [1], mappings, answer=claim, grounding_status="unchecked", answer_usage_location="indepth"
    )

    grounded = asyncio.run(
        team._ground_references(None, "G", claim, references, mappings, attribution_only=True)
    )

    assert grounded[0]["groundingStatus"] == "unsupported"
    assert grounded[0]["groundingChecks"][0]["not_found"] == "productive cough on 2025-10-22"


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
