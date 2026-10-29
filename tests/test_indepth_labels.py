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
