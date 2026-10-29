"""Rewrite-mode answer validator: the validator LOCALIZES each chart contradiction. Its verdict labels
the answer: a flagged answer is shown as written with a yellow note and needs_review metadata, and is
never replaced by the reviewer's correction or regenerated. Statements the deterministic date checks
confirmed are given to the reviewer as confirmed, and an error that targets one is set aside.

Selected by the profile's review prompt. Mocks only the model boundary (`_chat`)
and exercises the compiled stages end-to-end, branching on the response_format json_schema name so the
Answer synth, the label_verdict validator, and the In-Depth calls are served independently.

Run: pytest tests/test_validator_rewrite.py
"""

import asyncio
import json

import pytest

from server import team
from tests.factories import make_profile, run_profile, team_profile

_MESSAGES = [
    {"role": "system", "content": "envelope system"},
    {"role": "user", "content": "patient chart"},
    {"role": "user", "content": "the question"},
]
_RF = {"type": "json_schema", "json_schema": {"name": "chart_answer"}}


@pytest.fixture(autouse=True)
def _restore_chat_after_test():
    original_chat = team._chat
    try:
        yield
    finally:
        team._chat = original_chat


def _factory(calls, label_verdicts):
    """Mock _chat. Branches on the response_format json_schema name:
    "chart_answer"     (Answer synth)    -> {answer: ans-vN} (N = synth call index);
    "in_depth"         (In-Depth synth)  -> {claims:[...]};
    "label_verdict"  (rewrite validator) -> the Nth entry of `label_verdicts`, verbatim;
    "indepth_verdict"  -> {flag:[]} (no-op here);
    no response_format (orchestrator)    -> no tools (break to synthesis)."""
    state = {"answer_synth": 0, "indepth_synth": 0, "rw": 0}

    async def fake_chat(
        client,
        model,
        messages,
        *,
        tools=None,
        response_format=None,
        temperature=None,
        max_tokens=None,
        repeat_penalty=None,
        dry_multiplier=None,
        **kwargs,
    ):
        name = (response_format or {}).get("json_schema", {}).get("name")
        calls.append((model, name))
        if name == "chart_answer":
            n = state["answer_synth"]
            state["answer_synth"] += 1
            return {
                "content": json.dumps(
                    {"answer": f"ans-v{n}", "citations": [], "blocks": []}
                )
            }
        if name == "in_depth":
            n = state["indepth_synth"]
            state["indepth_synth"] += 1
            return {"content": json.dumps({"claims": [f"claim-A-v{n}"]})}
        if name == "label_verdict":
            v = (
                label_verdicts[min(state["rw"], len(label_verdicts) - 1)]
                if label_verdicts
                else {"answer_ok": True}
            )
            state["rw"] += 1
            return {"content": v if isinstance(v, str) else json.dumps(v)}
        if name == "indepth_verdict":
            return {"content": json.dumps({"flag": [], "issues": ""})}
        return {"content": "", "tool_calls": None}

    return fake_chat


def _counts(calls):
    return (
        sum(1 for _m, n in calls if n == "chart_answer"),
        sum(1 for _m, n in calls if n == "label_verdict"),
    )


def _run(label_verdicts, **kw):
    calls = []
    original_chat = team._chat
    try:
        team._chat = _factory(calls, label_verdicts)
        profile = team_profile(
            orchestrator="ORCH",
            answer="SYNTH",
            review="VALIDATOR",
            indepth="SYNTH",
            output="combined",
            review_prompt="validation-label",
            policies={"review_loops": int(kw.get("validator_max_loops", 1))},
        )
        out = asyncio.run(run_profile(profile, _MESSAGES, response_format=_RF))
        return calls, json.loads(out)
    finally:
        team._chat = original_chat


def _err(wrong, chart, fix):
    return {"wrong": wrong, "chart": chart, "fix": fix}


def _review_messages(answer="The patient is taking aspirin [1].", blocks=None):
    payload = {
        "schema_version": "answer_to_review.v1",
        "original_question": "What medications?",
        "answer": answer,
        "citations": [1],
        "blocks": blocks or [],
    }
    return [
        {"role": "system", "content": "envelope system"},
        {"role": "user", "content": "[1] Aspirin active"},
        {"role": "assistant", "content": answer},
        {
            "role": "user",
            "content": "Review this answer:\n```json\n" + json.dumps(payload) + "\n```",
        },
    ]


def _run_review(
    label_verdicts, *, answer="The patient is taking aspirin [1].", blocks=None
):
    calls = []
    original_chat = team._chat
    try:
        team._chat = _factory(calls, label_verdicts)
        profile = make_profile(
            topology="leg",
            stages=("context", "review"),
            models={"review": "REVIEWER"},
            prompts={"review": "validation-label"},
            output="review",
        )
        out = asyncio.run(
            run_profile(
                profile,
                _review_messages(answer=answer, blocks=blocks),
                response_format=_RF,
                context={"temporal": False},
            )
        )
        return calls, json.loads(out)
    finally:
        team._chat = original_chat


def test_answer_review_clean_pass_returns_checked_metadata():
    calls, env = _run_review(
        [{"answer_ok": True, "errors": [], "corrected_answer": ""}]
    )
    assert env["answer"] == "The patient is taking aspirin [1]."
    assert env["answerValidation"]["status"] == "checked"
    assert env["answerValidation"]["label"] == "Checked"
    assert env["confidence"]["answer"]["level"] == "green"
    assert [name for _model, name in calls].count("label_verdict") == 1


def test_answer_review_labels_a_flagged_answer_and_never_adopts_its_correction():
    calls, env = _run_review(
        [
            {
                "answer_ok": False,
                "errors": [_err("aspirin", "lisinopril [1]", "lisinopril")],
                "corrected_answer": "The patient is taking lisinopril [1].",
            },
            {"answer_ok": True, "errors": [], "corrected_answer": ""},
        ]
    )
    assert env["answer"] == "The patient is taking aspirin [1]."
    assert env["answerValidation"]["status"] == "needs_review"
    assert "originalAnswer" not in env["answerValidation"]
    assert env["answerValidation"]["issues"][0]["wrong"] == "aspirin"
    assert env["confidence"]["answer"]["level"] == "yellow"
    assert '"aspirin" against lisinopril [1]' in env["confidence"]["answer"]["note"]
    assert [name for _model, name in calls].count("label_verdict") == 1


def test_answer_review_labels_a_flag_that_offers_no_correction():
    _calls, env = _run_review(
        [
            {
                "answer_ok": False,
                "errors": [_err("aspirin", "no active med", "remove")],
                "corrected_answer": "",
            },
        ]
    )
    assert env["answer"] == "The patient is taking aspirin [1]."
    assert env["answerValidation"]["status"] == "needs_review"
    assert env["confidence"]["answer"]["level"] == "yellow"


def test_answer_review_ignores_an_error_whose_wrong_text_is_not_in_the_draft():
    calls, env = _run_review(
        [
            {
                "answer_ok": False,
                "errors": [
                    _err(
                        "lisinopril on 2 026-01-07",
                        "The chart says lisinopril on 2026-01-07 [1].",
                        "lisinopril on 2026-01-07",
                    )
                ],
                "corrected_answer": "The patient is taking lisinopril [1].",
            }
        ],
        answer="The patient is taking aspirin [1].",
    )

    assert env["answer"] == "The patient is taking aspirin [1]."
    assert env["answerValidation"]["status"] == "checked"
    assert env["answerValidation"]["issues"] == []
    assert [name for _model, name in calls].count("label_verdict") == 1


def test_answer_review_does_not_apply_a_mixed_localized_and_unlocalized_rewrite():
    _calls, env = _run_review(
        [
            {
                "answer_ok": False,
                "errors": [
                    _err("aspirin", "Lisinopril is active [1].", "lisinopril"),
                    _err("warfarin", "No warfarin record exists.", "remove"),
                ],
                "corrected_answer": "The patient is taking lisinopril [1].",
            }
        ],
        answer="The patient is taking aspirin [1].",
    )

    assert env["answer"] == "The patient is taking aspirin [1]."
    assert env["answerValidation"]["status"] == "needs_review"
    assert len(env["answerValidation"]["issues"]) == 1


def test_answer_review_derives_failure_from_localized_errors_when_flag_is_inconsistent():
    _calls, env = _run_review(
        [
            {
                "answer_ok": True,
                "errors": [
                    _err("aspirin", "Lisinopril is active [1].", "lisinopril")
                ],
                "corrected_answer": "",
            }
        ],
        answer="The patient is taking aspirin [1].",
    )

    assert env["answer"] == "The patient is taking aspirin [1]."
    assert env["answerValidation"]["status"] == "needs_review"
    assert len(env["answerValidation"]["issues"]) == 1


@pytest.mark.parametrize("malformed_verdict", ["{not-json", []])
def test_answer_review_does_not_mark_malformed_reviewer_output_checked(
    malformed_verdict,
):
    _calls, env = _run_review([malformed_verdict])

    assert env["answer"] == "The patient is taking aspirin [1]."
    assert env["answerValidation"]["status"] == "unavailable"
    assert env["confidence"]["answer"]["level"] == "yellow"


def _weight_review_messages(answer):
    question = (
        "Give only the earliest and latest documented weight measurements in the available "
        "past-year data, with exact date, value, and citation."
    )
    payload = {
        "schema_version": "answer_to_review.v1",
        "original_question": question,
        "answer": answer,
        "citations": [350, 19],
        "blocks": [],
    }
    return [
        {"role": "system", "content": "envelope system"},
        {
            "role": "user",
            "content": "\n".join(
                [
                    "[19] (2026-01-26) Finding — Weight (kg): 71.0 kg",
                    "[320] (2025-10-27) Finding — Weight (kg): 73.0 kg",
                    "[350] (2025-10-13) Finding — Weight (kg): 74.0 kg",
                ]
            ),
        },
        {"role": "assistant", "content": answer},
        {
            "role": "user",
            "content": "Review this answer:\n```json\n" + json.dumps(payload) + "\n```",
        },
    ]


def test_answer_review_sets_aside_an_error_on_an_endpoint_the_date_checks_confirmed():
    """The 21 Oct weight-endpoints turn: the answer named the right earliest weight, the reviewer
    said 73 kg on a later date was earliest, and the turn ended red. Code confirms the endpoint, the
    reviewer is told so, and its error on that statement no longer labels the answer."""
    answer = (
        "The earliest documented weight measurement in the past year was 74 kg on "
        "2025-10-13 [350], and the latest is 71 kg on 2026-01-26 [19]."
    )
    review_inputs = []

    async def fake_chat(client, model, messages, *, response_format=None, **kwargs):
        review_inputs.append(messages[-1]["content"])
        return {
            "content": json.dumps(
                {
                    "answer_ok": False,
                    "errors": [
                        _err(
                            "The earliest documented weight measurement in the past year was 74 kg",
                            "Weight (kg): 73 kg on 2025-10-27 [320]",
                            "The earliest documented weight measurement in the past year was 73 kg",
                        )
                    ],
                    "corrected_answer": answer.replace("74 kg", "73 kg"),
                }
            )
        }

    team._chat = fake_chat
    profile = make_profile(
        topology="leg",
        stages=("context", "review"),
        models={"review": "REVIEWER"},
        prompts={"review": "validation-label"},
        output="review",
        policies={"temporal_gate": "enforce", "anchor": "2026-06-20"},
    )
    env = json.loads(
        asyncio.run(
            run_profile(
                profile,
                _weight_review_messages(answer),
                response_format=_RF,
                context={"temporal_gate": "enforce"},
            )
        )
    )

    assert "=== CONFIRMED BY DATE CHECKS ===" in review_inputs[0]
    assert "74 kg on 2025-10-13" in review_inputs[0]
    assert env["answer"] == answer
    assert env["answerValidation"]["status"] == "checked"
    assert env["confidence"]["answer"]["level"] == "green"


@pytest.mark.parametrize(
    "wrong",
    [
        "this medication is safe",
        "The latest weight was 71 kg on 2026-01-26 [19], and this medication is safe",
    ],
    ids=["other-assertion", "whole-clause"],
)
def test_answer_review_keeps_an_error_on_an_assertion_beside_a_confirmed_endpoint(wrong):
    """The date checks confirm only the endpoint's date and value. A reviewer error on another
    assertion in the same clause, alone or together with the endpoint, still labels the answer."""
    answer = "The latest weight was 71 kg on 2026-01-26 [19], and this medication is safe."

    async def fake_chat(client, model, messages, *, response_format=None, **kwargs):
        return {
            "content": json.dumps(
                {
                    "answer_ok": False,
                    "errors": [_err(wrong, "No record supports a safety claim", "Remove the safety claim")],
                    "corrected_answer": "The latest weight was 71 kg on 2026-01-26 [19].",
                }
            )
        }

    team._chat = fake_chat
    profile = make_profile(
        topology="leg",
        stages=("context", "review"),
        models={"review": "REVIEWER"},
        prompts={"review": "validation-label"},
        output="review",
        policies={"temporal_gate": "enforce", "anchor": "2026-06-20"},
    )
    env = json.loads(
        asyncio.run(
            run_profile(
                profile,
                _weight_review_messages(answer),
                response_format=_RF,
                context={"temporal_gate": "enforce"},
            )
        )
    )

    assert env["answerValidation"]["status"] == "needs_review", env["answerValidation"]


def _dated_review_messages(answer):
    """Like _review_messages, but the chart context carries a dated appointment record so the
    deterministic temporal gate has something to check the (possibly reviewer-rewritten) answer
    against — mirrors test_scaffolding.py's _APPT_MSGS fixture."""
    payload = {
        "schema_version": "answer_to_review.v1",
        "original_question": "Does this patient have any upcoming appointments?",
        "answer": answer,
        "citations": [2],
        "blocks": [],
    }
    return [
        {"role": "system", "content": "envelope system"},
        {
            "role": "user",
            "content": "\n".join(
                [
                    "[1] (2026-01-07) Assessment — Scheduled visit: No",
                    "[2] (2026-01-07) Misc — Return visit date: 2026-01-07",
                ]
            ),
        },
        {"role": "assistant", "content": answer},
        {
            "role": "user",
            "content": "Review this answer:\n```json\n" + json.dumps(payload) + "\n```",
        },
    ]


def test_answer_review_correction_that_would_add_a_wrong_date_never_ships():
    """The draft answer is temporally CORRECT. The reviewer "corrects" it to a wrong upcoming-date
    claim. The reviewer only labels, so the correct draft ships and the wrong date never does."""
    calls = []
    team._chat = _factory(
        calls,
        [
            {
                "answer_ok": False,
                "errors": [_err("wording", "chart", "clarify")],
                "corrected_answer": "The next upcoming appointment is 2026-01-07 [2].",
            },
            {"answer_ok": True, "errors": [], "corrected_answer": ""},
        ],
    )
    profile = make_profile(
        topology="leg",
        stages=("context", "review"),
        models={"review": "REVIEWER"},
        prompts={"review": "validation-label"},
        output="review",
        policies={"temporal_gate": "enforce", "anchor": "2026-06-20"},
    )
    out = asyncio.run(
        run_profile(
            profile,
            _dated_review_messages(
                "No upcoming appointment is documented after 2026-06-20 [2]."
            ),
            response_format=_RF,
            context={"temporal_gate": "enforce"},
        )
    )
    env = json.loads(out)
    assert "upcoming appointment is 2026-01-07" not in env["answer"], env["answer"]
    assert env["answer"].startswith(
        "No upcoming appointment is documented after 2026-06-20"
    ), env["answer"]


def _factory_answers(calls, answers, label_verdicts):
    """Like _factory but the writer returns answers[N] (so a test can force a non-substantive draft)."""
    state = {"answer_synth": 0, "indepth_synth": 0, "rw": 0}

    async def fake_chat(
        client,
        model,
        messages,
        *,
        tools=None,
        response_format=None,
        temperature=None,
        max_tokens=None,
        repeat_penalty=None,
        dry_multiplier=None,
        **kwargs,
    ):
        name = (response_format or {}).get("json_schema", {}).get("name")
        calls.append((model, name))
        if name == "chart_answer":
            n = state["answer_synth"]
            state["answer_synth"] += 1
            ans = answers[min(n, len(answers) - 1)]
            return {
                "content": json.dumps({"answer": ans, "citations": [], "blocks": []})
            }
        if name == "in_depth":
            n = state["indepth_synth"]
            state["indepth_synth"] += 1
            return {"content": json.dumps({"claims": [f"claim-A-v{n}"]})}
        if name == "label_verdict":
            v = (
                label_verdicts[min(state["rw"], len(label_verdicts) - 1)]
                if label_verdicts
                else {"answer_ok": True}
            )
            state["rw"] += 1
            return {"content": json.dumps(v)}
        if name == "indepth_verdict":
            return {"content": json.dumps({"flag": [], "issues": ""})}
        return {"content": "", "tool_calls": None}

    return fake_chat


def test_nonsubstantive_answer_ships_red_not_green():
    """THE SUBSTANCE GATE: a "." answer the LLM validator can't fault (no claim to contradict) must NOT
    ship green. After retries that stay non-substantive, the hub ships the fallback with RED confidence.
    """
    calls = []
    # writer always emits "."; validator finds nothing to localize (clean) -> w/o the gate this is green.
    team._chat = _factory_answers(
        calls, ["."], [{"answer_ok": True, "errors": [], "corrected_answer": ""}]
    )
    profile = team_profile(
        orchestrator="ORCH",
        answer="SYNTH",
        review="VALIDATOR",
        indepth="SYNTH",
        output="combined",
        review_prompt="validation-label",
    )
    env = json.loads(asyncio.run(run_profile(profile, _MESSAGES, response_format=_RF)))
    assert env["confidence"]["answer"]["level"] == "red", env["confidence"]
    assert env["answer"].strip() != ".", env["answer"]
    assert any(ch.isalnum() for ch in env["answer"]), env[
        "answer"
    ]  # fallback prose, not punctuation


def test_clean_pass_is_green_and_untouched():
    """No localized contradiction -> green, the original answer ships, validator runs ONCE, no re-synth."""
    calls, env = _run([{"answer_ok": True, "errors": [], "corrected_answer": ""}])
    synth, rw = _counts(calls)
    assert synth == 1 and rw == 1, calls  # one synth, one audit, NO regeneration
    assert env["confidence"]["answer"]["level"] == "green"
    assert "ans-v0" in env["answer"]


def test_flagged_answer_is_neither_rewritten_nor_regenerated():
    """Flagged with a correction -> the draft ships as written, labelled yellow; the writer is not asked
    again and the reviewer is not asked to recheck a rewrite."""
    calls, env = _run(
        [
            {
                "answer_ok": False,
                "errors": [_err("ans-v0", "weight 71 kg on 2026-01-26 [15]", "71 kg")],
                "corrected_answer": "The patient weighs 71 kg [15].",
            },
            {"answer_ok": True, "errors": [], "corrected_answer": ""},
        ]
    )
    synth, rw = _counts(calls)
    assert synth == 1, calls
    assert rw == 1, calls
    assert env["confidence"]["answer"]["level"] == "yellow"
    assert "ans-v0" in env["answer"] and "71 kg" not in env["answer"]
