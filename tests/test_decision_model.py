"""Behavior and invariant tests for the Laya/Jev decision-model verification slice."""

from __future__ import annotations

import json
import unittest
from dataclasses import replace
from unittest.mock import patch

from server import decision_model, team
from server.levels_loader import compile_profile, get_profile


class DecisionModelSliceTests(unittest.IsolatedAsyncioTestCase):
    def test_laya_profile_matches_baseline_except_decision_verifier(self) -> None:
        base = get_profile("single-12b-checked")
        noground = get_profile("single-12b-checked-noground")
        laya = get_profile("single-12b-checked-laya")

        self.assertEqual(laya.stages, base.stages)
        self.assertEqual(laya.stages, noground.stages)
        self.assertEqual(laya.prompts, base.prompts)
        self.assertEqual(laya.models["answer"], base.models["answer"])
        self.assertEqual(laya.models["review"], base.models["review"])
        self.assertEqual(laya.models["indepth"], base.models["indepth"])
        self.assertEqual(laya.models["grounding"], "laya")
        self.assertEqual(laya.policies["review"]["grounding"], "decision")
        self.assertEqual(laya.policies["review"]["low_threshold"], 0.35)
        self.assertEqual(laya.policies["review"]["high_threshold"], 0.75)
        self.assertTrue(laya.policies["review"]["verify_review_errors"])

    def test_compile_profile_refuses_jev_router_as_decision_model(self) -> None:
        """Permanent invariant: typesafe/jev-router is an LLM router and must be refused."""
        laya = get_profile("single-12b-checked-laya")
        bad = replace(
            laya,
            id="bad-router-profile",
            models={**dict(laya.models), "grounding": "typesafe/jev-router"},
        )
        with self.assertRaisesRegex(ValueError, "forbidden"):
            compile_profile(bad)

    def test_three_band_classification_and_temperature_scaling(self) -> None:
        self.assertEqual(
            decision_model.classify_probability_band(0.88, low_threshold=0.35, high_threshold=0.75),
            (True, "verified"),
        )
        self.assertEqual(
            decision_model.classify_probability_band(0.55, low_threshold=0.35, high_threshold=0.75),
            (None, "uncertain"),
        )
        self.assertEqual(
            decision_model.classify_probability_band(0.18, low_threshold=0.35, high_threshold=0.75),
            (False, "unsupported"),
        )
        cooled = decision_model.apply_temperature_scaling(0.9, temperature_scale=2.0)
        self.assertLess(cooled, 0.9)
        self.assertGreater(cooled, 0.5)

    async def test_ground_references_with_decision_grounding_three_bands(self) -> None:
        references = [
            {
                "index": 1,
                "resolutionStatus": "resolved",
                "usage": [{"location": "answer", "text": "Creatinine was 132 [1]."}],
            },
            {
                "index": 2,
                "resolutionStatus": "resolved",
                "usage": [{"location": "answer", "text": "Renal trend remained borderline [2]."}],
            },
            {
                "index": 3,
                "resolutionStatus": "resolved",
                "usage": [{"location": "indepth", "text": "Dialysis started in 2009 [3]."}],
            },
        ]
        mappings = [
            {"index": 1, "date": "2008-05-12", "text": "Serum creatinine: 132.0"},
            {"index": 2, "date": "2008-08-19", "text": "Serum creatinine: 97.0"},
            {"index": 3, "date": "2008-11-11", "text": "Serum creatinine: 88.0"},
        ]

        async def fake_chat(client, model, messages, **kwargs):
            payload = {
                "decisions": [
                    {"verdict": "YES", "probability": 0.93, "missing_facts": []},
                    {"verdict": "YES", "probability": 0.52, "missing_facts": []},
                    {
                        "verdict": "NO",
                        "probability": 0.08,
                        "missing_facts": ["Dialysis started in 2009"],
                    },
                ]
            }
            return {"content": json.dumps(payload)}

        with patch.object(team, "_chat", side_effect=fake_chat):
            grounded = await team._ground_references(
                None,  # type: ignore[arg-type]
                "laya",
                "Creatinine was 132 [1]. Renal trend remained borderline [2]. Dialysis started in 2009 [3].",
                references,
                mappings,
                decision_grounding=True,
                low_threshold=0.35,
                high_threshold=0.75,
            )

        self.assertEqual(grounded[0]["groundingStatus"], "verified")
        self.assertIs(grounded[0]["grounded"], True)
        self.assertEqual(grounded[0]["groundingChecks"][0]["method"], "decision_model")
        self.assertEqual(grounded[0]["groundingChecks"][0]["band"], "verified")
        self.assertAlmostEqual(grounded[0]["groundingChecks"][0]["probability"], 0.93)

        self.assertEqual(grounded[1]["groundingStatus"], "uncertain")
        self.assertIsNone(grounded[1]["grounded"])
        self.assertEqual(grounded[1]["groundingChecks"][0]["status"], "uncertain")
        self.assertEqual(grounded[1]["groundingChecks"][0]["band"], "uncertain")

        self.assertEqual(grounded[2]["groundingStatus"], "unsupported")
        self.assertIs(grounded[2]["grounded"], False)
        self.assertEqual(
            grounded[2]["groundingChecks"][0]["not_found"], "Dialysis started in 2009"
        )

    async def test_review_existing_answer_overrules_false_alarm_with_laya(self) -> None:
        calls = []

        async def fake_chat(client, model, messages, **kwargs):
            calls.append(model)
            if model == "gemma-4-12b":
                return {
                    "content": json.dumps(
                        {
                            "answer_ok": False,
                            "errors": [
                                {
                                    "wrong": "132.0 on 2008-05-12",
                                    "chart": "(2008-05-12) Serum creatinine: 132.0",
                                }
                            ],
                        }
                    )
                }
            return {
                "content": json.dumps(
                    {
                        "decisions": [
                            {"verdict": "NO", "probability": 0.09},
                        ]
                    }
                )
            }

        steps = []
        with patch.object(team, "_chat", side_effect=fake_chat):
            (
                _content,
                answer_conf,
                _answer_text,
                validation,
                _gate,
                _orig,
            ) = await team._review_existing_answer(
                None,  # type: ignore[arg-type]
                messages=[{"role": "user", "content": "What was creatinine?"}],
                gathered="",
                chart="[1] (2008-05-12) Serum creatinine: 132.0",
                temporal_facts=None,
                temporal_gate_mode="off",
                reviewer_model="gemma-4-12b",
                reviewer_prompt="validation-label",
                validator_temperature=0.0,
                validator_repeat_penalty=None,
                validator_dry=None,
                max_tokens=512,
                steps=steps,
                payload_override={
                    "schema_version": "answer_to_review.v1",
                    "original_question": "What was creatinine?",
                    "answer": "Serum creatinine was 132.0 on 2008-05-12 [1].",
                    "citations": [1],
                    "blocks": [],
                },
                decision_verifier_model="laya",
                low_threshold=0.35,
                high_threshold=0.75,
            )

        self.assertEqual(calls, ["gemma-4-12b", "laya"])
        self.assertEqual(answer_conf["level"], "green")
        self.assertEqual(validation["status"], "checked")
        review_step = next(s for s in steps if s["role"] == "answer_review")
        self.assertTrue(review_step["answer_ok"])
        self.assertEqual(review_step["n_errors"], 0)
        self.assertEqual(len(review_step["overruled_by_decision_model"]), 1)
        self.assertEqual(review_step["decision_verifier_model"], "laya")


if __name__ == "__main__":
    unittest.main()
