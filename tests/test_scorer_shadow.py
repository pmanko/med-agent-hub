"""The entailment scorer in shadow: its verdict sits beside each citation check and never
sets a label (confidence-labels L6.2)."""

from __future__ import annotations

import asyncio
import json

import server.config as config
import server.team as team


class _Response:
    def __init__(self, payload, status=200):
        self._payload, self.status_code = payload, status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"status {self.status_code}")

    def json(self):
        return self._payload


class _Client:
    def __init__(self, payload, status=200):
        self.payload, self.status, self.calls = payload, status, []

    async def post(self, url, json=None, timeout=None):
        self.calls.append((url, json))
        return _Response(self.payload, self.status)


def test_scorer_verdicts_read_the_probabilities_and_fail_open(monkeypatch):
    monkeypatch.setattr(team, "scorer_config", config.ScorerConfig(base_url="http://scorer:8090"))
    client = _Client({"scores": [{"supported": 0.91, "contradicted": 0.02},
                                 {"supported": 0.10, "contradicted": 0.80},
                                 {"supported": 0.30, "contradicted": 0.20}]})

    verdicts = asyncio.run(team._scorer_verdicts(client, [("s1", "c1"), ("s2", "c2"), ("s3", "c3")]))

    assert client.calls[0][0] == "http://scorer:8090/score"
    assert [v["support"] for v in verdicts] == ["supported", "contradicted", "not_addressed"]
    assert verdicts[1]["contradicted"] == 0.8

    failing = _Client({}, status=500)
    assert [v["support"] for v in asyncio.run(team._scorer_verdicts(failing, [("s", "c")]))] == ["unchecked"]
    monkeypatch.setattr(team, "scorer_config", config.ScorerConfig(base_url=""))
    assert [v["support"] for v in asyncio.run(team._scorer_verdicts(client, [("s", "c")]))] == ["unchecked"]


def test_shadow_scorer_is_recorded_on_the_check_and_never_sets_the_label(monkeypatch):
    monkeypatch.setattr(team, "scorer_config", config.ScorerConfig(base_url="http://scorer:8090"))

    async def fake_chat(_client, _model, _messages, **_kwargs):
        return {"content": json.dumps({"pairs": [{"facts": [{"fact": "41 kg", "status": "found"}]}]})}

    monkeypatch.setattr(team, "_chat", fake_chat)
    client = _Client({"scores": [{"supported": 0.05, "contradicted": 0.9}]})
    claim = "The weight was 41 kg on 2026-01-07 [1]."
    mappings = [{"index": 1, "date": "2026-01-07", "text": "Weight (kg): 41 kg"}]
    references = team._resolve_references([1], mappings, answer=claim, grounding_status="unchecked")

    grounded = asyncio.run(
        team._ground_references(client, "G", claim, references, mappings, shadow_scorer=True)
    )

    check = grounded[0]["groundingChecks"][0]
    assert check["support"] == "supported" and grounded[0]["support"] == "supported"
    # The scorer judges the listed fact, not the whole sentence, and its verdict stays beside.
    assert client.calls[0][1]["pairs"] == [{"source": "[1] 2026-01-07 Weight (kg): 41 kg", "statement": "41 kg"}]
    assert check["scorer"]["support"] == "contradicted"
    assert check["scorer"]["facts"] == [{"fact": "41 kg", "supported": 0.05, "contradicted": 0.9, "support": "contradicted"}]


def test_shadow_scorer_multi_fact_aggregation_and_deterministic_checks(monkeypatch):
    monkeypatch.setattr(team, "scorer_config", config.ScorerConfig(base_url="http://scorer:8090"))

    async def fake_chat(_client, _model, _messages, **_kwargs):
        return {
            "content": json.dumps(
                {
                    "pairs": [
                        {
                            "facts": [
                                {"fact": "2026-01-07", "status": "found"},
                                {"fact": "41 kg", "status": "found"},
                            ]
                        }
                    ]
                }
            )
        }

    monkeypatch.setattr(team, "_chat", fake_chat)
    client = _Client(
        {
            "scores": [
                {"supported": 0.9, "contradicted": 0.01},
                {"supported": 0.05, "contradicted": 0.85},
            ]
        }
    )
    claim = "The weight was 41 kg on 2026-01-07 [1]."
    mappings = [{"index": 1, "date": "2026-01-07", "text": "Weight (kg): 41 kg"}]
    references = team._resolve_references([1], mappings, answer=claim, grounding_status="unchecked")

    grounded = asyncio.run(
        team._ground_references(client, "G", claim, references, mappings, shadow_scorer=True)
    )
    check = grounded[0]["groundingChecks"][0]
    assert [f["support"] for f in check["scorer"]["facts"]] == ["supported", "contradicted"]
    assert check["scorer"]["support"] == "contradicted"

    # Deterministic exact table cell check also receives shadow scorer verdict.
    exact_client = _Client({"scores": [{"supported": 0.92, "contradicted": 0.01}]})
    exact_refs = team._resolve_references([1], mappings, blocks=[{"text": "41 kg", "refs": [1]}])
    exact_grounded = asyncio.run(
        team._ground_references(exact_client, "G", "", exact_refs, mappings, shadow_scorer=True)
    )
    exact_check = exact_grounded[0]["groundingChecks"][0]
    assert exact_check["method"] == "deterministic_exact"
    assert exact_check["scorer"]["support"] == "supported"

    # Deterministic temporal pass also receives shadow scorer verdict.
    temporal_client = _Client({"scores": [{"supported": 0.88, "contradicted": 0.02}]})
    temporal_refs = team._resolve_references([1], mappings, answer=claim)
    temporal_grounded = asyncio.run(
        team._ground_references(
            temporal_client,
            "G",
            claim,
            temporal_refs,
            mappings,
            shadow_scorer=True,
            deterministic_checks=[{"status": "pass", "claim": claim, "source_indices": [1]}],
        )
    )
    temporal_check = temporal_grounded[0]["groundingChecks"][0]
    assert temporal_check["method"] == "deterministic_temporal"
    assert temporal_check["scorer"]["support"] == "supported"


def test_scorer_rejects_invalid_probabilities_and_batches_large_requests(monkeypatch):
    monkeypatch.setattr(team, "scorer_config", config.ScorerConfig(base_url="http://scorer:8090"))
    for bad_sup, bad_con in [
        (True, False),
        (1.5, -0.2),
        (float("nan"), float("inf")),
        (10**400, 0),
        ("0.9", 0.1),
    ]:
        client = _Client({"scores": [{"supported": bad_sup, "contradicted": bad_con}]})
        verdicts = asyncio.run(team._scorer_verdicts(client, [("s", "c")]))
        assert verdicts == [{"supported": None, "contradicted": None, "support": "unchecked"}]

    batch_client = _Client({"scores": [{"supported": 0.9, "contradicted": 0.01}] * 64})
    many_pairs = [(f"s{i}", f"c{i}") for i in range(70)]
    batch_verdicts = asyncio.run(team._scorer_verdicts(batch_client, many_pairs))
    assert len(batch_client.calls) == 2
    assert len(batch_verdicts) == 70


def test_engine_and_profile_compilation_for_shadow_scorer(monkeypatch):
    import dataclasses
    import httpx
    import pytest
    from server.levels_loader import compile_profile
    from tests import test_review_policy as rp

    monkeypatch.setattr(team, "scorer_config", config.ScorerConfig(base_url="http://scorer:8090"))
    base_profile = rp._full_profile("folded")
    for bad_shadow in [False, "bad", {}, [], ["scorer"]]:
        with pytest.raises(ValueError, match="invalid review shadow"):
            compile_profile(
                dataclasses.replace(
                    base_profile,
                    policies={
                        **base_profile.policies,
                        "review": {**base_profile.policies["review"], "shadow": bad_shadow},
                    },
                )
            )

    rp._stub_indepth(
        monkeypatch,
        ["CD4 was 341 cells/uL [2], which calls for monitoring."],
        {"flag": [], "issues": "", "unsupported": []},
    )

    async def fake_post(self, url, **kwargs):
        pairs = (kwargs.get("json") or {}).get("pairs") or []
        return httpx.Response(
            200,
            json={"scores": [{"supported": 0.9, "contradicted": 0.02} for _ in pairs]},
            request=httpx.Request("POST", url),
        )

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
    shadow_profile = compile_profile(
        dataclasses.replace(
            base_profile,
            policies={
                **base_profile.policies,
                "review": {**base_profile.policies["review"], "shadow": "scorer"},
            },
        )
    )
    _names, final = rp._final(shadow_profile)
    for ref in final["references"]:
        for check in ref.get("groundingChecks") or []:
            assert "scorer" in check
            assert check["scorer"]["support"] == "supported"

