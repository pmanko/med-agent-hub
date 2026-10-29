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
