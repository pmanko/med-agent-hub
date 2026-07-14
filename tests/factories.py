"""Small architecture-native fixtures for stage-engine tests."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence

from server import engine
from server.context_sources import (
    EvidenceLedger,
    EvidenceRecord,
    SourceRegistry,
    StaticKnowledgeSource,
)
from server.levels_loader import Profile, compile_profile

TEST_ORCHESTRATOR_MODEL = "test-orchestrator"
TEST_EXPERT_MODEL = "test-expert"
TEST_ANSWER_MODEL = "test-answer"


class _StaticPatientSource:
    name = "test-patient"
    priority = 100
    supports_patient = True

    def __init__(self, ledger: EvidenceLedger) -> None:
        self.ledger = ledger

    async def fetch(self, _request):
        return self.ledger


def patient_source_registry(
    chart: str,
    mappings: List[Dict[str, Any]],
    raw_records: Optional[List[Dict[str, Any]]] = None,
) -> SourceRegistry:
    raw_records = raw_records or []
    records = []
    for position, mapping in enumerate(mappings):
        raw = raw_records[position] if position < len(raw_records) else {}
        records.append(
            EvidenceRecord(
                stable_id=str(
                    mapping.get("sourceId")
                    or f"test:{mapping.get('resourceType', 'record')}:{mapping.get('resourceUuid', position + 1)}"
                ),
                source="test-patient",
                source_priority=100,
                resource_type=str(mapping.get("resourceType") or "Record"),
                resource_uuid=mapping.get("resourceUuid"),
                date=mapping.get("date"),
                text=str(mapping.get("text") or ""),
                mandatory=bool((raw.get("metadata") or {}).get("mandatory_context")),
                metadata=raw.get("metadata") or {},
                raw=raw,
            )
        )
    return SourceRegistry(
        [
            _StaticPatientSource(EvidenceLedger(tuple(records), original_text=chart)),
            StaticKnowledgeSource(),
        ]
    )


class _TestTokenCounter:
    async def count(self, _model: str, text: str) -> int:
        return len(text.split())

    async def count_chat(self, _model: str, payload: Mapping[str, Any]) -> int:
        return len(
            " ".join(
                str(message.get("content") or "")
                for message in payload.get("messages") or []
            ).split()
        )


def make_profile(
    *,
    stages: Sequence[str],
    models: Mapping[str, str],
    output: str,
    topology: str,
    prompts: Optional[Mapping[str, str]] = None,
    policies: Optional[Mapping[str, Any]] = None,
    knobs: Optional[Mapping[str, Any]] = None,
    profile_id: str = "test-profile",
) -> Profile:
    merged_policies = {
        "output": output,
        "temporal_gate": "enforce" if output == "product" else "off",
        "temporal_render": "full",
        **dict(policies or {}),
    }
    return compile_profile(
        Profile(
            id=profile_id,
            label="Test profile",
            topology=topology,
            stages=tuple(stages),
            models=dict(models),
            prompts=dict(prompts or {}),
            policies=merged_policies,
            knobs=dict(knobs or {}),
            context_window=24576 if output == "product" else 0,
            reserved_output_tokens=4096 if output == "product" else 0,
            exact_tokenizer=output == "product",
        )
    )


def team_profile(
    *,
    orchestrator: str,
    answer: str,
    output: str,
    expert: Optional[str] = None,
    review: Optional[str] = None,
    indepth: Optional[str] = None,
    answer_prompt: str = "synthesis-answer",
    review_prompt: str = "validation-rewrite",
    knobs: Optional[Mapping[str, Any]] = None,
    policies: Optional[Mapping[str, Any]] = None,
    profile_id: str = "test-team-profile",
) -> Profile:
    stages = ["context", "gather", "answer", "gate"]
    models = {"orchestrator": orchestrator, "answer": answer}
    prompts = {"orchestrator": "orchestrator", "answer": answer_prompt}
    if expert:
        models["expert"] = expert
        prompts["expert"] = "medical_expert"
    if review:
        stages.extend(("review", "gate"))
        models["review"] = review
        prompts["review"] = review_prompt
    if indepth:
        stages.append("indepth")
        models["indepth"] = indepth
        prompts["indepth"] = "synthesis-indepth"
    return make_profile(
        topology="team",
        stages=stages,
        models=models,
        prompts=prompts,
        output=output,
        knobs=knobs,
        policies=policies,
        profile_id=profile_id,
    )


def single_profile(
    *,
    answer: str,
    output: str,
    review: Optional[str] = None,
    indepth: Optional[str] = None,
    answer_prompt: str = "synthesis-answer",
    review_prompt: str = "validation-rewrite",
    knobs: Optional[Mapping[str, Any]] = None,
    policies: Optional[Mapping[str, Any]] = None,
    profile_id: str = "test-single-profile",
) -> Profile:
    stages = ["context", "answer", "gate"]
    models = {"answer": answer}
    prompts = {"answer": answer_prompt}
    if review:
        stages.extend(("review", "gate"))
        models["review"] = review
        prompts["review"] = review_prompt
    if indepth:
        stages.append("indepth")
        models["indepth"] = indepth
        prompts["indepth"] = "synthesis-indepth"
    return make_profile(
        topology="single",
        stages=stages,
        models=models,
        prompts=prompts,
        output=output,
        knobs=knobs,
        policies=policies,
        profile_id=profile_id,
    )


async def run_profile(
    profile: Profile,
    messages: Sequence[Mapping[str, Any]],
    **request_fields: Any,
) -> str:
    if profile.exact_tokenizer and "token_counter" not in request_fields:
        request_fields["token_counter"] = _TestTokenCounter()
    return await engine.drain_profile(
        engine.ExecutionRequest(
            profile=profile,
            messages=messages,
            **request_fields,
        )
    )


async def stream_profile(
    profile: Profile,
    messages: Sequence[Mapping[str, Any]],
    **request_fields: Any,
):
    if profile.exact_tokenizer and "token_counter" not in request_fields:
        request_fields["token_counter"] = _TestTokenCounter()
    request = engine.ExecutionRequest(
        profile=profile,
        messages=messages,
        **request_fields,
    )
    events = engine.execute_profile(request)
    try:
        async for event in events:
            yield event
    finally:
        await events.aclose()
