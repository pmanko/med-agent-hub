from __future__ import annotations

import asyncio
from copy import deepcopy
import hashlib
import json
import uuid
from unittest.mock import patch

import pytest
import rfc8785
from fastapi.testclient import TestClient

from server.main import app
from tests.test_catalyst_query import (
    _assert_shipped_contract,
    _content,
    _flat_repair,
    _queued_backend,
    _ready_candidate,
    _request,
    _review,
    VIEW_NAME,
)


CURRENT_INSTRUCTION = "Keep the current query, but return only results after 2026-01-01"
CURRENT_TURN_ID = "00000000-0000-4000-8000-000000000099"


def _digest(character: str) -> str:
    return character * 64


def _canonical_digest(value) -> str:
    return hashlib.sha256(rfc8785.dumps(value)).hexdigest()


def _history_item(ordinal: int, *, kind: str = "followup") -> dict:
    instruction = (
        "Show viral load results"
        if kind == "initial"
        else f"Prior refinement {ordinal}"
    )
    return {
        "turnId": f"00000000-0000-4000-8000-{ordinal:012d}",
        "ordinal": ordinal,
        "kind": kind,
        "instruction": instruction,
        "instructionDigest": hashlib.sha256(instruction.encode("utf-8")).hexdigest(),
    }


def _revision() -> dict:
    candidate = _ready_candidate()
    history = [
        _history_item(1, kind="initial"),
        *[_history_item(i) for i in range(4, 9)],
    ]
    omitted = [_history_item(2), _history_item(3)]
    omitted_refs = [
        {key: item[key] for key in ("turnId", "ordinal", "kind", "instructionDigest")}
        for item in omitted
    ]
    editor_content = {
        "sql": candidate["sql"],
        "parameters": candidate["parameters"],
        "expectedColumns": candidate["expectedColumns"],
    }
    editor_digest = _canonical_digest(editor_content)
    revision = {
        "contractVersion": "catalyst.query.revision-context.v1",
        "turnId": CURRENT_TURN_ID,
        "currentInstruction": CURRENT_INSTRUCTION,
        "instructionDigest": hashlib.sha256(
            CURRENT_INSTRUCTION.encode("utf-8")
        ).hexdigest(),
        "baseClassification": "reused",
        "observedBase": {
            "versionId": "10000000-0000-4000-8000-000000000001",
            "queryDigest": editor_digest,
        },
        "effectiveBaseVersion": {
            "versionId": "10000000-0000-4000-8000-000000000001",
            "queryDigest": editor_digest,
        },
        "editorSnapshot": {
            "contractVersion": "catalyst.workbench.editor-snapshot.v1",
            **editor_content,
            "editorDigest": editor_digest,
        },
        "instructionHistory": history,
        "validationContext": None,
        "executionContext": None,
        "selection": {
            "includedHistoryTurnIds": [item["turnId"] for item in history],
            "validationRef": None,
            "executionRef": None,
            "omissions": {
                "historyInstructionsOmitted": len(omitted),
                "validationFindingsOmitted": 0,
                "executionColumnsOmitted": 0,
                "diagnosticTextTruncated": False,
                "prohibitedClasses": [
                    "database_credentials",
                    "database_connection_details",
                    "database_dsn",
                    "execution_result_rows",
                    "hidden_reasoning",
                    "historical_sql_copies",
                    "raw_chat_transcript",
                    "raw_model_outputs",
                    "raw_reasoning_traces",
                    "unrelated_session_history",
                    "unrelated_historical_sql",
                ],
                "omittedHistory": omitted_refs,
                "omittedHistoryDigest": _canonical_digest(omitted_refs),
            },
        },
    }
    revision["contextDigest"] = _canonical_digest(revision)
    return revision


def _v2_request() -> dict:
    request = _request()
    request["model"] = "catalyst-query-gemma-4-12b"
    request["messages"] = [{"role": "user", "content": CURRENT_INSTRUCTION}]
    request["catalystQuery"]["contractVersion"] = "catalyst.query.request.v2"
    request["catalystQuery"]["revision"] = _revision()
    return request


def _post_v2(responses: list[dict | str | BaseException], request: dict | None = None):
    calls: list[dict] = []
    with patch(
        "server.catalyst_query._backend_chat",
        side_effect=_queued_backend(responses, calls),
    ):
        response = TestClient(app).post(
            "/v1/chat/completions", json=request or _v2_request()
        )
    return response, calls


def _completion(response) -> dict:
    assert response.status_code == 200, response.text
    return response.json()


def _model_payload(call: dict) -> dict:
    user_messages = [
        message for message in call["messages"] if message["role"] == "user"
    ]
    assert len(user_messages) == 1
    return json.loads(user_messages[0]["content"])


def _all_keys(value) -> set[str]:
    if isinstance(value, dict):
        return set(value).union(*(_all_keys(item) for item in value.values()), set())
    if isinstance(value, list):
        return set().union(*(_all_keys(item) for item in value), set())
    return set()


def test_contract_registry_resolves_v1_v2_and_transitive_refs_offline():
    from server.catalyst_contracts import REQUEST_V1_ID, REQUEST_V2_ID, validator_for

    validator_for(REQUEST_V1_ID).validate(_request())
    validator_for(REQUEST_V2_ID).validate(_v2_request())


def test_v2_contract_accepts_bounded_execution_value_warnings():
    from server.catalyst_contracts import REQUEST_V2_ID, validator_for

    request = _v2_request()
    revision = request["catalystQuery"]["revision"]
    execution_id = "20000000-0000-4000-8000-000000000001"
    version_id = revision["effectiveBaseVersion"]["versionId"]
    query_digest = revision["editorSnapshot"]["editorDigest"]
    revision["executionContext"] = {
        "executionId": execution_id,
        "versionId": version_id,
        "queryDigest": query_digest,
        "status": "succeeded",
        "validationStatus": "valid",
        "rowCount": {
            "returned": 100,
            "truncated": True,
            "truncationReason": "configured_limit",
        },
        "columns": [
            {
                "ordinal": 0,
                "name": "name_display",
                "databaseType": "text",
                "logicalType": "string",
            }
        ],
        "warnings": ["`name_display` was blank or NULL in all 100 displayed rows."],
        "databaseDiagnostic": None,
        "durationMs": 10,
    }
    revision["selection"]["executionRef"] = {
        "executionId": execution_id,
        "versionId": version_id,
        "queryDigest": query_digest,
    }
    revision["contextDigest"] = _canonical_digest(
        {key: value for key, value in revision.items() if key != "contextDigest"}
    )

    validator_for(REQUEST_V2_ID).validate(request)


def test_v2_request_sends_exact_instruction_base_and_bounded_history_to_both_roles():
    response, calls = _post_v2([_ready_candidate(), _review()])

    payload = _content(response)
    _completion(response)
    assert payload["status"] == "ready"
    assert payload["question"] == CURRENT_INSTRUCTION
    assert payload["modelCollaboration"]["writer"]["disposition"] == "selected"
    assert payload["modelCollaboration"]["base"] == {
        "baseClassification": "reused",
        "observedBase": _revision()["observedBase"],
        "effectiveBaseVersion": _revision()["effectiveBaseVersion"],
        "editorDigest": _revision()["editorSnapshot"]["editorDigest"],
    }
    assert [call["model"] for call in calls] == ["gemma-4-12b", "qwen2.5-14b"]
    for call in calls:
        system_prompt = call["messages"][0]["content"]
        assert "revision.editorSnapshot" in system_prompt
        assert "exact" in system_prompt
        assert "current instruction" in system_prompt.lower()
        model_input = _model_payload(call)
        assert model_input["instruction"] == CURRENT_INSTRUCTION
        assert model_input["revision"]["currentInstruction"] == CURRENT_INSTRUCTION
        assert (
            model_input["revision"]["editorSnapshot"] == _revision()["editorSnapshot"]
        )
        assert [
            item["ordinal"] for item in model_input["revision"]["instructionHistory"]
        ] == [
            1,
            4,
            5,
            6,
            7,
            8,
        ]
        serialized = json.dumps(model_input)
        assert "Prior refinement 2" not in serialized
        assert "Prior refinement 3" not in serialized
        assert {
            "resultRows",
            "databaseCredentials",
            "databaseConnectionDetails",
            "reasoningTrace",
            "rawModelOutputs",
        }.isdisjoint(_all_keys(model_input))


@pytest.mark.parametrize(
    "prohibited",
    [
        "databaseCredentials",
        "databaseConnectionDetails",
        "databaseDsn",
        "resultRows",
        "hiddenReasoning",
        "historicalSqlCopies",
        "rawChatTranscript",
        "rawModelOutputs",
        "reasoningTrace",
        "unrelatedSessionHistory",
        "unrelatedHistoricalSql",
    ],
)
def test_v2_rejects_prohibited_untyped_context_before_model_call(prohibited: str):
    request = _v2_request()
    request["catalystQuery"]["revision"][prohibited] = ["secret"]

    response, calls = _post_v2([], request)

    assert response.status_code == 422
    assert calls == []


def test_v2_rejects_message_instruction_mismatch_before_model_call():
    request = _v2_request()
    request["messages"][0]["content"] = "A different instruction"

    response, calls = _post_v2([], request)

    assert response.status_code == 422
    assert calls == []


def test_v2_rejects_instruction_digest_mismatch_before_model_call():
    request = _v2_request()
    request["catalystQuery"]["revision"]["instructionDigest"] = _digest("f")

    response, calls = _post_v2([], request)

    assert response.status_code == 422
    assert calls == []


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda revision: revision["editorSnapshot"].update(
                {"editorDigest": _digest("f")}
            ),
            "editorDigest",
        ),
        (
            lambda revision: revision["selection"]["omissions"].update(
                {"omittedHistoryDigest": _digest("f")}
            ),
            "omittedHistoryDigest",
        ),
        (
            lambda revision: revision.update({"contextDigest": _digest("f")}),
            "contextDigest",
        ),
        (
            lambda revision: revision["effectiveBaseVersion"].update(
                {"queryDigest": _digest("f")}
            ),
            "effectiveBaseVersion",
        ),
    ],
)
def test_v2_rejects_forged_revision_lineage_digests_before_model_call(mutate, message):
    request = _v2_request()
    mutate(request["catalystQuery"]["revision"])

    response, calls = _post_v2([], request)

    assert response.status_code == 422
    assert message in response.text
    assert calls == []


def test_v2_rejects_profile_without_configured_different_family_roles():
    request = _v2_request()
    request["model"] = "catalyst-query-checked"

    response, calls = _post_v2([], request)

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "profile_not_revision_capable"
    assert calls == []


def test_lint_clean_writer_is_always_reviewed_and_complete_correction_is_relinted():
    corrected = _ready_candidate()
    corrected["sql"] = (
        f"SELECT release_date FROM {corrected['target']['approvedViews'][0]} "
        "WHERE release_date >= :since"
    )
    corrected["expectedColumns"] = [
        {"name": "release_date", "logicalType": "date", "nullable": False}
    ]

    response, calls = _post_v2(
        [_ready_candidate(), _review("failed", decision="repair", candidate=corrected)]
    )

    payload = _content(response)
    assert len(calls) == 2
    assert payload["status"] == "ready"
    assert payload["sql"] == corrected["sql"]
    assert payload["modelCollaboration"]["writer"]["disposition"] == "superseded"
    assert payload["modelCollaboration"]["reviewer"]["disposition"] == "selected"
    assert payload["modelCollaboration"]["finalLintFindings"] == []


def test_v2_reviewer_contract_failure_gets_one_reviewer_only_correction():
    response, calls = _post_v2([_ready_candidate(), {"checks": []}, _review()])

    payload = _content(response)
    envelope = _completion(response)
    assert payload["status"] == "ready"
    assert [call["model"] for call in calls] == [
        "gemma-4-12b",
        "qwen2.5-14b",
        "qwen2.5-14b",
    ]
    assert [call["dry_multiplier"] for call in calls] == [0.0, 0.0, 0.0]
    correction_feedback = calls[2]["messages"][-1]["content"]
    assert "strict output contract" in correction_feedback
    assert "decision" in correction_feedback
    assert [item["role"] for item in envelope["modelInvocations"]] == [
        "writer",
        "reviewer",
        "reviewer",
    ]
    assert [item["outcome"] for item in envelope["modelInvocations"]] == [
        "succeeded",
        "contract_failed",
        "succeeded",
    ]
    assert [item["attempt"] for item in envelope["modelInvocations"]] == [1, 1, 2]
    assert [
        item["configuration"]["dryMultiplier"] for item in envelope["modelInvocations"]
    ] == [0.0, 0.0, 0.0]


def test_v2_reviewer_contract_correction_stops_after_one_retry():
    response, calls = _post_v2(
        [
            _ready_candidate(),
            {"checks": []},
            {"checks": []},
        ]
    )

    payload = _content(response)
    envelope = _completion(response)
    assert payload["status"] == "rejected"
    assert [call["model"] for call in calls] == [
        "gemma-4-12b",
        "qwen2.5-14b",
        "qwen2.5-14b",
    ]
    assert [item["outcome"] for item in envelope["modelInvocations"]] == [
        "succeeded",
        "contract_failed",
        "contract_failed",
    ]


def test_v2_reviewer_missing_checks_is_hydrated_without_retry():
    response, calls = _post_v2([_ready_candidate(), {"decision": "approve"}])

    payload = _content(response)
    envelope = _completion(response)

    assert payload["status"] == "ready"
    assert len(calls) == 2
    assert payload["modelCollaboration"]["reviewer"]["checks"] == [
        {
            "name": "reviewer_output_hydrated",
            "status": "passed",
            "message": (
                "The reviewer returned a decision without labelled checks; "
                "the Hub retained that decision and hydrated this evidence marker."
            ),
        }
    ]
    assert [item["outcome"] for item in envelope["modelInvocations"]] == [
        "succeeded",
        "succeeded",
    ]


def test_failed_reviewer_repair_preserves_repair_candidate_and_findings():
    writer = _ready_candidate()
    writer[
        "sql"
    ] = f"SELECT invented_writer_column FROM {VIEW_NAME} WHERE release_date >= :since"
    writer["expectedColumns"] = [
        {
            "name": "invented_writer_column",
            "logicalType": "string",
            "nullable": True,
        }
    ]
    repaired = deepcopy(writer)
    repaired["sql"] = repaired["sql"].replace(
        "invented_writer_column", "invented_reviewer_column"
    )
    repaired["expectedColumns"][0]["name"] = "invented_reviewer_column"

    response, calls = _post_v2([writer, _flat_repair(repaired)])

    payload = _content(response)
    envelope = _completion(response)
    collaboration = payload["modelCollaboration"]
    assert len(calls) == 2
    assert payload["status"] == "rejected"
    assert payload["diagnosticCandidate"]["candidate"] == repaired
    assert collaboration["writer"]["candidate"] == writer
    assert collaboration["writer"]["disposition"] == "retained_unselected"
    assert collaboration["reviewer"]["decision"] == "repair"
    assert collaboration["reviewer"]["candidate"] == repaired
    assert collaboration["reviewer"]["disposition"] == "diagnostic_only"
    assert [finding["code"] for finding in collaboration["finalLintFindings"]] == [
        "catalog.unknown_column"
    ]
    assert (
        collaboration["finalLintFindings"][0]["evidence"] == "invented_reviewer_column"
    )
    assert envelope["modelInvocations"][-1]["outcome"] == "validation_failed"


def test_failed_reviewer_repair_preserves_location_bearing_lint_findings():
    writer = _ready_candidate()
    writer["sql"] = f"SELECT viral_load_value\nFROM {VIEW_NAME}\nWHERE ("
    repaired = deepcopy(writer)
    repaired["sql"] = f"SELECT viral_load_value\nFROM {VIEW_NAME}\nORDER BY ("

    response, calls = _post_v2([writer, _flat_repair(repaired)])

    payload = _content(response)
    envelope = _completion(response)
    collaboration = payload["modelCollaboration"]
    assert response.status_code == 200
    assert len(calls) == 2
    assert payload["status"] == "rejected"
    assert payload["diagnosticCandidate"]["candidate"] == repaired
    assert collaboration["writer"]["candidate"] == writer
    assert collaboration["reviewer"]["candidate"] == repaired
    assert collaboration["writer"]["disposition"] == "retained_unselected"
    assert collaboration["reviewer"]["disposition"] == "diagnostic_only"
    assert collaboration["writer"]["lintFindings"][0]["code"] == "sql.parse_error"
    assert collaboration["finalLintFindings"][0]["code"] == "sql.parse_error"
    for finding in (
        collaboration["writer"]["lintFindings"][0],
        collaboration["finalLintFindings"][0],
    ):
        assert finding["line"] >= 1
        assert finding["column"] >= 1
    assert envelope["modelInvocations"][-1]["outcome"] == "validation_failed"


def test_reviewer_transport_failure_retains_valid_writer_as_unselected_evidence():
    response, calls = _post_v2([_ready_candidate(), RuntimeError("router unavailable")])

    payload = _content(response)
    envelope = _completion(response)
    assert len(calls) == 2
    assert payload["status"] == "rejected"
    assert payload["modelCollaboration"]["writer"]["candidate"] == _ready_candidate()
    assert (
        payload["modelCollaboration"]["writer"]["disposition"] == "retained_unselected"
    )
    assert payload["modelCollaboration"]["reviewer"]["decision"] == "failed"
    assert "sql" not in payload
    assert [item["outcome"] for item in envelope["modelInvocations"]] == [
        "succeeded",
        "transport_failed",
    ]


def test_invalid_writer_output_is_diagnostic_only_and_records_contract_failure():
    response, calls = _post_v2(["not-json"])

    payload = _content(response)
    envelope = _completion(response)
    assert len(calls) == 1
    assert payload["status"] == "rejected"
    assert payload["diagnosticCandidate"]["rawOutput"] == "not-json"
    assert "candidate" not in payload["diagnosticCandidate"]
    assert envelope["modelInvocations"][0]["outcome"] == "contract_failed"
    assert "sql" not in payload


def test_writer_timeout_is_typed_and_timed_in_failure_evidence():
    response, calls = _post_v2([TimeoutError("model deadline exceeded")])

    payload = _content(response)
    envelope = _completion(response)
    assert len(calls) == 1
    assert payload["status"] == "rejected"
    invocation = envelope["modelInvocations"][0]
    assert invocation["role"] == "writer"
    assert invocation["outcome"] == "timed_out"
    assert invocation["endedAt"] >= invocation["startedAt"]
    assert invocation["responseDigest"] is None
    assert len(invocation["failureDigest"]) == 64


def test_cancelled_model_call_still_closes_its_invocation_record():
    from server.catalyst_query import _invoke_backend

    invocations: list[dict] = []

    async def exercise() -> None:
        with patch(
            "server.catalyst_query._backend_chat",
            side_effect=asyncio.CancelledError(),
        ):
            with pytest.raises(asyncio.CancelledError):
                await _invoke_backend(
                    None,
                    "gemma-4-12b",
                    [{"role": "user", "content": "instruction"}],
                    response_format={"type": "json_schema"},
                    temperature=0,
                    dry_multiplier=0,
                    max_tokens=None,
                    invocations=invocations,
                    role="writer",
                    stage="followup_generation",
                    attempt=1,
                )

    asyncio.run(exercise())

    assert len(invocations) == 1
    invocation = invocations[0]
    assert invocation["outcome"] == "cancelled"
    assert invocation["endedAt"] >= invocation["startedAt"]
    assert invocation["durationMs"] >= 0
    assert invocation["responseDigest"] is None
    assert len(invocation["failureDigest"]) == 64


def test_cancelled_generation_persists_terminal_invocation_before_propagating():
    from server.catalyst_query import execute_query_profile
    from server.engine import ExecutionRequest
    from server.levels_loader import get_profile

    request_payload = _v2_request()
    execution = ExecutionRequest(
        profile=get_profile(request_payload["model"]),
        messages=request_payload["messages"],
        catalyst_query=request_payload["catalystQuery"],
    )
    traces: list[dict] = []

    async def exercise() -> None:
        with (
            patch(
                "server.catalyst_query._backend_chat",
                side_effect=asyncio.CancelledError(),
            ),
            patch(
                "server.catalyst_query._write_trace",
                side_effect=lambda _request, _extension, result, _steps: traces.append(
                    result
                ),
            ),
        ):
            with pytest.raises(asyncio.CancelledError):
                async for _event in execute_query_profile(execution):
                    pass

    asyncio.run(exercise())

    assert len(traces) == 1
    invocation = traces[0]["_hubEvidence"]["modelInvocations"][0]
    assert traces[0]["status"] == "rejected"
    assert invocation["outcome"] == "cancelled"
    assert invocation["endedAt"] >= invocation["startedAt"]


def test_catalyst_never_promotes_provider_reasoning_content_to_candidate_output():
    from server.catalyst_query import QueryContractError, _backend_chat

    async def exercise() -> None:
        with patch(
            "server.catalyst_query._chat",
            return_value={
                "content": None,
                "reasoning_content": json.dumps(_ready_candidate()),
            },
        ):
            with pytest.raises(QueryContractError, match="assistant content"):
                await _backend_chat(
                    None,
                    "gemma-4-12b",
                    [{"role": "user", "content": "instruction"}],
                    response_format={"type": "json_schema"},
                    temperature=0,
                    dry_multiplier=0,
                    max_tokens=None,
                )

    asyncio.run(exercise())


def test_every_model_invocation_has_reproducible_role_model_config_and_timing():
    response, _calls = _post_v2([_ready_candidate(), _review()])

    payload = _content(response)
    envelope = _completion(response)
    assert "modelInvocations" not in payload
    assert "profileEvidence" not in payload
    invocations = envelope["modelInvocations"]
    assert [item["role"] for item in invocations] == ["writer", "reviewer"]
    assert [item["stage"] for item in invocations] == [
        "followup_generation",
        "review",
    ]
    assert envelope["totalModelInvocationDurationMs"] == sum(
        item["durationMs"] for item in invocations
    )
    for item in invocations:
        uuid.UUID(item["invocationId"])
        assert item["providerId"] == "openai-compatible"
        assert item["configuration"]["temperature"] == 0
        assert item["configuration"]["dryMultiplier"] == 0
        assert item["endedAt"] >= item["startedAt"]
        assert item["durationMs"] >= 0
        assert len(item["requestDigest"]) == 64
        assert len(item["responseDigest"]) == 64
        assert item["failureDigest"] is None
        assert item["outcome"] == "succeeded"

    profile = envelope["profileEvidence"]
    assert profile["profileId"] == "catalyst-query-gemma-4-12b"
    assert len(profile["profileDigest"]) == 64
    assert profile["writer"]["modelClass"] == "gemma-4"
    assert profile["writer"]["modelId"] == "gemma-4-12b"
    assert profile["reviewer"]["modelClass"] == "qwen-2.5"
    assert profile["reviewer"]["modelId"] == "qwen2.5-14b"
    assert profile["writer"]["modelClass"] != profile["reviewer"]["modelClass"]
    for role in (profile["writer"], profile["reviewer"]):
        assert role["providerId"] == "openai-compatible"
        assert role["config"]["temperature"] == 0
        assert role["config"]["dry"] == 0
        assert role["systemPrompt"]["text"]
        assert len(role["systemPrompt"]["promptDigest"]) == 64

    evidenced_payload = {
        **payload,
        "modelInvocations": invocations,
        "totalModelInvocationDurationMs": envelope[
            "totalModelInvocationDurationMs"
        ],
    }
    _assert_shipped_contract(evidenced_payload)
