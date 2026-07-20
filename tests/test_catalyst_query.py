from __future__ import annotations

import copy
from dataclasses import replace
import json
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator, FormatChecker

from server.catalyst_query import (
    QueryContractError,
    _missing_parameter_name_paths,
    _normalize_grounded_parameter_names,
    _parse_candidate,
    _patch_format,
)
from server.levels_loader import compile_profile, get_profile
from server.main import app

QUESTION = (
    "Show viral load results since 2026-01-01 with value, unit, release date, "
    "and receipt-to-release time"
)
TRACE_ID = "trace-catalyst-query-001"
CONTEXT_SOURCE_ID = "catalog:analytics-catalog-v1"
VIEW_NAME = "analytics.lab_result_fact_v1"
TARGET = {
    "dataSource": "openelis-demo-analytics",
    "catalogVersion": "analytics-catalog-v1",
    "dialect": "postgresql",
}
RESPONSE_TARGET = {**TARGET, "approvedViews": [VIEW_NAME]}


def _request() -> dict:
    return {
        "model": "catalyst-query-checked",
        "stream": False,
        "messages": [{"role": "user", "content": QUESTION}],
        "catalystQuery": {
            "contractVersion": "catalyst.query.request.v1",
            "requiredOutputContract": "catalyst.query.v1",
            "target": copy.deepcopy(TARGET),
            "catalog": {
                "contextSourceId": CONTEXT_SOURCE_ID,
                "views": [
                    {
                        "name": VIEW_NAME,
                        "version": "1",
                        "grain": "one row per finalized lab result",
                        "fields": [
                            {
                                "name": "viral_load_value",
                                "type": "decimal",
                                "description": "Final viral load result",
                                "unit": "copies/mL",
                            },
                            {
                                "name": "release_date",
                                "type": "date",
                                "description": "Result release date",
                            },
                        ],
                    }
                ],
            },
            "policy": {
                "allowedOperation": "select",
                "requirePreview": True,
                "maxRows": 100,
                "statementTimeoutMs": 5000,
            },
            "correlation": {
                "requestId": "request-catalyst-query-001",
                "traceId": TRACE_ID,
            },
        },
    }


def _ready_candidate(*, target: dict | None = None) -> dict:
    return {
        "status": "ready",
        "target": copy.deepcopy(target or RESPONSE_TARGET),
        "sql": (
            "SELECT viral_load_value, release_date "
            f"FROM {VIEW_NAME} WHERE release_date >= :since"
        ),
        "parameters": [
            {
                "name": "since",
                "type": "date",
                "source": "question",
                "value": "2026-01-01",
            }
        ],
        "expectedColumns": [
            {
                "name": "viral_load_value",
                "logicalType": "decimal",
                "nullable": False,
                "unit": "copies/mL",
            },
            {
                "name": "release_date",
                "logicalType": "date",
                "nullable": False,
            },
        ],
    }


def _semantic_request() -> dict:
    request = _request()
    view = request["catalystQuery"]["catalog"]["views"][0]
    view["fields"].append(
        {
            "name": "test_name",
            "type": "string",
            "description": "Canonical laboratory analyte display name.",
        }
    )
    view["semanticDimensions"] = [
        {
            "field": "test_name",
            "semanticType": "analyte",
            "values": [
                {
                    "canonical": "Viral Load",
                    "aliases": ["viral load", "HIV viral load"],
                }
            ],
        }
    ]
    return request


def _semantic_candidate() -> dict:
    candidate = _ready_candidate()
    candidate["sql"] = (
        "SELECT viral_load_value, release_date "
        f"FROM {VIEW_NAME} WHERE test_name = :test_name "
        "AND release_date >= :since"
    )
    candidate["parameters"].insert(
        0,
        {
            "name": "test_name",
            "type": "string",
            "source": "question",
            "value": "Viral Load",
        },
    )
    return candidate


def _review(
    status: str = "passed",
    *,
    decision: str = "approve",
    candidate: dict | None = None,
) -> dict:
    body = {
        "decision": decision,
        "checks": [
            {
                "name": "catalog_and_policy",
                "status": status,
                "message": "Candidate matches the approved catalog and request policy.",
            }
        ],
    }
    if candidate is not None:
        body["candidate"] = candidate
    return body


def _flat_repair(candidate: dict, status: str = "failed") -> dict:
    return {
        "decision": "repair",
        "checks": [
            {
                "name": "named_analyte_constraint",
                "status": status,
                "message": "The analyte predicate is repaired from the catalog.",
            }
        ],
        **candidate,
    }


def _semantic_generation_patch(*, add_parameter: bool = True) -> dict:
    patches = [
        {
            "findingCode": "semantic.named_analyte_constraint",
            "op": "replace_text",
            "path": "/sql",
            "oldValue": "WHERE release_date >= :since",
            "replacement": ("WHERE test_name = :test_name AND release_date >= :since"),
        }
    ]
    if add_parameter:
        patches.append(
            {
                "findingCode": "semantic.named_analyte_constraint",
                "op": "add",
                "path": "/parameters/-",
                "value": {
                    "name": "test_name",
                    "type": "string",
                    "source": "question",
                    "value": "Viral Load",
                },
            }
        )
    return {"patches": patches}


def _queued_backend(responses: list[dict | str | BaseException], calls: list[dict]):
    queue = list(responses)

    async def fake_backend(
        client,
        model,
        messages,
        *,
        response_format,
        temperature,
        dry_multiplier,
        max_tokens,
    ):
        calls.append(
            {
                "model": model,
                "messages": messages,
                "response_format": response_format,
                "temperature": temperature,
                "dry_multiplier": dry_multiplier,
                "max_tokens": max_tokens,
            }
        )
        response = queue.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response if isinstance(response, str) else json.dumps(response)

    return fake_backend


def _post_with_backend(responses: list[dict | str | BaseException]):
    calls: list[dict] = []
    with patch(
        "server.catalyst_query._backend_chat",
        side_effect=_queued_backend(responses, calls),
    ):
        response = TestClient(app).post("/v1/chat/completions", json=_request())
    return response, calls


def _post_request_with_backend(
    request: dict, responses: list[dict | str | BaseException]
):
    calls: list[dict] = []
    with patch(
        "server.catalyst_query._backend_chat",
        side_effect=_queued_backend(responses, calls),
    ):
        response = TestClient(app).post("/v1/chat/completions", json=request)
    return response, calls


def _content(response) -> dict:
    assert response.status_code == 200, response.text
    return json.loads(response.json()["choices"][0]["message"]["content"])


def _assert_shipped_contract(payload: dict) -> None:
    schema_path = (
        Path(__file__).parents[1]
        / "server"
        / "contracts"
        / "catalyst-query-v1.schema.json"
    )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(payload)


def test_query_profile_has_dedicated_stages_models_and_discovery_contract():
    profile = get_profile("catalyst-query-checked")

    assert profile.output_mode == "query"
    assert profile.stages == (
        "context",
        "query_generate",
        "query_lint",
        "query_review",
        "query_finalize",
    )
    assert profile.models["query_generate"] == "qwen2.5-14b"
    assert profile.models["query_review"] == "qwen2.5-14b"
    assert profile.knobs["query_generate"]["temperature"] == 0
    assert profile.knobs["query_review"]["temperature"] == 0
    assert profile.knobs["query_generate"]["dry"] == 0
    assert profile.knobs["query_review"]["dry"] == 0
    assert profile.output_contracts == ("catalyst.query.v1",)
    assert get_profile("single-e4b-checked").default is True

    with patch(
        "server.openai_compat._served_backend_model_metadata",
        return_value={
            profile.models["query_generate"]: {"id": profile.models["query_generate"]}
        },
    ):
        response = TestClient(app).get("/v1/models")
    by_id = {item["id"]: item for item in response.json()["data"]}
    assert by_id["catalyst-query-checked"]["outputContracts"] == ["catalyst.query.v1"]
    assert by_id["catalyst-query-checked"]["available"] is True
    assert by_id["catalyst-query-checked"]["revisionCapable"] is False
    assert by_id["single-e4b-checked"]["default"] is True


@pytest.mark.parametrize("dry", [None, 0.8])
def test_query_profile_requires_router_dry_to_be_disabled(dry):
    profile = get_profile("catalyst-query-gemma-4-12b")
    knobs = {
        role: dict(configured) for role, configured in profile.knobs.items()
    }
    if dry is None:
        knobs["query_generate"].pop("dry")
    else:
        knobs["query_generate"]["dry"] = dry

    with pytest.raises(ValueError, match="query_generate.*dry 0"):
        compile_profile(replace(profile, knobs=knobs))


@pytest.mark.parametrize("role", ["query_generate", "query_review"])
@pytest.mark.parametrize(
    ("knob", "value"),
    [
        ("temperature", False),
        ("temperature", "0"),
        ("dry", False),
        ("dry", "0"),
    ],
)
def test_query_profile_rejects_non_numeric_and_boolean_zero_knobs(role, knob, value):
    profile = get_profile("catalyst-query-gemma-4-12b")
    knobs = {
        configured_role: dict(configured)
        for configured_role, configured in profile.knobs.items()
    }
    knobs[role][knob] = value

    with pytest.raises(ValueError, match=rf"{role!s}.*{knob!s} 0"):
        compile_profile(replace(profile, knobs=knobs))


def test_gemma_query_profile_uses_router_model_id_and_is_available():
    profile = get_profile("catalyst-query-gemma-e4b")

    assert profile.models["query_generate"] == "gemma-e4b"
    assert profile.models["query_review"] == "gemma-e4b"
    assert profile.policies["model_classes"] == {
        "query_generate": "gemma-4",
        "query_review": "gemma-4",
    }
    assert "require_preview" not in profile.policies

    with patch(
        "server.openai_compat._served_backend_model_metadata",
        return_value={"gemma-e4b": {"id": "gemma-e4b"}},
    ):
        response = TestClient(app).get("/v1/models")

    assert response.status_code == 200
    by_id = {item["id"]: item for item in response.json()["data"]}
    advertised = by_id["catalyst-query-gemma-e4b"]
    assert advertised["available"] is True
    assert advertised["role_models"] == {
        "query_generate": "gemma-e4b",
        "query_review": "gemma-e4b",
    }
    assert advertised["role_model_classes"] == {
        "query_generate": "gemma-4",
        "query_review": "gemma-4",
    }
    assert advertised["profileEvidence"]["writer"]["modelClass"] == "gemma-4"
    assert advertised["profileEvidence"]["reviewer"]["modelClass"] == "gemma-4"
    assert "unavailableReasons" not in advertised


def test_gemma_12b_query_profile_is_truthful_available_and_provenanced():
    profile = get_profile("catalyst-query-gemma-4-12b")
    e4b = get_profile("catalyst-query-gemma-e4b")
    backend_model = {
        "id": "gemma-4-12b",
        "object": "model",
        "owned_by": "llamacpp",
        "status": {"value": "loaded"},
        "meta": {
            "n_ctx": 24576,
            "n_params": 11_907_350_576,
            "size": 12_653_822_144,
        },
    }

    assert dict(profile.models) == {
        "query_generate": "gemma-4-12b",
        "query_review": "qwen2.5-14b",
    }
    assert profile.stages == e4b.stages
    assert dict(profile.prompts) == dict(e4b.prompts)
    assert profile.policies["collaborative_review"] is True
    assert profile.policies["generation_attempts"] == 1
    assert dict(profile.capabilities) == dict(e4b.capabilities)
    assert profile.output_contracts == e4b.output_contracts == ("catalyst.query.v1",)
    assert profile.visibility == e4b.visibility == "product"
    assert dict(profile.knobs) == dict(e4b.knobs)
    assert profile.knobs["query_generate"]["temperature"] == 0
    assert profile.knobs["query_review"]["temperature"] == 0
    assert profile.knobs["query_generate"]["dry"] == 0
    assert profile.knobs["query_review"]["dry"] == 0
    assert profile.default is False
    assert e4b.default is False
    assert get_profile("single-e4b-checked").default is True

    with patch(
        "server.openai_compat._served_backend_model_metadata",
        return_value={
            "gemma-4-12b": backend_model,
            "qwen2.5-14b": {"id": "qwen2.5-14b", "status": {"value": "loaded"}},
        },
    ):
        response = TestClient(app).get("/v1/models")

    assert response.status_code == 200
    by_id = {item["id"]: item for item in response.json()["data"]}
    advertised = by_id["catalyst-query-gemma-4-12b"]
    assert advertised["available"] is True
    assert advertised["unavailable_reasons"] == []
    assert advertised["required_models"] == ["gemma-4-12b", "qwen2.5-14b"]
    assert advertised["role_models"] == dict(profile.models)
    assert advertised["role_model_classes"] == {
        "query_generate": "gemma-4",
        "query_review": "qwen-2.5",
    }
    assert advertised["revisionCapable"] is True
    assert advertised["profileEvidence"]["writer"]["modelClass"] == "gemma-4"
    assert advertised["profileEvidence"]["reviewer"]["modelClass"] == "qwen-2.5"
    assert advertised["profileEvidence"]["writer"]["systemPrompt"]["text"]
    assert advertised["role_knobs"] == {
        "query_generate": {"temperature": 0, "dry": 0},
        "query_review": {"temperature": 0, "dry": 0},
    }
    assert advertised["backend_model_metadata"] == {
        "gemma-4-12b": backend_model,
        "qwen2.5-14b": {
            "id": "qwen2.5-14b",
            "status": {"value": "loaded"},
        },
    }
    assert advertised["profile_configuration_digest"].startswith("sha256:")
    assert (
        advertised["profile_configuration_digest"]
        != by_id["catalyst-query-gemma-e4b"]["profile_configuration_digest"]
    )
    assert (
        advertised["role_prompt_digests"]
        == by_id["catalyst-query-gemma-e4b"]["role_prompt_digests"]
    )


def test_bundled_qwen_query_profile_uses_truthful_router_model_id():
    profile = get_profile("catalyst-query-qwen-coder-1.5b")

    expected = "qwen2.5-coder-1.5b-instruct-q4_k_m"
    assert profile.models["query_generate"] == expected
    assert profile.models["query_review"] == expected

    with patch(
        "server.openai_compat._served_backend_model_metadata",
        return_value={expected: {"id": expected}},
    ):
        response = TestClient(app).get("/v1/models")

    assert response.status_code == 200
    by_id = {item["id"]: item for item in response.json()["data"]}
    assert by_id["catalyst-query-qwen-coder-1.5b"]["available"] is True


@pytest.mark.parametrize(
    ("mutate", "error_location"),
    [
        (lambda body: body.update({"temperature": 0}), ["body", "temperature"]),
        (lambda body: body.update({"stream": True}), ["body", "stream"]),
        (
            lambda body: body["messages"].append(
                {"role": "user", "content": "second question"}
            ),
            ["body", "messages"],
        ),
        (
            lambda body: body["messages"][0].update({"role": "system"}),
            ["body", "messages", 0, "role"],
        ),
        (
            lambda body: body["catalystQuery"].update({"unexpected": True}),
            ["body", "catalystQuery", "unexpected"],
        ),
        (
            lambda body: body["catalystQuery"]["policy"].update({"maxRows": "100"}),
            ["body", "catalystQuery", "policy", "maxRows"],
        ),
        (
            lambda body: body["catalystQuery"].update(
                {"requiredOutputContract": "other.contract"}
            ),
            ["body", "catalystQuery", "requiredOutputContract"],
        ),
        (
            lambda body: body["catalystQuery"]["catalog"]["views"][0]["fields"][
                0
            ].update({"unit": None}),
            [
                "body",
                "catalystQuery",
                "catalog",
                "views",
                0,
                "fields",
                0,
                "unit",
            ],
        ),
        (
            lambda body: body["catalystQuery"]["catalog"]["views"][0].update(
                {"relationships": [""]}
            ),
            [
                "body",
                "catalystQuery",
                "catalog",
                "views",
                0,
                "relationships",
                0,
            ],
        ),
    ],
)
def test_query_request_is_strict_and_rejects_overrides(mutate, error_location):
    request = _request()
    mutate(request)

    with patch("server.catalyst_query._backend_chat") as backend:
        response = TestClient(app).post("/v1/chat/completions", json=request)

    assert response.status_code == 422
    assert any(error["loc"] == error_location for error in response.json()["detail"])
    backend.assert_not_called()


@pytest.mark.parametrize(
    ("candidate", "review_status", "expected_status", "validation_status"),
    [
        (_ready_candidate(), "passed", "ready", "passed"),
        (
            {
                "status": "needs_clarification",
                "clarification": "Which date field should define the reporting window?",
            },
            "warned",
            "needs_clarification",
            "warned",
        ),
        (
            {
                "status": "unsupported",
                "message": "The approved catalog does not contain medication data.",
            },
            "failed",
            "unsupported",
            "rejected",
        ),
        (
            {
                "status": "rejected",
                "message": "The request asks for a non-read-only operation.",
            },
            "failed",
            "rejected",
            "rejected",
        ),
    ],
)
def test_query_pipeline_returns_every_contract_status(
    candidate, review_status, expected_status, validation_status
):
    response, calls = _post_with_backend([candidate, _review(review_status)])

    payload = _content(response)
    _assert_shipped_contract(payload)
    assert payload["status"] == expected_status
    assert payload["validation"]["status"] == validation_status
    assert payload["question"] == QUESTION
    assert len(calls) == 2
    assert all(call["temperature"] == 0 for call in calls)


def test_unknown_named_analyte_is_unsupported_without_model_generation():
    request = _semantic_request()
    request["messages"][0]["content"] = "Show bilirubin results since 2026-01-01"

    response, calls = _post_request_with_backend(request, [])

    payload = _content(response)
    _assert_shipped_contract(payload)
    assert payload["status"] == "unsupported"
    assert "bilirubin" in payload["message"]
    assert payload["validation"]["checks"][0]["name"] == "catalog_scope"
    assert calls == []


@pytest.mark.parametrize(
    "question",
    [
        "Show the 10 most recent laboratory results since 2026-01-01",
        "Show latest lab test results since 2026-01-01",
        "List all tests results since 2026-01-01",
        "Show abnormal laboratory results since 2026-01-01",
        "List all available numeric test results since 2026-01-01",
        "Find flagged patient results since 2026-01-01",
    ],
)
def test_generic_result_subject_reaches_model_generation(question):
    request = _semantic_request()
    request["messages"][0]["content"] = question

    response, calls = _post_request_with_backend(
        request,
        [_ready_candidate(), _review("passed")],
    )

    assert _content(response)["status"] == "ready"
    assert len(calls) == 2


@pytest.mark.parametrize(
    "question",
    [
        "Show the 10 most recent bilirubin results since 2026-01-01",
        "Show top 5 bilirubin results since 2026-01-01",
    ],
)
def test_unknown_named_analyte_with_count_is_still_unsupported(question):
    request = _semantic_request()
    request["messages"][0]["content"] = question

    response, calls = _post_request_with_backend(request, [])

    payload = _content(response)
    assert payload["status"] == "unsupported"
    assert "bilirubin" in payload["message"]
    assert calls == []


def test_query_roles_receive_dedicated_prompts_and_strict_backend_schemas():
    response, calls = _post_with_backend([_ready_candidate(), _review("passed")])

    assert _content(response)["status"] == "ready"
    assert [call["model"] for call in calls] == [
        "qwen2.5-14b",
        "qwen2.5-14b",
    ]
    assert "generation stage" in calls[0]["messages"][0]["content"]
    assert "independent review stage" in calls[1]["messages"][0]["content"]
    assert all(
        call["response_format"]["json_schema"]["strict"] is True for call in calls
    )
    assert calls[0]["response_format"]["json_schema"]["name"] == (
        "catalyst_query_candidate"
    )
    assert calls[1]["response_format"]["json_schema"]["name"] == (
        "catalyst_query_review"
    )
    generation_schema = calls[0]["response_format"]["json_schema"]["schema"]
    assert generation_schema["properties"]["status"] == {"const": "ready"}
    assert set(generation_schema["required"]) == {
        "status",
        "target",
        "sql",
        "parameters",
        "expectedColumns",
    }
    assert set(generation_schema["$defs"]["parameter"]["required"]) == {
        "type",
        "value",
    }
    shipped_schema = json.loads(
        (
            Path(__file__).parents[1]
            / "server"
            / "contracts"
            / "catalyst-query-v1.schema.json"
        ).read_text(encoding="utf-8")
    )
    assert "name" in shipped_schema["$defs"]["parameter"]["required"]
    review_schema = calls[1]["response_format"]["json_schema"]["schema"]
    assert review_schema["properties"]["decision"] == {
        "enum": ["approve", "repair", "reject"]
    }
    assert set(review_schema["required"]) == {"decision", "checks"}


def test_query_writer_and_reviewer_disable_router_dry():
    request = _request()
    request["model"] = "catalyst-query-gemma-4-12b"
    responses = [
        {"content": json.dumps(_ready_candidate())},
        {"content": json.dumps(_review("passed"))},
    ]
    with patch("server.catalyst_query._chat", side_effect=responses) as router_chat:
        response = TestClient(app).post("/v1/chat/completions", json=request)

    assert _content(response)["status"] == "ready"
    assert [call.args[1] for call in router_chat.call_args_list] == [
        "gemma-4-12b",
        "qwen2.5-14b",
    ]
    assert [
        call.kwargs["dry_multiplier"] for call in router_chat.call_args_list
    ] == [0.0, 0.0]


def test_single_date_normalization_never_rewrites_an_unrelated_parameter():
    request = _request()
    candidate = _ready_candidate()
    candidate["sql"] = (
        f"SELECT viral_load_value FROM {VIEW_NAME} "
        "WHERE viral_load_value > :threshold"
    )
    candidate["parameters"] = [
        {"type": "integer", "source": "question", "value": 1000}
    ]
    candidate["expectedColumns"] = [
        {"name": "viral_load_value", "logicalType": "decimal", "nullable": True}
    ]

    parsed, _normalized = _parse_candidate(
        json.dumps(candidate),
        "Show results above 1000 since 2026-01-01",
        request["catalystQuery"],
        label="numeric threshold candidate",
    )

    assert parsed["parameters"] == [
        {
            "name": "threshold",
            "type": "integer",
            "source": "question",
            "value": 1000,
        }
    ]


def test_12b_writer_findings_go_to_distinct_reviewer_as_complete_candidate():
    request = _request()
    request["model"] = "catalyst-query-gemma-4-12b"
    writer = _ready_candidate()
    writer[
        "sql"
    ] = f"SELECT COUNT(viral_load_value) FROM {VIEW_NAME} WHERE release_date >= :since"
    writer["expectedColumns"] = [
        {"name": "count", "logicalType": "integer", "nullable": False}
    ]
    repaired = copy.deepcopy(writer)
    repaired["sql"] = (
        f"SELECT COUNT(viral_load_value) AS count FROM {VIEW_NAME} "
        "WHERE release_date >= :since"
    )

    response, calls = _post_request_with_backend(
        request, [writer, _flat_repair(repaired)]
    )

    payload = _content(response)
    assert payload["status"] == "ready"
    assert payload["sql"] == repaired["sql"]
    assert [call["model"] for call in calls] == ["gemma-4-12b", "qwen2.5-14b"]
    assert calls[1]["response_format"]["json_schema"]["name"] == (
        "catalyst_query_repair"
    )
    review_payload = json.loads(calls[1]["messages"][-1]["content"])
    assert review_payload["candidate"] == writer
    assert review_payload["deterministicFindings"][0]["code"] == (
        "output.projection_mismatch"
    )
    assert payload["modelCollaboration"]["writer"] == {
        "model": "gemma-4-12b",
        "candidate": writer,
        "lintFindings": review_payload["deterministicFindings"],
    }
    assert payload["modelCollaboration"]["reviewer"]["model"] == "qwen2.5-14b"
    assert payload["modelCollaboration"]["reviewer"]["candidate"] == repaired
    assert payload["modelCollaboration"]["finalLintFindings"] == []


def test_collaborative_reviewer_deduplicates_identical_unnamed_bindings():
    request = _semantic_request()
    request["model"] = "catalyst-query-gemma-4-12b"
    request["messages"][0][
        "content"
    ] = "How many patients had viral load tests above 1000 copies/mL?"
    writer = _semantic_candidate()
    writer["sql"] = (
        f"SELECT COUNT(viral_load_value) FROM {VIEW_NAME} "
        "WHERE test_name = :test_name AND viral_load_value > :threshold"
    )
    writer["expectedColumns"] = [
        {"name": "count", "logicalType": "integer", "nullable": False}
    ]
    writer["parameters"][1] = {
        "name": "threshold",
        "type": "integer",
        "source": "question",
        "value": 1000,
    }
    repaired = copy.deepcopy(writer)
    repaired["sql"] = repaired["sql"].replace(
        "COUNT(viral_load_value)", "COUNT(viral_load_value) AS count"
    )
    repaired["parameters"] = [
        {"type": "string", "value": "Viral Load"},
        {"type": "integer", "value": 1000},
    ] * 2

    response, calls = _post_request_with_backend(
        request, [writer, _flat_repair(repaired)]
    )

    payload = _content(response)
    assert payload["status"] == "ready"
    assert [call["model"] for call in calls] == ["gemma-4-12b", "qwen2.5-14b"]
    assert payload["parameters"] == [
        {
            "name": "test_name",
            "type": "string",
            "source": "question",
            "value": "Viral Load",
        },
        {
            "name": "threshold",
            "type": "integer",
            "source": "question",
            "value": 1000,
        },
    ]


def test_collaborative_reviewer_candidate_is_relinted_before_finalization():
    request = _request()
    request["model"] = "catalyst-query-gemma-4-12b"
    writer = _ready_candidate()
    writer["sql"] = writer["sql"].replace(VIEW_NAME, "analytics.missing_view")
    still_invalid = copy.deepcopy(writer)

    response, calls = _post_request_with_backend(
        request,
        [writer, _flat_repair(still_invalid)],
    )

    payload = _content(response)
    assert payload["status"] == "rejected"
    assert [call["model"] for call in calls] == ["gemma-4-12b", "qwen2.5-14b"]
    assert "review repair failed deterministic lint" in payload["message"]


def test_question_date_literals_are_bound_before_independent_review():
    draft = _ready_candidate()
    draft["sql"] = (
        f"SELECT viral_load_value, release_date FROM {VIEW_NAME} "
        "WHERE release_date >= '2026-01-01'"
    )
    draft["parameters"] = []

    response, calls = _post_with_backend([draft, _review("passed")])

    payload = _content(response)
    assert payload["status"] == "ready"
    assert payload["sql"].endswith("WHERE release_date >= :date_1")
    assert payload["parameters"] == [
        {
            "name": "date_1",
            "type": "date",
            "source": "question",
            "value": "2026-01-01",
        }
    ]
    review_payload = json.loads(calls[1]["messages"][1]["content"])
    assert review_payload["candidate"]["sql"] == payload["sql"]
    assert review_payload["candidate"]["parameters"] == payload["parameters"]


def test_postgres_date_literal_binding_does_not_leave_invalid_date_parameter():
    draft = _ready_candidate()
    draft["sql"] = (
        f"SELECT viral_load_value, release_date FROM {VIEW_NAME} "
        "WHERE release_date >= DATE '2026-01-01'"
    )
    draft["parameters"] = []

    response, calls = _post_with_backend([draft, _review("passed")])

    payload = _content(response)
    assert payload["status"] == "ready"
    assert payload["sql"].endswith("WHERE release_date >= :date_1")
    assert "DATE :" not in payload["sql"]
    review_payload = json.loads(calls[1]["messages"][1]["content"])
    assert review_payload["candidate"]["sql"] == payload["sql"]


def test_generation_binds_grounded_analyte_and_missing_date_parameters():
    draft = _semantic_candidate()
    draft["parameters"] = [{"value": "viral load", "type": "string"}]

    response, calls = _post_request_with_backend(
        _semantic_request(), [draft, _review("passed")]
    )

    payload = _content(response)
    assert payload["status"] == "ready"
    assert payload["parameters"] == [
        {
            "name": "test_name",
            "value": "Viral Load",
            "type": "string",
            "source": "question",
        },
        {
            "name": "since",
            "type": "date",
            "source": "question",
            "value": "2026-01-01",
        },
    ]
    assert len(calls) == 2


@pytest.mark.parametrize(
    "bad_generation",
    [
        "Here is the JSON:\n" + json.dumps(_ready_candidate()),
        "```json\n" + json.dumps(_ready_candidate()) + "\n```",
        '{"status":"ready"',
        json.dumps({**_ready_candidate(), "unexpected": True}),
    ],
)
def test_generation_prose_malformed_or_unknown_fields_fail_closed(bad_generation):
    response, calls = _post_with_backend([bad_generation, bad_generation])

    payload = _content(response)
    _assert_shipped_contract(payload)
    assert payload["status"] == "rejected"
    assert payload["validation"]["status"] == "rejected"
    assert payload["validation"]["checks"][0]["status"] == "failed"
    assert payload["diagnosticCandidate"]["executable"] is False
    assert payload["diagnosticCandidate"]["rawOutput"] == bad_generation
    assert payload["diagnosticCandidate"]["attempts"][-1]["findings"][0]["code"] in {
        "contract.invalid_candidate",
        "generation.unchanged_candidate",
    }
    assert len(calls) == 2


def test_generation_preserves_best_candidate_and_latest_malformed_raw_output():
    best_candidate = _ready_candidate()
    second_attempt = _semantic_candidate()
    second_attempt["sql"] += " AND viral_load_value > :threshold"
    second_attempt["parameters"].append(
        {"type": "integer", "source": "question", "value": 2000}
    )
    latest_attempt = copy.deepcopy(second_attempt)
    latest_attempt["parameters"][-1]["value"] = 3000
    second_raw = json.dumps(second_attempt, separators=(",", ":"))
    latest_raw = json.dumps(latest_attempt, separators=(",", ":"))

    response, calls = _post_request_with_backend(
        _semantic_request(), [best_candidate, second_raw, latest_raw]
    )

    payload = _content(response)
    _assert_shipped_contract(payload)
    assert payload["status"] == "rejected"
    diagnostic = payload["diagnosticCandidate"]
    assert diagnostic["executable"] is False
    assert diagnostic["candidate"] == best_candidate
    assert diagnostic["rawOutput"] == latest_raw
    assert [attempt["finding_codes"] for attempt in diagnostic["attempts"]] == [
        ["semantic.named_analyte_constraint"],
        ["contract.invalid_patch"],
        ["contract.invalid_patch"],
    ]
    assert len(calls) == 3


def test_generation_retry_applies_only_an_anchored_sql_text_patch():
    best_candidate = _ready_candidate()
    best_candidate["sql"] = best_candidate["sql"].replace(
        VIEW_NAME, "analytics.unapproved_lab_results"
    )
    sql_patch = {
        "patches": [
            {
                "findingCode": "catalog.unapproved_view",
                "op": "replace_text",
                "path": "/sql",
                "oldValue": "analytics.unapproved_lab_results",
                "replacement": VIEW_NAME,
            }
        ]
    }

    response, calls = _post_with_backend([best_candidate, sql_patch, _review("passed")])

    payload = _content(response)
    assert payload["status"] == "ready"
    assert payload["sql"] == _ready_candidate()["sql"]
    assert payload["target"] == RESPONSE_TARGET
    assert payload["parameters"] == best_candidate["parameters"]
    assert payload["expectedColumns"] == best_candidate["expectedColumns"]
    assert len(calls) == 3

    retry_format = calls[1]["response_format"]["json_schema"]
    assert retry_format["name"] == "catalyst_query_candidate_patch"
    assert retry_format["strict"] is True
    retry_payload = json.loads(calls[1]["messages"][-1]["content"])["correctionRequest"]
    assert retry_payload["baseCandidate"] == best_candidate
    assert retry_payload["allowedPatchPaths"] == ["/sql"]
    assert retry_payload["findings"][0]["code"] == "catalog.unapproved_view"
    assert "replacement candidate" not in retry_payload["instruction"].lower()


def test_generation_retry_repairs_only_the_failing_parameter_name_leaf():
    request = _request()
    request["messages"][0][
        "content"
    ] = "Show viral load results above 1000 copies/mL since 2026-01-01"
    best_candidate = _ready_candidate()
    best_candidate["sql"] += " AND viral_load_value > :threshold"
    best_candidate["parameters"].append(
        {
            "name": "wrong_name",
            "type": "integer",
            "source": "question",
            "value": 1000,
        }
    )
    name_patch = {
        "patches": [
            {
                "findingCode": "binding.placeholder_mismatch",
                "op": "replace",
                "path": "/parameters/1/name",
                "value": "threshold",
            }
        ]
    }

    response, calls = _post_request_with_backend(
        request, [best_candidate, name_patch, _review("passed")]
    )

    payload = _content(response)
    assert payload["status"] == "ready"
    assert payload["sql"] == best_candidate["sql"]
    assert payload["target"] == RESPONSE_TARGET
    assert payload["expectedColumns"] == best_candidate["expectedColumns"]
    assert payload["parameters"][0] == best_candidate["parameters"][0]
    assert payload["parameters"][1] == {
        "name": "threshold",
        "type": "integer",
        "source": "question",
        "value": 1000,
    }
    retry_payload = json.loads(calls[1]["messages"][-1]["content"])["correctionRequest"]
    assert retry_payload["allowedPatchPaths"] == ["/parameters/1/name"]


def test_generation_pairs_multiple_missing_names_without_a_name_only_retry():
    request = _request()
    request["messages"][0][
        "content"
    ] = "Show viral load results above 1000 since 2026-01-01"
    partial_candidate = _ready_candidate()
    partial_candidate["sql"] += " AND viral_load_value > :threshold"
    del partial_candidate["parameters"][0]["name"]
    partial_candidate["parameters"].append(
        {
            "type": "integer",
            "source": "question",
            "value": 1000,
        }
    )
    response, calls = _post_request_with_backend(
        request, [partial_candidate, _review("passed")]
    )

    payload = _content(response)
    assert payload["status"] == "ready"
    assert payload["parameters"] == [
        {
            "name": "since",
            "type": "date",
            "source": "question",
            "value": "2026-01-01",
        },
        {
            "name": "threshold",
            "type": "integer",
            "source": "question",
            "value": 1000,
        },
    ]
    assert len(calls) == 2
    assert calls[1]["response_format"]["json_schema"]["name"] == (
        "catalyst_query_review"
    )


def test_generation_retry_reuses_grounded_name_normalization_for_a_partial_patch():
    base_candidate = _ready_candidate()
    partial_patch = _semantic_generation_patch()
    del partial_patch["patches"][1]["value"]["name"]

    response, calls = _post_request_with_backend(
        _semantic_request(),
        [base_candidate, partial_patch, _review("passed")],
    )

    payload = _content(response)
    assert payload["status"] == "ready"
    assert payload["sql"] == _semantic_candidate()["sql"]
    assert payload["parameters"] == [
        base_candidate["parameters"][0],
        _semantic_candidate()["parameters"][0],
    ]
    assert payload["target"] == RESPONSE_TARGET
    assert payload["expectedColumns"] == base_candidate["expectedColumns"]
    assert len(calls) == 3


def test_missing_name_patch_schema_requires_an_add_operation():
    response_format = _patch_format(
        ["/parameters/1/name"],
        ["contract.parameter_name_required"],
        add_only_paths={"/parameters/1/name"},
    )
    name_variant = response_format["json_schema"]["schema"]["properties"]["patches"][
        "items"
    ]["oneOf"][0]
    assert name_variant["properties"]["path"] == {"const": "/parameters/1/name"}
    assert name_variant["properties"]["op"] == {"const": "add"}


def test_missing_name_retry_rejects_duplicate_frozen_bindings():
    candidate = _ready_candidate()
    candidate["sql"] += " AND viral_load_value > :threshold"
    candidate["parameters"].extend(
        [
            {"name": "since", "type": "integer", "source": "question", "value": 1000},
            {"type": "integer", "source": "question", "value": 1000},
        ]
    )

    assert _missing_parameter_name_paths(candidate, _request()["catalystQuery"]) == []


def test_generation_retry_rejects_a_patch_that_regresses_a_valid_parameter():
    request = _request()
    request["messages"][0][
        "content"
    ] = "Show viral load results above 1000 copies/mL since 2026-01-01"
    best_candidate = _ready_candidate()
    best_candidate["sql"] += " AND viral_load_value > :threshold"
    best_candidate["parameters"].append(
        {
            "name": "wrong_name",
            "type": "integer",
            "source": "question",
            "value": 1000,
        }
    )

    def regressive_patch(date_value: str) -> dict:
        return {
            "patches": [
                {
                    "findingCode": "binding.placeholder_mismatch",
                    "op": "replace",
                    "path": "/parameters/1/name",
                    "value": "threshold",
                },
                {
                    "findingCode": "binding.placeholder_mismatch",
                    "op": "replace",
                    "path": "/parameters/0/value",
                    "value": date_value,
                },
            ]
        }

    latest_patch = regressive_patch("2024-01-01")
    response, calls = _post_request_with_backend(
        request,
        [
            best_candidate,
            regressive_patch("2025-01-01"),
            latest_patch,
        ],
    )

    payload = _content(response)
    assert payload["status"] == "rejected"
    diagnostic = payload["diagnosticCandidate"]
    assert diagnostic["candidate"] == best_candidate
    assert diagnostic["rawOutput"] == json.dumps(latest_patch)
    assert diagnostic["attempts"][-1]["finding_codes"] == [
        "generation.patch_out_of_scope"
    ]
    assert len(calls) == 3


@pytest.mark.parametrize(
    ("first_retry", "latest_retry", "finding_code"),
    [
        (
            {
                "patches": [
                    {
                        "findingCode": "catalog.unapproved_view",
                        "op": "replace_text",
                        "path": "/sql",
                        "oldValue": "release_date",
                        "replacement": "observed_at",
                    }
                ]
            },
            {
                "patches": [
                    {
                        "findingCode": "catalog.unapproved_view",
                        "op": "replace_text",
                        "path": "/sql",
                        "oldValue": "release_date",
                        "replacement": "issued_at",
                    }
                ]
            },
            "generation.patch_ambiguous",
        ),
        (
            {
                "patches": [
                    {
                        "findingCode": "catalog.unapproved_view",
                        "op": "replace_text",
                        "path": "/sql",
                        "oldValue": "analytics.unapproved_lab_results",
                        "replacement": VIEW_NAME,
                    },
                    {
                        "findingCode": "catalog.unapproved_view",
                        "op": "replace_text",
                        "path": "/sql",
                        "oldValue": "analytics.unapproved_lab_results",
                        "replacement": VIEW_NAME,
                    },
                ]
            },
            {
                "patches": [
                    {
                        "findingCode": "catalog.unapproved_view",
                        "op": "replace_text",
                        "path": "/sql",
                        "oldValue": "analytics.unapproved_lab_results",
                        "replacement": VIEW_NAME,
                    },
                    {
                        "findingCode": "catalog.unapproved_view",
                        "op": "replace_text",
                        "path": "/sql",
                        "oldValue": "unapproved_lab_results",
                        "replacement": "lab_result_fact_v1",
                    },
                ]
            },
            "generation.patch_ambiguous",
        ),
        (
            {
                "patches": [
                    {
                        "findingCode": "catalog.unapproved_view",
                        "op": "replace",
                        "path": "/target/dialect",
                        "value": "postgresql",
                    }
                ]
            },
            {
                "patches": [
                    {
                        "findingCode": "catalog.unapproved_view",
                        "op": "replace",
                        "path": "/target/dataSource",
                        "value": "other-source",
                    }
                ]
            },
            "generation.patch_out_of_scope",
        ),
        (
            {
                **_ready_candidate(),
                "parameters": [
                    {"type": "date", "source": "question", "value": "2026-01-01"},
                    {"type": "integer", "source": "question", "value": 1000},
                ],
            },
            {
                **_ready_candidate(),
                "sql": _ready_candidate()["sql"] + " LIMIT 10",
                "parameters": [
                    {"type": "date", "source": "question", "value": "2026-01-01"},
                    {"type": "integer", "source": "question", "value": 2000},
                ],
            },
            "contract.invalid_patch",
        ),
    ],
    ids=[
        "ambiguous-anchored-sql-text",
        "duplicate-or-overlapping-patches",
        "out-of-scope-query-metadata",
        "full-candidate-with-multiple-missing-names",
    ],
)
def test_generation_retry_rejects_non_local_repairs_and_preserves_evidence(
    first_retry, latest_retry, finding_code
):
    best_candidate = _ready_candidate()
    best_candidate["sql"] = best_candidate["sql"].replace(
        VIEW_NAME, "analytics.unapproved_lab_results"
    )

    response, calls = _post_with_backend([best_candidate, first_retry, latest_retry])

    payload = _content(response)
    assert payload["status"] == "rejected"
    diagnostic = payload["diagnosticCandidate"]
    assert diagnostic["candidate"] == best_candidate
    assert diagnostic["rawOutput"] == json.dumps(latest_retry)
    assert diagnostic["attempts"][-1]["finding_codes"] == [finding_code]
    assert len(calls) == 3


def test_one_bounded_generation_contract_correction_can_recover():
    response, calls = _post_with_backend(
        ['{"status":"ready"', _ready_candidate(), _review("passed")]
    )

    payload = _content(response)
    assert payload["status"] == "ready"
    assert len(calls) == 3
    correction = json.loads(calls[1]["messages"][-1]["content"])
    assert correction["correctionRequest"]["findings"][0]["code"] == (
        "contract.invalid_candidate"
    )


def test_generator_backend_failure_fails_closed():
    response, calls = _post_with_backend([RuntimeError("model router unavailable")])

    payload = _content(response)
    _assert_shipped_contract(payload)
    assert payload["status"] == "rejected"
    assert payload["validation"]["checks"][0]["name"] == "query_generate"
    assert len(calls) == 1


def test_catalog_target_metadata_is_deterministically_canonicalized_before_review():
    mismatched = _ready_candidate(
        target={**RESPONSE_TARGET, "approvedViews": ["analytics.secret_view"]}
    )

    response, calls = _post_with_backend([mismatched, _review("passed")])

    payload = _content(response)
    _assert_shipped_contract(payload)
    assert payload["status"] == "ready"
    assert payload["target"] == RESPONSE_TARGET
    assert len(calls) == 2


def test_repair_is_strictly_parsed_and_re_reviewed_before_shipping():
    draft = _ready_candidate()
    repaired = _ready_candidate()
    repaired["sql"] += " LIMIT 100"
    responses = [
        draft,
        _review("failed", decision="repair", candidate=repaired),
        _review("passed"),
    ]

    response, calls = _post_with_backend(responses)

    payload = _content(response)
    _assert_shipped_contract(payload)
    assert payload["status"] == "ready"
    assert payload["sql"] == repaired["sql"]
    assert payload["validation"]["status"] == "passed"
    assert len(calls) == 3
    assert calls[1]["response_format"]["json_schema"]["schema"]["properties"][
        "decision"
    ] == {"enum": ["approve", "repair", "reject"]}
    re_review_payload = json.loads(calls[2]["messages"][1]["content"])
    assert re_review_payload["candidate"] == repaired
    assert re_review_payload["reviewAttempt"] == 2


def test_catalog_semantics_force_missing_analyte_filter_through_repair():
    draft = _ready_candidate()
    draft["parameters"].append(
        {"type": "string", "source": "question", "value": "viral load"}
    )
    response, calls = _post_request_with_backend(
        _semantic_request(),
        [
            draft,
            _semantic_generation_patch(),
            _review("passed"),
        ],
    )

    payload = _content(response)
    assert payload["status"] == "ready"
    assert payload["sql"] == _semantic_candidate()["sql"]
    assert (
        next(
            parameter
            for parameter in payload["parameters"]
            if parameter["name"] == "test_name"
        )["value"]
        == "Viral Load"
    )
    assert payload["validation"]["status"] == "warned"
    assert (
        "semantic.named_analyte_constraint"
        in payload["validation"]["checks"][0]["message"]
    )
    assert payload["validation"]["checks"][-1]["name"] == ("named_analyte_constraint")
    correction = json.loads(calls[1]["messages"][-1]["content"])
    finding = correction["correctionRequest"]["findings"][0]
    assert finding["code"] == "semantic.named_analyte_constraint"
    assert finding["evidence"] == "field=test_name; canonical=Viral Load"
    assert "bind its string value exactly as 'Viral Load'" in finding["suggestedAction"]
    review_payload = json.loads(calls[2]["messages"][1]["content"])
    assert "deterministicFindings" not in review_payload
    assert review_payload["candidate"] == {
        key: payload[key]
        for key in ("status", "target", "sql", "parameters", "expectedColumns")
    }
    assert calls[2]["response_format"]["json_schema"]["name"] == (
        "catalyst_query_review"
    )


def test_deterministic_semantic_failure_never_reaches_reviewer(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setenv("TEAM_TRACE_DIR", str(tmp_path))
    response, calls = _post_request_with_backend(
        _semantic_request(), [_ready_candidate(), _ready_candidate()]
    )

    payload = _content(response)
    assert payload["status"] == "rejected"
    assert payload["validation"]["checks"][0]["name"] == "query_generate"
    diagnostic = payload["diagnosticCandidate"]
    assert diagnostic["executable"] is False
    assert diagnostic["candidate"]["sql"] == _ready_candidate()["sql"]
    assert diagnostic["attempts"][-1]["findings"][0]["code"] == (
        "generation.unchanged_candidate"
    )
    assert len(calls) == 2
    trace = json.loads((tmp_path / "trace.jsonl").read_text(encoding="utf-8"))
    lint_steps = [step for step in trace["steps"] if step["role"] == "query_lint"]
    assert [step["finding_codes"] for step in lint_steps] == [
        ["semantic.named_analyte_constraint"],
        ["generation.unchanged_candidate"],
    ]


def test_semantic_generation_correction_gets_one_bounded_contract_retry():
    incomplete_repair = {
        "decision": "repair",
        "checks": [
            {
                "name": "named_analyte_constraint",
                "status": "failed",
                "message": "The analyte predicate is missing.",
            }
        ],
        "status": "ready",
    }
    response, calls = _post_request_with_backend(
        _semantic_request(),
        [
            _ready_candidate(),
            incomplete_repair,
            _semantic_generation_patch(),
            _review("passed"),
        ],
    )

    payload = _content(response)
    assert payload["status"] == "ready"
    assert len(calls) == 4
    correction = json.loads(calls[2]["messages"][-1]["content"])
    assert correction["correctionRequest"]["findings"][0]["code"] == (
        "semantic.named_analyte_constraint"
    )
    assert correction["correctionRequest"]["lastPatchRejection"][0]["code"] == (
        "contract.invalid_patch"
    )


def test_semantic_repair_binds_unambiguous_missing_parameter_names():
    response, calls = _post_request_with_backend(
        _semantic_request(),
        [
            _ready_candidate(),
            _semantic_generation_patch(),
            _review("passed"),
        ],
    )

    payload = _content(response)
    assert payload["status"] == "ready"
    assert {parameter["name"] for parameter in payload["parameters"]} == {
        "test_name",
        "since",
    }
    assert len(calls) == 3


def test_grounded_unnamed_parameter_binds_to_sole_remaining_placeholder():
    request = _request()
    question = "Show results above 1000 copies/mL since 2026-01-01"
    candidate = _ready_candidate()
    candidate["sql"] += " AND viral_load_value > :threshold"
    candidate["parameters"].append(
        {"type": "integer", "source": "question", "value": 1000}
    )

    parsed, binding_normalized = _parse_candidate(
        json.dumps(candidate),
        question,
        request["catalystQuery"],
        label="grounded threshold candidate",
    )

    assert binding_normalized is True
    assert parsed["parameters"][-1] == {
        "name": "threshold",
        "type": "integer",
        "source": "question",
        "value": 1000,
    }


def test_longer_query_pairs_unnamed_parameters_in_placeholder_order():
    request = _request()
    question = "Show results between 1000 and 2000 since 2026-01-01"
    candidate = _ready_candidate()
    candidate["sql"] += (
        " AND viral_load_value >= :lower_threshold"
        " AND viral_load_value <= :upper_threshold"
    )
    candidate["parameters"].extend(
        [
            {"type": "integer", "source": "question", "value": 1000},
            {"type": "integer", "source": "question", "value": 2000},
        ]
    )

    parsed, binding_normalized = _parse_candidate(
        json.dumps(candidate),
        question,
        request["catalystQuery"],
        label="ordered threshold candidate",
    )

    assert binding_normalized is True
    assert [parameter["name"] for parameter in parsed["parameters"]] == [
        "since",
        "lower_threshold",
        "upper_threshold",
    ]


def test_ordered_pairing_does_not_add_a_question_grounding_rule():
    request = _request()
    question = "Show results above 1000 copies/mL since 2026-01-01"
    candidate = _ready_candidate()
    candidate["sql"] += " AND viral_load_value > :threshold"
    candidate["parameters"].append(
        {"type": "integer", "source": "question", "value": 2000}
    )

    parsed, binding_normalized = _parse_candidate(
        json.dumps(candidate),
        question,
        request["catalystQuery"],
        label="ordered threshold candidate",
    )

    assert binding_normalized is True
    assert parsed["parameters"][-1]["name"] == "threshold"
    assert parsed["parameters"][-1]["value"] == 2000


def test_parameter_placeholder_count_mismatch_remains_editable_invalid_output():
    request = _request()
    candidate = _ready_candidate()
    candidate["sql"] += " AND viral_load_value > :threshold"
    candidate["parameters"] = [{"type": "integer", "value": 1000}]

    with pytest.raises(QueryContractError, match="'name' is a required property"):
        _parse_candidate(
            json.dumps(candidate),
            QUESTION,
            request["catalystQuery"],
            label="count mismatch candidate",
        )


def test_existing_named_parameters_are_unchanged_by_cardinality_normalization():
    request = _request()
    question = "Show results above 1000 copies/mL since 2026-01-01"
    candidate = _ready_candidate()
    candidate["sql"] += " AND viral_load_value > :threshold"
    candidate["parameters"].append(
        {
            "name": "threshold",
            "type": "integer",
            "source": "question",
            "value": 1000,
        }
    )

    assert (
        _normalize_grounded_parameter_names(
            candidate, question, request["catalystQuery"]
        )
        == candidate
    )


def test_turnaround_repair_binds_derived_minutes_parameter_name():
    request = _semantic_request()
    request["messages"][0][
        "content"
    ] = "Show viral load results with turnaround over 24 hours since 2026-01-01"
    request["catalystQuery"]["catalog"]["views"][0]["fields"].append(
        {
            "name": "receipt_to_release_minutes",
            "type": "integer",
            "description": "Receipt-to-release turnaround in minutes.",
        }
    )
    candidate = _semantic_candidate()
    candidate["sql"] += " AND receipt_to_release_minutes > :threshold_minutes"
    candidate["parameters"].append(
        {"type": "integer", "source": "question", "value": 1440}
    )

    response, calls = _post_request_with_backend(
        request,
        [candidate, _review("passed")],
    )

    payload = _content(response)
    assert payload["status"] == "ready"
    assert payload["parameters"][-1] == {
        "name": "threshold_minutes",
        "type": "integer",
        "source": "question",
        "value": 1440,
    }
    assert len(calls) == 2


def test_semantic_patch_uses_catalog_grounded_analyte_value():
    response, calls = _post_request_with_backend(
        _semantic_request(),
        [
            _ready_candidate(),
            _semantic_generation_patch(),
            _review("passed"),
        ],
    )

    payload = _content(response)
    assert payload["status"] == "ready"
    assert next(
        parameter
        for parameter in payload["parameters"]
        if parameter["name"] == "test_name"
    ) == {
        "name": "test_name",
        "type": "string",
        "source": "question",
        "value": "Viral Load",
    }
    review_payload = json.loads(calls[2]["messages"][1]["content"])
    assert any(
        parameter["value"] == "Viral Load"
        for parameter in review_payload["candidate"]["parameters"]
    )
    assert len(calls) == 3


@pytest.mark.parametrize(
    "review_failure",
    [
        RuntimeError("model router unavailable"),
        "review prose instead of JSON",
        {"decision": "repair", "checks": []},
    ],
)
def test_reviewer_backend_or_contract_failure_fails_closed(review_failure):
    response, calls = _post_with_backend([_ready_candidate(), review_failure])

    payload = _content(response)
    _assert_shipped_contract(payload)
    assert payload["status"] == "rejected"
    assert payload["validation"]["status"] == "rejected"
    assert "review" in payload["message"].lower()
    assert len(calls) == 2


def test_reviewer_rejection_preserves_findings_and_returns_no_query():
    review = {
        "decision": "reject",
        "checks": [
            {
                "name": "read_only",
                "status": "failed",
                "message": "The candidate is not safely reviewable.",
            }
        ],
        "message": "The candidate could not be safely reviewed.",
    }
    response, calls = _post_with_backend([_ready_candidate(), review])

    payload = _content(response)
    _assert_shipped_contract(payload)
    assert payload["status"] == "rejected"
    assert payload["validation"]["checks"] == review["checks"]
    assert "sql" not in payload
    assert len(calls) == 2


def test_second_repair_is_rejected_instead_of_shipping_unreviewed_content():
    response, calls = _post_with_backend(
        [
            _ready_candidate(),
            _review("failed", decision="repair", candidate=_ready_candidate()),
            _review("failed", decision="repair", candidate=_ready_candidate()),
        ]
    )

    payload = _content(response)
    _assert_shipped_contract(payload)
    assert payload["status"] == "rejected"
    assert len(calls) == 3


def test_question_target_trace_and_context_source_are_exact_server_echoes():
    response, _calls = _post_with_backend([_ready_candidate(), _review("passed")])

    payload = _content(response)
    assert response.json()["id"] == TRACE_ID
    assert payload["question"] == _request()["messages"][0]["content"]
    assert payload["target"] == RESPONSE_TARGET
    assert payload["provenance"] == {
        "profileId": "catalyst-query-checked",
        "traceId": TRACE_ID,
        "contextSourceIds": [CONTEXT_SOURCE_ID],
    }


def test_query_dispatch_does_not_enter_the_clinical_stage_family():
    with (
        patch(
            "server.catalyst_query._backend_chat",
            side_effect=_queued_backend([_ready_candidate(), _review("passed")], []),
        ),
        patch(
            "server.engine._execute_stages",
            side_effect=AssertionError("clinical fallback must not run"),
        ),
    ):
        response = TestClient(app).post("/v1/chat/completions", json=_request())

    assert _content(response)["status"] == "ready"
