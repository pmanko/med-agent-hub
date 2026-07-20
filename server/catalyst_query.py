"""Fail-closed stages for the governed Catalyst analytics-query profile."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import time
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Mapping, Optional, Tuple

import httpx
import rfc8785
from jsonschema import Draft202012Validator, FormatChecker

from .catalyst_query_lint import lint_candidate, turnaround_threshold
from .config import llm_config
from .prompt_loader import load_prompt
from .team import _chat

logger = logging.getLogger(__name__)

_CONTRACT_PATH = Path(__file__).parent / "contracts" / "catalyst-query-v1.schema.json"
_FINAL_SCHEMA = json.loads(_CONTRACT_PATH.read_text(encoding="utf-8"))
Draft202012Validator.check_schema(_FINAL_SCHEMA)
_FORMAT_CHECKER = FormatChecker()
_FINAL_VALIDATOR = Draft202012Validator(_FINAL_SCHEMA, format_checker=_FORMAT_CHECKER)

_STATUS_FIELDS = {
    "ready": ("target", "sql", "parameters", "expectedColumns"),
    "needs_clarification": ("clarification",),
    "unsupported": ("message",),
    "rejected": ("message",),
}
_ALL_STATUS_FIELDS = {
    "target",
    "sql",
    "parameters",
    "expectedColumns",
    "clarification",
    "message",
}


def _status_branch(
    status: str, required: Tuple[str, ...], forbidden: Tuple[str, ...]
) -> Dict[str, Any]:
    branch: Dict[str, Any] = {
        "properties": {"status": {"const": status}},
        "required": list(required),
    }
    if forbidden:
        branch["not"] = {"anyOf": [{"required": [field]} for field in forbidden]}
    return branch


_CANDIDATE_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["status"],
    "properties": {
        "status": {
            "enum": [
                "ready",
                "needs_clarification",
                "unsupported",
                "rejected",
            ]
        },
        "target": {"$ref": "#/$defs/target"},
        "sql": _FINAL_SCHEMA["properties"]["sql"],
        "parameters": _FINAL_SCHEMA["properties"]["parameters"],
        "expectedColumns": _FINAL_SCHEMA["properties"]["expectedColumns"],
        "clarification": _FINAL_SCHEMA["properties"]["clarification"],
        "message": _FINAL_SCHEMA["properties"]["message"],
    },
    "oneOf": [
        _status_branch(
            status,
            fields,
            tuple(sorted(_ALL_STATUS_FIELDS - set(fields))),
        )
        for status, fields in _STATUS_FIELDS.items()
    ],
    "$defs": {
        name: deepcopy(_FINAL_SCHEMA["$defs"][name])
        for name in ("target", "parameter", "column")
    },
}
Draft202012Validator.check_schema(_CANDIDATE_SCHEMA)
_CANDIDATE_VALIDATOR = Draft202012Validator(
    _CANDIDATE_SCHEMA, format_checker=_FORMAT_CHECKER
)

_CHECK_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["name", "status"],
    "properties": {
        "name": {"type": "string", "minLength": 1},
        "status": {"enum": ["passed", "warned", "failed"]},
        "message": {"type": "string"},
    },
}
_REVIEW_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["decision", "checks"],
    "properties": {
        "decision": {"enum": ["approve", "repair", "reject"]},
        "checks": {
            "type": "array",
            "minItems": 1,
            "items": _CHECK_SCHEMA,
        },
        "candidate": {"$ref": "#/$defs/candidate"},
        "message": {"type": "string", "minLength": 1},
    },
    "oneOf": [
        {
            "properties": {"decision": {"const": "approve"}},
            "not": {
                "anyOf": [
                    {"required": ["candidate"]},
                    {"required": ["message"]},
                ]
            },
        },
        {
            "properties": {"decision": {"const": "repair"}},
            "required": ["candidate"],
            "not": {"required": ["message"]},
        },
        {
            "properties": {"decision": {"const": "reject"}},
            "required": ["message"],
            "not": {"required": ["candidate"]},
        },
    ],
    "$defs": {
        **deepcopy(_CANDIDATE_SCHEMA["$defs"]),
        "candidate": {
            key: deepcopy(value)
            for key, value in _CANDIDATE_SCHEMA.items()
            if key != "$defs"
        },
    },
}
Draft202012Validator.check_schema(_REVIEW_SCHEMA)
_REVIEW_VALIDATOR = Draft202012Validator(_REVIEW_SCHEMA, format_checker=_FORMAT_CHECKER)


class QueryContractError(ValueError):
    """A model response failed a strict query-stage contract."""


class QueryGenerationError(QueryContractError):
    """Generation stopped after preserving its deterministic attempt history."""

    def __init__(
        self,
        message: str,
        history: list[dict[str, Any]],
        *,
        candidate: Optional[Mapping[str, Any]] = None,
        raw_output: Optional[str] = None,
    ) -> None:
        self.history = deepcopy(history)
        self.candidate = deepcopy(candidate) if candidate is not None else None
        self.raw_output = raw_output
        super().__init__(message)


class QueryPatchError(QueryContractError):
    """A generation correction patch violated its strict local scope."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


def _structured_format(name: str, schema: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "type": "json_schema",
        "json_schema": {
            "name": name,
            "strict": True,
            "schema": schema,
        },
    }


_BACKEND_GENERATION_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "status",
        "target",
        "sql",
        "parameters",
        "expectedColumns",
    ],
    "properties": {
        "status": {"const": "ready"},
        "target": {"$ref": "#/$defs/target"},
        "sql": deepcopy(_CANDIDATE_SCHEMA["properties"]["sql"]),
        "parameters": deepcopy(_CANDIDATE_SCHEMA["properties"]["parameters"]),
        "expectedColumns": deepcopy(_CANDIDATE_SCHEMA["properties"]["expectedColumns"]),
    },
    "$defs": deepcopy(_CANDIDATE_SCHEMA["$defs"]),
}
_BACKEND_GENERATION_SCHEMA["$defs"]["parameter"]["required"] = ["type", "value"]
_BACKEND_REVIEW_SCHEMA: Dict[str, Any] = deepcopy(_REVIEW_SCHEMA)
_BACKEND_REPAIR_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "decision",
        "checks",
        "status",
        "target",
        "sql",
        "parameters",
        "expectedColumns",
    ],
    "properties": {
        "decision": {"const": "repair"},
        "checks": {
            "type": "array",
            "minItems": 1,
            "items": deepcopy(_CHECK_SCHEMA),
        },
        "status": {"const": "ready"},
        "target": {"$ref": "#/$defs/target"},
        "sql": deepcopy(_CANDIDATE_SCHEMA["properties"]["sql"]),
        "parameters": deepcopy(_CANDIDATE_SCHEMA["properties"]["parameters"]),
        "expectedColumns": deepcopy(_CANDIDATE_SCHEMA["properties"]["expectedColumns"]),
    },
    "$defs": deepcopy(_CANDIDATE_SCHEMA["$defs"]),
}
Draft202012Validator.check_schema(_BACKEND_GENERATION_SCHEMA)
Draft202012Validator.check_schema(_BACKEND_REVIEW_SCHEMA)
Draft202012Validator.check_schema(_BACKEND_REPAIR_SCHEMA)

_GENERATION_FORMAT = _structured_format(
    "catalyst_query_candidate", _BACKEND_GENERATION_SCHEMA
)
_REVIEW_FORMAT = _structured_format("catalyst_query_review", _BACKEND_REVIEW_SCHEMA)
_REPAIR_FORMAT = _structured_format("catalyst_query_repair", _BACKEND_REPAIR_SCHEMA)

_PATCH_OPERATION_SCHEMA: Dict[str, Any] = {
    "oneOf": [
        {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "findingCode",
                "op",
                "path",
                "oldValue",
                "replacement",
            ],
            "properties": {
                "findingCode": {"type": "string", "minLength": 1},
                "op": {"const": "replace_text"},
                "path": {"const": "/sql"},
                "oldValue": {"type": "string", "minLength": 1},
                "replacement": {"type": "string"},
            },
        },
        {
            "type": "object",
            "additionalProperties": False,
            "required": ["findingCode", "op", "path", "value"],
            "properties": {
                "findingCode": {"type": "string", "minLength": 1},
                "op": {"enum": ["add", "replace"]},
                "path": {"type": "string", "pattern": "^/"},
                "value": {},
            },
        },
    ]
}
_BACKEND_PATCH_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["patches"],
    "properties": {
        "patches": {
            "type": "array",
            "minItems": 1,
            "maxItems": 12,
            "items": _PATCH_OPERATION_SCHEMA,
        }
    },
}
Draft202012Validator.check_schema(_BACKEND_PATCH_SCHEMA)
_PATCH_VALIDATOR = Draft202012Validator(
    _BACKEND_PATCH_SCHEMA, format_checker=_FORMAT_CHECKER
)


def _patch_format(
    allowed_paths: list[str],
    finding_codes: list[str],
    *,
    add_only_paths: Optional[set[str]] = None,
) -> Dict[str, Any]:
    """Return a strict private response schema narrowed to current findings."""
    codes = sorted(set(finding_codes))
    add_only = add_only_paths or set()
    operation_variants: list[dict[str, Any]] = []
    if "/sql" in allowed_paths:
        text_variant = deepcopy(_PATCH_OPERATION_SCHEMA["oneOf"][0])
        text_variant["properties"]["findingCode"] = {"enum": codes}
        operation_variants.append(text_variant)

    for path in (path for path in allowed_paths if path != "/sql"):
        leaf_variant = deepcopy(_PATCH_OPERATION_SCHEMA["oneOf"][1])
        leaf_variant["properties"]["findingCode"] = {"enum": codes}
        leaf_variant["properties"]["path"] = {"const": path}
        if path == "/parameters/-":
            leaf_variant["properties"]["op"] = {"const": "add"}
            leaf_variant["properties"]["value"] = deepcopy(
                _CANDIDATE_SCHEMA["$defs"]["parameter"]
            )
        elif re.fullmatch(r"/parameters/\d+/name", path):
            if path in add_only:
                leaf_variant["properties"]["op"] = {"const": "add"}
            leaf_variant["properties"]["value"] = deepcopy(
                _CANDIDATE_SCHEMA["$defs"]["parameter"]["properties"]["name"]
            )
        elif re.fullmatch(r"/expectedColumns/\d+/name", path):
            leaf_variant["properties"]["op"] = {"const": "replace"}
            leaf_variant["properties"]["value"] = deepcopy(
                _CANDIDATE_SCHEMA["$defs"]["column"]["properties"]["name"]
            )
        operation_variants.append(leaf_variant)

    schema = deepcopy(_BACKEND_PATCH_SCHEMA)
    schema["properties"]["patches"]["items"]["oneOf"] = operation_variants
    return _structured_format("catalyst_query_candidate_patch", schema)


def _validation_error(
    validator: Draft202012Validator, value: Any, label: str
) -> QueryContractError:
    errors = sorted(
        validator.iter_errors(value),
        key=lambda error: [str(item) for item in error.absolute_path],
    )
    if not errors:
        return QueryContractError("")
    error = errors[0]
    location = ".".join(str(item) for item in error.absolute_path) or "<root>"
    return QueryContractError(f"{label} failed at {location}: {error.message}")


def _parse_exact_object(
    content: str,
    validator: Draft202012Validator,
    *,
    label: str,
) -> Dict[str, Any]:
    value = _decode_exact_object(content, label=label)
    error = _validation_error(validator, value, label)
    if str(error):
        raise error
    return value


def _parse_review_object(
    content: str,
    *,
    label: str,
    flat_repair: bool,
    question: str,
    extension: Mapping[str, Any],
) -> Dict[str, Any]:
    value = _decode_exact_object(content, label=label)
    decision = value.get("decision")
    default_status = {
        "approve": "passed",
        "repair": "warned",
        "reject": "failed",
    }.get(decision)
    checks = value.get("checks")
    if default_status is not None and (checks is None or checks == []):
        value["checks"] = [
            {
                "name": "reviewer_output_hydrated",
                "status": default_status,
                "message": (
                    "The reviewer returned a decision without labelled checks; "
                    "the Hub retained that decision and hydrated this evidence marker."
                ),
            }
        ]
    elif isinstance(checks, list):
        hydrated_checks = []
        for index, check in enumerate(checks, start=1):
            if not isinstance(check, Mapping):
                hydrated_checks.append(check)
                continue
            hydrated = deepcopy(dict(check))
            if (
                not isinstance(hydrated.get("name"), str)
                or not hydrated["name"].strip()
            ):
                hydrated["name"] = f"review_check_{index}"
            if "status" not in hydrated and default_status is not None:
                hydrated["status"] = default_status
            hydrated_checks.append(hydrated)
        value["checks"] = hydrated_checks
    if flat_repair and "candidate" not in value:
        candidate_fields = (
            "status",
            "target",
            "sql",
            "parameters",
            "expectedColumns",
        )
        if any(field in value for field in candidate_fields):
            candidate = {field: value.get(field) for field in candidate_fields}
            candidate, _ = _normalize_exact_duplicate_parameter_bindings(candidate)
            candidate, _ = _normalize_ordered_parameter_bindings(candidate)
            candidate = _normalize_grounded_parameter_names(
                candidate, question, extension
            )
            value = {
                "decision": value.get("decision"),
                "checks": value.get("checks"),
                "candidate": candidate,
            }
    candidate = value.get("candidate")
    if isinstance(candidate, Mapping) and candidate.get("status") == "ready":
        normalized_candidate = deepcopy(dict(candidate))
        normalized_candidate["target"] = _canonical_target(extension)
        value["candidate"] = normalized_candidate
    error = _validation_error(_REVIEW_VALIDATOR, value, label)
    if str(error):
        raise error
    return value


def _decode_exact_object(content: str, *, label: str) -> Dict[str, Any]:
    if not isinstance(content, str) or not content:
        raise QueryContractError(f"{label} was empty")

    def object_without_duplicates(
        pairs: list[tuple[str, Any]],
    ) -> Dict[str, Any]:
        value: Dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise QueryContractError(f"{label} repeated JSON key {key!r}")
            value[key] = item
        return value

    def reject_non_json_constant(value: str) -> None:
        raise QueryContractError(f"{label} used non-JSON numeric constant {value!r}")

    try:
        value = json.loads(
            content,
            object_pairs_hook=object_without_duplicates,
            parse_constant=reject_non_json_constant,
        )
    except json.JSONDecodeError as exc:
        raise QueryContractError(f"{label} was not valid JSON") from exc
    if not isinstance(value, dict):
        raise QueryContractError(f"{label} was not a JSON object")
    return value


_NAMED_PARAMETER = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)")


def _normalize_single_date_binding(
    candidate: Mapping[str, Any], question: str
) -> tuple[Dict[str, Any], bool]:
    """Repair only one unambiguous date binding; never infer general parameters."""
    normalized = deepcopy(dict(candidate))
    if normalized.get("status") != "ready":
        return normalized, False
    placeholders = list(
        dict.fromkeys(_NAMED_PARAMETER.findall(str(normalized.get("sql", ""))))
    )
    dates = list(dict.fromkeys(_ISO_DATE_LITERAL.findall(question)))
    if len(placeholders) != 1 or len(dates) != 1:
        return normalized, False
    parameters = normalized.get("parameters")
    if (
        not isinstance(parameters, list)
        or len(parameters) != 1
        or not isinstance(parameters[0], Mapping)
    ):
        return normalized, False
    parameter = parameters[0]
    if (
        parameter.get("type") != "date"
        or str(parameter.get("value", "")) != dates[0]
    ):
        return normalized, False
    expected = {
        "name": placeholders[0],
        "type": "date",
        "source": "question",
        "value": dates[0],
    }
    if normalized.get("parameters") == [expected]:
        return normalized, False
    normalized["parameters"] = [expected]
    return normalized, True


def _normalize_ordered_parameter_bindings(
    candidate: Mapping[str, Any],
) -> tuple[Dict[str, Any], bool]:
    """Pair unnamed generated parameters with SQL placeholders in order."""
    normalized = deepcopy(dict(candidate))
    if normalized.get("status") != "ready":
        return normalized, False
    parameters = normalized.get("parameters")
    if not isinstance(parameters, list) or not all(
        isinstance(parameter, dict) for parameter in parameters
    ):
        return normalized, False
    placeholders = list(
        dict.fromkeys(_NAMED_PARAMETER.findall(str(normalized.get("sql", ""))))
    )
    if len(parameters) != len(placeholders):
        return normalized, False

    changed = False
    for placeholder, parameter in zip(placeholders, parameters):
        if not parameter.get("name"):
            parameter["name"] = placeholder
            changed = True
        if not parameter.get("source"):
            parameter["source"] = "question"
            changed = True
    return normalized, changed


def _normalize_exact_duplicate_parameter_bindings(
    candidate: Mapping[str, Any],
) -> tuple[Dict[str, Any], bool]:
    """Drop exact duplicate bindings only when SQL cardinality proves the result."""
    normalized = deepcopy(dict(candidate))
    if normalized.get("status") != "ready":
        return normalized, False
    parameters = normalized.get("parameters")
    if not isinstance(parameters, list) or not all(
        isinstance(parameter, dict) for parameter in parameters
    ):
        return normalized, False
    placeholders = list(
        dict.fromkeys(_NAMED_PARAMETER.findall(str(normalized.get("sql", ""))))
    )
    unique: list[dict[str, Any]] = []
    for parameter in parameters:
        if parameter not in unique:
            unique.append(parameter)
    if len(unique) != len(placeholders) or len(unique) == len(parameters):
        return normalized, False
    normalized["parameters"] = unique
    return normalized, True


def _normalize_candidate_draft(
    content: str,
    question: str,
    extension: Mapping[str, Any],
    *,
    label: str,
) -> tuple[Dict[str, Any], bool]:
    value = _decode_exact_object(content, label=label)
    deduplicated, duplicate_normalized = _normalize_exact_duplicate_parameter_bindings(
        value
    )
    ordered, binding_normalized = _normalize_ordered_parameter_bindings(deduplicated)
    binding_normalized = binding_normalized or duplicate_normalized
    normalized, date_normalized = _normalize_single_date_binding(ordered, question)
    binding_normalized = binding_normalized or date_normalized
    grounded = _normalize_grounded_parameter_names(normalized, question, extension)
    binding_normalized = binding_normalized or grounded != normalized
    return grounded, binding_normalized


def _parse_candidate(
    content: str,
    question: str,
    extension: Mapping[str, Any],
    *,
    label: str,
) -> tuple[Dict[str, Any], bool]:
    normalized, binding_normalized = _normalize_candidate_draft(
        content, question, extension, label=label
    )
    if normalized.get("status") == "ready":
        # Target metadata is supplied by Catalyst, not inferred by the model.
        # Canonicalize it deterministically so a typo in an opaque catalog
        # digest cannot discard otherwise valid SQL or skip independent review.
        normalized["target"] = _canonical_target(extension)
    error = _validation_error(_CANDIDATE_VALIDATOR, normalized, label)
    if str(error):
        raise error
    return normalized, binding_normalized


def _canonical_target(extension: Mapping[str, Any]) -> Dict[str, Any]:
    target = extension["target"]
    views = extension["catalog"]["views"]
    return {
        "dataSource": target["dataSource"],
        "catalogVersion": target["catalogVersion"],
        "dialect": target["dialect"],
        "approvedViews": [view["name"] for view in views],
    }


def _candidate_matches_catalog(
    candidate: Mapping[str, Any], canonical_target: Mapping[str, Any]
) -> bool:
    if candidate.get("status") != "ready":
        return True
    return candidate.get("target") == canonical_target


_ISO_DATE_LITERAL = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")


def _bind_question_date_literals(
    candidate: Mapping[str, Any], question: str
) -> Dict[str, Any]:
    """Convert exact question dates into valid named PostgreSQL parameters."""
    normalized = deepcopy(dict(candidate))
    if normalized.get("status") != "ready":
        return normalized

    sql = str(normalized["sql"])
    parameters = list(normalized["parameters"])
    existing_names = {str(parameter["name"]) for parameter in parameters}
    existing_values = {
        str(parameter.get("value"))
        for parameter in parameters
        if parameter.get("type") == "date"
    }
    date_index = 1
    for value in dict.fromkeys(_ISO_DATE_LITERAL.findall(question)):
        quoted = f"'{value}'"
        if quoted not in sql or value in existing_values:
            continue
        while f"date_{date_index}" in existing_names:
            date_index += 1
        name = f"date_{date_index}"
        placeholder = f":{name}"
        typed_date = re.compile(rf"\bDATE\s+{re.escape(quoted)}", flags=re.IGNORECASE)
        sql, replacements = typed_date.subn(placeholder, sql)
        if not replacements:
            sql = sql.replace(quoted, placeholder)
        parameters.append(
            {
                "name": name,
                "type": "date",
                "source": "question",
                "value": value,
            }
        )
        existing_names.add(name)
        existing_values.add(value)
        date_index += 1

    normalized["sql"] = sql
    normalized["parameters"] = parameters
    return normalized


def _phrase_in_question(question: str, phrase: str) -> bool:
    pattern = rf"(?<!\w){re.escape(phrase.strip())}(?!\w)"
    return re.search(pattern, question, flags=re.IGNORECASE) is not None


def _named_semantic_values(
    question: str, extension: Mapping[str, Any]
) -> list[dict[str, str]]:
    """Resolve question terms only against catalog-supplied canonical values."""
    matches: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for view in extension["catalog"]["views"]:
        for dimension in view.get("semanticDimensions") or []:
            if dimension.get("semanticType") != "analyte":
                continue
            field = str(dimension["field"])
            for value in dimension["values"]:
                canonical = str(value["canonical"])
                phrases = [canonical, *value.get("aliases", [])]
                if not any(_phrase_in_question(question, phrase) for phrase in phrases):
                    continue
                key = (field.casefold(), canonical.casefold())
                if key not in seen:
                    matches.append({"field": field, "canonical": canonical})
                    seen.add(key)
    return matches


_RESULT_SUBJECT = re.compile(
    r"^\s*(?:show|list|find)\s+(?:the\s+)?(.+?)\s+results?\b",
    re.IGNORECASE,
)
_GENERIC_RESULT_SUBJECTS = {
    "all",
    "all lab",
    "all laboratory",
    "lab",
    "lab test",
    "lab tests",
    "laboratory",
    "laboratory test",
    "laboratory tests",
    "test",
    "tests",
}
_GENERIC_RESULT_WORDS = {
    "abnormal",
    "all",
    "available",
    "final",
    "flagged",
    "lab",
    "laboratory",
    "latest",
    "negative",
    "non-numeric",
    "normal",
    "numeric",
    "patient",
    "patients",
    "pending",
    "positive",
    "recent",
    "released",
    "test",
    "tests",
}
_RESULT_SUBJECT_MODIFIERS = re.compile(
    r"^(?:(?:top|first|last|most\s+recent|latest|recent|all)\s+|\d+\s+)+",
    re.IGNORECASE,
)


def _unknown_result_analyte(question: str, extension: Mapping[str, Any]) -> str | None:
    """Detect a narrow result-name request outside catalog terminology."""
    has_analyte_terminology = any(
        dimension.get("semanticType") == "analyte" and dimension.get("values")
        for view in extension["catalog"]["views"]
        for dimension in view.get("semanticDimensions") or []
    )
    if not has_analyte_terminology:
        return None
    if _named_semantic_values(question, extension):
        return None
    match = _RESULT_SUBJECT.search(question)
    if not match:
        return None
    subject = _RESULT_SUBJECT_MODIFIERS.sub("", match.group(1).strip()).strip()
    normalized_subject = subject.casefold()
    subject_words = set(re.findall(r"[a-z]+(?:-[a-z]+)?", normalized_subject))
    if (
        not subject
        or normalized_subject in _GENERIC_RESULT_SUBJECTS
        or (subject_words and subject_words <= _GENERIC_RESULT_WORDS)
    ):
        return None
    return subject


def _semantic_binding_failures(
    candidate: Mapping[str, Any],
    question: str,
    extension: Mapping[str, Any],
) -> list[str]:
    """Require named analytes to be bound in predicates on their catalog field."""
    if candidate.get("status") != "ready":
        return []
    requirements = _named_semantic_values(question, extension)
    if not requirements:
        return []

    sql = str(candidate.get("sql", ""))
    parameters = list(candidate.get("parameters", []))
    failures: list[str] = []
    for requirement in requirements:
        field = requirement["field"]
        canonical = requirement["canonical"]
        parameter_names = [
            str(parameter.get("name"))
            for parameter in parameters
            if str(parameter.get("value", "")).casefold() == canonical.casefold()
        ]
        if not parameter_names:
            failures.append(
                f"The named analyte {canonical!r} is not bound as a parameter."
            )
            continue

        qualified_field = rf'(?:\b[A-Za-z_][A-Za-z0-9_]*\.)?"?{re.escape(field)}"?'
        bound = False
        for name in parameter_names:
            placeholder = rf":{re.escape(name)}\b"
            direct = (
                rf"(?:{qualified_field}\s*=\s*{placeholder}|"
                rf"{placeholder}\s*=\s*{qualified_field})"
            )
            membership = rf"{qualified_field}\s+IN\s*\([^)]*{placeholder}[^)]*\)"
            if re.search(direct, sql, re.IGNORECASE) or re.search(
                membership, sql, re.IGNORECASE
            ):
                bound = True
                break
        if not bound:
            failures.append(
                f"The named analyte {canonical!r} is not constrained by {field}."
            )
    return failures


def _semantic_placeholder_names(sql: str, field: str) -> set[str]:
    qualified_field = rf'(?:\b[A-Za-z_][A-Za-z0-9_]*\.)?"?{re.escape(field)}"?'
    name = r"([A-Za-z_][A-Za-z0-9_]*)"
    names = set(re.findall(rf"{qualified_field}\s*=\s*:{name}\b", sql, re.IGNORECASE))
    names.update(re.findall(rf":{name}\b\s*=\s*{qualified_field}", sql, re.IGNORECASE))
    for membership in re.findall(
        rf"{qualified_field}\s+IN\s*\(([^)]*)\)", sql, re.IGNORECASE
    ):
        names.update(_NAMED_PARAMETER.findall(membership))
    return names


_QUESTION_NUMBER_LITERAL = re.compile(r"(?<![\w.])[-+]?\d[\d,]*(?:\.\d+)?(?![\w.])")


def _numeric_value_in_question(value: Any, question: str) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        expected = Decimal(str(value))
    except InvalidOperation:
        return False
    question_without_dates = _ISO_DATE_LITERAL.sub(" ", question)
    for token in _QUESTION_NUMBER_LITERAL.findall(question_without_dates):
        try:
            if Decimal(token.replace(",", "")) == expected:
                return True
        except InvalidOperation:
            continue
    return False


def _parameter_value_grounded_in_question(
    parameter: Mapping[str, Any], question: str
) -> bool:
    """Verify a complete typed parameter value is stated in the question."""
    if parameter.get("source") != "question" or "value" not in parameter:
        return False
    parameter_type = parameter.get("type")
    value = parameter.get("value")
    if parameter_type == "string":
        return (
            isinstance(value, str)
            and bool(value)
            and _phrase_in_question(question, value)
        )
    if parameter_type in {"integer", "number"}:
        return _numeric_value_in_question(value, question)
    if parameter_type == "boolean":
        return isinstance(value, bool) and _phrase_in_question(
            question, str(value).lower()
        )
    if parameter_type in {"date", "date-time"}:
        return isinstance(value, str) and bool(value) and value in question
    if parameter_type == "string-list":
        return (
            isinstance(value, list)
            and bool(value)
            and all(
                isinstance(item, str)
                and bool(item)
                and _phrase_in_question(question, item)
                for item in value
            )
        )
    if parameter_type == "integer-list":
        return (
            isinstance(value, list)
            and bool(value)
            and all(_numeric_value_in_question(item, question) for item in value)
        )
    return False


def _normalize_grounded_parameter_names(
    candidate: Mapping[str, Any],
    question: str,
    extension: Mapping[str, Any],
) -> Dict[str, Any]:
    """Fill only names uniquely grounded by SQL, catalog semantics, and question."""
    normalized = deepcopy(dict(candidate))
    if normalized.get("status") != "ready":
        return normalized
    sql = str(normalized.get("sql", ""))
    placeholders = set(_NAMED_PARAMETER.findall(sql))
    parameters = list(normalized.get("parameters") or [])
    assigned = {
        str(parameter["name"])
        for parameter in parameters
        if isinstance(parameter, Mapping) and parameter.get("name")
    }

    for requirement in _named_semantic_values(question, extension):
        semantic_names = _semantic_placeholder_names(sql, requirement["field"])
        named_parameters = [
            parameter
            for parameter in parameters
            if isinstance(parameter, dict) and parameter.get("name") in semantic_names
        ]
        if len(named_parameters) == 1:
            parameter = named_parameters[0]
            parameter["value"] = requirement["canonical"]
            parameter["type"] = "string"
            parameter["source"] = "question"
            assigned.add(str(parameter["name"]))
            continue

        matching = [
            parameter
            for parameter in parameters
            if isinstance(parameter, dict)
            and str(parameter.get("value", "")).casefold()
            == requirement["canonical"].casefold()
        ]
        available = semantic_names - assigned
        unnamed_strings = [
            parameter
            for parameter in parameters
            if isinstance(parameter, dict)
            and not parameter.get("name")
            and parameter.get("type") == "string"
        ]
        if len(unnamed_strings) == 1 and len(available) == 1:
            parameter = unnamed_strings[0]
            name = available.pop()
            parameter["name"] = name
            parameter["value"] = requirement["canonical"]
            parameter["type"] = "string"
            parameter["source"] = "question"
            assigned.add(name)
            continue
        if len(matching) == 1:
            parameter = matching[0]
            existing_name = parameter.get("name")
            if existing_name in semantic_names:
                assigned.add(str(existing_name))
            elif not existing_name and len(available) == 1:
                name = available.pop()
                parameter["name"] = name
                assigned.add(name)
            parameter.setdefault("type", "string")
            parameter.setdefault("source", "question")
        elif not matching and len(available) == 1:
            name = available.pop()
            parameters.append(
                {
                    "name": name,
                    "type": "string",
                    "source": "question",
                    "value": requirement["canonical"],
                }
            )
            assigned.add(name)

    question_dates = set(_ISO_DATE_LITERAL.findall(question))
    bound_question_dates = {
        str(parameter.get("value"))
        for parameter in parameters
        if isinstance(parameter, Mapping)
        and parameter.get("name")
        and parameter.get("type") == "date"
        and str(parameter.get("value", "")) in question_dates
    }
    unbound_question_dates = question_dates - bound_question_dates
    unnamed_dates = [
        parameter
        for parameter in parameters
        if isinstance(parameter, dict)
        and not parameter.get("name")
        and parameter.get("type") == "date"
        and str(parameter.get("value", "")) in unbound_question_dates
    ]
    available = placeholders - assigned
    if len(unnamed_dates) == 1 and len(available) == 1:
        unnamed_dates[0]["name"] = available.pop()
        unnamed_dates[0].setdefault("source", "question")
    elif not unnamed_dates and len(unbound_question_dates) == 1 and len(available) == 1:
        parameters.append(
            {
                "name": available.pop(),
                "type": "date",
                "source": "question",
                "value": next(iter(unbound_question_dates)),
            }
        )

    turnaround = turnaround_threshold(question)
    if turnaround:
        _operator, threshold_minutes = turnaround
        qualified_field = (
            r'(?:\b[A-Za-z_][A-Za-z0-9_]*\.)?"?receipt_to_release_minutes"?'
        )
        name = r"([A-Za-z_][A-Za-z0-9_]*)"
        threshold_names = set(
            re.findall(rf"{qualified_field}\s*(?:>=|>)\s*:{name}\b", sql, re.IGNORECASE)
        )
        threshold_names.update(
            re.findall(rf":{name}\b\s*(?:<=|<)\s*{qualified_field}", sql, re.IGNORECASE)
        )
        if len(threshold_names) == 1:
            threshold_name = next(iter(threshold_names))
            named = [
                parameter
                for parameter in parameters
                if isinstance(parameter, dict)
                and parameter.get("name") == threshold_name
            ]
            unnamed_numeric = [
                parameter
                for parameter in parameters
                if isinstance(parameter, dict)
                and not parameter.get("name")
                and parameter.get("type") in {"integer", "number"}
            ]
            target_parameter = None
            if len(named) == 1:
                target_parameter = named[0]
            elif len(unnamed_numeric) == 1:
                target_parameter = unnamed_numeric[0]
                target_parameter["name"] = threshold_name
            elif not named and not unnamed_numeric:
                target_parameter = {"name": threshold_name}
                parameters.append(target_parameter)
            if target_parameter is not None:
                target_parameter["type"] = (
                    "integer" if threshold_minutes.is_integer() else "number"
                )
                target_parameter["source"] = "question"
                target_parameter["value"] = (
                    int(threshold_minutes)
                    if threshold_minutes.is_integer()
                    else threshold_minutes
                )

    assigned = {
        str(parameter["name"])
        for parameter in parameters
        if isinstance(parameter, Mapping) and parameter.get("name")
    }
    available = placeholders - assigned
    unnamed = [
        parameter
        for parameter in parameters
        if isinstance(parameter, dict) and not parameter.get("name")
    ]
    if (
        len(available) == 1
        and len(unnamed) == 1
        and _parameter_value_grounded_in_question(unnamed[0], question)
    ):
        unnamed[0]["name"] = next(iter(available))
        unnamed[0].setdefault("source", "question")
        assigned.add(str(unnamed[0]["name"]))

    if not placeholders - assigned:
        parameters = [
            parameter
            for parameter in parameters
            if not (isinstance(parameter, Mapping) and not parameter.get("name"))
        ]

    normalized["parameters"] = parameters
    return normalized


def _semantic_checks(
    checks: list[dict[str, Any]],
    question: str,
    extension: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if not _named_semantic_values(question, extension):
        return checks
    return [
        *checks,
        {
            "name": "named_analyte_constraint",
            "status": "passed",
            "message": (
                "Every analyte named in the question is constrained by its "
                "catalog semantic dimension and canonical bound value."
            ),
        },
    ]


def _lint_validation_checks(
    history: list[dict[str, Any]], checks: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    process_checks = []
    for item in history:
        codes = list(item["finding_codes"])
        process_checks.append(
            {
                "name": f"query_lint_attempt_{item['attempt']}",
                "status": "warned" if codes else "passed",
                "message": (
                    f"Deterministic correction requested for: {', '.join(codes)}."
                    if codes
                    else "Candidate passed deterministic SQL lint."
                ),
            }
        )
    return [*process_checks, *checks]


def _semantic_lint_findings(
    candidate: Mapping[str, Any],
    question: str,
    extension: Mapping[str, Any],
) -> list[dict[str, Any]]:
    requirements = _named_semantic_values(question, extension)
    findings: list[dict[str, Any]] = []
    for message in _semantic_binding_failures(candidate, question, extension):
        requirement = next(
            (
                item
                for item in requirements
                if item["canonical"].casefold() in message.casefold()
            ),
            requirements[0],
        )
        field = requirement["field"]
        canonical = requirement["canonical"]
        findings.append(
            {
                "code": "semantic.named_analyte_constraint",
                "stage": "semantic_constraints",
                "severity": "error",
                "path": "sql",
                "message": message,
                "evidence": f"field={field}; canonical={canonical}",
                "suggestedAction": (
                    f"Add a predicate on {field} using a named parameter and bind its "
                    f"string value exactly as {canonical!r}."
                ),
            }
        )
    return findings


def _contract_lint_finding(error: QueryContractError) -> dict[str, Any]:
    return {
        "code": "contract.invalid_candidate",
        "stage": "output_contract",
        "severity": "error",
        "path": "$",
        "message": str(error),
        "evidence": "candidate failed the strict JSON Schema contract",
        "suggestedAction": (
            "Return exactly one complete JSON candidate matching the supplied schema."
        ),
    }


def _patch_lint_finding(error: QueryPatchError) -> dict[str, Any]:
    return {
        "code": error.code,
        "stage": "query_correct",
        "severity": "error",
        "path": "$",
        "message": str(error),
        "evidence": "generation correction patch was rejected",
        "suggestedAction": (
            "Return only permitted patch operations against the supplied base candidate."
        ),
    }


def _missing_parameter_name_paths(
    candidate: Mapping[str, Any], extension: Mapping[str, Any]
) -> list[str]:
    """Return exact missing-name leaves only when they are the sole schema errors."""
    if candidate.get("status") != "ready":
        return []
    if candidate.get("target") != _canonical_target(extension):
        return []
    parameters = candidate.get("parameters")
    if not isinstance(parameters, list):
        return []
    errors = list(_CANDIDATE_VALIDATOR.iter_errors(candidate))
    if not errors:
        return []

    paths: list[str] = []
    for error in errors:
        absolute_path = list(error.absolute_path)
        schema_path = list(error.absolute_schema_path)
        if (
            error.validator != "required"
            or len(absolute_path) != 2
            or absolute_path[0] != "parameters"
            or not isinstance(absolute_path[1], int)
            or absolute_path[1] < 0
            or absolute_path[1] >= len(parameters)
            or schema_path[-4:] != ["properties", "parameters", "items", "required"]
        ):
            return []
        parameter = parameters[absolute_path[1]]
        if not isinstance(parameter, Mapping):
            return []
        missing = set(error.validator_value) - set(parameter)
        if missing != {"name"}:
            return []
        paths.append(f"/parameters/{absolute_path[1]}/name")

    unique_paths = sorted(set(paths))
    placeholders = set(_NAMED_PARAMETER.findall(str(candidate.get("sql", ""))))
    assigned_names = [
        str(parameter.get("name"))
        for parameter in parameters
        if isinstance(parameter, Mapping) and parameter.get("name")
    ]
    assigned = set(assigned_names)
    if (
        len(unique_paths) != 1
        or len(assigned_names) != len(assigned)
        or not assigned.issubset(placeholders)
        or len(placeholders - assigned) != 1
    ):
        return []
    return unique_paths


def _missing_name_findings(paths: list[str]) -> list[dict[str, Any]]:
    return [
        {
            "code": "contract.parameter_name_required",
            "stage": "output_contract",
            "severity": "error",
            "path": path,
            "message": "A bound parameter is missing its required SQL placeholder name.",
            "evidence": path,
            "suggestedAction": (
                "Add exactly the SQL placeholder name for this parameter without "
                "changing any other field."
            ),
        }
        for path in paths
    ]


def _candidate_parameter_name_paths(candidate: Mapping[str, Any]) -> list[str]:
    sql_names = set(_NAMED_PARAMETER.findall(str(candidate.get("sql", ""))))
    parameters = list(candidate.get("parameters") or [])
    assigned = {
        str(parameter.get("name"))
        for parameter in parameters
        if isinstance(parameter, Mapping) and parameter.get("name") in sql_names
    }
    unbound = sql_names - assigned
    repairable_indices = [
        index
        for index, parameter in enumerate(parameters)
        if isinstance(parameter, Mapping)
        and (not parameter.get("name") or str(parameter.get("name")) not in sql_names)
    ]
    if repairable_indices and len(repairable_indices) == len(unbound):
        return [f"/parameters/{index}/name" for index in repairable_indices]
    return []


def _allowed_patch_paths(
    candidate: Mapping[str, Any], findings: list[dict[str, Any]]
) -> list[str]:
    paths: set[str] = set()
    parameter_name_paths = _candidate_parameter_name_paths(candidate)
    for finding in findings:
        code = str(finding.get("code", ""))
        path = str(finding.get("path", "")).removeprefix("$.")
        if path == "sql" or code.startswith(("catalog.", "policy.", "semantic.")):
            paths.add("/sql")
        if path == "parameters" or code.startswith("binding."):
            paths.update(parameter_name_paths)
            if not parameter_name_paths:
                paths.add("/parameters/-")
        if code in {
            "policy.unbound_predicate_literal",
            "semantic.named_analyte_constraint",
            "semantic.turnaround_threshold",
        }:
            paths.add("/parameters/-")
        if path == "expectedColumns" or code == "output.projection_mismatch":
            paths.add("/sql")
            for index, column in enumerate(candidate.get("expectedColumns") or []):
                if isinstance(column, Mapping) and "name" in column:
                    paths.add(f"/expectedColumns/{index}/name")
    return sorted(paths)


def _decode_pointer(path: str) -> list[str]:
    if not path.startswith("/"):
        raise QueryPatchError(
            "generation.patch_out_of_scope", "Patch path is not a JSON Pointer."
        )
    return [
        segment.replace("~1", "/").replace("~0", "~") for segment in path[1:].split("/")
    ]


def _apply_leaf_patch(candidate: Dict[str, Any], operation: Mapping[str, Any]) -> None:
    path = str(operation["path"])
    segments = _decode_pointer(path)
    parent: Any = candidate
    for segment in segments[:-1]:
        if isinstance(parent, list):
            try:
                parent = parent[int(segment)]
            except (ValueError, IndexError) as error:
                raise QueryPatchError(
                    "generation.patch_out_of_scope",
                    f"Patch path {path!r} does not exist in the base candidate.",
                ) from error
        elif isinstance(parent, dict) and segment in parent:
            parent = parent[segment]
        else:
            raise QueryPatchError(
                "generation.patch_out_of_scope",
                f"Patch path {path!r} does not exist in the base candidate.",
            )

    leaf = segments[-1]
    op = str(operation["op"])
    value = deepcopy(operation.get("value"))
    if isinstance(parent, list):
        if leaf == "-" and op == "add":
            parent.append(value)
            return
        try:
            index = int(leaf)
        except ValueError as error:
            raise QueryPatchError(
                "generation.patch_out_of_scope",
                f"Patch path {path!r} is not a valid list index.",
            ) from error
        if index < 0 or index >= len(parent) or op != "replace":
            raise QueryPatchError(
                "generation.patch_out_of_scope",
                f"Patch operation {op!r} cannot target {path!r}.",
            )
        parent[index] = value
        return
    if not isinstance(parent, dict):
        raise QueryPatchError(
            "generation.patch_out_of_scope",
            f"Patch path {path!r} has no object parent.",
        )
    if op == "add":
        if leaf in parent:
            raise QueryPatchError(
                "generation.patch_out_of_scope",
                f"Patch add path {path!r} already exists.",
            )
        parent[leaf] = value
        return
    if op != "replace" or leaf not in parent:
        raise QueryPatchError(
            "generation.patch_out_of_scope",
            f"Patch replace path {path!r} does not exist.",
        )
    parent[leaf] = value


def _parse_and_apply_patch(
    content: str,
    base_candidate: Mapping[str, Any],
    findings: list[dict[str, Any]],
    allowed_paths: list[str],
    *,
    required_paths: Optional[set[str]] = None,
) -> Dict[str, Any]:
    try:
        value = _decode_exact_object(content, label="query generation patch")
        error = _validation_error(_PATCH_VALIDATOR, value, "query generation patch")
        if str(error):
            raise error
    except QueryContractError as error:
        raise QueryPatchError("contract.invalid_patch", str(error)) from error

    current_codes = {str(finding.get("code")) for finding in findings}
    allowed = set(allowed_paths)
    operations = list(value["patches"])
    for operation in operations:
        if operation["findingCode"] not in current_codes:
            raise QueryPatchError(
                "generation.patch_out_of_scope",
                "Patch findingCode does not match a current deterministic finding.",
            )
        if operation["path"] not in allowed:
            raise QueryPatchError(
                "generation.patch_out_of_scope",
                f"Patch path {operation['path']!r} is outside the permitted scope.",
            )

    leaf_paths = [
        str(operation["path"])
        for operation in operations
        if operation["op"] != "replace_text"
    ]
    if len(leaf_paths) != len(set(leaf_paths)):
        raise QueryPatchError(
            "generation.patch_ambiguous",
            "Patch contains duplicate or overlapping JSON Pointer paths.",
        )
    if required_paths is not None and set(leaf_paths) != required_paths:
        raise QueryPatchError(
            "generation.patch_out_of_scope",
            "Patch must address every and only the required missing-name path.",
        )

    patched = deepcopy(dict(base_candidate))
    sql = str(patched.get("sql", ""))
    text_edits: list[tuple[int, int, str]] = []
    for operation in operations:
        if operation["op"] != "replace_text":
            continue
        old_value = str(operation["oldValue"])
        starts = [match.start() for match in re.finditer(re.escape(old_value), sql)]
        if len(starts) != 1:
            raise QueryPatchError(
                "generation.patch_ambiguous",
                f"Anchored SQL text {old_value!r} must occur exactly once.",
            )
        start = starts[0]
        end = start + len(old_value)
        if any(
            start < other_end and other_start < end
            for other_start, other_end, _ in text_edits
        ):
            raise QueryPatchError(
                "generation.patch_ambiguous",
                "SQL text patches overlap in the frozen base candidate.",
            )
        text_edits.append((start, end, str(operation["replacement"])))

    for start, end, replacement in sorted(text_edits, reverse=True):
        sql = f"{sql[:start]}{replacement}{sql[end:]}"
    if text_edits:
        patched["sql"] = sql

    for operation in operations:
        if operation["op"] != "replace_text":
            _apply_leaf_patch(patched, operation)

    if required_paths:
        parameters = list(patched.get("parameters") or [])
        repaired_indices = {int(path.split("/")[2]) for path in required_paths}
        names = [
            str(parameters[int(path.split("/")[2])].get("name", ""))
            for path in sorted(required_paths)
        ]
        placeholders = set(_NAMED_PARAMETER.findall(str(patched.get("sql", ""))))
        already_assigned_names = [
            str(parameter.get("name"))
            for index, parameter in enumerate(parameters)
            if index not in repaired_indices
            and isinstance(parameter, Mapping)
            and parameter.get("name")
        ]
        already_assigned = set(already_assigned_names)
        if (
            len(names) != len(set(names))
            or len(already_assigned_names) != len(already_assigned)
            or not already_assigned.issubset(placeholders)
            or set(names) != placeholders - already_assigned
        ):
            raise QueryPatchError(
                "generation.patch_out_of_scope",
                "Missing-name patches must map bijectively to the SQL placeholders.",
            )

    return patched


def _request_payload(
    request: Any,
    extension: Mapping[str, Any],
    *,
    candidate: Optional[Mapping[str, Any]] = None,
    review_attempt: Optional[int] = None,
    deterministic_findings: Optional[list[str]] = None,
) -> Dict[str, Any]:
    instruction = str(request.messages[0]["content"])
    payload: Dict[str, Any] = {
        "question": instruction,
        "target": _canonical_target(extension),
        "catalog": extension["catalog"],
        "policy": extension["policy"],
        "requiredOutputContract": extension["requiredOutputContract"],
        "correlation": extension["correlation"],
    }
    if extension.get("contractVersion") == "catalyst.query.request.v2":
        payload["instruction"] = instruction
        payload["revision"] = deepcopy(extension["revision"])
    if candidate is not None:
        payload["candidate"] = candidate
    if review_attempt is not None:
        payload["reviewAttempt"] = review_attempt
    if deterministic_findings:
        payload["deterministicFindings"] = deterministic_findings
    return payload


async def _backend_chat(
    client: httpx.AsyncClient,
    model: str,
    messages: list[dict[str, str]],
    *,
    response_format: Mapping[str, Any],
    temperature: float,
    max_tokens: Optional[int],
) -> str:
    """Use the Hub's shared backend call and response extraction primitives."""
    message = await _chat(
        client,
        model,
        messages,
        response_format=dict(response_format),
        temperature=temperature,
        max_tokens=max_tokens,
    )
    content = message.get("content") if isinstance(message, Mapping) else None
    if not isinstance(content, str) or not content.strip():
        raise QueryContractError("model response did not contain assistant content")
    return content.strip()


def _evidence_digest(value: Any) -> str:
    if isinstance(value, str):
        encoded = value.encode("utf-8")
    else:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_evidence_digest(value: Any) -> str:
    return hashlib.sha256(rfc8785.dumps(value)).hexdigest()


def query_profile_evidence(
    profile: Any, *, max_tokens: Optional[int] = None
) -> dict[str, Any]:
    """Return the exact credential-free writer/reviewer profile snapshot."""
    model_classes = profile.policies.get("model_classes") or {}

    def role_evidence(public_role: str, configured_role: str) -> dict[str, Any]:
        prompt_id = str(profile.prompts[configured_role])
        prompt_text = load_prompt(prompt_id)
        config = dict(profile.knobs.get(configured_role) or {})
        config["maxTokens"] = max_tokens
        return {
            "role": public_role,
            "providerId": str(getattr(llm_config, "provider", "openai-compatible")),
            "modelClass": str(
                model_classes.get(configured_role)
                or profile.models[configured_role].split("-", 1)[0]
            ),
            "modelId": profile.models[configured_role],
            "config": config,
            "systemPrompt": {
                "promptId": prompt_id,
                "version": "1",
                "promptRef": f"server/prompts/{prompt_id}.txt",
                "promptDigest": _evidence_digest(prompt_text),
                "text": prompt_text,
            },
        }

    evidence = {
        "profileId": profile.id,
        "profileName": profile.label,
        "writer": role_evidence("writer", "query_generate"),
        "reviewer": role_evidence("reviewer", "query_review"),
    }
    compact = deepcopy(evidence)
    compact["writer"]["systemPrompt"].pop("text")
    compact["reviewer"]["systemPrompt"].pop("text")
    return {**evidence, "profileDigest": _canonical_evidence_digest(compact)}


def _profile_evidence(request: Any) -> dict[str, Any]:
    return query_profile_evidence(
        request.profile,
        max_tokens=request.max_tokens,
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _finish_invocation(
    invocation: dict[str, Any],
    *,
    outcome: str,
    failure: Any = None,
) -> None:
    invocation["outcome"] = outcome
    invocation["failureDigest"] = (
        None if outcome == "succeeded" else _evidence_digest(failure or outcome)
    )


def _mark_model_validation(
    invocation: dict[str, Any], findings: list[dict[str, Any]]
) -> None:
    if not findings:
        return
    outcome = (
        "contract_failed"
        if any(str(item.get("code", "")).startswith("contract.") for item in findings)
        else "validation_failed"
    )
    _finish_invocation(
        invocation,
        outcome=outcome,
        failure={"findingCodes": [item.get("code") for item in findings]},
    )


async def _invoke_backend(
    client: httpx.AsyncClient,
    model: str,
    messages: list[dict[str, str]],
    *,
    response_format: Mapping[str, Any],
    temperature: float,
    max_tokens: Optional[int],
    invocations: list[dict[str, Any]],
    role: str,
    stage: str,
    attempt: int,
) -> str:
    """Record every physical model call, including transport failures."""
    started_at = _utc_now()
    started = time.monotonic()
    configuration = {
        "temperature": temperature,
        "maxTokens": max_tokens,
        "responseFormat": (
            (response_format.get("json_schema") or {}).get("name")
            if isinstance(response_format, Mapping)
            else None
        ),
    }
    request_payload = {
        "model": model,
        "messages": messages,
        "response_format": response_format,
        "configuration": configuration,
    }
    invocation: dict[str, Any] = {
        "invocationId": str(uuid.uuid4()),
        "role": role,
        "stage": stage,
        "attempt": attempt,
        "providerId": str(getattr(llm_config, "provider", "openai-compatible")),
        "modelId": model,
        "configuration": configuration,
        "startedAt": started_at,
        "endedAt": None,
        "durationMs": None,
        "requestDigest": _evidence_digest(request_payload),
        "responseDigest": None,
        "failureDigest": None,
        "outcome": "in_progress",
    }
    invocations.append(invocation)
    try:
        content = await _backend_chat(
            client,
            model,
            messages,
            response_format=response_format,
            temperature=temperature,
            max_tokens=max_tokens,
        )
    except asyncio.CancelledError as exc:
        _finish_invocation(invocation, outcome="cancelled", failure=repr(exc))
        raise
    except (TimeoutError, httpx.TimeoutException) as exc:
        _finish_invocation(invocation, outcome="timed_out", failure=repr(exc))
        raise
    except QueryContractError as exc:
        invocation["responseDigest"] = _evidence_digest("")
        _finish_invocation(invocation, outcome="contract_failed", failure=str(exc))
        raise
    except Exception as exc:
        _finish_invocation(
            invocation,
            outcome="transport_failed",
            failure={"type": type(exc).__name__, "message": str(exc)},
        )
        raise
    else:
        invocation["responseDigest"] = _evidence_digest(content)
        _finish_invocation(invocation, outcome="succeeded")
        return content
    finally:
        invocation["endedAt"] = _utc_now()
        invocation["durationMs"] = max(0, int((time.monotonic() - started) * 1000))


async def _generate(
    client: httpx.AsyncClient,
    request: Any,
    extension: Mapping[str, Any],
    invocations: list[dict[str, Any]],
) -> tuple[Dict[str, Any], int, bool, list[dict[str, Any]]]:
    profile = request.profile
    prompt = load_prompt(str(profile.prompts["query_generate"]))
    messages = [
        {"role": "system", "content": prompt},
        {
            "role": "user",
            "content": json.dumps(
                _request_payload(request, extension),
                separators=(",", ":"),
            ),
        },
    ]
    max_attempts = int(profile.policies.get("generation_attempts", 2))
    question = request.messages[0]["content"]
    seen_outputs: set[str] = set()
    history: list[dict[str, Any]] = []
    binding_normalized = False
    last_candidate: Optional[Dict[str, Any]] = None
    last_output: Optional[str] = None
    correction_base: Optional[Dict[str, Any]] = None
    correction_findings: list[dict[str, Any]] = []
    allowed_patch_paths: list[str] = []
    required_patch_paths: Optional[set[str]] = None

    for attempt in range(1, max_attempts + 1):
        using_patch = correction_base is not None
        response_format = (
            _patch_format(
                allowed_patch_paths,
                [str(finding["code"]) for finding in correction_findings],
                add_only_paths=required_patch_paths,
            )
            if using_patch
            else _GENERATION_FORMAT
        )
        content = await _invoke_backend(
            client,
            profile.models["query_generate"],
            messages,
            response_format=response_format,
            temperature=float(profile.knobs["query_generate"]["temperature"]),
            max_tokens=request.max_tokens,
            invocations=invocations,
            role="writer",
            stage=(
                "followup_generation"
                if extension.get("contractVersion") == "catalyst.query.request.v2"
                else "initial_generation"
            ),
            attempt=attempt,
        )
        last_output = content
        if content in seen_outputs:
            finding = {
                "code": "generation.unchanged_candidate",
                "stage": "query_correct",
                "severity": "error",
                "path": "$",
                "message": "The model repeated an unchanged candidate after feedback.",
                "evidence": "candidate output matched an earlier attempt",
                "suggestedAction": "Stop retrying and reject this generation run.",
            }
            history.append(
                {
                    "attempt": attempt,
                    "status": "failed",
                    "finding_codes": [finding["code"]],
                    "findings": [finding],
                }
            )
            _mark_model_validation(invocations[-1], [finding])
            raise QueryGenerationError(
                finding["message"],
                history,
                candidate=last_candidate,
                raw_output=last_output,
            )
        seen_outputs.add(content)

        parsed: Optional[Dict[str, Any]] = None
        normalized_this_attempt = False
        patch_rejected: Optional[list[dict[str, Any]]] = None
        partial_base = False
        if using_patch:
            try:
                parsed = _parse_and_apply_patch(
                    content,
                    correction_base,
                    correction_findings,
                    allowed_patch_paths,
                    required_paths=required_patch_paths,
                )
                grounded = _normalize_grounded_parameter_names(
                    parsed, question, extension
                )
                normalized_this_attempt = grounded != parsed
                parsed = grounded
                candidate_error = _validation_error(
                    _CANDIDATE_VALIDATOR,
                    parsed,
                    f"query generation patch attempt {attempt}",
                )
                if str(candidate_error):
                    missing_paths = _missing_parameter_name_paths(parsed, extension)
                    if not missing_paths:
                        raise QueryPatchError(
                            "contract.invalid_patch", str(candidate_error)
                        )
                    correction_base = deepcopy(parsed)
                    correction_findings = _missing_name_findings(missing_paths)
                    allowed_patch_paths = missing_paths
                    required_patch_paths = set(missing_paths)
                    findings = correction_findings
                    partial_base = True
                    parsed = None
                else:
                    if not _candidate_matches_catalog(
                        parsed, _canonical_target(extension)
                    ):
                        raise QueryPatchError(
                            "generation.patch_out_of_scope",
                            "Patch reconstruction changed or retained a non-canonical target.",
                        )
                    last_candidate = deepcopy(parsed)
                    findings = [
                        *lint_candidate(parsed, extension, instruction=question),
                        *_semantic_lint_findings(parsed, question, extension),
                    ]
            except QueryPatchError as error:
                logger.warning("Catalyst query correction patch failed: %s", error)
                findings = [_patch_lint_finding(error)]
                patch_rejected = findings
        else:
            try:
                parsed, normalized_this_attempt = _parse_candidate(
                    content,
                    question,
                    extension,
                    label=f"query generation attempt {attempt}",
                )
                bound = _bind_question_date_literals(parsed, question)
                normalized_this_attempt = normalized_this_attempt or bound != parsed
                parsed = bound
                last_candidate = deepcopy(parsed)
                findings = [
                    *lint_candidate(parsed, extension, instruction=question),
                    *_semantic_lint_findings(parsed, question, extension),
                ]
            except QueryContractError as error:
                logger.warning(
                    "Catalyst query candidate failed deterministic validation: %s",
                    error,
                )
                findings = [_contract_lint_finding(error)]
                try:
                    draft, normalized_this_attempt = _normalize_candidate_draft(
                        content,
                        question,
                        extension,
                        label=f"query generation attempt {attempt}",
                    )
                    missing_paths = _missing_parameter_name_paths(draft, extension)
                except QueryContractError:
                    draft = None
                    missing_paths = []
                if draft is not None and missing_paths:
                    correction_base = deepcopy(draft)
                    correction_findings = _missing_name_findings(missing_paths)
                    allowed_patch_paths = missing_paths
                    required_patch_paths = set(missing_paths)
                    findings = correction_findings
                    partial_base = True

        binding_normalized = binding_normalized or normalized_this_attempt
        _mark_model_validation(invocations[-1], findings)
        history.append(
            {
                "attempt": attempt,
                "status": "passed" if not findings else "failed",
                "finding_codes": [finding["code"] for finding in findings],
                "findings": findings,
            }
        )
        if profile.policies.get("collaborative_review") is True and parsed is not None:
            return parsed, attempt, binding_normalized, history
        if not findings and parsed is not None:
            return parsed, attempt, binding_normalized, history
        if attempt == max_attempts:
            codes = ", ".join(finding["code"] for finding in findings)
            raise QueryGenerationError(
                f"query generation exhausted deterministic correction budget: {codes}",
                history,
                candidate=last_candidate,
                raw_output=last_output,
            )

        if using_patch:
            if patch_rejected is None and parsed is not None:
                correction_base = deepcopy(parsed)
                correction_findings = findings
                allowed_patch_paths = _allowed_patch_paths(parsed, findings)
                required_patch_paths = None
            if not allowed_patch_paths:
                raise QueryGenerationError(
                    "query generation findings could not be localized to patch paths",
                    history,
                    candidate=last_candidate,
                    raw_output=last_output,
                )
        elif parsed is not None:
            correction_base = deepcopy(parsed)
            correction_findings = findings
            allowed_patch_paths = _allowed_patch_paths(parsed, findings)
            required_patch_paths = None
        elif not partial_base:
            correction_base = None

        if correction_base is not None:
            if not allowed_patch_paths:
                raise QueryGenerationError(
                    "query generation findings could not be localized to patch paths",
                    history,
                    candidate=last_candidate,
                    raw_output=last_output,
                )
            correction_request: Dict[str, Any] = {
                "attempt": attempt + 1,
                "instruction": (
                    "Return only typed patch operations for the permitted paths. "
                    "Do not return the full candidate. Preserve every unaffected "
                    "field and return JSON only."
                ),
                "baseCandidate": correction_base,
                "allowedPatchPaths": allowed_patch_paths,
                "findings": correction_findings,
            }
            if patch_rejected is not None:
                correction_request["lastPatchRejection"] = patch_rejected
        else:
            correction_request = {
                "attempt": attempt + 1,
                "instruction": (
                    "The prior response was not a structurally parseable candidate. "
                    "Return one complete candidate matching the supplied schema, "
                    "without changing the question, target, catalog, or policy. "
                    "Return JSON only."
                ),
                "findings": findings,
            }
        feedback = {"correctionRequest": correction_request}
        messages = [
            *messages,
            {"role": "assistant", "content": content},
            {
                "role": "user",
                "content": json.dumps(feedback, separators=(",", ":")),
            },
        ]

    raise AssertionError("generation attempt loop terminated unexpectedly")


async def _review(
    client: httpx.AsyncClient,
    request: Any,
    extension: Mapping[str, Any],
    candidate: Mapping[str, Any],
    *,
    attempt: int,
    invocations: list[dict[str, Any]],
    deterministic_findings: Optional[list[dict[str, Any]]] = None,
) -> tuple[Dict[str, Any], int]:
    profile = request.profile
    prompt = load_prompt(str(profile.prompts["query_review"]))
    messages = [
        {"role": "system", "content": prompt},
        {
            "role": "user",
            "content": json.dumps(
                _request_payload(
                    request,
                    extension,
                    candidate=candidate,
                    review_attempt=attempt,
                    deterministic_findings=deterministic_findings,
                ),
                separators=(",", ":"),
            ),
        },
    ]
    response_format = (
        _REPAIR_FORMAT
        if deterministic_findings
        else _REVIEW_FORMAT
    )
    content = await _invoke_backend(
        client,
        profile.models["query_review"],
        messages,
        response_format=response_format,
        temperature=float(profile.knobs["query_review"]["temperature"]),
        max_tokens=request.max_tokens,
        invocations=invocations,
        role="reviewer",
        stage="review",
        attempt=attempt,
    )
    try:
        return (
            _parse_review_object(
                content,
                label="query review",
                flat_repair=bool(deterministic_findings),
                question=request.messages[0]["content"],
                extension=extension,
            ),
            1,
        )
    except QueryContractError as error:
        _finish_invocation(
            invocations[-1], outcome="contract_failed", failure=str(error)
        )
        is_revision = extension.get("contractVersion") == "catalyst.query.request.v2"
        if not deterministic_findings and not is_revision:
            raise
        if deterministic_findings:
            correction_instruction = (
                "Your repair JSON failed the strict output contract: "
                f"{error}. Return one corrected JSON object only. The "
                "top-level repair fields must be complete, including "
                "status, exact target, full SQL, all parameters, and "
                "expected columns."
            )
        else:
            correction_instruction = (
                "Your review JSON failed the strict output contract: "
                f"{error}. Return one corrected JSON object only with "
                "decision and checks, plus one complete candidate only when "
                "decision is repair."
            )
        corrected = await _invoke_backend(
            client,
            profile.models["query_review"],
            [
                *messages,
                {"role": "assistant", "content": content},
                {
                    "role": "user",
                    "content": correction_instruction,
                },
            ],
            response_format=response_format,
            temperature=float(profile.knobs["query_review"]["temperature"]),
            max_tokens=request.max_tokens,
            invocations=invocations,
            role="reviewer",
            stage="review",
            attempt=attempt + 1,
        )
        try:
            parsed = _parse_review_object(
                corrected,
                label="query review correction",
                flat_repair=bool(deterministic_findings),
                question=request.messages[0]["content"],
                extension=extension,
            )
        except QueryContractError as correction_error:
            _finish_invocation(
                invocations[-1],
                outcome="contract_failed",
                failure=str(correction_error),
            )
            raise
        return parsed, 2


def _validation_for(
    candidate: Mapping[str, Any], checks: list[dict[str, Any]]
) -> Dict[str, Any]:
    status = candidate["status"]
    statuses = {check["status"] for check in checks}
    if status == "ready":
        if "failed" in statuses:
            raise QueryContractError("review approved a ready query with failed checks")
        validation_status = "warned" if "warned" in statuses else "passed"
    elif status == "needs_clarification":
        validation_status = "warned"
        if "warned" not in statuses:
            checks = [
                *checks,
                {
                    "name": "query_status",
                    "status": "warned",
                    "message": "The query requires clarification.",
                },
            ]
    else:
        validation_status = "rejected"
        if "failed" not in statuses:
            checks = [
                *checks,
                {
                    "name": "query_status",
                    "status": "failed",
                    "message": "The query is not executable.",
                },
            ]
    return {"status": validation_status, "checks": checks}


def _provenance(profile_id: str, extension: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "profileId": profile_id,
        "traceId": extension["correlation"]["traceId"],
        "contextSourceIds": [extension["catalog"]["contextSourceId"]],
    }


def _collaboration_for_response(
    collaboration: Mapping[str, Any], extension: Mapping[str, Any]
) -> dict[str, Any]:
    value = deepcopy(dict(collaboration))
    if extension.get("contractVersion") == "catalyst.query.request.v2":
        revision = extension["revision"]
        value["base"] = {
            "baseClassification": revision["baseClassification"],
            "observedBase": deepcopy(revision["observedBase"]),
            "effectiveBaseVersion": deepcopy(revision["effectiveBaseVersion"]),
            "editorDigest": revision["editorSnapshot"]["editorDigest"],
        }
    return value


def _finalize(
    question: str,
    extension: Mapping[str, Any],
    candidate: Mapping[str, Any],
    checks: list[dict[str, Any]],
    *,
    profile_id: str,
    model_collaboration: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    status = str(candidate["status"])
    result: Dict[str, Any] = {
        "contractVersion": "catalyst.query.v1",
        "deploymentMode": "demo",
        "status": status,
        "question": question,
    }
    if status == "ready":
        result["target"] = _canonical_target(extension)
        for field in ("sql", "parameters", "expectedColumns"):
            result[field] = deepcopy(candidate[field])
    else:
        for field in _STATUS_FIELDS[status]:
            result[field] = candidate[field]
    result["validation"] = _validation_for(candidate, checks)
    result["provenance"] = _provenance(profile_id, extension)
    if model_collaboration is not None:
        result["modelCollaboration"] = _collaboration_for_response(
            model_collaboration, extension
        )
    error = _validation_error(_FINAL_VALIDATOR, result, "final query contract")
    if str(error):
        raise error
    return result


def _rejected(
    question: str,
    extension: Mapping[str, Any],
    *,
    message: str,
    check_name: str,
    checks: Optional[list[dict[str, Any]]] = None,
    diagnostic_candidate: Optional[Mapping[str, Any]] = None,
    profile_id: str = "catalyst-query-checked",
    model_collaboration: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    final_checks = deepcopy(checks or [])
    if not any(check.get("status") == "failed" for check in final_checks):
        final_checks.append(
            {
                "name": check_name,
                "status": "failed",
                "message": message,
            }
        )
    result = {
        "contractVersion": "catalyst.query.v1",
        "deploymentMode": "demo",
        "status": "rejected",
        "question": question,
        "message": message,
        "validation": {
            "status": "rejected",
            "checks": final_checks,
        },
        "provenance": _provenance(profile_id, extension),
    }
    if diagnostic_candidate is not None:
        result["diagnosticCandidate"] = deepcopy(diagnostic_candidate)
    if model_collaboration is not None:
        result["modelCollaboration"] = _collaboration_for_response(
            model_collaboration, extension
        )
    error = _validation_error(_FINAL_VALIDATOR, result, "rejected query contract")
    if str(error):  # pragma: no cover - fixed fields are covered by contract tests
        raise error
    return result


def _write_trace(
    request: Any,
    extension: Mapping[str, Any],
    result: Mapping[str, Any],
    steps: list[dict[str, Any]],
) -> None:
    """Append query correlation metadata without using clinical trace fields."""
    try:
        trace_dir = Path(os.getenv("TEAM_TRACE_DIR", "/app/trace"))
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level_id": request.profile.id,
            "trace_id": extension["correlation"]["traceId"],
            "request_id": extension["correlation"]["requestId"],
            "question": str(request.messages[0]["content"])[:2000],
            "context_source_ids": [extension["catalog"]["contextSourceId"]],
            "models": {
                "generator": request.profile.models["query_generate"],
                "reviewer": request.profile.models["query_review"],
            },
            "sampling": {
                "generator_temperature": request.profile.knobs["query_generate"][
                    "temperature"
                ],
                "reviewer_temperature": request.profile.knobs["query_review"][
                    "temperature"
                ],
            },
            "status": result["status"],
            "steps": steps,
            "model_invocations": deepcopy(
                (result.get("_hubEvidence") or {}).get("modelInvocations", [])
            ),
        }
        trace_dir.mkdir(parents=True, exist_ok=True)
        with (trace_dir / "trace.jsonl").open("a", encoding="utf-8") as file:
            file.write(json.dumps(entry, separators=(",", ":")) + "\n")
    except Exception as exc:  # pragma: no cover - tracing is best effort
        logger.warning("query trace write failed (non-fatal): %s", exc)


def _attach_model_evidence(
    result: Dict[str, Any], request: Any, invocations: list[dict[str, Any]]
) -> None:
    result["_hubEvidence"] = {
        "profileEvidence": _profile_evidence(request),
        "modelInvocations": deepcopy(invocations),
        "totalModelInvocationDurationMs": sum(
            int(item["durationMs"]) for item in invocations
        ),
    }


async def execute_query_profile(
    request: Any,
) -> AsyncIterator[Tuple[str, str]]:
    """Execute generation, independent review, one repair, and final validation."""
    extension = request.catalyst_query
    question = request.messages[0].get("content", "") if request.messages else ""
    if not isinstance(extension, Mapping):
        raise QueryContractError(
            "query profile execution requires a validated catalystQuery context"
        )
    is_revision = extension.get("contractVersion") == "catalyst.query.request.v2"
    steps: list[dict[str, Any]] = [
        {
            "role": "context",
            "context_source_ids": [extension["catalog"]["contextSourceId"]],
        }
    ]
    invocations: list[dict[str, Any]] = []
    result: Dict[str, Any]
    canonical_target = _canonical_target(extension)
    approved_views = canonical_target["approvedViews"]
    if len(approved_views) != len(set(approved_views)):
        result = _rejected(
            question,
            extension,
            message="The approved catalog contains duplicate view names.",
            check_name="catalog_context",
            profile_id=request.profile.id,
        )
        _attach_model_evidence(result, request, invocations)
        _write_trace(request, extension, result, steps)
        yield "result", json.dumps(result, separators=(",", ":"))
        return

    unknown_analyte = _unknown_result_analyte(question, extension)
    if unknown_analyte:
        message = (
            "The approved catalog does not contain a grounded analyte matching "
            f"{unknown_analyte!r}."
        )
        result = _finalize(
            question,
            extension,
            {"status": "unsupported", "message": message},
            [{"name": "catalog_scope", "status": "failed", "message": message}],
            profile_id=request.profile.id,
        )
        steps.append(
            {
                "role": "catalog_scope",
                "status": "unsupported",
                "subject": unknown_analyte,
            }
        )
        steps.append({"role": "query_finalize", "status": result["status"]})
        _attach_model_evidence(result, request, invocations)
        _write_trace(request, extension, result, steps)
        yield "result", json.dumps(result, separators=(",", ":"))
        return

    async with httpx.AsyncClient() as client:
        try:
            (
                candidate,
                generation_attempts,
                binding_normalized,
                lint_history,
            ) = await _generate(client, request, extension, invocations)
            steps.append(
                {
                    "role": "query_generate",
                    "status": candidate["status"],
                    "attempts": generation_attempts,
                    "binding_normalized": binding_normalized,
                }
            )
            steps.extend(
                {
                    "role": "query_lint",
                    "attempt": lint_attempt["attempt"],
                    "status": lint_attempt["status"],
                    "finding_codes": lint_attempt["finding_codes"],
                    "findings": lint_attempt["findings"],
                }
                for lint_attempt in lint_history
            )
        except asyncio.CancelledError as exc:
            result = _rejected(
                question,
                extension,
                message="Query generation was cancelled.",
                check_name="query_generate",
                profile_id=request.profile.id,
            )
            steps.append(
                {"role": "query_generate", "status": "cancelled", "message": str(exc)}
            )
            _attach_model_evidence(result, request, invocations)
            _write_trace(request, extension, result, steps)
            raise
        except Exception as exc:
            logger.warning("Catalyst query generation failed: %s", exc)
            diagnostic_candidate = None
            if isinstance(exc, QueryGenerationError):
                diagnostic_candidate = {
                    "executable": False,
                    "attempts": exc.history,
                }
                if exc.candidate is not None:
                    diagnostic_candidate["candidate"] = exc.candidate
                if exc.raw_output is not None:
                    diagnostic_candidate["rawOutput"] = exc.raw_output
            result = _rejected(
                question,
                extension,
                message="Query generation failed its structured-output contract.",
                check_name="query_generate",
                diagnostic_candidate=diagnostic_candidate,
                profile_id=request.profile.id,
            )
            steps.append(
                {
                    "role": "query_generate",
                    "status": "failed",
                    "message": str(exc),
                }
            )
            if isinstance(exc, QueryGenerationError):
                steps.extend(
                    {
                        "role": "query_lint",
                        "attempt": lint_attempt["attempt"],
                        "status": lint_attempt["status"],
                        "finding_codes": lint_attempt["finding_codes"],
                        "findings": lint_attempt["findings"],
                    }
                    for lint_attempt in exc.history
                )
        else:
            if not _candidate_matches_catalog(candidate, canonical_target):
                result = _rejected(
                    question,
                    extension,
                    message=(
                        "Generated query did not echo the approved catalog target."
                    ),
                    check_name="catalog_target",
                    diagnostic_candidate={
                        "executable": False,
                        "candidate": candidate,
                    },
                    profile_id=request.profile.id,
                )
                steps.append({"role": "catalog_target", "status": "failed"})
            else:
                try:
                    collaborative_review = (
                        request.profile.policies.get("collaborative_review") is True
                    )
                    if collaborative_review:
                        model_classes = request.profile.policies.get("model_classes")
                        if not isinstance(model_classes, Mapping) or model_classes.get(
                            "query_generate"
                        ) == model_classes.get("query_review"):
                            raise QueryContractError(
                                "collaborative query roles require different model "
                                "classes"
                            )
                    model_collaboration = None
                    writer_findings = (
                        deepcopy(lint_history[-1]["findings"])
                        if collaborative_review and lint_history
                        else []
                    )
                    semantic_failures = (
                        []
                        if collaborative_review
                        else _semantic_binding_failures(candidate, question, extension)
                    )
                    if not collaborative_review and semantic_failures:
                        raise QueryContractError(
                            "candidate reached review with deterministic semantic "
                            "failures"
                        )
                    review, review_model_attempts = await _review(
                        client,
                        request,
                        extension,
                        candidate,
                        attempt=1,
                        invocations=invocations,
                        deterministic_findings=writer_findings or None,
                    )
                    steps.append(
                        {
                            "role": "query_review",
                            "attempt": 1,
                            "model_attempts": review_model_attempts,
                            "decision": review["decision"],
                            "deterministic_findings": len(writer_findings),
                        }
                    )
                    if writer_findings and review["decision"] == "approve":
                        raise QueryContractError(
                            "review approved a candidate with deterministic findings"
                        )
                    if review["decision"] == "reject":
                        model_collaboration = (
                            {
                                "writer": {
                                    "model": request.profile.models["query_generate"],
                                    "candidate": deepcopy(candidate),
                                    "lintFindings": writer_findings,
                                    **(
                                        {"disposition": "retained_unselected"}
                                        if is_revision
                                        else {}
                                    ),
                                },
                                "reviewer": {
                                    "model": request.profile.models["query_review"],
                                    "decision": "reject",
                                    "checks": deepcopy(review["checks"]),
                                    **(
                                        {"disposition": "diagnostic_only"}
                                        if is_revision
                                        else {}
                                    ),
                                },
                                "finalLintFindings": writer_findings,
                            }
                            if collaborative_review
                            else None
                        )
                        result = _rejected(
                            question,
                            extension,
                            message=review["message"],
                            check_name="query_review",
                            checks=list(review["checks"]),
                            diagnostic_candidate={
                                "executable": False,
                                "candidate": candidate,
                            },
                            profile_id=request.profile.id,
                            model_collaboration=model_collaboration,
                        )
                    elif review["decision"] == "repair":
                        repaired = _bind_question_date_literals(
                            review["candidate"], question
                        )
                        candidate_error = _validation_error(
                            _CANDIDATE_VALIDATOR,
                            repaired,
                            "query repair",
                        )
                        if str(candidate_error):
                            raise candidate_error
                        if not _candidate_matches_catalog(repaired, canonical_target):
                            raise QueryContractError(
                                "query repair changed the approved catalog target"
                            )
                        repaired_findings = [
                            *lint_candidate(repaired, extension, instruction=question),
                            *_semantic_lint_findings(repaired, question, extension),
                        ]
                        model_collaboration = (
                            {
                                "writer": {
                                    "model": request.profile.models["query_generate"],
                                    "candidate": deepcopy(candidate),
                                    "lintFindings": writer_findings,
                                    **(
                                        {"disposition": "superseded"}
                                        if is_revision
                                        else {}
                                    ),
                                },
                                "reviewer": {
                                    "model": request.profile.models["query_review"],
                                    "decision": "repair",
                                    "candidate": deepcopy(repaired),
                                    "checks": deepcopy(review["checks"]),
                                    **(
                                        {"disposition": "selected"}
                                        if is_revision
                                        else {}
                                    ),
                                },
                                "finalLintFindings": deepcopy(repaired_findings),
                            }
                            if collaborative_review
                            else None
                        )
                        steps.append(
                            {
                                "role": "query_lint",
                                "attempt": "review_repair",
                                "status": ("failed" if repaired_findings else "passed"),
                                "finding_codes": [
                                    finding["code"] for finding in repaired_findings
                                ],
                                "findings": repaired_findings,
                            }
                        )
                        if repaired_findings:
                            _finish_invocation(
                                invocations[-1],
                                outcome="validation_failed",
                                failure={
                                    "findingCodes": [
                                        finding["code"] for finding in repaired_findings
                                    ]
                                },
                            )
                            logger.warning(
                                "Catalyst repaired candidate retained deterministic "
                                "lint failures: %s",
                                "; ".join(
                                    finding["message"] for finding in repaired_findings
                                ),
                            )
                            raise QueryContractError(
                                "review repair failed deterministic lint"
                            )
                        if collaborative_review:
                            final_checks = [
                                {
                                    "name": "reviewer_correction_lint",
                                    "status": "passed",
                                    "message": (
                                        "The reviewer's complete corrected query "
                                        "passed the deterministic contract and SQL "
                                        "lint."
                                    ),
                                }
                            ]
                        else:
                            second_review, second_review_model_attempts = await _review(
                                client,
                                request,
                                extension,
                                repaired,
                                attempt=2,
                                invocations=invocations,
                            )
                            steps.append(
                                {
                                    "role": "query_review",
                                    "attempt": 2,
                                    "model_attempts": second_review_model_attempts,
                                    "decision": second_review["decision"],
                                    "deterministic_findings": 0,
                                }
                            )
                            if second_review["decision"] != "approve":
                                raise QueryContractError(
                                    "repaired query did not pass independent re-review"
                                )
                            final_checks = list(second_review["checks"])
                        result = _finalize(
                            question,
                            extension,
                            repaired,
                            _semantic_checks(
                                _lint_validation_checks(lint_history, final_checks),
                                question,
                                extension,
                            ),
                            profile_id=request.profile.id,
                            model_collaboration=model_collaboration,
                        )
                    else:
                        model_collaboration = (
                            {
                                "writer": {
                                    "model": request.profile.models["query_generate"],
                                    "candidate": deepcopy(candidate),
                                    "lintFindings": writer_findings,
                                    **(
                                        {"disposition": "selected"}
                                        if is_revision
                                        else {}
                                    ),
                                },
                                "reviewer": {
                                    "model": request.profile.models["query_review"],
                                    "decision": "approve",
                                    "checks": deepcopy(review["checks"]),
                                    **(
                                        {"disposition": "selected"}
                                        if is_revision
                                        else {}
                                    ),
                                },
                                "finalLintFindings": [],
                            }
                            if collaborative_review
                            else None
                        )
                        result = _finalize(
                            question,
                            extension,
                            candidate,
                            _semantic_checks(
                                _lint_validation_checks(
                                    lint_history, list(review["checks"])
                                ),
                                question,
                                extension,
                            ),
                            profile_id=request.profile.id,
                            model_collaboration=model_collaboration,
                        )
                except asyncio.CancelledError as exc:
                    result = _rejected(
                        question,
                        extension,
                        message="Query review was cancelled.",
                        check_name="query_review",
                        diagnostic_candidate={
                            "executable": False,
                            "candidate": candidate,
                        },
                        profile_id=request.profile.id,
                    )
                    steps.append(
                        {
                            "role": "query_review",
                            "status": "cancelled",
                            "message": str(exc),
                        }
                    )
                    _attach_model_evidence(result, request, invocations)
                    _write_trace(request, extension, result, steps)
                    raise
                except Exception as exc:
                    logger.warning("Catalyst query review failed: %s", exc)
                    if is_revision and collaborative_review:
                        if model_collaboration is not None:
                            model_collaboration = deepcopy(model_collaboration)
                            model_collaboration["writer"][
                                "disposition"
                            ] = "retained_unselected"
                            model_collaboration["reviewer"][
                                "disposition"
                            ] = "diagnostic_only"
                            if not model_collaboration[
                                "finalLintFindings"
                            ] and isinstance(exc, QueryContractError):
                                model_collaboration["finalLintFindings"] = [
                                    _contract_lint_finding(exc)
                                ]
                        else:
                            repaired_candidate = locals().get("repaired")
                            review_result = locals().get("review")
                            if (
                                isinstance(repaired_candidate, Mapping)
                                and isinstance(review_result, Mapping)
                                and review_result.get("decision") == "repair"
                            ):
                                repair_findings = locals().get("repaired_findings")
                                if not isinstance(repair_findings, list):
                                    repair_findings = (
                                        [_contract_lint_finding(exc)]
                                        if isinstance(exc, QueryContractError)
                                        else []
                                    )
                                model_collaboration = {
                                    "writer": {
                                        "model": request.profile.models[
                                            "query_generate"
                                        ],
                                        "candidate": deepcopy(candidate),
                                        "lintFindings": deepcopy(writer_findings),
                                        "disposition": "retained_unselected",
                                    },
                                    "reviewer": {
                                        "model": request.profile.models["query_review"],
                                        "decision": "repair",
                                        "candidate": deepcopy(repaired_candidate),
                                        "checks": deepcopy(
                                            review_result.get("checks", [])
                                        ),
                                        "disposition": "diagnostic_only",
                                    },
                                    "finalLintFindings": deepcopy(repair_findings),
                                }
                            else:
                                model_collaboration = {
                                    "writer": {
                                        "model": request.profile.models[
                                            "query_generate"
                                        ],
                                        "candidate": deepcopy(candidate),
                                        "lintFindings": deepcopy(writer_findings),
                                        "disposition": "retained_unselected",
                                    },
                                    "reviewer": {
                                        "model": request.profile.models["query_review"],
                                        "decision": "failed",
                                        "checks": [],
                                        "disposition": "diagnostic_only",
                                    },
                                    "finalLintFindings": deepcopy(writer_findings),
                                }
                    result = _rejected(
                        question,
                        extension,
                        message=f"Query review failed: {exc}",
                        check_name="query_review",
                        diagnostic_candidate={
                            "executable": False,
                            "candidate": locals().get("repaired", candidate),
                        },
                        profile_id=request.profile.id,
                        model_collaboration=model_collaboration,
                    )
                    steps.append({"role": "query_review", "status": "failed"})

    steps.append({"role": "query_finalize", "status": result["status"]})
    _attach_model_evidence(result, request, invocations)
    _write_trace(request, extension, result, steps)
    yield "result", json.dumps(result, separators=(",", ":"))
