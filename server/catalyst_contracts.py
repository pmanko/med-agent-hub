"""Offline JSON Schema bundle for the Catalyst query request protocols."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource


CONTRACT_DIR = Path(__file__).parent / "contracts"
REQUEST_V1_ID = (
    "https://openelis-global.org/catalyst/contracts/"
    "catalyst-query-request-v1.schema.json"
)
REQUEST_V2_ID = (
    "https://openelis-global.org/catalyst/contracts/"
    "catalyst-query-request-v2.schema.json"
)

_BUNDLED_FILES = (
    "catalyst-query-request-v1.schema.json",
    "catalyst-query-request-v2.schema.json",
    "catalyst-query-revision-context-v1.schema.json",
    "catalyst-workbench-editor-snapshot-v1.schema.json",
    "catalyst-workbench-turn-request-v1.schema.json",
)


def _load_bundle() -> dict[str, dict[str, Any]]:
    bundle: dict[str, dict[str, Any]] = {}
    for filename in _BUNDLED_FILES:
        schema = json.loads((CONTRACT_DIR / filename).read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)
        schema_id = schema.get("$id")
        if not isinstance(schema_id, str) or not schema_id:
            raise ValueError(f"bundled Catalyst contract {filename!r} has no $id")
        if schema_id in bundle:
            raise ValueError(f"duplicate bundled Catalyst contract id {schema_id!r}")
        bundle[schema_id] = schema
    return bundle


CONTRACT_BUNDLE = _load_bundle()
CONTRACT_REGISTRY = Registry().with_resources(
    (schema_id, Resource.from_contents(schema))
    for schema_id, schema in CONTRACT_BUNDLE.items()
)
_FORMAT_CHECKER = FormatChecker()


def schema_for(schema_id: str) -> Mapping[str, Any]:
    """Return a bundled schema without permitting a network retrieval fallback."""
    try:
        return CONTRACT_BUNDLE[schema_id]
    except KeyError as exc:
        raise KeyError(f"unknown bundled Catalyst contract {schema_id!r}") from exc


def validator_for(schema_id: str) -> Draft202012Validator:
    """Build an offline validator whose transitive references use the bundle."""
    return Draft202012Validator(
        schema_for(schema_id),
        registry=CONTRACT_REGISTRY,
        format_checker=_FORMAT_CHECKER,
    )
