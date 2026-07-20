"""Deterministic, actionable lint feedback for Catalyst SQL candidates."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import re
from typing import Any, Mapping

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError
from sqlglot.optimizer.scope import Scope, traverse_scope


@dataclass(frozen=True)
class LintFinding:
    code: str
    stage: str
    severity: str
    path: str
    message: str
    evidence: str
    suggestedAction: str
    line: int | None = None
    column: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if value is not None}


def _parse_finding(sql: str, error: ParseError) -> LintFinding:
    detail = error.errors[0] if error.errors else {}
    line = detail.get("line") if isinstance(detail.get("line"), int) else None
    column = detail.get("col") if isinstance(detail.get("col"), int) else None
    evidence = "".join(
        str(detail.get(key, ""))
        for key in ("start_context", "highlight", "end_context")
    ).strip()
    typed_parameter = re.search(
        r"\bDATE\s+(:[A-Za-z_][A-Za-z0-9_]*)\b", sql, flags=re.IGNORECASE
    )
    if typed_parameter:
        placeholder = typed_parameter.group(1)
        return LintFinding(
            code="sql.invalid_typed_parameter",
            stage="sql_parse",
            severity="error",
            path="sql",
            line=line,
            column=column,
            message="DATE cannot prefix a named bind parameter in PostgreSQL.",
            evidence=typed_parameter.group(0),
            suggestedAction=(
                f"Use {placeholder} directly and keep its declared parameter type date."
            ),
        )
    return LintFinding(
        code="sql.parse_error",
        stage="sql_parse",
        severity="error",
        path="sql",
        line=line,
        column=column,
        message=f"SQL could not be parsed as PostgreSQL: {error}",
        evidence=evidence or sql[:240],
        suggestedAction="Return one syntactically valid PostgreSQL SELECT statement.",
    )


def _table_name(table: exp.Table) -> str:
    parts = [table.catalog, table.db, table.name]
    return ".".join(part for part in parts if part)


def _catalog_relations(extension: Mapping[str, Any]) -> dict[str, set[str]]:
    return {
        str(view["name"]).casefold(): {
            str(field["name"]).casefold()
            for field in view.get("fields", [])
            if field.get("name")
        }
        for view in extension["catalog"]["views"]
    }


def _literal_limit(statement: exp.Expression) -> int | None:
    limit = statement.args.get("limit")
    if limit is None:
        return None
    expression = limit.args.get("expression")
    if not isinstance(expression, exp.Literal) or not expression.is_int:
        return -1
    return int(expression.this)


def _scope_source_fields(
    source: exp.Expression | Scope,
    catalog_relations: Mapping[str, set[str]],
) -> set[str]:
    if isinstance(source, exp.Table):
        return set(catalog_relations.get(_table_name(source).casefold(), set()))
    if isinstance(source, Scope):
        projected = source.outer_columns or source.expression.named_selects
        fields = {str(name).casefold() for name in projected if name}
        if "*" in fields:
            fields.remove("*")
            for nested_source in source.sources.values():
                fields.update(_scope_source_fields(nested_source, catalog_relations))
        return fields
    return set()


def _unknown_columns(
    statement: exp.Expression,
    catalog_relations: Mapping[str, set[str]],
) -> set[str]:
    """Resolve each column against the relation or derived source in its SQL scope."""

    def fields_for(scope: Scope) -> dict[str, set[str]]:
        return {
            str(alias).casefold(): _scope_source_fields(source, catalog_relations)
            for alias, source in scope.sources.items()
        }

    def resolves_in_parent(scope: Scope, column: exp.Column) -> bool:
        parent = scope.parent
        while parent is not None:
            parent_fields = fields_for(parent)
            if column.table:
                allowed = parent_fields.get(column.table.casefold())
                if allowed is not None:
                    return column.name.casefold() in allowed
            elif column.name.casefold() in set().union(
                *parent_fields.values(), set()
            ):
                return True
            parent = parent.parent
        return False

    invalid: set[str] = set()
    for scope in traverse_scope(statement):
        source_fields = fields_for(scope)
        all_source_fields = set().union(*source_fields.values(), set())
        local_projection_names = {
            projection.alias.casefold()
            for projection in scope.expression.expressions
            if isinstance(projection, exp.Alias) and projection.alias
        }
        external_ids = {id(column) for column in scope.external_columns}
        for column in scope.columns:
            if not column.name:
                continue
            if id(column) in external_ids and resolves_in_parent(scope, column):
                continue
            name = column.name.casefold()
            if column.table:
                allowed = source_fields.get(column.table.casefold())
                if allowed is None or name not in allowed:
                    invalid.add(column.sql())
            elif name not in all_source_fields and name not in local_projection_names:
                invalid.add(column.sql())
    return invalid


def turnaround_threshold(question: str) -> tuple[str, float] | None:
    if not re.search(r"\b(?:turnaround|receipt[- ]to[- ]release)\b", question, re.I):
        return None
    match = re.search(
        r"\b(over|greater\s+than|more\s+than|at\s+least|no\s+less\s+than)\s+"
        r"(\d+(?:\.\d+)?)\s*(minutes?|hours?|days?)\b",
        question,
        re.I,
    )
    if not match:
        return None
    operator = (
        "gte" if match.group(1).casefold() in {"at least", "no less than"} else "gt"
    )
    value = float(match.group(2))
    unit = match.group(3).casefold()
    if unit.startswith("hour"):
        value *= 60
    elif unit.startswith("day"):
        value *= 1440
    return operator, value


def _turnaround_threshold_satisfied(
    statement: exp.Expression,
    parameters: list[Mapping[str, Any]],
    requirement: tuple[str, float],
) -> bool:
    operator, expected_minutes = requirement
    parameter_values = {
        str(parameter.get("name")): parameter.get("value") for parameter in parameters
    }

    def is_turnaround(node: exp.Expression | None) -> bool:
        return (
            isinstance(node, exp.Column)
            and node.name.casefold() == "receipt_to_release_minutes"
        )

    def is_expected(node: exp.Expression | None) -> bool:
        if not isinstance(node, exp.Placeholder) or not node.name:
            return False
        try:
            return float(parameter_values.get(node.name)) == expected_minutes
        except (TypeError, ValueError):
            return False

    forward_types = (exp.GTE,) if operator == "gte" else (exp.GT,)
    reverse_types = (exp.LTE,) if operator == "gte" else (exp.LT,)
    for predicate_type in forward_types:
        for predicate in statement.find_all(predicate_type):
            if is_turnaround(predicate.this) and is_expected(predicate.expression):
                return True
    for predicate_type in reverse_types:
        for predicate in statement.find_all(predicate_type):
            if is_expected(predicate.this) and is_turnaround(predicate.expression):
                return True
    return False


def _requires_latest_per_patient(question: str) -> bool:
    return bool(
        re.search(r"\blatest\b", question, re.I)
        and re.search(r"\b(?:for\s+each|per)\s+patient\b", question, re.I)
    )


def _has_latest_per_patient_grain(sql: str) -> bool:
    qualified_patient = r'(?:"?[A-Za-z_][A-Za-z0-9_]*"?\.)?"?patient_id"?'
    window = re.search(
        rf"\bROW_NUMBER\s*\(\s*\)\s*OVER\s*\([^)]*"
        rf"\bPARTITION\s+BY\s+{qualified_patient}",
        sql,
        re.I | re.S,
    )
    distinct_on = re.search(
        rf"\bDISTINCT\s+ON\s*\(\s*{qualified_patient}\s*\)",
        sql,
        re.I,
    )
    return bool(window or distinct_on)


def lint_candidate(
    candidate: Mapping[str, Any],
    extension: Mapping[str, Any],
    *,
    instruction: str = "",
) -> list[dict[str, Any]]:
    """Return stable findings using the explicit request instruction for intent."""
    if candidate.get("status") != "ready":
        return []

    sql = str(candidate.get("sql", ""))
    try:
        statements = sqlglot.parse(sql, read="postgres")
    except ParseError as error:
        return [_parse_finding(sql, error).as_dict()]

    if len(statements) != 1 or statements[0] is None:
        return [
            LintFinding(
                code="sql.statement_count",
                stage="sql_parse",
                severity="error",
                path="sql",
                message="Exactly one PostgreSQL statement is required.",
                evidence=sql[:240],
                suggestedAction="Return one read-only SELECT statement only.",
            ).as_dict()
        ]

    statement = statements[0]
    findings: list[LintFinding] = []
    if not isinstance(statement, exp.Select) or any(
        statement.find(node_type) is not None
        for node_type in (
            exp.Into,
            exp.Insert,
            exp.Update,
            exp.Delete,
            exp.Create,
            exp.Drop,
            exp.Alter,
            exp.Command,
            exp.Lock,
            exp.Merge,
        )
    ):
        findings.append(
            LintFinding(
                code="policy.operation_not_allowed",
                stage="operation_policy",
                severity="error",
                path="sql",
                message="Only one read-only SELECT statement is allowed.",
                evidence=statement.key,
                suggestedAction="Replace the statement with a read-only SELECT.",
            )
        )

    scopes = list(traverse_scope(statement))
    referenced_views = {
        _table_name(source)
        for scope in scopes
        for source in scope.sources.values()
        if isinstance(source, exp.Table)
    }
    catalog_relations = _catalog_relations(extension)
    approved_views = set(catalog_relations)
    invalid_views = sorted(
        view for view in referenced_views if view.casefold() not in approved_views
    )
    if invalid_views or not referenced_views:
        findings.append(
            LintFinding(
                code="catalog.unapproved_view",
                stage="catalog_identifiers",
                severity="error",
                path="sql",
                message="Every relation must be an approved analytics view.",
                evidence=", ".join(invalid_views) if invalid_views else "none",
                suggestedAction="Use only the exact fully qualified views in the catalog.",
            )
        )

    invalid_columns = sorted(_unknown_columns(statement, catalog_relations))
    if invalid_columns:
        findings.append(
            LintFinding(
                code="catalog.unknown_column",
                stage="catalog_identifiers",
                severity="error",
                path="sql",
                message="SQL references fields absent from the approved catalog.",
                evidence=", ".join(invalid_columns),
                suggestedAction="Replace or remove every field not present in the catalog.",
            )
        )

    parameters = list(candidate.get("parameters") or [])
    parameter_names = [str(item.get("name", "")) for item in parameters]
    if len(parameter_names) != len(set(parameter_names)):
        findings.append(
            LintFinding(
                code="binding.duplicate_parameter",
                stage="parameter_binding",
                severity="error",
                path="parameters",
                message="Parameter names must be unique.",
                evidence=", ".join(parameter_names),
                suggestedAction="Declare each SQL placeholder exactly once.",
            )
        )
    placeholders = {
        node.name for node in statement.find_all(exp.Placeholder) if node.name
    }
    if placeholders != set(parameter_names):
        findings.append(
            LintFinding(
                code="binding.placeholder_mismatch",
                stage="parameter_binding",
                severity="error",
                path="parameters",
                message="SQL placeholders and declared parameters must match exactly.",
                evidence=(
                    f"placeholders={sorted(placeholders)}; "
                    f"parameters={sorted(parameter_names)}"
                ),
                suggestedAction="Add, rename, or remove bindings until the sets match.",
            )
        )

    turnaround_requirement = turnaround_threshold(instruction)
    if turnaround_requirement and not _turnaround_threshold_satisfied(
        statement, parameters, turnaround_requirement
    ):
        operator, minutes = turnaround_requirement
        comparison = ">=" if operator == "gte" else ">"
        findings.append(
            LintFinding(
                code="semantic.turnaround_threshold",
                stage="semantic_grounding",
                severity="error",
                path="sql",
                message=(
                    "The requested turnaround threshold is not enforced by "
                    "receipt_to_release_minutes."
                ),
                evidence=f"required receipt_to_release_minutes {comparison} {minutes:g}",
                suggestedAction=(
                    "Add the required comparison using receipt_to_release_minutes "
                    "and a named numeric parameter whose value is the converted "
                    f"threshold ({minutes:g} minutes)."
                ),
            )
        )

    if _requires_latest_per_patient(instruction) and not _has_latest_per_patient_grain(
        sql
    ):
        findings.append(
            LintFinding(
                code="semantic.latest_per_patient_grain",
                stage="semantic_grounding",
                severity="error",
                path="sql",
                message=(
                    "The question requires one latest result per patient, but the "
                    "SQL does not preserve patient-level grain."
                ),
                evidence="required partition or DISTINCT ON by patient_id",
                suggestedAction=(
                    "Use ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY "
                    "observed_at DESC) and filter to the first rank with a named "
                    "parameter, or use PostgreSQL DISTINCT ON (patient_id) with "
                    "matching ordering."
                ),
            )
        )

    projected = [item.alias_or_name for item in statement.expressions]
    expected = [str(item.get("name", "")) for item in candidate["expectedColumns"]]
    if projected != expected:
        findings.append(
            LintFinding(
                code="output.projection_mismatch",
                stage="output_agreement",
                severity="error",
                path="expectedColumns",
                message="Projected SQL columns and expectedColumns must agree in order.",
                evidence=f"projected={projected}; expected={expected}",
                suggestedAction="Alias projections or update expectedColumns to match exactly.",
            )
        )

    limit = _literal_limit(statement)
    max_rows = int(extension["policy"]["maxRows"])
    if limit == -1 or (limit is not None and limit > max_rows):
        findings.append(
            LintFinding(
                code="policy.row_limit_exceeded",
                stage="resource_policy",
                severity="error",
                path="sql",
                message=f"LIMIT must be a literal integer no greater than {max_rows}.",
                evidence=str(limit),
                suggestedAction=f"Use a literal LIMIT between 1 and {max_rows}.",
            )
        )

    return [finding.as_dict() for finding in findings]
