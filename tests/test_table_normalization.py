"""Tests for sparse/imperfect clinical table block normalization."""

from server import team


def test_normalize_backfills_optional_missing_cell_keys():
    blocks = [
        {
            "kind": "table",
            "title": "Active medications",
            "columns": [
                {"key": "medication", "label": "Medication"},
                {"key": "dose", "label": "Dose"},
                {"key": "route", "label": "Route"},
                {"key": "notes", "label": "Notes"},
            ],
            "rows": [
                {
                    "cells": {
                        "medication": {"text": "Stavudine", "refs": [12]},
                        "dose": {"text": "30 mg", "refs": [12]},
                    }
                },
                {
                    "cells": {
                        "medication": {"text": "Co-trimoxazole", "refs": [13]},
                        "dose": {"text": "960 mg", "refs": [13]},
                        "route": {"text": "Oral", "refs": [13]},
                    }
                },
            ],
        }
    ]

    normalized, issues = team._normalize_product_blocks(blocks)

    assert issues == []
    assert len(normalized) == 1
    table = normalized[0]
    # "notes" column is empty across all rows and gets pruned; "route" stays because row 2 has "Oral".
    assert [c["key"] for c in table["columns"]] == ["medication", "dose", "route"]
    assert table["rows"][0]["cells"]["route"] == {"text": "", "refs": []}
    assert table["rows"][1]["cells"]["route"] == {"text": "Oral", "refs": [13]}


def test_normalize_remaps_column_labels_and_allows_empty_placeholders():
    blocks = [
        {
            "kind": "table",
            "title": "HIV visits",
            "columns": [
                {"key": "date", "label": "Date"},
                {"key": "condition", "label": "Condition"},
            ],
            "rows": [
                {
                    "cells": {
                        "Date": {"text": "—", "refs": [1]},
                        "Condition": {"text": "Oral candidiasis", "refs": [1]},
                    }
                }
            ],
        }
    ]

    normalized, issues = team._normalize_product_blocks(blocks)

    assert issues == []
    assert len(normalized) == 1
    assert normalized[0]["rows"][0]["cells"]["date"] == {"text": "—", "refs": [1]}
    assert normalized[0]["rows"][0]["cells"]["condition"] == {
        "text": "Oral candidiasis",
        "refs": [1],
    }


def test_normalize_still_rejects_unknown_keys_and_non_object_cells():
    unknown_key_block = [
        {
            "kind": "table",
            "title": "Medications",
            "columns": [
                {"key": "med", "label": "Medication"},
                {"key": "dose", "label": "Dose"},
            ],
            "rows": [
                {
                    "cells": {
                        "med": {"text": "Aspirin", "refs": [1]},
                        "unexpected_column": {"text": "81 mg", "refs": [1]},
                    }
                }
            ],
        }
    ]
    normalized, issues = team._normalize_product_blocks(unknown_key_block)
    assert normalized == []
    assert issues and issues[0]["id"] == "table_contract"

    flat_cell_block = [
        {
            "kind": "table",
            "title": "Medications",
            "columns": [{"key": "med", "label": "Medication"}],
            "rows": [{"cells": {"med": "Aspirin"}}],
        }
    ]
    normalized2, issues2 = team._normalize_product_blocks(flat_cell_block)
    assert normalized2 == []
    assert issues2 and issues2[0]["id"] == "table_contract"
