"""Deterministic WAHS chart tool re-exports delegating to `wahs_core.wahs_tools`."""

from __future__ import annotations

from wahs_core.wahs_tools import (
    get_lab_history,
    read_clinical_note,
    retrieve_clinical_note_passages,
    search_chart_entries,
)

__all__ = [
    "get_lab_history",
    "read_clinical_note",
    "retrieve_clinical_note_passages",
    "search_chart_entries",
]
