from __future__ import annotations

from server.chart_serializer import render_chart


def _obs(resource_uuid="obs-1", **overrides):
    record = {
        "resourceType": "Observation",
        "resourceUuid": resource_uuid,
        "date": "2026-01-15",
        "text": "Weight: 58 kg",
    }
    record.update(overrides)
    return record


def test_the_grounding_source_text_excludes_the_date_kind_marker():
    # The [dateKind] marker is a temporal-safety hint the answer model reads on the chart
    # line. It must NOT leak into mappings[k].text, which is the source fed to the entailment
    # grounding layer: a marker in the source makes grounding reject otherwise-supported
    # claims (regression: everything failed review -> in-depth withheld).
    text, mappings = render_chart([_obs(dateKind="administrative", text="Drug order: Didanosine")])
    assert text == "[1] (2026-01-15) Drug order: Didanosine [administrative]\n"
    assert mappings[0]["text"] == "(2026-01-15) Drug order: Didanosine"
    assert "[administrative]" not in mappings[0]["text"]


def test_a_clinical_event_record_renders_its_clinical_date_with_no_marker():
    text, mappings = render_chart(
        [_obs(clinicalDate="2026-01-15", dateKind="clinical_event")]
    )

    assert text == "[1] (2026-01-15) Weight: 58 kg\n"
    assert mappings[0]["date"] == "2026-01-15"


def test_a_clinical_event_record_prefers_clinical_date_over_the_sort_date():
    # Condition's record_date is dateCreated (audit); its onset is the real clinical event.
    text, _ = render_chart(
        [
            _obs(
                date="2026-01-20",
                clinicalDate="2026-01-15",
                dateKind="clinical_event",
                text="Condition: Hypertension",
            )
        ]
    )

    assert text == "[1] (2026-01-15) Condition: Hypertension\n"


def test_a_clinical_date_is_shown_unmarked_even_when_date_kind_is_administrative():
    # This is querystore's actual Condition shape: record_date is dateCreated (administrative,
    # audit-only), but clinicalDate carries the real onset — a genuine clinical fact that must not
    # be hidden behind an administrative marker just because dateKind describes record_date, not
    # clinicalDate.
    text, mappings = render_chart(
        [
            _obs(
                date="2026-01-20",
                clinicalDate="2020-03-15",
                dateKind="administrative",
                text="Condition: Hypertension. Status: ACTIVE",
            )
        ]
    )

    assert text == "[1] (2020-03-15) Condition: Hypertension. Status: ACTIVE\n"
    assert mappings[0]["date"] == "2020-03-15"


def test_an_administrative_record_still_shows_a_date_but_is_marked_non_clinical():
    # Never omit the date outright: chart_serializer must not create a dateless line, which
    # would let temporal.py's run-length compression silently attribute a neighboring record's
    # date to this one.
    text, mappings = render_chart(
        [_obs(dateKind="administrative", text="Patient: Jane Doe")]
    )

    assert text == "[1] (2026-01-15) Patient: Jane Doe [administrative]\n"
    assert mappings[0]["date"] == "2026-01-15"


def test_an_unknown_date_kind_record_is_marked_unknown():
    text, _ = render_chart([_obs(dateKind="unknown", text="Legacy record")])

    assert text == "[1] (2026-01-15) Legacy record [unknown]\n"


def test_a_record_without_date_kind_defaults_to_clinical_for_backward_compatibility():
    # Inline/static-knowledge sources never populate dateKind; preserve today's behavior for them.
    text, _ = render_chart([_obs()])

    assert text == "[1] (2026-01-15) Weight: 58 kg\n"


def test_the_marker_is_appended_after_the_obs_group_label():
    text, _ = render_chart(
        [
            _obs(
                dateKind="administrative",
                text="Weight: 58 kg",
                metadata={"obs_group_uuid": "grp-1", "obs_group_concept_name": "Vitals"},
            )
        ]
    )

    assert text == "[1] (2026-01-15) Weight: 58 kg (part of: Vitals) [administrative]\n"


def test_a_record_with_no_date_at_all_gets_no_marker_since_nothing_needs_qualifying():
    text, _ = render_chart(
        [_obs(date=None, dateKind="unknown", text="Legacy record with no date")]
    )

    assert text == "[1] Legacy record with no date\n"


def test_multiple_records_mix_clinical_and_administrative_markers():
    text, _ = render_chart(
        [
            _obs(resource_uuid="enc-1", dateKind="clinical_event", clinicalDate="2026-01-15", text="Visit"),
            _obs(
                resource_uuid="patient-1",
                date="2018-04-22",
                dateKind="administrative",
                text="Patient: Jane Doe",
            ),
        ]
    )

    assert text == (
        "[1] (2026-01-15) Visit\n"
        "[2] (2018-04-22) Patient: Jane Doe [administrative]\n"
    )
