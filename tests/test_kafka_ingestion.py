from __future__ import annotations

import json
from pathlib import Path

import pytest

from manufacturing_data_platform.kafka_ingestion.contracts import (
    EventContractError,
    sample_machine_event,
    serialize_machine_event,
    validate_machine_event,
)
from manufacturing_data_platform.kafka_ingestion.landing import (
    KafkaRecord,
    LandingConsistencyError,
    SimulatedCrashAfterLanding,
    land_records,
    load_landing_index,
)


TOPIC = "manufacturing.machine-events.v1"


def _record(offset: int, event_index: int | None = None) -> KafkaRecord:
    index = event_index if event_index is not None else offset + 1
    event = sample_machine_event(index)
    return KafkaRecord(
        topic=TOPIC,
        partition=0,
        offset=offset,
        key=event["machine_id"],
        value=serialize_machine_event(event),
        timestamp_ms=1_783_000_000_000 + offset,
    )


def _jsonl(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def test_machine_event_contract_normalizes_and_rejects_unknown_fields():
    event = sample_machine_event(1)
    event["machine_id"] = " MC-101 "
    normalized = validate_machine_event(event)

    assert normalized["machine_id"] == "mc-101"
    assert normalized["schema_version"] == 1

    with pytest.raises(EventContractError, match="unknown fields"):
        validate_machine_event({**event, "unexpected": "value"})


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("schema_version", 2, "unsupported schema_version"),
        ("event_time", "2026-06-29T08:00:00", "include a timezone"),
        ("defect_count", 11, "must be <= units_produced"),
        ("operation", "unknown", "operation must be one of"),
    ],
)
def test_machine_event_contract_rejects_invalid_values(field, value, message):
    event = sample_machine_event(1)
    event[field] = value
    with pytest.raises(EventContractError, match=message):
        validate_machine_event(event)


def test_landing_writes_accepted_jsonl_and_manifest_atomically(tmp_path):
    result = land_records([_record(0), _record(1)], tmp_path / "raw")

    assert result.status == "landed"
    assert result.accepted_count == 2
    assert result.accepted_total == 2
    assert result.committable_offsets == (
        {"topic": TOPIC, "partition": 0, "next_offset": 2},
    )

    batch_path = Path(result.batch_path)
    manifest = json.loads((batch_path / "manifest.json").read_text())
    accepted = _jsonl(batch_path / "accepted.jsonl")
    assert manifest["accepted_count"] == 2
    assert [row["event"]["event_id"] for row in accepted] == [
        "evt-20260629-000001",
        "evt-20260629-000002",
    ]
    assert [row["kafka"]["offset"] for row in accepted] == [0, 1]


def test_redelivered_coordinates_are_reused_without_new_batch(tmp_path):
    output = tmp_path / "raw"
    first = land_records([_record(0), _record(1)], output)
    second = land_records([_record(0), _record(1)], output)

    assert first.status == "landed"
    assert second.status == "reused"
    assert second.reused_coordinate_count == 2
    assert second.accepted_count == 0
    assert second.accepted_total == 2
    assert second.batch_path is None
    assert len(list(output.glob("topic=*/partition=*/batch=*/manifest.json"))) == 1


def test_same_event_id_at_new_offset_is_preserved_as_duplicate_evidence(tmp_path):
    output = tmp_path / "raw"
    first = land_records([_record(0, event_index=1)], output)
    duplicate = land_records([_record(1, event_index=1)], output)

    assert first.accepted_total == 1
    assert duplicate.accepted_count == 0
    assert duplicate.duplicate_event_count == 1
    assert duplicate.accepted_total == 1

    duplicate_path = Path(duplicate.batch_path)
    rows = _jsonl(duplicate_path / "duplicates.jsonl")
    assert rows[0]["event"]["event_id"] == "evt-20260629-000001"
    assert rows[0]["duplicate_of"]["offset"] == 0


def test_invalid_event_is_quarantined_and_offset_remains_committable(tmp_path):
    invalid = KafkaRecord(
        topic=TOPIC,
        partition=0,
        offset=0,
        key="mc-101",
        value=b'{"event_id":"broken"}',
    )
    result = land_records([invalid], tmp_path / "raw")

    assert result.accepted_count == 0
    assert result.quarantine_count == 1
    assert result.accepted_total == 0
    assert result.committable_offsets[0]["next_offset"] == 1


def test_crash_after_atomic_rename_is_recovered_by_coordinate_reuse(tmp_path):
    output = tmp_path / "raw"
    records = [_record(0), _record(1)]

    with pytest.raises(SimulatedCrashAfterLanding):
        land_records(records, output, simulate_crash_after_rename=True)

    index_after_crash = load_landing_index(output)
    assert len(index_after_crash["coordinates"]) == 2
    assert len(index_after_crash["accepted_events"]) == 2

    retry = land_records(records, output)
    assert retry.status == "reused"
    assert retry.reused_coordinate_count == 2
    assert retry.committable_offsets[0]["next_offset"] == 2


def test_same_coordinate_with_changed_payload_is_rejected(tmp_path):
    output = tmp_path / "raw"
    land_records([_record(0, event_index=1)], output)

    with pytest.raises(LandingConsistencyError, match="changed payload or key"):
        land_records([_record(0, event_index=2)], output)
