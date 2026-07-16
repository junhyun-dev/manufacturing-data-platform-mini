from __future__ import annotations

import json
from pathlib import Path

import pytest

from manufacturing_data_platform.kafka_ingestion.batch_adapter import (
    CANONICAL_COLUMNS,
    AdapterConsistencyError,
    LandingIntegrityError,
    NoEligibleEventsError,
    adapt_landing_to_batch,
    canonical_csv_bytes,
    discover_accepted_events,
    run_bridge,
    select_for_business_date,
)
from manufacturing_data_platform.kafka_ingestion.contracts import (
    sample_machine_event,
    serialize_machine_event,
)
from manufacturing_data_platform.kafka_ingestion.landing import (
    KafkaRecord,
    land_records,
)
from manufacturing_data_platform.pipeline.lakehouse import DATASET_ID, state_dir


TOPIC = "manufacturing.machine-events.v1"
BUSINESS_DATE = "2026-06-29"
OTHER_DATE = "2026-06-30"


# --------------------------------------------------------------------------- #
# Helpers: build real K1 landings with the real landing writer
# --------------------------------------------------------------------------- #
def _event(index: int, business_date: str | None = None) -> dict:
    event = sample_machine_event(index)
    if business_date is not None:
        event["business_date"] = business_date
    return event


def _record(offset: int, event: dict) -> KafkaRecord:
    return KafkaRecord(
        topic=TOPIC,
        partition=0,
        offset=offset,
        key=event["machine_id"],
        value=serialize_machine_event(event),
        timestamp_ms=1_783_000_000_000 + offset,
    )


def _land(landing_dir: Path, pairs: list[tuple[int, dict]]) -> None:
    land_records([_record(offset, event) for offset, event in pairs], landing_dir)


def _three_event_landing(landing_dir: Path) -> Path:
    _land(landing_dir, [(0, _event(1)), (1, _event(2)), (2, _event(3))])
    return landing_dir


def _state_path(lakehouse_dir: Path, business_date: str = BUSINESS_DATE) -> Path:
    return state_dir(lakehouse_dir, DATASET_ID) / f"business_date={business_date}.json"


def _accepted_paths(landing_dir: Path) -> list[Path]:
    return sorted(landing_dir.glob("topic=*/partition=*/batch=*/accepted.jsonl"))


def _rewrite_accepted(path: Path, rows: list[dict]) -> None:
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=True, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _bridge(landing_dir: Path, tmp_path: Path, business_date: str = BUSINESS_DATE) -> dict:
    return run_bridge(
        landing_dir=landing_dir,
        business_date=business_date,
        adapter_output_dir=tmp_path / "adapter",
        lakehouse_output_dir=tmp_path / "lakehouse",
        evidence_file=tmp_path / "bridge_evidence.json",
    )


# --------------------------------------------------------------------------- #
# 1. Golden path
# --------------------------------------------------------------------------- #
def test_golden_path_produces_deterministic_csv_and_quality_passed_gold(tmp_path):
    landing = _three_event_landing(tmp_path / "raw")

    evidence = _bridge(landing, tmp_path)

    adapter = evidence["adapter"]
    assert adapter["status"] == "created"
    assert adapter["selected_event_count"] == 3
    assert adapter["selected_event_ids"] == [
        "evt-20260629-000001",
        "evt-20260629-000002",
        "evt-20260629-000003",
    ]
    assert [c["offset"] for c in adapter["selected_coordinates"]] == [0, 1, 2]
    assert all(c["topic"] == TOPIC and c["partition"] == 0 for c in adapter["selected_coordinates"])

    # Canonical CSV: fixed header, ordered by Kafka coordinate, provenance carried.
    csv_lines = Path(adapter["csv_path"]).read_text(encoding="utf-8").splitlines()
    assert csv_lines[0] == ",".join(CANONICAL_COLUMNS)
    assert len(csv_lines) == 4
    assert [line.split(",")[CANONICAL_COLUMNS.index("kafka_offset")] for line in csv_lines[1:]] == [
        "0",
        "1",
        "2",
    ]

    # provenance.json agrees with the selected set and uses landing-relative paths.
    provenance = json.loads(Path(adapter["provenance_path"]).read_text(encoding="utf-8"))
    assert provenance["source_hash"] == adapter["source_hash"]
    assert provenance["selected_event_count"] == 3
    assert provenance["selected_coordinates"] == adapter["selected_coordinates"]
    assert all(not p.startswith("/") for p in provenance["landing_manifest_paths"])
    assert len(provenance["source_record_fingerprints"]) == 3

    lakehouse = evidence["lakehouse"]
    assert lakehouse["status"] == "processed"
    assert lakehouse["quality_passed"] is True
    assert lakehouse["source_hash"] == adapter["source_hash"]
    assert lakehouse["row_counts"]["silver"] == 3

    # Gold conserves silver totals: units 10+20+30, defects 0+1+2, cycle avg 2460/3.
    gold = lakehouse["gold_rows"]
    assert len(gold) == 1
    assert int(gold[0]["units_produced"]) == 60
    assert int(gold[0]["defect_count"]) == 3
    assert float(gold[0]["defect_rate"]) == 0.05
    assert float(gold[0]["avg_cycle_time_ms"]) == 820.0
    assert gold[0]["business_date"] == BUSINESS_DATE


# --------------------------------------------------------------------------- #
# 2. Idempotent rerun
# --------------------------------------------------------------------------- #
def test_same_landing_rerun_reuses_adapter_version_and_skips_pipeline(tmp_path):
    landing = _three_event_landing(tmp_path / "raw")

    first = _bridge(landing, tmp_path)
    second = _bridge(landing, tmp_path)

    assert first["adapter"]["status"] == "created"
    assert second["adapter"]["status"] == "reused"
    assert second["adapter"]["source_hash"] == first["adapter"]["source_hash"]
    assert second["adapter"]["version_dir"] == first["adapter"]["version_dir"]

    assert first["lakehouse"]["status"] == "processed"
    assert second["lakehouse"]["status"] == "skipped"
    assert second["lakehouse"]["run_id"] == first["lakehouse"]["run_id"]

    # The trusted result is reused, not doubled.
    run_dirs = list((tmp_path / "lakehouse" / f"business_date={BUSINESS_DATE}").glob("run_id=*"))
    assert len(run_dirs) == 1
    assert second["lakehouse"]["gold_rows"] == first["lakehouse"]["gold_rows"]
    assert int(second["lakehouse"]["gold_rows"][0]["units_produced"]) == 60

    version_dirs = list((tmp_path / "adapter" / f"business_date={BUSINESS_DATE}").glob("source_hash=*"))
    assert len(version_dirs) == 1


# --------------------------------------------------------------------------- #
# 3. Explicit date boundary
# --------------------------------------------------------------------------- #
def test_mixed_date_landing_selects_only_the_requested_date(tmp_path):
    landing = tmp_path / "raw"
    _land(
        landing,
        [
            (0, _event(1)),
            (1, _event(2, business_date=OTHER_DATE)),
            (2, _event(3)),
        ],
    )

    evidence = _bridge(landing, tmp_path)

    assert evidence["adapter"]["selected_event_count"] == 2
    assert evidence["adapter"]["selected_event_ids"] == [
        "evt-20260629-000001",
        "evt-20260629-000003",
    ]
    assert [c["offset"] for c in evidence["adapter"]["selected_coordinates"]] == [0, 2]
    assert evidence["lakehouse"]["row_counts"]["silver"] == 2
    assert evidence["lakehouse"]["quality_passed"] is True
    # units 10+30, defects 0+2
    assert int(evidence["lakehouse"]["gold_rows"][0]["units_produced"]) == 40
    assert int(evidence["lakehouse"]["gold_rows"][0]["defect_count"]) == 2


def test_missing_date_fails_without_synthetic_sample_or_zero_row_gold(tmp_path):
    landing = _three_event_landing(tmp_path / "raw")

    with pytest.raises(NoEligibleEventsError, match=OTHER_DATE):
        _bridge(landing, tmp_path, business_date=OTHER_DATE)

    # The lakehouse pipeline was never invoked, so it could not generate its
    # synthetic sample CSV nor a zero-row gold placeholder.
    assert not (tmp_path / "lakehouse").exists()
    assert not (tmp_path / "adapter" / f"business_date={OTHER_DATE}").exists()
    assert list(tmp_path.glob("**/gold/*.csv")) == []


def test_multiple_partitions_are_rejected_until_that_scope_is_verified(tmp_path):
    landing = tmp_path / "raw"
    event_1 = _event(1)
    event_2 = _event(2)
    land_records([_record(0, event_1)], landing)
    partition_1 = KafkaRecord(
        topic=TOPIC,
        partition=1,
        offset=0,
        key=event_2["machine_id"],
        value=serialize_machine_event(event_2),
        timestamp_ms=1_783_000_000_002,
    )
    land_records([partition_1], landing)

    with pytest.raises(LandingIntegrityError, match="one topic/partition"):
        _bridge(landing, tmp_path)

    assert not (tmp_path / "lakehouse").exists()


# --------------------------------------------------------------------------- #
# 4. Tamper / failure boundary
# --------------------------------------------------------------------------- #
def _tamper_event_id(rows: list[dict]) -> list[dict]:
    rows[0]["event"]["event_id"] = "evt-20260629-999999"
    return rows


def _tamper_kafka_key(rows: list[dict]) -> list[dict]:
    rows[0]["kafka"]["key"] = "mc-999"
    return rows


def _tamper_timestamp(rows: list[dict]) -> list[dict]:
    rows[0]["kafka"]["timestamp_ms"] = 1
    return rows


def _tamper_offset(rows: list[dict]) -> list[dict]:
    rows[0]["kafka"]["offset"] = 987
    return rows


def _drop_accepted_row(rows: list[dict]) -> list[dict]:
    return rows[1:]


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (_tamper_event_id, "event_id"),
        (_tamper_kafka_key, "key"),
        (_tamper_timestamp, "timestamp_ms"),
        (_tamper_offset, "absent from"),
        (_drop_accepted_row, "accepted_count"),
    ],
)
def test_envelope_disagreeing_with_manifest_fails_before_trusted_state(
    tmp_path, mutate, message
):
    landing = _three_event_landing(tmp_path / "raw")
    accepted_path = _accepted_paths(landing)[0]
    rows = [json.loads(line) for line in accepted_path.read_text().splitlines() if line.strip()]
    _rewrite_accepted(accepted_path, mutate(rows))

    with pytest.raises(LandingIntegrityError, match=message):
        _bridge(landing, tmp_path)

    # No lakehouse current-state pointer was created.
    assert not _state_path(tmp_path / "lakehouse").exists()
    assert not (tmp_path / "lakehouse").exists()


def test_tampered_rerun_does_not_advance_existing_trusted_state(tmp_path):
    landing = _three_event_landing(tmp_path / "raw")
    first = _bridge(landing, tmp_path)

    state_path = _state_path(tmp_path / "lakehouse")
    before = json.loads(state_path.read_text(encoding="utf-8"))
    assert before["run_id"] == first["lakehouse"]["run_id"]

    accepted_path = _accepted_paths(landing)[0]
    rows = [json.loads(line) for line in accepted_path.read_text().splitlines() if line.strip()]
    _rewrite_accepted(accepted_path, _tamper_event_id(rows))

    with pytest.raises(LandingIntegrityError):
        _bridge(landing, tmp_path)

    after = json.loads(state_path.read_text(encoding="utf-8"))
    assert after["run_id"] == before["run_id"]
    assert after["source_hash"] == before["source_hash"]


def test_manifest_accepted_count_must_match_entries_and_jsonl(tmp_path):
    landing = _three_event_landing(tmp_path / "raw")
    manifest_path = next(landing.glob("topic=*/partition=*/batch=*/manifest.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["accepted_count"] += 1
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    with pytest.raises(LandingIntegrityError, match="accepted_count"):
        _bridge(landing, tmp_path)

    assert not (tmp_path / "lakehouse").exists()


def test_business_metric_tamper_changes_identity_but_is_not_rejected(tmp_path):
    """Documents the honest boundary: K1.5 cross-checks only the fields visible in
    both files. A changed business metric is not detected as tampering, but it does
    produce a different canonical source identity rather than reusing the old one.
    """
    landing = _three_event_landing(tmp_path / "raw")
    clean = adapt_landing_to_batch(
        landing_dir=landing,
        business_date=BUSINESS_DATE,
        adapter_output_dir=tmp_path / "adapter_clean",
    )

    accepted_path = _accepted_paths(landing)[0]
    rows = [json.loads(line) for line in accepted_path.read_text().splitlines() if line.strip()]
    rows[0]["event"]["units_produced"] = 999
    _rewrite_accepted(accepted_path, rows)

    tampered = adapt_landing_to_batch(
        landing_dir=landing,
        business_date=BUSINESS_DATE,
        adapter_output_dir=tmp_path / "adapter_tampered",
    )
    assert tampered.source_hash != clean.source_hash


def test_conflicting_persisted_adapter_version_is_rejected(tmp_path):
    landing = _three_event_landing(tmp_path / "raw")
    result = adapt_landing_to_batch(
        landing_dir=landing,
        business_date=BUSINESS_DATE,
        adapter_output_dir=tmp_path / "adapter",
    )
    result.csv_path.write_text("corrupted\n", encoding="utf-8")

    with pytest.raises(AdapterConsistencyError, match="does not match"):
        adapt_landing_to_batch(
            landing_dir=landing,
            business_date=BUSINESS_DATE,
            adapter_output_dir=tmp_path / "adapter",
        )


@pytest.mark.parametrize("mode", ["missing", "corrupt", "mismatched"])
def test_missing_or_conflicting_persisted_provenance_is_rejected(tmp_path, mode):
    landing = _three_event_landing(tmp_path / "raw")
    result = adapt_landing_to_batch(
        landing_dir=landing,
        business_date=BUSINESS_DATE,
        adapter_output_dir=tmp_path / "adapter",
    )

    if mode == "missing":
        result.provenance_path.unlink()
    elif mode == "corrupt":
        result.provenance_path.write_text("not-json\n", encoding="utf-8")
    else:
        provenance = json.loads(result.provenance_path.read_text(encoding="utf-8"))
        provenance["selected_event_ids"] = ["wrong-event"]
        result.provenance_path.write_text(
            json.dumps(provenance, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

    with pytest.raises(AdapterConsistencyError, match="provenance"):
        adapt_landing_to_batch(
            landing_dir=landing,
            business_date=BUSINESS_DATE,
            adapter_output_dir=tmp_path / "adapter",
        )


# --------------------------------------------------------------------------- #
# 5. Provenance identity
# --------------------------------------------------------------------------- #
def test_changing_event_id_changes_source_identity_with_same_metrics(tmp_path):
    base = _event(1)
    renamed = dict(base)
    renamed["event_id"] = "evt-20260629-777777"

    landing_a = tmp_path / "a"
    landing_b = tmp_path / "b"
    _land(landing_a, [(0, base)])
    _land(landing_b, [(0, renamed)])

    hash_a = adapt_landing_to_batch(
        landing_dir=landing_a, business_date=BUSINESS_DATE, adapter_output_dir=tmp_path / "out_a"
    ).source_hash
    hash_b = adapt_landing_to_batch(
        landing_dir=landing_b, business_date=BUSINESS_DATE, adapter_output_dir=tmp_path / "out_b"
    ).source_hash

    assert base["units_produced"] == renamed["units_produced"]
    assert hash_a != hash_b


def test_changing_kafka_coordinate_changes_source_identity(tmp_path):
    event = _event(1)
    landing_a = tmp_path / "a"
    landing_b = tmp_path / "b"
    _land(landing_a, [(0, event)])
    _land(landing_b, [(7, event)])

    hash_a = adapt_landing_to_batch(
        landing_dir=landing_a, business_date=BUSINESS_DATE, adapter_output_dir=tmp_path / "out_a"
    ).source_hash
    hash_b = adapt_landing_to_batch(
        landing_dir=landing_b, business_date=BUSINESS_DATE, adapter_output_dir=tmp_path / "out_b"
    ).source_hash

    assert hash_a != hash_b


# --------------------------------------------------------------------------- #
# 6. Determinism
# --------------------------------------------------------------------------- #
def test_batch_grouping_and_creation_order_do_not_change_source_hash(tmp_path):
    events = [(0, _event(1)), (1, _event(2)), (2, _event(3))]

    landing_a = tmp_path / "a"
    _land(landing_a, events)

    # Same events, different batch grouping and creation order -> different batch
    # directory names and different manifest created_at values.
    landing_b = tmp_path / "b"
    _land(landing_b, [events[2]])
    _land(landing_b, [events[0], events[1]])

    result_a = adapt_landing_to_batch(
        landing_dir=landing_a, business_date=BUSINESS_DATE, adapter_output_dir=tmp_path / "out_a"
    )
    result_b = adapt_landing_to_batch(
        landing_dir=landing_b, business_date=BUSINESS_DATE, adapter_output_dir=tmp_path / "out_b"
    )

    assert len(list(landing_a.glob("topic=*/partition=*/batch=*"))) == 1
    assert len(list(landing_b.glob("topic=*/partition=*/batch=*"))) == 2
    assert result_a.source_hash == result_b.source_hash
    assert result_a.csv_path.read_bytes() == result_b.csv_path.read_bytes()


def test_same_identity_with_different_manifest_grouping_is_a_provenance_conflict(tmp_path):
    events = [(0, _event(1)), (1, _event(2)), (2, _event(3))]
    landing_a = tmp_path / "a"
    landing_b = tmp_path / "b"
    _land(landing_a, events)
    _land(landing_b, [events[2]])
    _land(landing_b, [events[0], events[1]])

    output = tmp_path / "adapter"
    first = adapt_landing_to_batch(
        landing_dir=landing_a,
        business_date=BUSINESS_DATE,
        adapter_output_dir=output,
    )
    separately_computed = adapt_landing_to_batch(
        landing_dir=landing_b,
        business_date=BUSINESS_DATE,
        adapter_output_dir=tmp_path / "adapter_b",
    )
    assert separately_computed.source_hash == first.source_hash

    # The CSV identity is grouping-independent, but the physical manifest paths
    # are not. Never return stale persisted provenance for a different grouping.
    with pytest.raises(AdapterConsistencyError, match="provenance"):
        adapt_landing_to_batch(
            landing_dir=landing_b,
            business_date=BUSINESS_DATE,
            adapter_output_dir=output,
        )


def test_canonical_csv_is_independent_of_input_discovery_order(tmp_path):
    landing = _three_event_landing(tmp_path / "raw")
    events = discover_accepted_events(landing)
    selected = select_for_business_date(events, BUSINESS_DATE)

    shuffled = [selected[2], selected[0], selected[1]]

    assert canonical_csv_bytes(shuffled) == canonical_csv_bytes(selected)
    assert canonical_csv_bytes(shuffled).endswith(b"\n")
    assert b"\r" not in canonical_csv_bytes(shuffled)
