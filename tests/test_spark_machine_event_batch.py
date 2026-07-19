from __future__ import annotations

import csv
import importlib.util
import json
import shutil
from pathlib import Path

import pytest

from manufacturing_data_platform.kafka_ingestion.batch_adapter import (
    CANONICAL_COLUMNS,
    adapt_landing_to_batch,
)
from manufacturing_data_platform.kafka_ingestion.contracts import (
    sample_machine_event,
    serialize_machine_event,
)
from manufacturing_data_platform.kafka_ingestion.landing import KafkaRecord, land_records
from manufacturing_data_platform.pipeline import spark_machine_event_batch as s7
from manufacturing_data_platform.pipeline.lakehouse import transform_gold, transform_silver


TOPIC = "manufacturing.machine-events.v1"
BUSINESS_DATE = "2026-06-29"
OTHER_DATE = "2026-06-30"


# --------------------------------------------------------------------------- #
# Shared builders
# --------------------------------------------------------------------------- #
def _event(index: int, *, business_date: str | None = None, **overrides) -> dict:
    event = sample_machine_event(index)
    if business_date is not None:
        event["business_date"] = business_date
    event.update(overrides)
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


def _land_and_adapt(tmp_path: Path, name: str, pairs, business_date: str = BUSINESS_DATE):
    landing = tmp_path / f"raw_{name}"
    land_records([_record(offset, event) for offset, event in pairs], landing)
    return adapt_landing_to_batch(
        landing_dir=landing,
        business_date=business_date,
        adapter_output_dir=tmp_path / f"adapter_{name}",
    )


def _write_direct_canonical_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(CANONICAL_COLUMNS), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _canonical_row(**over) -> dict:
    base = {
        "event_time": "2026-06-29T08:00:00Z",
        "plant_id": "plant-a",
        "line_id": "line-1",
        "work_order_id": "wo-1001",
        "machine_id": "mc-101",
        "product_code": "gearbox-a",
        "operation": "assembly",
        "units_produced": "10",
        "defect_count": "0",
        "cycle_time_ms": "810",
        "business_date": BUSINESS_DATE,
        "event_id": "evt-20260629-000001",
        "schema_version": "1",
        "kafka_topic": TOPIC,
        "kafka_partition": "0",
        "kafka_offset": "0",
        "kafka_key": "mc-101",
        "kafka_timestamp_ms": "1783000000000",
        "kafka_record_fingerprint": "deadbeef",
    }
    base.update({k: str(v) for k, v in over.items()})
    return base


# --------------------------------------------------------------------------- #
# Always-on pure tests (no Spark)
# --------------------------------------------------------------------------- #
def test_validate_business_date_rejects_non_iso():
    s7.validate_business_date("2026-06-29")
    with pytest.raises(ValueError, match="ISO date"):
        s7.validate_business_date("2026/06/29")
    with pytest.raises(ValueError):
        s7.validate_business_date("")


def test_assert_single_business_date_fails_on_other_date():
    rows = [_canonical_row(), _canonical_row(business_date=OTHER_DATE)]
    with pytest.raises(s7.BusinessDateMismatchError, match=OTHER_DATE):
        s7.assert_single_business_date(rows, BUSINESS_DATE)


def test_decide_publish_action_skip_same_source_write_otherwise():
    state = {"source_hash": "h1", "snapshot_id": 42}
    history = {42, 99}
    assert s7.decide_publish_action(state, "h1", history) == "skip"
    assert s7.decide_publish_action(state, "h2", history) == "write"
    assert s7.decide_publish_action(None, "h1", history) == "write"
    # A recorded attempt without a committed snapshot must still write.
    assert s7.decide_publish_action({"source_hash": "h1"}, "h1", history) == "write"
    # H2: same source but the recorded snapshot is gone from the table history -> write.
    assert s7.decide_publish_action(state, "h1", {99}) == "write"
    assert s7.decide_publish_action(state, "h1", set()) == "write"


def test_main_exits_nonzero_on_quality_failure(monkeypatch, tmp_path):
    # A quality-failed run must fail the orchestration task (M1), not exit 0.
    monkeypatch.setattr(
        s7, "run_bridge_spark_batch", lambda **kwargs: {"status": "quality_failed", "published": False}
    )
    argv = [
        "--landing-dir", "L", "--business-date", BUSINESS_DATE,
        "--adapter-output-dir", "A", "--warehouse", "W", "--output-dir", str(tmp_path),
    ]
    with pytest.raises(SystemExit) as excinfo:
        s7.main(argv)
    assert excinfo.value.code == 1

    # A published run returns normally (no raise).
    monkeypatch.setattr(
        s7, "run_bridge_spark_batch",
        lambda **kwargs: {"status": "published", "published": True, "physical_plan": {"exchange_observed": True}},
    )
    s7.main(argv)


def test_bridge_persists_adapter_identity(monkeypatch, tmp_path):
    # M2: adapter identity must be in the *persisted* evidence, not only the return value.
    from types import SimpleNamespace

    fake_adapter = SimpleNamespace(
        status="created", source_hash="hA", selected_event_count=2, csv_path=Path("/x.csv")
    )
    monkeypatch.setattr(s7, "adapt_landing_to_batch", lambda **kwargs: fake_adapter)

    def fake_run(*, csv_path, source_hash, business_date, warehouse_path,
                 evidence_output_dir, table_name=s7.TABLE_NAME, run_id=None, extra_evidence=None):
        evidence = {"status": "published", "source_hash": source_hash, **(extra_evidence or {})}
        s7._write_evidence(Path(evidence_output_dir), evidence)
        return evidence

    monkeypatch.setattr(s7, "run_spark_machine_event_batch", fake_run)

    ev_dir = tmp_path / "ev"
    evidence = s7.run_bridge_spark_batch(
        landing_dir="L", business_date=BUSINESS_DATE, adapter_output_dir="A",
        warehouse_path="W", evidence_output_dir=ev_dir,
    )
    persisted = json.loads((ev_dir / "spark_machine_event_batch.json").read_text())
    assert evidence["adapter"]["source_hash"] == "hA"
    assert persisted["adapter"]["source_hash"] == "hA"
    assert persisted == evidence


def test_evaluate_quality_passes_conserved_result():
    source_rows = [
        {
            "event_time": "t1", "business_date": BUSINESS_DATE, "plant_id": "plant-a",
            "line_id": "line-1", "work_order_id": "wo-1", "machine_id": "mc-1",
            "product_code": "p", "operation": "assembly",
            "units_produced": "10", "defect_count": "1", "cycle_time_ms": "100",
        }
    ]
    silver = transform_silver(source_rows, BUSINESS_DATE, "h")
    gold = transform_gold(silver, BUSINESS_DATE)
    checks, passed = s7.evaluate_quality(source_rows, silver, gold, BUSINESS_DATE)
    assert passed is True
    assert {c["name"] for c in checks} >= {
        "row_count_source_to_silver",
        "unit_conservation_silver_to_gold",
        "numeric_range_within_bounds",
    }


def test_evaluate_quality_fails_on_broken_conservation():
    source_rows = [
        {
            "event_time": "t1", "business_date": BUSINESS_DATE, "plant_id": "plant-a",
            "line_id": "line-1", "work_order_id": "wo-1", "machine_id": "mc-1",
            "product_code": "p", "operation": "assembly",
            "units_produced": "10", "defect_count": "1", "cycle_time_ms": "100",
        }
    ]
    silver = transform_silver(source_rows, BUSINESS_DATE, "h")
    gold = transform_gold(silver, BUSINESS_DATE)
    gold[0]["units_produced"] = 999  # tamper: gold no longer conserves silver
    checks, passed = s7.evaluate_quality(source_rows, silver, gold, BUSINESS_DATE)
    assert passed is False
    assert any(c["name"] == "unit_conservation_silver_to_gold" and c["status"] == "fail" for c in checks)


def test_read_canonical_rows_requires_all_columns(tmp_path):
    good = tmp_path / "good.csv"
    _write_direct_canonical_csv(good, [_canonical_row()])
    rows = s7.read_canonical_rows(good)
    assert rows[0]["event_id"] == "evt-20260629-000001"

    bad = tmp_path / "bad.csv"
    bad.write_text("event_time,units_produced\nt,1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="missing columns"):
        s7.read_canonical_rows(bad)


# --------------------------------------------------------------------------- #
# Spark integration tests
# --------------------------------------------------------------------------- #
spark = pytest.mark.skipif(
    importlib.util.find_spec("pyspark") is None,
    reason="optional Spark/Iceberg dependency not installed; run `pip install -r requirements-spark.txt`",
)


@spark
def test_engine_parity_silver_and_gold_match_python(tmp_path):
    adapter = _land_and_adapt(
        tmp_path, "parity", [(0, _event(1)), (1, _event(2)), (2, _event(3))]
    )
    source_rows = s7.read_canonical_rows(adapter.csv_path)
    python_silver = transform_silver(source_rows, BUSINESS_DATE, adapter.source_hash)
    python_gold = transform_gold(python_silver, BUSINESS_DATE)

    session = s7.build_spark_session(tmp_path / "wh")
    try:
        raw = s7.read_canonical_dataframe(session, adapter.csv_path)
        silver_rows = s7.collect_silver_rows(s7.spark_transform_silver(raw, BUSINESS_DATE, adapter.source_hash))
        gold_df = s7.spark_transform_gold(s7.spark_transform_silver(raw, BUSINESS_DATE, adapter.source_hash))
        gold_rows = s7.collect_gold_rows(gold_df)
        _, exchange = s7.plan_evidence(gold_df)
    finally:
        session.stop()

    python_silver_sorted = sorted(
        python_silver, key=lambda r: (r["work_order_id"], r["machine_id"], r["event_time"])
    )
    assert silver_rows == python_silver_sorted
    assert gold_rows == python_gold
    # Gold conserves and matches the batch spine totals (units 10+20+30, defects 0+1+2).
    assert gold_rows[0]["units_produced"] == 60
    assert gold_rows[0]["defect_count"] == 3
    assert gold_rows[0]["defect_rate"] == 0.05
    assert gold_rows[0]["avg_cycle_time_ms"] == 820.0
    assert exchange is True  # groupBy shuffle


@spark
def test_dedup_keeps_one_row_per_natural_key(tmp_path):
    # Two accepted events share the silver natural key (same wo/machine/event_time)
    # but carry different event_id, so both land; silver must keep exactly one.
    base = _event(1)
    twin = _event(1)
    twin["event_id"] = "evt-20260629-090001"
    adapter = _land_and_adapt(
        tmp_path, "dedup", [(0, base), (1, twin), (2, _event(2))]
    )
    source_rows = s7.read_canonical_rows(adapter.csv_path)
    assert len(source_rows) == 3

    session = s7.build_spark_session(tmp_path / "wh")
    try:
        raw = s7.read_canonical_dataframe(session, adapter.csv_path)
        silver_rows = s7.collect_silver_rows(s7.spark_transform_silver(raw, BUSINESS_DATE, adapter.source_hash))
    finally:
        session.stop()

    python_silver = transform_silver(source_rows, BUSINESS_DATE, adapter.source_hash)
    assert len(silver_rows) == len(python_silver) == 2
    keys = {(r["work_order_id"], r["machine_id"], r["event_time"]) for r in silver_rows}
    assert len(keys) == 2


@spark
def test_quality_gate_blocks_publish_without_touching_state(tmp_path):
    bad_csv = tmp_path / "adapter" / "bad.csv"
    _write_direct_canonical_csv(
        bad_csv, [_canonical_row(units_produced=5, defect_count=9)]  # defect > units
    )
    evidence = s7.run_spark_machine_event_batch(
        csv_path=bad_csv,
        source_hash="hash-bad",
        business_date=BUSINESS_DATE,
        warehouse_path=tmp_path / "wh",
        evidence_output_dir=tmp_path / "evidence",
    )
    assert evidence["status"] == "quality_failed"
    assert evidence["published"] is False
    assert evidence["gold_snapshot_id"] is None
    assert any(c["name"] == "numeric_range_within_bounds" for c in evidence["quality_fail_detail"])
    # No success state pointer was written.
    assert not s7._publish_state_path(tmp_path / "evidence", s7.TABLE_NAME, BUSINESS_DATE).exists()


@spark
def test_publish_rerun_correction_and_other_date_preservation(tmp_path):
    warehouse = tmp_path / "wh"
    evidence_dir = tmp_path / "evidence"

    a = _land_and_adapt(tmp_path, "srcA", [(0, _event(1)), (1, _event(2))])
    b = _land_and_adapt(
        tmp_path,
        "srcB",
        [(0, _event(4, work_order_id="wo-9001", machine_id="mc-901", units_produced=200, defect_count=5))],
    )
    d2 = _land_and_adapt(
        tmp_path, "d2", [(0, _event(3, business_date=OTHER_DATE))], business_date=OTHER_DATE
    )

    def run(adapter, business_date):
        return s7.run_spark_machine_event_batch(
            csv_path=adapter.csv_path,
            source_hash=adapter.source_hash,
            business_date=business_date,
            warehouse_path=warehouse,
            evidence_output_dir=evidence_dir,
        )

    r_d2 = run(d2, OTHER_DATE)
    r_a1 = run(a, BUSINESS_DATE)
    r_a2 = run(a, BUSINESS_DATE)
    r_b = run(b, BUSINESS_DATE)

    assert r_d2["status"] == "published"
    assert r_a1["status"] == "published"
    assert r_a1["snapshot_count"] == 2  # d2 + a
    assert r_a1["gold_snapshot_id"] != r_d2["gold_snapshot_id"]

    # Same source retry: skipped, no new snapshot.
    assert r_a2["status"] == "skipped"
    assert r_a2["published"] is False
    assert r_a2["gold_snapshot_id"] == r_a1["gold_snapshot_id"]
    assert r_a2["snapshot_count"] == r_a1["snapshot_count"]

    # Correction source B: exactly one new snapshot, target partition replaced.
    assert r_b["status"] == "published"
    assert r_b["snapshot_count"] == r_a1["snapshot_count"] + 1
    assert r_b["gold_snapshot_id"] != r_a1["gold_snapshot_id"]

    d1_rows = [r for r in r_b["current_table_rows"] if r["business_date"] == BUSINESS_DATE]
    d2_rows = [r for r in r_b["current_table_rows"] if r["business_date"] == OTHER_DATE]
    # D1 replaced by source B (units 200), D2 untouched from its own publish.
    assert sum(r["units_produced"] for r in d1_rows) == 200
    assert d2_rows == r_d2["target_partition_rows"]
    assert r_b["physical_plan"]["exchange_observed"] is True

    # M2 (real path): the persisted evidence equals the returned evidence.
    persisted = json.loads((evidence_dir / "spark_machine_event_batch.json").read_text())
    assert persisted == r_b


@spark
def test_gold_rounding_matches_python_at_half_boundary(tmp_path):
    # 40 events, cycle_time_ms sum 32107 -> avg 802.675, which is stored as 802.6749...:
    # Python round -> 802.67, naive Spark bround -> 802.68. Gold must match Python (H1).
    rows = []
    total = 0
    for i in range(40):
        cycle = 829 if i == 0 else 802  # 829 + 39*802 = 32107
        total += cycle
        rows.append(
            _canonical_row(
                event_time=f"2026-06-29T08:{i:02d}:00Z",
                work_order_id=f"wo-{2000 + i}",
                machine_id="mc-777",
                units_produced=10,
                defect_count=0,
                cycle_time_ms=cycle,
                event_id=f"evt-20260629-{700000 + i:06d}",
                kafka_offset=i,
            )
        )
    assert total == 32107
    csv_path = tmp_path / "adapter" / "boundary.csv"
    _write_direct_canonical_csv(csv_path, rows)

    source_rows = s7.read_canonical_rows(csv_path)
    python_gold = transform_gold(transform_silver(source_rows, BUSINESS_DATE, "hB"), BUSINESS_DATE)
    assert python_gold[0]["avg_cycle_time_ms"] == 802.67  # Python round-half-even on the stored double

    session = s7.build_spark_session(tmp_path / "wh")
    try:
        raw = s7.read_canonical_dataframe(session, csv_path)
        gold_rows = s7.collect_gold_rows(s7.spark_transform_gold(s7.spark_transform_silver(raw, BUSINESS_DATE, "hB")))
    finally:
        session.stop()

    assert gold_rows == python_gold
    assert gold_rows[0]["avg_cycle_time_ms"] == 802.67
    assert gold_rows[0]["units_produced"] == 400


@spark
def test_stale_success_state_on_recreated_warehouse_rewrites(tmp_path):
    # H2: evidence state persists but the warehouse is emptied/recreated. The recorded
    # snapshot is no longer in table history, so the same source must WRITE, not skip.
    a = _land_and_adapt(tmp_path, "h2", [(0, _event(1)), (1, _event(2))])
    warehouse = tmp_path / "wh"
    evidence_dir = tmp_path / "evidence"

    def run():
        return s7.run_spark_machine_event_batch(
            csv_path=a.csv_path, source_hash=a.source_hash, business_date=BUSINESS_DATE,
            warehouse_path=warehouse, evidence_output_dir=evidence_dir,
        )

    r1 = run()
    assert r1["status"] == "published"
    assert r1["target_partition_row_count"] >= 1

    shutil.rmtree(warehouse)  # warehouse gone; evidence _state JSON remains

    r2 = run()
    assert r2["status"] == "published"  # recovered, not a false skip
    assert r2["target_partition_row_count"] >= 1
    assert len(r2["current_table_rows"]) >= 1
