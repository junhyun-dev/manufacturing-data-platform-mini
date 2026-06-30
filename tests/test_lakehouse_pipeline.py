from datetime import datetime, timezone
from pathlib import Path

import mongomock

from robot_data_platform.db import ensure_indexes
from robot_data_platform.pipeline.lakehouse import (
    DATASET_ID,
    build_schema_drift_check,
    run_lakehouse_pipeline,
    transform_gold,
    transform_silver,
)


HEADER = (
    "event_time,plant_id,line_id,work_order_id,robot_id,product_code,"
    "operation,units_produced,defect_count,cycle_time_ms,business_date"
)


def _mongo():
    db = mongomock.MongoClient()["test_robot_data_platform"]
    ensure_indexes(db)
    return db


def _write_csv(path: Path, body_rows: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(HEADER + "\n" + "\n".join(body_rows) + "\n", encoding="utf-8")
    return path


def _check(result, name):
    return next(c for c in result.quality_checks if c["name"] == name)


# --------------------------------------------------------------------------- #
# transform / IO separation
# --------------------------------------------------------------------------- #
def test_transform_silver_filters_other_dates_and_dedups():
    rows = [
        {"event_time": "t1", "business_date": "2026-06-29", "plant_id": "plant-a", "line_id": "line-1", "work_order_id": "wo-1", "robot_id": "rb-1", "product_code": "p", "operation": "assembly", "units_produced": "10", "defect_count": "1", "cycle_time_ms": "100"},
        # exact natural-key duplicate -> dropped
        {"event_time": "t1", "business_date": "2026-06-29", "plant_id": "plant-a", "line_id": "line-1", "work_order_id": "wo-1", "robot_id": "rb-1", "product_code": "p", "operation": "assembly", "units_produced": "10", "defect_count": "1", "cycle_time_ms": "100"},
        {"event_time": "t2", "business_date": "2026-06-29", "plant_id": "plant-a", "line_id": "line-1", "work_order_id": "wo-2", "robot_id": "rb-2", "product_code": "p", "operation": "assembly", "units_produced": "20", "defect_count": "0", "cycle_time_ms": "120"},
        # different business_date -> filtered out
        {"event_time": "t3", "business_date": "2026-06-28", "plant_id": "plant-a", "line_id": "line-1", "work_order_id": "wo-3", "robot_id": "rb-3", "product_code": "p", "operation": "assembly", "units_produced": "30", "defect_count": "0", "cycle_time_ms": "130"},
    ]
    silver = transform_silver(rows, "2026-06-29", "hash")
    assert len(silver) == 2
    assert all(r["business_date"] == "2026-06-29" for r in silver)
    assert all(r["source_hash"] == "hash" for r in silver)
    assert all(isinstance(r["units_produced"], int) for r in silver)


def test_transform_gold_conserves_units_and_defects():
    silver = transform_silver(
        [
            {"event_time": "t1", "business_date": "d", "plant_id": "a", "line_id": "1", "work_order_id": "w1", "robot_id": "r1", "product_code": "p", "operation": "assembly", "units_produced": "10", "defect_count": "1", "cycle_time_ms": "100"},
            {"event_time": "t2", "business_date": "d", "plant_id": "a", "line_id": "1", "work_order_id": "w2", "robot_id": "r2", "product_code": "p", "operation": "assembly", "units_produced": "20", "defect_count": "2", "cycle_time_ms": "200"},
        ],
        "d",
        "h",
    )
    gold = transform_gold(silver, "d")
    assert sum(r["units_produced"] for r in gold) == sum(r["units_produced"] for r in silver) == 30
    assert sum(r["defect_count"] for r in gold) == sum(r["defect_count"] for r in silver) == 3


# --------------------------------------------------------------------------- #
# quality suite
# --------------------------------------------------------------------------- #
def test_quality_suite_passes_on_synthetic_sample(tmp_path):
    db = _mongo()
    result = run_lakehouse_pipeline(
        raw_path=tmp_path / "raw" / "m.csv",  # absent -> synthetic sample written
        output_dir=tmp_path / "lakehouse",
        db=db,
        catalog_backend="mongo",
    )
    assert result.quality_passed is True
    assert result.status == "processed"
    names = {c["name"] for c in result.quality_checks}
    assert {
        "row_count_source_to_silver",
        "unit_conservation_silver_to_gold",
        "not_null_required_columns",
        "unique_natural_key",
        "accepted_values_operation",
        "numeric_range_within_bounds",
        "freshness_business_date",
        "schema_drift",
    } <= names


def test_quality_reconciliation_distinguishes_filtering_from_loss(tmp_path):
    rows = [
        "2026-06-29T08:00:00Z,plant-a,line-1,wo-1,rb-1,gearbox-a,assembly,10,1,100,2026-06-29",
        "2026-06-29T08:00:00Z,plant-a,line-1,wo-1,rb-1,gearbox-a,assembly,10,1,100,2026-06-29",  # dup
        "2026-06-29T09:00:00Z,plant-a,line-1,wo-2,rb-2,gearbox-a,assembly,20,0,120,2026-06-29",
        "2026-06-29T10:00:00Z,plant-a,line-2,wo-3,rb-3,motor-b,assembly,30,2,130,2026-06-29",
        "2026-06-28T08:00:00Z,plant-a,line-1,wo-0,rb-1,gearbox-a,assembly,99,0,100,2026-06-28",  # other date
    ]
    raw = _write_csv(tmp_path / "raw" / "m.csv", rows)
    result = run_lakehouse_pipeline(
        raw_path=raw,
        output_dir=tmp_path / "lh",
        business_date="2026-06-29",
        catalog_backend="json",
    )
    assert result.quality_passed is True
    recon = _check(result, "row_count_source_to_silver")
    assert recon["expected"] == 3  # distinct natural keys on the active date
    assert recon["actual"] == 3  # silver rows
    assert "for_business_date=4" in recon["detail"]
    assert "duplicates_in_source=1" in recon["detail"]


def test_quality_fails_on_accepted_values_violation(tmp_path):
    rows = ["2026-06-29T08:00:00Z,plant-a,line-1,wo-1,rb-1,gearbox-a,teleport,10,1,100,2026-06-29"]
    raw = _write_csv(tmp_path / "raw" / "m.csv", rows)
    result = run_lakehouse_pipeline(
        raw_path=raw, output_dir=tmp_path / "lh", business_date="2026-06-29", catalog_backend="json"
    )
    assert result.quality_passed is False
    acc = _check(result, "accepted_values_operation")
    assert acc["status"] == "fail"
    assert "teleport" in acc["actual"]


def test_quality_fails_on_numeric_range_violation(tmp_path):
    # defect_count (20) > units_produced (10)
    rows = ["2026-06-29T08:00:00Z,plant-a,line-1,wo-1,rb-1,gearbox-a,assembly,10,20,100,2026-06-29"]
    raw = _write_csv(tmp_path / "raw" / "m.csv", rows)
    result = run_lakehouse_pipeline(
        raw_path=raw, output_dir=tmp_path / "lh", business_date="2026-06-29", catalog_backend="json"
    )
    assert result.quality_passed is False
    rng = _check(result, "numeric_range_within_bounds")
    assert rng["status"] == "fail"


def test_quality_fails_on_not_null_violation(tmp_path):
    # empty plant_id (a string field, so no cast crash) -> not_null fail
    rows = ["2026-06-29T08:00:00Z,,line-1,wo-1,rb-1,gearbox-a,assembly,10,1,100,2026-06-29"]
    raw = _write_csv(tmp_path / "raw" / "m.csv", rows)
    result = run_lakehouse_pipeline(
        raw_path=raw, output_dir=tmp_path / "lh", business_date="2026-06-29", catalog_backend="json"
    )
    assert result.quality_passed is False
    nn = _check(result, "not_null_required_columns")
    assert nn["status"] == "fail"
    assert "plant_id" in nn["detail"]


def test_quality_fails_on_unparseable_business_date(tmp_path):
    # freshness check also guards that the active partition is a valid ISO date
    rows = ["2026-06-29T08:00:00Z,plant-a,line-1,wo-1,rb-1,gearbox-a,assembly,10,1,100,not-a-date"]
    raw = _write_csv(tmp_path / "raw" / "m.csv", rows)
    result = run_lakehouse_pipeline(
        raw_path=raw, output_dir=tmp_path / "lh", business_date="not-a-date", catalog_backend="json"
    )
    assert result.quality_passed is False
    fresh = _check(result, "freshness_business_date")
    assert fresh["status"] == "fail"


# --------------------------------------------------------------------------- #
# schema drift
# --------------------------------------------------------------------------- #
def test_schema_drift_helper_states():
    assert build_schema_drift_check(None, "a" * 64)["status"] == "pass"
    assert build_schema_drift_check("a" * 64, "a" * 64)["status"] == "pass"
    assert build_schema_drift_check("a" * 64, "b" * 64)["status"] == "warn"


def test_schema_drift_warns_against_previous_successful_run(tmp_path):
    db = _mongo()
    # Seed an older successful run with a different schema_hash.
    db.lakehouse_runs.insert_one(
        {
            "dataset_id": DATASET_ID,
            "run_id": "seed-old-run",
            "business_date": "2026-06-01",
            "source_hash": "seed-source",
            "schema_hash": "0" * 64,
            "quality": {"passed": True, "checks": []},
            "created_at": datetime(2020, 1, 1, tzinfo=timezone.utc),
        }
    )
    result = run_lakehouse_pipeline(
        raw_path=tmp_path / "raw" / "m.csv",
        output_dir=tmp_path / "lakehouse",
        db=db,
        catalog_backend="mongo",
    )
    drift = _check(result, "schema_drift")
    assert drift["status"] == "warn"
    assert result.quality_passed is True  # warn policy does not fail the run
    run_doc = db.lakehouse_runs.find_one({"run_id": result.run_id}, {"_id": 0})
    assert run_doc["schema_drift"]["status"] == "warn"
    assert run_doc["schema_drift"]["policy"] == "warn"


def test_schema_stable_when_schema_unchanged_across_dates(tmp_path):
    db = _mongo()
    run_lakehouse_pipeline(
        raw_path=tmp_path / "raw" / "m.csv",  # sample, business_date 2026-06-29
        output_dir=tmp_path / "lakehouse",
        db=db,
        catalog_backend="mongo",
    )
    # Same schema, different date + different source content -> not skipped, stable.
    rows = ["2026-06-28T08:00:00Z,plant-a,line-1,wo-9,rb-9,gearbox-a,assembly,10,1,100,2026-06-28"]
    raw2 = _write_csv(tmp_path / "raw2" / "m.csv", rows)
    second = run_lakehouse_pipeline(
        raw_path=raw2,
        output_dir=tmp_path / "lakehouse",
        business_date="2026-06-28",
        db=db,
        catalog_backend="mongo",
    )
    assert _check(second, "schema_drift")["status"] == "pass"


def test_schema_drift_warns_on_added_column(tmp_path):
    # Real drift detection: an extra CSV column (beyond required) must change
    # schema_hash and surface as a warn. Guards the REQUIRED_COLUMNS-vs-actual
    # bug found in self-audit (added columns were previously invisible).
    db = _mongo()
    out = tmp_path / "lakehouse"
    run_lakehouse_pipeline(
        raw_path=tmp_path / "raw" / "m.csv", output_dir=out, db=db, catalog_backend="mongo"
    )  # baseline: 11 required columns

    raw2 = tmp_path / "raw2" / "m.csv"
    raw2.parent.mkdir(parents=True)
    raw2.write_text(
        HEADER + ",operator_id\n"
        "2026-06-28T08:00:00Z,plant-a,line-1,wo-9,rb-9,gearbox-a,assembly,10,1,100,2026-06-28,op-1\n",
        encoding="utf-8",
    )
    second = run_lakehouse_pipeline(
        raw_path=raw2, output_dir=out, business_date="2026-06-28", db=db, catalog_backend="mongo"
    )
    drift = _check(second, "schema_drift")
    assert drift["status"] == "warn"
    assert drift["expected"] != drift["actual"]  # previous vs current schema hash differ
    assert second.quality_passed is True  # warn policy does not fail the run


# --------------------------------------------------------------------------- #
# idempotency
# --------------------------------------------------------------------------- #
def test_rerun_same_source_and_date_is_skipped_mongo(tmp_path):
    db = _mongo()
    raw = tmp_path / "raw" / "m.csv"
    out = tmp_path / "lakehouse"
    first = run_lakehouse_pipeline(raw_path=raw, output_dir=out, db=db, catalog_backend="mongo")
    second = run_lakehouse_pipeline(raw_path=raw, output_dir=out, db=db, catalog_backend="mongo")

    assert first.status == "processed"
    assert second.status == "skipped"
    assert second.run_id == first.run_id
    assert db.lakehouse_runs.count_documents({"dataset_id": DATASET_ID}) == 1
    reused = db.lakehouse_runs.find_one({"run_id": first.run_id})
    assert reused.get("reuse_count") == 1


def test_rerun_same_source_and_date_is_skipped_json(tmp_path):
    raw = tmp_path / "raw" / "m.csv"
    out = tmp_path / "lakehouse"
    first = run_lakehouse_pipeline(raw_path=raw, output_dir=out, catalog_backend="json")
    second = run_lakehouse_pipeline(raw_path=raw, output_dir=out, catalog_backend="json")

    assert first.status == "processed"
    assert second.status == "skipped"
    assert second.run_id == first.run_id


def test_changed_source_same_date_creates_new_run_mongo(tmp_path):
    # Same business_date but different source content -> different source_hash
    # -> NOT skipped: a genuinely new run is processed.
    db = _mongo()
    out = tmp_path / "lakehouse"
    first = run_lakehouse_pipeline(
        raw_path=tmp_path / "raw" / "m.csv", output_dir=out, db=db, catalog_backend="mongo"
    )
    rows = ["2026-06-29T08:00:00Z,plant-a,line-1,wo-1001,rb-101,gearbox-a,assembly,121,2,840,2026-06-29"]
    raw2 = _write_csv(tmp_path / "raw2" / "m.csv", rows)
    second = run_lakehouse_pipeline(
        raw_path=raw2, output_dir=out, business_date="2026-06-29", db=db, catalog_backend="mongo"
    )
    assert first.status == "processed"
    assert second.status == "processed"
    assert second.run_id != first.run_id
    assert second.source_hash != first.source_hash
    assert db.lakehouse_runs.count_documents(
        {"dataset_id": DATASET_ID, "business_date": "2026-06-29"}
    ) == 2


# --------------------------------------------------------------------------- #
# preserved coverage: layers + catalog documents
# --------------------------------------------------------------------------- #
def test_layers_and_mongo_catalog_documents(tmp_path):
    db = _mongo()
    result = run_lakehouse_pipeline(
        raw_path=tmp_path / "raw" / "m.csv",
        output_dir=tmp_path / "lakehouse",
        db=db,
        catalog_backend="mongo",
    )
    assert result.paths.bronze_path.exists()
    assert result.paths.silver_path.exists()
    assert result.paths.gold_path.exists()
    assert result.paths.quality_path.exists()
    assert result.paths.manifest_path.exists()

    run_doc = db.lakehouse_runs.find_one({"run_id": result.run_id}, {"_id": 0})
    assert run_doc is not None
    assert [layer["name"] for layer in run_doc["layers"]] == ["bronze", "silver", "gold"]
    assert run_doc["quality"]["passed"] is True
    assert run_doc["source_hash"] == result.source_hash


def test_json_catalog_backend_writes_entry(tmp_path):
    result = run_lakehouse_pipeline(
        raw_path=tmp_path / "raw" / "m.csv",
        output_dir=tmp_path / "lakehouse",
        catalog_backend="json",
    )
    entry = result.paths.manifest_path.parent / "catalog_entry.json"
    assert entry.exists()
    assert DATASET_ID in entry.read_text(encoding="utf-8")
