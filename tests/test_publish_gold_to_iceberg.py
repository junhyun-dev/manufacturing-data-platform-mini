from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from manufacturing_data_platform.pipeline.lakehouse import run_lakehouse_pipeline
from manufacturing_data_platform.pipeline.publish_gold_to_iceberg import (
    load_successful_gold_run,
    publish_gold_to_iceberg,
    read_gold_rows,
    validate_table_name,
)


HEADER = (
    "event_time,plant_id,line_id,work_order_id,machine_id,product_code,"
    "operation,units_produced,defect_count,cycle_time_ms,business_date"
)


def _write_csv(path: Path, body_rows: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(HEADER + "\n" + "\n".join(body_rows) + "\n", encoding="utf-8")
    return path


def test_load_successful_gold_run_reads_json_catalog_state(tmp_path):
    lakehouse_dir = tmp_path / "lakehouse"
    result = run_lakehouse_pipeline(
        raw_path=tmp_path / "raw" / "manufacturing_events.csv",
        output_dir=lakehouse_dir,
        business_date="2026-06-29",
        catalog_backend="json",
    )

    doc = load_successful_gold_run(lakehouse_dir, "2026-06-29")
    rows = read_gold_rows(Path(doc["paths"]["gold"]))

    assert doc["run_id"] == result.run_id
    assert doc["quality"]["passed"] is True
    assert rows
    assert isinstance(rows[0]["units_produced"], int)
    assert isinstance(rows[0]["defect_rate"], float)


def test_load_successful_gold_run_fails_when_no_success_state_exists(tmp_path):
    with pytest.raises(FileNotFoundError, match="no successful JSON catalog state"):
        load_successful_gold_run(tmp_path / "missing-lakehouse", "2026-06-29")


def test_validate_table_name_rejects_non_dotted_sql_identifiers():
    validate_table_name("local.db.gold_daily_metrics")
    with pytest.raises(ValueError, match="dotted"):
        validate_table_name("local.db.gold_daily_metrics;DROP TABLE x")


@pytest.mark.skipif(
    importlib.util.find_spec("pyspark") is None,
    reason="optional Spark/Iceberg dependency not installed; run `pip install -r requirements-spark.txt`",
)
def test_publish_gold_to_iceberg_overwrites_target_partition_and_skips_retry(tmp_path):
    lakehouse_dir = tmp_path / "lakehouse"
    warehouse = tmp_path / "warehouse"
    evidence_dir = tmp_path / "evidence"

    run_lakehouse_pipeline(
        raw_path=tmp_path / "raw" / "initial.csv",
        output_dir=lakehouse_dir,
        business_date="2026-06-29",
        catalog_backend="json",
    )
    first_publish = publish_gold_to_iceberg(
        lakehouse_output_dir=lakehouse_dir,
        business_date="2026-06-29",
        warehouse_path=warehouse,
        output_dir=evidence_dir,
    )

    retry_publish = publish_gold_to_iceberg(
        lakehouse_output_dir=lakehouse_dir,
        business_date="2026-06-29",
        warehouse_path=warehouse,
        output_dir=evidence_dir,
    )

    assert first_publish["status"] == "published"
    assert retry_publish["status"] == "skipped"
    assert retry_publish["gold_snapshot_id"] == first_publish["gold_snapshot_id"]

    other_date_raw = _write_csv(
        tmp_path / "raw" / "other-date.csv",
        [
            "2026-06-30T08:00:00Z,plant-a,line-2,wo-3001,mc-301,motor-b,assembly,50,1,700,2026-06-30",
        ],
    )
    run_lakehouse_pipeline(
        raw_path=other_date_raw,
        output_dir=lakehouse_dir,
        business_date="2026-06-30",
        catalog_backend="json",
    )
    publish_gold_to_iceberg(
        lakehouse_output_dir=lakehouse_dir,
        business_date="2026-06-30",
        warehouse_path=warehouse,
        output_dir=evidence_dir,
    )

    corrected_raw = _write_csv(
        tmp_path / "raw" / "corrected.csv",
        [
            "2026-06-29T08:00:00Z,plant-a,line-1,wo-9001,mc-901,gearbox-a,assembly,150,6,850,2026-06-29",
        ],
    )
    run_lakehouse_pipeline(
        raw_path=corrected_raw,
        output_dir=lakehouse_dir,
        business_date="2026-06-29",
        catalog_backend="json",
    )
    corrected_publish = publish_gold_to_iceberg(
        lakehouse_output_dir=lakehouse_dir,
        business_date="2026-06-29",
        warehouse_path=warehouse,
        output_dir=evidence_dir,
    )

    target_rows = corrected_publish["target_partition_rows"]
    other_rows = [
        row
        for row in corrected_publish["current_table_rows"]
        if row["business_date"] == "2026-06-30"
    ]

    assert corrected_publish["status"] == "published"
    assert corrected_publish["gold_snapshot_id"] != first_publish["gold_snapshot_id"]
    assert target_rows == [
        {
            "business_date": "2026-06-29",
            "plant_id": "plant-a",
            "line_id": "line-1",
            "product_code": "gearbox-a",
            "units_produced": 150,
            "defect_count": 6,
            "defect_rate": 0.04,
            "avg_cycle_time_ms": 850.0,
            "closing_status": "provisional",
        }
    ]
    assert other_rows == [
        {
            "business_date": "2026-06-30",
            "plant_id": "plant-a",
            "line_id": "line-2",
            "product_code": "motor-b",
            "units_produced": 50,
            "defect_count": 1,
            "defect_rate": 0.02,
            "avg_cycle_time_ms": 700.0,
            "closing_status": "provisional",
        }
    ]
    assert (evidence_dir / "gold_iceberg_publish.json").exists()
