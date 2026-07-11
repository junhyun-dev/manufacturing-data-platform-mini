from __future__ import annotations

import importlib.util

import pytest

from manufacturing_data_platform.pipeline import spark_iceberg_skeleton as skeleton


pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("pyspark") is None,
    reason="optional Spark/Iceberg dependency not installed; run `pip install -r requirements-spark.txt`",
)


def test_environment_gate_creates_and_reads_iceberg_table(tmp_path):
    result = skeleton.run_environment_gate(tmp_path / "warehouse")

    assert result["table"] == skeleton.GATE_TABLE_NAME
    assert result["row_count"] == 1
    assert result["rows"] == [{"id": 1, "label": "ok"}]
    assert isinstance(result["current_snapshot_id"], int)
    assert result["snapshot_count"] >= 1


def test_partition_overwrite_preserves_other_partition_and_records_snapshots(tmp_path):
    evidence = skeleton.run_spark_iceberg_skeleton(
        warehouse_path=tmp_path / "warehouse",
        output_dir=tmp_path / "evidence",
    )

    runs = evidence["runs"]
    initial = runs[0]
    same_source = runs[1]
    corrected = runs[2]

    assert initial["status"] == "processed"
    assert same_source["status"] == "skipped"
    assert corrected["status"] == "processed"
    assert isinstance(initial["gold_snapshot_id"], int)
    assert isinstance(corrected["gold_snapshot_id"], int)
    assert corrected["gold_snapshot_id"] != initial["gold_snapshot_id"]
    assert same_source["gold_snapshot_id"] == initial["gold_snapshot_id"]
    assert same_source["snapshot_count"] == initial["snapshot_count"]

    assertions = evidence["partition_overwrite_assertions"]
    assert assertions["target_partition_row_count"] == assertions["corrected_row_count"]
    assert assertions["snapshot_increment"] == 1
    assert assertions["same_source_created_snapshot"] is False
    assert assertions["target_partition_rows"] == skeleton._corrected_gold_rows()
    assert assertions["other_partition_rows"] == skeleton._other_date_gold_rows()

    assert (tmp_path / "evidence" / "run_snapshot_map.json").exists()
    assert (tmp_path / "evidence" / "current_gold.json").exists()
    assert (tmp_path / "evidence" / "snapshot_comparison.json").exists()
