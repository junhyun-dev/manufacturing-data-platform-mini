import pytest

from manufacturing_data_platform.pipeline.lakehouse import DATASET_ID, run_lakehouse_pipeline
from manufacturing_data_platform.pipeline.operator_report import (
    build_operator_report,
    load_successful_run,
)


def test_operator_report_summarizes_run_quality_and_lineage(tmp_path):
    result = run_lakehouse_pipeline(
        raw_path=tmp_path / "raw" / "manufacturing_events.csv",
        output_dir=tmp_path / "lakehouse",
        catalog_backend="json",
    )

    run_doc = load_successful_run(tmp_path / "lakehouse", result.business_date)
    report = build_operator_report(run_doc)

    assert report["dataset_id"] == DATASET_ID
    assert report["business_date"] == result.business_date
    assert report["gold_grain"]["row_grain"] == [
        "business_date",
        "plant_id",
        "line_id",
        "product_code",
    ]
    assert report["run"]["run_id"] == result.run_id
    assert report["run"]["source_hash"] == result.source_hash
    assert report["run"]["schema_hash"] == result.schema_hash
    assert report["quality_summary"]["fail_count"] == 0
    assert report["quality_summary"]["warn_count"] == 0
    assert {
        check["name"] for check in report["quality_summary"]["rca_focus_checks"]
    } == {
        "row_count_source_to_silver",
        "unit_conservation_silver_to_gold",
        "freshness_business_date",
        "schema_drift",
    }
    assert [item["name"] for item in report["lineage_trace"]] == [
        "gold",
        "silver",
        "bronze",
        "source",
    ]
    assert report["lineage_trace"][-1]["hash"] == result.source_hash
    assert "column-level lineage" in report["claim_boundary"]["does_not_support"]


def test_operator_report_missing_state_is_explicit(tmp_path):
    with pytest.raises(FileNotFoundError, match="no successful run state found"):
        load_successful_run(tmp_path / "lakehouse", "2026-06-29")
