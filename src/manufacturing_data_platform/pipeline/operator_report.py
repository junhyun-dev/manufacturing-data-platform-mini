from __future__ import annotations

import argparse
import json
from pathlib import Path

from manufacturing_data_platform.pipeline.lakehouse import DATASET_ID, read_json_file, state_dir


MANUFACTURING_GOLD_GRAIN = {
    "dataset_id": DATASET_ID,
    "row_grain": ["business_date", "plant_id", "line_id", "product_code"],
    "metrics": [
        "units_produced",
        "defect_count",
        "defect_rate",
        "avg_cycle_time_ms",
        "closing_status",
    ],
}


def load_successful_run(
    output_dir: str | Path,
    business_date: str,
    dataset_id: str = DATASET_ID,
) -> dict:
    """Load the successful JSON catalog state for one business_date."""
    path = state_dir(Path(output_dir), dataset_id) / f"business_date={business_date}.json"
    doc = read_json_file(path)
    if not doc:
        raise FileNotFoundError(
            f"no successful run state found for dataset_id={dataset_id}, "
            f"business_date={business_date} at {path}"
        )
    return doc


def build_operator_report(run_doc: dict) -> dict:
    checks = run_doc.get("quality", {}).get("checks", [])
    return {
        "dataset_id": run_doc["dataset_id"],
        "business_date": run_doc["business_date"],
        "gold_grain": gold_grain_for(run_doc["dataset_id"]),
        "run": {
            "run_id": run_doc["run_id"],
            "source_hash": run_doc["source_hash"],
            "schema_hash": run_doc["schema_hash"],
            "quality_passed": run_doc.get("quality", {}).get("passed"),
            "reuse_count": run_doc.get("reuse_count", 0),
            "created_at": run_doc.get("created_at"),
        },
        "source": run_doc.get("source", {}),
        "stats": run_doc.get("stats", {}),
        "schema_drift": run_doc.get("schema_drift", {}),
        "quality_summary": summarize_quality(checks),
        "lineage_trace": build_lineage_trace(run_doc),
        "claim_boundary": {
            "supports": [
                "table/path-level lineage",
                "operator-inspectable run evidence",
                "source/schema identity trace",
                "quality check summary",
            ],
            "does_not_support": [
                "column-level lineage",
                "OpenLineage backend integration",
                "interactive lineage UI",
                "production incident workflow",
            ],
        },
    }


def gold_grain_for(dataset_id: str) -> dict:
    if dataset_id != DATASET_ID:
        return {"dataset_id": dataset_id, "row_grain": "unknown", "metrics": []}
    return MANUFACTURING_GOLD_GRAIN


def summarize_quality(checks: list[dict]) -> dict:
    fail = [check for check in checks if check.get("status") == "fail"]
    warn = [check for check in checks if check.get("status") == "warn"]
    passed = [check for check in checks if check.get("status") == "pass"]
    focus_names = {
        "row_count_source_to_silver",
        "unit_conservation_silver_to_gold",
        "schema_drift",
        "freshness_business_date",
    }
    focus = [check for check in checks if check.get("name") in focus_names]
    return {
        "total_checks": len(checks),
        "pass_count": len(passed),
        "warn_count": len(warn),
        "fail_count": len(fail),
        "failed_checks": [check.get("name") for check in fail],
        "warning_checks": [check.get("name") for check in warn],
        "rca_focus_checks": focus,
    }


def build_lineage_trace(run_doc: dict) -> list[dict]:
    layers_by_name = {layer.get("name"): layer for layer in run_doc.get("layers", [])}
    trace: list[dict] = []
    for layer_name in ("gold", "silver", "bronze"):
        layer = layers_by_name.get(layer_name)
        if layer:
            trace.append(
                {
                    "name": layer_name,
                    "path": layer.get("path"),
                    "parents": layer.get("parents", []),
                }
            )
    source = run_doc.get("source", {})
    if source:
        trace.append(
            {
                "name": "source",
                "path": source.get("path"),
                "hash": source.get("hash"),
                "row_count": source.get("row_count"),
            }
        )
    return trace


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect the run/source/quality/lineage evidence for one business_date."
    )
    parser.add_argument("--output-dir", default="data/lakehouse")
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--dataset-id", default=DATASET_ID)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_doc = load_successful_run(args.output_dir, args.business_date, args.dataset_id)
    report = build_operator_report(run_doc)
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
