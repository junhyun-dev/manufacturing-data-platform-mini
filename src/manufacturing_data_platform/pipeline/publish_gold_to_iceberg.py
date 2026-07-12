from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
from pathlib import Path
from typing import Any

from manufacturing_data_platform.pipeline.lakehouse import (
    DATASET_ID,
    read_json_file,
    state_dir,
    write_json_file,
)
from manufacturing_data_platform.pipeline.spark_iceberg_skeleton import (
    ICEBERG_RUNTIME_COORDINATE,
    build_spark_session,
)


TABLE_NAME = "local.db.gold_daily_metrics"
DEFAULT_WAREHOUSE = "/tmp/manufacturing-mini-lakehouse-iceberg-warehouse"
DEFAULT_OUTPUT_DIR = "/tmp/manufacturing-mini-lakehouse-iceberg-evidence"
TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){2,}$")


def publish_gold_to_iceberg(
    *,
    lakehouse_output_dir: str | Path,
    business_date: str,
    warehouse_path: str | Path = DEFAULT_WAREHOUSE,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    table_name: str = TABLE_NAME,
    clean: bool = False,
) -> dict[str, Any]:
    """Publish the latest successful JSON-backed lakehouse gold CSV to Iceberg.

    This is intentionally a narrow bridge slice: the existing Python lakehouse
    pipeline remains the source of transform/quality/catalog truth. Iceberg is
    used here as the current gold table format for a successful run.
    """
    validate_table_name(table_name)

    warehouse = Path(warehouse_path)
    output = Path(output_dir)
    if clean:
        shutil.rmtree(warehouse, ignore_errors=True)
        shutil.rmtree(output, ignore_errors=True)
    output.mkdir(parents=True, exist_ok=True)

    run_doc = load_successful_gold_run(lakehouse_output_dir, business_date)
    publish_state_path = _publish_state_path(output, business_date)
    existing_publish = read_json_file(publish_state_path)
    if _is_same_successful_publish(existing_publish, run_doc, table_name):
        evidence = dict(existing_publish)
        evidence["status"] = "skipped"
        evidence["skipped_reason"] = "same lakehouse run already published"
        _write_publish_evidence(output, evidence)
        return evidence

    gold_rows = read_gold_rows(Path(run_doc["paths"]["gold"]))

    spark = build_spark_session(
        warehouse,
        app_name="manufacturing-mini-lakehouse-iceberg-publish",
    )
    try:
        create_gold_table_if_missing(spark, table_name)
        gold_dataframe(spark, gold_rows).writeTo(table_name).overwritePartitions()
        snapshot_id = current_snapshot_id(spark, table_name)
        snapshots = snapshot_rows(spark, table_name)
        current_rows = current_gold_rows(spark, table_name)
    finally:
        spark.stop()

    target_partition_rows = [
        row for row in current_rows if row["business_date"] == business_date
    ]
    evidence = {
        "dataset_id": DATASET_ID,
        "table": table_name,
        "business_date": business_date,
        "warehouse": str(warehouse.resolve()),
        "iceberg_runtime_coordinate": ICEBERG_RUNTIME_COORDINATE,
        "status": "published",
        "pipeline_run_id": run_doc["run_id"],
        "source_hash": run_doc["source_hash"],
        "schema_hash": run_doc["schema_hash"],
        "gold_path": run_doc["paths"]["gold"],
        "gold_snapshot_id": snapshot_id,
        "snapshot_count": len(snapshots),
        "snapshots": snapshots,
        "published_row_count": len(gold_rows),
        "target_partition_row_count": len(target_partition_rows),
        "target_partition_rows": target_partition_rows,
        "current_table_rows": current_rows,
        "claim_boundary": {
            "supports": [
                "publishing the latest successful JSON-backed lakehouse gold CSV",
                "local Spark/Iceberg table creation",
                "business_date partition overwrite",
                "run_id to snapshot_id publish evidence",
                "same lakehouse run publish retry without a new snapshot",
            ],
            "does_not_support": [
                "Mongo-backed publish lookup",
                "full medallion Spark rewrite",
                "Spark-based quality suite",
                "production Airflow deployment",
                "cluster Spark",
                "concurrent writer handling",
            ],
        },
    }
    write_json_file(publish_state_path, evidence)
    _write_publish_evidence(output, evidence)
    return evidence


def load_successful_gold_run(
    lakehouse_output_dir: str | Path,
    business_date: str,
) -> dict[str, Any]:
    state_path = (
        state_dir(Path(lakehouse_output_dir), DATASET_ID)
        / f"business_date={business_date}.json"
    )
    doc = read_json_file(state_path)
    if doc is None:
        raise FileNotFoundError(
            f"no successful JSON catalog state found for business_date={business_date}: {state_path}"
        )
    if not doc.get("quality", {}).get("passed"):
        raise ValueError(f"catalog state is not quality-passed: {state_path}")
    gold_path = Path(doc["paths"]["gold"])
    if not gold_path.exists():
        raise FileNotFoundError(f"gold CSV does not exist: {gold_path}")
    return doc


def read_gold_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = [_normalize_gold_csv_row(row) for row in reader]
    if not rows:
        raise ValueError(f"gold CSV is empty: {path}")
    return rows


def validate_table_name(table_name: str) -> None:
    if not TABLE_NAME_RE.match(table_name):
        raise ValueError(
            "table_name must be a dotted catalog.namespace.table identifier"
        )


def create_gold_table_if_missing(spark, table_name: str = TABLE_NAME) -> None:
    namespace = ".".join(table_name.split(".")[:-1])
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          business_date STRING,
          plant_id STRING,
          line_id STRING,
          product_code STRING,
          units_produced BIGINT,
          defect_count BIGINT,
          defect_rate DOUBLE,
          avg_cycle_time_ms DOUBLE,
          closing_status STRING
        )
        USING iceberg
        PARTITIONED BY (business_date)
        """
    )


def gold_dataframe(spark, rows: list[dict[str, Any]]):
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("business_date", StringType(), False),
            StructField("plant_id", StringType(), False),
            StructField("line_id", StringType(), False),
            StructField("product_code", StringType(), False),
            StructField("units_produced", LongType(), False),
            StructField("defect_count", LongType(), False),
            StructField("defect_rate", DoubleType(), False),
            StructField("avg_cycle_time_ms", DoubleType(), False),
            StructField("closing_status", StringType(), False),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


def current_snapshot_id(spark, table_name: str) -> int:
    rows = spark.sql(
        f"SELECT snapshot_id FROM {table_name}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    if not rows:
        raise RuntimeError(f"no snapshots found for {table_name}")
    return int(rows[0]["snapshot_id"])


def snapshot_rows(spark, table_name: str) -> list[dict[str, Any]]:
    rows = spark.sql(
        f"SELECT snapshot_id, committed_at, operation FROM {table_name}.snapshots ORDER BY committed_at"
    ).collect()
    return [
        {
            "snapshot_id": int(row["snapshot_id"]),
            "committed_at": row["committed_at"].isoformat(),
            "operation": row["operation"],
        }
        for row in rows
    ]


def current_gold_rows(spark, table_name: str) -> list[dict[str, Any]]:
    rows = spark.sql(
        f"""
        SELECT
          business_date,
          plant_id,
          line_id,
          product_code,
          units_produced,
          defect_count,
          defect_rate,
          avg_cycle_time_ms,
          closing_status
        FROM {table_name}
        ORDER BY business_date, plant_id, line_id, product_code
        """
    ).collect()
    return [_normalize_spark_row(row.asDict()) for row in rows]


def _normalize_gold_csv_row(row: dict[str, str]) -> dict[str, Any]:
    return {
        "business_date": row["business_date"],
        "plant_id": row["plant_id"],
        "line_id": row["line_id"],
        "product_code": row["product_code"],
        "units_produced": int(row["units_produced"]),
        "defect_count": int(row["defect_count"]),
        "defect_rate": float(row["defect_rate"]),
        "avg_cycle_time_ms": float(row["avg_cycle_time_ms"]),
        "closing_status": row["closing_status"],
    }


def _normalize_spark_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "business_date": row["business_date"],
        "plant_id": row["plant_id"],
        "line_id": row["line_id"],
        "product_code": row["product_code"],
        "units_produced": int(row["units_produced"]),
        "defect_count": int(row["defect_count"]),
        "defect_rate": float(row["defect_rate"]),
        "avg_cycle_time_ms": float(row["avg_cycle_time_ms"]),
        "closing_status": row["closing_status"],
    }


def _publish_state_path(output_dir: Path, business_date: str) -> Path:
    return output_dir / "_state" / DATASET_ID / f"business_date={business_date}.json"


def _is_same_successful_publish(
    existing_publish: dict[str, Any] | None,
    run_doc: dict[str, Any],
    table_name: str,
) -> bool:
    if existing_publish is None:
        return False
    return (
        existing_publish.get("table") == table_name
        and existing_publish.get("pipeline_run_id") == run_doc["run_id"]
        and existing_publish.get("source_hash") == run_doc["source_hash"]
        and existing_publish.get("status") in {"published", "skipped"}
        and isinstance(existing_publish.get("gold_snapshot_id"), int)
    )


def _write_publish_evidence(output_dir: Path, evidence: dict[str, Any]) -> None:
    write_json_file(output_dir / "gold_iceberg_publish.json", evidence)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish the latest successful lakehouse gold CSV to Iceberg."
    )
    parser.add_argument("--lakehouse-output-dir", default="data/lakehouse_airflow")
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--warehouse", default=DEFAULT_WAREHOUSE)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--table", default=TABLE_NAME)
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove the Iceberg warehouse and publish evidence before running.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    evidence = publish_gold_to_iceberg(
        lakehouse_output_dir=args.lakehouse_output_dir,
        business_date=args.business_date,
        warehouse_path=args.warehouse,
        output_dir=args.output_dir,
        table_name=args.table,
        clean=args.clean,
    )
    print(json.dumps(evidence, indent=2))


if __name__ == "__main__":
    main()
