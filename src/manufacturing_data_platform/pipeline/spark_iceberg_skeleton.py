from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path
from typing import Any

from manufacturing_data_platform.pipeline.lakehouse import (
    DATASET_ID,
    transform_gold,
    transform_silver,
)


TABLE_NAME = "local.db.gold_daily_metrics"
GATE_TABLE_NAME = "local.db._spark_iceberg_gate"
ICEBERG_RUNTIME_COORDINATE = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0"
)
DEFAULT_WAREHOUSE = "/tmp/manufacturing-mini-iceberg-warehouse"
DEFAULT_OUTPUT_DIR = "/tmp/manufacturing-mini-iceberg-evidence"
BUSINESS_DATE = "2026-06-29"
OTHER_BUSINESS_DATE = "2026-06-30"
INITIAL_SOURCE_HASH = "source-hash-initial-001"
CORRECTED_SOURCE_HASH = "source-hash-corrected-002"


class SparkIcebergUnavailable(RuntimeError):
    """Raised when the optional Spark/Iceberg runtime cannot be started."""


def build_spark_session(warehouse_path: str | Path, app_name: str = "manufacturing-mini-iceberg"):
    try:
        from pyspark.sql import SparkSession
    except ModuleNotFoundError as exc:
        raise SparkIcebergUnavailable(
            "pyspark unavailable; install requirements-spark.txt"
        ) from exc

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    warehouse = str(Path(warehouse_path).resolve())
    try:
        return (
            SparkSession.builder.appName(app_name)
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.jars.packages", ICEBERG_RUNTIME_COORDINATE)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse)
            .getOrCreate()
        )
    except Exception as exc:  # pragma: no cover - environment-specific gate
        raise SparkIcebergUnavailable(f"Spark/Iceberg startup failed: {exc}") from exc


def run_environment_gate(warehouse_path: str | Path) -> dict[str, Any]:
    spark = build_spark_session(warehouse_path, app_name="manufacturing-mini-iceberg-gate")
    try:
        _create_namespace(spark)
        spark.sql(f"DROP TABLE IF EXISTS {GATE_TABLE_NAME}")
        spark.sql(
            f"""
            CREATE TABLE {GATE_TABLE_NAME} (
              id BIGINT,
              label STRING
            )
            USING iceberg
            """
        )
        spark.sql(f"INSERT INTO {GATE_TABLE_NAME} VALUES (1, 'ok')")
        rows = [row.asDict() for row in spark.sql(f"SELECT * FROM {GATE_TABLE_NAME}").collect()]
        snapshots = _snapshot_rows(spark, GATE_TABLE_NAME)
        return {
            "table": GATE_TABLE_NAME,
            "row_count": len(rows),
            "rows": rows,
            "snapshot_count": len(snapshots),
            "current_snapshot_id": _current_snapshot_id(spark, GATE_TABLE_NAME),
        }
    finally:
        spark.stop()


def run_spark_iceberg_skeleton(
    warehouse_path: str | Path = DEFAULT_WAREHOUSE,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    warehouse = Path(warehouse_path)
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    spark = build_spark_session(warehouse, app_name="manufacturing-mini-iceberg-skeleton")
    try:
        _create_gold_table(spark)

        initial_rows = _initial_gold_rows()
        _gold_dataframe(spark, initial_rows).writeTo(TABLE_NAME).append()
        initial_snapshot_id = _current_snapshot_id(spark, TABLE_NAME)
        initial_snapshot_count = len(_snapshot_rows(spark, TABLE_NAME))

        # Same source_hash rerun remains a no-op: no Iceberg commit is created.
        same_source_snapshot_id = initial_snapshot_id
        same_source_snapshot_count = len(_snapshot_rows(spark, TABLE_NAME))

        corrected_rows = _corrected_gold_rows()
        _gold_dataframe(spark, corrected_rows).writeTo(TABLE_NAME).overwritePartitions()
        corrected_snapshot_id = _current_snapshot_id(spark, TABLE_NAME)
        corrected_snapshot_count = len(_snapshot_rows(spark, TABLE_NAME))

        current_gold_rows = _current_gold_rows(spark)
        d_rows = [row for row in current_gold_rows if row["business_date"] == BUSINESS_DATE]
        d2_rows = [
            row
            for row in current_gold_rows
            if row["business_date"] == OTHER_BUSINESS_DATE
        ]

        evidence = {
            "dataset_id": DATASET_ID,
            "table": TABLE_NAME,
            "business_date": BUSINESS_DATE,
            "other_business_date": OTHER_BUSINESS_DATE,
            "warehouse": str(warehouse.resolve()),
            "iceberg_runtime_coordinate": ICEBERG_RUNTIME_COORDINATE,
            "runs": [
                {
                    "run_id": "spark-skeleton-r1",
                    "source_hash": INITIAL_SOURCE_HASH,
                    "status": "processed",
                    "gold_snapshot_id": initial_snapshot_id,
                    "snapshot_count": initial_snapshot_count,
                },
                {
                    "run_id": "spark-skeleton-r1-retry",
                    "source_hash": INITIAL_SOURCE_HASH,
                    "status": "skipped",
                    "gold_snapshot_id": same_source_snapshot_id,
                    "snapshot_count": same_source_snapshot_count,
                },
                {
                    "run_id": "spark-skeleton-r2",
                    "source_hash": CORRECTED_SOURCE_HASH,
                    "status": "processed",
                    "gold_snapshot_id": corrected_snapshot_id,
                    "snapshot_count": corrected_snapshot_count,
                },
            ],
            "partition_overwrite_assertions": {
                "target_partition_row_count": len(d_rows),
                "corrected_row_count": len(corrected_rows),
                "target_partition_rows": d_rows,
                "other_partition_rows": d2_rows,
                "other_partition_expected_rows": _other_date_gold_rows(),
                "snapshot_increment": corrected_snapshot_count - initial_snapshot_count,
                "same_source_created_snapshot": same_source_snapshot_count
                != initial_snapshot_count,
            },
            "claim_boundary": {
                "supports": [
                    "local Spark/Iceberg table creation",
                    "business_date partition overwrite",
                    "snapshot id evidence",
                    "run_id to snapshot_id mapping",
                    "same source_hash rerun without a new snapshot",
                ],
                "does_not_support": [
                    "full lakehouse",
                    "production Spark pipeline",
                    "full medallion Spark rewrite",
                    "Iceberg rollback system",
                    "concurrent writer handling",
                    "production Airflow scheduler/worker orchestration of Spark",
                    "Spark-based quality suite",
                ],
            },
        }

        _write_json(output / "run_snapshot_map.json", evidence)
        _write_json(output / "current_gold.json", {"rows": current_gold_rows})
        _write_json(
            output / "snapshot_comparison.json",
            {
                "initial_snapshot_id": initial_snapshot_id,
                "corrected_snapshot_id": corrected_snapshot_id,
                "initial_snapshot_count": initial_snapshot_count,
                "corrected_snapshot_count": corrected_snapshot_count,
            },
        )
        return evidence
    finally:
        spark.stop()


def _create_namespace(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")


def _create_gold_table(spark) -> None:
    _create_namespace(spark)
    spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (
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


def _gold_dataframe(spark, rows: list[dict[str, Any]]):
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


def _initial_gold_rows() -> list[dict[str, Any]]:
    return _gold_rows_for_source(_initial_source_rows(), BUSINESS_DATE, INITIAL_SOURCE_HASH) + _other_date_gold_rows()


def _corrected_gold_rows() -> list[dict[str, Any]]:
    return _gold_rows_for_source(
        _corrected_source_rows(),
        BUSINESS_DATE,
        CORRECTED_SOURCE_HASH,
    )


def _other_date_gold_rows() -> list[dict[str, Any]]:
    return _gold_rows_for_source(
        _other_date_source_rows(),
        OTHER_BUSINESS_DATE,
        INITIAL_SOURCE_HASH,
    )


def _gold_rows_for_source(
    source_rows: list[dict[str, str]],
    business_date: str,
    source_hash: str,
) -> list[dict[str, Any]]:
    silver_rows = transform_silver(source_rows, business_date, source_hash)
    return transform_gold(silver_rows, business_date)


def _initial_source_rows() -> list[dict[str, str]]:
    return [
        _source_row("2026-06-29T08:00:00Z", "wo-1001", "mc-101", "100", "2", "800", BUSINESS_DATE),
        _source_row("2026-06-29T09:00:00Z", "wo-1002", "mc-102", "20", "1", "900", BUSINESS_DATE),
    ]


def _corrected_source_rows() -> list[dict[str, str]]:
    return [
        _source_row("2026-06-29T08:00:00Z", "wo-2001", "mc-201", "150", "6", "850", BUSINESS_DATE),
    ]


def _other_date_source_rows() -> list[dict[str, str]]:
    return [
        _source_row("2026-06-30T08:00:00Z", "wo-3001", "mc-301", "50", "1", "700", OTHER_BUSINESS_DATE),
    ]


def _source_row(
    event_time: str,
    work_order_id: str,
    machine_id: str,
    units_produced: str,
    defect_count: str,
    cycle_time_ms: str,
    business_date: str,
) -> dict[str, str]:
    return {
        "event_time": event_time,
        "plant_id": "plant-a",
        "line_id": "line-1",
        "work_order_id": work_order_id,
        "machine_id": machine_id,
        "product_code": "gearbox-a",
        "operation": "assembly",
        "units_produced": units_produced,
        "defect_count": defect_count,
        "cycle_time_ms": cycle_time_ms,
        "business_date": business_date,
    }


def _current_snapshot_id(spark, table_name: str) -> int:
    rows = spark.sql(
        f"SELECT snapshot_id FROM {table_name}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    if not rows:
        raise RuntimeError(f"no snapshots found for {table_name}")
    return int(rows[0]["snapshot_id"])


def _snapshot_rows(spark, table_name: str) -> list[dict[str, Any]]:
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


def _current_gold_rows(spark) -> list[dict[str, Any]]:
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
        FROM {TABLE_NAME}
        ORDER BY business_date, plant_id, line_id, product_code
        """
    ).collect()
    return [_normalize_row(row.asDict()) for row in rows]


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the Spark/Iceberg single-gold-table walking skeleton."
    )
    parser.add_argument("--warehouse", default=DEFAULT_WAREHOUSE)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove the warehouse and output directory before running.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.clean:
        shutil.rmtree(args.warehouse, ignore_errors=True)
        shutil.rmtree(args.output_dir, ignore_errors=True)
    evidence = run_spark_iceberg_skeleton(
        warehouse_path=args.warehouse,
        output_dir=args.output_dir,
    )
    print(json.dumps(evidence, indent=2))


if __name__ == "__main__":
    main()
