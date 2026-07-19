"""S7: local bounded Spark batch over a K1.5 canonical Kafka landing.

The input contract is the deterministic canonical CSV and ``source_hash`` produced by
``kafka_ingestion.batch_adapter.adapt_landing_to_batch``. Spark re-expresses the *existing*
Python ``transform_silver``/``transform_gold`` semantics with DataFrame built-ins (no UDFs),
runs the existing quality suite on the Spark-materialized result, and publishes only a
quality-passed gold to one local Iceberg table with ``overwritePartitions()``.

Engine-parity choices that matter:

* Dedup uses the raw silver natural key ``(work_order_id, machine_id, event_time)`` and keeps
  the first row in Kafka-coordinate order, exactly as the Python loop keeps the first CSV row.
* Gold rounding uses Spark ``bround`` (round-half-to-even), matching Python 3 ``round``.
* Quality is the same ``build_quality_checks`` suite applied to the Spark silver/gold plus the
  input rows, so "Spark quality" cannot silently diverge from the batch spine's quality.

This module reuses the Iceberg/Spark helpers from ``spark_iceberg_skeleton`` and
``publish_gold_to_iceberg`` unchanged; it does not add streaming, a direct Kafka sink, or a
full medallion rewrite.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import date
from pathlib import Path
from typing import Any

from manufacturing_data_platform.kafka_ingestion.batch_adapter import (
    CANONICAL_COLUMNS,
    adapt_landing_to_batch,
)
from manufacturing_data_platform.pipeline.lakehouse import (
    DATASET_ID,
    build_quality_checks,
    build_run_id,
    read_json_file,
    write_json_file,
)
from manufacturing_data_platform.pipeline.publish_gold_to_iceberg import (
    TABLE_NAME,
    create_gold_table_if_missing,
    current_gold_rows,
    current_snapshot_id,
    gold_dataframe,
    snapshot_rows,
    validate_table_name,
)
from manufacturing_data_platform.pipeline.spark_iceberg_skeleton import (
    ICEBERG_RUNTIME_COORDINATE,
    build_spark_session,
)


# Output shape of the existing Python silver transform, in column order.
SILVER_COLUMNS = (
    "event_time",
    "business_date",
    "plant_id",
    "line_id",
    "work_order_id",
    "machine_id",
    "product_code",
    "operation",
    "units_produced",
    "defect_count",
    "cycle_time_ms",
    "source_hash",
)
_STRING_NORMALIZED = ("plant_id", "line_id", "work_order_id", "machine_id", "product_code", "operation")
_SILVER_INT_COLUMNS = ("units_produced", "defect_count", "cycle_time_ms")
GOLD_COLUMNS = (
    "business_date",
    "plant_id",
    "line_id",
    "product_code",
    "units_produced",
    "defect_count",
    "defect_rate",
    "avg_cycle_time_ms",
    "closing_status",
)

PLAN_EXCERPT_LIMIT = 4000

CLAIM_BOUNDARY = {
    "supports": [
        "local bounded Spark batch over a provenance-checked Kafka landing adapter",
        "engine parity with the existing Python silver/gold grain and reconciliation",
        "Spark quality gate before any Iceberg commit",
        "business_date partition overwrite with other-date preservation",
        "same-source no-op and changed-source correction snapshot evidence",
    ],
    "does_not_support": [
        "production or cluster Spark",
        "large-scale performance or throughput claims",
        "full Spark/Iceberg medallion pipeline",
        "continuous Kafka/Spark streaming or Structured Streaming",
        "direct Kafka-to-Iceberg sink",
        "end-to-end exactly-once or distributed transaction",
        "concurrent Iceberg writer correctness",
        "distributed Spark-native quality evaluation (quality is collected to the driver and reuses the Python suite)",
    ],
}


class SparkBatchError(RuntimeError):
    """Base error for the S7 Spark machine-event batch."""


class BusinessDateMismatchError(SparkBatchError):
    """The canonical CSV contains rows outside the requested business_date."""


# --------------------------------------------------------------------------- #
# Pure helpers (testable without Spark)
# --------------------------------------------------------------------------- #
def validate_business_date(business_date: str) -> None:
    if not isinstance(business_date, str) or not business_date.strip():
        raise ValueError("business_date must be a non-empty ISO date string")
    try:
        date.fromisoformat(business_date)
    except ValueError as exc:
        raise ValueError(f"business_date must be an ISO date: {business_date!r}") from exc


def read_canonical_rows(csv_path: str | Path) -> list[dict[str, str]]:
    """Read the adapter canonical CSV as the batch input rows."""
    path = Path(csv_path)
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = list(reader.fieldnames or [])
        rows = list(reader)
    missing = [c for c in CANONICAL_COLUMNS if c not in columns]
    if missing:
        raise ValueError(f"canonical CSV missing columns: {', '.join(missing)}")
    return rows


def assert_single_business_date(source_rows: list[dict[str, str]], business_date: str) -> None:
    """Fail before Spark/publish if the input carries any other date."""
    off = sorted({r["business_date"] for r in source_rows if r.get("business_date") != business_date})
    if off:
        raise BusinessDateMismatchError(
            f"requested business_date={business_date} but canonical CSV also holds {off}"
        )


def evaluate_quality(
    source_rows: list[dict[str, Any]],
    silver_rows: list[dict[str, Any]],
    gold_rows: list[dict[str, Any]],
    business_date: str,
) -> tuple[list[dict[str, Any]], bool]:
    """Run the existing quality suite on the Spark-materialized result."""
    checks = build_quality_checks(source_rows, silver_rows, gold_rows, business_date)
    passed = not any(check["status"] == "fail" for check in checks)
    return checks, passed


def quality_summary(checks: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [{"name": c["name"], "status": c["status"]} for c in checks]


def decide_publish_action(
    previous_state: dict[str, Any] | None,
    source_hash: str,
    existing_snapshot_ids: set[int] | frozenset[int] | list[int],
) -> str:
    """'skip' only when the same source already published AND its recorded snapshot
    still exists in the current table's snapshot history.

    Checking history membership (not "is current snapshot") keeps the skip valid after a
    later commit for another date, but forces a rewrite when the warehouse was emptied or
    recreated while the evidence state persisted (H2).
    """
    if (
        previous_state is not None
        and previous_state.get("source_hash") == source_hash
        and isinstance(previous_state.get("snapshot_id"), int)
        and previous_state["snapshot_id"] in set(existing_snapshot_ids)
    ):
        return "skip"
    return "write"


def _publish_state_path(output_dir: Path, table_name: str, business_date: str) -> Path:
    safe_table = table_name.replace(".", "_")
    return output_dir / "_state" / DATASET_ID / f"{safe_table}__business_date={business_date}.json"


# --------------------------------------------------------------------------- #
# Spark transforms (parity with lakehouse.transform_silver / transform_gold)
# --------------------------------------------------------------------------- #
def _canonical_schema():
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType([StructField(column, StringType(), True) for column in CANONICAL_COLUMNS])


def read_canonical_dataframe(spark, csv_path: str | Path):
    return (
        spark.read.option("header", True)
        .schema(_canonical_schema())
        .csv(str(Path(csv_path)))
    )


def spark_transform_silver(df, business_date: str, source_hash: str):
    """Filter the date, dedup the raw natural key (keep first by Kafka coordinate),
    then normalize and cast — the same contract as ``transform_silver``."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    filtered = df.filter(F.col("business_date") == F.lit(business_date))

    # Keep the first row per natural key in Kafka-coordinate order == the first CSV
    # row the Python loop would keep, since the adapter writes rows in that order.
    dedup_order = Window.partitionBy("work_order_id", "machine_id", "event_time").orderBy(
        F.col("kafka_topic"),
        F.col("kafka_partition").cast("long"),
        F.col("kafka_offset").cast("long"),
    )
    deduped = (
        filtered.withColumn("_rn", F.row_number().over(dedup_order))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    projections = [F.col("event_time"), F.col("business_date")]
    projections += [F.lower(F.trim(F.col(name))).alias(name) for name in _STRING_NORMALIZED]
    projections += [F.col(name).cast("long").alias(name) for name in _SILVER_INT_COLUMNS]
    projections.append(F.lit(source_hash).alias("source_hash"))
    return deduped.select(*projections)


def _round_like_python(col, scale: int):
    """Round a double the way Python 3 ``round(x, scale)`` does.

    Spark ``bround``/``round``/decimal-cast diverge from Python ``round`` at valid boundary
    doubles: e.g. ``32107/40`` is stored as ``802.6749999…`` so Python gives ``802.67`` while
    ``bround`` gives ``802.68`` (bounded audit: 204 mismatches / 40,400 integer-ratio cases at
    scale 2). ``format_number`` matched Python ``round`` in the same 40,400-case
    integer-ratio probe (0 mismatches; ``0.125 -> 0.12`` half-even and
    ``802.675 -> 802.67`` both correct). It is a built-in expression (no UDF); this is bounded
    evidence for the metric domain, not a universal float-identity claim.
    """
    from pyspark.sql import functions as F

    return F.regexp_replace(F.format_number(col, scale), ",", "").cast("double")


def spark_transform_gold(silver_df):
    """Aggregate to the existing gold grain with Python-round parity."""
    from pyspark.sql import functions as F

    grouped = silver_df.groupBy(
        "business_date", "plant_id", "line_id", "product_code"
    ).agg(
        F.sum("units_produced").alias("units_produced"),
        F.sum("defect_count").alias("defect_count"),
        F.sum("cycle_time_ms").alias("_cycle_sum"),
        F.count(F.lit(1)).alias("_events"),
    )

    defect_rate = F.when(
        F.col("units_produced") > 0,
        _round_like_python(F.col("defect_count") / F.col("units_produced"), 6),
    ).otherwise(F.lit(0.0))
    avg_cycle = F.when(
        F.col("_events") > 0,
        _round_like_python(F.col("_cycle_sum") / F.col("_events"), 2),
    ).otherwise(F.lit(0.0))

    return grouped.select(
        F.col("business_date"),
        F.col("plant_id"),
        F.col("line_id"),
        F.col("product_code"),
        F.col("units_produced").cast("long"),
        F.col("defect_count").cast("long"),
        defect_rate.cast("double").alias("defect_rate"),
        avg_cycle.cast("double").alias("avg_cycle_time_ms"),
        F.lit("provisional").alias("closing_status"),
    ).orderBy("business_date", "plant_id", "line_id", "product_code")


def collect_silver_rows(silver_df) -> list[dict[str, Any]]:
    rows = [row.asDict() for row in silver_df.collect()]
    result = []
    for row in rows:
        item = {column: row[column] for column in SILVER_COLUMNS}
        for column in _SILVER_INT_COLUMNS:
            item[column] = int(item[column])
        result.append(item)
    result.sort(key=lambda r: (r["work_order_id"], r["machine_id"], r["event_time"]))
    return result


def collect_gold_rows(gold_df) -> list[dict[str, Any]]:
    rows = [row.asDict() for row in gold_df.collect()]
    result = []
    for row in rows:
        result.append(
            {
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
        )
    result.sort(key=lambda r: (r["business_date"], r["plant_id"], r["line_id"], r["product_code"]))
    return result


def plan_evidence(gold_df) -> tuple[str, bool]:
    """Executed physical plan excerpt + whether a shuffle Exchange is present.

    Learning evidence for the groupBy shuffle, not a performance claim.
    """
    plan = gold_df._jdf.queryExecution().executedPlan().toString()
    return plan[:PLAN_EXCERPT_LIMIT], ("Exchange" in plan)


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def run_spark_machine_event_batch(
    *,
    csv_path: str | Path,
    source_hash: str,
    business_date: str,
    warehouse_path: str | Path,
    evidence_output_dir: str | Path,
    table_name: str = TABLE_NAME,
    run_id: str | None = None,
    extra_evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run silver/gold in Spark, gate on quality, and publish only if it passes.

    ``extra_evidence`` is merged into both the persisted and returned evidence so callers
    (e.g. the bridge) record the same identity in the file and the return value (M2).
    """
    validate_table_name(table_name)
    validate_business_date(business_date)
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"canonical CSV does not exist: {csv_path}")

    output = Path(evidence_output_dir)
    output.mkdir(parents=True, exist_ok=True)

    source_rows = read_canonical_rows(csv_path)
    assert_single_business_date(source_rows, business_date)
    run_id = run_id or build_run_id(business_date)

    spark = build_spark_session(warehouse_path, app_name="manufacturing-mini-spark-machine-event-batch")
    try:
        raw_df = read_canonical_dataframe(spark, csv_path)
        silver_df = spark_transform_silver(raw_df, business_date, source_hash)
        gold_df = spark_transform_gold(silver_df)

        silver_rows = collect_silver_rows(silver_df)
        gold_rows = collect_gold_rows(gold_df)
        plan_excerpt, exchange_observed = plan_evidence(gold_df)
        checks, passed = evaluate_quality(source_rows, silver_rows, gold_rows, business_date)

        base_evidence = {
            "slice": "s7-spark-machine-event-batch",
            "table": table_name,
            "business_date": business_date,
            "run_id": run_id,
            "source_hash": source_hash,
            "iceberg_runtime_coordinate": ICEBERG_RUNTIME_COORDINATE,
            "row_counts": {
                "input": len(source_rows),
                "silver": len(silver_rows),
                "gold": len(gold_rows),
            },
            "dedup_count": len(source_rows) - len(silver_rows),
            "quality": {"passed": passed, "checks": quality_summary(checks)},
            "physical_plan": {
                "exchange_observed": exchange_observed,
                "executed_plan_excerpt": plan_excerpt,
            },
            "claim_boundary": CLAIM_BOUNDARY,
            **(extra_evidence or {}),
        }

        if not passed:
            # Quality gate: never touch the Iceberg table or the success state.
            evidence = {
                **base_evidence,
                "status": "quality_failed",
                "published": False,
                "gold_snapshot_id": None,
                "quality_fail_detail": [c for c in checks if c["status"] == "fail"],
            }
            _write_evidence(output, evidence)
            return evidence

        create_gold_table_if_missing(spark, table_name)
        # Skip only if the recorded snapshot still lives in the current table history.
        existing_snapshot_ids = {s["snapshot_id"] for s in snapshot_rows(spark, table_name)}
        state_path = _publish_state_path(output, table_name, business_date)
        previous_state = read_json_file(state_path)
        action = decide_publish_action(previous_state, source_hash, existing_snapshot_ids)

        if action == "skip":
            snapshot_id = previous_state["snapshot_id"]
            status = "skipped"
        else:
            gold_dataframe(spark, gold_rows).writeTo(table_name).overwritePartitions()
            snapshot_id = current_snapshot_id(spark, table_name)
            status = "published"
            write_json_file(
                state_path,
                {
                    "table": table_name,
                    "business_date": business_date,
                    "source_hash": source_hash,
                    "run_id": run_id,
                    "snapshot_id": snapshot_id,
                },
            )

        snapshots = snapshot_rows(spark, table_name)
        current_rows = current_gold_rows(spark, table_name)
        target_rows = [r for r in current_rows if r["business_date"] == business_date]

        evidence = {
            **base_evidence,
            "status": status,
            "published": action == "write",
            "gold_snapshot_id": snapshot_id,
            "snapshot_count": len(snapshots),
            "snapshots": snapshots,
            "target_partition_row_count": len(target_rows),
            "target_partition_rows": target_rows,
            "current_table_rows": current_rows,
            "gold_rows": gold_rows,
        }
        _write_evidence(output, evidence)
        return evidence
    finally:
        spark.stop()


def run_bridge_spark_batch(
    *,
    landing_dir: str | Path,
    business_date: str,
    adapter_output_dir: str | Path,
    warehouse_path: str | Path,
    evidence_output_dir: str | Path,
    table_name: str = TABLE_NAME,
) -> dict[str, Any]:
    """Adapter (provenance-checked canonical CSV) -> Spark batch -> Iceberg publish."""
    adapter = adapt_landing_to_batch(
        landing_dir=landing_dir,
        business_date=business_date,
        adapter_output_dir=adapter_output_dir,
    )
    # Thread adapter identity through extra_evidence so it is *persisted*, not only
    # attached to the return value (M2).
    adapter_evidence = {
        "adapter": {
            "status": adapter.status,
            "source_hash": adapter.source_hash,
            "selected_event_count": adapter.selected_event_count,
            "csv_path": str(adapter.csv_path),
        }
    }
    return run_spark_machine_event_batch(
        csv_path=adapter.csv_path,
        source_hash=adapter.source_hash,
        business_date=business_date,
        warehouse_path=warehouse_path,
        evidence_output_dir=evidence_output_dir,
        table_name=table_name,
        extra_evidence=adapter_evidence,
    )


def _write_evidence(output_dir: Path, evidence: dict[str, Any]) -> None:
    write_json_file(output_dir / "spark_machine_event_batch.json", evidence)


# --------------------------------------------------------------------------- #
# Bounded CLI
# --------------------------------------------------------------------------- #
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the local Spark machine-event batch over a K1.5 canonical landing."
    )
    parser.add_argument("--landing-dir", required=True)
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--adapter-output-dir", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--table", default=TABLE_NAME)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    evidence = run_bridge_spark_batch(
        landing_dir=args.landing_dir,
        business_date=args.business_date,
        adapter_output_dir=args.adapter_output_dir,
        warehouse_path=args.warehouse,
        evidence_output_dir=args.output_dir,
        table_name=args.table,
    )
    printable = {key: value for key, value in evidence.items() if key != "physical_plan"}
    plan = evidence.get("physical_plan")
    if plan is not None:
        printable["physical_plan"] = {"exchange_observed": plan.get("exchange_observed")}
    print(json.dumps(printable, indent=2, sort_keys=True))
    # A quality-failed run must fail the orchestration task, not exit 0 (M1).
    if evidence.get("status") == "quality_failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
