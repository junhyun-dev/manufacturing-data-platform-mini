"""S9: recovery-gated Spark/Iceberg publish.

This slice adds **no** new transform, quality, adapter, Kafka, or Iceberg behaviour. It composes
two already verified contracts and refuses to let the second one start until the first one holds:

```text
sealed edge session (S8)
-> shared require_recovery_ready gate
-> existing K1.5 adapt_landing_to_batch (deterministic canonical CSV + source_hash)
-> exact sealed-event-set == canonical-adapter-event-set check
-> existing S7 run_spark_machine_event_batch (Spark silver/gold + quality gate + Iceberg)
-> one S9 evidence document binding the whole identity chain
```

Three boundaries are deliberate:

* **Membership is not sufficient.** S8 coverage proves every sealed event arrived centrally, but
  the deterministic adapter selects *every* accepted event for the requested date. An extra
  same-date accepted event would silently widen the published batch beyond the recovered session,
  so the canonical event-id set must equal the sealed set exactly before Spark starts.
* **Imports stay lazy until the gate passes**, so an incomplete recovery fails before the Spark and
  batch dependencies are needed at all.
* **An attempt is not the run that created the snapshot.** S7 mints a fresh `run_id` on every
  invocation, including one it then decides to skip, and it does not expose the `run_id` of the
  attempt that originally committed the snapshot. So this module records
  `spark_attempt_run_id` plus an explicit `snapshot_relation`, and never pairs a skipped attempt's
  id with the reused snapshot as if it had produced it.

Synthetic, local, bounded: one sealed session, one machine/date/topic/partition, one local Iceberg
gold table. Not a streaming sink, not a medallion platform, not production operation.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from manufacturing_data_platform.edge_recovery import (
    CLAIM_BOUNDARY as EDGE_CLAIM_BOUNDARY,
)
from manufacturing_data_platform.edge_recovery import require_recovery_ready


CLAIM_BOUNDARY = {
    "supports": [
        "recovery-gated batch publish binding one sealed edge session to one Iceberg gold snapshot",
        "shared S8 readiness gate reused, not reimplemented",
        "exact sealed-event-set equality with the deterministic K1.5 canonical adapter input",
        "existing S7 Spark quality gate and Iceberg business_date overwrite reused unchanged",
        "same source retry creates no new Iceberg snapshot and performs no partition overwrite",
    ],
    "does_not_support": [
        "continuous streaming, Spark Structured Streaming, or a direct Kafka-to-Iceberg sink",
        "full Spark/Iceberg medallion platform or cluster Spark",
        "multi machine/session/partition or rebalance correctness",
        "concurrent Iceberg writers, distributed atomicity, or end-to-end exactly-once",
        "production Airflow/HA operation",
        "real edge hardware or OPC UA / MQTT / ROS 2 / DDS integration",
    ],
}


class RecoveredPublishError(RuntimeError):
    """Base error for the S9 recovery-gated publish."""


class SessionInputMismatchError(RecoveredPublishError):
    """The canonical adapter input does not represent exactly the sealed session."""


class UnexpectedSparkStatusError(RecoveredPublishError):
    """S7 returned a status this slice has no evidence statement for."""


# Every S7 status maps to exactly one snapshot relation. There is no default branch: an unknown
# status must fail loudly rather than be silently classified as a reuse of a snapshot that may not
# exist at all.
SNAPSHOT_RELATION_BY_STATUS = {
    "published": "created_by_current_attempt",
    "skipped": "reused_from_prior_attempt",
    "quality_failed": "no_snapshot",
}


def assert_exact_session_input(
    *, sealed_event_ids: list[str], adapter_event_ids: list[str], sealed_count: int
) -> None:
    """Membership alone is not enough: the published batch must BE the recovered session.

    Raises before Spark starts, so no Iceberg table, snapshot, or success state is touched.
    """
    sealed = set(sealed_event_ids)
    selected = set(adapter_event_ids)
    if len(adapter_event_ids) != sealed_count or selected != sealed:
        extra = sorted(selected - sealed)
        missing = sorted(sealed - selected)
        raise SessionInputMismatchError(
            "canonical adapter input does not equal the sealed session: "
            f"selected_count={len(adapter_event_ids)} sealed_count={sealed_count} "
            f"extra_event_ids={extra} missing_event_ids={missing}; "
            "Spark/Iceberg publication is blocked"
        )


def build_evidence(
    *,
    coverage: dict[str, Any],
    adapter: Any,
    spark: dict[str, Any],
) -> dict[str, Any]:
    """Bind the whole identity chain without conflating any of its spaces.

    The snapshot relation is exhaustive over the S7 statuses:

    ```text
    published      this attempt created the recorded snapshot
    skipped        S7 found the same source_hash already published and reused that snapshot;
                   the producer attempt is unknown because S7 does not expose it
    quality_failed nothing was committed, so there is no snapshot to have created OR reused
    ```

    A status outside that set raises rather than defaulting to "reused", which would assert the
    reuse of a snapshot that may not exist.
    """
    status = spark["status"]
    if status not in SNAPSHOT_RELATION_BY_STATUS:
        raise UnexpectedSparkStatusError(
            f"S7 returned status={status!r}, which has no S9 evidence statement; "
            f"known statuses are {sorted(SNAPSHOT_RELATION_BY_STATUS)}"
        )
    snapshot_relation = SNAPSHOT_RELATION_BY_STATUS[status]
    created_here = snapshot_relation == "created_by_current_attempt"
    return {
        "slice": "s9-recovery-gated-spark-iceberg-publish",
        "business_date": spark["business_date"],
        "status": spark["status"],
        "edge": {
            "edge_source_id": coverage["edge_source_id"],
            "boot_session_id": coverage["boot_session_id"],
            "expected_last_sequence": coverage["expected_last_sequence"],
            "sequence_range": [1, coverage["expected_last_sequence"]],
            "recovered_sequences": coverage["recovered_sequences"],
            "event_ids": coverage["sealed_event_ids"],
            "recovery_complete": coverage["recovery_complete"],
        },
        "kafka": {"recovered_coordinates": coverage["recovered_coordinates"]},
        "adapter": {
            "status": adapter.status,
            "source_hash": adapter.source_hash,
            "selected_event_count": adapter.selected_event_count,
            "csv_path": str(adapter.csv_path),
            "selected_event_ids": adapter.provenance["selected_event_ids"],
        },
        "spark": {
            # Kept under its original name for compatibility with the S7 evidence shape; this is
            # the id of the CURRENT attempt, which is freshly minted even when S7 then skips.
            "run_id": spark["run_id"],
            "attempt_run_id": spark["run_id"],
            "source_hash": spark["source_hash"],
            "quality_passed": spark["quality"]["passed"],
            "quality_checks": spark["quality"]["checks"],
            "row_counts": spark["row_counts"],
        },
        "iceberg": {
            "table": spark["table"],
            "status": spark["status"],
            "gold_snapshot_id": spark.get("gold_snapshot_id"),
            "snapshot_count": spark.get("snapshot_count"),
            "target_partition_row_count": spark.get("target_partition_row_count"),
            "snapshot_relation": snapshot_relation,
            "snapshot_created_by_current_attempt": created_here,
            "producer_attempt_run_id": spark["run_id"] if created_here else None,
        },
        "identity_chain": {
            "edge_sequence": coverage["recovered_sequences"],
            "event_id": coverage["sealed_event_ids"],
            "kafka_coordinate": [
                {k: c[k] for k in ("kafka_topic", "kafka_partition", "kafka_offset")}
                for c in coverage["recovered_coordinates"]
            ],
            "adapter_source_hash": adapter.source_hash,
            "spark_attempt_run_id": spark["run_id"],
            "iceberg_snapshot_id": spark.get("gold_snapshot_id"),
            "snapshot_created_by_current_attempt": created_here,
            "snapshot_relation": snapshot_relation,
            "note": (
                "edge sequence, business event_id, Kafka coordinate, batch source_hash, "
                "Spark attempt run_id and Iceberg snapshot_id are separate identity spaces. "
                "spark_attempt_run_id identifies THIS attempt. Read snapshot_relation before "
                "pairing it with iceberg_snapshot_id: created_by_current_attempt means this "
                "attempt committed the snapshot; reused_from_prior_attempt means an earlier "
                "attempt did, and S7 does not expose which, so no run -> snapshot causal relation "
                "may be read from the pair; no_snapshot means nothing was committed and "
                "iceberg_snapshot_id is null"
            ),
        },
        "claim_boundary": {
            "s9": CLAIM_BOUNDARY,
            "inherited_edge_recovery": EDGE_CLAIM_BOUNDARY,
        },
    }


def run_recovered_telemetry_publish(
    *,
    spool_root: str | Path,
    edge_source_id: str,
    boot_session_id: str,
    landing_dir: str | Path,
    business_date: str,
    adapter_output_dir: str | Path,
    warehouse_path: str | Path,
    evidence_output_dir: str | Path,
    table_name: str | None = None,
    evidence_file: str | Path | None = None,
) -> dict[str, Any]:
    """Publish a recovered session to Iceberg only when every gate holds."""
    # 1. Shared S8 gate. Raises before anything downstream exists.
    _session, coverage = require_recovery_ready(
        spool_root=spool_root,
        edge_source_id=edge_source_id,
        boot_session_id=boot_session_id,
        landing_dir=landing_dir,
        business_date=business_date,
    )

    # Imported only after the recovery gate passes, so an incomplete recovery never needs the
    # batch/Spark dependency stack.
    from manufacturing_data_platform.kafka_ingestion.batch_adapter import (
        adapt_landing_to_batch,
    )
    from manufacturing_data_platform.pipeline.spark_machine_event_batch import (
        TABLE_NAME,
        run_spark_machine_event_batch,
    )

    # 2. Existing deterministic adapter — identity is its SHA-256, not a new hash.
    adapter = adapt_landing_to_batch(
        landing_dir=landing_dir,
        business_date=business_date,
        adapter_output_dir=adapter_output_dir,
    )

    # 3. Exact-session input check. Adapter staging may exist but is not trusted output.
    assert_exact_session_input(
        sealed_event_ids=list(coverage["sealed_event_ids"]),
        adapter_event_ids=list(adapter.provenance["selected_event_ids"]),
        sealed_count=coverage["expected_sequence_count"],
    )

    # 4. Existing S7 path: Spark silver/gold, quality gate, Iceberg publish.
    spark = run_spark_machine_event_batch(
        csv_path=adapter.csv_path,
        source_hash=adapter.source_hash,
        business_date=business_date,
        warehouse_path=warehouse_path,
        evidence_output_dir=evidence_output_dir,
        table_name=table_name or TABLE_NAME,
    )

    evidence = build_evidence(coverage=coverage, adapter=adapter, spark=spark)
    target = Path(evidence_file) if evidence_file else Path(evidence_output_dir) / "s9_recovered_telemetry_publish.json"
    _write_json(target, evidence)
    evidence["evidence_path"] = str(target)
    return evidence


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


# --------------------------------------------------------------------------- #
# Bounded CLI
# --------------------------------------------------------------------------- #
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Publish one recovered sealed edge session to the local Iceberg gold table, "
            "gated on complete recovery and exact session input."
        )
    )
    parser.add_argument("--spool-root", required=True)
    parser.add_argument("--edge-source-id", required=True)
    parser.add_argument("--boot-session-id", required=True)
    parser.add_argument("--landing-dir", required=True)
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--adapter-output-dir", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--table", default=None)
    parser.add_argument("--evidence-file", default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    from manufacturing_data_platform.edge_recovery import (
        EdgeRecoveryError,
        RecoveryIncompleteError,
    )

    args = parse_args(argv)
    try:
        evidence = run_recovered_telemetry_publish(
            spool_root=args.spool_root,
            edge_source_id=args.edge_source_id,
            boot_session_id=args.boot_session_id,
            landing_dir=args.landing_dir,
            business_date=args.business_date,
            adapter_output_dir=args.adapter_output_dir,
            warehouse_path=args.warehouse,
            evidence_output_dir=args.output_dir,
            table_name=args.table,
            evidence_file=args.evidence_file,
        )
    except (RecoveryIncompleteError, EdgeRecoveryError, SessionInputMismatchError) as exc:
        # Gate refusal: no Spark started, no Iceberg table/snapshot/success state touched.
        print(f"publish blocked: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    printable = dict(evidence)
    spark = printable.get("spark", {})
    if isinstance(spark, dict):
        printable["spark"] = {k: v for k, v in spark.items() if k != "quality_checks"}
    print(json.dumps(printable, indent=2, sort_keys=True))

    # A quality-failed publish must fail the orchestration task, not exit 0.
    if evidence.get("status") == "quality_failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
