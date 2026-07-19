#!/usr/bin/env python3
"""Bounded S7 verification: K1.5 canonical landing -> Spark batch -> local Iceberg.

Builds real in-process K1 landings (no broker) with the real landing writer, runs each
through the real adapter, then asserts the S7 state transitions against one persistent
local Iceberg gold table:

    quality-passed source A -> published
    same source A           -> skipped / snapshot unchanged
    changed source B         -> published / target partition replaced / snapshot +1
    other business_date      -> unchanged
    gold aggregation plan    -> Exchange observed
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

from manufacturing_data_platform.kafka_ingestion.batch_adapter import adapt_landing_to_batch
from manufacturing_data_platform.kafka_ingestion.contracts import (
    sample_machine_event,
    serialize_machine_event,
)
from manufacturing_data_platform.kafka_ingestion.landing import KafkaRecord, land_records
from manufacturing_data_platform.pipeline.spark_machine_event_batch import (
    run_spark_machine_event_batch,
)


TOPIC = "manufacturing.machine-events.v1"
BUSINESS_DATE = "2026-06-29"
OTHER_DATE = "2026-06-30"


def _record(offset: int, event: dict) -> KafkaRecord:
    return KafkaRecord(
        topic=TOPIC,
        partition=0,
        offset=offset,
        key=event["machine_id"],
        value=serialize_machine_event(event),
        timestamp_ms=1_783_000_000_000 + offset,
    )


def _event(index: int, *, business_date: str | None = None, **overrides) -> dict:
    event = sample_machine_event(index)
    if business_date is not None:
        event["business_date"] = business_date
    event.update(overrides)
    return event


def _land_and_adapt(root: Path, name: str, pairs, business_date: str):
    landing = root / f"raw_{name}"
    land_records([_record(offset, event) for offset, event in pairs], landing)
    return adapt_landing_to_batch(
        landing_dir=landing,
        business_date=business_date,
        adapter_output_dir=root / f"adapter_{name}",
    )


def verify(*, output_dir: Path, clean: bool) -> dict:
    warehouse = output_dir / "warehouse"
    evidence_dir = output_dir / "evidence"
    landings = output_dir / "landings"
    if clean:
        for path in (warehouse, evidence_dir, landings):
            shutil.rmtree(path, ignore_errors=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    a = _land_and_adapt(landings, "srcA", [(0, _event(1)), (1, _event(2))], BUSINESS_DATE)
    b = _land_and_adapt(
        landings,
        "srcB",
        [(0, _event(4, work_order_id="wo-9001", machine_id="mc-901", units_produced=200, defect_count=5))],
        BUSINESS_DATE,
    )
    d2 = _land_and_adapt(
        landings, "d2", [(0, _event(3, business_date=OTHER_DATE))], OTHER_DATE
    )

    def run(adapter, business_date):
        return run_spark_machine_event_batch(
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

    d1_rows = [r for r in r_b["current_table_rows"] if r["business_date"] == BUSINESS_DATE]
    d2_rows = [r for r in r_b["current_table_rows"] if r["business_date"] == OTHER_DATE]

    checks = []

    def check(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "status": "pass" if passed else "fail", "detail": detail})

    check("source_a_published", r_a1["status"] == "published", f"status={r_a1['status']}")
    check("source_a_quality_passed", r_a1["quality"]["passed"] is True, "quality passed before publish")
    check(
        "same_source_a_skipped_no_new_snapshot",
        r_a2["status"] == "skipped"
        and r_a2["gold_snapshot_id"] == r_a1["gold_snapshot_id"]
        and r_a2["snapshot_count"] == r_a1["snapshot_count"],
        f"retry status={r_a2['status']} snapshot={r_a2['gold_snapshot_id']}",
    )
    check(
        "correction_source_b_new_snapshot",
        r_b["status"] == "published"
        and r_b["snapshot_count"] == r_a1["snapshot_count"] + 1
        and r_b["gold_snapshot_id"] != r_a1["gold_snapshot_id"],
        f"snapshot_count {r_a1['snapshot_count']} -> {r_b['snapshot_count']}",
    )
    check(
        "target_partition_replaced",
        sum(r["units_produced"] for r in d1_rows) == 200,
        f"D1 units_produced={sum(r['units_produced'] for r in d1_rows)}",
    )
    check(
        "other_date_preserved",
        d2_rows == r_d2["target_partition_rows"],
        f"D2 rows stable={d2_rows == r_d2['target_partition_rows']}",
    )
    check(
        "aggregation_exchange_observed",
        r_b["physical_plan"]["exchange_observed"] is True,
        "groupBy shuffle Exchange present in executed plan",
    )
    check(
        "adapter_identity_is_batch_source_identity",
        r_a1["source_hash"] == a.source_hash,
        "Spark batch source_hash == adapter source_hash",
    )

    passed = all(item["status"] == "pass" for item in checks)
    evidence = {
        "scope": "bounded local Spark machine-event batch over K1.5 canonical landing",
        "status": "passed" if passed else "failed",
        "table": r_a1["table"],
        "iceberg_runtime_coordinate": r_a1["iceberg_runtime_coordinate"],
        "checks": checks,
        "transitions": {
            "d2": _summary(r_d2),
            "source_a": _summary(r_a1),
            "source_a_retry": _summary(r_a2),
            "correction_b": _summary(r_b),
        },
        "physical_plan_exchange_observed": r_b["physical_plan"]["exchange_observed"],
        "row_counts": {
            "source_a": r_a1["row_counts"],
            "correction_b": r_b["row_counts"],
        },
        "claim_boundary": r_a1["claim_boundary"],
    }
    (output_dir / "spark_machine_event_batch_verification.json").write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return evidence


def _summary(result: dict) -> dict:
    return {
        "status": result["status"],
        "business_date": result["business_date"],
        "source_hash": result["source_hash"],
        "run_id": result["run_id"],
        "gold_snapshot_id": result["gold_snapshot_id"],
        "snapshot_count": result.get("snapshot_count"),
        "quality_passed": result["quality"]["passed"],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify the bounded S7 Spark machine-event batch.")
    parser.add_argument("--output-dir", default="/tmp/manufacturing-mini-spark-machine-event-batch")
    parser.add_argument("--clean", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    evidence = verify(output_dir=Path(args.output_dir), clean=args.clean)
    print(json.dumps(evidence["checks"], indent=2))
    if evidence["status"] != "passed":
        print(f"S7 verification FAILED; evidence: {args.output_dir}", file=sys.stderr)
        raise SystemExit(1)
    print(f"S7 Spark machine-event batch verification passed; evidence: {args.output_dir}")


if __name__ == "__main__":
    main()
