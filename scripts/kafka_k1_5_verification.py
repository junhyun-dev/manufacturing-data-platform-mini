#!/usr/bin/env python3
"""Bounded K1.5 verification against a real K1 landing.

Runs the landing-to-batch bridge twice and asserts the rerun contract:
the same accepted set reuses the adapter version and skips the pipeline
instead of doubling the trusted gold result.

Requires an existing K1 landing, normally produced by ./scripts/verify_kafka_k1.sh.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

from manufacturing_data_platform.kafka_ingestion.batch_adapter import (
    NoEligibleEventsError,
    run_bridge,
)


DEFAULT_LANDING = "/tmp/manufacturing-mini-kafka-k1-evidence/raw"
DEFAULT_OUTPUT = "/tmp/manufacturing-mini-kafka-k1-5-evidence"
DEFAULT_BUSINESS_DATE = "2026-06-29"


def verify(
    *,
    landing_dir: Path,
    business_date: str,
    output_dir: Path,
    clean: bool,
) -> dict:
    adapter_dir = output_dir / "adapter"
    lakehouse_dir = output_dir / "lakehouse"
    if clean:
        shutil.rmtree(adapter_dir, ignore_errors=True)
        shutil.rmtree(lakehouse_dir, ignore_errors=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    first = run_bridge(
        landing_dir=landing_dir,
        business_date=business_date,
        adapter_output_dir=adapter_dir,
        lakehouse_output_dir=lakehouse_dir,
        evidence_file=output_dir / "bridge_run_1.json",
    )
    second = run_bridge(
        landing_dir=landing_dir,
        business_date=business_date,
        adapter_output_dir=adapter_dir,
        lakehouse_output_dir=lakehouse_dir,
        evidence_file=output_dir / "bridge_run_2.json",
    )

    checks: list[dict] = []

    def check(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "status": "pass" if passed else "fail", "detail": detail})

    check(
        "first_run_processed",
        first["lakehouse"]["status"] == "processed",
        f"first lakehouse status={first['lakehouse']['status']}",
    )
    check(
        "first_run_quality_passed",
        first["lakehouse"]["quality_passed"] is True,
        f"quality_passed={first['lakehouse']['quality_passed']}",
    )
    check(
        "adapter_version_reused_on_rerun",
        first["adapter"]["status"] == "created" and second["adapter"]["status"] == "reused",
        f"{first['adapter']['status']} -> {second['adapter']['status']}",
    )
    check(
        "source_hash_stable",
        first["adapter"]["source_hash"] == second["adapter"]["source_hash"],
        f"source_hash={first['adapter']['source_hash']}",
    )
    check(
        "adapter_identity_is_lakehouse_source_identity",
        first["adapter"]["source_hash"] == first["lakehouse"]["source_hash"],
        "adapter SHA-256 equals the pipeline source_hash",
    )
    check(
        "rerun_skipped_same_run_id",
        second["lakehouse"]["status"] == "skipped"
        and second["lakehouse"]["run_id"] == first["lakehouse"]["run_id"],
        f"rerun status={second['lakehouse']['status']} run_id={second['lakehouse']['run_id']}",
    )
    check(
        "trusted_gold_not_doubled",
        second["lakehouse"]["gold_rows"] == first["lakehouse"]["gold_rows"],
        f"gold_rows={second['lakehouse']['gold_rows']}",
    )
    run_dirs = sorted((lakehouse_dir / f"business_date={business_date}").glob("run_id=*"))
    check(
        "single_lakehouse_run_directory",
        len(run_dirs) == 1,
        f"run directories={[p.name for p in run_dirs]}",
    )
    version_dirs = sorted((adapter_dir / f"business_date={business_date}").glob("source_hash=*"))
    check(
        "single_immutable_adapter_version",
        len(version_dirs) == 1,
        f"adapter versions={[p.name for p in version_dirs]}",
    )
    check(
        "provenance_records_selected_coordinates",
        len(first["adapter"]["selected_coordinates"]) == first["adapter"]["selected_event_count"],
        f"selected_event_count={first['adapter']['selected_event_count']}",
    )

    # A date with no accepted event must fail before the pipeline can fabricate
    # its synthetic sample or a zero-row gold placeholder.
    empty_date = "1999-01-01"
    empty_probe_dir = output_dir / "empty_date_probe"
    shutil.rmtree(empty_probe_dir, ignore_errors=True)
    try:
        run_bridge(
            landing_dir=landing_dir,
            business_date=empty_date,
            adapter_output_dir=empty_probe_dir / "adapter",
            lakehouse_output_dir=empty_probe_dir / "lakehouse",
        )
        check("empty_date_fails_before_pipeline", False, "bridge unexpectedly succeeded")
    except NoEligibleEventsError as exc:
        check(
            "empty_date_fails_before_pipeline",
            not (empty_probe_dir / "lakehouse").exists(),
            f"raised NoEligibleEventsError and created no lakehouse state: {exc}",
        )

    passed = all(item["status"] == "pass" for item in checks)
    evidence = {
        "scope": "bounded local Kafka K1.5 landing-to-batch bridge",
        "status": "passed" if passed else "failed",
        "business_date": business_date,
        "landing_dir": str(landing_dir),
        "checks": checks,
        "first_run": first,
        "second_run": second,
        "claim_boundary": {
            "supports": [
                "bounded local bridge from immutable accepted Kafka landing to one business_date",
                "deterministic adapter identity reused across reruns",
                "existing quality/gold pipeline and its skip contract",
            ],
            "does_not_support": [
                "continuous streaming",
                "Spark Structured Streaming",
                "direct Kafka-to-Iceberg sink",
                "end-to-end exactly-once",
                "production operation",
            ],
        },
    }
    (output_dir / "kafka_k1_5_verification.json").write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return evidence


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify the bounded Kafka K1.5 bridge.")
    parser.add_argument("--landing-dir", default=DEFAULT_LANDING)
    parser.add_argument("--business-date", default=DEFAULT_BUSINESS_DATE)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove prior adapter/lakehouse evidence before running.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    landing_dir = Path(args.landing_dir)
    if not landing_dir.is_dir():
        raise SystemExit(
            f"K1 landing not found at {landing_dir}. Run ./scripts/verify_kafka_k1.sh first."
        )

    evidence = verify(
        landing_dir=landing_dir,
        business_date=args.business_date,
        output_dir=Path(args.output_dir),
        clean=args.clean,
    )
    print(json.dumps(evidence["checks"], indent=2))
    output_dir = Path(args.output_dir)
    if evidence["status"] != "passed":
        print(f"Kafka K1.5 verification FAILED; evidence: {output_dir}", file=sys.stderr)
        raise SystemExit(1)
    print(f"Kafka K1.5 verification passed; evidence: {output_dir}")


if __name__ == "__main__":
    main()
