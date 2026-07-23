#!/usr/bin/env python3
"""Bounded S9 runtime verification: recovered session -> Iceberg publish.

Reuses the existing S8 spool/gate and the existing S7 Spark/Iceberg path. Phases are separate
processes because their contract is the persisted spool/landing/warehouse evidence on disk:

  --phase spool    prepare and seal the edge session while NO broker process is running
  --phase broker   inside the shared local-Kafka runbook: partial replay -> gate blocks,
                   then complete replay -> recovery_complete
  --phase publish  the Spark-capable interpreter chosen by PYTHON_BIN in
                   scripts/verify_recovered_telemetry_publish.sh (the verified command uses the
                   system python, NOT the project .venv, which has no pyspark):
                   S9 publish, then retry (published -> skipped)

The broker phase never imports Spark: the S9 gate raises before those imports are reached.

`phase_state.json` is the working handoff between the three processes. The publish phase writes
the canonical `s9_verification.json` and then removes that scratch file, so the phase results are
not persisted twice.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

from manufacturing_data_platform.edge_recovery import (
    RecoveryIncompleteError,
    append_edge_event,
    compute_recovery_coverage,
    load_sealed_session,
    seal_edge_session,
)
from manufacturing_data_platform.kafka_ingestion.contracts import sample_machine_event


EDGE_SOURCE_ID = "edge-plant-a"
BOOT_SESSION_ID = "boot-s9-0001"
BUSINESS_DATE = "2026-06-29"
TOPIC = "manufacturing.machine-events.v1"
GROUP_ID = "manufacturing-s9-recovered-publish"
SEALED_LAST_SEQUENCE = 3


def _paths(root: Path) -> dict[str, Path]:
    return {
        "spool": root / "spool",
        "landing": root / "raw",
        "adapter": root / "adapter",
        "warehouse": root / "warehouse",
        "evidence_dir": root / "evidence",
        "state": root / "phase_state.json",
        "final": root / "s9_verification.json",
    }


def _read_state(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _write_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _check(name: str, passed: bool, detail: str) -> dict:
    return {"name": name, "status": "pass" if passed else "fail", "detail": detail}


def _fail_if_any(checks: list[dict], label: str) -> None:
    failed = [c for c in checks if c["status"] != "pass"]
    if failed:
        print(f"{label} FAILED: {json.dumps(failed, indent=2)}", file=sys.stderr)
        raise SystemExit(1)


def _coverage(paths: dict[str, Path]) -> dict:
    session = load_sealed_session(
        spool_root=paths["spool"], edge_source_id=EDGE_SOURCE_ID, boot_session_id=BOOT_SESSION_ID
    )
    return compute_recovery_coverage(session=session, landing_dir=paths["landing"])


# --------------------------------------------------------------------------- #
# Phase 1 — spool while disconnected
# --------------------------------------------------------------------------- #
def phase_spool(paths: dict[str, Path]) -> None:
    for seq in range(1, SEALED_LAST_SEQUENCE + 1):
        append_edge_event(
            spool_root=paths["spool"], edge_source_id=EDGE_SOURCE_ID,
            boot_session_id=BOOT_SESSION_ID, sequence_no=seq, event=sample_machine_event(seq),
        )
    seal = seal_edge_session(
        spool_root=paths["spool"], edge_source_id=EDGE_SOURCE_ID,
        boot_session_id=BOOT_SESSION_ID, expected_last_sequence=SEALED_LAST_SEQUENCE,
    )
    state = {
        "phase_spool": {
            "sealed_event_count": seal["sealed_event_count"],
            "machine_id": seal["machine_id"],
            "business_date": seal["business_date"],
            "broker_absent_during_spool": True,
        }
    }
    _write_state(paths["state"], state)
    print(json.dumps(state["phase_spool"], indent=2))


# --------------------------------------------------------------------------- #
# Phase 2 — replay through the real broker; prove the S9 gate blocks when partial
# --------------------------------------------------------------------------- #
def phase_broker(paths: dict[str, Path], bootstrap_servers: str) -> None:
    from manufacturing_data_platform.kafka_ingestion.runtime import (
        consume_and_land,
        ensure_single_partition_topic,
        produce_events,
    )
    from manufacturing_data_platform.pipeline.recovered_telemetry_publish import (
        run_recovered_telemetry_publish,
    )

    ensure_single_partition_topic(bootstrap_servers, TOPIC)
    state = _read_state(paths["state"])

    def replay(label: str, sequences: list[int]) -> dict:
        events = [sample_machine_event(seq) for seq in sequences]
        deliveries = produce_events(events, bootstrap_servers=bootstrap_servers, topic=TOPIC)
        consume_and_land(
            bootstrap_servers=bootstrap_servers, topic=TOPIC, group_id=GROUP_ID,
            output_dir=paths["landing"], max_messages=len(events),
        )
        coverage = _coverage(paths)
        step = {
            "phase": label,
            "replayed_edge_sequences": sequences,
            "produced_kafka_offsets": [d["offset"] for d in deliveries],
            "central_accepted_total": coverage["central_accepted_total"],
            "missing_sequences": coverage["missing_sequences"],
            "recovery_complete": coverage["recovery_complete"],
        }
        print(json.dumps(step, indent=2))
        return step

    partial = replay("partial", [1, 2])

    # The real S9 publish gate must refuse while sequence 3 is missing, and must do so
    # before Spark/Iceberg is touched at all.
    blocked, detail = False, "S9 publish unexpectedly succeeded while recovery was incomplete"
    try:
        run_recovered_telemetry_publish(
            spool_root=paths["spool"], edge_source_id=EDGE_SOURCE_ID,
            boot_session_id=BOOT_SESSION_ID, landing_dir=paths["landing"],
            business_date=BUSINESS_DATE, adapter_output_dir=paths["adapter"],
            warehouse_path=paths["warehouse"], evidence_output_dir=paths["evidence_dir"],
        )
    except RecoveryIncompleteError as exc:
        blocked, detail = True, f"RecoveryIncompleteError: {exc}"

    gate = {
        "partial_publish_blocked": blocked,
        "no_warehouse_created": not paths["warehouse"].exists(),
        "no_adapter_created": not paths["adapter"].exists(),
        "detail": detail,
    }
    print(json.dumps({"phase": "partial_publish_gate", **gate}, indent=2))

    complete = replay("complete", [1, 2, 3])

    checks = [
        _check("partial_missing_is_3", partial["missing_sequences"] == [3],
               f"missing={partial['missing_sequences']}"),
        _check("partial_publish_blocked", gate["partial_publish_blocked"] is True, gate["detail"]),
        _check("partial_left_no_spark_iceberg_state",
               gate["no_warehouse_created"] and gate["no_adapter_created"],
               f"warehouse_absent={gate['no_warehouse_created']} adapter_absent={gate['no_adapter_created']}"),
        _check("complete_recovery_true", complete["recovery_complete"] is True,
               f"missing={complete['missing_sequences']}"),
        _check("complete_accepted_is_3", complete["central_accepted_total"] == 3,
               f"accepted={complete['central_accepted_total']}"),
    ]
    state["phase_broker"] = {
        "partial": partial, "complete": complete, "partial_publish_gate": gate, "checks": checks,
    }
    _write_state(paths["state"], state)
    _fail_if_any(checks, "broker phase")


# --------------------------------------------------------------------------- #
# Phase 3 — S9 publish, then retry
# --------------------------------------------------------------------------- #
def phase_publish(paths: dict[str, Path]) -> None:
    from manufacturing_data_platform.pipeline.recovered_telemetry_publish import (
        run_recovered_telemetry_publish,
    )

    state = _read_state(paths["state"])

    def publish(tag: str) -> dict:
        # Distinct evidence files: each attempt's persisted document is compared to the value
        # that same attempt returned. (A skipped retry legitimately carries a fresh Spark attempt
        # run_id while the Iceberg snapshot stays the same - inherited S7 behaviour.)
        return run_recovered_telemetry_publish(
            spool_root=paths["spool"], edge_source_id=EDGE_SOURCE_ID,
            boot_session_id=BOOT_SESSION_ID, landing_dir=paths["landing"],
            business_date=BUSINESS_DATE, adapter_output_dir=paths["adapter"],
            warehouse_path=paths["warehouse"], evidence_output_dir=paths["evidence_dir"],
            evidence_file=paths["evidence_dir"] / f"s9_publish_{tag}.json",
        )

    first = publish("first")
    second = publish("retry")
    persisted = json.loads(Path(first["evidence_path"]).read_text(encoding="utf-8"))
    persisted_retry = json.loads(Path(second["evidence_path"]).read_text(encoding="utf-8"))

    edge_ids = set(first["edge"]["event_ids"])
    adapter_ids = set(first["adapter"]["selected_event_ids"])

    checks = [
        _check("first_published", first["status"] == "published", f"status={first['status']}"),
        _check("first_quality_passed", first["spark"]["quality_passed"] is True,
               "Spark quality gate passed before the Iceberg commit"),
        _check("snapshot_id_present", isinstance(first["iceberg"]["gold_snapshot_id"], int),
               f"snapshot_id={first['iceberg']['gold_snapshot_id']}"),
        _check("retry_skipped", second["status"] == "skipped", f"status={second['status']}"),
        _check("retry_same_source_hash",
               second["adapter"]["source_hash"] == first["adapter"]["source_hash"],
               f"source_hash={first['adapter']['source_hash']}"),
        _check("retry_same_snapshot_id",
               second["iceberg"]["gold_snapshot_id"] == first["iceberg"]["gold_snapshot_id"],
               f"snapshot_id={second['iceberg']['gold_snapshot_id']}"),
        _check("retry_creates_no_new_snapshot",
               second["iceberg"]["snapshot_count"] == first["iceberg"]["snapshot_count"],
               f"snapshot_count={first['iceberg']['snapshot_count']} unchanged; the retry performed "
               "no partition overwrite (it still ran Spark and quality before deciding to skip)"),
        _check("attempt_run_ids_differ",
               second["spark"]["attempt_run_id"] != first["spark"]["attempt_run_id"],
               f"first={first['spark']['attempt_run_id']} retry={second['spark']['attempt_run_id']}"),
        _check("first_snapshot_created_by_current_attempt",
               first["iceberg"]["snapshot_created_by_current_attempt"] is True
               and first["iceberg"]["snapshot_relation"] == "created_by_current_attempt",
               f"producer_attempt_run_id={first['iceberg']['producer_attempt_run_id']}"),
        _check("retry_snapshot_not_created_by_current_attempt",
               second["iceberg"]["snapshot_created_by_current_attempt"] is False
               and second["iceberg"]["snapshot_relation"] == "reused_from_prior_attempt"
               and second["iceberg"]["producer_attempt_run_id"] is None,
               "the skipped attempt reuses a snapshot it did not create; S7 does not expose the "
               "producer run_id, so it is recorded as unknown rather than guessed"),
        _check("edge_event_ids_equal_adapter_event_ids", edge_ids == adapter_ids,
               f"{len(edge_ids)} sealed == {len(adapter_ids)} selected"),
        _check("identity_chain_persisted",
               persisted["identity_chain"] == first["identity_chain"]
               and persisted_retry["identity_chain"] == second["identity_chain"],
               "each attempt's returned and persisted identity chains agree"),
        _check("edge_sequence_not_kafka_offsets",
               first["identity_chain"]["edge_sequence"]
               != [c["kafka_offset"] for c in first["identity_chain"]["kafka_coordinate"]],
               f"edge_sequence={first['identity_chain']['edge_sequence']} != kafka_offsets="
               f"{[c['kafka_offset'] for c in first['identity_chain']['kafka_coordinate']]}; "
               "this is the observed counterexample only, not a general identity-space proof"),
    ]
    state["phase_publish"] = {
        "first_status": first["status"], "second_status": second["status"],
        "source_hash": first["adapter"]["source_hash"],
        "first_spark_attempt_run_id": first["spark"]["attempt_run_id"],
        "retry_spark_attempt_run_id": second["spark"]["attempt_run_id"],
        "gold_snapshot_id": first["iceberg"]["gold_snapshot_id"],
        "first_snapshot_relation": first["iceberg"]["snapshot_relation"],
        "retry_snapshot_relation": second["iceberg"]["snapshot_relation"],
        "identity_chain": first["identity_chain"],
        "checks": checks,
    }
    _write_state(paths["state"], state)

    passed = all(
        item["status"] == "pass"
        for phase in state.values() if isinstance(phase, dict)
        for item in phase.get("checks", [])
    )
    paths["final"].write_text(
        json.dumps(
            {
                "scope": "bounded local S9 recovery-gated Spark/Iceberg publish",
                "status": "passed" if passed else "failed",
                "phases": {
                    key.removeprefix("phase_"): value for key, value in state.items()
                },
                "claim_boundary": first["claim_boundary"],
            },
            indent=2, sort_keys=True,
        ) + "\n",
        encoding="utf-8",
    )
    # The canonical document now holds every phase result, so the cross-process scratch file is
    # not kept as a second copy of the same state.
    paths["state"].unlink(missing_ok=True)
    print(json.dumps(checks, indent=2))
    _fail_if_any(checks, "publish phase")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify the bounded S9 recovery-gated publish.")
    parser.add_argument("--phase", required=True, choices=["spool", "broker", "publish"])
    parser.add_argument("--output-dir", default="/tmp/manufacturing-mini-s9-verification")
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--clean", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = Path(args.output_dir)
    paths = _paths(root)

    if args.phase == "spool":
        if args.clean:
            shutil.rmtree(root, ignore_errors=True)
        phase_spool(paths)
    elif args.phase == "broker":
        if not args.bootstrap_servers:
            raise SystemExit("--bootstrap-servers is required for the broker phase")
        phase_broker(paths, args.bootstrap_servers)
    else:
        phase_publish(paths)

    print(f"S9 phase '{args.phase}' completed; evidence root: {root}")


if __name__ == "__main__":
    main()
