#!/usr/bin/env python3
"""Bounded S8 runtime verification: disconnected edge spool -> Kafka replay -> K1.5 gate.

Three phases, deliberately separate processes because their contract is the persisted
spool/landing evidence on disk:

  --phase spool    prepare and seal the edge session while NO broker process is running
  --phase broker   run inside the shared local-Kafka runbook: partial / complete / repeat replay
  --phase promote  project .venv: run the K1.5 promotion gate (processed -> skipped)

The broker phase must not import the batch pipeline: the shared Kafka runbook venv
intentionally lacks its dependencies. ``edge_recovery`` keeps that import lazy.
"""

from __future__ import annotations

import argparse
import json
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
from manufacturing_data_platform.kafka_ingestion.landing import load_landing_index


EDGE_SOURCE_ID = "edge-plant-a"
BOOT_SESSION_ID = "boot-0001"
BUSINESS_DATE = "2026-06-29"
TOPIC = "manufacturing.machine-events.v1"
GROUP_ID = "manufacturing-s8-edge-recovery"
SEALED_LAST_SEQUENCE = 3


def _paths(root: Path) -> dict[str, Path]:
    return {
        "spool": root / "spool",
        "landing": root / "raw",
        "adapter": root / "adapter",
        "lakehouse": root / "lakehouse",
        "evidence": root / "edge_recovery_verification.json",
        "state": root / "phase_state.json",
    }


def _read_state(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _write_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _accepted_total(landing: Path) -> int:
    return len(load_landing_index(landing)["accepted_events"])


def _coverage(spool: Path, landing: Path) -> dict:
    session = load_sealed_session(
        spool_root=spool, edge_source_id=EDGE_SOURCE_ID, boot_session_id=BOOT_SESSION_ID
    )
    return compute_recovery_coverage(session=session, landing_dir=landing)


# --------------------------------------------------------------------------- #
# Phase 1 — spool while disconnected (no broker running)
# --------------------------------------------------------------------------- #
def phase_spool(paths: dict[str, Path]) -> dict:
    for seq in range(1, SEALED_LAST_SEQUENCE + 1):
        append_edge_event(
            spool_root=paths["spool"],
            edge_source_id=EDGE_SOURCE_ID,
            boot_session_id=BOOT_SESSION_ID,
            sequence_no=seq,
            event=sample_machine_event(seq),
        )
    seal = seal_edge_session(
        spool_root=paths["spool"],
        edge_source_id=EDGE_SOURCE_ID,
        boot_session_id=BOOT_SESSION_ID,
        expected_last_sequence=SEALED_LAST_SEQUENCE,
    )
    before = _coverage(paths["spool"], paths["landing"])
    state = {
        "phase_spool": {
            "sealed_event_count": seal["sealed_event_count"],
            "expected_last_sequence": seal["expected_last_sequence"],
            "central_accepted_total_before_replay": before["central_accepted_total"],
            "missing_sequences_before_replay": before["missing_sequences"],
        }
    }
    _write_state(paths["state"], state)
    print(json.dumps(state["phase_spool"], indent=2))
    return state


# --------------------------------------------------------------------------- #
# Phase 2 — reconnect and replay through the real local broker
# --------------------------------------------------------------------------- #
def phase_broker(paths: dict[str, Path], bootstrap_servers: str) -> dict:
    from manufacturing_data_platform.kafka_ingestion.runtime import (
        consume_and_land,
        ensure_single_partition_topic,
        produce_events,
    )

    ensure_single_partition_topic(bootstrap_servers, TOPIC)
    state = _read_state(paths["state"])
    transitions: list[dict] = []

    def replay(label: str, sequences: list[int]) -> dict:
        events = [sample_machine_event(seq) for seq in sequences]
        deliveries = produce_events(
            events, bootstrap_servers=bootstrap_servers, topic=TOPIC
        )
        landed = consume_and_land(
            bootstrap_servers=bootstrap_servers,
            topic=TOPIC,
            group_id=GROUP_ID,
            output_dir=paths["landing"],
            max_messages=len(events),
        )
        coverage = _coverage(paths["spool"], paths["landing"])
        step = {
            "phase": label,
            "replayed_edge_sequences": sequences,
            "produced_kafka_offsets": [d["offset"] for d in deliveries],
            "landing_status": landed["landing"]["status"],
            "accepted_this_batch": landed["landing"]["accepted_count"],
            "duplicate_event_ids_this_batch": landed["landing"]["duplicate_event_count"],
            "central_accepted_total": coverage["central_accepted_total"],
            "missing_sequences": coverage["missing_sequences"],
            "recovery_complete": coverage["recovery_complete"],
        }
        transitions.append(step)
        print(json.dumps(step, indent=2))
        return step

    partial = replay("partial", [1, 2])

    # H2: actually call the real promotion gate while sequence 3 is still missing, so
    # "promotion blocked" is runtime evidence rather than a unit-test-only assertion.
    # The lazy run_bridge import means the incomplete branch raises before this venv would
    # need the batch stack, which is why this is safe inside the Kafka runbook.
    blocked = _assert_partial_promotion_blocked(paths)

    complete = replay("complete", [1, 2, 3])
    repeat = replay("repeat", [1, 2, 3])

    checks = [
        _check("partial_accepted_is_2", partial["central_accepted_total"] == 2,
               f"accepted={partial['central_accepted_total']}"),
        _check("partial_missing_is_3", partial["missing_sequences"] == [3],
               f"missing={partial['missing_sequences']}"),
        _check("partial_promotion_blocked", blocked["partial_promotion_blocked"] is True,
               blocked["detail"]),
        _check("partial_promotion_left_no_output", blocked["no_downstream_output"] is True,
               f"absent={blocked['absent_paths']}"),
        _check("complete_accepted_is_3", complete["central_accepted_total"] == 3,
               f"accepted={complete['central_accepted_total']}"),
        _check("complete_missing_empty", complete["missing_sequences"] == [],
               "sealed range fully represented"),
        _check("repeat_accepted_stays_3", repeat["central_accepted_total"] == 3,
               f"accepted={repeat['central_accepted_total']}"),
        _check("repeat_used_new_offsets",
               min(repeat["produced_kafka_offsets"]) > max(complete["produced_kafka_offsets"]),
               f"offsets={repeat['produced_kafka_offsets']}"),
        _check("repeat_was_duplicate_only", repeat["accepted_this_batch"] == 0,
               f"accepted_this_batch={repeat['accepted_this_batch']}"),
    ]

    state["phase_broker"] = {
        "partial_promotion_gate": blocked,
        "accepted_total_transition": [
            state.get("phase_spool", {}).get("central_accepted_total_before_replay", 0),
            partial["central_accepted_total"],
            complete["central_accepted_total"],
            repeat["central_accepted_total"],
        ],
        "transitions": transitions,
        "checks": checks,
    }
    _write_state(paths["state"], state)
    _fail_if_any(checks, "broker phase")
    return state


# --------------------------------------------------------------------------- #
# Phase 3 — K1.5 promotion gate (project .venv)
# --------------------------------------------------------------------------- #
def phase_promote(paths: dict[str, Path]) -> dict:
    from manufacturing_data_platform.edge_recovery import promote_recovered_session

    state = _read_state(paths["state"])

    def promote() -> dict:
        return promote_recovered_session(
            spool_root=paths["spool"],
            edge_source_id=EDGE_SOURCE_ID,
            boot_session_id=BOOT_SESSION_ID,
            landing_dir=paths["landing"],
            business_date=BUSINESS_DATE,
            adapter_output_dir=paths["adapter"],
            lakehouse_output_dir=paths["lakehouse"],
        )

    first = promote()
    second = promote()

    checks = [
        _check("first_bridge_processed", first["bridge"]["lakehouse"]["status"] == "processed",
               f"status={first['bridge']['lakehouse']['status']}"),
        _check("first_quality_passed", first["bridge"]["lakehouse"]["quality_passed"] is True,
               "quality gate passed before publish"),
        _check("second_bridge_skipped", second["bridge"]["lakehouse"]["status"] == "skipped",
               f"status={second['bridge']['lakehouse']['status']}"),
        _check("source_hash_unchanged",
               first["bridge"]["adapter"]["source_hash"] == second["bridge"]["adapter"]["source_hash"],
               f"source_hash={first['bridge']['adapter']['source_hash']}"),
        _check("run_id_unchanged",
               first["bridge"]["lakehouse"]["run_id"] == second["bridge"]["lakehouse"]["run_id"],
               f"run_id={first['bridge']['lakehouse']['run_id']}"),
        _check("gold_not_doubled",
               first["bridge"]["lakehouse"]["gold_rows"] == second["bridge"]["lakehouse"]["gold_rows"],
               "trusted gold identical across reruns"),
        _check("identity_spaces_distinct",
               first["identities"]["edge_sequence"]
               != [c["kafka_offset"] for c in first["coverage"]["recovered_coordinates"]],
               "edge sequence != kafka offsets"),
    ]

    state["phase_promote"] = {
        "first_bridge_status": first["bridge"]["lakehouse"]["status"],
        "second_bridge_status": second["bridge"]["lakehouse"]["status"],
        "source_hash": first["bridge"]["adapter"]["source_hash"],
        "run_id": first["bridge"]["lakehouse"]["run_id"],
        "quality_passed": first["bridge"]["lakehouse"]["quality_passed"],
        "identities": first["identities"],
        "checks": checks,
    }
    _write_state(paths["state"], state)

    evidence = {
        "scope": "bounded local S8 edge/cloud disconnection and recovery simulation",
        "status": "passed" if _all_pass(state) else "failed",
        "edge_source_id": EDGE_SOURCE_ID,
        "boot_session_id": BOOT_SESSION_ID,
        "business_date": BUSINESS_DATE,
        "phases": state,
        "claim_boundary": first["claim_boundary"],
    }
    paths["evidence"].parent.mkdir(parents=True, exist_ok=True)
    paths["evidence"].write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(state["phase_promote"]["checks"], indent=2))
    _fail_if_any(checks, "promote phase")
    return state


def _assert_partial_promotion_blocked(paths: dict[str, Path]) -> dict:
    """Invoke the real promotion gate while the sealed range is incomplete (H2)."""
    from manufacturing_data_platform.edge_recovery import promote_recovered_session

    promotion_evidence = paths["evidence"].parent / "partial_promotion_evidence.json"
    blocked, detail = False, "promotion unexpectedly succeeded"
    try:
        promote_recovered_session(
            spool_root=paths["spool"],
            edge_source_id=EDGE_SOURCE_ID,
            boot_session_id=BOOT_SESSION_ID,
            landing_dir=paths["landing"],
            business_date=BUSINESS_DATE,
            adapter_output_dir=paths["adapter"],
            lakehouse_output_dir=paths["lakehouse"],
            evidence_file=promotion_evidence,
        )
    except RecoveryIncompleteError as exc:
        blocked, detail = True, f"RecoveryIncompleteError: {exc}"

    absent = {
        "adapter_output_dir": not paths["adapter"].exists(),
        "lakehouse_output_dir": not paths["lakehouse"].exists(),
        "promotion_evidence": not promotion_evidence.exists(),
    }
    result = {
        "partial_promotion_blocked": blocked,
        "no_downstream_output": all(absent.values()),
        "absent_paths": absent,
        "detail": detail,
    }
    print(json.dumps({"phase": "partial_promotion_gate", **result}, indent=2))
    return result


def _check(name: str, passed: bool, detail: str) -> dict:
    return {"name": name, "status": "pass" if passed else "fail", "detail": detail}


def _all_pass(state: dict) -> bool:
    for phase in state.values():
        for item in phase.get("checks", []) if isinstance(phase, dict) else []:
            if item["status"] != "pass":
                return False
    return True


def _fail_if_any(checks: list[dict], label: str) -> None:
    failed = [c for c in checks if c["status"] != "pass"]
    if failed:
        print(f"{label} FAILED: {json.dumps(failed, indent=2)}", file=sys.stderr)
        raise SystemExit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify the bounded S8 edge recovery slice.")
    parser.add_argument("--phase", required=True, choices=["spool", "broker", "promote"])
    parser.add_argument("--output-dir", default="/tmp/manufacturing-mini-s8-edge-recovery")
    parser.add_argument("--bootstrap-servers", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paths = _paths(Path(args.output_dir))

    if args.phase == "spool":
        phase_spool(paths)
    elif args.phase == "broker":
        if not args.bootstrap_servers:
            raise SystemExit("--bootstrap-servers is required for the broker phase")
        phase_broker(paths, args.bootstrap_servers)
    else:
        try:
            phase_promote(paths)
        except RecoveryIncompleteError as exc:
            print(f"promotion blocked: {exc}", file=sys.stderr)
            raise SystemExit(1) from exc

    print(f"S8 phase '{args.phase}' completed; evidence root: {args.output_dir}")


if __name__ == "__main__":
    main()
