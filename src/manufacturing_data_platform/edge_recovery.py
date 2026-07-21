"""S8: bounded local edge/cloud disconnection and recovery simulation.

One disconnected edge session is buffered into an **immutable local spool** while no broker
is running, sealed with an explicit ``expected_last_sequence``, then replayed through the
existing local Kafka/K1 landing after reconnect. The existing K1.5 batch/gold path may run
only after every sealed edge sequence is represented in the central accepted set.

Three identity spaces stay separate on purpose:

* ``(edge_source_id, boot_session_id, sequence_no)`` — edge ordering/completeness.
* ``event_id`` — business identity; this is what prevents a producer replay at new Kafka
  offsets from growing the accepted business-event set.
* ``(topic, partition, offset)`` — Kafka transport evidence.

Edge completeness is therefore **never** inferred from Kafka offset continuity: K1 already
allows legitimate offset gaps, and the two spaces are unrelated.

This module reuses K1/K1.5 through public APIs only and does not modify them. It is a
synthetic, local, bounded, single machine/session/partition simulation — not an edge gateway,
not a product-grade offline buffer, and not a durability claim beyond a local Linux filesystem.
"""

from __future__ import annotations

import json
import os
import re
import shutil
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import uuid4

from manufacturing_data_platform.kafka_ingestion.contracts import validate_machine_event
from manufacturing_data_platform.kafka_ingestion.landing import load_landing_index


FORMAT_VERSION = 1
SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9._-]+$")
ENTRY_NAME = "entry.json"
SEAL_NAME = "session_seal.json"

CLAIM_BOUNDARY = {
    "supports": [
        "bounded local simulation of one disconnected edge session with an immutable sealed spool",
        "replay of synthetic machine events through the existing local Kafka/K1 landing",
        "edge-sequence completeness measured against the central accepted event_id set",
        "downstream batch/gold promotion blocked while recovery is incomplete",
        "repeated replay without growing the accepted business-event set or the trusted batch result",
    ],
    "does_not_support": [
        "real edge gateway hardware or a product-grade offline buffer",
        "OPC UA / MQTT / ROS 2 / DDS integration",
        "continuous or large-scale real-time streaming",
        "power-loss-safe or distributed durability",
        "multiple machines/sessions/partitions or concurrent writers",
        "multi-partition ordering or rebalance correctness",
        "production Kafka/Spark/Airflow operation or end-to-end exactly-once",
    ],
}


class EdgeRecoveryError(RuntimeError):
    """Base error for the S8 edge-recovery simulation."""


class EdgeIdentifierError(EdgeRecoveryError):
    """An edge identifier is empty or not path-safe."""


class EdgeSpoolConflictError(EdgeRecoveryError):
    """The same edge coordinate was appended with different canonical bytes."""


class EdgeSealError(EdgeRecoveryError):
    """A session seal is missing, incomplete, changed, or appended to after sealing."""


class RecoveryIncompleteError(EdgeRecoveryError):
    """Downstream promotion was attempted while the sealed range is not fully recovered."""


class EdgeSessionScopeError(EdgeRecoveryError):
    """The bounded S8 session scope (one machine_id, one business_date) was violated."""


@dataclass(frozen=True)
class EdgeEntry:
    edge_source_id: str
    boot_session_id: str
    sequence_no: int
    event_id: str
    fingerprint: str
    event: dict[str, Any]


@dataclass(frozen=True)
class SealedSession:
    edge_source_id: str
    boot_session_id: str
    expected_last_sequence: int
    machine_id: str
    business_date: str
    entries: tuple[EdgeEntry, ...]

    @property
    def event_ids(self) -> tuple[str, ...]:
        return tuple(entry.event_id for entry in self.entries)


@dataclass(frozen=True)
class AppendResult:
    status: str  # "appended" | "reused"
    sequence_no: int
    event_id: str
    fingerprint: str
    entry_path: Path


# --------------------------------------------------------------------------- #
# Canonical envelope (identity) — no wall-clock values
# --------------------------------------------------------------------------- #
def validate_identifier(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EdgeIdentifierError(f"{field} must be a non-empty string")
    if not SAFE_IDENTIFIER_RE.fullmatch(value):
        raise EdgeIdentifierError(f"unsafe {field}: {value!r}")
    return value


def validate_sequence_no(sequence_no: int) -> int:
    if isinstance(sequence_no, bool) or not isinstance(sequence_no, int):
        raise EdgeRecoveryError("sequence_no must be an integer")
    if sequence_no < 1:
        raise EdgeRecoveryError("sequence_no must be >= 1")
    return sequence_no


def canonical_envelope(
    *, edge_source_id: str, boot_session_id: str, sequence_no: int, event: dict[str, Any]
) -> dict[str, Any]:
    """Build the identity envelope. The inner event uses the unchanged strict v1 contract."""
    validate_identifier(edge_source_id, "edge_source_id")
    validate_identifier(boot_session_id, "boot_session_id")
    validate_sequence_no(sequence_no)
    return {
        "format_version": FORMAT_VERSION,
        "edge_source_id": edge_source_id,
        "boot_session_id": boot_session_id,
        "sequence_no": sequence_no,
        "event": validate_machine_event(event),
    }


def canonical_envelope_bytes(envelope: dict[str, Any]) -> bytes:
    """Deterministic bytes. No wall-clock value ever enters content identity."""
    return json.dumps(
        envelope, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def envelope_fingerprint(envelope_bytes: bytes) -> str:
    return sha256(envelope_bytes).hexdigest()


# --------------------------------------------------------------------------- #
# Spool layout
# --------------------------------------------------------------------------- #
def session_dir(spool_root: str | Path, edge_source_id: str, boot_session_id: str) -> Path:
    validate_identifier(edge_source_id, "edge_source_id")
    validate_identifier(boot_session_id, "boot_session_id")
    return (
        Path(spool_root)
        / f"edge_source_id={edge_source_id}"
        / f"boot_session_id={boot_session_id}"
    )


def entry_dir(session_path: Path, sequence_no: int) -> Path:
    return session_path / f"seq={sequence_no:020d}"


def seal_path(session_path: Path) -> Path:
    return session_path / SEAL_NAME


def is_sealed(spool_root: str | Path, edge_source_id: str, boot_session_id: str) -> bool:
    return seal_path(session_dir(spool_root, edge_source_id, boot_session_id)).exists()


# --------------------------------------------------------------------------- #
# Task 1 — immutable append + seal
# --------------------------------------------------------------------------- #
def append_edge_event(
    *,
    spool_root: str | Path,
    edge_source_id: str,
    boot_session_id: str,
    sequence_no: int,
    event: dict[str, Any],
) -> AppendResult:
    """Append one immutable edge entry (staging -> fsync -> atomic rename -> dir fsync)."""
    envelope = canonical_envelope(
        edge_source_id=edge_source_id,
        boot_session_id=boot_session_id,
        sequence_no=sequence_no,
        event=event,
    )
    payload = canonical_envelope_bytes(envelope)
    fingerprint = envelope_fingerprint(payload)
    event_id = envelope["event"]["event_id"]

    session_path = session_dir(spool_root, edge_source_id, boot_session_id)
    if seal_path(session_path).exists():
        raise EdgeSealError(
            f"session {edge_source_id}/{boot_session_id} is sealed; append is not allowed"
        )

    target_dir = entry_dir(session_path, sequence_no)
    target_file = target_dir / ENTRY_NAME

    if target_file.exists():
        existing = target_file.read_bytes()
        if existing == payload:
            return AppendResult("reused", sequence_no, event_id, fingerprint, target_file)
        raise EdgeSpoolConflictError(
            f"edge coordinate ({edge_source_id}, {boot_session_id}, {sequence_no}) "
            "already exists with different canonical bytes"
        )

    # The same business event must not be spooled at two different edge sequences.
    for entry in _scan_entries(session_path):
        if entry.event_id == event_id and entry.sequence_no != sequence_no:
            raise EdgeSpoolConflictError(
                f"event_id={event_id!r} already spooled at sequence {entry.sequence_no}"
            )

    staging = session_path / ".staging" / f"{sequence_no:020d}-{uuid4().hex}"
    staging.mkdir(parents=True, exist_ok=False)
    try:
        staged_file = staging / ENTRY_NAME
        with staged_file.open("wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        _fsync_directory(staging)

        target_dir.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staging, target_dir)
        _fsync_directory(target_dir.parent)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise

    return AppendResult("appended", sequence_no, event_id, fingerprint, target_file)


def seal_edge_session(
    *,
    spool_root: str | Path,
    edge_source_id: str,
    boot_session_id: str,
    expected_last_sequence: int,
) -> dict[str, Any]:
    """Seal one bounded session. Completeness is the full range 1..expected_last_sequence."""
    validate_sequence_no(expected_last_sequence)
    session_path = session_dir(spool_root, edge_source_id, boot_session_id)
    existing_seal = seal_path(session_path)

    # A sealed session is immutable: decide reuse-vs-conflict before re-validating the
    # range, so a changed seal reports the seal conflict rather than a range symptom.
    if existing_seal.exists():
        previous = json.loads(existing_seal.read_text(encoding="utf-8"))
        if not isinstance(previous, dict):
            raise EdgeSealError(f"{existing_seal}: seal must be a JSON object")
        if previous.get("expected_last_sequence") != expected_last_sequence:
            raise EdgeSealError(
                "session is already sealed with a different expected_last_sequence "
                f"({previous.get('expected_last_sequence')} != {expected_last_sequence})"
            )
        # Reuse must pass the same validation as a fresh load, not just the range check.
        _validate_seal(previous, _scan_entries(session_path), edge_source_id, boot_session_id)
        return previous

    entries = _scan_entries(session_path)
    present = {entry.sequence_no for entry in entries}
    missing = [n for n in range(1, expected_last_sequence + 1) if n not in present]
    if missing:
        raise EdgeSealError(
            f"cannot seal: sequences {missing} are absent from the local spool"
        )
    extra = sorted(n for n in present if n > expected_last_sequence)
    if extra:
        raise EdgeSealError(
            f"cannot seal at {expected_last_sequence}: spool also holds sequences {extra}"
        )

    # S8 is bounded to one machine and one business_date per sealed session. Deriving and
    # persisting them makes the scope an explicit, checkable session invariant instead of
    # an assumption, and lets promotion refuse a mismatched date.
    machine_id, business_date = _session_scope(entries)

    seal_doc = {
        "format_version": FORMAT_VERSION,
        "edge_source_id": edge_source_id,
        "boot_session_id": boot_session_id,
        "expected_last_sequence": expected_last_sequence,
        "machine_id": machine_id,
        "business_date": business_date,
        "sealed_event_count": len(entries),
        "entries": [
            {
                "sequence_no": entry.sequence_no,
                "event_id": entry.event_id,
                "fingerprint": entry.fingerprint,
            }
            for entry in entries
        ],
        "claim_boundary": CLAIM_BOUNDARY,
    }

    _write_json_atomic(existing_seal, seal_doc)
    return seal_doc


def _session_scope(entries: list[EdgeEntry]) -> tuple[str, str]:
    """Derive the bounded session scope, rejecting a mixed machine or business_date."""
    if not entries:
        raise EdgeSealError("cannot derive session scope from an empty spool")
    machines = sorted({entry.event["machine_id"] for entry in entries})
    dates = sorted({entry.event["business_date"] for entry in entries})
    if len(machines) != 1:
        raise EdgeSessionScopeError(
            f"S8 session must hold exactly one machine_id; found {machines}"
        )
    if len(dates) != 1:
        raise EdgeSessionScopeError(
            f"S8 session must hold exactly one business_date; found {dates}"
        )
    return machines[0], dates[0]


def _validate_seal(
    seal_doc: Any, entries: list[EdgeEntry], edge_source_id: str, boot_session_id: str
) -> tuple[int, str, str]:
    """Re-validate a seal against the spool. Missing AND extra entries are rejected."""
    if not isinstance(seal_doc, dict):
        raise EdgeSealError("seal must be a JSON object")
    if seal_doc.get("format_version") != FORMAT_VERSION:
        raise EdgeSealError(f"unsupported seal format_version: {seal_doc.get('format_version')!r}")
    if seal_doc.get("edge_source_id") != edge_source_id:
        raise EdgeSealError("seal edge_source_id does not match the requested session")
    if seal_doc.get("boot_session_id") != boot_session_id:
        raise EdgeSealError("seal boot_session_id does not match the requested session")

    expected_last = seal_doc.get("expected_last_sequence")
    if isinstance(expected_last, bool) or not isinstance(expected_last, int) or expected_last < 1:
        raise EdgeSealError("seal has an unusable expected_last_sequence")

    declared_raw = seal_doc.get("entries")
    if not isinstance(declared_raw, list):
        raise EdgeSealError("seal entries must be a list")
    declared: dict[int, tuple[str, str]] = {}
    for item in declared_raw:
        if not isinstance(item, dict):
            raise EdgeSealError("every seal entry must be a JSON object")
        seq, fingerprint, event_id = (
            item.get("sequence_no"), item.get("fingerprint"), item.get("event_id")
        )
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 1:
            raise EdgeSealError("seal entry has an unusable sequence_no")
        if not isinstance(fingerprint, str) or not isinstance(event_id, str):
            raise EdgeSealError(f"seal entry {seq} has an unusable fingerprint/event_id")
        if seq in declared:
            raise EdgeSealError(f"seal declares sequence {seq} more than once")
        declared[seq] = (fingerprint, event_id)

    if seal_doc.get("sealed_event_count") != len(declared):
        raise EdgeSealError(
            f"seal sealed_event_count={seal_doc.get('sealed_event_count')!r} disagrees with "
            f"{len(declared)} declared entries"
        )
    if set(declared) != set(range(1, expected_last + 1)):
        raise EdgeSealError(
            f"seal must declare exactly sequences 1..{expected_last}; got {sorted(declared)}"
        )

    present = {entry.sequence_no: entry for entry in entries}
    missing = sorted(set(declared) - set(present))
    extra = sorted(set(present) - set(declared))
    if missing:
        raise EdgeSealError(f"sealed session is missing spool entries {missing}")
    if extra:
        raise EdgeSealError(f"sealed session has extra spool entries {extra} added after sealing")

    for seq, entry in present.items():
        fingerprint, event_id = declared[seq]
        if entry.fingerprint != fingerprint:
            raise EdgeSealError(f"sequence {seq} fingerprint disagrees with the seal manifest")
        if entry.event_id != event_id:
            raise EdgeSealError(f"sequence {seq} event_id disagrees with the seal manifest")
        if entry.edge_source_id != edge_source_id or entry.boot_session_id != boot_session_id:
            raise EdgeSealError(f"sequence {seq} does not belong to the requested session")

    machine_id, business_date = _session_scope(list(present.values()))
    if seal_doc.get("machine_id") != machine_id:
        raise EdgeSealError(
            f"seal machine_id={seal_doc.get('machine_id')!r} disagrees with spool {machine_id!r}"
        )
    if seal_doc.get("business_date") != business_date:
        raise EdgeSealError(
            f"seal business_date={seal_doc.get('business_date')!r} disagrees with spool "
            f"{business_date!r}"
        )
    return expected_last, machine_id, business_date


def load_sealed_session(
    *, spool_root: str | Path, edge_source_id: str, boot_session_id: str
) -> SealedSession:
    """Load and re-validate a sealed session. Filenames alone are never trusted."""
    session_path = session_dir(spool_root, edge_source_id, boot_session_id)
    seal_file = seal_path(session_path)
    if not seal_file.exists():
        raise EdgeSealError(f"session is not sealed: {session_path}")

    seal_doc = json.loads(seal_file.read_text(encoding="utf-8"))
    entries = _scan_entries(session_path)
    expected_last, machine_id, business_date = _validate_seal(
        seal_doc, entries, edge_source_id, boot_session_id
    )
    return SealedSession(
        edge_source_id=edge_source_id,
        boot_session_id=boot_session_id,
        expected_last_sequence=expected_last,
        machine_id=machine_id,
        business_date=business_date,
        entries=tuple(sorted(entries, key=lambda item: item.sequence_no)),
    )


# --------------------------------------------------------------------------- #
# Task 2 — coverage and promotion gate
# --------------------------------------------------------------------------- #
def compute_recovery_coverage(
    *, session: SealedSession, landing_dir: str | Path
) -> dict[str, Any]:
    """Compare sealed edge event IDs to the central accepted set (read-only).

    Edge completeness is decided by ``event_id`` membership, never by Kafka offset
    continuity — K1 permits legitimate offset gaps.
    """
    accepted = load_landing_index(landing_dir)["accepted_events"]

    covered: list[int] = []
    missing: list[int] = []
    coordinates: list[dict[str, Any]] = []
    for entry in session.entries:
        coordinate = accepted.get(entry.event_id)
        if coordinate is None:
            missing.append(entry.sequence_no)
            continue
        covered.append(entry.sequence_no)
        coordinates.append(
            {
                "sequence_no": entry.sequence_no,
                "event_id": entry.event_id,
                "kafka_topic": coordinate[0],
                "kafka_partition": coordinate[1],
                "kafka_offset": coordinate[2],
            }
        )

    return {
        "edge_source_id": session.edge_source_id,
        "boot_session_id": session.boot_session_id,
        "expected_last_sequence": session.expected_last_sequence,
        "expected_sequence_count": len(session.entries),
        "central_accepted_sequence_count": len(covered),
        "central_accepted_total": len(accepted),
        "recovered_sequences": covered,
        "missing_sequences": missing,
        "recovery_complete": not missing,
        "sealed_event_ids": list(session.event_ids),
        "recovered_coordinates": coordinates,
        "claim_boundary": CLAIM_BOUNDARY,
    }


def promote_recovered_session(
    *,
    spool_root: str | Path,
    edge_source_id: str,
    boot_session_id: str,
    landing_dir: str | Path,
    business_date: str,
    adapter_output_dir: str | Path,
    lakehouse_output_dir: str | Path,
    evidence_file: str | Path | None = None,
) -> dict[str, Any]:
    """Run the existing K1.5 bridge only when the sealed range is fully recovered.

    A blocked promotion raises before ``run_bridge`` is called, so no adapter or lakehouse
    output is created. K1.5 transform/quality logic is delegated, never copied.
    """
    session = load_sealed_session(
        spool_root=spool_root,
        edge_source_id=edge_source_id,
        boot_session_id=boot_session_id,
    )

    # The sealed session owns exactly one business_date. Promoting a different date would
    # publish a batch that does not represent the sealed range, so refuse before any output.
    if business_date != session.business_date:
        raise EdgeSessionScopeError(
            f"requested business_date={business_date} does not match the sealed session date "
            f"{session.business_date}; promotion is blocked"
        )

    coverage = compute_recovery_coverage(session=session, landing_dir=landing_dir)

    if not coverage["recovery_complete"]:
        raise RecoveryIncompleteError(
            f"recovery incomplete: missing edge sequences {coverage['missing_sequences']} "
            f"of 1..{coverage['expected_last_sequence']}; downstream promotion is blocked"
        )

    # Imported lazily: the shared Kafka runbook venv intentionally lacks the batch
    # pipeline's dependencies, and the spool/coverage path must stay importable there.
    from manufacturing_data_platform.kafka_ingestion.batch_adapter import run_bridge

    bridge = run_bridge(
        landing_dir=landing_dir,
        business_date=business_date,
        adapter_output_dir=adapter_output_dir,
        lakehouse_output_dir=lakehouse_output_dir,
    )

    evidence = {
        "slice": "s8-edge-cloud-recovery",
        "business_date": business_date,
        "coverage": coverage,
        "bridge": bridge,
        "identities": _identity_evidence(coverage, bridge),
        "claim_boundary": CLAIM_BOUNDARY,
    }
    if evidence_file is not None:
        _write_json_atomic(Path(evidence_file), evidence)
    return evidence


def _identity_evidence(coverage: dict[str, Any], bridge: dict[str, Any]) -> dict[str, Any]:
    """Keep the five identity spaces visibly distinct in one evidence block."""
    return {
        "edge_sequence": coverage["recovered_sequences"],
        "event_id": coverage["sealed_event_ids"],
        "kafka_coordinate": [
            {k: c[k] for k in ("kafka_topic", "kafka_partition", "kafka_offset")}
            for c in coverage["recovered_coordinates"]
        ],
        "adapter_source_hash": bridge["adapter"]["source_hash"],
        "lakehouse_run_id": bridge["lakehouse"]["run_id"],
        "note": (
            "edge sequence, business event_id, Kafka coordinate, batch source_hash and "
            "pipeline run_id are separate identity spaces; completeness is decided by "
            "event_id, not by Kafka offset continuity"
        ),
    }


# --------------------------------------------------------------------------- #
# Internal helpers
# --------------------------------------------------------------------------- #
def _scan_entries(session_path: Path) -> list[EdgeEntry]:
    """Read every persisted entry and re-derive identity from file content."""
    if not session_path.is_dir():
        return []
    entries: list[EdgeEntry] = []
    for entry_file in sorted(session_path.glob(f"seq=*/{ENTRY_NAME}")):
        entries.append(_load_entry(entry_file))
    entries.sort(key=lambda item: item.sequence_no)
    return entries


def _load_entry(entry_file: Path) -> EdgeEntry:
    raw = entry_file.read_bytes()
    try:
        envelope = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EdgeRecoveryError(f"{entry_file}: entry is not valid JSON") from exc
    if not isinstance(envelope, dict):
        raise EdgeRecoveryError(f"{entry_file}: entry must be a JSON object")

    rebuilt = canonical_envelope(
        edge_source_id=envelope.get("edge_source_id"),
        boot_session_id=envelope.get("boot_session_id"),
        sequence_no=envelope.get("sequence_no"),
        event=envelope.get("event") or {},
    )
    if canonical_envelope_bytes(rebuilt) != raw:
        raise EdgeRecoveryError(f"{entry_file}: content is not canonical for its envelope")

    # Do not trust the directory names: every path segment must agree with the envelope.
    declared = entry_file.parent.name
    if declared != f"seq={rebuilt['sequence_no']:020d}":
        raise EdgeRecoveryError(
            f"{entry_file}: directory {declared!r} disagrees with envelope sequence_no"
        )
    session_segment = entry_file.parent.parent.name
    source_segment = entry_file.parent.parent.parent.name
    if session_segment != f"boot_session_id={rebuilt['boot_session_id']}":
        raise EdgeRecoveryError(
            f"{entry_file}: path {session_segment!r} disagrees with envelope boot_session_id"
        )
    if source_segment != f"edge_source_id={rebuilt['edge_source_id']}":
        raise EdgeRecoveryError(
            f"{entry_file}: path {source_segment!r} disagrees with envelope edge_source_id"
        )

    return EdgeEntry(
        edge_source_id=rebuilt["edge_source_id"],
        boot_session_id=rebuilt["boot_session_id"],
        sequence_no=rebuilt["sequence_no"],
        event_id=rebuilt["event"]["event_id"],
        fingerprint=envelope_fingerprint(raw),
        event=rebuilt["event"],
    )


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    staging = path.parent / f".{path.name}.{uuid4().hex}.tmp"
    with staging.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(staging, path)
    _fsync_directory(path.parent)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
