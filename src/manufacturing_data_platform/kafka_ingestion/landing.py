from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from manufacturing_data_platform.kafka_ingestion.contracts import (
    EventContractError,
    validate_machine_event,
)


TOPIC_RE = re.compile(r"^[A-Za-z0-9._-]+$")


class LandingConsistencyError(RuntimeError):
    """Raised when immutable Kafka evidence conflicts with a prior landing."""


class SimulatedCrashAfterLanding(RuntimeError):
    """Failure injection after durable landing and before offset commit."""


@dataclass(frozen=True)
class KafkaRecord:
    topic: str
    partition: int
    offset: int
    key: str | None
    value: bytes
    timestamp_ms: int | None = None

    @property
    def coordinate(self) -> tuple[str, int, int]:
        return (self.topic, self.partition, self.offset)


@dataclass(frozen=True)
class LandingResult:
    status: str
    input_record_count: int
    accepted_count: int
    duplicate_event_count: int
    quarantine_count: int
    reused_coordinate_count: int
    accepted_total: int
    batch_path: str | None
    committable_offsets: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["committable_offsets"] = list(self.committable_offsets)
        return payload


def record_fingerprint(record: KafkaRecord) -> str:
    digest = hashlib.sha256()
    digest.update(record.topic.encode("utf-8"))
    digest.update(b"\0")
    digest.update(str(record.partition).encode("ascii"))
    digest.update(b"\0")
    digest.update(str(record.offset).encode("ascii"))
    digest.update(b"\0")
    digest.update((record.key or "").encode("utf-8"))
    digest.update(b"\0")
    digest.update(record.value)
    return digest.hexdigest()


def load_landing_index(output_dir: str | Path) -> dict[str, Any]:
    root = Path(output_dir)
    coordinates: dict[tuple[str, int, int], dict[str, Any]] = {}
    accepted_events: dict[str, tuple[str, int, int]] = {}

    for manifest_path in sorted(root.glob("topic=*/partition=*/batch=*/manifest.json")):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for entry in manifest["entries"]:
            coordinate = (
                entry["topic"],
                int(entry["partition"]),
                int(entry["offset"]),
            )
            previous = coordinates.get(coordinate)
            if previous is not None and previous["fingerprint"] != entry["fingerprint"]:
                raise LandingConsistencyError(
                    f"conflicting persisted fingerprints for coordinate={coordinate!r}"
                )
            coordinates[coordinate] = entry
            if entry["status"] == "accepted":
                event_id = entry["event_id"]
                previous_coordinate = accepted_events.get(event_id)
                if previous_coordinate is not None and previous_coordinate != coordinate:
                    raise LandingConsistencyError(
                        f"event_id={event_id!r} was accepted at multiple coordinates"
                    )
                accepted_events[event_id] = coordinate

    return {
        "coordinates": coordinates,
        "accepted_events": accepted_events,
    }


def land_records(
    records: Iterable[KafkaRecord],
    output_dir: str | Path,
    *,
    simulate_crash_after_rename: bool = False,
) -> LandingResult:
    batch = list(records)
    if not batch:
        raise ValueError("at least one Kafka record is required")
    _validate_batch_scope(batch)

    root = Path(output_dir)
    index = load_landing_index(root)
    known_coordinates = dict(index["coordinates"])
    accepted_events = dict(index["accepted_events"])

    entries: list[dict[str, Any]] = []
    accepted_rows: list[dict[str, Any]] = []
    duplicate_rows: list[dict[str, Any]] = []
    quarantine_rows: list[dict[str, Any]] = []
    reused_coordinate_count = 0

    for record in sorted(batch, key=lambda item: item.offset):
        fingerprint = record_fingerprint(record)
        existing = known_coordinates.get(record.coordinate)
        if existing is not None:
            if existing["fingerprint"] != fingerprint:
                raise LandingConsistencyError(
                    f"coordinate={record.coordinate!r} changed payload or key"
                )
            reused_coordinate_count += 1
            continue

        entry = _base_entry(record, fingerprint)
        envelope = _record_envelope(record)
        try:
            raw_event = json.loads(record.value.decode("utf-8"))
            if not isinstance(raw_event, dict):
                raise EventContractError("event payload must be a JSON object")
            event = validate_machine_event(raw_event)
            envelope["event"] = event
            event_id = event["event_id"]
            entry["event_id"] = event_id

            duplicate_of = accepted_events.get(event_id)
            if duplicate_of is None:
                entry["status"] = "accepted"
                accepted_events[event_id] = record.coordinate
                accepted_rows.append(envelope)
            else:
                entry["status"] = "duplicate_event_id"
                entry["duplicate_of"] = _coordinate_dict(duplicate_of)
                envelope["duplicate_of"] = _coordinate_dict(duplicate_of)
                duplicate_rows.append(envelope)
        except (UnicodeDecodeError, json.JSONDecodeError, EventContractError) as exc:
            entry["status"] = "quarantined"
            entry["validation_error"] = str(exc)
            quarantine_rows.append(
                {
                    **envelope,
                    "raw_value": record.value.decode("utf-8", errors="replace"),
                    "validation_error": str(exc),
                }
            )

        entries.append(entry)
        known_coordinates[record.coordinate] = entry

    committable_offsets = (
        {
            "topic": batch[0].topic,
            "partition": batch[0].partition,
            "next_offset": max(record.offset for record in batch) + 1,
        },
    )

    if not entries:
        return LandingResult(
            status="reused",
            input_record_count=len(batch),
            accepted_count=0,
            duplicate_event_count=0,
            quarantine_count=0,
            reused_coordinate_count=reused_coordinate_count,
            accepted_total=len(accepted_events),
            batch_path=None,
            committable_offsets=committable_offsets,
        )

    batch_path = _write_immutable_batch(
        root=root,
        records=batch,
        entries=entries,
        accepted_rows=accepted_rows,
        duplicate_rows=duplicate_rows,
        quarantine_rows=quarantine_rows,
    )
    if simulate_crash_after_rename:
        raise SimulatedCrashAfterLanding(
            f"simulated crash after durable landing at {batch_path}"
        )

    return LandingResult(
        status="landed",
        input_record_count=len(batch),
        accepted_count=len(accepted_rows),
        duplicate_event_count=len(duplicate_rows),
        quarantine_count=len(quarantine_rows),
        reused_coordinate_count=reused_coordinate_count,
        accepted_total=len(accepted_events),
        batch_path=str(batch_path),
        committable_offsets=committable_offsets,
    )


def _validate_batch_scope(records: list[KafkaRecord]) -> None:
    topics = {record.topic for record in records}
    partitions = {record.partition for record in records}
    if len(topics) != 1 or len(partitions) != 1:
        raise ValueError("K1 landing batch must contain one topic and one partition")
    topic = records[0].topic
    if not TOPIC_RE.fullmatch(topic):
        raise ValueError(f"unsafe Kafka topic name: {topic!r}")
    if records[0].partition < 0:
        raise ValueError("partition must be >= 0")

    seen: dict[tuple[str, int, int], str] = {}
    for record in records:
        if record.offset < 0:
            raise ValueError("offset must be >= 0")
        fingerprint = record_fingerprint(record)
        previous = seen.get(record.coordinate)
        if previous is not None and previous != fingerprint:
            raise LandingConsistencyError(
                f"input contains conflicting coordinate={record.coordinate!r}"
            )
        if previous is not None:
            raise ValueError(f"input repeats coordinate={record.coordinate!r}")
        seen[record.coordinate] = fingerprint


def _base_entry(record: KafkaRecord, fingerprint: str) -> dict[str, Any]:
    return {
        "topic": record.topic,
        "partition": record.partition,
        "offset": record.offset,
        "key": record.key,
        "timestamp_ms": record.timestamp_ms,
        "fingerprint": fingerprint,
        "status": "pending",
        "event_id": None,
    }


def _record_envelope(record: KafkaRecord) -> dict[str, Any]:
    return {
        "kafka": {
            "topic": record.topic,
            "partition": record.partition,
            "offset": record.offset,
            "key": record.key,
            "timestamp_ms": record.timestamp_ms,
        }
    }


def _coordinate_dict(coordinate: tuple[str, int, int]) -> dict[str, Any]:
    return {
        "topic": coordinate[0],
        "partition": coordinate[1],
        "offset": coordinate[2],
    }


def _write_immutable_batch(
    *,
    root: Path,
    records: list[KafkaRecord],
    entries: list[dict[str, Any]],
    accepted_rows: list[dict[str, Any]],
    duplicate_rows: list[dict[str, Any]],
    quarantine_rows: list[dict[str, Any]],
) -> Path:
    topic = records[0].topic
    partition = records[0].partition
    offsets = [entry["offset"] for entry in entries]
    digest = hashlib.sha256(
        json.dumps(entries, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:12]
    batch_id = f"{min(offsets):020d}-{max(offsets):020d}-{digest}"
    partition_dir = root / f"topic={topic}" / f"partition={partition:05d}"
    final_dir = partition_dir / f"batch={batch_id}"
    staging_dir = root / ".staging" / f"{batch_id}-{uuid4().hex}"

    staging_dir.mkdir(parents=True, exist_ok=False)
    try:
        _write_jsonl(staging_dir / "accepted.jsonl", accepted_rows)
        _write_jsonl(staging_dir / "duplicates.jsonl", duplicate_rows)
        _write_jsonl(staging_dir / "quarantine.jsonl", quarantine_rows)
        manifest = {
            "format_version": 1,
            "batch_id": batch_id,
            "topic": topic,
            "partition": partition,
            "input_offset_start": min(record.offset for record in records),
            "input_offset_end": max(record.offset for record in records),
            "input_record_count": len(records),
            "new_coordinate_count": len(entries),
            "accepted_count": len(accepted_rows),
            "duplicate_event_count": len(duplicate_rows),
            "quarantine_count": len(quarantine_rows),
            "entries": entries,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        _write_json(staging_dir / "manifest.json", manifest)
        _fsync_directory(staging_dir)

        partition_dir.mkdir(parents=True, exist_ok=True)
        if final_dir.exists():
            raise LandingConsistencyError(f"immutable batch already exists: {final_dir}")
        os.replace(staging_dir, final_dir)
        _fsync_directory(partition_dir)
    except Exception:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise

    return final_dir


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
