"""K1.5 bridge: immutable accepted Kafka landing -> deterministic batch input.

The adapter turns the accepted JSONL evidence produced by K1 into a canonical CSV
for exactly one explicit ``business_date``, then hands that CSV to the existing
JSON-backed lakehouse pipeline. It reads immutable files only: no Kafka client,
no broker, and no Structured Streaming are involved.

Two boundaries are deliberate:

* Identity is the SHA-256 of the canonical CSV, and the CSV carries ``event_id``
  plus the Kafka coordinate, so provenance changes change the source identity.
* The K1 fingerprint hashes the original Kafka record bytes. A normalized accepted
  envelope does not necessarily reproduce those bytes, so this module carries the
  fingerprint forward and cross-checks only the fields visible in both the envelope
  and the manifest. It does not claim a cryptographic payload-integrity chain.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import shutil
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import uuid4

from manufacturing_data_platform.kafka_ingestion.contracts import (
    EventContractError,
    validate_machine_event,
)
from manufacturing_data_platform.pipeline.lakehouse import (
    REQUIRED_COLUMNS,
    run_lakehouse_pipeline,
)


FORMAT_VERSION = 1

# Kafka provenance travels with the business columns so it survives into the
# source hash and bronze. Silver/gold keep projecting only their own columns.
KAFKA_PROVENANCE_COLUMNS = (
    "event_id",
    "schema_version",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_key",
    "kafka_timestamp_ms",
    "kafka_record_fingerprint",
)

CANONICAL_COLUMNS = (*REQUIRED_COLUMNS, *KAFKA_PROVENANCE_COLUMNS)

# Fixed on purpose: the canonical bytes must not depend on the host platform.
CSV_LINE_TERMINATOR = "\n"

ADAPTER_CSV_NAME = "manufacturing_events.csv"
PROVENANCE_NAME = "provenance.json"

CLAIM_BOUNDARY = {
    "supports": [
        "bounded local bridge from immutable accepted Kafka landing to one business_date batch",
        "deterministic canonical CSV whose SHA-256 is the lakehouse source identity",
        "event_id and topic/partition/offset provenance preserved in source and bronze",
        "reuse of the existing quality/gold pipeline and its source-hash rerun contract",
    ],
    "does_not_support": [
        "continuous streaming or Spark Structured Streaming",
        "direct Kafka-to-Iceberg sink",
        "end-to-end exactly-once",
        "column-level Kafka lineage in silver/gold",
        "cryptographic end-to-end payload integrity",
        "concurrent adapter writers",
        "production/HA/scale operation",
    ],
}


class AdapterError(RuntimeError):
    """Base error for the K1.5 bridge."""


class LandingIntegrityError(AdapterError):
    """Accepted landing evidence disagrees with its sibling manifest."""


class NoEligibleEventsError(AdapterError):
    """No accepted event exists for the explicitly requested business_date."""


class AdapterConsistencyError(AdapterError):
    """A persisted adapter version conflicts with the recomputed canonical bytes."""


@dataclass(frozen=True)
class SelectedEvent:
    topic: str
    partition: int
    offset: int
    key: str | None
    timestamp_ms: int | None
    fingerprint: str
    event_id: str
    event: dict[str, Any]
    manifest_rel: str

    @property
    def sort_key(self) -> tuple[str, int, int]:
        return (self.topic, self.partition, self.offset)

    @property
    def coordinate(self) -> dict[str, Any]:
        return {"topic": self.topic, "partition": self.partition, "offset": self.offset}


@dataclass(frozen=True)
class AdapterResult:
    status: str  # "created" | "reused"
    business_date: str
    source_hash: str
    version_dir: Path
    csv_path: Path
    provenance_path: Path
    selected_event_count: int
    provenance: dict[str, Any]


# --------------------------------------------------------------------------- #
# Discovery + manifest cross-check
# --------------------------------------------------------------------------- #
def discover_accepted_events(landing_dir: str | Path) -> list[SelectedEvent]:
    """Read every accepted envelope and cross-check it against its manifest."""
    root = Path(landing_dir)
    if not root.is_dir():
        raise LandingIntegrityError(f"landing directory does not exist: {root}")

    manifest_paths = sorted(root.glob("topic=*/partition=*/batch=*/manifest.json"))
    if not manifest_paths:
        raise LandingIntegrityError(f"no K1 landing manifests found under {root}")

    events: list[SelectedEvent] = []
    seen_coordinates: dict[tuple[str, int, int], str] = {}
    seen_event_ids: dict[str, tuple[str, int, int]] = {}
    for manifest_path in manifest_paths:
        events.extend(
            _read_accepted_batch(root, manifest_path, seen_coordinates, seen_event_ids)
        )
    return events


def _read_accepted_batch(
    root: Path,
    manifest_path: Path,
    seen_coordinates: dict[tuple[str, int, int], str],
    seen_event_ids: dict[str, tuple[str, int, int]],
) -> list[SelectedEvent]:
    manifest = _read_json(manifest_path)
    if not isinstance(manifest, dict):
        raise LandingIntegrityError(f"{manifest_path}: manifest must be a JSON object")

    raw_entries = manifest.get("entries")
    if not isinstance(raw_entries, list):
        raise LandingIntegrityError(f"{manifest_path}: manifest entries must be a list")

    entries: dict[tuple[str, int, int], dict[str, Any]] = {}
    for entry in raw_entries:
        if not isinstance(entry, dict):
            raise LandingIntegrityError(
                f"{manifest_path}: every manifest entry must be a JSON object"
            )
        try:
            coordinate = (entry["topic"], int(entry["partition"]), int(entry["offset"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise LandingIntegrityError(
                f"{manifest_path}: manifest entry has an unusable Kafka coordinate"
            ) from exc
        if coordinate in entries:
            raise LandingIntegrityError(
                f"{manifest_path}: manifest repeats coordinate {coordinate}"
            )
        entries[coordinate] = entry

    accepted_path = manifest_path.parent / "accepted.jsonl"
    if not accepted_path.exists():
        raise LandingIntegrityError(f"missing accepted.jsonl next to {manifest_path}")

    rows = _read_jsonl(accepted_path)
    accepted_entries = [c for c, e in entries.items() if e.get("status") == "accepted"]
    declared_accepted_count = manifest.get("accepted_count")
    if (
        isinstance(declared_accepted_count, bool)
        or not isinstance(declared_accepted_count, int)
        or declared_accepted_count != len(accepted_entries)
        or declared_accepted_count != len(rows)
    ):
        raise LandingIntegrityError(
            f"{accepted_path}: holds {len(rows)} rows; manifest accepted_count="
            f"{declared_accepted_count!r}; accepted entries={len(accepted_entries)}"
        )

    manifest_rel = manifest_path.relative_to(root).as_posix()
    return [
        _cross_check_envelope(
            row, entries, accepted_path, manifest_rel, seen_coordinates, seen_event_ids
        )
        for row in rows
    ]


def _cross_check_envelope(
    row: dict[str, Any],
    entries: dict[tuple[str, int, int], dict[str, Any]],
    accepted_path: Path,
    manifest_rel: str,
    seen_coordinates: dict[tuple[str, int, int], str],
    seen_event_ids: dict[str, tuple[str, int, int]],
) -> SelectedEvent:
    kafka = row.get("kafka")
    event = row.get("event")
    if not isinstance(kafka, dict) or not isinstance(event, dict):
        raise LandingIntegrityError(
            f"{accepted_path}: accepted envelope must carry 'kafka' and 'event' objects"
        )

    try:
        coordinate = (kafka["topic"], int(kafka["partition"]), int(kafka["offset"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise LandingIntegrityError(
            f"{accepted_path}: accepted envelope has an unusable Kafka coordinate"
        ) from exc

    entry = entries.get(coordinate)
    if entry is None:
        raise LandingIntegrityError(
            f"{accepted_path}: coordinate {coordinate} is absent from {manifest_rel}"
        )
    if entry.get("status") != "accepted":
        raise LandingIntegrityError(
            f"{accepted_path}: coordinate {coordinate} has manifest status "
            f"{entry.get('status')!r}, so it is not batch input"
        )

    try:
        normalized = validate_machine_event(event)
    except EventContractError as exc:
        raise LandingIntegrityError(
            f"{accepted_path}: coordinate {coordinate} violates the v1 event contract: {exc}"
        ) from exc

    # Only the fields present in BOTH files can be cross-checked. The fingerprint
    # covers the original record bytes, which a normalized envelope cannot reproduce.
    for field, envelope_value, manifest_value in (
        ("event_id", normalized["event_id"], entry.get("event_id")),
        ("key", kafka.get("key"), entry.get("key")),
        ("timestamp_ms", kafka.get("timestamp_ms"), entry.get("timestamp_ms")),
    ):
        if envelope_value != manifest_value:
            raise LandingIntegrityError(
                f"{accepted_path}: coordinate {coordinate} {field}={envelope_value!r} "
                f"disagrees with manifest {field}={manifest_value!r}"
            )

    fingerprint = entry.get("fingerprint")
    if not isinstance(fingerprint, str) or not fingerprint:
        raise LandingIntegrityError(
            f"{accepted_path}: coordinate {coordinate} has no manifest fingerprint"
        )

    if coordinate in seen_coordinates:
        raise LandingIntegrityError(
            f"coordinate {coordinate} is accepted in more than one immutable batch"
        )
    seen_coordinates[coordinate] = fingerprint

    event_id = normalized["event_id"]
    previous_coordinate = seen_event_ids.get(event_id)
    if previous_coordinate is not None and previous_coordinate != coordinate:
        raise LandingIntegrityError(
            f"event_id={event_id!r} is accepted at {previous_coordinate} and {coordinate}"
        )
    seen_event_ids[event_id] = coordinate

    return SelectedEvent(
        topic=coordinate[0],
        partition=coordinate[1],
        offset=coordinate[2],
        key=kafka.get("key"),
        timestamp_ms=kafka.get("timestamp_ms"),
        fingerprint=fingerprint,
        event_id=event_id,
        event=normalized,
        manifest_rel=manifest_rel,
    )


# --------------------------------------------------------------------------- #
# Explicit-date selection + canonical serialization
# --------------------------------------------------------------------------- #
def select_for_business_date(
    events: list[SelectedEvent],
    business_date: str,
) -> list[SelectedEvent]:
    """Select one explicit date. The date is never inferred from the first row."""
    validate_business_date(business_date)
    selected = [item for item in events if item.event["business_date"] == business_date]
    if not selected:
        raise NoEligibleEventsError(
            f"no accepted Kafka event for business_date={business_date}; "
            "refusing to invoke the lakehouse pipeline"
        )
    selected.sort(key=lambda item: item.sort_key)
    selected_scopes = {(item.topic, item.partition) for item in selected}
    if len(selected_scopes) != 1:
        raise LandingIntegrityError(
            "K1.5 accepts one topic/partition per business_date; "
            f"found {sorted(selected_scopes)!r}"
        )
    return selected


def validate_business_date(business_date: str) -> None:
    if not isinstance(business_date, str) or not business_date.strip():
        raise ValueError("business_date must be a non-empty ISO date string")
    try:
        date.fromisoformat(business_date)
    except ValueError as exc:
        raise ValueError(f"business_date must be an ISO date: {business_date!r}") from exc


def canonical_csv_bytes(selected: list[SelectedEvent]) -> bytes:
    """Serialize the canonical CSV. Ordering and bytes must not depend on discovery."""
    ordered = sorted(selected, key=lambda item: item.sort_key)
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(
        buffer,
        fieldnames=list(CANONICAL_COLUMNS),
        lineterminator=CSV_LINE_TERMINATOR,
        extrasaction="raise",
    )
    writer.writeheader()
    for item in ordered:
        writer.writerow(_canonical_row(item))
    return buffer.getvalue().encode("utf-8")


def _canonical_row(item: SelectedEvent) -> dict[str, Any]:
    row = {column: item.event[column] for column in REQUIRED_COLUMNS}
    row["event_id"] = item.event_id
    row["schema_version"] = item.event["schema_version"]
    row["kafka_topic"] = item.topic
    row["kafka_partition"] = item.partition
    row["kafka_offset"] = item.offset
    row["kafka_key"] = "" if item.key is None else item.key
    row["kafka_timestamp_ms"] = "" if item.timestamp_ms is None else item.timestamp_ms
    row["kafka_record_fingerprint"] = item.fingerprint
    return row


def source_hash_for(csv_bytes: bytes) -> str:
    return sha256(csv_bytes).hexdigest()


def build_provenance(
    business_date: str,
    source_hash: str,
    selected: list[SelectedEvent],
) -> dict[str, Any]:
    """Provenance holds no wall-clock value, so an adapter version is reproducible."""
    return {
        "format_version": FORMAT_VERSION,
        "business_date": business_date,
        "source_hash": source_hash,
        "selected_event_count": len(selected),
        "canonical_columns": list(CANONICAL_COLUMNS),
        "landing_manifest_paths": sorted({item.manifest_rel for item in selected}),
        "selected_coordinates": [item.coordinate for item in selected],
        "selected_event_ids": [item.event_id for item in selected],
        "source_record_fingerprints": [
            {**item.coordinate, "fingerprint": item.fingerprint} for item in selected
        ],
        "claim_boundary": CLAIM_BOUNDARY,
    }


# --------------------------------------------------------------------------- #
# Immutable content-addressed output
# --------------------------------------------------------------------------- #
def write_adapter_version(
    adapter_output_dir: str | Path,
    business_date: str,
    source_hash: str,
    csv_bytes: bytes,
    provenance: dict[str, Any],
) -> tuple[Path, str]:
    """Write staging, fsync, then rename on the same local filesystem.

    Same durability caveat as K1: verified on a local Linux filesystem only. No
    power-loss, NFS, or object-store claim, and single-writer scope.
    """
    base = Path(adapter_output_dir)
    final_dir = base / f"business_date={business_date}" / f"source_hash={source_hash}"
    if final_dir.exists():
        _assert_existing_version_matches(final_dir, csv_bytes, provenance)
        return final_dir, "reused"

    staging_dir = base / ".staging" / f"{source_hash[:12]}-{uuid4().hex}"
    staging_dir.mkdir(parents=True, exist_ok=False)
    try:
        _write_bytes(staging_dir / ADAPTER_CSV_NAME, csv_bytes)
        _write_json(staging_dir / PROVENANCE_NAME, provenance)
        _fsync_directory(staging_dir)

        final_dir.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staging_dir, final_dir)
        _fsync_directory(final_dir.parent)
    except Exception:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise
    return final_dir, "created"


def _assert_existing_version_matches(
    final_dir: Path,
    csv_bytes: bytes,
    provenance: dict[str, Any],
) -> None:
    csv_path = final_dir / ADAPTER_CSV_NAME
    if not csv_path.exists():
        raise AdapterConsistencyError(
            f"adapter version {final_dir} exists without {ADAPTER_CSV_NAME}"
        )
    if csv_path.read_bytes() != csv_bytes:
        raise AdapterConsistencyError(
            f"adapter version {final_dir} does not match the recomputed canonical CSV"
        )

    provenance_path = final_dir / PROVENANCE_NAME
    if not provenance_path.exists():
        raise AdapterConsistencyError(
            f"adapter version {final_dir} exists without {PROVENANCE_NAME}"
        )
    try:
        persisted_provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise AdapterConsistencyError(
            f"adapter version {final_dir} has invalid {PROVENANCE_NAME}"
        ) from exc
    if persisted_provenance != provenance:
        raise AdapterConsistencyError(
            f"adapter version {final_dir} does not match the recomputed provenance"
        )


def adapt_landing_to_batch(
    *,
    landing_dir: str | Path,
    business_date: str,
    adapter_output_dir: str | Path,
) -> AdapterResult:
    """Discover, cross-check, select one date, and persist an immutable version."""
    validate_business_date(business_date)
    events = discover_accepted_events(landing_dir)
    selected = select_for_business_date(events, business_date)
    csv_bytes = canonical_csv_bytes(selected)
    source_hash = source_hash_for(csv_bytes)
    provenance = build_provenance(business_date, source_hash, selected)
    version_dir, status = write_adapter_version(
        adapter_output_dir, business_date, source_hash, csv_bytes, provenance
    )
    return AdapterResult(
        status=status,
        business_date=business_date,
        source_hash=source_hash,
        version_dir=version_dir,
        csv_path=version_dir / ADAPTER_CSV_NAME,
        provenance_path=version_dir / PROVENANCE_NAME,
        selected_event_count=len(selected),
        provenance=provenance,
    )


# --------------------------------------------------------------------------- #
# Bridge: adapter -> existing lakehouse pipeline
# --------------------------------------------------------------------------- #
def run_bridge(
    *,
    landing_dir: str | Path,
    business_date: str,
    adapter_output_dir: str | Path,
    lakehouse_output_dir: str | Path,
    evidence_file: str | Path | None = None,
) -> dict[str, Any]:
    """Adapt first; only a valid adapter version may reach the lakehouse pipeline."""
    adapter = adapt_landing_to_batch(
        landing_dir=landing_dir,
        business_date=business_date,
        adapter_output_dir=adapter_output_dir,
    )

    result = run_lakehouse_pipeline(
        raw_path=adapter.csv_path,
        output_dir=lakehouse_output_dir,
        business_date=business_date,
        catalog_backend="json",
    )

    # The adapter CSV IS the pipeline source, so the two identities must agree.
    if result.source_hash != adapter.source_hash:
        raise AdapterConsistencyError(
            f"lakehouse source_hash={result.source_hash} does not match adapter "
            f"source_hash={adapter.source_hash}"
        )

    evidence = build_evidence(adapter, result)
    if evidence_file is not None:
        _write_json(Path(evidence_file), evidence)
    return evidence


def build_evidence(adapter: AdapterResult, result: Any) -> dict[str, Any]:
    gold_rows = _read_csv_rows(result.paths.gold_path)
    return {
        "slice": "kafka-k1.5-landing-to-batch-bridge",
        "business_date": adapter.business_date,
        "adapter": {
            "status": adapter.status,
            "source_hash": adapter.source_hash,
            "selected_event_count": adapter.selected_event_count,
            "version_dir": str(adapter.version_dir),
            "csv_path": str(adapter.csv_path),
            "provenance_path": str(adapter.provenance_path),
            "landing_manifest_paths": adapter.provenance["landing_manifest_paths"],
            "selected_coordinates": adapter.provenance["selected_coordinates"],
            "selected_event_ids": adapter.provenance["selected_event_ids"],
        },
        "lakehouse": {
            "status": result.status,
            "run_id": result.run_id,
            "dataset_id": result.dataset_id,
            "source_hash": result.source_hash,
            "schema_hash": result.schema_hash,
            "quality_passed": result.quality_passed,
            "quality_checks": [
                {"name": check["name"], "status": check["status"]}
                for check in result.quality_checks
            ],
            "paths": {
                "raw": str(result.paths.raw_path),
                "bronze": str(result.paths.bronze_path),
                "silver": str(result.paths.silver_path),
                "gold": str(result.paths.gold_path),
                "quality": str(result.paths.quality_path),
            },
            "row_counts": {
                "selected_events": adapter.selected_event_count,
                "silver": len(_read_csv_rows(result.paths.silver_path)),
                "gold": len(gold_rows),
            },
            "gold_rows": gold_rows,
        },
        "claim_boundary": CLAIM_BOUNDARY,
    }


# --------------------------------------------------------------------------- #
# Small IO utilities
# --------------------------------------------------------------------------- #
def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise LandingIntegrityError(f"{path} is not valid JSON") from exc


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise LandingIntegrityError(f"{path}: line {number} is not valid JSON") from exc
        if not isinstance(row, dict):
            raise LandingIntegrityError(
                f"{path}: line {number} must be a JSON object"
            )
        rows.append(row)
    return rows


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_bytes(path: Path, payload: bytes) -> None:
    with path.open("wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
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


# --------------------------------------------------------------------------- #
# Bounded CLI
# --------------------------------------------------------------------------- #
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Adapt an immutable accepted Kafka landing into one business_date batch "
            "and run the existing JSON-backed lakehouse pipeline."
        )
    )
    parser.add_argument("--landing-dir", required=True)
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--adapter-output-dir", required=True)
    parser.add_argument("--lakehouse-output-dir", required=True)
    parser.add_argument("--evidence-file", default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    evidence = run_bridge(
        landing_dir=args.landing_dir,
        business_date=args.business_date,
        adapter_output_dir=args.adapter_output_dir,
        lakehouse_output_dir=args.lakehouse_output_dir,
        evidence_file=args.evidence_file,
    )
    print(json.dumps(evidence, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
