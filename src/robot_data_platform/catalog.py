from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from pymongo.database import Database

from robot_data_platform.ingest import IngestedFile, inspect_csv


CATALOG_SCHEMA_VERSION = 1


def ingest_dataset(
    db: Database,
    dataset_id: str,
    file_path: str | Path,
    description: str | None = None,
) -> dict:
    inspected = inspect_csv(file_path)
    existing_version = db.dataset_versions.find_one(
        {"dataset_id": dataset_id, "source_hash": inspected.source_hash},
        {"_id": 0},
    )
    if existing_version:
        return get_dataset(db, dataset_id)

    version = next_version(db, dataset_id)
    now = datetime.now(timezone.utc)

    dataset_doc = {
        "dataset_id": dataset_id,
        "description": description or "",
        "latest_version": version,
        "schema": inspected.schema,
        "schema_version": CATALOG_SCHEMA_VERSION,
        "created_at": now,
        "updated_at": now,
    }

    existing = db.datasets.find_one({"dataset_id": dataset_id})
    if existing:
        db.datasets.update_one(
            {"dataset_id": dataset_id},
            {
                "$set": {
                    "description": description
                    if description is not None
                    else existing.get("description", ""),
                    "latest_version": version,
                    "schema": inspected.schema,
                    "schema_version": CATALOG_SCHEMA_VERSION,
                    "updated_at": now,
                }
            },
        )
    else:
        db.datasets.insert_one(dataset_doc)

    version_doc = build_version_doc(dataset_id, version, inspected)
    db.dataset_versions.insert_one(version_doc)

    return get_dataset(db, dataset_id)


def next_version(db: Database, dataset_id: str) -> str:
    count = db.dataset_versions.count_documents({"dataset_id": dataset_id})
    return f"v{count + 1}"


def build_version_doc(
    dataset_id: str,
    version: str,
    inspected: IngestedFile,
) -> dict:
    return {
        "dataset_id": dataset_id,
        "version": version,
        "source": str(inspected.path),
        "source_hash": inspected.source_hash,
        "schema_hash": inspected.schema_hash,
        "row_count": inspected.row_count,
        "stats": {"null_counts": inspected.null_counts},
        "ingested_at": inspected.ingested_at,
    }


def list_datasets(db: Database) -> list[dict]:
    return [
        serialize_dataset(doc)
        for doc in db.datasets.find({}, {"_id": 0}).sort("dataset_id", 1)
    ]


def get_dataset(db: Database, dataset_id: str) -> dict | None:
    dataset = db.datasets.find_one({"dataset_id": dataset_id}, {"_id": 0})
    if not dataset:
        return None

    versions = list(
        db.dataset_versions.find({"dataset_id": dataset_id}, {"_id": 0}).sort(
            "version", 1
        )
    )
    serialized = serialize_dataset(dataset)
    serialized["versions"] = [serialize_version(version) for version in versions]
    return serialized


def serialize_dataset(doc: dict) -> dict:
    serialized = dict(doc)
    for key in ("created_at", "updated_at"):
        if key in serialized and hasattr(serialized[key], "isoformat"):
            serialized[key] = serialized[key].isoformat()
    return serialized


def serialize_version(doc: dict) -> dict:
    serialized = dict(doc)
    if "ingested_at" in serialized and hasattr(serialized["ingested_at"], "isoformat"):
        serialized["ingested_at"] = serialized["ingested_at"].isoformat()
    return serialized
