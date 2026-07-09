from pymongo import MongoClient
from pymongo.database import Database

from manufacturing_data_platform.config import Settings, get_settings


def get_database(settings: Settings | None = None) -> Database:
    active_settings = settings or get_settings()
    client = MongoClient(active_settings.mongo_uri)
    db = client[active_settings.mongo_db]
    ensure_indexes(db)
    return db


def ensure_indexes(db: Database) -> None:
    db.datasets.create_index("dataset_id", unique=True)
    db.dataset_versions.create_index(
        [("dataset_id", 1), ("version", 1)],
        unique=True,
    )
    db.dataset_versions.create_index("source_hash")
    db.lakehouse_runs.create_index("run_id", unique=True)
    db.lakehouse_runs.create_index([("dataset_id", 1), ("business_date", 1)])
    # Idempotency lookup: a successful run for the same source content + date.
    db.lakehouse_runs.create_index(
        [("dataset_id", 1), ("business_date", 1), ("source_hash", 1)]
    )
    db.lineage_events.create_index(
        [("dataset_id", 1), ("run_id", 1)],
        unique=True,
    )
