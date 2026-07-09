from pathlib import Path

import mongomock

from manufacturing_data_platform.catalog import get_dataset, ingest_dataset, list_datasets
from manufacturing_data_platform.db import ensure_indexes


def test_ingest_registers_dataset_and_version_documents(tmp_path: Path) -> None:
    csv_path = tmp_path / "sensor.csv"
    csv_path.write_text(
        "timestamp,sensor_id,value\n"
        "2026-06-26T09:00:00Z,s-1,10.5\n"
        "2026-06-26T09:00:01Z,s-1,\n",
        encoding="utf-8",
    )
    db = mongomock.MongoClient()["test_manufacturing_data_platform"]
    ensure_indexes(db)

    body = ingest_dataset(
        db,
        dataset_id="temp_sensor",
        file_path=csv_path,
        description="temperature sensor",
    )

    assert body["dataset_id"] == "temp_sensor"
    assert body["latest_version"] == "v1"
    assert body["schema_version"] == 1
    assert body["versions"][0]["row_count"] == 2
    assert body["versions"][0]["stats"]["null_counts"] == {"value": 1}
    assert len(body["versions"][0]["source_hash"]) == 64
    assert len(body["versions"][0]["schema_hash"]) == 64

    stored = get_dataset(db, "temp_sensor")
    assert stored is not None
    assert stored["versions"][0]["version"] == "v1"


def test_list_datasets_returns_registered_catalog_entries(tmp_path: Path) -> None:
    csv_path = tmp_path / "sensor.csv"
    csv_path.write_text("timestamp,sensor_id,value\nnow,s-1,1\n", encoding="utf-8")
    db = mongomock.MongoClient()["test_manufacturing_data_platform"]
    ensure_indexes(db)

    ingest_dataset(db, dataset_id="temp_sensor", file_path=csv_path)
    datasets = list_datasets(db)

    assert datasets[0]["dataset_id"] == "temp_sensor"


def test_missing_dataset_returns_none() -> None:
    db = mongomock.MongoClient()["test_manufacturing_data_platform"]
    ensure_indexes(db)

    assert get_dataset(db, "missing") is None


def test_reingesting_same_file_is_idempotent(tmp_path: Path) -> None:
    csv_path = tmp_path / "sensor.csv"
    csv_path.write_text("timestamp,sensor_id,value\nnow,s-1,1\n", encoding="utf-8")
    db = mongomock.MongoClient()["test_manufacturing_data_platform"]
    ensure_indexes(db)

    first = ingest_dataset(db, dataset_id="temp_sensor", file_path=csv_path)
    second = ingest_dataset(db, dataset_id="temp_sensor", file_path=csv_path)

    assert first["latest_version"] == "v1"
    assert second["latest_version"] == "v1"
    assert len(second["versions"]) == 1


def test_ingesting_changed_file_creates_next_version(tmp_path: Path) -> None:
    first_path = tmp_path / "sensor_v1.csv"
    first_path.write_text("timestamp,sensor_id,value\nnow,s-1,1\n", encoding="utf-8")
    second_path = tmp_path / "sensor_v2.csv"
    second_path.write_text(
        "timestamp,sensor_id,value,battery_pct\nnow,s-1,1,98\n",
        encoding="utf-8",
    )
    db = mongomock.MongoClient()["test_manufacturing_data_platform"]
    ensure_indexes(db)

    ingest_dataset(db, dataset_id="temp_sensor", file_path=first_path)
    dataset = ingest_dataset(db, dataset_id="temp_sensor", file_path=second_path)

    assert dataset["latest_version"] == "v2"
    assert len(dataset["versions"]) == 2
    assert dataset["versions"][0]["schema_hash"] != dataset["versions"][1]["schema_hash"]


def test_header_only_csv_preserves_columns(tmp_path: Path) -> None:
    csv_path = tmp_path / "empty.csv"
    csv_path.write_text("timestamp,sensor_id,value\n", encoding="utf-8")  # 헤더만, 0행
    db = mongomock.MongoClient()["test_manufacturing_data_platform"]
    ensure_indexes(db)

    body = ingest_dataset(db, dataset_id="temp_sensor", file_path=csv_path)

    # 데이터 0행이어도 컬럼은 카탈로그에 보존돼야 함 (카탈로그 본질)
    assert [c["name"] for c in body["schema"]] == ["timestamp", "sensor_id", "value"]
    assert all(c["type"] == "unknown" for c in body["schema"])
    assert body["versions"][0]["row_count"] == 0
