from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
import csv
import json


@dataclass(frozen=True)
class IngestedFile:
    path: Path
    schema: list[dict[str, str]]
    schema_hash: str
    source_hash: str
    row_count: int
    null_counts: dict[str, int]
    ingested_at: datetime


def inspect_csv(path: str | Path) -> IngestedFile:
    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(f"File not found: {csv_path}")
    if csv_path.suffix.lower() != ".csv":
        raise ValueError("v0 supports CSV ingest only")

    columns, rows = read_csv(csv_path)
    schema = infer_schema(columns, rows)
    null_counts = count_nulls(columns, rows)

    return IngestedFile(
        path=csv_path,
        schema=schema,
        schema_hash=hash_schema(schema),
        source_hash=hash_file(csv_path),
        row_count=len(rows),
        null_counts=null_counts,
        ingested_at=datetime.now(timezone.utc),
    )


def read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    """헤더(컬럼)와 데이터 행을 분리해 읽는다. 0행이어도 컬럼은 보존된다."""
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = list(reader.fieldnames or [])
        rows = list(reader)
    return columns, rows


def infer_schema(columns: list[str], rows: list[dict[str, str]]) -> list[dict[str, str]]:
    # 데이터가 없어도(헤더만) 컬럼은 카탈로그에 남긴다. 타입은 추론 불가 시 "unknown".
    return [
        {
            "name": column,
            "type": infer_column_type([row.get(column, "") for row in rows]) if rows else "unknown",
        }
        for column in columns
    ]


def infer_column_type(values: list[str]) -> str:
    non_null_values = [value for value in values if value not in ("", None)]
    if not non_null_values:
        return "string"
    if all(is_integer(value) for value in non_null_values):
        return "integer"
    if all(is_float(value) for value in non_null_values):
        return "float"
    if all(is_datetime_like(value) for value in non_null_values):
        return "datetime"
    return "string"


def count_nulls(columns: list[str], rows: list[dict[str, str]]) -> dict[str, int]:
    counts = {
        column: sum(1 for row in rows if row.get(column) in ("", None))
        for column in columns
    }
    return {column: count for column, count in counts.items() if count > 0}


def is_integer(value: str) -> bool:
    try:
        int(value)
    except ValueError:
        return False
    return True


def is_float(value: str) -> bool:
    try:
        float(value)
    except ValueError:
        return False
    return True


def is_datetime_like(value: str) -> bool:
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def hash_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def hash_schema(schema: list[dict[str, str]]) -> str:
    canonical = json.dumps(schema, sort_keys=True, separators=(",", ":"))
    return sha256(canonical.encode("utf-8")).hexdigest()
