from __future__ import annotations

import csv
import json
import shutil
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pymongo.database import Database

from manufacturing_data_platform.domain import ACCEPTED_OPERATIONS
from manufacturing_data_platform.ingest import hash_file, hash_schema, infer_schema
from manufacturing_data_platform.pipeline.models import PipelinePaths, PipelineResult
from manufacturing_data_platform.pipeline.sample_data import ensure_sample_manufacturing_csv


DATASET_ID = "manufacturing_daily_metrics"

REQUIRED_COLUMNS = [
    "event_time",
    "plant_id",
    "line_id",
    "work_order_id",
    "machine_id",
    "product_code",
    "operation",
    "units_produced",
    "defect_count",
    "cycle_time_ms",
    "business_date",
]

# The grain of a silver event. Used for dedup and for the uniqueness check.
NATURAL_KEY_COLUMNS = ("work_order_id", "machine_id", "event_time")

# Schema-drift policy. "warn" surfaces drift in the quality report without
# failing the run, so legitimate schema evolution is not blocked (Iceberg-style
# schema evolution philosophy). Switch to "fail" to make drift a hard gate.
SCHEMA_DRIFT_POLICY = "warn"


def run_lakehouse_pipeline(
    raw_path: str | Path = "data/raw/manufacturing_events.csv",
    output_dir: str | Path = "data/lakehouse",
    business_date: str | None = None,
    db: Database | None = None,
    catalog_backend: str = "mongo",
) -> PipelineResult:
    if catalog_backend not in ("mongo", "json"):
        raise ValueError("catalog_backend must be 'mongo' or 'json'")
    if catalog_backend == "mongo" and db is None:
        raise ValueError("db is required when catalog_backend='mongo'")

    source_path = ensure_sample_manufacturing_csv(raw_path)
    columns, rows = read_rows(source_path)
    active_business_date = business_date or infer_business_date(rows)
    base_dir = Path(output_dir)

    source_hash = hash_file(source_path)
    # schema_hash is based on the ACTUAL CSV header, so an added/removed column
    # changes it (real schema drift) — not only a type change within the
    # required columns.
    schema = infer_schema(columns, rows)
    schema_hash = hash_schema(schema)

    # --- Idempotency gate ---
    # A successful run for the same (dataset_id, business_date, source content)
    # is reused instead of reprocessed. This makes retries and backfills safe:
    # re-running the same date+input is a no-op that returns the prior run.
    existing = find_existing_successful_run(
        DATASET_ID,
        active_business_date,
        source_hash,
        db=db,
        catalog_backend=catalog_backend,
        output_dir=base_dir,
    )
    if existing is not None:
        record_run_reuse(
            existing, db=db, catalog_backend=catalog_backend, output_dir=base_dir
        )
        return result_from_doc(existing, catalog_backend, status="skipped")

    run_id = build_run_id(active_business_date)
    paths = build_paths(base_dir, active_business_date, run_id, source_path)

    # --- Transform / IO are separated so a future engine swap (Spark) only
    # replaces the transform_* functions, not the orchestration. ---
    write_bronze(source_path, rows, paths, active_business_date, source_hash, schema_hash)

    silver_rows = transform_silver(rows, active_business_date, source_hash)
    write_silver(silver_rows, paths.silver_path)

    gold_rows = transform_gold(silver_rows, active_business_date)
    write_gold(gold_rows, paths.gold_path)

    # --- Schema drift vs the most recent successful run for this dataset ---
    previous_schema_hash = lookup_previous_schema_hash(
        DATASET_ID,
        db=db,
        catalog_backend=catalog_backend,
        output_dir=base_dir,
        current_run_id=run_id,
    )
    drift_check = build_schema_drift_check(previous_schema_hash, schema_hash)

    # --- Quality suite (dbt-style generic tests + reconciliation) ---
    quality_checks = build_quality_checks(
        rows, silver_rows, gold_rows, active_business_date
    )
    quality_checks.append(drift_check)
    quality_passed = not any(check["status"] == "fail" for check in quality_checks)
    write_quality_report(quality_checks, quality_passed, paths.quality_path)

    lineage_doc = build_lineage_doc(
        run_id=run_id,
        business_date=active_business_date,
        source_path=source_path,
        paths=paths,
        source_hash=source_hash,
        schema_hash=schema_hash,
        source_rows=len(rows),
        silver_rows=len(silver_rows),
        gold_rows=len(gold_rows),
        quality_passed=quality_passed,
        quality_checks=quality_checks,
        previous_schema_hash=previous_schema_hash,
        drift_status=drift_check["status"],
    )

    persist_catalog(
        lineage_doc,
        db=db,
        catalog_backend=catalog_backend,
        output_dir=base_dir,
        quality_passed=quality_passed,
    )

    return PipelineResult(
        run_id=run_id,
        dataset_id=DATASET_ID,
        business_date=active_business_date,
        source_hash=source_hash,
        schema_hash=schema_hash,
        paths=paths,
        quality_passed=quality_passed,
        quality_checks=quality_checks,
        catalog_backend=catalog_backend,
        status="processed",
    )


# --------------------------------------------------------------------------- #
# Read / partition helpers
# --------------------------------------------------------------------------- #
def read_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    """Return (actual CSV header columns, rows).

    The header drives ``schema_hash`` so an added/removed column is detected as
    schema drift. A missing required column is still a hard ``ValueError`` (the
    pipeline cannot build silver/gold without it) — that policy is unchanged.
    """
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = list(reader.fieldnames or [])
        rows = list(reader)
    missing = [c for c in REQUIRED_COLUMNS if c not in columns]
    if missing:
        raise ValueError(f"missing required columns: {', '.join(missing)}")
    return columns, rows


def infer_business_date(rows: list[dict[str, str]]) -> str:
    if not rows:
        return datetime.now(timezone.utc).date().isoformat()
    return rows[0]["business_date"]


def build_run_id(business_date: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{business_date}-{stamp}-{uuid4().hex[:8]}"


def build_paths(
    base_dir: Path,
    business_date: str,
    run_id: str,
    source_path: Path,
) -> PipelinePaths:
    run_dir = base_dir / f"business_date={business_date}" / f"run_id={run_id}"
    return PipelinePaths(
        raw_path=source_path,
        bronze_path=run_dir / "bronze" / "manufacturing_events.csv",
        silver_path=run_dir / "silver" / "manufacturing_events.csv",
        gold_path=run_dir / "gold" / "daily_line_metrics.csv",
        quality_path=run_dir / "quality" / "quality_report.json",
        manifest_path=run_dir / "manifest.json",
    )


# --------------------------------------------------------------------------- #
# Bronze
# --------------------------------------------------------------------------- #
def write_bronze(
    source_path: Path,
    rows: list[dict[str, str]],
    paths: PipelinePaths,
    business_date: str,
    source_hash: str,
    schema_hash: str,
) -> None:
    paths.bronze_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source_path, paths.bronze_path)

    manifest = {
        "dataset_id": DATASET_ID,
        "stage": "bronze",
        "source": str(source_path),
        "business_date": business_date,
        "source_hash": source_hash,
        "schema_hash": schema_hash,
        "row_count": len(rows),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    paths.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    paths.manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


# --------------------------------------------------------------------------- #
# Silver — transform is pure; write is IO only
# --------------------------------------------------------------------------- #
def transform_silver(
    rows: list[dict[str, str]],
    business_date: str,
    source_hash: str,
) -> list[dict]:
    """Filter to the active business_date, dedup on the natural key, normalize,
    and type-cast. Pure function: no file IO, so a Spark port only swaps the
    engine, not the orchestration.
    """
    silver_rows: list[dict] = []
    seen_keys: set[tuple[str, str, str]] = set()
    for row in rows:
        if row["business_date"] != business_date:
            continue
        key = (row["work_order_id"], row["machine_id"], row["event_time"])
        if key in seen_keys:
            continue
        seen_keys.add(key)
        silver_rows.append(
            {
                "event_time": row["event_time"],
                "business_date": row["business_date"],
                "plant_id": row["plant_id"].strip().lower(),
                "line_id": row["line_id"].strip().lower(),
                "work_order_id": row["work_order_id"].strip().lower(),
                "machine_id": row["machine_id"].strip().lower(),
                "product_code": row["product_code"].strip().lower(),
                "operation": row["operation"].strip().lower(),
                "units_produced": int(row["units_produced"]),
                "defect_count": int(row["defect_count"]),
                "cycle_time_ms": int(row["cycle_time_ms"]),
                "source_hash": source_hash,
            }
        )
    return silver_rows


def write_silver(silver_rows: list[dict], path: Path) -> None:
    write_csv(path, silver_rows)


# --------------------------------------------------------------------------- #
# Gold — transform is pure; write is IO only
# --------------------------------------------------------------------------- #
def transform_gold(silver_rows: list[dict], business_date: str) -> list[dict]:
    """Aggregate silver events into a daily line/product mart row. Pure."""
    grouped = defaultdict(
        lambda: {"units_produced": 0, "defect_count": 0, "cycle_time_ms": 0, "events": 0}
    )
    for row in silver_rows:
        key = (row["business_date"], row["plant_id"], row["line_id"], row["product_code"])
        grouped[key]["units_produced"] += row["units_produced"]
        grouped[key]["defect_count"] += row["defect_count"]
        grouped[key]["cycle_time_ms"] += row["cycle_time_ms"]
        grouped[key]["events"] += 1

    gold_rows: list[dict] = []
    for (row_date, plant_id, line_id, product_code), metrics in sorted(grouped.items()):
        units = metrics["units_produced"]
        defects = metrics["defect_count"]
        events = metrics["events"]
        gold_rows.append(
            {
                "business_date": row_date,
                "plant_id": plant_id,
                "line_id": line_id,
                "product_code": product_code,
                "units_produced": units,
                "defect_count": defects,
                "defect_rate": round(defects / units, 6) if units else 0,
                "avg_cycle_time_ms": round(metrics["cycle_time_ms"] / events, 2) if events else 0,
                "closing_status": "provisional",
            }
        )

    if not gold_rows:
        gold_rows.append(
            {
                "business_date": business_date,
                "plant_id": "",
                "line_id": "",
                "product_code": "",
                "units_produced": 0,
                "defect_count": 0,
                "defect_rate": 0,
                "avg_cycle_time_ms": 0,
                "closing_status": "provisional",
            }
        )
    return gold_rows


def write_gold(gold_rows: list[dict], path: Path) -> None:
    write_csv(path, gold_rows)


# --------------------------------------------------------------------------- #
# Quality — dbt-style generic tests + cross-layer reconciliation
# --------------------------------------------------------------------------- #
def make_check(name: str, status: str, expected, actual, detail: str) -> dict:
    return {
        "name": name,
        "status": status,  # "pass" | "fail" | "warn"
        "expected": expected,
        "actual": actual,
        "detail": detail,
    }


def build_quality_checks(
    source_rows: list[dict[str, str]],
    silver_rows: list[dict],
    gold_rows: list[dict],
    business_date: str,
) -> list[dict]:
    checks: list[dict] = []

    source_for_bd = [r for r in source_rows if r.get("business_date") == business_date]
    source_keys = [tuple(r[c] for c in NATURAL_KEY_COLUMNS) for r in source_for_bd]
    distinct_source_keys = len(set(source_keys))
    duplicates_in_source = len(source_keys) - distinct_source_keys

    # 1. Row reconciliation source -> silver (relationships/reconciliation).
    #    `expected` is computed INDEPENDENTLY of how silver was built: the count
    #    of distinct natural keys on the active date. A mismatch therefore means
    #    real row loss, not the expected date-filtering or dedup. The detail
    #    breaks down filtering vs dedup so a reviewer can see they are intended.
    checks.append(
        make_check(
            "row_count_source_to_silver",
            "pass" if len(silver_rows) == distinct_source_keys else "fail",
            distinct_source_keys,
            len(silver_rows),
            (
                f"source_total={len(source_rows)}, "
                f"for_business_date={len(source_for_bd)}, "
                f"distinct_natural_keys={distinct_source_keys}, "
                f"duplicates_in_source={duplicates_in_source}, "
                f"silver_rows={len(silver_rows)} "
                "(date filtering and dedup are expected; a mismatch here means "
                "unexpected row loss)"
            ),
        )
    )

    # 2. Unit/defect conservation silver -> gold. Aggregation must not create or
    #    lose totals.
    silver_units = sum(r["units_produced"] for r in silver_rows)
    gold_units = sum(r["units_produced"] for r in gold_rows)
    silver_defects = sum(r["defect_count"] for r in silver_rows)
    gold_defects = sum(r["defect_count"] for r in gold_rows)
    checks.append(
        make_check(
            "unit_conservation_silver_to_gold",
            "pass"
            if (silver_units == gold_units and silver_defects == gold_defects)
            else "fail",
            {"units_produced": silver_units, "defect_count": silver_defects},
            {"units_produced": gold_units, "defect_count": gold_defects},
            "gold aggregation must preserve total units_produced and defect_count from silver",
        )
    )

    # 3. not_null on required columns (dbt not_null), on the active-date source.
    null_counts = {
        col: sum(1 for r in source_for_bd if r.get(col) in (None, ""))
        for col in REQUIRED_COLUMNS
    }
    null_counts = {col: n for col, n in null_counts.items() if n}
    checks.append(
        make_check(
            "not_null_required_columns",
            "pass" if not null_counts else "fail",
            0,
            sum(null_counts.values()),
            f"null/empty counts per required column: {null_counts or 'none'}",
        )
    )

    # 4. unique on the natural key of the silver output (dbt unique). Guards the
    #    dedup logic: any duplicate surviving into silver is a regression.
    silver_keys = [tuple(r[c] for c in NATURAL_KEY_COLUMNS) for r in silver_rows]
    silver_dups = len(silver_keys) - len(set(silver_keys))
    checks.append(
        make_check(
            "unique_natural_key",
            "pass" if silver_dups == 0 else "fail",
            0,
            silver_dups,
            f"natural key = {NATURAL_KEY_COLUMNS}; duplicate keys in silver must be 0",
        )
    )

    # 5. accepted_values for `operation` (dbt accepted_values).
    bad_ops = sorted({r["operation"] for r in silver_rows if r["operation"] not in ACCEPTED_OPERATIONS})
    checks.append(
        make_check(
            "accepted_values_operation",
            "pass" if not bad_ops else "fail",
            sorted(ACCEPTED_OPERATIONS),
            bad_ops,
            f"operation must be within the accepted set; unexpected={bad_ops or 'none'}",
        )
    )

    # 6. Numeric range / domain integrity.
    range_violations: list[tuple] = []
    for r in silver_rows:
        key = tuple(r[c] for c in NATURAL_KEY_COLUMNS)
        if r["units_produced"] < 0:
            range_violations.append((key, "units_produced<0"))
        if r["defect_count"] < 0:
            range_violations.append((key, "defect_count<0"))
        if r["cycle_time_ms"] <= 0:
            range_violations.append((key, "cycle_time_ms<=0"))
        if r["defect_count"] > r["units_produced"]:
            range_violations.append((key, "defect_count>units_produced"))
    checks.append(
        make_check(
            "numeric_range_within_bounds",
            "pass" if not range_violations else "fail",
            0,
            len(range_violations),
            (
                "rules: units_produced>=0, defect_count>=0, cycle_time_ms>0, "
                "defect_count<=units_produced; "
                f"violations={range_violations[:5] or 'none'}"
            ),
        )
    )

    # 7. Freshness / partition correctness: silver holds only the active date,
    #    and that date is a valid ISO date.
    off_partition = sorted({r["business_date"] for r in silver_rows if r["business_date"] != business_date})
    parseable = is_iso_date(business_date)
    checks.append(
        make_check(
            "freshness_business_date",
            "pass" if (not off_partition and parseable) else "fail",
            business_date,
            {"off_partition_dates": off_partition, "business_date_parseable": parseable},
            "silver must contain only the active business_date partition, which must be a valid ISO date",
        )
    )

    return checks


def build_schema_drift_check(previous_schema_hash: str | None, current_schema_hash: str) -> dict:
    short = current_schema_hash[:12]
    if previous_schema_hash is None:
        return make_check(
            "schema_drift",
            "pass",
            None,
            short,
            "no previous successful run; baseline schema established",
        )
    if previous_schema_hash == current_schema_hash:
        return make_check(
            "schema_drift",
            "pass",
            previous_schema_hash[:12],
            short,
            "schema stable vs previous successful run",
        )
    status = "fail" if SCHEMA_DRIFT_POLICY == "fail" else "warn"
    return make_check(
        "schema_drift",
        status,
        previous_schema_hash[:12],
        short,
        f"schema drift vs previous successful run (policy={SCHEMA_DRIFT_POLICY})",
    )


def write_quality_report(checks: list[dict], passed: bool, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"passed": passed, "checks": checks}
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def is_iso_date(value: str) -> bool:
    try:
        datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return False
    return True


# --------------------------------------------------------------------------- #
# Lineage / catalog
# --------------------------------------------------------------------------- #
def build_lineage_doc(
    *,
    run_id: str,
    business_date: str,
    source_path: Path,
    paths: PipelinePaths,
    source_hash: str,
    schema_hash: str,
    source_rows: int,
    silver_rows: int,
    gold_rows: int,
    quality_passed: bool,
    quality_checks: list[dict],
    previous_schema_hash: str | None,
    drift_status: str,
) -> dict:
    return {
        "dataset_id": DATASET_ID,
        "run_id": run_id,
        "business_date": business_date,
        "source_hash": source_hash,
        "schema_hash": schema_hash,
        "source": {"path": str(source_path), "hash": source_hash, "row_count": source_rows},
        "paths": {
            "raw": str(paths.raw_path),
            "bronze": str(paths.bronze_path),
            "silver": str(paths.silver_path),
            "gold": str(paths.gold_path),
            "quality": str(paths.quality_path),
            "manifest": str(paths.manifest_path),
        },
        "layers": [
            {"name": "bronze", "path": str(paths.bronze_path), "parents": [str(source_path)]},
            {"name": "silver", "path": str(paths.silver_path), "parents": [str(paths.bronze_path)]},
            {"name": "gold", "path": str(paths.gold_path), "parents": [str(paths.silver_path)]},
        ],
        "stats": {
            "source_rows": source_rows,
            "silver_rows": silver_rows,
            "gold_rows": gold_rows,
        },
        "schema_drift": {
            "status": drift_status,
            "policy": SCHEMA_DRIFT_POLICY,
            "previous_schema_hash": previous_schema_hash,
            "current_schema_hash": schema_hash,
        },
        "quality": {"passed": quality_passed, "checks": quality_checks},
        "created_at": datetime.now(timezone.utc),
    }


def persist_catalog(
    lineage_doc: dict,
    *,
    db: Database | None,
    catalog_backend: str,
    output_dir: Path,
    quality_passed: bool,
) -> None:
    if catalog_backend == "mongo":
        if db is None:
            raise ValueError("db is required when catalog_backend='mongo'")
        write_mongo_catalog(db, lineage_doc)
        return

    # json backend: per-run entry (offline demo) + state pointers for
    # idempotency and schema-drift baseline.
    manifest_parent = Path(lineage_doc["paths"]["manifest"]).parent
    write_json_catalog(manifest_parent / "catalog_entry.json", lineage_doc)
    if quality_passed:
        state = state_dir(output_dir, lineage_doc["dataset_id"])
        write_json_file(state / f"business_date={lineage_doc['business_date']}.json", lineage_doc)
        write_json_file(state / "latest_successful_run.json", lineage_doc)


def write_mongo_catalog(db: Database, lineage_doc: dict) -> None:
    db.lakehouse_runs.update_one(
        {"run_id": lineage_doc["run_id"]},
        {"$set": lineage_doc},
        upsert=True,
    )
    db.lineage_events.update_one(
        {"run_id": lineage_doc["run_id"], "dataset_id": lineage_doc["dataset_id"]},
        {"$set": lineage_doc},
        upsert=True,
    )


def write_json_catalog(path: Path, lineage_doc: dict) -> None:
    write_json_file(path, lineage_doc)


# --------------------------------------------------------------------------- #
# Idempotency + schema-drift lookups (backend-agnostic)
# --------------------------------------------------------------------------- #
def find_existing_successful_run(
    dataset_id: str,
    business_date: str,
    source_hash: str,
    *,
    db: Database | None,
    catalog_backend: str,
    output_dir: Path,
) -> dict | None:
    if catalog_backend == "mongo":
        return db.lakehouse_runs.find_one(
            {
                "dataset_id": dataset_id,
                "business_date": business_date,
                "source_hash": source_hash,
                "quality.passed": True,
            },
            {"_id": 0},
        )
    doc = read_json_file(state_dir(output_dir, dataset_id) / f"business_date={business_date}.json")
    if doc and doc.get("source_hash") == source_hash and doc.get("quality", {}).get("passed"):
        return doc
    return None


def lookup_previous_schema_hash(
    dataset_id: str,
    *,
    db: Database | None,
    catalog_backend: str,
    output_dir: Path,
    current_run_id: str,
) -> str | None:
    if catalog_backend == "mongo":
        docs = list(
            db.lakehouse_runs.find(
                {
                    "dataset_id": dataset_id,
                    "quality.passed": True,
                    "run_id": {"$ne": current_run_id},
                },
                {"_id": 0, "schema_hash": 1},
            )
            .sort("created_at", -1)
            .limit(1)
        )
        return docs[0]["schema_hash"] if docs else None
    doc = read_json_file(state_dir(output_dir, dataset_id) / "latest_successful_run.json")
    return doc.get("schema_hash") if doc else None


def record_run_reuse(
    existing_doc: dict,
    *,
    db: Database | None,
    catalog_backend: str,
    output_dir: Path,
) -> None:
    """Record that an idempotent re-run reused an existing run (audit trail)."""
    if catalog_backend == "mongo":
        db.lakehouse_runs.update_one(
            {"run_id": existing_doc["run_id"]},
            {"$inc": {"reuse_count": 1}, "$set": {"last_reused_at": datetime.now(timezone.utc)}},
        )
        return
    state_file = (
        state_dir(output_dir, existing_doc["dataset_id"])
        / f"business_date={existing_doc['business_date']}.json"
    )
    doc = read_json_file(state_file) or dict(existing_doc)
    doc["reuse_count"] = doc.get("reuse_count", 0) + 1
    write_json_file(state_file, doc)


def result_from_doc(doc: dict, catalog_backend: str, *, status: str) -> PipelineResult:
    p = doc["paths"]
    paths = PipelinePaths(
        raw_path=Path(p["raw"]),
        bronze_path=Path(p["bronze"]),
        silver_path=Path(p["silver"]),
        gold_path=Path(p["gold"]),
        quality_path=Path(p["quality"]),
        manifest_path=Path(p["manifest"]),
    )
    return PipelineResult(
        run_id=doc["run_id"],
        dataset_id=doc["dataset_id"],
        business_date=doc["business_date"],
        source_hash=doc["source_hash"],
        schema_hash=doc["schema_hash"],
        paths=paths,
        quality_passed=doc["quality"]["passed"],
        quality_checks=doc["quality"]["checks"],
        catalog_backend=catalog_backend,
        status=status,
    )


# --------------------------------------------------------------------------- #
# Small IO utilities
# --------------------------------------------------------------------------- #
def state_dir(output_dir: Path, dataset_id: str) -> Path:
    return Path(output_dir) / "_state" / dataset_id


def read_json_file(path: Path) -> dict | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_json_file(path: Path, doc: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serializable = json.loads(json.dumps(doc, default=str))
    path.write_text(json.dumps(serializable, indent=2), encoding="utf-8")


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def result_to_dict(result: PipelineResult) -> dict:
    body = asdict(result)
    body["paths"] = {key: str(value) for key, value in body["paths"].items()}
    return body
