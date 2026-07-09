"""EAV mini slice — multiple wide formats -> mapping config -> EAV (long) -> gold.

Reuses the Slice 1 lakehouse spine (idempotency, schema-drift, catalog/lineage,
dbt-style quality shape) instead of forking a parallel system. Only the dataset
profile and the check pack differ:

  many wide CSVs (different columns/units) -> mapping config -> EAV long
  -> pivot/aggregate -> gold metric mart -> quality -> Mongo catalog/lineage

Honesty/claim note: EAV is a standard data-modeling pattern implemented here
clean-room on fully synthetic data. No company code/data/names are used.
"""
from __future__ import annotations

import json
import shutil
from dataclasses import asdict, dataclass
from collections import defaultdict
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

from pymongo.database import Database

from manufacturing_data_platform.ingest import hash_file, read_csv
from manufacturing_data_platform.pipeline.lakehouse import (
    build_run_id,
    build_schema_drift_check,
    find_existing_successful_run,
    is_iso_date,
    lookup_previous_schema_hash,
    make_check,
    persist_catalog,
    record_run_reuse,
    write_csv,
    write_quality_report,
)
from manufacturing_data_platform.pipeline.sample_eav import ensure_sample_eav_inputs


DATASET_ID = "manufacturing_wide_eav"

STANDARD_ATTRIBUTES = ["units_produced", "defect_count", "temperature_c", "pressure_kpa"]
REQUIRED_ATTRIBUTES = set(STANDARD_ATTRIBUTES)

# How each standard attribute rolls up from EAV long rows into a gold row.
AGGREGATIONS = {
    "units_produced": "sum",
    "defect_count": "sum",
    "temperature_c": "avg",
    "pressure_kpa": "avg",
}

# Deterministic unit conversions referenced by mapping configs.
CONVERTERS = {
    "f_to_c": lambda x: (x - 32) * 5 / 9,
    "bar_to_kpa": lambda x: x * 100,
}


@dataclass(frozen=True)
class EavResult:
    run_id: str
    dataset_id: str
    business_date: str
    source_hash: str
    schema_hash: str
    paths: dict
    quality_passed: bool
    quality_checks: list[dict]
    catalog_backend: str
    status: str = "processed"


def run_eav_pipeline(
    raw_dir: str | Path = "data/raw/eav",
    mapping_dir: str | Path = "config/eav_mappings",
    output_dir: str | Path = "data/lakehouse_eav",
    business_date: str | None = None,
    db: Database | None = None,
    catalog_backend: str = "mongo",
) -> EavResult:
    if catalog_backend not in ("mongo", "json"):
        raise ValueError("catalog_backend must be 'mongo' or 'json'")
    if catalog_backend == "mongo" and db is None:
        raise ValueError("db is required when catalog_backend='mongo'")

    ensure_sample_eav_inputs(raw_dir, mapping_dir)
    sources = load_sources(raw_dir, mapping_dir)
    if not sources:
        raise ValueError("no EAV sources found (need mapping configs + matching CSVs)")

    active_business_date = business_date or infer_business_date(sources)
    base_dir = Path(output_dir)

    source_hash = combined_source_hash(sources)
    schema_hash = eav_schema_hash(sources)

    # --- Idempotency gate (shared with the lakehouse slice) ---
    existing = find_existing_successful_run(
        DATASET_ID,
        active_business_date,
        source_hash,
        db=db,
        catalog_backend=catalog_backend,
        output_dir=base_dir,
    )
    if existing is not None:
        record_run_reuse(existing, db=db, catalog_backend=catalog_backend, output_dir=base_dir)
        return result_from_doc(existing, catalog_backend, status="skipped")

    run_id = build_run_id(active_business_date)
    paths = build_eav_paths(base_dir, active_business_date, run_id)

    write_bronze(sources, paths, active_business_date, source_hash, schema_hash)

    # --- silver: EAV long format (all dates retained; gold filters) ---
    eav_rows: list[dict] = []
    type_errors: list[dict] = []
    for source in sources:
        rows, errors = transform_to_eav(source["rows"], source["mapping"], source["file_id"])
        eav_rows.extend(rows)
        type_errors.extend(errors)
    write_csv(paths["eav"], eav_rows)

    # --- gold: pivot/aggregate EAV for the active date ---
    gold_rows = transform_eav_to_gold(eav_rows, active_business_date)
    write_csv(paths["gold"], gold_rows)

    # --- schema drift vs previous successful EAV run ---
    previous_schema_hash = lookup_previous_schema_hash(
        DATASET_ID,
        db=db,
        catalog_backend=catalog_backend,
        output_dir=base_dir,
        current_run_id=run_id,
    )
    drift_check = build_schema_drift_check(previous_schema_hash, schema_hash)

    # --- quality ---
    quality_checks = build_eav_quality_checks(
        sources, eav_rows, gold_rows, type_errors, active_business_date
    )
    quality_checks.append(drift_check)
    quality_passed = not any(c["status"] == "fail" for c in quality_checks)
    write_quality_report(quality_checks, quality_passed, Path(paths["quality"]))

    lineage_doc = build_eav_lineage_doc(
        run_id=run_id,
        business_date=active_business_date,
        sources=sources,
        paths=paths,
        source_hash=source_hash,
        schema_hash=schema_hash,
        eav_rows=len(eav_rows),
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

    return EavResult(
        run_id=run_id,
        dataset_id=DATASET_ID,
        business_date=active_business_date,
        source_hash=source_hash,
        schema_hash=schema_hash,
        paths={k: str(v) for k, v in paths.items()},
        quality_passed=quality_passed,
        quality_checks=quality_checks,
        catalog_backend=catalog_backend,
        status="processed",
    )


# --------------------------------------------------------------------------- #
# Load + identity
# --------------------------------------------------------------------------- #
def load_sources(raw_dir: str | Path, mapping_dir: str | Path) -> list[dict]:
    """Config-driven ingest: each mapping JSON names its source CSV. Adding a new
    file format = drop in one more mapping config; no pipeline code changes."""
    raw_path = Path(raw_dir)
    sources = []
    for mapping_file in sorted(Path(mapping_dir).glob("*.json")):
        mapping = json.loads(mapping_file.read_text(encoding="utf-8"))
        csv_path = raw_path / mapping["source_file"]
        if not csv_path.exists():
            continue
        columns, rows = read_csv(csv_path)
        sources.append(
            {
                "mapping": mapping,
                "columns": columns,
                "rows": rows,
                "file_path": csv_path,
                "file_id": hash_file(csv_path),
            }
        )
    return sources


def infer_business_date(sources: list[dict]) -> str:
    for source in sources:
        date_field = source["mapping"]["business_date_field"]
        for row in source["rows"]:
            value = (row.get(date_field) or "").strip()
            if value:
                return value
    return datetime.now(timezone.utc).date().isoformat()


def combined_source_hash(sources: list[dict]) -> str:
    digest = sha256()
    for file_id in sorted(s["file_id"] for s in sources):
        digest.update(file_id.encode("utf-8"))
    return digest.hexdigest()


def eav_schema_hash(sources: list[dict]) -> str:
    """Schema = the set of source ids + standard attributes produced. Adding a
    new source/config or a new standard attribute changes it -> schema drift."""
    schema = {
        "sources": sorted(s["mapping"]["source_id"] for s in sources),
        "attributes": sorted(
            {spec["standard"] for s in sources for spec in s["mapping"]["attributes"].values()}
        ),
    }
    canonical = json.dumps(schema, sort_keys=True, separators=(",", ":"))
    return sha256(canonical.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# Transforms (pure) + bronze IO
# --------------------------------------------------------------------------- #
def transform_to_eav(
    rows: list[dict[str, str]],
    mapping: dict,
    source_file_id: str,
) -> tuple[list[dict], list[dict]]:
    """Wide row -> one EAV row per mapped attribute. Pure. Unparseable values are
    captured gracefully (value=None + a type_error record), not crashed on."""
    source_id = mapping["source_id"]
    entity_field = mapping["entity_field"]
    date_field = mapping["business_date_field"]

    eav_rows: list[dict] = []
    type_errors: list[dict] = []
    for row in rows:
        entity_id = (row.get(entity_field) or "").strip()
        business_date = (row.get(date_field) or "").strip()
        for src_col, spec in mapping["attributes"].items():
            value, error = normalize_value(row.get(src_col, ""), spec)
            if error:
                type_errors.append(
                    {"source_id": source_id, "attribute": spec["standard"], "raw": row.get(src_col, "")}
                )
            eav_rows.append(
                {
                    "entity_id": entity_id,
                    "business_date": business_date,
                    "attribute": spec["standard"],
                    "value": value,
                    "value_type": spec["type"],
                    "source_id": source_id,
                    "source_file_id": source_file_id,
                }
            )
    return eav_rows, type_errors


def normalize_value(raw, spec: dict):
    """Return (value, error). error is None|"type_error". Empty -> (None, None)."""
    if raw is None or str(raw).strip() == "":
        return None, None
    try:
        if spec["type"] in ("int", "float"):
            value = float(raw)
            converter = spec.get("convert")
            if converter:
                value = CONVERTERS[converter](value)
            value = int(round(value)) if spec["type"] == "int" else round(float(value), 4)
        else:
            value = str(raw).strip()
    except (ValueError, TypeError, KeyError):
        return None, "type_error"
    return value, None


def transform_eav_to_gold(eav_rows: list[dict], business_date: str) -> list[dict]:
    """Pivot EAV long rows for the active date back into a wide gold metric row
    per (business_date, entity_id), aggregating per attribute. Pure."""
    by_entity: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    for row in eav_rows:
        if row["business_date"] != business_date or row["value"] is None:
            continue
        by_entity[row["entity_id"]][row["attribute"]].append(row["value"])

    gold_rows: list[dict] = []
    for entity_id in sorted(by_entity):
        attrs = by_entity[entity_id]
        record = {"business_date": business_date, "entity_id": entity_id}
        for attr in STANDARD_ATTRIBUTES:
            values = attrs.get(attr, [])
            if not values:
                record[attr] = 0
            elif AGGREGATIONS[attr] == "sum":
                record[attr] = sum(values)
            else:
                record[attr] = round(sum(values) / len(values), 4)
        units = record["units_produced"]
        defects = record["defect_count"]
        record["defect_rate"] = round(defects / units, 6) if units else 0
        record["reading_count"] = max((len(v) for v in attrs.values()), default=0)
        gold_rows.append(record)

    if not gold_rows:
        gold_rows.append(
            {
                "business_date": business_date,
                "entity_id": "",
                **{attr: 0 for attr in STANDARD_ATTRIBUTES},
                "defect_rate": 0,
                "reading_count": 0,
            }
        )
    return gold_rows


def build_eav_paths(base_dir: Path, business_date: str, run_id: str) -> dict:
    run_dir = base_dir / f"business_date={business_date}" / f"run_id={run_id}"
    return {
        "bronze_dir": run_dir / "bronze",
        "eav": run_dir / "silver" / "eav.csv",
        "gold": run_dir / "gold" / "entity_daily_metrics.csv",
        "quality": run_dir / "quality" / "quality_report.json",
        "manifest": run_dir / "manifest.json",
    }


def write_bronze(
    sources: list[dict],
    paths: dict,
    business_date: str,
    source_hash: str,
    schema_hash: str,
) -> None:
    bronze_dir = Path(paths["bronze_dir"])
    bronze_dir.mkdir(parents=True, exist_ok=True)
    files = []
    for source in sources:
        dest = bronze_dir / source["file_path"].name
        shutil.copyfile(source["file_path"], dest)
        files.append(
            {
                "source_id": source["mapping"]["source_id"],
                "file": source["file_path"].name,
                "file_id": source["file_id"],
                "columns": source["columns"],
                "row_count": len(source["rows"]),
            }
        )

    manifest = {
        "dataset_id": DATASET_ID,
        "stage": "bronze",
        "business_date": business_date,
        "source_hash": source_hash,
        "schema_hash": schema_hash,
        "files": files,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    Path(paths["manifest"]).parent.mkdir(parents=True, exist_ok=True)
    Path(paths["manifest"]).write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")


# --------------------------------------------------------------------------- #
# Quality — EAV-specific dbt-style checks (reuses make_check shape)
# --------------------------------------------------------------------------- #
def build_eav_quality_checks(
    sources: list[dict],
    eav_rows: list[dict],
    gold_rows: list[dict],
    type_errors: list[dict],
    business_date: str,
) -> list[dict]:
    checks: list[dict] = []

    # 1. mapping_coverage: each source must map every required standard attribute.
    missing = {}
    for source in sources:
        mapped = {spec["standard"] for spec in source["mapping"]["attributes"].values()}
        gap = sorted(REQUIRED_ATTRIBUTES - mapped)
        if gap:
            missing[source["mapping"]["source_id"]] = gap
    checks.append(
        make_check(
            "mapping_coverage",
            "pass" if not missing else "fail",
            sorted(REQUIRED_ATTRIBUTES),
            missing or "all sources complete",
            "each source mapping must cover all required standard attributes",
        )
    )

    # 2. unmapped_source_columns: surface (not silently drop) extra columns. warn.
    unmapped = {}
    for source in sources:
        mapping = source["mapping"]
        known = set(mapping["attributes"]) | {mapping["entity_field"], mapping["business_date_field"]}
        extra = sorted(set(source["columns"]) - known)
        if extra:
            unmapped[mapping["source_id"]] = extra
    checks.append(
        make_check(
            "unmapped_source_columns",
            "warn" if unmapped else "pass",
            [],
            unmapped or "none",
            "source columns absent from the mapping are surfaced; policy=warn (does not fail the run)",
        )
    )

    active_eav = [r for r in eav_rows if r["business_date"] == business_date]

    # 3. not_null_value: mapped values for the active date must be non-null.
    nulls = sum(1 for r in active_eav if r["value"] is None)
    checks.append(
        make_check(
            "not_null_value",
            "pass" if nulls == 0 else "fail",
            0,
            nulls,
            "every EAV value for the active date must be non-null after mapping",
        )
    )

    # 4. accepted_values_attribute: every attribute must be a known standard one.
    bad_attrs = sorted({r["attribute"] for r in eav_rows if r["attribute"] not in REQUIRED_ATTRIBUTES})
    checks.append(
        make_check(
            "accepted_values_attribute",
            "pass" if not bad_attrs else "fail",
            sorted(REQUIRED_ATTRIBUTES),
            bad_attrs or "none",
            "mapping must only produce known standard attributes",
        )
    )

    # 5. value_type_valid: unparseable source values for a typed attribute.
    checks.append(
        make_check(
            "value_type_valid",
            "pass" if not type_errors else "fail",
            0,
            len(type_errors),
            f"values that failed type conversion: {type_errors[:5] or 'none'}",
        )
    )

    # 6. numeric_range on the gold metrics.
    violations: list[tuple] = []
    for g in gold_rows:
        if g["units_produced"] < 0:
            violations.append((g["entity_id"], "units_produced<0"))
        if g["defect_count"] < 0:
            violations.append((g["entity_id"], "defect_count<0"))
        if g["defect_count"] > g["units_produced"]:
            violations.append((g["entity_id"], "defect_count>units_produced"))
        if not (-50 <= g["temperature_c"] <= 500):
            violations.append((g["entity_id"], "temperature_c_out_of_range"))
        if g["pressure_kpa"] < 0:
            violations.append((g["entity_id"], "pressure_kpa<0"))
    checks.append(
        make_check(
            "numeric_range_within_bounds",
            "pass" if not violations else "fail",
            0,
            len(violations),
            (
                "gold rules: units>=0, defects>=0, defects<=units, -50<=temp_c<=500, pressure>=0; "
                f"violations={violations[:5] or 'none'}"
            ),
        )
    )

    # 7. eav_to_gold_conservation: additive measures must be preserved.
    eav_units = sum(r["value"] for r in active_eav if r["attribute"] == "units_produced" and r["value"] is not None)
    eav_defects = sum(r["value"] for r in active_eav if r["attribute"] == "defect_count" and r["value"] is not None)
    gold_units = sum(g["units_produced"] for g in gold_rows)
    gold_defects = sum(g["defect_count"] for g in gold_rows)
    checks.append(
        make_check(
            "eav_to_gold_conservation",
            "pass" if (eav_units == gold_units and eav_defects == gold_defects) else "fail",
            {"units_produced": eav_units, "defect_count": eav_defects},
            {"units_produced": gold_units, "defect_count": gold_defects},
            "additive measures must be conserved from EAV to gold for the active date",
        )
    )

    # 8. freshness: the active partition must be a valid ISO date. EAV retains all
    #    dates by design, so other dates present is informational, not a failure.
    other_dates = sorted({r["business_date"] for r in eav_rows if r["business_date"] != business_date})
    checks.append(
        make_check(
            "freshness_business_date",
            "pass" if is_iso_date(business_date) else "fail",
            business_date,
            {"business_date_parseable": is_iso_date(business_date), "other_dates_in_eav": other_dates},
            "active business_date must be a valid ISO date; gold is filtered to it (EAV keeps all dates)",
        )
    )

    return checks


# --------------------------------------------------------------------------- #
# Lineage doc + skip reconstruction
# --------------------------------------------------------------------------- #
def build_eav_lineage_doc(
    *,
    run_id: str,
    business_date: str,
    sources: list[dict],
    paths: dict,
    source_hash: str,
    schema_hash: str,
    eav_rows: int,
    gold_rows: int,
    quality_passed: bool,
    quality_checks: list[dict],
    previous_schema_hash: str | None,
    drift_status: str,
) -> dict:
    str_paths = {k: str(v) for k, v in paths.items()}
    source_layer = [
        {"source_id": s["mapping"]["source_id"], "file": s["file_path"].name, "file_id": s["file_id"]}
        for s in sources
    ]
    return {
        "dataset_id": DATASET_ID,
        "run_id": run_id,
        "business_date": business_date,
        "source_hash": source_hash,
        "schema_hash": schema_hash,
        "paths": str_paths,
        "sources": source_layer,
        "layers": [
            {"name": "bronze", "path": str_paths["bronze_dir"], "parents": [s["file"] for s in source_layer]},
            {"name": "silver_eav", "path": str_paths["eav"], "parents": [str_paths["bronze_dir"]]},
            {"name": "gold", "path": str_paths["gold"], "parents": [str_paths["eav"]]},
        ],
        "stats": {"eav_rows": eav_rows, "gold_rows": gold_rows, "source_count": len(sources)},
        "schema_drift": {
            "status": drift_status,
            "policy": "warn",
            "previous_schema_hash": previous_schema_hash,
            "current_schema_hash": schema_hash,
        },
        "quality": {"passed": quality_passed, "checks": quality_checks},
        "created_at": datetime.now(timezone.utc),
    }


def result_from_doc(doc: dict, catalog_backend: str, *, status: str) -> EavResult:
    return EavResult(
        run_id=doc["run_id"],
        dataset_id=doc["dataset_id"],
        business_date=doc["business_date"],
        source_hash=doc["source_hash"],
        schema_hash=doc["schema_hash"],
        paths={k: str(v) for k, v in doc.get("paths", {}).items()},
        quality_passed=doc["quality"]["passed"],
        quality_checks=doc["quality"]["checks"],
        catalog_backend=catalog_backend,
        status=status,
    )


def result_to_dict(result: EavResult) -> dict:
    return asdict(result)
