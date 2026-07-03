from pathlib import Path

import mongomock

from robot_data_platform.db import ensure_indexes
from robot_data_platform.pipeline.eav import (
    DATASET_ID,
    run_eav_pipeline,
    transform_eav_to_gold,
    transform_to_eav,
)


def _mongo():
    db = mongomock.MongoClient()["test_robot_data_platform"]
    ensure_indexes(db)
    return db


def _check(result, name):
    return next(c for c in result.quality_checks if c["name"] == name)


def _write(path: Path, header: str, body: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(header + "\n" + "\n".join(body) + "\n", encoding="utf-8")
    return path


def _mapping(path: Path, mapping: dict) -> None:
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(mapping, ensure_ascii=False), encoding="utf-8")


# --------------------------------------------------------------------------- #
# pure transforms
# --------------------------------------------------------------------------- #
def test_transform_to_eav_maps_and_converts_units():
    mapping = {
        "source_id": "plant_b",
        "source_file": "b.csv",
        "entity_field": "machine_id",
        "business_date_field": "date",
        "attributes": {
            "output_units": {"standard": "units_produced", "type": "int"},
            "temp_f": {"standard": "temperature_c", "type": "float", "convert": "f_to_c"},
            "pressure_bar": {"standard": "pressure_kpa", "type": "float", "convert": "bar_to_kpa"},
        },
    }
    rows = [{"date": "2026-06-29", "machine_id": "MC-B1", "output_units": "80", "temp_f": "131.0", "pressure_bar": "1.0"}]
    eav, errors = transform_to_eav(rows, mapping, "fileid")
    assert errors == []
    by_attr = {r["attribute"]: r["value"] for r in eav}
    assert by_attr["units_produced"] == 80
    assert by_attr["temperature_c"] == 55.0  # (131-32)*5/9
    assert by_attr["pressure_kpa"] == 100.0  # 1.0 bar -> 100 kPa
    assert all(r["source_file_id"] == "fileid" for r in eav)


def test_transform_to_eav_captures_type_errors_gracefully():
    mapping = {
        "source_id": "s", "source_file": "s.csv", "entity_field": "e", "business_date_field": "d",
        "attributes": {"q": {"standard": "units_produced", "type": "int"}},
    }
    rows = [{"e": "E1", "d": "2026-06-29", "q": "not-a-number"}]
    eav, errors = transform_to_eav(rows, mapping, "fid")
    assert eav[0]["value"] is None  # graceful, no crash
    assert len(errors) == 1 and errors[0]["attribute"] == "units_produced"


def test_transform_eav_to_gold_aggregates_sum_and_avg():
    eav = [
        {"entity_id": "E1", "business_date": "d", "attribute": "units_produced", "value": 100, "value_type": "int", "source_id": "s", "source_file_id": "f"},
        {"entity_id": "E1", "business_date": "d", "attribute": "units_produced", "value": 120, "value_type": "int", "source_id": "s", "source_file_id": "f"},
        {"entity_id": "E1", "business_date": "d", "attribute": "temperature_c", "value": 55.0, "value_type": "float", "source_id": "s", "source_file_id": "f"},
        {"entity_id": "E1", "business_date": "d", "attribute": "temperature_c", "value": 57.0, "value_type": "float", "source_id": "s", "source_file_id": "f"},
    ]
    gold = transform_eav_to_gold(eav, "d")
    row = gold[0]
    assert row["units_produced"] == 220  # sum
    assert row["temperature_c"] == 56.0  # avg


# --------------------------------------------------------------------------- #
# full run on synthetic 3-format sample
# --------------------------------------------------------------------------- #
def test_eav_run_passes_and_unifies_three_formats(tmp_path):
    db = _mongo()
    result = run_eav_pipeline(
        raw_dir=tmp_path / "raw",
        mapping_dir=tmp_path / "map",
        output_dir=tmp_path / "out",
        db=db,
        catalog_backend="mongo",
    )
    assert result.status == "processed"
    assert result.quality_passed is True
    names = {c["name"] for c in result.quality_checks}
    assert {
        "mapping_coverage",
        "unmapped_source_columns",
        "not_null_value",
        "accepted_values_attribute",
        "value_type_valid",
        "numeric_range_within_bounds",
        "eav_to_gold_conservation",
        "freshness_business_date",
        "schema_drift",
    } <= names
    # conservation holds across all three formats for the active date
    cons = _check(result, "eav_to_gold_conservation")
    assert cons["status"] == "pass"
    assert cons["expected"]["units_produced"] == cons["actual"]["units_produced"] == 540

    run_doc = db.lakehouse_runs.find_one({"run_id": result.run_id}, {"_id": 0})
    assert run_doc["dataset_id"] == DATASET_ID
    assert [layer["name"] for layer in run_doc["layers"]] == ["bronze", "silver_eav", "gold"]


def test_eav_json_backend_writes_gold_and_eav(tmp_path):
    result = run_eav_pipeline(
        raw_dir=tmp_path / "raw",
        mapping_dir=tmp_path / "map",
        output_dir=tmp_path / "out",
        catalog_backend="json",
    )
    assert Path(result.paths["eav"]).exists()
    assert Path(result.paths["gold"]).exists()
    gold_text = Path(result.paths["gold"]).read_text(encoding="utf-8")
    assert "entity_id" in gold_text and "MC-B1" in gold_text  # converted source present


# --------------------------------------------------------------------------- #
# config-driven extensibility + idempotency
# --------------------------------------------------------------------------- #
def test_new_format_is_onboarded_by_adding_one_config(tmp_path):
    # Seed the 3 standard sample sources, then drop in a 4th format via config only.
    run_eav_pipeline(raw_dir=tmp_path / "raw", mapping_dir=tmp_path / "map", output_dir=tmp_path / "out1", catalog_backend="json")
    _write(
        tmp_path / "raw" / "vendor_d.csv",
        "yyyymmdd,unit_name,made,scrap,deg_c,kpa",
        ["2026-06-29,VD-1,40,0,50.0,99.0"],
    )
    _mapping(
        tmp_path / "map" / "vendor_d.json",
        {
            "source_id": "vendor_d", "source_file": "vendor_d.csv",
            "entity_field": "unit_name", "business_date_field": "yyyymmdd",
            "attributes": {
                "made": {"standard": "units_produced", "type": "int"},
                "scrap": {"standard": "defect_count", "type": "int"},
                "deg_c": {"standard": "temperature_c", "type": "float"},
                "kpa": {"standard": "pressure_kpa", "type": "float"},
            },
        },
    )
    result = run_eav_pipeline(
        raw_dir=tmp_path / "raw", mapping_dir=tmp_path / "map", output_dir=tmp_path / "out2",
        business_date="2026-06-29", catalog_backend="json",
    )
    assert result.quality_passed is True
    entities = {r["entity_id"] for r in _read_gold(result)}
    assert "VD-1" in entities  # 4th format accepted with no code change


def test_eav_idempotent_rerun_is_skipped(tmp_path):
    db = _mongo()
    first = run_eav_pipeline(raw_dir=tmp_path / "raw", mapping_dir=tmp_path / "map", output_dir=tmp_path / "out", db=db, catalog_backend="mongo")
    second = run_eav_pipeline(raw_dir=tmp_path / "raw", mapping_dir=tmp_path / "map", output_dir=tmp_path / "out", db=db, catalog_backend="mongo")
    assert first.status == "processed"
    assert second.status == "skipped"
    assert second.run_id == first.run_id
    assert db.lakehouse_runs.count_documents({"dataset_id": DATASET_ID}) == 1


# --------------------------------------------------------------------------- #
# quality failure cases
# --------------------------------------------------------------------------- #
def test_mapping_coverage_fails_when_required_attribute_unmapped(tmp_path):
    _write(tmp_path / "raw" / "x.csv", "d,e,q", ["2026-06-29,E1,10"])
    _mapping(
        tmp_path / "map" / "x.json",
        {
            "source_id": "x", "source_file": "x.csv", "entity_field": "e", "business_date_field": "d",
            "attributes": {"q": {"standard": "units_produced", "type": "int"}},  # missing defect/temp/pressure
        },
    )
    result = run_eav_pipeline(raw_dir=tmp_path / "raw", mapping_dir=tmp_path / "map", output_dir=tmp_path / "out", business_date="2026-06-29", catalog_backend="json")
    assert result.quality_passed is False
    assert _check(result, "mapping_coverage")["status"] == "fail"


def test_value_type_invalid_fails(tmp_path):
    _write(
        tmp_path / "raw" / "x.csv",
        "d,e,q,bad,t,p",
        ["2026-06-29,E1,10,oops,50.0,100.0"],
    )
    _mapping(
        tmp_path / "map" / "x.json",
        {
            "source_id": "x", "source_file": "x.csv", "entity_field": "e", "business_date_field": "d",
            "attributes": {
                "q": {"standard": "units_produced", "type": "int"},
                "bad": {"standard": "defect_count", "type": "int"},  # 'oops' -> type error
                "t": {"standard": "temperature_c", "type": "float"},
                "p": {"standard": "pressure_kpa", "type": "float"},
            },
        },
    )
    result = run_eav_pipeline(raw_dir=tmp_path / "raw", mapping_dir=tmp_path / "map", output_dir=tmp_path / "out", business_date="2026-06-29", catalog_backend="json")
    assert result.quality_passed is False
    assert _check(result, "value_type_valid")["status"] == "fail"


def test_unmapped_columns_warn_does_not_fail(tmp_path):
    _write(
        tmp_path / "raw" / "x.csv",
        "d,e,q,defect,t,p,extra_note",
        ["2026-06-29,E1,10,1,50.0,100.0,ignore-me"],
    )
    _mapping(
        tmp_path / "map" / "x.json",
        {
            "source_id": "x", "source_file": "x.csv", "entity_field": "e", "business_date_field": "d",
            "attributes": {
                "q": {"standard": "units_produced", "type": "int"},
                "defect": {"standard": "defect_count", "type": "int"},
                "t": {"standard": "temperature_c", "type": "float"},
                "p": {"standard": "pressure_kpa", "type": "float"},
            },
        },
    )
    result = run_eav_pipeline(raw_dir=tmp_path / "raw", mapping_dir=tmp_path / "map", output_dir=tmp_path / "out", business_date="2026-06-29", catalog_backend="json")
    warn = _check(result, "unmapped_source_columns")
    assert warn["status"] == "warn"
    assert "extra_note" in warn["actual"]["x"]
    assert result.quality_passed is True  # warn does not fail the run


def _read_gold(result):
    import csv

    with open(result.paths["gold"], encoding="utf-8") as handle:
        return list(csv.DictReader(handle))
