from __future__ import annotations

import json
from pathlib import Path


# Fully synthetic, public-safe manufacturing inputs in THREE different wide
# formats (different column names, languages, and units). No company code, file
# names, customer identifiers, or business logic are copied — this is a
# clean-room demonstration of the EAV ingest pattern.
#
# The point: a new file format is onboarded by adding ONE mapping config, not by
# changing pipeline code. Each mapping declares per-source columns -> standard
# fields, with optional deterministic unit conversions.
#
# Standard (canonical) attributes after mapping:
#   units_produced (int), defect_count (int), temperature_c (float), pressure_kpa (float)

SAMPLE_SOURCES = [
    {
        "source_id": "plant_a",
        "source_file": "plant_a_daily.csv",
        # Korean headers, already in canonical units (C / kPa).
        "header": ["생산일자", "설비", "생산수량", "불량수량", "온도_C", "압력_kPa"],
        "rows": [
            ["2026-06-29", "EQP-A1", "100", "2", "55.0", "101.3"],
            ["2026-06-29", "EQP-A1", "120", "1", "57.0", "102.0"],  # 2nd reading -> aggregated
            ["2026-06-29", "EQP-A2", "90", "3", "60.0", "100.0"],
        ],
        "mapping": {
            "source_id": "plant_a",
            "source_file": "plant_a_daily.csv",
            "entity_field": "설비",
            "business_date_field": "생산일자",
            "attributes": {
                "생산수량": {"standard": "units_produced", "type": "int"},
                "불량수량": {"standard": "defect_count", "type": "int"},
                "온도_C": {"standard": "temperature_c", "type": "float"},
                "압력_kPa": {"standard": "pressure_kpa", "type": "float"},
            },
        },
    },
    {
        "source_id": "plant_b",
        "source_file": "plant_b_export.csv",
        # English headers, DIFFERENT units (Fahrenheit / bar) -> needs conversion.
        "header": ["date", "machine_id", "output_units", "reject_count", "temp_f", "pressure_bar"],
        "rows": [
            ["2026-06-29", "MC-B1", "80", "1", "131.0", "1.0"],  # 131F -> 55.0C ; 1.0 bar -> 100.0 kPa
            ["2026-06-28", "MC-B1", "70", "0", "130.0", "1.0"],  # prior date -> excluded from gold
        ],
        "mapping": {
            "source_id": "plant_b",
            "source_file": "plant_b_export.csv",
            "entity_field": "machine_id",
            "business_date_field": "date",
            "attributes": {
                "output_units": {"standard": "units_produced", "type": "int"},
                "reject_count": {"standard": "defect_count", "type": "int"},
                "temp_f": {"standard": "temperature_c", "type": "float", "convert": "f_to_c"},
                "pressure_bar": {"standard": "pressure_kpa", "type": "float", "convert": "bar_to_kpa"},
            },
        },
    },
    {
        "source_id": "line_c",
        "source_file": "line_c_report.csv",
        # Yet another header style, canonical units.
        "header": ["business_date", "asset", "qty_produced", "qty_defective", "temperature_celsius", "pressure_kpa"],
        "rows": [
            ["2026-06-29", "LN-C1", "150", "5", "58.0", "103.0"],
        ],
        "mapping": {
            "source_id": "line_c",
            "source_file": "line_c_report.csv",
            "entity_field": "asset",
            "business_date_field": "business_date",
            "attributes": {
                "qty_produced": {"standard": "units_produced", "type": "int"},
                "qty_defective": {"standard": "defect_count", "type": "int"},
                "temperature_celsius": {"standard": "temperature_c", "type": "float"},
                "pressure_kpa": {"standard": "pressure_kpa", "type": "float"},
            },
        },
    },
]


def ensure_sample_eav_inputs(raw_dir: str | Path, mapping_dir: str | Path) -> None:
    """Write the synthetic wide CSVs and their JSON mapping configs if missing.

    Mirrors the committed ``config/eav_mappings/*.json`` files so tests can run in
    a fresh tmp dir without depending on repo-relative paths.
    """
    raw_path = Path(raw_dir)
    map_path = Path(mapping_dir)
    raw_path.mkdir(parents=True, exist_ok=True)
    map_path.mkdir(parents=True, exist_ok=True)

    for source in SAMPLE_SOURCES:
        csv_file = raw_path / source["source_file"]
        if not csv_file.exists():
            lines = [",".join(source["header"])]
            lines.extend(",".join(row) for row in source["rows"])
            csv_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

        mapping_file = map_path / f"{source['source_id']}.json"
        if not mapping_file.exists():
            mapping_file.write_text(
                json.dumps(source["mapping"], indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
