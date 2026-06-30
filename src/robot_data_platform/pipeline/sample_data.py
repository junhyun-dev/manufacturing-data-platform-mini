from __future__ import annotations

from pathlib import Path


# Fully synthetic manufacturing rows. No real plant/customer/work-order data.
# The set is shaped to exercise the quality suite end to end:
#   - 3 distinct natural keys on the active business_date (2026-06-29)
#   - 1 exact natural-key duplicate (exercises silver dedup)
#   - 1 row on a prior business_date (exercises business_date filtering)
# So a correct run yields 3 silver rows, and the reconciliation check can tell
# "expected filtering/dedup" apart from "unexpected row loss".
SAMPLE_ROWS = [
    {
        "event_time": "2026-06-29T08:00:00Z",
        "plant_id": "plant-a",
        "line_id": "line-1",
        "work_order_id": "wo-1001",
        "robot_id": "rb-101",
        "product_code": "gearbox-a",
        "operation": "assembly",
        "units_produced": "120",
        "defect_count": "2",
        "cycle_time_ms": "840",
        "business_date": "2026-06-29",
    },
    {
        "event_time": "2026-06-29T09:00:00Z",
        "plant_id": "plant-a",
        "line_id": "line-1",
        "work_order_id": "wo-1002",
        "robot_id": "rb-102",
        "product_code": "gearbox-a",
        "operation": "inspection",
        "units_produced": "118",
        "defect_count": "1",
        "cycle_time_ms": "910",
        "business_date": "2026-06-29",
    },
    {
        "event_time": "2026-06-29T10:00:00Z",
        "plant_id": "plant-a",
        "line_id": "line-2",
        "work_order_id": "wo-1003",
        "robot_id": "rb-201",
        "product_code": "motor-b",
        "operation": "assembly",
        "units_produced": "96",
        "defect_count": "3",
        "cycle_time_ms": "1020",
        "business_date": "2026-06-29",
    },
    {
        # Exact natural-key duplicate of the first row -> dropped by silver dedup.
        "event_time": "2026-06-29T08:00:00Z",
        "plant_id": "plant-a",
        "line_id": "line-1",
        "work_order_id": "wo-1001",
        "robot_id": "rb-101",
        "product_code": "gearbox-a",
        "operation": "assembly",
        "units_produced": "120",
        "defect_count": "2",
        "cycle_time_ms": "840",
        "business_date": "2026-06-29",
    },
    {
        # Prior business_date -> filtered out when processing 2026-06-29.
        "event_time": "2026-06-28T08:00:00Z",
        "plant_id": "plant-a",
        "line_id": "line-1",
        "work_order_id": "wo-0990",
        "robot_id": "rb-101",
        "product_code": "gearbox-a",
        "operation": "assembly",
        "units_produced": "100",
        "defect_count": "1",
        "cycle_time_ms": "800",
        "business_date": "2026-06-28",
    },
]


def ensure_sample_manufacturing_csv(path: str | Path) -> Path:
    csv_path = Path(path)
    if csv_path.exists():
        return csv_path

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    headers = list(SAMPLE_ROWS[0].keys())
    lines = [",".join(headers)]
    for row in SAMPLE_ROWS:
        lines.append(",".join(row[header] for header in headers))
    csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return csv_path
