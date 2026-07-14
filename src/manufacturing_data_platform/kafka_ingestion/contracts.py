from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Mapping

from manufacturing_data_platform.domain import ACCEPTED_OPERATIONS


EVENT_SCHEMA_VERSION = 1
REQUIRED_EVENT_FIELDS = (
    "event_id",
    "schema_version",
    "event_time",
    "business_date",
    "plant_id",
    "line_id",
    "work_order_id",
    "machine_id",
    "product_code",
    "operation",
    "units_produced",
    "defect_count",
    "cycle_time_ms",
)
STRING_EVENT_FIELDS = (
    "event_id",
    "event_time",
    "business_date",
    "plant_id",
    "line_id",
    "work_order_id",
    "machine_id",
    "product_code",
    "operation",
)
INTEGER_EVENT_FIELDS = (
    "schema_version",
    "units_produced",
    "defect_count",
    "cycle_time_ms",
)


class EventContractError(ValueError):
    """Raised when an event cannot enter the accepted raw landing."""


def validate_machine_event(event: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and normalize the strict v1 manufacturing event contract."""
    missing = sorted(set(REQUIRED_EVENT_FIELDS) - set(event))
    unknown = sorted(set(event) - set(REQUIRED_EVENT_FIELDS))
    if missing:
        raise EventContractError(f"missing required fields: {', '.join(missing)}")
    if unknown:
        raise EventContractError(f"unknown fields for schema v1: {', '.join(unknown)}")

    normalized = dict(event)
    for field in STRING_EVENT_FIELDS:
        value = normalized[field]
        if not isinstance(value, str) or not value.strip():
            raise EventContractError(f"{field} must be a non-empty string")
        normalized[field] = value.strip()

    for field in INTEGER_EVENT_FIELDS:
        value = normalized[field]
        if isinstance(value, bool) or not isinstance(value, int):
            raise EventContractError(f"{field} must be an integer")

    if normalized["schema_version"] != EVENT_SCHEMA_VERSION:
        raise EventContractError(
            f"unsupported schema_version={normalized['schema_version']}; "
            f"expected {EVENT_SCHEMA_VERSION}"
        )

    try:
        date.fromisoformat(normalized["business_date"])
    except ValueError as exc:
        raise EventContractError("business_date must be an ISO date") from exc

    try:
        parsed_time = datetime.fromisoformat(
            normalized["event_time"].replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise EventContractError("event_time must be an ISO timestamp") from exc
    if parsed_time.tzinfo is None:
        raise EventContractError("event_time must include a timezone")

    normalized["plant_id"] = normalized["plant_id"].lower()
    normalized["line_id"] = normalized["line_id"].lower()
    normalized["work_order_id"] = normalized["work_order_id"].lower()
    normalized["machine_id"] = normalized["machine_id"].lower()
    normalized["product_code"] = normalized["product_code"].lower()
    normalized["operation"] = normalized["operation"].lower()

    if normalized["operation"] not in ACCEPTED_OPERATIONS:
        raise EventContractError(
            f"operation must be one of {sorted(ACCEPTED_OPERATIONS)}"
        )
    if normalized["units_produced"] < 0:
        raise EventContractError("units_produced must be >= 0")
    if normalized["defect_count"] < 0:
        raise EventContractError("defect_count must be >= 0")
    if normalized["defect_count"] > normalized["units_produced"]:
        raise EventContractError("defect_count must be <= units_produced")
    if normalized["cycle_time_ms"] <= 0:
        raise EventContractError("cycle_time_ms must be > 0")

    return normalized


def serialize_machine_event(event: Mapping[str, Any]) -> bytes:
    normalized = validate_machine_event(event)
    return json.dumps(
        normalized,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sample_machine_event(index: int = 1) -> dict[str, Any]:
    if index < 1:
        raise ValueError("index must be >= 1")
    event_time = datetime(2026, 6, 29, 8, tzinfo=timezone.utc) + timedelta(
        minutes=index - 1
    )
    return {
        "event_id": f"evt-20260629-{index:06d}",
        "schema_version": EVENT_SCHEMA_VERSION,
        "event_time": event_time.isoformat().replace("+00:00", "Z"),
        "business_date": "2026-06-29",
        "plant_id": "plant-a",
        "line_id": "line-1",
        "work_order_id": f"wo-{1000 + index}",
        "machine_id": "mc-101",
        "product_code": "gearbox-a",
        "operation": "assembly",
        "units_produced": 10 * index,
        "defect_count": index - 1,
        "cycle_time_ms": 800 + (10 * index),
    }
