"""Bounded Kafka raw-ingestion contracts and runtime adapters."""

from manufacturing_data_platform.kafka_ingestion.contracts import (
    EVENT_SCHEMA_VERSION,
    EventContractError,
    sample_machine_event,
    validate_machine_event,
)
from manufacturing_data_platform.kafka_ingestion.landing import (
    KafkaRecord,
    LandingConsistencyError,
    LandingResult,
    SimulatedCrashAfterLanding,
    land_records,
)

__all__ = [
    "EVENT_SCHEMA_VERSION",
    "EventContractError",
    "KafkaRecord",
    "LandingConsistencyError",
    "LandingResult",
    "SimulatedCrashAfterLanding",
    "land_records",
    "sample_machine_event",
    "validate_machine_event",
]
