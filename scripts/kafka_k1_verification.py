#!/usr/bin/env python3
"""Verify K1 landing, commit ordering, recovery, replay, and quarantine."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any

from confluent_kafka import Producer

from manufacturing_data_platform.kafka_ingestion.contracts import sample_machine_event
from manufacturing_data_platform.kafka_ingestion.landing import (
    SimulatedCrashAfterLanding,
    load_landing_index,
)
from manufacturing_data_platform.kafka_ingestion.runtime import (
    DEFAULT_TOPIC,
    consume_and_land,
    ensure_single_partition_topic,
    produce_events,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:19092")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/tmp/manufacturing-mini-kafka-k1-evidence"),
    )
    return parser.parse_args()


def produce_invalid_event(bootstrap_servers: str, topic: str) -> dict[str, Any]:
    deliveries: list[dict[str, Any]] = []
    errors: list[str] = []

    def callback(error: Any, message: Any) -> None:
        if error is not None:
            errors.append(str(error))
            return
        deliveries.append(
            {
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
            }
        )

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "manufacturing-k1-invalid-event-producer",
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    producer.produce(
        topic=topic,
        key=b"mc-101",
        value=b'{"event_id":"invalid-contract-event"}',
        on_delivery=callback,
    )
    remaining = producer.flush(30)
    if errors or remaining != 0 or len(deliveries) != 1:
        raise RuntimeError(
            f"invalid-event delivery failed: errors={errors!r}, "
            f"remaining={remaining}, deliveries={deliveries!r}"
        )
    return deliveries[0]


def write_evidence(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def main() -> None:
    args = parse_args()
    shutil.rmtree(args.output_dir, ignore_errors=True)
    landing_dir = args.output_dir / "raw"
    topic_evidence = ensure_single_partition_topic(args.bootstrap_servers, args.topic)

    initial_events = [sample_machine_event(index) for index in range(1, 4)]
    initial_deliveries = produce_events(
        initial_events,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
    )
    initial = consume_and_land(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id="manufacturing-k1-main",
        output_dir=landing_dir,
        max_messages=3,
    )
    assert initial["landing"]["accepted_count"] == 3
    assert initial["offset_commit"]["offsets"][0]["next_offset"] == 3

    correction_delivery = produce_events(
        [sample_machine_event(4)],
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
    )[0]
    crash_observed = False
    try:
        consume_and_land(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            group_id="manufacturing-k1-main",
            output_dir=landing_dir,
            max_messages=1,
            simulate_crash_after_landing=True,
        )
    except SimulatedCrashAfterLanding:
        crash_observed = True
    assert crash_observed

    retry = consume_and_land(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id="manufacturing-k1-main",
        output_dir=landing_dir,
        max_messages=1,
    )
    assert retry["landing"]["status"] == "reused"
    assert retry["landing"]["reused_coordinate_count"] == 1
    assert retry["offset_commit"]["offsets"][0]["next_offset"] == 4

    replay = consume_and_land(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id="manufacturing-k1-bounded-replay",
        output_dir=landing_dir,
        max_messages=4,
        replay_start_offset=0,
        commit_offsets=False,
    )
    assert replay["landing"]["status"] == "reused"
    assert replay["landing"]["reused_coordinate_count"] == 4
    assert replay["offset_commit"]["performed"] is False

    invalid_delivery = produce_invalid_event(args.bootstrap_servers, args.topic)
    invalid = consume_and_land(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id="manufacturing-k1-main",
        output_dir=landing_dir,
        max_messages=1,
    )
    assert invalid["landing"]["quarantine_count"] == 1
    assert invalid["offset_commit"]["offsets"][0]["next_offset"] == 5

    index = load_landing_index(landing_dir)
    manifests = list(landing_dir.glob("topic=*/partition=*/batch=*/manifest.json"))
    assert len(index["coordinates"]) == 5
    assert len(index["accepted_events"]) == 4
    assert len(manifests) == 3

    evidence = {
        "status": "passed",
        "scope": "bounded local Kafka K1 raw ingestion",
        "topic": topic_evidence,
        "producer": {
            "enable_idempotence": True,
            "acks": "all",
            "initial_deliveries": initial_deliveries,
            "post_commit_delivery": correction_delivery,
            "invalid_delivery": invalid_delivery,
        },
        "initial_landing": initial,
        "crash_after_landing_before_commit_observed": crash_observed,
        "recovery": retry,
        "bounded_replay": replay,
        "invalid_event": invalid,
        "reconciliation": {
            "produced_record_count": 5,
            "persisted_coordinate_count": len(index["coordinates"]),
            "accepted_event_count": len(index["accepted_events"]),
            "quarantined_event_count": 1,
            "immutable_batch_count": len(manifests),
        },
        "claim_boundary": {
            "verified": [
                "bounded local producer and consumer",
                "payload plus topic/partition/offset raw evidence",
                "atomic immutable JSONL batch landing",
                "manual offset commit after landing",
                "crash-window redelivery without accepted-set duplication",
                "bounded offset replay without changing normal group progress",
                "invalid event quarantine without blocking the partition",
            ],
            "not_verified": [
                "continuous streaming service",
                "multi-partition ordering or rebalance",
                "multi-broker availability",
                "end-to-end exactly-once",
                "Spark Structured Streaming or direct Iceberg sink",
                "production security or operations",
            ],
        },
    }
    evidence_path = args.output_dir / "kafka_k1_verification.json"
    write_evidence(evidence_path, evidence)
    print(json.dumps(evidence, indent=2, sort_keys=True))
    print(f"Kafka K1 verification passed; evidence: {evidence_path}")


if __name__ == "__main__":
    main()
