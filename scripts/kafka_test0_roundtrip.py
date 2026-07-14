#!/usr/bin/env python3
"""Run the smallest broker-backed Kafka produce/consume verification."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer, libversion
from confluent_kafka.admin import AdminClient, NewTopic


TEST_EVENT = {
    "event_id": "evt-test0-000001",
    "schema_version": 1,
    "event_time": "2026-06-29T08:00:00Z",
    "business_date": "2026-06-29",
    "plant_id": "plant-a",
    "line_id": "line-1",
    "machine_id": "mc-101",
    "units_produced": 1,
    "defect_count": 0,
}
TEST_KEY = "mc-101"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:19092")
    parser.add_argument(
        "--topic", default="manufacturing.machine-events.v1.test0"
    )
    parser.add_argument("--group-id", default="manufacturing-kafka-test0")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    return parser.parse_args()


def create_single_partition_topic(admin: AdminClient, topic: str) -> None:
    future = admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1)]
    )[topic]
    try:
        future.result(timeout=30)
    except Exception as exc:
        error = exc.args[0] if exc.args else None
        if not hasattr(error, "code") or error.code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise

    metadata = admin.list_topics(topic=topic, timeout=30)
    topic_metadata = metadata.topics.get(topic)
    if topic_metadata is None or topic_metadata.error is not None:
        raise RuntimeError(f"Topic metadata unavailable for {topic}: {topic_metadata}")
    if len(topic_metadata.partitions) != 1:
        raise RuntimeError(
            f"Expected one partition for {topic}, got {len(topic_metadata.partitions)}"
        )


def produce_event(bootstrap_servers: str, topic: str) -> dict[str, Any]:
    deliveries: list[dict[str, Any]] = []

    def on_delivery(error: Any, message: Any) -> None:
        if error is not None:
            raise RuntimeError(f"Kafka delivery failed: {error}")
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
            "client.id": "manufacturing-kafka-test0-producer",
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    producer.produce(
        topic=topic,
        key=TEST_KEY.encode("utf-8"),
        value=json.dumps(TEST_EVENT, sort_keys=True).encode("utf-8"),
        on_delivery=on_delivery,
    )
    remaining = producer.flush(30)
    if remaining != 0 or len(deliveries) != 1:
        raise RuntimeError(
            f"Expected one acknowledged delivery, remaining={remaining}, "
            f"deliveries={deliveries!r}"
        )
    return deliveries[0]


def consume_event(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    timeout_seconds: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "client.id": "manufacturing-kafka-test0-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    deadline = time.monotonic() + timeout_seconds
    try:
        consumer.subscribe([topic])
        while time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error() is not None:
                raise RuntimeError(f"Kafka consume failed: {message.error()}")

            payload = json.loads(message.value().decode("utf-8"))
            if payload.get("event_id") != TEST_EVENT["event_id"]:
                continue

            key = message.key().decode("utf-8") if message.key() else None
            if payload != TEST_EVENT or key != TEST_KEY:
                raise RuntimeError(
                    f"Round-trip mismatch: key={key!r}, payload={payload!r}"
                )

            committed = consumer.commit(message=message, asynchronous=False)
            coordinate = {
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
            }
            commit_evidence = [
                {
                    "topic": item.topic,
                    "partition": item.partition,
                    "next_offset": item.offset,
                }
                for item in committed
            ]
            return coordinate, {
                "group_id": group_id,
                "manual_commit": True,
                "committed_offsets": commit_evidence,
            }
    finally:
        consumer.close()

    raise TimeoutError(
        f"Did not consume event_id={TEST_EVENT['event_id']} within {timeout_seconds}s"
    )


def write_json_atomically(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def main() -> None:
    args = parse_args()
    admin = AdminClient({"bootstrap.servers": args.bootstrap_servers})
    create_single_partition_topic(admin, args.topic)

    produced = produce_event(args.bootstrap_servers, args.topic)
    consumed, commit = consume_event(
        args.bootstrap_servers,
        args.topic,
        args.group_id,
        args.timeout_seconds,
    )
    if produced != consumed:
        raise RuntimeError(
            f"Produced and consumed Kafka coordinates differ: {produced!r} != {consumed!r}"
        )

    client_version, client_version_number = libversion()
    evidence = {
        "status": "passed",
        "scope": "local Kafka Test 0 produce/consume round-trip",
        "bootstrap_servers": args.bootstrap_servers,
        "topic": args.topic,
        "partition_count": 1,
        "event_id": TEST_EVENT["event_id"],
        "message_key": TEST_KEY,
        "producer": {
            "enable_idempotence": True,
            "acks": "all",
            "coordinate": produced,
        },
        "consumer": {"coordinate": consumed, **commit},
        "client": {
            "name": "confluent-kafka/librdkafka",
            "librdkafka_version": client_version,
            "librdkafka_version_number": client_version_number,
        },
        "claim_boundary": {
            "verified": [
                "one local broker connection",
                "one topic with one partition",
                "one acknowledged produce",
                "one consume with matching key and payload",
                "manual consumer offset commit",
            ],
            "not_verified": [
                "raw landing",
                "restart or replay behavior",
                "multi-broker availability",
                "end-to-end exactly-once",
                "production security or operations",
            ],
        },
    }
    write_json_atomically(args.output, evidence)
    print(json.dumps(evidence, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
