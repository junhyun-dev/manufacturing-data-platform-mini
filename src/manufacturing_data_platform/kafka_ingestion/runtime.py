from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Mapping

from manufacturing_data_platform.kafka_ingestion.contracts import (
    serialize_machine_event,
    validate_machine_event,
)
from manufacturing_data_platform.kafka_ingestion.landing import (
    KafkaRecord,
    LandingResult,
    land_records,
)


DEFAULT_TOPIC = "manufacturing.machine-events.v1"


def ensure_single_partition_topic(
    bootstrap_servers: str,
    topic: str = DEFAULT_TOPIC,
) -> dict[str, Any]:
    AdminClient, NewTopic, KafkaError = _admin_types()
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
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
        raise RuntimeError(f"topic metadata unavailable for {topic}: {topic_metadata}")
    partition_count = len(topic_metadata.partitions)
    if partition_count != 1:
        raise RuntimeError(f"K1 requires one partition, got {partition_count}")
    return {"topic": topic, "partition_count": partition_count}


def produce_events(
    events: Iterable[Mapping[str, Any]],
    *,
    bootstrap_servers: str,
    topic: str = DEFAULT_TOPIC,
) -> list[dict[str, Any]]:
    Producer = _producer_type()
    deliveries: list[dict[str, Any]] = []
    delivery_errors: list[str] = []

    def delivery_callback(event_id: str) -> Any:
        def on_delivery(error: Any, message: Any) -> None:
            if error is not None:
                delivery_errors.append(str(error))
                return
            deliveries.append(
                {
                    "event_id": event_id,
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                }
            )

        return on_delivery

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "manufacturing-k1-producer",
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    expected = 0
    for raw_event in events:
        event = validate_machine_event(raw_event)
        producer.produce(
            topic=topic,
            key=event["machine_id"].encode("utf-8"),
            value=serialize_machine_event(event),
            on_delivery=delivery_callback(event["event_id"]),
        )
        producer.poll(0)
        expected += 1

    remaining = producer.flush(30)
    if delivery_errors:
        raise RuntimeError(f"Kafka delivery failed: {delivery_errors!r}")
    if remaining != 0 or len(deliveries) != expected:
        raise RuntimeError(
            f"Expected {expected} acknowledged deliveries, remaining={remaining}, "
            f"deliveries={deliveries!r}"
        )
    return sorted(deliveries, key=lambda item: (item["partition"], item["offset"]))


def consume_and_land(
    *,
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    output_dir: str | Path,
    max_messages: int,
    timeout_seconds: float = 30.0,
    replay_start_offset: int | None = None,
    commit_offsets: bool = True,
    simulate_crash_after_landing: bool = False,
) -> dict[str, Any]:
    if max_messages < 1:
        raise ValueError("max_messages must be >= 1")
    if replay_start_offset is not None and replay_start_offset < 0:
        raise ValueError("replay_start_offset must be >= 0")
    if replay_start_offset is not None and commit_offsets:
        raise ValueError("bounded replay must not mutate the normal committed offset")

    Consumer, TopicPartition = _consumer_types()
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "client.id": "manufacturing-k1-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    records: list[KafkaRecord] = []
    try:
        if replay_start_offset is None:
            consumer.subscribe([topic])
        else:
            consumer.assign([TopicPartition(topic, 0, replay_start_offset)])

        import time

        deadline = time.monotonic() + timeout_seconds
        while len(records) < max_messages and time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error() is not None:
                raise RuntimeError(f"Kafka consume failed: {message.error()}")
            timestamp_type, timestamp_ms = message.timestamp()
            records.append(
                KafkaRecord(
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                    key=(
                        message.key().decode("utf-8", errors="strict")
                        if message.key() is not None
                        else None
                    ),
                    value=message.value(),
                    timestamp_ms=timestamp_ms if timestamp_type > 0 else None,
                )
            )

        if len(records) != max_messages:
            raise TimeoutError(
                f"expected {max_messages} Kafka messages, received {len(records)} "
                f"within {timeout_seconds}s"
            )

        landing = land_records(
            records,
            output_dir,
            simulate_crash_after_rename=simulate_crash_after_landing,
        )
        commit_evidence: list[dict[str, Any]] = []
        if commit_offsets:
            offsets = [
                TopicPartition(item["topic"], item["partition"], item["next_offset"])
                for item in landing.committable_offsets
            ]
            committed = consumer.commit(offsets=offsets, asynchronous=False)
            commit_evidence = [
                {
                    "topic": item.topic,
                    "partition": item.partition,
                    "next_offset": item.offset,
                }
                for item in committed
            ]

        return {
            "mode": "replay" if replay_start_offset is not None else "normal",
            "group_id": group_id,
            "input_coordinates": [
                {
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                }
                for record in records
            ],
            "landing": landing.to_dict(),
            "offset_commit": {
                "performed": commit_offsets,
                "offsets": commit_evidence,
            },
        }
    finally:
        consumer.close()


def _producer_type() -> Any:
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise RuntimeError(
            "Kafka runtime dependency is missing; install requirements-kafka.txt"
        ) from exc
    return Producer


def _consumer_types() -> tuple[Any, Any]:
    try:
        from confluent_kafka import Consumer, TopicPartition
    except ImportError as exc:
        raise RuntimeError(
            "Kafka runtime dependency is missing; install requirements-kafka.txt"
        ) from exc
    return Consumer, TopicPartition


def _admin_types() -> tuple[Any, Any, Any]:
    try:
        from confluent_kafka import KafkaError
        from confluent_kafka.admin import AdminClient, NewTopic
    except ImportError as exc:
        raise RuntimeError(
            "Kafka runtime dependency is missing; install requirements-kafka.txt"
        ) from exc
    return AdminClient, NewTopic, KafkaError
