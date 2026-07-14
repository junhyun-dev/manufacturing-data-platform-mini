from __future__ import annotations

import argparse
import json

from manufacturing_data_platform.kafka_ingestion.contracts import sample_machine_event
from manufacturing_data_platform.kafka_ingestion.runtime import (
    DEFAULT_TOPIC,
    consume_and_land,
    ensure_single_partition_topic,
    produce_events,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run bounded Kafka K1 commands.")
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:19092")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    subparsers = parser.add_subparsers(dest="command", required=True)

    produce = subparsers.add_parser("produce-sample")
    produce.add_argument("--count", type=int, default=3)

    consume = subparsers.add_parser("consume")
    consume.add_argument("--group-id", default="manufacturing-k1")
    consume.add_argument("--output-dir", default="data/kafka_raw")
    consume.add_argument("--max-messages", type=int, required=True)
    consume.add_argument("--timeout-seconds", type=float, default=30.0)
    consume.add_argument("--replay-start-offset", type=int, default=None)
    consume.add_argument("--no-commit", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    topic = ensure_single_partition_topic(args.bootstrap_servers, args.topic)
    if args.command == "produce-sample":
        if args.count < 1:
            raise ValueError("count must be >= 1")
        result = {
            "topic": topic,
            "deliveries": produce_events(
                [sample_machine_event(index) for index in range(1, args.count + 1)],
                bootstrap_servers=args.bootstrap_servers,
                topic=args.topic,
            ),
        }
    else:
        result = {
            "topic": topic,
            "consumer": consume_and_land(
                bootstrap_servers=args.bootstrap_servers,
                topic=args.topic,
                group_id=args.group_id,
                output_dir=args.output_dir,
                max_messages=args.max_messages,
                timeout_seconds=args.timeout_seconds,
                replay_start_offset=args.replay_start_offset,
                commit_offsets=not args.no_commit,
            ),
        }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
