# Kafka Event Identity and Message Key

ADR Status: Implemented
상태: accepted local K1 decision

## Context

Kafka record에는 topic/partition/offset이 있지만, producer가 같은 업무 사건을 새
record로 다시 만들면 offset은 달라진다. transport 위치와 business identity를 같은
것으로 보면 replay와 업무 중복을 구분할 수 없다.

## Decision

```text
business identity = event_id
transport evidence = (topic, partition, offset)
consumer progress = (consumer_group, topic, partition, committed_next_offset)
message key = machine_id
K1 partition count = 1
```

- `event_id`는 payload에 들어 있고 schema v1의 required field다.
- Kafka coordinate는 accepted/duplicate/quarantine manifest entry마다 보존한다.
- 같은 coordinate + 같은 fingerprint 재전달은 이전 landing을 재사용한다.
- 다른 coordinate + 같은 `event_id`는 accepted set에 더하지 않고 duplicate evidence로 남긴다.
- 같은 coordinate의 key/payload가 바뀌면 immutable-log consistency error다.
- `machine_id` key는 향후 같은 설비 event ordering 의도를 표현한다. K1 partition이
  하나이므로 key-based routing 자체를 검증했다고 claim하지 않는다.

## Alternatives

| Option | Why not K1 |
|---|---|
| offset만 identity로 사용 | producer가 재생성한 business duplicate를 못 잡음 |
| payload hash만 사용 | 같은 payload의 정당한 다른 event를 합칠 수 있음 |
| natural key 조합 | producer contract가 복잡해지고 correction 의미가 불명확 |
| global ordering key | 모든 event가 한 hot partition으로 모이는 설계가 됨 |

## Consequences

얻는 것:

- replay와 business duplicate를 별도로 설명할 수 있다.
- Kafka coordinate에서 raw event까지 추적 가능하다.
- K1.5 batch adapter가 `event_id`와 Kafka evidence를 보존할 수 있다.

경계:

- producer가 `event_id`를 안정적으로 재사용한다는 계약이 필요하다.
- event_id 충돌/오용을 중앙 registry로 막지는 않는다.
- multi-partition ordering/rebalance는 Backlog다.

## Evidence

- `learn/system-design/source-contracts/02-kafka-machine-event-v1.md`
- `src/manufacturing_data_platform/kafka_ingestion/contracts.py`
- `src/manufacturing_data_platform/kafka_ingestion/landing.py`
- `tests/test_kafka_ingestion.py`
