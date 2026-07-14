# Kafka Manufacturing Event v1 Source Contract

ADR Status: Implemented
상태: local K1 implemented and broker-verified

이 문서는 Kafka K1이 받는 synthetic manufacturing event 한 건의 입력 계약을 고정한다.
CSV source contract와 달리 입력 단위는 파일이 아니라 Kafka record 한 건이다.

## 1. 입력과 grain

```text
input unit     = one Kafka record
business grain = one manufacturing machine event
transport key = (topic, partition, offset)
business key  = event_id
message key   = machine_id
```

`event_id`는 producer가 같은 업무 사건을 다시 만들었는지 판단한다.
Kafka coordinate는 그 record가 log의 어디에서 왔는지 보여주는 transport evidence다.
둘은 서로 대체하지 않는다.

## 2. Topic과 partition

```text
topic      = manufacturing.machine-events.v1
partitions = 1 (K1 local scope)
key        = machine_id
```

K1의 partition 1개는 전체 ordering을 단순화한다. 여러 partition에서 같은 key routing,
consumer rebalance, parallelism은 검증하지 않는다.

## 3. Event schema v1

| Field | Type | Meaning |
|---|---|---|
| `event_id` | non-empty string | stable business-event identity |
| `schema_version` | integer `1` | event contract version |
| `event_time` | timezone-aware ISO timestamp | event occurrence time |
| `business_date` | ISO date | batch/publish boundary 후보 |
| `plant_id` | non-empty string | plant identity |
| `line_id` | non-empty string | line identity |
| `work_order_id` | non-empty string | work-order identity |
| `machine_id` | non-empty string | machine identity and Kafka message key |
| `product_code` | non-empty string | product identity |
| `operation` | accepted operation string | operation domain |
| `units_produced` | integer, `>= 0` | additive measure |
| `defect_count` | integer, `0 <= defects <= units` | additive defect measure |
| `cycle_time_ms` | integer, `> 0` | cycle-time measure |

v1은 strict contract다. required field 누락, unknown field, type/range 위반은 accepted
landing에 넣지 않고 quarantine evidence로 남긴다. Schema Registry는 K1 범위 밖이다.

## 4. Raw landing envelope

accepted JSONL 한 줄은 payload와 Kafka evidence를 함께 보존한다.

```json
{
  "event": {"event_id": "evt-20260629-000001", "schema_version": 1},
  "kafka": {
    "topic": "manufacturing.machine-events.v1",
    "partition": 0,
    "offset": 0,
    "key": "mc-101",
    "timestamp_ms": 1783999454137
  }
}
```

실제 `event`에는 v1 필드 전체가 들어간다. `manifest.json`에는 각 coordinate의
fingerprint, event_id, `accepted | duplicate_event_id | quarantined` 상태가 남는다.

## 5. Delivery와 failure contract

```text
consume record
-> validate and classify
-> write JSONL + manifest in staging directory
-> fsync files/directory
-> atomic rename to immutable batch path
-> commit next Kafka offset
```

landing 뒤 offset commit 전에 죽으면 같은 coordinate가 재전달된다. 기존 immutable
manifest의 fingerprint가 같으면 새 accepted row를 만들지 않고 offset commit만 복구한다.
같은 coordinate의 payload/key가 달라지면 consistency error다.

## 6. Claim boundary

구현 및 검증:

```text
bounded local producer/consumer
one topic / one partition
strict JSON event contract
immutable atomic JSONL landing
manual offset commit after landing
coordinate redelivery dedup
event_id duplicate evidence
invalid-event quarantine
bounded offset replay
```

미구현:

```text
continuous streaming service
multi-partition routing/rebalance
Schema Registry / Avro / Protobuf
TLS/SASL/ACL
multi-broker HA
Spark Structured Streaming / direct Iceberg sink
end-to-end exactly-once
```

## 7. Evidence

- `src/manufacturing_data_platform/kafka_ingestion/contracts.py`
- `src/manufacturing_data_platform/kafka_ingestion/landing.py`
- `src/manufacturing_data_platform/kafka_ingestion/runtime.py`
- `tests/test_kafka_ingestion.py`
- `scripts/verify_kafka_k1.sh`
- `VERIFICATION_LOG.md`
