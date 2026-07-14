# 03. Scenario Walkthrough - 제조 설비 이벤트를 Kafka로 받아 raw에 보존한다

상태: design-reviewed / design-only / Kafka Test 0 pending

목적: 현재의 일 단위 CSV batch 앞에 Kafka ingestion이 정말 필요한 상황을 만들고, 구현 전에 풀어야 할 질문을 구체적인 운영 흐름에서 도출한다.

이 문서는 `Kafka를 써보고 싶다`에서 출발하지 않는다.

```text
어떤 사용자가
어떤 시간 압력과 실패 위험 때문에
파일 도착을 기다리는 현재 방식만으로는 부족한가?
```

를 먼저 고정한다.

## 1. Current State

현재 입력은 하루치 manufacturing event row가 들어 있는 CSV 파일이다.

```text
source CSV file
-> Python bronze/silver/gold pipeline
-> quality/catalog/lineage evidence
-> successful gold CSV
-> local Spark/Iceberg publish
```

현재 방식이 이미 잘 푸는 문제:

- 같은 파일 재실행을 `source_hash`로 skip한다.
- 같은 `business_date`의 정정 결과를 Iceberg partition overwrite로 교체한다.
- operator가 run/source/quality/lineage evidence를 조회할 수 있다.

현재 방식이 아직 다루지 않는 문제:

- 설비 이벤트를 파일 마감 전에 계속 수신한다.
- event 단위 identity와 순서를 보존한다.
- consumer 장애 후 마지막 처리 위치에서 다시 시작한다.
- 늦게 도착하거나 중복 전달된 event를 다룬다.
- 특정 offset부터 event를 replay한다.

## 2. Service Pressure

가상의 공장 설비가 생산 이벤트를 계속 발생시킨다.

```text
mc-101 -> 08:00 production event
mc-102 -> 08:01 inspection event
mc-101 -> 08:02 production event
...
```

Source owner는 하루가 끝난 뒤 CSV를 한 번 전달하는 대신, event가 생길 때마다 ingestion endpoint를 통해 보낸다.

운영자와 데이터 사용자는 다음을 원한다.

```text
파일 마감 전에 event를 받아 raw 형태로 보존하고 싶다.
consumer가 잠시 죽어도 Kafka에 남은 event를 다시 읽고 싶다.
특정 event가 어느 topic/partition/offset에서 왔는지 알고 싶다.
같은 event가 재전송되어도 downstream 결과가 두 배가 되지 않게 하고 싶다.
```

여기서 첫 slice의 서비스 가치는 실시간 dashboard가 아니다.

```text
event를 잃지 않고 replay 가능한 raw landing으로 넘기는 것
```

이다.

## 2.1 왜 Kafka인가 (file-drop이 아니라)

Kafka를 고른 이유는 실시간 latency 요구 때문이 아니다. offset, replay(seek), consumer group restart, partition ordering, at-least-once 재전달 같은 **log 기반 ingestion primitive를 synthetic 데이터로 직접 exercise**하기 위함이다.

```text
file-drop으로 충분한 것:   EOD 전 수신 + durable landing
file-drop으로 부족한 것:   seek-to-offset replay, consumer group committed offset,
                          partition ordering, 재전달 event dedup
DB queue로 충분한 것:      durable at-least-once work queue
DB queue로 부족한 것:      불변 event log + retention 기반 replay
```

즉 이 slice의 목적은 "이 공장이 실시간이 필요하다"가 아니라 "log ingestion semantics를 학습하고 검증한다"이다. 없는 latency SLA를 만들지 않는다.

## 3. Actors

| Actor | Wants to do | Needs to inspect | Must not assume |
|---|---|---|---|
| Machine event producer | 발생한 event를 broker에 전달 | delivery result, event_id | send success가 downstream publish success라는 것 |
| Ingestion operator | topic과 consumer가 정상인지 확인 | partition, offset, lag, error evidence | DAG success만으로 모든 event가 반영됐다는 것 |
| Raw landing consumer | event를 durable raw file로 기록 | topic/partition/offset, event_id | poll한 순간 처리가 끝났다는 것 |
| Batch/Spark pipeline | landed raw를 읽어 silver/gold로 변환 | event contract, landing manifest | Kafka offset만으로 business duplicate가 제거된다는 것 |
| Reviewer | Kafka claim이 evidence와 맞는지 확인 | code, broker run, replay test | local one-broker proof를 production streaming 운영으로 확대하는 것 |

## 4. Candidate Event Shape

아래는 확정 source contract가 아니라 질문을 구체화하기 위한 후보다.

```json
{
  "event_id": "evt-20260629-mc101-000001",
  "schema_version": 1,
  "event_time": "2026-06-29T08:00:00Z",
  "plant_id": "plant-a",
  "line_id": "line-1",
  "work_order_id": "wo-1001",
  "machine_id": "mc-101",
  "product_code": "gearbox-a",
  "operation": "assembly",
  "units_produced": 120,
  "defect_count": 2,
  "cycle_time_ms": 840,
  "business_date": "2026-06-29"
}
```

현재 CSV row와 다른 후보 필드:

| Field | Why it may be needed |
|---|---|
| `event_id` | retry와 business duplicate를 구분하는 event identity |
| `schema_version` | producer/consumer contract evolution |
| Kafka key | 같은 설비의 event를 같은 partition으로 보내는 ordering 단위 후보 |
| topic/partition/offset | broker log에서 event 위치를 찾고 replay하는 transport identity |

중요한 구분:

```text
event_id                        = business event identity
topic/partition/offset          = Kafka transport position (log에서의 물리적 위치)
consumer-group committed offset = 특정 consumer group이 다음에 읽을 위치 (group마다 다르며 raw offset과 별개)
run_id                          = downstream batch/publish execution identity
snapshot_id                     = Iceberg table commit identity
```

네 값은 서로 대체하지 않는다.

## 5. Primary Scenario

```text
1. synthetic producer가 manufacturing event 10개를 만든다.
2. producer가 event를 Kafka topic에 publish한다.
3. message key에 따라 event가 partition에 기록된다.
4. raw landing consumer가 consumer group으로 event를 읽는다.
5. consumer가 payload와 Kafka coordinates를 immutable raw output에 기록한다.
6. durable write가 끝난 뒤에만 처리 위치를 확정한다.
7. operator가 produced count, landed count, offset range, error count를 확인한다.
8. consumer를 다시 실행해도 이미 확정된 event가 조용히 두 배로 쌓이지 않는지 확인한다.
9. 필요하면 시작 offset을 되돌려 같은 event를 replay하고 결과를 비교한다.
```

## 6. Failure Variants

### A. Producer retry

```text
broker 응답을 받기 전에 producer connection이 끊긴다.
producer가 같은 event를 다시 보낸다.
```

질문:

- Kafka producer idempotence로 transport duplicate를 막을 것인가?
- 같은 business event가 새 request로 다시 생성된 경우 `event_id`로 따로 제거할 것인가?

### B. Consumer crashes before durable write

```text
consumer가 poll은 했지만 raw file을 완성하기 전에 죽는다.
```

질문:

- offset이 아직 확정되지 않아 재시작 시 다시 읽히는가?
- incomplete temp file은 어떻게 구분하고 정리하는가?

### C. Consumer crashes after durable write but before offset commit

```text
raw file은 완성됐지만 offset commit 전에 consumer가 죽는다.
```

질문:

- 같은 event가 다시 읽혀 duplicate raw record가 생기는가?
- `(topic, partition, offset)` 또는 `event_id`로 idempotent landing을 만들 것인가?

### D. Invalid event

```text
필수 필드가 없거나 schema_version을 모르는 event가 들어온다.
```

질문:

- 전체 consumer를 멈출 것인가?
- retry 후 quarantine/DLQ로 보낼 것인가?
- 원본 payload와 validation error를 어떤 evidence로 남길 것인가?

### E. Late or out-of-order event

```text
event_time=08:00인 event가 09:00 event보다 늦게 도착한다.
```

질문:

- raw landing은 도착 순서대로 보존하고 downstream에서 event time을 해석할 것인가?
- `business_date` gold를 언제 닫고, 늦은 event가 오면 correction으로 다시 열 것인가?

### F. Consumer group rebalance

```text
consumer 인스턴스가 추가/제거되거나 poll이 너무 길어 group에서 빠지면 partition 소유자가 바뀐다.
```

질문:

- revoke 시점에 처리 중이던 event의 flush/commit 계약은 무엇인가?
- Kafka 4.0의 KIP-848 protocol(서버 기본 on, consumer는 `group.protocol=consumer`로 opt-in)과 classic protocol 중 무엇을 쓰는가?
- K1은 1-consumer/1-partition이라 rebalance가 사실상 발생하지 않는다 — 이 variant는 multi-consumer Backlog에서 다룬다.

### G. Broker unavailable

```text
broker가 잠시 죽거나 네트워크가 끊긴다.
```

질문:

- producer는 event를 어디에 쌓는가(bounded retry / local spool / fail)?
- consumer 재접속 시 committed offset에서 안전하게 재개하는가?
- broker retention이 필요한 replay 구간보다 짧으면 어떤 source를 믿는가?

## 7. State Trace Candidate

| Moment | State | Candidate evidence |
|---|---|---|
| t1 | event created | `event_id`, `event_time`, `schema_version` |
| t2 | producer acknowledged | topic, partition, offset |
| t3 | consumer polled | consumer group, poll timestamp |
| t4 | raw write in progress | temporary landing path |
| t5 | raw write durable | final immutable path, row count, offset range |
| t6 | offset committed | committed offset by partition |
| t7 | downstream batch starts | `run_id`, input landing manifest |
| t8 | Iceberg publish commits | `snapshot_id`, source event/offset range |

이 표는 아직 구현 상태가 아니다. audit와 decision에서 실제 state contract로 줄인다.

## 8. Questions Triggered

이 시나리오가 여는 핵심 질문 영역:

```text
service latency / why streaming
event identity / schema contract
topic / key / partition / ordering
producer acknowledgement / retry / idempotence
consumer group / offset / commit / rebalance
event time / late data / watermark
raw landing atomicity / duplicate handling
failure / retry / replay / quarantine
retention / compaction
security / credential handling
lag / throughput / error observability
local reproducibility / integration testing
Airflow vs long-running consumer ownership
Kafka -> Spark Structured Streaming -> Iceberg boundary
public claim boundary
```

상세 질문은 [`../question-bank/08-kafka-streaming-ingestion.ko.md`](../question-bank/08-kafka-streaming-ingestion.ko.md)에 둔다.

## 9. Candidate First Slice Success

아직 구현 전이며, 아래는 audit에서 자를 후보다.

```text
local single-broker Kafka starts
synthetic producer publishes a bounded event set
consumer lands payload + topic/partition/offset evidence
landed event count reconciles with produced event count
consumer restart does not silently double the accepted landing set
one replay path is demonstrated and bounded
```

## 10. Explicit Non-Goals

첫 slice에서 구현하지 않을 후보:

```text
production Kafka cluster
multi-broker failure tolerance claim
exactly-once end-to-end claim
Schema Registry deployment
CDC connector
Spark Structured Streaming job
direct Kafka -> Iceberg production sink
real-time dashboard
Airflow-managed infinite consumer
Kubernetes deployment
TLS/SASL/ACL runtime
K1을 continuous "streaming pipeline"이라 부르기 (K1은 bounded raw ingestion이지 계속 도는 streaming job이 아니다)
```

## 11. Audit Request

Claude/외부 audit에서는 다음을 확인한다.

```text
이 시나리오에 Kafka가 실제로 필요한가, 단순 queue/file watch로 충분한가?
빠진 producer/consumer/offset/rebalance/failure 질문은 무엇인가?
event_id와 Kafka coordinates의 경계가 정확한가?
first slice Core가 너무 크거나 너무 작은가?
Kafka -> raw landing 뒤 Spark/Iceberg 연결 순서가 타당한가?
local proof를 public claim으로 옮길 때 과장 위험은 무엇인가?
```
