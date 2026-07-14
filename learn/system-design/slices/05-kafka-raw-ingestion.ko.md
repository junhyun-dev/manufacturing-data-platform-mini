# 05. Kafka Raw Ingestion Slice

상태: design-reviewed / design-only / Kafka Test 0 pending

> 이 문서는 첫 Kafka build에서 무엇을 Core로 잡고 무엇을 뒤로 미룰지 정하는 얇은 slice map이다.
> Kafka code, client dependency, broker runtime evidence는 아직 없다.

## 1. Slice Thesis

```text
synthetic manufacturing event를 local Kafka에 publish하고,
bounded consumer가 payload와 topic/partition/offset evidence를
replay 가능한 immutable raw landing으로 남긴다.
```

이 slice의 목적은 실시간 gold dashboard나 production streaming platform이 아니다.

## 2. Primary Scenario

Source owner가 하루치 CSV 마감을 기다리지 않고 manufacturing event를 계속 보낸다.

운영자는 consumer가 잠시 실패해도 broker에 남은 event를 다시 읽고, 어떤 event가 어느 Kafka 위치에서 왔는지 확인하고 싶다.

관련 문서:

- [`../scenarios/03-kafka-machine-event-ingestion.md`](../scenarios/03-kafka-machine-event-ingestion.md)
- [`../question-bank/08-kafka-streaming-ingestion.ko.md`](../question-bank/08-kafka-streaming-ingestion.ko.md)
- [`../source-contracts/01-manufacturing-csv.md`](../source-contracts/01-manufacturing-csv.md)

새 event source contract는 질문 audit 뒤 별도 문서로 확정한다.

## 3. Question Areas Pulled

관련 question-bank 영역:

- service latency / why Kafka
- event grain / identity / schema version
- topic / key / partition / ordering
- producer acknowledgement / retry / idempotence
- consumer group / offset / commit / replay
- delivery semantics / duplicate handling
- raw landing atomicity
- failure / invalid event / quarantine
- retention / lifecycle
- security / credential boundary
- observability / reconciliation / lag
- testing / local reproducibility
- Airflow / Spark / Iceberg responsibility boundary
- public claim boundary

### Core Questions

| Core question | Why Core |
|---|---|
| Kafka가 필요한 service pressure는 무엇인가? | 이유가 없으면 queue/file batch로 충분하다. |
| event 한 건의 grain과 `event_id`는 무엇인가? | duplicate와 downstream metric 정확성을 결정한다. |
| topic과 message key는 무엇인가? | partition placement와 ordering 범위를 결정한다. |
| producer는 어떤 ack/retry/idempotence 계약을 갖는가? | send failure와 transport duplicate를 다룬다. |
| consumer group과 offset commit 시점은 무엇인가? | event 유실과 재전달의 핵심 경계다. |
| raw landing은 어떤 atomic unit으로 완성되는가? | partial file 노출과 restart duplicate를 막는다. |
| `(topic, partition, offset)`과 `event_id`를 어디에 남기는가? | replay, lineage, transport/business dedup evidence다. |
| produced count와 accepted landing count를 어떻게 reconcile하는가? | first slice end-to-end proof다. |
| broker 없는 테스트와 broker runtime test를 어떻게 나누는가? | 빠른 contract test와 Kafka skill evidence를 모두 만든다. |
| public claim은 local one-broker proof 어디까지인가? | 분산/HA/production overclaim을 막는다. |

### Demo Questions

| Demo question | Why not Core |
|---|---|
| consumer lag를 CLI로 보여줄 것인가? | 운영 감각은 보여주지만 raw landing contract를 바꾸지 않는다. |
| 여러 partition에서 같은 key ordering을 시각화할 것인가? | ordering 이해에는 좋지만 first slice correctness test로 대체할 수 있다. |
| 명시적 offset reset으로 replay를 보여줄 것인가? | 유용하지만 restart/re-read contract가 먼저다. |

### Backlog Questions

| Backlog question | Reason |
|---|---|
| Schema Registry를 배포할 것인가? | event JSON contract proof보다 큰 운영 surface다. |
| DLQ topic과 retry topic을 만들 것인가? | first slice는 local quarantine evidence로 단순화할 수 있다. |
| Spark Structured Streaming으로 직접 읽을 것인가? | checkpoint/window/state/connector version을 다루는 별도 slice다. |
| Kafka event를 Iceberg에 직접 쓰는가? | stream checkpoint와 table commit consistency를 별도로 설계해야 한다. |
| Airflow가 continuous consumer를 실행하는가? | 끝나지 않는 workload와 batch scheduler lifecycle이 맞는지 별도 결정이 필요하다. |
| multi-broker replication/failover를 검증할 것인가? | local one-broker functional proof 범위를 넘는다. |
| TLS/SASL/ACL을 실제 구성할 것인가? | production security slice다. credential leak scan은 계속 Core publication gate다. |
| Kubernetes에 broker/consumer를 배포할 것인가? | deployment/operations는 Kafka contract 뒤의 별도 slice다. |

### Unknowns

| Unknown | How to close |
|---|---|
| local Kafka KRaft binary와 Python client가 현재 WSL에서 동작하는가? | version pin 후 broker start, topic create, produce/consume Test 0. |
| 어떤 Python client를 쓸 것인가? | confluent-kafka(librdkafka C wheel) / kafka-python(pure Python) / aiokafka(async)를 sync API 적합성, runtime dependency, wheel availability, protocol support, testability로 비교 후 ADR. 셋 다 2026 기준 유지되므로 "유지보수"만으로 결정하지 않는다. idempotence 기본값이 client마다 다른 점(Java true, librdkafka false)도 함께 본다. |
| topic partition 수와 key를 무엇으로 할 것인가? | event rate/order pressure와 audit feedback으로 결정. |
| raw landing format을 JSONL/Parquet 중 무엇으로 할 것인가? | first-slice readability, atomicity, small-file tradeoff ADR. |
| offset commit과 landing write 사이 duplicate를 어디서 제거할 것인가? | failure injection test contract로 결정. |

현재 환경 evidence (`2026-07-13` discovery snapshot; 구현 직전 재검증):

```text
Java 17 available
usable Docker runtime unavailable
Kafka binary not installed
```

## 4. Decision Candidates

audit 전 working direction이며 Accepted decision이 아니다.

```text
Use a downloaded local Kafka KRaft runtime (Kafka 4.x is KRaft-only) because Docker is unavailable.
Use one local broker, one topic, and one partition, with synthetic data only.
Keep the event contract JSON and versioned for the first slice.
Use machine_id as the initial message-key candidate.
Use a stable consumer group and manual offset commit after durable landing.
Land bounded immutable JSONL batches through temp-file -> atomic rename.
Preserve event_id plus topic/partition/offset in raw evidence.
Assume at-least-once delivery and do not claim end-to-end exactly-once.
Decide producer idempotence explicitly per chosen client (Java default true, librdkafka/confluent-kafka default false).
Keep Spark Structured Streaming, Iceberg sink, and continuous Airflow ownership out of K1.
```

K1 단순화 경계:

```text
partition=1은 단순화를 위한 선택이다. total ordering을 자명하게 만들지만,
key-based partition routing과 consumer rebalance는 검증하지 못한다.
multi-partition routing/rebalance는 별도 Backlog slice에서 다룬다.
```

필요한 decision note 후보:

```text
reference-decisions/kafka-event-identity-and-key.md
reference-decisions/kafka-offset-and-landing-commit.md
```

둘 다 audit 뒤 Core로 남은 경우에만 만든다.

## 5. Evidence

현재:

```text
scenario: written
question bank: written
slice scope: proposed
Kafka code: none
Kafka tests: none
Kafka runtime verification: none
```

구현 뒤 필요한 evidence 후보:

- producer/consumer source files
- event contract and landing unit tests
- optional Kafka integration tests with explicit skip reason
- local KRaft verification runbook
- `VERIFICATION_LOG.md` runtime entry

Test 0 (runtime gate — 구현 전):

```text
Kafka 4.x KRaft 단일 broker:
  random-uuid -> format --standalone -t <cluster-id> -c config/server.properties
  -> kafka-server-start.sh config/server.properties
topic 1개(partition=1) 생성
선택한 client wheel 설치 + produce 1 / consume 1 round-trip
broker 없으면 integration test는 명시적 skip 사유로 남긴다 (false green 금지)
```

Test contract candidate:

```text
given a clean local topic and a bounded synthetic event set
when the producer publishes N valid events
and the raw landing consumer processes them
then N accepted events are durably landed
and each accepted event records event_id/topic/partition/offset
and a normal consumer restart resumes from the committed offset
and a simulated crash after durable landing but before offset commit
    causes redelivery without silently doubling the accepted set
and one replay path has explicit, inspectable evidence
```

## 6. Claim Boundary

Allowed now:

```text
designed a Kafka raw-ingestion slice
mapped event identity, partition ordering, offset, replay, and failure questions
```

Allowed only after runtime verification:

```text
local one-broker Kafka producer/consumer proof
synthetic event raw landing with Kafka coordinate evidence
bounded restart/replay behavior verified locally
```

Forbidden:

```text
production Kafka operation
multi-broker HA/failover
large-scale real-time processing
end-to-end exactly-once
Kafka -> Spark -> Iceberg production streaming pipeline
Airflow-managed continuous streaming service
secure multi-tenant Kafka cluster
calling K1 a "streaming pipeline" (K1 is bounded raw ingestion, not a continuous streaming job)
```

## 7. Next Questions

```text
Can Test 0 start a pinned local KRaft broker without Docker?
Which exact Kafka patch version and Python client version should Test 0 pin?
Should K1 end at raw JSONL landing, then K1.5 adapt landed JSONL into the existing batch row contract to reuse gold/Iceberg publish (no Spark Structured Streaming yet)?
Is Spark Structured Streaming (K2) actually needed, or only when a window/latency pressure is named? Otherwise the next real pressure may be failure-state forensics.
Does Airflow own only bounded replay/backfill and downstream publish, not the continuous consumer?
```
