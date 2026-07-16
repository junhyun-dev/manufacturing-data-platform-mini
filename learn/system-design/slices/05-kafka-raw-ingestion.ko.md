# 05. Kafka Raw Ingestion Slice

상태: implemented / broker-verified local K1

> 이 문서는 첫 Kafka build에서 무엇을 Core로 잡고 무엇을 뒤로 미룰지 정하는 얇은 slice map이다.
> K1 bounded producer/consumer, immutable JSONL landing, crash recovery, bounded replay,
> invalid-event quarantine까지 local broker에서 검증됐다.

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
- [`../source-contracts/02-kafka-machine-event-v1.md`](../source-contracts/02-kafka-machine-event-v1.md)

CSV batch source와 Kafka event source는 입력 단위와 identity가 다르므로 별도 contract로 유지한다.

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

### Closed K1 Decisions

| Question | K1 decision |
|---|---|
| topic partition 수와 key를 무엇으로 할 것인가? | `manufacturing.machine-events.v1`, partition 1개, key=`machine_id`. |
| raw landing format을 JSONL/Parquet 중 무엇으로 할 것인가? | 읽기 쉬운 bounded JSONL batch + manifest. Parquet은 scale pressure 전까지 Backlog. |
| offset commit과 landing write 사이 duplicate를 어디서 제거할 것인가? | immutable manifest의 coordinate+fingerprint로 재전달을 재사용하고 landing 뒤 commit. |

닫힌 환경 gate (`2026-07-14`; 상세 결과는 [`../../../VERIFICATION_LOG.md`](../../../VERIFICATION_LOG.md)):

```text
Java 17 available
usable Docker runtime unavailable
Kafka 4.3.1 KRaft binary checksum verified and started locally
confluent-kafka 2.15.0 wheel installed in an isolated venv
one-topic / one-partition produce-consume-manual-commit round-trip passed
```

## 4. Decisions

Test 0에서 확정한 runtime decision:

```text
Kafka runtime: downloaded Apache Kafka 4.3.1 KRaft binary
Python client: confluent-kafka 2.15.0 in an isolated venv
Reason: synchronous API fit, explicit idempotent producer support, available CPython 3.10 wheel
Producer contract: enable.idempotence=true and acks=all explicitly configured
```

K1 accepted decision:

```text
Use a downloaded local Kafka KRaft runtime (Kafka 4.x is KRaft-only) because Docker is unavailable.
Use one local broker, one topic, and one partition, with synthetic data only.
Keep the event contract JSON and versioned for the first slice.
Use machine_id as the message key.
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

결정 노트:

- [`../../reference-decisions/kafka-event-identity-and-key.md`](../../reference-decisions/kafka-event-identity-and-key.md)
- [`../../reference-decisions/kafka-offset-and-landing-commit.md`](../../reference-decisions/kafka-offset-and-landing-commit.md)

## 5. Evidence

현재:

```text
scenario: written
question bank: written
slice scope: design-reviewed
Kafka Test 0 runbook: scripts/verify_kafka_test0.sh
Kafka Test 0 client: scripts/kafka_test0_roundtrip.py
Kafka runtime verification: passed (one broker/topic/partition/event + manual offset commit)
K1 contract/landing/runtime: src/manufacturing_data_platform/kafka_ingestion/
K1 unit tests: tests/test_kafka_ingestion.py
K1 broker verification: scripts/verify_kafka_k1.sh
K1 runtime evidence: /tmp/manufacturing-mini-kafka-k1-evidence/kafka_k1_verification.json
```

구현 evidence:

- producer/consumer source files
- event contract and landing unit tests
- broker-backed K1 verification with explicit runtime runbook
- local KRaft verification runbook
- `VERIFICATION_LOG.md` runtime entry

Test 0 (runtime gate — verified 2026-07-14):

```text
Kafka 4.x KRaft 단일 broker:
  random-uuid -> format --standalone -t <cluster-id> -c config/server.properties
  -> kafka-server-start.sh config/server.properties
topic 1개(partition=1) 생성
선택한 client wheel 설치 + produce 1 / consume 1 round-trip
broker 없으면 integration test는 명시적 skip 사유로 남긴다 (false green 금지)
```

재현 명령:

```bash
./scripts/verify_kafka_test0.sh
```

Test 0은 환경과 최소 client/broker 계약만 닫는다. K1 correctness는 별도
`scripts/verify_kafka_k1.sh`와 unit tests가 검증한다.

K1 test contract (verified):

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
local one-broker Kafka producer/consumer proof
synthetic event raw landing with Kafka coordinate evidence
bounded restart/replay behavior verified locally
invalid event quarantine without blocking the single partition
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

> K1.5 (accepted JSONL -> batch bridge)는 구현 및 local runtime 검증됐다: [`06-kafka-landing-to-batch.ko.md`](06-kafka-landing-to-batch.ko.md).

```text
K1.5 answer: accepted JSONL is adapted deterministically into the existing batch row contract, reusing quality/gold/Iceberg publish without Spark Structured Streaming.
Is Spark Structured Streaming (K2) actually needed, or only when a window/latency pressure is named? Otherwise the next real pressure may be failure-state forensics.
Does Airflow own only bounded replay/backfill and downstream publish, not the continuous consumer?
```
