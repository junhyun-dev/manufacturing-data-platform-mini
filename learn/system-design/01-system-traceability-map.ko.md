# 01. 전체 설계 추적 지도

상태: canonical design trace / implementation-backed index

이 문서는 프로젝트 전체를 아래 순서로 연결한다.

```text
서비스 목적
-> 시나리오와 실패 압력
-> Question Bank에서 가져온 질문
-> 이번 slice가 지킬 계약과 결정
-> 구현한 기능
-> code/test/runtime evidence
-> 아직 주장하지 않는 범위
```

최신 테스트 수와 실행 결과는 [`../../VERIFICATION_LOG.md`](../../VERIFICATION_LOG.md)가
source of truth다. 이 문서는 상태 숫자를 복사하지 않고 연결 관계만 관리한다.

## 1. 먼저 바로잡을 것: 이 프로젝트는 Kafka 프로젝트가 아니다

프로젝트의 중심은 다음 질문이다.

> 제조 스타일 데이터를 믿고 쓸 수 있는 지표로 바꾸고, 그 결과의 입력·품질·실행·재처리 근거를 설명할 수 있는가?

Kafka는 이 시스템의 여러 입력 방식 중 하나다.

```text
batch CSV ------------------------------------+
                                                |
Kafka event -> durable raw landing -> adapter --+-> batch pipeline
                                                     -> bronze / silver / gold
                                                     -> quality / catalog / lineage
                                                     -> local Iceberg publish

wide CSV variants -> mapping config -> EAV -> EAV gold

Airflow -> 위 CLI들을 호출하고 순서를 조정
operator report -> 남은 evidence를 읽어 원인을 좁힘
```

역할을 나누면 다음과 같다.

| 구성요소 | 해결하는 문제 |
|---|---|
| batch lakehouse spine | raw를 정제·집계하고 품질과 실행 근거를 남김 |
| EAV slice | 서로 다른 wide 형식을 mapping contract로 표준화 |
| Spark/Iceberg | 같은 날짜 정정 결과를 partition 교체와 snapshot으로 표현 |
| Airflow | business logic을 소유하지 않고 검증된 CLI의 실행 순서를 조정 |
| Kafka K1 | offset/replay가 있는 event log를 durable raw landing으로 보존 |
| Kafka K1.5 | accepted landing을 기존 batch spine이 읽는 결정적 입력으로 변환 |
| operator report | gold 숫자를 source/run/quality/lineage evidence로 추적 |

## 2. 기능은 출발점이 아니라 질문에 대한 답이다

이 프로젝트의 설계 순서는 아래와 같다.

```text
"Kafka를 붙인다"                         X
"Airflow DAG를 만든다"                   X

"commit 전에 consumer가 죽으면 무엇을 잃나?"
-> durable landing 뒤 offset commit
-> Kafka landing feature                  O

"같은 날짜 정정 파일이 오면 중복 없이 어떻게 교체하나?"
-> business_date partition overwrite
-> Iceberg publish feature                O
```

Question Bank는 모든 질문에 답하기 위한 체크리스트가 아니다. 시나리오가 생겼을 때
관련 영역의 질문을 넓게 가져오고, 그중 이번 결과를 바꾸는 질문만 Core contract로 내린다.

## 3. 시나리오별 추적

### S0. 일별 CSV를 믿을 수 있는 gold 지표로 만든다

| 단계 | 연결 |
|---|---|
| 문제 상황 | [`scenarios/00-scenario-seed.md`](scenarios/00-scenario-seed.md): raw CSV만으로는 중복·유실·schema 변화·출처를 설명할 수 없음 |
| 가져온 질문 | 입력 identity는 무엇인가? gold 한 행의 grain은 무엇인가? 필터링과 유실을 어떻게 구분하는가? 품질 실패가 trusted state를 바꾸는가? |
| Question Bank | [`question-bank/01-service-identity-contract.ko.md`](question-bank/01-service-identity-contract.ko.md), [`question-bank/02-quality-rerun-failure.ko.md`](question-bank/02-quality-rerun-failure.ko.md) |
| 계약/결정 | [`source-contracts/01-manufacturing-csv.md`](source-contracts/01-manufacturing-csv.md), [`../reference-decisions/gold-grain.md`](../reference-decisions/gold-grain.md), [`../reference-decisions/schema-drift.md`](../reference-decisions/schema-drift.md) |
| 구현 기능 | `source_hash`, `schema_hash`, bronze/silver/gold, quality reconciliation, catalog/lineage, same-input skip |
| Evidence | [`../../src/manufacturing_data_platform/pipeline/lakehouse.py`](../../src/manufacturing_data_platform/pipeline/lakehouse.py), [`../../tests/test_lakehouse_pipeline.py`](../../tests/test_lakehouse_pipeline.py) |
| 경계 | synthetic/local batch이며 production scale, real Mongo runtime, distributed transaction을 주장하지 않음 |

현재 빠진 설계 기록:

- source-hash idempotency를 독립적으로 설명하는 decision note
- quality reconciliation과 `latest_successful` 전진 조건을 묶는 publish/trust decision note
- catalog/run/lineage의 최소 계약을 한곳에 모은 decision note

코드와 테스트 계약은 있지만, 질문에서 결정으로 내려간 설명이 다른 기능보다 덜 보이는 부분이다.

### S1. 서로 다른 wide CSV 형식을 같은 분석 모델로 온보딩한다

| 단계 | 연결 |
|---|---|
| 문제 상황 | source마다 column 이름과 단위가 다르면 format 추가 때마다 transform code가 늘어남 |
| 가져온 질문 | mapping은 code인가 config인가? 변환 단위와 type 오류는 어디서 검증하는가? EAV와 gold의 grain은 무엇인가? mapping coverage가 부족하면 fail인가 warn인가? |
| Question Bank | [`question-bank/01-service-identity-contract.ko.md`](question-bank/01-service-identity-contract.ko.md), [`question-bank/07-external-benchmark-backlog-areas.ko.md`](question-bank/07-external-benchmark-backlog-areas.ko.md) |
| 계약/결정 | JSON mapping config, EAV row contract, [`../reference-decisions/gold-grain.md`](../reference-decisions/gold-grain.md)의 EAV grain |
| 구현 기능 | wide CSV -> normalized EAV -> entity daily gold, mapping coverage/type quality |
| Evidence | [`../../src/manufacturing_data_platform/pipeline/eav.py`](../../src/manufacturing_data_platform/pipeline/eav.py), [`../../tests/test_eav_pipeline.py`](../../tests/test_eav_pipeline.py) |
| 경계 | company schema 복제가 아닌 clean-room synthetic mapping이며 dimensional/SCD 모델은 Backlog |

현재 빠진 설계 기록:

- EAV용 독립 scenario walkthrough
- mapping config가 보장해야 하는 필드·단위·coverage를 고정한 source-integration contract

### S2. gold 숫자가 이상할 때 출처와 처리 상태를 조사한다

| 단계 | 연결 |
|---|---|
| 문제 상황 | output CSV만으로는 숫자가 어느 입력과 run에서 왔는지 설명할 수 없음 |
| 가져온 질문 | 운영자는 어떤 순서로 evidence를 읽는가? scheduler 상태와 data correctness는 같은가? lineage를 어디까지 주장할 수 있는가? |
| Question Bank | [`question-bank/04-orchestration-observability.ko.md`](question-bank/04-orchestration-observability.ko.md), [`question-bank/06-cross-area-connection-questions.ko.md`](question-bank/06-cross-area-connection-questions.ko.md) |
| 계약/결정 | successful run의 read-only evidence만 읽고 anomaly detection이나 자동 RCA를 주장하지 않음 |
| 구현 기능 | gold grain, run/source/schema identity, quality summary, path-level lineage trace를 한 report로 조회 |
| Evidence | [`scenarios/02-operator-debugging-wrong-gold.md`](scenarios/02-operator-debugging-wrong-gold.md), [`../../src/manufacturing_data_platform/pipeline/operator_report.py`](../../src/manufacturing_data_platform/pipeline/operator_report.py), [`../../tests/test_operator_report.py`](../../tests/test_operator_report.py) |
| 경계 | successful-run evidence 조회이며 failure-state forensics, column lineage, OpenLineage backend는 아님 |

### S3. 같은 business_date 정정 결과를 중복 없이 교체한다

| 단계 | 연결 |
|---|---|
| 문제 상황 | 같은 날짜 정정 입력을 append하면 gold가 중복되고, skip하면 정정을 반영하지 못함 |
| 가져온 질문 | append/skip/overwrite/MERGE 중 무엇인가? 다른 partition은 보존되는가? run_id와 snapshot_id는 같은가? retry가 새 snapshot을 만드는가? |
| Question Bank | [`question-bank/02-quality-rerun-failure.ko.md`](question-bank/02-quality-rerun-failure.ko.md), [`question-bank/03-storage-spark-consistency.ko.md`](question-bank/03-storage-spark-consistency.ko.md), [`question-bank/06-cross-area-connection-questions.ko.md`](question-bank/06-cross-area-connection-questions.ko.md) |
| 계약/결정 | [`../reference-decisions/iceberg-write-semantics.md`](../reference-decisions/iceberg-write-semantics.md): single gold table, COW-style `business_date` overwrite, other partition preserved, same publish retry skipped |
| 구현 기능 | local SparkSession, Iceberg table, partition overwrite, snapshot evidence, JSON run-to-snapshot evidence |
| Evidence | [`scenarios/01-rerun-same-business-date.md`](scenarios/01-rerun-same-business-date.md), [`slices/spark-iceberg-partition-overwrite/00-slice-map.ko.md`](slices/spark-iceberg-partition-overwrite/00-slice-map.ko.md), [`../../tests/test_spark_iceberg_skeleton.py`](../../tests/test_spark_iceberg_skeleton.py), [`../../tests/test_publish_gold_to_iceberg.py`](../../tests/test_publish_gold_to_iceberg.py) |
| 경계 | full medallion Iceberg, MERGE, concurrent writers, branch WAP, cluster Spark는 아님 |

### S4. 같은 business logic을 Airflow에서 실행한다

| 단계 | 연결 |
|---|---|
| 문제 상황 | 스케줄러마다 transform을 다시 작성하면 CLI와 DAG의 결과 계약이 갈라짐 |
| 가져온 질문 | DAG가 business logic을 가져야 하는가? `business_date`는 어디서 받는가? Airflow retry와 pipeline idempotency의 책임은 어떻게 나누는가? worker dependency는 어디에 설치되는가? |
| Question Bank | [`question-bank/04-orchestration-observability.ko.md`](question-bank/04-orchestration-observability.ko.md), [`question-bank/05-security-performance-testing-claim.ko.md`](question-bank/05-security-performance-testing-claim.ko.md), [`question-bank/06-cross-area-connection-questions.ko.md`](question-bank/06-cross-area-connection-questions.ko.md) |
| 계약/결정 | DAG는 검증된 CLI command만 조립하고, idempotency와 data correctness는 pipeline이 소유 |
| 구현 기능 | lakehouse, Spark/Iceberg skeleton, lakehouse-to-Iceberg publish DAG wrappers |
| Evidence | [`slices/02-airflow-wrapper-command-contract.ko.md`](slices/02-airflow-wrapper-command-contract.ko.md), [`slices/03-airflow-spark-iceberg-runtime.ko.md`](slices/03-airflow-spark-iceberg-runtime.ko.md), [`slices/04-lakehouse-to-iceberg-publish.ko.md`](slices/04-lakehouse-to-iceberg-publish.ko.md), [`../../tests/test_orchestration.py`](../../tests/test_orchestration.py), [`../../tests/test_airflow_dags.py`](../../tests/test_airflow_dags.py) |
| 경계 | local `dags test`와 standalone/LocalExecutor evidence이며 production scheduler, HA, distributed executor는 아님 |

Airflow는 별도 데이터 처리 엔진이 아니다. 위에서 이미 결정한 pipeline contract를 언제 어떤
parameter로 실행할지 조정한다.

### S5. Kafka event를 offset/replay 가능한 raw landing으로 보존한다

| 단계 | 연결 |
|---|---|
| 문제 상황 | file-drop만으로는 consumer offset, replay, redelivery, partition order를 직접 검증할 수 없음 |
| 가져온 질문 | event identity와 Kafka coordinate는 같은가? durable write와 offset commit 중 무엇이 먼저인가? invalid event는 partition을 막는가? replay가 normal group offset을 바꾸는가? |
| Question Bank | [`question-bank/08-kafka-streaming-ingestion.ko.md`](question-bank/08-kafka-streaming-ingestion.ko.md), [`question-bank/02-quality-rerun-failure.ko.md`](question-bank/02-quality-rerun-failure.ko.md), [`question-bank/05-security-performance-testing-claim.ko.md`](question-bank/05-security-performance-testing-claim.ko.md) |
| 계약/결정 | [`source-contracts/02-kafka-machine-event-v1.md`](source-contracts/02-kafka-machine-event-v1.md), [`../reference-decisions/kafka-event-identity-and-key.md`](../reference-decisions/kafka-event-identity-and-key.md), [`../reference-decisions/kafka-offset-and-landing-commit.md`](../reference-decisions/kafka-offset-and-landing-commit.md) |
| 구현 기능 | one-topic/one-partition bounded producer-consumer, accepted/quarantine immutable landing, landing-before-commit recovery, bounded no-commit replay |
| Evidence | [`scenarios/03-kafka-machine-event-ingestion.md`](scenarios/03-kafka-machine-event-ingestion.md), [`slices/05-kafka-raw-ingestion.ko.md`](slices/05-kafka-raw-ingestion.ko.md), [`../../tests/test_kafka_ingestion.py`](../../tests/test_kafka_ingestion.py) |
| 경계 | continuous streaming, exactly-once, multi-partition/rebalance, HA Kafka, TLS/SASL은 아님 |

### S6. Kafka landing을 기존 batch/quality/Iceberg 경로에 연결한다

| 단계 | 연결 |
|---|---|
| 문제 상황 | durable landing만 만들고 trusted dataset 경로와 연결하지 않으면 두 개의 고립된 demo가 됨 |
| 가져온 질문 | accepted set의 identity는 무엇인가? 같은 landing 재실행이 같은 source_hash를 만드는가? Kafka provenance를 잃지 않는가? invalid/tampered landing이 trusted state를 전진시키는가? |
| Question Bank | identity, quality/current-state, rerun, source-integration, Kafka-to-batch 경계 질문 |
| 계약/결정 | [`../reference-decisions/kafka-landing-to-batch-adapter.md`](../reference-decisions/kafka-landing-to-batch-adapter.md): 한 날짜·한 partition, canonical CSV SHA-256, provenance cross-check, pipeline 호출 전 fail |
| 구현 기능 | accepted landing -> deterministic CSV/provenance -> existing lakehouse -> local Iceberg publish |
| Evidence | [`slices/06-kafka-landing-to-batch.ko.md`](slices/06-kafka-landing-to-batch.ko.md), [`../../src/manufacturing_data_platform/kafka_ingestion/batch_adapter.py`](../../src/manufacturing_data_platform/kafka_ingestion/batch_adapter.py), [`../../tests/test_kafka_batch_adapter.py`](../../tests/test_kafka_batch_adapter.py) |
| 경계 | Spark Structured Streaming이나 direct Kafka-to-Iceberg sink가 아니라 bounded batch bridge |

### S7. landing된 한 날짜를 Spark batch로 다시 표현한다

| 단계 | 연결 |
|---|---|
| 문제 상황 | 연산 표현을 Spark로 옮기더라도 기존 gold grain·합계·재실행 계약은 바뀌면 안 됨 |
| 가져온 질문 | 같은 grain을 Spark로 어떻게 동일 표현하는가? dedup "first"와 round를 Python과 어떻게 맞추는가? quality fail이 trusted current를 막는가? 같은 source와 정정 source를 snapshot으로 어떻게 구분하는가? |
| Question Bank | storage/spark consistency, quality/current-state, rerun, orchestration 경계 질문 |
| 계약/결정 | [`../reference-decisions/spark-engine-swap-contract.md`](../reference-decisions/spark-engine-swap-contract.md): adapter input 재사용, `format_number` Python-round parity, coordinate-order dedup, 기존 quality suite 적용, `overwritePartitions()` |
| 구현 기능 | canonical CSV -> Spark silver/gold(parity) -> Spark quality gate -> Iceberg partition overwrite, same-source skip / correction snapshot, shuffle-plan evidence, 얇은 Airflow wrapper |
| Evidence | [`scenarios/04-spark-machine-event-batch.md`](scenarios/04-spark-machine-event-batch.md), [`slices/07-spark-machine-event-batch.ko.md`](slices/07-spark-machine-event-batch.ko.md), [`../../src/manufacturing_data_platform/pipeline/spark_machine_event_batch.py`](../../src/manufacturing_data_platform/pipeline/spark_machine_event_batch.py), [`../../tests/test_spark_machine_event_batch.py`](../../tests/test_spark_machine_event_batch.py) |
| 경계 | cluster/분산 Spark, 성능·throughput, Structured Streaming, exactly-once, concurrent writer는 아님 |

### S8. 단절 구간을 봉인해 모으고 완결된 뒤에만 downstream을 허용한다

| 단계 | 연결 |
|---|---|
| 문제 상황 | 현장과 중앙 사이 링크가 끊기면 수집은 멈출 수 없고, 복구 후에는 "무엇이 빠졌는지" 말할 수 있어야 함 |
| 가져온 질문 | edge 순서는 무엇으로 식별하나? durable progress는 무엇인가? "아직 안 옴"과 "유실"을 어떻게 구분하나? 완결성을 offset 연속성으로 판정해도 되나? |
| Question Bank | [`question-bank/02-quality-rerun-failure.ko.md`](question-bank/02-quality-rerun-failure.ko.md), [`question-bank/08-kafka-streaming-ingestion.ko.md`](question-bank/08-kafka-streaming-ingestion.ko.md), [`question-bank/06-cross-area-connection-questions.ko.md`](question-bank/06-cross-area-connection-questions.ko.md) |
| 계약/결정 | [`../reference-decisions/edge-buffer-and-recovery-progress.md`](../reference-decisions/edge-buffer-and-recovery-progress.md), [`source-contracts/03-edge-recovery-envelope.md`](source-contracts/03-edge-recovery-envelope.md): `(edge_source_id, boot_session_id, sequence_no)` edge 순서, immutable 파일 자체가 progress, `expected_last_sequence` 봉인, `event_id` 집합으로 완결성 판정 |
| 구현 기능 | fsync + atomic rename durable append, seal, 기존 K1 landing으로 replay, 미완결 시 `run_bridge` 호출 전 차단, 반복 replay에도 accepted/`source_hash`/gold 불변 |
| Evidence | [`scenarios/05-industrial-telemetry-recovery.md`](scenarios/05-industrial-telemetry-recovery.md), [`slices/08-edge-cloud-recovery.ko.md`](slices/08-edge-cloud-recovery.ko.md), [`../../src/manufacturing_data_platform/edge_recovery.py`](../../src/manufacturing_data_platform/edge_recovery.py), [`../../tests/test_edge_recovery.py`](../../tests/test_edge_recovery.py), [`../../scripts/verify_edge_recovery.sh`](../../scripts/verify_edge_recovery.sh) |
| 경계 | 실제 edge gateway/OT 프로토콜, power-loss durability, concurrent writer, multi machine/session/partition, continuous service는 아님 |

### S9. 복구가 완결된 세션만 Spark/Iceberg로 발행한다

S8(단절 세션 봉인·복구 판정)과 S7(Spark 발행)은 각각 검증됐지만 한 실행 경로로 이어진 적이 없었다.
S9는 둘을 **재구현하지 않고 조합**한다.

| 단계 | 연결 |
|---|---|
| 문제 상황 | 복구가 덜 된 세션으로 batch를 돌리면 trusted snapshot이 먼저 전진해 무음 손실이 됨. 반대로 완결 판정만 믿으면 같은 날짜의 세션 밖 event가 섞인 batch도 "완결"로 발행됨 |
| 가져온 질문 | 완결 gate를 누가 소유하는가? gate는 Spark 시작 전인가 후인가? membership(봉인 ⊆ accepted)으로 충분한가? 재실행에서 무엇이 불변이고 무엇이 새로 발급되는가? |
| Question Bank | quality/current-state, rerun, cross-area connection, orchestration 경계 질문 |
| 계약/결정 | [`../reference-decisions/recovery-gated-publish-boundary.md`](../reference-decisions/recovery-gated-publish-boundary.md): S8에서 추출한 공유 `require_recovery_ready`, Spark import 이전 gate, 봉인 event 집합 == adapter 선택 집합, S7 callable 호출만 |
| 구현 기능 | sealed session -> 공유 readiness gate -> 기존 결정적 adapter -> 집합 동등성 검사 -> 기존 Spark quality gate/Iceberg overwrite -> 각 identity space를 자기 field에 담고 attempt와 snapshot 생성 주체를 구분한 evidence, 얇은 Airflow wrapper |
| Evidence | [`slices/09-recovery-gated-spark-iceberg.ko.md`](slices/09-recovery-gated-spark-iceberg.ko.md), [`slices/08-edge-cloud-recovery.ko.md`](slices/08-edge-cloud-recovery.ko.md), [`../../src/manufacturing_data_platform/pipeline/recovered_telemetry_publish.py`](../../src/manufacturing_data_platform/pipeline/recovered_telemetry_publish.py), [`../../tests/test_recovered_telemetry_publish.py`](../../tests/test_recovered_telemetry_publish.py), [`../../scripts/verify_recovered_telemetry_publish.sh`](../../scripts/verify_recovered_telemetry_publish.sh) |
| 경계 | streaming sink, cluster Spark, multi session/partition, concurrent Iceberg writer, gate 통과 후 분산 원자성, production Airflow 운영은 아님 |

## 4. 시나리오가 Question Bank를 가져오는 방식

| 시나리오 | 주로 가져오는 영역 | 이번에 일부러 가져오지 않는 영역 |
|---|---|---|
| S0 trusted batch | service/identity, grain, quality, rerun, lineage | distributed compute, streaming, production RBAC |
| S1 EAV onboarding | source integration, mapping, grain, quality | SCD, semantic layer, schema registry |
| S2 operator debugging | observability, lineage, current state, claim boundary | anomaly detection platform, column lineage backend |
| S3 correction publish | rerun, storage, consistency, snapshot identity | MERGE, concurrent writer, branch WAP |
| S4 Airflow | orchestration, retry ownership, dependency packaging | Celery/Kubernetes executor, HA scheduler |
| S5 Kafka landing | event identity, offset/replay, durability, quarantine | multi-partition rebalance, Schema Registry, continuous streaming |
| S6 batch bridge | cross-system identity, current-state guard, deterministic rerun | direct streaming sink, window/watermark |
| S7 Spark batch | engine parity, storage/spark consistency, quality gate, snapshot identity | cluster/분산 Spark, 성능/throughput, streaming |
| S8 edge 복구 | durable progress, 완결성 판정, identity 분리, 장애/재전송 | 실제 edge 하드웨어, power-loss durability, continuous service |
| S9 recovery-gated publish | 복구 완결 gate 소유권, 입력 집합 동등성, current-state guard, rerun 불변식 | streaming sink, multi session/partition, concurrent writer, 분산 원자성 |

이 표의 목적은 질문을 줄이는 것이 아니라, 질문을 넓게 본 뒤 **현재 contract를 바꾸는 질문만
Core로 선택했는지** 확인하는 것이다.

## 5. 계약은 무엇을 고정하는가

| 계약 종류 | 이 프로젝트의 예 | 깨지면 생기는 문제 |
|---|---|---|
| source contract | CSV row, Kafka event envelope | 같은 입력의 의미가 달라짐 |
| identity contract | source_hash, event_id, Kafka coordinate, run_id, snapshot_id | dedup/replay/version 추적이 섞임 |
| grain contract | manufacturing gold, EAV gold | 집계 중복과 metric 오해 |
| quality contract | reconciliation, conservation, not-null/range, mapping coverage | trusted state가 잘못 전진할 수 있음 |
| rerun/publish contract | same input skip, corrected partition overwrite | 중복 또는 정정 미반영 |
| orchestration contract | DAG는 CLI wrapper, pipeline이 idempotency 소유 | scheduler retry와 data correctness가 섞임 |
| evidence contract | run/source/schema/quality/lineage/snapshot 기록 | 성공을 재현하거나 실패를 설명할 수 없음 |
| claim contract | local/synthetic/one-partition 등 한계 명시 | portfolio 문장이 evidence를 넘어감 |

## 6. 아직 설계가 덜 연결된 곳

아래는 기능 추가 목록이 아니라 문서/계약 gap이다.

| 우선순위 | Gap | 닫는 방법 |
|---|---|---|
| P1 | batch idempotency 결정이 code/test에만 강하게 보임 | source-hash idempotency decision note 1장 |
| P1 | quality pass와 `latest_successful` 전진 조건이 분산됨 | quality/current-state decision note 1장 |
| P1 | EAV는 구현됐지만 독립 scenario와 mapping contract가 약함 | EAV onboarding scenario + source-integration contract |
| P2 | failure-state model은 Proposed이고 successful-run evidence가 중심 | [`../reference-decisions/failure-state-model.md`](../reference-decisions/failure-state-model.md)를 별도 slice로 검증 |
| P2 | Kafka milestone 포트폴리오는 있으나 전체 플랫폼 promotion page는 없음 | 전체 architecture와 S0~S6를 압축한 project-level portfolio overview |

이 gap을 전부 한 번에 구현하지 않는다. 다음 대표 scenario가 실제로 요구할 때 하나씩 닫는다.

## 7. 처음부터 읽는 순서

```text
1. 00-service-purpose-charter.md
   왜 존재하고 누가 무엇을 판단하는가

2. 00a-plain-project-map.md
   데이터가 실제로 어떻게 흐르는가

3. 이 문서
   시나리오, 질문, 계약, 기능, evidence가 어떻게 연결되는가

4. 관심 있는 scenarios/*.md 하나
   구체적으로 어떤 문제가 생기는가

5. 그 시나리오가 pull한 question-bank 문서만
   어떤 선택지와 실패 경계가 있는가

6. reference-decisions + slices
   무엇을 골랐고 이번 구현 범위를 어디서 잘랐는가

7. code/tests + VERIFICATION_LOG.md
   말이 아니라 무엇으로 증명했는가
```

프로젝트 전체를 이해하려고 Kafka 문서부터 읽을 필요는 없다. Kafka는 S5/S6에서 입력 경로를
확장한 것이고, S0의 batch/quality/catalog spine이 여전히 전체 시스템의 중심이다.
