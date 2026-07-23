# 09. Recovery-Gated Spark/Iceberg Publish Slice (S9)

상태: Implemented / accepted-closed — Codex 독립 검토 완료 (2026-07-23)

> 이미 검증된 S8 복구 계약과 S7 발행 계약을 **어느 쪽도 재구현하지 않고** 하나의 실행 경로로
> 잇는 얇은 slice index다. 상세 결정은
> [`../../reference-decisions/recovery-gated-publish-boundary.md`](../../reference-decisions/recovery-gated-publish-boundary.md),
> 최신 실행 결과는 [`../../../VERIFICATION_LOG.md`](../../../VERIFICATION_LOG.md)가 source of truth다.

## 1. Slice Thesis

```text
봉인된 edge 세션이 실제 Kafka/K1 landing에 전부 반영됐고,
그 세션의 event 집합이 batch 입력과 정확히 일치할 때에만
기존 Spark silver/gold + quality gate + Iceberg business_date overwrite를 실행한다.
```

새 엔진도 새 파이프라인도 아니라 **composition slice**다. 새로 쓴 것은 gate와 동등성 검사뿐이다.

## 2. Primary Scenario

[`../scenarios/05-industrial-telemetry-recovery.md`](../scenarios/05-industrial-telemetry-recovery.md)
— S8이 만든 복구 판정을 실제 발행까지 잇는 구간.

## 3. Core Questions

| Core question | 채택한 계약 |
|---|---|
| 복구 완결 판정을 누가 하나? | S8에서 추출한 공유 `require_recovery_ready(...)`. S9는 재구현하지 않는다 |
| gate는 어디에 있나? | Spark import·세션 생성 **이전**. 미완결이면 warehouse/adapter가 생기지 않는다 |
| membership만으로 충분한가? | **아니다.** 같은 날짜의 세션 밖 event가 batch에 섞일 수 있다 |
| 그래서 무엇을 더 보나? | 봉인 event_id 집합 == adapter 선택 집합 (개수 + 집합) |
| 불일치는 어떻게 말하나? | `extra_event_ids` / `missing_event_ids`로 방향을 보고 |
| Spark/Iceberg 로직은? | S7 callable 호출만. transform/quality/overwrite 코드 없음 |
| 재실행은? | 같은 `source_hash` → `skipped`. **새 snapshot 없음·partition overwrite 없음**(전체 no-op은 아님 — S7은 판정 전에 Spark/quality를 돌린다) |
| 그럼 attempt는 어떻게 기록하나? | `spark_attempt_run_id` + `snapshot_relation`. skip이면 producer run_id는 `null`(S7이 노출 안 함) |
| 품질 실패는? | `no_snapshot`. 만든 것도 재사용한 것도 없으므로 skip과 같은 relation을 쓰면 거짓말이 된다 |
| Airflow는 무엇을 증명하나? | import/wiring/command 실행까지. scheduler/executor 운영은 아니다 |

## 4. 검증한 상태 전이

```text
broker 없음 상태에서 spool 1..3 append -> seal(expected_last_sequence=3)
-> 재연결 후 1..2만 replay : accepted 2, missing [3]
   -> S9 발행 시도 : RecoveryIncompleteError로 차단
      warehouse 없음 · adapter 없음 (Spark/Iceberg state 0)
-> 1..3 replay (새 offset) : accepted 3, missing [], recovery_complete=true
-> S9 발행 : status=published, quality 7/7 통과, snapshot_id 발급
             snapshot_relation=created_by_current_attempt
-> 같은 세션·landing 재실행 : status=skipped, 같은 source_hash, 같은 snapshot_id
             새 snapshot 없음 · partition overwrite 없음
             attempt run_id는 새 값, snapshot_relation=reused_from_prior_attempt
             producer_attempt_run_id=null (S7이 노출하지 않으므로 추측하지 않는다)
-> 봉인 event_id 집합 (3개) == adapter 선택 집합 (3개)
```

status별 evidence 분기 (published / skipped는 runtime, quality_failed는 직접 evidence test):

```text
published      -> created_by_current_attempt  · producer=이번 attempt · snapshot_id 있음
skipped        -> reused_from_prior_attempt   · producer=null        · snapshot_id 그대로
quality_failed -> no_snapshot                 · producer=null        · snapshot_id=null
그 외 status   -> UnexpectedSparkStatusError로 거부 (조용한 default 분기 없음)
```

runtime에서 edge sequence `[1,2,3]`이 Kafka offset `[0,1,4]`에 대응했다 — 완결성을 offset
연속성으로 판정하지 않는 이유의 직접 증거이고, S8 slice와 같은 결과다. 검증 스크립트는 이 관측
하나만 확인한다(`edge_sequence_not_kafka_offsets`). identity space 분리는 schema/semantics 계약이지
값 부등식으로 증명되는 것이 아니다.

Airflow `dags test`도 같은 봉인 세션과 별도 clean warehouse로 실행해 DagRun success,
task exit 0, `status=published`, `snapshot_count=1`을 확인했다.

## 5. Backlog / Not Claimed

```text
continuous streaming, Structured Streaming, Kafka→Iceberg streaming sink
full Spark/Iceberg medallion 플랫폼, cluster Spark, 성능/처리량 개선
multi machine/session/partition, rebalance, concurrent Iceberg writer
end-to-end exactly-once, gate 통과 후 Spark↔Iceberg 분산 원자성
production/HA/분산 Airflow 운영
실제 edge 하드웨어, OPC UA / MQTT / ROS 2 / DDS
digital twin · 이상탐지 · 예지보전 · 기계 제어
```

## 6. Evidence

- [`../../../src/manufacturing_data_platform/pipeline/recovered_telemetry_publish.py`](../../../src/manufacturing_data_platform/pipeline/recovered_telemetry_publish.py)
- [`../../../tests/test_recovered_telemetry_publish.py`](../../../tests/test_recovered_telemetry_publish.py)
- [`../../../scripts/verify_recovered_telemetry_publish.sh`](../../../scripts/verify_recovered_telemetry_publish.sh)
- [`../../../dags/manufacturing_recovered_telemetry_publish.py`](../../../dags/manufacturing_recovered_telemetry_publish.py)
- [`08-edge-cloud-recovery.ko.md`](08-edge-cloud-recovery.ko.md), [`07-spark-machine-event-batch.ko.md`](07-spark-machine-event-batch.ko.md)
- [`../../../VERIFICATION_LOG.md`](../../../VERIFICATION_LOG.md)

## 7. Next Questions

```text
gate 통과 후 Spark 실패로 남는 adapter 산출물을 failure-state slice에서 다룰 것인가?
여러 세션을 한 business_date에 발행해야 하는 실제 압력이 있는가?
S7의 skipped 재실행 run_id 재발급을 계약으로 고정할 것인가, 바꿀 것인가?
```
