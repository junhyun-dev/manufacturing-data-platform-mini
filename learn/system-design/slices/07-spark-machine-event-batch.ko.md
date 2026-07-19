# 07. Spark Machine-Event Batch Slice (S7)

상태: implemented / local Spark engine-parity + quality-gated Iceberg publish verified

> K1.5 canonical landing을 기존 batch 계약 그대로 Spark로 다시 표현하는 얇은 slice index다.
> 상세 결정은 [`../../reference-decisions/spark-engine-swap-contract.md`](../../reference-decisions/spark-engine-swap-contract.md),
> 최신 테스트 수와 실행 결과는 [`../../../VERIFICATION_LOG.md`](../../../VERIFICATION_LOG.md)가 source of truth다.

## 1. Slice Thesis

```text
K1.5가 만든 한 날짜 canonical CSV/source_hash를 입력 계약으로 재사용하고,
기존 silver/gold 의미를 Spark DataFrame built-in으로 동일하게 표현한 뒤,
quality를 통과한 결과만 한 Iceberg gold 테이블 partition에 publish한다.
```

이 slice는 새 처리 플랫폼이 아니라 batch spine의 실행 엔진을 한 slice에서 Spark로 바꾼 것이다.

## 2. Primary Scenario

[`../scenarios/04-spark-machine-event-batch.md`](../scenarios/04-spark-machine-event-batch.md) 참조.

```text
운영자가 durable하게 landing된 한 business_date를 Spark batch로 backfill한다.
같은 source 재실행은 새 snapshot을 만들지 않고, 정정 source는 대상 partition만 교체한다.
quality 실패 결과는 Iceberg current가 되지 않는다.
```

## 3. Core Questions

| Core question | 채택한 계약 |
|---|---|
| 입력은 무엇인가? | adapter canonical CSV + `source_hash` (Spark가 raw JSONL 재해석 안 함). |
| grain은? | gold `(business_date, plant_id, line_id, product_code)`, 기존과 동일. |
| dedup은? | silver natural key, Kafka coordinate 순서로 first 유지 → Python parity. |
| round는? | `format_number`+strip+cast가 40,400개 정수비 표본과 경계 test에서 Python `round`와 일치 (`bround`는 boundary 불일치). |
| quality gate는? | 기존 `build_quality_checks`를 Spark 결과에 driver-collect 적용, fail이면 write 금지 + CLI non-zero exit. distributed Spark-native quality는 아님. |
| write는? | `overwritePartitions()`; same-source skip / correction 새 snapshot. |
| identity는? | `source_hash` / `run_id` / `snapshot_id` 별도 기록. |
| Airflow는? | 검증된 CLI 하나를 호출하는 single-task wrapper, `max_active_runs=1`. |

## 4. Backlog / Not Claimed

```text
full bronze/silver/gold Iceberg 테이블
Spark Structured Streaming / watermark / window
direct Kafka -> Iceberg sink
multi-partition Kafka / rebalance
cluster Spark / Kubernetes / distributed executor
concurrent Iceberg writer / branch WAP / MERGE / compaction
production 성능·throughput·HA claim
```

## 5. Evidence

- [`../../../src/manufacturing_data_platform/pipeline/spark_machine_event_batch.py`](../../../src/manufacturing_data_platform/pipeline/spark_machine_event_batch.py)
- [`../../../tests/test_spark_machine_event_batch.py`](../../../tests/test_spark_machine_event_batch.py)
- [`../../../dags/manufacturing_spark_machine_event_batch.py`](../../../dags/manufacturing_spark_machine_event_batch.py)
- [`../../../scripts/verify_spark_machine_event_batch.sh`](../../../scripts/verify_spark_machine_event_batch.sh)
- [`../../../VERIFICATION_LOG.md`](../../../VERIFICATION_LOG.md)

## 6. Next Questions

```text
window/latency 압력이 실제로 명명되면 Spark Structured Streaming(K2)이 필요한가?
two-system(commit↔evidence) atomicity를 failure-state slice로 닫을 것인가?
성능/throughput은 별도 벤치 slice가 생길 때만 측정한다.
```
