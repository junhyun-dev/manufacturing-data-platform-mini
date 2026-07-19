# 04. Scenario — 설비 event 한 날짜를 Spark batch로 backfill한다

상태: implementation-backed walkthrough / local Spark+Iceberg scope

목적: S7이 "Kafka를 Spark로 스트리밍한다"가 아니라 "이미 durable한 한 날짜를 기존 batch 계약
그대로 Spark로 다시 표현한다"임을 하나의 운영 시나리오로 고정한다.

관련 문서:

- 계약: [`../../reference-decisions/spark-engine-swap-contract.md`](../../reference-decisions/spark-engine-swap-contract.md)
- slice map: [`../slices/07-spark-machine-event-batch.ko.md`](../slices/07-spark-machine-event-batch.ko.md)
- 앞선 입력 경로: [`03-kafka-machine-event-ingestion.md`](03-kafka-machine-event-ingestion.md) (S5) → K1.5 adapter (S6)

## 1. Actor / Trigger

```text
Actor: 데이터 엔지니어(운영자)
Trigger: 이미 landing된 설비 event 한 business_date를 backfill해 gold를 다시 만든다.
연산량/표현 이유로 Spark batch를 쓰지만, gold grain과 합계 계약은 그대로여야 한다.
```

## 2. Desired outcome

```text
같은 accepted set을 다시 backfill해도 trusted gold가 두 배가 되면 안 된다.
정정 source가 오면 그 business_date partition만 교체되고 다른 날짜는 보존돼야 한다.
quality 실패 결과는 Iceberg current가 되면 안 된다.
Spark로 만든 silver/gold가 기존 Python 결과와 같은 grain·합계여야 한다.
```

## 3. Failure / correction variant

```text
- 같은 source 재실행: 새 snapshot을 만들지 않는다 (idempotency).
- 다른 source 같은 날짜(정정): 새 snapshot 1개 + 대상 partition 교체.
- quality-violating 입력: publish 전에 막고 success state를 전진시키지 않는다.
- table commit 성공 후 evidence write 실패: two-system 불일치로 남기고 이 slice에서 풀지 않는다.
```

## 4. 가져온 질문 (Question Pull)

```text
같은 gold grain을 Spark로 어떻게 동일하게 표현하는가? (parity)
natural-key dedup의 "first"를 Spark에서 어떻게 결정적으로 맞추는가?
round-half 규칙이 Python과 Spark에서 같은가?
quality를 Spark 결과에 어떻게 적용하고, 실패 시 무엇을 막는가?
같은 source 재실행과 정정 source를 snapshot 관점에서 어떻게 구분하는가?
run_id / source_hash / snapshot_id는 어디에 따로 기록되는가?
```

가져오되 이번에 Core로 내리지 않은 것: window/watermark, multi-partition, cluster/분산 실행,
concurrent writer, 성능/throughput.

## 5. 계약으로 수렴 (Decision)

핵심 계약은 [`spark-engine-swap-contract.md`](../../reference-decisions/spark-engine-swap-contract.md)에
정리했다. 요약:

```text
input  = adapter canonical CSV + source_hash
parity = transform_silver/transform_gold 의미 유지, format_number로 Python round parity
quality= 기존 build_quality_checks를 Spark 결과에 적용, fail이면 write 금지
publish= overwritePartitions(), same-source skip / correction new snapshot
```

## 6. Evidence

```text
src/manufacturing_data_platform/pipeline/spark_machine_event_batch.py
tests/test_spark_machine_event_batch.py (engine parity / dedup / quality gate / rerun+correction)
scripts/verify_spark_machine_event_batch.sh
VERIFICATION_LOG.md (2026-07-17 S7 entry)
```

## 7. Claim boundary

이 시나리오가 증명하는 것은 local bounded Spark batch의 engine parity와 quality-gated Iceberg
publish다. production/cluster Spark, 성능 우월성, continuous streaming, exactly-once는 증명하지
않는다.
