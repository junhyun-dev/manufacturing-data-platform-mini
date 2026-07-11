# Spark/Iceberg Design Notes

상태: Spark/Iceberg slice supporting docs

이 폴더는 Spark/Iceberg walking skeleton으로 가기까지의 질문, state 재표현, primer, 구현 gate를 모은다.

이 폴더의 문서는 source of truth가 아니다.

```text
최신 실행/테스트 결과:
  ../../../../VERIFICATION_LOG.md

한 build 단위의 얇은 지도:
  00-slice-map.ko.md

write semantics 결정:
  ../../../reference-decisions/iceberg-write-semantics.md
```

## Reading Order

1. [`01-question-map.md`](01-question-map.md)
   - Spark/Iceberg로 갈 때 어떤 질문들이 생기는지 넓게 펼친다.
2. [`02-state-shift.md`](02-state-shift.md)
   - Slice1의 state transition을 Spark DataFrame과 Iceberg snapshot으로 다시 표현한다.
3. [`03-mini-primer.md`](03-mini-primer.md)
   - Iceberg/Spark 개념을 `business_date` 재처리 시나리오에 필요한 만큼만 연결한다.
4. [`04-walking-skeleton-plan.md`](04-walking-skeleton-plan.md)
   - 구현 직전 Core/Demo/Backlog/Unknown과 test contract를 고정한다.
5. [`05-version-pin.md`](05-version-pin.md)
   - PySpark, Iceberg runtime jar, Scala suffix, Java, catalog 설정을 고정한 implementation gate다.

## Boundary

Implemented:

```text
local Spark/Iceberg single-gold-table walking skeleton
business_date partition overwrite
snapshot metadata evidence
same source_hash rerun -> no new snapshot
```

Backlog:

```text
full bronze/silver/gold Spark medallion rewrite
Spark-based quality suite
Airflow-triggered Spark runtime
concurrent writer handling
retention/rollback operation
```
