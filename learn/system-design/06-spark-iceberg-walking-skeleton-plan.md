# 06. Spark/Iceberg Walking Skeleton Plan

상태: implementation pre-plan / Claude audit target
프로젝트: `manufacturing-data-platform-mini`

> **STATUS: design-only.** 이 repo에는 아직 Spark/Iceberg 구현 코드가 없고, `pyspark`도 설치되어 있지 않다. 이 문서는 구현 직전의 작은 question map + test contract다.

감사 반영 상태:

```text
Claude external benchmark audit 반영 완료:
  - D2 partition 보존을 Core로 승격
  - write API를 overwritePartitions() 중심으로 명시
  - version/jar/scala/java/catalog gate를 별도 Unknown으로 분해
  - same-source rerun -> no new snapshot test 추가
  - "single gold table walking skeleton" claim boundary 강화
```

## 1. Build Thesis

```text
같은 business_date에 정정 source가 들어왔을 때,
gold 결과를 중복 없이 교체하고,
overwrite 전후 snapshot evidence를 남겨 비교할 수 있게 한다.
```

이 slice는 "Spark/Iceberg를 붙였다"가 목적이 아니다.

목적은 Slice1의 핵심 contract를 Spark/Iceberg의 table/snapshot 모델로 작게 재표현하는 것이다.

```text
Slice1:
  same source_hash rerun -> skip
  changed source same business_date -> new run, but CSV output is run-folder based

Walking skeleton:
  same source_hash rerun -> skip remains possible
  changed source same business_date -> business_date partition overwrite
  overwrite creates a new Iceberg snapshot
  run metadata records run_id -> snapshot_id
```

## 2. Primary Scenario

```text
business_date=2026-06-29 gold_daily_metrics가 이미 있다.
나중에 같은 business_date의 정정 데이터가 들어온다.
운영자는 같은 날짜 결과가 append 중복되지 않고 새 값으로 교체되길 원한다.
또한 재처리 전/후 결과를 snapshot으로 비교할 수 있어야 한다.
```

## 3. Wide Question Expansion

질문은 넓게 뽑고, 이번 walking skeleton에서 Core만 구현한다.

| Axis | Question | Initial classification |
|---|---|---:|
| Local feasibility | 이 WSL/local 환경에서 `pyspark` + Iceberg runtime jar가 실제로 뜨는가? | Unknown/Core |
| Catalog | local Iceberg catalog는 hadoop catalog로 충분한가? warehouse path는 어디인가? | Core |
| Table scope | bronze/silver/gold 전체가 아니라 `gold_daily_metrics` 하나만으로 충분한가? | Core |
| Partitioning | partition column은 `business_date` 하나로 고정해도 되는가? | Core |
| Write semantics | append / whole-table overwrite / partition overwrite 중 무엇인가? | Core |
| Write API | `df.writeTo(table).overwritePartitions()`를 쓸 것인가, SQL `INSERT OVERWRITE`를 쓸 것인가? | Core |
| Idempotency | 같은 source_hash rerun은 skip할 것인가, overwrite를 또 할 것인가? | Core |
| Correction | 다른 source_hash + 같은 business_date는 partition overwrite로 처리할 것인가? | Core |
| Snapshot | overwrite 후 current snapshot id를 어떻게 읽을 것인가? | Core |
| Run metadata | `run_id`와 `snapshot_id`를 어떻게 구분하고 기록할 것인가? | Core |
| Time travel | 이전 snapshot 읽기는 Core인가 Demo인가? | Demo |
| Other partitions | 다른 business_date partition이 보존되는지 확인할 것인가? | Core |
| Namespace / DDL | namespace 생성과 `PARTITIONED BY (business_date)` DDL을 어떻게 고정할 것인가? | Core-lite |
| Snapshot test design | snapshot id는 non-deterministic인데 무엇을 assert할 것인가? | Core |
| Schema evolution | added column을 Iceberg table schema에 반영할 것인가? | Backlog |
| Quality on Spark | 기존 quality suite를 Spark agg로 옮길 것인가? | Backlog |
| Full Spark port | bronze/silver/gold 전체를 DataFrame으로 이식할 것인가? | Backlog |
| MERGE/upsert | late row 단위 upsert를 할 것인가? | Backlog |
| Concurrency | concurrent writers를 검증할 것인가? | Backlog |
| Runtime Airflow | Airflow가 Spark/Iceberg run을 trigger할 것인가? | Backlog for this slice |
| Public claim | 구현 후 어디까지 말할 수 있는가? | Core |

## 4. Core Scope

이번 skeleton에서 구현할 최소 범위:

```text
1. local SparkSession 생성
2. local Iceberg catalog / warehouse 설정
3. `gold_daily_metrics` Iceberg table 생성
4. business_date=2026-06-29 initial rows write
5. snapshot S1 읽기
6. 같은 business_date corrected rows로 `DataFrameWriterV2.overwritePartitions()` partition overwrite
7. snapshot S2 읽기
8. current table에 corrected rows만 있고 중복이 없는지 확인
9. 다른 business_date partition이 유지되는지 확인
10. `.snapshots` metadata로 snapshot 증가와 current snapshot을 확인
11. 가능하면 S1/S2를 비교해서 time-travel demo 확인
12. run_id -> gold_snapshot_id mapping을 JSON evidence로 저장
```

write API 결정:

```text
Core decision:
  Use df.writeTo("local.db.gold_daily_metrics").overwritePartitions()

Avoid for this skeleton:
  SQL INSERT OVERWRITE without explicitly controlling dynamic partition overwrite mode

Why:
  The skeleton must prove "D partition changed, D2 partition survived".
  A static whole-table overwrite can accidentally pass a one-date test.
```

구현하지 않을 것:

```text
bronze/silver/gold full Spark rewrite
quality-on-Spark
schema evolution
MERGE/upsert
retention/expire
concurrent writers
cluster deployment
Airflow runtime integration
Kafka streaming
production rollback
```

## 5. Proposed File/Module Shape

초안:

```text
src/manufacturing_data_platform/pipeline/spark_iceberg_skeleton.py
tests/test_spark_iceberg_skeleton.py
```

CLI 후보:

```bash
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.spark_iceberg_skeleton \
  --warehouse /tmp/manufacturing-mini-iceberg-warehouse \
  --output-dir /tmp/manufacturing-mini-iceberg-evidence
```

Evidence output 후보:

```text
/tmp/manufacturing-mini-iceberg-evidence/
  run_snapshot_map.json
  current_gold.json
  snapshot_comparison.json
```

Local runtime compatibility gate:

```text
Before coding the skeleton, pin one compatible set:
  pyspark version
  Iceberg Spark runtime jar name
  Scala binary version in the jar suffix
  Java version

The fragile part is not the Python code.
It is Spark version + Scala binary version + Iceberg runtime jar + Java compatibility.
```

`run_snapshot_map.json` 예:

```json
{
  "dataset_id": "manufacturing_daily_metrics",
  "table": "local.db.gold_daily_metrics",
  "business_date": "2026-06-29",
  "runs": [
    {
      "run_id": "R1",
      "source_hash": "H1",
      "gold_snapshot_id": 111
    },
    {
      "run_id": "R2",
      "source_hash": "H2",
      "gold_snapshot_id": 222
    }
  ],
  "claim_boundary": {
    "supports": [
      "local Spark/Iceberg table creation",
      "business_date partition overwrite",
      "snapshot id evidence",
      "run_id to snapshot_id mapping"
    ],
    "does_not_support": [
      "production rollback",
      "concurrent writer handling",
      "full medallion Spark rewrite",
      "Airflow runtime orchestration"
    ]
  }
}
```

Invariant for this skeleton:

```text
one pipeline run -> one gold table commit -> one gold_snapshot_id
```

If a future implementation writes multiple Iceberg tables or multiple commits per run, the mapping becomes 1:N and this evidence shape must change.

## 6. Test Contract

### Test 0. Environment gate

```text
given local environment
when starting SparkSession with Iceberg extensions, hadoop catalog, and warehouse
then either:
  test creates and reads a trivial Iceberg table
or:
  test is explicitly skipped with a concrete reason:
    pyspark unavailable
    Java incompatible
    Iceberg runtime jar unavailable
    Spark/Scala/Iceberg jar mismatch
    catalog/warehouse config failed
```

이 test는 실패를 숨기기 위한 skip이 아니다. 환경 제약을 구체적으로 명시하기 위한 gate다.

### Test 1. Table creation

```text
given warehouse path
when skeleton runs initial write
then gold_daily_metrics table exists
and current rows include business_date=2026-06-29
and a snapshot id is recorded
```

### Test 2. Partition overwrite

```text
given initial rows for business_date D and another date D2
when corrected rows for D are written by partition overwrite
then D rows reflect corrected values
and D rows are not duplicated
and D2 rows remain unchanged
and new snapshot id != previous snapshot id
and snapshot count increased by exactly 1 for the correction write
```

핵심 assertion:

```text
D row count == corrected row count
D row values == corrected values
D2 row values == original D2 values
current_snapshot_id changed
```

이 test가 없으면 static whole-table overwrite를 해도 단일 날짜 테스트에서는 실수를 못 잡는다.

### Test 3. Run metadata mapping

```text
given initial run R1 and correction run R2
when evidence is written
then run_snapshot_map.json records:
  R1 -> snapshot S1
  R2 -> snapshot S2
and run_id is not treated as snapshot_id
and run_id is string-like while snapshot_id is numeric/long-like
```

### Test 4. Same source rerun does not create a new snapshot

```text
given business_date D, source_hash H1, and current snapshot S1
when the same source_hash is processed again
then status is skipped
and current snapshot remains S1
and no new gold table commit is recorded
```

이 test는 Slice1 idempotency contract가 engine/storage 변경 후에도 살아있다는 증거다.

### Test 5. Time travel demo

```text
given S1 and S2 exist
when reading VERSION AS OF S1 and S2
then old and corrected gold metrics can be compared
```

Classification: Demo.

Primary demo evidence:

```text
SELECT * FROM local.db.gold_daily_metrics.snapshots
```

If `VERSION AS OF` SQL has Spark/Iceberg version friction, keep snapshot metadata evidence and do not overclaim time-travel reads.

## 7. Claim Boundary

If skeleton passes, allowed wording:

```text
Built a local Spark/Iceberg single-gold-table walking skeleton that overwrites
a business_date partition and records snapshot metadata for correction rerun comparison.
```

Still forbidden:

```text
implemented full Spark/Iceberg medallion pipeline
implemented an Iceberg lakehouse
operated production Iceberg lakehouse
handled concurrent writers
implemented MERGE/upsert
implemented production rollback/retention
verified Airflow-triggered Spark runtime
```

Partition-overwrite wording is allowed only after the D2-preserved test passes.

Before skeleton passes, wording remains:

```text
Designed the Spark/Iceberg translation path for business_date partition overwrite
and run_id -> snapshot_id lineage, implementation pending.
```

## 8. Claude Audit Prompt

```text
너는 external benchmark auditor + supplementer다.

아래 문서를 Apache Iceberg/Spark 공식 문서와 2025-2026 lakehouse 관점에서 감사해줘.
목표는 구현 범위를 늘리는 게 아니라, walking skeleton의 질문 품질과 claim boundary를 강화하는 것이다.

감사 기준:
1. Core/Demo/Backlog/Unknown 분류가 맞는가?
2. business_date partition overwrite 구현 전에 빠진 Core 질문이 있는가?
3. run_id와 snapshot_id 구분이 정확한가?
4. Spark/Iceberg local skeleton에서 가장 깨지기 쉬운 버전/jar/catalog 설정 질문은 무엇인가?
5. time travel을 Demo로 둔 것이 정직한가?
6. public README/blog/resume에서 과장 위험이 있는 표현은 무엇인가?
7. 더 좋은 test contract가 있는가?

현재 repo 상태:
- Python/CSV Slice1, EAV, operator report는 구현+테스트됨.
- Airflow wrapper command contract는 test-covered지만 runtime Airflow는 미검증.
- Spark/Iceberg code는 0줄이고 pyspark도 미설치.
- 목표는 full Spark rewrite가 아니라 gold_daily_metrics table 하나로 partition overwrite + snapshot evidence를 확인하는 walking skeleton.
```

## 9. Next Action Before Coding

구현 전에 아래를 먼저 결정한다.

```text
1. Java version 확인
2. pyspark version 선택
3. Spark version에 맞는 Scala binary suffix 확인
4. Iceberg runtime jar coordinate 확정
5. jar 획득 방식 선택:
   - --packages로 Maven Central에서 받기
   - 또는 local jar path를 spark.jars로 지정
6. hadoop catalog warehouse path 확정
```

version pin 기록 형식:

```text
java_version:
pyspark_version:
spark_version:
scala_binary_version:
iceberg_version:
iceberg_runtime_coordinate:
jar_resolution:
warehouse_path:
```

이 결정이 끝나기 전에는 Spark/Iceberg code를 작성하지 않는다.
