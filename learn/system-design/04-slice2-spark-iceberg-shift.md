# 04. Slice2 — Spark/Iceberg로의 이동 (state 재표현)

상태: state-transition design bridge
프로젝트: `manufacturing-data-platform-mini`

> **Scope status:** 이 문서는 Spark/Iceberg로 state를 어떻게 재표현할지 설명하는 design bridge다.
> local single-gold-table walking skeleton은 구현됐고, 최신 runtime/test 결과는 [`../../VERIFICATION_LOG.md`](../../VERIFICATION_LOG.md)가 source of truth다.
> full Spark medallion rewrite와 production lakehouse operation은 아직 Backlog다.

이 문서는 Slice2를 "Spark/Iceberg를 붙인다"로 보지 않는다.
대신 이렇게 본다.

```text
Slice1의 데이터 상태 전이(state transition)를
Spark DataFrame과 Iceberg table/snapshot으로 다시 표현한다.
contract는 유지하고, 엔진과 저장소만 바꾼다.
```

`DECISION_LEARNING_PLAYBOOK.md`의 Core Loop를 따른다: scenario -> problem -> options -> reference decision -> tradeoff -> state -> contract -> test -> 구현.

## 1. 결론부터

- Slice2에서 **바뀌는 것** = 실행 엔진(python csv/dict -> Spark DataFrame)과 저장소(run별 CSV 폴더 -> Iceberg table + snapshot).
- Slice2에서 **그대로인 것** = transform 로직(순수 함수), quality check 정의, natural key, business_date partition 개념, source/schema identity, idempotency **contract**, lineage record 모양.
- Iceberg 기능은 **"있으니까" 넣지 않는다.** 이 프로젝트의 real pressure가 요구할 때만 넣고, 아니면 정직하게 "demo"로 표시한다.

## 2. 지금 Slice1의 state trace (data-first, t1 -> tN)

입력: 정형 CSV 파일 1개 (예: `manufacturing_events.csv`, 5 rows = 3 distinct + 1 dup + 1 prior-date).

```text
t1  read_rows(file)          -> columns[], rows[]            (실제 CSV header 캡처)
t2  hash                     -> source_hash, schema_hash     (schema_hash는 실제 header 기준)
t3  idempotency gate         -> find_existing_successful_run(dataset_id, business_date, source_hash)
                                이미 있으면 status=skipped 반환
t4  build run                -> run_id, paths(business_date=.../run_id=.../...)
t5  write_bronze             -> bronze/원본 copy + manifest.json(source_hash, schema_hash, row_count)
t6  transform_silver(rows)   -> silver rows (business_date 필터 + natural key dedup + normalize/cast)  [순수]
    write_silver             -> silver/*.csv
t7  transform_gold(silver)   -> gold rows (plant/line/product 일별 지표: units/defects/rate/avg_cycle)  [순수]
    write_gold               -> gold/*.csv
t8  schema drift             -> lookup_previous_schema_hash -> build_schema_drift_check(warn 정책)
t9  quality                  -> build_quality_checks(7개 dbt식) + drift -> quality_report.json
                                quality_passed = fail 없음
t10 catalog/lineage          -> build_lineage_doc(layers parents, paths, schema_drift, quality)
                                persist: mongo(lakehouse_runs+lineage_events) 또는 json(catalog_entry + _state 포인터)
=>  PipelineResult(status = processed | skipped)
```

state 모양(무엇이 흐르나):

```text
source CSV file
-> bronze: raw copy + manifest.json
-> silver: typed/deduped rows (csv)
-> gold: daily metric rows (csv)
-> quality_report.json (pass/fail per check)
-> lakehouse_runs doc / _state json (run + lineage + drift)
```

## 3. Spark/Iceberg로 가면 무엇이 그대로고 무엇이 바뀌나

| 요소 | Slice1 (지금) | Slice2 (Spark/Iceberg) | 성격 |
|---|---|---|---|
| transform 로직 | 순수 python 함수(rows->rows) | 순수 함수(DataFrame->DataFrame) | **유지** (테스트 가능성 그대로) |
| quality check 정의 | dbt식 check dict | 같은 정의, Spark agg로 계산 | **유지** |
| natural key / dedup | `work_order_id+machine_id+event_time` | 동일 key로 dropDuplicates | **유지** |
| business_date | run 인자 / row 값 | Iceberg partition column | 개념 유지, 표현 바뀜 |
| 실행 엔진 | python csv 모듈 | Spark local[*] DataFrame | **바뀜** |
| 저장소 | run별 폴더의 csv | Iceberg table (bronze/silver/gold) | **바뀜** |
| idempotency 메커니즘 | "이미 성공 run이면 skip" | partition 단위 atomic overwrite (또는 skip 유지) | **바뀜 — 결정 필요** |
| run vs commit 식별 | `run_id` 폴더 1개 | `run_id`(파이프라인 실행) + 참조하는 snapshot ids(table commit) | **대체 아님 — run이 snapshot을 참조** |
| reconciliation | python sum 비교 | Spark agg 비교 | 유지(엔진만) |
| schema drift | `schema_hash` warn | Iceberg schema evolution(add column) + 우리 warn 유지? | 결정 필요 |

> ★ 왜 이게 깔끔하게 되나: Slice1 hardening에서 **transform/IO를 이미 분리**해뒀기 때문. 엔진 교체가 재설계가 아니라 이식이 된다. 이게 그때 그 결정의 payoff.

### 3.1 run_id ≠ snapshot_id (중요 · 대체 아님 참조)

흔한 오해: "Iceberg로 가면 run_id가 snapshot_id로 대체된다." **아니다.** 둘은 단위가 다르다.

```text
run_id       = 우리 파이프라인 실행 1회 (lakehouse_runs)
snapshot_id  = Iceberg table commit 1회 (table마다)

한 번의 run이 여러 commit을 만든다:
  run_id=R
    -> bronze_events  commit -> bronze_snapshot_id
    -> silver_events  commit -> silver_snapshot_id
    -> gold_daily     commit -> gold_snapshot_id

lakehouse_runs.run_id는 그대로 남고,
그 안에 silver_snapshot_id / gold_snapshot_id를 참조로 기록한다.
```

면접 정확도: "snapshot이 run을 대체합니다" ❌ → **"우리 run metadata가 Iceberg snapshot id를 참조합니다"** ⭕. lineage도 이 참조로 이어진다("이 gold snapshot은 어느 run/어느 silver snapshot에서 왔나").

### 3.2 저장 layer 모델 (bronze의 의미 정리)

"bronze는 raw file 유지"와 "bronze/silver/gold Iceberg medallion"이 충돌하지 않게, 4층으로 나눈다.

```text
source_archive / raw_file      = 원본 CSV 그대로 보존 (immutable source archive, 변형 X)
bronze_events   Iceberg table  = raw를 거의 변형 없이 Spark가 읽은 append/raw-ish table
silver_events   Iceberg table  = typed / filtered / deduped
gold_daily_metrics Iceberg table = aggregate mart
```

이렇게 하면 "원본은 파일로 보존"도 맞고 "bronze/silver/gold Iceberg medallion"도 맞다. bronze를 원본 그 자체로 착각하지 않고 **"immutable source archive + raw-preserving bronze table"**로 설명한다.

## 4. Iceberg 기능 → 이 프로젝트의 어떤 pressure가 요구하나 (cargo-cult guard)

각 기능은 "Iceberg에 있으니까"가 아니라 **이 프로젝트의 real pressure**로만 정당화한다.

| Iceberg 기능 | 숨은 질문 (pressure) | 지금 Slice1은? | Iceberg가 주는 것 | real? |
|---|---|---|---|---|
| ACID / atomic write | 같은 business_date를 재처리하면 half-written/중복 gold가 안 생겨야 | 재처리를 아예 skip(idempotency) | partition 단위 **atomic overwrite** (재처리 허용하면서 원자적 교체) | **REAL** — skip보다 유연 |
| partitioning | business_date로 필터할 때 전부 안 읽고 싶다 | 폴더 `business_date=.../`로 이미 흉내 | partition spec + **partition pruning** | **REAL** — 이미 우리가 흉내 중 |
| schema evolution | `operator_id` 추가를 파일 rewrite 없이 | `schema_hash` warn(감지만) | table metadata로 add column, 과거 snapshot 보존 | **SEMI** — 감지는 이미 함, evolve가 새 능력 |
| snapshot / time travel | 재처리 전후 결과를 비교/재현 | run_id 폴더가 여러 벌(수동) | snapshot id로 `VERSION AS OF` | **DEMO** (core 아님) — 아래 3단 분리 |
| MERGE / upsert | late-arriving row를 기존 partition에 반영 | 없음(전체 재처리) | `MERGE INTO` | 나중 — 지금은 과함(Avoid) |

이 표가 곧 cargo-cult 방지장치다. **time travel의 정직한 위치**(가장 위험한 지점):

```text
core pressure   : atomic overwrite로 재처리 가능 (이건 진짜 필요)
supporting demo : overwrite 전후 snapshot을 읽어 이전 gold를 재현 (snapshot이 남으니 "덤"으로 됨)
not v0          : production rollback / RESTORE 운영 / snapshot retention·expire 정책
```

즉 time travel을 "운영 복구 핵심 기능"처럼 말하지 않는다. **"Iceberg snapshot이 남기 때문에 재처리 전후 결과를 비교/재현할 수 있음을 test로 확인했다"** 정도가 정직하다.

## 5. 그래서 나올 reference-decision 노트 (다음에 같이 쓸 것)

`learn/reference-decisions/`에 `schema-drift.md`와 같은 16-section 포맷으로:

```text
iceberg-write-semantics.md   append vs overwrite vs merge; idempotency를 어떻게 재해석하나
partitioning-and-shuffle.md  partition column 선택; Spark shuffle이 어디서 나나(groupBy)
schema-evolution.md          schema_hash warn -> Iceberg add-column; 무엇을 허용/금지
time-travel-snapshot.md      real pressure를 먼저 정하고, snapshot 전후 비교 test로 증명(또는 demo로 표시)
```

각 노트는 코드보다 먼저 나오고, 마지막에 **test contract**부터 쓴다 (playbook Test-First).

## 6. 전제 / 한계 (정직 가드)

- **pyspark 미설치.** Slice2의 step 0 = `pyspark` + `iceberg-spark-runtime` jar로 **로컬에서 Iceberg table 1개를 만들고 read/snapshot 확인**(walking skeleton). 이게 되기 전엔 medallion 이식 안 함. "로컬에서 실제로 도는가"를 먼저 증명.
- 클러스터 없음. `local[*]` mode + 작은 synthetic 데이터. **production lakehouse를 만든 척 안 한다.**
- 목표는 "전문가"가 아니라 **"로컬에서 Spark+Iceberg medallion을 작게 직접 구현 + 원리 5분 설명"** 수준.
- 회사 코드/데이터 미사용. synthetic만.

## 7. 같이 볼 질문 (여기서부터 대화)

바로 코드/설계 확정하지 말고, 아래를 같이 정한다.

1. Iceberg를 Slice1 옆 **3번째 backend 옵션**(mongo/json/iceberg)으로 붙일까, 아니면 별도 `pipeline/spark_lakehouse.py`로 둘까?
2. idempotency를 "skip existing run"에서 "partition **atomic overwrite**"로 바꿀까, 둘 다 유지할까? (재처리를 허용할지 vs 막을지)
3. **time travel의 진짜 pressure**는 뭐로 잡을까? late-arriving correction 재현? bad run rollback? — 없으면 "demo"로 정직하게 표시할까?
4. (§3.2에서 초안 정리됨) 4층 모델 — `source_archive`(원본 파일) + `bronze_events`/`silver_events`/`gold_daily_metrics`(Iceberg). 남은 질문: `bronze_events`는 append-only인가 partition overwrite인가?
5. shuffle을 "설명 가능"하게 보려면 gold의 `groupBy` 하나면 충분한가, join도 넣어야 하나?
6. transform을 Spark로 옮길 때 순수성(테스트 가능성)을 어떻게 유지할까? `DataFrame -> DataFrame` 순수 함수 + 작은 golden test로?

## 8. 면접 답변 (초안 · 30-60초)

> Slice2에서 저는 Spark/Iceberg를 새로 붙였다기보다, Slice1의 bronze/silver/gold 상태 전이를 Iceberg table과 snapshot으로 다시 표현했습니다. transform 로직은 순수 함수라 엔진만 Spark DataFrame으로 바꿨고, quality/lineage/idempotency contract는 유지했습니다. Iceberg의 ACID overwrite로 같은 business_date 재처리를 원자적으로 다루고, partition으로 date pruning을 설명할 수 있습니다. time travel/schema evolution은 이 프로젝트에서 실제로 필요한 시나리오가 있을 때만 넣고, 없으면 demo라고 정직하게 구분했습니다. 로컬 single-node라 production 대규모는 backlog로 명시했습니다.
