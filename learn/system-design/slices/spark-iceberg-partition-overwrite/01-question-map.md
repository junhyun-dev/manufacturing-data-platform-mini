# 01. Spark/Iceberg Question Map

상태: audited question input / local walking skeleton implemented
프로젝트: `manufacturing-data-platform-mini`

> 목적: Slice2에서 "무슨 질문들이 나올 수 있고, 각각 어디서/어떻게 풀리는가"를 먼저 넓게 펼친다.
> 방식: `plastic-labs-honcho/learn/15-backend-question-map.md`와 같은 question-map. decision을 확정하기 전에 질문 지도를 먼저 그린다.
> 짝 문서: [`02-state-shift.md`](02-state-shift.md) (무엇이 유지/바뀌나 + pressure 개요).

이 문서가 Slice2 설계 대화의 중심이다. `01`은 질문을 만들기 위한 scenario seed이고, `04`는 질문을 state trace로 검증하기 위한 보조 지도다.

읽는 법:

```text
../../08-area-question-bank.ko.md
  -> 보안 / 분산처리 / 재처리 / 장애 / 품질 / 운영 / claim 같은 전체 질문 축

01-question-map.md
  -> 그중 Spark/Iceberg Slice2에 걸리는 질문만 모은 slice-specific 지도

04-walking-skeleton-plan.md
  -> 02에서 고른 Core 질문을 실제 walking skeleton test contract로 내린 문서
```

감사 상태: **Claude / 외부 benchmark 기반 question map audit 필요.**

이 문서는 Codex 단독 최종본이 아니다. 질문을 잘 뽑는 것이 설계 품질을 결정하므로, 구현 전에 반드시 다른 관점으로 빠진 질문/과한 질문/과장 위험을 감사한다.

---

## One Sentence

Slice2를 엔진/저장소 관점에서 보면 "Spark/Iceberg를 붙인다"가 아니라, 아래 문제들의 묶음이다.

```text
table format / ACID commit semantics
write semantics (append / overwrite / merge)
partitioning & file layout
shuffle & compute cost
schema evolution (detect -> evolve)
time travel / rollback
catalog integration (Iceberg catalog vs 우리 Mongo/JSON catalog)
quality on Spark
lineage across snapshots
idempotency re-expressed
local single-node execution reality
testing (pure transform vs engine vs table)
operability / what an operator sees
```

핵심 관점: Slice1의 **contract**(quality, lineage, idempotency, natural key, schema identity)는 유지하고, 그것을 Spark DataFrame과 Iceberg table/snapshot으로 **다시 표현**할 때 어떤 질문이 생기는가.

---

## Scenario -> Question Map

question map은 추상 체크리스트가 아니다. 항상 작은 시나리오에서 출발한다.

```text
scenario:
  같은 business_date를 다시 처리한다.

questions:
  append / overwrite / merge 중 무엇인가?
  Slice1의 skip idempotency를 무엇으로 대체하나?
  overwrite 전 결과는 snapshot으로 남는가?
  run_id와 snapshot_id는 어떻게 연결되는가?
```

```text
scenario:
  source CSV에 operator_id 컬럼이 새로 들어온다.

questions:
  schema_hash warn만 할 것인가, Iceberg add column을 수행할 것인가?
  silver/gold contract가 조용히 바뀌지 않게 무엇을 막을 것인가?
  과거 snapshot은 어떤 schema로 읽히는가?
```

```text
scenario:
  gold 숫자가 이상해서 어제 결과와 비교해야 한다.

questions:
  time travel이 real pressure인가, demo인가?
  어떤 snapshot 전후를 비교해야 하는가?
  lineage에 어떤 snapshot id를 남겨야 하는가?
```

```text
scenario:
  gold defect_rate가 이상해서 operator가 원인을 좁혀야 한다.

questions:
  gold row grain은 무엇인가?
  latest successful run을 어떻게 찾는가?
  quality check 중 어떤 fail/warn이 RCA에 유효한가?
  lineage parent links로 gold -> silver -> bronze/source를 어떻게 역추적하나?
  source_hash/schema_hash/reuse_count를 보고 data change와 retry를 어떻게 구분하나?
```

질문을 볼 때는 각 항목에 아래 태그를 붙인다.

```text
Core     v0 구현 전에 반드시 답해야 한다.
Demo     기능 시연으로 충분하다. README에서 과장하지 않는다.
Backlog  production/운영 수준이라 v0에서는 피한다.
Unknown  walking skeleton이나 작은 테스트를 해봐야 답할 수 있다.
```

---

## Already Covered (Slice1이 이미 답한 것)

| 영역 | Covered in |
|---|---|
| medallion bronze/silver/gold 흐름 | `lakehouse.py`, `../../scenarios/00-scenario-seed.md`, `../../source-contracts/01-manufacturing-csv.md` |
| source/schema identity (`source_hash`, `schema_hash`) | `../../source-contracts/01-manufacturing-csv.md`, `../../../reference-decisions/schema-drift.md` |
| schema drift detect + warn 정책 | `../../../reference-decisions/schema-drift.md` |
| idempotency (skip existing successful run) | `lakehouse.find_existing_successful_run`, `02` |
| quality suite (dbt식 check dict) | `lakehouse.build_quality_checks` |
| lineage (parent links, run record) | `lakehouse.build_lineage_doc` |
| 무엇이 유지/바뀌나 + pressure 개요 | `02-state-shift.md` |

즉 Slice2는 이 위에서 **엔진/저장소만** 바꾼다. 아래는 그때 새로 생기는 질문들이다.

---

## Missing / New (Slice2가 새로 던지는 것)

```text
table format ACID commit          (파일 더미 -> metadata 있는 table)
snapshot                          (table commit 단위; run_id를 대체하지 않고 run이 참조)
time travel                       (snapshot이 남아 재처리 전후 비교 — core 아니라 demo)
partition spec + pruning          (folder 흉내 -> Iceberg partition)
shuffle                           (python sum -> Spark 분산 집계)
Iceberg catalog config            (namespace/warehouse/catalog type)
local Spark session               (pyspark 미설치 -> walking skeleton)
schema evolve as table op         (detect만 -> add column 실제 수행)
operator RCA path                 (gold metric -> run/source/quality/lineage evidence)
gold grain contract               (row 하나의 의미를 먼저 고정)
age freshness SLA                 (현재 freshness_business_date와 별도 backlog)
```

> ★ run_id ≠ snapshot_id: `run_id`(파이프라인 실행)는 유지되고, table마다의 `snapshot_id`(commit)를 참조로 기록한다. 한 run이 silver/gold snapshot을 각각 만든다. 자세히는 [`02-state-shift.md` §3.1](02-state-shift.md).

---

## Question Catalog

한 번에 다 풀지 않는다. 지금 볼 pressure만 고르고, Slice1 state에 연결하고, reference를 본 뒤 copy/simplify/avoid를 정한다.

### 1. Table format / ACID commit semantics

```text
왜 raw file 더미가 아니라 metadata layer가 있는 table이 필요한가?
"atomic commit"이 무슨 뜻인가? 반쪽 write(half-written gold)가 왜 안 생기나?
write 하나가 실패하면 table 상태는 어떻게 남나? (이전 snapshot 그대로?)
snapshot은 언제 생기나 — 매 write(insert/overwrite)마다?
single process인 우리 v0에서 ACID가 실제로 사는 지점은 무엇인가? (동시성 아니라 원자성)
```

볼 곳: `02` §4, Iceberg docs → 미래 노트 `reference-decisions/iceberg-write-semantics.md`

### 2. Write semantics — append / overwrite / merge

```text
같은 business_date를 재처리하면 append인가 overwrite인가 merge인가?
Slice1의 "skip existing run"을 무엇으로 대체하나?
partition 단위 overwrite가 idempotency를 어떻게 재현하나?
overwrite하면 이전 결과는 사라지나, snapshot으로 남나?
late-arriving row 하나는 전체 재처리인가 MERGE INTO인가?
```

볼 곳: `lakehouse.find_existing_successful_run` (현재 skip 로직) → `iceberg-write-semantics.md`

### 3. Partitioning & file layout

```text
partition column은 business_date인가? 왜 그게 자연스러운가?
Iceberg hidden partitioning은 우리 폴더 `business_date=.../`와 무엇이 다른가?
partition pruning이 query에서 실제 이득을 준다는 걸 어떻게 보이나?
partition이 너무 잘게 쪼개지면(small files) 무슨 문제가 생기나?
plant_id/line_id로도 partition할까, business_date만 할까?
```

볼 곳: `lakehouse.build_paths` (지금 folder partition 흉내) → `partitioning-and-shuffle.md`

### 4. Shuffle & compute cost

```text
Spark에서 shuffle은 어디서 발생하나 — gold의 groupBy? join? repartition?
silver dedup(dropDuplicates)도 shuffle인가?
shuffle이 왜 비싼지를 이 작은 프로젝트에서 어떻게 관측/설명하나?
local[*] mode에서 shuffle stage를 실제로 볼 수 있나? (Spark UI / explain())
집계 전에 partition을 맞추면 shuffle이 줄어드는가?
```

볼 곳: `lakehouse.transform_gold` (지금 python groupby) → `partitioning-and-shuffle.md`

### 5. Schema evolution — detect에서 evolve로

```text
Slice1은 schema_hash로 detect + warn만 한다. Iceberg는 add column을 table op로 한다. 무엇을 허용/금지할까?
컬럼 추가 / 삭제 / rename / type change 중 Iceberg가 안전하게 지원하는 건 무엇인가?
우리 schema_drift warn과 Iceberg schema evolution을 둘 다 둘까, 하나로 합칠까?
add column 후 과거 snapshot은 옛 schema로 읽히나?
downstream gold/mart contract가 조용히 바뀌는 걸 어떻게 막나?
```

볼 곳: `reference-decisions/schema-drift.md` → `schema-evolution.md`

### 6. Time travel / rollback (정직한 위치 = core 아님)

```text
core pressure   : atomic overwrite로 재처리 가능 (이건 진짜 필요, write-semantics에서 다룸)
supporting demo : overwrite 전후 snapshot을 읽어 이전 gold 재현 (snapshot이 남으니 덤)
not v0          : production rollback / RESTORE 운영 / snapshot retention·expire

demo 증명 질문:
  snapshot S1(재처리 전 gold)과 S2(재처리 후 gold)를 어떤 비교 test로 보이나?
  time travel을 "운영 복구 핵심"으로 말하지 않고 "재현/비교 확인"으로만 주장하나?
```

볼 곳: `02` §4 (core/demo/backlog 3단 분리) → `time-travel-snapshot.md` (demo 범위로)

### 7. Catalog integration — Iceberg catalog vs 우리 catalog

```text
Iceberg도 namespace/table catalog를 갖는다. 우리 Mongo `datasets`/`lakehouse_runs`와 역할이 겹치나?
로컬에선 어떤 Iceberg catalog를 쓰나 — hadoop(파일 기반) / rest / jdbc / hive?
warehouse path는 어디로 두나?
결론: Iceberg는 storage/table로만 쓰고, business catalog/lineage는 우리 Mongo/JSON에 유지할까?
`lakehouse_runs`에 snapshot id를 추가로 기록할까?
```

볼 곳: `catalog.py`, `lakehouse.persist_catalog` → `iceberg-write-semantics.md` 또는 별도 catalog 노트

### 8. Quality on Spark

```text
dbt식 check를 Spark DataFrame에서 어떻게 계산하나? (count / agg / filter)
reconciliation(source->silver, silver->gold conservation)을 Spark agg로 옮겨도 결과가 같나?
quality_report는 여전히 json인가, Iceberg table로 남기나?
count 같은 Spark action이 여러 번이면 재계산 비용은? cache/persist가 필요한가?
check가 실패하면 write를 막나(fail run), 아니면 write하고 warn만 남기나?
```

볼 곳: `lakehouse.build_quality_checks` (대체로 정의는 유지, 계산 엔진만 교체)

### 9. Lineage across snapshots

```text
lakehouse_runs.run_id에 silver_snapshot_id / gold_snapshot_id를 참조로 기록하나? (run이 snapshot을 참조 — 대체 아님)
input file -> source_hash -> run_id -> (bronze/silver/gold snapshot id) 체인을 어떻게 남기나?
Slice1의 layers[].parents(파일 path 기반)를 snapshot 참조로 확장할까, 둘 다 둘까?
운영자가 "이 gold 숫자가 어느 run_id / 어느 gold_snapshot에서 나왔나"를 어떻게 역추적?
```

볼 곳: `lakehouse.build_lineage_doc` → 미래 lineage 노트

### 10. Idempotency re-expressed

```text
"같은 입력 재처리 = 중복 없음"을 Iceberg에서 어떻게 보장/증명하나?
source_hash는 여전히 idempotency key다 (snapshot이 이걸 대체하지 않음).
같은 source_hash면 skip, 정정된 source(다른 hash)면 partition atomic overwrite -> 그 partition에 새 gold_snapshot?
재처리 test: 같은 파일 2번 -> gold row 수가 안 늘어난다를 어떻게 assert?
```

볼 곳: `02` §4, `lakehouse.find_existing_successful_run` → `iceberg-write-semantics.md`

### 11. Local single-node execution reality (walking skeleton)

```text
pyspark + iceberg-spark-runtime jar를 로컬에 어떻게 세팅하나? (버전 매칭이 제일 잘 깨진다)
SparkSession config에 무엇이 필요한가 — catalog impl, warehouse path, extensions?
Iceberg table 1개 create / insert / read / snapshot 확인이 실제로 로컬에서 도는가?
Docker 없이 되나? (Slice1은 Docker 없어서 mongomock/json으로 우회했다)
jar 다운로드/오프라인 환경 제약은 없나?
```

볼 곳: (아직 없음) → 별도 setup/walking-skeleton 노트. **이게 Slice2의 step 0.**

### 12. Testing — pure transform vs engine vs table

```text
transform을 DataFrame -> DataFrame 순수 함수로 두면 golden test가 되나?
Spark test는 session 생성이 느리다. 무엇을 unit(순수)로, 무엇을 integration(Spark)로 나누나?
time travel / schema evolution을 어떤 test로 증명하나? (playbook의 golden example test 형태)
local Spark test가 CI/이 환경에서 실제로 도는가?
Slice1의 golden test(input 5 -> silver 3 -> gold 1 -> quality pass/pass/warn)를 그대로 재현할 수 있나?
```

볼 곳: `tests/test_lakehouse_pipeline.py` 스타일 → 각 decision 노트의 Test Contract

### 13. Operability / what an operator sees

```text
snapshot 목록과 각 snapshot의 row count/summary를 어떻게 보나? (Iceberg metadata tables: `.snapshots`, `.history`)
quality fail은 어디에 남나?
commit 안 된 실패 write는 어떻게 보이나(안 보이나)?
운영자가 raw data를 안 열고 "이 table이 언제 어떤 run으로 바뀌었나"를 알 수 있나?
```

볼 곳: → operability 노트 (대부분 v0 backlog일 가능성)

### 14. Operator RCA path — suspicious gold metric

```text
gold 숫자가 이상하면 operator는 무엇을 어떤 순서로 조회하나?
gold grain을 모르면 defect_rate 이상을 어떤 단위로 볼 수 있나?
latest successful run과 source_hash/schema_hash는 어디에 남나?
quality fail/warn 중 어떤 것이 원인 후보인가?
lineage parent links는 gold -> silver -> bronze/source 역추적에 충분한가?
reuse_count는 단순 retry와 새 입력 처리를 구분하는 데 도움이 되나?
```

볼 곳: `scenarios/02-operator-debugging-wrong-gold.md`, `../reference-decisions/gold-grain.md`, `lakehouse.build_lineage_doc`, `persist_catalog`

분류:

```text
Core-design: walkthrough + doc contract
Core-implementation candidate: read-only evidence report helper
Backlog: OpenLineage backend, column-level lineage, UI
```

### 15. Freshness meaning — date validity vs age SLA

```text
현재 freshness_business_date는 dbt/DataHub식 "source가 몇 시간 이내에 도착했나"가 아니다.
active business_date가 유효한 ISO date이고 gold가 그 날짜로 필터링됐는지 보는 partition/date guard다.
이름이 freshness라서 age-based SLA로 오독될 수 있다.
```

분류:

```text
Core-doc: README와 decision notes에서 의미를 명시
Backlog: source-arrival timestamp 기반 age freshness SLA
Possible rename: active_business_date_validity
```

---

## How To Use This Map

honcho 방식 그대로.

```text
1. scenario seed를 잡는다.
2. 이 문서에서 scenario가 만드는 질문을 넓게 펼친다.
3. 각 질문에 Core / Demo / Backlog / Unknown 태그를 붙인다.
4. Claude / 외부 benchmark로 question map audit을 받는다.
5. 빠진 질문, 과한 질문, 과장 위험을 반영한다.
6. 03의 state trace에서 질문이 실제 어디서 발생하는지 확인한다.
7. reference(Iceberg/Spark/dbt/playbook)의 답을 본다.
8. Slice1에 이미 있는 contract와 연결한다.
9. copy / simplify / avoid를 정한다.
10. 그 결과를 reference-decisions/*.md(schema-drift.md 포맷)로 내려쓴다.
11. test contract를 먼저 쓰고, 작은 구현으로 검증한다.
```

## Claude Audit Prompt

```text
아래 문서를 구현 계획이 아니라 question map audit 대상으로 봐줘.

목표:
- Apache Spark / Apache Iceberg / lakehouse medallion / data quality / lineage 관점에서 빠진 설계 질문을 찾는다.
- 잘 만든 프로젝트나 공식 문서의 관점과 비교한다.
- 단, 구현 범위를 늘리지 말고 질문만 보강한다.
- 각 피드백은 Core / Demo / Backlog / Unknown으로 분류한다.
- v0에서 하지 말아야 할 것도 명시한다.

볼 문서:
- `01-question-map.md`
- `02-state-shift.md`
- workspace-level decision learning playbook, if available

출력 형식:
1. 현재 question map에서 잘 잡힌 질문
2. 빠진 질문
3. 과한 질문
4. Core로 올려야 할 질문
5. Backlog로 내려야 할 질문
6. README/면접에서 과장 위험이 있는 표현
7. 수정 제안
```

예:

```text
"같은 날짜를 다시 돌리면?" 을 볼 때:
  write semantics (2)
  idempotency (10)
  ACID commit (1)
  testing (12)

"partition을 왜 그렇게 잡나?" 를 볼 때:
  partitioning (3)
  shuffle (4)
  operability (13)

"컬럼이 추가되면?" 을 볼 때:
  schema evolution (5)
  (이미 있는) schema-drift.md
  time travel (6, 과거 snapshot 읽기)
```

---

## Suggested Next Deep Dives (순서)

```text
0. operator RCA path (14)            먼저: 기존 lineage/catalog claim을 walkthrough로 검증
1. gold-grain contract               gold row의 의미를 문서/면접 claim으로 고정
2. walking skeleton (11)             그 다음: pyspark+iceberg가 로컬에서 실제로 도는가
3. iceberg-write-semantics (1,2,10)  append/overwrite/merge + idempotency 재해석 = Slice2의 심장
4. partitioning-and-shuffle (3,4)    partition 선택 + shuffle 발생 지점(설명 가능성)
5. schema-evolution (5)              detect -> evolve, 무엇을 허용/금지
6. time-travel-snapshot (6)          real pressure 정하기(또는 demo로 표시) + 증명 test
```

이 순서가 좋은 이유:

```text
walking skeleton:  안 돌면 나머지 설계가 공중에 뜬다.
write semantics:   idempotency/재처리는 Slice1의 핵심 contract라 여기부터.
partition/shuffle: "Spark를 안다"의 최소 threshold(면접 단골).
schema evolution:  이미 있는 schema-drift.md를 table op로 확장.
time travel:       cargo-cult 위험이 가장 큰 곳 -> 마지막에 정직하게.
```

---

## Interview Answer

30-60초:

> Slice2를 설계할 때 저는 "Spark/Iceberg를 붙인다"가 아니라, Slice1의 상태 전이를 다시 표현할 때 어떤 질문이 생기는지를 먼저 지도로 그렸습니다. 크게 table format의 atomic commit, write semantics(append/overwrite/merge), partitioning과 shuffle, schema evolution, time travel, catalog 통합, Spark 위에서의 quality/lineage, 그리고 로컬 single-node 실행 현실입니다. 예를 들어 같은 business_date 재처리는 Slice1에선 skip으로 막았는데, Iceberg에선 partition 단위 atomic overwrite로 재해석할 수 있습니다. time travel처럼 실제 pressure가 약한 기능은 억지로 넣지 않고 demo로 정직하게 구분했습니다. 그다음에야 하나씩 decision 노트로 내려가 test부터 쓰고 구현했습니다.
