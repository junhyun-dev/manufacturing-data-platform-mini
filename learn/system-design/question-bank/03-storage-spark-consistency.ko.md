# 03. Storage / Spark / Consistency 질문 상세

상위 문서: [`../08-area-question-bank.ko.md`](../08-area-question-bank.ko.md)

이 문서는 파일 저장 방식, Spark 같은 분산 처리 엔진, Iceberg 같은 table format, atomicity/consistency 질문을 다룬다.

## 1. Storage / Table Format / File Layout

### 질문의 의도

파일을 쓴다는 것과 table을 운영한다는 것은 다르다.

```text
파일:
  path에 bytes가 있다.

table format:
  schema, partition, snapshot, metadata, commit history가 있다.
```

이 영역은 "왜 Iceberg/Delta/Hudi 같은 table format이 필요한가"를 묻는다.

### 핵심 질문

| 질문 | 의도 | 선택지 | Core가 되는 경우 |
|---|---|---|---|
| current state는 어디에 있는가? | downstream이 읽을 기준 | latest run folder / table current snapshot / manifest pointer | correction/backfill이 있을 때 |
| partition key는 무엇인가? | overwrite/query 범위 | business_date / plant_id / line_id / hidden partition | partition overwrite나 pruning을 claim할 때 |
| snapshot은 언제 생기는가? | version evidence | every commit / only successful publish / not tracked | Iceberg/Delta를 쓸 때 |
| warehouse path는 어디인가? | reproducibility | `/tmp` / repo data dir / object storage | local test나 CLI를 만들 때 |
| metadata table에서 무엇을 읽나? | evidence | snapshots / history / files / manifests | snapshot evidence를 claim할 때 |

### 선택지 예시

current state:

```text
run-folder only:
  history가 명확하지만 current pointer가 약하다.

latest_successful_run file:
  current run을 가리킬 수 있다.

Iceberg current snapshot:
  table 자체가 current state를 가진다.
```

partition strategy:

```text
business_date:
  재처리/백필 단위와 맞다.

plant_id + business_date:
  query pruning은 좋아질 수 있지만 small files 위험이 커진다.

no partition:
  단순하지만 날짜 correction 범위가 약하다.
```

### 놓치기 쉬운 질문

```text
partition은 write 범위인가, query 최적화인가, 둘 다인가?
raw/bronze/silver/gold를 모두 Iceberg로 바꿀 필요가 있는가?
snapshot retention을 안 하면 오래된 snapshot이 계속 쌓이지 않는가?
```

## 2. Spark / Distributed Processing

### 질문의 의도

Spark는 "대용량이라 멋있어 보이는 도구"가 아니다.

Spark를 도입하면 새 질문이 생긴다.

```text
shuffle은 어디서 발생하는가?
action은 몇 번 발생하는가?
partitioning은 write/query에 어떤 영향을 주는가?
local mode와 cluster mode의 차이는 무엇인가?
```

### 핵심 질문

| 질문 | 의도 | 선택지 | Core가 되는 경우 |
|---|---|---|---|
| Spark가 필요한 pressure는 무엇인가? | 도구 목적화 방지 | row volume / groupBy cost / multi-file / table format integration | Spark claim을 할 때 |
| transform contract는 유지되는가? | 엔진 교체 안전성 | same output schema / changed contract / separate demo | Python -> Spark port 시 |
| shuffle은 어디서 생기나? | 성능 이해 | groupBy / join / dropDuplicates / repartition | distributed processing을 설명할 때 |
| action은 몇 번 실행되나? | 비용 이해 | count/collect/write 반복 / cache / one-pass agg | Spark quality checks를 만들 때 |
| local mode 검증은 무엇을 의미하나? | claim boundary | runtime feasibility / production scale proof 아님 | local skeleton을 만들 때 |

### 선택지 예시

Spark 도입 범위:

```text
single-table walking skeleton:
  version/jar/catalog/write semantics를 작게 검증한다.

full transform port:
  bronze/silver/gold transform 전체를 DataFrame으로 옮긴다.

quality-on-Spark:
  quality check 계산도 Spark agg/filter로 옮긴다.
```

현재 프로젝트는 첫 번째만 했다.

shuffle 관찰:

```text
explain plan:
  문서/학습용으로 좋다.

Spark UI:
  local에서도 stage를 볼 수 있지만 자동화하기 어렵다.

test assertion:
  성능보다 contract를 검증한다.
```

### 놓치기 쉬운 질문

```text
Spark local mode 통과를 cluster 운영으로 과장하고 있지 않은가?
PySpark version, Scala suffix, Iceberg jar version이 서로 맞는가?
Spark job이 실패했을 때 Iceberg commit은 어떤 상태인가?
```

## 3. Concurrency / Atomicity / Consistency

### 질문의 의도

단일 로컬 실행에서는 잘 보이지 않지만, production data platform에서 중요한 영역이다.

```text
동시에 쓰면?
중간에 죽으면?
catalog는 업데이트됐는데 table commit은 실패하면?
```

### 핵심 질문

| 질문 | 의도 | 선택지 | Core가 되는 경우 |
|---|---|---|---|
| write atomicity가 필요한가? | half-written output 방지 | file temp+rename / table atomic commit / ignore | overwrite/correction이 있을 때 |
| concurrent writer를 처리하는가? | race condition 방지 | single writer assumption / optimistic commit / lock | production-like write claim 시 |
| business catalog와 table metadata가 불일치하면? | consistency gap 확인 | table first then catalog / catalog first / reconciliation job | run_id->snapshot_id를 기록할 때 |
| commit 실패 시 current state는? | recovery | previous snapshot 유지 / partial visible / manual cleanup | Iceberg/Delta write claim 시 |

### 선택지 예시

catalog/table update order:

```text
table commit first, catalog after:
  table은 안전하지만 catalog write 실패 시 mapping이 빠질 수 있다.

catalog pending, table commit, catalog success:
  상태 모델이 복잡하지만 복구 여지가 있다.

single JSON evidence only:
  local skeleton에는 충분하지만 production claim은 불가.
```

concurrency:

```text
single writer assumption:
  local skeleton에 적합하다.

optimistic concurrency:
  Iceberg production write에서 고려해야 한다.

external lock:
  orchestration/catalog 레벨에서 통제한다.
```

### 놓치기 쉬운 질문

```text
run_id -> snapshot_id JSON write가 실패하면 실제 table commit은 이미 성공한 것 아닌가?
same business_date correction 두 개가 동시에 오면 어떤 source가 current가 되는가?
atomic commit을 "rollback system"으로 과장하고 있지 않은가?
```
