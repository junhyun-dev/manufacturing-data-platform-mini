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

### Iceberg 실무 심화 질문

| 질문 | 쉬운 말로 풀면 | 왜 묻는가 | 선택지 | 이번 프로젝트 판단 |
|---|---|---|---|---|
| 어떤 catalog를 쓸 것인가? | table 이름과 metadata를 누가 관리하는가? | local hadoop catalog와 production REST/Hive/Glue catalog는 운영 성격이 다르다. Hadoop catalog는 atomic rename에 의존하므로 single-writer local 범위로 한정한다. | hadoop / Hive / REST / Glue / Nessie | Core-lite: local hadoop catalog. production catalog와 concurrent writer는 Backlog. |
| Iceberg current snapshot이 source of truth인가? | downstream은 어떤 snapshot을 읽는가? | run folder와 table current가 동시에 있으면 기준이 필요하다. | latest run folder / JSON current pointer / Iceberg current snapshot | publish slice: single Iceberg table은 publish 결과, JSON catalog가 publish source. 전체 gold mart Iceberg화 claim은 하지 않는다. |
| Iceberg branch로 write-audit-publish(WAP)를 할 것인가? | 검사 끝나기 전엔 안 보이는 staging branch에 써두고, 통과하면 main으로 갈아끼운다. | quality 통과 전 데이터가 소비자에게 노출되지 않게 하는 Iceberg-native 패턴이다. 현재 publish는 latest successful JSON만 읽는 branch 없는 단순화 WAP다. | JSON-level publish gate(현재) / Iceberg branch WAP / dual-table swap | Backlog(named): branch WAP는 구현하지 않고 production reference로만 언급한다. |
| partition evolution을 고려하는가? | 나중에 partition 기준을 바꿀 수 있는가? | Iceberg는 partition spec evolution이 가능하지만 write semantics가 복잡해진다. | fixed `business_date` / hidden partition / evolved partition spec | Core: fixed `business_date`. evolution은 Backlog. |
| schema evolution은 어디까지 허용하는가? | 컬럼 추가/삭제/타입 변경을 어떻게 다룰 것인가? | schema drift warn과 Iceberg schema evolution claim이 섞이면 과장된다. | warn only / add column / rename / type promotion | 현재: CSV schema drift warn. Iceberg schema evolution 구현 아님. |
| small file 문제를 어떻게 볼 것인가? | 작은 파일이 너무 많이 쌓이지 않는가? | partition overwrite를 자주 하면 file count가 늘 수 있다. | ignore local / compaction job / write target file size | Backlog: local small data라 compaction 없음. |
| write 전 분포와 sort order를 어떻게 둘 것인가? | 데이터를 파일로 쓰기 전에 어떻게 섞고 정렬할지 정한다. | write distribution mode와 sort order는 파일 수, small-files, query pruning에 영향을 준다. | none(현재 skeleton) / hash / range + sort order | Backlog: local small data라 default로 충분하다. tuning은 scale claim 전까지 하지 않는다. |
| snapshot retention/expire는 필요한가? | snapshot이 무한히 쌓이면 어떻게 하나? | time travel은 좋지만 metadata/storage cost가 생긴다. | keep all / expire snapshots / retention policy | Backlog: expire/retention 구현 없음. |
| orphan file은 어떻게 처리하는가? | commit 실패 후 남은 파일을 치우는가? | table metadata에 없는 data file이 남을 수 있다. | ignore local / remove_orphan_files / storage lifecycle | Backlog: production maintenance 영역. |
| overwrite와 MERGE 중 무엇을 쓸 것인가? | 날짜 partition 전체 교체인가, row-level 수정인가? | late event/upsert 문제와 correction file 문제는 다르다. | append / partition overwrite / MERGE | Core: business_date correction은 partition overwrite. MERGE는 Backlog. |
| update/delete를 한다면 COW인가 MOR인가, table format은 v2/v3 중 무엇인가? | 한 줄 고칠 때 파일을 통째로 다시 쓸지, 삭제 표식만 남길지 정한다. | MERGE/late-row를 Backlog로 둔 이유를 선명하게 한다. 이 프로젝트의 partition overwrite는 COW식 full-partition 교체라 delete file, MOR, v3 deletion vector가 필요 없다. | COW full-partition overwrite(현재) / MOR + equality deletes / v3 deletion vectors | Backlog(named): MERGE/delete/MOR/v3는 구현하지 않는다. |
| time travel을 claim할 수 있는가? | 이전 snapshot을 직접 읽어봤는가? | snapshot metadata와 time travel read는 다른 claim이다. | metadata only / VERSION AS OF read / rollback workflow | 현재: snapshot evidence 중심. rollback system 아님. |
| table maintenance는 누가 실행하는가? | rewrite/expire/orphan cleanup을 어디서 돌리나? | Iceberg 운영은 write만이 아니라 maintenance도 포함한다. | Airflow maintenance DAG / manual CLI / catalog service | Backlog. |

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

### Spark 실무 심화 질문

| 질문 | 쉬운 말로 풀면 | 왜 묻는가 | 선택지 | 이번 프로젝트 판단 |
|---|---|---|---|---|
| Spark를 쓰는 pressure가 실제로 있는가? | 데이터가 커서 필요한가, table format 때문에 필요한가? | "Spark를 써봤다"가 아니라 문제-도구 연결이 필요하다. | scale pressure / Iceberg integration / learning skeleton | 현재: Iceberg write semantics 검증이 목적. scale claim 아님. |
| local mode와 cluster mode의 차이를 어떻게 설명할 것인가? | 내 노트북에서 된 것이 운영 클러스터와 같은가? | dependency, executor, shuffle, storage 접근 방식이 달라진다. | local[2] / standalone cluster / YARN / K8s | Core-lite: local[2]. cluster는 Backlog. |
| Spark/Iceberg jar version은 어떻게 고정되는가? | PySpark, Scala suffix, Iceberg runtime이 맞는가? | 불일치 시 런타임 classpath 오류가 난다. | `--packages` / local jars / image baked jars | Core: `pyspark==3.5.8`, runtime `3.5_2.12:1.11.0`. |
| shuffle partition 수는 왜 정했는가? | 작은 데이터인데 task가 너무 많이 생기지 않는가? | local test에서 불필요한 overhead와 flaky를 줄인다. | default 200 / small fixed / adaptive query execution | Core-lite: local skeleton은 `spark.sql.shuffle.partitions=1`. |
| `collect()`는 어디까지 허용되는가? | driver로 데이터를 다 가져와도 되는가? | local evidence에는 괜찮지만 대규모 데이터에는 위험하다. | collect evidence only / write files / aggregated metrics only | Core: evidence row만 collect. production data collect claim 없음. |
| UDF를 쓸 것인가? | Python 함수를 Spark worker에서 돌릴 것인가? | UDF는 성능/직렬화/배포 문제가 생긴다. | built-in functions / SQL / Python UDF / pandas UDF | Backlog: 현재 Spark transform port 없음. |
| cache/persist가 필요한가? | 같은 DataFrame을 여러 번 계산하는가? | quality checks에서 action이 많으면 비용이 커진다. | no cache / cache before repeated actions / checkpoint | Backlog: Spark quality suite에서 다시 질문. |
| skew를 어떻게 감지하는가? | 특정 key에 데이터가 몰리는가? | groupBy/join이 느려지는 대표 원인이다. | explain/Spark UI / key distribution check / salting | Backlog: scale/performance claim 전까지 구현 안 함. |
| schema/type casting 실패는 어디서 처리하는가? | bad row가 Spark job 전체를 죽이는가? | strict cast와 quarantine policy를 결정해야 한다. | fail-fast / permissive parse / quarantine table | Backlog: manufacturing strict cast 개선과 연결. |
| Spark app failure와 Airflow retry는 어떻게 만나는가? | Spark job이 죽으면 Airflow는 같은 command를 재시도한다. | retry가 table/catalog state와 충돌하지 않아야 한다. | no retry / idempotent publish / pending state model | Core: publish retry skip. partial failure는 Backlog. |

### Spark/Iceberg 연결 seam 질문

실제로 깨지는 지점은 Spark 따로, Iceberg 따로가 아니라 둘 사이 seam에 많다.

| 질문 | 왜 중요한가 | 현재 답 |
|---|---|---|
| SparkSession extension이 빠지면 어떤 기능이 안 되는가? | SQL 확장, Iceberg write/read behavior가 달라질 수 있다. | skeleton에서 `IcebergSparkSessionExtensions` 설정. |
| catalog 이름 `local`이 DAG/CLI/test에서 일관되는가? | table identifier가 바뀌면 같은 table을 보고 있다고 착각할 수 있다. | `local.db.gold_daily_metrics`로 고정. |
| table commit은 됐는데 publish evidence JSON write가 실패하면? | Iceberg current와 run->snapshot mapping이 어긋난다. | Backlog: failure-state forensics에서 다룰 질문. |
| 같은 business_date를 두 Airflow run이 동시에 publish하면? | 마지막 commit이 current가 되며 의도한 source 우선순위가 필요하다. | local DAG는 `max_active_runs=1`; production concurrency는 claim하지 않음. |
| Maven Central 접근이 안 되면 runtime이 뜨는가? | `--packages` 방식은 네트워크/캐시 상태에 의존한다. | local에서는 cache/resolution 통과. offline packaging은 Backlog. |
| object storage를 쓰면 path/rename/consistency가 달라지는가? | local filesystem과 S3/GCS/ADLS는 운영 성격이 다르다. | local warehouse only. object storage는 Backlog. |

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

### consistency 실무 심화 질문

| 질문 | 쉬운 말로 풀면 | 왜 묻는가 | 선택지 | 이번 프로젝트 판단 |
|---|---|---|---|---|
| source/catalog/table 중 어떤 순서로 state가 바뀌는가? | 어느 단계까지 성공했는지 어떻게 아는가? | failure recovery는 state transition을 알아야 가능하다. | source first / catalog pending / table commit / catalog success | 현재 local slice는 simple evidence. pending state는 Backlog. |
| quality fail run이 current table로 갈 수 있는 경로가 있는가? | 실패한 데이터가 downstream에 노출되는가? | quality gate와 publish gate의 연결이 핵심이다. | no publish on fail / publish with warning / quarantine table | publish CLI는 latest successful JSON state만 읽는다. 이는 branch 없는 단순화 WAP다. |
| table commit과 catalog update를 하나의 transaction으로 묶을 수 있는가? | 둘 중 하나만 성공하면 어떻게 하나? | 서로 다른 system이면 exactly-once가 어렵다. | not atomic / reconciliation / transactional catalog | 현재 exactly-once transaction claim 없음. |
| retry가 table commit을 중복 생성하는가? | 같은 run retry가 snapshot을 계속 만들지 않는가? | scheduler retry가 metadata noise를 만들 수 있다. | always write / source_hash skip / publish state skip | publish는 같은 run/source 재발행 skip. |
| correction 우선순위는 무엇인가? | 두 정정 파일 중 어떤 것이 current인가? | event-time, arrival-time, approved flag 같은 정책이 필요하다. | latest successful / explicit version / operator approval | 현재 latest successful per business_date. 승인 workflow 없음. |
| failure cleanup은 자동인가 수동인가? | 실패 후 남은 파일/evidence를 누가 치우나? | cleanup이 없으면 다음 retry가 오염될 수 있다. | manual / idempotent overwrite / cleanup task | Backlog. |
