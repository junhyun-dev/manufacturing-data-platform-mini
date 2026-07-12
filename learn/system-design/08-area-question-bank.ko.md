# 08. 영역별 질문 은행

상태: 설계 브레인스토밍 지도 / 한글 기준 문서
프로젝트: `manufacturing-data-platform-mini`

> 이 문서는 바로 구현할 기능 목록이 아니다.
> "좋은 데이터 플랫폼을 만들려면 어떤 질문을 영역별로 던져야 하는가"를 모아둔 질문 은행이다.

## 1. 왜 이 문서가 필요한가

지금까지의 문서는 특정 slice를 닫기 위한 질문에 강했다.

예:

```text
같은 business_date 정정 데이터가 오면?
-> append / skip / overwrite / merge 중 무엇인가?
-> Iceberg partition overwrite로 작게 검증한다.
```

하지만 프로젝트 전체를 키우려면 질문을 더 넓게 봐야 한다.

```text
보안은?
분산처리는?
재처리는?
장애가 나면?
품질 실패는?
Airflow는 어디까지?
Spark는 왜 필요한가?
Iceberg는 무엇을 해결하는가?
면접/블로그에서 어디까지 말할 수 있나?
```

이 문서는 그런 질문들을 영역별로 정리한다.

## 2. 사용 방법

새 기능을 만들기 전에 아래 순서로 쓴다.

```text
1. 만들고 싶은 slice의 thesis를 한 문단으로 쓴다.
2. 이 문서에서 관련 영역 질문을 넓게 가져온다.
3. 질문마다 현재 답을 적는다.
4. Core / Demo / Backlog / Unknown으로 분류한다.
5. Core 질문만 decision note + test contract로 내린다.
6. 구현 후 VERIFICATION_LOG와 claim boundary를 갱신한다.
```

중요한 규칙:

```text
질문을 많이 뽑는 것 != 구현 범위를 키우는 것
```

질문은 넓게 뽑고, 구현은 좁게 한다.

## 2.1 상세 문서

이 문서는 전체 지도다. 질문의 의도, 선택지, Core로 내려오는 조건, 놓치기 쉬운 질문은 아래 상세 문서에 나눠 둔다.

| 상세 문서 | 다루는 영역 |
|---|---|
| [`question-bank/00-plain-language-guide.ko.md`](question-bank/00-plain-language-guide.ko.md) | 어려운 설계 용어를 쉬운 말과 예시로 번역 |
| [`question-bank/01-service-identity-contract.ko.md`](question-bank/01-service-identity-contract.ko.md) | service workflow, identity, source contract |
| [`question-bank/02-quality-rerun-failure.ko.md`](question-bank/02-quality-rerun-failure.ko.md) | quality, rerun/correction, failure state |
| [`question-bank/03-storage-spark-consistency.ko.md`](question-bank/03-storage-spark-consistency.ko.md) | storage/table format, Spark/distributed processing, consistency |
| [`question-bank/04-orchestration-observability.ko.md`](question-bank/04-orchestration-observability.ko.md) | Airflow/orchestration, operator evidence |
| [`question-bank/05-security-performance-testing-claim.ko.md`](question-bank/05-security-performance-testing-claim.ko.md) | security, performance, testing, public claim |
| [`question-bank/06-cross-area-connection-questions.ko.md`](question-bank/06-cross-area-connection-questions.ko.md) | 영역 사이에서 생기는 연결 질문 |
| [`question-bank/07-external-benchmark-backlog-areas.ko.md`](question-bank/07-external-benchmark-backlog-areas.ko.md) | 외부 benchmark 기준으로 named backlog에 둬야 할 영역 |

상세 문서는 아래 형식을 따른다.

```text
질문
-> 질문의 의도
-> 선택지
-> Core가 되는 경우
-> 놓치기 쉬운 질문
```

상태 정보 주의:

```text
이 문서는 질문 은행이다.
최신 테스트 수, CLI 검증 결과, 런타임 설치 상태는 VERIFICATION_LOG.md가 source of truth다.
아래 "현재 답" 표는 질문 이해를 돕기 위한 예시이며, 구현 상태를 확인할 때는 반드시 verification log를 다시 본다.
```

## 3. 현재 프로젝트의 큰 목적

이 프로젝트의 목적은 도구 이름을 많이 붙이는 것이 아니다.

```text
synthetic manufacturing-style/tabular data를
cataloged, versioned, quality-checked dataset/mart로 만들고,
운영자와 reviewer가 "이 숫자가 어디서 왔는지" 설명할 수 있는
evidence를 남기는 작은 데이터 플랫폼을 만든다.
```

따라서 모든 질문은 아래 service question으로 돌아와야 한다.

```text
이 데이터셋을 믿고 써도 되는가?
이 숫자는 어디서 왔는가?
같은 입력을 다시 돌려도 안전한가?
정정 입력이 오면 중복 없이 반영되는가?
schema나 source가 바뀌었을 때 알 수 있는가?
실패/품질 문제를 운영자가 좁혀갈 수 있는가?
```

## 4. 질문 분류 기준

| 분류 | 의미 |
|---|---|
| Core | 답이 바뀌면 이번 slice의 코드/테이블/파일/계약이 바뀐다. |
| Demo | 보여주면 좋지만 핵심 contract는 바꾸지 않는다. |
| Backlog | 중요하지만 이번 slice의 범위를 넘는다. |
| Unknown | 작은 실험이나 외부 조사 없이는 답할 수 없다. |

예:

```text
"business_date partition overwrite를 어떻게 검증할까?"
-> Spark/Iceberg skeleton에서는 Core

"concurrent writer conflict를 어떻게 처리할까?"
-> production Iceberg 운영에서는 중요하지만 이번 local skeleton에서는 Backlog

"이 WSL 환경에서 Iceberg jar가 실제로 뜰까?"
-> 구현 전에는 Unknown, Test 0 통과 후 Answered
```

## 5. 영역별 질문 지도

### 5.1 Service / User Workflow

목적: 기능이 아니라 사용자가 답해야 하는 질문에서 출발한다.

질문:

```text
누가 이 시스템을 쓰는가?
운영자는 어떤 순간에 막히는가?
분석가는 어떤 gold 숫자를 믿고 싶은가?
raw file을 바로 열지 않고 먼저 확인해야 하는 evidence는 무엇인가?
이 slice가 끝나면 사용자는 어떤 질문에 답할 수 있어야 하는가?
```

현재 답:

| 질문 | 현재 상태 | evidence |
|---|---|---|
| "이 숫자가 어디서 왔나?" | 구현됨 | `operator_report.py`, B4 |
| "같은 입력 재실행은 안전한가?" | 구현됨 | source_hash idempotency tests |
| "정정 입력은 어떻게 반영하나?" | local skeleton 구현됨 | Spark/Iceberg B5 |

다음에 볼 질문:

```text
실패한 run도 운영자가 같은 방식으로 조사할 수 있는가?
```

분류: Backlog / future slice.

### 5.2 Data Grain / Identity / Versioning

목적: row 하나, run 하나, source 하나의 identity를 명확히 한다.

질문:

```text
source file identity는 무엇인가? source_hash인가 file_id인가?
schema identity는 무엇인가? schema_hash는 actual header 기준인가?
gold row grain은 무엇인가?
run_id는 무엇을 의미하는가?
dataset_version과 run은 같은가 다른가?
Iceberg snapshot_id는 run_id를 대체하는가, run이 참조하는가?
```

현재 답:

| 항목 | 현재 답 | 상태 |
|---|---|---|
| source identity | CSV slice는 `source_hash`, EAV는 file hash 기반 `source_file_id` | 구현됨 |
| schema identity | actual CSV header 기반 `schema_hash` | 구현됨 |
| manufacturing gold grain | `(business_date, plant_id, line_id, product_code)` | 문서화됨 |
| EAV gold grain | `(business_date, entity_id)` | 문서화됨 |
| Iceberg snapshot | table commit identity, run_id와 별도 | skeleton 구현됨 |

Core로 자주 내려오는 질문:

```text
이 slice가 새 파일/테이블/evidence를 만들면 그 identity는 무엇인가?
```

### 5.3 Source Contract / Schema Evolution

목적: 입력이 바뀌었을 때 조용히 깨지지 않게 한다.

질문:

```text
필수 컬럼은 무엇인가?
필수 컬럼이 없으면 fail-fast인가 quality fail인가?
새 컬럼이 추가되면 fail인가 warn인가?
컬럼 삭제/rename/type change는 어떻게 처리할까?
schema_hash와 Iceberg schema evolution은 어떤 관계인가?
schema가 바뀌었을 때 downstream gold contract가 조용히 바뀌지 않게 무엇을 막을까?
```

현재 답:

| 질문 | 현재 답 | 상태 |
|---|---|---|
| added column detection | `schema_hash`가 actual header 기준이라 warn 가능 | 구현됨 |
| schema drift policy | `warn`, run은 실패시키지 않음 | 구현됨 |
| Iceberg add column | 아직 구현하지 않음 | Backlog |
| missing required column | `ValueError` fail-fast | 알려진 한계 |

다음 후보:

```text
missing required column을 structured quality report로 남길 것인가?
```

분류: Backlog.

### 5.4 Quality / Reconciliation / Anomaly Boundary

목적: "데이터가 만들어졌다"가 아니라 "검증 가능한가"를 본다.

질문:

```text
not_null / unique / accepted_values / range는 무엇을 보장하나?
source -> silver row count reconciliation은 filtering/dedup과 real loss를 구분하나?
silver -> gold conservation은 어떤 metric을 보존해야 하나?
freshness_business_date는 age freshness인가 partition/date validity인가?
quality fail이면 write를 막나, evidence만 남기나?
이상치 탐지는 quality check인가, 별도 anomaly detection인가?
```

현재 답:

| 항목 | 현재 답 | 상태 |
|---|---|---|
| dbt-style checks | not_null/unique/accepted/range/freshness/reconciliation | 구현됨 |
| freshness 의미 | age freshness가 아니라 partition/date validity | 문서화됨 |
| anomaly detection | 구현하지 않음 | Backlog |
| Spark quality checks | 구현하지 않음 | Backlog |

주의:

```text
"quality check가 있다"와 "이상치를 자동 탐지한다"는 다르다.
```

### 5.5 Rerun / Backfill / Correction

목적: 다시 돌려도 중복/오염이 생기지 않게 한다.

질문:

```text
같은 입력이 다시 들어온 것과 정정 입력이 들어온 것을 어떻게 구분하나?
same source_hash rerun은 skip인가 overwrite인가?
different source_hash + same business_date는 새 run인가 correction인가?
CSV run-folder 방식에서는 current gold 개념을 어떻게 표현하나?
Iceberg에서는 어떤 partition을 overwrite하나?
overwrite 전후 evidence는 무엇인가?
```

현재 답:

| 시나리오 | 현재 동작 | 상태 |
|---|---|---|
| same source_hash + same business_date | existing successful run 재사용 | 구현됨 |
| changed source_hash + same business_date in CSV slice | 새 run 생성 | 구현됨 |
| changed source_hash + same business_date in Iceberg skeleton | `business_date` partition overwrite + new snapshot | skeleton 구현됨 |

다음 후보:

```text
실패한 correction run이 있으면 current table은 어떤 상태로 남아야 하나?
```

분류: Backlog / failure-state slice.

### 5.6 Storage / Table Format / File Layout

목적: 파일 더미와 table format의 차이를 이해한다.

질문:

```text
raw file copy, bronze manifest, silver/gold output은 어디에 저장되나?
folder partition과 Iceberg partition은 무엇이 다른가?
Iceberg snapshot은 언제 생기나?
metadata table에서 어떤 evidence를 읽을 수 있나?
warehouse path는 어디인가?
retention/expire snapshot은 필요한가?
```

현재 답:

| 항목 | 현재 답 | 상태 |
|---|---|---|
| CSV lakehouse path | `business_date=.../run_id=...` | 구현됨 |
| Iceberg warehouse | `/tmp/manufacturing-mini-iceberg-warehouse` | skeleton 구현됨 |
| Iceberg table | `local.db.gold_daily_metrics` 하나 | skeleton 구현됨 |
| retention/expire | 구현하지 않음 | Backlog |

### 5.7 Spark / Distributed Processing

목적: Spark를 "붙이는 것"이 아니라, 분산 처리에서 새로 생기는 질문을 본다.

질문:

```text
Spark가 필요한 데이터 크기/연산 압력은 무엇인가?
현재 Python transform을 Spark DataFrame으로 옮기면 contract가 유지되는가?
groupBy에서 shuffle이 발생하는가?
dedup/dropDuplicates도 shuffle을 만들 수 있는가?
local mode에서 explain plan이나 stage를 볼 수 있는가?
partitioning이 query pruning이나 shuffle에 어떤 영향을 주는가?
Spark action(count/collect/write)이 여러 번이면 재계산 비용은 어떤가?
cache/persist가 필요한가?
```

현재 답:

| 항목 | 현재 답 | 상태 |
|---|---|---|
| local SparkSession | Spark/Iceberg local runtime 검증 기록은 `VERIFICATION_LOG.md` 확인 | verification log |
| full transform Spark port | 구현하지 않음 | Backlog |
| shuffle 관찰 | 아직 안 함 | Demo/Backlog |
| quality-on-Spark | 구현하지 않음 | Backlog |

중요한 경계:

```text
Spark/Iceberg skeleton을 구현했다 != full Spark pipeline을 구현했다
```

### 5.8 Concurrency / Atomicity / Consistency

목적: 동시에 쓰거나 실패했을 때 상태가 어떻게 보존되는지 묻는다.

질문:

```text
write 중 실패하면 이전 gold 상태가 유지되는가?
같은 table/partition에 concurrent writer가 있으면 어떻게 되는가?
atomic commit은 local skeleton에서 어떤 의미로만 검증했나?
Mongo/JSON catalog와 Iceberg commit 사이에 불일치가 생기면 어떻게 복구하나?
run_id -> snapshot_id mapping write가 실패하면 무엇이 source of truth인가?
```

현재 답:

| 항목 | 현재 답 | 상태 |
|---|---|---|
| single writer partition overwrite | local skeleton으로 검증 | Core-lite |
| concurrent writer handling | 구현하지 않음 | Backlog |
| catalog/Iceberg two-phase consistency | 구현하지 않음 | Backlog |

이 영역은 production에서는 중요하지만 현재 portfolio slice에서는 과장 금지.

### 5.9 Failure State / Retry / Recovery

목적: 실패를 숨기지 않고 조사 가능한 상태로 남긴다.

질문:

```text
run이 중간에 죽으면 어떤 partial state가 남는가?
quality fail run도 catalog에 남기는가?
failed run과 successful run을 어떻게 구분하나?
retry하면 같은 source_hash를 재사용하는가?
operator가 failure reason을 어디서 보는가?
failure-state forensics는 성공 run evidence와 같은 report로 볼 수 있는가?
```

현재 답:

| 항목 | 현재 답 | 상태 |
|---|---|---|
| successful run reuse | 같은 source_hash successful run 재사용 | 구현됨 |
| quality fail result | 일부 failure는 quality result로 드러남 | 일부 구현됨 |
| failed partial-state forensics | 중간 실패 상태 조사 report는 없음 | 구현하지 않음 |

다음 후보:

```text
실패 run catalog record를 남기는 slice
```

분류: Backlog.

### 5.10 Orchestration / Scheduling / Airflow

목적: business logic과 orchestration을 분리한다.

질문:

```text
Airflow DAG는 business logic을 갖는가, CLI를 호출만 하는가?
business_date/raw_path/output_dir/catalog_backend는 어떻게 전달하나?
retry는 Airflow가 담당하나, idempotency는 pipeline이 담당하나?
Airflow runtime import/trigger를 이 환경에서 검증할 수 있는가?
task를 bronze/silver/gold/quality/catalog로 쪼갤 기준은 무엇인가?
Spark/Iceberg skeleton을 Airflow가 trigger하는 것은 언제 Core가 되는가?
```

현재 답:

| 항목 | 현재 답 | 상태 |
|---|---|---|
| CLI command contract | `orchestration.py` + tests | 구현됨 |
| Airflow runtime trigger | Airflow 3.3.0 isolated venv + `dags test` | local runtime 검증됨 |
| Airflow-triggered Spark runtime | `manufacturing_iceberg_skeleton` + `dags test` | local runtime 검증됨 |

### 5.11 Observability / Operator Evidence

목적: 운영자가 raw를 열기 전에 상태를 좁힐 수 있어야 한다.

질문:

```text
run_id/source_hash/schema_hash/quality status는 어디서 보나?
row count와 conservation 결과는 어디서 보나?
lineage trace는 path-level인가 column-level인가?
report가 anomaly detection을 하는가, evidence를 묶어주는가?
logs/metrics/alerts는 있는가?
```

현재 답:

| 항목 | 현재 답 | 상태 |
|---|---|---|
| operator report | JSON catalog 기반 read-only report | 구현됨 |
| path-level lineage | gold -> silver -> bronze -> source trace | 구현됨 |
| column-level lineage | 컬럼 단위 변환 추적 없음 | 구현하지 않음 |
| metrics/alerts | SLI/SLO, delay alert, Prometheus 등 없음 | 구현하지 않음 |

### 5.12 Security / Privacy / Governance / Retention

목적: 공개 repo와 실제 운영 모두에서 민감정보/권한/보존 기간을 묻는다.

질문:

```text
repo에 secret/API key/path/non-public contact가 들어가지 않았나?
synthetic data임이 명확한가?
회사/고객/내부 schema가 섞이지 않았나?
PII column이 있다면 어떻게 분류하나?
권한/role은 필요한가?
dataset retention과 snapshot retention은 어떻게 정하나?
blog/resume에 private detail을 넣지 않았나?
```

현재 답:

| 항목 | 현재 답 | 상태 |
|---|---|---|
| synthetic data | README/blog에서 synthetic임을 명시 | 명시됨 |
| public secret scan | publication checklist로 수행 | 필요 시 재실행 |
| PII governance | synthetic data라 PII tagging/RBAC 없음 | 구현하지 않음 |
| retention policy | snapshot/data retention 자동화 없음 | 구현하지 않음 |

이 영역은 모든 public push 전에 Core gate다.

### 5.13 Performance / Scale / Cost

목적: toy input에서도 미래 확장 질문을 놓치지 않는다.

질문:

```text
현재 row 수에서는 Python으로 충분한가?
Spark가 필요한 threshold는 무엇인가?
small files 문제가 생길 수 있는가?
partition을 너무 많이 만들면 어떤 문제가 생기나?
count/check를 여러 번 하면 비용이 커지는가?
warehouse storage cost와 retention은 어떻게 관리하나?
```

현재 답:

| 항목 | 현재 답 | 상태 |
|---|---|---|
| toy local input | 현재 synthetic row 수는 Python으로 충분 | 구현상 Python 충분 |
| Spark scale proof | load test나 throughput benchmark 없음 | 구현하지 않음 |
| cost/retention | storage cost/retention 자동 정책 없음 | Backlog |

### 5.14 Testing / Local Reproducibility / CI

목적: claim을 테스트와 CLI evidence로 닫는다.

질문:

```text
순수 transform test와 IO/integration test를 분리했나?
환경 의존 테스트는 skip reason이 구체적인가?
CLI smoke가 있는가?
verification log에 명령과 결과가 남았나?
Spark/Iceberg jar/version 문제를 Test 0으로 잡았나?
```

현재 답:

| 항목 | 현재 답 | 상태 |
|---|---|---|
| pytest | 최신 수치는 `VERIFICATION_LOG.md` 확인 | verification log |
| Spark optional tests | optional dependency 설치 후 검증 기록 확인 | verification log |
| CLI smoke | lakehouse/EAV/operator/Spark CLI 기록 확인 | verification log |
| version pin | `slices/spark-iceberg-partition-overwrite/05-version-pin.md` | 문서화됨 |

### 5.15 Public Claim / Blog / Resume Boundary

목적: 구현보다 크게 말하지 않는다.

질문:

```text
이 claim은 code + test + verification log가 있는가?
walking skeleton을 production처럼 말하고 있지 않은가?
runtime unverified를 verified처럼 말하고 있지 않은가?
Spark/Iceberg skeleton과 full Spark pipeline을 구분했는가?
블로그가 decision pressure와 tradeoff를 설명하는가, 도구 홍보가 되었는가?
```

현재 답:

| claim | 허용 | 금지 |
|---|---|---|
| Spark/Iceberg | local single-gold-table walking skeleton | full Spark/Iceberg pipeline |
| Airflow | wrapper command contract + local `dags test` runtime verified, including Spark/Iceberg skeleton trigger | production scheduler/worker operated |
| Mongo | model + mongomock tests | real runtime verified |
| lineage | path-level evidence | column-level lineage system |

## 6. Spark/Iceberg 질문을 다시 구조화하면

기존 `slices/spark-iceberg-partition-overwrite/01-question-map.md`와 `slices/spark-iceberg-partition-overwrite/04-walking-skeleton-plan.md`의 질문은 아래처럼 읽으면 된다.

```text
Service question:
  정정 source가 오면 같은 business_date gold를 중복 없이 교체할 수 있는가?

Storage question:
  CSV run-folder 대신 Iceberg table/snapshot으로 current state를 표현할 수 있는가?

Write question:
  append / overwrite / merge 중 무엇인가?

Partition question:
  어떤 partition을 교체해야 하는가?

Distributed processing question:
  Spark DataFrame write가 local에서 실제로 실행되는가?

Version/runtime question:
  PySpark/Spark/Scala/Iceberg jar/Java 조합이 맞는가?

Evidence question:
  run_id와 snapshot_id를 구분해 남겼는가?

Claim question:
  이걸 full lakehouse라고 과장하지 않았는가?
```

현재 구현은 이 중 아래만 닫았다.

```text
local SparkSession
Iceberg catalog setup
single gold table DDL
business_date partition overwrite
D2 partition preservation
snapshot metadata read
run_id -> snapshot_id JSON evidence
same source_hash -> no new snapshot
```

아직 닫지 않은 것:

```text
full bronze/silver/gold Spark rewrite
Spark quality checks
schema evolution as Iceberg table op
MERGE/upsert
concurrent writer conflict
production Airflow-triggered Spark runtime
retention/expire snapshots
production rollback
```

## 7. 다음 slice를 고를 때 쓰는 짧은 템플릿

정식 템플릿은 [`slices/TEMPLATE.ko.md`](slices/TEMPLATE.ko.md)를 쓴다.

짧게 생각할 때는 아래만 먼저 채운다.

```text
Slice thesis:
  무엇을 만들고 싶은가?

Primary scenario:
  누가 어떤 상황에서 막히는가?

Question areas to pull:
  service / grain / schema / quality / rerun / storage / Spark /
  concurrency / failure / orchestration / observability / security /
  performance / testing / claim

Core questions:
  이번 코드/테이블/파일/계약을 바꾸는 질문만 적는다.

Backlog:
  중요하지만 이번에 하지 않는 질문을 명시한다.

Test contract:
  Core 질문이 참임을 무엇으로 증명할까?

Claim boundary:
  구현 후 어디까지 말할 수 있고, 무엇은 말하면 안 되는가?
```

## 8. 현재 우선순위 후보

우선순위는 "재미있어 보이는 기술"이 아니라 아래 기준으로 고른다.

| 기준 | 질문 |
|---|---|
| Claim gap closure | 지금 claim에서 가장 오해되거나 비어 있는 부분을 닫는가? |
| Resume/JD impact | 채용시장 언어로 설명 가능한 evidence가 생기는가? |
| Blog value | scenario -> pressure -> decision -> evidence로 글이 되는가? |
| Implementation risk | 작은 slice로 끝낼 수 있는가, 환경/버전 리스크가 큰가? |
| Learning value | 다음 설계 질문을 더 잘 이해하게 만드는가? |
| Evidence reuse | 이미 있는 code/test/catalog/log를 재사용해 닫을 수 있는가? |

추천 판단:

```text
높음 = claim gap을 닫고, 작게 구현/검증 가능하며, 블로그/이력서 evidence가 된다.
중간 = 중요하지만 별도 설계나 runtime 준비가 필요하다.
낮음 = 이름은 알아야 하지만 지금 구현하면 scope가 커진다.
```

현재 후보:

| 후보 | 관련 질문 영역 | 우선순위 근거 | 추천 |
|---|---|---|---|
| B5 블로그 audit/publish | claim / evidence / Spark/Iceberg | 구현 evidence가 이미 있고, full lakehouse가 아니라 walking skeleton이라는 claim boundary를 연습할 수 있다. | 높음 |
| Airflow runtime verification | orchestration / local reproducibility | 완료: wrapper contract와 local `dags test` runtime gap을 닫았다. 남은 것은 production scheduler/worker deployment다. | 완료 |
| failure-state forensics | failure / observability / catalog | 성공 run 중심 operator report의 다음 gap이다. 기존 catalog/report 구조를 재사용할 수 있다. | 중간 |
| Spark quality checks | distributed processing / quality / cost | JD impact는 있지만 full Spark port로 커질 위험이 크다. 별도 slice로 작게 잘라야 한다. | 낮음 |
| security/PII governance slice | governance / claim | 공개 repo gate로는 중요하지만 synthetic data라 product feature evidence는 작다. | 낮음 |
