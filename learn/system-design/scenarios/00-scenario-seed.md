# 01. Scenario seed — 시스템 시나리오

상태: 같이 검토할 초안  
프로젝트: `manufacturing-data-platform-mini`

이 문서는 `schema drift`, `idempotency`, `quality`, `catalog`, `lineage` 같은 개별 기능으로 들어가기 전에, 질문을 만들기 위한 **scenario seed**를 정리한다.

중요한 점: 시나리오는 하나로 고정되지 않는다. 이 문서는 최종 결론이 아니라 question map의 재료다. 앞으로 Slice가 늘어나면 `late-arriving correction`, `bad run rollback`, `schema evolution`, `operator debugging` 같은 시나리오를 계속 추가할 수 있다.

## 1. 한 줄 시스템

`manufacturing-data-platform-mini`는 synthetic 제조 스타일 이벤트 CSV를 받아서, 재현 가능하고 검증 가능한 데이터셋과 mart로 바꾸는 작은 데이터 플랫폼이다.

```text
source CSV
-> raw 보존
-> 정제된 표준 데이터
-> business metric mart
-> quality result
-> catalog / version / lineage record
```

## 2. 사용자와 운영자

이 시스템을 쓰는 사람을 나눠보면 세 부류가 있다.

| 역할 | 알고 싶은 것 |
|---|---|
| 데이터 사용자 / 분석가 | 어떤 데이터셋이 있고, 어떤 컬럼이 있고, 어떤 기간을 믿고 쓸 수 있는가? |
| ML / 로봇 데이터 사용자 | 어떤 source로 만든 dataset version인지, 다시 재현 가능한가? |
| 운영자 / 데이터 엔지니어 | run이 성공했는가, 실패했는가, 품질은 통과했는가, 문제가 생기면 어디서 생겼는가? |

이 시스템은 단순히 CSV를 읽어 gold table을 만드는 것이 아니라, 이 사람들이 나중에 질문할 정보를 남기는 것이 목표다.

## 2.1 서비스가 필요한 이유

이 프로젝트의 서비스 이유는 "로봇 데이터를 처리한다"보다 더 구체적이어야 한다.

```text
로봇/제조/ML 데이터 팀은 raw file만 보고는
어떤 데이터셋을 믿고 분석/학습/리포팅에 써도 되는지 판단하기 어렵다.
```

raw file만 있으면 사용자는 매번 같은 질문을 다시 해야 한다.

```text
이 파일은 전에 처리한 것과 같은가?
이 데이터셋은 어떤 schema인가?
이 날짜 결과는 어느 source에서 왔는가?
품질검사는 통과했는가?
schema가 바뀌었는데도 조용히 지나간 건 아닌가?
같은 날짜를 다시 돌렸을 때 중복이 생기지 않았는가?
```

그래서 이 mini service는 raw file을 바로 "쓸 수 있는 데이터"라고 주장하지 않는다.

대신 아래 상태를 만들어 사용자가 판단할 수 있게 한다.

```text
source identity
schema identity
bronze/silver/gold 상태
quality result
catalog/version metadata
lineage/run evidence
idempotent rerun evidence
```

즉 서비스의 핵심 가치는:

> raw manufacturing-style/tabular files를 분석/ML 사용자가 믿고 쓸 수 있는 cataloged, versioned, quality-checked dataset/mart로 바꾸고, 운영자가 나중에 설명할 수 있는 증거를 남기는 것.

## 2.2 Primary Service Scenario

이 프로젝트의 대표 시나리오는 아래 하나로 잡는다.

```text
로봇/제조 이벤트 파일이 하루 단위로 들어온다.
데이터 엔지니어는 이 파일을 처리해 daily metric mart를 만들고 싶다.
분석가/ML 사용자는 결과 숫자를 쓰기 전에 데이터셋의 schema, freshness, quality, source version을 알고 싶다.
운영자는 같은 날짜를 다시 처리해도 중복되지 않고, 문제가 생기면 어느 source/run에서 왔는지 추적하고 싶다.
```

시간 순서로 쓰면:

```text
1. Source owner가 synthetic manufacturing-style CSV를 제공한다.
2. Pipeline이 source_hash와 schema_hash를 계산한다.
3. Pipeline이 bronze raw copy와 manifest를 남긴다.
4. Pipeline이 silver에서 business_date 필터링, type 정리, natural key dedup을 수행한다.
5. Pipeline이 gold daily metrics를 만든다.
6. Quality suite가 row reconciliation, conservation, not_null, unique, accepted_values, range, freshness, schema_drift를 확인한다.
7. Catalog/lineage record가 run_id, source_hash, schema_hash, quality result, layer parent links를 저장한다.
8. 사용자는 catalog/run record를 보고 데이터셋을 사용할지 판단한다.
9. 같은 source가 다시 들어오면 idempotency gate가 기존 successful run을 재사용한다.
```

이 시나리오가 있기 때문에 개별 기능이 생긴다.

| 기능 | 서비스 질문 |
|---|---|
| `source_hash` | 이 입력은 전에 처리한 것과 같은가? |
| `schema_hash` / `schema_drift` | source 구조가 바뀌었는가? |
| bronze/silver/gold | raw 보존, 정제, mart를 어디서 나누는가? |
| quality checks | 결과를 믿어도 되는 근거는 무엇인가? |
| catalog/version | 데이터를 열지 않고 무엇을 알 수 있어야 하는가? |
| lineage/run record | 이 숫자는 어떤 source/run에서 왔는가? |
| idempotency | 같은 입력 재실행이 중복을 만들지 않는가? |

## 3. 들어오는 source

v0의 source는 synthetic manufacturing CSV다.

예상 row:

```text
event_time,plant_id,line_id,work_order_id,machine_id,product_code,
operation,units_produced,defect_count,cycle_time_ms,business_date
```

예시:

```text
2026-06-29T08:00:00Z,plant-a,line-1,wo-1,mc-1,gearbox-a,
assembly,10,1,100,2026-06-29
```

이 source는 실제 회사 데이터가 아니다. 목적은 domain realism이 아니라 데이터 플랫폼의 상태와 의사결정을 작게 연습하는 것이다.

## 4. 파일만 쌓으면 생기는 문제

raw CSV만 폴더에 쌓으면 처음에는 쉬워 보인다.

```text
data/raw/2026-06-29.csv
data/raw/2026-06-30.csv
```

하지만 운영 관점에서는 바로 문제가 생긴다.

| 문제 | 왜 위험한가 |
|---|---|
| 어떤 파일이 같은 입력인지 모름 | 파일명은 바뀔 수 있고, 같은 파일을 다시 올릴 수 있다 |
| 재실행하면 중복될 수 있음 | append만 하면 같은 날짜 결과가 두 번 들어갈 수 있다 |
| source schema가 바뀌어도 모를 수 있음 | 새 컬럼/삭제 컬럼이 조용히 지나가면 downstream이 불안정해진다 |
| raw와 mart 사이에서 row가 줄어도 이유를 모름 | 정상 필터링인지 데이터 유실인지 설명할 수 없다 |
| mart 값이 이상할 때 원인을 추적하기 어려움 | 어떤 source/run/code가 만들었는지 모르면 debugging이 어렵다 |
| 데이터셋 버전을 재현하기 어려움 | 어떤 source hash와 schema로 만들었는지 남지 않으면 재현성이 약하다 |

그래서 이 시스템은 파일 변환기가 아니라, **상태와 근거를 남기는 작은 platform**이어야 한다.

## 5. 필요한 큰 흐름

최소한 이런 흐름이 필요하다.

```text
1. source CSV가 들어온다.
2. source identity를 계산한다.
3. raw/bronze 상태로 원본과 manifest를 보존한다.
4. silver에서 타입 정리, 날짜 필터링, 중복 제거를 한다.
5. gold에서 business grain의 metric mart를 만든다.
6. quality check로 boundary가 지켜졌는지 확인한다.
7. catalog/run/lineage record에 무엇이 일어났는지 남긴다.
8. 같은 입력 재실행은 안전하게 skip하거나 재사용한다.
```

## 6. Core States

이 시스템에서 중요한 state는 다음과 같다.

| State | 의미 | 왜 필요한가 |
|---|---|---|
| source file | 들어온 원본 CSV | 처리의 시작점 |
| `source_hash` | 파일 내용 identity | 같은 입력인지 판단, idempotency |
| `schema_hash` | 파일 구조 identity | schema drift 감지 |
| bronze | raw copy + manifest | 원본 보존, replay 가능성 |
| silver | 정제/타입변환/중복제거된 row | 분석 가능한 공통 재료 |
| gold | daily line/product metric | 사용자가 보는 mart |
| quality checks | expected/actual/status | publish 가능 여부와 근거 |
| run record | run_id, status, input/output | 성공/실패 inspect |
| lineage record | input -> output 관계 | 원인 추적, 영향 분석 |
| dataset/catalog record | dataset/version metadata | 데이터를 열지 않고도 구조 파악 |

## 7. 전체 상태 변화

```text
source CSV arrives
-> source_hash/schema_hash computed
-> bronze manifest written
-> silver rows created
-> gold mart rows created
-> quality checks generated
-> run status decided
-> catalog/lineage records written
-> latest successful run pointer updated
```

실패가 생기면 중요한 것은 "실패했다"뿐 아니라, 어디까지 state가 만들어졌고 무엇을 보고 복구할 수 있는지다.

## 8. 이 시스템의 핵심 의사결정들

이 시스템을 공부할 때 하나씩 분해할 decision들:

| Decision | 질문 |
|---|---|
| Bronze/Silver/Gold | 왜 raw, clean, mart를 나누는가? |
| Grain | gold mart의 한 row는 무엇을 의미하는가? |
| Schema drift | source 컬럼 변화는 warn, fail, evolve 중 무엇인가? |
| Idempotency | 같은 source를 다시 돌리면 append, overwrite, skip 중 무엇인가? |
| Quality reconciliation | row count 감소가 정상 필터링인지 유실인지 어떻게 구분하는가? |
| Catalog | 데이터를 열지 않고 무엇을 알 수 있어야 하는가? |
| Lineage | 어떤 input이 어떤 output을 만들었는지 어디까지 남겨야 하는가? |
| Orchestration | Airflow/Dagster에는 무엇을 두고, business logic은 어디에 둬야 하는가? |

## 9. v0에서 일부러 작게 잡은 것

이 시스템은 production lakehouse를 만들지 않는다.

v0에서 작게 잡은 것:

- single-machine Python/CSV
- synthetic data
- Mongo 또는 JSON metadata backend
- simple quality check shape
- table-level/path-level lineage
- `schema_hash` 기반 drift detection
- `source_hash` 기반 idempotency

v0에서 피한 것:

- full Spark/Iceberg platform runtime
- Kafka/Flink streaming
- full schema registry
- governance/RBAC
- lineage graph backend
- UI
- production scheduler 운영

작게 잡는 이유는 기능을 못해서가 아니라, 먼저 핵심 의사결정을 검증하기 위해서다.

## 10. 이 문서 다음에 볼 것

이 문서를 이해한 뒤에는 개별 의사결정으로 내려간다.

먼저 source contract를 본다.

- [`../source-contracts/01-manufacturing-csv.md`](../source-contracts/01-manufacturing-csv.md)
- [`../../reference-decisions/gold-grain.md`](../../reference-decisions/gold-grain.md)

첫 번째 decision note:

- [`../../reference-decisions/schema-drift.md`](../../reference-decisions/schema-drift.md)

다음으로 만들 후보:

- `idempotency.md`
- `quality-reconciliation.md`
- `bronze-silver-gold.md`
- `catalog-lineage.md`

## 11. 같이 볼 질문

이 문서는 아직 초안이다. 다음 질문을 먼저 같이 검토한다.

1. 이 시스템의 사용자는 분석가/ML 사용자/운영자로 나누는 게 맞나?
2. v0 source를 manufacturing CSV로 두는 게 충분히 좋은 연습 시나리오인가?
3. 이 시스템의 핵심은 "mart 생성"인가, "상태와 근거를 남기는 platform"인가?
4. gold mart의 grain을 먼저 더 자세히 잡아야 하나?
5. `catalog`와 `lineage`를 이 v0에서 어디까지 분리해서 봐야 하나?

## 12. 이 문서를 어떻게 쓸 것인가

이 문서는 혼자 결론을 내리는 문서가 아니다. 사용 순서는 아래와 같다.

```text
1. 이 문서에서 scenario seed를 잡는다.
2. question map에서 그 시나리오가 만드는 질문을 넓게 펼친다.
3. state trace 문서에서 질문이 실제 데이터 상태 전이 어디에서 생기는지 확인한다.
4. reference decision note에서 질문 하나를 선택해 결정으로 수렴한다.
5. test contract와 구현으로 검증한다.
```

예:

```text
scenario:
  같은 business_date를 다시 처리한다.

question map:
  append / overwrite / merge 중 무엇인가?
  source_hash는 여전히 idempotency key인가?
  overwrite 전 결과는 snapshot으로 남는가?

state trace:
  Slice1의 skip run이 Slice2에서 Iceberg overwrite + snapshot으로 어떻게 바뀌는가?

decision:
  v0는 partition overwrite를 선택하고 MERGE는 backlog로 둔다.
```
