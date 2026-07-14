# 07. 외부 Benchmark 기준 Named Backlog 영역

상위 문서: [`../08-area-question-bank.ko.md`](../08-area-question-bank.ko.md)

이 문서는 현재 구현하자는 뜻이 아니다.

목적:

```text
DataHub / OpenMetadata / OpenLineage / dbt / Airflow / Spark / Iceberg /
Databricks 같은 외부 기준에서 빠지기 쉬운 질문 영역을 이름 붙여 둔다.
```

이름을 붙여두면 면접이나 다음 slice에서 "몰라서 빠진 것"과 "알고도 scope 밖으로 둔 것"을 구분할 수 있다.

## 1. Source Integration / Harmonization / Mapping

왜 필요한가:

```text
실제 데이터 플랫폼은 한 가지 CSV만 받지 않는다.
여러 source의 컬럼명, 단위, 코드 체계를 표준 모델로 맞춰야 한다.
```

질문:

```text
새 파일 형식을 code 변경 없이 온보딩할 수 있는가?
mapping config의 필수 항목은 무엇인가?
단위 변환은 어디에서 정의하나?
mapping coverage 실패는 quality fail인가 warn인가?
source별 column naming 차이를 표준 attribute로 어떻게 맞추나?
```

현재 프로젝트 연결:

```text
EAV slice가 이미 이 영역의 작은 proof다.
하지만 question-bank에는 named area가 없었으므로 여기서 명시한다.
```

분류:

```text
Core evidence exists for EAV mini.
Future: richer source onboarding contract는 Backlog.
```

## 2. Code / Logic Version Identity

왜 필요한가:

```text
같은 source_hash와 schema_hash라도 transform code가 바뀌면 결과가 달라질 수 있다.
재현성은 데이터 identity + schema identity + logic identity가 함께 있어야 강하다.
```

질문:

```text
run record에 pipeline_version이나 git commit을 남기는가?
같은 데이터·같은 schema인데 transform code가 바뀌면 새 dataset version인가?
blog/resume에서 reproducible이라고 말하려면 code version evidence가 필요한가?
OpenLineage job facet/dbt model version 같은 외부 개념과 어떻게 연결되나?
```

선택지:

```text
not tracked:
  현재 상태. source/schema 중심 재현성만 말한다.

manual pipeline_version:
  작고 싸다. run record에 문자열 하나를 남긴다.

git commit/package version:
  더 강하지만 packaging/CI와 연결해야 한다.
```

분류:

```text
Candidate small Core slice.
다른 backlog보다 구현 비용이 작고 재현성 claim의 실제 구멍을 막는다.
```

## 3. Dimensional Modeling / SCD / Reference Data

왜 필요한가:

```text
gold metric만 있으면 끝이 아니다.
plant/product/line 같은 기준 정보가 시간에 따라 바뀔 수 있다.
accepted_values도 누가 소유하고 버전 관리하는 reference data인지 질문이 생긴다.
```

질문:

```text
gold는 fact table인가, 그냥 aggregate file인가?
plant/product/line 속성은 dimension으로 관리하는가?
plant/product 속성이 바뀌면 SCD Type 1/2 중 무엇인가?
accepted operation 목록은 code 상수인가, reference table인가?
reference data 변경은 quality result와 어떻게 연결되는가?
```

분류:

```text
Backlog.
면접 modeling 질문에는 중요하지만 현재 slice 구현 범위는 아니다.
```

## 4. Downstream Impact / Ownership / Exposures

왜 필요한가:

```text
lineage는 upstream만 보는 것이 아니다.
내가 gold를 바꾸면 어떤 dashboard/model/user가 영향을 받는지도 알아야 한다.
```

질문:

```text
gold dataset의 owner는 누구인가?
누가 이 gold를 소비하는가?
gold schema나 metric definition을 바꾸면 어떤 downstream asset이 깨지는가?
dbt exposures처럼 dashboard/model 소비자를 기록할 필요가 있는가?
impact analysis를 path-level lineage로 할 수 있는가, 별도 catalog가 필요한가?
```

분류:

```text
Backlog.
현재 synthetic project에는 real downstream consumer가 없으므로 named question으로만 둔다.
```

## 5. Streaming / CDC

왜 필요한가:

```text
현재 프로젝트는 batch 중심이다.
하지만 데이터 플랫폼 질문 은행이라면 streaming/CDC 질문의 이름은 있어야 한다.
```

질문:

```text
business_date는 event-time인가 processing-time인가?
late event는 어느 business_date로 귀속되는가?
watermark는 필요한가?
exactly-once를 claim하려면 어떤 source/sink contract가 필요한가?
CDC update/delete는 append와 어떻게 다르게 처리하는가?
Kafka/Flink/Spark Structured Streaming 중 무엇이 문제에 맞는가?
```

분류:

```text
Kafka ingestion은 별도 scenario/question map/slice로 discovery를 시작했다.
아직 design-only이며 Kafka code/runtime evidence는 없다.
Flink/CDC와 Spark Structured Streaming 구현은 계속 named Backlog다.
```

활성화된 설계 문서:

- [`../scenarios/03-kafka-machine-event-ingestion.md`](../scenarios/03-kafka-machine-event-ingestion.md)
- [`08-kafka-streaming-ingestion.ko.md`](08-kafka-streaming-ingestion.ko.md)
- [`../slices/05-kafka-raw-ingestion.ko.md`](../slices/05-kafka-raw-ingestion.ko.md)

## 6. Metric / Semantic Definition

왜 필요한가:

```text
defect_rate 같은 metric은 계산식과 grain이 명확해야 한다.
같은 이름의 metric을 팀마다 다르게 계산하면 gold를 믿기 어렵다.
```

질문:

```text
defect_rate = defect_count / units_produced 정의는 어디에 있는가?
metric definition도 versioning하는가?
metric의 grain은 gold row grain과 일치하는가?
closing_status 같은 business rule은 어디서 정의되는가?
semantic layer/dbt metrics 같은 외부 개념과 연결할 필요가 있는가?
```

분류:

```text
Backlog.
현재는 README/code로 설명 가능한 수준. 별도 semantic layer 구현은 하지 않는다.
```

## 7. What Not To Build Yet

아래는 이름은 붙여두되 지금 구현하지 않는다.

```text
streaming / CDC / Flink
Kafka runtime/code before the Kafka slice audit and Core cut
PII/RBAC/governance stack
column-level lineage system
dbt exposures / semantic layer tool
full dimensional/SCD implementation
metrics/Prometheus/alert stack
production retention/rollback system
```

질문 은행의 목적은 scope를 키우는 것이 아니라, 나중에 필요해질 질문을 잊지 않는 것이다.
