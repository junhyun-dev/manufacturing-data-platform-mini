# 01. Source Contract — 무엇을 받는가?

상태: 같이 검토할 초안  
프로젝트: `robot-data-platform-mini`

이 문서는 시스템의 첫 입력을 정리한다.  
`event를 받는다`라고만 말하면 모호하므로, 이 프로젝트에서는 정확히 어떤 형태의 event/source를 받는지 먼저 고정한다.

## 1. 결론부터

이 프로젝트는 streaming event를 하나씩 받는 시스템이 아니다.

현재 v0는 다음을 받는다.

```text
정형화된 manufacturing/robot event row들이 들어있는 batch CSV file
```

즉 입력 단위는 event 1개가 아니라 **CSV 파일 1개**다.  
그 CSV 안에 여러 event row가 들어 있다.

```text
input unit  = source CSV file
row unit    = manufacturing robot event
run unit    = business_date 기준 pipeline run
```

## 2. Source file 예시

파일 예:

```text
data/raw/manufacturing_robot_events.csv
```

header:

```text
event_time,plant_id,line_id,work_order_id,robot_id,product_code,
operation,units_produced,defect_count,cycle_time_ms,business_date
```

row 예시:

```text
2026-06-29T08:00:00Z,plant-a,line-1,wo-1001,rb-101,gearbox-a,
assembly,120,2,840,2026-06-29
```

## 3. 컬럼 의미

| Column | 의미 | 예시 | 설계상 역할 |
|---|---|---|---|
| `event_time` | 이벤트 발생 시각 | `2026-06-29T08:00:00Z` | natural key 일부, 시간 정보 |
| `plant_id` | 공장/사이트 ID | `plant-a` | gold grouping 차원 |
| `line_id` | 생산 라인 ID | `line-1` | gold grouping 차원 |
| `work_order_id` | 작업 지시 ID | `wo-1001` | natural key 일부 |
| `robot_id` | 로봇 ID | `rb-101` | natural key 일부 |
| `product_code` | 제품 코드 | `gearbox-a` | gold grouping 차원 |
| `operation` | 작업 종류 | `assembly` | accepted_values 품질 검사 대상 |
| `units_produced` | 생산 수량 | `120` | additive metric |
| `defect_count` | 불량 수량 | `2` | additive metric |
| `cycle_time_ms` | cycle time | `840` | 평균 metric 계산 대상 |
| `business_date` | 처리/집계 기준 날짜 | `2026-06-29` | partition/run 기준 |

## 4. 이 source는 얼마나 정형화되어 있나?

v0 source는 꽤 정형화되어 있다.

필수 컬럼 목록이 코드에 고정되어 있다.

```text
event_time
plant_id
line_id
work_order_id
robot_id
product_code
operation
units_produced
defect_count
cycle_time_ms
business_date
```

즉 이 프로젝트의 lakehouse slice는 다음을 가정한다.

```text
CSV header가 있고,
required columns가 존재하고,
각 row는 manufacturing event를 나타내며,
numeric field는 int로 변환 가능해야 한다.
```

다만 source는 완전히 깨끗하다고 가정하지 않는다.

예상하는 문제:

- 같은 natural key row가 중복될 수 있다.
- 다른 `business_date` row가 같은 파일에 섞일 수 있다.
- `operation`에 허용되지 않은 값이 들어올 수 있다.
- numeric range가 이상할 수 있다.
- required column 값이 비어 있을 수 있다.
- source header에 새 컬럼이 추가될 수 있다.

## 5. Row grain

source row 하나는 다음 의미를 가진다.

```text
특정 시각(event_time)에
특정 작업지시(work_order_id)를
특정 로봇(robot_id)이
특정 제품(product_code)에 대해 수행한
제조/로봇 작업 이벤트
```

현재 silver dedup 기준, 즉 natural key는:

```text
work_order_id + robot_id + event_time
```

이 말은 같은 `work_order_id`, `robot_id`, `event_time` 조합이 두 번 나오면 같은 event가 중복으로 들어온 것으로 본다는 뜻이다.

## 6. Batch CSV와 streaming event의 차이

이 프로젝트에서는 event row라는 말을 쓰지만, ingestion 방식은 streaming이 아니다.

| 구분 | 현재 v0 |
|---|---|
| 입력 방식 | batch CSV file |
| 처리 단위 | file/run |
| event order 보장 | 다루지 않음 |
| Kafka offset/checkpoint | 없음 |
| late event 처리 | backlog |
| idempotency 기준 | `dataset_id + business_date + source_hash` |

그래서 지금 공부 대상은 Kafka/Flink의 streaming semantics가 아니라:

```text
batch file이 들어왔을 때
그 안의 event rows를 어떻게 정제하고
어떤 상태와 metadata를 남길 것인가
```

이다.

## 7. 파일은 곧 테이블인가?

아니다. 이 프로젝트의 v0에서는 단순화를 위해:

```text
CSV file 1개 = header 1개 + table 1개
```

로 본다.

하지만 실제 현업 source file은 더 복잡할 수 있다.

예를 들어 Excel은 보통 이렇게 생긴다.

```text
workbook file
-> sheet 여러 개
-> sheet 안의 table/range 여러 개
-> header row
-> data rows
```

즉 현실적인 source 계층은 이렇게 봐야 한다.

```text
file
-> sheet
-> table/range
-> row
-> cell
```

예:

```text
production_report_2026-06-29.xlsx
├── Summary sheet
│   └── KPI summary table
├── Line A sheet
│   ├── production events table
│   └── defect detail table
└── Line B sheet
    └── production events table
```

이 경우 `source_hash`만으로는 부족할 수 있다. 아래 identity가 더 필요하다.

| Level | Identity example | 왜 필요한가 |
|---|---|---|
| file | `source_file_id = hash(file bytes)` | 같은 파일인지 판단 |
| sheet | `sheet_name = "Line A"` | 어떤 sheet를 읽었는지 판단 |
| table/range | `table_name` 또는 `cell_range = A5:K200` | sheet 안의 어떤 표인지 판단 |
| schema | header columns hash | 표 구조 변화 감지 |
| row | natural key | 중복 row 판단 |

그래서 더 일반적인 source contract는 이렇게 된다.

```text
source file arrives
-> detect sheets
-> select expected sheet/table/range
-> parse header and rows
-> compute file/table/schema identity
-> normalize into one or more bronze/silver tables
```

이 프로젝트 v0는 그중 가장 작은 경우만 구현한다.

```text
single CSV file
-> single table
-> single header
-> event rows
```

따라서 현재 v0의 정확한 claim은:

```text
multi-sheet Excel parser를 만든 것이 아니라,
정형 CSV table을 입력으로 받아 platform state와 metadata를 남기는 흐름을 연습한다.
```

나중에 Excel/multi-table source를 다루려면 새 decision note가 필요하다.

후보 decision:

```text
Excel workbook에서 어떤 sheet/table을 source table로 식별할 것인가?
sheet name이 바뀌면 fail인가 mapping인가?
table range가 밀리면 어떻게 감지할 것인가?
한 파일 안의 여러 table은 하나의 dataset인가 여러 dataset인가?
file_hash와 table_hash를 둘 다 둘 것인가?
```

## 8. 왜 이 정도로 정형화된 source에서 시작하나?

처음부터 복잡한 semi-structured log나 streaming event로 시작하면, 핵심 의사결정이 흐려진다.

v0에서는 source를 정형화해서 다음을 먼저 연습한다.

- source identity: `source_hash`
- schema identity: `schema_hash`
- bronze/silver/gold boundary
- natural key dedup
- quality check
- reconciliation
- idempotent rerun
- catalog/lineage record

즉 source를 단순하게 둔 이유는 설계가 단순해서가 아니라, **플랫폼 의사결정의 기본 뼈대를 보기 위해서**다.

## 9. Source에서 바로 파생되는 의사결정

이 source contract를 정하면 다음 질문들이 생긴다.

| 질문 | 연결되는 decision |
|---|---|
| 같은 event를 어떻게 알아보나? | natural key / dedup |
| 같은 파일을 다시 돌리면 어떻게 하나? | `source_hash` / idempotency |
| 새 컬럼이 추가되면 어떻게 하나? | schema drift |
| 다른 날짜 row가 섞이면 어떻게 하나? | business_date filter / freshness |
| row count가 줄면 유실인가 정상 처리인가? | reconciliation |
| 생산 수량과 불량 수량은 어떻게 검증하나? | numeric range / unit conservation |
| 어떤 단위로 mart를 만들 것인가? | gold grain |
| 한 파일에 sheet/table이 여러 개면 어떻게 하나? | file/sheet/table identity |

## 10. 이 source contract의 한계

정직한 한계:

- 실제 로봇 sensor stream이 아니다.
- ROS bag / MCAP / binary log가 아니다.
- Kafka topic이 아니다.
- multi-sheet Excel workbook parser가 아니다.
- 한 파일 안의 여러 table/range를 자동 식별하지 않는다.
- event ordering, watermark, late event를 다루지 않는다.
- numeric parsing 실패는 manufacturing slice에서는 graceful quarantine이 아니라 fail-fast에 가깝다.

이 한계는 나쁜 것이 아니라 v0 scope boundary다.

## 11. 같이 볼 질문

다음 질문을 같이 검토한다.

1. 이 source row의 grain을 `work_order_id + robot_id + event_time`으로 보는 게 자연스러운가?
2. `business_date`는 source에 들어오는 값으로 둘까, file path/run argument에서 오는 값으로 둘까?
3. 이 source는 “robot data platform”이라고 부르기에 충분한가, 아니면 “manufacturing event platform”이라고 더 정확히 불러야 하나?
4. 지금 v0에서 streaming을 일부러 제외했다는 점을 README/학습 노트에서 더 강하게 말해야 하나?
5. source contract를 먼저 고정하면, 다음 decision은 natural key/dedup을 보는 게 맞나?
6. Excel/multi-sheet source를 별도 학습 시나리오로 추가할 필요가 있나?
