# Schema Drift 의사결정 노트

상태: 같이 검토할 초안  
프로젝트: `manufacturing-data-platform-mini`

관련 문서/코드:

- [`README.md`](../../README.md)
- [`DESIGN.md`](../../DESIGN.md)
- [`src/manufacturing_data_platform/pipeline/lakehouse.py`](../../src/manufacturing_data_platform/pipeline/lakehouse.py)
- [`tests/test_lakehouse_pipeline.py`](../../tests/test_lakehouse_pipeline.py)

## 1. 시나리오

제조/로봇 이벤트 CSV가 매일 들어온다.

pipeline은 이 CSV를 다음 흐름으로 처리한다.

```text
raw CSV
-> bronze raw copy + source manifest
-> silver typed / normalized / deduplicated rows
-> gold daily line/product metrics
-> quality report
-> catalog / lineage run record
```

그런데 어느 날 source file의 컬럼 구조가 바뀐다.

기존 header:

```text
event_time,plant_id,line_id,work_order_id,machine_id,product_code,
operation,units_produced,defect_count,cycle_time_ms,business_date
```

새 header:

```text
event_time,plant_id,line_id,work_order_id,machine_id,product_code,
operation,units_produced,defect_count,cycle_time_ms,business_date,operator_id
```

즉 `operator_id`라는 새 컬럼이 추가됐다.

## 2. 문제

source schema가 바뀌었다. 시스템은 이 변화를 어떻게 처리할지 결정해야 한다.

위험:

- 그냥 무시하면 운영자는 source 구조가 바뀐 사실을 모른다.
- 새 컬럼이 생길 때마다 실패시키면 정상적인 schema evolution도 막힌다.
- 새 컬럼을 자동으로 silver/gold에 추가하면 downstream mart contract가 조용히 바뀔 수 있다.
- `schema_hash`를 고정된 required column 목록 기준으로만 만들면, 추가 컬럼이 감지되지 않는다.

마지막 문제는 이 프로젝트에서 실제로 한 번 발생했던 버그다. 예전에는 `schema_hash`가 고정 `REQUIRED_COLUMNS`에 너무 묶여 있어서 added column drift를 놓쳤다. 이후 `read_rows`가 실제 CSV header를 반환하고, 그 실제 header 기준으로 `schema_hash`를 만들도록 수정했다.

## 3. 선택지

| 선택지 | 장점 | 비용 / 위험 |
|---|---|---|
| drift 무시 | pipeline은 계속 돈다 | source 변화가 보이지 않는다 |
| 모든 drift를 fail | 강하게 보호한다 | 정상적인 컬럼 추가도 pipeline을 막는다 |
| warn으로 기록하고 계속 진행 | 변화가 보이고 pipeline도 계속 돈다 | warning을 사람이 보지 않으면 놓칠 수 있다 |
| silver/gold schema 자동 진화 | 새 필드를 빨리 쓸 수 있다 | mart contract가 조용히 바뀔 수 있다 |
| full schema registry / table format evolution | production에 가까운 강한 모델 | v0에는 너무 무겁다 |

## 4. 상용/OSS의 의사결정

참고할 수 있는 reference pattern:

- Iceberg/Delta 계열 table format은 schema evolution을 단순 파일 변화가 아니라 metadata로 관리한다.
- dbt / Great Expectations / Soda 같은 quality tool은 변화나 위반을 check result로 드러낸다.
- DataHub/OpenMetadata 같은 catalog system은 raw data를 열지 않고도 schema metadata를 볼 수 있게 한다.

여기서 뽑을 수 있는 일반 의사결정:

```text
schema change를 invisible하게 두지 않는다.
metadata/check result로 드러낸다.
그리고 gate policy를 정한다: warn, fail, drop/quarantine, evolve.
```

## 5. Tradeoff

이 프로젝트의 v0 결정은 `warn and continue`다.

| 얻는 것 | 비용 / tradeoff |
|---|---|
| added/removed column이 quality/catalog metadata에 보인다 | warning을 리뷰하지 않으면 지나칠 수 있다 |
| 정상적인 schema evolution이 daily run을 막지 않는다 | incompatible change는 별도 hard check가 필요하다 |
| full schema registry 없이 v0를 작게 유지한다 | 자동 downstream schema migration은 없다 |
| run record가 previous/current schema hash를 설명한다 | hash만으로는 어떤 컬럼이 바뀌었는지 바로 알기 어렵다 |

중요한 구분:

```text
schema_drift warn
= source shape이 previous successful run과 달라졌다

missing required column hard failure
= silver/gold contract를 만들 수 없다
```

## 6. Row / File / Record Trace

| 순간 | table/file/document | key fields | 예시 | 의미 |
|---|---|---|---|---|
| source 도착 | raw CSV | header | `...,business_date,operator_id` | source shape이 바뀜 |
| read | in-memory parse | `columns`, `rows` | `columns=["event_time", ..., "operator_id"]` | 실제 header를 capture |
| bronze | source manifest | `source_hash`, `schema_hash`, `business_date` | `schema_hash=abc...` | run의 source/schema identity |
| quality | quality check | `name`, `status`, `expected`, `actual` | `schema_drift`, `warn`, `old_hash`, `new_hash` | drift가 보이게 됨 |
| catalog/lineage | `lakehouse_runs` / JSON state | `schema_drift`, `previous_schema_hash`, `current_schema_hash` | policy=`warn` | 운영자가 inspect 가능 |

## 7. State Changes

```text
previous successful run 존재
-> 새 source file 도착
-> 실제 CSV header 읽음
-> actual header 기준 current schema_hash 계산
-> previous successful schema_hash 조회
-> schema_drift check 추가
-> policy=warn이면 run 계속 진행
-> run/lineage record에 previous/current schema hash와 status 저장
```

## 8. 살아남아야 하는 정보

다음 정보가 남아야 한다.

- `dataset_id`
- `business_date`
- `source_hash`
- current `schema_hash`
- previous successful `schema_hash`
- `schema_drift.status`
- `schema_drift.policy`
- quality check list
- run status
- input/output layer paths

이유:

- `source_hash`는 같은 파일 내용인지 알려준다.
- `schema_hash`는 파일 구조가 바뀌었는지 알려준다.
- previous successful hash는 비교 기준점이다.
- quality check status는 변화를 inspect 가능하게 만든다.
- policy는 왜 run이 실패하지 않았는지 설명한다.

## 9. Tables / Columns / Files

현재 구현은 Mongo backend와 JSON backend를 모두 지원한다.

Mongo path:

- `lakehouse_runs.dataset_id`
- `lakehouse_runs.business_date`
- `lakehouse_runs.source_hash`
- `lakehouse_runs.schema_hash`
- `lakehouse_runs.quality.checks`
- `lakehouse_runs.schema_drift.previous_schema_hash`
- `lakehouse_runs.schema_drift.current_schema_hash`
- `lakehouse_runs.schema_drift.status`
- `lakehouse_runs.schema_drift.policy`

JSON path:

- `_state/<dataset_id>/latest_successful_run.json`
- `_state/<dataset_id>/business_date=<date>.json`
- `quality_report.json`
- bronze/silver/gold output files under run directory

## 10. Functions / APIs

읽어볼 코드 포인트:

- `read_rows`
  - 실제 CSV header와 rows를 반환한다.
  - required column이 없으면 hard failure를 낸다.

- `hash_schema(infer_schema(columns, rows))`
  - 실제 header 기준으로 current `schema_hash`를 계산한다.

- `lookup_previous_schema_hash`
  - previous successful run의 schema hash를 가져온다.

- `build_schema_drift_check`
  - baseline/stable이면 `pass`
  - current hash가 다르고 policy가 warn이면 `warn`

- `run_lakehouse_pipeline`
  - read -> bronze -> silver -> gold -> quality -> catalog/lineage 흐름을 실행한다.

읽어볼 test:

- `test_schema_drift_helper_states`
- `test_schema_drift_warns_against_previous_successful_run`
- `test_schema_stable_when_schema_unchanged_across_dates`
- `test_schema_drift_warns_on_added_column`

## 11. 설계 판단

Copy:

- schema change는 보이게 해야 한다.
- schema identity는 metadata로 저장해야 한다.
- quality result는 review 가능한 형태여야 한다: `name`, `status`, `expected`, `actual`, `detail`.
- 비교 기준은 failed/partial run이 아니라 previous successful run이어야 한다.

Simplify:

- full schema registry 대신 `schema_hash`를 쓴다.
- full schema evolution workflow 대신 `warn` policy를 둔다.
- metadata platform 대신 Mongo/JSON에 run metadata를 남긴다.

Avoid for v0:

- downstream gold/mart schema 자동 migration
- real schema registry
- Iceberg/Delta table metadata 직접 구현
- 모든 source schema change를 fatal로 처리

## 12. My Project v0

현재 local contract:

```text
첫 successful run이면:
  schema_drift = pass
  detail = baseline schema established

current schema_hash == previous successful schema_hash이면:
  schema_drift = pass
  detail = schema stable

current schema_hash != previous successful schema_hash이면:
  policy=warn일 때 schema_drift = warn
  quality_passed는 true 유지
  run/lineage에 previous/current schema hash 저장

required column이 없으면:
  read_rows가 ValueError 발생
  pipeline은 silver/gold를 만들 수 없음
```

## 13. Test Contract

이미 있는 test:

| Test | Given | When | Then |
|---|---|---|---|
| baseline | previous successful run 없음 | pipeline 실행 | `schema_drift=pass` |
| stable | 같은 schema의 previous successful run 있음 | 다른 날짜/source content로 실행 | `schema_drift=pass` |
| drift | previous successful run과 다른 schema hash | pipeline 실행 | `schema_drift=warn`, run은 계속 성공 |
| added column | baseline header 존재 | 다음 file에 `operator_id` 추가 | `schema_drift=warn`, quality는 계속 pass |

열어둔 test 질문:

| 질문 | 왜 중요한가 |
|---|---|
| missing required column도 lakehouse path에서 명시적 test로 있어야 하나? | 지금은 `read_rows`에서 hard failure가 나는데, contract가 더 선명해질 수 있다 |
| warning에 hash뿐 아니라 added/removed column names도 넣어야 하나? | hash는 차이를 증명하지만, column name은 운영자 debugging에 더 좋다 |

## 14. Claim Boundary

정직하게 말할 수 있는 것:

```text
lakehouse slice는 실제 CSV header에서 schema hash를 만들고,
previous successful run과 비교해 schema drift를 감지한다.
drift는 quality warning으로 남고, run/lineage record에 저장된다.
```

말하면 안 되는 것:

```text
full schema registry
automatic schema migration
production Iceberg/Delta schema evolution
column-level semantic compatibility checking
```

## 15. 면접 답변

30-60초 버전:

> 저는 schema drift를 단순 parsing 문제가 아니라 운영 문제로 봤습니다. source CSV에 새 컬럼이 추가됐을 때 그냥 무시하면 변화가 보이지 않고, 반대로 모든 추가 컬럼을 실패시키면 정상적인 schema evolution도 막힙니다. 그래서 v0에서는 실제 CSV header 기준으로 `schema_hash`를 만들고, previous successful run과 비교해 `schema_drift` quality check를 남겼습니다. added-column drift는 `warn`이라 run은 계속되지만 catalog/run record에 증거가 남습니다. 반면 required column이 없으면 silver/gold contract를 만들 수 없기 때문에 hard failure로 둡니다.

## 16. 다음에 같이 볼 질문

바로 코드 수정하지 말고, 먼저 아래 질문을 같이 검토한다.

1. added column은 `warn and continue`가 맞나?
2. removed optional column도 `warn`인가, 일부 제거는 `fail`인가?
3. `schema_drift` check가 hash만 보여줘도 충분한가, column names도 보여줘야 하나?
4. missing required column은 `ValueError`로 터뜨릴지, structured quality failure로 남길지?

