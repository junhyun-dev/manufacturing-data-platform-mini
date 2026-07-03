# 시나리오 -> 상태 변화 지도

이 문서는 코드 읽기 루프를 `robot-data-platform-mini`에 적용한 학습용 지도다.

```text
시나리오 -> 상태 변화 -> 필요한 정보 -> 테이블/컬럼/파일 -> 함수/API
```

목표는 모든 함수를 외우는 것이 아니다. 각 파이프라인 단계가 끝났을 때
무슨 상태가 남아야 하는지 이해하는 것이다. 그래야 나중에 재실행, 장애,
리뷰, 면접 상황에서 아래 질문에 답할 수 있다.

```text
무슨 입력으로 실행됐나?
어디까지 처리됐나?
어떤 품질 검사를 통과/실패했나?
같은 입력을 다시 돌려도 안전한가?
이 gold 결과는 어떤 source/bronze/silver에서 왔나?
```

## 한 줄 시스템

`robot-data-platform-mini`는 작은 데이터 플랫폼 slice다.

```text
source files
-> reproducible ingest/version metadata
-> transformed outputs
-> quality report
-> catalog/lineage record
```

현재 세 가지 흐름이 있다.

- Phase 1 catalog loop: CSV 1개 -> Mongo `datasets` / `dataset_versions`
- Phase 2 medallion loop: 제조 CSV 1개 -> bronze/silver/gold -> quality -> lineage
- EAV mini loop: 여러 wide 양식 -> mapping config -> EAV long -> gold mart -> quality -> lineage

## 핵심 상태 객체

| 상태 객체 | 위치 | 답하는 질문 |
|---|---|---|
| `datasets` | Mongo | 어떤 데이터셋이 있고 최신 schema/version은 무엇인가? |
| `dataset_versions` | Mongo | 어떤 source file version이 어떤 hash/stats로 ingest됐는가? |
| bronze file | filesystem | 이번 run이 보존한 원본 파일은 정확히 무엇인가? |
| manifest | filesystem | 어떤 source_hash/schema_hash/business_date가 이 run을 만들었나? |
| silver file | filesystem | transform 후 typed/normalized 중간 데이터는 무엇인가? |
| EAV file | filesystem | wide 파일에서 추출된 entity/attribute/value fact는 무엇인가? |
| gold file | filesystem | 사용자/면접에서 설명 가능한 최종 metric table은 무엇인가? |
| quality report | filesystem + Mongo doc | 어떤 assertion이 pass/fail/warn인가? |
| `lakehouse_runs` | Mongo 또는 JSON fallback | 한 pipeline run에서 무슨 일이 일어났나? |
| `lineage_events` | Mongo | source -> bronze -> silver/EAV -> gold가 어떻게 연결됐나? |
| `_state/*.json` | JSON backend fallback | idempotency/drift 기준으로 재사용할 successful run은 무엇인가? |

## 시나리오 1 — Phase 1 Catalog Ingest

### 시나리오

```text
사용자/API가 CSV를 named dataset으로 ingest 요청한다.
시스템이 파일을 inspect한다.
같은 dataset_id + source_hash가 이미 있으면 기존 dataset을 반환한다.
없으면 dataset metadata를 생성/갱신하고 dataset_version을 추가한다.
운영자는 나중에 datasets 목록이나 특정 dataset의 version 목록을 조회한다.
```

### 상태 변화

```text
file inspected
source_hash computed
schema inferred
dataset row created or updated
dataset_version row inserted
same source_hash rerun returns existing dataset
```

### 필요한 정보

- 이 파일은 어떤 `dataset_id`에 속하는가?
- source file 경로는 어디인가?
- 컬럼과 타입은 무엇인가?
- row count와 null count는 얼마인가?
- 이 exact file content가 이미 등록됐는가?
- 새 version 번호는 무엇인가?

### 테이블 / 컬럼

`datasets`

- `dataset_id`
- `description`
- `latest_version`
- `schema`
- `schema_version`
- `created_at`
- `updated_at`

`dataset_versions`

- `dataset_id`
- `version`
- `source`
- `source_hash`
- `schema_hash`
- `row_count`
- `stats.null_counts`
- `ingested_at`

### 함수 / API

- `POST /datasets/{dataset_id}/ingest`
- `ingest_dataset`
- `inspect_csv`
- `hash_file`
- `hash_schema`
- `next_version`
- `build_version_doc`
- `get_dataset`
- `list_datasets`

핵심 판단:

```text
dataset = 데이터셋의 정체성
dataset_version = 파일을 한 번 ingest한 기록
source_hash = 같은 파일인지 판단하는 재현성/idempotency 기준
schema_hash = schema drift를 감지하기 위한 기준
```

## 시나리오 2 — Medallion Daily Manufacturing Pipeline

### 시나리오

```text
사용자/CLI가 daily manufacturing pipeline을 실행한다.
입력 파일이 없으면 synthetic manufacturing CSV를 만든다.
원본 파일을 bronze로 보존한다.
active business_date row를 필터링하고 natural key로 dedup해 silver를 만든다.
silver를 daily line/product gold mart로 aggregate한다.
quality report와 catalog/lineage를 기록한다.
```

### 상태 변화

```text
raw CSV exists
source_hash and schema_hash computed
idempotency lookup checks prior successful run
bronze copy written
manifest written
silver CSV written
gold CSV written
quality_report written
lakehouse_runs row upserted
lineage_events row upserted
```

### 필요한 정보

- active `business_date`는 무엇인가?
- 처리 중인 exact file content는 무엇인가?
- 실제 CSV header에서 나온 schema는 무엇인가?
- silver event의 natural key는 무엇인가?
- 어떤 row가 정상적으로 filtering/dedup돼야 하는가?
- silver -> gold에서 수량/불량 수가 보존됐는가?
- 이전 successful run 대비 schema drift가 있는가?
- 같은 date/file을 다시 돌려도 skip 가능한가?

### 파일 / 컬럼

Bronze manifest:

- `dataset_id`
- `stage`
- `source`
- `business_date`
- `source_hash`
- `schema_hash`
- `row_count`
- `created_at`

Silver rows:

- `event_time`
- `business_date`
- `plant_id`
- `line_id`
- `work_order_id`
- `robot_id`
- `product_code`
- `operation`
- `units_produced`
- `defect_count`
- `cycle_time_ms`
- `source_hash`

Gold rows:

- `business_date`
- `plant_id`
- `line_id`
- `product_code`
- `units_produced`
- `defect_count`
- `defect_rate`
- `avg_cycle_time_ms`
- `closing_status`

Run/lineage doc:

- `dataset_id`
- `run_id`
- `business_date`
- `source_hash`
- `schema_hash`
- `source.path`
- `paths.raw/bronze/silver/gold/quality/manifest`
- `layers[].parents`
- `stats.source_rows/silver_rows/gold_rows`
- `schema_drift`
- `quality.passed`
- `quality.checks`

### 함수 / CLI

- `python -m robot_data_platform.pipeline.run`
- `run_lakehouse_pipeline`
- `read_rows`
- `build_paths`
- `write_bronze`
- `transform_silver`
- `write_silver`
- `transform_gold`
- `write_gold`
- `build_quality_checks`
- `build_schema_drift_check`
- `build_lineage_doc`
- `persist_catalog`

읽는 순서:

```text
run_lakehouse_pipeline
-> idempotency gate
-> write_bronze
-> transform_silver
-> transform_gold
-> build_quality_checks
-> persist_catalog
```

## 시나리오 3 — Idempotent Rerun / Backfill Safety

### 시나리오

```text
사용자가 같은 dataset_id, business_date, source content로 다시 실행한다.
시스템이 이전 successful run을 찾는다.
bronze/silver/gold를 다시 쓰지 않는다.
기존 run을 status="skipped"로 반환하고 reuse를 기록한다.
```

### 상태 변화

```text
existing successful run found
reuse_count incremented
last_reused_at updated (Mongo)
result reconstructed from existing run doc
no new run_id created
```

### 필요한 정보

- "같은 작업"을 무엇으로 정의할 것인가?
- 이전 run이 성공했는가?
- 어떤 `run_id`를 재사용해야 하는가?
- 몇 번 재사용됐는가?

### 테이블 / 파일

Mongo:

- `lakehouse_runs.dataset_id`
- `lakehouse_runs.business_date`
- `lakehouse_runs.source_hash`
- `lakehouse_runs.quality.passed`
- `lakehouse_runs.reuse_count`
- `lakehouse_runs.last_reused_at`

JSON fallback:

- `_state/<dataset_id>/business_date=<date>.json`
- `_state/<dataset_id>/latest_successful_run.json`

### 함수

- `find_existing_successful_run`
- `record_run_reuse`
- `result_from_doc`
- `state_dir`
- `read_json_file`
- `write_json_file`

설계 결정:

```text
same date + same content + prior success = overwrite가 아니라 skip
```

이 정도면 Slice 1의 retry/backfill 설명은 가능하다. Iceberg/Delta의
overwrite/snapshot semantics는 다음 slice의 backlog다.

## 시나리오 4 — Schema Drift / Quality Failure

### 시나리오

```text
이전 successful run이 있다.
새 입력의 schema가 바뀌었거나 값 품질이 나쁘다.
시스템이 현재 schema_hash와 이전 successful schema_hash를 비교한다.
schema drift는 warn으로 surface한다.
나쁜 data quality는 run을 fail시킬 수 있다.
```

### 상태 변화

```text
previous successful schema_hash loaded
schema_drift check appended
quality_report written with pass/fail/warn checks
quality.passed derived from fail checks only
failed quality run recorded but not used as successful baseline
```

### 필요한 정보

- 이전 successful schema는 무엇인가?
- 현재 schema는 무엇인가?
- schema drift를 fail로 막을 것인가, warn으로 노출할 것인가?
- 어떤 check가 왜 실패했는가?
- 이 run을 다음 drift/idempotency 기준으로 삼아도 되는가?

### Quality Check Shape

모든 check는 같은 모양이다.

```json
{
  "name": "check_name",
  "status": "pass|fail|warn",
  "expected": "...",
  "actual": "...",
  "detail": "human-readable reason"
}
```

Manufacturing quality checks:

- `row_count_source_to_silver`
- `unit_conservation_silver_to_gold`
- `not_null_required_columns`
- `unique_natural_key`
- `accepted_values_operation`
- `numeric_range_within_bounds`
- `freshness_business_date`
- `schema_drift`

### 함수

- `lookup_previous_schema_hash`
- `build_schema_drift_check`
- `build_quality_checks`
- `write_quality_report`
- `persist_catalog`

핵심 판단:

```text
schema drift policy = warn
```

이유는 legitimate schema evolution을 막지 않기 위해서다. 운영 게이트로
강하게 막아야 하는 상황이면 `fail` 정책으로 바꿀 수 있다.

## 시나리오 5 — EAV Mini Multi-Format Intake

### 시나리오

```text
서로 다른 header와 단위를 가진 source file 여러 개가 들어온다.
각 source에는 JSON mapping config가 있다.
시스템이 mapping과 CSV를 읽는다.
각 wide row를 여러 EAV row로 변환한다.
mapping config에 따라 단위 변환도 수행한다.
EAV row를 다시 pivot/aggregate해서 entity/day gold metric table을 만든다.
EAV-specific quality와 catalog/lineage를 기록한다.
```

### 상태 변화

```text
synthetic source files/configs ensured
mapping configs loaded
source file_id computed per source
combined source_hash computed
EAV schema_hash computed from sources + standard attributes
idempotency lookup checks prior successful EAV run
bronze source copies written
EAV long CSV written
gold entity metrics CSV written
quality report written
lakehouse_runs row upserted
lineage_events row upserted
```

### 필요한 정보

- 어떤 source file들이 참여하는가?
- 각 파일은 어떤 `source_id`와 mapping config를 가지는가?
- 어떤 source column이 어떤 standard attribute로 매핑되는가?
- 어떤 단위 변환이 필요한가?
- 각 source의 entity id column은 무엇인가?
- 각 source의 business date column은 무엇인가?
- 모든 source가 required standard attributes를 제공하는가?
- mapping되지 않은 source column이 있는가?
- typed value parsing에 실패한 값이 있는가?
- EAV additive measure가 gold aggregate에서 보존됐는가?

### Config / Columns

Mapping config:

- `source_id`
- `source_file`
- `entity_field`
- `business_date_field`
- `attributes.<source_column>.standard`
- `attributes.<source_column>.type`
- `attributes.<source_column>.convert`

EAV rows:

- `entity_id`
- `business_date`
- `attribute`
- `value`
- `value_type`
- `source_id`
- `source_file_id`

Gold rows:

- `business_date`
- `entity_id`
- `units_produced`
- `defect_count`
- `temperature_c`
- `pressure_kpa`
- `defect_rate`
- `reading_count`

Run/lineage doc:

- `dataset_id = manufacturing_wide_eav`
- `run_id`
- `business_date`
- `source_hash`
- `schema_hash`
- `sources[].source_id`
- `sources[].file`
- `sources[].file_id`
- `layers = bronze -> silver_eav -> gold`
- `stats.eav_rows/gold_rows/source_count`
- `quality`
- `schema_drift`

### 함수 / CLI

- `python -m robot_data_platform.pipeline.run_eav`
- `run_eav_pipeline`
- `ensure_sample_eav_inputs`
- `load_sources`
- `combined_source_hash`
- `eav_schema_hash`
- `transform_to_eav`
- `normalize_value`
- `transform_eav_to_gold`
- `build_eav_quality_checks`
- `build_eav_lineage_doc`
- shared: `find_existing_successful_run`, `lookup_previous_schema_hash`,
  `persist_catalog`, `record_run_reuse`

읽는 순서:

```text
run_eav_pipeline
-> ensure_sample_eav_inputs
-> load_sources
-> transform_to_eav
-> transform_eav_to_gold
-> build_eav_quality_checks
-> build_eav_lineage_doc
-> persist_catalog
```

## 시나리오 6 — 새 Wide Format 추가

### 시나리오

```text
새 vendor/source가 다른 column name을 가진 파일을 보낸다.
개발자는 CSV와 mapping JSON을 하나 추가한다.
pipeline code는 바꾸지 않는다.
실행하면 새 entity가 EAV와 gold에 나타난다.
```

### 상태 변화

```text
new mapping config discovered by load_sources
new source file participates in combined_source_hash
new source id participates in eav_schema_hash
new file copied into bronze
new rows emitted into EAV
new entity appears in gold
schema_drift may warn if source set/attribute set changes vs prior success
```

### 필요한 정보

- source file 이름은 무엇인가?
- entity column은 무엇인가?
- business date column은 무엇인가?
- 어떤 source column이 어떤 standard attribute로 매핑되는가?
- 단위 변환이 필요한가?

### 함수 / 테스트

- `load_sources`
- `transform_to_eav`
- `test_new_format_is_onboarded_by_adding_one_config`

설계 결정:

```text
new format = pipeline code change가 아니라 config change
```

이것이 EAV mini slice의 핵심 모델링 학습 포인트다.

## 다음에 코드를 읽는 순서

무작정 파일을 열지 말고 이 순서로 본다.

1. 위 시나리오 중 하나를 고른다.
2. 그 시나리오에서 바뀌어야 하는 상태 객체를 찾는다.
3. 그 상태를 쓰는 함수를 찾는다.
4. 그 다음에 transform 세부 구현을 읽는다.

예시: EAV 새 format 추가

```text
질문: 새 source format을 코드 변경 없이 어떻게 onboard하는가?
상태 객체: mapping JSON + EAV rows + gold rows
entry point: run_eav_pipeline
상태 writer: load_sources -> transform_to_eav -> transform_eav_to_gold
검증: test_new_format_is_onboarded_by_adding_one_config
```

예시: idempotency

```text
질문: 왜 rerun이 안전한가?
상태 객체: successful lakehouse_runs row 또는 JSON _state file
entry point: run_lakehouse_pipeline / run_eav_pipeline
상태 reader/writer: find_existing_successful_run -> record_run_reuse
검증: idempotent rerun tests
```

## 아직 열어둔 설계 질문

현재 slice에서 일부러 풀지 않은 질문들이다.

- 같은 date지만 source가 바뀐 rerun은 canonical partition overwrite인가, 새 run인가?
- schema drift는 계속 `warn`이어야 하나, 운영에서는 `fail`이어야 하나?
- failed quality run을 successful run과 분리해서 조회할 별도 상태가 필요한가?
- EAV mapping config를 versioned catalog object로 승격해야 하는가?
- Spark/Iceberg로 갈 때 어떤 상태는 Mongo에 남기고 어떤 상태는 table metadata로 옮길 것인가?

이 질문들이 현재 learning slice에서 다음 platform slice로 넘어가는 다리다.
