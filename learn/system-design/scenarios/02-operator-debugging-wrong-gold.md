# 02. Scenario Walkthrough — gold 숫자가 이상할 때 원인 추적

상태: implemented read-only helper / existing evidence exercise

목적: 이 프로젝트의 catalog/lineage claim을 "문서상 존재"가 아니라 operator가 실제로 따라갈 수 있는 RCA path로 검증한다.

## Scenario

```text
분석가가 business_date=2026-06-29의 defect_rate가 이상하다고 보고한다.
운영자는 raw CSV를 바로 열기 전에, run/catalog/quality/lineage evidence로 원인을 좁히고 싶다.
```

이 시나리오는 새 엔진을 붙이는 작업이 아니다. 이미 있는 Slice1/EAV evidence를 operator 관점으로 걷는 작업이다.

## Actor / Responsibility Map

| Actor | Wants to know | Looks at | Must not assume |
|---|---|---|---|
| Analyst | 이 gold 숫자를 써도 되는가 | gold mart, quality summary | pass/warn 의미를 모른 채 숫자만 신뢰 |
| Operator / DE | 어떤 source/run이 숫자를 만들었는가 | run record, source_hash, schema_hash, lineage links | 파일명만 보고 원인을 단정 |
| Pipeline | 어떤 상태와 증거를 남겼는가 | quality checks, catalog/lineage doc | warning을 숨기기 |
| Reviewer / interviewer | lineage claim이 실제인가 | test/log/doc evidence | column-level lineage로 오해 |

## Debugging Flow

```text
1. gold row grain을 확인한다.
2. 해당 business_date의 latest successful run을 찾는다.
3. run_id, source_hash, schema_hash, status, quality result를 본다.
4. quality checks에서 fail/warn을 확인한다.
5. lineage layer parent links로 gold -> silver -> bronze/source path를 역추적한다.
6. schema_drift warning이나 row reconciliation 차이가 있는지 본다.
7. source_hash가 이전 run과 같은지/다른지 확인한다.
8. 원인을 "data changed", "schema changed", "transform/regression", "expected filter/dedup" 중 하나로 좁힌다.
```

## Row / Record Trace

| moment | record/file | key fields | meaning |
|---|---|---|---|
| reported | gold CSV | `business_date`, `plant_id`, `line_id`, `product_code`, `defect_rate` | 이상해 보이는 metric |
| identify grain | gold grain note | `(business_date, plant_id, line_id, product_code)` | 한 row가 무엇인지 확정 |
| find run | JSON/Mongo state | `dataset_id`, `business_date`, `run_id`, `status` | 어떤 run이 publish candidate인지 확인 |
| inspect quality | quality report | `row_count_source_to_silver`, `unit_conservation_silver_to_gold`, `schema_drift` | row loss/aggregation/schema issue 확인 |
| trace lineage | lineage doc | `layers[].parents`, `source_hash`, `schema_hash` | output이 어떤 source에서 왔는지 추적 |
| compare rerun | prior/latest run | `source_hash`, `reuse_count` | 같은 source 재실행인지 정정 source인지 판단 |

## Existing Evidence

Already implemented:

```text
run_id
source_hash
schema_hash
quality checks
schema_drift warn
layer parent links
JSON catalog state
mongomock catalog path tests
idempotent skip + reuse_count
```

This supports:

```text
table/path-level lineage record
operator-inspectable run evidence
source/run identity trace
```

This does not support:

```text
column-level lineage
OpenLineage backend integration
interactive lineage UI
production incident workflow
```

## Implemented Helper

Command:

```bash
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.operator_report \
  --output-dir data/lakehouse \
  --business-date 2026-06-29
```

Implemented evidence:

```text
src/manufacturing_data_platform/pipeline/operator_report.py
tests/test_operator_report.py
```

The helper reads the JSON catalog state for a successful `business_date` and returns:

```text
gold grain
run_id
source_hash
schema_hash
quality status summary
lineage path chain: gold -> silver -> bronze -> source
claim boundary
```

## Test Contract

Small next slice:

```text
given a processed business_date
when an operator asks for the run evidence for that date
then the helper returns:
  gold grain
  run_id
  source_hash
  schema_hash
  quality status summary
  lineage path chain gold -> silver -> bronze/source
```

This can be implemented as a small read-only helper or CLI report over the existing JSON catalog output. It should not require Spark/Iceberg.

## Blog / Resume Use

Blog candidate:

```text
B4: gold 숫자가 이상할 때 source_hash, quality, lineage로 원인 좁히기
```

Resume-safe wording:

```text
Implemented table/path-level lineage records and quality metadata that let an operator trace a synthetic gold metric back to its source hash, schema hash, run id, and parent layer paths.
```

Forbidden wording:

```text
built a production lineage system
implemented column-level lineage
integrated OpenLineage backend
```
