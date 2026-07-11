# Gold Grain 의사결정 노트

ADR Status: Implemented
상태: implemented contract summary
프로젝트: `manufacturing-data-platform-mini`

이 노트는 gold mart의 한 row가 무엇을 의미하는지 고정한다. 면접이나 블로그에서 "gold를 만들었다"라고 말할 때는 반드시 grain이 같이 설명되어야 한다.

관련 코드/테스트:

- `src/manufacturing_data_platform/pipeline/lakehouse.py` (`transform_gold`)
- `src/manufacturing_data_platform/pipeline/eav.py` (`transform_eav_to_gold`)
- `tests/test_lakehouse_pipeline.py` (`test_transform_gold_conserves_units_and_defects`)
- `tests/test_eav_pipeline.py` (`test_transform_eav_to_gold_aggregates_sum_and_avg`)

## 1. Decision

### Manufacturing slice

Gold row grain:

```text
one row per (business_date, plant_id, line_id, product_code)
```

Metrics:

```text
units_produced       sum
defect_count         sum
defect_rate          defect_count / units_produced
avg_cycle_time_ms    average over source events in the group
closing_status       provisional
```

Why:

- `business_date` is the rerun/backfill boundary.
- `plant_id + line_id` is the operational inspection boundary.
- `product_code` keeps product-mix changes visible instead of hiding them in one daily total.

### EAV slice

Gold row grain:

```text
one row per (business_date, entity_id)
```

Metrics:

```text
units_produced    sum
defect_count      sum
temperature_c     average
pressure_kpa      average
defect_rate       defect_count / units_produced
reading_count     max attribute reading count
```

Why:

- EAV is used because source files have heterogeneous columns.
- `entity_id` is the stable business entity after mapping.
- Counts remain additive, sensor readings are averaged.

## 2. What This Enables

This grain lets the project answer concrete questions:

```text
Which line/product had the defect-rate change on a business date?
Which source/run produced that daily metric?
Did aggregation preserve additive measures from silver/EAV to gold?
Can the same business_date be rerun without duplicating the published grain?
```

## 3. What This Does Not Claim

This is not a production semantic layer.

Not claimed:

```text
full dimensional model
SCD handling
product hierarchy
plant calendar logic
late event watermarking
column-level lineage
```

## 4. Evidence Boundary

Implemented:

```text
manufacturing transform groups by (business_date, plant_id, line_id, product_code)
EAV transform groups by (business_date, entity_id)
quality checks verify additive conservation
tests cover gold aggregation behavior
```

Backlog:

```text
age-based freshness SLA
semantic catalog contract
agent-readable asset contract
Iceberg partition overwrite for corrected business_date reruns
```

## 5. Interview Line

```text
The manufacturing gold mart has one row per business_date, plant, line, and product.
That grain was chosen because business_date is the rerun boundary, while plant/line/product
are the operational dimensions an analyst would inspect. For the EAV slice, the gold grain is
business_date and entity_id, because heterogeneous source formats are normalized to a stable entity before aggregation.
```
