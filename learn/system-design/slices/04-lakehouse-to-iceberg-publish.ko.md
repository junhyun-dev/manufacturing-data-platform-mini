# 04. Lakehouse Gold -> Iceberg Publish Slice

상태: implemented / local CLI + Airflow `dags test` verified / production deployment not claimed

> 이 문서는 기존 JSON lakehouse run의 gold 결과를 Iceberg current table로 발행하는 설계 흐름을 한눈에 보기 위한 얇은 slice map이다.  
> 최신 테스트 수와 실행 결과는 [`../../../VERIFICATION_LOG.md`](../../../VERIFICATION_LOG.md)가 source of truth다.

## 1. Slice Thesis

```text
검증된 lakehouse pipeline run이 만든 gold CSV만
local Iceberg gold table의 current state로 publish한다.
```

이 slice의 목적은 full Spark rewrite가 아니다.

```text
existing Python lakehouse pipeline remains transform/quality/catalog owner
JSON catalog latest successful run is the publish source
Iceberg stores the current gold table
business_date publish uses partition overwrite
Airflow runs pipeline task -> publish task in one DAG
```

## 2. Primary Scenario

시나리오:

```text
운영자가 Airflow에서 business_date=2026-06-29를 처리한다.
첫 task는 기존 lakehouse CLI로 bronze/silver/gold/quality/catalog를 만든다.
quality-passed JSON catalog state가 생긴 뒤에만,
두 번째 task가 해당 run의 gold CSV를 Iceberg table로 publish한다.

나중에 같은 business_date의 정정 source가 들어오면,
Iceberg table에서는 그 날짜 partition만 교체되고 다른 날짜 partition은 유지되어야 한다.
```

관련 배경:

- [`../scenarios/01-rerun-same-business-date.md`](../scenarios/01-rerun-same-business-date.md)
- [`spark-iceberg-partition-overwrite/00-slice-map.ko.md`](spark-iceberg-partition-overwrite/00-slice-map.ko.md)
- [`03-airflow-spark-iceberg-runtime.ko.md`](03-airflow-spark-iceberg-runtime.ko.md)

## 3. Question Areas Pulled

관련 question-bank 영역:

- orchestration / Airflow
- storage / table format / Iceberg
- rerun / correction / idempotency
- quality / current-state gate
- catalog / run identity
- observability / operator evidence
- public claim boundary

### Core Questions

| Core question | Why Core |
|---|---|
| Iceberg publish의 source of truth는 무엇인가? | raw CSV를 다시 읽으면 quality/catalog gate를 우회한다. |
| quality-fail run도 Iceberg current가 될 수 있는가? | current table 신뢰 경계가 깨진다. |
| 같은 lakehouse run을 다시 publish하면 새 snapshot을 만들 것인가? | Airflow retry가 Iceberg snapshot noise를 만들 수 있다. |
| 정정 source는 append인가, partition overwrite인가? | 중복 gold row를 막는 write contract다. |
| `run_id`와 `snapshot_id`는 어디에 연결되는가? | pipeline 실행과 table commit을 혼동하지 않는다. |
| Airflow DAG가 Spark/Iceberg logic을 갖는가? | DAG는 task 순서만 소유하고 logic은 CLI에 둔다. |

### Demo Questions

| Demo question | Why not Core |
|---|---|
| Airflow standalone scheduler로 이 2-task DAG까지 실행할 것인가? | CLI/test contract가 먼저이며 runtime verification은 별도 evidence 단계다. |
| browser UI click flow를 캡처할 것인가? | HTTP/UI 확인은 이미 별도 Airflow runtime slice에서 다뤘다. |

### Backlog Questions

| Backlog question | Reason |
|---|---|
| Mongo catalog에서 latest successful run을 읽을 것인가? | real Mongo runtime verification 뒤에 해야 한다. |
| bronze/silver/gold 전체를 Spark/Iceberg로 저장할 것인가? | full medallion Spark rewrite로 scope가 커진다. |
| Spark로 quality suite를 다시 구현할 것인가? | 별도 quality-on-Spark slice다. |
| pipeline success 후 publish 실패의 partial state를 어떻게 복구할 것인가? | failure-state forensics slice와 연결된다. |
| concurrent publish conflict를 검증할 것인가? | production table operation 영역이다. |

### Unknowns

| Unknown | How to close |
|---|---|
| 이 2-task DAG가 Airflow standalone scheduler에서도 통과하는가? | 필요하면 별도 runtime verification으로 닫는다. 현재는 `dags test`까지만 verified. |
| publish state JSON write 실패 후 재시도하면 중복 snapshot이 생기는가? | failure-state slice에서 다룬다. |

## 4. Decisions

```text
Use JSON catalog state as the publish source.
Read only latest successful run for the target business_date.
Publish the run's gold CSV to local.db.gold_daily_metrics.
Use DataFrameWriterV2.overwritePartitions().
Record pipeline_run_id -> gold_snapshot_id evidence.
Skip publish retry for the same pipeline_run_id + source_hash.
Create one Airflow DAG with run_lakehouse_task >> publish_gold_to_iceberg_task.
Keep Mongo-backed publish lookup and full Spark rewrite out of scope.
```

## 5. Evidence

Code / test:

- [`../../../src/manufacturing_data_platform/pipeline/publish_gold_to_iceberg.py`](../../../src/manufacturing_data_platform/pipeline/publish_gold_to_iceberg.py)
- [`../../../src/manufacturing_data_platform/orchestration.py`](../../../src/manufacturing_data_platform/orchestration.py)
- [`../../../dags/manufacturing_lakehouse_to_iceberg_daily.py`](../../../dags/manufacturing_lakehouse_to_iceberg_daily.py)
- [`../../../tests/test_publish_gold_to_iceberg.py`](../../../tests/test_publish_gold_to_iceberg.py)
- [`../../../tests/test_orchestration.py`](../../../tests/test_orchestration.py)
- [`../../../tests/test_airflow_dags.py`](../../../tests/test_airflow_dags.py)

Verification:

- [`../../../VERIFICATION_LOG.md`](../../../VERIFICATION_LOG.md)
  - `2026-07-12 — Lakehouse gold -> Iceberg publish DAG`

## 6. Claim Boundary

Allowed:

```text
one local Airflow DAG can chain the JSON lakehouse CLI and the Iceberg publish CLI
the publish step reads the latest successful JSON catalog state
the publish step writes a local Iceberg gold table
same pipeline run publish retry is skipped without a new snapshot
corrected business_date publish replaces that partition and preserves another partition
```

Forbidden:

```text
full Spark/Iceberg medallion pipeline
Spark-based quality suite
Mongo-backed publish lookup
production Airflow deployment
cluster Spark
concurrent writer handling
exactly-once table/catalog transaction
```

## 7. Next Questions

```text
Should this 2-task DAG get local Airflow runtime verification after pytest passes?
Should failure-state forensics model pipeline-success/publish-failure partial state next?
Should the next Spark slice move one quality check to Spark, or keep Spark as storage publish only?
```
