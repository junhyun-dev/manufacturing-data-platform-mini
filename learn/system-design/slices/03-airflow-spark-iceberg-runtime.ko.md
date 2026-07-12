# 03. Airflow-triggered Spark/Iceberg Runtime Slice

상태: local Airflow `dags test` + local `standalone` scheduler verified / production deployment not claimed

> 이 문서는 Airflow가 Spark/Iceberg skeleton을 trigger하는 설계 흐름을 한눈에 보기 위한 얇은 slice map이다.  
> 최신 테스트 수와 실행 결과는 [`../../../VERIFICATION_LOG.md`](../../../VERIFICATION_LOG.md)가 source of truth다.

## 1. Slice Thesis

```text
Airflow는 Spark/Iceberg logic을 소유하지 않고,
이미 검증된 Spark/Iceberg skeleton CLI를 local runtime에서 trigger해야 한다.
```

이 slice의 목적은 production Airflow나 full Spark platform이 아니다.

```text
thin DAG wrapper
testable Spark/Iceberg command builder
warehouse/output_dir runtime parameter pass-through
local Airflow dags test evidence
local Airflow standalone scheduler/LocalExecutor evidence
Spark/Iceberg partition-overwrite evidence reuse
```

## 2. Primary Scenario

시나리오:

```text
운영자가 Airflow에서 Spark/Iceberg partition-overwrite demo를 수동 실행하고 싶다.
DAG는 warehouse와 output_dir만 넘긴다.
SparkSession/Iceberg table/write logic은 DAG 안에 들어가면 안 된다.
실제 증거는 Spark/Iceberg skeleton이 생성하는 JSON evidence로 남아야 한다.
```

관련 배경:

- [`spark-iceberg-partition-overwrite/00-slice-map.ko.md`](spark-iceberg-partition-overwrite/00-slice-map.ko.md)
- [`02-airflow-wrapper-command-contract.ko.md`](02-airflow-wrapper-command-contract.ko.md)
- [`../../../dags/manufacturing_iceberg_skeleton.py`](../../../dags/manufacturing_iceberg_skeleton.py)
- [`../../../src/manufacturing_data_platform/pipeline/spark_iceberg_skeleton.py`](../../../src/manufacturing_data_platform/pipeline/spark_iceberg_skeleton.py)

## 3. Question Areas Pulled

관련 question-bank 영역:

- orchestration / scheduling / Airflow
- storage / Spark / Iceberg
- rerun / idempotency
- testing / local reproducibility
- public claim boundary

### Core Questions

| Core question | Why Core |
|---|---|
| Airflow DAG가 Spark logic을 갖는가, CLI를 호출만 하는가? | DAG와 processing code의 ownership을 분리한다. |
| Airflow task가 어떤 Python/Spark runtime을 쓰는가? | worker shell의 dependency가 맞지 않으면 runtime에서 깨진다. |
| `warehouse`, `output_dir`는 어떻게 전달되는가? | Airflow 실행 evidence 위치를 통제해야 한다. |
| Spark/Iceberg evidence는 무엇으로 판단하는가? | DAG success만으로 partition overwrite를 증명할 수 없다. |
| `dags test`는 scheduler/executor 검증인가? | local command execution과 scheduler path claim을 분리한다. |
| standalone worker venv에는 어떤 dependency가 필요한가? | scheduler/LocalExecutor 경로에서는 worker shell의 `python`이 실제 CLI dependency를 모두 가져야 한다. |
| 이걸 production Airflow 운영으로 말할 수 있는가? | public claim boundary를 결정한다. |

### Demo Questions

| Demo question | Why not Core |
|---|---|
| Airflow UI를 browser에서 조작할 것인가? | API server HTTP 응답과 scheduler run은 검증했지만 UI click flow는 별도다. |
| scheduler/worker를 장시간 계속 켜둘 것인가? | production-like 운영 claim으로 커진다. |

### Backlog Questions

| Backlog question | Reason |
|---|---|
| production scheduler/worker/webserver deployment를 검증할 것인가? | 별도 Airflow deployment slice다. |
| Spark/Iceberg를 full medallion pipeline으로 확장할 것인가? | 현재는 단일 gold table skeleton이다. |
| Airflow task를 Spark submit/cluster 모드로 바꿀 것인가? | cluster Spark/runtime packaging 문제가 생긴다. |
| Airflow 실패 attempt와 Iceberg partial state를 연결할 것인가? | failure-state forensics slice와 연결된다. |

### Unknowns

| Unknown | Current handling |
|---|---|
| Airflow worker shell의 `python`이 Spark/project dependency를 갖는가? | local standalone에서는 `requirements-airflow.txt`, `requirements.txt`, `requirements-spark.txt`를 같은 venv에 설치해야 통과. |
| Maven/Iceberg runtime jar resolution이 항상 가능한가? | 현재 local run에서는 통과. offline/locked network는 별도 gate. |
| UI click flow나 장기 실행도 필요한가? | 이번 slice 범위 밖. |

## 4. Decisions

```text
Create a separate DAG: manufacturing_iceberg_skeleton
Keep one BashOperator task: run_spark_iceberg_skeleton_task
Call manufacturing_data_platform.pipeline.spark_iceberg_skeleton
Use dag_run.conf for warehouse/output_dir
Use --clean for deterministic local evidence output
Keep production Airflow and cluster Spark claims out of scope
For standalone, install Airflow + project runtime deps + Spark deps into the same worker venv
```

## 5. Evidence

Code / test:

- [`../../../dags/manufacturing_iceberg_skeleton.py`](../../../dags/manufacturing_iceberg_skeleton.py)
- [`../../../src/manufacturing_data_platform/orchestration.py`](../../../src/manufacturing_data_platform/orchestration.py)
- [`../../../tests/test_orchestration.py`](../../../tests/test_orchestration.py)
- [`../../../tests/test_airflow_dags.py`](../../../tests/test_airflow_dags.py)
- [`../../../src/manufacturing_data_platform/pipeline/spark_iceberg_skeleton.py`](../../../src/manufacturing_data_platform/pipeline/spark_iceberg_skeleton.py)
- [`../../../scripts/verify_airflow_standalone.sh`](../../../scripts/verify_airflow_standalone.sh)

Verification:

- [`../../../VERIFICATION_LOG.md`](../../../VERIFICATION_LOG.md)
  - `2026-07-12 — Airflow-triggered Spark/Iceberg skeleton`
  - `2026-07-12 — Airflow standalone scheduler run for Spark/Iceberg skeleton`

## 6. Claim Boundary

Allowed:

```text
Airflow local dags test triggers the Spark/Iceberg skeleton CLI.
Airflow local standalone starts api-server, scheduler, dag-processor, triggerer, and LocalExecutor workers.
Manual airflow dags trigger can run the Spark/Iceberg skeleton to success through LocalExecutor.
The standalone verification is reproducible through scripts/verify_airflow_standalone.sh.
The task creates local Iceberg evidence JSON.
The same Spark/Iceberg partition-overwrite assertions still hold under Airflow.
Spark/Iceberg logic remains outside the DAG body.
Optional DagBag tests parse the DAGs when Airflow is installed.
Worker dependency packaging requirement is documented.
```

Forbidden:

```text
production Airflow scheduler/worker deployment
distributed Airflow queue/executor/worker behavior
browser-driven UI workflow
cluster Spark
full Spark medallion pipeline
Airflow-operated production lakehouse
Spark-based quality suite
```

## 7. Next Questions

```text
Should we add browser-level UI screenshots, or are HTTP 200 + scheduler-run evidence enough?
Should the next implementation slice be failure-state forensics instead of more Airflow deployment?
Should Spark/Iceberg remain single-table, or should a later slice port one quality check to Spark?
How should worker dependency packaging be handled if this moves beyond local dags test?
```
