# 02. Airflow Wrapper Command Contract Slice

상태: wrapper command contract implemented / runtime Airflow unverified

> 이 문서는 Airflow 관련 설계 흐름을 한눈에 보기 위한 얇은 slice map이다.  
> 최신 테스트 수와 실행 결과는 [`../../../VERIFICATION_LOG.md`](../../../VERIFICATION_LOG.md)가 source of truth다.

## 1. Slice Thesis

```text
Airflow는 business logic을 소유하지 않고,
이미 검증된 lakehouse CLI entrypoint를 같은 parameter contract로 호출해야 한다.
```

이 slice의 목적은 Airflow 운영 전체가 아니다.

```text
thin DAG wrapper
testable command builder
business_date/raw_path/output_dir/catalog_backend parameter pass-through
pipeline idempotency remains inside the pipeline
```

## 2. Primary Scenario

시나리오:

```text
운영자가 특정 business_date를 Airflow에서 수동 backfill-style로 실행하고 싶다.
DAG는 raw_path, output_dir, catalog_backend를 넘긴다.
하지만 bronze/silver/gold/quality/catalog business logic은 DAG 안에 들어가면 안 된다.
같은 source를 retry해도 idempotency는 pipeline의 source_hash logic이 보장해야 한다.
```

관련 배경:

- [`../question-bank/04-orchestration-observability.ko.md`](../question-bank/04-orchestration-observability.ko.md)
- [`../question-bank/06-cross-area-connection-questions.ko.md`](../question-bank/06-cross-area-connection-questions.ko.md)
  - rerun x orchestration
  - idempotency x failure
- [`../../../README.md`](../../../README.md)
  - `Airflow Wrapper`
- [`../../../PROJECT_PROGRESS_MAP.md`](../../../PROJECT_PROGRESS_MAP.md)
  - `Airflow runtime verification`

## 3. Question Areas Pulled

관련 question-bank 영역:

- orchestration / scheduling / Airflow
- rerun / idempotency
- failure state / retry
- testing / local reproducibility
- public claim boundary

### Core Questions

| Core question | Why Core |
|---|---|
| Airflow DAG가 business logic을 갖는가, CLI를 호출만 하는가? | code ownership과 testability가 달라진다. |
| DAG가 호출하는 CLI entrypoint는 local verification과 같은가? | Airflow와 local 실행 결과가 갈라지면 안 된다. |
| `business_date`, `raw_path`, `output_dir`, `catalog_backend`는 어떻게 전달되는가? | backfill/manual run contract가 달라진다. |
| command construction을 Airflow 없이 테스트할 수 있는가? | 현재 환경에 Airflow가 없어도 contract evidence를 만들 수 있다. |
| Airflow retry와 pipeline idempotency는 어떻게 분리되는가? | retry가 중복 output을 만들지 않게 한다. |
| runtime verified라고 말할 수 있는가? | public claim boundary를 결정한다. |

### Demo Questions

| Demo question | Why not Core |
|---|---|
| Airflow UI에서 DAG를 수동 trigger할 것인가? | Airflow runtime 설치가 필요하고 현재 wrapper contract와 별도다. |
| task를 bronze/silver/gold/quality/catalog로 쪼갤 것인가? | one-task wrapper contract가 먼저 안정화되어야 한다. |

### Backlog Questions

| Backlog question | Reason |
|---|---|
| Airflow runtime import/trigger를 실제로 검증할 것인가? | Airflow 설치/runtime 환경이 필요하다. |
| Docker Compose에서 scheduler/worker/webserver를 띄울 것인가? | production-like orchestration scope로 커진다. |
| Airflow가 Spark/Iceberg skeleton을 trigger할 것인가? | Spark optional dependency + Airflow runtime을 같이 다뤄야 한다. |
| task-level failed run forensics를 만들 것인가? | failure-state slice와 연결된다. |
| alerts/SLA miss를 구현할 것인가? | observability/operations scope다. |

### Unknowns

| Unknown | Current handling |
|---|---|
| 이 환경에서 Airflow package/runtime이 설치되어 있는가? | runtime unverified로 명시한다. |
| DAG import/trigger가 실제 Airflow에서 성공하는가? | Backlog: Airflow runtime verification slice. |
| Airflow retry attempt와 pipeline run status를 어떻게 같이 보여줄 것인가? | future failure/observability question. |

## 4. Decisions

긴 decision 문서는 아직 없다. 이 slice의 durable 결정은 아래다.

```text
Keep business logic in manufacturing_data_platform.pipeline.run
Build the BashOperator command through manufacturing_data_platform.orchestration
Make the command builder testable without Airflow installed
Pass runtime values through dag_run.conf/Jinja
Keep runtime Airflow trigger unverified until actual Airflow runtime exists
```

이 결정은 아래 구현/테스트가 증명한다.

## 5. Evidence

Code / test:

- [`../../../src/manufacturing_data_platform/orchestration.py`](../../../src/manufacturing_data_platform/orchestration.py)
- [`../../../dags/manufacturing_lakehouse_daily.py`](../../../dags/manufacturing_lakehouse_daily.py)
- [`../../../tests/test_orchestration.py`](../../../tests/test_orchestration.py)

Verification:

- [`../../../VERIFICATION_LOG.md`](../../../VERIFICATION_LOG.md)
  - `2026-07-11 — Airflow wrapper command contract`

Related run command:

- [`../../../README.md`](../../../README.md)
  - `Airflow Wrapper`

## 6. Claim Boundary

Allowed:

```text
Airflow DAG wrapper command contract is test-covered.
The DAG calls the same lakehouse CLI entrypoint.
The command builder supports business_date/raw_path/output_dir/catalog_backend parameters.
Business logic remains outside the DAG body.
```

Forbidden:

```text
Airflow runtime trigger verified
operated production Airflow pipeline
scheduler/worker deployment verified
multi-task production DAG
Airflow-triggered Spark runtime verified
```

## 7. Next Questions

```text
Should Airflow runtime import/trigger be verified in this environment?
Should execution_date and business_date be explicitly separated in docs/tests?
Should failed Airflow attempts be linked to pipeline run evidence?
Should a future task split produce better observability, or just extra complexity?
Should Airflow-triggered Spark/Iceberg be a separate slice after runtime Airflow works?
```
