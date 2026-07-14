# manufacturing-data-platform-mini 한국어판

원문: [`README.md`](README.md)

## 한 줄 요약

synthetic manufacturing-ish CSV를 ingest해서 bronze -> silver -> gold -> quality -> catalog/lineage -> dataset version manifest까지 이어지는 작은 data platform slice다.

```text
CSV
-> bronze raw copy
-> silver typed/deduped rows
-> gold daily metrics
-> quality checks
-> Mongo/json catalog + lineage records
```

## 프로젝트 목적

이 프로젝트는 "도구 이름을 써봤다"가 아니라 데이터 플랫폼의 운영 spine을 작게 증명하는 것이 목적이다.

핵심 키워드:

- metadata catalog
- dataset version manifest
- source/schema hash
- idempotency
- schema drift
- data quality
- medallion architecture
- lineage
- EAV multi-format intake
- Spark/Iceberg partition overwrite skeleton

## Phase 1

MongoDB catalog gate다.

```text
CSV ingest
-> datasets document
-> dataset_versions document
-> GET /datasets
-> GET /datasets/{id}
```

여기서 중요한 것은 "데이터 파일을 열지 않고도 어떤 dataset인지 알 수 있게 하는 catalog"다.

## Phase 2 — lakehouse slice

작은 lakehouse flow를 구현한다.

```text
synthetic manufacturing CSV
-> bronze
-> silver
-> gold
-> quality
-> catalog/lineage
```

quality suite는 단순 row count가 아니라 다음을 본다.

- source -> silver reconciliation
- silver -> gold unit conservation
- required column not null
- natural key unique
- accepted operation values
- numeric range
- freshness
- schema drift

## Idempotency

같은 `dataset_id + business_date + source_hash`로 이미 성공한 run이 있으면 재실행하지 않고 이전 run을 재사용한다.

이 설계가 retry/backfill을 안전하게 만든다.

## Schema drift

CSV header에서 `schema_hash`를 만들고, 이전 successful run과 비교한다.

정책은 `warn`이다. 즉 schema 변화는 기록하지만 run을 바로 실패시키지는 않는다.

## EAV mini slice

여러 wide file format을 config로 표준화한다.

```text
Korean headers / English headers / mixed units
-> mapping config
-> EAV long table
-> gold entity_daily_metrics
```

새 file format은 pipeline code를 바꾸지 않고 mapping config 하나를 추가해서 onboarding한다.

## Spark/Iceberg walking skeleton

full Spark rewrite가 아니라, `business_date` 정정 시 gold partition을 중복 없이 교체하는 작은 skeleton이다.

```bash
pip install -r requirements-spark.txt

PYTHONPATH=src python -m manufacturing_data_platform.pipeline.spark_iceberg_skeleton \
  --warehouse /tmp/manufacturing-mini-iceberg-warehouse \
  --output-dir /tmp/manufacturing-mini-iceberg-evidence \
  --clean
```

구현된 범위:

- local SparkSession + Iceberg hadoop catalog
- `local.db.gold_daily_metrics` 단일 gold table
- `business_date` partition overwrite
- same `source_hash` rerun 시 새 snapshot 없음
- `run_id -> snapshot_id` evidence JSON

정직한 경계: full Spark medallion pipeline, production lakehouse, rollback system은 아니다.

## Lakehouse gold -> Iceberg publish

기존 lakehouse CLI가 만든 successful gold CSV를 Iceberg current table로 발행하는 연결 slice다.

```bash
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run \
  --business-date 2026-06-29 \
  --raw-path data/raw/manufacturing_events.csv \
  --output-dir /tmp/manufacturing-mini-lakehouse-to-iceberg/lakehouse \
  --catalog-backend json

PYTHONPATH=src python -m manufacturing_data_platform.pipeline.publish_gold_to_iceberg \
  --lakehouse-output-dir /tmp/manufacturing-mini-lakehouse-to-iceberg/lakehouse \
  --business-date 2026-06-29 \
  --warehouse /tmp/manufacturing-mini-lakehouse-to-iceberg/warehouse \
  --output-dir /tmp/manufacturing-mini-lakehouse-to-iceberg/evidence \
  --clean
```

이 publish step은 JSON catalog의 latest successful run을 읽고, 그 run의 gold CSV만 `local.db.gold_daily_metrics`에 Iceberg `overwritePartitions()`로 반영한다. 같은 `pipeline_run_id + source_hash`를 다시 publish하면 새 snapshot을 만들지 않고 skip한다.

정직한 경계: JSON catalog 기반 local publish다. Mongo-backed publish lookup, Spark-based quality suite, full Spark medallion rewrite, production Airflow deployment, cluster Spark는 아니다.

## Airflow runtime wrapper

Airflow는 business logic을 갖지 않고, 이미 검증된 lakehouse CLI를 호출하는 얇은 wrapper다.

검증된 범위:

- Airflow 3.3.0 별도 virtualenv 설치
- `airflow db migrate`
- `airflow dags list`에서 `manufacturing_lakehouse_daily` import 확인
- `airflow tasks list manufacturing_lakehouse_daily`에서 `run_pipeline_task` 확인
- `airflow dags test`로 같은 CLI task 실행 성공
- 같은 `dags test` 재실행 시 pipeline `status="skipped"` 확인
- `dag_run.conf`로 `business_date`, `raw_path`, `output_dir`, `catalog_backend` 전달

즉 Airflow retry/backfill 안전성은 Airflow가 아니라 pipeline의 `source_hash` idempotency gate가 보장한다.

정직한 경계: production scheduler/worker/webserver deployment를 운영한 것은 아니다.

## Airflow-triggered Spark/Iceberg skeleton

`dags/manufacturing_iceberg_skeleton.py`는 `manufacturing_iceberg_skeleton` DAG를 정의한다.

이 DAG의 단일 task는 Spark/Iceberg skeleton CLI를 호출한다.

```bash
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.spark_iceberg_skeleton \
  --warehouse <path> \
  --output-dir <path> \
  --clean
```

local runtime 검증:

```bash
AIRFLOW_HOME=/tmp/manufacturing-mini-airflow-home \
AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
PYTHONPATH=src \
/tmp/manufacturing-mini-airflow-venv/bin/airflow dags test manufacturing_iceberg_skeleton 2026-06-29 \
  -c '{"warehouse":"/tmp/manufacturing-mini-airflow-iceberg-warehouse","output_dir":"/tmp/manufacturing-mini-airflow-iceberg-evidence"}'
```

검증된 것은 local Airflow가 Spark/Iceberg walking skeleton을 trigger할 수 있다는 점이다. 이 task는 local Iceberg table을 만들고, `run_id -> snapshot_id` evidence를 남기며, 정정된 `business_date` partition만 overwrite하고 다른 partition은 유지한다.

`airflow dags test`는 단일 DagRun을 local에서 실행한다. DAG import, task wiring, templated command rendering, command execution은 검증하지만 scheduler, queue, executor, worker, webserver 동작은 검증하지 않는다.

추가로 Airflow 3.3.0 `standalone`도 local에서 검증했다. 이 경로에서는 worker가 실제 shell에서 CLI를 실행하므로, Airflow venv 하나에 Airflow dependency뿐 아니라 project runtime dependency와 Spark dependency도 같이 설치돼 있어야 한다.

```bash
/tmp/manufacturing-mini-airflow-venv/bin/python -m pip install -r requirements-airflow.txt
/tmp/manufacturing-mini-airflow-venv/bin/python -m pip install -r requirements.txt -r requirements-spark.txt

export AIRFLOW_HOME=/tmp/manufacturing-mini-airflow-standalone-home
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export PYTHONPATH=src
export PATH="/tmp/manufacturing-mini-airflow-venv/bin:$PATH"

airflow standalone
```

재현용 runbook은 아래 스크립트다.

```bash
scripts/verify_airflow_standalone.sh
```

이 local standalone run에서 API server는 `127.0.0.1:8080`에 응답했고, scheduler는 project DAG 2개를 parse했으며, `airflow dags trigger manufacturing_iceberg_skeleton` manual run은 LocalExecutor 경로로 `dag=success`, `task=success`까지 확인했다.

정직한 경계: development-only local standalone 검증이다. production Airflow scheduler/worker deployment, cluster Spark, full Spark medallion pipeline은 아니다.

## Lakehouse to Iceberg DAG

`dags/manufacturing_lakehouse_to_iceberg_daily.py`는 두 task를 순서대로 실행한다.

```text
run_lakehouse_task -> publish_gold_to_iceberg_task
```

첫 task는 JSON-backed lakehouse CLI를 실행한다. 두 번째 task는 같은 `business_date`의 latest successful JSON catalog state를 읽고, 그 gold CSV를 local Iceberg table로 publish한다.

검증된 범위는 local `airflow dags test`다. DAG import, task ordering, command rendering, command execution은 확인했지만 production scheduler/worker/webserver deployment나 cluster Spark runtime은 아니다.

## Kafka K1 bounded raw ingestion

Kafka K1은 bounded local raw-ingestion proof로 구현했다. 공통 runbook은 Apache
Kafka 4.3.1 binary의 SHA-512를 확인하고 local KRaft broker 1개를 띄우며,
별도 virtualenv에 `confluent-kafka==2.15.0`을 설치한다.

환경만 확인하는 Test 0:

```bash
./scripts/verify_kafka_test0.sh
```

K1 전체 검증:

```bash
./scripts/verify_kafka_k1.sh
```

strict versioned JSON event를 `machine_id` key로 publish한다. bounded consumer는
payload와 `topic/partition/offset`을 immutable JSONL batch에 쓰고 fsync + atomic
rename 뒤에만 next offset을 commit한다. landing 뒤 commit 전 crash를 주입하면
같은 consumer group이 record를 다시 받고, 기존 coordinate를 재사용해 accepted set을
늘리지 않은 채 commit한다. bounded replay와 invalid-event quarantine도 검증한다.

evidence는 `/tmp/manufacturing-mini-kafka-k1-evidence`에 남고 broker는 자동 종료된다.
continuous streaming service, multi-partition routing/rebalance, multi-broker,
end-to-end exactly-once, Spark Structured Streaming, production Kafka 운영은 증명하지 않는다.

## 정직한 한계

- Spark/Iceberg는 단일 gold table walking skeleton까지만 구현됐다. full Spark medallion rewrite는 backlog다.
- runtime Mongo는 현재 환경에서 완전 검증되지 않았다. Airflow는 local `dags test`와 local `standalone` scheduler/LocalExecutor run까지만 검증했다.
- Kafka는 bounded local K1 raw landing과 복구/replay까지만 검증됐다. continuous/production streaming은 아니다.
- manufacturing strict numeric cast는 일부 bad row를 graceful quarantine하지 못하고 fail-fast한다.
- EAV 쪽은 unparseable value를 graceful quality failure로 잡는다.

## 읽는 순서

1. 이 파일
2. [`PROJECT_PROGRESS_MAP.ko.md`](PROJECT_PROGRESS_MAP.ko.md)
3. [`DESIGN.ko.md`](DESIGN.ko.md)
4. [`docs/scenario-state-map.md`](docs/scenario-state-map.md)
5. [`BENCHMARKS.ko.md`](BENCHMARKS.ko.md)
6. [`ROADMAP.ko.md`](ROADMAP.ko.md)

## 면접 답변용 설명

이 프로젝트는 synthetic CSV를 bronze/silver/gold로 처리하고, quality check와 schema drift, idempotent rerun, catalog/lineage 기록까지 남기는 작은 data platform입니다. 핵심은 단순 ETL이 아니라 운영자가 재처리, drift, 품질 실패, lineage를 inspect할 수 있는 metadata surface를 만든 점입니다.
