# 로드맵 (ROADMAP)

> 이 문서는 [`ROADMAP.md`](ROADMAP.md)의 한글 번역이다. **원본(source of truth)은 영어 `ROADMAP.md`** — 내용이 어긋나면 영어판을 따른다. 기술 용어는 영어 그대로 둔다.

## Phase 1 v0 — MongoDB catalog gate

현재 active scope. 로보티즈 갭 중 가장 가까운 것부터 닫는다: **NoSQL/MongoDB + metadata catalog**.

- [x] **MongoDB metadata catalog** — sensor 성격의 CSV를 ingest하고 `schema · row/null stats · source · ingested_at`를 등록.
- [x] **Version manifest** — `dataset_id · version · source_hash · schema_hash · ingested_at · row_count`.
- [x] **FastAPI catalog endpoints** — `GET /datasets`, `GET /datasets/{id}`.
- [x] **README 설계 근거** — 아키텍처, tradeoff, 실행 명령, Done 체크리스트.
- [ ] **Runtime Mongo verification** — `docker compose up` + 실제 Mongo ingest. 현재 환경에서는 Docker Desktop 엔진 미가동으로 **보류**.

v0 종료 = catalog loop가 구현되고 test로 덮인 상태. Docker 가능한 머신에서 runtime Mongo가 검증되면, 자소서 주장을 "구현 착수"에서 "catalog/version manifest 구현 완료"로 올릴 수 있다.

## Phase 1 v0.5 — DaaS extract

- [ ] `GET /datasets/{id}/extract?version=&columns=` — 로보티즈 DaaS 키워드용 조건부 extract.

## Phase 1 v1 — orchestration polish

- [ ] Docker Compose 위 self-hosted Airflow orchestration: ingest → catalog → version → serve.

나중에 유용하지만, 지금의 MongoDB/catalog gate 범위는 아니다.

## Phase 2 — Mini Lakehouse

### Slice 1 (구현 완료)
- [x] **Slice 1 CLI** — 합성 manufacturing CSV → bronze → silver → gold → quality → Mongo catalog/lineage.
- [x] **Airflow wrapper** — `dags/manufacturing_lakehouse_daily.py`가 CLI를 운영 wrapper로 trigger.

### Slice 1 hardening — NOW (이번 pass에서 구현)
목표: 주장(data quality, schema drift, idempotency, transform/IO 분리)을 코드에서 실제로 참으로 만든다.
- [x] **transform/IO 분리** — 순수 `transform_silver` / `transform_gold`; `write_*`는 IO만 (Spark 엔진 교체 준비).
- [x] **Quality suite** — dbt-style checks (`not_null`, `unique`, `accepted_values`, range, freshness) + 정상 filtering/dedup과 실제 row 손실을 구분하는 reconciliation. tautology check 제거.
- [x] **Schema drift** — `schema_hash`를 실제 CSV 헤더 기준으로 계산(컬럼 추가/삭제 감지), 직전 successful run과 비교; `schema_drift` check, 정책 = `warn`; run/lineage doc에 저장.
- [x] **Idempotency** — `dataset_id + business_date + source_hash`에 이미 성공 run이 있으면 재실행 skip (안전한 retry/backfill).
- [x] **BENCHMARKS.md** — reference 패턴, JD 매핑, anti-benchmark (의도적 제외).

### EAV mini slice — CORE (이번 pass에서 구현)
목표: data modeling + 다양한 양식 intake를, Slice 1 spine을 재사용해서 (fork 없이).
- [x] **3개 합성 wide 양식** — 컬럼/단위 제각각 (한글/영문 헤더, °F/bar), 완전 가상 (`sample_eav.py`).
- [x] **Config-driven mapping** — `config/eav_mappings/*.json` → 표준 필드 + 결정적 단위 변환 (`f_to_c`, `bar_to_kpa`). 새 양식 = config 하나 추가 (테스트로 증명).
- [x] **wide → EAV (long) → gold pivot/aggregate** — 순수 `transform_to_eav` / `transform_eav_to_gold`.
- [x] **EAV quality suite** — `mapping_coverage`, `unmapped_source_columns` (warn), `not_null_value`, `accepted_values_attribute`, `value_type_valid`, `numeric_range`, `eav_to_gold_conservation`, `freshness` + 공유 `schema_drift`.
- [x] **Catalog/lineage + idempotency 재사용** — 동일한 `lakehouse_runs`/`lineage_events`, `file_id`(파일 해시) idempotency.

### Spark/Iceberg walking skeleton — CORE-lite (구현 완료)
목표: full Spark rewrite가 아니라, 정정된 `business_date`를 중복 없이 교체하는 storage/table contract를 증명한다.
- [x] **Optional Spark dependency pin** — `requirements-spark.txt`에서 `pyspark==3.5.8` 고정.
- [x] **Local Iceberg catalog** — Spark hadoop catalog + local warehouse.
- [x] **단일 gold table** — `local.db.gold_daily_metrics`, `business_date` partition.
- [x] **Partition overwrite** — 정정 row는 `DataFrameWriterV2.overwritePartitions()` 사용.
- [x] **안전성 assertion** — 대상 날짜는 중복 없이 교체되고, 다른 날짜 partition은 유지됨.
- [x] **Snapshot evidence** — `run_id -> snapshot_id` evidence JSON; 같은 `source_hash` rerun은 새 snapshot을 만들지 않음.
- [ ] **Full medallion Spark rewrite** — 의도적으로 미구현.
- [x] **Airflow-triggered Spark runtime (local `dags test` + standalone)** — local Airflow가 `dags test`와 development `standalone` scheduler/LocalExecutor run 두 경로로 Spark/Iceberg skeleton을 trigger.

### Airflow runtime wrapper — CORE-lite (구현 완료)
목표: business logic을 DAG 안으로 옮기지 않고, Airflow가 같은 lakehouse CLI task를 local runtime에서 trigger할 수 있음을 증명한다.
- [x] **Optional Airflow dependency pin** — `requirements-airflow.txt`에서 `apache-airflow==3.3.0`, `apache-airflow-providers-standard==1.15.0` + Python 3.10 공식 constraints 사용.
- [x] **DAG import** — `airflow dags list`에서 `manufacturing_lakehouse_daily` 로드 확인.
- [x] **Task discovery** — `airflow tasks list manufacturing_lakehouse_daily`에서 `run_pipeline_task` 확인.
- [x] **Local runtime trigger** — `airflow dags test`가 BashOperator를 실행하고 JSON catalog CLI 성공.
- [x] **Retry/idempotency boundary** — 같은 `dags test` 재실행 시 pipeline `status="skipped"` 확인.
- [x] **Runtime conf** — `dag_run.conf`로 `business_date`, `raw_path`, `output_dir`, `catalog_backend` 전달.
- [ ] **Scheduler/worker/webserver deployment** — 의도적으로 미구현.

### Airflow-triggered Spark/Iceberg skeleton — CORE-lite (구현 완료)
목표: Spark logic을 DAG 안으로 옮기지 않고, local Airflow가 기존 Spark/Iceberg partition-overwrite skeleton을 trigger할 수 있음을 증명한다.
- [x] **DAG wrapper** — `dags/manufacturing_iceberg_skeleton.py`가 Spark/Iceberg CLI 호출.
- [x] **Command contract** — `build_spark_iceberg_cli_command` test-covered.
- [x] **DAG parse contract** — Airflow가 설치된 환경에서 optional DagBag test가 DAG id, task id, BashOperator command를 검증.
- [x] **Local runtime trigger** — `airflow dags test manufacturing_iceberg_skeleton` 성공.
- [x] **Local standalone scheduler trigger** — Airflow 3.3.0 `standalone`이 API server/scheduler/dag-processor/triggerer를 띄우고, manual `airflow dags trigger` run이 LocalExecutor 경로로 성공.
- [x] **Standalone verification runbook** — `scripts/verify_airflow_standalone.sh`가 startup, trigger, state polling, evidence assertion, cleanup을 재현.
- [x] **Worker dependency packaging** — standalone worker venv는 `requirements-airflow.txt`, `requirements.txt`, `requirements-spark.txt`를 모두 가져야 함.
- [x] **Iceberg evidence** — `run_snapshot_map.json`, `current_gold.json`, `snapshot_comparison.json` 생성.
- [x] **Partition overwrite assertions** — `snapshot_increment=1`, `same_source_created_snapshot=false`, 대상 날짜 교체, 다른 날짜 유지.
- [ ] **Production Airflow scheduler/worker deployment** — 의도적으로 미구현.
- [ ] **Cluster Spark / full Spark medallion pipeline** — 의도적으로 미구현.

### Lakehouse gold -> Iceberg publish DAG — CORE-lite (구현 완료)
목표: full Spark rewrite 없이, 구현된 JSON-backed lakehouse pipeline을 local Iceberg current table에 연결한다.
- [x] **Publish CLI** — `publish_gold_to_iceberg`가 특정 `business_date`의 latest successful JSON catalog state를 읽음.
- [x] **Gold CSV publish** — 선택된 run의 gold CSV를 `local.db.gold_daily_metrics`에 기록.
- [x] **Partition overwrite** — publish는 `DataFrameWriterV2.overwritePartitions()` 사용.
- [x] **Publish idempotency** — 같은 `pipeline_run_id + source_hash` 재발행은 새 snapshot 없이 skip.
- [x] **Airflow DAG** — `manufacturing_lakehouse_to_iceberg_daily`가 `run_lakehouse_task -> publish_gold_to_iceberg_task` 순서로 실행.
- [x] **Command contract + DAG parse tests** — 새 DAG의 orchestration과 optional Airflow DagBag test 추가.
- [x] **Local runtime trigger** — `airflow dags test manufacturing_lakehouse_to_iceberg_daily` 성공.
- [ ] **Mongo-backed publish lookup** — runtime Mongo 검증 전까지 의도적으로 미구현.
- [ ] **Full Spark medallion pipeline / Spark quality suite** — 의도적으로 미구현.

### Kafka raw ingestion — K1 (구현 및 local broker 검증 완료)
목표: Spark Structured Streaming을 검토하기 전에 bounded log-based raw ingestion을 증명한다.
- [x] **Kafka Test 0 runtime pin** — Apache Kafka 4.3.1 KRaft binary + SHA-512 검증.
- [x] **Python client pin** — 별도 환경에 `confluent-kafka==2.15.0` 고정.
- [x] **Broker/client round-trip** — local broker 1개, topic 1개, partition 1개, event 1건, manual offset commit.
- [x] **재현 runbook** — `scripts/verify_kafka_test0.sh`가 broker 기동, 검증, 종료를 재현.
- [x] **K1 event/source contract** — strict JSON v1, `event_id`, `machine_id` key, Kafka coordinate evidence.
- [x] **K1 immutable raw landing** — bounded consumer가 payload + Kafka coordinates를 fsync + atomic rename으로 기록.
- [x] **K1 recovery evidence** — durable landing 뒤 commit 전 crash, redelivery reuse, offset 복구, bounded replay.
- [x] **K1 quarantine evidence** — invalid event를 durable quarantine하고 single partition 진행 유지.
- [x] **K1.5 landing -> batch bridge** — provenance를 보존하는 결정적 CSV로 기존 quality/gold/Iceberg 경로를 재사용하고 같은 입력은 skip.
- [ ] **Spark Structured Streaming** — window/watermark/latency pressure가 생길 때까지 Backlog.

### Spark machine-event batch — S7 (구현 및 local runtime 검증 완료)
목표: landing된 한 `business_date`의 기존 Python silver/gold를 full medallion rewrite나 streaming 없이 Spark로 다시 표현한다.
- [x] **Adapter 입력 계약** — K1.5 canonical CSV + `source_hash`를 재사용하고, Spark가 raw JSONL을 다시 해석하지 않는다.
- [x] **Engine parity** — Spark DataFrame built-in이 `transform_silver`/`transform_gold`의 grain과 합계를 동일하게 재현(`802.675` 같은 boundary에서 Python `round`와 일치하는 `format_number` 기반 반올림, coordinate 순서 natural-key dedup 포함).
- [x] **Spark quality gate** — 기존 quality suite를 Spark 결과에 적용하고, 실패 시 Iceberg write와 success pointer를 모두 막는다.
- [x] **Partition overwrite + idempotency** — `overwritePartitions()`, 같은 source는 skip(새 snapshot 없음), 정정 source는 새 snapshot 1개, 다른 날짜 partition은 보존.
- [x] **Shuffle-plan evidence** — gold `groupBy`의 executed plan과 `Exchange` 관찰을 학습 evidence로 기록(성능 claim 아님).
- [x] **얇은 Airflow wrapper** — single-task DAG가 검증된 CLI 하나만 호출하고 `max_active_runs=1`, DAG body에 transform 로직 없음.
- [ ] **Cluster/분산 Spark, 성능·throughput claim** — 의도적으로 미구현.

## 범위: CORE vs OPTIONAL

- **CORE** (thesis): medallion pipeline · EAV mini · quality checks · catalog/lineage · local Spark/Iceberg · bounded Kafka K1/K1.5 · S7 Spark machine-event batch.
- **OPTIONAL** (특정 면접이 실제로 관련될 때만 — 예: 래브라도랩스류): AI Dataset QA · RAG/vectorDB/LLM-preprocessing.

### BACKLOG (freeze — 앞으로 당겨오지 말 것)
CORE-backlog:
- [ ] **Full Spark/Iceberg 번역** — optional future work: 전체 `transform_*` engine을 Spark로 교체하고 더 많은 layer를 Iceberg/Delta로 저장. 현재 evidence는 단일 gold table walking skeleton이다.
- [ ] **Runtime Mongo verification** — 여기선 보류(Docker 엔진 없음). Mongo 경로는 `mongomock`으로 커버.
- [x] **Runtime Airflow trigger verification** — local Airflow 3.3.0 `dags test`로 CLI wrapper 검증; local `standalone` scheduler/LocalExecutor로 Spark/Iceberg wrapper 검증.
- [ ] **Production Airflow scheduler/worker/webserver deployment** — 미구현.
- [ ] **Task split** — one-task wrapper 안정화 후 `bronze_task → silver_task → gold_task → quality_task → catalog_task`로 분리.
- [ ] **Graceful null/bad-row quarantine** — manufacturing `transform_silver`의 strict cast는 아직 fail-fast (EAV는 이미 graceful 처리).

OPTIONAL-backlog (면접이 요구하기 전까지 구현하지 말 것):
- [ ] **AI Dataset QA slice** — text/sample dataset ingest → duplicate/empty/null/PII-mock checks → label distribution → train/validation split manifest → dataset version manifest → quality report → catalog/lineage.
- [ ] **RAG / vectorDB / LLM-preprocessing** — vector store를 짓는 게 아니라, 학습/RAG 진입 전 dataset의 quality/version/PII/분포를 관리하는 규율로 설명.

optional slice들은 동일한 `ingest → quality → catalog/lineage` spine을 재사용한다. 이 프로젝트는 SK/CJ/카카오뱅크류에는 Lakehouse/Data Mart/modeling/quality로, (optional slice를 통해) 래브라도랩스류에는 AI 학습데이터 quality/governance로 설명된다.

## Phase 3 — 산업 시나리오 (scenario-led)

Phase 3은 기술 목록이 아니라 **운영자 시나리오와 실패 압력**으로 정리한다. 구현된 사실은 위 Phase 2 절에 그대로 두고, 여기서는 증명된 것 / 제안된 것 / 의도적으로 먼 것만 나눈다.

### 구현된 foundation (위에서 이미 증명됨)

- [x] **bounded Kafka raw landing(K1)과 landing -> batch bridge(K1.5)** — `### Kafka raw ingestion — K1` 참조.
- [x] **Spark machine-event batch(S7)** — Python parity와 quality-gated Iceberg publish. `### Spark machine-event batch — S7` 참조.
- [x] **edge/cloud 단절 복구(S8)** — immutable sealed edge spool, 기존 local Kafka/K1 landing으로 replay, 봉인 구간이 완전히 복구되기 전에는 downstream batch 차단. synthetic·local·bounded·단일 machine/session/partition 시뮬레이션. slice: [`learn/system-design/slices/08-edge-cloud-recovery.ko.md`](learn/system-design/slices/08-edge-cloud-recovery.ko.md).

### 제안된 다음 시나리오 (미구현)

운영자 시나리오에서 도출하고 공식 산업 플랫폼 문서와 대조했다(`BENCHMARKS.ko.md` 산업 lane 참조). 각 항목은 bounded slice로 설계·검증되기 전까지 `Proposed`다.

- [ ] **sensor/tag/단위/schema 교체** — EAV mapping config와 schema-drift check 재사용.
- [ ] **의심스러운 품질 지표를 source/telemetry까지 역추적** — operator evidence report 확장.
- [ ] **late/out-of-order telemetry와 sequence gap** — 실제 late-data/window 압력이 명명될 때만.
- [ ] **asset/시계열/문서 contextualization** — 이 프로젝트 규모로 축소한 cross-source identity 해소.

### Backlog / Unknown (먼 범위 — 당겨오지 말 것)

- [ ] 모사 **ROS2 bag / MCAP-ish** ingest.
- [ ] 실제 PLC/센서/로봇 source, OPC UA / MQTT / ROS 2 / DDS 연동.
- [ ] product 수준의 edge gateway 또는 단절 durable buffer.
- [ ] continuous/event-time streaming, watermark, Flink 또는 Spark Structured Streaming.
- [ ] asset hierarchy / Unified Namespace / digital twin.
- [ ] anomaly 모델, 예지보전, closed-loop 제어.
- [ ] production / HA / cluster 운영.

---
*원칙: 각 phase는 설명 가능한 산출물을 낸다. 현재 phase의 Done 기준이 체크되기 전에는 다음 phase를 시작하지 않는다.*
