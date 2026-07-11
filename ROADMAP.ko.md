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
- [ ] **Airflow-triggered Spark runtime** — 미검증.

## 범위: CORE vs OPTIONAL

- **CORE** (thesis): medallion pipeline · EAV mini · quality checks · catalog/lineage · Spark/Iceberg.
- **OPTIONAL** (특정 면접이 실제로 관련될 때만 — 예: 래브라도랩스류): AI Dataset QA · RAG/vectorDB/LLM-preprocessing.

### BACKLOG (freeze — 앞으로 당겨오지 말 것)
CORE-backlog:
- [ ] **Full Spark/Iceberg 번역** — optional future work: 전체 `transform_*` engine을 Spark로 교체하고 더 많은 layer를 Iceberg/Delta로 저장. 현재 evidence는 단일 gold table walking skeleton이다.
- [ ] **Runtime Mongo verification** — 여기선 보류(Docker 엔진 없음). Mongo 경로는 `mongomock`으로 커버.
- [ ] **Runtime Airflow trigger verification** — 이 환경에 Airflow 미설치.
- [ ] **Task split** — one-task wrapper 안정화 후 `bronze_task → silver_task → gold_task → quality_task → catalog_task`로 분리.
- [ ] **Graceful null/bad-row quarantine** — manufacturing `transform_silver`의 strict cast는 아직 fail-fast (EAV는 이미 graceful 처리).

OPTIONAL-backlog (면접이 요구하기 전까지 구현하지 말 것):
- [ ] **AI Dataset QA slice** — text/sample dataset ingest → duplicate/empty/null/PII-mock checks → label distribution → train/validation split manifest → dataset version manifest → quality report → catalog/lineage.
- [ ] **RAG / vectorDB / LLM-preprocessing** — vector store를 짓는 게 아니라, 학습/RAG 진입 전 dataset의 quality/version/PII/분포를 관리하는 규율로 설명.

optional slice들은 동일한 `ingest → quality → catalog/lineage` spine을 재사용한다. 이 프로젝트는 SK/CJ/카카오뱅크류에는 Lakehouse/Data Mart/modeling/quality로, (optional slice를 통해) 래브라도랩스류에는 AI 학습데이터 quality/governance로 설명된다.

## Phase 3 — domain / streaming

- [ ] 모사 **ROS2 bag / MCAP-ish** ingest.
- [ ] **Kafka** streaming ingest 경로.

---
*원칙: 각 phase는 설명 가능한 산출물을 낸다. 현재 phase의 Done 기준이 체크되기 전에는 다음 phase를 시작하지 않는다.*
