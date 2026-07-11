# DESIGN — manufacturing-data-platform-mini (v0)

> "제대로 참고해서 만든" 설계. **결정 + 왜**를 남긴다(= 면접에서 보여줄 핵심).
> 참고(사례집): **honcho**(서비스 골격), **OpenMetadata/DataHub**(카탈로그 데이터 모델), **DVC·OpenLineage**(버전·lineage).
> ★ 원칙: 사례집은 *결정/패턴*만 참고하고 **scope에 맞게 덜어낸다.** 무거운 것(그래프 lineage·거버넌스·이벤트 스트림·브랜칭)은 v0에 안 넣음 = 과설계 회피.
> This document records implementation decisions and tradeoffs for the public learning project.

---

## 0. 목적 (왜 만드나)
로보티즈 지원자격의 **NoSQL/MongoDB + 메타데이터 카탈로그** 갭을 *만들어서* 닫고, 자소서 주장("MongoDB 카탈로그·version manifest·추출 API mini 구현")을 **면접에서 보여줄 최소 작동 버전**으로 뒷받침.

### Current strategy: deep design + small executable slice

이 프로젝트는 단순히 도구 이름을 모으는 repo 가 아니다. 반복적으로 확인되는 데이터 엔지니어링 JD gap 은 "Kafka/Spark 를 써봤는가"만이 아니라, **batch/streaming, data mart, quality, lineage, monitoring, backfill, failure recovery 를 하나의 데이터 플랫폼으로 설계하는 큰 그림**이다.

따라서 이후 slice 는 다음 순서로 진행한다:

```text
JD/public benchmark -> real-service scenario -> state changes -> required metadata
-> tables/files/functions/API design -> small executable slice -> tests/docs
```

설계는 실서비스 수준으로 깊게 쓰되, 구현은 검증 가능한 작은 범위로 자른다. 구현하지 않은 production 기능은 backlog 로 명시한다.

## 1. v0 scope
**v0 IN**: 파일 ingest → MongoDB 카탈로그 등록 → version manifest → `GET /datasets`·`GET /datasets/{id}`. Docker Compose(Mongo).
**v0.5 IN**: 추출 API(`/extract`) — 로보티즈 DaaS 키워드용, 코어 후.
**OUT(나중/다른 프로젝트)**: 그래프 lineage(→ source-tracker/lineage-lens) · ownership·tags 거버넌스 · 브랜칭/atomic commit(lakeFS) · 이벤트 스트림(DataHub) · 인증/멀티테넌시.

---

## 2. 서비스 골격 — honcho 참고 (덜어서)

| honcho 패턴 | mini v0 적용 | 왜 / 덜어낸 것 |
|---|---|---|
| API server / worker 분리 (enqueue→consume) | v0는 **ingest를 동기**로(파일→카탈로그 즉시). worker 분리는 보류 | 큐/worker는 de-job-runner의 훈련 주제. 카탈로그 v0엔 과함 |
| `/v3/{resource}/{id}/{action}` 라우팅 | `/datasets`, `/datasets/{id}`, `/extract` | REST 자원 중심 — honcho 컨벤션 차용 |
| config 우선순위 env>.env>config>defaults | 동일 (env > .env > 기본값) | 환경 분리 패턴 그대로 |
| 마이그레이션(Alembic) | Mongo라 스키마리스 — 단 **카탈로그 document에 `schema_version` 필드**로 변경 추적 | "변경 모델"을 가볍게 흉내 |
| Docker Compose로 의존성 | `docker-compose.yml`에 MongoDB | clone 후 1분 내 실행(honcho 정신) |

## 3. 데이터 모델 — OpenMetadata/DVC 참고 (덜어서)

### 결정 ①: `dataset`(정의) vs `dataset_version`(적재 1회) 분리
- **왜**: OpenMetadata의 dataset entity + lakeFS/DVC의 version, 그리고 (네가 이미 푼) **job vs job_run** 과 같은 사고. 같은 데이터셋이 매달 새로 들어와도(temp_sensor 2025-01, 02…) **정의는 하나, 적재본은 여러 개**. 과거 적재 기록은 재현성 위해 보존.

### `datasets` 컬렉션 (= 카탈로그 '명함', OpenMetadata dataset 축소판)
```json
{
  "dataset_id": "temp_sensor",
  "description": "센서 온습도 로그",
  "latest_version": "v3",
  "schema": [ {"name":"timestamp","type":"datetime"}, {"name":"sensor_id","type":"string"}, ... ],
  "created_at": "...", "updated_at": "..."
}
```
> 덜어낸 것: ownership·tags·urn·lineage 그래프 (거버넌스/관계는 v0 제외).

### `dataset_versions` 컬렉션 (= manifest, DVC/OpenLineage 축소판)
```json
{
  "dataset_id": "temp_sensor",
  "version": "v3",
  "source": "temp_sensor_2025-01.csv",
  "source_hash": "sha256(파일내용)",     // 재현성: 같은 파일=같은 버전
  "schema_hash": "sha256(스키마)",        // 스키마 드리프트 감지
  "row_count": 1000,
  "stats": { "null_counts": {"humidity": 12} },  // 품질 — 네 source-tracker DNA
  "ingested_at": "..."
}
```
> 덜어낸 것: parent version 체인·diff·브랜칭 (v0은 평면 버전 목록).

**결정 ②: `schema`를 왜 따로 저장?** — 데이터는 파일에 있지만, ML팀/사용자는 *데이터를 열기 전에* "이 데이터셋에 무슨 컬럼이 있나"를 알아야 함(검색·계획). 카탈로그의 존재 이유 = **데이터 안 열고도 알 수 있게.** (OpenMetadata 핵심도 이것.)
**결정 ③: `source_hash`·`schema_hash` 왜?** — 재현성/드리프트. 같은 source_hash = 같은 데이터(재현). schema_hash 바뀜 = 스키마 드리프트(컬럼 추가/타입 변경) 자동 감지.

## 4. API 계약 (★ 진짜 게이트 = MongoDB 카탈로그. extract는 나중 — Codex 피드백)
**v0 코어 (이것부터):**
- `POST /datasets/{id}/ingest` — 파일 받아 카탈로그+버전 등록
- `GET /datasets` — 목록/검색 (이름·스키마로)
- `GET /datasets/{id}` — 명함 + 버전 목록

**v0.5 (시간 남으면):**
- `GET /datasets/{id}/extract?version=&columns=` — 조건부 추출 (로보티즈 DaaS 키워드용. 단 Phase1 게이트는 카탈로그라 우선순위 뒤.)

> stats는 v0에서 **row_count + null_counts까지만.** 평균·분포·이상치는 나중.

---

## 5. 면접 연결 (이 설계가 주는 어휘)
- "메타데이터 카탈로그를 OpenMetadata식 base 모델로 단순화해 구현" / "재현성을 source_hash·schema_hash manifest로(DVC·OpenLineage 사상)" → 로보티즈·캐치잇 거버넌스 질문 정조준.
- source-tracker(lineage 진단)와 OpenLineage(lineage 표준)를 연결해 말할 수 있음.

## 6. 다음 (구현 = Codex)
1. docker-compose(Mongo) + FastAPI 골격
2. ingest → `datasets`·`dataset_versions` 등록 (source_hash·schema_hash·row_count·null_counts)
3. `GET /datasets` · `GET /datasets/{id}`
4. README에 이 DESIGN 요약 + "honcho/OpenMetadata 참고, v0로 덜어냄" 명시.
5. (v0.5, 시간 남으면) `GET /datasets/{id}/extract`

## 8. Phase 2 Lakehouse Slice

Phase 2 keeps Phase 1's Mongo catalog intact and adds a lakehouse-style pipeline as a separate module:

```text
synthetic manufacturing CSV -> bronze -> silver -> gold -> quality -> Mongo catalog/lineage
```

Design decisions:
- Pipeline logic lives under `manufacturing_data_platform.pipeline` and is executable by CLI.
- Airflow is only the operational wrapper: schedule, retry, timeout, date parameters, and manual trigger config.
- `business_date` is explicit and can come from CLI args or Airflow `dag_run.conf`.
- `source_hash` and `schema_hash` are the idempotency/drift primitives.
- Lineage is stored as layer parent paths: source -> bronze -> silver -> gold.
- Sample data is fully synthetic manufacturing data.

### Slice 1 hardening (2026-06-30) — closing claim↔code gaps

A review found four places where the docs claimed more than the code did. These are now closed in code + tests (`tests/test_lakehouse_pipeline.py`); the decisions:

**결정 ④: transform 과 IO 를 분리** — `transform_silver(rows, business_date, source_hash)` 와 `transform_gold(silver_rows, business_date)` 는 순수 함수, `write_silver`/`write_gold` 는 파일 쓰기만. 왜: Slice 2 에서 Spark 로 갈 때 **엔진만 교체**(transform 함수 내부)하면 되고 orchestration 은 그대로. (Kedro/Dagster node 분리 + dbt model/test 분리 사상.)

**결정 ⑤: quality 를 진짜 DQ 로** — 이전 `bronze_source_row_count` 는 `len(source)` vs `len(source)` 라 절대 실패 못 하는 tautology 였음 → 제거. 대신 dbt generic test 어휘의 suite: `not_null` · `unique`(natural key) · `accepted_values`(operation) · numeric range · freshness, 그리고 핵심으로 **`row_count_source_to_silver` reconciliation 이 정상 filtering/dedup 과 실제 row 손실을 구분**한다. `expected` = active date 의 distinct natural key 수를 **silver 생성 방식과 독립적으로** 계산 → 불일치 = 진짜 손실. 각 check 는 `{name, status, expected, actual, detail}`.

**결정 ⑥: schema drift 를 실제로 비교** — `schema_hash` 를 직전 **successful** run 과 비교(`lookup_previous_schema_hash`). 다르면 `schema_drift` check 에 기록. **정책 = `warn`** (run 을 실패시키지 않고 surface) — 이유: 정당한 schema evolution(컬럼 추가 등)을 막지 않기 위해(Iceberg schema-evolution 사상). `SCHEMA_DRIFT_POLICY="fail"` 로 바꾸면 hard gate. run/lineage doc 에 저장.
> ★ self-audit 수정(2026-06-30): `schema_hash` 는 **실제 CSV 헤더** 기준(`read_rows` 가 헤더를 반환)으로 계산한다. 이전엔 고정 `REQUIRED_COLUMNS` 기준이라 **컬럼 추가/삭제 drift 를 놓쳤다** (type 변경만 감지). 이제 컬럼 추가/삭제도 감지 → "컬럼 추가/타입 변경 감지"라는 §3 결정 ③ 주장이 lakehouse 경로에서도 참이 됨. required column 누락은 기존대로 `read_rows` 의 `ValueError`.

**결정 ⑦: idempotency 정책 = skip-on-reuse** — `dataset_id + business_date + source_hash` 에 이미 successful run 이 있으면 **재처리하지 않고 기존 run 을 반환**(`status="skipped"`, `reuse_count` 증가). 왜: retry/backfill 이 같은 날짜+입력에 대해 안전한 no-op 이 되어야 함. (Phase 1 catalog 의 source_hash dedup 과 같은 사고를 lakehouse 층에 적용.) mongo 는 `lakehouse_runs` 조회, json backend 는 `_state/` 포인터 파일로 동일하게 동작.

> 정직 가드: `transform_silver` 의 numeric cast 는 strict — 파싱 불가 숫자는 graceful quality `fail` 이 아니라 transform 시점에 fail-fast 한다. graceful quarantine 은 **backlog**. runtime Mongo는 미검증이며, Airflow는 local `dags test` wrapper까지만 검증됐다.

Airflow reference patterns extracted from private code, without copying code or names:
- DAGs are thin orchestration files that assemble configured tasks and pass a shared config object.
- Shared helper modules centralize schedule calculation, date-window parsing, task factories, and task callables.
- Manual runs override scheduled dates through trigger config; scheduled runs derive the processing window from Airflow context.
- Retries, retry delay, email/failure handling, timeout, and catchup policy are DAG/task-level operations.
- Source/customer variation is expressed as configuration lists with enable flags and per-source options.
- Business logic stays in external functions/operators; DAG files mainly define dependencies.

### Phase 2 — EAV mini slice (2026-06-30) — CORE

데이터 모델링 + 다양한 양식 intake 를 같은 spine 으로 (fork 금지). 모듈 `pipeline/eav.py`, dataset_id `manufacturing_wide_eav`.

```text
여러 wide CSV(컬럼·단위 제각각) -> mapping config(JSON) -> EAV long -> pivot/aggregate -> gold -> quality -> catalog/lineage
```

**결정 ⑧: config-driven 매핑** — source 별 `config/eav_mappings/*.json` 가 컬럼 → 표준필드(`units_produced·defect_count·temperature_c·pressure_kpa`) + 선택적 단위 변환(`f_to_c`·`bar_to_kpa`)을 선언. **새 양식 = config 하나 추가**(코드 변경 X) — 테스트로 증명. load 는 config-driven(각 mapping 이 자기 `source_file` 을 가리킴).

**결정 ⑨: EAV(long)를 silver 로** — `entity_id·business_date·attribute·value·value_type·source_id·source_file_id`. `source_file_id` = 파일 내용 해시 = **file-level 멱등 키**(내가 직접 재설계/구현으로 방어 가능한 부분). EAV 는 모든 날짜 보존, 날짜 필터는 gold 에서.

**결정 ⑩: gold 는 pivot/aggregate** — `(business_date, entity_id)` 그레인, count 는 sum / sensor 값은 avg. `eav_to_gold_conservation` 으로 additive measure 보존 검증.

**결정 ⑪: graceful 처리** — EAV 의 `normalize_value` 는 파싱 불가/빈 값을 crash 가 아니라 `value=None` + `value_type_valid` quality fail 로 잡는다 (manufacturing silver 의 strict fail-fast 와 대비 — EAV 는 양식이 제각각이라 graceful 이 맞음).

> ★ 정직 가드 (EAV claim): 실무에서는 EAV 기반 구조를 **운영·개선**(다양한 양식 처리), 이 공개 프로젝트에서는 **가상 데이터로 직접 구현**해 모델링 이해 보강. "실무에서 EAV 를 설계했다" 라고는 쓰지 않는다. 회사 코드/데이터/이름/스키마 미사용 (clean-room).

### Phase 2 Extension: AI Dataset QA (OPTIONAL — 면접 시에만)

> 이 slice 는 **optional** 이다. 래브라도랩스류 면접이 실제로 진행될 때만 후속 slice 로 붙인다. core(medallion·EAV·quality·catalog/lineage·Spark/Iceberg) 가 먼저다.

The Labrador Labs-style gap should be covered by a later slice in the same project, not by a new repo and not by expanding Slice 1. The reusable core is `ingest -> version manifest -> quality report -> catalog/lineage`; only the dataset shape and QA checks change.

Planned AI Dataset QA flow:

```text
synthetic text/sample dataset -> duplicate/empty/null checks -> PII mock detection -> label distribution -> train/validation split manifest -> dataset version manifest -> quality report -> Mongo catalog/lineage
```

This lets the same implementation translate into different job languages:
- SK/CJ/KakaoBank-style explanation: lakehouse, data mart, ETL/ELT, Spark/Iceberg, data quality, lineage.
- Labrador Labs-style explanation: AI training dataset quality, QA automation, governance, LLM preprocessing readiness, RAG/vectorDB-adjacent metadata discipline.

## 7. ★ Done 기준 (이게 있어야 "작게 만들기"가 끝남 — v0 안 늘어나게)
- [ ] `docker compose up`으로 Mongo 실행
- [ ] 샘플 CSV ingest 성공
- [ ] `datasets`·`dataset_versions`에 document 생성
- [ ] `source_hash`·`schema_hash`·`row_count`·`null_counts` 저장
- [ ] `GET /datasets/{id}`로 확인 가능
- [ ] README에 실행 명령 + 설계 결정 3개(dataset/version 분리·schema 저장 이유·hash) 설명
→ 6개 다 체크되면 Phase 1 **완료**. extract는 별개(v0.5).
