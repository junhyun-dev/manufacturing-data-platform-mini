# Claude Audit — Industrial Data Platform Direction (2026-07-21)

> Status: accepted-closed after Codex corrections
>
> Mode: Delegated Documentation · review profile: System Traceability + Public Reader + External Benchmark.
> 모든 분석과 direct edit은 candidate이며 Codex 검토 대상이다. 구현은 실행/수정하지 않았다.

**Preflight**: HEAD `862527b Close completed audit lifecycles` = target commit. `git status` = clean except this untracked request package. 상태 불일치 없음. `VERIFICATION_LOG.md`와 현재 code/tests를 implementation truth로 사용했고, 과거 Kafka/Spark 결론을 재사용하지 않았다.

---

## A. Executive Verdict

**A1. 현재 foundation은 일관적인가 — YES.**

S0~S7은 기능 나열이 아니라 **하나의 척추**를 반복한다.

```text
source contract -> identity(source_hash/event_id/coordinate) -> quality gate
-> trusted state 전진 여부 -> publish(partition overwrite) -> evidence/claim boundary
```

Kafka(S5/S6)와 Spark(S7)는 새 플랫폼이 아니라 **같은 척추의 입력 경로·실행 엔진 확장**이고, `01-system-traceability-map.ko.md`가 이미 scenario→question→contract→feature→evidence로 연결한다. 각 slice가 "무엇을 주장하지 않는가"를 명시한 점이 이 repo의 최대 강점이다.

**A2. 가장 강한 plain-language service thesis**

> **"합성 제조 데이터를 믿고 쓸 수 있는 지표로 바꾸고, 그 숫자가 어느 입력·실행·품질 판정에서 나왔는지 설명하고 재현할 수 있게 하는 local 데이터 플랫폼."**

3 actor로 풀면:

| Actor | 이 플랫폼에서 얻는 것 |
|---|---|
| **plant data operator** (현장 데이터 담당) | 오늘 치 데이터가 다 들어왔는지, 빠진 구간을 어떻게 다시 넣는지, 재처리해도 중복되지 않는지 |
| **process/quality engineer** (공정·품질) | gold 지표를 믿어도 되는지, 품질검사가 통과했는지, 이상하면 어느 source/run에서 왔는지 |
| **platform operator** (플랫폼 운영) | 실패가 어디서 났고 무엇이 전진하지 않았는지, 같은 입력 재실행이 안전한지, 정정이 어떤 partition/snapshot을 바꿨는지 |

**A3. 가장 가치 있는 다음 시나리오 (단 1개)**

> **S8 후보 — "edge/cloud 단절 후 재연결 replay"** (§D-1, §E). AWS와 Azure가 이 압력을 직접 다루고(§C), S0~S7 자산(K1 landing-before-commit·offset replay·`source_hash` idempotency·quality gate)을 **거의 그대로 재사용**하며, 첫 proof는 새 프로토콜·하드웨어 없이 **파일/큐 경계 시뮬레이션만으로** 증명 가능하다. 이는 실제 edge runtime 증거가 아니다. 상태는 `Proposed`이며 구현 package를 만들지 않았다.

---

## B. Korean Documentation Audit (EN/KO drift)

reader-visible gap만 표기한다. 휘발성 test count는 어느 overview 문서에도 넣지 않는다(현재 0건 — §B2).

| # | 파일 | Drift (reader-visible) | 조치 |
|---|---|---|---|
| B-1 | `README.ko.md` | **S7 전용 섹션 없음.** line 35 capability 목록에 이름만 있고, K1/K1.5처럼 흐름·재현 경로·경계를 설명하는 절이 없다. EN도 전용 절은 없으나 `Scope: core vs optional`(README.md:95)에 S7 한 줄이 있다. | **직접 수정**: 간결한 S7 절 추가(기존 EN claim 범위 내, 새 주장 없음) |
| B-2 | `README.ko.md:293` | 정직한 한계가 **"Spark/Iceberg는 단일 gold table walking skeleton까지만 구현됐다"** — S7이 한 slice의 silver/gold를 Spark로 재표현했으므로 부정확(단 full medallion이 backlog인 것은 여전히 사실) | **직접 수정**: S7 사실을 반영하되 full medallion backlog는 유지 |
| B-3 | `ROADMAP.ko.md` | **S7 섹션 없음.** EN `ROADMAP.md:109 "### Spark machine-event batch — S7"`에 해당하는 한국어 절이 부재 | **직접 수정**: EN과 동일 사실로 S7 절 추가 |
| B-4 | `ROADMAP.md:163-166` / `ROADMAP.ko.md:129-132` | **Phase 3가 stale**: `- [ ] **Kafka** streaming ingest path` / `- [ ] **Kafka** streaming ingest 경로`가 미체크인데 K1/K1.5는 구현·검증됨. 또한 Phase 3가 "기술 나열"이라 scenario-led가 아님 | **직접 수정**: Implemented Foundation / Proposed Next Scenarios / Backlog·Unknown 3분할로 교체(K1·K1.5·S7 사실 보존) |
| B-5 | `BENCHMARKS.md:148` | BACKLOG-core에 **"Kafka K1.5 batch adapter decision"**이 남아 있으나 K1.5는 구현 완료. NOW 목록에도 K1.5·S7 누락 | **직접 수정** |
| B-6 | `BENCHMARKS.ko.md:59,83` | JD mapping에 **K1.5·S7 행 없음**, CORE에 `Spark/Iceberg translation backlog`(S7이 부분 수행) | **직접 수정** |
| B-7 | 양쪽 BENCHMARKS | 산업(OT/edge/contextualization) 벤치마크 lane이 **아예 없음** → §C 결과를 반영할 자리 부재 | **직접 수정**: pressure→local decision 매핑으로 추가(벤더 기능 나열 금지) |
| B-8 | `BENCHMARKS.md` JD 표 | **역드리프트**: KO(`BENCHMARKS.ko.md:59`)에는 `Kafka K1` 행이 있는데 **EN JD 표에는 Kafka 행이 없다** | **직접 수정**: EN에 Kafka/S7 행 추가로 정렬 |
| B-9 | `BENCHMARKS.ko.md:61` Anti-benchmark | EN(`:121`)은 `Excluded \| Why it is out` 2열로 **제외 근거**를 주는데, KO는 근거 열 없는 평면 목록 → 한국어 독자가 **rationale 축을 통째로 잃음** | **직접 수정**: 근거 열 복원 |
| B-10 | `DESIGN.md` / `DESIGN.ko.md` | **양쪽 모두 K1.5·S7 언급 0건** (두 slice 뒤처짐). 단 DESIGN은 이번 패키지 편집 허용 목록에 **없음** | **미수정 → Codex 후속**(§H-7) |
| B-11 | `README.md:32` | "Spark, Kafka, Iceberg/Delta ... are intentionally out of v0" — v0 한정 문장이나 현재 구현 상태(S3~S7)와 나란히 읽히면 **현재 제외처럼 오독** 가능. README.md는 편집 허용 목록에 **없음** | **미수정 → Codex 후속**(§H-2) |

**편집 전 저장소 사실**: 토큰 `S7`은 repo 루트 전체에서 **단 2곳**(`README.md:95`, `ROADMAP.md:109`)에만 존재했다. 한국어 문서·BENCHMARKS·DESIGN 어디에도 없었던 것이 이번 편집의 핵심 대상이다.

**B2. 휘발성 수치**: `README(.ko)/ROADMAP(.ko)/BENCHMARKS(.ko)`에서 하드코딩된 `N passed / N skipped` **0건** 확인. 현행 유지가 맞고, 이번 편집에서도 추가하지 않는다.

**B3. EN↔KO 정렬 주의**: README.md는 이번 패키지의 편집 허용 목록에 없다. 따라서 KO에 S7 절을 추가하면 **KO가 EN보다 풍부해지는 역드리프트**가 생긴다 → EN README에 대응 절을 넣을지는 **Codex 판단 사항**으로 남긴다(§H 위험 판단 2).

---

## C. External Benchmark Matrix

접근일 2026-07-21. 분류: `confirmed`(1차 문서 문장 확인) / `inference` / `unknown` / `vendor-claim`.

### C-1. AWS IoT SiteWise Edge gateway — `confirmed`

```text
service/user problem : 공장 현장에서 네트워크가 끊겨도 수집이 멈추면 안 된다
core state/contract  : gateway가 로컬 수집·처리, 클라우드 복구 시 sync
failure handled      : 인터넷 단절 구간의 수집 연속성
copy/simplify/avoid  : COPY(단절→로컬 durable→복구 후 sync 계약) / AVOID(Greengrass·Siemens Edge 런타임, OPC-UA·MQTT 실제 연결)
relevance            : 높음 — S8 후보의 핵심 압력과 동일
```
> "Offline operation — Continues collecting and processing data during internet outages, syncing with the cloud when connectivity is restored." · "Local data collection and processing — Supports data collection from industrial assets using protocols like OPC-UA and MQTT."
> 출처: https://docs.aws.amazon.com/iot-sitewise/latest/userguide/gateways.html

### C-2. Azure IoT Operations data flows — `confirmed`

```text
service/user problem : edge 메시지를 변환·문맥화해 클라우드로 안전하게 보낸다
core state/contract  : source -> transform -> destination + schema registry(edge/cloud 동기) 
                       + 전달 실패 시 "source 메시지를 ack하지 않음" + broker 큐 보관 + 재시도 + disk persistence
failure handled      : destination/네트워크 불가용, 메시지 유실
copy/simplify/avoid  : COPY(성공 전 ack 금지 = durable 처리 후 진행) / SIMPLIFY(unit 변환·reference data enrichment)
                       / AVOID(MQTT broker, Arc/K8s 배포, dataflow graph)
relevance            : 매우 높음 — 이 repo의 landing-before-commit과 유사한 안전 ordering
```
> "If delivery can't complete, the data flow doesn't acknowledge the source message. The MQTT broker keeps the message in the subscriber queue and the data flow retries delivery." · 변환 예: "Converting units", "Contextualizing data: Add reference data to messages for enrichment".
> 출처: https://learn.microsoft.com/en-us/azure/iot-operations/connect-to-cloud/overview-dataflow (문서 갱신 2026-07-15)

**핵심 대응**: Azure와 K1은 구현이나 delivery guarantee가 같지 않다. 다만 K1의
*durable landing 전에 offset commit 금지*는 "durable downstream 결과 전에 progress를
전진시키지 않는다"는 **유사한 안전 ordering**으로 연결할 수 있다. 이 연결은 `inference`다.

### C-3. Cognite Data Fusion — contextualization — `confirmed`

```text
service/user problem : 시스템마다 ID가 달라 "이 설비의 데이터"를 한 번에 못 본다
core state/contract  : 서로 다른 source system의 리소스를 하나의 데이터 모델로 매핑,
                       동일 엔티티가 CDF에서 같은 식별자를 갖게 한 뒤 실제 관계대로 연결
failure handled      : 소스별 ID 불일치로 인한 조회 실패/오해
copy/simplify/avoid  : PROPOSE(cross-source identity) / 현재 EAV는 컬럼·단위 harmonization까지만
                       / AVOID(ML 매칭 엔진, 3D/P&ID, asset hierarchy 제품화)
relevance            : 중간 — S8-F(문맥화) 시나리오의 근거이나 즉시 구현 대상은 아님
```
> "combine machine learning, a powerful rules engine, and domain expertise to map resources from different source systems to each other in the CDF data model" · "each unique entity shares the same identifier in CDF, even if it has different IDs in the source systems".
> 출처: https://docs.cognite.com/cdf/integration/concepts/contextualization

### C-4. HighByte Unified Namespace — `vendor-claim`

```text
service/user problem : disconnected 시스템의 data silo
core state/contract  : 일관된 추상 구조 하나로 산업 데이터를 제공(벤더 주장)
copy/simplify/avoid  : AVOID(제품/네임스페이스 도입) / 개념만 참고(일관된 asset·topic 명명 규칙의 가치)
relevance            : 낮음 — 이 repo 규모에서 UNS 도입은 과함. "명명 일관성" 교훈만 취함
```
> "a consolidated, abstracted structure that provides all business applications with consistent, real-time industrial data through a single application" — **벤더 마케팅 포지셔닝으로 분류**(독립 표준 정의 아님).
> 출처: https://www.highbyte.com/intelligence-hub/unified-namespace

### C-5. 종합 판단

4개 소스는 하나의 공통 패턴이 아니라 두 lane을 보여준다: ① AWS/Azure의
**단절·전달 실패 시 연속성**, ② Cognite/HighByte의 **소스 간 식별과 명명**. 이 repo는
K1의 관련 durability ordering과 EAV 컬럼·단위 harmonization을 갖고 있지만, edge buffer와
cross-source asset identity는 아직 없다. 따라서 다음 시나리오는 첫 번째 lane만 선택한다.

---

## D. Manufacturing Scenario Catalog (6)

모두 `Proposed`. 어떤 것도 구현·package화하지 않았다.

### D-1. Edge/cloud 단절 후 재연결 replay ★ 최우선 후보
```text
actor      : plant data operator
trigger    : 현장↔중앙 링크가 N분 단절됐다가 복구된다
input/out  : 단절 구간에 쌓인 로컬 buffer -> 복구 후 중앙 landing/batch에 반영
invariant  : 단절 구간 데이터가 유실되지 않는다 · 재연결 후 중복 반영되지 않는다
             · buffer가 durable해지기 전에 진행 포인터를 전진시키지 않는다
failure/rec: 복구 중 재실패 -> 마지막 durable 지점부터 재개(중복은 identity로 흡수)
smallest evidence : 단절 주입 -> buffer 적재 -> 최초 복구 시 누락된 고유 event만 추가 -> 같은 구간 재복구 시 accepted set 불변
non-goals  : 실제 OPC UA/MQTT 연결, edge 하드웨어, 실시간 SLA, HA
```

### D-2. Late / out-of-order telemetry와 sequence gap
```text
actor      : platform operator
trigger    : 늦게 도착한 구간이 이미 마감된 business_date에 속한다
invariant  : 늦은 데이터가 조용히 사라지지 않는다 · 마감된 날짜의 정정은 명시적 재발행으로만
failure/rec: 순서 뒤집힘/구멍 -> 구멍을 evidence로 남기고 정정 경로로 유도
smallest evidence : gap/late 케이스가 quality check로 드러나고 trusted state를 자동 전진시키지 않음
non-goals  : event-time watermark, Flink/Structured Streaming(진짜 window 압력 생기기 전엔 불필요)
```

### D-3. Sensor/tag/unit/schema 교체
```text
actor      : process engineer
trigger    : 설비 교체로 tag 이름·단위(℃↔℉, bar↔kPa)·schema가 바뀐다
invariant  : 과거 데이터의 의미가 소급 변형되지 않는다 · 단위 변환은 선언적이고 검증된다
failure/rec: 미매핑 tag -> fail이 아니라 coverage 경고 + 격리
smallest evidence : mapping config 교체 전/후 gold 보존 + coverage check (EAV 자산 재사용)
non-goals  : 매핑 DSL/UI, asset hierarchy 제품화
```

### D-4. 의심스러운 품질 지표의 source/telemetry RCA
```text
actor      : process/quality engineer
trigger    : gold 지표가 평소와 다르다
invariant  : 모든 gold 숫자는 run/source/quality/lineage로 역추적 가능
failure/rec: 원인이 입력인지 처리인지 구분되는 evidence 제공
smallest evidence : 기존 operator report를 telemetry 경로까지 확장한 단일 조회
non-goals  : 자동 이상탐지·근본원인 자동판정(모델/평가 slice 전에는 금지)
```

### D-5. 늦은 검사 정정과 trusted-state 재발행
```text
actor      : quality engineer + platform operator
trigger    : 검사 결과가 뒤늦게 정정된다
invariant  : 대상 business_date partition만 교체 · 다른 날짜 불변 · 같은 source 재실행은 새 snapshot 없음
failure/rec: 정정 실패 시 기존 trusted state 유지
smallest evidence : S3/S7 자산(partition overwrite + snapshot 증거) 재사용
non-goals  : MERGE, branch WAP, concurrent writer
```

### D-6. Asset / 도면 / 시계열 contextualization
```text
actor      : process engineer
trigger    : "이 설비의 데이터 전부"를 한 번에 보고 싶다
invariant  : source마다 다른 ID가 하나의 canonical 식별자로 수렴 · 매핑 근거가 남는다
failure/rec: 매핑 불확실 -> 자동 확정 금지, unknown으로 표기
smallest evidence : 2개 이상 source ID -> canonical asset id 해소 + 근거 기록 (Cognite 개념의 축소판)
non-goals  : ML 매칭, 3D/P&ID, UNS 제품 도입
```

*(추가 backlog: versioned anomaly inference with human approval — 모델/평가 slice가 생기기 전까지 **먼 backlog**로만 표기.)*

---

## E. Priority Recommendation

| 순위 | 시나리오 | 사용자/운영 가치 | evidence gap | S0~S7 재사용 | 구현 risk | 포트폴리오 가치 |
|---|---|---|---|---|---|---|
| **1** | **D-1 단절→replay** | 높음(현장 1급 문제) | 높음(단절 경계 미증명) | **매우 높음**(K1 commit 계약·replay·idempotency·quality gate) | **낮음**(프로토콜/HW 불필요, 파일·큐 경계 시뮬레이션) | 높음(산업 DP 서사 + 외부 4소스와 정렬) |
| 2 | D-3 tag/unit/schema 교체 | 높음 | 중간 | 높음(EAV mapping·schema drift) | 낮음 | 중상 |
| 3 | D-4 RCA 확장 | 중상 | 중간 | 높음(operator report) | 낮음 | 중 |

> **권고: 다음 slice는 D-1 하나만. 상태 `Proposed`.**
> 근거: (a) §C의 AWS/Azure가 직접 다루는 단절·전달 실패 압력, (b) K1의 landing-before-commit과 Azure ack 규칙이 공유하는 **유사한 안전 ordering**을 단절 경계로 확장, (c) 첫 proof는 새 외부 의존성 없이 가능, (d) 실패 주입으로 증명 가능해 이 repo의 evidence 문화와 일치. 구현·delivery guarantee가 같다는 주장은 아니다.
> 구현 package는 만들지 않았고 "구현됐다"고 표기하지 않았다. 시나리오 문서는 `learn/system-design/scenarios/05-industrial-telemetry-recovery.md`에 **Proposed**로만 추가했다.

---

## F. Safe Claim Boundary

**Safe (현재 evidence로 말할 수 있음)**
```text
합성/로컬/bounded 제조 데이터 플랫폼 foundation을 구현·검증했다.
batch bronze/silver/gold + quality/catalog/lineage, EAV multi-format intake, operator evidence,
local Spark/Iceberg partition overwrite·publish, 문서화된 경계 안의 local Airflow wrapper,
bounded Kafka K1 raw landing과 K1.5 batch bridge, S7 local Spark machine-event batch
(Python parity + quality-gated Iceberg publish)를 local runtime으로 검증했다.
산업 edge 연속성과 contextualization은 "제안된 다음 시나리오"로 조사 중이다.
```

**Forbidden (구현 전 금지)**
```text
자율공장/디지털 트윈/산업 IoT 플랫폼을 구축·운영했다
runtime evidence 없는 real-time·대규모 streaming
구현 전 OPC UA / MQTT / ROS2 / DDS / MCAP 연동
모델·평가 slice 전 예지보전·이상탐지
로봇 제어·안전 제어·closed-loop actuation
production / HA / cluster 운영
실제 공장·실제 설비 데이터 사용
```

**Q7 판정 — 편집 전 실질적 과장 표현: 사실상 없음(저위험 1건).** 두 경로(직접 grep + 독립 전수 스캔)로 교차 확인했다. 이번 편집은 아래 용어를 Proposed/Backlog/Non-goal로만 추가했다.

- `digital twin` / `디지털 트윈` / `예지보전` / `predictive maintenance` / `이상 탐지` / `OPC UA` / `MQTT` / `PLC` / `real factory` / `실제 공장`: **전 문서 0건**.
- `ROS2·MCAP` 언급은 전부 (a) Phase 3 **미체크 backlog**, (b) charter/plain-map/source-contract의 **backlog·부정 표기**, (c) `README.md:15`의 명시적 부정("not real ROS2 bag / MCAP / Jetson data")뿐.
- `continuous streaming` / `production` / `HA` / `cluster` 관련 30여 건은 **전부 부정문 또는 backlog 표기**(예: `BENCHMARKS.md:129`, `README.ko.md:296`, `DESIGN.md:205`). RISK 0건.
- **저위험 1건**: `learn/system-design/question-bank/08-kafka-streaming-ingestion.ko.md:32` — event-time 용어 정의 표의 "**실제 설비**에서 event가 발생한 시각". 일반 용어 정의이고 같은 파일 `:346`이 K1을 streaming pipeline이라 부르지 말라고 못 박지만, 훑어 읽는 독자가 "실제 설비 데이터를 받는다"로 오독할 여지가 아주 조금 있다. **Question Bank는 이번 pass 편집 금지 대상**이라 수정하지 않고 보고만 한다(§H-6).

이번 편집에서 새 주장·새 기술 언급을 추가하지 않았다.

---

## G. Changed Files

| 파일 | 변경 이유 |
|---|---|
| `README.ko.md` | S7 한국어 절 추가(B-1) + `정직한 한계`의 Spark 문장 정정(B-2) |
| `ROADMAP.md` | stale Phase 3 → scenario-led 3분할(Implemented Foundation / Proposed Next Scenarios / Backlog·Unknown)(B-4) |
| `ROADMAP.ko.md` | S7 절 추가(B-3) + 동일 Phase 3 교체(B-4) |
| `BENCHMARKS.md` | NOW/BACKLOG stale 정정(B-5) + 산업 플랫폼 lane 추가(B-7, §C) |
| `BENCHMARKS.ko.md` | JD mapping에 K1.5/S7 추가·CORE 정정(B-6) + 산업 lane 요약(B-7) |
| `learn/system-design/scenarios/05-industrial-telemetry-recovery.md` | **신규**, D-1을 `Proposed`로만 기록 |
| `learn/system-design/README.md` | 위 시나리오 링크 1줄 |
| 이 `claude-audit.md` | 분석 보고 |

**미변경(금지 준수)**: `src/`, `tests/`, `dags/`, `scripts/`, dependency, data/runtime state, `VERIFICATION_LOG.md`, `PROJECT_PROGRESS_MAP(.ko).md`, README.md. 구현 실행·테스트 실행 없음. commit/push 없음.

---

## H. Remaining Unknowns & Risky Judgments (Codex 재검토 요망)

```text
1. D-1을 "단일 다음 slice"로 고른 판단.
   D-3(tag/unit 교체)이 현장 빈도는 더 높을 수 있다. AWS/Azure의 continuity 근거 + 기존 계약 확장성으로 D-1을 택했다.
2. README.md 역드리프트.
   허용 목록에 README.md가 없어 KO에만 S7 절이 생겼다. EN에 대응 절을 넣을지 Codex 판단 필요.
3. Phase 3 교체 범위.
   기존 "ROS2 bag / MCAP-ish ingest" 항목을 삭제하지 않고 Backlog·Unknown으로 이동시켰다.
   완전 삭제를 원하면 Codex가 판단.
4. HighByte UNS를 vendor-claim으로 강등한 판정(독립 표준 정의가 아님).
5. Azure "ack 금지" 계약과 이 repo의 landing-before-commit 대응은 문서 대조 기반이며,
   두 시스템의 전달 보장 수준이 동일하다는 주장은 아니다.
6. 저위험 wording 1건 미수정: question-bank/08:32 "실제 설비" (Question Bank는 이번 pass 편집 금지).
   Codex가 용어 정의임을 유지할지, 한정어를 붙일지 판단.
7. 편집 허용 목록 밖 drift 2건 — Codex 후속 필요:
   - DESIGN.md / DESIGN.ko.md 가 K1.5·S7을 전혀 언급하지 않음(두 slice 뒤처짐).
     추가로 DESIGN.md는 `## 7 Done 기준`이 `## 9 Kafka` 뒤에 오는 번호 순서 결함, EN §9 vs KO §8 번호 불일치.
   - README.md:32 v0 제외 문장이 현재 구현과 나란히 오독될 수 있음(B-11).
8. Unknown 유지: 실제 산업 현장의 단절 빈도/지속시간 분포, OT 프로토콜 실물 동작,
   외부 제품의 내부 구현 세부(1차 문서가 서술하지 않는 부분).
```

---

## I. Codex Review Disposition

**Decision: revise-and-accept.** S8은 구현 승인이 아니라 다음 bounded slice의
`Proposed` 후보로 채택했다. D-3 tag/unit/schema 교체도 유효하지만 기존 EAV·schema-drift
증거와 겹치므로, 현재 비어 있는 단절·복구 경계를 먼저 검증하는 편이 증거 가치가 높다.

수정한 판단:

- 4개 reference가 같은 압력을 다룬다는 표현을 폐기했다. AWS/Azure는 continuity,
  Cognite/HighByte는 identity/contextualization lane이다.
- Azure ack와 K1 offset commit은 동일 계약이 아니라 유사한 safety ordering이다.
- EAV는 컬럼·단위 harmonization이지 cross-source asset identity 해소가 아니다.
- 최초 복구는 누락된 고유 event를 accepted set에 추가하고, 동일 구간의 반복 replay만
  accepted set을 더 늘리지 않는 것으로 test contract를 교정했다.
- `README.md`의 historical v0 문구와 EN S7 설명, `DESIGN.md`/`DESIGN.ko.md`의 K1.5·S7
  drift와 영문 section 번호를 정리했다.

독립 검증:

```text
.venv base suite: 90 passed, 14 skipped
system Spark focused suite: 14 passed
S7 runtime verification: 8/8 passed
git diff --check: passed
publication scan: no credential finding; request package의 local absolute path 제거
```

Claim boundary는 유지한다. S8은 synthetic file/queue boundary simulation으로 시작할 수
있지만, 실제 edge gateway, OPC UA/MQTT, product-grade offline buffer, continuous streaming,
production/HA를 증명하지 않는다.
