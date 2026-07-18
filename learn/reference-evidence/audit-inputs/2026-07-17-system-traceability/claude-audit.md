# System Traceability Audit — 전체 설계 균형 + S0~S6 추적성

Status: reviewed / accepted with Codex corrections (2026-07-18)

Auditor: 외부 독자 + 실무 데이터 플랫폼 설계 관점 (Delegated Documentation Review Mode).
Codex가 모든 material 판정을 accept / revise / reject / keep-unknown으로 재검토한다.
과거 "Kafka design-only / Spark 미구현" 결론은 재사용하지 않고, 현재 git diff와 repo 실물을 처음부터 확인했다.

## Preflight facts (확인함)

```text
HEAD: 413d969 Promote Kafka K1 and K1.5 portfolio evidence
git status: main...origin/main (not ahead now); 12 tracked 문서 M + 1 untracked 새 문서
uncommitted candidate diff (Codex): README(.ko), PROJECT_PROGRESS_MAP(.ko),
  00-service-purpose-charter, 00a-plain-project-map, system-design/README,
  scenarios/00·01, slices/README, slices/spark.../01-question-map, source-contracts/01
new untracked: learn/system-design/01-system-traceability-map.ko.md
검증: pytest 80 passed, 7 skipped (VERIFICATION_LOG = source of truth)
```

교차검증(subagent + 직접): 새 traceability map의 **링크 40개 전부 resolve(broken 0)**, S0~S6가 참조하는 **evidence 파일 14개 전부 존재**.

---

## 1. Overall verdict

**revise (minor) — candidate diff는 핵심 문제(Kafka 편중·추적성)를 실제로 해결한다. 사실 오류는 stale 1건뿐(직접 수정함). 나머지는 P1~P2 문서 gap 권고.**

Codex의 candidate diff는 "최신 기술 문서 하나가 프로젝트 전체처럼 보이는" 문제를 정확히 겨냥했고, 새 `01-system-traceability-map.ko.md`가 그 spine 역할을 한다. 판정: **candidate diff 대부분 accept**, stale 1건 revise(수정 완료), gap은 신규 문서를 지금 만들지 말고 기록만 유지.

## 2. Kafka 편중 판정 (audit Q1)

**Before candidate diff: Kafka 프로젝트처럼 보였다.** README flagship = Kafka walkthrough, 최근 커밋·문서가 Kafka 편중.

**After candidate diff: 해결됨. Kafka는 S5/S6 입력 경로로 읽힌다.** 근거:

- 새 traceability map §1 첫 문장이 "이 프로젝트는 Kafka 프로젝트가 아니다"로 시작하고, batch spine을 중심에 둔 whole-flow 다이어그램 제시.
- 루트 README: "Flagship walkthrough: Kafka…" → "**Overall design trace**"를 먼저, Kafka는 "an ingestion-path milestone, **not the whole platform architecture**"로 명시적 강등.
- README.ko: "대표 포트폴리오" → "**전체 설계 지도**" 먼저, Kafka는 "Kafka Milestone Walkthrough"로 분리.
- plain-project-map §4.1 "현재 전체 흐름" 추가 + "핵심은 기존 batch spine … Kafka는 입력을 durable하게 받는 방법을 추가".
- system-design README thinking-order/root-docs에 traceability map 삽입, live-study-notes를 "source of truth 아님"으로 강등.

**batch/quality/catalog spine이 중심으로 보이는가: YES** — traceability map §3의 S0가 spine이고 S5/S6가 그 위에 얹힌 입력 경로임을 §1·§7이 반복 확인.

## 3. S0~S6 claim-to-evidence matrix

| S | Claim (traceability map) | Evidence (존재·확인) | Verdict |
|---|---|---|---|
| S0 trusted batch | source_hash/schema_hash, bronze/silver/gold, quality reconciliation, catalog/lineage, same-input skip | `lakehouse.py`(직접 통독: 7 quality+1 drift=8, idempotency gate, latest_successful) + `test_lakehouse_pipeline.py` | supported |
| S1 EAV onboarding | wide CSV→EAV→entity gold, mapping coverage/type quality | `eav.py`, `test_eav_pipeline.py` 존재 | supported (문서 gap은 §5) |
| S2 operator debugging | run/source/schema identity + quality + path-level lineage를 한 report로 | `scenarios/02`, `operator_report.py`, `test_operator_report.py` | supported |
| S3 correction publish | single gold table, business_date partition overwrite, run_id→snapshot_id, retry skip | `publish_gold_to_iceberg.py`(직접 통독), `iceberg-write-semantics.md`(Implemented), `test_publish_gold_to_iceberg.py`, `test_spark_iceberg_skeleton.py` | supported |
| S4 Airflow | DAG=CLI wrapper, pipeline이 idempotency 소유 | slices 02/03/04, `test_orchestration.py`, `test_airflow_dags.py` | supported |
| S5 Kafka landing | one-topic/one-partition bounded, accepted/quarantine landing, landing-before-commit recovery, no-commit replay | `landing.py`+`runtime.py`(직접 통독), `scenarios/03`, `slice 05`, `test_kafka_ingestion.py`, broker-verified | supported |
| S6 batch bridge | accepted landing→deterministic CSV/provenance→lakehouse→Iceberg, pipeline 전 fail | `batch_adapter.py`+`kafka-landing-to-batch-adapter.md`(Implemented), `slice 06`, `test_kafka_batch_adapter.py` | supported |

전 S supported. matrix 링크·evidence 파일 broken/missing 0.

## 4. 빠진 scenario/question/contract (audit Q3·Q4)

traceability map §6가 이미 정직하게 기록한 gap(전부 **real**로 확인):

| 우선순위 | Gap | 판정 |
|---|---|---|
| P1 | source-hash idempotency decision note (code/test에만 강함) | accept — `lakehouse.find_existing_successful_run`+skip 계약이 code-only |
| P1 | quality-pass → `latest_successful` 전진 계약 note | accept — `persist_catalog`의 quality_passed→state write가 code-only |
| P1 | EAV 독립 scenario + source-integration(mapping) contract | accept — 구현됐으나 scenarios/·source-contracts/에 EAV 문서 없음 |
| P2 | failure-state model(Proposed)을 별도 slice로 검증 | keep-unknown — Proposed 표기 정확, 검증 slice는 backlog |
| P2 | 전체 platform portfolio overview (Kafka만 있음) | revise — 내부용은 새 traceability map이 이미 충족. **남은 gap은 "공개 portfolio overview"뿐**이며 발행 필요 시점까지 backlog |

map이 **놓친** 추가 관찰(경미):

- **S1/S4/S6는 dedicated scenario 문서가 없다** (S0/S2/S3/S5만 `scenarios/*.md` 보유). map은 S1 EAV scenario gap만 flag했고 S4(Airflow)·S6(bridge)의 scenario 부재는 언급 안 함. orchestration/bridge는 기존 계약 재사용이라 필수는 아니나, 추적성 대칭을 위해 인지 필요.
- **cross-system identity 질문**(Kafka coordinate/event_id ↔ batch source_hash ↔ run_id ↔ snapshot_id, B6의 5-identity)이 `question-bank/06`(cross-area)에 정식 질문으로는 없다. map §5 identity 행이 개념은 담지만, S6 문서화 시 qb/06에 한 줄 추가 여지.

## 5. 암묵적 계약과 문서화 우선순위 (audit Q4)

code/test에만 있고 설계 문서에 약한 계약 = 위 P1 세 건. 실무 관점 우선순위:

1. **source-hash idempotency** (P1) — 가장 자주 인터뷰/블로그에서 설명되는 핵심인데 독립 note가 없다. 1장.
2. **quality-gate → current-state 전진** (P1) — "품질 실패가 trusted state를 전진시키지 않는다"는 이 플랫폼의 신뢰 경계인데 분산 서술됨. 1장.
3. **EAV mapping/source-integration contract** (P1) — 구현은 있으나 "mapping이 보장하는 필드·단위·coverage"가 고정 안 됨.

**단, 지금 이 note들을 새로 만들지 말 것을 권고한다.** 이번 audit의 목적은 추적성 확인이고, map §6이 gap을 이미 명시적으로 기록했다. "새 meta 문서는 꼭 필요할 때만"과 문서 재팽창 방지(Q7) 원칙상, 다음 대표 scenario가 실제로 요구할 때 하나씩 닫는 것이 맞다.

## 6. Stale / overclaim finding (audit Q5)

- **[Fixed] `question-bank/07-external-benchmark-backlog-areas.ko.md:163-164`** — "Kafka ingestion은 discovery를 시작했다 / 아직 design-only이며 code/runtime evidence는 없다"는 K1/K1.5 구현·broker 검증과 **정면으로 모순되는 stale**이었다. §9에서 직접 수정(streaming 잔여 영역만 Backlog로 유지하고 S5/S6 구현 사실 반영). **이 audit이 겨냥한 바로 그 Kafka-status 불일치.**
- **[Low, 미수정] `slices/spark-iceberg-partition-overwrite/05-version-pin.md:54-55`** — 하드코딩 pytest 수(`2 passed`, `40 passed`). "Verification result on 2026-07-11" 날짜 블록 안이라 frozen snapshot으로 방어되지만, Q7의 "test 숫자를 여러 문서에 복제하지 마라"에 걸린다. 역사적 기록이라 내가 고치면 과거 검증 note를 재작성하게 되므로 **미수정** → Codex가 VERIFICATION_LOG pointer로 대체할지 판단.
- overclaim 스캔: 나머지 "design-only / Kafka streaming implemented" 히트는 전부 **"이렇게 주장하면 안 된다" 예시 블록**이거나 정확한 out-of-scope 목록(continuous/multi-partition 등). stale 아님.
- charter diff의 claim 이동(Kafka를 v0 boundary=implemented로, "continuous/production Kafka streaming"·"exactly-once"를 forbidden으로)은 **정확**. underclaim/overclaim 없음.

## 7. Beginner reading-order 평가 (audit Q6)

요청 순서: purpose → plain map → traceability → scenario → selected questions → decision/slice → evidence.

- **충족.** system-design README thinking-order와 root-docs가 정확히 이 순서로 재배열됐고, plain-map §9·traceability map §7이 동일 순서를 반복. live-study-notes를 "source of truth 아님"으로 강등한 것도 초급자 혼동을 줄인다.
- **경미한 마찰**: 최상단 "입구" 문서가 3개(charter / plain-map / traceability-map)로 늘었다. system-design README가 순서를 잡아줘서 navigable하지만, 초급자에게는 "어디부터?"가 한 번 더 생길 수 있다. plain-map이 "쉽게 보기", traceability-map이 "연결 보기"로 역할이 갈려 중복은 아님 — 유지 가능.
- traceability map §7이 "Kafka 문서부터 읽을 필요 없다. S0 spine이 중심"이라고 못 박은 것은 Q6·Q1을 동시에 만족.

## 8. accept / revise / reject / keep-unknown 판정

| 대상 | 판정 | 이유 |
|---|---|---|
| 새 `01-system-traceability-map.ko.md` | **accept** | 링크 40/40 resolve, S0~S6 evidence 정확, Kafka 탈중심의 핵심 축. |
| charter/README/README.ko/plain-map/system-design README diff | **accept** | claim 정확, 추적성·탈Kafka 일관. |
| scenarios/00·01, source-contracts/01, slices spark question-map status "초안→implementation-backed" | **accept** | 구현 사실과 일치. |
| PROJECT_PROGRESS_MAP(.ko) traceability 링크 추가 | **accept** | one-screen map에서 derivation 진입점 제공, 숫자 복제 없음. |
| qb/07:163-164 "Kafka design-only" | **revise → 수정 완료** | stale, 구현 사실과 모순. |
| P1 decision-note 3종을 지금 생성 | **reject(지금은)** | map §6에 gap 기록됨. 문서 재팽창 방지, scenario-driven으로 미룸. |
| 전체 portfolio overview page | **keep-unknown** | 공개 발행 필요 시점 전까지 backlog. 내부용은 map이 충족. |
| failure-state-model(Proposed) | **keep-unknown** | Proposed 표기 정확, 검증 slice는 별도 backlog. |
| 05-version-pin 하드코딩 count | **keep-unknown** | dated snapshot vs 복제 금지 원칙 충돌, Codex 판단. |

## 9. 직접 수정한 파일과 이유

| 파일 | 변경 | 이유 |
|---|---|---|
| `learn/system-design/question-bank/07-external-benchmark-backlog-areas.ko.md` | streaming 분류 블록의 "Kafka discovery 시작 / design-only / code·runtime evidence 없음"을 "S5 raw landing·S6 bridge는 local broker로 구현·검증됨, 남은 streaming(continuous/watermark/exactly-once/Flink/CDC/SSS)만 Backlog"로 교체 | 유일한 명확한 correctness stale. 구현 사실과 모순되던 문장을 정정하되 이 섹션의 backlog 의도(streaming은 여전히 미구현)는 보존 |

그 외에는 직접 수정하지 않았다. Codex candidate diff가 대부분 정확하고, 남은 gap은 신규 문서 생성이라 "audit + 필요한 보완" 범위를 넘고 문서 재팽창 위험이 있어 권고로만 남겼다. `src/**`·`tests/**`·`VERIFICATION_LOG.md`·blog draft 미변경. commit/push 안 함.

## 10. Codex가 재검토해야 할 위험한 판단

1. **P1 note 3종을 "지금 만들지 말라"** 한 것 — 추적성 완결성 관점에서는 지금 만드는 게 낫다는 반론 가능. 나는 문서 재팽창 방지·scenario-driven 원칙을 우선했으나, source-hash idempotency 하나만이라도 지금 쓸지는 Codex/사용자 판단.
2. **qb/07 수정 문구** — "streaming 잔여 영역만 Backlog"로 바꿨는데, 이 섹션 전체 톤(external-benchmark backlog)과 어긋나지 않는지 Codex 확인.
3. **candidate diff를 대부분 accept** — 나는 링크·evidence 존재·claim 정확성을 확인했으나, plain-map/system-design README의 74·49줄 재작성 전체를 문장 단위로 검수하진 않았다. Codex가 나머지 산문을 최종 확인 권장.
4. **S1/S4/S6 scenario 문서 부재**를 "필수 아님"으로 판단 — 추적성 대칭을 중시하면 S1(EAV) scenario는 P1로 올릴 수 있음.
5. **05-version-pin 하드코딩 count 미수정** — dated snapshot이라 뒀으나, "test 숫자 복제 금지"를 엄격 적용하면 VERIFICATION_LOG pointer로 교체가 맞을 수 있음.
6. **scenario-seed 제목 01→00 재번호** — scenarios/ 폴더 내 번호(00 seed…03 kafka)는 정합하나, 다른 문서가 옛 "01. Scenario seed" 제목으로 참조하지 않는지 Codex가 최종 grep 권장(링크는 파일명 기준이라 깨지지 않음).

모든 material 항목은 Codex의 accept/revise/reject/keep-unknown 재검토 대상이며, 상태는 `returned-unreviewed / Codex review required`. 코드·기술 scope는 시작하지 않았고 설계 추적성만 감사·보완했다.

## 11. Codex disposition — 2026-07-18

| 대상 | 판정 | 최종 처리 |
|---|---|---|
| 전체 추적성 지도와 README/charter/plain-map 변경 | accept | S0 batch spine을 중심으로 S1~S6를 연결하고 Kafka를 입력 경로 milestone으로 한정한 구성을 유지한다. |
| Kafka `design-only` stale 문구 수정 | accept | K1/K1.5 local broker/runtime evidence와 일치한다. |
| P1 decision note 3종 즉시 추가 | reject for now | traceability map에 gap만 남기고 실제 scenario가 요구할 때 작성한다. |
| failure-state forensics 구현 | keep unknown | 별도 scenario/slice가 선택되기 전까지 Proposed 상태를 유지한다. |
| version-pin의 dated test count | accept as historical evidence | 2026-07-11 검증 블록 안의 point-in-time 결과이므로 재작성하지 않는다. 최신 상태는 `VERIFICATION_LOG.md`를 따른다. |
| B6 portfolio status | revise | 2026-07-18 DEV.to 공개 발행 완료 상태로 progress map을 갱신한다. |

독립 확인 결과 새 traceability map의 local link는 모두 resolve됐고 `git diff --check`도 통과했다.
이번 audit 디렉터리에는 사전 `REQUEST.md`가 없었다. 과거 direct handoff 결과를 사후 package로
위장해 만들지는 않는다. 다음 external audit부터는 `REQUEST.md`의 target HEAD, expected dirty
files, 허용 수정 범위를 먼저 고정한 뒤 위임한다.
