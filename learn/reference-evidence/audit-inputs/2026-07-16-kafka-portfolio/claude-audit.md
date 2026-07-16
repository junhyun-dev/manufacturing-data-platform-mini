# External Reader Audit — Kafka K1/K1.5 Portfolio Promotion

Status: returned-unreviewed / Codex review required

Codex disposition: reviewed / accepted with minor corrections

Auditor role: external public reader + benchmark auditor (Delegated Documentation Mode).
Codex independently accepts / revises / rejects / keeps-unknown every material item below.

## 1. Preflight facts

```text
git status --short --branch : ## main...origin/main [ahead 5]
  M PROJECT_PROGRESS_MAP.ko.md / PROJECT_PROGRESS_MAP.md / README.ko.md / README.md / VERIFICATION_LOG.md
  ?? docs/portfolio/  ?? learn/reference-evidence/audit-inputs/2026-07-16-kafka-portfolio/
git log -2 : c42527a Implement Kafka landing-to-batch bridge / 6bc05f2 Fix Kafka K1 offset-gap contract
```

- HEAD `c42527a` == REQUEST의 "Current implementation evidence commit". 일치.
- 포트폴리오 package와 promotion 문서 갱신은 아직 **미커밋 + unpushed** (ahead 5, package untracked). → 아래 F1의 근거.
- REQUEST의 promotion recheck facts(80 passed·source_hash `9efd6173…`·snapshot `3544754184027092485`)가 `runtime-evidence.json` 및 VERIFICATION_LOG promotion entry와 3자 일치.

## 2. Overall verdict

**revise (minor) — 정정할 correctness/claim 결함은 없음. 아래 pre-publish gate만 닫으면 publish-ready.**

이 package는 evidence-first 원칙을 잘 지켰다. 세 이미지가 `runtime-evidence.json`과 **정확히** 일치하고, report.html은 같은 JSON을 data-binding으로 렌더링하며, 인용한 공식 문서가 실제로 본문 주장을 뒷받침한다. credential/private-path/overclaim 스캔 clean. 블로킹은 전부 **발행 순서(repo push/public)** 와 **한 가지 readability(identity 구분)** 이며, 후자는 이번에 직접 수정했다.

- 최고 우선 pre-publish gate: **F1** (이미지 raw URL이 public repo push에 의존).
- 최고 우선 readability: **F3** (Q4 five-identity 구분) — B6에 직접 반영 완료.

## 3. Claim-to-evidence matrix

| Claim (README/B6/report) | Evidence | Verdict |
|---|---|---|
| produced 5 = accepted 4 + quarantined 1, persisted 5, lost 0 | `runtime-evidence.json` reconciliation; image 01; VERIFICATION_LOG promotion | supported |
| landing 후·commit 전 crash → 재전달 1 → coordinate reuse → accepted 4 유지 → next offset 4 commit | `failure_recovery`(redelivered 1, status reused, total 4, next 4); image 02; `landing.py` `SimulatedCrashAfterLanding` + coordinate reuse; `test_kafka_ingestion.py` crash-recovery test | supported |
| bounded replay 4 coordinate 재사용, normal group commit 안 함 | `bounded_replay`(reused 4, commit false); `runtime.py` replay guard `replay_start_offset + commit_offsets` 금지 | supported |
| adapter created→reused, lakehouse processed→skipped, gold 1행 유지 | `kafka_k1_5` first/second; image 03; `verify_kafka_k1_5.sh` 11 checks; `test_kafka_batch_adapter` idempotent-rerun test | supported |
| adapter CSV의 SHA-256 = pipeline `source_hash` = `9efd6173…` | `runtime-evidence.json` source_hash; `batch_adapter.canonical_csv_bytes`+`source_hash_for`; `run_bridge` 런타임 assert; image 03 | supported |
| quality 8/8 pass | `lakehouse.build_quality_checks`(7) + drift(1) = 8; runtime-evidence `quality_check_count` 8; image 03 | supported |
| gold units=100, defects=6, defect_rate 0.06, avg 825.0 | `runtime-evidence.json` gold; image 03; accepted 4건 합산(10+20+30+40=100, 0+1+2+3=6) | supported |
| Iceberg published→skipped, snapshot 1→1, id `3544754184027092485` | `iceberg_publish`; image 03; `publish_gold_to_iceberg` `_is_same_successful_publish` | supported |
| invalid/empty-date 입력은 pipeline 호출 전에 실패 (false-green 차단) | `batch_adapter` `NoEligibleEventsError`/`LandingIntegrityError`, `run_lakehouse_pipeline` 호출 전; tamper/empty-date tests; verify check `empty_date_fails_before_pipeline` | supported |
| "at-least-once, not exactly-once / power-loss" | Kafka 4.3 javadoc(재전달 window); `kafka-offset-and-landing-commit.md`; README·B6·image 명시적 not-claimed | supported |

전 항목 supported. 근거 없는 claim 미발견.

## 4. Technical correctness findings (severity order)

- **F1 (Medium, pre-publish gate)** — B6의 이미지가 `https://raw.githubusercontent.com/junhyun-dev/manufacturing-data-platform-mini/main/docs/portfolio/kafka-k1-k1-5/assets/0{2,3}-*.png`와 repo tree 링크를 건다. 현재 branch는 **ahead 5·unpushed**, package는 **untracked**. 이 상태로 DEV.to에 발행하면 이미지가 깨진다. 발행 전 조건: (a) `junhyun-dev/manufacturing-data-platform-mini`가 **public**, (b) 이 asset들이 `main`에 **push**됨. 나는 repo public 여부를 확인할 수 없어 **unknown**으로 남긴다.
- **F2 (Low, doc consistency)** — VERIFICATION_LOG의 K1.5 *bridge* entry는 snapshot `2896841135077514634`, promotion entry는 `3544754184027092485`. Iceberg가 clean publish마다 새 snapshot을 발급하므로 정상이지만, 두 값이 나란히 있어 리뷰어에게 혼동을 준다. 마찬가지로 source_hash도 broker 재실행마다 달라진다(아래 F5). promotion entry에 "clean 재발행이라 snapshot/​source_hash가 bridge entry와 다르다" 한 줄이면 해소. (VERIFICATION_LOG는 내 편집 금지 → Codex.)
- **F5 (Low, claim precision)** — "same accepted Kafka set → same canonical CSV bytes → same source_hash"는 **하나의 immutable landing 안에서만** 참이다. canonical CSV가 `kafka_timestamp_ms`와 record fingerprint를 포함하므로, producer를 다시 돌리면 broker가 새 timestamp를 찍어 → 새 fingerprint → 새 CSV → 새 source_hash가 된다(2ec0300b→9efd6173 실제로 변함). 이건 의도된 provenance 설계지 버그가 아니다. 다만 "deterministic"을 "같은 논리 이벤트는 언제나 같은 hash"로 오독할 수 있으니, determinism이 "주어진 landing에 대해"임을 B6가 한 번 못 박으면 좋다. (B6 line 164 "same accepted Kafka set"이 사실상 방어하고 있어 must-fix 아님.)

정정이 필요한 사실 오류는 없음. Q3(landing→crash→redelivery→reuse→commit)와 Q4의 identity 정확성은 코드·runtime evidence와 일치.

## 5. External-reader readability findings

- **F3 (Medium — 직접 수정함)** — Q4의 다섯 identity(Kafka 위치 / `event_id` / adapter `source_hash` / pipeline `run_id` / Iceberg `snapshot_id`) 중 B6는 `run_id`·`snapshot_id`를 **별도 개념으로 명명하지 않았다**. identity 분리가 이 글의 교육 목표이므로, B6에 5행 identity 표를 추가했다(§9).
- **F4 (Low, optional trim)** — "batch bridge before Spark SS" 논지가 intro / "왜 바로 SS로 안 갔나"(line 123 표) / "What This Changed"(line 200) 3곳에 나온다. 각기 각도(thesis / option표 / slice 순서)가 달라 치명적 중복은 아니나, "What This Changed"를 2~3줄로 줄이면 밀도가 오른다. 판단은 Codex.
- **F6 (Low)** — B6는 이미지 01(runtime overview)을 싣지 않는다(02·03만). portfolio README는 3장 다 싣는다. Q1(2분 내 전체 경로 이해)에는 overview가 최상단에 있으면 유리하다. 이미지 URL 의존(F1)을 새로 늘리지 않으려고 **직접 추가 대신 권장**만 한다: B6 Scenario 앞에 01을 넣을지 Codex 판단.
- **강점** — Q1/Q2: 시나리오가 latency SLA를 지어내지 않고 "durable·replay·no-double-count" 압력으로 Kafka를 정직하게 정당화. Q7: not-claimed가 README Claim Boundary·image 01 패널·B6 Limitations·runtime-evidence `not_verified` 4중으로 노출되어 skim에도 살아남음.

## 6. Image and walkthrough audit (Q6)

세 PNG 모두 `runtime-evidence.json`과 **정확히 일치**:

| 이미지 | 대조 결과 |
|---|---|
| 01 overview | Kafka 4.3.1, 1/1/1, date 2026-06-29, commit c42527a, produced 5 / accepted 4 / quarantined 1 / lost 0 — 일치 |
| 02 failure-recovery | coordinate p0/o3, injected 1, redelivered 1, accepted 4, replay commits 0, next offset 4, date 2026-07-16 — 일치 |
| 03 batch-iceberg | selected 4, created→reused, processed→skipped, 8/8, gold 1, units/defects 100/6, source_hash `9efd6173…`, snapshot `3544754184027092485`, 1→1 — 일치 |

report.html은 동일 JSON을 `data-field`로 바인딩 → 구조적으로 faithful. walkthrough(README "Failure→Investigation→Recovery" 6단계, image 02 타임라인)는 코드 동작과 일관. VERIFICATION_LOG promotion entry가 이미지의 source_hash·snapshot을 동일하게 기록 → JSON·이미지·로그 3자 일치. **Q6: PASS.**

## 7. Series title and B1–B6 order (Q9)

- 제목 **"Synthetic Manufacturing Data Platform: Evidence First"** — 정직하고 명확. "Synthetic"이 clean-room 경계를, "Evidence First"가 이 프로젝트의 실제 원칙을 반영. 과장 없음. **채택 권장.**
- 순서 **B1→B6**: registry상 B6는 Kafka bridge이고, batch quality/gold/Iceberg가 이미 있다고 전제하며 재사용한다("이미 검증한 batch 경로 재사용"). 즉 medallion/quality/Iceberg(B1~B5)가 먼저 와야 B6가 성립 → **B1→B6 순서 타당.**
- 주의: B6는 batch pipeline·Iceberg publish 사전 지식을 가정한다. 시리즈로 읽으면 문제없지만, 단독 노출 시 "기존 pipeline"이 무엇인지 한 줄 링크(B4/B5)가 있으면 친절하다.

## 8. Official sources checked (primary, current)

| 사실 | 출처 | 확인 문구 |
|---|---|---|
| committed offset = 다음에 읽을 message 위치 (= last+1) | Kafka 4.3 `KafkaConsumer` javadoc | commit 값은 application이 다음에 읽을 record 위치를 가리킨다. |
| manual commit + 처리 후 commit → at-least-once 재전달 window | 동 javadoc | 처리 후 commit 전에 실패하면 마지막 committed 위치부터 다시 읽어 이전 batch가 재처리될 수 있다. |
| offset 비연속(gap) 정상 | 동 javadoc | compacted topic이나 transaction marker 때문에 소비되는 offset은 연속적이지 않을 수 있다. |
| `os.fsync`가 disk write 강제 + flush 선행 | Python 3 `os` docs | buffered data를 flush한 뒤 file descriptor를 `fsync`하는 순서를 설명한다. |
| `os.replace` 동일 FS atomic rename | Python 3 `os` docs (본 세션 fetch는 truncate; 이전 확인 유지) | POSIX atomic rename, cross-filesystem 시 실패 가능 — B6는 "local Linux FS에서만 검증, power-loss 미주장"으로 보수적 |

B6 Sources에 링크된 Kafka 4.3.1 javadoc이 **실제로** 본문 주장을 뒷받침함을 확인(인용 출처와 주장 일치). Spark Structured Streaming은 "왜 아직 불필요한가" 근거로만 언급되며 도입을 권하지 않음 — REQUEST 제약과 부합.

## 9. Direct edits made

| 파일 | 변경 | 이유 |
|---|---|---|
| `blog-drafts/…/B6-kafka-landing-to-batch-bridge.md` | identity chain 문단 뒤에 5행 identity 표 + 1문장 추가 | Q4의 다섯 identity를 B6가 명시적으로 구분하지 않던 gap(F3) 해소. 표의 다섯 identity는 모두 코드·`runtime-evidence.json`에 실재. volatile 값(run_id/snapshot 숫자)은 넣지 않아 staleness 회피 |

portfolio `README.md`/`README.ko.md`는 직접 수정하지 않음 — 현재 claim이 evidence와 일치하고, 남은 개선은 발행-순서(F1)·mermaid 렌더(F7 아래)라 Codex 확인 대상.

## 10. Remaining Codex actions

Must (pre-publish):
1. **F1** — 발행 전 `junhyun-dev/manufacturing-data-platform-mini`를 public으로 하고 이 package(assets 포함)를 `main`에 push. 안 하면 DEV.to에서 이미지 broken. (repo public 여부 unknown — 확인 요망.)

Recommended:
2. **F7(mermaid)** — portfolio `README.md`/`README.ko.md`의 mermaid node label이 `"Kafka 4.3.1\n1 broker…"`처럼 `\n`을 쓴다. GitHub mermaid 버전에 따라 리터럴 `\n`으로 렌더될 수 있음. `<br/>`로 바꾸는 게 안전. 나는 렌더를 확인할 수 없어 **직접 수정 대신 서술**만 함 — Codex가 GitHub preview로 확인 후 필요시 교체.
3. **F2** — VERIFICATION_LOG promotion entry에 "clean 재발행이라 snapshot_id/​source_hash가 bridge entry와 다르다" 한 줄 추가(내 편집 금지 파일).
4. **F5** — B6에 determinism이 "주어진 immutable landing에 대해" 성립임을 한 구절로 못 박을지 판단(현재도 방어되어 optional).
5. **F6** — B6 최상단에 overview 이미지(01) 추가할지 판단(F1 의존 증가 vs Q1 이득).
6. **F4** — B6 "What This Changed" 절 축약 여부 판단.

Unknown:
- `junhyun-dev` GitHub repo의 public/pushed 상태 (F1) — 세션에서 확인 불가.

블로킹 correctness/claim 결함 없음. 이 audit의 모든 material 항목은 Codex의 accept/revise/reject/keep-unknown 재검토 대상이며, 상태는 `returned-unreviewed / Codex review required`.

## 11. Codex disposition

Reviewed on 2026-07-16 against the implementation, tests, runtime evidence, and reader-facing diff.

| Finding | Decision | Applied action |
|---|---|---|
| F1 public/push dependency | accept | GitHub repository visibility independently confirmed as `PUBLIC`; commit/push remains a hard gate before any DEV.to API action. |
| F2 point-in-time snapshot/hash difference | accept | Added a `VERIFICATION_LOG.md` note explaining fresh Kafka provenance and clean Iceberg snapshot identities. |
| F3 five-identity table | accept | Claude's B6 table matches code and runtime contracts; retained. |
| F4 repeated Spark Structured Streaming rationale | reject change | Intro, option comparison, and changed slice order serve distinct reader tasks; no trim applied. |
| F5 determinism scope | accept | B6 now says determinism is relative to one immutable landing and explains why a fresh producer run changes provenance/hash. |
| F6 overview image | accept | Added image 01 near the Scenario so a standalone reader sees the full path before failure details. |
| F7 Mermaid line breaks | accept | Replaced `\\n` labels with `<br/>` in both portfolio READMEs. |

Final Codex verdict: **accepted for project commit and push**. B6 DEV.to draft creation and B1-B6 series synchronization remain gated on successful asset URL checks after push.
