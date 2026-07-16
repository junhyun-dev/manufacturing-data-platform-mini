# Kafka K1 Claude Audit Input

상태: reviewed / Codex disposition recorded

이 파일은 Claude의 external-reference-backed audit 원문을 보존하는 inbox다.
여기에 기록된 판단은 Codex가 source와 repo evidence를 재검토하기 전까지 accepted
reference evidence나 최종 설계가 아니다.

- Mode: Delegated Documentation Mode
- Target commit: `689a4e9 Implement bounded Kafka raw ingestion`
- Audit date: 2026-07-14
- Preflight: PASS (HEAD=689a4e9; scenario/qb/slice = "implemented / broker-verified"; VERIFICATION_LOG `Kafka K1 bounded raw ingestion` + `56 passed, 7 skipped`; confluent-kafka 2.15.0; Kafka 4.3.1 KRaft). 과거 design-only 결론을 폐기하고 현재 코드/테스트/런타임 evidence를 처음부터 읽고 감사함.

> 아래 `Audit Report`는 Claude가 제출한 원문이다. 최종 수용 여부는 맨 아래
> `Codex disposition`에 기록한다.

## Audit Report

### 1. Executive verdict

- **K1은 publish/portfolio-ready.** 구현이 견고하고 claim이 evidence와 일치하며 경계가 정직하다: fsync + atomic rename immutable landing, coordinate+fingerprint dedup, landing-before-commit at-least-once, quarantine-before-commit, replay `assign`+no-commit(강제), producer `enable.idempotence=true`/`acks=all`(confluent-kafka 기본 false라 명시 필요 — 정확히 명시).
- **최고 심각도 이슈(단 하나, correctness) = H2**: `committable_offset = max(offset)+1`가 batch offset 연속성을 assert하지 않는다. 정상 runtime 경로는 consumer가 순차 poll이라 연속이므로 안전하지만, `land_records`는 public 함수이고 gap이 있는 batch(compacted topic, 미래 non-sequential 입력)에는 offset을 silent skip해 유실을 만들 수 있다. 이 전제가 코드/문서에 명시돼 있지 않다.
- **NOW fixes(범위 미증가)**: (a) `_validate_batch_scope`에 offset 연속성 assert + gapped-offset 테스트, (b) landing durability의 filesystem 경계 명시, (c) 주입 실패가 in-process(논리적) 복구이지 OS crash/power-loss 검증이 아님을 명시. (b)(c)는 이 감사에서 decision note에 proposed로 반영. (a)는 code라 Codex 몫.

### 2. Claim-to-evidence matrix

| Claim | Exact code/test/runtime evidence | Verdict | Required correction |
|---|---|---|---|
| landing-before-commit 순서 | `runtime.consume_and_land`(land_records 후 `commit(asynchronous=False)`), `kafka-offset-and-landing-commit.md` | Supported | 없음 |
| crash after landing/before commit → 재전달, accepted set 미증가 | `test_crash_after_atomic_rename_is_recovered_by_coordinate_reuse`(unit, 논리) + `kafka_k1_verification.py`(runtime offset3 crash→retry reused, next=4) | Supported | 표현: "injected in-process failure" 유지(H7) |
| immutable atomic JSONL landing (fsync + atomic rename) | `landing._write_immutable_batch`(파일 fsync → staging dir fsync → `os.replace` → parent dir fsync) | Supported | durability의 FS 경계 명시(H1) |
| commit next offset = max(offset)+1 | `landing.land_records` committable_offsets | **Supported (조건부)** | **연속성 assert 필요(H2)** |
| bounded replay가 normal group progress 미변경 | `runtime`(replay는 `assign`+`commit_offsets=False` 강제, `enable.auto.commit=False`) + `kafka_k1_verification`(별도 group `...bounded-replay` + `performed=False`) | Supported (구성적 증명) | 선택: 동일 group 관찰 테스트(H5, Backlog) |
| event_id(business) ≠ (topic,partition,offset)(transport) | `contracts`(event_id required) + `landing`(accepted_events map) + 테스트 | Supported | 없음 |
| 같은 event_id, 다른 coordinate = duplicate evidence | `landing`(first-wins) + `test_same_event_id_at_new_offset...` | Supported | 없음 |
| invalid event quarantine + offset 진행 | `landing`(quarantine.jsonl raw_value+error, durable before commit) + unit + runtime(next=5) | Supported | 선택: binary payload lossless(base64)(H4, Backlog) |
| `machine_id` message key | `runtime.produce_events`(key=machine_id) | Supported (key 설정) | routing 미검증 명시(H8) — **이미 문서에 있음** |
| `enable.idempotence=true`, `acks=all` | `runtime.produce_events` producer config | Supported | confluent-kafka 기본 false라 명시가 정확 |
| Kafka 4.3.1 KRaft / confluent-kafka 2.15.0 broker-verified | `VERIFICATION_LOG` 2026-07-14, `scripts/kafka_k1_verification.py`, `/tmp/...kafka_k1_verification.json` | Supported | 없음 |

### 3. Source registry (직접 공식 URL, 2026-07-14 확인)

| Source | Direct URL | Grade | Version/date | Confirmed behavior | Unknown/stale risk |
|---|---|---|---|---|---|
| Confluent producer configs | https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html | A(vendor) | current, 2026-07-14 | librdkafka/confluent-kafka `enable.idempotence` **기본 false**; Java는 KIP-679(3.0)로 기본 true; enable 시 acks=all·retries>0·max.in.flight≤5 | 2.15.0 정확 값은 Codex가 설치본에서 재확인 |
| Confluent consumer docs | https://docs.confluent.io/platform/current/clients/consumer.html | A | 2026-07-14 | `enable.auto.commit` **기본 true**(at-least-once); `commit()` 기본 `asynchronous=True`; sync commit은 broker 확인까지 block+retry; close는 auto.commit=false면 commit 안 함 | — |
| confluent-kafka-python overview | https://docs.confluent.io/kafka-clients/python/current/overview.html | A | 2026-07-14 | Python client API(Producer/Consumer/AdminClient/TopicPartition) | 2.15.0 signature |
| Apache Kafka 4.0 release | https://kafka.apache.org/blog/2025/03/18/apache-kafka-4.0.0-release-announcement/ | A(ASF) | 4.0, 2025-03 | KRaft-only, ZooKeeper 제거 | repo는 4.3.1 사용(더 신형) |
| Kafka consumer position/committed offset | https://kafka.apache.org/documentation/#design_consumerposition | A | 2026-07-14 | committed offset = "다음에 읽을 위치"(= 마지막+1). 코드의 next_offset=max+1과 일치 | — |
| Python `os.replace` / `os.fsync` | https://docs.python.org/3/library/os.html#os.replace , https://docs.python.org/3/library/os.html#os.fsync | A(Python) | 3.x, 2026-07-14 | `os.replace`는 atomic(POSIX 요구), cross-FS면 실패; `os.fsync`는 파일을 디스크로 강제 | **디렉토리 fsync의 rename durability는 Python 문서에 명시 없음** → OS/POSIX-level(local FS)에서만 성립. H1 [INFERENCE] |

검색 결과 URL이 아니라 위 공식 문서만 사용함.

### 4. Decision audit (C1–C9)

| ID | keep/revise/demote/unknown | official evidence | reason | smallest correction |
|---|---|---|---|---|
| C1 event_id vs coordinate | **keep** | contracts/landing + 테스트 | 재전달과 business 재생성을 정확히 분리 | 없음 |
| C2 machine_id key / 1 partition | **keep** | producer key=machine_id; source-contract §2 | key는 설정되나 routing 미검증을 이미 명시(H8) | 없음 |
| C3 enable.idempotence=true, acks=all | **keep** | Confluent producer-configs(기본 false) | confluent-kafka 기본 false라 명시가 정확 | 없음 |
| C4 strict JSON v1 + quarantine | **keep** | contracts.validate_machine_event + 테스트 | required/unknown/type/range·bool-as-int 방지까지 엄격 | 없음 |
| C5 fsync + atomic rename before commit | **keep + caveat** | landing._write_immutable_batch; os.replace/os.fsync 문서 | 순서 정확. 단 durability의 FS 경계·연속성 전제 미명시 | **H1·H2 caveat 추가(반영)** |
| C6 coordinate+fingerprint reuse | **keep** | landing + `test_crash_after_atomic_rename` | crash window를 단일 writer에서 닫음 | 없음 |
| C7 same event_id 다른 coordinate = duplicate | **keep** | landing first-wins + 테스트 | business duplicate를 accepted set에 안 더함 | 없음 |
| C8 replay assign + no commit | **keep + note** | runtime 강제 guard + verification(별도 group, performed=False) | 구성적으로 정확. 동일-group 관찰 증명은 아님 | 선택 테스트(H5, Backlog) |
| C9 manifest 재구성, no external DB | **keep** | landing.load_landing_index(O(n) glob, .staging 제외) | single-writer/local-small-data 전제 명시됨 | 없음 |

### 5. Missing-question supplement (correctness/operability/claim 경계만)

- **Offset 연속성 계약(H2)** — 질문: bounded batch가 연속 offset이 아닐 수 있는가? 쉬운 말: 0,1,3만 들어오면 2는 어디로 가나. 왜 중요: max+1 commit이 gap을 silent skip하면 유실. 옵션: 연속성 assert / 문서화된 전제 / 무시. 분류: **Core(NOW)**. 대상: `kafka-offset-and-landing-commit.md`(전제 명시, 반영) + `landing._validate_batch_scope`(assert, Codex). ref: Kafka consumer position.
- **Landing durability FS 경계(H1)** — 질문: 이 durability가 어떤 저장소에서 성립하나? 쉬운 말: 로컬 디스크에서만인가. 왜 중요: object store/NFS는 atomic rename·directory fsync 의미가 다름. 옵션: local-POSIX 한정 명시 / 무경계 주장(금지). 분류: **Core(claim gate, NOW)**. 대상: decision note(반영). ref: Python os.replace/os.fsync(디렉토리 durability 미문서 → INFERENCE).
- **주입 실패의 성격(H7)** — 질문: 이건 어떤 종류의 crash를 증명하나? 쉬운 말: 프로그램이 예외로 죽은 것과 전원이 나간 것은 다르다. 왜 중요: "crash-tested"가 OS/power-loss로 오독될 수 있음. 옵션: "in-process 논리 복구"로 표현 / power-loss fault-injection(Backlog). 분류: **Core(claim, NOW 표현)**. 대상: decision note(반영).
- (Backlog, named only) binary poison payload lossless 보존(base64)(H4) · 동일-group assign-replay committed-offset 관찰 테스트(H5) · multi-partition key routing/ordering(H8) · concurrent writer(H3/H9).

### 6. Test audit

- **의미 있는 assertion인가**: 예. landing/contract 유닛 테스트가 accepted/duplicate/quarantine/reuse/consistency-error/crash-recovery를 실제 상태로 검증(smoke 아님). runtime 스크립트는 produce→consume→commit→crash→retry→replay→quarantine→reconciliation을 assert.
- **H2 (false-green 위험)**: **있음.** 모든 테스트가 연속 offset만 사용 → max+1이 gap에서 유실을 내는 경로가 미검증. **NOW: gapped-offset 테스트 + 연속성 assert.**
- **H4**: quarantine + offset 진행이 unit·runtime 모두 검증됨. 충분. (binary lossless는 Backlog.)
- **H5**: replay-no-commit이 runtime(별도 group + performed=False)로 검증됨 → claim을 뒷받침. 더 tight한 증명(동일 group assign-replay 전후 committed offset 관찰)은 Backlog nicety.
- **H7**: crash가 유닛(SimulatedCrashAfterLanding)·runtime에서 검증되나 **in-process 예외(fsync 이후)** 라 논리 복구 증명이다. OS-kill/power-loss는 미검증 → Backlog. 표현은 "injected in-process failure"로 유지.

### 7. NOW vs BACKLOG

NOW (범위 미증가):
1. H2 offset 연속성: `_validate_batch_scope` assert(min..max+1==count) + gapped-offset 테스트 (code=Codex) · 전제 문서화(반영).
2. H1 durability FS 경계 명시(반영).
3. H7 in-process 실패 성격 명시(반영).

BACKLOG (실제 시스템에 흔하다는 이유만으로 승격 금지 — 명시적으로 hold): multi-partition ordering/rebalance · Schema Registry/Avro/Protobuf · TLS/SASL/ACL · Spark Structured Streaming · direct Iceberg sink · multi-broker HA · continuous consumer service · production ops · end-to-end exactly-once · binary-lossless quarantine · same-group replay 관찰 테스트 · OS/power-loss fault injection · concurrent-writer landing.

### 8. K1.5 recommendation

**판정: proceed now — 기존 project loop를 닫기 때문(technology stitching 아님).**
근거: accepted JSONL envelope(event_id + Kafka coordinate + 정규화 payload)가 이미 있고, 기존 batch가 이미 quality/gold/Iceberg publish를 낸다. 둘을 잇는 adapter는 새 외부 기술(Spark SS 등) 없이 "streamed event → 기존 gold"라는 실제 사용자 압력(scenario 03에 이미 명시)을 충족하고, 기존 quality/lineage/idempotency evidence를 재사용한다.

구현 전 **가장 작은 gate**:
- decision note `reference-decisions/kafka-landing-to-batch-adapter.md`: (1) source identity — CSV의 `source_hash` 대신 landing-manifest/offset-range 기반 identity(kafka-offset note가 이미 "landing manifest hash" 후보 제시), (2) idempotency — 같은 accepted set 재adapt가 gold를 두 배로 만들지 않게 기존 `business_date` partition overwrite 재사용, (3) grain mapping — accepted event(event_id/business_date) → 기존 silver/gold natural key/grain.
- golden test 1개: accepted JSONL N개 → 기존 silver M → gold rows(기존 conservation/quality 재사용).
이 gate만 통과하면 새 Kafka 코드 없이 기존 pipeline 재사용으로 loop를 닫는다.

### 9. Safe public wording

**블로그(한국어, evidence-backed)**: "로컬 단일 broker Apache Kafka 4.3.1(KRaft) + confluent-kafka 2.15.0으로 bounded raw ingestion을 구현했다. 각 event를 정규화 payload와 `(topic, partition, offset)` 좌표와 함께 immutable JSONL로 landing하고, durable landing(파일 fsync → atomic rename → 부모 디렉토리 fsync) 뒤에만 offset을 수동으로 commit한다. landing 후·commit 전에 in-process 실패를 주입해 재전달이 accepted set을 늘리지 않음을, bounded replay가 정상 consumer-group 진행을 바꾸지 않음을 로컬 broker로 검증했다. confluent-kafka는 idempotence 기본이 false라 `enable.idempotence=true`·`acks=all`을 명시했다."

**이력서(영문)**: "Built a bounded, local single-broker Kafka raw-ingestion slice (Apache Kafka 4.3.1 KRaft, confluent-kafka 2.15.0): immutable JSONL landing with business identity + Kafka-coordinate evidence, landing-before-commit at-least-once recovery, bounded offset replay, and strict-schema invalid-event quarantine, verified against a local broker."

**금지(이유)**: "streaming pipeline / continuous / production / HA / multi-broker / exactly-once / Spark streaming / Iceberg sink"(범위 밖) · "crash-tested"를 power-loss/OS-kill로 함의(H7: in-process 논리 복구만) · "partition-key routing/ordering 검증"(H8: key만 설정, routing 미검증) · "durable on any storage"(H1: local POSIX FS 한정).

### 10. Changed files & Codex action items

**이 감사가 만든 변경(제안 상태):**
- `learn/reference-decisions/kafka-offset-and-landing-commit.md` — H1(FS durability 경계)·H2(offset 연속성 전제)·H7(in-process 실패 성격) caveat 추가.
- `learn/reference-evidence/audit-inputs/2026-07-14-kafka-k1/claude-audit.md` — 이 파일(received-unreviewed).

**Codex action items:**
- must-fix (correctness): **H2** — `landing._validate_batch_scope`에 offset 연속성 assert(또는 committable_offset 계약을 "연속 batch 전용"으로 코드에 강제) + gapped-offset 테스트 추가. 유일한 실제 correctness gap.
- recommended polish: decision note의 H1/H7 caveat 수용 여부 결정; 필요 시 source-contract §5에도 FS 경계 한 줄.
- Backlog(승격 금지): §7 목록.
- next slice: §8 K1.5 gate(decision note + golden test) 승인 여부 결정.

*Inference/Unknown 표시*: H1의 "directory fsync가 rename을 durable하게 만든다"는 Python 문서에 없고 POSIX/OS(local ext4류) 동작에 근거한 [INFERENCE]다. confluent-kafka 2.15.0의 정확한 기본값은 vendor 문서(current) 기준이며 Codex가 설치본에서 재확인 권장.

## Codex disposition (2026-07-16 final verification)

| Finding | Disposition | Reason / action |
|---|---|---|
| K1 publish-ready | Accept | 코드, unit test, local broker evidence와 claim boundary가 일치한다. |
| H1 filesystem boundary | Revise and accept | `os.replace` atomicity와 `os.fsync` 동작은 공식 Python 문서로 확인했다. 다만 전체 순서를 보편적 power-loss durability로 표현하지 않고 local Linux 검증 범위로 낮췄다. |
| H2 contiguous-offset assertion | Reject | Apache Kafka 공식 문서는 offset이 compacted topic/transaction에서 연속적이지 않을 수 있다고 명시한다. gap 거부는 정상 record를 거부할 수 있다. 대신 `land_records`가 consumer poll 순서인 strictly increasing offsets를 요구하고 마지막 처리 record의 `offset + 1`을 commit하도록 계약과 테스트를 고정했다. |
| H7 in-process failure boundary | Accept | SIGKILL/power-loss가 아니라 landing 뒤·commit 전 in-process 논리 복구임을 ADR과 source contract에 명시했다. |
| auto-commit default wording | Revise audit input only | `enable.auto.commit=true`와 automatic offset store 기본 조합은 처리 완료 전 commit될 수 있어 일반적인 at-least-once 보장으로 표현하지 않는다. K1 runtime은 `enable.auto.commit=false`와 처리 후 synchronous commit을 사용하므로 code 변경은 없다. |
| K1.5 next slice | Keep as approved candidate | source identity, idempotency, grain mapping decision note와 golden test를 먼저 고정한 뒤 별도 slice로 진행한다. |

검토한 공식 근거:

- Apache Kafka `KafkaConsumer`: offset은 연속 보장이 없으며 committed offset은 다음에 처리할 위치다.
- confluent-kafka 2.15.0 API: message commit은 해당 message의 `offset + 1`, consumer position은 마지막 consumed message의 `offset + 1`이다.
- librdkafka configuration: `enable.idempotence` 기본값은 false이며 true일 때 `acks=all`, retries, in-flight 제약을 조정한다.
- Python `os`: 같은 filesystem에서 성공한 `os.replace`는 atomic이고 `os.fsync`는 write를 disk로 강제한다.
