# Recovery-Gated Publish Boundary (S9)

ADR Status: Implemented
검토 상태: accepted-closed — Codex 독립 검토 완료 (2026-07-23)

> code/test/local Kafka + Spark/Iceberg runtime + local Airflow `dags test`로 검증했으나
> Codex 독립 검토 전이다. 최신 테스트 수와 실행 결과는 `VERIFICATION_LOG.md`가 source of truth다.

## Context

S8은 "봉인 구간이 전부 중앙에 반영됐는가"를 판정할 수 있게 했고, S7은 "그 landing을 Spark로
집계해 Iceberg gold에 발행"할 수 있게 했다. 둘 다 개별로는 검증됐지만, 두 계약이 **어느 쪽도
재구현하지 않고** 하나의 실행 경로로 이어진 적은 없었다.

S9의 압력은 하나다.

```text
복구가 완결됐다는 판정과, 그 세션의 데이터로만 batch가 돌았다는 보장이
Spark가 시작되기 전에 함께 성립해야 한다.
```

두 번째 절이 핵심이다. S8의 completeness는 **membership**(봉인된 event가 전부 accepted set에
있는가)만 말한다. 그런데 adapter는 해당 business_date의 accepted event를 **전부** 고른다.
같은 날짜에 다른 경로로 들어온 event가 하나라도 있으면 "복구 완결"은 참인데 발행되는 batch는
봉인 세션이 아니다. membership만으로는 이 상태를 막지 못한다.

## Decision

```text
sealed session -> [공유 readiness gate] -> 기존 K1.5 adapter
              -> [exact-session-input 동등성 검사] -> 기존 S7 Spark/quality/Iceberg
```

세부 계약:

| 결정 | 내용 | 이유 |
|---|---|---|
| gate 공유 | `require_recovery_ready(...)`를 S8에서 추출해 승격/발행 양쪽이 **같은 함수**를 쓴다 | gate가 두 벌이면 언젠가 갈라진다 |
| gate 위치 | Spark/Iceberg import·세션 생성 **이전** | 미완결 복구가 warehouse를 만들면 안 된다 |
| 완결성 판정 | S8 것을 그대로 재사용. S9는 재구현하지 않는다 | 판정 로직 중복은 계약 분기의 시작 |
| 입력 동등성 | 봉인 event_id 집합 == adapter 선택 event_id 집합 (**개수 + 집합 모두**) | membership ≠ 정확한 입력 집합 |
| 불일치 처리 | `SessionInputMismatchError`로 차단, `extra_event_ids`/`missing_event_ids` 보고 | 어느 쪽으로 어긋났는지 말할 수 있어야 한다 |
| Spark 계약 | S7 `run_spark_machine_event_batch`를 **호출만** 한다. transform/quality/Iceberg 코드 없음 | S7은 forbidden change |
| lazy import | `edge_recovery`는 module level에서 pyspark/pymongo를 import하지 않는다 | Kafka runbook venv가 gate를 import할 수 있어야 한다 |
| 실패 exit | gate 거부·집합 불일치·quality 실패 모두 exit 1 | 부분 상태로 전진 금지 |
| 실패 시 남는 것 | gate/date 거부: 아무 산출물 없음 · 집합 불일치: adapter staging은 남을 수 있음(trusted 아님) · quality 실패: S7 failure evidence는 남되 snapshot/success-state는 전진 없음 | "산출물 없음"으로 뭉뚱그리면 실제보다 강한 주장이 된다 |
| attempt vs producer | evidence는 `spark_attempt_run_id` + `snapshot_relation`을 기록한다. skip일 때 producer run_id는 **모른다**고 적는다 | S7이 노출하지 않는 값을 추측하면 안 된다 |
| status 대응 | published/skipped/quality_failed를 각각 다른 relation으로 적고, 모르는 status는 거부한다 | 이분법(published냐 아니냐)은 품질 실패를 "snapshot 재사용"으로 둔갑시킨다 |
| Airflow | BashOperator 1개짜리 wrapper. `max_active_runs=1` | recovery/coverage/Spark 로직이 DAG body로 새면 안 된다 |

### membership과 exact set은 다르다

```text
sealed  = {e1, e2, e3}
accepted(2026-06-29) = {e1, e2, e3, e9}   <- e9는 다른 경로로 들어옴

S8 completeness : sealed ⊆ accepted  -> true  (복구는 실제로 완결됐다)
S9 exact input  : sealed == selected -> false (발행될 batch는 이 세션이 아니다)
```

S9는 두 번째 판정을 **추가**한다. 첫 번째를 대체하지 않는다. `extra_event_ids=[e9]`로 차단한다.

### 왜 gate를 S9 안에 복사하지 않았나

복사하면 S8의 `promote_recovered_session`과 S9의 발행 경로가 서로 다른 완결성 정의를 갖게 되는
날이 온다. 그래서 S8 파일에서 gate를 추출하고 S8 자신도 그 함수를 호출하도록 바꿨다.
S8의 외부 동작(예외 타입·메시지·차단 시점)은 변하지 않았고 기존 S8 테스트가 그대로 통과한다.

### 다섯 identity space는 각자의 field에 기록된다

```text
edge sequence             (edge_source_id, boot_session_id, sequence_no)  복구 순서
event_id                  business identity                              중복 흡수 · 집합 판정
(topic,partition,offset)  transport evidence                             replay마다 달라짐
source_hash               batch 입력 identity                            idempotency 판정
attempt run_id / snapshot_id  실행 / 테이블 commit identity               감사 추적
```

이건 **schema/semantics 계약**이다. 각 space를 별도 field에 담고 뜻을 문서로 고정한다는 뜻이지,
"다섯 값이 서로 다르다"를 런타임에 증명한다는 뜻이 아니다 (값이 우연히 같아질 수 있는지는
이 계약과 무관하다).

runtime에서 관측된 반례는 하나다: edge sequence `[1,2,3]`이 offset `[0,1,4]`에 대응했다.
이 관측 하나가 "완결성을 offset 연속성으로 판정하면 안 된다"의 근거다(K1 offset 계약과 같은 이유).
검증 스크립트도 딱 이 관측만 `edge_sequence_not_kafka_offsets`로 확인한다.

### 물려받은 S7 동작: skipped 재실행은 새 `run_id`를 만든다

같은 `source_hash`로 재실행하면 S7은 `skipped`를 반환하는데, 이때 `run_id`는 **새로 발급**되고
`snapshot_id`는 그대로다. 실행 identity와 테이블 commit identity가 분리돼 있으니 일관된 동작이지만,
"identity chain 전체가 재실행 간 동일하다"는 주장은 **성립하지 않는다**. S9는 S7을 수정할 수
없으므로 이 동작을 바꾸지 않고 그대로 기록한다. 재실행 간 불변인 것은 `source_hash`와
`gold_snapshot_id`다.

그런데 그냥 기록만 하면 새 run_id와 기존 snapshot이 한 줄에 나란히 놓여 **"이 run이 이 snapshot을
만들었다"로 읽힌다.** 그래서 evidence를 이렇게 쓴다.

```text
spark.attempt_run_id                          이번 시도의 id (어떤 status든 항상 새 값)
iceberg.snapshot_relation                     status마다 정확히 하나로 결정된다 (아래 표)
iceberg.snapshot_created_by_current_attempt   true | false
iceberg.producer_attempt_run_id               published면 이번 attempt id, 그 외에는 null
```

status → snapshot relation은 **빠짐없이 대응**시킨다. default 분기를 두지 않는다.

| S7 status | snapshot_relation | created_by_current | producer_attempt_run_id | gold_snapshot_id |
|---|---|---|---|---|
| `published` | `created_by_current_attempt` | true | 이번 attempt | 발급된 값 |
| `skipped` | `reused_from_prior_attempt` | false | `null` (S7이 노출 안 함) | 이전 값 그대로 |
| `quality_failed` | `no_snapshot` | false | `null` | `null` |
| 그 외 | — | — | — | `UnexpectedSparkStatusError`로 거부 |

`skipped`일 때 producer run_id를 `null`로 두는 이유는 단순하다 — **S7이 그 값을 노출하지 않는다.**
모르는 값을 이번 attempt id로 채우면 그게 바로 거짓 인과다.

`quality_failed`를 따로 두는 이유도 같은 종류다. 품질 실패는 **아무것도 commit하지 않았다.**
만든 snapshot도 없고 재사용한 snapshot도 없다. 그런데 published가 아니라는 이유로 묶어서
`reused_from_prior_attempt`로 적으면 **존재하지도 않는 snapshot을 재사용했다고 주장**하게 된다.
exit code는 이미 맞았지만 evidence 문장이 거짓이었다. 그래서 relation을 세 갈래로 나누고,
모르는 status가 오면 조용히 "재사용"으로 분류하지 않고 예외로 거부한다.

### "재실행은 no-op"이 아니다

정확히는 이렇다.

```text
맞다 : 같은 source 재실행은 새 Iceberg snapshot을 만들지 않고 partition overwrite도 하지 않는다
아니다: 전체 파이프라인이 아무 일도 안 한다
```

S7은 `skipped`로 판정하기 **전에** 이미 Spark를 띄우고 silver/gold를 계산하고 quality를 돌린다.
비용은 발생하고 로그도 남는다. 불변인 것은 발행된 테이블 상태와 입력 identity다.

## Failure states

| Failure point | Result | Recovery |
|---|---|---|
| 봉인 세션 없음 / seal 불일치 | gate에서 차단, Spark 미시작 | spool 조사 |
| 요청 business_date ≠ 세션 날짜 | `EdgeSessionScopeError`, 산출물 없음 | 올바른 날짜로 재요청 |
| 복구 미완결 | `RecoveryIncompleteError`, warehouse/adapter 생성 없음 | 남은 sequence replay |
| 같은 날짜에 세션 밖 event 존재 | `SessionInputMismatchError` + `extra_event_ids` | 입력 경로 조사 |
| 봉인 event가 adapter 선택에서 빠짐 | `SessionInputMismatchError` + `missing_event_ids` | landing 조사 |
| quality 실패 | exit 1, snapshot/success-state 전진 없음. S7 failure evidence는 남을 수 있음. evidence는 `snapshot_relation=no_snapshot` · `gold_snapshot_id=null` | 데이터 조사 |
| 같은 입력 재실행 | `skipped`. 새 snapshot 없음·partition overwrite 없음, 같은 `source_hash`·`snapshot_id`. attempt run_id는 새 값이고 `snapshot_relation=reused_from_prior_attempt` | 정상 |

## Boundaries

- **조합 경계**: S8/S7 계약을 **잇기만** 한다. 어느 쪽도 재구현하거나 수정하지 않는다.
- **범위 경계**: session 1개 · machine 1개 · business_date 1개 · topic/partition 1개 · Iceberg gold table 1개 · single writer.
- **원자성 경계**: gate 통과 후 Spark/Iceberg 사이의 분산 원자성은 주장하지 않는다. 집합 불일치나 발행 실패 시 adapter staging 산출물은 남을 수 있고, 이는 trusted output이 아니다.
- **재실행 경계**: 재실행이 아끼는 것은 테이블 상태(새 snapshot·overwrite 없음)이지 연산 비용이 아니다. S7은 skip 판정 전에 Spark를 띄우고 quality를 돌린다.
- **Airflow 경계**: `dags test` 수준의 import/wiring/command 실행 증거일 뿐 scheduler/executor 운영 증거가 아니다.
- **연속성 경계**: streaming sink가 아니다. 봉인된 한 세션을 bounded batch로 발행한다.

## Alternatives

| Option | Why not S9 |
|---|---|
| S9 안에 완결성 판정 복사 | 두 벌의 gate가 갈라진다. 그래서 공유 함수로 추출. |
| membership만으로 발행 허용 | 같은 날짜의 세션 밖 event가 섞인 batch를 "완결"로 발행한다. |
| adapter에 session filter를 추가 | K1.5 결정성 계약을 바꾼다(forbidden). 검사는 조합 layer의 책임. |
| Spark 실행 후 사후 검증 | 잘못된 snapshot이 이미 커밋된 뒤다. gate는 앞에 있어야 한다. |
| Kafka → Iceberg streaming sink | 이 압력(봉인 세션의 정확한 발행)과 무관하고 scope를 폭발시킨다. |
| Airflow에 bronze/silver/gold task 분해 | S7이 이미 단일 CLI 계약으로 결정했다. DAG는 얇게 유지. |

## Claim boundary

말할 수 있는 것:

```text
synthetic·local·bounded recovery-gated batch 경로를 구현했다: 봉인된 edge 세션 1개와
실제 local Kafka landing 증거를 기존 결정적 adapter · Spark quality gate ·
Iceberg gold table snapshot 1개에 묶었다. 미완결 복구와 같은 날짜 event-set 불일치는
Spark/Iceberg 발행을 차단하고, 같은 source 재실행은 새 Iceberg snapshot을 만들지 않고
partition overwrite도 하지 않는다(전체 파이프라인 no-op은 아니다 — S7은 skip 판정 전에 Spark와
quality를 실행한다).
통합 CLI는 얇은 local Airflow dags-test wrapper로 호출된다.
필수 수식어: synthetic · local · bounded · session/machine/date/topic/partition 각 1개 ·
local Iceberg gold table 1개 · Airflow dags test.
```

말할 수 없는 것:

```text
production 산업 IoT / 자율공장 플랫폼
continuous·대규모 streaming, Kafka→Iceberg streaming sink
full Spark/Iceberg medallion 플랫폼, cluster Spark, 성능 개선
production/HA/분산 Airflow 운영
multi-partition/rebalance/concurrent-writer correctness
end-to-end exactly-once 또는 분산 원자성
실제 edge 하드웨어·OT 프로토콜 연동
```

## Evidence

- `src/manufacturing_data_platform/pipeline/recovered_telemetry_publish.py`
- `src/manufacturing_data_platform/edge_recovery.py` (`require_recovery_ready`)
- `tests/test_recovered_telemetry_publish.py`
- `scripts/verify_recovered_telemetry_publish.sh`, `scripts/recovered_telemetry_publish_verification.py`
- `dags/manufacturing_recovered_telemetry_publish.py`
- `learn/reference-decisions/edge-buffer-and-recovery-progress.md` (S8 gate)
- `learn/reference-decisions/spark-engine-swap-contract.md` (S7 발행 계약)
- `VERIFICATION_LOG.md`
