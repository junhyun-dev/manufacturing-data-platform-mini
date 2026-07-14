# 08. Kafka / Streaming Ingestion 상세 질문 은행

상태: audited / local K1 implemented and broker-verified

상위 문서:

- [`../08-area-question-bank.ko.md`](../08-area-question-bank.ko.md)
- [`../scenarios/03-kafka-machine-event-ingestion.md`](../scenarios/03-kafka-machine-event-ingestion.md)

이 문서는 Kafka 기능 목록이 아니다.

```text
제조 설비 event를 Kafka로 받아 raw landing에 보존한다면
어떤 질문에 답해야 설계와 테스트가 만들어지는가?
```

를 영역별로 넓게 펼친다.

질문은 넓게 유지하되 구현은 [`../slices/05-kafka-raw-ingestion.ko.md`](../slices/05-kafka-raw-ingestion.ko.md)에서 Core만 자른다.

## 0. 먼저 구분할 말

| 용어 | 쉬운 뜻 | 이 프로젝트에서 아직 결정할 것 |
|---|---|---|
| event | 설비가 발생시킨 한 건의 사실 | row와 같은 business grain인가 |
| topic | 같은 종류의 event가 쌓이는 이름 있는 log | topic을 event type별로 나눌지 |
| partition | topic을 나눈 ordered log 조각 | key와 partition 수 |
| offset | partition 안에서 event가 놓인 위치 | replay/evidence에 어떻게 남길지 |
| producer | event를 Kafka에 쓰는 프로그램 | acknowledgement와 retry 정책 |
| consumer group | partition을 나눠 읽는 consumer 묶음 | group identity와 scale 방식 |
| committed offset | consumer가 다음에 다시 읽을 위치 | raw write 전/후 commit 시점 |
| event time | 실제 설비에서 event가 발생한 시각 | `business_date`와의 관계 |
| processing time | consumer/Spark가 event를 본 시각 | 지연 측정에 사용할지 |
| watermark | 너무 늦은 event를 언제까지 기다릴지 정하는 경계 | 첫 slice에는 필요한지 |
| replay | 이전 offset으로 돌아가 다시 읽기 | 결과 중복을 어떻게 막을지 |
| delivery semantics | 유실/중복을 어느 쪽으로 허용할지에 대한 계약 | at-least-once가 맞는지 |

## 1. Service Purpose / Latency Pressure

| 질문 | 쉬운 말 | 왜 묻는가 | 선택지 | 초기 분류 |
|---|---|---|---|---|
| CSV batch보다 빨라야 하는 사용자는 누구인가? | 누가 하루 마감을 기다리지 못하는가? | Kafka 도입 이유를 도구가 아니라 사용자 압력에 묶는다. | operator raw visibility / near-real-time metric / ML feature / 이유 없음 | Core |
| 필요한 latency는 얼마인가? | 몇 초, 몇 분, 한 시간 중 어디까지면 되는가? | 요구 시간이 micro-batch와 continuous 처리 선택을 바꾼다. | seconds / minutes / hourly / daily | Core |
| 첫 slice가 만드는 가치는 무엇인가? | 화면을 빨리 보여주는 것인가, event를 잃지 않고 받는 것인가? | ingestion과 analytics scope를 분리한다. | raw landing / alert / live aggregate / dashboard | Core |
| Kafka 없이 풀 수 있는가? | file watch나 단순 queue로도 충분하지 않은가? | 기술 과잉을 막는다. | file drop / database queue / Kafka | Core |
| 언제 streaming을 중단하고 batch fallback을 쓰는가? | broker가 오래 죽으면 파일로 받을 수 있는가? | source delivery continuity를 설계한다. | no fallback / local spool / daily CSV fallback | Backlog |

Working direction:

```text
첫 slice의 목적은 실시간 dashboard가 아니라
bounded synthetic event set을 replay 가능한 raw landing으로 넘기는 것이다.
```

## 2. Event Grain / Identity / Contract

| 질문 | 쉬운 말 | 왜 묻는가 | 선택지 | 초기 분류 |
|---|---|---|---|---|
| event 한 건은 정확히 무엇인가? | 한 메시지가 생산 누계인가, 변화량인가, 상태 변경인가? | metric 합산과 duplicate 영향이 달라진다. | snapshot / delta / state transition | Core |
| business event identity는 무엇인가? | 같은 사건이 다시 온 것을 어떻게 아는가? | Kafka offset은 재전송된 business duplicate를 식별하지 못한다. | producer `event_id` / natural key / payload hash | Core |
| Kafka coordinates는 identity인가 evidence인가? | offset이 같아야만 같은 사건인가? | replay 위치와 business identity를 혼동하지 않는다. | transport identity only / business identity로 오용 | Core |
| message key는 무엇인가? | 어떤 event들을 같은 줄에 세워 순서를 지킬 것인가? | key가 partition 배치와 ordering 범위를 정한다. | `machine_id` / `line_id` / `work_order_id` / null | Core |
| schema version을 payload에 넣는가? | 메시지 모양이 바뀌면 consumer가 어떻게 아는가? | producer/consumer 독립 배포와 evolution에 필요하다. | integer field / header / registry id / 없음 | Core-lite |
| 필수/선택 필드는 무엇인가? | 빠지면 거부할 값과 비어도 되는 값은 무엇인가? | invalid event 처리 계약을 만든다. | JSON validation / typed serialization / loose dict | Core |
| metric 값은 누계인가 증가량인가? | `units_produced=120`을 매번 더해도 되는가? | 누계를 delta처럼 합치면 gold가 틀린다. | cumulative / delta / explicit event type | Core |
| event contract 소유자는 누구인가? | producer가 마음대로 필드를 바꿀 수 있는가? | contract change 책임과 호환성 판단이 필요하다. | producer / platform / shared governance | Backlog(named) |
| payload serialization format은 무엇인가? | JSON으로 보낼지, Avro/Protobuf 같은 binary schema로 보낼지 | schema evolution 방식, 메시지 크기, Schema Registry 필요성이 달라진다. | JSON(schema-on-read) / Avro·Protobuf + Schema Registry / raw bytes | Core-lite: K1은 JSON. Avro/registry는 Backlog. |

놓치기 쉬운 질문:

```text
event_id가 producer 재시작 뒤에도 유일한가?
event_id 생성 실패 시 event를 버릴 것인가?
같은 payload지만 다른 실제 사건이면 hash dedup이 잘못 합치지 않는가?
schema_version만 올리고 실제 compatibility 검사를 안 하는 것은 아닌가?
```

## 3. Topic / Key / Partition / Ordering

| 질문 | 쉬운 말 | 왜 묻는가 | 선택지 | 초기 분류 |
|---|---|---|---|---|
| topic을 무엇으로 나누는가? | 생산/불량 event를 한 통에 넣을지 나눌지 | schema, retention, consumer ownership이 달라진다. | one event topic / event-type topics / plant topics | Core |
| ordering은 어디까지 필요한가? | 모든 공장 전체 순서인가, 같은 machine 안의 순서인가? | Kafka ordering은 partition 안에서만 성립한다. | global / machine / work order / 불필요 | Core |
| key가 null이면 어떤 문제가 생기는가? | 같은 machine event가 다른 partition에 흩어져도 되는가? | 순서와 stateful 처리 정확성에 영향을 준다. | null 허용 / key 필수 | Core |
| partition 수는 어떻게 정하는가? | 병렬성을 얼마나 열어둘 것인가? | consumer 수, ordering, 운영 비용을 함께 결정한다. | 1 / small fixed number / throughput 기반 | Core-lite |
| partition 수를 나중에 늘리면 key ordering이 어떻게 되는가? | 같은 key의 과거/미래 위치가 달라질 수 있는가? | 재분배 후 전역적인 key history 해석에 주의해야 한다. | immutable count / controlled expansion | Backlog |
| hot key가 생기면 어떻게 아는가? | 한 machine/line에 event가 몰리는가? | partition skew와 lag를 만든다. | per-partition rate / key sampling / ignore locally | Backlog(named) |

Working direction candidate:

```text
topic: manufacturing.machine-events.v1
key candidate: machine_id
ordering claim: same partition/key 범위만
local broker: one broker, replication factor 1
```

아직 decision이 아니다. audit에서 topic/key/partition 근거를 검토한다.

## 4. Producer Reliability

| 질문 | 쉬운 말 | 왜 묻는가 | 선택지 | 초기 분류 |
|---|---|---|---|---|
| 어떤 acknowledgement를 요구하는가? | broker가 어디까지 저장해야 성공으로 볼 것인가? | durability와 latency가 달라진다. | `acks=0/1/all` | Core |
| producer retry를 켜는가? | 일시 오류 때 다시 보낼 것인가? | 유실을 줄이지만 duplicate 가능성을 만든다. | no retry / bounded retry / client default | Core |
| idempotent producer를 어떻게 켜는가? | retry가 같은 record를 broker log에 중복 기록하지 않게 할 것인가? | client마다 기본값이 다르다. Java producer는 KIP-679(Kafka 3.0) 이후 기본 true지만, librdkafka/confluent-kafka-python은 기본 false다. confluent-kafka를 쓰면 `enable.idempotence=true`를 명시해야 하고, 켜면 acks=all·retries>0·max.in.flight<=5가 요구된다. | client default 확인 후 명시적으로 enable / disable | Core |
| send 결과를 어떻게 evidence로 남기는가? | 몇 건을 어느 partition/offset에 썼는가? | produced count와 landing reconciliation에 필요하다. | delivery callback / summary JSON / logs only | Core |
| serialization 실패는 어떻게 처리하는가? | 잘못된 event 하나 때문에 전체 batch를 멈출 것인가? | partial send와 source accountability를 다룬다. | fail-fast / collect errors / quarantine before send | Core |
| producer가 broker보다 빨리 만들면 어떻게 제어하는가? | 메모리에 계속 쌓여도 되는가? | buffer exhaustion과 latency를 막는다. | block / bounded buffer / drop | Backlog(named) |
| transaction producer가 필요한가? | 여러 topic write를 한 묶음으로 commit할 것인가? | 단일 topic raw ingestion에는 과할 수 있다. | none / Kafka transaction | Backlog |

중요한 경계:

```text
idempotent producer는 같은 producer session의 retry duplicate 문제를 줄인다.
단 client마다 기본값이 다르다: Java producer는 기본 true, librdkafka/confluent-kafka-python은 기본 false이므로, 선택한 client에서 명시적으로 켰는지 확인해야 한다.
business event가 새 요청으로 다시 만들어지는 문제는 event_id/dedup contract가 별도로 필요하다.
```

## 5. Consumer Group / Offset / Commit / Rebalance

| 질문 | 쉬운 말 | 왜 묻는가 | 선택지 | 초기 분류 |
|---|---|---|---|---|
| consumer group id는 무엇을 의미하는가? | 같은 일을 나눠 하는 consumer 묶음의 이름은 무엇인가? | 재시작 위치와 독립 소비자를 분리한다. | stable versioned group / random group | Core |
| auto commit을 쓸 것인가? | 읽기만 하면 자동으로 처리 완료 표시할 것인가? | durable write 전에 commit하면 event 유실이 가능하다. | auto / manual after durable write | Core |
| offset은 정확히 언제 commit하는가? | 파일 쓰기 전인가, 끝난 뒤인가? | 유실과 duplicate 사이의 핵심 경계다. | before write / after write / transaction | Core |
| poll과 processing 시간이 길면 어떻게 되는가? | consumer가 죽은 것으로 오인되어 partition이 넘어가는가? | rebalance와 duplicate processing을 만든다. | small poll batch / tune timeout / worker split | Core-lite |
| rebalance 중 처리 중 event는 어떻게 되는가? | partition 소유자가 바뀔 때 unfinished write가 남는가? | revoke 시 flush/commit 계약이 필요하다. | stop-and-flush / abandon and replay | Backlog(named) |
| offset out of range면 어디서 시작하는가? | retention으로 과거 offset이 사라졌다면? | recovery 시 data loss를 숨기지 않는다. | earliest / latest / fail and investigate | Core-lite |
| consumer 수가 partition 수보다 많으면 무슨 의미인가? | 놀고 있는 consumer가 생기는가? | scale claim을 정확히 한다. | consumer <= partition / standby 허용 | Demo |
| group을 바꾸면 전체 replay가 되는가? | 새 group이 처음부터 읽는가? | replay 방법과 production consumer state를 분리한다. | new group / seek / offset reset | Core-lite |
| 새 rebalance protocol(KIP-848)을 쓸 것인가? | 재분배를 stop-the-world 없이 할 것인가? | Kafka 4.0에서 GA. 서버 기본 on이고, consumer는 `group.protocol=consumer`로 opt-in한다(안 하면 classic). | classic / consumer | Backlog(named). K1은 1-consumer라 classic으로 충분. |
| `isolation.level`은 무엇으로 두는가? | 아직 commit 안 된 transactional 메시지도 읽을 것인가? | 같은 topic에 transactional record가 있을 때 visibility를 바꾼다. | read_uncommitted(기본) / read_committed | Backlog(named). K1 topic은 non-transactional record만 받으므로 두 모드의 결과 차이가 없다. transactional writer를 도입할 때 다시 결정한다. |

## 6. Delivery Semantics / Duplicate Contract

| 질문 | 쉬운 말 | 왜 묻는가 | 선택지 | 초기 분류 |
|---|---|---|---|---|
| 유실과 중복 중 어느 쪽을 허용할 것인가? | 한 번 더 받기와 못 받기 중 무엇이 더 안전한가? | delivery contract의 출발점이다. | at-most-once / at-least-once / exactly-once scope | Core |
| at-least-once duplicate는 어디서 제거하는가? | 재전달된 event를 raw와 silver 중 어디서 합칠 것인가? | raw 보존과 accepted dataset의 의미를 나눈다. | raw keeps all + silver dedup / landing idempotence / sink upsert | Core |
| `(topic, partition, offset)` dedup과 `event_id` dedup은 어떻게 다른가? | 같은 log record와 같은 business 사건은 같은 개념인가? | transport replay와 producer duplicate를 모두 다룬다. | both keys / one key only | Core |
| exactly-once라고 말하려면 경계가 어디까지인가? | Kafka 안에서만인가, raw file과 Iceberg까지인가? | end-to-end 과장을 막는다. | producer only / Kafka transaction / Spark checkpoint+sink / no claim | Core claim gate |
| replay 결과는 기존 landing과 합쳐지는가 분리되는가? | 다시 읽은 event를 새 폴더에 둘 것인가? | auditability와 duplicate control이 달라진다. | overwrite accepted set / replay run folder / append all | Core-lite |

Working direction candidate:

```text
at-least-once ingestion을 가정한다.
raw evidence는 Kafka coordinates를 보존한다.
accepted landing/downstream은 event_id와 transport coordinates의 역할을 분리해 dedup한다.
end-to-end exactly-once는 claim하지 않는다.
```

## 7. Event Time / Late Data / Watermark

| 질문 | 쉬운 말 | 왜 묻는가 | 선택지 | 초기 분류 |
|---|---|---|---|---|
| `business_date`는 event time에서 계산하는가? | 도착한 날짜가 아니라 실제 발생 날짜로 묶는가? | late event의 partition 귀속을 결정한다. | producer field / consumer derive / processing date | Core |
| clock skew를 어떻게 다루는가? | 설비 시계가 틀리면 event_time을 믿을 수 있는가? | 잘못된 partition과 window를 만들 수 있다. | trust source / bound validation / ingest timestamp fallback | Backlog(named) |
| 얼마나 늦은 event를 정상으로 받는가? | 하루 뒤 event도 이전 gold를 고칠 것인가? | correction window와 state retention을 결정한다. | unlimited / bounded lateness / manual backfill | Backlog |
| watermark가 첫 slice에 필요한가? | raw 보존만 하는데 늦은 event cutoff가 필요한가? | processing과 ingestion 범위를 분리한다. | no watermark in raw landing / Structured Streaming slice | Backlog |
| late event가 오면 Iceberg를 어떻게 고치는가? | 지난 `business_date` partition을 다시 계산할 것인가? | 현재 partition-overwrite contract와 연결된다. | batch correction / micro-batch overwrite / MERGE | Next slice |

첫 slice 판단 후보:

```text
raw landing은 late event를 버리지 않고 event_time + ingest_time을 함께 보존한다.
watermark와 window aggregate는 Spark Structured Streaming slice로 미룬다.
```

## 8. Raw Landing / File Commit / Existing Pipeline Boundary

| 질문 | 쉬운 말 | 왜 묻는가 | 선택지 | 초기 분류 |
|---|---|---|---|---|
| raw landing unit은 무엇인가? | event마다 파일인가, offset 묶음마다 파일인가? | small-files와 재시작 contract를 결정한다. | one file/event / bounded JSONL batch / Parquet micro-batch | Core |
| 파일은 언제 완성됐다고 보는가? | 쓰는 중인 파일을 downstream이 읽지 않게 하는가? | partial file 노출을 막는다. | temp + atomic rename / manifest publish / direct append | Core |
| landing path에 무엇을 넣는가? | 날짜/topic/partition별로 찾을 수 있는가? | replay와 downstream scan 범위를 정한다. | ingest date / event date / topic-partition / run id | Core |
| raw payload를 그대로 보존하는가? | parse한 값만 남기면 원본을 재검증할 수 있는가? | schema bug와 audit를 위해 원문이 필요할 수 있다. | original bytes/base64 / decoded JSON / both | Core-lite |
| Kafka metadata를 row마다 남기는가? | offset range만 manifest에 쓰면 개별 event를 찾을 수 있는가? | lineage와 dedup granularity가 달라진다. | per-row coordinates / file-level range / both | Core |
| 기존 CSV pipeline에 어떻게 넘기는가? | JSONL을 바로 읽게 바꿀지 CSV snapshot을 만들지 | 기존 증거를 재사용할 범위를 정한다. | adapter to existing rows / new source contract / Spark reader | Backlog boundary |
| source_hash는 Kafka에서 무엇을 hash하는가? | file hash가 없는데 같은 input을 어떻게 표현하는가? | 기존 idempotency 모델을 그대로 복사할 수 없다. | landing manifest hash / offset range identity / event set hash | Next decision |

## 9. Failure / Retry / Replay / Quarantine

| 질문 | 쉬운 말 | 왜 묻는가 | 선택지 | 초기 분류 |
|---|---|---|---|---|
| invalid event 하나가 partition을 막는가? | 계속 실패하는 한 건 때문에 뒤 event를 못 읽는가? | poison message 운영이 필요하다. | fail-stop / retry then quarantine / skip with evidence | Core-lite |
| retry 횟수와 간격은 누가 소유하는가? | client 내부 retry와 app retry가 겹치는가? | retry storm과 지연을 막는다. | client / application / orchestrator | Core-lite |
| DLQ는 원본 topic인가 별도 topic인가? | 실패 event를 어디에 보관하는가? | reprocessing과 민감정보 경계가 생긴다. | local quarantine file / DLQ topic / no DLQ | Backlog |
| replay는 누가 승인하는가? | 아무나 offset을 되돌려도 되는가? | duplicate output과 운영 사고를 막는다. | CLI explicit / operator workflow / automated | Core-lite |
| replay 전에 영향 범위를 계산하는가? | 어느 business_date와 Iceberg partition이 바뀌는가? | downstream correction blast radius를 줄인다. | offset-to-date manifest / full rebuild | Backlog |
| broker unavailable 시 producer는 어디에 쌓는가? | event를 메모리에만 두다 잃지 않는가? | source-side durability를 다룬다. | fail / local spool / bounded retry | Backlog(named) |

## 10. Retention / Compaction / Data Lifecycle

| 질문 | 쉬운 말 | 왜 묻는가 | 선택지 | 초기 분류 |
|---|---|---|---|---|
| topic retention은 얼마인가? | 언제까지 replay할 수 있는가? | recovery window와 저장비용을 결정한다. | hours / days / indefinite local | Core-lite |
| cleanup policy는 delete인가 compact인가? | 오래된 event를 시간으로 지울지 key별 최신값만 둘지 | event log와 current-state topic의 의미가 다르다. | delete / compact / both | Core-lite |
| 이 event topic에 compaction이 맞는가? | 과거 생산 사건을 key별 최신 한 건만 남겨도 되는가? | append event history가 사라질 수 있다. | event log uses delete / state topic uses compact | Core-lite |
| raw landing retention과 Kafka retention은 같은가? | broker에서 지워져도 raw에는 남는가? | source-of-truth와 replay 가능 범위를 명확히 한다. | independent policies / same window | Backlog(named) |
| 삭제/PII 요청이 오면 둘 다 지울 수 있는가? | Kafka와 raw copy에 남은 데이터를 어떻게 처리하는가? | governance scope를 드러낸다. | synthetic no-PII / production policy | Backlog |

## 11. Security / Access Boundary

| 질문 | 쉬운 말 | 왜 묻는가 | 선택지 | 초기 분류 |
|---|---|---|---|---|
| local broker는 plaintext인가? | 학습 환경과 production 보안을 구분하는가? | local simplicity를 운영 claim으로 확대하지 않는다. | PLAINTEXT local / TLS production | Core claim gate |
| producer/consumer 인증은 어떻게 하는가? | 아무 client나 topic을 읽고 쓸 수 있는가? | production access control에 필수다. | none local / SASL / mTLS | Backlog(named) |
| topic ACL은 actor별로 다른가? | producer는 write만, consumer는 read만 가능한가? | least privilege를 설계한다. | broad local / role ACL | Backlog |
| credential은 어디에 보관하는가? | 코드나 DAG에 credential 값을 넣지 않는가? | 공개 repo 안전과 배포 경계를 지킨다. | env/credential manager / committed config 금지 | Core publication gate |
| payload에 민감정보가 있는가? | raw event를 그대로 오래 보존해도 되는가? | retention과 masking에 영향을 준다. | synthetic no-PII / classify production | Core claim gate |

## 12. Observability / Operator Evidence

| 질문 | 쉬운 말 | 왜 묻는가 | 선택지 | 초기 분류 |
|---|---|---|---|---|
| produced/consumed/landed count를 비교하는가? | 중간에 몇 건이 빠졌는지 아는가? | first slice의 가장 직접적인 reconciliation이다. | summary JSON / metrics / logs only | Core |
| consumer lag를 어떻게 보는가? | producer보다 얼마나 뒤처졌는가? | stream health의 기본 신호다. | CLI/admin API / exported metric / not measured | Demo |
| per-partition offset range를 남기는가? | 어느 구간을 처리했는지 아는가? | replay와 RCA evidence다. | manifest / log only | Core |
| invalid/retried/quarantined count를 구분하는가? | 실패를 한 숫자로 뭉개지 않는가? | operator action이 달라진다. | structured counters / generic error log | Core-lite |
| report 자체가 오래된 것은 어떻게 아는가? | 마지막 성공 시각을 모르고 정상이라 믿지 않는가? | observability freshness를 다룬다. | generated_at + last offset timestamp | Demo |
| trace context를 event에서 downstream run까지 잇는가? | Kafka event와 Iceberg snapshot을 연결할 수 있는가? | lineage 확장에 필요하다. | event_id/offset manifest -> run_id -> snapshot_id | Backlog |

## 13. Performance / Scale / Backpressure

| 질문 | 쉬운 말 | 왜 묻는가 | 선택지 | 초기 분류 |
|---|---|---|---|---|
| expected event rate와 message size는 얼마인가? | 초당 몇 건, 한 건 몇 KB인가? | Kafka 필요성과 partition 수 근거다. | measured synthetic profile / unknown | Core-lite |
| producer batch/compression은 필요한가? | 여러 event를 묶고 압축할 것인가? | throughput과 latency tradeoff다. | defaults / linger+batch / compression | Backlog |
| consumer poll batch 크기는 얼마인가? | 한 번에 너무 많이 읽어 write가 늦어지지 않는가? | memory와 rebalance risk를 바꾼다. | bounded count/bytes/time | Core-lite |
| downstream이 느릴 때 어디에 압력이 쌓이는가? | broker lag, client memory, local disk 중 어디가 차는가? | 장애 전파와 capacity를 이해한다. | Kafka retention buffer / bounded landing queue | Backlog(named) |
| scale claim은 무엇으로 증명하는가? | local 10건 실행으로 대규모를 말하지 않는가? | public claim 경계다. | functional proof only / measured load test | Core claim gate |

## 14. Local Runtime / Testing / Reproducibility

| 질문 | 쉬운 말 | 왜 묻는가 | 선택지 | 초기 분류 |
|---|---|---|---|---|
| local broker를 어떻게 띄우는가? | Docker 없이도 재현 가능한가? | 현재 환경의 첫 구현 gate다. | downloaded KRaft binary / Docker / remote broker | Unknown/Core gate |
| Kafka와 client version을 어떻게 pin하는가? | 나중에 같은 환경을 만들 수 있는가? | API/config drift를 막는다. | requirements + runtime note | Core |
| broker가 없을 때 unit test는 무엇을 검증하는가? | 모든 테스트를 integration에 의존하지 않는가? | 빠른 contract test와 runtime proof를 분리한다. | pure serializer/landing tests / mocks | Core |
| integration test는 broker 부재 시 어떻게 되는가? | CI에서 조용히 false green이 되지 않는가? | optional dependency 정책을 명확히 한다. | explicit skip reason / separate job / hard fail in verification | Core |
| runtime proof는 무엇인가? | import 성공이 아니라 실제 send/read/replay를 했는가? | Kafka skill claim의 최소 evidence다. | broker start + topic + produce + consume + assertions | Core |
| clean rerun은 가능한가? | 이전 topic/offset/data가 결과를 오염시키지 않는가? | deterministic verification을 만든다. | unique topic/group / cleanup script | Core |

현재 환경 확인 (`2026-07-14`; 상세 결과는 [`../../../VERIFICATION_LOG.md`](../../../VERIFICATION_LOG.md)):

```text
Java 17: available
usable Docker runtime: unavailable in this environment
Kafka runtime: Apache Kafka 4.3.1 downloaded binary, SHA-512 verified
Python client: confluent-kafka 2.15.0, isolated venv
Test 0: one broker/topic/partition/event round-trip + manual offset commit passed
```

따라서 local KRaft binary 실행과 Python client 호환성 Unknown은 닫혔다.
K1 raw landing, landing-after-rename/before-commit crash recovery, bounded replay,
invalid-event quarantine도 broker-backed verification으로 닫혔다.

### Test 0 (2026-07-14 검증 완료)

```text
1. Kafka 4.x는 KRaft-only(ZooKeeper 제거)다. 단일 노드 절차:
   kafka-storage.sh random-uuid
   -> kafka-storage.sh format --standalone -t <cluster-id> -c config/server.properties
   -> kafka-server-start.sh config/server.properties
   (첫 기동 전 storage format 누락이 가장 흔한 첫 실패다.)
2. Java 17+ 확인(broker 요구). 현재 환경 available.
3. topic 1개(partition=1) 생성.
4. 선택한 Python client wheel 설치 + broker 연결 확인.
5. produce 1 / consume 1 round-trip.
broker가 없으면 integration test는 명시적 skip 사유로 남기고 false green을 만들지 않는다.
```

재현 명령:

```bash
./scripts/verify_kafka_test0.sh
```

### Python client 비교와 Test 0 선택

셋 다 2026 기준 유지되고 있으므로 "유지보수" 하나로 고르지 않는다. 아래 기준으로 비교했다.

| client | runtime dependency | idempotence 기본값 | 특징 | 확인할 점 |
|---|---|---|---|---|
| confluent-kafka | librdkafka C extension(wheel) | false (명시 필요) | 기능 완전(transaction/registry), 빠름 | C wheel의 플랫폼/파이썬 호환 |
| kafka-python | pure Python | 선택 client에서 확인 | 순수 파이썬이라 설치 단순, 3.x에서 protocol 재작성 | 최신 broker protocol 지원 범위 |
| aiokafka | asyncio 기반 | 선택 client에서 확인 | async I/O에 적합 | 현재 sync 코드베이스와의 정합성 |

비교 기준: sync API 적합성 / runtime dependency / wheel availability / protocol support / testability.

Test 0과 K1의 선택은 `confluent-kafka==2.15.0`이다. 현재 sync 코드베이스에 맞고,
CPython 3.10 wheel 설치와 Kafka 4.3.1 연결이 실제로 통과했기 때문이다.
`enable.idempotence=true`와 `acks=all`은 기본값에 기대지 않고 명시한다.

## 15. Airflow / Spark / Iceberg Integration Boundary

| 질문 | 쉬운 말 | 왜 묻는가 | 선택지 | 초기 분류 |
|---|---|---|---|---|
| 계속 도는 consumer를 Airflow task로 띄우는가? | 끝나지 않는 프로그램을 batch scheduler가 소유해야 하는가? | task lifecycle과 retry semantics가 맞지 않을 수 있다. | standalone service / bounded consume job / Airflow sensor/trigger | Core architecture question |
| Airflow는 Kafka에서 무엇을 맡는가? | topic 소비 자체인가, backfill/replay job 조정인가? | orchestration과 streaming runtime 책임을 나눈다. | bounded replay / batch publish / no role in continuous path | Core-lite |
| Spark Structured Streaming은 언제 필요한가? | raw landing consumer 다음에 왜 Spark를 붙이는가? | stateful window/scale pressure가 생길 때만 도입한다. | no Spark first slice / next micro-batch slice | Backlog |
| Kafka offset과 Spark checkpoint는 어떤 관계인가? | Spark가 재시작 위치를 어디에 저장하는가? | 별도 consumer commit 모델을 그대로 적용하면 안 된다. | Spark checkpoint owns progress / manual consumer offset | Backlog |
| Kafka -> Iceberg sink의 exactly-once 경계는 무엇인가? | checkpoint와 table commit이 함께 안전한가? | 강한 claim에는 failure injection evidence가 필요하다. | foreachBatch + idempotent batch id / connector / no claim | Backlog |
| late event correction은 기존 partition overwrite와 연결되는가? | 늦은 event가 오면 같은 날짜 gold를 다시 만들 것인가? | batch와 streaming design을 하나의 business contract로 연결한다. | trigger batch correction / micro-batch MERGE/overwrite | Backlog |

초기 방향:

```text
Slice K1:   Kafka producer/consumer -> bounded raw landing
Slice K1.5 candidate: landed JSONL을 기존 batch pipeline row contract로 변환해 gold/Iceberg publish를 재사용
                      (K1 완료 후 adapter 비용과 contract를 확인해야 하며, 현재 Core 아님)
Slice K2:   Spark Structured Streaming read/checkpoint/window (Backlog — window/latency pressure가 명시될 때만 Core로 승격)
Slice K3:   bounded Iceberg publish + late-event correction evidence
```

Airflow는 K1의 long-running consumer를 소유하지 않는다. 필요하면 bounded replay/backfill 또는 downstream batch publish를 조정하는 별도 질문으로 다룬다.

## 16. Public Claim Boundary

현재 설계 단계에서 허용:

```text
designed a Kafka raw-ingestion slice
documented event identity, offset, replay, and failure questions
```

runtime 검증 뒤에만 허용할 후보:

```text
built a local Kafka producer/consumer ingestion proof
landed synthetic events with topic/partition/offset evidence
verified one bounded restart/replay scenario on a local broker
```

금지:

```text
operated production Kafka
built a fault-tolerant Kafka cluster
implemented end-to-end exactly-once streaming
implemented Kafka -> Spark -> Iceberg production pipeline
processed large-scale real-time data
operated secure multi-tenant Kafka
called K1 a "streaming pipeline" (K1은 bounded raw ingestion이지 continuous streaming job이 아니다)
```

## 17. Cross-Area Audit Questions

Claude/외부 review에서 특히 볼 연결 질문:

1. `event_id x offset`: 같은 business event가 다른 offset으로 두 번 오면 어디서 합치는가?
2. `offset commit x file atomicity`: final file rename 성공 후 commit 전에 죽으면 무엇이 중복되는가?
3. `schema evolution x poison event`: 새 schema_version이 기존 consumer를 영구 정지시키는가?
4. `partition key x gold grain`: `machine_id` ordering과 `(date, plant, line, product)` 집계 grain이 어떻게 연결되는가?
5. `late event x Iceberg correction`: 이전 날짜 event가 오면 어떤 run/snapshot evidence를 남기는가?
6. `replay x idempotency`: replay가 새 raw evidence는 만들되 accepted gold는 두 배로 만들지 않는가?
7. `retention x recovery`: 필요한 replay 구간보다 Kafka retention이 짧으면 어떤 source를 믿는가?
8. `consumer group x deployment`: 새 배포가 실수로 새 group을 써서 전체 replay하지 않는가?
9. `Airflow x long-running job`: scheduler retry가 consumer instance를 중복 기동하지 않는가?
10. `local proof x public claim`: one-broker functional test를 분산/HA 경험으로 오해시키지 않는가?
11. `producer idempotence 기본값 x business dedup`: client 기본값(Java true / librdkafka false)을 명시적으로 켰는가, 그리고 transport dedup과 별개로 event_id business dedup을 어디서 하는가?
12. `client runtime x local reproducibility`: confluent-kafka는 librdkafka C wheel이라 순수 파이썬과 설치 fragility가 다르다 — Test 0가 wheel 설치+연결을 증명하는가?
13. `KRaft storage-format x 첫 실행`: 첫 기동 전 storage dir format을 빠뜨려 broker가 안 뜨는 첫 실패를 runbook에 명시했는가?

## 18. Official Reference Anchors

- [Apache Kafka Introduction](https://kafka.apache.org/documentation/)
- [Apache Kafka 4.0 Quickstart](https://kafka.apache.org/40/getting-started/quickstart/)
- [Apache Kafka Consumer Rebalance Protocol](https://kafka.apache.org/42/operations/consumer-rebalance-protocol/)
- [Apache Kafka Producer Configs](https://kafka.apache.org/40/configuration/producer-configs/)
- [librdkafka Configuration](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html)
- [Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming/index.html)
- [Spark Structured Streaming + Kafka Integration](https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html)

버전, client defaults, KRaft command, Spark connector artifact는 구현 직전 다시 공식 문서로 pin한다.
