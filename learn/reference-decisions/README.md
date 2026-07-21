# Reference Decision Notes

이 폴더는 `manufacturing-data-platform-mini`를 공부하면서 상용 서비스/OSS의 의사결정을 작은 local contract로 바꾸는 학습 노트다.

읽는 방식:

```text
시나리오
-> 문제
-> 선택지
-> 상용/OSS의 의사결정
-> tradeoff
-> state/metadata
-> local contract
-> test
-> 구현
```

## Status Field

각 decision note는 ADR처럼 상단에 status를 둔다.

```text
ADR Status: Proposed | Accepted | Implemented | Superseded
```

원칙:

```text
Proposed    = 선택지를 검토 중이다.
Accepted    = 이 프로젝트의 현재 설계 결정으로 채택했다.
Implemented = code/test/verification evidence가 있다.
Superseded  = 더 최신 decision으로 대체됐다.
```

status는 decision의 lifecycle만 말한다. 최신 테스트 수와 실행 결과는 `VERIFICATION_LOG.md`가 source of truth다.

## Notes

1. [`schema-drift.md`](schema-drift.md)
   - CSV schema가 바뀌었을 때 왜 무시하지 않고 `schema_drift` check로 남기는가?
   - 왜 v0에서는 added column을 `warn`으로 두고, required column missing은 hard failure로 보는가?
2. [`gold-grain.md`](gold-grain.md)
   - manufacturing gold mart와 EAV gold mart의 한 row가 무엇을 의미하는가?
   - 왜 grain을 blog/resume claim의 일부로 고정해야 하는가?
3. [`iceberg-write-semantics.md`](iceberg-write-semantics.md) (Slice2)
   - 같은 business_date 재처리를 append/overwrite/merge 중 무엇으로 다루는가?
   - Slice1의 skip을 어떻게 partition atomic overwrite로 확장하는가?
   - `run_id`(파이프라인 실행)와 `snapshot_id`(table commit)는 왜 대체가 아니라 참조 관계인가?
4. [`failure-state-model.md`](failure-state-model.md)
   - 성공 run evidence만으로 답하지 못하는 실패/partial-state 질문은 무엇인가?
   - quality_failed, failed_before_commit, committed_unpublished 같은 상태를 어떻게 구분하는가?
   - 왜 production WAP/rollback/incident workflow는 Backlog로 두는가?
5. [`kafka-event-identity-and-key.md`](kafka-event-identity-and-key.md) (K1)
   - business `event_id`와 Kafka transport coordinate를 왜 분리하는가?
   - 왜 K1의 message key는 `machine_id`이고 partition은 하나인가?
6. [`kafka-offset-and-landing-commit.md`](kafka-offset-and-landing-commit.md) (K1)
   - durable landing과 offset commit의 순서를 왜 분리하는가?
   - landing 뒤 commit 전 실패를 어떻게 재전달·reuse로 복구하는가?
7. [`kafka-landing-to-batch-adapter.md`](kafka-landing-to-batch-adapter.md) (K1.5)
   - accepted Kafka landing을 한 business_date의 결정적 batch input으로 어떻게 바꾸는가?
   - 왜 source identity가 Kafka provenance를 포함한 canonical CSV의 SHA-256인가?
   - 왜 invalid/tampered input은 lakehouse pipeline 호출 전에 실패해야 하는가?
8. [`spark-engine-swap-contract.md`](spark-engine-swap-contract.md) (S7)
   - 기존 Python silver/gold를 Spark로 옮길 때 코드보다 먼저 무엇을 고정했는가? (grain, dedup, round, quality)
   - 왜 dedup을 Kafka coordinate 순서로 맞추고, round를 `format_number`(Python round parity)로 하는가?
   - 왜 Spark quality를 새로 짜지 않고 기존 suite를 Spark 결과에 적용하는가?
9. [`edge-buffer-and-recovery-progress.md`](edge-buffer-and-recovery-progress.md) (S8)
   - 단절 구간을 로컬에 모을 때 무엇을 durable progress로 볼 것인가? (immutable 파일 vs mutable cursor)
   - "아직 안 옴"과 "유실"을 어떻게 구분하는가? (expected_last_sequence 봉인)
   - 왜 완결성을 Kafka offset 연속성으로 판정하면 안 되는가?
