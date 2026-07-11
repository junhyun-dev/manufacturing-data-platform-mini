# Slice Map Template

상태: template / thin index only

이 템플릿은 새 build slice를 시작하거나, 이미 구현한 slice의 설계 흐름을 정리할 때 쓴다.

중요한 규칙:

```text
slice map은 source of truth가 아니다.
테스트 수, CLI 결과, runtime 상태는 VERIFICATION_LOG.md에 둔다.
긴 decision reasoning은 reference-decisions/ 또는 기존 design 문서에 둔다.
구현 세부는 code/test 파일을 링크한다.
```

## 1. Slice Thesis

```text
이 slice는 무엇을 증명하려는가?
한 문장으로 쓴다.
```

## 2. Primary Scenario

```text
누가 어떤 상황에서 막히는가?
이 slice가 끝나면 그 사람이 어떤 질문에 답할 수 있어야 하는가?
```

관련 문서:

- `scenarios/...`
- `question-bank/...`
- `reference-decisions/...`

## 3. Question Areas Pulled

관련 question-bank 영역:

- service / user workflow
- grain / identity / versioning
- source contract / schema evolution
- quality / reconciliation
- rerun / backfill / correction
- storage / table format
- Spark / distributed processing
- concurrency / atomicity / consistency
- failure / retry / recovery
- orchestration / Airflow
- observability / operator evidence
- security / governance
- performance / cost
- testing / local reproducibility
- public claim boundary

### Core Questions

| Core question | Why Core |
|---|---|
| 질문 | 답이 바뀌면 코드/테이블/파일/계약이 어떻게 바뀌는가 |

### Demo Questions

| Demo question | Why not Core |
|---|---|
| 질문 | 보여주면 좋지만 이번 contract를 바꾸지 않는 이유 |

### Backlog Questions

| Backlog question | Reason |
|---|---|
| 질문 | 이번 slice 밖으로 뺀 이유 |

### Unknowns

| Unknown | How to close |
|---|---|
| 아직 모르는 것 | 작은 실험, 공식 문서 확인, runtime gate, audit 등 |

## 4. Decisions

세부 decision 문서:

- `../reference-decisions/...`

핵심 결정만 요약:

```text
Decision 1
Decision 2
Decision 3
```

## 5. Evidence

Code / test:

- `src/...`
- `tests/...`

Verification:

- `VERIFICATION_LOG.md`

## 6. Claim Boundary

Allowed:

```text
구현과 검증 evidence가 있는 claim만 쓴다.
```

Forbidden:

```text
walking skeleton을 production처럼 말하지 않는다.
runtime 미검증을 verified처럼 말하지 않는다.
design-only를 implemented처럼 말하지 않는다.
```

## 7. Next Questions

```text
이 slice가 열어둔 다음 질문은 무엇인가?
다음 slice 후보는 무엇인가?
무엇은 아직 Backlog/Unknown으로 남기는가?
```
