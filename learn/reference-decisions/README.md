# Reference Decision Notes

이 폴더는 `robot-data-platform-mini`를 공부하면서 상용 서비스/OSS의 의사결정을 작은 local contract로 바꾸는 학습 노트다.

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

## Notes

1. [`schema-drift.md`](schema-drift.md)
   - CSV schema가 바뀌었을 때 왜 무시하지 않고 `schema_drift` check로 남기는가?
   - 왜 v0에서는 added column을 `warn`으로 두고, required column missing은 hard failure로 보는가?
2. [`iceberg-write-semantics.md`](iceberg-write-semantics.md) (Slice2)
   - 같은 business_date 재처리를 append/overwrite/merge 중 무엇으로 다루는가?
   - Slice1의 skip을 어떻게 partition atomic overwrite로 확장하는가?
   - `run_id`(파이프라인 실행)와 `snapshot_id`(table commit)는 왜 대체가 아니라 참조 관계인가?

