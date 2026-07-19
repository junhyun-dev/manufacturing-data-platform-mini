# AGENTS.md — manufacturing-data-platform-mini

AI-assisted development notes. Human scope and review gates stay in place.

## 이 프로젝트의 한 줄
manufacturing/ML-style data platform의 핵심 루프(수집→카탈로그→버전/재현성→서빙)를 **얇게 한 번 관통**하는 public-safe demo.

## 절대 규칙
- **스코프를 키우지 말 것.** 현재 local Spark/Iceberg walking skeleton과 bounded Kafka K1/K1.5만 구현돼 있다. 승인된 Slice/package 없이 full Spark medallion, continuous streaming, multi-partition Kafka, cluster/Kubernetes, ROS2/MCAP·Jetson으로 확장하지 않는다.
- 공개 repo 전제 — 회사 내부 도구명/고객명/기밀/개인정보를 넣지 말 것.

## 코드 컨벤션
- Python 3.10+, `src/manufacturing_data_platform/` 패키지 레이아웃.
- MongoDB 접근은 `pymongo`, 테스트는 `mongomock` (실 DB 없이 CI 가능하게).
- 작은 함수 + 명확한 데이터 계약(매니페스트 스키마)을 우선. 데이터-먼저: "무슨 객체가 흐르고 뭘로 바뀌나".
- 각 컴포넌트는 README의 design decision과 일치해야 함 — 코드가 "왜 이렇게"를 배신하지 않게.

## 학습 모드
이 repo는 **MongoDB·버전관리·lakehouse 품질 모델링 학습 갭을 직접 메우는** 자리다. 데이터-먼저로 "무슨 객체가 흐르고 무엇으로 바뀌는지"를 설명 가능하게 유지한다.
