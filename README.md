# CandleForge

실시간으로 쏟아지는 체결 데이터를 감당 가능한 크기로 "길들여" 저장하고, 그 위에 지표 조회와 조건 알림 서비스를 올린 백엔드 파이프라인.

> "주식 서비스를 만든다"가 아니라 **"대용량 스트리밍 파이프라인 문제를 푼다"**로 정의한 프로젝트. 시세 데이터는 그 문제를 담는 그릇이다.

## 다루는 백엔드 역량

- 실시간 스트리밍 수집 (재연결 · 멱등성)
- 시계열 저장 최적화 (다운샘플링 · 보관정책 · 압축)
- 조회 성능 튜닝 (인덱싱 · 사전집계 · 캐싱)
- 이벤트 기반 비동기 처리 (조건 매칭 · 알림 · 재시도)

## 기술 스택

| 영역 | 기술 |
|---|---|
| 프레임워크 | Spring Boot 3.x · Kotlin (JVM 21) · Gradle |
| 데이터 소스 | Upbit WebSocket |
| 저장소 | TimescaleDB (PostgreSQL 확장) |
| 메시지 큐 | Kafka (Phase 4~) |
| 캐시 | Redis |

## 로드맵

- **Phase 0** — 세팅 · GitHub 연결
- **Phase 1** — Walking Skeleton (1종목 수집→저장→조회)
- **Phase 2** — 데이터 길들이기 (다중 종목 · 다운샘플링 · 보관정책 · 압축)
- **Phase 3** — 지표 API (캔들 조회 · 파생지표 · 캐싱)
- **Phase 4** — 알림 엔진 + Kafka (fan-out · 조건 매칭 · 비동기 발송)
- **Phase 5** — 마무리 (부하 테스트 · 모니터링 · README 정리)

설계 상세: [docs/superpowers/specs/2026-07-14-candleforge-design.md](docs/superpowers/specs/2026-07-14-candleforge-design.md)

## 로컬 실행

전제: Docker Desktop 실행 중, JDK 21.

```bash
# 1) 인프라 기동 (TimescaleDB + Redis)
docker compose up -d

# 2) 앱 실행 (KRW-BTC 실시간 수집 시작)
./gradlew bootRun

# 3) 수집 확인 — 로그에 30초마다 [측정] received/stored 증가
# 4) 조회
curl "http://localhost:8080/api/v1/trades?code=KRW-BTC&limit=5"
# 5) 측정 지표
curl "http://localhost:8080/actuator/metrics/candleforge.trades.stored"
```

테스트: `docker compose up -d` 후 `./gradlew test` (저장소 테스트가 로컬 DB 사용).

**진행 상황:** Phase 1(Walking Skeleton) 완료 — 1종목 수집→저장→조회 관통 + 측정.
