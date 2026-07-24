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

# 2) 앱 실행 (전체 KRW 마켓 ~270종목 실시간 수집 시작)
./gradlew bootRun

# 3) 수집 확인 — 로그에 30초마다 [측정] received/stored 증가
# 4) 조회
curl "http://localhost:8080/api/v1/trades?code=KRW-BTC&limit=5"
# 5) 캔들(1분봉) 확인
docker exec candleforge-timescaledb psql -U candleforge -d candleforge \
  -c "SELECT code, bucket, open, high, low, close, volume FROM candles_1m ORDER BY bucket DESC LIMIT 5;"
# 6) DB 크기 측정
docker exec -i candleforge-timescaledb psql -U candleforge -d candleforge < db/measure.sql
```

테스트: `docker compose up -d` 후 `./gradlew test` (저장소 테스트가 로컬 DB 사용).

## 데이터 길들이기 (Phase 2)

전체 KRW 마켓(~270종목) 실시간 수집 → 배치 저장 → 1분봉 자동 집계 → 원본 24h 삭제 + 캔들 압축으로 저장 비용을 통제한다.

**적용한 수명주기 정책 (TimescaleDB, 자동 job):**
- 연속집계 `candles_1m` — 1분마다 갱신(`start_offset` 1h < 보관 24h로 캔들 무결성 보장)
- 보관정책 — 원본 `trades` 24시간 뒤 청크 단위 삭제 (청크 간격 1일로 정렬)
- 압축정책 — 7일 지나 굳은 캔들 청크 무손실 압축

**측정 (스냅샷, ~270종목 · 약 2분 수집):**

| 테이블 | 크기 | 행 수 |
|---|---|---|
| `trades` (원본) | 448 kB | 1,685 |
| `candles_1m` (캔들) | 104 kB | 194 |

- 다운샘플링: 원본 행이 1분봉으로 압축됨(활발한 구간일수록 수백 체결 → 캔들 1개로 비율 증가)
- 보관정책으로 원본은 24h rolling(무한증가 차단), 압축 효과는 7일 경과 후 실측 예정
- 저장 최적화: 단건 INSERT → 배치 INSERT(`JdbcTemplate.batchUpdate`) + 0.5초 버퍼 flush

**진행 상황:** Phase 2(데이터 길들이기) 완료 — 다중 종목 · 배치 저장 · 연속집계 · 보관/압축 · 측정.
