-- CandleForge 기본 스키마 (TimescaleDB 최초 기동 시 1회 자동 실행)
-- 이 파일은 docker-entrypoint-initdb.d 로 마운트되어, 빈 DB일 때만 실행된다.

-- 1) TimescaleDB 확장 활성화 (하이퍼테이블/연속집계/보관정책/압축 기능 제공)
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 2) 원본 체결 테이블
--    Upbit 체결(trade) 스트림의 핵심 필드만 담는다. 원본은 짧게 보관 후 삭제(Phase 2 보관정책).
CREATE TABLE IF NOT EXISTS trades (
    time          TIMESTAMPTZ NOT NULL,   -- 체결 시각 (Upbit trade_timestamp)
    code          TEXT        NOT NULL,   -- 종목 코드 (예: KRW-BTC)
    price         NUMERIC     NOT NULL,   -- 체결 가격
    volume        NUMERIC     NOT NULL,   -- 체결 수량
    ask_bid       TEXT        NOT NULL,   -- 매수/매도 (ASK/BID)
    sequential_id BIGINT      NOT NULL    -- Upbit 체결 고유 id (종목별 유일) — 중복방지용
);

-- 3) 하이퍼테이블로 전환 (time 기준 자동 시간분할 → 대용량 시계열에 최적화)
--    청크 간격 1일: 원본 보관정책(24h)과 맞춰 "지난 청크 통째 삭제"가 깔끔하게 돌게 함
SELECT create_hypertable('trades', by_range('time', INTERVAL '1 day'), if_not_exists => TRUE);

-- 4) 멱등성(중복 저장 방지) 인덱스
--    같은 (종목, 시각, 체결id)는 한 번만. 재연결/중복 수신 시 ON CONFLICT DO NOTHING으로 흡수 가능.
--    (하이퍼테이블의 UNIQUE 인덱스는 분할 컬럼 time을 반드시 포함해야 함)
CREATE UNIQUE INDEX IF NOT EXISTS uq_trades_dedup
    ON trades (code, time, sequential_id);

-- 5) 조회용 인덱스 (종목별 최신순 조회가 흔함)
CREATE INDEX IF NOT EXISTS ix_trades_code_time
    ON trades (code, time DESC);
