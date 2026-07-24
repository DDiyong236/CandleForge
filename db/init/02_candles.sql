-- Phase 2: 1분봉 연속집계 (TimescaleDB 최초 기동 시 실행. 기존 DB엔 볼륨 초기화로 적용)

-- 1) 1분봉 연속집계 정의 (OHLCV)
--    time_bucket이 체결 시각을 1분 구간으로 내림, GROUP BY로 같은 (구간,종목)끼리 묶음
CREATE MATERIALIZED VIEW IF NOT EXISTS candles_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    code,
    first(price, time) AS open,    -- 시간순 첫 가격 = 시가
    max(price)         AS high,    -- 고가
    min(price)         AS low,     -- 저가
    last(price, time)  AS close,   -- 시간순 마지막 가격 = 종가
    sum(volume)        AS volume   -- 거래량 합
FROM trades
GROUP BY bucket, code
WITH NO DATA;

-- 2) 갱신 정책: 1분마다, 최근 1시간 구간을, 1분 전까지 갱신
--    start_offset(1h) < 원본 보관(24h) 이므로 삭제된 영역을 건드리지 않음(캔들 무결성)
--    end_offset(1분): 진행 중인 미완성 봉은 제외
SELECT add_continuous_aggregate_policy('candles_1m',
    start_offset      => INTERVAL '1 hour',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute');
