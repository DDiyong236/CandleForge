-- CandleForge DB 크기 측정 (before/after 기록용)
-- 실행: docker exec candleforge-timescaledb psql -U candleforge -d candleforge -f - < db/measure.sql

-- 1) 원본/캔들 총 크기 (인덱스 포함)
SELECT 'trades'     AS name, pg_size_pretty(hypertable_size('trades'))     AS size
UNION ALL
SELECT 'candles_1m' AS name, pg_size_pretty(hypertable_size('candles_1m')) AS size;

-- 2) 행 수 (다운샘플링 효과: 원본 대비 캔들이 얼마나 적은가)
SELECT 'trades' AS name, count(*) AS rows FROM trades
UNION ALL
SELECT 'candles_1m' AS name, count(*) AS rows FROM candles_1m;

-- 3) 캔들 압축 통계 (7일 지난 청크가 압축된 뒤에야 값이 나옴)
SELECT * FROM hypertable_compression_stats('candles_1m');
