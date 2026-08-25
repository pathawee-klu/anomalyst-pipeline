-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- 1. Create Raw Stock Ticks Table
CREATE TABLE IF NOT EXISTS stock_ticks (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    price NUMERIC(12, 4) NOT NULL,
    volume NUMERIC(16, 4) NOT NULL
);

-- Convert stock_ticks to TimescaleDB Hypertable partitioned by time
SELECT create_hypertable('stock_ticks', 'timestamp', if_not_exists => TRUE);

-- Create composite index for efficient queries by symbol and time
CREATE INDEX IF NOT EXISTS idx_stock_ticks_symbol_time ON stock_ticks (symbol, timestamp DESC);

-- 2. Create Stock Anomalies Table
CREATE TABLE IF NOT EXISTS stock_anomalies (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    current_price NUMERIC(12, 4) NOT NULL,
    previous_price NUMERIC(12, 4) NOT NULL,
    percent_change NUMERIC(8, 2) NOT NULL,
    anomaly_type VARCHAR(10) NOT NULL, -- 'SPIKE' or 'DROP'
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stock_anomalies_symbol_time ON stock_anomalies (symbol, timestamp DESC);

-- 3. Create Continuous Aggregate View for 1-minute Candlesticks (OHLCV)
CREATE MATERIALIZED VIEW stock_candlesticks_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', timestamp) AS bucket,
    symbol,
    FIRST(price, timestamp) AS open_price,
    MAX(price) AS high_price,
    MIN(price) AS low_price,
    LAST(price, timestamp) AS close_price,
    SUM(volume) AS total_volume,
    COUNT(*) AS tick_count
FROM stock_ticks
GROUP BY bucket, symbol
WITH NO DATA;

-- 4. Add Continuous Aggregate Refresh Policy (refreshes every 10 seconds)
SELECT add_continuous_aggregate_policy('stock_candlesticks_1m',
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '1 second',
    schedule_interval => INTERVAL '10 seconds',
    if_not_exists => TRUE);
