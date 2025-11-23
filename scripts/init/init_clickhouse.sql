-- Initialize ClickHouse schema for Binance ETL pipeline
-- Equivalent to init_clickhouse.py (without dynamic Python logic)
-- Adjust database/table names below if you use non-default values.

-- Database
CREATE DATABASE IF NOT EXISTS binance_trades;

-- Silver (raw trades standardized)
CREATE TABLE IF NOT EXISTS binance_trades.processed_trades (
    symbol String,
    price Float64,
    quantity Float64,
    event_time DateTime64(3),
    trade_time DateTime64(3),
    is_maker UInt8,
    ingest_time DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (symbol, trade_time)
SETTINGS index_granularity = 8192;

-- Gold (1-minute OHLCV + vwap)
CREATE TABLE IF NOT EXISTS binance_trades.aggregated_trades (
    window_start DateTime64(3),
    window_end DateTime64(3),
    symbol String,
    open_price Float64,
    high Float64,
    low Float64,
    close_price Float64,
    volume Float64,
    vwap Float64
)
ENGINE = MergeTree
PARTITION BY toDate(window_start)
ORDER BY (symbol, window_start)
SETTINGS index_granularity = 8192;

-- Price Alerts
CREATE TABLE IF NOT EXISTS binance_trades.price_alerts (
    window_start DateTime64(3),
    window_end DateTime64(3),
    symbol String,
    window_size String,
    alert_type String,
    direction String,
    pct_change Float64,
    range_pct Float64,
    open_price Float64,
    high Float64,
    low Float64,
    close_price Float64,
    volume Float64,
    severity String,
    details String
)
ENGINE = MergeTree
PARTITION BY toDate(window_start)
ORDER BY (symbol, window_start, alert_type)
SETTINGS index_granularity = 8192;

-- Trade Anomalies
CREATE TABLE IF NOT EXISTS binance_trades.trade_anomalies (
    window_start DateTime64(3),
    window_end DateTime64(3),
    symbol String,
    anomaly_type String,
    severity String,
    trade_id UInt64,
    trade_time DateTime64(3),
    price Float64,
    quantity Float64,
    z_score Float64,
    avg_metric Float64,
    stddev_metric Float64,
    window_sec UInt32,
    details String
)
ENGINE = MergeTree
PARTITION BY toDate(window_start)
ORDER BY (symbol, window_start, anomaly_type)
SETTINGS index_granularity = 8192;
