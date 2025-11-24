-- Initialize ClickHouse schema for Binance ETL pipeline

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

-- Trade Anomalies
CREATE TABLE IF NOT EXISTS binance_trades.trade_anomalies (
    window_start DateTime64(3),
    window_end DateTime64(3),
    symbol String,
    anomaly_type String,
    category String,
    severity String,
    direction String,
    metric Float64,
    threshold Float64,
    window_sec UInt32,
    trade_id UInt64,
    trade_time DateTime64(3),
    price Float64,
    quantity Float64,
    volume Float64,
    trade_count UInt64,
    buy_volume Float64,
    sell_volume Float64,
    gap_ms UInt64,
    details String
)
ENGINE = MergeTree
PARTITION BY toDate(window_start)
ORDER BY (symbol, window_start, anomaly_type)
SETTINGS index_granularity = 8192;
