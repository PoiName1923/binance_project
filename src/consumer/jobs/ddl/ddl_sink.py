from settings import settings

# ==================== ClickHouse Sinks ====================

ddl_sink_bronze_to_clickhouse = f"""
CREATE TABLE sink_bronze_clickhouse (
    e STRING,
    E BIGINT,
    s STRING,
    t BIGINT,
    p STRING,
    q STRING,
    T BIGINT,
    m BOOLEAN,
    M BOOLEAN,
    ts TIMESTAMP(3)
) WITH (
    'connector' = 'clickhouse',
    'url' = 'jdbc:clickhouse://{settings.CLICKHOUSE_HOST}:{settings.CLICKHOUSE_HTTP_PORT}/{settings.CLICKHOUSE_DATABASE}',
    'use-local' = 'true',
    'database-name' = '{settings.CLICKHOUSE_DATABASE}',
    'table-name' = '{settings.CLICKHOUSE_BRONZE_TABLE}',
    'username' = '{settings.CLICKHOUSE_USER}',
    'password' = '{settings.CLICKHOUSE_PASSWORD}',
    'sink.ignore-delete' = 'true',
    'sink.batch-size' = '10000',
    'sink.flush-interval' = '3s',
    'sink.max-retries' = '3'
);
"""

ddl_sink_silver_to_clickhouse = f"""
CREATE TABLE sink_silver_clickhouse (
    symbol STRING,
    price DOUBLE,
    quantity DOUBLE,
    event_time TIMESTAMP(3),
    trade_time TIMESTAMP(3),
    is_maker BOOLEAN
) WITH (
    'connector' = 'clickhouse',
    'url' = 'jdbc:clickhouse://{settings.CLICKHOUSE_HOST}:{settings.CLICKHOUSE_HTTP_PORT}/{settings.CLICKHOUSE_DATABASE}',
    'use-local' = 'true',
    'database-name' = '{settings.CLICKHOUSE_DATABASE}',
    'table-name' = '{settings.CLICKHOUSE_RAW_TABLE}',
    'username' = '{settings.CLICKHOUSE_USER}',
    'password' = '{settings.CLICKHOUSE_PASSWORD}',
    'sink.ignore-delete' = 'true',
    'sink.batch-size' = '10000',
    'sink.flush-interval' = '3s',
    'sink.max-retries' = '3'
);
"""

ddl_sink_gold_to_clickhouse = f"""
CREATE TABLE sink_gold_clickhouse (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    symbol STRING,
    open_price DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close_price DOUBLE,
    volume DOUBLE,
    vwap DOUBLE
) WITH (
    'connector' = 'clickhouse',
    'url' = 'jdbc:clickhouse://{settings.CLICKHOUSE_HOST}:{settings.CLICKHOUSE_HTTP_PORT}/{settings.CLICKHOUSE_DATABASE}',
    'use-local' = 'true',
    'database-name' = '{settings.CLICKHOUSE_DATABASE}',
    'table-name' = '{settings.CLICKHOUSE_AGG_TABLE}',
    'username' = '{settings.CLICKHOUSE_USER}',
    'password' = '{settings.CLICKHOUSE_PASSWORD}',
    'sink.ignore-delete' = 'true',
    'sink.batch-size' = '5000',
    'sink.flush-interval' = '5s',
    'sink.max-retries' = '3'
);
"""

ddl_sink_price_alerts_clickhouse = f"""
CREATE TABLE sink_price_alerts_clickhouse (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    symbol STRING,
    window_size STRING,
    alert_type STRING,
    direction STRING,
    pct_change DOUBLE,
    range_pct DOUBLE,
    open_price DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close_price DOUBLE,
    volume DOUBLE,
    severity STRING,
    details STRING
) WITH (
    'connector' = 'clickhouse',
    'url' = 'jdbc:clickhouse://{settings.CLICKHOUSE_HOST}:{settings.CLICKHOUSE_HTTP_PORT}/{settings.CLICKHOUSE_DATABASE}',
    'use-local' = 'true',
    'database-name' = '{settings.CLICKHOUSE_DATABASE}',
    'table-name' = '{settings.CLICKHOUSE_ALERT_TABLE}',
    'username' = '{settings.CLICKHOUSE_USER}',
    'password' = '{settings.CLICKHOUSE_PASSWORD}',
    'sink.ignore-delete' = 'true',
    'sink.batch-size' = '3000',
    'sink.flush-interval' = '5s',
    'sink.max-retries' = '3'
);
"""

ddl_sink_trade_anomalies_clickhouse = f"""
CREATE TABLE sink_trade_anomalies_clickhouse (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    symbol STRING,
    anomaly_type STRING,
    severity STRING,
    trade_id BIGINT,
    trade_time TIMESTAMP(3),
    price DOUBLE,
    quantity DOUBLE,
    z_score DOUBLE,
    avg_metric DOUBLE,
    stddev_metric DOUBLE,
    window_sec INT,
    details STRING
) WITH (
    'connector' = 'clickhouse',
    'url' = 'jdbc:clickhouse://{settings.CLICKHOUSE_HOST}:{settings.CLICKHOUSE_HTTP_PORT}/{settings.CLICKHOUSE_DATABASE}',
    'use-local' = 'true',
    'table-name' = '{settings.CLICKHOUSE_ANOM_TABLE}',
    'database-name' = '{settings.CLICKHOUSE_DATABASE}',
    'username' = '{settings.CLICKHOUSE_USER}',
    'password' = '{settings.CLICKHOUSE_PASSWORD}',
    'sink.ignore-delete' = 'true',
    'sink.batch-size' = '3000',
    'sink.flush-interval' = '5s',
    'sink.max-retries' = '3'
);
"""
