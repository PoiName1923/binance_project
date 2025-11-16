from settings import settings
# =================================================================================================================
# ===== DDL Nguồn dữ liệu từ kafka =====
ddl_kafka_source = f"""
CREATE TABLE kafka_sources (
    e STRING,                    -- event type ("trade")
    E BIGINT,                    -- event time (ms)
    s STRING,                    -- symbol
    t BIGINT,                    -- trade id
    p STRING,                    -- price (string)
    q STRING,                    -- quantity (string)
    T BIGINT,                    -- trade time (ms)
    m BOOLEAN,                   -- is buyer the market maker?
    M BOOLEAN,                   -- ignore
    ts AS TO_TIMESTAMP_LTZ(E, 3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '{settings.KAFKA_TOPIC}',
    'properties.bootstrap.servers' = '{settings.KAFKA_HOST}:{settings.KAFKA_PORT}',
    'properties.group.id' = 'flink-trade',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'json'
);
"""


