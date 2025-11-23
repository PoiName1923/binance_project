from settings import settings

# ---------------------------------
# Views / Transformations
# ---------------------------------
ddl_create_silver_view = """
CREATE TEMPORARY VIEW silver_view AS
SELECT
    s AS symbol,
    CAST(p AS DOUBLE) AS price,
    CAST(q AS DOUBLE) AS quantity,
    ts AS event_time,
    TO_TIMESTAMP_LTZ(T, 3) AS trade_time,
    T AS trade_time_ms,
    m AS is_maker,
    t AS trade_id
FROM kafka_sources
WHERE s IS NOT NULL AND p IS NOT NULL AND q IS NOT NULL;
"""

# ---------------------------------
# Gold views (1m, 5m, 15m)
# ---------------------------------
ddl_create_gold_1m_view = """
CREATE TEMPORARY VIEW gold_1m_view AS
WITH base AS (
  SELECT window_start, window_end, symbol, trade_time_ms, price, quantity
  FROM TABLE(TUMBLE(TABLE silver_view, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
),
open_row AS (
  SELECT window_start, window_end, symbol, price AS open_price
  FROM (
    SELECT window_start, window_end, symbol, price,
           ROW_NUMBER() OVER (PARTITION BY window_start, window_end, symbol ORDER BY trade_time_ms ASC) AS rn
    FROM base
  ) WHERE rn = 1
),
close_row AS (
  SELECT window_start, window_end, symbol, price AS close_price
  FROM (
    SELECT window_start, window_end, symbol, price,
           ROW_NUMBER() OVER (PARTITION BY window_start, window_end, symbol ORDER BY trade_time_ms DESC) AS rn
    FROM base
  ) WHERE rn = 1
),
agg AS (
  SELECT window_start, window_end, symbol,
         MAX(price) AS high,
         MIN(price) AS low,
         SUM(quantity) AS volume,
         SUM(price * quantity) / NULLIF(SUM(quantity), 0) AS vwap
  FROM base
  GROUP BY window_start, window_end, symbol
)
SELECT a.window_start, a.window_end, a.symbol, o.open_price, a.high, a.low, c.close_price, a.volume, a.vwap
FROM agg a
LEFT JOIN open_row o
  ON a.window_start = o.window_start AND a.window_end = o.window_end AND a.symbol = o.symbol
LEFT JOIN close_row c
  ON a.window_start = c.window_start AND a.window_end = c.window_end AND a.symbol = c.symbol;
"""

ddl_create_gold_5m_view = """
CREATE TEMPORARY VIEW gold_5m_view AS
WITH base AS (
  SELECT window_start, window_end, symbol, trade_time_ms, price, quantity
  FROM TABLE(TUMBLE(TABLE silver_view, DESCRIPTOR(event_time), INTERVAL '5' MINUTE))
),
open_row AS (
  SELECT window_start, window_end, symbol, price AS open_price
  FROM (
    SELECT window_start, window_end, symbol, price,
           ROW_NUMBER() OVER (PARTITION BY window_start, window_end, symbol ORDER BY trade_time_ms ASC) AS rn
    FROM base
  ) WHERE rn = 1
),
close_row AS (
  SELECT window_start, window_end, symbol, price AS close_price
  FROM (
    SELECT window_start, window_end, symbol, price,
           ROW_NUMBER() OVER (PARTITION BY window_start, window_end, symbol ORDER BY trade_time_ms DESC) AS rn
    FROM base
  ) WHERE rn = 1
),
agg AS (
  SELECT window_start, window_end, symbol,
         MAX(price) AS high,
         MIN(price) AS low,
         SUM(quantity) AS volume,
         SUM(price * quantity) / NULLIF(SUM(quantity), 0) AS vwap
  FROM base
  GROUP BY window_start, window_end, symbol
)
SELECT a.window_start, a.window_end, a.symbol, o.open_price, a.high, a.low, c.close_price, a.volume, a.vwap
FROM agg a
LEFT JOIN open_row o
  ON a.window_start = o.window_start AND a.window_end = o.window_end AND a.symbol = o.symbol
LEFT JOIN close_row c
  ON a.window_start = c.window_start AND a.window_end = c.window_end AND a.symbol = c.symbol;
"""

ddl_create_gold_15m_view = """
CREATE TEMPORARY VIEW gold_15m_view AS
WITH base AS (
  SELECT window_start, window_end, symbol, trade_time_ms, price, quantity
  FROM TABLE(TUMBLE(TABLE silver_view, DESCRIPTOR(event_time), INTERVAL '15' MINUTE))
),
open_row AS (
  SELECT window_start, window_end, symbol, price AS open_price
  FROM (
    SELECT window_start, window_end, symbol, price,
           ROW_NUMBER() OVER (PARTITION BY window_start, window_end, symbol ORDER BY trade_time_ms ASC) AS rn
    FROM base
  ) WHERE rn = 1
),
close_row AS (
  SELECT window_start, window_end, symbol, price AS close_price
  FROM (
    SELECT window_start, window_end, symbol, price,
           ROW_NUMBER() OVER (PARTITION BY window_start, window_end, symbol ORDER BY trade_time_ms DESC) AS rn
    FROM base
  ) WHERE rn = 1
),
agg AS (
  SELECT window_start, window_end, symbol,
         MAX(price) AS high,
         MIN(price) AS low,
         SUM(quantity) AS volume,
         SUM(price * quantity) / NULLIF(SUM(quantity), 0) AS vwap
  FROM base
  GROUP BY window_start, window_end, symbol
)
SELECT a.window_start, a.window_end, a.symbol, o.open_price, a.high, a.low, c.close_price, a.volume, a.vwap
FROM agg a
LEFT JOIN open_row o
  ON a.window_start = o.window_start AND a.window_end = o.window_end AND a.symbol = o.symbol
LEFT JOIN close_row c
  ON a.window_start = c.window_start AND a.window_end = c.window_end AND a.symbol = c.symbol;
"""

ddl_insert_bronze_to_minio = """
INSERT INTO sink_bronze_minio
SELECT
    e, E, s, t, p, q, T, m, M, ts,
    DATE_FORMAT(ts, 'yyyy-MM-dd') AS dt,
    DATE_FORMAT(ts, 'HH') AS hour_bucket
FROM kafka_sources;
"""

# ---------------------------------
# Inserts (Silver -> ClickHouse)
# ---------------------------------
ddl_insert_silver_to_clickhouse = """
INSERT INTO sink_silver_clickhouse
SELECT symbol, price, quantity, event_time, trade_time, is_maker
FROM silver_view;
"""

ddl_insert_silver_to_minio = """
INSERT INTO sink_silver_minio
SELECT
    symbol, price, quantity, event_time, trade_time, is_maker,
    DATE_FORMAT(event_time, 'yyyy-MM-dd') AS dt,
    DATE_FORMAT(event_time, 'HH') AS hour_bucket
FROM silver_view;
"""

# ---------------------------------
# Inserts (Gold -> ClickHouse)
# ---------------------------------
ddl_insert_gold_to_clickhouse = """
INSERT INTO sink_gold_clickhouse
SELECT window_start, window_end, symbol, open_price, high, low, close_price, volume, vwap
FROM gold_1m_view;
"""

ddl_insert_gold_to_minio = """
INSERT INTO sink_gold_minio
SELECT
    window_start, window_end, symbol, open_price, high, low, close_price, volume, vwap,
    DATE_FORMAT(window_start, 'yyyy-MM-dd') AS dt,
    DATE_FORMAT(window_start, 'HH') AS hour_bucket
FROM gold_1m_view;
"""

# ---------------------------------
# Alerts: Price Movement (1m/5m/15m)
# ---------------------------------
ddl_insert_price_alerts_clickhouse = """
INSERT INTO sink_price_alerts_clickhouse
-- 1 minute, candle move
SELECT window_start, window_end, symbol, '1m' AS window_size,
       'candle_move' AS alert_type,
       CASE WHEN close_price > open_price THEN 'UP' ELSE 'DOWN' END AS direction,
       (close_price - open_price) / NULLIF(open_price, 0) * 100.0 AS pct_change,
       (high - low) / NULLIF(open_price, 0) * 100.0 AS range_pct,
       open_price, high, low, close_price, volume,
       CASE WHEN ABS((close_price - open_price)/NULLIF(open_price,0)*100.0) >= 5 THEN 'CRITICAL'
            WHEN ABS((close_price - open_price)/NULLIF(open_price,0)*100.0) >= 3 THEN 'HIGH'
            WHEN ABS((close_price - open_price)/NULLIF(open_price,0)*100.0) >= 1 THEN 'MEDIUM'
            ELSE 'LOW' END AS severity,
       CONCAT('Strong 1m move ', CAST(ROUND((close_price-open_price)/NULLIF(open_price,0)*100.0, 3) AS STRING), '%') AS details
FROM gold_1m_view
WHERE ABS((close_price - open_price) / NULLIF(open_price, 0) * 100.0) >= 0.7

UNION ALL
-- 1 minute, range spike
SELECT window_start, window_end, symbol, '1m' AS window_size,
       'range_spike' AS alert_type,
       CASE WHEN close_price > open_price THEN 'UP' ELSE 'DOWN' END AS direction,
       (close_price - open_price) / NULLIF(open_price, 0) * 100.0 AS pct_change,
       (high - low) / NULLIF(open_price, 0) * 100.0 AS range_pct,
       open_price, high, low, close_price, volume,
       CASE WHEN (high - low)/NULLIF(open_price,0)*100.0 >= 7 THEN 'CRITICAL'
            WHEN (high - low)/NULLIF(open_price,0)*100.0 >= 4 THEN 'HIGH'
            WHEN (high - low)/NULLIF(open_price,0)*100.0 >= 2 THEN 'MEDIUM'
            ELSE 'LOW' END AS severity,
       CONCAT('Range spike 1m ', CAST(ROUND((high-low)/NULLIF(open_price,0)*100.0, 3) AS STRING), '%') AS details
FROM gold_1m_view
WHERE (high - low) / NULLIF(open_price, 0) * 100.0 >= 1.0

UNION ALL
-- 5 minute, candle move
SELECT window_start, window_end, symbol, '5m' AS window_size,
       'candle_move' AS alert_type,
       CASE WHEN close_price > open_price THEN 'UP' ELSE 'DOWN' END AS direction,
       (close_price - open_price) / NULLIF(open_price, 0) * 100.0 AS pct_change,
       (high - low) / NULLIF(open_price, 0) * 100.0 AS range_pct,
       open_price, high, low, close_price, volume,
       CASE WHEN ABS((close_price - open_price)/NULLIF(open_price,0)*100.0) >= 8 THEN 'CRITICAL'
            WHEN ABS((close_price - open_price)/NULLIF(open_price,0)*100.0) >= 5 THEN 'HIGH'
            WHEN ABS((close_price - open_price)/NULLIF(open_price,0)*100.0) >= 2 THEN 'MEDIUM'
            ELSE 'LOW' END AS severity,
       CONCAT('Strong 5m move ', CAST(ROUND((close_price-open_price)/NULLIF(open_price,0)*100.0, 3) AS STRING), '%') AS details
FROM gold_5m_view
WHERE ABS((close_price - open_price) / NULLIF(open_price, 0) * 100.0) >= 1.5

UNION ALL
-- 5 minute, range spike
SELECT window_start, window_end, symbol, '5m' AS window_size,
       'range_spike' AS alert_type,
       CASE WHEN close_price > open_price THEN 'UP' ELSE 'DOWN' END AS direction,
       (close_price - open_price) / NULLIF(open_price, 0) * 100.0 AS pct_change,
       (high - low) / NULLIF(open_price, 0) * 100.0 AS range_pct,
       open_price, high, low, close_price, volume,
       CASE WHEN (high - low)/NULLIF(open_price,0)*100.0 >= 10 THEN 'CRITICAL'
            WHEN (high - low)/NULLIF(open_price,0)*100.0 >= 6 THEN 'HIGH'
            WHEN (high - low)/NULLIF(open_price,0)*100.0 >= 3 THEN 'MEDIUM'
            ELSE 'LOW' END AS severity,
       CONCAT('Range spike 5m ', CAST(ROUND((high-low)/NULLIF(open_price,0)*100.0, 3) AS STRING), '%') AS details
FROM gold_5m_view
WHERE (high - low) / NULLIF(open_price, 0) * 100.0 >= 2.0

UNION ALL
-- 15 minute, candle move
SELECT window_start, window_end, symbol, '15m' AS window_size,
       'candle_move' AS alert_type,
       CASE WHEN close_price > open_price THEN 'UP' ELSE 'DOWN' END AS direction,
       (close_price - open_price) / NULLIF(open_price, 0) * 100.0 AS pct_change,
       (high - low) / NULLIF(open_price, 0) * 100.0 AS range_pct,
       open_price, high, low, close_price, volume,
       CASE WHEN ABS((close_price - open_price)/NULLIF(open_price,0)*100.0) >= 12 THEN 'CRITICAL'
            WHEN ABS((close_price - open_price)/NULLIF(open_price,0)*100.0) >= 7 THEN 'HIGH'
            WHEN ABS((close_price - open_price)/NULLIF(open_price,0)*100.0) >= 3 THEN 'MEDIUM'
            ELSE 'LOW' END AS severity,
       CONCAT('Strong 15m move ', CAST(ROUND((close_price-open_price)/NULLIF(open_price,0)*100.0, 3) AS STRING), '%') AS details
FROM gold_15m_view
WHERE ABS((close_price - open_price) / NULLIF(open_price, 0) * 100.0) >= 3.0

UNION ALL
-- 15 minute, range spike
SELECT window_start, window_end, symbol, '15m' AS window_size,
       'range_spike' AS alert_type,
       CASE WHEN close_price > open_price THEN 'UP' ELSE 'DOWN' END AS direction,
       (close_price - open_price) / NULLIF(open_price, 0) * 100.0 AS pct_change,
       (high - low) / NULLIF(open_price, 0) * 100.0 AS range_pct,
       open_price, high, low, close_price, volume,
       CASE WHEN (high - low)/NULLIF(open_price,0)*100.0 >= 15 THEN 'CRITICAL'
            WHEN (high - low)/NULLIF(open_price,0)*100.0 >= 9 THEN 'HIGH'
            WHEN (high - low)/NULLIF(open_price,0)*100.0 >= 4 THEN 'MEDIUM'
            ELSE 'LOW' END AS severity,
       CONCAT('Range spike 15m ', CAST(ROUND((high-low)/NULLIF(open_price,0)*100.0, 3) AS STRING), '%') AS details
FROM gold_15m_view
WHERE (high - low) / NULLIF(open_price, 0) * 100.0 >= 4.0
"""

ddl_insert_price_alerts_minio = """
INSERT INTO sink_price_alerts_minio
SELECT window_start, window_end, symbol, window_size, alert_type, direction,
       pct_change, range_pct, open_price, high, low, close_price, volume,
       severity, details,
       DATE_FORMAT(window_start, 'yyyy-MM-dd') AS dt,
       DATE_FORMAT(window_start, 'HH') AS hour_bucket
FROM (
    -- 1 minute, candle move
    SELECT window_start, window_end, symbol, '1m' AS window_size,
           'candle_move' AS alert_type,
           CASE WHEN close_price > open_price THEN 'UP' ELSE 'DOWN' END AS direction,
           (close_price - open_price) / NULLIF(open_price, 0) * 100.0 AS pct_change,
           (high - low) / NULLIF(open_price, 0) * 100.0 AS range_pct,
           open_price, high, low, close_price, volume,
           CASE WHEN ABS((close_price - open_price)/NULLIF(open_price,0)*100.0) >= 5 THEN 'CRITICAL'
                WHEN ABS((close_price - open_price)/NULLIF(open_price,0)*100.0) >= 3 THEN 'HIGH'
                WHEN ABS((close_price - open_price)/NULLIF(open_price,0)*100.0) >= 1 THEN 'MEDIUM'
                ELSE 'LOW' END AS severity,
           CONCAT('Strong 1m move ', CAST(ROUND((close_price-open_price)/NULLIF(open_price,0)*100.0, 3) AS STRING), '%') AS details
    FROM gold_1m_view
    WHERE ABS((close_price - open_price) / NULLIF(open_price, 0) * 100.0) >= 0.7

    UNION ALL
    -- 1 minute, range spike
    SELECT window_start, window_end, symbol, '1m' AS window_size,
           'range_spike' AS alert_type,
           CASE WHEN close_price > open_price THEN 'UP' ELSE 'DOWN' END AS direction,
           (close_price - open_price) / NULLIF(open_price, 0) * 100.0 AS pct_change,
           (high - low) / NULLIF(open_price, 0) * 100.0 AS range_pct,
           open_price, high, low, close_price, volume,
           CASE WHEN (high - low)/NULLIF(open_price,0)*100.0 >= 7 THEN 'CRITICAL'
                WHEN (high - low)/NULLIF(open_price,0)*100.0 >= 4 THEN 'HIGH'
                WHEN (high - low)/NULLIF(open_price,0)*100.0 >= 2 THEN 'MEDIUM'
                ELSE 'LOW' END AS severity,
           CONCAT('Range spike 1m ', CAST(ROUND((high-low)/NULLIF(open_price,0)*100.0, 3) AS STRING), '%') AS details
    FROM gold_1m_view
    WHERE (high - low) / NULLIF(open_price, 0) * 100.0 >= 1.0

    UNION ALL
    -- 5 minute, candle move
    SELECT window_start, window_end, symbol, '5m' AS window_size,
           'candle_move' AS alert_type,
           CASE WHEN close_price > open_price THEN 'UP' ELSE 'DOWN' END AS direction,
           (close_price - open_price) / NULLIF(open_price, 0) * 100.0 AS pct_change,
           (high - low) / NULLIF(open_price, 0) * 100.0 AS range_pct,
           open_price, high, low, close_price, volume,
           CASE WHEN ABS((close_price - open_price)/NULLIF(open_price,0)*100.0) >= 8 THEN 'CRITICAL'
                WHEN ABS((close_price - open_price)/NULLIF(open_price,0)*100.0) >= 5 THEN 'HIGH'
                WHEN ABS((close_price - open_price)/NULLIF(open_price,0)*100.0) >= 2 THEN 'MEDIUM'
                ELSE 'LOW' END AS severity,
           CONCAT('Strong 5m move ', CAST(ROUND((close_price-open_price)/NULLIF(open_price,0)*100.0, 3) AS STRING), '%') AS details
    FROM gold_5m_view
    WHERE ABS((close_price - open_price) / NULLIF(open_price, 0) * 100.0) >= 1.5

    UNION ALL
    -- 5 minute, range spike
    SELECT window_start, window_end, symbol, '5m' AS window_size,
           'range_spike' AS alert_type,
           CASE WHEN close_price > open_price THEN 'UP' ELSE 'DOWN' END AS direction,
           (close_price - open_price) / NULLIF(open_price, 0) * 100.0 AS pct_change,
           (high - low) / NULLIF(open_price, 0) * 100.0 AS range_pct,
           open_price, high, low, close_price, volume,
           CASE WHEN (high - low)/NULLIF(open_price,0)*100.0 >= 10 THEN 'CRITICAL'
                WHEN (high - low)/NULLIF(open_price,0)*100.0 >= 6 THEN 'HIGH'
                WHEN (high - low)/NULLIF(open_price,0)*100.0 >= 3 THEN 'MEDIUM'
                ELSE 'LOW' END AS severity,
           CONCAT('Range spike 5m ', CAST(ROUND((high-low)/NULLIF(open_price,0)*100.0, 3) AS STRING), '%') AS details
    FROM gold_5m_view
    WHERE (high - low) / NULLIF(open_price, 0) * 100.0 >= 2.0

    UNION ALL
    -- 15 minute, candle move
    SELECT window_start, window_end, symbol, '15m' AS window_size,
           'candle_move' AS alert_type,
           CASE WHEN close_price > open_price THEN 'UP' ELSE 'DOWN' END AS direction,
           (close_price - open_price) / NULLIF(open_price, 0) * 100.0 AS pct_change,
           (high - low) / NULLIF(open_price, 0) * 100.0 AS range_pct,
           open_price, high, low, close_price, volume,
           CASE WHEN ABS((close_price - open_price)/NULLIF(open_price,0)*100.0) >= 12 THEN 'CRITICAL'
                WHEN ABS((close_price - open_price)/NULLIF(open_price,0)*100.0) >= 7 THEN 'HIGH'
                WHEN ABS((close_price - open_price)/NULLIF(open_price,0)*100.0) >= 3 THEN 'MEDIUM'
                ELSE 'LOW' END AS severity,
           CONCAT('Strong 15m move ', CAST(ROUND((close_price-open_price)/NULLIF(open_price,0)*100.0, 3) AS STRING), '%') AS details
    FROM gold_15m_view
    WHERE ABS((close_price - open_price) / NULLIF(open_price, 0) * 100.0) >= 3.0

    UNION ALL
    -- 15 minute, range spike
    SELECT window_start, window_end, symbol, '15m' AS window_size,
           'range_spike' AS alert_type,
           CASE WHEN close_price > open_price THEN 'UP' ELSE 'DOWN' END AS direction,
           (close_price - open_price) / NULLIF(open_price, 0) * 100.0 AS pct_change,
           (high - low) / NULLIF(open_price, 0) * 100.0 AS range_pct,
           open_price, high, low, close_price, volume,
           CASE WHEN (high - low)/NULLIF(open_price,0)*100.0 >= 15 THEN 'CRITICAL'
                WHEN (high - low)/NULLIF(open_price,0)*100.0 >= 9 THEN 'HIGH'
                WHEN (high - low)/NULLIF(open_price,0)*100.0 >= 4 THEN 'MEDIUM'
                ELSE 'LOW' END AS severity,
           CONCAT('Range spike 15m ', CAST(ROUND((high-low)/NULLIF(open_price,0)*100.0, 3) AS STRING), '%') AS details
    FROM gold_15m_view
    WHERE (high - low) / NULLIF(open_price, 0) * 100.0 >= 4.0
)
"""

# ---------------------------------
# Trade Anomalies (5m and 15m) — separate statements (no repeated WITHs)
# ---------------------------------
ddl_insert_trade_anom_qty_5m_clickhouse = """
INSERT INTO sink_trade_anomalies_clickhouse
WITH base AS (
  SELECT window_start, window_end, symbol, trade_id, trade_time_ms, price, quantity
  FROM TABLE(TUMBLE(TABLE silver_view, DESCRIPTOR(event_time), INTERVAL '5' MINUTE))
),
agg AS (
  SELECT window_start, window_end, symbol,
         AVG(quantity) AS avg_q,
         STDDEV_POP(quantity) AS std_q
  FROM base
  GROUP BY window_start, window_end, symbol
),
top AS (
  SELECT window_start, window_end, symbol, trade_id,
         TO_TIMESTAMP_LTZ(trade_time_ms, 3) AS trade_time,
         price, quantity,
         ROW_NUMBER() OVER (PARTITION BY window_start, window_end, symbol ORDER BY quantity DESC) AS rn
  FROM base
)
SELECT b.window_start, b.window_end, b.symbol,
       'large_trade_qty' AS anomaly_type,
       CASE WHEN ((t.quantity - a.avg_q) / NULLIF(a.std_q, 0)) >= 6 THEN 'CRITICAL'
            WHEN ((t.quantity - a.avg_q) / NULLIF(a.std_q, 0)) >= 4 THEN 'HIGH'
            WHEN ((t.quantity - a.avg_q) / NULLIF(a.std_q, 0)) >= 3 THEN 'MEDIUM'
            ELSE 'LOW' END AS severity,
       t.trade_id, t.trade_time, t.price, t.quantity,
       ((t.quantity - a.avg_q) / NULLIF(a.std_q, 0)) AS z_score,
       a.avg_q AS avg_metric,
       a.std_q AS stddev_metric,
       300 AS window_sec,
       CONCAT('Top qty trade z-score=', CAST(ROUND(((t.quantity - a.avg_q)/NULLIF(a.std_q,0)), 3) AS STRING)) AS details
FROM top t
JOIN agg a ON t.window_start = a.window_start AND t.window_end = a.window_end AND t.symbol = a.symbol
JOIN base b ON t.window_start = b.window_start AND t.window_end = b.window_end AND t.symbol = b.symbol
WHERE t.rn = 1 AND a.std_q IS NOT NULL AND a.std_q > 0 AND ((t.quantity - a.avg_q) / NULLIF(a.std_q, 0)) >= 3
"""

ddl_insert_trade_anom_price_up_5m_clickhouse = """
INSERT INTO sink_trade_anomalies_clickhouse
WITH base AS (
  SELECT window_start, window_end, symbol, trade_id, trade_time_ms, price, quantity
  FROM TABLE(TUMBLE(TABLE silver_view, DESCRIPTOR(event_time), INTERVAL '5' MINUTE))
),
agg AS (
  SELECT window_start, window_end, symbol,
         AVG(price) AS avg_price,
         STDDEV_POP(price) AS std_price
  FROM base
  GROUP BY window_start, window_end, symbol
),
top AS (
  SELECT window_start, window_end, symbol, trade_id,
         TO_TIMESTAMP_LTZ(trade_time_ms, 3) AS trade_time,
         price, quantity,
         ROW_NUMBER() OVER (PARTITION BY window_start, window_end, symbol ORDER BY price DESC) AS rn
  FROM base
)
SELECT b.window_start, b.window_end, b.symbol,
       'price_spike_up' AS anomaly_type,
       CASE WHEN ((t.price - a.avg_price) / NULLIF(a.std_price, 0)) >= 8 THEN 'CRITICAL'
            WHEN ((t.price - a.avg_price) / NULLIF(a.std_price, 0)) >= 5 THEN 'HIGH'
            WHEN ((t.price - a.avg_price) / NULLIF(a.std_price, 0)) >= 3 THEN 'MEDIUM'
            ELSE 'LOW' END AS severity,
       t.trade_id, t.trade_time, t.price, t.quantity,
       ((t.price - a.avg_price) / NULLIF(a.std_price, 0)) AS z_score,
       a.avg_price AS avg_metric,
       a.std_price AS stddev_metric,
       300 AS window_sec,
       CONCAT('High price z-score=', CAST(ROUND(((t.price - a.avg_price)/NULLIF(a.std_price,0)), 3) AS STRING)) AS details
FROM top t
JOIN agg a ON t.window_start = a.window_start AND t.window_end = a.window_end AND t.symbol = a.symbol
JOIN base b ON t.window_start = b.window_start AND t.window_end = b.window_end AND t.symbol = b.symbol
WHERE t.rn = 1 AND a.std_price IS NOT NULL AND a.std_price > 0 AND ((t.price - a.avg_price) / NULLIF(a.std_price, 0)) >= 3
"""

ddl_insert_trade_anom_price_down_5m_clickhouse = """
INSERT INTO sink_trade_anomalies_clickhouse
WITH base AS (
  SELECT window_start, window_end, symbol, trade_id, trade_time_ms, price, quantity
  FROM TABLE(TUMBLE(TABLE silver_view, DESCRIPTOR(event_time), INTERVAL '5' MINUTE))
),
agg AS (
  SELECT window_start, window_end, symbol,
         AVG(price) AS avg_price,
         STDDEV_POP(price) AS std_price
  FROM base
  GROUP BY window_start, window_end, symbol
),
low AS (
  SELECT window_start, window_end, symbol, trade_id,
         TO_TIMESTAMP_LTZ(trade_time_ms, 3) AS trade_time,
         price, quantity,
         ROW_NUMBER() OVER (PARTITION BY window_start, window_end, symbol ORDER BY price ASC) AS rn
  FROM base
)
SELECT b.window_start, b.window_end, b.symbol,
       'price_spike_down' AS anomaly_type,
       CASE WHEN ((a.avg_price - l.price) / NULLIF(a.std_price, 0)) >= 8 THEN 'CRITICAL'
            WHEN ((a.avg_price - l.price) / NULLIF(a.std_price, 0)) >= 5 THEN 'HIGH'
            WHEN ((a.avg_price - l.price) / NULLIF(a.std_price, 0)) >= 3 THEN 'MEDIUM'
            ELSE 'LOW' END AS severity,
       l.trade_id, l.trade_time, l.price, l.quantity,
       ((a.avg_price - l.price) / NULLIF(a.std_price, 0)) AS z_score,
       a.avg_price AS avg_metric,
       a.std_price AS stddev_metric,
       300 AS window_sec,
       CONCAT('Low price z-score=', CAST(ROUND(((a.avg_price - l.price)/NULLIF(a.std_price,0)), 3) AS STRING)) AS details
FROM low l
JOIN agg a ON l.window_start = a.window_start AND l.window_end = a.window_end AND l.symbol = a.symbol
JOIN base b ON l.window_start = b.window_start AND l.window_end = b.window_end AND l.symbol = b.symbol
WHERE l.rn = 1 AND a.std_price IS NOT NULL AND a.std_price > 0 AND ((a.avg_price - l.price) / NULLIF(a.std_price, 0)) >= 3
"""

ddl_insert_trade_anom_qty_15m_clickhouse = """
INSERT INTO sink_trade_anomalies_clickhouse
WITH base AS (
  SELECT window_start, window_end, symbol, trade_id, trade_time_ms, price, quantity
  FROM TABLE(TUMBLE(TABLE silver_view, DESCRIPTOR(event_time), INTERVAL '15' MINUTE))
),
agg AS (
  SELECT window_start, window_end, symbol,
         AVG(quantity) AS avg_q,
         STDDEV_POP(quantity) AS std_q
  FROM base
  GROUP BY window_start, window_end, symbol
),
top AS (
  SELECT window_start, window_end, symbol, trade_id,
         TO_TIMESTAMP_LTZ(trade_time_ms, 3) AS trade_time,
         price, quantity,
         ROW_NUMBER() OVER (PARTITION BY window_start, window_end, symbol ORDER BY quantity DESC) AS rn
  FROM base
)
SELECT b.window_start, b.window_end, b.symbol,
       'large_trade_qty' AS anomaly_type,
       CASE WHEN ((t.quantity - a.avg_q) / NULLIF(a.std_q, 0)) >= 6 THEN 'CRITICAL'
            WHEN ((t.quantity - a.avg_q) / NULLIF(a.std_q, 0)) >= 4 THEN 'HIGH'
            WHEN ((t.quantity - a.avg_q) / NULLIF(a.std_q, 0)) >= 3 THEN 'MEDIUM'
            ELSE 'LOW' END AS severity,
       t.trade_id, t.trade_time, t.price, t.quantity,
       ((t.quantity - a.avg_q) / NULLIF(a.std_q, 0)) AS z_score,
       a.avg_q AS avg_metric,
       a.std_q AS stddev_metric,
       900 AS window_sec,
       CONCAT('Top qty trade z-score=', CAST(ROUND(((t.quantity - a.avg_q)/NULLIF(a.std_q,0)), 3) AS STRING)) AS details
FROM top t
JOIN agg a ON t.window_start = a.window_start AND t.window_end = a.window_end AND t.symbol = a.symbol
JOIN base b ON t.window_start = b.window_start AND t.window_end = b.window_end AND t.symbol = b.symbol
WHERE t.rn = 1 AND a.std_q IS NOT NULL AND a.std_q > 0 AND ((t.quantity - a.avg_q) / NULLIF(a.std_q, 0)) >= 3
"""

ddl_insert_trade_anom_qty_5m_minio = """
INSERT INTO sink_trade_anomalies_minio
WITH base AS (
  SELECT window_start, window_end, symbol, trade_id, trade_time_ms, price, quantity
  FROM TABLE(TUMBLE(TABLE silver_view, DESCRIPTOR(event_time), INTERVAL '5' MINUTE))
),
agg AS (
  SELECT window_start, window_end, symbol,
         AVG(quantity) AS avg_q,
         STDDEV_POP(quantity) AS std_q
  FROM base
  GROUP BY window_start, window_end, symbol
),
top AS (
  SELECT window_start, window_end, symbol, trade_id,
         TO_TIMESTAMP_LTZ(trade_time_ms, 3) AS trade_time,
         price, quantity,
         ROW_NUMBER() OVER (PARTITION BY window_start, window_end, symbol ORDER BY quantity DESC) AS rn
  FROM base
)
SELECT b.window_start, b.window_end, b.symbol,
       'large_trade_qty' AS anomaly_type,
       CASE WHEN ((t.quantity - a.avg_q) / NULLIF(a.std_q, 0)) >= 6 THEN 'CRITICAL'
            WHEN ((t.quantity - a.avg_q) / NULLIF(a.std_q, 0)) >= 4 THEN 'HIGH'
            WHEN ((t.quantity - a.avg_q) / NULLIF(a.std_q, 0)) >= 3 THEN 'MEDIUM'
            ELSE 'LOW' END AS severity,
       t.trade_id, t.trade_time, t.price, t.quantity,
       ((t.quantity - a.avg_q) / NULLIF(a.std_q, 0)) AS z_score,
       a.avg_q AS avg_metric,
       a.std_q AS stddev_metric,
       300 AS window_sec,
       CONCAT('Top qty trade z-score=', CAST(ROUND(((t.quantity - a.avg_q)/NULLIF(a.std_q,0)), 3) AS STRING)) AS details,
       DATE_FORMAT(b.window_start, 'yyyy-MM-dd') AS dt,
       DATE_FORMAT(b.window_start, 'HH') AS hour_bucket
FROM top t
JOIN agg a ON t.window_start = a.window_start AND t.window_end = a.window_end AND t.symbol = a.symbol
JOIN base b ON t.window_start = b.window_start AND t.window_end = b.window_end AND t.symbol = b.symbol
WHERE t.rn = 1 AND a.std_q IS NOT NULL AND a.std_q > 0 AND ((t.quantity - a.avg_q) / NULLIF(a.std_q, 0)) >= 3
"""

ddl_insert_trade_anom_price_up_5m_minio = """
INSERT INTO sink_trade_anomalies_minio
WITH base AS (
  SELECT window_start, window_end, symbol, trade_id, trade_time_ms, price, quantity
  FROM TABLE(TUMBLE(TABLE silver_view, DESCRIPTOR(event_time), INTERVAL '5' MINUTE))
),
agg AS (
  SELECT window_start, window_end, symbol,
         AVG(price) AS avg_price,
         STDDEV_POP(price) AS std_price
  FROM base
  GROUP BY window_start, window_end, symbol
),
top AS (
  SELECT window_start, window_end, symbol, trade_id,
         TO_TIMESTAMP_LTZ(trade_time_ms, 3) AS trade_time,
         price, quantity,
         ROW_NUMBER() OVER (PARTITION BY window_start, window_end, symbol ORDER BY price DESC) AS rn
  FROM base
)
SELECT b.window_start, b.window_end, b.symbol,
       'price_spike_up' AS anomaly_type,
       CASE WHEN ((t.price - a.avg_price) / NULLIF(a.std_price, 0)) >= 8 THEN 'CRITICAL'
            WHEN ((t.price - a.avg_price) / NULLIF(a.std_price, 0)) >= 5 THEN 'HIGH'
            WHEN ((t.price - a.avg_price) / NULLIF(a.std_price, 0)) >= 3 THEN 'MEDIUM'
            ELSE 'LOW' END AS severity,
       t.trade_id, t.trade_time, t.price, t.quantity,
       ((t.price - a.avg_price) / NULLIF(a.std_price, 0)) AS z_score,
       a.avg_price AS avg_metric,
       a.std_price AS stddev_metric,
       300 AS window_sec,
       CONCAT('High price z-score=', CAST(ROUND(((t.price - a.avg_price)/NULLIF(a.std_price,0)), 3) AS STRING)) AS details,
       DATE_FORMAT(b.window_start, 'yyyy-MM-dd') AS dt,
       DATE_FORMAT(b.window_start, 'HH') AS hour_bucket
FROM top t
JOIN agg a ON t.window_start = a.window_start AND t.window_end = a.window_end AND t.symbol = a.symbol
JOIN base b ON t.window_start = b.window_start AND t.window_end = b.window_end AND t.symbol = b.symbol
WHERE t.rn = 1 AND a.std_price IS NOT NULL AND a.std_price > 0 AND ((t.price - a.avg_price) / NULLIF(a.std_price, 0)) >= 3
"""

ddl_insert_trade_anom_price_down_5m_minio = """
INSERT INTO sink_trade_anomalies_minio
WITH base AS (
  SELECT window_start, window_end, symbol, trade_id, trade_time_ms, price, quantity
  FROM TABLE(TUMBLE(TABLE silver_view, DESCRIPTOR(event_time), INTERVAL '5' MINUTE))
),
agg AS (
  SELECT window_start, window_end, symbol,
         AVG(price) AS avg_price,
         STDDEV_POP(price) AS std_price
  FROM base
  GROUP BY window_start, window_end, symbol
),
low AS (
  SELECT window_start, window_end, symbol, trade_id,
         TO_TIMESTAMP_LTZ(trade_time_ms, 3) AS trade_time,
         price, quantity,
         ROW_NUMBER() OVER (PARTITION BY window_start, window_end, symbol ORDER BY price ASC) AS rn
  FROM base
)
SELECT b.window_start, b.window_end, b.symbol,
       'price_spike_down' AS anomaly_type,
       CASE WHEN ((a.avg_price - l.price) / NULLIF(a.std_price, 0)) >= 8 THEN 'CRITICAL'
            WHEN ((a.avg_price - l.price) / NULLIF(a.std_price, 0)) >= 5 THEN 'HIGH'
            WHEN ((a.avg_price - l.price) / NULLIF(a.std_price, 0)) >= 3 THEN 'MEDIUM'
            ELSE 'LOW' END AS severity,
       l.trade_id, l.trade_time, l.price, l.quantity,
       ((a.avg_price - l.price) / NULLIF(a.std_price, 0)) AS z_score,
       a.avg_price AS avg_metric,
       a.std_price AS stddev_metric,
       300 AS window_sec,
       CONCAT('Low price z-score=', CAST(ROUND(((a.avg_price - l.price)/NULLIF(a.std_price,0)), 3) AS STRING)) AS details,
       DATE_FORMAT(b.window_start, 'yyyy-MM-dd') AS dt,
       DATE_FORMAT(b.window_start, 'HH') AS hour_bucket
FROM low l
JOIN agg a ON l.window_start = a.window_start AND l.window_end = a.window_end AND l.symbol = a.symbol
JOIN base b ON l.window_start = b.window_start AND l.window_end = b.window_end AND l.symbol = b.symbol
WHERE l.rn = 1 AND a.std_price IS NOT NULL AND a.std_price > 0 AND ((a.avg_price - l.price) / NULLIF(a.std_price, 0)) >= 3
"""

ddl_insert_trade_anom_qty_15m_minio = """
INSERT INTO sink_trade_anomalies_minio
WITH base AS (
  SELECT window_start, window_end, symbol, trade_id, trade_time_ms, price, quantity
  FROM TABLE(TUMBLE(TABLE silver_view, DESCRIPTOR(event_time), INTERVAL '15' MINUTE))
),
agg AS (
  SELECT window_start, window_end, symbol,
         AVG(quantity) AS avg_q,
         STDDEV_POP(quantity) AS std_q
  FROM base
  GROUP BY window_start, window_end, symbol
),
top AS (
  SELECT window_start, window_end, symbol, trade_id,
         TO_TIMESTAMP_LTZ(trade_time_ms, 3) AS trade_time,
         price, quantity,
         ROW_NUMBER() OVER (PARTITION BY window_start, window_end, symbol ORDER BY quantity DESC) AS rn
  FROM base
)
SELECT b.window_start, b.window_end, b.symbol,
       'large_trade_qty' AS anomaly_type,
       CASE WHEN ((t.quantity - a.avg_q) / NULLIF(a.std_q, 0)) >= 6 THEN 'CRITICAL'
            WHEN ((t.quantity - a.avg_q) / NULLIF(a.std_q, 0)) >= 4 THEN 'HIGH'
            WHEN ((t.quantity - a.avg_q) / NULLIF(a.std_q, 0)) >= 3 THEN 'MEDIUM'
            ELSE 'LOW' END AS severity,
       t.trade_id, t.trade_time, t.price, t.quantity,
       ((t.quantity - a.avg_q) / NULLIF(a.std_q, 0)) AS z_score,
       a.avg_q AS avg_metric,
       a.std_q AS stddev_metric,
       900 AS window_sec,
       CONCAT('Top qty trade z-score=', CAST(ROUND(((t.quantity - a.avg_q)/NULLIF(a.std_q,0)), 3) AS STRING)) AS details,
       DATE_FORMAT(b.window_start, 'yyyy-MM-dd') AS dt,
       DATE_FORMAT(b.window_start, 'HH') AS hour_bucket
FROM top t
JOIN agg a ON t.window_start = a.window_start AND t.window_end = a.window_end AND t.symbol = a.symbol
JOIN base b ON t.window_start = b.window_start AND t.window_end = b.window_end AND t.symbol = b.symbol
WHERE t.rn = 1 AND a.std_q IS NOT NULL AND a.std_q > 0 AND ((t.quantity - a.avg_q) / NULLIF(a.std_q, 0)) >= 3
"""
