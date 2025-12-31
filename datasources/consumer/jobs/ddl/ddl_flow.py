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
# Trade Anomalies (5m only) — separate statements (no repeated WITHs)
# ---------------------------------
ddl_insert_trade_anomalies_clickhouse = """
INSERT INTO sink_trade_anomalies_clickhouse
WITH base_5m AS (
    SELECT window_start, window_end, window_time, symbol, event_time, price, quantity, is_maker
    FROM TABLE(TUMBLE(TABLE silver_view, DESCRIPTOR(event_time), INTERVAL '5' MINUTE))
),
stats_5m AS (
    SELECT window_start, window_end, window_time, symbol,
           MIN(price) AS low,
           MAX(price) AS high,
           SUM(quantity) AS volume,
           COUNT(*) AS trade_count,
           SUM(CASE WHEN is_maker THEN quantity ELSE 0 END) AS sell_volume,
           SUM(CASE WHEN NOT is_maker THEN quantity ELSE 0 END) AS buy_volume
    FROM base_5m
    GROUP BY window_start, window_end, window_time, symbol
),
open_5m AS (
    SELECT window_start, window_end, symbol, window_time, price AS open_price
    FROM (
        SELECT window_start, window_end, window_time, symbol, price,
               ROW_NUMBER() OVER (PARTITION BY window_start, window_end, window_time, symbol ORDER BY window_time ASC) AS rn
        FROM base_5m
    )
    WHERE rn = 1
),
close_5m AS (
    SELECT window_start, window_end, symbol, window_time, price AS close_price
    FROM (
        SELECT window_start, window_end, window_time, symbol, price,
               ROW_NUMBER() OVER (PARTITION BY window_start, window_end, window_time, symbol ORDER BY window_time DESC) AS rn
        FROM base_5m
    )
    WHERE rn = 1
),
agg_5m AS (
    SELECT s.window_start, s.window_end, s.window_time, s.symbol,
           o.open_price, s.high, s.low, c.close_price,
           s.volume, s.trade_count, s.buy_volume, s.sell_volume
    FROM stats_5m s
    JOIN open_5m o ON s.window_start = o.window_start AND s.window_end = o.window_end AND s.symbol = o.symbol
    JOIN close_5m c ON s.window_start = c.window_start AND s.window_end = c.window_end AND s.symbol = c.symbol
),
agg_5m_feat AS (
    SELECT *,
           (close_price - open_price) / NULLIF(open_price, 0) * 100.0 AS pct_change,
           (high - low) / NULLIF(open_price, 0) * 100.0 AS range_pct,
           AVG(volume) OVER (PARTITION BY symbol ORDER BY window_time ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS avg_vol_20
    FROM agg_5m
),
intertrade_gaps AS (
    SELECT symbol,
           event_time AS window_start,
           event_time AS window_end,
           trade_id,
           event_time AS trade_time,
           price,
           quantity,
           CAST((EXTRACT(EPOCH FROM event_time) - EXTRACT(EPOCH FROM LAG(event_time) OVER (PARTITION BY symbol ORDER BY event_time))) * 1000 AS BIGINT) AS gap_ms
    FROM silver_view
),
burst_5s AS (
    SELECT window_start, window_end, symbol,
           COUNT(*) AS trade_count,
           SUM(quantity) AS volume
    FROM TABLE(TUMBLE(TABLE silver_view, DESCRIPTOR(event_time), INTERVAL '5' SECOND))
    GROUP BY window_start, window_end, symbol
),
data_quality AS (
    SELECT ts AS window_start,
           ts AS window_end,
           s AS symbol,
           t AS trade_id,
           ts AS trade_time,
           CAST(p AS DOUBLE) AS price,
           CAST(q AS DOUBLE) AS quantity
    FROM kafka_sources
    WHERE s IS NULL OR p IS NULL OR q IS NULL OR CAST(p AS DOUBLE) <= 0 OR CAST(q AS DOUBLE) <= 0
)
SELECT * FROM (
    SELECT window_start, window_end, symbol, 'price_spike_up_5m' AS anomaly_type, 'price' AS category,
           CASE WHEN pct_change >= 6 THEN 'CRITICAL'
                WHEN pct_change >= 4 THEN 'HIGH'
                WHEN pct_change >= 2.5 THEN 'MEDIUM' ELSE 'LOW' END AS severity,
           'UP' AS direction,
           pct_change AS metric, 3.0 AS threshold, 300 AS window_sec,
           CAST(0 AS BIGINT) AS trade_id, window_end AS trade_time, close_price AS price, 0.0 AS quantity,
           volume, trade_count, buy_volume, sell_volume, 0 AS gap_ms,
           CONCAT('Δ%=', CAST(ROUND(pct_change, 3) AS STRING)) AS details
    FROM agg_5m_feat
    WHERE pct_change >= 2.5

    UNION ALL
    SELECT window_start, window_end, symbol, 'price_spike_down_5m', 'price',
           CASE WHEN pct_change <= -6 THEN 'CRITICAL'
                WHEN pct_change <= -4 THEN 'HIGH'
                WHEN pct_change <= -2.5 THEN 'MEDIUM' ELSE 'LOW' END,
           'DOWN',
           pct_change, -3.0, 300,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('Δ%=', CAST(ROUND(pct_change, 3) AS STRING))
    FROM agg_5m_feat
    WHERE pct_change <= -2.5

    UNION ALL
    SELECT window_start, window_end, symbol, 'volatility_spike_5m', 'price',
           CASE WHEN range_pct >= 10 THEN 'CRITICAL'
                WHEN range_pct >= 7 THEN 'HIGH'
                WHEN range_pct >= 5 THEN 'MEDIUM' ELSE 'LOW' END,
           'FLAT',
           range_pct, 5.0, 300,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('Range%=', CAST(ROUND(range_pct, 3) AS STRING))
    FROM agg_5m_feat
    WHERE range_pct >= 5

    UNION ALL
    SELECT window_start, window_end, symbol, 'volume_spike_5m', 'volume',
           CASE WHEN volume / NULLIF(avg_vol_20, 0) >= 8 THEN 'CRITICAL'
                WHEN volume / NULLIF(avg_vol_20, 0) >= 5 THEN 'HIGH'
                WHEN volume / NULLIF(avg_vol_20, 0) >= 3 THEN 'MEDIUM' ELSE 'LOW' END,
           'FLAT',
           volume / NULLIF(avg_vol_20, 0), 3.0, 300,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('Volume x=', CAST(ROUND(volume / NULLIF(avg_vol_20, 0), 2) AS STRING))
    FROM agg_5m_feat
    WHERE avg_vol_20 IS NOT NULL AND avg_vol_20 > 0 AND volume / avg_vol_20 >= 3

    UNION ALL
    SELECT window_start, window_end, symbol, 'intertrade_gap', 'time',
           CASE WHEN gap_ms >= 300000 THEN 'HIGH' ELSE 'MEDIUM' END,
           'FLAT',
           CAST(gap_ms AS DOUBLE), 60000.0, 0,
           trade_id, trade_time, price, quantity,
           0.0 AS volume, 0 AS trade_count, 0.0 AS buy_volume, 0.0 AS sell_volume, CAST(gap_ms AS BIGINT),
           CONCAT('Gap ms=', CAST(gap_ms AS STRING))
    FROM intertrade_gaps
    WHERE gap_ms IS NOT NULL AND gap_ms >= 60000

    UNION ALL
    SELECT window_start, window_end, symbol, 'burst_of_trades', 'time',
           CASE WHEN trade_count >= 200 THEN 'CRITICAL'
                WHEN trade_count >= 100 THEN 'HIGH'
                ELSE 'MEDIUM' END,
           'FLAT',
           CAST(trade_count AS DOUBLE), 50.0, 5,
           CAST(0 AS BIGINT), window_end, 0.0, 0.0,
           volume, trade_count, 0.0, 0.0, 0,
           CONCAT('Trades/5s=', CAST(trade_count AS STRING))
    FROM burst_5s
    WHERE trade_count >= 50

    UNION ALL
    SELECT window_start, window_end, symbol, 'data_quality', 'data',
           'HIGH',
           'FLAT',
           1.0, 1.0, 0,
           trade_id, trade_time, price, quantity,
           0.0 AS volume, 0 AS trade_count, 0.0 AS buy_volume, 0.0 AS sell_volume, 0,
           'Invalid/null fields detected' AS details
    FROM data_quality
)
"""
ddl_insert_trade_anomalies_minio = """
INSERT INTO sink_trade_anomalies_minio
WITH base_5m AS (
    SELECT window_start, window_end, window_time, symbol, event_time, price, quantity, is_maker
    FROM TABLE(TUMBLE(TABLE silver_view, DESCRIPTOR(event_time), INTERVAL '5' MINUTE))
),
stats_5m AS (
    SELECT window_start, window_end, window_time, symbol,
           MIN(price) AS low,
           MAX(price) AS high,
           SUM(quantity) AS volume,
           COUNT(*) AS trade_count,
           SUM(CASE WHEN is_maker THEN quantity ELSE 0 END) AS sell_volume,
           SUM(CASE WHEN NOT is_maker THEN quantity ELSE 0 END) AS buy_volume
    FROM base_5m
    GROUP BY window_start, window_end, window_time, symbol
),
open_5m AS (
    SELECT window_start, window_end, symbol, window_time, price AS open_price
    FROM (
        SELECT window_start, window_end, window_time, symbol, price,
               ROW_NUMBER() OVER (PARTITION BY window_start, window_end, window_time, symbol ORDER BY window_time ASC) AS rn
        FROM base_5m
    )
    WHERE rn = 1
),
close_5m AS (
    SELECT window_start, window_end, symbol, window_time, price AS close_price
    FROM (
        SELECT window_start, window_end, window_time, symbol, price,
               ROW_NUMBER() OVER (PARTITION BY window_start, window_end, window_time, symbol ORDER BY window_time DESC) AS rn
        FROM base_5m
    )
    WHERE rn = 1
),
agg_5m AS (
    SELECT s.window_start, s.window_end, s.window_time, s.symbol,
           o.open_price, s.high, s.low, c.close_price,
           s.volume, s.trade_count, s.buy_volume, s.sell_volume
    FROM stats_5m s
    JOIN open_5m o ON s.window_start = o.window_start AND s.window_end = o.window_end AND s.symbol = o.symbol
    JOIN close_5m c ON s.window_start = c.window_start AND s.window_end = c.window_end AND s.symbol = c.symbol
),
agg_5m_feat AS (
    SELECT *,
           (close_price - open_price) / NULLIF(open_price, 0) * 100.0 AS pct_change,
           (high - low) / NULLIF(open_price, 0) * 100.0 AS range_pct,
           AVG(volume) OVER (PARTITION BY symbol ORDER BY window_time ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS avg_vol_20
    FROM agg_5m
),
intertrade_gaps AS (
    SELECT symbol,
           event_time AS window_start,
           event_time AS window_end,
           trade_id,
           event_time AS trade_time,
           price,
           quantity,
           CAST((EXTRACT(EPOCH FROM event_time) - EXTRACT(EPOCH FROM LAG(event_time) OVER (PARTITION BY symbol ORDER BY event_time))) * 1000 AS BIGINT) AS gap_ms
    FROM silver_view
),
burst_5s AS (
    SELECT window_start, window_end, symbol,
           COUNT(*) AS trade_count,
           SUM(quantity) AS volume
    FROM TABLE(TUMBLE(TABLE silver_view, DESCRIPTOR(event_time), INTERVAL '5' SECOND))
    GROUP BY window_start, window_end, symbol
),
data_quality AS (
    SELECT ts AS window_start,
           ts AS window_end,
           s AS symbol,
           t AS trade_id,
           ts AS trade_time,
           CAST(p AS DOUBLE) AS price,
           CAST(q AS DOUBLE) AS quantity
    FROM kafka_sources
    WHERE s IS NULL OR p IS NULL OR q IS NULL OR CAST(p AS DOUBLE) <= 0 OR CAST(q AS DOUBLE) <= 0
)
SELECT * FROM (
    SELECT window_start, window_end, symbol, 'price_spike_up_5m' AS anomaly_type, 'price' AS category,
           CASE WHEN pct_change >= 6 THEN 'CRITICAL'
                WHEN pct_change >= 4 THEN 'HIGH'
                WHEN pct_change >= 2.5 THEN 'MEDIUM' ELSE 'LOW' END AS severity,
           'UP' AS direction,
           pct_change AS metric, 3.0 AS threshold, 300 AS window_sec,
           CAST(0 AS BIGINT) AS trade_id, window_end AS trade_time, close_price AS price, 0.0 AS quantity,
           volume, trade_count, buy_volume, sell_volume, 0 AS gap_ms,
           CONCAT('Δ%=', CAST(ROUND(pct_change, 3) AS STRING)) AS details,
           DATE_FORMAT(window_start, 'yyyy-MM-dd') AS dt,
           DATE_FORMAT(window_start, 'HH') AS hour_bucket
    FROM agg_5m_feat
    WHERE pct_change >= 2.5

    UNION ALL
    SELECT window_start, window_end, symbol, 'price_spike_down_5m', 'price',
           CASE WHEN pct_change <= -6 THEN 'CRITICAL'
                WHEN pct_change <= -4 THEN 'HIGH'
                WHEN pct_change <= -2.5 THEN 'MEDIUM' ELSE 'LOW' END,
           'DOWN',
           pct_change, -3.0, 300,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('Δ%=', CAST(ROUND(pct_change, 3) AS STRING)),
           DATE_FORMAT(window_start, 'yyyy-MM-dd'),
           DATE_FORMAT(window_start, 'HH')
    FROM agg_5m_feat
    WHERE pct_change <= -2.5

    UNION ALL
    SELECT window_start, window_end, symbol, 'volatility_spike_5m', 'price',
           CASE WHEN range_pct >= 10 THEN 'CRITICAL'
                WHEN range_pct >= 7 THEN 'HIGH'
                WHEN range_pct >= 5 THEN 'MEDIUM' ELSE 'LOW' END,
           'FLAT',
           range_pct, 5.0, 300,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('Range%=', CAST(ROUND(range_pct, 3) AS STRING)),
           DATE_FORMAT(window_start, 'yyyy-MM-dd'),
           DATE_FORMAT(window_start, 'HH')
    FROM agg_5m_feat
    WHERE range_pct >= 5

    UNION ALL
    SELECT window_start, window_end, symbol, 'volume_spike_5m', 'volume',
           CASE WHEN volume / NULLIF(avg_vol_20, 0) >= 8 THEN 'CRITICAL'
                WHEN volume / NULLIF(avg_vol_20, 0) >= 5 THEN 'HIGH'
                WHEN volume / NULLIF(avg_vol_20, 0) >= 3 THEN 'MEDIUM' ELSE 'LOW' END,
           'FLAT',
           volume / NULLIF(avg_vol_20, 0), 3.0, 300,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('Volume x=', CAST(ROUND(volume / NULLIF(avg_vol_20, 0), 2) AS STRING)),
           DATE_FORMAT(window_start, 'yyyy-MM-dd'),
           DATE_FORMAT(window_start, 'HH')
    FROM agg_5m_feat
    WHERE avg_vol_20 IS NOT NULL AND avg_vol_20 > 0 AND volume / avg_vol_20 >= 3

    UNION ALL
    SELECT window_start, window_end, symbol, 'intertrade_gap', 'time',
           CASE WHEN gap_ms >= 300000 THEN 'HIGH' ELSE 'MEDIUM' END,
           'FLAT',
           CAST(gap_ms AS DOUBLE), 60000.0, 0,
           trade_id, trade_time, price, quantity,
           0.0 AS volume, 0 AS trade_count, 0.0 AS buy_volume, 0.0 AS sell_volume, CAST(gap_ms AS BIGINT),
           CONCAT('Gap ms=', CAST(gap_ms AS STRING)),
           DATE_FORMAT(window_start, 'yyyy-MM-dd'),
           DATE_FORMAT(window_start, 'HH')
    FROM intertrade_gaps
    WHERE gap_ms IS NOT NULL AND gap_ms >= 60000

    UNION ALL
    SELECT window_start, window_end, symbol, 'burst_of_trades', 'time',
           CASE WHEN trade_count >= 200 THEN 'CRITICAL'
                WHEN trade_count >= 100 THEN 'HIGH'
                ELSE 'MEDIUM' END,
           'FLAT',
           CAST(trade_count AS DOUBLE), 50.0, 5,
           CAST(0 AS BIGINT), window_end, 0.0, 0.0,
           volume, trade_count, 0.0, 0.0, 0,
           CONCAT('Trades/5s=', CAST(trade_count AS STRING)),
           DATE_FORMAT(window_start, 'yyyy-MM-dd'),
           DATE_FORMAT(window_start, 'HH')
    FROM burst_5s
    WHERE trade_count >= 50

    UNION ALL
    SELECT window_start, window_end, symbol, 'data_quality', 'data',
           'HIGH',
           'FLAT',
           1.0, 1.0, 0,
           trade_id, trade_time, price, quantity,
           0.0 AS volume, 0 AS trade_count, 0.0 AS buy_volume, 0.0 AS sell_volume, 0,
           'Invalid/null fields detected' AS details,
           DATE_FORMAT(window_start, 'yyyy-MM-dd'),
           DATE_FORMAT(window_start, 'HH')
    FROM data_quality
)
"""
