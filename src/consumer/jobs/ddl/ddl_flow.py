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
# Trade Anomalies (5m and 15m) — separate statements (no repeated WITHs)
# ---------------------------------
ddl_insert_trade_anomalies_clickhouse = """
INSERT INTO sink_trade_anomalies_clickhouse
WITH agg_1m AS (
    SELECT window_start, window_end, symbol,
           MIN_BY(price, event_time) AS open_price,
           MAX(price) AS high,
           MIN(price) AS low,
           MAX_BY(price, event_time) AS close_price,
           SUM(quantity) AS volume,
           COUNT(*) AS trade_count,
           SUM(CASE WHEN is_maker THEN quantity ELSE 0 END) AS sell_volume,
           SUM(CASE WHEN NOT is_maker THEN quantity ELSE 0 END) AS buy_volume
    FROM TABLE(TUMBLE(TABLE silver_view, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
    GROUP BY window_start, window_end, symbol
),
agg_1m_feat AS (
    SELECT *,
           (close_price - open_price) / NULLIF(open_price, 0) * 100.0 AS pct_change,
           (high - low) / NULLIF(open_price, 0) * 100.0 AS range_pct,
           buy_volume / NULLIF(buy_volume + sell_volume, 0) * 100.0 AS buy_share_pct,
           sell_volume / NULLIF(buy_volume + sell_volume, 0) * 100.0 AS sell_share_pct,
           LAG(close_price) OVER (PARTITION BY symbol ORDER BY window_start) AS prev_close,
           AVG(close_price) OVER (PARTITION BY symbol ORDER BY window_start ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma_20,
           STDDEV_POP(close_price) OVER (PARTITION BY symbol ORDER BY window_start ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS std_20,
           AVG(volume) OVER (PARTITION BY symbol ORDER BY window_start ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS avg_vol_20
    FROM agg_1m
),
agg_1m_feat2 AS (
    SELECT *,
           (close_price - ma_20) / NULLIF(std_20, 0) AS z_close,
           (open_price - prev_close) / NULLIF(prev_close, 0) * 100.0 AS gap_pct
    FROM agg_1m_feat
),
agg_5m AS (
    SELECT window_start, window_end, symbol,
           MIN_BY(price, event_time) AS open_price,
           MAX(price) AS high,
           MIN(price) AS low,
           MAX_BY(price, event_time) AS close_price,
           SUM(quantity) AS volume,
           COUNT(*) AS trade_count,
           SUM(CASE WHEN is_maker THEN quantity ELSE 0 END) AS sell_volume,
           SUM(CASE WHEN NOT is_maker THEN quantity ELSE 0 END) AS buy_volume
    FROM TABLE(TUMBLE(TABLE silver_view, DESCRIPTOR(event_time), INTERVAL '5' MINUTE))
    GROUP BY window_start, window_end, symbol
),
agg_5m_feat AS (
    SELECT *,
           (close_price - open_price) / NULLIF(open_price, 0) * 100.0 AS pct_change,
           (high - low) / NULLIF(open_price, 0) * 100.0 AS range_pct,
           AVG(volume) OVER (PARTITION BY symbol ORDER BY window_start ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS avg_vol_20
    FROM agg_5m
),
whale_1m AS (
    WITH base AS (
        SELECT window_start, window_end, symbol, trade_id, trade_time_ms, price, quantity
        FROM TABLE(TUMBLE(TABLE silver_view, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
    ),
    stats AS (
        SELECT window_start, window_end, symbol,
               AVG(quantity) AS avg_q,
               STDDEV_POP(quantity) AS std_q
        FROM base
        GROUP BY window_start, window_end, symbol
    ),
    ranked AS (
        SELECT b.window_start, b.window_end, b.symbol, b.trade_id, b.trade_time_ms, b.price, b.quantity,
               ROW_NUMBER() OVER (PARTITION BY b.window_start, b.window_end, b.symbol ORDER BY b.quantity DESC) AS rn,
               s.avg_q, s.std_q
        FROM base b
        JOIN stats s ON b.window_start = s.window_start AND b.window_end = s.window_end AND b.symbol = s.symbol
    )
    SELECT window_start, window_end, symbol, trade_id, trade_time_ms, price, quantity, avg_q, std_q,
           (quantity - avg_q) / NULLIF(std_q, 0) AS z_qty
    FROM ranked
    WHERE rn = 1 AND std_q IS NOT NULL AND std_q > 0
),
intertrade_gaps AS (
    SELECT symbol,
           event_time AS window_start,
           event_time AS window_end,
           trade_id,
           event_time AS trade_time,
           price,
           quantity,
           (UNIX_TIMESTAMP(event_time) - UNIX_TIMESTAMP(LAG(event_time) OVER (PARTITION BY symbol ORDER BY event_time))) * 1000 AS gap_ms
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
    SELECT window_start, window_end, symbol, 'price_spike_up_1m' AS anomaly_type, 'price' AS category,
           CASE WHEN pct_change >= 5 THEN 'CRITICAL'
                WHEN pct_change >= 3 THEN 'HIGH'
                WHEN pct_change >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS severity,
           'UP' AS direction,
           pct_change AS metric, 3.0 AS threshold, 60 AS window_sec,
           CAST(0 AS BIGINT) AS trade_id, window_end AS trade_time, close_price AS price, 0.0 AS quantity,
           volume, trade_count, buy_volume, sell_volume, 0 AS gap_ms,
           CONCAT('Δ%=', CAST(ROUND(pct_change, 3) AS STRING)) AS details
    FROM agg_1m_feat2
    WHERE pct_change >= 2

    UNION ALL
    SELECT window_start, window_end, symbol, 'price_spike_down_1m', 'price',
           CASE WHEN pct_change <= -5 THEN 'CRITICAL'
                WHEN pct_change <= -3 THEN 'HIGH'
                WHEN pct_change <= -2 THEN 'MEDIUM' ELSE 'LOW' END,
           'DOWN',
           pct_change, -3.0, 60,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('Δ%=', CAST(ROUND(pct_change, 3) AS STRING))
    FROM agg_1m_feat2
    WHERE pct_change <= -2

    UNION ALL
    SELECT window_start, window_end, symbol, 'volatility_spike_1m', 'price',
           CASE WHEN range_pct >= 8 THEN 'CRITICAL'
                WHEN range_pct >= 5 THEN 'HIGH'
                WHEN range_pct >= 3 THEN 'MEDIUM' ELSE 'LOW' END,
           'FLAT',
           range_pct, 3.0, 60,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('Range%=', CAST(ROUND(range_pct, 3) AS STRING))
    FROM agg_1m_feat2
    WHERE range_pct >= 3

    UNION ALL
    SELECT window_start, window_end, symbol,
           CASE WHEN gap_pct >= 0 THEN 'gap_up_1m' ELSE 'gap_down_1m' END,
           'price',
           CASE WHEN ABS(gap_pct) >= 3 THEN 'HIGH' ELSE 'MEDIUM' END,
           CASE WHEN gap_pct >= 0 THEN 'UP' ELSE 'DOWN' END,
           gap_pct, 1.5, 60,
           CAST(0 AS BIGINT), window_start, open_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('Gap%=', CAST(ROUND(gap_pct, 3) AS STRING))
    FROM agg_1m_feat2
    WHERE prev_close IS NOT NULL AND ABS(gap_pct) >= 1.5

    UNION ALL
    SELECT window_start, window_end, symbol,
           CASE WHEN z_close >= 0 THEN 'mean_reversion_break_up' ELSE 'mean_reversion_break_down' END,
           'price',
           CASE WHEN ABS(z_close) >= 4 THEN 'CRITICAL'
                WHEN ABS(z_close) >= 3 THEN 'HIGH'
                ELSE 'MEDIUM' END,
           CASE WHEN z_close >= 0 THEN 'UP' ELSE 'DOWN' END,
           z_close, 3.0, 60,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('z=', CAST(ROUND(z_close, 3) AS STRING))
    FROM agg_1m_feat2
    WHERE std_20 IS NOT NULL AND std_20 > 0 AND ABS(z_close) >= 3

    UNION ALL
    SELECT window_start, window_end, symbol, 'volume_spike_1m', 'volume',
           CASE WHEN volume / NULLIF(avg_vol_20, 0) >= 8 THEN 'CRITICAL'
                WHEN volume / NULLIF(avg_vol_20, 0) >= 5 THEN 'HIGH'
                WHEN volume / NULLIF(avg_vol_20, 0) >= 3 THEN 'MEDIUM' ELSE 'LOW' END,
           'FLAT',
           volume / NULLIF(avg_vol_20, 0), 3.0, 60,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('Volume x=', CAST(ROUND(volume / NULLIF(avg_vol_20, 0), 2) AS STRING))
    FROM agg_1m_feat2
    WHERE avg_vol_20 IS NOT NULL AND avg_vol_20 > 0 AND volume / avg_vol_20 >= 3

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
    SELECT window_start, window_end, symbol, 'absorption_flat_price', 'volume',
           CASE WHEN volume / NULLIF(avg_vol_20, 0) >= 6 THEN 'HIGH' ELSE 'MEDIUM' END,
           'FLAT',
           volume / NULLIF(avg_vol_20, 0), 4.0, 60,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('High vol flat px; range%=', CAST(ROUND(range_pct, 3) AS STRING))
    FROM agg_1m_feat2
    WHERE avg_vol_20 IS NOT NULL AND avg_vol_20 > 0 AND volume / avg_vol_20 >= 4 AND range_pct <= 0.2

    UNION ALL
    SELECT window_start, window_end, symbol, 'dry_volume', 'volume',
           'HIGH',
           'FLAT',
           volume / NULLIF(avg_vol_20, 0), 0.2, 60,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('Vol x=', CAST(ROUND(volume / NULLIF(avg_vol_20, 0), 2) AS STRING))
    FROM agg_1m_feat2
    WHERE avg_vol_20 IS NOT NULL AND avg_vol_20 > 0 AND volume / avg_vol_20 <= 0.2

    UNION ALL
    SELECT window_start, window_end, symbol, 'price_spike_up_5m', 'price',
           CASE WHEN pct_change >= 6 THEN 'CRITICAL'
                WHEN pct_change >= 4 THEN 'HIGH'
                WHEN pct_change >= 2.5 THEN 'MEDIUM' ELSE 'LOW' END,
           'UP',
           pct_change, 3.0, 300,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('Δ%=', CAST(ROUND(pct_change, 3) AS STRING))
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
    SELECT window_start, window_end, symbol, 'buy_imbalance', 'order_flow',
           CASE WHEN buy_share_pct >= 95 THEN 'CRITICAL'
                WHEN buy_share_pct >= 90 THEN 'HIGH'
                ELSE 'MEDIUM' END,
           'BUY',
           buy_share_pct, 90.0, 60,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('Buy%=', CAST(ROUND(buy_share_pct, 2) AS STRING))
    FROM agg_1m_feat2
    WHERE buy_share_pct >= 90

    UNION ALL
    SELECT window_start, window_end, symbol, 'sell_imbalance', 'order_flow',
           CASE WHEN sell_share_pct >= 95 THEN 'CRITICAL'
                WHEN sell_share_pct >= 90 THEN 'HIGH'
                ELSE 'MEDIUM' END,
           'SELL',
           sell_share_pct, 90.0, 60,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('Sell%=', CAST(ROUND(sell_share_pct, 2) AS STRING))
    FROM agg_1m_feat2
    WHERE sell_share_pct >= 90

    UNION ALL
    SELECT w.window_start, w.window_end, w.symbol, 'whale_trade', 'volume',
           CASE WHEN z_qty >= 8 THEN 'CRITICAL'
                WHEN z_qty >= 6 THEN 'HIGH'
                WHEN z_qty >= 4 THEN 'MEDIUM' ELSE 'LOW' END,
           'UNKNOWN',
           z_qty, 4.0, 60,
           trade_id, TO_TIMESTAMP_LTZ(trade_time_ms, 3) AS trade_time, price, quantity,
           0.0 AS volume, 1 AS trade_count, 0.0 AS buy_volume, 0.0 AS sell_volume, 0,
           CONCAT('Whale z=', CAST(ROUND(z_qty, 3) AS STRING))
    FROM whale_1m w
    WHERE z_qty >= 4

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
WITH agg_1m AS (
    SELECT window_start, window_end, symbol,
           MIN_BY(price, event_time) AS open_price,
           MAX(price) AS high,
           MIN(price) AS low,
           MAX_BY(price, event_time) AS close_price,
           SUM(quantity) AS volume,
           COUNT(*) AS trade_count,
           SUM(CASE WHEN is_maker THEN quantity ELSE 0 END) AS sell_volume,
           SUM(CASE WHEN NOT is_maker THEN quantity ELSE 0 END) AS buy_volume
    FROM TABLE(TUMBLE(TABLE silver_view, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
    GROUP BY window_start, window_end, symbol
),
agg_1m_feat AS (
    SELECT *,
           (close_price - open_price) / NULLIF(open_price, 0) * 100.0 AS pct_change,
           (high - low) / NULLIF(open_price, 0) * 100.0 AS range_pct,
           buy_volume / NULLIF(buy_volume + sell_volume, 0) * 100.0 AS buy_share_pct,
           sell_volume / NULLIF(buy_volume + sell_volume, 0) * 100.0 AS sell_share_pct,
           LAG(close_price) OVER (PARTITION BY symbol ORDER BY window_start) AS prev_close,
           AVG(close_price) OVER (PARTITION BY symbol ORDER BY window_start ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma_20,
           STDDEV_POP(close_price) OVER (PARTITION BY symbol ORDER BY window_start ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS std_20,
           AVG(volume) OVER (PARTITION BY symbol ORDER BY window_start ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS avg_vol_20
    FROM agg_1m
),
agg_1m_feat2 AS (
    SELECT *,
           (close_price - ma_20) / NULLIF(std_20, 0) AS z_close,
           (open_price - prev_close) / NULLIF(prev_close, 0) * 100.0 AS gap_pct
    FROM agg_1m_feat
),
agg_5m AS (
    SELECT window_start, window_end, symbol,
           MIN_BY(price, event_time) AS open_price,
           MAX(price) AS high,
           MIN(price) AS low,
           MAX_BY(price, event_time) AS close_price,
           SUM(quantity) AS volume,
           COUNT(*) AS trade_count,
           SUM(CASE WHEN is_maker THEN quantity ELSE 0 END) AS sell_volume,
           SUM(CASE WHEN NOT is_maker THEN quantity ELSE 0 END) AS buy_volume
    FROM TABLE(TUMBLE(TABLE silver_view, DESCRIPTOR(event_time), INTERVAL '5' MINUTE))
    GROUP BY window_start, window_end, symbol
),
agg_5m_feat AS (
    SELECT *,
           (close_price - open_price) / NULLIF(open_price, 0) * 100.0 AS pct_change,
           (high - low) / NULLIF(open_price, 0) * 100.0 AS range_pct,
           AVG(volume) OVER (PARTITION BY symbol ORDER BY window_start ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS avg_vol_20
    FROM agg_5m
),
whale_1m AS (
    WITH base AS (
        SELECT window_start, window_end, symbol, trade_id, trade_time_ms, price, quantity
        FROM TABLE(TUMBLE(TABLE silver_view, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
    ),
    stats AS (
        SELECT window_start, window_end, symbol,
               AVG(quantity) AS avg_q,
               STDDEV_POP(quantity) AS std_q
        FROM base
        GROUP BY window_start, window_end, symbol
    ),
    ranked AS (
        SELECT b.window_start, b.window_end, b.symbol, b.trade_id, b.trade_time_ms, b.price, b.quantity,
               ROW_NUMBER() OVER (PARTITION BY b.window_start, b.window_end, b.symbol ORDER BY b.quantity DESC) AS rn,
               s.avg_q, s.std_q
        FROM base b
        JOIN stats s ON b.window_start = s.window_start AND b.window_end = s.window_end AND b.symbol = s.symbol
    )
    SELECT window_start, window_end, symbol, trade_id, trade_time_ms, price, quantity, avg_q, std_q,
           (quantity - avg_q) / NULLIF(std_q, 0) AS z_qty
    FROM ranked
    WHERE rn = 1 AND std_q IS NOT NULL AND std_q > 0
),
intertrade_gaps AS (
    SELECT symbol,
           event_time AS window_start,
           event_time AS window_end,
           trade_id,
           event_time AS trade_time,
           price,
           quantity,
           (UNIX_TIMESTAMP(event_time) - UNIX_TIMESTAMP(LAG(event_time) OVER (PARTITION BY symbol ORDER BY event_time))) * 1000 AS gap_ms
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
    SELECT window_start, window_end, symbol, 'price_spike_up_1m' AS anomaly_type, 'price' AS category,
           CASE WHEN pct_change >= 5 THEN 'CRITICAL'
                WHEN pct_change >= 3 THEN 'HIGH'
                WHEN pct_change >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS severity,
           'UP' AS direction,
           pct_change AS metric, 3.0 AS threshold, 60 AS window_sec,
           CAST(0 AS BIGINT) AS trade_id, window_end AS trade_time, close_price AS price, 0.0 AS quantity,
           volume, trade_count, buy_volume, sell_volume, 0 AS gap_ms,
           CONCAT('Δ%=', CAST(ROUND(pct_change, 3) AS STRING)) AS details,
           DATE_FORMAT(window_start, 'yyyy-MM-dd') AS dt,
           DATE_FORMAT(window_start, 'HH') AS hour_bucket
    FROM agg_1m_feat2
    WHERE pct_change >= 2

    UNION ALL
    SELECT window_start, window_end, symbol, 'price_spike_down_1m', 'price',
           CASE WHEN pct_change <= -5 THEN 'CRITICAL'
                WHEN pct_change <= -3 THEN 'HIGH'
                WHEN pct_change <= -2 THEN 'MEDIUM' ELSE 'LOW' END,
           'DOWN',
           pct_change, -3.0, 60,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('Δ%=', CAST(ROUND(pct_change, 3) AS STRING)),
           DATE_FORMAT(window_start, 'yyyy-MM-dd'),
           DATE_FORMAT(window_start, 'HH')
    FROM agg_1m_feat2
    WHERE pct_change <= -2

    UNION ALL
    SELECT window_start, window_end, symbol, 'volatility_spike_1m', 'price',
           CASE WHEN range_pct >= 8 THEN 'CRITICAL'
                WHEN range_pct >= 5 THEN 'HIGH'
                WHEN range_pct >= 3 THEN 'MEDIUM' ELSE 'LOW' END,
           'FLAT',
           range_pct, 3.0, 60,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('Range%=', CAST(ROUND(range_pct, 3) AS STRING)),
           DATE_FORMAT(window_start, 'yyyy-MM-dd'),
           DATE_FORMAT(window_start, 'HH')
    FROM agg_1m_feat2
    WHERE range_pct >= 3

    UNION ALL
    SELECT window_start, window_end, symbol,
           CASE WHEN gap_pct >= 0 THEN 'gap_up_1m' ELSE 'gap_down_1m' END,
           'price',
           CASE WHEN ABS(gap_pct) >= 3 THEN 'HIGH' ELSE 'MEDIUM' END,
           CASE WHEN gap_pct >= 0 THEN 'UP' ELSE 'DOWN' END,
           gap_pct, 1.5, 60,
           CAST(0 AS BIGINT), window_start, open_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('Gap%=', CAST(ROUND(gap_pct, 3) AS STRING)),
           DATE_FORMAT(window_start, 'yyyy-MM-dd'),
           DATE_FORMAT(window_start, 'HH')
    FROM agg_1m_feat2
    WHERE prev_close IS NOT NULL AND ABS(gap_pct) >= 1.5

    UNION ALL
    SELECT window_start, window_end, symbol,
           CASE WHEN z_close >= 0 THEN 'mean_reversion_break_up' ELSE 'mean_reversion_break_down' END,
           'price',
           CASE WHEN ABS(z_close) >= 4 THEN 'CRITICAL'
                WHEN ABS(z_close) >= 3 THEN 'HIGH'
                ELSE 'MEDIUM' END,
           CASE WHEN z_close >= 0 THEN 'UP' ELSE 'DOWN' END,
           z_close, 3.0, 60,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('z=', CAST(ROUND(z_close, 3) AS STRING)),
           DATE_FORMAT(window_start, 'yyyy-MM-dd'),
           DATE_FORMAT(window_start, 'HH')
    FROM agg_1m_feat2
    WHERE std_20 IS NOT NULL AND std_20 > 0 AND ABS(z_close) >= 3

    UNION ALL
    SELECT window_start, window_end, symbol, 'volume_spike_1m', 'volume',
           CASE WHEN volume / NULLIF(avg_vol_20, 0) >= 8 THEN 'CRITICAL'
                WHEN volume / NULLIF(avg_vol_20, 0) >= 5 THEN 'HIGH'
                WHEN volume / NULLIF(avg_vol_20, 0) >= 3 THEN 'MEDIUM' ELSE 'LOW' END,
           'FLAT',
           volume / NULLIF(avg_vol_20, 0), 3.0, 60,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('Volume x=', CAST(ROUND(volume / NULLIF(avg_vol_20, 0), 2) AS STRING)),
           DATE_FORMAT(window_start, 'yyyy-MM-dd'),
           DATE_FORMAT(window_start, 'HH')
    FROM agg_1m_feat2
    WHERE avg_vol_20 IS NOT NULL AND avg_vol_20 > 0 AND volume / avg_vol_20 >= 3

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
    SELECT window_start, window_end, symbol, 'absorption_flat_price', 'volume',
           CASE WHEN volume / NULLIF(avg_vol_20, 0) >= 6 THEN 'HIGH' ELSE 'MEDIUM' END,
           'FLAT',
           volume / NULLIF(avg_vol_20, 0), 4.0, 60,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('High vol flat px; range%=', CAST(ROUND(range_pct, 3) AS STRING)),
           DATE_FORMAT(window_start, 'yyyy-MM-dd'),
           DATE_FORMAT(window_start, 'HH')
    FROM agg_1m_feat2
    WHERE avg_vol_20 IS NOT NULL AND avg_vol_20 > 0 AND volume / avg_vol_20 >= 4 AND range_pct <= 0.2

    UNION ALL
    SELECT window_start, window_end, symbol, 'dry_volume', 'volume',
           'HIGH',
           'FLAT',
           volume / NULLIF(avg_vol_20, 0), 0.2, 60,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('Vol x=', CAST(ROUND(volume / NULLIF(avg_vol_20, 0), 2) AS STRING)),
           DATE_FORMAT(window_start, 'yyyy-MM-dd'),
           DATE_FORMAT(window_start, 'HH')
    FROM agg_1m_feat2
    WHERE avg_vol_20 IS NOT NULL AND avg_vol_20 > 0 AND volume / avg_vol_20 <= 0.2

    UNION ALL
    SELECT window_start, window_end, symbol, 'price_spike_up_5m', 'price',
           CASE WHEN pct_change >= 6 THEN 'CRITICAL'
                WHEN pct_change >= 4 THEN 'HIGH'
                WHEN pct_change >= 2.5 THEN 'MEDIUM' ELSE 'LOW' END,
           'UP',
           pct_change, 3.0, 300,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('Δ%=', CAST(ROUND(pct_change, 3) AS STRING)),
           DATE_FORMAT(window_start, 'yyyy-MM-dd'),
           DATE_FORMAT(window_start, 'HH')
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
    SELECT window_start, window_end, symbol, 'buy_imbalance', 'order_flow',
           CASE WHEN buy_share_pct >= 95 THEN 'CRITICAL'
                WHEN buy_share_pct >= 90 THEN 'HIGH'
                ELSE 'MEDIUM' END,
           'BUY',
           buy_share_pct, 90.0, 60,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('Buy%=', CAST(ROUND(buy_share_pct, 2) AS STRING)),
           DATE_FORMAT(window_start, 'yyyy-MM-dd'),
           DATE_FORMAT(window_start, 'HH')
    FROM agg_1m_feat2
    WHERE buy_share_pct >= 90

    UNION ALL
    SELECT window_start, window_end, symbol, 'sell_imbalance', 'order_flow',
           CASE WHEN sell_share_pct >= 95 THEN 'CRITICAL'
                WHEN sell_share_pct >= 90 THEN 'HIGH'
                ELSE 'MEDIUM' END,
           'SELL',
           sell_share_pct, 90.0, 60,
           CAST(0 AS BIGINT), window_end, close_price, 0.0,
           volume, trade_count, buy_volume, sell_volume, 0,
           CONCAT('Sell%=', CAST(ROUND(sell_share_pct, 2) AS STRING)),
           DATE_FORMAT(window_start, 'yyyy-MM-dd'),
           DATE_FORMAT(window_start, 'HH')
    FROM agg_1m_feat2
    WHERE sell_share_pct >= 90

    UNION ALL
    SELECT w.window_start, w.window_end, w.symbol, 'whale_trade', 'volume',
           CASE WHEN z_qty >= 8 THEN 'CRITICAL'
                WHEN z_qty >= 6 THEN 'HIGH'
                WHEN z_qty >= 4 THEN 'MEDIUM' ELSE 'LOW' END,
           'UNKNOWN',
           z_qty, 4.0, 60,
           trade_id, TO_TIMESTAMP_LTZ(trade_time_ms, 3) AS trade_time, price, quantity,
           0.0 AS volume, 1 AS trade_count, 0.0 AS buy_volume, 0.0 AS sell_volume, 0,
           CONCAT('Whale z=', CAST(ROUND(z_qty, 3) AS STRING)),
           DATE_FORMAT(window_start, 'yyyy-MM-dd'),
           DATE_FORMAT(window_start, 'HH')
    FROM whale_1m w
    WHERE z_qty >= 4

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
