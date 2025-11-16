from pyflink.table import EnvironmentSettings, TableEnvironment

from settings import settings
from ddl.ddl_schema import ddl_kafka_source
from ddl.ddl_sink import (
    ddl_sink_bronze_to_clickhouse,
    ddl_sink_silver_to_clickhouse,
    ddl_sink_gold_to_clickhouse,
    ddl_sink_price_alerts_clickhouse,
    ddl_sink_trade_anomalies_clickhouse,
)
from ddl.ddl_flow import (
    ddl_create_silver_view,
    ddl_create_gold_1m_view,
    ddl_create_gold_5m_view,
    ddl_create_gold_15m_view,
    ddl_insert_bronze_to_clickhouse,
    ddl_insert_silver_to_clickhouse,
    ddl_insert_gold_to_clickhouse,
    ddl_insert_price_alerts_clickhouse,
    ddl_insert_trade_anom_qty_5m_clickhouse,
    ddl_insert_trade_anom_price_up_5m_clickhouse,
    ddl_insert_trade_anom_price_down_5m_clickhouse,
    ddl_insert_trade_anom_qty_15m_clickhouse,
)

import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("flink-pipeline")


def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # Source
    logger.info("Creating Kafka source table...")
    t_env.execute_sql(ddl_kafka_source)

    # Sinks
    logger.info("Creating ClickHouse sink tables...")
    t_env.execute_sql(ddl_sink_bronze_to_clickhouse)
    t_env.execute_sql(ddl_sink_silver_to_clickhouse)
    t_env.execute_sql(ddl_sink_gold_to_clickhouse)
    t_env.execute_sql(ddl_sink_price_alerts_clickhouse)
    t_env.execute_sql(ddl_sink_trade_anomalies_clickhouse)

    # Transformations / Views
    logger.info("Creating silver view (standardize + cast + filter)...")
    t_env.execute_sql(ddl_create_silver_view)
    t_env.execute_sql(ddl_create_gold_1m_view)
    t_env.execute_sql(ddl_create_gold_5m_view)
    t_env.execute_sql(ddl_create_gold_15m_view)

    # Statement set to run all inserts concurrently
    logger.info("Submitting inserts to run concurrently (ClickHouse sinks)...")
    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql(ddl_insert_bronze_to_clickhouse)
    stmt_set.add_insert_sql(ddl_insert_silver_to_clickhouse)
    stmt_set.add_insert_sql(ddl_insert_gold_to_clickhouse)
    stmt_set.add_insert_sql(ddl_insert_price_alerts_clickhouse)
    stmt_set.add_insert_sql(ddl_insert_trade_anom_qty_5m_clickhouse)
    stmt_set.add_insert_sql(ddl_insert_trade_anom_price_up_5m_clickhouse)
    stmt_set.add_insert_sql(ddl_insert_trade_anom_price_down_5m_clickhouse)
    stmt_set.add_insert_sql(ddl_insert_trade_anom_qty_15m_clickhouse)

    result = stmt_set.execute()
    logger.info("Flink job started. Waiting for completion (streaming job will run continuously)...")
    result.wait()


if __name__ == "__main__":
    main()
