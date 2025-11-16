from dotenv import load_dotenv
import os

load_dotenv()

class Settings:
    # Kafka Configuration
    KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
    KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "binance-trades")

    # ClickHouse Configuration
    CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    CLICKHOUSE_TCP_PORT = int(os.getenv("CLICKHOUSE_TCP_PORT", "9000"))
    CLICKHOUSE_HTTP_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
    CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
    CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
    CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "trading")
    CLICKHOUSE_BRONZE_TABLE = os.getenv("CLICKHOUSE_BRONZE_TABLE", "bronze_trades")
    CLICKHOUSE_RAW_TABLE = os.getenv("CLICKHOUSE_RAW_TABLE", "raw_trades")
    CLICKHOUSE_AGG_TABLE = os.getenv("CLICKHOUSE_AGG_TABLE", "aggregated_trades")
    CLICKHOUSE_ALERT_TABLE = os.getenv("CLICKHOUSE_ALERT_TABLE", "price_alerts")
    CLICKHOUSE_ANOM_TABLE = os.getenv("CLICKHOUSE_ANOM_TABLE", "trade_anomalies")

    # MinIO Configuration
    MINIO_HOST = os.getenv("MINIO_HOST", "localhost")
    MINIO_PORT = os.getenv("MINIO_PORT", "9000")
    MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
    MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    MINIO_BUCKET = os.getenv("MINIO_BUCKET", "flink-data")

settings = Settings()
