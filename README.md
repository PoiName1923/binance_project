# Hệ thống ETL thời gian thực Binance với Flink, Kafka, ClickHouse, MinIO

Dự án này xây dựng một pipeline xử lý dữ liệu thời gian thực cho sự kiện giao dịch (trade) từ Binance. Dữ liệu được đưa vào Kafka, xử lý bằng Apache Flink (PyFlink Table API/SQL) và được ghi ra ClickHouse để phân tích. MinIO được sử dụng cho checkpoint/savepoint của Flink. Hệ thống cũng bao gồm cảnh báo (price alerts) và phát hiện bất thường (trade anomalies), cùng giao diện quan sát trên ClickHouse UI.

## Kiến trúc tổng quan

- Kafka (broker + Kafka UI)
  - Chủ đề: `binance-trades`
  - Producer: WebSocket Binance → Kafka (Python bất đồng bộ)
  - Service init: `kafka-init` tự tạo topic nếu chưa có
- Flink (JobManager, TaskManager, consumer job bằng PyFlink)
  - Đọc từ Kafka, xử lý cửa sổ (window) và ghi vào ClickHouse qua JDBC
  - Checkpoint/savepoint lưu trên MinIO thông qua S3A
- ClickHouse
  - Lưu bảng silver (raw chuẩn hoá), gold (OHLCV), alerts và anomalies để truy vấn phân tích
  - Có sẵn web UI (Tabix client)
  - Service init: `clickhouse-init` tự chạy `init_clickhouse.sql` tạo DB/bảng nếu chưa có
- MinIO
  - Lưu checkpoint/savepoint của Flink
  - Service init: `minio-init` tự tạo bucket nếu chưa có

Các cổng mặc định (có thể thay đổi qua `.env`/compose):
- Kafka UI: http://localhost:8080
- Flink UI: http://localhost:8081
- ClickHouse HTTP: http://localhost:8123 (UI Tabix: http://localhost:8086)
- MinIO: http://localhost:9000 (console: http://localhost:8084)

## Luồng dữ liệu & các lớp dữ liệu

- Bronze (raw từ Kafka)
  - Dữ liệu JSON sự kiện trade từ Binance được đọc từ Kafka (bảng nguồn `kafka_sources`).
  - Có sink ClickHouse để lưu bản sao dữ liệu thô (bảng bronze).
- Silver (chuẩn hoá)
  - Chuẩn hoá và ép kiểu: `symbol`, `price` (DOUBLE), `quantity` (DOUBLE), `event_time`, `trade_time`, `is_maker`.
  - Được định nghĩa dưới dạng view tạm thời `silver_view` và ghi vào bảng ClickHouse silver.
- Gold (tổng hợp)
  - Tính OHLCV + VWAP theo cửa sổ 1 phút (và view 5m/15m để phục vụ alert/anomaly).
  - Phương pháp lấy Open/Close dùng `ROW_NUMBER` theo thứ tự thời gian trong từng cửa sổ.
  - Kết quả (1 phút) ghi vào bảng ClickHouse gold (aggregated trades).
- Alerts (cảnh báo biến động giá)
  - Cảnh báo cho 1m/5m/15m: candle move và range spike, chấm điểm severity theo ngưỡng phần trăm.
  - Ghi vào bảng ClickHouse `price_alerts`.
- Anomalies (bất thường giao dịch)
  - 5m: giao dịch khối lượng lớn, giá tăng/giảm đột biến; 15m: khối lượng lớn.
  - Dùng z-score so với trung bình/độ lệch chuẩn trong cửa sổ.
  - Ghi vào bảng ClickHouse `trade_anomalies`.

Các file chính:

- Consumer (Flink) – thư mục `src/consumer/jobs/`
  - `ddl/ddl_schema.py`: DDL nguồn Kafka
  - `ddl/ddl_sink.py`: DDL sink ClickHouse (JDBC)
  - `ddl/ddl_flow.py`: SQL chuẩn hoá (silver), tổng hợp (gold), alerts và anomalies
  - `main.py`: Điểm vào job, tạo bảng/view và submit các insert đồng thời
  - `settings.py`: Đọc cấu hình từ biến môi trường `.env`
  - `init_clickhouse.sql`: Script khởi tạo database/bảng ClickHouse (DDL thuần SQL)
- Producer (Kafka) – thư mục `src/producer/`
  - `producer.py`: WebSocket Binance → Kafka (async, aiokafka)
  - `settings.py`: Đọc cấu hình Kafka và đường dẫn file symbol
  - `symbols_1.txt`, `symbols_2.txt`: Danh sách symbol cho từng instance producer

## Lược đồ ClickHouse (chính)

- Bronze: `e, E, s, t, p, q, T, m, M, ts, ingest_time`
- Silver (raw chuẩn hoá): `symbol, price, quantity, event_time, trade_time, is_maker, ingest_time`
- Gold (OHLCV + VWAP): `window_start, window_end, symbol, open_price, high, low, close_price, volume, vwap`
- Price Alerts: `window_start, window_end, symbol, window_size, alert_type, direction, pct_change, range_pct, open_price, high, low, close_price, volume, severity, details`
- Trade Anomalies: `window_start, window_end, symbol, anomaly_type, severity, trade_id, trade_time, price, quantity, z_score, avg_metric, stddev_metric, window_sec, details`

Lưu ý: file `init_clickhouse.sql` chứa toàn bộ DDL tạo database/bảng. Nếu cần migrate schema cũ, trong file có sẵn các câu `ALTER TABLE` dạng comment bạn có thể bật lên và chạy thủ công bằng `clickhouse-client`.

## Cấu trúc dự án

```
src/
  producer/              # Producer Binance → Kafka (Python async)
    Dockerfile
    producer.py
    requirements.txt
    settings.py
    symbols_1.txt
    symbols_2.txt
  consumer/              # Flink consumer (PyFlink)
    Dockerfile           # Ảnh Flink với PyFlink + drivers/connectors
    jars/                # Các JAR connector/driver copy vào /opt/flink/lib
    jobs/
      main.py            # Orchestrate DDL + statement set
      settings.py        # Cấu hình Kafka/ClickHouse/MinIO
      init_clickhouse.sql
      ddl/
        ddl_schema.py    # Kafka source
        ddl_sink.py      # ClickHouse sinks (JDBC)
        ddl_flow.py      # Silver/Gold, Alerts, Anomalies
docker-compose.yml       # Toàn bộ stack: Kafka, Flink, MinIO, ClickHouse
data/                    # Volume dữ liệu (ClickHouse, Kafka, MinIO, ...)
logs/                    # Log ClickHouse, Kafka
.env.example             # Mẫu biến môi trường
scripts/
  init/
    kafka_topic_init.sh  # Tạo Kafka topic nếu chưa tồn tại (service kafka-init)
    minio_bucket_init.sh # Tạo MinIO bucket nếu chưa tồn tại (service minio-init)
  bootstrap_env.sh       # (Tuỳ chọn) bootstrap thủ công topic/bucket/schema
  start_with_bootstrap.sh# (Tuỳ chọn) start stack + bootstrap một lần
```

## Yêu cầu môi trường

- Docker + Docker Compose
- RAM gợi ý: ~4–6 GB cho toàn bộ stack
- Internet để kéo image và (tuỳ chọn) tải các JAR connector của Flink

## Thiết lập biến môi trường


Các biến chính cần thiết lập trong `.env` ở thư mục gốc:
- Kafka: `KAFKA_HOST`, `KAFKA_PORT`, `KAFKA_TOPIC`
- ClickHouse: `CLICKHOUSE_HOST`, `CLICKHOUSE_*_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DATABASE`, tên bảng
- MinIO: `MINIO_HOST`, `MINIO_PORT`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MINIO_BUCKET`

## Cách chạy

1) Build và khởi động toàn bộ stack:

```
docker compose up -d --build
```

2) Kiểm tra dịch vụ & dữ liệu:

- Flink UI: http://localhost:8081 (job consumer ở trạng thái RUNNING)
- Kafka UI: http://localhost:8080 (topic `binance-trades` có dữ liệu)
- ClickHouse UI: http://localhost:8086 (truy vấn các bảng silver/gold/alerts/anomalies)
- MinIO: http://localhost:9000 (để ý checkpoint/savepoint của Flink)

Các service `kafka-init`, `minio-init`, `clickhouse-init` sẽ tự chạy một lần khi stack khởi động để đảm bảo:
- Kafka đã có topic `binance-trades`.
- MinIO đã có bucket `${MINIO_BUCKET}`.
- ClickHouse đã có DB/bảng theo `init_clickhouse.sql`.

## Câu lệnh ClickHouse mẫu

OHLCV 1 phút gần nhất của một symbol:

```
SELECT *
FROM ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_AGG_TABLE}
WHERE symbol = 'btcusdt'
ORDER BY window_start DESC
LIMIT 100;
```

Cảnh báo giá mức CRITICAL/HIGH:

```
SELECT window_start, symbol, window_size, alert_type, direction, pct_change, range_pct, severity
FROM ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_ALERT_TABLE}
WHERE severity IN ('CRITICAL', 'HIGH')
ORDER BY window_start DESC
LIMIT 200;
```

Giao dịch bất thường 5m/15m:

```
SELECT window_start, symbol, anomaly_type, severity, trade_id, trade_time, price, quantity, z_score, details
FROM ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_ANOM_TABLE}
WHERE severity IN ('CRITICAL','HIGH')
ORDER BY window_start DESC
LIMIT 200;
```
