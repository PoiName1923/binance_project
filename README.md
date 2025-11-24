# Hệ thống streaming giao dịch Binance (Flink + Kafka + ClickHouse + MinIO)

Pipeline thời gian thực lấy dữ liệu trade từ Binance WebSocket, đẩy vào Kafka, xử lý bằng PyFlink Table API/SQL và ghi ra ClickHouse và/hoặc MinIO. MinIO đồng thời lưu checkpoint/savepoint cho Flink.

## Thành phần & cổng mặc định
- Kafka broker + Kafka UI (8080), topic chính `KAFKA_TOPIC` (`binance-trades` mặc định)
- 2 producer Python (WebSocket Binance → Kafka) dùng `symbols_1.txt` và `symbols_2.txt`
- Flink: JobManager UI 8081, TaskManager, service `flink-submit` tự chạy job `BinanceTradePipeline`
- ClickHouse server (TCP 9002, HTTP 8123) + UI `ch-ui` tại 3000; service `clickhouse-init` tạo schema
- MinIO (S3-compatible) 9000, console 8084; service `minio-init` tạo bucket
- Grafana (3001) trống, dùng để tự dựng dashboard nếu cần

## Luồng dữ liệu
1. `producer-1` và `producer-2` mở WebSocket Binance, đọc danh sách symbol từ `src/producer/symbols_1.txt` và `src/producer/symbols_2.txt`, gửi trade JSON vào Kafka topic.
2. Flink job đọc Kafka nguồn `kafka_sources`, chuẩn hoá và tạo view `silver_view`.
3. Tuỳ `SINK_TARGET` (`clickhouse|minio|both`), job:
   - Ghi dữ liệu chuẩn hoá (silver) vào ClickHouse bảng `processed_trades` và/hoặc MinIO (`silver/hourly`).
   - Ghi dữ liệu thô (bronze) vào MinIO (`raw/hourly`).
   - Tính toán bất thường 5 phút (price spike up/down, volatility spike, volume spike so với trung bình 20 window, intertrade gap >= 60s, burst giao dịch 5s, lỗi dữ liệu) và ghi vào ClickHouse bảng `trade_anomalies` và/hoặc MinIO (`anomalies`).
4. Checkpoint/savepoint của Flink lưu trên MinIO (`s3a://<MINIO_BUCKET>/flink/...`).

## Lược đồ chính
- Kafka source `kafka_sources`: `e, E, s, t, p, q, T, m, M, ts (event_time), WATERMARK ts - 5s`.
- ClickHouse `processed_trades` (`scripts/init/init_clickhouse.sql`): `symbol, price, quantity, event_time, trade_time, is_maker, ingest_time`.
- ClickHouse `trade_anomalies`: `window_start, window_end, symbol, anomaly_type, category, severity, direction, metric, threshold, window_sec, trade_id, trade_time, price, quantity, volume, trade_count, buy_volume, sell_volume, gap_ms, details`.
- MinIO sinks (filesystem connector):
  - `raw/hourly`: dữ liệu thô + partition `dt`, `hour_bucket`.
  - `silver/hourly`: dữ liệu chuẩn hoá + partition `dt`, `hour_bucket`.
  - `anomalies`: dữ liệu bất thường + partition `dt`, `hour_bucket`.

## Chuẩn bị
1. Cài Docker + Docker Compose (RAM ~4–6 GB cho toàn bộ stack).
2. Tạo file `.env` từ `.env.example` rồi chỉnh thông số:
   - Kafka: `KAFKA_HOST`, `KAFKA_PORT`, `KAFKA_TOPIC`.
   - MinIO: `MINIO_HOST`, `MINIO_PORT`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MINIO_BUCKET`.
   - ClickHouse: `CLICKHOUSE_HOST`, `CLICKHOUSE_TCP_PORT`, `CLICKHOUSE_HTTP_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DATABASE`, tên bảng nếu đổi.
   - Flink sink: `SINK_TARGET=clickhouse|minio|both` (mặc định `clickhouse` trong `.env.example`).
   - Tuỳ chọn Grafana: `GF_SECURITY_ADMIN_USER`, `GF_SECURITY_ADMIN_PASSWORD`.

## Chạy nhanh
```bash
docker compose up -d --build
```
- `kafka-init`, `minio-init`, `clickhouse-init` chạy một lần để tạo topic/bucket/schema.
- `flink-submit` đợi TaskManager sẵn sàng rồi nộp job `BinanceTradePipeline`.
- Kiểm tra: `docker compose ps` hoặc Flink UI http://localhost:8081.

Tắt stack và xoá volume nếu cần:
```bash
docker compose down -v
```

## Quan sát & truy cập
- Kafka UI: http://localhost:8080 (thấy topic và message).
- Flink UI: http://localhost:8081 (job đang RUNNING).
- ClickHouse UI: http://localhost:3000 (ch-ui), hoặc HTTP API tại http://localhost:8123.
- MinIO: http://localhost:9000 (console: http://localhost:8084).
- Grafana: http://localhost:3001 (chưa có dashboard sẵn).

## Truy vấn ClickHouse mẫu
- Trades mới nhất:
```sql
SELECT symbol, price, quantity, event_time, trade_time, is_maker
FROM binance_trades.processed_trades
ORDER BY event_time DESC
LIMIT 100;
```
- Bất thường mức MEDIUM trở lên:
```sql
SELECT window_start, symbol, anomaly_type, severity, metric, details
FROM binance_trades.trade_anomalies
WHERE severity IN ('CRITICAL','HIGH','MEDIUM')
ORDER BY window_start DESC
LIMIT 200;
```

## Cấu trúc thư mục chính
- `docker-compose.yml`: định nghĩa toàn bộ stack và lệnh submit Flink.
- `src/producer/producer.py`: WebSocket Binance → Kafka (async, aiokafka); `symbols_1.txt` / `symbols_2.txt` chứa danh sách cặp giao dịch.
- `src/consumer/jobs/main.py`: khởi tạo nguồn/sink, tạo view và chạy statement set.
- `src/consumer/jobs/ddl/ddl_schema.py`: Kafka source DDL.
- `src/consumer/jobs/ddl/ddl_sink.py`: sink ClickHouse/MinIO và tuỳ biến theo `SINK_TARGET`.
- `src/consumer/jobs/ddl/ddl_flow.py`: logic chuẩn hoá và phát hiện bất thường 5 phút.
- `src/consumer/flink-conf.yml`: cấu hình Flink, checkpoint S3A về MinIO.
- `scripts/init/init_clickhouse.sql`: DDL tạo `processed_trades`, `trade_anomalies`.
- `scripts/init/kafka_topic_init.sh`, `scripts/init/minio_bucket_init.sh`: init topic/bucket.

## Lưu ý vận hành
- `SINK_TARGET=both` để vừa ghi ClickHouse vừa lưu file trên MinIO (hữu ích cho backup/lake).
- Các JAR connector (Kafka, ClickHouse, S3, JSON) đã được copy sẵn vào `src/consumer/jars`.
- Chỉnh danh sách symbol để giảm tải hoặc thêm cặp mới trước khi `docker compose up`.
- Flink checkpoint/savepoint nằm trong bucket MinIO, giữ lại khi restart để tránh mất trạng thái.
