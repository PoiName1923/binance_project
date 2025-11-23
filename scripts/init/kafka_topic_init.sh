#!/usr/bin/env bash
set -euo pipefail

# Kafka topic init script
# - Dùng cho service kafka-init trong docker-compose
# - Tạo topic nếu chưa tồn tại (idempotent)

KAFKA_HOST="${KAFKA_HOST:-kafka}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-binance-trades}"

# Kafka CLI binary path differs between Kafka distros; explicit path avoids PATH issues
KAFKA_TOPICS_BIN="${KAFKA_TOPICS_BIN:-/opt/kafka/bin/kafka-topics.sh}"

echo "[kafka-init] Ensuring topic '${KAFKA_TOPIC}' exists on ${KAFKA_HOST}:${KAFKA_PORT} ..."

"${KAFKA_TOPICS_BIN}" \
  --bootstrap-server "${KAFKA_HOST}:${KAFKA_PORT}" \
  --topic "${KAFKA_TOPIC}" \
  --create --if-not-exists \
  --partitions 3 \
  --replication-factor 1

echo "[kafka-init] Done."
