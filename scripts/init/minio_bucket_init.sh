#!/usr/bin/env sh
set -e

# MinIO bucket init script
# - Dùng cho service minio-init trong docker-compose
# - Tạo bucket nếu chưa tồn tại (idempotent)

MINIO_HOST="${MINIO_HOST:-minio}"
MINIO_PORT="${MINIO_PORT:-9000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_BUCKET="${MINIO_BUCKET:-binance-trades}"

echo "[minio-init] Ensuring bucket '${MINIO_BUCKET}' exists on ${MINIO_HOST}:${MINIO_PORT} ..."

mc alias set minio "http://${MINIO_HOST}:${MINIO_PORT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"
mc mb --ignore-existing "minio/${MINIO_BUCKET}"

echo "[minio-init] Done."

