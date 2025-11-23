#!/usr/bin/env bash
set -euo pipefail

# Render Flink config from template (envsubst) if present
CONF_TEMPLATE="/opt/flink/custom-conf/config.yaml.template"
CONF_TARGET="/opt/flink/conf/config.yaml"
if [[ -f "${CONF_TEMPLATE}" ]]; then
    echo "[entrypoint] Rendering Flink config from template..."
    envsubst < "${CONF_TEMPLATE}" > "${CONF_TARGET}"
fi

# 1. Nếu là TaskManager thì chạy ngay và dừng script tại đây
if [[ "${1:-}" == "taskmanager" ]]; then
    exec /docker-entrypoint.sh taskmanager
fi

# 2. Start JobManager ở chế độ background để không block script
/docker-entrypoint.sh jobmanager &
JM_PID=$!

# 3. Vòng lặp chờ tài nguyên (Thay thế cho cả wait_api và wait_tm)
# Logic: Chờ đến khi API trả về "slots-total" > 0 (tức là có TM kết nối)
echo "[entrypoint] Waiting for TaskManager resources..."
while ! curl -s http://localhost:8081/overview | grep -E '"slots-total":[1-9][0-9]*' > /dev/null; do
    sleep 2
done

# 4. Submit Job
echo "[entrypoint] Resource available. Submitting job..."
flink run -m localhost:8081 \
     -py /opt/flink/jobs/main.py \
     --pyFiles /opt/flink/jobs

# 5. Giữ process JobManager chạy
wait "$JM_PID"
