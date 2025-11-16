FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install system dependencies if needed (libssl, etc. for Kafka client)
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application code and symbol files
COPY producer.py settings.py ./
COPY symbols_1.txt symbols_2.txt ./

# Default command (envs + SYMBOL_FILE_DIR sẽ được set từ docker-compose)
CMD ["python", "-u", "producer.py"]
