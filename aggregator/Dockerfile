from python:3.11-slim as builder

workdir /build

run apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

copy requirements.txt .
run pip install --no-cache-dir --user -r requirements.txt

from python:3.11-slim

workdir /app

run apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

run useradd -m -u 1000 appuser && \
    mkdir -p /var/log/aggregator && \
    chown -R appuser:appuser /var/log/aggregator

copy --from=builder /root/.local /home/appuser/.local

copy --chown=appuser:appuser . .

user appuser

env path=/home/appuser/.local/bin:$path

expose 8080

healthcheck --interval=30s --timeout=10s --start-period=40s --retries=3 \
    cmd curl -f http://localhost:8080/health || exit 1

cmd ["sh", "-c", "python consumer.py & uvicorn app:app --host 0.0.0.0 --port 8080"]
