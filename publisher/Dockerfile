from python:3.11-slim

workdir /app

copy requirements.txt .
run pip install --no-cache-dir -r requirements.txt

run useradd -m -u 1000 appuser

copy --chown=appuser:appuser publisher.py .

user appuser

cmd ["python", "publisher.py"]
