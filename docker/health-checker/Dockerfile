FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    psycopg2-binary>=2.9.10 \
    requests==2.31 \
    psutil==6.1.1

ENV PYTHONPATH=/app

CMD ["python", "-u", "/app/health_check.py"]
