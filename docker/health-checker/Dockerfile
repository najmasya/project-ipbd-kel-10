FROM python:3.11-alpine

RUN apk add --no-cache \
    postgresql-dev \
    gcc \
    musl-dev \
    linux-headers

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    psycopg2-binary==2.9 \
    requests==2.31 \
    psutil==6.1.1

ENV PYTHONPATH=/app

CMD ["python", "-u", "/app/health_check.py"]
