FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    mlflow==2.10.2 \
    psycopg2-binary>=2.9.10 \
    boto3==1.34 \
    pyarrow==14.0

EXPOSE 5000

ENTRYPOINT ["mlflow"]
