FROM apache/airflow:2.9.3

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://get.docker.com -o /tmp/get-docker.sh && \
    sh /tmp/get-docker.sh && \
    rm /tmp/get-docker.sh

USER airflow
RUN pip install --no-cache-dir \
    psycopg2-binary>=2.9.10 \
    yfinance>=0.2.60 \
    requests==2.31 \
    pandas>=2.0 \
    numpy>=1.24 \
    minio==7.1 \
    python-dotenv==1.0 \
    pyarrow>=14.0 \
    mlflow==2.10.2 \
    scikit-learn \
    sqlalchemy \
    s3fs
