FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    streamlit==1.28 \
    "streamlit-authenticator>=0.3.0" \
    pandas==2.0 \
    numpy==1.24 \
    plotly==5.18 \
    altair==5.2 \
    trino==0.328 \
    python-dotenv==1.0

COPY . /app

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')"

ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
