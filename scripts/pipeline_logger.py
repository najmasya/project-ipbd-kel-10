import os
import time
from datetime import datetime
from functools import wraps

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "ipbd_user")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "ipbd_pass")
POSTGRES_DB = os.getenv("POSTGRES_DB", "pipeline_db")


def get_conn():
    import psycopg2
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        user=POSTGRES_USER, password=POSTGRES_PASS,
        dbname=POSTGRES_DB,
    )


def log_pipeline(pipeline_name, task_name=None, status="started",
                 message="", records_count=0, duration_ms=0,
                 started_at=None, finished_at=None):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO pipeline_logs
                (pipeline_name, task_name, status, message, records_count,
                 duration_ms, started_at, finished_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            pipeline_name, task_name, status, message, records_count,
            duration_ms,
            started_at or datetime.utcnow(),
            finished_at or datetime.utcnow(),
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[LOGGER] Failed to write log: {e}")


def with_pipeline_log(pipeline_name, task_name=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            started_at = datetime.utcnow()
            start_ts = time.time()
            try:
                result = func(*args, **kwargs)
                duration = int((time.time() - start_ts) * 1000)
                records = len(result) if hasattr(result, '__len__') else 0
                log_pipeline(
                    pipeline_name=pipeline_name,
                    task_name=task_name,
                    status="success",
                    message=f"{pipeline_name} completed",
                    records_count=records,
                    duration_ms=duration,
                    started_at=started_at,
                )
                return result
            except Exception as e:
                duration = int((time.time() - start_ts) * 1000)
                log_pipeline(
                    pipeline_name=pipeline_name,
                    task_name=task_name,
                    status="failed",
                    message=str(e),
                    duration_ms=duration,
                    started_at=started_at,
                )
                raise
        return wrapper
    return decorator
