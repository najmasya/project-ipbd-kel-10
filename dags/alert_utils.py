import os
import requests
import psycopg2
from datetime import datetime

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_USER = os.getenv("POSTGRES_USER", "ipbd_user")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "ipbd_pass")
PG_DB = os.getenv("POSTGRES_DB", "pipeline_db")


def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[ALERT] Telegram not configured")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
        }, timeout=10)
    except Exception as e:
        print(f"[ALERT] Failed: {e}")


def log_to_db(pipeline_name, task_name, status, severity, message, started_at=None):
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, user=PG_USER,
            password=PG_PASS, dbname=PG_DB,
        )
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO pipeline_logs
                (pipeline_name, task_name, status, severity, message, started_at, finished_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
        """, (
            pipeline_name, task_name, status, severity, message,
            started_at or datetime.utcnow(),
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[ALERT] Failed to log to DB: {e}")


def alert_on_failure(context):
    dag_id = context.get("dag_run").dag_id if context.get("dag_run") else "unknown"
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown"
    reason = context.get("exception", "No exception detail")
    exec_date = context.get("dag_run").execution_date if context.get("dag_run") else datetime.utcnow()

    msg = (
        f"[X] [SYSTEM] PIPELINE FAILED\n"
        f"DAG: {dag_id} | Task: {task_id}\n"
        f"Time: {datetime.utcnow().isoformat()}\n"
        f"Reason: {reason}"
    )
    send_telegram(msg)
    log_to_db(dag_id, task_id, "failed", "FATAL", str(reason), exec_date)


def alert_on_success(context):
    dag_id = context.get("dag_run").dag_id if context.get("dag_run") else "unknown"
    exec_date = context.get("dag_run").execution_date if context.get("dag_run") else datetime.utcnow()
    msg = (
        f"[OK] [SYSTEM] PIPELINE SUCCESS\n"
        f"DAG: {dag_id}\n"
        f"Time: {datetime.utcnow().isoformat()}"
    )
    send_telegram(msg)
    log_to_db(dag_id, None, "success", "INFO", f"{dag_id} completed successfully", exec_date)
