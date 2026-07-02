import os
import requests
from datetime import datetime

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


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


def alert_on_failure(context):
    dag_id = context.get("dag_run").dag_id if context.get("dag_run") else "unknown"
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown"
    reason = context.get("exception", "No exception detail")

    msg = (
        f"[X] [SYSTEM] PIPELINE FAILED\n"
        f"DAG: {dag_id} | Task: {task_id}\n"
        f"Time: {datetime.utcnow().isoformat()}\n"
        f"Reason: {reason}"
    )
    send_telegram(msg)


def alert_on_success(context):
    dag_id = context.get("dag_run").dag_id if context.get("dag_run") else "unknown"
    msg = (
        f"[OK] [SYSTEM] PIPELINE SUCCESS\n"
        f"DAG: {dag_id}\n"
        f"Time: {datetime.utcnow().isoformat()}"
    )
    send_telegram(msg)
