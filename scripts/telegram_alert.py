import os
import requests

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[ALERT] Telegram not configured. Skipping.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
        }, timeout=10)
        resp.raise_for_status()
        print(f"[ALERT] Sent: {message[:60]}...")
    except Exception as e:
        print(f"[ALERT] Failed: {e}")


def send_system_alert(service: str, status: str, detail: str = ""):
    icon = "[OK]" if status == "healthy" else "[!]" if status == "warning" else "[X]"
    msg = (
        f"{icon} <b>SYSTEM ALERT</b>\n"
        f"Service: {service}\n"
        f"Status: {status}\n"
        f"Detail: {detail}"
    )
    send_telegram(msg)


def send_business_alert(metric: str, value: float, threshold: float, direction: str):
    arrow = "^" if direction == "above" else "v"
    msg = (
        f" <b>BUSINESS ALERT</b>\n"
        f"Metric: {metric}\n"
        f"Value: {value:.2f} {arrow} Threshold: {threshold:.2f}\n"
        f"Condition: Price {direction} threshold"
    )
    send_telegram(msg)


def send_pipeline_alert(dag_name: str, task_name: str, status: str, reason: str = ""):
    icon = "[OK]" if status == "success" else "[X]"
    msg = (
        f"{icon} <b>PIPELINE {status.upper()}</b>\n"
        f"DAG: {dag_name}\n"
        f"Task: {task_name}\n"
        f"Reason: {reason}"
    )
    send_telegram(msg)


def send_resource_alert(resource: str, value: float, threshold: float, host: str = ""):
    msg = (
        f"[X] <b>RESOURCE ALERT</b>\n"
        f"Resource: {resource}\n"
        f"Value: {value:.1f}% | Threshold: {threshold:.0f}%\n"
        f"Host: {host}"
    )
    send_telegram(msg)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        send_system_alert("PostgreSQL", "healthy")
        send_business_alert("XAUUSD Price", 1950.5, 1900.0, "above")
    else:
        print("Usage: python telegram_alert.py test")
