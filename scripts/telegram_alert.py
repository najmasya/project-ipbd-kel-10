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
        f"{icon} [SYSTEM] {service} — {status.upper()}\n"
        f"Detail: {detail}"
    )
    send_telegram(msg)


def send_business_alert(metric: str, value: float, threshold: float, direction: str):
    arrow = "^" if direction == "above" else "v"
    msg = (
        f" [BUSINESS] {metric}\n"
        f"Value: {value:.2f} {arrow} Threshold: {threshold:.2f}\n"
        f"Condition: Price {direction} threshold"
    )
    send_telegram(msg)


def send_pipeline_alert(dag_name: str, task_name: str, status: str, reason: str = ""):
    icon = "[OK]" if status == "success" else "[X]"
    msg = (
        f"{icon} [SYSTEM] PIPELINE {status.upper()}\n"
        f"DAG: {dag_name} | Task: {task_name}\n"
        f"Reason: {reason}"
    )
    send_telegram(msg)


def send_resource_alert(resource: str, value: float, threshold: float, host: str = ""):
    msg = (
        f"[X] [SYSTEM] RESOURCE ALERT — {resource} {value:.1f}%\n"
        f"Threshold: {threshold:.0f}% | Host: {host}"
    )
    send_telegram(msg)


def send_business_xauusd_spike(symbol: str, price_change: float, threshold: float):
    msg = (
        f" [BUSINESS] XAUUSD Spike — Symbol: {symbol}\n"
        f"Price Change: {price_change:+.2f} | Threshold: {threshold:+.2f}"
    )
    send_telegram(msg)


def send_business_kurs_spike(kurs_change: float, threshold: float):
    msg = (
        f" [BUSINESS] Kurs USD/IDR Spike\n"
        f"Change: {kurs_change:+.2f}% | Threshold: {threshold:+.2f}%"
    )
    send_telegram(msg)


def send_business_gold_anomaly(price: float, lower: float, upper: float):
    direction = "above upper" if price > upper else "below lower"
    msg = (
        f" [BUSINESS] Gold Price Anomaly\n"
        f"Price: {price:.2f} | Bounds: [{lower:.2f}, {upper:.2f}]\n"
        f"Condition: Price is {direction} bound"
    )
    send_telegram(msg)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        send_system_alert("PostgreSQL", "healthy")
        send_business_alert("Gold Price Prediction", 125000.0, 120000.0, "above")
        send_business_xauusd_spike("XAUUSDc", 2.5, 2.0)
    else:
        print("Usage: python telegram_alert.py test")
