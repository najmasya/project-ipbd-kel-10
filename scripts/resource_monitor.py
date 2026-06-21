import os
import time
import platform
import psutil
import psycopg2
from datetime import datetime

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "ipbd_user")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "ipbd_pass")
POSTGRES_DB = os.getenv("POSTGRES_DB", "pipeline_db")
POLL_INTERVAL = int(os.getenv("RESOURCE_POLL_INTERVAL", "30"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
CPU_ALERT_THRESHOLD = 90.0
MEMORY_ALERT_THRESHOLD = 90.0
DISK_ALERT_THRESHOLD = 90.0


def get_conn():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        user=POSTGRES_USER, password=POSTGRES_PASS,
        dbname=POSTGRES_DB,
    )


def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    import requests
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
        }, timeout=10)
    except Exception as e:
        print(f"[RESOURCE] Telegram failed: {e}")


def collect_metrics():
    hostname = platform.node()
    cpu = psutil.cpu_percent(interval=1)
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    net = psutil.net_io_counters()

    return {
        "hostname": hostname,
        "cpu_percent": cpu,
        "memory_percent": mem.percent,
        "memory_used_mb": mem.used / (1024 * 1024),
        "disk_percent": disk.percent,
        "disk_used_mb": disk.used / (1024 * 1024),
        "network_rx_mb": net.bytes_recv / (1024 * 1024),
        "network_tx_mb": net.bytes_sent / (1024 * 1024),
        "recorded_at": datetime.utcnow(),
    }


def save_metrics(metrics):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO resource_metrics
                (hostname, cpu_percent, memory_percent, memory_used_mb,
                 disk_percent, disk_used_mb, network_rx_mb, network_tx_mb, recorded_at)
            VALUES (%(hostname)s, %(cpu_percent)s, %(memory_percent)s,
                    %(memory_used_mb)s, %(disk_percent)s, %(disk_used_mb)s,
                    %(network_rx_mb)s, %(network_tx_mb)s, %(recorded_at)s)
        """, metrics)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[RESOURCE] DB save failed: {e}")


def check_alerts(metrics):
    alerts = []
    if metrics["cpu_percent"] > CPU_ALERT_THRESHOLD:
        alerts.append(("CPU", metrics["cpu_percent"], CPU_ALERT_THRESHOLD))
    if metrics["memory_percent"] > MEMORY_ALERT_THRESHOLD:
        alerts.append(("MEMORY", metrics["memory_percent"], MEMORY_ALERT_THRESHOLD))
    if metrics["disk_percent"] > DISK_ALERT_THRESHOLD:
        alerts.append(("DISK", metrics["disk_percent"], DISK_ALERT_THRESHOLD))

    for name, value, threshold in alerts:
        msg = (
            f"[X] <b>RESOURCE ALERT</b>\n"
            f"Resource: {name}\n"
            f"Value: {value:.1f}% | Threshold: {threshold:.0f}%\n"
            f"Host: {metrics['hostname']}"
        )
        send_telegram(msg)
        print(f"[RESOURCE] Alert: {name} = {value:.1f}%")


def monitor_loop():
    print(f"[RESOURCE] Starting resource monitor (interval={POLL_INTERVAL}s)")
    while True:
        try:
            metrics = collect_metrics()
            save_metrics(metrics)
            check_alerts(metrics)
            print(f"[RESOURCE] CPU={metrics['cpu_percent']:.1f}% "
                  f"RAM={metrics['memory_percent']:.1f}% "
                  f"Disk={metrics['disk_percent']:.1f}%")
        except Exception as e:
            print(f"[RESOURCE] Error: {e}")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    monitor_loop()
