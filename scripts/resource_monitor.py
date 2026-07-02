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
CPU_ALERT_THRESHOLD = 90.0
MEMORY_ALERT_THRESHOLD = 90.0
DISK_ALERT_THRESHOLD = 90.0


def get_conn():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        user=POSTGRES_USER, password=POSTGRES_PASS,
        dbname=POSTGRES_DB,
    )


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


def save_alert(alert_name, message):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO alert_log (alert_type, alert_name, severity, source, message, triggered_at)
            VALUES ('system', %s, 'warning', 'resource-monitor', %s, NOW())
        """, (alert_name, message))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[RESOURCE] DB alert failed: {e}")


def monitor_loop():
    print(f"[RESOURCE] Starting resource monitor (interval={POLL_INTERVAL}s)")
    while True:
        try:
            hostname = platform.node()
            cpu = psutil.cpu_percent(interval=1)
            mem = psutil.virtual_memory()
            disk = psutil.disk_usage("/")
            net = psutil.net_io_counters()

            metrics = {
                "hostname": hostname,
                "cpu_percent": cpu,
                "memory_percent": mem.percent,
                "memory_used_mb": round(mem.used / (1024 * 1024), 2),
                "disk_percent": disk.percent,
                "disk_used_mb": round(disk.used / (1024 * 1024), 2),
                "network_rx_mb": round(net.bytes_recv / (1024 * 1024), 2),
                "network_tx_mb": round(net.bytes_sent / (1024 * 1024), 2),
                "recorded_at": datetime.utcnow(),
            }

            save_metrics(metrics)

            for name, val, threshold in [
                ("CPU", cpu, 90),
                ("MEMORY", mem.percent, 90),
                ("DISK", disk.percent, 90),
            ]:
                if val > threshold:
                    alert_msg = (
                        f"[SYSTEM] RESOURCE ALERT — {name} {val:.1f}%\n"
                        f"Threshold: {threshold:.0f}% | Host: {hostname}"
                    )
                    save_alert(f"{name} THRESHOLD EXCEEDED", alert_msg)
                    print(f"[RESOURCE] Alert logged: {name} = {val:.1f}%")

        except Exception as e:
            print(f"[RESOURCE] Error: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    monitor_loop()
