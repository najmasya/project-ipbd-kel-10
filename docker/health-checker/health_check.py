import os
import time
import sys
import socket
import threading
import requests
from datetime import datetime

sys.path.insert(0, "/app")

CHECK_INTERVAL = int(os.getenv("HEALTH_CHECK_INTERVAL", "60"))
RESOURCE_INTERVAL = int(os.getenv("RESOURCE_POLL_INTERVAL", "30"))

SERVICES = [
    {"name": "PostgreSQL",  "host": "postgres", "port": 5432,  "type": "tcp"},
    {"name": "MinIO",       "host": "minio",    "port": 9000,  "type": "http",
     "url": "http://minio:9000/minio/health/live"},
    {"name": "Kafka",       "host": "kafka",    "port": 9092,  "type": "tcp"},
    {"name": "Spark Master","host": "spark-master", "port": 8080, "type": "http",
     "url": "http://spark-master:8080"},
    {"name": "Trino",       "host": "trino",    "port": 8080,  "type": "http",
     "url": "http://trino:8080/v1/info"},
    {"name": "MLflow",      "host": "mlflow",   "port": 5000,  "type": "http",
     "url": "http://mlflow:5000"},
    {"name": "Streamlit",   "host": "streamlit","port": 8501,  "type": "http",
     "url": "http://streamlit:8501"},
]

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "ipbd_user")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "ipbd_pass")
POSTGRES_DB = os.getenv("POSTGRES_DB", "pipeline_db")

service_status_cache = {}


def check_tcp(host, port, timeout=5):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((host, port))
        s.close()
        return True
    except Exception:
        return False


def check_http(url, timeout=10):
    try:
        r = requests.get(url, timeout=timeout)
        return r.status_code < 500
    except Exception:
        return False


def check_service(svc):
    if svc["type"] == "tcp":
        return check_tcp(svc["host"], svc["port"])
    elif svc["type"] == "http":
        return check_http(svc["url"])
    return False


def save_to_db(table, data_dict):
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT,
            user=POSTGRES_USER, password=POSTGRES_PASS,
            dbname=POSTGRES_DB,
        )
        cur = conn.cursor()
        cols = ", ".join(data_dict.keys())
        placeholders = ", ".join(["%s"] * len(data_dict))
        cur.execute(
            f"INSERT INTO {table} ({cols}) VALUES ({placeholders})",
            list(data_dict.values()),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[HEALTH] DB error: {e}")


def health_check_loop():
    print(f"[HEALTH] Service checker started (interval={CHECK_INTERVAL}s)")
    while True:
        for svc in SERVICES:
            was_up = service_status_cache.get(svc["name"], True)
            is_up = check_service(svc)
            service_status_cache[svc["name"]] = is_up

            if was_up and not is_up:
                msg = (
                    f"[SYSTEM] SERVICE DOWN — {svc['name']}\n"
                    f"Host: {svc['host']}:{svc['port']}\n"
                    f"Time: {datetime.utcnow().isoformat()}"
                )
                save_to_db("alert_log", {
                    "alert_type": "system",
                    "alert_name": f"{svc['name']} DOWN",
                    "severity": "critical",
                    "source": "health-checker",
                    "message": msg,
                    "triggered_at": datetime.utcnow(),
                })
                print(f"[HEALTH] {svc['name']} DOWN — logged to DB")

            elif not was_up and is_up:
                msg = (
                    f"[SYSTEM] SERVICE RECOVERED — {svc['name']}\n"
                    f"Time: {datetime.utcnow().isoformat()}"
                )
                save_to_db("alert_log", {
                    "alert_type": "system",
                    "alert_name": f"{svc['name']} RECOVERED",
                    "severity": "info",
                    "source": "health-checker",
                    "message": msg,
                    "triggered_at": datetime.utcnow(),
                })
                print(f"[HEALTH] {svc['name']} RECOVERED — logged to DB")

        time.sleep(CHECK_INTERVAL)


def resource_monitor_loop():
    try:
        import psutil
        import platform
    except ImportError:
        print("[RESOURCE] psutil not available, skipping resource monitor")
        return

    print(f"[RESOURCE] Resource monitor started (interval={RESOURCE_INTERVAL}s)")
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

            save_to_db("resource_metrics", metrics)

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
                    save_to_db("alert_log", {
                        "alert_type": "system",
                        "alert_name": f"{name} THRESHOLD EXCEEDED",
                        "severity": "warning",
                        "source": "resource-monitor",
                        "message": alert_msg,
                        "triggered_at": datetime.utcnow(),
                    })
                    print(f"[RESOURCE] Alert: {name} = {val:.1f}% — logged to DB")

        except Exception as e:
            print(f"[RESOURCE] Error: {e}")

        time.sleep(RESOURCE_INTERVAL)


if __name__ == "__main__":
    t1 = threading.Thread(target=health_check_loop, daemon=True)
    t2 = threading.Thread(target=resource_monitor_loop, daemon=True)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
