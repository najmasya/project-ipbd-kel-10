import os
import psycopg2
from pathlib import Path

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "ipbd_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ipbd_pass")
POSTGRES_DB = os.getenv("POSTGRES_DB", "pipeline_db")


def init_database():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    )
    conn.autocommit = True
    cur = conn.cursor()

    sql_path = Path(__file__).parent.parent / "sql" / "gold_layer.sql"
    sql_content = sql_path.read_text()

    cur.execute(sql_content)
    print("Gold Layer tables created successfully")

    cur.close()
    conn.close()


if __name__ == "__main__":
    init_database()
