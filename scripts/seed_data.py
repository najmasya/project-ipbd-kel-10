import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.batch.ingest_inflasi import ingest_inflasi
from src.batch.ingest_kurs import ingest_kurs_usd_idr
from src.batch.ingest_emas import ingest_gold_price
from datetime import date


def seed_all():
    print("=== Seeding Historical Data ===")

    for year in range(2018, date.today().year + 1):
        print(f"\n--- Inflasi {year} ---")
        ingest_inflasi(year)

    print("\n--- Kurs USD/IDR (2018-now) ---")
    ingest_kurs_usd_idr(start_date="2018-01-01")

    print("\n--- Harga Emas (2018-now) ---")
    ingest_gold_price(start_date="2018-01-01")

    print("\n=== Seeding Complete ===")


if __name__ == "__main__":
    seed_all()
