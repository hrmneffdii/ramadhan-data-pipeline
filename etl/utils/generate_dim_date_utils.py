import pandas as pd
import sqlite3
from config.settings import DB_PATH, DIM_DATE


def build_dim_date(start="2025-01-01", end="2030-12-31"):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        # Guard supaya tidak rebuild
        cursor.execute(f"SELECT COUNT(*) FROM {DIM_DATE}")
        count = cursor.fetchone()[0]

        if count > 0:
            print("dim_date already populated. Skipping...")
            return

        print("Building dim_date...")

        dates = pd.date_range(start=start, end=end)

        df = pd.DataFrame({
            "date_key": dates.strftime("%Y%m%d").astype(int),
            "full_date": dates.strftime("%Y-%m-%d"),
            "day": dates.day,
            "month": dates.month,
            "month_name": dates.strftime("%B"),
            "quarter": dates.quarter,
            "year": dates.year,
            "is_weekend": (dates.weekday >= 5).astype(int)
        })

        df.to_sql(
            name=DIM_DATE,
            con=conn,
            if_exists="append",
            index=False
        )

        print(f"dim_date successfully built with {len(df)} rows.")