# import library
import sqlite3
import pandas as pd
from config.settings import PROCESSED_DATA_PATH, DB_PATH, STAGING_TABLE
from etl.schema import create_tables, create_indexes

def load_to_warehouse():
    print("Loading data into SQLite warehouse...")

    df_new = pd.read_csv(PROCESSED_DATA_PATH)

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        inserted_rows = 0

        for _, row in df_new.iterrows():
            cursor.execute(f"""
                INSERT OR IGNORE INTO {STAGING_TABLE} (date, keyword, interest_score)
                VALUES (?, ?, ?)
            """, (row['date'], row['keyword'], row['interest_score']))

            if cursor.rowcount > 0:
                inserted_rows += 1

        conn.commit()

    print(f"{inserted_rows} new rows inserted.")
    return inserted_rows