# import library
import sqlite3
import pandas as pd
from config.settings import PROCESSED_DATA_PATH, DB_PATH, STAGING_TABLE

def get_last_loaded_date(conn):
    cursor = conn.cursor()
    cursor.execute(f"SELECT MAX(date) FROM {STAGING_TABLE}")
    result = cursor.fetchone()[0]
    return result


def load_to_warehouse(mode):
    print("Loading data into SQLite warehouse...")

    df_new = pd.read_csv(PROCESSED_DATA_PATH)

    if df_new.empty:
        print("No data to load.")
        return 0

    inserted_rows = 0

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        # INCREMENTAL MODE
        if mode == "incremental":
            last_loaded_date = get_last_loaded_date(conn)

            if last_loaded_date:
                df_new = df_new[df_new["date"] > last_loaded_date]

            if df_new.empty:
                print("No new data after incremental filtering.")
                return 0

            cursor.executemany(
                f"""
                INSERT OR IGNORE INTO {STAGING_TABLE}
                (date, keyword, interest_score)
                VALUES (?, ?, ?)
                """,
                df_new[["date", "keyword", "interest_score"]].values.tolist()
            )

        # BACKFILL MODE
        elif mode == "backfill":

            cursor.executemany(
                f"""
                INSERT OR IGNORE INTO {STAGING_TABLE}
                (date, keyword, interest_score)
                VALUES (?, ?, ?)
                """,
                df_new[["date", "keyword", "interest_score"]].values.tolist()
            )

        # FULL REBUILD MODE
        elif mode == "full-rebuild":
            # delete staging table
            cursor.execute(f"DELETE FROM {STAGING_TABLE}")
            conn.commit()

            cursor.executemany(
                f"""
                INSERT INTO {STAGING_TABLE}
                (date, keyword, interest_score)
                VALUES (?, ?, ?)
                """,
                df_new[["date", "keyword", "interest_score"]].values.tolist()
            )

        else:
            raise ValueError(f"Unknown mode: {mode}")

        inserted_rows = cursor.rowcount
        conn.commit()

    print(f"{inserted_rows} new rows inserted.")
    return inserted_rows