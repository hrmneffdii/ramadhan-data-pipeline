import sqlite3
import pandas as pd
from config.settings import PROCESSED_DATA_PATH, DB_PATH

TABLE_NAME = "ramadhan_trends"

def load_to_warehouse():
    print("Loading data into SQLite warehouse...")

    df_new = pd.read_csv(PROCESSED_DATA_PATH)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    # Check if the tables is exists
    try:
        df_existing = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)

        # combine & drop duplicate based on date
        df_combined = pd.concat([df_existing, df_new])
        df_combined = df_combined.drop_duplicates(subset=["date"])

        df_combined.to_sql(
            name=TABLE_NAME,
            con=conn,
            if_exists="replace",
            index=False
        )

        print("Incremental load completed (deduplicated by date)")

    except Exception:
        # if the table isn't exists
        df_new.to_sql(
            name=TABLE_NAME,
            con=conn,
            if_exists="replace",
            index=False
        )
        print("First time load completed")

    conn.close()