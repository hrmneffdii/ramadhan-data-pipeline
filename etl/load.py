# import library
import sqlite3
import pandas as pd
from config.settings import PROCESSED_DATA_PATH, DB_PATH, TABLE_NAME

# function append new data for incremental data
def load_to_warehouse():
    print("Loading data into SQLite warehouse...")

    # read processed csv
    df_new = pd.read_csv(PROCESSED_DATA_PATH)

    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()

            # check last date in warehouse
            try:
                cursor.execute(f"SELECT MAX(date) FROM {TABLE_NAME}")
                result = cursor.fetchone()
                last_date = result[0]
            except sqlite3.OperationalError:
                # table belum ada (first load)
                df_new.to_sql(
                    name=TABLE_NAME,
                    con=conn,
                    if_exists="replace",
                    index=False
                )
                print("First time load completed.")
                return len(df_new)

            # incremental filter
            if last_date:
                last_date = pd.to_datetime(last_date)
                df_new['date'] = df_new['date'].astype("datetime64[s]")
                df_new = df_new[df_new['date'] > last_date]

            # idempotent: tidak ada data baru
            if df_new.empty:
                print("No new data to load.")
                return 0

            # append new data
            df_new.to_sql(
                name=TABLE_NAME,
                con=conn,
                if_exists="append",
                index=False
            )

            print(f"Incremental load completed. {len(df_new)} rows inserted.")
            return len(df_new)

    except Exception as e:
        raise RuntimeError(f"Load to warehouse failed: {e}")
  
    # returning length of new data
    return len(df_new)