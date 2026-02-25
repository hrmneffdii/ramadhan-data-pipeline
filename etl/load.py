# import library
import sqlite3
import pandas as pd
from config.settings import PROCESSED_DATA_PATH, DB_PATH, TABLE_NAME

# function append new data for incremental data
def load_to_warehouse():
    print("Loading data into SQLite warehouse...")

    # read processed csv
    df_new = pd.read_csv(PROCESSED_DATA_PATH)

    # connect database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # first, check last date in warehouse
        cursor.execute(f"SELECT MAX(date) FROM {TABLE_NAME}")
        result = cursor.fetchone()
        last_date = result[0]

        if last_date:
            # second, filter only new records
            df_new = df_new[df_new["date"] > last_date]

        if df_new.empty:
            print("No new data to load.")
            conn.close()
            return 0

        # third, append only new data
        df_new.to_sql(
            name=TABLE_NAME,
            con=conn,
            if_exists="append",
            index=False
        )

        print(f"Incremental load completed. {len(df_new)} rows inserted.")

    except sqlite3.OperationalError:
        # first time load (table not exists)
        df_new.to_sql(
            name=TABLE_NAME,
            con=conn,
            if_exists="replace",
            index=False
        )

        print("First time load completed.")

    # close the connection of the database
    conn.close()

    # returning length of new data
    return len(df_new)