# import library
import sqlite3
from datetime import datetime
from config.settings import DB_PATH

# function to tract log pipeline and store it into database
def log_pipeline_run(status, rows_loaded):
    # connect the database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # create table if the table isn't exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_time TEXT,
            status TEXT,
            rows_loaded INTEGER
        )
    """)

    # insert newest logging info pipeline as well as it's status
    cursor.execute("""
        INSERT INTO pipeline_runs (run_time, status, rows_loaded)
        VALUES (?, ?, ?)
    """, (datetime.now().isoformat(), status, rows_loaded))

    # commit the execution
    conn.commit()

    # close the connection
    conn.close()