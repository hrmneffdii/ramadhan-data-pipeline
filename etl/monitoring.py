# import library
import sqlite3
from datetime import datetime
from config.settings import DB_PATH, PIPELINE_RUNS_TABLE

# function to tract log pipeline and store it into database
def log_pipeline_run(status, rows_loaded, error_message=None, duration=None):
    # connect the database
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        # insert newest logging info pipeline as well as it's status
        cursor.execute(f"""
            INSERT INTO {PIPELINE_RUNS_TABLE} (run_timestamp, status, rows_loaded, duration_seconds, error_message)
            VALUES (?, ?, ?, ?, ?)
        """, (datetime.now().isoformat(), status, rows_loaded, duration, error_message))

        # commit the execution
        conn.commit()
