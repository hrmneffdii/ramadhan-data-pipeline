import sqlite3
from config.settings import DB_PATH

import sqlite3

def truncate_fact_table():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        # Check if table exists
        cursor.execute("""
            SELECT name 
            FROM sqlite_master 
            WHERE type='table' AND name='fact_trends';
        """)
        
        table_exists = cursor.fetchone()

        if not table_exists:
            return 0

        # Get row count before delete (optional but good practice)
        cursor.execute("SELECT COUNT(*) FROM fact_trends;")
        row_count = cursor.fetchone()[0]

        cursor.execute("DELETE FROM fact_trends;")
        conn.commit()

        return row_count