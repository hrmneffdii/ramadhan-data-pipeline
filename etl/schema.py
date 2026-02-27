def create_tables(conn):
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_keyword (
            keyword_id INTEGER PRIMARY KEY,
            keyword_name TEXT UNIQUE
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_trends (
            date TEXT NOT NULL,
            keyword_id INTEGER NOT NULL,
            search_interest INTEGER,
            PRIMARY KEY (date, keyword_id),
            FOREIGN KEY (keyword_id)
                REFERENCES dim_keyword(keyword_id)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS data_quality_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            row_count INTEGER,
            total_sum INTEGER,
            min_date TEXT,
            max_date TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    conn.commit()


def create_indexes(conn):
    cursor = conn.cursor()

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_fact_date 
        ON fact_trends(date);
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_fact_keyword 
        ON fact_trends(keyword_id);
    """)

    conn.commit()