# import library
import sqlite3
import pandas as pd
from config.settings import (
    DB_PATH,
    STAGING_TABLE,
    DIM_KEYWORD,
    DIM_DATE,
    FACT_TRENDS,
    MART_WEEKLY_TRENDS_TABLE
)

# ==========================
# UPSERT DIM KEYWORD
# ==========================
def upsert_dim_keyword(conn):
    query = f"""
        INSERT INTO {DIM_KEYWORD} (keyword_name, first_seen_date)
        SELECT DISTINCT
            s.keyword,
            MIN(s.date)
        FROM {STAGING_TABLE} s
        LEFT JOIN {DIM_KEYWORD} d
            ON s.keyword = d.keyword_name
        WHERE d.keyword_name IS NULL
        GROUP BY s.keyword;
    """
    conn.execute(query)


# ==========================
# UPSERT FACT TRENDS
# ==========================
def upsert_fact_trends(conn):
    query = f"""
        INSERT INTO {FACT_TRENDS} (
            date_key,
            keyword_key,
            interest_score
        )
        SELECT
            d.date_key,
            k.keyword_key,
            s.interest_score
        FROM {STAGING_TABLE} s
        JOIN {DIM_DATE} d
            ON s.date = d.full_date
        JOIN {DIM_KEYWORD} k
            ON s.keyword = k.keyword_name
        ON CONFLICT(date_key, keyword_key)
        DO UPDATE SET
            interest_score = excluded.interest_score,
            load_timestamp = CURRENT_TIMESTAMP;
    """
    conn.execute(query)


# ==========================
# REBUILD WEEKLY MART
# ==========================
def rebuild_weekly_mart(conn):

    # Clear existing mart safely (keep schema & PK)
    conn.execute(f"DELETE FROM {MART_WEEKLY_TRENDS_TABLE};")

    query = f"""
        INSERT INTO {MART_WEEKLY_TRENDS_TABLE} (
            year,
            week,
            keyword_key,
            avg_interest_score
        )
        SELECT 
            d.year,
            CAST(strftime('%W', d.full_date) AS INTEGER) AS week,
            f.keyword_key,
            AVG(f.interest_score) AS avg_interest_score
        FROM {FACT_TRENDS} f
        JOIN {DIM_DATE} d
            ON f.date_key = d.date_key
        GROUP BY d.year, week, f.keyword_key;
    """

    conn.execute(query)


# ==========================
# MAIN MODELING FUNCTION
# ==========================
def run_modeling():
    print("Starting data warehouse modeling...")

    try:
        with sqlite3.connect(DB_PATH) as conn:

            # Single atomic transaction
            conn.execute("BEGIN")

            upsert_dim_keyword(conn)
            upsert_fact_trends(conn)
            rebuild_weekly_mart(conn)

            conn.commit()

        print("Data warehouse modeling completed successfully!")

    except Exception as e:
        print(f"Modeling failed: {e}")
        raise