import sqlite3
from config.settings import DB_PATH, STAGING_TABLE, DIM_DATE, DIM_KEYWORD, FACT_TRENDS, PIPELINE_RUNS_TABLE, DATA_QUALITY_METRICS_TABLE, MART_WEEKLY_TRENDS_TABLE
from etl.utils.generate_dim_date_utils import build_dim_date

def create_tables(conn):
    cursor = conn.cursor()

    # ==========================
    # STAGING TABLE
    # ==========================
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {STAGING_TABLE} (
        date TEXT NOT NULL,
        keyword TEXT NOT NULL,
        interest_score INTEGER,
        ingested_at TEXT DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT uq_stg_trends UNIQUE(date, keyword)
        );
    """)

    # ==========================
    # DIM_DATE
    # ==========================
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {DIM_DATE} (
        date_key INTEGER PRIMARY KEY,   -- format: YYYYMMDD
        full_date TEXT UNIQUE,
        day INTEGER,
        month INTEGER,
        month_name TEXT,
        quarter INTEGER,
        year INTEGER,
        is_weekend INTEGER
    );
    """)

    # ==========================
    # DIM_KEYWORD
    # ==========================
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {DIM_KEYWORD} (
        keyword_key INTEGER PRIMARY KEY AUTOINCREMENT,
        keyword_name TEXT UNIQUE,
        first_seen_date TEXT,
        is_active INTEGER DEFAULT 1
    );
    """)

    # ==========================
    # FACT_TRENDS
    # Grain:
    # 1 row = 1 keyword per 1 date
    # ==========================
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {FACT_TRENDS} (
        date_key INTEGER,
        keyword_key INTEGER,
        interest_score INTEGER,
        load_timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (date_key, keyword_key),
        FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
        FOREIGN KEY (keyword_key) REFERENCES dim_keyword(keyword_key)
    );
    """)

    # ==========================
    # PIPELINE RUN LOG
    # ==========================
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {PIPELINE_RUNS_TABLE} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_timestamp TEXT,
        status TEXT,
        rows_loaded INTEGER,
        duration_seconds REAL,
        error_message TEXT
    );
    """)

    # ==========================
    # DATA QUALITY METRICS
    # ==========================
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {DATA_QUALITY_METRICS_TABLE} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_timestamp TEXT,
        check_name TEXT,
        check_status TEXT,
        failed_rows INTEGER
    );
    """)

    # ==========================
    # MART: WEEKLY AGGREGATION
    # ==========================
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {MART_WEEKLY_TRENDS_TABLE} (
        year INTEGER,
        week INTEGER,
        keyword_key INTEGER,
        avg_interest_score REAL,
        PRIMARY KEY (year, week, keyword_key)
    );
    """)

    conn.commit()
        
def create_indexes(conn):
    cursor = conn.cursor()

    # ==========================
    # INDEXING DATE STAGING TABLE 
    # ==========================
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_stg_date
        ON stg_trends(date);
    """)
    
    # ==========================
    # INDEXING KEYWORD STAGING TABLE  
    # ==========================
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_stg_keyword
        ON stg_trends(keyword);
    """)
    
    # ==========================
    # INDEXING full_date DIM DATE   
    # ==========================
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_dim_date_full_date
        ON dim_date(full_date);
    """)
    
    # ==========================
    # INDEXING keyword_name DIM KEYWORD   
    # ==========================
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_dim_keyword_name
        ON dim_keyword(keyword_name);
    """)
    
    # ==========================
    # INDEXING date_key FACT TRENDS  
    # ==========================
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_fact_date
        ON fact_trends(date_key);
    """)

    # ==========================
    # INDEXING keyword_key FACT TRENDS   
    # ==========================
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_fact_keyword
        ON fact_trends(keyword_key);
    """)
    
    # ==========================
    # INDEXING keyword_key DATA MART  
    # ==========================
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_mart_week_keyword
        ON mart_weekly_trends(keyword_key);
    """)

    conn.commit()

def init_schema():
    with sqlite3.connect(DB_PATH) as conn:
        create_tables(conn)
        create_indexes(conn)
        
    build_dim_date()