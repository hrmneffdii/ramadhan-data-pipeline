import sqlite3
import pandas as pd
from config.settings import DB_PATH

SOURCE_TABLE = "ramadhan_trends"

def create_dim_keyword(conn, df):
    keywords = [col for col in df.columns if col not in ["date", "total_search_interest"]]
    
    dim_df = pd.DataFrame({
        "keyword_id": range(1, len(keywords) + 1),
        "keyword_name": keywords
    })

    dim_df.to_sql(
        name="dim_keyword",
        con=conn,
        if_exists="replace",
        index=False
    )
    
    return dim_df


def create_fact_trends(conn, df):
    keyword_cols = [col for col in df.columns if col not in ["date", "total_search_interest"]]
    
    fact_df = df.melt(
        id_vars=["date"],
        value_vars=keyword_cols,
        var_name="keyword",
        value_name="search_interest"
    )

    fact_df.to_sql(
        name="fact_trends",
        con=conn,
        if_exists="replace",
        index=False
    )

    return fact_df


def create_weekly_mart(conn):
    query = """
    SELECT 
        strftime('%Y-%W', date) AS year_week,
        keyword,
        AVG(search_interest) AS avg_search_interest
    FROM fact_trends
    GROUP BY year_week, keyword
    ORDER BY year_week;
    """

    mart_df = pd.read_sql(query, conn)

    mart_df.to_sql(
        name="mart_weekly_trends",
        con=conn,
        if_exists="replace",
        index=False
    )

    return mart_df


def run_modeling():
    print("🏗️ Starting data warehouse modeling...")

    conn = sqlite3.connect(DB_PATH)

    df = pd.read_sql(f"SELECT * FROM {SOURCE_TABLE}", conn)

    # Create Dimension Table
    create_dim_keyword(conn, df)

    # Create Fact Table
    create_fact_trends(conn, df)

    # Create Data Mart (Aggregated)
    create_weekly_mart(conn)

    conn.close()
    print("Data warehouse modeling completed!")