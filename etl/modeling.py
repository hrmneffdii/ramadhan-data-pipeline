# import library
import sqlite3
import pandas as pd
from config.settings import DB_PATH, DIM_KEYWORD, DIM_DATE, FACT_TRENDS

# function to create dimentional keywords
def upsert_dim_keyword(conn):
    query = f"""
        INSERT INTO {DIM_KEYWORD} (keyword_name)
        SELECT DISTINCT s.keyword
        FROM stg_trends s
        LEFT JOIN dim_keyword d
            ON s.keyword = d.keyword_name
        WHERE d.keyword_name IS NULL;
        """
    conn.execute(query)
    conn.commit()

# function to create fact trends
def insert_fact_trends(conn):
    query = f"""
        INSERT INTO {FACT_TRENDS} (date_key, keyword_key, interest_score)
        SELECT 
            s.date,
            d.keyword_key,
            s.interest_score
        FROM stg_trends s
        JOIN dim_keyword d
            ON s.keyword = d.keyword_name;
        """
    conn.execute(query)
    conn.commit()


# function to create data mart (aggregation phase)
def create_weekly_mart(conn):
    # create a query
    query = f"""
        SELECT 
            strftime('%Y-%W', date_key) AS year_week,
            keyword_key,
            AVG(interest_score) AS avg_interest_score
        FROM {FACT_TRENDS}
        GROUP BY year_week, keyword_key
        ORDER BY year_week;
    """

    # execute query 
    mart_df = pd.read_sql(query, conn)

    # save the result query into database
    mart_df.to_sql(
        name="mart_weekly_trends",
        con=conn,
        if_exists="replace",
        index=False
    )

    # return data for logging (optional)
    return mart_df

# main function to create data modeling warehouse
def run_modeling():
    print("Starting data warehouse modeling...")

    # create a connection for database
    with sqlite3.connect(DB_PATH) as conn:    
        # modeling the data
        upsert_dim_keyword(conn)
        insert_fact_trends(conn)
        create_weekly_mart(conn)

    print("Data warehouse modeling completed!")