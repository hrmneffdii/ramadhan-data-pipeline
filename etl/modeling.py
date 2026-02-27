# import library
import sqlite3
import pandas as pd
from config.settings import DB_PATH, TABLE_NAME
from etl.schema import create_tables, create_indexes

# function to create dimentional keywords
def create_dim_keyword(conn, df):

    # extract column variables as keywords
    keywords = [col for col in df.columns if col not in ["date", "total_search_interest"]]
    
    # create a dataframe with 
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

# function to create fact trends
def create_fact_trends(conn, df):

    keyword_cols = [
        col for col in df.columns 
        if col not in ["date", "total_search_interest"]
    ]

    fact_df = df.melt(
        id_vars=["date"],
        value_vars=keyword_cols,
        var_name="keyword_name",
        value_name="search_interest"
    )

    dim_keyword = pd.read_sql("SELECT * FROM dim_keyword", conn)

    fact_df = fact_df.merge(
        dim_keyword,
        on="keyword_name",
        how="left"
    )

    fact_df = fact_df[["date", "keyword_id", "search_interest"]]

    fact_df.to_sql(
        "fact_trends",
        conn,
        if_exists="append",
        index=False
    )

    return fact_df


# function to create data mart (aggregation phase)
def create_weekly_mart(conn):
    # create a query
    query = """
    SELECT 
        strftime('%Y-%W', date) AS year_week,
        keyword_id,
        AVG(search_interest) AS avg_search_interest
    FROM fact_trends
    GROUP BY year_week, keyword_id
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
        # read the data from sql
        df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)

        # 1. Ensure schema exists
        create_tables(conn)

        # 2. Transform & load
        create_dim_keyword(conn, df)
        create_fact_trends(conn, df)
        create_weekly_mart(conn)

        # 3. Ensure indexing
        create_indexes(conn)

    print("Data warehouse modeling completed!")