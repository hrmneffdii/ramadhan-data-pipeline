# import library
import sqlite3
import pandas as pd
from config.settings import DB_PATH, TABLE_NAME

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
    # extract the keywords
    keyword_cols = [col for col in df.columns if col not in ["date", "total_search_interest"]]
    
    fact_df = df.melt(
        id_vars=["date"],
        value_vars=keyword_cols,
        var_name="keyword",
        value_name="search_interest"
    )

    # save to database
    fact_df.to_sql(
        name="fact_trends",
        con=conn,
        if_exists="replace",
        index=False
    )

    # return fact trends as dataframe
    return fact_df

# function to create data mart (aggregation phase)
def create_weekly_mart(conn):
    # create a query
    query = """
    SELECT 
        strftime('%Y-%W', date) AS year_week,
        keyword,
        AVG(search_interest) AS avg_search_interest
    FROM fact_trends
    GROUP BY year_week, keyword
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
    conn = sqlite3.connect(DB_PATH)
    
    # read the data from sql
    df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)

    # Create Dimension Table
    create_dim_keyword(conn, df)

    # Create Fact Table
    create_fact_trends(conn, df)

    # Create Data Mart (Aggregated)
    create_weekly_mart(conn)

    # close connection from database
    conn.close()

    print("Data warehouse modeling completed!")