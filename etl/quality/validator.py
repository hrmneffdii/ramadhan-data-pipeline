import sqlite3
import pandas as pd
from config.settings import DB_PATH, TABLE_NAME
from etl.quality.contract import EXPECTED_COLUMNS
from etl.quality.checks import (
    validate_not_empty,
    validate_no_nulls,
    validate_interest_range,
    validate_total_integrity,
    validate_duplicates,
    validate_date_continuity,
)

def validate_schema(df):
    """Strict schema check untuk processed layer"""
    actual_cols = list(df.columns)

    if actual_cols != EXPECTED_COLUMNS:
        raise ValueError(
            f"Schema drift detected.\nExpected: {EXPECTED_COLUMNS}\nGot: {actual_cols}"
        )

def collect_metrics(df):
    metrics = {
        "row_count": int(len(df)),
        "total_sum": int(df["total_search_interest"].sum()),
        "min_date": str(df["date"].min()),
        "max_date": str(df["date"].max()),
    }

    metrics_df = pd.DataFrame([metrics])

    with sqlite3.connect(DB_PATH) as conn:
        metrics_df.to_sql(
            name="data_quality_metrics",
            con=conn,
            if_exists="append",
            index=False,
        )

def run_quality_checks(df):
    """
    Master Quality Gate
    Dipanggil di main pipeline sebelum load
    """
    # 1. Schema contract
    validate_schema(df)

    # 2. Core data validity
    validate_not_empty(df)
    validate_no_nulls(df)

    # 3. Domain-specific checks (Google Trends)
    validate_interest_range(df)
    validate_total_integrity(df)

    # 4. Time-series quality (advanced)
    validate_duplicates(df)
    validate_date_continuity(df)
    
    #5. Data metrics quality
    collect_metrics(df)