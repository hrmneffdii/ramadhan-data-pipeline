import pandas as pd
from etl.quality.contract import EXPECTED_COLUMNS, NUMERIC_COLUMNS

def validate_not_empty(df):
    """Critical check: dataframe tidak boleh kosong"""
    if df.empty:
        raise ValueError("Data Quality Failed: DataFrame is empty")


def validate_no_nulls(df):
    """Processed layer seharusnya tidak punya null"""
    null_count = df.isnull().sum().sum()
    if null_count > 0:
        raise ValueError(f"Data Quality Failed: Found {null_count} null values")


def validate_interest_range(df):
    """
    Google Trends interest harus di range 0–100
    Berlaku untuk semua keyword columns
    """
    for col in NUMERIC_COLUMNS:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

        invalid = ((df[col] < 0) | (df[col] > 100)).sum()
        if invalid > 0:
            raise ValueError(
                f"Data Quality Failed: {invalid} rows in '{col}' out of range (0-100)"
            )


def validate_total_integrity(df):
    """
    total_search_interest harus = sum(keyword columns)
    Ini sangat penting karena kolom ini derived metric
    """
    if "total_search_interest" not in df.columns:
        raise ValueError("Missing column: total_search_interest")

    calculated_total = df[NUMERIC_COLUMNS].sum(axis=1)
    mismatch = (calculated_total != df["total_search_interest"]).sum()

    if mismatch > 0:
        raise ValueError(
            f"Data Quality Failed: {mismatch} rows have incorrect total_search_interest"
        )


def validate_duplicates(df):
    """
    Tidak boleh ada duplicate date di processed layer (daily grain)
    """
    if "date" not in df.columns:
        raise ValueError("Missing column: date")

    dup_count = df.duplicated(subset=["date"]).sum()
    if dup_count > 0:
        raise ValueError(
            f"Data Quality Failed: {dup_count} duplicate date records detected"
        )


def validate_date_continuity(df):
    """
    Time series harus tidak bolong (daily continuity)
    Advanced but lightweight check (recruiter suka)
    """
    if "date" not in df.columns:
        raise ValueError("Missing column: date")

    df_sorted = df.sort_values("date").copy()
    df_sorted["date"] = pd.to_datetime(df_sorted["date"])

    expected_dates = pd.date_range(
        start=df_sorted["date"].min(),
        end=df_sorted["date"].max(),
        freq="D"
    )

    actual_dates = set(df_sorted["date"])
    missing_dates = [d for d in expected_dates if d not in actual_dates]

    if missing_dates:
        raise ValueError(
            f"Data Quality Failed: Missing {len(missing_dates)} dates in time series"
        )