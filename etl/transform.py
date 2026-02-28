# import library
import logging
import pandas as pd
from config.settings import RAW_DATA_PATH, PROCESSED_DATA_PATH

def transform_trends():
    print("Transforming raw data to long format...")

    # Read raw data
    df = pd.read_csv(RAW_DATA_PATH)

    # Standardize column names
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

    # Ensure date column exists
    if 'date' not in df.columns:
        raise ValueError("RAW contract violation: 'date' column missing")

    # Convert date
    df['date'] = pd.to_datetime(df['date'])

    # Drop non-keyword columns if exist
    if 'ispartial' in df.columns:
        df = df.drop(columns=['ispartial'])

    # Identify keyword columns (exclude date)
    keyword_cols = [col for col in df.columns if col != 'date']

    # Convert wide → long
    df_long = df.melt(
        id_vars=['date'],
        value_vars=keyword_cols,
        var_name='keyword',
        value_name='interest_score'
    )

    # Enforce data types
    df_long['interest_score'] = df_long['interest_score'].astype(int)

    # Contract validation rules
    if df_long['interest_score'].min() < 0 or df_long['interest_score'].max() > 100:
        raise ValueError("Processed contract violation: interest_score out of range")

    # Save processed data
    df_long.to_csv(PROCESSED_DATA_PATH, index=False)

    logging.info(f"Processed (long format) saved to {PROCESSED_DATA_PATH}")

    return df_long