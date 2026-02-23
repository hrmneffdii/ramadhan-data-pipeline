import pandas as pd
from config.settings import RAW_DATA_PATH, PROCESSED_DATA_PATH

def transform_trends():
    print("Transforming raw data...")

    df = pd.read_csv(RAW_DATA_PATH)

    # Standardize column names
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

    # Convert date column
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])

    # Drop column isPartial
    if 'ispartial' in df.columns:
        df = df.drop(columns=['ispartial'])

    # Feature Engineering to create a total search interests
    keyword_cols = [col for col in df.columns if col != 'date']
    df['total_search_interest'] = df[keyword_cols].sum(axis=1)

    # Save processed data
    PROCESSED_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(PROCESSED_DATA_PATH, index=False)

    print(f"Processed data saved to {PROCESSED_DATA_PATH}")
    return df