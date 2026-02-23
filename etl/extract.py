from pytrends.request import TrendReq
from config.settings import KEYWORDS, TIMEFRAME, COUNTRY, RAW_DATA_PATH
import logging

def extract_trends():
    print("Starting data extraction from Google Trends...")

    pytrends = TrendReq(hl='id-ID', tz=360)
    pytrends.build_payload(KEYWORDS, timeframe=TIMEFRAME, geo=COUNTRY)

    data = pytrends.interest_over_time()

    if data.empty:
        raise ValueError("Data Google Trends Empty!")

    # reset index
    data = data.reset_index()

    # ensure that folder is exists
    RAW_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Save to raw layer
    data.to_csv(RAW_DATA_PATH, index=False)

    print(f"Raw data saved to {RAW_DATA_PATH}")
    
    logging.info(f"     Extracted rows: {len(data)}")
    logging.info(f"     Keywords used: {KEYWORDS}")

    return data