# import library
import time
import logging
from pathlib import Path
from pytrends.request import TrendReq
from config.settings import KEYWORDS, COUNTRY, RAW_DATA_PATH

# function to extract data from google trends
def extract_trends(start_date, end_date=None):
    logging.info("Starting data extraction from Google Trends...")
    
    # data initialization
    data = None
    timeframe = None 
    
    # creating timeframe
    if end_date is None:
        timeframe = f"{start_date} {start_date}"
    else:
        timeframe = f"{start_date} {end_date}"

    # collecting data with raise error
    for attempt in range(3):
        try:
            pytrends = TrendReq(hl='id-ID', tz=360)
            pytrends.build_payload(KEYWORDS, timeframe=timeframe, geo=COUNTRY)
            data = pytrends.interest_over_time()
            break
        except Exception as e:
            logging.warning(f"Extract attempt {attempt+1} failed: {e}")
            time.sleep(5)
    else:
        raise RuntimeError("Failed to fetch Google Trends after 3 retries")
    
    # validating the data if the data is empty
    if data.empty:
        raise ValueError("Data Google Trends Empty!")
    
    # drop partial column
    if "isPartial" in data.columns:
        data = data.drop(columns=["isPartial"])

    # reset index for dataframe
    data = data.reset_index()

    # adjust saving data via cron
    raw_path = Path(RAW_DATA_PATH)
    raw_path.parent.mkdir(parents=True, exist_ok=True)

    # Save to raw layer
    data.to_csv(raw_path, index=False)

    # save the progress into logging 
    logging.info(f"Raw data saved to {RAW_DATA_PATH}")
