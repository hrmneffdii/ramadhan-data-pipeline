# import library
from pytrends.request import TrendReq
from config.settings import KEYWORDS, TIMEFRAME, COUNTRY, RAW_DATA_PATH
import logging

# function to extract data from google trends
def extract_trends():
    print("Starting data extraction from Google Trends...")

    # finding the data
    pytrends = TrendReq(hl='id-ID', tz=360)
    pytrends.build_payload(KEYWORDS, timeframe=TIMEFRAME, geo=COUNTRY)

    # getting the data
    data = pytrends.interest_over_time()

    # validating the data if the data is empty
    if data.empty:
        raise ValueError("Data Google Trends Empty!")

    # reset index for dataframe
    data = data.reset_index()

    # Save to raw layer
    data.to_csv(RAW_DATA_PATH, index=False)

    # save the progress into logging 
    logging.info(f"Raw data saved to {RAW_DATA_PATH}")
    logging.info(f"     Extracted rows: {len(data)}")
    logging.info(f"     Keywords used: {KEYWORDS}")
