# import library
import os
from dotenv import load_dotenv

# load .env
load_dotenv()

# getting data from .env
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH")
PROCESSED_DATA_PATH = os.getenv("PROCESSED_DATA_PATH")
DB_PATH = os.getenv("DB_PATH")

LOG_LEVEL = os.getenv("LOG_LEVEL")

# custom variabel as input
KEYWORDS = ["takjil", "baju lebaran", "snack lebaran", "diskon"]
TIMEFRAME = "today 3-m"
COUNTRY = "ID"

TABLE_NAME = "ramadhan_trends"