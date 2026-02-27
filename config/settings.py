# import library
import os
from pathlib import Path
from dotenv import load_dotenv

# load .env
load_dotenv()

# Project root (penting untuk cron)
BASE_DIR = Path(__file__).resolve().parent.parent

# getting data from .env
RAW_DATA_PATH = BASE_DIR / os.getenv("RAW_DATA_PATH")
PROCESSED_DATA_PATH = BASE_DIR / os.getenv("PROCESSED_DATA_PATH")
DB_PATH = BASE_DIR / os.getenv("DB_PATH")

# custom variabel as input
KEYWORDS = ["takjil", "baju lebaran", "snack lebaran", "diskon"]
TIMEFRAME = "today 3-m"
COUNTRY = "ID"

# custom table
TABLE_NAME = "stg_trends"
