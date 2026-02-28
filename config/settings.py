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

# table
STAGING_TABLE = "stg_trends"
DIM_DATE = "dim_date"
DIM_KEYWORD = "dim_keyword"
FACT_TRENDS = "fact_trends"
PIPELINE_RUNS_TABLE = "pipeline_runs"
DATA_QUALITY_METRICS_TABLE = "data_quality_metrics"
MART_WEEKLY_TRENDS_TABLE = "mart_weekly_trends" 