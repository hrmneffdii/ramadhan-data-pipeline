from pathlib import Path

# Base project directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Data paths (staging architecture)
RAW_DATA_PATH = BASE_DIR / "data" / "raw" / "trends_raw.csv"
PROCESSED_DATA_PATH = BASE_DIR / "data" / "processed" / "trends_processed.csv"
DB_PATH = BASE_DIR / "data" / "warehouse" / "trends.db"

# Google Trends Configuration
KEYWORDS = ["takjil", "baju lebaran", "snack lebaran"]
TIMEFRAME = "today 3-m"
COUNTRY = "ID"