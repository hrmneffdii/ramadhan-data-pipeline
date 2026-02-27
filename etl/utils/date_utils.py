import time
from datetime import datetime, timedelta

DATE_FORMAT = "%Y-%m-%d"

def get_today_date() -> str:
    """
    Return today's date in YYYY-MM-DD format.
    Centralized to ensure consistency across pipeline.
    """
    return datetime.utcnow().strftime(DATE_FORMAT)


def get_yesterday_date() -> str:
    """
    Useful for incremental pipelines that process D-1 data.
    """
    yesterday = datetime.utcnow() - timedelta(days=1)
    return yesterday.strftime(DATE_FORMAT)


def generate_date_range(start_date: str, end_date: str):
    validate_date_format(start_date)
    validate_date_format(end_date)
    
    start = datetime.strptime(start_date, DATE_FORMAT)
    end = datetime.strptime(end_date, DATE_FORMAT)

    dates = []
    current = start

    while current <= end:
        dates.append(current.strftime(DATE_FORMAT))
        current += timedelta(days=1)

    return dates

def validate_date_format(date_str: str):
    try:
        datetime.strptime(date_str, DATE_FORMAT)
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Use YYYY-MM-DD")
    
def get_duration(start_time):
    end_time = time.time()
    duration = end_time - start_time
    
    return duration