# import library
import os
import time
import logging
import argparse
from pathlib import Path
from etl.extract import extract_trends
from etl.load import load_to_warehouse
from etl.transform import transform_trends
from etl.modeling import run_modeling
from etl.monitoring import log_pipeline_run
from etl.quality.validator import run_quality_checks
from etl.utils.date_utils import get_today_date, get_duration
from etl.utils.db_utils import truncate_fact_table

# Setup logging
LOG_PATH = Path("logs/pipeline.log")
LOG_PATH.parent.mkdir(exist_ok=True)

# basic setup for output pipeline logging
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# main pipeline 
def run_pipeline_for_date(start_date, end_date=None):
    logging.info("Pipeline started!")
    logging.info(f"Current Working Directory: {os.getcwd()}")
    logging.info("Running in scheduled mode")

    # start time
    start = time.time()

    try:
        print("Pipeline started..")
        extract_trends(start_date, end_date)
        logging.info("Step 1: Extract completed!")

        df = transform_trends()
        logging.info("Step 2: Transform completed!")

        run_quality_checks(df)
        logging.info("Step 3: Validate data completed!")

        rows = load_to_warehouse()
        logging.info("Step 4: Load completed!")

        if rows > 0:
            run_modeling()
            logging.info("Step 5: Modeling completed!")
        else:
            logging.info("Step 5: Modeling doesn't needed!")

        print("Pipeline ended..")

        logging.info("Pipeline finished successfully!")
        
        log_pipeline_run("Success", rows, None, get_duration(start))

    except Exception as e:
        print(f"Pipeline Error: {e}")
        logging.error(f"Pipeline failed: {e}")

        log_pipeline_run("Failed", 0, None, get_duration(start))
        raise 
    
    logging.info(f"Time : {get_duration(start):.2f} s")
    
def parse_args():
    parser = argparse.ArgumentParser(description="Data Pipeline Runner")

    parser.add_argument(
        "--mode",
        type=str,
        default="incremental",
        choices=["incremental", "backfill", "full-rebuild"],
        help="Pipeline running mode"
    )

    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")

    return parser.parse_args()

def run_backfill(start_date: str, end_date: str):
    if not start_date or not end_date:
        raise ValueError("Backfill mode requires --start-date and --end-date")

    run_pipeline_for_date(start_date, end_date)
        
def run_full_rebuild(start_date: str, end_date: str):
    print("WARNING: Full rebuild initiated")

    rows_deleted = truncate_fact_table()

    logging.info(f"Rows deleted with run full rebuild {rows_deleted}")
    
    run_pipeline_for_date(start_date, end_date)
        
    
if __name__ == "__main__":
    args = parse_args()

    if args.mode == "incremental":
        today = get_today_date()  
        run_pipeline_for_date(today)

    elif args.mode == "backfill":
        run_backfill(args.start_date, args.end_date)

    elif args.mode == "full-rebuild":
        run_full_rebuild(args.start_date, args.end_date)
        