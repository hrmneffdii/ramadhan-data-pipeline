# import library
import time
import logging
from etl.extract import extract_trends
from etl.transform import transform_trends
from etl.load import load_to_warehouse
from etl.modeling import run_modeling
from etl.monitoring import log_pipeline_run
from etl.utils.date_utils import get_duration
from etl.utils.common_utils import parse_args
from etl.utils.date_utils import get_today_date
from etl.utils.db_utils import truncate_fact_table

# pipeline 
def run_pipeline(mode, start_date, end_date=None):
    start = time.time()
    
    try:
        logging.info("Pipeline started..")
        
        extract_trends(start_date, end_date)
        logging.info("Step 1: Extract completed!")

        transform_trends()
        logging.info("Step 2: Transform completed!")

        rows = load_to_warehouse(mode)
        logging.info("Step 3: Load completed!")

        if rows > 0:
            run_modeling()
            logging.info("Step 4: Modeling completed!")
        else:
            logging.info("Step 4: Modeling doesn't needed!")

        logging.info("Pipeline finished successfully!")
        
        log_pipeline_run("Success", rows, None, get_duration(start))

    except Exception as e:
        print(f"Pipeline Error: {e}")
        logging.error(f"Pipeline failed: {e}")

        log_pipeline_run("Failed", 0, str(e), get_duration(start))
    
    logging.info(f"Time : {get_duration(start):.2f} s")
    
    
def run_incremental(mode):
    today = get_today_date()  
    run_pipeline(mode, today)

def run_backfill(mode, start_date: str, end_date: str):
    if not start_date or not end_date:
        raise ValueError("Backfill mode requires --start-date and --end-date")

    run_pipeline(mode, start_date, end_date)
        
def run_full_rebuild(mode, start_date: str, end_date: str):
    if not start_date or not end_date:
        raise ValueError("full rebuild mode requires --start-date and --end-date")

    print("WARNING: Full rebuild initiated")
    rows_deleted = truncate_fact_table()

    logging.info(f"Rows deleted with run full rebuild {rows_deleted}")
    run_pipeline(mode, start_date, end_date)

if __name__ == "__main__":
    # parse argument
    args = parse_args()

    # incremental mode
    if args.mode == "incremental":
        run_incremental(args.mode)
    
    # backfill mode
    elif args.mode == "backfill":
        run_backfill(args.mode, args.start_date, args.end_date)

    # full rebuild mode
    elif args.mode == "full-rebuild":
        run_full_rebuild(args.mode, args.start_date, args.end_date)
        