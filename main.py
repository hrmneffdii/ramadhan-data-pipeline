import os
import logging
from pathlib import Path
from etl.extract import extract_trends
from etl.transform import transform_trends
from etl.load import load_to_warehouse
from etl.modeling import run_modeling

# Setup logging
LOG_PATH = Path("logs/pipeline.log")
LOG_PATH.parent.mkdir(exist_ok=True)

# basic for output pipeline logging
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# main pipeline 
def run_pipeline():
    logging.info("Pipeline started!")
    logging.info(f"Current Working Directory: {os.getcwd()}")
    logging.info("Running in scheduled mode")

    try:
        print("-----Pipeline started-----")
        logging.info("Step 1: Extract started")
        extract_trends()
        logging.info("Step 1: Extract completed")

        logging.info("Step 2: Transform started")
        transform_trends()
        logging.info("Step 2: Transform completed")

        logging.info("Step 3: Load started")
        load_to_warehouse()
        logging.info("Step 3: Load completed")

        logging.info("Step 4: Modeling started")
        run_modeling()
        logging.info("Step 4: Modeling completed")


        logging.info("Pipeline finished successfully!")
        print("-----ETL Pipeline Completed Successfully!-----")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        print(f"Pipeline Error: {e}")

if __name__ == "__main__":
    run_pipeline()