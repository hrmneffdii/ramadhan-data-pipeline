# import library
import os
import logging
from pathlib import Path
from etl.extract import extract_trends
from etl.load import load_to_warehouse
from etl.transform import transform_trends
from etl.utils import validate_data
from etl.modeling import run_modeling
from etl.monitoring import log_pipeline_run

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
def run_pipeline():
    logging.info("Pipeline started!")
    logging.info(f"Current Working Directory: {os.getcwd()}")
    logging.info("Running in scheduled mode")

    try:
        print("Pipeline started..")
        logging.info("Step 1: Extract started")
        extract_trends()
        logging.info("        Extract completed")

        logging.info("Step 2: Transform started")
        df = transform_trends()
        logging.info("        Transform completed")

        logging.info("Step 3: Validate data")
        validate_data(df)
        logging.info("        validate completed")

        logging.info("Step 4: Load started")
        rows = load_to_warehouse()
        logging.info("        Load completed")

        logging.info("Step 5: Modeling started")
        run_modeling()
        logging.info("        Modeling completed")

        log_pipeline_run("Success", rows)

        print("Pipeline ended..")

        logging.info("Pipeline finished successfully!")

    except Exception as e:
        print(f"Pipeline Error: {e}")
        log_pipeline_run("Failed", 0)
        
        logging.error(f"Pipeline failed: {e}")

if __name__ == "__main__":
    run_pipeline()