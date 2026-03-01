import sqlite3
import logging
from pathlib import Path
from config.settings import DB_PATH
from etl.schema import create_tables, create_indexes
from etl.utils.generate_dim_date_utils import build_dim_date

def setup_environment():
    # Setup logging
    LOG_PATH = Path("logs/pipeline.log")
    LOG_PATH.parent.mkdir(exist_ok=True)

    # basic setup for output pipeline logging
    logging.basicConfig(
        filename=LOG_PATH,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    # setup enviroment
    print("Initializing warehouse environment...")

    # create fact, dimentions table
    with sqlite3.connect(DB_PATH) as conn:
        # create table
        create_tables(conn)
        
        # create indexes
        create_indexes(conn)
        
        # build date dim
        build_dim_date(conn)

    print("Setup completed successfully.")

if __name__ == "__main__":
    setup_environment()