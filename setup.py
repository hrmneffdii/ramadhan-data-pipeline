import sqlite3
from config.settings import DB_PATH
from etl.schema import create_tables, create_indexes
from etl.utils.generate_dim_date_utils import build_dim_date


def setup_environment():
    print("Initializing warehouse environment...")

    with sqlite3.connect(DB_PATH) as conn:
        create_tables(conn)
        create_indexes(conn)

    build_dim_date()

    print("Setup completed successfully.")


if __name__ == "__main__":
    setup_environment()