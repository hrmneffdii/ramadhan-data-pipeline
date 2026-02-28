import yaml
import pandas as pd
import sqlite3
from datetime import datetime
from config.settings import DB_PATH, DATA_QUALITY_METRICS_TABLE


def log_quality_check(conn, check_name, status, failed_rows):
    conn.execute(f"""
        INSERT INTO {DATA_QUALITY_METRICS_TABLE}
        (run_timestamp, check_name, check_status, failed_rows)
        VALUES (?, ?, ?, ?)
    """, (
        datetime.utcnow().isoformat(),
        check_name,
        status,
        failed_rows
    ))
    conn.commit()


def validate_processed_contract(df: pd.DataFrame, contract_path="./etl/contracts/trends_contract.yaml"):

    with open(contract_path, "r") as f:
        contract = yaml.safe_load(f)

    schema = contract["processed"]["schema"]

    with sqlite3.connect(DB_PATH) as conn:

        # Check columns exist
        missing_columns = [col for col in schema.keys() if col not in df.columns]
        failed = len(missing_columns)
        status = "PASS" if failed == 0 else "FAIL"
        log_quality_check(conn, "missing_columns_check", status, failed)

        if failed > 0:
            raise ValueError(f"Missing columns: {missing_columns}")

        # Check nullability
        null_failed = 0
        for col, rules in schema.items():
            if not rules["nullable"]:
                null_failed += df[col].isnull().sum()

        status = "PASS" if null_failed == 0 else "FAIL"
        log_quality_check(conn, "null_check", status, int(null_failed))

        if null_failed > 0:
            raise ValueError("Null values detected")

        # Check interest_score range
        range_failed = df[(df["interest_score"] < 0) |
                          (df["interest_score"] > 100)].shape[0]

        status = "PASS" if range_failed == 0 else "FAIL"
        log_quality_check(conn, "interest_score_range_check",
                          status, int(range_failed))

        if range_failed > 0:
            raise ValueError("interest_score out of allowed range")

        # Check grain uniqueness
        duplicate_failed = df.duplicated(
            subset=["date", "keyword"]).sum()

        status = "PASS" if duplicate_failed == 0 else "FAIL"
        log_quality_check(conn, "duplicate_grain_check",
                          status, int(duplicate_failed))

        if duplicate_failed > 0:
            raise ValueError("Duplicate grain detected")

    print("Processed contract validation passed.")