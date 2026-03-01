import argparse

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
  