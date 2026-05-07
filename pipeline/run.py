"""
CLI entrypoint for the tollbooth_etc_mx Prefect pipeline.

Usage:
    uv run python pipeline/run.py --from-year 2024 --to-year 2026 --step stage
    uv run python pipeline/run.py --from-year 2024 --to-year 2026 --step reports
    uv run python pipeline/run.py --from-year 2024 --to-year 2026 --step all
"""
import argparse
import os
import sys

# Ensure the project root is on sys.path when running as `uv run python pipeline/run.py`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.flows.staging_flow import staging_flow
from pipeline.flows.report_flow import report_flow


def run_stage(from_year: int, to_year: int):
    for year in range(from_year, to_year + 1):
        staging_flow(year)


def run_reports(from_year: int, to_year: int):
    report_flow(from_year, to_year)


def main():
    parser = argparse.ArgumentParser(
        description="Run tollbooth_etc_mx Prefect pipeline"
    )
    parser.add_argument("--from-year", required=True, type=int)
    parser.add_argument("--to-year", required=True, type=int)
    parser.add_argument(
        "--step",
        required=True,
        choices=("stage", "reports", "all"),
        help="Pipeline step to run",
    )
    args = parser.parse_args()

    if args.step == "stage":
        run_stage(args.from_year, args.to_year)
    elif args.step == "reports":
        run_reports(args.from_year, args.to_year)
    elif args.step == "all":
        run_stage(args.from_year, args.to_year)
        run_reports(args.from_year, args.to_year)


if __name__ == "__main__":
    main()
