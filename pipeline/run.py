"""
CLI entrypoint for the tollbooth_etc_mx Prefect pipeline.

Usage:
    # Run a full flow
    uv run python pipeline/run.py --from-year 2024 --to-year 2026 --flow staging
    uv run python pipeline/run.py --from-year 2024 --to-year 2026 --flow reports
    uv run python pipeline/run.py --from-year 2024 --to-year 2026 --flow all

    # Resume a flow from a specific step (skips everything before it)
    uv run python pipeline/run.py --from-year 2025 --to-year 2026 --flow staging --from-step neighbours
    uv run python pipeline/run.py --from-year 2024 --to-year 2026 --flow reports --from-step manage_data

    # Run individual tasks directly (no flow)
    uv run python pipeline/run.py --from-year 2025 --to-year 2025 --tasks pub_tb dv_cleaner
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW", "ignore")

from pipeline.flows.staging_flow import staging_flow, STAGING_TASKS, _STEP_TO_GROUP as _STAGING_STEPS
from pipeline.flows.report_flow import report_flow, REPORT_TASKS, _STEP_TO_GROUP as _REPORT_STEPS

_ALL_FLOW_STEPS = list(_STAGING_STEPS) + list(_REPORT_STEPS)
_ALL_TASK_NAMES = list(STAGING_TASKS) + list(REPORT_TASKS)


def main():
    parser = argparse.ArgumentParser(
        description="Run tollbooth_etc_mx pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "flows:\n"
            "  staging — staging_flow (per year, parallel DAG)\n"
            "  reports — report_flow (cross-year)\n"
            "  all     — staging then reports\n\n"
            f"staging steps (for --from-step):\n"
            f"  {', '.join(_STAGING_STEPS)}\n\n"
            f"report steps (for --from-step):\n"
            f"  {', '.join(_REPORT_STEPS)}\n\n"
            f"individual tasks (for --tasks):\n"
            f"  {', '.join(_ALL_TASK_NAMES)}"
        ),
    )
    parser.add_argument("--from-year", required=True, type=int)
    parser.add_argument("--to-year", required=True, type=int)
    parser.add_argument(
        "--flow",
        choices=("staging", "reports", "all"),
        default=None,
        help="run a Prefect flow (staging | reports | all)",
    )
    parser.add_argument(
        "--from-step",
        choices=_ALL_FLOW_STEPS,
        default=None,
        metavar="STEP",
        help="start the flow from this step, skipping everything before it",
    )
    parser.add_argument(
        "--tasks",
        nargs="+",
        choices=_ALL_TASK_NAMES,
        default=None,
        metavar="TASK",
        help="run individual tasks directly, without a flow",
    )
    args = parser.parse_args()

    if not args.flow and not args.tasks:
        parser.error("at least one of --flow or --tasks is required")
    if args.from_step and not args.flow:
        parser.error("--from-step requires --flow")

    if args.flow in ("staging", "all"):
        for year in range(args.from_year, args.to_year + 1):
            staging_flow(year, from_step=args.from_step)

    if args.flow in ("reports", "all"):
        report_flow(args.from_year, args.to_year, from_step=args.from_step)

    if args.tasks:
        staging = [t for t in args.tasks if t in STAGING_TASKS]
        reports = [t for t in args.tasks if t in REPORT_TASKS]
        for year in range(args.from_year, args.to_year + 1):
            for name in staging:
                print(f"[{year}] {name}")
                STAGING_TASKS[name](year)
        for name in reports:
            print(f"[{args.from_year}-{args.to_year}] {name}")
            REPORT_TASKS[name](args.from_year, args.to_year)


if __name__ == "__main__":
    main()
