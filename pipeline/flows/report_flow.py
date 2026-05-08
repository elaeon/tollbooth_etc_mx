from typing import Any, Callable
from prefect import flow

from pipeline.tasks.report_tasks import (
    task_growth_rate_report,
    task_toll_update_date_report,
    task_manage_data,
    task_revenue,
)

_VEHICLE_TYPES = ["bike", "car", "bus", "ltruck", "htruck", "utruck"]

# Single source of truth for report step names and their standalone callables.
# Imported by run.py for --tasks dispatch.
REPORT_TASKS: dict[str, Callable[[int, int], Any]] = {
    "growth_rate":      lambda f, t: [task_growth_rate_report(f, t, vt, t) for vt in _VEHICLE_TYPES],
    "toll_update_date": lambda f, t: task_toll_update_date_report(f, t),
    "manage_data":      lambda f, t: task_manage_data(f, t),
    "revenue":          lambda f, t: task_revenue(f, t),
}

_STEP_TO_GROUP: dict[str, int] = {
    step: i
    for i, group in enumerate([
        ["growth_rate"],
        ["manage_data"],
    ])
    for step in group
}


@flow(name="reports")
def report_flow(from_year: int, to_year: int, to_year_sts: int | None = None, from_step: str | None = None):
    if to_year_sts is None:
        to_year_sts = to_year

    start = _STEP_TO_GROUP.get(from_step, 0) if from_step else 0

    # Group 0 (parallel): growth_rate (per vehicle type), toll_update_date, revenue
    growth_futures = []
    if start <= 0:
        growth_futures = [
            task_growth_rate_report.submit(from_year, to_year, vt, to_year_sts)
            for vt in _VEHICLE_TYPES
        ]
        task_toll_update_date_report.submit(from_year, to_year)
        task_revenue.submit(from_year, to_year)

    # Group 1: manage_data (depends on growth_rate CSVs)
    if start <= 1:
        task_manage_data.submit(from_year, to_year, wait_for=growth_futures) # type: ignore
