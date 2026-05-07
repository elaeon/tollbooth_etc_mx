from prefect import flow

from pipeline.tasks.report_tasks import (
    task_growth_rate_report,
    task_toll_update_date_report,
    task_manage_data,
    task_revenue,
)

_VEHICLE_TYPES = ["bike", "car", "bus", "ltruck", "htruck", "utruck"]


@flow(name="reports")
def report_flow(from_year: int, to_year: int, to_year_sts: int | None = None):
    if to_year_sts is None:
        to_year_sts = to_year

    # growth_rate_report for each vehicle type (parallel), then manage_data
    growth_futures = [
        task_growth_rate_report.submit(from_year, to_year, vt, to_year_sts)
        for vt in _VEHICLE_TYPES
    ]

    # manage_data depends on growth_rate CSVs being written
    manage = task_manage_data.submit(from_year, to_year, wait_for=growth_futures)

    # toll_update_date and revenue run in parallel alongside manage_data
    toll_date = task_toll_update_date_report.submit(from_year, to_year)
    rev = task_revenue.submit(from_year, to_year)

    return manage, toll_date, rev
