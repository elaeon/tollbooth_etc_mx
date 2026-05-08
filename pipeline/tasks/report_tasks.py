import os
from prefect import task

import scripts.reports as reports


# reports.py uses output_filepath only in __main__; patch it before calling.
_OUTPUT_DIR = "reports/"


def _ensure_output_dir():
    os.makedirs(_OUTPUT_DIR, exist_ok=True)


@task(name="growth-rate-report")
def task_growth_rate_report(
    from_year: int,
    to_year: int,
    vehicle_type: str,
    to_year_sts: int,
):
    _ensure_output_dir()
    reports.output_filepath = _OUTPUT_DIR
    return reports.growth_rate_report(from_year, to_year, vehicle_type, to_year_sts)


@task(name="toll-update-date-report")
def task_toll_update_date_report(from_year: int, to_year: int):
    _ensure_output_dir()
    reports.output_filepath = _OUTPUT_DIR
    return reports.toll_update_date_report(from_year, to_year)


@task(name="manage-data")
def task_manage_data(from_year: int, to_year: int):
    _ensure_output_dir()
    reports.output_filepath = _OUTPUT_DIR
    return reports.manage_data(from_year, to_year)


@task(name="revenue")
def task_revenue(from_year: int, to_year: int):
    _ensure_output_dir()
    return reports.revenue(from_year, to_year)
