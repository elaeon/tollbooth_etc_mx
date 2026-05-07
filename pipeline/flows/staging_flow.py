from typing import Any, Callable
from prefect import flow

from pipeline.tasks.stage_tasks import (
    task_pub_to_stg,
    task_raw_to_stg,
    task_dv_cleaner,
    task_tb_sts,
)
from pipeline.tasks.cluster_tasks import (
    task_tollbooth_neighbours,
    task_map_tb_id,
    task_tb_imt_stretch_id_rel,
    task_tb_stretch_id_sts,
)

# Single source of truth for step names and their standalone callables.
# Imported by run.py for --tasks dispatch.
STAGING_TASKS: dict[str, Callable[[int], Any]] = {
    "dv_cleaner":          lambda year: task_dv_cleaner(year),
    "pub_tb":              lambda year: task_pub_to_stg(year, "tollbooth", True),
    "pub_stretch":         lambda year: task_pub_to_stg(year, "stretch", True),
    "pub_road":            lambda year: task_pub_to_stg(year, "road", True),
    "pub_stretch_toll":    lambda year: task_pub_to_stg(year, "stretch_toll", True),
    "pub_tb_stretch_id":   lambda year: task_pub_to_stg(year, "tb_stretch_id", False),
    "raw_tb_imt":          lambda year: task_raw_to_stg(year, "tb_imt", True),
    "raw_tb_toll_imt":     lambda year: task_raw_to_stg(year, "tb_toll_imt", True),
    "raw_inflation":       lambda year: task_raw_to_stg(year, "inflation", False),
    "raw_manager_revenue": lambda year: task_raw_to_stg(year, "manager_revenue", True),
    "neighbours":          lambda year: task_tollbooth_neighbours(year),
    "map_tb_id":           lambda year: task_map_tb_id(year),
    "tb_sts":              lambda year: task_tb_sts(year),
    "imt_stretch_id":      lambda year: task_tb_imt_stretch_id_rel(year),
    "sts_stretch_id":      lambda year: task_tb_stretch_id_sts(year, year),
    "pub_osm":             lambda year: task_pub_to_stg(year, "osm_tb_distance", False),
}

_STEP_TO_GROUP: dict[str, int] = {
    step: i
    for i, group in enumerate([
        ["dv_cleaner"],
        [
            "pub_tb", "pub_stretch", "pub_road", "pub_stretch_toll", "pub_tb_stretch_id",
            "raw_tb_imt", "raw_tb_toll_imt", "raw_inflation", "raw_manager_revenue"
        ],
        ["neighbours"],
        ["map_tb_id", "tb_sts"],
        ["imt_stretch_id", "sts_stretch_id"],
        ["pub_osm"],
    ])
    for step in group
}


@flow(name="stage-year")
def staging_flow(year: int, from_step: str | None = None):
    start = _STEP_TO_GROUP.get(from_step, 0) if from_step else 0

    # Group 0 dv_cleaner is the most expensive time calc extraction.
    g0 = []
    if start <= 0:
        g0 = [
            task_dv_cleaner.submit(year),
        ]

    # Group 1 (parallel): all pub/raw sources
    g1 = []
    if start <= 1:
        g1 = [
            task_pub_to_stg.submit(year, "tollbooth", True),
            task_pub_to_stg.submit(year, "stretch", True),
            task_pub_to_stg.submit(year, "road", True),
            task_pub_to_stg.submit(year, "stretch_toll", True),
            task_pub_to_stg.submit(year, "tb_stretch_id", False),
            task_raw_to_stg.submit(year, "tb_imt", True),
            task_raw_to_stg.submit(year, "tb_toll_imt", True),
            task_raw_to_stg.submit(year, "inflation", False),
            task_raw_to_stg.submit(year, "manager_revenue", True),
        ]
    
    # Group 2: neighbours
    g2 = []
    if start <= 2:
        g2 = [task_tollbooth_neighbours.submit(year, wait_for=g0+g1)] # type: ignore

    # Group 3 (parallel): map_tb_id + tb_sts
    g3 = []
    if start <= 3:
        g3 = [
            task_map_tb_id.submit(year, wait_for=g2), # type: ignore
            task_tb_sts.submit(year, wait_for=g2), # type: ignore
        ]

    # Group 4 (parallel): imt_stretch_id + sts_stretch_id
    g4 = []
    if start <= 4:
        g4 = [
            task_tb_imt_stretch_id_rel.submit(year, wait_for=g3), # type: ignore
            task_tb_stretch_id_sts.submit(year, year, wait_for=g3), # type: ignore
        ]

    # Group 5: pub_osm
    if start <= 5:
        task_pub_to_stg.submit(year, "osm_tb_distance", False, wait_for=g4) # type: ignore
