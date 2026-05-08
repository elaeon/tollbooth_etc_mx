from collections.abc import Callable
from typing import Any

from prefect import flow

from src.data_files import DataModel, DataStage
from src.pipeline.tasks.cluster_tasks import (
    task_map_tb_id,
    task_tb_imt_stretch_id_rel,
    task_tb_stretch_id_sts,
    task_tollbooth_neighbours,
)
from src.pipeline.tasks.stage_tasks import (
    task_dv_cleaner,
    task_pub_to_stg,
    task_raw_to_stg,
    task_tb_sts,
)


def staging_tasks(year: int) -> dict[str, Callable[[], Any]]:
    pub = DataModel(year, DataStage.pub)
    stg = DataModel(year, DataStage.stg)
    return {
        DataModel.tb_sts_no_id.name:      lambda: task_dv_cleaner(year),
        DataModel.tollbooth.name:         lambda: task_pub_to_stg(pub.tollbooth,       stg.tollbooth,       True),
        DataModel.stretch.name:           lambda: task_pub_to_stg(pub.stretch,         stg.stretch,         True),
        DataModel.road.name:              lambda: task_pub_to_stg(pub.road,            stg.road,            True),
        DataModel.stretch_toll.name:      lambda: task_pub_to_stg(pub.stretch_toll,    stg.stretch_toll,    True),
        DataModel.tb_stretch_id.name:     lambda: task_pub_to_stg(pub.tb_stretch_id,   stg.tb_stretch_id,   False),
        DataModel.tb_imt.name:            lambda: task_raw_to_stg(pub.tb_imt,          stg.tb_imt,          True),
        DataModel.tb_toll_imt.name:       lambda: task_raw_to_stg(pub.tb_toll_imt,     stg.tb_toll_imt,     True),
        DataModel.inflation.name:         lambda: task_raw_to_stg(pub.inflation,       stg.inflation,       False),
        DataModel.manager_revenue.name:   lambda: task_raw_to_stg(pub.manager_revenue, stg.manager_revenue, True),
        DataModel.tb_neighbour.name:      lambda: task_tollbooth_neighbours(year),
        DataModel.map_tb_id.name:         lambda: task_map_tb_id(year),
        DataModel.tb_sts.name:            lambda: task_tb_sts(year),
        DataModel.tb_imt_stretch_id.name: lambda: task_tb_imt_stretch_id_rel(year),
        DataModel.tb_sts_stretch_id.name: lambda: task_tb_stretch_id_sts(year, year),
        DataModel.osm_tb_distance.name:   lambda: task_pub_to_stg(pub.osm_tb_distance, stg.osm_tb_distance, False),
    }

STAGING_TASK_NAMES: list[str] = list(staging_tasks(0))

_STEP_TO_GROUP: dict[str, int] = {
    step: i
    for i, group in enumerate([
        [DataModel.tb_sts_no_id.name],
        [DataModel.osm_tb_distance.name, DataModel.inflation.name],
        [
            DataModel.tollbooth.name, DataModel.stretch.name, DataModel.road.name, DataModel.stretch_toll.name, DataModel.tb_stretch_id.name,
            DataModel.tb_imt.name, DataModel.tb_toll_imt.name, DataModel.manager_revenue.name
        ],
        [DataModel.tb_neighbour.name],
        [DataModel.map_tb_id.name, DataModel.tb_sts.name],
        [DataModel.tb_imt_stretch_id.name, DataModel.tb_sts_stretch_id.name],
    ])
    for step in group
}


@flow(name="stage-year")
def staging_flow(year: int, from_step: str | None = None):
    start = _STEP_TO_GROUP.get(from_step, 0) if from_step else 0
    pub = DataModel(year, DataStage.pub)
    stg = DataModel(year, DataStage.stg)

    # Group 0: dv_cleaner is the most expensive time calc extraction.
    g0 = []
    if start <= 0:
        g0 = [
            task_dv_cleaner.submit(year),
        ]

    g00 = []
    if start <= 1:
        g00 = [
            task_pub_to_stg.submit(pub.osm_tb_distance, stg.osm_tb_distance, False),
            task_raw_to_stg.submit(pub.inflation, stg.inflation, False),
        ]
    # Group 1 (parallel): all pub/raw sources
    g1 = []
    if start <= 2:
        g1 = [
            task_pub_to_stg.submit(pub.tollbooth, stg.tollbooth, True),
            task_pub_to_stg.submit(pub.stretch, stg.stretch, True),
            task_pub_to_stg.submit(pub.road, stg.road, True),
            task_pub_to_stg.submit(pub.stretch_toll, stg.stretch_toll, True),
            task_pub_to_stg.submit(pub.tb_stretch_id, stg.tb_stretch_id, False),
            task_raw_to_stg.submit(pub.tb_imt, stg.tb_imt, True),
            task_raw_to_stg.submit(pub.tb_toll_imt, stg.tb_toll_imt, True),
            task_raw_to_stg.submit(pub.manager_revenue, stg.manager_revenue, True),
        ]

    # Group 2: neighbours
    g2 = []
    if start <= 3:
        g2 = [task_tollbooth_neighbours.submit(year, wait_for=g0+g00+g1)] # type: ignore

    # Group 3 (parallel): map_tb_id + tb_sts
    g3 = []
    if start <= 4:
        g3 = [
            task_map_tb_id.submit(year, wait_for=g2), # type: ignore
            task_tb_sts.submit(year, wait_for=g2), # type: ignore
        ]

    # Group 4 (parallel): imt_stretch_id + sts_stretch_id
    g4 = []
    if start <= 5:
        g4 = [
            task_tb_imt_stretch_id_rel.submit(year, wait_for=g3), # type: ignore
            task_tb_stretch_id_sts.submit(year, year, wait_for=g3), # type: ignore
        ]
