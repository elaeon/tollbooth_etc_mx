from typing import Any, Callable
from prefect import flow

from tb_map_editor.data_files import DataModel, DataStage

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

STAGING_TASKS: dict[str, Callable[[int], Any]] = {
    DataModel.tb_sts_no_id.name:          lambda year: task_dv_cleaner(year),
    DataModel.tollbooth.name:              lambda year: task_pub_to_stg(DataModel(year, DataStage.pub).tollbooth,       DataModel(year, DataStage.stg).tollbooth,       True),
    DataModel.stretch.name:         lambda year: task_pub_to_stg(DataModel(year, DataStage.pub).stretch,         DataModel(year, DataStage.stg).stretch,         True),
    DataModel.road.name:            lambda year: task_pub_to_stg(DataModel(year, DataStage.pub).road,            DataModel(year, DataStage.stg).road,            True),
    DataModel.stretch_toll.name:    lambda year: task_pub_to_stg(DataModel(year, DataStage.pub).stretch_toll,    DataModel(year, DataStage.stg).stretch_toll,    True),
    DataModel.tb_stretch_id.name:   lambda year: task_pub_to_stg(DataModel(year, DataStage.pub).tb_stretch_id,   DataModel(year, DataStage.stg).tb_stretch_id,   False),
    DataModel.tb_imt.name:          lambda year: task_raw_to_stg(DataModel(year, DataStage.pub).tb_imt,          DataModel(year, DataStage.stg).tb_imt,          True),
    DataModel.tb_toll_imt.name:     lambda year: task_raw_to_stg(DataModel(year, DataStage.pub).tb_toll_imt,     DataModel(year, DataStage.stg).tb_toll_imt,     True),
    DataModel.inflation.name:       lambda year: task_raw_to_stg(DataModel(year, DataStage.pub).inflation,       DataModel(year, DataStage.stg).inflation,       False),
    DataModel.manager_revenue.name: lambda year: task_raw_to_stg(DataModel(year, DataStage.pub).manager_revenue, DataModel(year, DataStage.stg).manager_revenue, True),
    DataModel.tb_neighbour.name:          lambda year: task_tollbooth_neighbours(year),
    DataModel.map_tb_id.name:           lambda year: task_map_tb_id(year),
    DataModel.tb_sts.name:              lambda year: task_tb_sts(year),
    DataModel.tb_imt_stretch_id.name:      lambda year: task_tb_imt_stretch_id_rel(year),
    DataModel.tb_sts_stretch_id.name:      lambda year: task_tb_stretch_id_sts(year, year),
    DataModel.osm_tb_distance.name:             lambda year: task_pub_to_stg(DataModel(year, DataStage.pub).osm_tb_distance, DataModel(year, DataStage.stg).osm_tb_distance, False),
}

_STEP_TO_GROUP: dict[str, int] = {
    step: i
    for i, group in enumerate([
        [DataModel.tb_sts_no_id.name],
        [
            DataModel.tollbooth.name, DataModel.stretch.name, DataModel.road.name, DataModel.stretch_toll.name, DataModel.tb_stretch_id.name,
            DataModel.tb_imt.name, DataModel.tb_toll_imt.name, DataModel.inflation.name, DataModel.manager_revenue.name
        ],
        [DataModel.tb_neighbour.name],
        [DataModel.map_tb_id.name, DataModel.tb_sts.name],
        [DataModel.tb_imt_stretch_id.name, DataModel.tb_sts_stretch_id.name],
        [DataModel.osm_tb_distance.name],
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

    # Group 1 (parallel): all pub/raw sources
    g1 = []
    if start <= 1:
        g1 = [
            task_pub_to_stg.submit(pub.tollbooth, stg.tollbooth, True),
            task_pub_to_stg.submit(pub.stretch, stg.stretch, True),
            task_pub_to_stg.submit(pub.road, stg.road, True),
            task_pub_to_stg.submit(pub.stretch_toll, stg.stretch_toll, True),
            task_pub_to_stg.submit(pub.tb_stretch_id, stg.tb_stretch_id, False),
            task_raw_to_stg.submit(pub.tb_imt, stg.tb_imt, True),
            task_raw_to_stg.submit(pub.tb_toll_imt, stg.tb_toll_imt, True),
            task_raw_to_stg.submit(pub.inflation, stg.inflation, False),
            task_raw_to_stg.submit(pub.manager_revenue, stg.manager_revenue, True),
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
        task_pub_to_stg.submit(pub.osm_tb_distance, stg.osm_tb_distance, False, wait_for=g4) # type: ignore
