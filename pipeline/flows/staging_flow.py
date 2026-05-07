"""
Staging flow: orchestrates per-year ETL from pub/raw sources to staging Parquet.

NOTE: scripts/stage.py pub_to_stg and raw_to_stg return LazyFrames but do not
sink to Parquet themselves (the sink calls are commented out in the scripts).
The tasks here call those functions; if sinking is needed it must be added to
the underlying scripts or done explicitly after the task returns.
"""
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


@flow(name="stage-year")
def staging_flow(year: int):
    # Group 1 (parallel): pub sources + raw sources + dv_cleaner
    pub_tb = task_pub_to_stg.submit(year, "tollbooth", True)
    pub_stretch = task_pub_to_stg.submit(year, "stretch", True)
    pub_road = task_pub_to_stg.submit(year, "road", True)
    pub_stretch_toll = task_pub_to_stg.submit(year, "stretch_toll", True)
    pub_tb_stretch_id = task_pub_to_stg.submit(year, "tb_stretch_id", False)

    raw_tb_imt = task_raw_to_stg.submit(year, "tb_imt", True)
    raw_tb_toll_imt = task_raw_to_stg.submit(year, "tb_toll_imt", True)
    raw_inflation = task_raw_to_stg.submit(year, "inflation", False)
    raw_manager_revenue = task_raw_to_stg.submit(year, "manager_revenue", True)

    #dv = task_dv_cleaner.submit(year, wait_for=[pub_tb, raw_tb_imt])

    # Group 2: tollbooth neighbours (needs tb, tb_imt, dv_cleaner output)
    #neighbours = task_tollbooth_neighbours.submit(year, wait_for=[dv, pub_tb, raw_tb_imt])

    # Group 3 (parallel): map_tb_id and stg_to_prod both depend on neighbours
    #map_id = task_map_tb_id.submit(year, wait_for=[neighbours])
    #tb_sts = task_tb_sts.submit(year, wait_for=[dv, neighbours])

    # Group 4 (parallel): imt_stretch_id_rel and tb_stretch_id_sts depend on group 3
    #imt_rel = task_tb_imt_stretch_id_rel.submit(year, wait_for=[map_id])
    #sts_rel = task_tb_stretch_id_sts.submit(year, year, wait_for=[map_id, tb_sts])

    # Group 5: osm_tb_distance (manual data already present; this just stages it)
    #osm = task_pub_to_stg.submit(year, "osm_tb_distance", False, wait_for=[imt_rel, sts_rel])

    #return osm
