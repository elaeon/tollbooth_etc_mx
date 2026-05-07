from prefect import task

import scripts.tollbooth_cluster as tollbooth_cluster
import scripts.join_tollbooths as join_tollbooths


@task(name="tollbooth-neighbours")
def task_tollbooth_neighbours(year: int):
    return tollbooth_cluster.tollbooth_neighbours(year)


@task(name="map-tb-id")
def task_map_tb_id(year: int):
    return join_tollbooths.map_tb_id(year)


@task(name="tb-imt-stretch-id-rel")
def task_tb_imt_stretch_id_rel(year: int):
    return join_tollbooths.tb_imt_stretch_id_rel(year)


@task(name="tb-stretch-id-sts")
def task_tb_stretch_id_sts(base_year: int, move_year: int):
    return join_tollbooths.tb_stretch_id_sts(base_year, move_year)
