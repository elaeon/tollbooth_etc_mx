from prefect import task

from tb_map_editor.data_files import PathModel
import scripts.stage as stage
import scripts.dv_cleaner as dv_cleaner


@task(name="pub-to-stg")
def task_pub_to_stg(pub: PathModel, stg: PathModel, normalize: bool = False):
    return stage.pub_to_stg(pub, stg, normalize)


@task(name="raw-to-stg")
def task_raw_to_stg(pub: PathModel, stg: PathModel, normalize: bool = False):
    return stage.raw_to_stg(pub, stg, normalize)


@task(name="dv-cleaner")
def task_dv_cleaner(year: int):
    pages = {
        2019: {"from_page": 51, "to_page": 200},
        2020: {"from_page": 51, "to_page": 200},
        2021: {"from_page": 51, "to_page": 200},
        2022: {"from_page": 53, "to_page": 200},
        2023: {"from_page": 54, "to_page": 200},
        2024: {"from_page": 55, "to_page": 200},
        2025: {"from_page": 55, "to_page": 200}
    }
    return dv_cleaner.main(year, pages[year]["from_page"], pages[year]["to_page"])


@task(name="tb-sts")
def task_tb_sts(year: int):
    return stage.sts_ids(year, start_year=2018)
