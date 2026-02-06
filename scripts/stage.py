import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import polars as pl
import polars_h3 as plh3
import argparse

from datetime import date
from tb_map_editor.data_files import DataModel, DataStage
from tb_map_editor.pipeline import DataPipeline


def sts_ids(year: int, start_year: int):
    hex_resolution = 10

    if year == start_year:
        from_year = year
        to_year = None
    elif year > start_year:
        from_year = year - 1
        to_year = year
    else:
        raise Exception("year should not be less than 2018")
    
    key = ["h3_cell", "tollbooth_name", "stretch_name"]
    def _split_dup_and_add_id(ldf_tb_sts_from: pl.LazyFrame, start_index: int) -> pl.LazyFrame:
        ldf_tb_sts_from = ldf_tb_sts_from.with_columns(
            pl.col("index").rank("ordinal").over(key, order_by=["index"]).alias("rank")
        )

        ldf_tb_sts_from_key_dup = ldf_tb_sts_from.filter(pl.col("rank") > 1)
        ldf_tb_sts_from = ldf_tb_sts_from.filter(pl.col("rank") == 1)
        ldf_tb_sts_from = pl.concat([ldf_tb_sts_from, ldf_tb_sts_from_key_dup])
        ldf_tb_sts_from = ldf_tb_sts_from.with_row_index(
            "tollbooth_id", start_index
        ).select(pl.exclude("rank", "h3_cell"))
        return ldf_tb_sts_from
        

    if to_year is None:
        data_model_from = DataModel(from_year, DataStage.stg)
        ldf_tb_sts_from = pl.scan_parquet(
            data_model_from.tb_sts.parquet
        )
        ldf_tb_sts_from = ldf_tb_sts_from.with_columns(
            h3_cell=plh3.latlng_to_cell("lat", "lng", hex_resolution)
        )
        ldf_all = _split_dup_and_add_id(ldf_tb_sts_from, start_index=1)
    else:
        data_model_from = DataModel(from_year, DataStage.prd)
        ldf_tb_sts_from = pl.scan_parquet(
            data_model_from.tb_sts.parquet,
        )
        data_model_to = DataModel(to_year, DataStage.stg)
        ldf_tb_sts_to = pl.scan_parquet(
            data_model_to.tb_sts.parquet, 
        )

        ldf_tb_sts_from = ldf_tb_sts_from.with_columns(
            h3_cell=plh3.latlng_to_cell("lat", "lng", hex_resolution)
        )
        ldf_tb_sts_to = ldf_tb_sts_to.with_columns(
            h3_cell=plh3.latlng_to_cell("lat", "lng", hex_resolution)
        )
        ldf_tb_sts_from_to_new = ldf_tb_sts_to.join(
            ldf_tb_sts_from,
            on=key,
            how="anti"
        )
        ldf_tb_sts_from_to_del = ldf_tb_sts_from.join(
            ldf_tb_sts_to,
            on=key,
            how="anti"
        )
        ldf_tb_sts_from_to_del = ldf_tb_sts_from_to_del.with_columns(
            pl.lit("closed").alias("status")
        ).select(pl.exclude("h3_cell"))

        ldf_tb_sts_to = ldf_tb_sts_to.join(
            ldf_tb_sts_from,
            on=key,
        ).select(["tollbooth_id"] + ldf_tb_sts_to.collect_schema().names())

        # locate duplicates over the key in two df
        ldf_tb_sts_from = ldf_tb_sts_from.with_columns(
            pl.col("index").rank("ordinal").over(key, order_by=["index"]).alias("rank")
        )
        ldf_tb_sts_to = ldf_tb_sts_to.with_columns(
            pl.col("index").rank("ordinal").over(key, order_by=["index"]).alias("rank")
        )
        ldf_tb_sts_to_dup = ldf_tb_sts_to.filter(pl.col("rank") > 1)
        ldf_tb_sts_to_dup = ldf_tb_sts_to_dup.join(
            ldf_tb_sts_from, on=key + ["rank"],
            how="anti"
        )
        ldf_tb_sts_to = ldf_tb_sts_to.join(
            ldf_tb_sts_to_dup, on=key + ["rank"],
            how="anti"
        )

        ldf_tb_sts_to_dup = ldf_tb_sts_to_dup.select(pl.exclude("rank", "tollbooth_id"))
        ldf_tb_sts_from_to_new = pl.concat([ldf_tb_sts_from_to_new, ldf_tb_sts_to_dup])
        ldf_tb_sts_from = ldf_tb_sts_from.select(pl.exclude("rank"))
        ldf_tb_sts_to = ldf_tb_sts_to.select(pl.exclude("rank", "h3_cell"))
        ##

        ldf_tb_sts_to = pl.concat([ldf_tb_sts_to, ldf_tb_sts_from_to_del])
        last_id = ldf_tb_sts_from.sort("tollbooth_id").last().select("tollbooth_id").collect().row(0)[0]
        ldf_tb_sts_from_to_new = _split_dup_and_add_id(ldf_tb_sts_from_to_new, start_index=last_id + 1)
        ldf_all = pl.concat([ldf_tb_sts_to, ldf_tb_sts_from_to_new])

    ldf_all.sink_parquet(DataModel(year, DataStage.prd).tb_sts.parquet)


def stg_to_prod(year:int, option_selected: str):
    models = ["tb_sts"]
    options = ["tb_sts"]

    catalogs = _opts_map(options, models)
    if option_selected == "tb_sts":
        sts_ids(year, start_year=2018)


def _opts_map(options, models):
    catalogs = {}
    for option, model in zip(options, models):
        catalogs[option] = model
    return catalogs


def pub_to_stg(year: int, option_selected: str, normalize: bool):
    pipeline = DataPipeline()
    
    models = ["tollbooths", "stretchs", "stretchs_toll", "roads"]
    options = ["tb", "stretch", "stretch_toll", "road"]
    catalogs = _opts_map(options, models)

    pipeline.simple_pub_stg(catalogs[option_selected], year, normalize)


def raw_to_stg(year: int, option_selected: str, normalize: bool):
    pipeline = DataPipeline()
    
    models = ["tb_imt", "tb_toll_imt"]
    options = ["tb_imt", "tb_toll_imt"]
    date_columns = None
    filter_exp = None

    if option_selected == "tb_imt":
        file_path = f"./tmp_data/plazas_{year}.csv"
        old_fields = [
            "ID_PLAZA", "NOMBRE", "SECCION", "SUBSECCION", "MODALIDAD",
            "FUNCIONAL", "CALIREPR", "ycoord", "xcoord"
        ]
    
    elif option_selected == "tb_toll_imt":
        file_path = f"./tmp_data/tarifas_imt_{year}.csv"
        old_fields = [
            "ID_PLAZA", "ID_PLAZA_E", "NOMBRE_SAL", "NOMBRE_ENT",
            "T_MOTO", "T_AUTO", "T_EJE_LIG", "T_AUTOBUS2",
            "T_AUTOBUS3", "T_AUTOBUS4", "T_CAMION2", "T_CAMION3",
            "T_CAMION4", "T_CAMION5", "T_CAMION6", "T_CAMION7",
            "T_CAMION8", "T_CAMION9", "T_EJE_PES"
        ]
        if year == 2025:
           date_format = "%Y-%m-%d %H:%M:%S"
        elif year in [2020, 2021, 2022, 2023, 2024]:
            date_format = "%Y-%m-%d"

        date_columns = {"FECHA_ACT": date_format}
        filter_exp = (pl.col("FECHA_ACT") < date(year + 1, 1, 1))
    
    catalogs = _opts_map(options, models)
    pipeline.simple_raw_stg(
        catalogs[option_selected], 
        year, 
        file_path, 
        old_fields, 
        date_columns=date_columns,
        filter_exp=filter_exp,
        normalize=normalize
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument(
        "--pub-to-stg", 
        help="generate parquet file", 
        required=False, type=str, 
        choices=["tb", "stretch", "stretch_toll", "road"]
    )
    parser.add_argument("--stg-to-prod", required=False, type=str)
    parser.add_argument("--raw-to-stg", required=False, type=str, choices=("tb_imt", "tb_toll_imt"))
    parser.add_argument("--normalize", required=False, action="store_true")

    args = parser.parse_args()
    if args.stg_to_prod:
        stg_to_prod(args.year, args.stg_to_prod)
    elif args.pub_to_stg:
        pub_to_stg(args.year, args.pub_to_stg, args.normalize)
    elif args.raw_to_stg:
        raw_to_stg(args.year, args.raw_to_stg, args.normalize)
