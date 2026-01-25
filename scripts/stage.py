import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import polars as pl
import polars_h3 as plh3
import argparse

from datetime import date
from tb_map_editor.data_files import DataModel, DataStage
from tb_map_editor.pipeline import DataPipeline


def sts_catalog():
    from_year = 2018
    to_year = 2024

    def concat_data():
        df_dict = {}
        for year in range(from_year, to_year + 1):
            data_model = DataModel(year, DataStage.stg)
            df_dict[year] = pl.read_parquet(
                data_model.tb_sts.parquet, 
                columns=["index", "tollbooth_name", "way", "lat", "lng", "info_year"]
            )
        df_tbsts = pl.concat(df_dict.values())
        return df_tbsts

    df_tbsts = concat_data()
    hex_resolution = 10
    df_tbsts = df_tbsts.with_columns(
        h3_cell=plh3.latlng_to_cell("lat", "lng", hex_resolution)
    )
    df_tbsts = df_tbsts.group_by("h3_cell", "tollbooth_name", "way").agg(pl.min("info_year"))
    df_tbsts = df_tbsts.sort("info_year", "tollbooth_name", "way")
    df_tbsts.with_row_index(
        "tollbooth_id", 1
    ).select("tollbooth_id", "tollbooth_name", "way", "info_year").write_parquet(DataModel(to_year, DataStage.stg).tb_sts_id.parquet)


def _opts_map(options, models):
    catalogs = {}
    for option, model in zip(options, models):
        catalogs[option] = model
    return catalogs


def pub_to_stg(year: int, option_selected: str):
    pipeline = DataPipeline()
    
    models = ["tollbooths", "stretchs", "stretchs_toll", "roads"]
    options = ["tb", "stretch", "stretch_toll", "road"]
    catalogs = _opts_map(options, models)

    pipeline.simple_pub_stg(catalogs[option_selected], year)


def raw_to_stg(year: int, option_selected: str):
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
        filter_exp=filter_exp
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=False, type=int)
    parser.add_argument("--pub-to-stg", help="generate parquet file", required=False, type=str)
    parser.add_argument("--sts-catalog", help="generate tollbooth sts id catalog", required=False, action="store_true")
    parser.add_argument("--raw-to-stg", required=False, type=str)

    args = parser.parse_args()
    if args.sts_catalog:
        sts_catalog()
    elif args.pub_to_stg:
        pub_to_stg(args.year, args.pub_to_stg)
    elif args.raw_to_stg:
        raw_to_stg(args.year, args.raw_to_stg)
