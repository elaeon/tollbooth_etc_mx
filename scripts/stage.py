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
    historic_id_name = "historic_id"

    def concat_data():
        df_dict = {}
        for year in range(from_year, to_year + 1):
            data_model = DataModel(year)
            df_dict[year] = pl.read_parquet(
                data_model.tollbooths_sts.parquet, columns=["tollbooth_name", "way", "lat", "lon"]
            ).with_columns(
                pl.lit(year).alias("year")
            )
        df_tbsts = pl.concat(df_dict.values())
        return df_tbsts

    df_tbsts = concat_data()
    hex_resolution = 10
    df_tbsts = df_tbsts.with_columns(
        h3_cell=plh3.latlng_to_cell("lat", "lon", hex_resolution)
    )
    df_h = df_tbsts.select(
        "h3_cell", "tollbooth_name"
    ).group_by("h3_cell", "tollbooth_name").len(name="tb_len").select("h3_cell").group_by("h3_cell").len().filter(pl.col("len") > 1)
    df_exceptions = df_h.join(df_tbsts, on="h3_cell").select("h3_cell", "tollbooth_name", "way", "year").unique().with_columns(pl.lit("bad").alias("status"))

    df_tbsts_id = df_tbsts.select(
        "h3_cell", "tollbooth_name", "way"
    ).unique(maintain_order=True).with_columns(pl.lit("ok").alias("status"))

    df_tbsts_id = df_tbsts_id.join(df_exceptions, on=["h3_cell", "tollbooth_name", "way"], how="left").filter(pl.col("status_right").is_null())
    df_tbsts_id = df_tbsts_id.with_row_index(historic_id_name, 1).with_columns(pl.lit(from_year).alias("ref_year"))
    
    df_exceptions = df_exceptions.group_by("h3_cell", "tollbooth_name", "way").agg(pl.min("year"))

    df_last_row = df_tbsts_id.tail(1)
    start_index = df_last_row.row(0, named=True)[historic_id_name]

    df_exceptions = df_exceptions.sort("h3_cell", "year")
    df_except_index = df_exceptions.select("h3_cell", "way").unique(maintain_order=True).with_row_index(historic_id_name, start_index + 1)
    df_exceptions = df_exceptions.join(df_except_index, on=["h3_cell", "way"])
    df_exceptions = df_exceptions.rename({"year": "ref_year"}).select(historic_id_name, "h3_cell", "tollbooth_name", "way", "ref_year")
    
    df_tbsts_id = pl.concat([
        df_tbsts_id.select(historic_id_name, "h3_cell", "tollbooth_name", "way", "ref_year"), 
        df_exceptions.sort("h3_cell")
    ])
    df_tbsts_id.with_row_index("tollboothsts_id", 1).write_parquet(DataModel(to_year).tbsts_id.parquet)


def _catalogs(options, models):
    catalogs = {}
    for option, model in zip(options, models):
        catalogs[option] = model
    return catalogs


def pub_to_stg(year: int, option_selected: str):
    pipeline = DataPipeline()
    
    models = ["tollbooths", "stretchs", "stretchs_toll", "roads"]
    options = ["tb", "stretch", "stretch_toll", "road"]
    catalogs = _catalogs(options, models)

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
    
    catalogs = _catalogs(options, models)
    pipeline.simple_raw_stg(
        catalogs[option_selected], 
        year, file_path, 
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
