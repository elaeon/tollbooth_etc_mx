import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import polars as pl
import polars_h3 as plh3
import argparse

from tb_map_editor.schemas import tollbooth_schema
from tb_map_editor.data_files import DataPathSchema


def join_tb_tbsts(year:int):
    hex_resolution = 10
    prev_year = year - 1
    data_path_prev_year = DataPathSchema(prev_year)
    df_tbsts = pl.read_csv(
        data_path_prev_year.tollbooths_sts_catalog.path , 
        schema=data_path_prev_year.tollbooths_sts_catalog.schema
    )

    df_tbsts = df_tbsts.with_columns(
        pl.col("coords").str.split_exact(",", 1)
    ).unnest("coords").rename({"field_0": "lat", "field_1": "lon"})
    df_tbsts = df_tbsts.with_columns(
        pl.col("lat").cast(pl.Float32),
        pl.col("lon").cast(pl.Float32)
    )
    df_tbsts = df_tbsts.with_columns(
        h3_cell=plh3.latlng_to_cell("lat", "lon", hex_resolution)
    )

    data_path = DataPathSchema(year)
    df_tb = pl.read_csv(
        data_path.tollbooths_catalog.path, 
        schema=data_path.tollbooths_catalog.schema
    ).with_columns(
        pl.col("coords").str.split_exact(",", 1)
    ).unnest("coords").rename({"field_0": "lat", "field_1": "lon"})
    df_tb = df_tb.with_columns(
        pl.col("lat").str.strip_chars().cast(pl.Float32),
        pl.col("lon").str.strip_chars().cast(pl.Float32)
    )
    df_tb = df_tb.with_columns(
        h3_cell=plh3.latlng_to_cell("lat", "lon", hex_resolution)
    )
    
    df_tb_tbsts = df_tb.join(df_tbsts, on="h3_cell", how="left").select(
        "tollbooth_id", "tollboothsts_id"
    )

    df_tb_tbsts.write_csv("./data/tables/tb_tbsts.csv")


def check_imt_tb(year: int):
    hex_resolution = 9
    df_plazas = pl.read_csv("./data/tables/tmp_data/plazas.csv", infer_schema=False)

    df_plazas = df_plazas.with_columns(
        pl.col("xcoord").cast(pl.Float32).alias("lon"),
        pl.col("ycoord").cast(pl.Float32).alias("lat")
    ).rename({"NOMBRE": "name", "ID_PLAZA": "id"})
    df_plazas = df_plazas.with_columns(
        h3_cell=plh3.latlng_to_cell("lat", "lon", hex_resolution)
    )

    data_path = DataPathSchema(year)
    df_tb = pl.read_csv(
        data_path.tollbooths_catalog.path, 
        schema=data_path.tollbooths_catalog.schema
    )
    df_tb_catalog = df_tb.with_columns(
        pl.col("coords").str.split_exact(",", 1)
    ).unnest("coords").rename({"field_0": "lat", "field_1": "lon"})
    df_tb_catalog = df_tb_catalog.with_columns(
        pl.col("lat").str.strip_chars().cast(pl.Float32),
        pl.col("lon").str.strip_chars().cast(pl.Float32)
    )
    df_tb_catalog = df_tb_catalog.with_columns(
        h3_cell=plh3.latlng_to_cell("lat", "lon", hex_resolution)
    )
    df_plazas_tb_catalog = df_plazas.join(df_tb_catalog, on="h3_cell", how="left")
    df_plazas_tb_catalog.filter(
        pl.col("tollbooth_id").is_null()
    ).select("id", "name", "lat", "lon").write_csv("./data/tables/tmp_data/tb_plazas_imt.csv")



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", help="data year", required=True, type=int)
    parser.add_argument("--tb-tbsts", help="join tollbooths their statistics", required=False, action='store_true')
    parser.add_argument("--check-tb-imt", required=False, action="store_true")
    args = parser.parse_args()
    if args.tb_tbsts:
        join_tb_tbsts(args.year)
    elif args.check_tb_imt:
        check_imt_tb(args.year)
    