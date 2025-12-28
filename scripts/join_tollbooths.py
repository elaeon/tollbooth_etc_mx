import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import polars as pl
import polars_h3 as plh3
import argparse

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


def tb_imt_tb_id(year: int):
    hex_resolution_max = 9
    hex_resolution_min = 12
    hex_resolution_max_name = "hex_rest_max"
    hex_resolution_min_name = "hex_rest_min"
    df_plazas = pl.read_csv("./tmp_data/plazas.csv", infer_schema=False)

    df_plazas = df_plazas.with_columns(
        pl.col("xcoord").cast(pl.Float32).alias("lon"),
        pl.col("ycoord").cast(pl.Float32).alias("lat")
    ).rename({"NOMBRE": "name", "ID_PLAZA": "tollbooth_imt_id"}).filter(pl.col("CALIREPR") != "Virtual")
    df_plazas = df_plazas.with_columns(
        plh3.latlng_to_cell("lat", "lon", hex_resolution_max).alias(hex_resolution_max_name),
        plh3.latlng_to_cell("lat", "lon", hex_resolution_min).alias(hex_resolution_min_name)
    )

    data_path = DataPathSchema(year)
    df_tb_catalog = pl.read_parquet(
        data_path.tollbooths.parquet
    )
    df_tb_catalog = df_tb_catalog.with_columns(
        plh3.latlng_to_cell("lat", "lon", hex_resolution_max).alias(hex_resolution_max_name),
        plh3.latlng_to_cell("lat", "lon", hex_resolution_min).alias(hex_resolution_min_name)
    )
    df_plazas_tb_catalog = df_plazas.join(df_tb_catalog, on=hex_resolution_max_name)
    df_plazas_tb_catalog = df_plazas_tb_catalog.with_columns(
        plh3.grid_distance("hex_rest_min", "hex_rest_min_right").alias("grid_distance")
    )
    df_plazas_tb_catalog_no_dup_imt_tb = df_plazas_tb_catalog.group_by("tollbooth_imt_id").agg(pl.col("grid_distance").min())
    df_plazas_tb_catalog_no_dup_tb = df_plazas_tb_catalog.group_by("tollbooth_id").agg(pl.col("grid_distance").min())
    df_plazas_tb_catalog_no_dup_tb = df_plazas_tb_catalog_no_dup_tb.join(
        df_plazas_tb_catalog, on=["tollbooth_id", "grid_distance"]
    ).select("tollbooth_id", "tollbooth_imt_id", "grid_distance")
    df_plazas_tb_catalog_no_dup_imt_tb = df_plazas_tb_catalog_no_dup_imt_tb.join(
        df_plazas_tb_catalog_no_dup_tb, on=["tollbooth_imt_id", "grid_distance"]
    ).select("tollbooth_id", "tollbooth_imt_id", "grid_distance")
    df_plazas_tb_catalog_no_dup_imt_tb = df_plazas_tb_catalog_no_dup_imt_tb.group_by("tollbooth_id", "grid_distance").agg(pl.col("tollbooth_imt_id").min())
    df_plazas_tb_catalog_no_dup_imt_tb = df_plazas_tb_catalog_no_dup_imt_tb.group_by("tollbooth_imt_id", "grid_distance").agg(pl.col("tollbooth_id").min())
    
    df_tb_not_found = df_plazas.join(
        df_tb_catalog, on=hex_resolution_max_name, how="left"
    ).filter(pl.col("tollbooth_name").is_null() & pl.col("lat_right").is_null()).select("tollbooth_imt_id")
    df_tb_not_found = df_tb_not_found.with_columns(
        pl.lit(None).alias("tollbooth_id"),
        pl.lit(None).alias("grid_distance")
    )
    df_plazas_tb_catalog_no_dup_imt_tb.select(
        "tollbooth_id", "tollbooth_imt_id", "grid_distance"
    ).extend(
        df_tb_not_found.select("tollbooth_id", "tollbooth_imt_id", "grid_distance")
    ).write_csv(data_path.tb_imt_tb_id.csv)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", help="data year", required=True, type=int)
    parser.add_argument("--tb-tbsts", help="join tollbooths their statistics", required=False, action='store_true')
    parser.add_argument("--tb-imt-tb-id", required=False, action="store_true")
    args = parser.parse_args()
    if args.tb_tbsts:
        join_tb_tbsts(args.year)
    elif args.tb_imt_tb_id:
        tb_imt_tb_id(args.year)
    