import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import polars as pl
import polars_h3 as plh3
import argparse

from tb_map_editor.data_files import DataModel


def join_tb_tbsts(year:int):
    hex_resolution = 10
    prev_year = year - 1
    data_path_prev_year = DataModel(prev_year)
    df_tbsts = pl.read_parquet(
        data_path_prev_year.tollbooths_sts.parquet
    )
    df_tbsts = df_tbsts.with_columns(
        pl.col("lat").cast(pl.Float32),
        pl.col("lon").cast(pl.Float32)
    )
    df_tbsts = df_tbsts.with_columns(
        h3_cell=plh3.latlng_to_cell("lat", "lon", hex_resolution)
    )

    data_path = DataModel(year)
    df_tb = pl.read_parquet(
        data_path.tollbooths.parquet
    )
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
    hex_resolution_max = 7
    hex_resolution_min = 12
    hex_resolution_max_name = "hex_rest_max"
    hex_resolution_min_name = "hex_rest_min"
    data_model_prev_year = DataModel(year - 1)
    df_tb_imt = pl.read_parquet(
        data_model_prev_year.tb_imt.parquet, 
        columns=["tollbooth_imt_id", "lat", "lng", "calirepr"]
    )
    df_tb_imt = df_tb_imt.filter(pl.col("calirepr") != "Virtual")
    df_tb_imt = df_tb_imt.with_columns(
        plh3.latlng_to_cell("lat", "lng", hex_resolution_max).alias(hex_resolution_max_name),
        plh3.latlng_to_cell("lat", "lng", hex_resolution_min).alias(hex_resolution_min_name)
    )

    data_model = DataModel(year)
    df_tb_catalog = pl.read_parquet(
        data_model.tollbooths.parquet,
        columns=["tollbooth_id", "lat", "lng"]
    )
    df_tb_catalog = df_tb_catalog.with_columns(
        plh3.latlng_to_cell("lat", "lng", hex_resolution_max).alias(hex_resolution_max_name),
        plh3.latlng_to_cell("lat", "lng", hex_resolution_min).alias(hex_resolution_min_name)
    )
    df_imt_tb_catalog = df_tb_imt.join(
        df_tb_catalog, on=hex_resolution_max_name
    )
    df_imt_tb_catalog = df_imt_tb_catalog.with_columns(
        #plh3.grid_distance("hex_rest_min", "hex_rest_min_right").alias("grid_distance"),
        plh3.great_circle_distance("lat", "lng", "lat_right", "lng_right").alias("grid_distance")
    ).select("tollbooth_id", "tollbooth_imt_id", "grid_distance")
    df_imt_tb_catalog_no_dup_tb = df_imt_tb_catalog.group_by("tollbooth_id").agg(pl.col("grid_distance").min())
    df_imt_tb_catalog_no_dup_tb = df_imt_tb_catalog_no_dup_tb.join(
        df_imt_tb_catalog, on=["tollbooth_id", "grid_distance"]
    )

    df_imt_tb_catalog_no_dup_imt_tb = df_imt_tb_catalog_no_dup_tb.group_by("tollbooth_imt_id").agg(pl.col("grid_distance").min())
    df_imt_tb_catalog_no_dup_tb_join = df_imt_tb_catalog_no_dup_imt_tb.join(
       df_imt_tb_catalog_no_dup_tb, on=["tollbooth_imt_id", "grid_distance"]
    ).select("tollbooth_id", "tollbooth_imt_id", "grid_distance")
    df_tb_not_found = df_imt_tb_catalog.join(
        df_imt_tb_catalog_no_dup_tb_join, on="tollbooth_id", how="left"
    ).filter(pl.col("tollbooth_imt_id_right").is_null()).select("tollbooth_id", "tollbooth_imt_id", "grid_distance")
    df_tb_not_found_best = df_tb_not_found.group_by("tollbooth_imt_id").agg(pl.col("grid_distance").min())
    df_tb_not_found = df_tb_not_found_best.join(
        df_tb_not_found, on=["tollbooth_imt_id", "grid_distance"], how="left"
    ).select("tollbooth_id", "tollbooth_imt_id", "grid_distance")
    df_tb_not_found = df_tb_not_found.join(
        df_imt_tb_catalog_no_dup_tb_join, on="tollbooth_imt_id", how="left"
    ).filter(pl.col("tollbooth_id_right").is_null()).select("tollbooth_id", "tollbooth_imt_id", "grid_distance")
    #print(df_tb_not_found)
    df_tb_match = df_imt_tb_catalog_no_dup_tb_join.extend(df_tb_not_found)
    df_no_match = df_tb_match.select("tollbooth_imt_id", "tollbooth_id").join(
        df_tb_imt.select("tollbooth_imt_id"), on=["tollbooth_imt_id"], how="right"
    ).filter(pl.col("tollbooth_id").is_null())
    
    df_no_match = df_imt_tb_catalog.join(
        df_no_match.select("tollbooth_imt_id").join(
            df_imt_tb_catalog, 
            on="tollbooth_imt_id"
        ).group_by("tollbooth_imt_id").agg(pl.col("grid_distance").min()), 
        on=["tollbooth_imt_id", "grid_distance"], 
        how="right")
    df_all_data = df_imt_tb_catalog_no_dup_tb_join.extend(df_no_match)
    df_all_data.write_parquet(data_model.tb_imt_tb_id.parquet)
    print("LEFT", df_all_data.join(df_tb_imt, on="tollbooth_imt_id", how="left").filter(pl.col("lat").is_null()).shape)


def tb_stretch_id_imt(year: int):
    data_model = DataModel(year)
    ldf_toll_imt = pl.scan_parquet(data_model.tb_toll_imt.parquet)
    ldf_stretch_toll = pl.scan_parquet(data_model.stretchs_toll.parquet)
    ldf_toll = ldf_toll_imt.join(
        ldf_stretch_toll, on=[
            "motorbike", "car", "bus_2_axle", "bus_3_axle", "bus_4_axle",
            "truck_2_axle", "truck_3_axle", "truck_4_axle", "truck_5_axle",
            "truck_6_axle", "truck_7_axle", "truck_8_axle", "truck_9_axle"
        ]
    ).select("stretch_id", "tollbooth_imt_id_a", "tollbooth_imt_id_b")
    ldf_tb_imt_tb_id = pl.scan_parquet(data_model.tb_imt_tb_id.parquet).select("tollbooth_id", "tollbooth_imt_id")
    ldf_stretch = ldf_toll.join(
        ldf_tb_imt_tb_id, left_on="tollbooth_imt_id_a", right_on="tollbooth_imt_id"
    ).rename({"tollbooth_id": "tollbooth_id_b"})
    ldf_stretch = ldf_stretch.join(
        ldf_tb_imt_tb_id, left_on="tollbooth_imt_id_b", right_on="tollbooth_imt_id"
    ).rename({"tollbooth_id": "tollbooth_id_a"})
    ldf_stretch = ldf_stretch.select(
        "stretch_id", "tollbooth_id_a", "tollbooth_id_b"
    ).unique()
    ldf_stretch_not_found = ldf_stretch_toll.select("stretch_id").join(
        ldf_stretch, on="stretch_id", how="anti"
    ).with_columns(
        pl.lit(None).alias("tollbooth_id_a"),
        pl.lit(None).alias("tollbooth_id_b")
    )
    pl.concat([ldf_stretch, ldf_stretch_not_found], how="vertical").sink_parquet(data_model.tb_stretch_id.parquet)


def tb_stretch_id_imt_eval(year: int):
    data_model = DataModel(year)
    ldf_stretch = pl.scan_parquet(data_model.tb_stretch_id.parquet)
    #ldf_stretch_base = pl.scan_csv("./data/tables/strech_tb_id_base.csv")
    ldf_stretch_base = pl.scan_csv("./data/tables/tb_strech_id_old.csv")
    ldf_eval = ldf_stretch.join(ldf_stretch_base, left_on="stretch_id", right_on="strech_id")
    ldf_eval = ldf_eval.select("tollbooth_id_a", "tollbooth_id").unique()
    ldf_result = ldf_eval.with_columns(
        (pl.col("tollbooth_id_a") == pl.col("tollbooth_id")).alias("match")
    ).group_by("match").len()
    print(ldf_result.collect())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", help="data year", required=True, type=int)
    parser.add_argument("--tb-tbsts", help="join tollbooths their statistics", required=False, action='store_true')
    parser.add_argument("--tb-imt-tb-id", required=False, action="store_true")
    parser.add_argument("--tb-stretch-id-imt", required=False, action="store_true")
    parser.add_argument("--eval-stretch", required=False, action="store_true")
    args = parser.parse_args()
    if args.tb_tbsts:
        join_tb_tbsts(args.year)
    elif args.tb_imt_tb_id:
        tb_imt_tb_id(args.year)
    elif args.tb_stretch_id_imt:
        tb_stretch_id_imt(args.year)
    elif args.eval_stretch:
        tb_stretch_id_imt_eval(args.year)
    