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


def tb_imt_tb_id(base_year: int, move_year: int):
    hex_resolution_max = 5
    hex_resolution_min = 12
    hex_resolution_max_name = "hex_rest_max"
    hex_resolution_min_name = "hex_rest_min"
    data_model_prev_year = DataModel(base_year)
    df_tb_imt = pl.read_parquet(
        data_model_prev_year.tb_imt.parquet, 
        columns=["tollbooth_imt_id", "lat", "lng", "calirepr"]
    )
    df_tb_imt = df_tb_imt.filter(pl.col("calirepr") != "Virtual")
    df_tb_imt = df_tb_imt.with_columns(
        plh3.latlng_to_cell("lat", "lng", hex_resolution_max).alias(hex_resolution_max_name),
        plh3.latlng_to_cell("lat", "lng", hex_resolution_min).alias(hex_resolution_min_name)
    )

    data_model = DataModel(move_year)
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
    df_all_data = df_imt_tb_catalog_no_dup_tb_join.extend(df_no_match).filter(pl.col("grid_distance") <= 0.3)
    print(df_all_data.shape)
    df_all_data.write_parquet(data_model.tb_imt_tb_id.parquet)
    print("LEFT", df_all_data.join(df_tb_imt, on="tollbooth_imt_id", how="left").filter(pl.col("lat").is_null()).shape)


def _tb_stretch_id_imt(ldf_toll_imt, ldf_stretch_toll, ldf_tb_imt_tb_id):
    ldf_stretch_toll = ldf_stretch_toll.with_columns(
        pl.all().fill_null(strategy="zero"),
    ).with_columns(
        pl.col("car").cast(pl.String)
    )
    ldf_toll_imt = ldf_toll_imt.with_columns(
        pl.col("car").cast(pl.String)
    )
    ldf_toll = ldf_toll_imt.join(
        ldf_stretch_toll, on=[
            "motorbike", "car", "bus_2_axle", "bus_3_axle", "bus_4_axle",
            "truck_2_axle", "truck_3_axle", "truck_4_axle", "truck_5_axle",
            "truck_6_axle", "truck_7_axle", "truck_8_axle", "truck_9_axle"
        ]
    ).select("stretch_id", "tollbooth_imt_id_a", "tollbooth_imt_id_b")
    ldf_stretch = ldf_toll.join(
        ldf_tb_imt_tb_id, left_on="tollbooth_imt_id_a", right_on="tollbooth_imt_id"
    ).rename({"tollbooth_id": "tollbooth_id_b"})
    ldf_stretch = ldf_stretch.join(
        ldf_tb_imt_tb_id, left_on="tollbooth_imt_id_b", right_on="tollbooth_imt_id"
    ).rename({"tollbooth_id": "tollbooth_id_a"})
    ldf_stretch = ldf_stretch.select(
        "stretch_id", "tollbooth_id_a", "tollbooth_id_b"
    ).unique()
    return ldf_stretch


def tb_stretch_id_imt(base_year: int, move_year: int):
    data_model_base = DataModel(base_year)
    data_model_move_year = DataModel(move_year)
    ldf_toll_imt = pl.scan_parquet(data_model_move_year.tb_toll_imt.parquet)
    ldf_stretch_toll = pl.scan_parquet(data_model_move_year.stretchs_toll.parquet)
    ldf_tb_imt_tb_id = pl.scan_parquet(data_model_base.tb_imt_tb_id.parquet).select("tollbooth_id", "tollbooth_imt_id")
    ldf_stretch = _tb_stretch_id_imt(ldf_toll_imt, ldf_stretch_toll, ldf_tb_imt_tb_id)
    ldf_stretch_not_found = ldf_stretch_toll.select("stretch_id").join(
        ldf_stretch, on="stretch_id", how="anti"
    ).with_columns(
        pl.lit(None).alias("tollbooth_id_a"),
        pl.lit(None).alias("tollbooth_id_b")
    )
    pl.concat([ldf_stretch, ldf_stretch_not_found], how="vertical").sink_parquet(data_model_move_year.tb_stretch_id.parquet)


def tb_stretch_id_imt_delta(base_year: int, move_year: int, pivot_year: int):
    data_model_base = DataModel(base_year)
    data_model_move_year = DataModel(move_year)
    data_model_pivot_year = DataModel(pivot_year)
    ldf_tb_stretch = pl.scan_parquet(data_model_base.tb_stretch_id.parquet)
    ldf_toll_imt = pl.scan_parquet(data_model_move_year.tb_toll_imt.parquet)
    ldf_stretch_toll = pl.scan_parquet(data_model_move_year.stretchs_toll.parquet)
    ldf_tb_imt_tb_id = pl.scan_parquet(data_model_pivot_year.tb_imt_tb_id.parquet).select("tollbooth_id", "tollbooth_imt_id")
    ldf_stretch = _tb_stretch_id_imt(ldf_toll_imt, ldf_stretch_toll, ldf_tb_imt_tb_id)
    ldf_tb_stretch = ldf_tb_stretch.join(ldf_stretch, on="stretch_id", how="left")
    ldf_tb_stretch = ldf_tb_stretch.update(
         ldf_stretch, on="stretch_id", how="left"
    ).select("stretch_id", "tollbooth_id_a", "tollbooth_id_b").unique()
    ldf_new_stretch = ldf_stretch.join(ldf_tb_stretch, on="stretch_id", how="anti")
    pl.concat([ldf_tb_stretch, ldf_new_stretch], how="vertical").sink_parquet(data_model_move_year.tb_stretch_id.parquet)


def tb_stretch_id_imt_patch(year: int):
    data_model = DataModel(year)
    ldf_tb_stretch_id = pl.scan_parquet(data_model.tb_stretch_id.parquet)
    ldf_tb_stretch_id_m = pl.scan_csv(data_model.tb_stretch_id_patch.csv)
    ldf_tb_stretch_id = ldf_tb_stretch_id.update(
        ldf_tb_stretch_id_m, on="stretch_id", how="left"
    ).unique()
    ldf_tb_stretch_id.sink_parquet(data_model.tb_stretch_id_patched.parquet)


def find_similarity_toll(base_year: int, move_year: int, stretch_id: int):
    data_model = DataModel(move_year)
    df_tb_imt = pl.read_parquet(data_model.tb_toll_imt.parquet)
    df_tb_imt = df_tb_imt.select(
        pl.exclude("tollbooth_id_a", "tollbooth_id_b", "info_year", "car_axle", 
                   "load_axle", "nombre_sal", "nombre_ent")
    )
    data_model_base = DataModel(base_year)
    df_tb_toll = pl.scan_parquet(data_model_base.stretchs_toll.parquet).select(
        pl.exclude("car_axle", "load_axle", "bicycle", "car_rush_hour", "pedestrian", "car_rush_hour_2",
                "car_evening_hour_2", "car_morning_night", "motorbike_axle", "toll_ref", "truck_10_axle",
                "car_evening_hour", "info_year")
    )
    df_tb_toll = df_tb_toll.filter(pl.col("stretch_id") == stretch_id).select(pl.exclude("stretch_id")).collect()
    df_tb_imt = pl.concat([df_tb_toll, df_tb_imt], how="vertical")
    df_tb_imt = df_tb_imt.with_columns(
        pl.all().fill_null(strategy="zero")
    )
    df = pl.DataFrame({
       "col1": list(range(df_tb_imt.shape[0])),
       "col2": df_tb_imt.rows()
    })
    df = df.with_row_index().lazy()

    cosine_similarity = lambda x, y: (
        (x * y).list.sum() / (
            (x * x).list.sum().sqrt() * (y * y).list.sum().sqrt()
        )
    )

    euclidean_distance = lambda x, y: (
        ((y - x)*(y - x)).list.sum().sqrt()
    )

    out_cosine = (
    df.join_where(df, pl.col.index == 0)
        .select(
            col = "col1",
            other = "col1_right",
            cosine = cosine_similarity(
                x = pl.col.col2,
                y = pl.col.col2_right
            )
        )
    )
    out_euclidean = (
    df.join_where(df, pl.col.index == 0)
        .select(
            col = "col1",
            other = "col1_right",
            eucli = euclidean_distance(
                x = pl.col.col2,
                y = pl.col.col2_right
            )
        )
    )
    best_i = []
    best_rows = out_cosine.filter(pl.col("cosine").is_not_nan()).top_k(3, by="cosine", reverse=False).collect()
    for best_opt in best_rows.rows(named=True):
        best_i.append(best_opt["other"])
    print(pl.concat([df_tb_imt[best_i], best_rows.select("cosine")], how="horizontal"))

    best_i = []
    best_rows = out_euclidean.filter(pl.col("eucli").is_not_nan()).top_k(3, by="eucli", reverse=True).collect()
    for best_opt in best_rows.rows(named=True):
        best_i.append(best_opt["other"])
    print(pl.concat([df_tb_imt[best_i], best_rows.select("eucli")], how="horizontal"))


def map_stretch_toll_imt(base_year: int, other_year: int):
    data_model_base = DataModel(base_year)
    data_model_other_year = DataModel(other_year)
    ldf_toll_imt = pl.scan_parquet(data_model_other_year.tb_toll_imt.parquet).select("tollbooth_imt_id_a", "tollbooth_imt_id_b")
    ldf_tb_imt_tb_id = pl.scan_parquet(data_model_base.tb_imt_tb_id.parquet).select("tollbooth_id", "tollbooth_imt_id")
    ldf_stretch_id = pl.scan_parquet(data_model_base.tb_stretch_id.parquet)
    ldf_stretch = ldf_toll_imt.join(
        ldf_tb_imt_tb_id, left_on="tollbooth_imt_id_a", right_on="tollbooth_imt_id"
    ).rename({"tollbooth_id": "tollbooth_id_b"})
    ldf_stretch = ldf_stretch.join(
        ldf_tb_imt_tb_id, left_on="tollbooth_imt_id_b", right_on="tollbooth_imt_id"
    ).rename({"tollbooth_id": "tollbooth_id_a"})
    ldf_stretch = ldf_stretch.select(
        "tollbooth_imt_id_b", 
        "tollbooth_imt_id_a",
        "tollbooth_id_a",
        "tollbooth_id_b"
    )
    ldf_stretch = ldf_stretch_id.join(ldf_stretch, on=["tollbooth_id_a", "tollbooth_id_b"])
    print(ldf_stretch.collect().shape)
    #print(ldf_stretch.filter(pl.col("stretch_id")==1343).collect())
    print(ldf_stretch.filter(pl.col("tollbooth_id_a")==791).collect())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", help="data year", required=True, type=int)
    parser.add_argument("--tb-tbsts", help="join tollbooths their statistics", required=False, action='store_true')
    parser.add_argument("--tb-imt-tb-id", required=False, type=int)
    parser.add_argument("--tb-stretch-id-imt", required=False, type=int)
    parser.add_argument("--tb-stretch-id-imt-delta", required=False, type=int)
    parser.add_argument("--pivot-year", required=False, type=int)
    parser.add_argument("--similarity-toll", required=False, type=int)
    parser.add_argument("--id", required=False, type=int)
    parser.add_argument("--tb-stretch-id-patch", required=False, action="store_true")
    parser.add_argument("--map-stretch-toll-imt", required=False)
    args = parser.parse_args()

    if args.tb_tbsts:
        join_tb_tbsts(args.year)
    elif args.tb_imt_tb_id:
        tb_imt_tb_id(args.year, args.tb_imt_tb_id)
    elif args.tb_stretch_id_imt:
        tb_stretch_id_imt(args.year, args.tb_stretch_id_imt)
    elif args.tb_stretch_id_imt_delta:
        tb_stretch_id_imt_delta(args.year, args.tb_stretch_id_imt_delta, args.pivot_year)
    elif args.similarity_toll:
        find_similarity_toll(args.year, args.similarity_toll, args.id)
    elif args.tb_stretch_id_patch:
        tb_stretch_id_imt_patch(args.year)
    elif args.map_stretch_toll_imt:
        map_stretch_toll_imt(args.year, args.map_stretch_toll_imt)
    