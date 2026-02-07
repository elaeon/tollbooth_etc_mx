import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import polars as pl
import polars_ds as plds
import argparse

from tb_map_editor.data_files import DataModel, DataStage


def _no_tb(ldf_neighbour, ldf, scope: str, threshold: float = 0.1):
    ldf_neighbour = ldf_neighbour.filter(pl.col("scope") == scope)
    ldf_closest = ldf_neighbour.filter(pl.col("distance") <= threshold)
    ldf_no_map = ldf.join(
        ldf_closest, left_on="tollbooth_id", right_on="neighbour_id", how="anti"
    )
    return ldf_no_map


def sts_no_tb(base_year: int, move_year: int):
    data_model_prev_year = DataModel(base_year, DataStage.prd)
    data_model = DataModel(move_year, DataStage.stg)

    ldf_neighbour = pl.scan_parquet(data_model.tb_neighbour.parquet)
    ldf_tb_sts = pl.scan_parquet(
        data_model_prev_year.tb_sts.parquet
    ).select("tollbooth_id", "lat", "lng", "tollbooth_name", "stretch_name")

    ldf_sts_no_map = _no_tb(ldf_neighbour, ldf_tb_sts, "local-sts")
    ldf_sts_no_map.select(
        "tollbooth_id", "tollbooth_name", "stretch_name"
    ).sink_csv(f"./tmp_data/{base_year}/sts_no_tb.csv")


def imt_no_tb(base_year: int, move_year: int):
    data_model_prev_year = DataModel(base_year, DataStage.stg)

    ldf_neighbour = pl.scan_parquet(data_model_prev_year.tb_neighbour.parquet)
    ldf_tb_imt = pl.scan_parquet(
        data_model_prev_year.tb_imt.parquet, 
    ).select("tollbooth_id", "lat", "lng", "area", "subarea")

    ldf_imt_no_map = _no_tb(ldf_neighbour, ldf_tb_imt, "local-imt")
    ldf_imt_no_map.select(
        "tollbooth_id", "area", "subarea"
    ).sink_csv(f"./tmp_data/{base_year}/imt_no_tb.csv")


def _tb_stretch_id_imt(ldf_toll_imt, ldf_stretch_toll, ldf_map_tb_imt):
    # ldf_neighbour.filter(
    #     pl.col("distance") == pl.col("distance").min().over("tollbooth_id")
    # )

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
    ).select("stretch_id", "tollbooth_id_a", "tollbooth_id_b", "area", "subarea", "tollbooth_name")
    ldf_toll = ldf_toll.rename({
        "tollbooth_id_a": "tollbooth_imt_id_a",
        "tollbooth_id_b": "tollbooth_imt_id_b"
    })
    ldf_stretch = ldf_toll.join(
        ldf_map_tb_imt, left_on="tollbooth_imt_id_a", right_on="tollbooth_imt_id"
    ).rename({"tollbooth_id": "tollbooth_id_b"})
    ldf_stretch = ldf_stretch.join(
        ldf_map_tb_imt, left_on="tollbooth_imt_id_b", right_on="tollbooth_imt_id"
    ).rename({"tollbooth_id": "tollbooth_id_a"})
    ldf_stretch = ldf_stretch.select(
        "stretch_id", "tollbooth_id_a", "tollbooth_id_b", "area", "subarea", "tollbooth_name"
    ).unique()
    return ldf_stretch


def tb_stretch_id_imt(base_year: int, move_year: int):
    data_model_base = DataModel(base_year, DataStage.stg)
    data_model_move_year = DataModel(move_year, DataStage.stg)
    
    ldf_toll_imt = pl.scan_parquet(data_model_move_year.tb_toll_imt.parquet)
    ldf_stretch_toll = pl.scan_parquet(data_model_move_year.stretchs_toll.parquet)
    ldf_map_tb_imt = pl.scan_parquet(data_model_base.map_tb_imt.parquet).select("tollbooth_id", "tollbooth_imt_id")
    ldf_stretch = pl.scan_parquet(data_model_base.stretchs.parquet).select("stretch_id", "stretch_name")
    ldf_tb_imt = pl.scan_parquet(data_model_base.tb_imt.parquet).select("tollbooth_id", "area", "subarea", "tollbooth_name")
    
    ldf_toll_imt = ldf_toll_imt.join(ldf_tb_imt, left_on="tollbooth_id_a", right_on="tollbooth_id")
    ldf_map_stretch = _tb_stretch_id_imt(ldf_toll_imt, ldf_stretch_toll, ldf_map_tb_imt)
    ldf_map_stretch = ldf_map_stretch.join(ldf_stretch, on="stretch_id")
    
    ldf_map_stretch = ldf_map_stretch.with_columns(
        plds.str_d_leven("area", "stretch_name", return_sim=True).alias("lev_area"),
        plds.str_d_leven("subarea", "stretch_name", return_sim=True).alias("lev_sub"),
        plds.str_d_leven("tollbooth_name", "stretch_name", return_sim=True).alias("lev_tb")
    )
    ldf_map_stretch = ldf_map_stretch.with_columns(
        pl.max_horizontal("lev_area", "lev_sub", "lev_tb").alias("lev_best")
    )
    ldf_map_stretch_g = ldf_map_stretch.group_by("tollbooth_id_a", "tollbooth_id_b").agg(pl.col("lev_best").max())
    ldf_map_stretch = ldf_map_stretch.join(
       ldf_map_stretch_g, on=["tollbooth_id_a", "tollbooth_id_b", "lev_best"]
    ).select("stretch_id", "tollbooth_id_a", "tollbooth_id_b")

    ldf_stretch_no_tb = ldf_stretch.join(ldf_map_stretch, on="stretch_id", how="anti")
    ldf_stretch_no_tb = ldf_stretch_no_tb.select("stretch_id")
    ldf_stretch_no_tb = ldf_stretch_no_tb.with_columns(
        pl.lit(None).alias("tollbooth_id_a"),
        pl.lit(None).alias("tollbooth_id_b")
    )
    ldf_map_stretch = pl.concat([ldf_map_stretch, ldf_stretch_no_tb])
    ldf_map_stretch.sink_parquet(data_model_move_year.tb_stretch_id.parquet)


def tb_stretch_id_imt_delta(base_year: int, move_year: int, pivot_year: int):
    data_model_base = DataModel(base_year, DataStage.stg)
    data_model_move_year = DataModel(move_year, DataStage.stg)
    data_model_pivot_year = DataModel(pivot_year, DataStage.stg)
    ldf_tb_stretch = pl.scan_parquet(data_model_base.tb_stretch_id.parquet)
    ldf_toll_imt = pl.scan_parquet(data_model_move_year.tb_toll_imt.parquet)
    ldf_stretch_toll = pl.scan_parquet(data_model_move_year.stretchs_toll.parquet)
    ldf_map_tb_imt = pl.scan_parquet(data_model_pivot_year.map_tb_imt.parquet).select("tollbooth_id", "tollbooth_imt_id")
    ldf_stretch = _tb_stretch_id_imt(ldf_toll_imt, ldf_stretch_toll, ldf_map_tb_imt)
    ldf_tb_stretch = ldf_tb_stretch.join(ldf_stretch, on="stretch_id", how="left")
    ldf_tb_stretch = ldf_tb_stretch.update(
         ldf_stretch, on="stretch_id", how="left"
    ).select("stretch_id", "tollbooth_id_a", "tollbooth_id_b").unique()
    ldf_new_stretch = ldf_stretch.join(ldf_tb_stretch, on="stretch_id", how="anti")
    pl.concat([ldf_tb_stretch, ldf_new_stretch], how="vertical").sink_parquet(data_model_move_year.tb_stretch_id.parquet)


def tb_stretch_id_imt_patch(year: int):
    data_model_pub = DataModel(year, DataStage.pub)
    data_model_stg = DataModel(year, DataStage.stg)
    data_model_prod = DataModel(year, DataStage.prd)
    ldf_tb_stretch_id = pl.scan_parquet(data_model_stg.tb_stretch_id.parquet)
    ldf_tb_stretch_id_m = pl.scan_csv(data_model_pub.tb_stretch_id_patch.csv)
    ldf_tb_stretch_id = ldf_tb_stretch_id.update(
        ldf_tb_stretch_id_m, on="stretch_id", how="left"
    ).unique()
    ldf_tb_stretch_id.sink_parquet(data_model_prod.tb_stretch_id.parquet)


def find_similarity_toll(base_year: int, move_year: int, stretch_id: int):
    data_model = DataModel(move_year, DataStage.stg)
    df_tb_imt = pl.read_parquet(data_model.tb_toll_imt.parquet)
    df_tb_imt = df_tb_imt.select(
        pl.exclude("tollbooth_id_a", "tollbooth_id_b", "info_year", "car_axle", 
                   "load_axle", "nombre_sal", "nombre_ent")
    )
    data_model_base = DataModel(base_year, DataStage.stg)
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


def first_parent(year: int):
    df = pl.read_csv("data/tables/area_operators_mx.csv", separator="|").select("parent", "short_name")
    df = df.join(df, left_on="parent", right_on="short_name", how="left")
    while True:
        if not df.filter(pl.col("parent_right").is_not_null()).is_empty():
            df = df.rename({"parent_right": "sparent"}).select("parent", "short_name", "sparent")
            df = df.with_columns(
                pl.when(pl.col("sparent").is_not_null()).then(pl.col("sparent")).otherwise("parent").alias("parent")
            ).select("parent", "short_name")
            df = df.join(df, left_on="parent", right_on="short_name", how="left")
        else:
            df = df.select("parent", "short_name")
            break
    
    df = df.with_columns(
        pl.when(pl.col("parent").is_null()).then(pl.col("short_name")).otherwise("parent").alias("parent")
    ).unique()
    df.write_parquet("tmp_data/first_parent.parquet")

    data_model = DataModel(year, DataStage.stg)
    df_tb = pl.scan_parquet(data_model.tollbooths.parquet)
    df_manage_parent = pl.scan_parquet("tmp_data/first_parent.parquet")
    df_tb = df_tb.join(df_manage_parent, left_on="manage", right_on="short_name", how="left")
    df_tb = df_tb.select(pl.exclude("short_name")).rename({"parent": "parent_manage"})
    df_tb.sink_parquet(DataModel(year, DataStage.prd).tollbooths.parquet)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", help="data year", required=True, type=int)
    parser.add_argument("--sts-no-tb", help="join tollbooths their statistics", required=False, type=int)
    parser.add_argument("--imt-no-tb", required=False, type=int)
    parser.add_argument("--tb-stretch-id-imt", required=False, type=int)
    parser.add_argument("--tb-stretch-id-imt-delta", required=False, type=int)
    parser.add_argument("--pivot-year", required=False, type=int)
    parser.add_argument("--similarity-toll", required=False, type=int)
    parser.add_argument("--id", required=False, type=int)
    parser.add_argument("--tb-stretch-id-patch", required=False, action="store_true")
    parser.add_argument("--map-stretch-toll-imt", required=False)
    parser.add_argument("--first-parent", required=False, action="store_true")
    args = parser.parse_args()

    if args.sts_no_tb:
        sts_no_tb(args.year, args.sts_no_tb)
    elif args.imt_no_tb:
        imt_no_tb(args.year, args.imt_no_tb)
    elif args.tb_stretch_id_imt:
        tb_stretch_id_imt(args.year, args.tb_stretch_id_imt)
    elif args.tb_stretch_id_imt_delta:
        tb_stretch_id_imt_delta(args.year, args.tb_stretch_id_imt_delta, args.pivot_year)
    elif args.similarity_toll:
        find_similarity_toll(args.year, args.similarity_toll, args.id)
    elif args.tb_stretch_id_patch:
        tb_stretch_id_imt_patch(args.year)
    elif args.first_parent:
        first_parent(args.year)
    