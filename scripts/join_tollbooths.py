import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import polars as pl
import polars_ds as plds
import argparse

from tb_map_editor.data_files import DataModel, DataStage
from tb_map_editor.utils.tools import find_closest_tb, join_tb_stretch_id_imt


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

    ldf_sts_no_map = _no_tb(ldf_neighbour, ldf_tb_sts, "local-sts", threshold=100)
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


def tb_stretch_id_imt(base_year: int, move_year: int):
    """
    Legacy method to get mapped stretch_id and tollbooths.
    Actual method is tb_imt_stretch_id_rel
    """
    data_model_base = DataModel(base_year, DataStage.stg)
    data_model_move_year = DataModel(move_year, DataStage.stg)
    data_model_pub = DataModel(base_year, DataStage.pub)
    
    ldf_toll_imt = pl.scan_parquet(data_model_move_year.tb_toll_imt.parquet)
    ldf_stretch_toll = pl.scan_parquet(data_model_move_year.stretchs_toll.parquet)
    ldf_stretch = pl.scan_parquet(data_model_base.stretchs.parquet).select("stretch_id", "stretch_name")
    ldf_tb_imt = pl.scan_parquet(data_model_base.tb_imt.parquet).select("tollbooth_id", "area", "subarea", "tollbooth_name")
    ldf_map_tb_id = pl.scan_parquet(data_model_base.map_tb_id.parquet).select("tollbooth_id", "tollbooth_imt_id")

    ldf_map_tb_id = ldf_map_tb_id.join(ldf_tb_imt, left_on="tollbooth_imt_id", right_on="tollbooth_id")
    ldf_toll_imt = ldf_toll_imt.join(
        ldf_map_tb_id, left_on="tollbooth_id_out", right_on="tollbooth_imt_id"
    ).rename({"tollbooth_id_out": "tollbooth_imt_id_out", "tollbooth_id": "tollbooth_id_out"})
    ldf_toll_imt = ldf_toll_imt.join(
        ldf_map_tb_id, left_on="tollbooth_id_in", right_on="tollbooth_imt_id"
    ).rename({"tollbooth_id_in": "tollbooth_imt_id_in", "tollbooth_id": "tollbooth_id_in"})
    ldf_map_stretch = join_tb_stretch_id_imt(ldf_toll_imt, ldf_stretch_toll)
    ldf_map_stretch = ldf_map_stretch.select(
        "stretch_id", "tollbooth_id_in", "tollbooth_id_out", "area", "subarea", "tollbooth_name"
    ).unique()
    ldf_map_stretch = ldf_map_stretch.join(ldf_stretch, on="stretch_id")
    print(ldf_map_stretch.collect().shape)

    ldf_map_stretch = ldf_map_stretch.with_columns(
        pl.col("area").replace("n_d", None),
        pl.col("subarea").replace("n_d", None)
    )
    ldf_map_stretch = ldf_map_stretch.with_columns(
        plds.str_jw("area", "stretch_name").alias("score_area"),
        plds.str_jw("subarea", "stretch_name").alias("score_sub"),
        (1 - plds.str_jw("tollbooth_name", "stretch_name")).alias("score_tb"),
        (plds.str_lcs_subseq_dist("tollbooth_name", "stretch_name")).alias("score_tb_lcs"),
    )
    ldf_map_stretch = ldf_map_stretch.with_columns(
        pl.mean_horizontal("score_area", "score_sub", "score_tb", "score_tb_lcs").alias("score_best")
    )
    ldf_map_stretch = ldf_map_stretch.filter(
       pl.col("score_best") == pl.col("score_best").max().over("tollbooth_id_in", "tollbooth_id_out")
    )
    ldf_map_stretch = ldf_map_stretch.filter(pl.col("score_best") > 0.35)
    ldf_map_stretch = ldf_map_stretch.select("stretch_id", "tollbooth_id_in", "tollbooth_id_out")

    print(ldf_map_stretch.collect().shape)
    ldf_tb_stretch_id_patch = pl.scan_csv(data_model_pub.tb_stretch_id_patch.csv).cast({"stretch_id": pl.UInt32})
    ldf_tb_stretch_id = ldf_map_stretch.update(
        ldf_tb_stretch_id_patch, on="stretch_id", how="full"
    ).unique()
    print(ldf_tb_stretch_id.collect().shape)

    ldf_stretch_no_tb = ldf_stretch.join(ldf_tb_stretch_id, on="stretch_id", how="anti")
    ldf_stretch_no_tb = ldf_stretch_no_tb.select("stretch_id")
    ldf_stretch_no_tb = ldf_stretch_no_tb.with_columns(
        pl.lit(None).alias("tollbooth_id_in"),
        pl.lit(None).alias("tollbooth_id_out")
    )
    ldf_tb_stretch_id = pl.concat([ldf_tb_stretch_id, ldf_stretch_no_tb])
    ldf_tb_stretch_id.sink_parquet(data_model_move_year.tb_stretch_id.parquet)


def tb_stretch_id_sts(base_year: int, move_year: int):
    data_model_base = DataModel(base_year, DataStage.stg)
    data_model_sts = DataModel(move_year, DataStage.prd)
    data_model_pub = DataModel(base_year, DataStage.pub)
    
    ldf_tb_sts = (
        pl.scan_parquet(data_model_sts.tb_sts.parquet)
        .select("tollbooth_id", "tollbooth_name", "stretch_name")
        .rename({"stretch_name": "stretch_name_sts", "tollbooth_name": "tollbooth_sts_name"})
    )
    ldf_tb = (
        pl.scan_parquet(data_model_base.tollbooths.parquet)
        .select("tollbooth_id", "tollbooth_name")
    )
    ldf_stretch = (
        pl.scan_parquet(data_model_base.stretchs.parquet)
        .select("stretch_id", "stretch_name")
    )
    ldf_tb_stretch_id = pl.scan_parquet(data_model_base.tb_stretch_id.parquet)
    ldf_neighbour = pl.scan_parquet(
        data_model_base.tb_neighbour.parquet
    )
    ldf_map_tb_id = (
        ldf_neighbour
        .filter(pl.col("scope") == "local-sts")
        .filter(pl.col("distance") <= 1)
        .select(pl.exclude("scope"))
        .rename({"neighbour_id": "tollbooth_sts_id"})
    )

    ldf_tb_stretch_id_in = (
        ldf_tb_stretch_id.join(ldf_map_tb_id, left_on="tollbooth_id_in", right_on="tollbooth_id", how="left")
        .rename({"tollbooth_id_in": "tollbooth_id"})
        .select(pl.exclude("tollbooth_id_out"))
    )
    ldf_tb_stretch_id_out = (
        ldf_tb_stretch_id.join(ldf_map_tb_id, left_on="tollbooth_id_out", right_on="tollbooth_id")
        .rename({"tollbooth_id_out": "tollbooth_id"})
        .select(pl.exclude("tollbooth_id_in"))
    )
    ldf_tb_stretch_map = pl.concat([ldf_tb_stretch_id_in, ldf_tb_stretch_id_out]).unique()
    ldf_tb_stretch_map = ldf_tb_stretch_map.join(ldf_tb, on="tollbooth_id")
    ldf_tb_stretch_map = ldf_stretch.join(ldf_tb_stretch_map, on="stretch_id", how="left")
    ldf_tb_stretch_sts = ldf_tb_stretch_map.join(ldf_tb_sts, left_on="tollbooth_sts_id", right_on="tollbooth_id")

    ldf_tb_stretch_sts = ldf_tb_stretch_sts.with_columns(
        plds.str_jw("stretch_name", "stretch_name_sts").alias("score_st"),
        (plds.str_lcs_subseq_dist("stretch_name", "stretch_name_sts")*.6).alias("score_st_lcs"),
        plds.str_jw("tollbooth_sts_name", "tollbooth_name").alias("score_tb"),
        (1 - pl.col("distance")).alias("score_distance_inv")
    )
    ldf_tb_stretch_sts = ldf_tb_stretch_sts.with_columns(
        pl.mean_horizontal("score_st", "score_st_lcs", "score_tb", "score_distance_inv").alias("score_mean")
    )
    ldf_tb_stretch_sts = ldf_tb_stretch_sts.filter(
        pl.col("score_mean") == pl.col("score_mean").max().over("tollbooth_sts_id")
    )
    ldf_tb_stretch_sts = (
        ldf_tb_stretch_sts
        .select("stretch_id", "tollbooth_id", "tollbooth_sts_id")
    )
    try:
        ldf_tb_stretch_id_patch = pl.scan_csv(data_model_pub.tb_sts_stretch_id_patch.csv).cast({"tollbooth_sts_id": pl.UInt16, "stretch_id": pl.UInt32})
        ldf_tb_stretch_sts = ldf_tb_stretch_sts.update(
            ldf_tb_stretch_id_patch, on="tollbooth_sts_id", how="full"
        ).unique()
    except FileNotFoundError as e:
        print("Patch not found. Skip.")
    else:
        print("Applying patch.")

    ldf_stretch_no_tb = ldf_stretch.join(ldf_tb_stretch_sts, on="stretch_id", how="anti")
    ldf_stretch_no_tb = ldf_stretch_no_tb.with_columns(
        pl.lit(None).alias("tollbooth_id"),
        pl.lit(None).alias("tollbooth_sts_id")
    ).select("stretch_id", "tollbooth_id", "tollbooth_sts_id")
    ldf_tb_stretch_sts = pl.concat([ldf_tb_stretch_sts, ldf_stretch_no_tb])
    ldf_tb_stretch_sts.sink_parquet(data_model_base.tb_sts_stretch_id.parquet)


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


def map_tb_id(year: int):
    data_model = DataModel(year, DataStage.stg)

    ldf_tb = pl.scan_parquet(
        data_model.tollbooths.parquet
    ).select("tollbooth_id")

    ldf_neighbour = pl.scan_parquet(
        data_model.tb_neighbour.parquet
    )
    
    # --- IMT Mapping: ensure 1:1 relationship between tollbooth_id and neighbour_id ---
    ldf_neighbour_imt = (
        ldf_neighbour
        .filter(pl.col("scope") == "local-imt")
        .filter(pl.col("distance") <= 0.3)
        .select(pl.exclude("scope", "info_year"))
    )
    closest_tb_imt = []
    for ldf in find_closest_tb(ldf_neighbour_imt):
        closest_tb_imt.append(ldf)

    ldf_map_tb_imt = pl.concat(closest_tb_imt)

    # Rename for clarity
    ldf_map_tb_imt = (
        ldf_map_tb_imt
        .rename({"neighbour_id": "tollbooth_imt_id"})
        .select(pl.exclude("distance"))
    )

    # --- STS Mapping: ensure each origin ID gets exactly one closest neighbour ---
    ldf_neighbour_sts = (
        ldf_neighbour
        .filter(pl.col("scope") == "local-sts")
        .filter(pl.col("distance") <= 1)
        .select(pl.exclude("scope", "info_year"))
    )
    closest_tb_sts = []
    for ldf in find_closest_tb(ldf_neighbour_sts):
        closest_tb_sts.append(ldf)
    
    ldf_map_tb_sts = pl.concat(closest_tb_sts)
    ldf_map_tb_sts = (
        ldf_map_tb_sts
        .rename({"neighbour_id": "tollbooth_sts_id"})
        .select(pl.exclude("distance"))
    )

    ldf_tb = ldf_tb.join(ldf_map_tb_imt, on="tollbooth_id", how="left")
    ldf_tb = ldf_tb.join(ldf_map_tb_sts, on="tollbooth_id", how="left")

    ldf_tb.sink_parquet(data_model.map_tb_id.parquet)
    print(f"Saved result in {data_model.map_tb_id.parquet}")


def tb_imt_stretch_id_rel(year: int):
    """
    Mapping tool to get stretch_id and imt tollbooth
    """
    data_model = DataModel(year, DataStage.stg)
    toll_columns = [
        "motorbike", "car", "car_axle",
        "bus_2_axle", "bus_3_axle", "bus_4_axle", "truck_2_axle",
        "truck_3_axle", "truck_4_axle", "truck_5_axle", "truck_6_axle", 
        "truck_7_axle", "truck_8_axle", "truck_9_axle", "load_axle"
    ]
    ldf_neighbour = (
        pl.scan_parquet(data_model.tb_neighbour.parquet)
        .select("tollbooth_id", "neighbour_id", "distance", "scope")
    )
    ldf_toll_imt = (
        pl.scan_parquet(data_model.tb_toll_imt.parquet)
        .select(["tollbooth_id_in", "tollbooth_id_out"]+toll_columns)
        .rename({"tollbooth_id_in": "tollbooth_imt_id_in", "tollbooth_id_out": "tollbooth_imt_id_out"})
    )
    ldf_stretch = (
        pl.scan_parquet(data_model.stretchs.parquet)
        .select("stretch_id", "stretch_name")
    )
    ldf_stretch_toll = (
        pl.scan_parquet(data_model.stretchs_toll.parquet)
        .select(
            ["stretch_id"] + toll_columns
        )
    )
    ldf_tb_imt = (
        pl.scan_parquet(data_model.tb_imt.parquet)
        .select("tollbooth_id", "tollbooth_name", "area", "subarea")
        .rename({"tollbooth_id": "tollbooth_imt_id"})
    )

    ldf_map_tb = (
        ldf_neighbour
        .filter(pl.col("scope") == "local-imt")
        .filter(pl.col("distance") <= 0.3)
        .select(pl.exclude("scope"))
        .rename({"neighbour_id": "tollbooth_imt_id"})
    )
    ldf_stretch_imt = (
        ldf_toll_imt
        .join(ldf_map_tb, left_on="tollbooth_imt_id_in", right_on="tollbooth_imt_id")
        .rename({"tollbooth_id": "tollbooth_id_in"})
        .filter(
            pl.col("distance") == pl.col("distance").min().over("tollbooth_imt_id_in")
        )
        .select(pl.exclude("distance"))
    )
    ldf_stretch_imt = (
        ldf_stretch_imt
        .join(ldf_map_tb, left_on="tollbooth_imt_id_out", right_on="tollbooth_imt_id")
        .rename({"tollbooth_id": "tollbooth_id_out"})
        .filter(
            pl.col("distance") == pl.col("distance").min().over("tollbooth_imt_id_out")
        )
        .select(pl.exclude("distance"))
    )
    ldf_tb_imt_stretch_id = (
        ldf_stretch_imt
        .join(ldf_tb_imt, left_on="tollbooth_imt_id_in", right_on="tollbooth_imt_id")
        .rename({"tollbooth_name": "tollbooth_name_in"})
        .join(ldf_tb_imt, left_on="tollbooth_imt_id_out", right_on="tollbooth_imt_id")
        .rename({"tollbooth_name": "tollbooth_name_out"})
    )

    ldf_stretch_toll = join_tb_stretch_id_imt(ldf_tb_imt_stretch_id, ldf_stretch_toll)
    ldf_stretch_toll = ldf_stretch_toll.select(
       "stretch_id", "tollbooth_id_in", "tollbooth_id_out",
       "tollbooth_imt_id_in", "tollbooth_imt_id_out", 
       "tollbooth_name_in", "tollbooth_name_out", "area", "subarea",
    )
    ldf_stretch_toll = ldf_stretch_toll.with_columns(
        (
            pl.when(pl.col("tollbooth_name_out") != pl.col("tollbooth_name_in"))
            .then(pl.col("tollbooth_name_out") + "_" + pl.col("tollbooth_name_in"))
            .otherwise(pl.col("tollbooth_name_out"))
            .alias("tollbooth_name")
        )
    )
    ldf_stretch_toll = ldf_stretch_toll.select(pl.exclude("tollbooth_name_in", "tollbooth_name_out"))
    ldf_stretch_toll = ldf_stretch_toll.join(ldf_stretch, on="stretch_id")
    ldf_stretch_toll = ldf_stretch_toll.with_columns(
        pl.col("area").replace("n_d", None),
        pl.col("subarea").replace("n_d", None)
    )
    ldf_stretch_toll = ldf_stretch_toll.with_columns(
        plds.str_jw("area", "stretch_name").alias("score_area"),
        plds.str_jw("subarea", "stretch_name").alias("score_sub"),
        (plds.str_jw("tollbooth_name", "stretch_name")).alias("score_tb"),
        (plds.str_lcs_subseq_dist("tollbooth_name", "stretch_name")).alias("score_tb_lcs"),
    )
    ldf_stretch_toll = ldf_stretch_toll.with_columns(
        pl.mean_horizontal("score_area", "score_sub", "score_tb", "score_tb_lcs").alias("score_best")
    )
    ldf_stretch_toll = ldf_stretch_toll.filter(
       pl.col("score_best") == pl.col("score_best").max().over("tollbooth_imt_id_in", "tollbooth_imt_id_out")
    )
    ldf_stretch_toll = ldf_stretch_toll.filter(pl.col("score_best") > 0.35)
    ldf_stretch_toll = (
        ldf_stretch_toll
        .select(
            "stretch_id", "tollbooth_imt_id_in", "tollbooth_imt_id_out", 
            "tollbooth_id_in", "tollbooth_id_out"
        )
    )
    ldf_stretch_toll = ldf_stretch_toll.sort("stretch_id")
    ldf_stretch_toll.sink_parquet(data_model.tb_imt_stretch_id.parquet)


def fill_toll_from_year(year: int, origin_year: int):
    data_model = DataModel(year, DataStage.stg)
    data_model_origin = DataModel(origin_year, DataStage.stg)

    ldf_tb_imt_stretch_id = (
        pl.scan_parquet(data_model.tb_imt_stretch_id.parquet)
        .filter(pl.col("stretch_id").is_not_null())
    )
    ldf_imt_toll = pl.scan_parquet(data_model_origin.tb_toll_imt.parquet)
    ldf_stretch_toll = (
        pl.scan_parquet(data_model_origin.stretchs_toll.parquet)
        .select(pl.exclude("info_year"))
    )

    ldf_tb_imt_stretch_id = ldf_tb_imt_stretch_id.join(ldf_stretch_toll, on="stretch_id", how="anti")
    empty_cols = [
        "truck_10_axle", "toll_ref", "motorbike_axle", "car_rush_hour", "car_evening_hour",
        "pedestrian", "bicycle", "car_rush_hour_2", "car_evening_hour_2", "car_morning_night_hour"
    ]
    pl_expr = []
    for col in empty_cols:
        pl_expr.append(pl.lit(None).alias(col))

    ldf_tb_imt_stretch_id = (
        ldf_tb_imt_stretch_id
        .join(
            ldf_imt_toll, 
            left_on=["tollbooth_imt_id_out", "tollbooth_imt_id_in"],
            right_on=["tollbooth_id_out", "tollbooth_id_in"]
        )
        .with_columns(pl_expr)
        .with_columns(
            toll_ref=(
                pl.when(pl.col("tollbooth_imt_id_out").is_not_null() & pl.col("tollbooth_imt_id_in").is_not_null())
                .then(pl.lit("imt"))
                .otherwise(pl.col("toll_ref"))
            )
        )
        .select(pl.exclude(
            "tollbooth_imt_id_out", "tollbooth_imt_id_in", "tollbooth_id_out", "tollbooth_id_in",
            "update_date", "info_year", "nombre_sal", "nombre_ent"
            )
        )
    )
    ldf_stretch_toll_fill = pl.concat([ldf_stretch_toll, ldf_tb_imt_stretch_id]).unique()
    ldf_stretch_toll_fill = ldf_stretch_toll_fill.sort("stretch_id")
    ldf_stretch_toll_fill.sink_csv(f"./tmp_data/stretchs_toll_{origin_year}.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", help="data year", required=True, type=int)
    parser.add_argument("--sts-no-tb", help="join tollbooths their statistics", required=False, type=int)
    parser.add_argument("--imt-no-tb", required=False, type=int)
    parser.add_argument("--tb-stretch-id-imt", required=False, type=int)
    parser.add_argument("--tb-stretch-id-sts", required=False, type=int)
    parser.add_argument("--similarity-toll", required=False, type=int)
    parser.add_argument("--id", required=False, type=int)
    parser.add_argument("--map-stretch-toll-imt", required=False)
    parser.add_argument("--map-tb-id", required=False, action="store_true")
    parser.add_argument("--tb-imt-stretch-id", required=False, action="store_true")
    parser.add_argument("--fill-toll", required=False, type=int)
    parser.add_argument("--tb-imt-stretch-id-rel", required=False, action="store_true")
    args = parser.parse_args()

    if args.sts_no_tb:
        sts_no_tb(args.year, args.sts_no_tb)
    elif args.imt_no_tb:
        imt_no_tb(args.year, args.imt_no_tb)
    elif args.tb_stretch_id_imt:
        tb_stretch_id_imt(args.year, args.tb_stretch_id_imt)
    elif args.tb_stretch_id_sts:
        tb_stretch_id_sts(args.year, args.tb_stretch_id_sts)
    elif args.similarity_toll:
        find_similarity_toll(args.year, args.similarity_toll, args.id)
    elif args.map_tb_id:
        map_tb_id(args.year)
    elif args.tb_imt_stretch_id_rel:
        tb_imt_stretch_id_rel(args.year)
    elif args.fill_toll:
        fill_toll_from_year(args.year, args.fill_toll)
    