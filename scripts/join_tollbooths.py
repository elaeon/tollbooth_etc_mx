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


def _tb_stretch_id_imt(ldf_toll_imt, ldf_stretch_toll):
    ldf_stretch_toll = ldf_stretch_toll.with_columns(
        pl.all().fill_null(strategy="zero"),
    ).with_columns(
        pl.col("car").cast(pl.String)
    )
    ldf_toll_imt = ldf_toll_imt.with_columns(
        pl.col("car").cast(pl.String)
    )
    ldf_stretch = ldf_toll_imt.join(
        ldf_stretch_toll, on=[
            "motorbike", "car", "bus_2_axle", "bus_3_axle", "bus_4_axle",
            "truck_2_axle", "truck_3_axle", "truck_4_axle", "truck_5_axle",
            "truck_6_axle", "truck_7_axle", "truck_8_axle", "truck_9_axle"
        ]
    )
    ldf_stretch = ldf_stretch.select(
        "stretch_id", "tollbooth_id_in", "tollbooth_id_out", "area", "subarea", "tollbooth_name"
    ).unique()
    return ldf_stretch


def tb_stretch_id_imt(base_year: int, move_year: int):
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
    ldf_map_stretch = _tb_stretch_id_imt(ldf_toll_imt, ldf_stretch_toll)
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
    
    ldf_tb_sts = pl.scan_parquet(data_model_sts.tb_sts.parquet).select("tollbooth_id", "tollbooth_name", "stretch_name").rename({"stretch_name": "stretch_name_sts"})
    ldf_stretch = pl.scan_parquet(data_model_base.stretchs.parquet).select("stretch_id", "stretch_name")
    ldf_tb_stretch_id = pl.scan_parquet(data_model_base.tb_stretch_id.parquet)
    ldf_map_tb_id = pl.scan_parquet(data_model_base.map_tb_id.parquet).select("tollbooth_id", "tollbooth_sts_id")

    ldf_tb_stretch_name = ldf_stretch.join(ldf_tb_stretch_id, on="stretch_id")
    ldf_tb_stretch_name_in = ldf_map_tb_id.join(
        ldf_tb_stretch_name, left_on="tollbooth_id", right_on="tollbooth_id_in", how="left",
    ).select(pl.exclude("tollbooth_id_out"))
    ldf_tb_stretch_name_out = ldf_map_tb_id.join(
        ldf_tb_stretch_name, left_on="tollbooth_id", right_on="tollbooth_id_out", how="left",
    ).select(pl.exclude("tollbooth_id_in"))
    ldf_tb_stretch_sts = pl.concat([ldf_tb_stretch_name_in, ldf_tb_stretch_name_out]).unique()
    ldf_tb_stretch_sts = ldf_tb_stretch_sts.join(ldf_tb_sts, left_on="tollbooth_sts_id", right_on="tollbooth_id")

    ldf_tb_stretch_sts = ldf_tb_stretch_sts.with_columns(
        plds.str_jaccard("stretch_name", "stretch_name_sts").alias("score_jac"),
        plds.str_jw("stretch_name", "stretch_name_sts").alias("score_jw")
    )
    ldf_tb_stretch_sts = ldf_tb_stretch_sts.with_columns(
        pl.mean_horizontal("score_jac", "score_jw").alias("score_best")
    )
    ldf_tb_stretch_sts = ldf_tb_stretch_sts.filter(
        pl.col("score_best") == pl.col("score_best").max().over("stretch_id")
    )
    ldf_tb_stretch_sts = ldf_tb_stretch_sts.filter(
        pl.col("score_best") == pl.col("score_best").max().over("tollbooth_sts_id")
    )

    ldf_tb_stretch_sts = (
        ldf_tb_stretch_sts
        .select("stretch_id", "tollbooth_id", "tollbooth_sts_id")
    )

    ldf_stretch_no_tb = ldf_stretch.join(ldf_tb_stretch_sts, on="stretch_id", how="anti")
    ldf_stretch_no_tb = ldf_stretch_no_tb.with_columns(
        pl.lit(None).alias("tollbooth_id"),
        pl.lit(None).alias("tollbooth_sts_id")
    ).select("stretch_id", "tollbooth_id", "tollbooth_sts_id")
    ldf_tb_stretch_sts = pl.concat([ldf_tb_stretch_sts, ldf_stretch_no_tb])
    ldf_tb_stretch_sts.sink_parquet(data_model_base.tbsts_stretch_id.parquet)


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


def _find_closest_tb(ldf_neighbour):
    i = 0
    while i < 2:
        # Step 1: Find closest neighbour for each origin (tollbooth_id)
        ldf_neighbour_closest = ldf_neighbour.filter(
            pl.col("distance") == pl.col("distance").min().over("tollbooth_id")
        )
        # Step 2: Ensure each neighbour_id is matched to at most one tollbooth_id 
        #         (if multiple tollbooths have same closest neighbour, keep the one with smallest distance)
        ldf_neighbour_unique = ldf_neighbour_closest.filter(
            pl.col("distance") == pl.col("distance").min().over("neighbour_id")
        )
        # Step 3: Get each tollbooth and neighbour left without a close match
        ldf_ids_unmatched_tb = ldf_neighbour.join(
            ldf_neighbour_unique, on=["tollbooth_id"], how="anti"
        )
        ldf_ids_unmatched_nb = ldf_neighbour.join(
            ldf_neighbour_unique, on=["neighbour_id"], how="anti"
        )
        ldf_imt_ids_unmatched = ldf_ids_unmatched_tb.join(
            ldf_ids_unmatched_nb, on=["tollbooth_id", "neighbour_id"]
        ).select(pl.exclude("distance_right"))
        # Step 4: Do another search with the remaining relations
        ldf_neighbour = ldf_imt_ids_unmatched
        i = i + 1
        yield ldf_neighbour_unique


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
        .select(pl.exclude("scope"))
    )
    closest_tb_imt = []
    for ldf in _find_closest_tb(ldf_neighbour_imt):
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
        .select(pl.exclude("scope"))
    )
    closest_tb_sts = []
    for ldf in _find_closest_tb(ldf_neighbour_sts):
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
    