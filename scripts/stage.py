import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import polars as pl
import polars_h3 as plh3
import polars_ds as plds
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
        columns = ldf_tb_sts_from.collect_schema().names()
        data_model_to = DataModel(to_year, DataStage.stg)
        ldf_tb_sts_to_base = pl.scan_parquet(
            data_model_to.tb_sts.parquet, 
        )

        ldf_tb_sts_from = ldf_tb_sts_from.with_columns(
            h3_cell=plh3.latlng_to_cell("lat", "lng", hex_resolution)
        )
        ldf_tb_sts_to_base = ldf_tb_sts_to_base.with_columns(
            h3_cell=plh3.latlng_to_cell("lat", "lng", hex_resolution)
        )

        ldf_tb_sts_to = ldf_tb_sts_to_base.with_columns(
            plh3.grid_disk("h3_cell", 1).alias("h3_disk")
        )

        ldf_tb_sts_to = ldf_tb_sts_to.explode("h3_disk")
        ldf_tb_sts_to = ldf_tb_sts_to.join(
            ldf_tb_sts_from, left_on="h3_disk", right_on="h3_cell", how="left"
        )
        ldf_tb_sts_to = ldf_tb_sts_to.with_columns(
            plds.str_jw("stretch_name", "stretch_name_right").alias("score_st"),
            (plds.str_lcs_subseq_dist("stretch_name", "stretch_name_right")*.6).alias("score_st_lcs"),
            plds.str_jw("tollbooth_name", "tollbooth_name_right").alias("score_tb"),
        )
        ldf_tb_sts_to = ldf_tb_sts_to.with_columns(
            pl.mean_horizontal("score_st", "score_st_lcs", "score_tb").alias("score_mean")
        )
        ldf_tb_sts_to = ldf_tb_sts_to.filter(
            pl.col("score_mean") == pl.col("score_mean").max().over("tollbooth_id")
        )

        ldf_tb_sts_from_to_new = ldf_tb_sts_to_base.join(
            ldf_tb_sts_to.select(columns).select(pl.exclude("tollbooth_id", "status")),
            on="index",
            how="anti"
        )
        
        columns[columns.index("index")] = "index_right"
        ldf_tb_sts_from_to_del = ldf_tb_sts_from.join(
            ldf_tb_sts_to.select(columns),
            left_on="index",
            right_on="index_right",
            how="anti"
        )

        ldf_tb_sts_from_to_del = ldf_tb_sts_from_to_del.with_columns(
            pl.lit("closed").alias("status")
        ).select(pl.exclude("h3_cell"))
        
        last_id = ldf_tb_sts_from.sort("tollbooth_id").last().select("tollbooth_id").collect().row(0)[0]
        ldf_tb_sts_from_to_new = _split_dup_and_add_id(ldf_tb_sts_from_to_new, start_index=last_id + 1)

        columns[columns.index("index_right")] = "index"
        ldf_all = pl.concat([ldf_tb_sts_to.select(columns), ldf_tb_sts_from_to_new, ldf_tb_sts_from_to_del])
        ldf_all = ldf_all.sort("tollbooth_id")
        #ldf_all.sink_csv(f"./tmp_data/tb_sts_{year}.csv")
    ldf_all.sink_parquet(DataModel(year, DataStage.prd).tb_sts.parquet)


def get_parent_manage() -> pl.DataFrame:
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
    
    df = df.rename({"parent": "parent_manage"})
    return df


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

    if option_selected == "road":
        date_format = "%d-%m-%Y"
        date_columns = {"operation_date": date_format}
    else:
        date_columns = None
    
    ldf, data_model = pipeline.simple_pub_stg(catalogs[option_selected], year, normalize, date_columns=date_columns)
    if option_selected == "tb":
        df_parent_manage = get_parent_manage()
        ldf = ldf.collect().join(df_parent_manage, left_on="manage", right_on="short_name", how="left")
        ldf.write_parquet(data_model.parquet)
    else:
        ldf.sink_parquet(data_model.parquet)
    print(f'Sink file: {data_model.parquet}')


def raw_to_stg(year: int, option_selected: str, normalize: bool):
    pipeline = DataPipeline()
    
    models = ["tb_imt", "tb_toll_imt"]
    options = ["tb_imt", "tb_toll_imt"]
    date_columns = None
    filter_exp = None

    if option_selected == "tb_imt":
        file_path = f"./tmp_data/plazas_{year}.csv"
        old_fields = [
            "ID_PLAZA", "ADMINISTRA", "NOMBRE", "SECCION", "SUBSECCION", "MODALIDAD",
            "FUNCIONAL", "FECHA_ACT", "CALIREPR", "ycoord", "xcoord"
        ]
        date_format = "%Y-%m-%d %H:%M:%S"
        date_columns = {"FECHA_ACT": date_format}
    
    elif option_selected == "tb_toll_imt":
        file_path = f"./tmp_data/tarifas_imt_{year}.csv"
        old_fields = [
            "ID_PLAZA", "ID_PLAZA_E", "NOMBRE_SAL", "NOMBRE_ENT",
            "T_MOTO", "T_AUTO", "T_EJE_LIG", "T_AUTOBUS2",
            "T_AUTOBUS3", "T_AUTOBUS4", "T_CAMION2", "T_CAMION3",
            "T_CAMION4", "T_CAMION5", "T_CAMION6", "T_CAMION7",
            "T_CAMION8", "T_CAMION9", "T_EJE_PES", "FECHA_ACT"
        ]
        if year == 2025:
           date_format = "%Y-%m-%d %H:%M:%S"
        elif year in [2020, 2021, 2022, 2023, 2024]:
            date_format = "%Y-%m-%d"

        date_columns = {"FECHA_ACT": date_format}
        filter_exp = (pl.col("FECHA_ACT") < date(year + 1, 1, 1))
    elif option_selected == "inflation":
        data_model = DataModel(year, DataStage.stg)
        file_path = f"./raw_data/inegi/monthly_inflation.csv"
        df = pl.read_csv(file_path).transpose(include_header=True).rename({"column": "year", "column_0": "value"})
        df = df.filter(pl.col("year").str.contains("/12"))
        df = df.with_columns(
            pl.col("year").str.replace(r"\/12", "")
        ).cast(data_model.inflation.schema)
        df.write_parquet(data_model.inflation.parquet)
        return
    
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
    parser.add_argument("--raw-to-stg", required=False, type=str, choices=("tb_imt", "tb_toll_imt", "inflation"))
    parser.add_argument("--normalize", required=False, action="store_true")

    args = parser.parse_args()
    if args.stg_to_prod:
        stg_to_prod(args.year, args.stg_to_prod)
    elif args.pub_to_stg:
        pub_to_stg(args.year, args.pub_to_stg, args.normalize)
    elif args.raw_to_stg:
        raw_to_stg(args.year, args.raw_to_stg, args.normalize)
