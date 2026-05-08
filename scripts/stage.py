import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import polars as pl
import polars_h3 as plh3
import polars_ds as plds

from datetime import date
from tb_map_editor import model as tb_model
from tb_map_editor.data_files import DataModel, DataStage, PathModel
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
            data_model_from.tb_sts_no_id.parquet
        )
        ldf_tb_sts_from = ldf_tb_sts_from.with_columns(
            h3_cell=plh3.latlng_to_cell("lat", "lng", hex_resolution)
        )
        ldf_all = _split_dup_and_add_id(ldf_tb_sts_from, start_index=1)
    else:
        data_model_from = DataModel(from_year, DataStage.stg)
        ldf_tb_sts_from = pl.scan_parquet(
            data_model_from.tb_sts.parquet,
        )
        columns = ldf_tb_sts_from.collect_schema().names()
        data_model_to = DataModel(to_year, DataStage.stg)
        ldf_tb_sts_to_base = pl.scan_parquet(
            data_model_to.tb_sts_no_id.parquet,
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
        ldf_tb_sts_to = ldf_tb_sts_to.filter(
            pl.col("index") == pl.col("index").min().over("tollbooth_id")
        )
        ldf_tb_sts_to = ldf_tb_sts_to.with_columns(
            status=pl.when(
                pl.col("tollbooth_id")==pl.col("tollbooth_id").min().over("index")
            ).then(pl.col("status")).otherwise(pl.lit("closed"))

        )
        ldf_tb_sts_from_to_new = ldf_tb_sts_to_base.join(
            ldf_tb_sts_to.select(columns).filter(pl.col("status")=="open").select(pl.exclude("tollbooth_id", "status")),
            on="index",
            how="anti"
        )

        columns[columns.index("index")] = "index_right"
        ldf_tb_sts_from_to_del = ldf_tb_sts_from.join(
            ldf_tb_sts_to.select(columns).filter(pl.col("status")=="open"),
            on="tollbooth_id",
            how="anti"
        )
        ldf_tb_sts_from_to_del = (
            ldf_tb_sts_from_to_del
            .with_columns(
                pl.lit("closed").alias("status")
            )
            .select(pl.exclude("h3_cell"))
        )
        last_id = ldf_tb_sts_from.sort("tollbooth_id").last().select("tollbooth_id").collect().row(0)[0]
        ldf_tb_sts_from_to_new = _split_dup_and_add_id(ldf_tb_sts_from_to_new, start_index=last_id + 1)

        columns[columns.index("index_right")] = "index"
        ldf_all = pl.concat([ldf_tb_sts_to.select(columns).unique(), ldf_tb_sts_from_to_new, ldf_tb_sts_from_to_del])
        ldf_all = (
            ldf_all
            .filter(
                pl.col("info_year") == pl.col("info_year").min().over("tollbooth_id")
            )
        )
        ldf_all = ldf_all.sort("tollbooth_id")

    ldf_all.sink_parquet(DataModel(year, DataStage.stg).tb_sts.parquet)


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


def pub_to_stg(pub: PathModel, stg: PathModel, normalize: bool):
    pipeline = DataPipeline()

    if stg.model == tb_model.Road:
        date_format = "%d-%m-%Y"
        date_columns = {
            "operation_date": date_format,
            "start_contract_date": date_format,
            "end_contract_date": date_format,
        }
    else:
        date_columns = None

    lf = pipeline.simple_pub_stg(pub, normalize, date_columns=date_columns)

    if stg.model == tb_model.Tollbooth:
        df_parent_manage = get_parent_manage()
        lf = lf.collect().join(df_parent_manage, left_on="manage", right_on="short_name", how="left")
        lf.write_parquet(stg.parquet)
    elif stg.model == tb_model.OsmTbDistance:
        lf = lf.filter(pl.col("distance") == pl.col("distance").max().over("stretch_id"))
        lf = lf.with_columns(pl.col("distance").round(2))
        lf.sink_parquet(stg.parquet)
    elif stg.model == tb_model.Stretch:
        lf_osm_tb_distance = (
            pl.scan_parquet(DataModel(stg.attr["year"], DataStage.stg).osm_tb_distance.parquet)
            .select("stretch_id", "distance")
            .rename({"distance": "stretch_length_km"})
        )
        lf = (
            lf
            .join(lf_osm_tb_distance, on="stretch_id", how="left")
            .with_columns(
               stretch_length_km=(
                   pl.when(pl.col("stretch_length_km_right").is_null())
                   .then(pl.col("stretch_length_km"))
                   .otherwise(pl.col("stretch_length_km_right"))
               )
            )
        )
        lf = lf.select(stg.model.dict_schema().keys())
        lf.sink_parquet(stg.parquet)
    else:
        lf.sink_parquet(stg.parquet)
    return lf


def raw_to_stg(pub: PathModel, stg: PathModel, normalize: bool):
    pipeline = DataPipeline()
    year = stg.attr["year"]

    date_columns = None
    filter_expr = None
    extra_expr = None
    extra_pipe = None

    if stg.model == tb_model.TbImt:
        file_path = f"./raw_data/toll_data/imt/plazas_{year}.csv"
        old_fields = [
            "ID_PLAZA", "ADMINISTRA", "NOMBRE", "SECCION", "SUBSECCION", "MODALIDAD",
            "FUNCIONAL", "FECHA_ACT", "CALIREPR", "ycoord", "xcoord"
        ]
        date_columns = {"FECHA_ACT": "%Y-%m-%d %H:%M:%S"}

    elif stg.model == tb_model.TbTollImt:
        file_path = f"./raw_data/toll_data/imt/tarifas_imt_{year}.csv"
        old_fields = [
            "ID_PLAZA", "ID_PLAZA_E", "NOMBRE_SAL", "NOMBRE_ENT",
            "T_MOTO", "T_AUTO", "T_EJE_LIG", "T_AUTOBUS2",
            "T_AUTOBUS3", "T_AUTOBUS4", "T_CAMION2", "T_CAMION3",
            "T_CAMION4", "T_CAMION5", "T_CAMION6", "T_CAMION7",
            "T_CAMION8", "T_CAMION9", "T_EJE_PES", "FECHA_ACT"
        ]
        date_format = "%Y-%m-%d" if year in [2020, 2021, 2022, 2023, 2024] else "%Y-%m-%d %H:%M:%S"
        date_columns = {"FECHA_ACT": date_format}
        filter_expr = pl.col("FECHA_ACT") < date(year + 1, 1, 1)

    elif stg.model == tb_model.Inflation:
        lf = pl.scan_csv("./raw_data/inegi/monthly_inflation.csv")
        lf = (
            lf
            .with_columns(
                pl.col("year").str.split("/").list.to_struct(fields=["year", "month"])
            )
            .unnest("year")
            .sort("year", "month")
            .group_by("year", maintain_order=True)
            .last()
        )
        lf = lf.cast(stg.schema)
        lf.sink_parquet(stg.parquet)
        return

    elif stg.model == tb_model.ManagerRevenue:
        file_path = f"./raw_data/capufe/capufe_ingresos_{year}.csv"
        numeric_cols = stg.model.numeric_cols()
        extra_expr = [pl.col(numeric_cols).str.replace("-", "0"), pl.lit("capufe").alias("manager")]
        old_fields = [
            "Tramo", "Enero", "Febrero", "Marzo",
            "Abril", "Mayo", "Junio", "Julio",
            "Agosto", "Septiembre", "Octubre", "Noviembre",
            "Diciembre",
        ]
        def extra_pipe_fn(lf: pl.LazyFrame):
            lf = (
                lf
                .unique()
                .group_by("stretch_name")
                .agg([pl.col(col).sum().alias(col) for col in numeric_cols])
                .sort("stretch_name")
            )
            return lf
        extra_pipe = extra_pipe_fn
    else:
        file_path = ""
        old_fields = []

    lf = pipeline.simple_raw_stg(
        pub,
        file_path,
        old_fields,
        date_columns=date_columns,
        filter_exp=filter_expr,
        normalize=normalize,
        extra_expr=extra_expr
    )

    if extra_pipe is not None:
        lf = lf.pipe(extra_pipe)
    lf.sink_parquet(stg.parquet)
    return lf
