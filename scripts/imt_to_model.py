import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import polars as pl
import polars_h3 as plh3
import argparse
from datetime import date

from tb_map_editor.model import TbImt, TbTollImt
from tb_map_editor.data_files import DataModel


def plazas(year: int):
    data_model = DataModel(year)
    ldf_tb_imt = pl.scan_csv(f"./tmp_data/plazas_{year}.csv", infer_schema=False)
    
    fields = data_model.tb_imt.model.dict_schema()
    old_fields = [
        "ID_PLAZA", "NOMBRE", "SECCION", "SUBSECCION", "MODALIDAD",
        "FUNCIONAL", "CALIREPR", "ycoord", "xcoord"
    ]
    field_map = {}
    for old_name, new_name in zip(old_fields, fields.keys()):
        field_map[old_name] = new_name
    ldf_tb_imt = ldf_tb_imt.rename(field_map)

    ldf_tb_imt = ldf_tb_imt.with_columns(data_model.tb_imt.model.str_normalize())
    ldf_tb_imt = ldf_tb_imt.with_columns(
        pl.lit(year).alias("info_year")
    )

    schema = TbImt.dict_schema()
    ldf_tb_imt = ldf_tb_imt.cast(schema)
    ldf_tb_imt.select(list(schema.keys())).sink_parquet(data_model.tb_imt.parquet)


def tb_imt_delta(base_year:int, next_year: int):
    base_data_model = DataModel(base_year)
    next_data_model = DataModel(next_year)
    ldf_tb_imt = pl.scan_parquet(base_data_model.tb_imt.parquet)
    ldf_tb_imt_up = pl.scan_parquet(next_data_model.tb_imt.parquet)
    ldf_tb_imt_new = ldf_tb_imt_up.join(ldf_tb_imt, on="tollbooth_imt_id", how="anti")
    ldf_tb_imt_new = ldf_tb_imt_new.with_columns(
        pl.lit("new").alias("status")
    )
    ldf_tb_imt_closed = ldf_tb_imt.join(ldf_tb_imt_up, on="tollbooth_imt_id", how="anti")
    ldf_tb_imt_closed = ldf_tb_imt_closed.with_columns(
        pl.lit("closed").alias("status")
    )
    pl.concat(
        [ldf_tb_imt_new, ldf_tb_imt_closed], how="vertical"
    ).sink_parquet(next_data_model.tb_imt_delta.parquet)


def tarifas(year: int):
    data_model = DataModel(year)
    df_toll_imt = pl.read_csv(f"./tmp_data/tarifas_imt_{year}.csv", infer_schema=False)
    if year == 2025:
        date_format = "%Y-%m-%d %H:%M:%S"
    elif year in [2020, 2021, 2022, 2023, 2024]:
        date_format = "%Y-%m-%d"

    df_toll_imt = df_toll_imt.with_columns(
        pl.col("FECHA_ACT").str.to_date(date_format)
    )
    df_toll_imt = df_toll_imt.filter(
        (pl.col("FECHA_ACT") < date(year + 1, 1, 1))
    )
    fields = data_model.tb_toll_imt.model.dict_schema()
    old_fields = [
        "ID_PLAZA", "ID_PLAZA_E", "NOMBRE_SAL", "NOMBRE_ENT",
        "T_MOTO", "T_AUTO", "T_EJE_LIG", "T_AUTOBUS2",
        "T_AUTOBUS3", "T_AUTOBUS4", "T_CAMION2", "T_CAMION3",
        "T_CAMION4", "T_CAMION5", "T_CAMION6", "T_CAMION7",
        "T_CAMION8", "T_CAMION9", "T_EJE_PES"
    ]
    field_map = {}
    for old_name, new_name in zip(old_fields, fields.keys()):
        field_map[old_name] = new_name
    
    df_toll_imt = df_toll_imt.with_columns(data_model.tb_toll_imt.model.str_normalize())
    df_toll_imt = df_toll_imt.with_columns(
        pl.lit(year).alias("info_year")
    )
    schema = TbTollImt.dict_schema()
    df_toll_imt = df_toll_imt.rename(field_map).cast(schema)
    df_toll_imt.select(list(schema.keys())).write_parquet(data_model.tb_toll_imt.parquet)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--plazas", required=False, action="store_true")
    parser.add_argument("--tarifas", required=False, action="store_true")
    parser.add_argument("--tb-imt-delta", required=False, type=int)
    args = parser.parse_args()
    if args.plazas:
        plazas(args.year)
    elif args.tarifas:
        tarifas(args.year)
    elif args.tb_imt_delta:
        tb_imt_delta(args.year, args.tb_imt_delta)
