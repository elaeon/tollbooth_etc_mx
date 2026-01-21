import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import polars as pl
import polars_h3 as plh3
import argparse
from datetime import date

from tb_map_editor.model import TbImt, TbTollImt
from tb_map_editor.data_files import DataModel


def plazas(imt_year: int, model_year: int):
    data_model = DataModel(imt_year)
    ldf_tb_imt = pl.scan_csv(f"./tmp_data/plazas_{imt_year}.csv", infer_schema=False)
    actual_data_model = DataModel(model_year)
    ldf_tollbooth = pl.scan_parquet(actual_data_model.tollbooths.parquet)
    
    field_map = {
        "ID_PLAZA": "tollbooth_imt_id",
        "NOMBRE": "tollbooth_name",
        "SECCION": "area",
        "SUBSECCION": "subarea",
        "MODALIDAD": "type",
        "FUNCIONAL": "function",
        "CALIREPR": "calirepr",
        "ycoord": "lat",
        "xcoord": "lng",
        "state": "state"
    }
    ldf_tb_imt = ldf_tb_imt.rename(field_map)
    schema = TbImt.dict_schema()
    del schema["state"]
    ldf_tb_imt = ldf_tb_imt.cast(schema)
    
    hex_resolution = 8
    hex_resolution_name = "h3_cell"
    ldf_tb_imt = ldf_tb_imt.with_columns(
        plh3.latlng_to_cell("lat", "lng", hex_resolution).alias(hex_resolution_name)
    )
    ldf_tollbooth = ldf_tollbooth.with_columns(
        plh3.latlng_to_cell("lat", "lng", hex_resolution).alias(hex_resolution_name)
    )
    ldf_tb_imt = ldf_tb_imt.join(ldf_tollbooth.select(hex_resolution_name, "state"), on=hex_resolution_name, how="left")
    ldf_tb_imt = ldf_tb_imt.select(pl.exclude(hex_resolution_name)).unique()
    
    ldf_tb_unq = ldf_tb_imt.group_by("tollbooth_imt_id").first()
    ldf_tb_unq.select(list(field_map.values())).sink_parquet(data_model.tb_imt.parquet)


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
    df_toll_imt = df_toll_imt.with_columns(
        pl.col("FECHA_ACT").str.to_date("%Y-%m-%d %H:%M:%S")
    )
    df_toll_imt = df_toll_imt.filter(
        (pl.col("FECHA_ACT") >= date(year, 1, 1)) &
        (pl.col("FECHA_ACT") < date(year + 1, 1, 1))
    )
    
    field_map = {
        "ID_PLAZA": "tollbooth_imt_id_a",
        "ID_PLAZA_E": "tollbooth_imt_id_b",
        "T_MOTO" : "motorbike",
        "T_AUTO" : "car",
        "T_EJE_LIG" : "car_axle",
        "T_AUTOBUS2" : "bus_2_axle",
        "T_AUTOBUS3" : "bus_3_axle",
        "T_AUTOBUS4" : "bus_4_axle",
        "T_CAMION2" : "truck_2_axle",
        "T_CAMION3" : "truck_3_axle",
        "T_CAMION4" : "truck_4_axle",
        "T_CAMION5" : "truck_5_axle",
        "T_CAMION6" : "truck_6_axle",
        "T_CAMION7" : "truck_7_axle",
        "T_CAMION8" : "truck_8_axle",
        "T_CAMION9": "truck_9_axle",
        "T_EJE_PES": "load_axle"
    }

    df_toll_imt = df_toll_imt.with_columns(
        pl.lit(year).alias("info_year")
    )
    df_toll_imt = df_toll_imt.rename(field_map).cast(TbTollImt.dict_schema())
    df_toll_imt.select(list(field_map.values()) + ["info_year"]).write_parquet(data_model.tb_toll_imt.parquet)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--plazas", required=False, type=int)
    parser.add_argument("--tarifas", required=False, action="store_true")
    parser.add_argument("--tb-imt-delta", required=False, type=int)
    args = parser.parse_args()
    if args.plazas:
        plazas(args.year, args.plazas)
    elif args.tarifas:
        tarifas(args.year)
    elif args.tb_imt_delta:
        tb_imt_delta(args.year, args.tb_imt_delta)
