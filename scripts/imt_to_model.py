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
    next_year = data_model.attr.get("year") + 1
    actual_data_model = DataModel(next_year)
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


def tarifas(year: int):
    data_model = DataModel(year)
    df_toll_imt = pl.read_csv(f"./tmp_data/tarifas_imt_{year}.csv", infer_schema=False)
    df_toll_imt = df_toll_imt.cast({"FECHA_ACT,C,10": pl.Date}).filter(pl.col("FECHA_ACT,C,10") >= date(year, 1, 1))
    
    field_map = {
        "ID_PLAZA,N,11,0": "tollbooth_imt_id_a",
        "ID_PLAZA_E,N,11,0": "tollbooth_imt_id_b",
        "T_MOTO,N,32,10" : "motorbike",
        "T_AUTO,N,32,10" : "car",
        "T_EJE_LIG,N,32,10" : "car_axle",
        "T_AUTOBUS2,N,32,10" : "bus_2_axle",
        "T_AUTOBUS3,N,32,10" : "bus_3_axle",
        "T_AUTOBUS4,N,32,10" : "bus_4_axle",
        "T_CAMION2,N,32,10" : "truck_2_axle",
        "T_CAMION3,N,32,10" : "truck_3_axle",
        "T_CAMION4,N,32,10" : "truck_4_axle",
        "T_CAMION5,N,32,10" : "truck_5_axle",
        "T_CAMION6,N,32,10" : "truck_6_axle",
        "T_CAMION7,N,32,10" : "truck_7_axle",
        "T_CAMION8,N,32,10" : "truck_8_axle",
        "T_CAMION9,N,32,10": "truck_9_axle",
        "T_EJE_PES,N,32,10": "load_axle"
    }

    df_toll_imt = df_toll_imt.with_columns(
        pl.lit(year).alias("info_year")
    )
    df_toll_imt = df_toll_imt.rename(field_map).cast(TbTollImt.dict_schema())
    df_toll_imt.select(list(field_map.values()) + ["info_year"]).write_parquet(data_model.tb_toll_imt.parquet)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--plazas", required=False, action="store_true")
    parser.add_argument("--tarifas", required=False, action="store_true")
    args = parser.parse_args()
    if args.plazas:
        plazas(args.year)
    elif args.tarifas:
        tarifas(args.year)
    