import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import polars as pl
import argparse
from datetime import date

from tb_map_editor.model import TbImt, TollImt


def plazas(year: int):
    df_tb_imt = pl.read_csv(f"./tmp_data/plazas_{year}.csv", infer_schema=False)
    field_map = {
        "ID_PLAZA": "tollbooth_imt_id",
        "NOMBRE": "tollbooth_name",
        "SECCION": "area",
        "SUBSECCION": "subarea",
        "MODALIDAD": "type",
        "FUNCIONAL": "function",
        "CALIREPR": "calirepr",
        "ycoord": "lat",
        "xcoord": "lon"
    }
    df_tb_imt = df_tb_imt.rename(field_map).cast(TbImt.dict_schema())
    df_tb_imt.select(list(field_map.values())).write_parquet("./tmp_data/plazas.parquet")


def tarifas(year: int):
    df_toll_imt = pl.read_csv("./tmp_data/tarifas_imt.csv", infer_schema=False)
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
    df_toll_imt = df_toll_imt.rename(field_map).cast(TollImt.dict_schema())
    df_toll_imt.select(list(field_map.values()) + ["info_year"]).write_parquet(f"./tmp_data/tarifas_imt_{year}.parquet")


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
    