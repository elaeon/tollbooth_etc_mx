import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import polars as pl
import argparse

from datetime import date
from tb_map_editor.data_files import DataModel, DataStage


def calc_inflation_rate(filepath, from_year, to_year):
    years = range(from_year, to_year + 1)
    df_strech_toll_dict = {}
    for year in years:
        df_strech_toll_dict[year] = {"dm": DataModel(year, DataStage.stg).stretchs_toll.parquet}
    
    actual_data_model = DataModel(to_year, DataStage.stg)
    df_strechs = pl.scan_parquet(actual_data_model.stretchs.parquet)
    df_tollbooths = pl.scan_parquet(actual_data_model.tollbooths.parquet).select("tollbooth_id", "state", "manage")
    df_tb_stretch_id = pl.scan_parquet(actual_data_model.tb_stretch_id.parquet).select("stretch_id", "tollbooth_id_a")
    
    df_tb_stretch_id = df_tb_stretch_id.join(
        df_tollbooths, left_on="tollbooth_id_a", right_on="tollbooth_id", how="left"
    )
    for year in df_strech_toll_dict:
        df_strech_toll_dict[year]["df"] = pl.scan_parquet(df_strech_toll_dict[year]["dm"])

    df_calc_dict = {}
    for year in df_strech_toll_dict:
        df_calc_dict[year] = df_strech_toll_dict[year]["df"].select(
            "stretch_id", "motorbike", "car", "car_axle", "bus_2_axle", "bus_3_axle",
            "bus_4_axle", "truck_2_axle", "truck_3_axle", "truck_4_axle",
            "truck_5_axle", "truck_6_axle", "truck_7_axle", "truck_8_axle",
            "truck_9_axle", "load_axle"
        ).rename({"car": f"car_{year}", "motorbike": f"bike_{year}"})
        df_calc_dict[year] = df_calc_dict[year].fill_null(0)
        df_calc_dict[year] = df_calc_dict[year].with_columns(pl.sum_horizontal(
            f"bike_{year}", f"car_{year}", "bus_2_axle", "bus_3_axle", "bus_4_axle", "truck_2_axle", 
            "truck_3_axle", "truck_4_axle", "truck_5_axle", "truck_6_axle", "truck_7_axle",
            "truck_8_axle", "truck_9_axle", "car_axle", "load_axle"
        ).alias(f"total_{year}"))
        df_calc_dict[year] = df_calc_dict[year].select(pl.exclude("car_axle", "load_axle"))
        df_calc_dict[year] = df_calc_dict[year].with_columns(
            pl.mean_horizontal("bus_2_axle", "bus_3_axle", "bus_4_axle").alias(f"bus_{year}")
        ).select(pl.exclude("bus_2_axle", "bus_3_axle", "bus_4_axle"))
        df_calc_dict[year] = df_calc_dict[year].with_columns(
            pl.mean_horizontal("truck_2_axle", "truck_3_axle").alias(f"light_truck_{year}")
        ).select(pl.exclude("truck_2_axle", "truck_3_axle"))
        df_calc_dict[year] = df_calc_dict[year].with_columns(
            pl.mean_horizontal("truck_4_axle", "truck_5_axle", "truck_6_axle").alias(f"heavy_truck_{year}")
        ).select(pl.exclude("truck_4_axle", "truck_5_axle", "truck_6_axle"))
        df_calc_dict[year] = df_calc_dict[year].with_columns(
            pl.mean_horizontal("truck_7_axle", "truck_8_axle", "truck_9_axle").alias(f"uheavy_truck_{year}")
        ).select(pl.exclude("truck_7_axle", "truck_8_axle", "truck_9_axle"))

    df_toll = df_calc_dict[from_year]
    for year in years[1:]:
        df_toll = df_toll.join(df_calc_dict[year], how="left", on="stretch_id")

    df_toll = df_toll.join(df_strechs.select("stretch_id", "stretch_name", "road_id"), on="stretch_id")
    df_toll = df_toll.join(df_tb_stretch_id, on="stretch_id", how="left")

    df_total_toll = df_toll.filter(
        pl.col(f"total_{from_year}") != 0
    )
    for year in years[1:-1]:
        df_total_toll = df_total_toll.filter(
            pl.col(f"total_{year}") != 0
        )

    inflation_columns = []
    inflation_rate_exp = []
    for prev_year, base_year in zip(years, years[1:]):
        result_col_name = f"inflation_rate_{base_year}"
        inflation_rate_exp.append(
            ((pl.col(f"total_{base_year}") - pl.col(f"total_{prev_year}")) * 100 / pl.col(f"total_{prev_year}")).round(2).alias(result_col_name)
        )
        inflation_columns.append(result_col_name)
    
    accum_inflation_rate_exp = []
    for prev_year, base_year in zip(years[1:], years[2:]):
        result_col_name = f"accum_inflation_rate_{from_year+1}_{base_year}"
        accum_inflation_rate_exp.append(
            ((pl.col(f"total_{base_year}") / pl.col(f"total_{from_year}") - 1) * 100).round(2).alias(result_col_name)
        )
        inflation_columns.append(result_col_name)
    
    result_col_name = f"accum_inflation_rate_avg_{from_year+1}_{to_year}"
    inflation_columns.append(result_col_name)
    avg_accum_inflation_rate_exp = (((pl.col(f"total_{to_year}") / pl.col(f"total_{from_year}")).pow(1/4) - 1) * 100).round(2).alias(result_col_name)
    df_total_toll = df_total_toll.with_columns(
        inflation_rate_exp + accum_inflation_rate_exp + [avg_accum_inflation_rate_exp]
    ).filter(pl.col(f"accum_inflation_rate_{from_year+1}_{to_year}").is_not_null())

    df_total_toll = df_total_toll.sort(f"accum_inflation_rate_{from_year+1}_{to_year}", descending=True).select(
        ["stretch_id", "stretch_name", "state", "manage"] + inflation_columns
        
    ).unique(maintain_order=True)#.write_csv("inflation_rate_total.csv")
    filepath = os.path.join(filepath, f"inflation_rate_{from_year}_{to_year}.csv")
    df_total_toll.sink_csv(filepath)
    print(f"Saved result in {filepath}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--save", required=True, type=str)

    args = parser.parse_args()
    if args.save:
        calc_inflation_rate(args.save, from_year=2021, to_year=2025)
