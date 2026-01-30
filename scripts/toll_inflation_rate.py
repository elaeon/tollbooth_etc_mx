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
    actual_data_model_prd = DataModel(to_year, DataStage.prd)
    df_strechs = pl.scan_parquet(actual_data_model.stretchs.parquet).rename({"manage": "stretch_manage"})
    df_tollbooths = pl.scan_parquet(actual_data_model_prd.tollbooths.parquet).select("tollbooth_id", "state", "manage", "parent_manage").rename({"manage": "tb_manage", "parent_manage": "parent_tb_manage"})
    df_tb_stretch_id = pl.scan_parquet(actual_data_model_prd.tb_stretch_id.parquet).select("stretch_id", "tollbooth_id_a")
    df_road = pl.scan_parquet(actual_data_model.roads.parquet).select("road_id", "road_name", "operation_date", "bond_issuance_date")

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
        df_calc_dict[year] = df_calc_dict[year].with_columns(pl.mean_horizontal(
            f"bike_{year}", f"car_{year}", "bus_2_axle", "bus_3_axle", "bus_4_axle", "truck_2_axle", 
            "truck_3_axle", "truck_4_axle", "truck_5_axle", "truck_6_axle", "truck_7_axle",
            "truck_8_axle", "truck_9_axle", "car_axle", "load_axle"
        ).alias(f"total_mean_{year}"))
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
    stretchs = []
    for year in years[1:]:
        new_stretch_ids = df_calc_dict[year].join(df_toll, how="anti", on="stretch_id")
        stretchs.append(new_stretch_ids.select("stretch_id"))
    
    df_stretch_ids = pl.concat(stretchs).unique()
    df_toll = df_toll.join(df_stretch_ids, on="stretch_id", how="full")
    df_toll = df_toll.with_columns(
        stretch_id=pl.when(pl.col("stretch_id_right").is_null()).then(pl.col("stretch_id")).otherwise(pl.col("stretch_id_right"))
    ).select(pl.exclude("stretch_id_right"))

    for year in years[1:]:
        df_toll = df_toll.join(df_calc_dict[year], how="left", on="stretch_id")

    df_toll = df_toll.join(df_strechs, on="stretch_id")
    df_toll = df_toll.join(df_road, on="road_id", how="left")
    df_toll = df_toll.join(df_tb_stretch_id, on="stretch_id", how="left")

    inflation_columns = []
    inflation_rate_exp = []
    for start_year, end_year in zip(years, years[1:]):
        result_col_name = f"inflation_rate_{end_year}"
        inflation_rate_exp.append(
            ((pl.col(f"total_{end_year}") - pl.col(f"total_{start_year}")) * 100 / pl.col(f"total_{start_year}")).round(2).alias(result_col_name)
        )
        inflation_columns.append(result_col_name)
    
    avg_cum_inflation_rate_exp = []
    for _, end_year in zip(years[1:], years[2:]):
        result_col_name = f"avg_cum_inflation_rate_{from_year+1}_{end_year}"
        avg_cum_inflation_rate_exp.append(
            ((pl.col(f"total_{end_year}") / pl.col(f"total_{from_year}") - 1) * 100).round(2).alias(result_col_name)
        )
        inflation_columns.append(result_col_name)
    
    result_col_name = f"cagr_inflation_rate_{from_year+1}_{to_year}"
    inflation_columns.append(result_col_name)
    num_of_years = len(years)
    cagr_inflation_rate_exp = (((pl.col(f"total_{to_year}") / pl.col(f"total_{from_year}")).pow(1/num_of_years) - 1) * 100).round(2).alias(result_col_name)
    df_toll = df_toll.with_columns(
        inflation_rate_exp + avg_cum_inflation_rate_exp + [cagr_inflation_rate_exp]
    )

    df_toll = df_toll.with_columns(
        pl.when(
            (pl.col("stretch_length_km").is_null()) | (pl.col("stretch_length_km") == 0)
        ).then(None).otherwise((pl.col(f"total_mean_{to_year}") / pl.col("stretch_length_km")).round(2)).alias("cost_per_km")
    )
    df_toll = df_toll.sort(
        f"cagr_inflation_rate_{from_year+1}_{to_year}", descending=True, nulls_last=True
    ).select(
        ["stretch_id", "stretch_name", "state", "tb_manage", "parent_tb_manage", 
         "stretch_length_km", "stretch_manage", "road_name", "operation_date", 
         "bond_issuance_date", "cost_per_km"
        ] + inflation_columns
        
    ).unique(maintain_order=True)
    filepath = os.path.join(filepath, f"inflation_rate_{from_year}_{to_year}.csv")
    df_toll.sink_csv(filepath)
    print(f"Saved result in {filepath}")


def join_range(start, end, dict_data, data_join_key: str):
    range_keys = range(start, end + 1)
    df_join_range = dict_data[start]
    df_ids = df_join_range.select(data_join_key)

    for key in range_keys[1:]:
        new_tb_ids = dict_data[key].select(data_join_key).join(df_ids, how="anti", on=data_join_key)
        df_ids = pl.concat([df_ids, new_tb_ids])
        
    df_join_range = df_join_range.join(df_ids, on=data_join_key, how="full")
    df_join_range = df_join_range.with_columns(
        (pl.when(
            pl.col(f"{data_join_key}_right").is_null()
        ).then(
            pl.col(data_join_key)
        ).otherwise(pl.col(f"{data_join_key}_right"))
        ).alias(data_join_key)
    ).select(pl.exclude(f"{data_join_key}_right"))

    for key in range_keys[1:]:
        df_join_range = df_join_range.join(dict_data[key], how="left", on="tollbooth_id")

    return df_join_range


def growth_rate_exprs(start, end, prefix_col: str):
    range_keys = range(start, end + 1)
    growth_rate_columns = []
    growth_rate_exp = []
    
    def growth_rate():
        for start_year, end_year in zip(range_keys, range_keys[1:]):
            result_col_name = f"{prefix_col}_growth_rate_{end_year}"
            growth_rate_exp.append(
                ((pl.col(f"{prefix_col}_{end_year}") - pl.col(f"{prefix_col}_{start_year}")) * 100 / pl.col(f"{prefix_col}_{start_year}")).round(2).alias(result_col_name)
            )
            growth_rate_columns.append(result_col_name)

    def cum_growth_rate():
        for _, end_year in zip(range_keys[1:], range_keys[2:]):
            result_col_name = f"{prefix_col}_cum_growth_rate_{start+1}_{end_year}"
            growth_rate_exp.append(
                ((pl.col(f"{prefix_col}_{end_year}") / pl.col(f"{prefix_col}_{start}") - 1) * 100).round(2).alias(result_col_name)
            )
            growth_rate_columns.append(result_col_name)
        
    def cagr_growth_rate():
        result_col_name = f"{prefix_col}_cagr_growth_rate_{start+1}_{end}"
        growth_rate_columns.append(result_col_name)
        num_of_years = len(range_keys)
        cagr_inflation_rate_exp = (((pl.col(f"{prefix_col}_{end}") / pl.col(f"{prefix_col}_{start}")).pow(1/num_of_years) - 1) * 100).round(2).alias(result_col_name)
        growth_rate_exp.append(cagr_inflation_rate_exp)
    
    growth_rate()
    cum_growth_rate()
    cagr_growth_rate()

    return growth_rate_columns, growth_rate_exp


def tdpa_vta_growth_rate(from_year, to_year):
    years = range(from_year, to_year + 1)
    df_tb_dict = {}

    actual_data_model_stg = DataModel(to_year, DataStage.stg)
    df_map_tb_sts = pl.scan_parquet(
        actual_data_model_stg.map_tb_sts.parquet
    ).select("tollbooth_id", "tollbooth_sts_id")

    for year in years:
        filepath = DataModel(year, DataStage.prd).tb_sts.parquet
        df_tb_dict[year] = pl.scan_parquet(filepath)
    
    tdpa_cols = []
    for year in df_tb_dict:
        df_tb_dict[year] = df_tb_dict[year].select(
            "tollbooth_id", "tdpa", "vta"
        ).cast({"tdpa": pl.Int32, "vta": pl.Int64}).rename({"tdpa": f"tdpa_{year}", "vta": f"vta_{year}"})
        tdpa_cols.append(f"tdpa_{year}")

    df_sts = join_range(from_year, to_year, df_tb_dict, data_join_key="tollbooth_id")
    tdpa_growth_rate_columns, tdpa_growth_rate_expr = growth_rate_exprs(from_year, to_year, "tdpa")
    vta_growth_rate_columns, vta_growth_rate_expr = growth_rate_exprs(from_year, to_year, "vta")

    df_sts = df_sts.with_columns(
        tdpa_growth_rate_expr + vta_growth_rate_expr
    ).select(["tollbooth_id"] + tdpa_growth_rate_columns + vta_growth_rate_columns)

    #print(df_sts.collect().select(["tollbooth_id"]+vta_growth_rate_columns).filter(pl.col("tollbooth_id")==874))
    df_sts = df_map_tb_sts.join(
        df_sts, left_on="tollbooth_sts_id", right_on="tollbooth_id"
    ).select(pl.exclude("tollbooth_sts_id"))

    return df_sts


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--save", required=True, type=str)

    args = parser.parse_args()
    if args.save:
        #calc_inflation_rate(args.save, from_year=2021, to_year=2025)
        tdpa_vta_growth_rate(from_year=2021, to_year=2024)
