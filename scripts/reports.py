import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import polars as pl
import argparse
from collections import defaultdict

from tb_map_editor.data_files import DataModel, DataStage


_BIKE: list = ["motorbike"]
_CAR: list = ["car"]
_BUS: list = [
    "bus_2_axle", "bus_3_axle", "bus_4_axle"
]
_LIGH_TRUCK: list = [
    "truck_2_axle", "truck_3_axle", "truck_4_axle",
]
_HEAVY_TRUCK: list = [
    "truck_5_axle", "truck_6_axle", "truck_7_axle", 
]
_U_HEAVY_TRUCK: list = [
    "truck_8_axle", "truck_9_axle", "truck_10_axle"
]
_TRUCK: list = _LIGH_TRUCK + _HEAVY_TRUCK + _U_HEAVY_TRUCK
_EXTRA_AXLE: list = [
    "car_axle", "load_axle"
]
_VEHICLE_TYPE_DICT: dict = {
    "bike": _BIKE,
    "car": _CAR,
    "bus": _BUS,
    "truck": _TRUCK,
    "light_truck": _LIGH_TRUCK,
    "extra_axle": _EXTRA_AXLE,
    "all": _BIKE + _CAR + _BUS + _TRUCK + _EXTRA_AXLE
}


def inflation_growth_rate(from_year, to_year, vehicle_type):
    years = range(from_year, to_year + 1)
    df_strech_toll_dict = {}

    for year in years:
        filepath = DataModel(year, DataStage.stg).stretchs_toll.parquet
        df_strech_toll_dict[year] = pl.scan_parquet(filepath).select(
            ["stretch_id"] + _VEHICLE_TYPE_DICT[vehicle_type]
        )
        df_strech_toll_dict[year] = df_strech_toll_dict[year].fill_null(0)
        df_strech_toll_dict[year] = df_strech_toll_dict[year].with_columns(
            pl.sum_horizontal(
                _VEHICLE_TYPE_DICT[vehicle_type]
            ).alias(f"total_{year}")
        )
        df_strech_toll_dict[year] = df_strech_toll_dict[year].with_columns(
            pl.mean_horizontal(
                _VEHICLE_TYPE_DICT[vehicle_type]
            ).alias(f"total_mean_{year}")
        )
        df_strech_toll_dict[year] = df_strech_toll_dict[year].select(pl.exclude(_VEHICLE_TYPE_DICT[vehicle_type]))

    df_toll = join_range(from_year, to_year, df_strech_toll_dict, data_join_key="stretch_id")
    infla_growth_rate_columns, infla_growth_rate_expr = growth_rate_exprs(from_year, to_year, "total", "inflation")

    df_toll = df_toll.with_columns(
        infla_growth_rate_expr
    ).select(["stretch_id"] + infla_growth_rate_columns + [f"total_mean_{year}"])

    return df_toll


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
        df_join_range = df_join_range.join(dict_data[key], how="left", on=data_join_key)

    return df_join_range


def growth_rate_exprs(start, end, prefix_col: str, result_prefix_col: str):
    range_keys = range(start, end + 1)
    growth_rate_columns = []
    growth_rate_exp = []
    
    def growth_rate():
        for start_year, end_year in zip(range_keys, range_keys[1:]):
            result_col_name = f"{result_prefix_col}_growth_rate_{end_year}"
            growth_rate_exp.append(
                ((pl.col(f"{prefix_col}_{end_year}") - pl.col(f"{prefix_col}_{start_year}")) * 100 / pl.col(f"{prefix_col}_{start_year}")).round(2).alias(result_col_name)
            )
            growth_rate_columns.append(result_col_name)

    def cum_growth_rate():
        for _, end_year in zip(range_keys[1:], range_keys[2:]):
            result_col_name = f"{result_prefix_col}_cum_growth_rate_{start+1}_{end_year}"
            growth_rate_exp.append(
                ((pl.col(f"{prefix_col}_{end_year}") / pl.col(f"{prefix_col}_{start}") - 1) * 100).round(2).alias(result_col_name)
            )
            growth_rate_columns.append(result_col_name)
        
    def cagr_growth_rate():
        result_col_name = f"{result_prefix_col}_cagr_growth_rate_{start+1}_{end}"
        growth_rate_columns.append(result_col_name)
        num_of_years = len(range_keys)
        cagr_inflation_rate_exp = (((pl.col(f"{prefix_col}_{end}") / pl.col(f"{prefix_col}_{start}")).pow(1/num_of_years) - 1) * 100).round(2).alias(result_col_name)
        growth_rate_exp.append(cagr_inflation_rate_exp)
    
    def rank():
        for year in range_keys:
            result_col_name = f"{prefix_col}_rank_{year}"
            growth_rate_exp.append(
                pl.col(f"{prefix_col}_{year}").rank(method="ordinal", descending=True).alias(result_col_name)
            )
            growth_rate_columns.append(result_col_name)

    growth_rate()
    cum_growth_rate()
    cagr_growth_rate()
    rank()

    return growth_rate_columns, growth_rate_exp


def tdpa_vta_growth_rate(from_year, to_year, vehicle_type):
    years = range(from_year, to_year + 1)
    df_tb_dict = {}

    exclude_vehicle = ["load_axle", "truck_10_axle"]
    rename_vehicle = {"bus_2_axle": "bus", "bus_3_axle":"bus", "bus_4_axle": "bus"}
    vehicle_type_cols = defaultdict(list)

    for vehicle in _VEHICLE_TYPE_DICT[vehicle_type]:
        if vehicle in exclude_vehicle:
            continue
        elif vehicle in rename_vehicle:
            if rename_vehicle[vehicle] not in vehicle_type_cols[vehicle_type]:
                vehicle_type_cols[vehicle_type].append(rename_vehicle[vehicle])
        else:
            vehicle_type_cols[vehicle_type].append(vehicle)

    for year in years:
        filepath = DataModel(year, DataStage.prd).tb_sts.parquet
        df_tb_dict[year] = pl.scan_parquet(filepath).select(
            ["tollbooth_id", "tdpa", "vta"] + vehicle_type_cols[vehicle_type]
        )
        df_tb_dict[year] = df_tb_dict[year].fill_null(0)
        df_tb_dict[year] = df_tb_dict[year].cast(
            {"tdpa": pl.Int32, "vta": pl.Int64}
        ).rename({"tdpa": f"tdpa_{year}", "vta": f"vta_{year}"})

        if vehicle_type != "all":        
            df_tb_dict[year] = df_tb_dict[year].with_columns(
                (pl.sum_horizontal(
                    vehicle_type_cols[vehicle_type]
                ) / 100.
                ).alias("tdpa_vehicle_ratio")
            )
            df_tb_dict[year] = df_tb_dict[year].with_columns(
                (pl.col("tdpa_vehicle_ratio") * pl.col(f"tdpa_{year}")).alias(f"tdpa_{year}"),
                (pl.col("tdpa_vehicle_ratio") * pl.col(f"vta_{year}")).alias(f"vta_{year}"),
            )
            
        df_tb_dict[year] = df_tb_dict[year].select(pl.exclude(["tdpa_vehicle_ratio"] + vehicle_type_cols[vehicle_type]))

    df_sts = join_range(from_year, to_year, df_tb_dict, data_join_key="tollbooth_id")
    tdpa_growth_rate_columns, tdpa_growth_rate_expr = growth_rate_exprs(from_year, to_year, "tdpa", "tdpa")
    vta_growth_rate_columns, vta_growth_rate_expr = growth_rate_exprs(from_year, to_year, "vta", "vta")

    df_sts = df_sts.with_columns(
        tdpa_growth_rate_expr + vta_growth_rate_expr
    ).select(["tollbooth_id"] + tdpa_growth_rate_columns + vta_growth_rate_columns)

    return df_sts


def growth_rate_report(from_year: int, to_year: int, vehicle_type):
    ldf_toll = inflation_growth_rate(from_year, to_year, vehicle_type)
    ldf_sts = tdpa_vta_growth_rate(from_year, to_year=to_year-1, vehicle_type=vehicle_type)

    toll_col_names = ldf_toll.collect_schema().names()
    sts_col_names = ldf_sts.collect_schema().names()[1:]

    output_cols = [
       "stretch_id", "stretch_name", "tollbooth_name", "state", "tb_manage", #"parent_tb_manage", 
       "stretch_length_km", "stretch_manage", "road_name", "operation_date", 
       "bond_issuance_date", "km_cost"
    ] + toll_col_names + sts_col_names
    output_cols_dict = dict((k, None) for k in output_cols)

    data_model = DataModel(to_year, DataStage.stg)

    ldf_strechs = (
        pl.scan_parquet(data_model.stretchs.parquet)
        .rename({"manage": "stretch_manage"})
        .select("stretch_id", "stretch_name", "stretch_length_km", "road_id", "stretch_manage")
    )
    ldf_tollbooths = (
        pl.scan_parquet(data_model.tollbooths.parquet)
        .select("tollbooth_id", "tollbooth_name", "state", "manage")
        .rename({"manage": "tb_manage"})
    )
    ldf_tb_stretch_id = (
        pl.scan_parquet(data_model.tb_stretch_id.parquet)
        .select("stretch_id", "tollbooth_id_out")
    )
    ldf_road = (
        pl.scan_parquet(data_model.roads.parquet)
        .select("road_id", "road_name", "operation_date", "bond_issuance_date")
    )

    ldf_tb_stretch = ldf_tb_stretch_id.join(
        ldf_tollbooths, left_on="tollbooth_id_out", right_on="tollbooth_id", how="left"
    )
    ldf_toll = ldf_toll.join(ldf_strechs, on="stretch_id")
    ldf_toll = ldf_toll.join(ldf_road, on="road_id", how="left")
    ldf_toll = ldf_toll.join(ldf_tb_stretch, on="stretch_id", how="left")
    ldf_toll = ldf_toll.select(pl.exclude("road_id"))

    ldf_toll = ldf_toll.with_columns(
        pl.when(
            (pl.col("stretch_length_km").is_null()) | (pl.col("stretch_length_km") == 0)
        ).then(None).otherwise((pl.col(f"total_mean_{to_year}") / pl.col("stretch_length_km")).round(2)).alias("km_cost")
    )
    ldf_toll = ldf_toll.select(pl.exclude(f"total_mean_{to_year}"))
    del output_cols_dict[f"total_mean_{to_year}"]

    ldf_tbsts_stretch_id = pl.scan_parquet(
        data_model.tbsts_stretch_id.parquet
    ).select("stretch_id", "tollbooth_id", "tollbooth_sts_id")

    ldf_tbsts_stretch_id = ldf_tbsts_stretch_id.join(
        ldf_sts, left_on="tollbooth_sts_id", right_on="tollbooth_id", how="full"
    ).select(pl.exclude("tollbooth_sts_id"))

    ldf_toll_sts = ldf_toll.join(
        ldf_tbsts_stretch_id, left_on=["stretch_id", "tollbooth_id_out"], right_on=["stretch_id", "tollbooth_id"], how="left"
    )

    filepath = os.path.join(output_filepath, f"growth_rate_{vehicle_type}_{from_year}_{to_year}.csv")
    ldf_toll_sts = ldf_toll_sts.select(list(output_cols_dict.keys()))
    # stretchs could have different tollbooth_id_out
    # so it's eliminated from columns and call unique
    ldf_toll_sts = ldf_toll_sts.unique()
    ldf_toll_sts.sort("stretch_name").sink_csv(filepath)
    print(f"Saved result in {filepath}")


def toll_update_date_report(from_year: int, to_year: int):
    years = range(from_year, to_year + 1)
    ldf_dict = {}
    data = []

    data_model = DataModel(to_year, DataStage.stg)
    ldf_tb = pl.scan_parquet(
            data_model.tollbooths.parquet
        ).select("tollbooth_id", "manage")
    ldf_neighbour = pl.scan_parquet(
        data_model.tb_neighbour.parquet
    )
    ldf_neighbour = ldf_neighbour.filter(pl.col("scope") == "local-imt")
    ldf_neighbour_closest = ldf_neighbour.filter(
        pl.col("distance") == pl.col("distance").min().over("neighbour_id")
    )
    ldf_neighbour_closest = ldf_neighbour_closest.filter(pl.col("distance") <= 0.1)
    ldf_neighbour_closest = ldf_neighbour_closest.select(pl.exclude("distance", "scope"))
    ldf_neighbour_closest = ldf_neighbour_closest.join(ldf_tb, on="tollbooth_id")
    
    for year in years:
        data_model = DataModel(year, DataStage.stg)
        ldf_dict[year] = pl.scan_parquet(
            data_model.tb_toll_imt.parquet
        ).select("tollbooth_id_out", "update_date", "tollbooth_id_in")

        ldf_dict[year] = ldf_dict[year].with_columns(
            month=pl.col("update_date").dt.month()
        ).select(pl.exclude("update_date"))

        ldf_dict[year] = ldf_dict[year].select("tollbooth_id_out", "month").rename({"tollbooth_id_out": "tollbooth_id"})
        ldf_dict[year] = ldf_dict[year].join(ldf_neighbour_closest, left_on="tollbooth_id", right_on="neighbour_id").select(pl.exclude("tollbooth_id_right"))
        data.append(ldf_dict[year])

    ldf = pl.concat(data)
    ldf = ldf.group_by("manage").agg(pl.col("month").explode())
    ldf = ldf.with_columns(
        pl.col("month").list.unique().sort()
    )

    month_exprs = [
        pl.when(pl.col("month").list.contains(m))
        .then(m)
        .otherwise(None)
        .alias(f"month_{m}")
        for m in range(1, 13)
    ]
    ldf = ldf.with_columns(month_exprs).select(pl.exclude("month"))
    ldf = ldf.sort("manage")

    filepath = os.path.join(output_filepath, f"toll_update_date_{from_year}_{to_year}.csv")
    ldf.sink_csv(filepath)
    print(f"Saved result in {filepath}")


def tollbooth_names_report(year: int):
    data_model = DataModel(year, DataStage.stg)
    data_model_sts = DataModel(year - 1, DataStage.prd)

    ldf_tb = pl.scan_parquet(
        data_model.tollbooths.parquet
    ).select("tollbooth_id", "tollbooth_name")
    ldf_tb_imt = pl.scan_parquet(
        data_model.tb_imt.parquet
    ).select("tollbooth_id", "tollbooth_name").rename({"tollbooth_name": "tollbooth_imt_name"})
    ldf_tb_sts = pl.scan_parquet(
        data_model_sts.tb_sts.parquet
    ).select("tollbooth_id", "tollbooth_name").rename({"tollbooth_name": "tollbooth_sts_name"})
    ldf_map_tb_id = pl.scan_parquet(
        data_model.map_tb_id.parquet
    )

    ldf_map_tb_id = ldf_map_tb_id.join(ldf_tb, on="tollbooth_id")
    ldf_map_tb_id = ldf_map_tb_id.join(ldf_tb_imt, left_on="tollbooth_imt_id", right_on="tollbooth_id", how="left")
    ldf_map_tb_id = ldf_map_tb_id.join(ldf_tb_sts, left_on="tollbooth_sts_id", right_on="tollbooth_id", how="left")

    filepath = os.path.join(output_filepath, f"tollbooth_names_{year}.csv")
    ldf_map_tb_id.sort("tollbooth_name").sink_csv(filepath)
    print(f"Saved result in {filepath}")


def stretch_names_report(year: int):
    data_model = DataModel(year, DataStage.stg)
    data_model_sts = DataModel(year - 1, DataStage.prd)

    ldf_stretch = pl.scan_parquet(
        data_model.stretchs.parquet
    ).select("stretch_id", "stretch_name")
    ldf_tb_sts = pl.scan_parquet(
        data_model_sts.tb_sts.parquet
    ).select("tollbooth_id", "stretch_name").rename({"stretch_name": "stretch_sts_name"})
    ldf_tbsts_stretch_id = pl.scan_parquet(
        data_model.tbsts_stretch_id.parquet
    )

    ldf_tbsts_stretch_id = ldf_tbsts_stretch_id.join(ldf_stretch, on="stretch_id")
    ldf_tbsts_stretch_id = ldf_tbsts_stretch_id.join(ldf_tb_sts, left_on="tollbooth_sts_id", right_on="tollbooth_id", how="left")

    filepath = os.path.join(output_filepath, f"stretch_names_{year}.csv")
    ldf_tbsts_stretch_id.sort("stretch_name").sink_csv(filepath)
    print(f"Saved result in {filepath}")


def tollbooth_stretch_rel(year: int):
    data_model = DataModel(year, DataStage.stg)

    ldf_tb_stretch_id = pl.scan_parquet(
        data_model.tb_stretch_id.parquet
    )
    ldf_stretch = pl.scan_parquet(
        data_model.stretchs.parquet
    ).select("stretch_id", "stretch_name")
    ldf_tollbooth = pl.scan_parquet(
        data_model.tollbooths.parquet
    ).select("tollbooth_id", "tollbooth_name")
    ldf_tb_stretch_id = ldf_stretch.join(
        ldf_tb_stretch_id, on="stretch_id", how="left"
    )
    ldf_tb_stretch_id = ldf_tb_stretch_id.join(
        ldf_tollbooth, left_on="tollbooth_id_out", right_on="tollbooth_id", how="left"
    ).rename({"tollbooth_name": "tollbooth_name_out"})
    ldf_tb_stretch_id = ldf_tb_stretch_id.join(
        ldf_tollbooth, left_on="tollbooth_id_in", right_on="tollbooth_id", how="left"
    ).rename({"tollbooth_name": "tollbooth_name_in"})
    ldf_tb_stretch_id = ldf_tb_stretch_id.select(
        "stretch_id", "tollbooth_id_in", "tollbooth_id_out", "stretch_name", "tollbooth_name_in", "tollbooth_name_out"
    )

    filepath = os.path.join(output_filepath, f"tollbooth_stretch_rel_{year}.csv")
    ldf_tb_stretch_id.sort("stretch_id").sink_csv(filepath)
    print(f"Saved result in {filepath}")


def tollbooth_without_stretch(year: int):
    data_model = DataModel(year, DataStage.stg)

    ldf_tb_stretch_id = pl.scan_parquet(
        data_model.tb_stretch_id.parquet
    )
    ldf_tollbooth = pl.scan_parquet(
        data_model.tollbooths.parquet
    ).select("tollbooth_id", "tollbooth_name", "state")
    ldf_map_tb_id = pl.scan_parquet(
        data_model.map_tb_id.parquet
    ).select("tollbooth_id", "tollbooth_imt_id")
    ldf_tb_imt = pl.scan_parquet(
        data_model.tb_imt.parquet
    ).select("tollbooth_id", "area", "subarea", "calirepr")

    ldf_tb_stretch_in = ldf_tb_stretch_id.select(
        "stretch_id", "tollbooth_id_in"
    ).rename({"tollbooth_id_in": "tollbooth_id"})
    ldf_tb_stretch_out = ldf_tb_stretch_id.select(
        "stretch_id", "tollbooth_id_out"
    ).rename({"tollbooth_id_out": "tollbooth_id"})
    ldf_tb_stretch = pl.concat([ldf_tb_stretch_in, ldf_tb_stretch_out])
    ldf_tb_stretch = ldf_tb_stretch.unique()
    ldf_tb_no_stretch = ldf_tollbooth.join(
        ldf_tb_stretch, on="tollbooth_id", how="anti"
    )
    ldf_tb_no_stretch = ldf_tb_no_stretch.join(
        ldf_map_tb_id, on="tollbooth_id", how="left"
    )
    ldf_tb_no_stretch = ldf_tb_no_stretch.join(
        ldf_tb_imt, left_on="tollbooth_imt_id", right_on="tollbooth_id", how="left"
    )
    
    filepath = os.path.join(output_filepath, f"tollbooth_wo_stretch{year}.csv")
    ldf_tb_no_stretch.sort("tollbooth_id").sink_csv(filepath)
    print(f"Saved result in {filepath}")



if __name__ == "__main__":
    output_filepath = "reports/"

    parser = argparse.ArgumentParser()
    parser.add_argument("--growth-rate", required=False, choices=tuple(_VEHICLE_TYPE_DICT))
    parser.add_argument("--tb-update-date", required=False, action="store_true")
    parser.add_argument("--tb-names", required=False, action="store_true")
    parser.add_argument("--stretch-names", required=False, action="store_true")
    parser.add_argument("--tb-stretch-rel", required=False, action="store_true")
    parser.add_argument("--tb-wo-stretch", required=False, action="store_true")

    args = parser.parse_args()
    if args.growth_rate:
        growth_rate_report(from_year=2021, to_year=2025, vehicle_type=args.growth_rate)
    elif args.tb_update_date:
        toll_update_date_report(from_year=2024, to_year=2025)
    elif args.tb_names:
        tollbooth_names_report(year=2025)
    elif args.stretch_names:
        stretch_names_report(year=2025)
    elif args.tb_stretch_rel:
        tollbooth_stretch_rel(year=2025)
    elif args.tb_wo_stretch:
        tollbooth_without_stretch(year=2025)
