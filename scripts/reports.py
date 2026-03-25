import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import polars as pl
import argparse
from collections import defaultdict

from tb_map_editor.data_files import DataModel, DataStage
from tb_map_editor.model import _str_normalize


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
    "ltruck": _LIGH_TRUCK,
    "htruck": _HEAVY_TRUCK,
    "utruck": _U_HEAVY_TRUCK,
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
            ).alias(f"toll_{year}")
        )
        df_strech_toll_dict[year] = df_strech_toll_dict[year].with_columns(
            pl.mean_horizontal(
                _VEHICLE_TYPE_DICT[vehicle_type]
            ).alias(f"mean_toll_{year}")
        )
        df_strech_toll_dict[year] = df_strech_toll_dict[year].select(pl.exclude(_VEHICLE_TYPE_DICT[vehicle_type]))

    df_toll = join_range(from_year, to_year, df_strech_toll_dict, data_join_key="stretch_id")
    infla_growth_rate_columns, infla_growth_rate_expr = growth_rate_exprs(from_year, to_year, "mean_toll", "toll")

    df_toll = df_toll.with_columns(
        infla_growth_rate_expr
    ).select(["stretch_id"] + infla_growth_rate_columns + [f"mean_toll_{year}"])

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


def growth_rate(range_keys, prefix_col: str, result_prefix_col: str, growth_rate_columns: list, growth_rate_exp: list):
    for start_year, end_year in zip(range_keys, range_keys[1:]):
        result_col_name = f"{result_prefix_col}_growth_rate_{end_year}"
        growth_rate_exp.append(
            (
                pl.when(pl.col(f"{prefix_col}_{start_year}") != 0)
                .then(
                    ((pl.col(f"{prefix_col}_{end_year}") - pl.col(f"{prefix_col}_{start_year}")) * 100 / pl.col(f"{prefix_col}_{start_year}")).round(2)
                )
                .otherwise(None)
                .alias(result_col_name)
            )
        )
        growth_rate_columns.append(result_col_name)


def cum_growth_rate(range_keys, prefix_col: str, result_prefix_col: str, growth_rate_columns: list, growth_rate_exp: list):
    start = range_keys[0]
    for _, end_year in zip(range_keys[1:], range_keys[2:]):
        result_col_name = f"{result_prefix_col}_cum_growth_rate_{start}_{end_year}"
        growth_rate_exp.append(
            (
                pl.when(pl.col(f"{prefix_col}_{start}") != 0)
                .then(
                    ((pl.col(f"{prefix_col}_{end_year}") / pl.col(f"{prefix_col}_{start}") - 1) * 100).round(2)
                )
                .otherwise(None)
                .alias(result_col_name)
            )
        )
        growth_rate_columns.append(result_col_name)


def cagr_growth_rate(range_keys, prefix_col: str, result_prefix_col: str, growth_rate_columns: list, growth_rate_exp: list):
    start = range_keys[0]
    end = range_keys[-1]
    result_col_name = f"{result_prefix_col}_cagr_growth_rate_{start}_{end}"
    growth_rate_columns.append(result_col_name)
    num_of_years = end - start
    cagr_inflation_rate_exp = (
        pl.when(pl.col(f"{prefix_col}_{start}") != 0)
        .then(
            (((pl.col(f"{prefix_col}_{end}") / pl.col(f"{prefix_col}_{start}")).pow(1/num_of_years) - 1) * 100).round(2)
        )
        .otherwise(None)
        .alias(result_col_name)
    )
    growth_rate_exp.append(cagr_inflation_rate_exp)


def round(range_keys, prefix_col: str, result_prefix_col: str, growth_rate_columns: list, growth_rate_exp: list):
    for year in range_keys:
        result_col_name = f"{result_prefix_col}_round_{year}"
        growth_rate_exp.append(
            pl.col(f"{prefix_col}_{year}").round().alias(result_col_name)
        )
        growth_rate_columns.append(result_col_name)


def growth_rate_exprs(start, end, prefix_col: str, result_prefix_col: str):
    range_keys = range(start, end + 1)
    growth_rate_columns = []
    growth_rate_exp = []

    growth_rate(range_keys, prefix_col, result_prefix_col, growth_rate_columns, growth_rate_exp)
    cum_growth_rate(range_keys, prefix_col, result_prefix_col, growth_rate_columns, growth_rate_exp)
    cagr_growth_rate(range_keys, prefix_col, result_prefix_col, growth_rate_columns, growth_rate_exp)
    round(range_keys, prefix_col, result_prefix_col, growth_rate_columns, growth_rate_exp)

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
       "stretch_id", "stretch_name", "stretch_way", "tollbooth_name", "state", "tb_manage",
       "parent_tb_manage", "stretch_length_km", "stretch_manage", "road_name",
       "start_contract_date", "end_contract_date", "operation_date", "bond_issuance_date",
       "farac", "bond_issuance_date", "km_cost", "operation_contract_days", 
       "end_start_contract_days", "gate_to"
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
        .select("tollbooth_id", "tollbooth_name", "state", "manage", "parent_manage", "gate_to")
        .rename({"manage": "tb_manage", "parent_manage": "parent_tb_manage"})
    )
    ldf_tb_stretch_id = (
        pl.scan_parquet(data_model.tb_stretch_id.parquet)
        .select("stretch_id", "tollbooth_id_out")
    )
    ldf_road = (
        pl.scan_parquet(data_model.roads.parquet)
        .select(
            "road_id", "road_name", "operation_date",
            "start_contract_date", "end_contract_date",
            "bond_issuance_date", "farac"
        )
    )
    ldf_road = ldf_road.with_columns(
        ((pl.col("operation_date") - pl.col("start_contract_date")).dt.total_days()).alias("operation_contract_days"),
        ((pl.col("end_contract_date") - pl.col("start_contract_date")).dt.total_days()).alias("end_start_contract_days")
    )

    df_inflation = (
        pl.read_parquet(data_model.inflation.parquet)
    )
    df_inflation = df_inflation.filter((pl.col("year") > from_year) & (pl.col("year") <= to_year))
    df_inflation = df_inflation.cast({"year": pl.String})
    df_inflation = df_inflation.with_columns(
        pl.lit(0).alias("index"),
        ("annual_inflation_" + pl.col("year")).alias("year")
    )
    inflation_label = f"inflation_{from_year}_{to_year}"
    df_inflation_mean = (
        df_inflation
        .with_columns(
            (1 + (pl.col("value")/100.)).alias(inflation_label)
        )
        .group_by("index")
        .agg(pl.col(inflation_label).product())
        .with_columns(
            ((pl.col(inflation_label).pow(1/(to_year - from_year)) - 1)*100).round(2)
        )
        .select(pl.exclude("index"))
    )
    df_inflation = df_inflation.pivot(index="index", on="year", values="value")
    df_inflation = df_inflation.select(pl.exclude("index"))
    ldf_inflation = pl.concat([df_inflation, df_inflation_mean], how="horizontal").lazy()
    
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
        ).then(None).otherwise((pl.col(f"mean_toll_{to_year}") / pl.col("stretch_length_km")).round(2)).alias("km_cost")
    )
    ldf_toll = ldf_toll.select(pl.exclude(f"mean_toll_{to_year}"))
    del output_cols_dict[f"mean_toll_{to_year}"]

    ldf_tbsts_stretch_id = pl.scan_parquet(
        data_model.tb_sts_stretch_id.parquet
    ).select("stretch_id", "tollbooth_id", "tollbooth_sts_id")

    ldf_sts_name = (
        pl.scan_parquet(DataModel(to_year - 1, DataStage.prd).tb_sts.parquet)
        .select("tollbooth_id", "stretch_name")
        .rename({"stretch_name": "stretch_way"})
        .join(ldf_sts, on="tollbooth_id")
    )
    ldf_tbsts_stretch_id = ldf_tbsts_stretch_id.join(
        ldf_sts_name, left_on="tollbooth_sts_id", right_on="tollbooth_id", how="full"
    ).select(pl.exclude("tollbooth_id_right"))

    ldf_toll_sts = ldf_toll.join(
        ldf_tbsts_stretch_id, 
        left_on=["stretch_id", "tollbooth_id_out"], 
        right_on=["stretch_id", "tollbooth_id"], 
        how="left"
    )
    # Keep only groups where at least one row has non-null stretch_way for each stretch_id and tollbooth_name
    ldf_toll_sts = ldf_toll_sts.with_columns(
        pl.col("stretch_id").rank("ordinal").over("stretch_id", "tollbooth_name", order_by="stretch_way").alias("stretch_way_grp")
    ).filter(
        pl.col("stretch_way_grp") == pl.col("stretch_way_grp").max().over("stretch_id", "tollbooth_name")
    ).select(
       pl.exclude("stretch_way_grp")
    )
    filepath = os.path.join(output_filepath, f"growth_rate_{vehicle_type}_{from_year}_{to_year}.csv")
    ldf_toll_sts = ldf_toll_sts.select(list(output_cols_dict.keys()))
    # stretchs could have distincts tollbooth_id_out
    # so it's eliminated from the columns
    ldf_toll_sts = ldf_toll_sts.unique()
    ldf_toll_sts = ldf_toll_sts.join(ldf_inflation, how="cross")
    ldf_toll_sts = ldf_toll_sts.with_columns(
        pl.when(
            pl.col(f"toll_cagr_growth_rate_{from_year}_{to_year}").is_null()
        ).then((pl.col(f"toll_growth_rate_{to_year}") - pl.col(f"inflation_{from_year}_{to_year}"))).otherwise(
            (pl.col(f"toll_cagr_growth_rate_{from_year}_{to_year}") - pl.col(f"inflation_{from_year}_{to_year}"))
        ).round(2).alias("toll_inflation_diff")
    )
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
        data_model.tb_sts_stretch_id.parquet
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


def mx_projects_report():
    data_model = DataModel(2026, DataStage.pub)

    def extract(url):
        ldf_mxpj = (
            pl.scan_csv(url)
            .select("Proyecto", "Sector", "Subsector", "Tipo de contrato", "Entidad responsable")
            #.select("Proyecto")
            .with_columns(
                pl.col("Proyecto").str.extract_groups(r"(?P<id>\d{4})\s+(?P<name>.+)")
            )
            .unnest("Proyecto")
            .filter(pl.col("id").is_not_null())
            .cast({"id": pl.UInt16})
        )
        return ldf_mxpj

    ldf_mxpj = extract("./raw_data/proyectos_mexico/Proyectos_Progreso – Proyectos México_202601.csv")
    ldf_mxpj_old = extract("./raw_data/proyectos_mexico/Proyectos – Proyectos México_202511.csv")

    print(ldf_mxpj.join(ldf_mxpj_old, on="id", how="anti").collect())
    print(ldf_mxpj_old.join(ldf_mxpj, on="id", how="anti").collect())
    

def toll_ref(year: int):
    data_model = DataModel(year, DataStage.stg)
    data_model_pub = DataModel(year, DataStage.pub)
    ldf_toll = pl.scan_csv(data_model_pub.stretchs_toll.csv)
    ldf_stretch_id = pl.scan_csv(data_model_pub.tb_stretch_id.csv)
    ldf_tb = pl.scan_parquet(data_model.tollbooths.parquet).select("tollbooth_id", "manage", "tollbooth_name")
    ldf_operator = pl.scan_csv("data/tables/area_operators_mx.csv", separator="|").select("short_name", "toll_ref")
    
    ldf_tb = ldf_tb.join(ldf_operator, left_on="manage", right_on="short_name").select(pl.exclude("manage"))
    ldf_toll = ldf_toll.join(ldf_stretch_id, on="stretch_id", how="left")
    ldf_toll = (
        ldf_toll
        .join(ldf_tb, left_on="tollbooth_id_out", right_on="tollbooth_id", how="left")
        .rename({"toll_ref_right": "toll_manage_ref"})
        .select("stretch_id", "stretch_name", "tollbooth_name", "toll_ref", "toll_manage_ref")
    )
    ldf_toll = ldf_toll.unique().sort("stretch_id")
    ldf_toll.sink_csv(f"./reports/toll_ref_{year}.csv")


def tollbooth_stretch_manage(year: int):
    data_model = DataModel(year, DataStage.stg)
    
    ldf_tb_imt = (
        pl.scan_parquet(data_model.tb_imt.parquet)
        .select("tollbooth_id", "manage")
        .rename({"manage": "manage_imt"})
    )
    ldf_tb = (
        pl.scan_parquet(data_model.tollbooths.parquet)
        .select("tollbooth_id", "manage")
    )
    ldf_map_tb = (
        pl.scan_parquet(data_model.map_tb_id.parquet)
        .select("tollbooth_id", "tollbooth_imt_id")
    )
    ldf_tb_stretch = (
        pl.scan_parquet(data_model.tb_stretch_id.parquet)
    )
    ldf_stretch = (
        pl.scan_parquet(data_model.stretchs.parquet)
        .select("stretch_id", "manage")
        .rename({"manage": "stretch_manage"})
    )
    ldf_tb = ldf_tb.join(ldf_map_tb, on="tollbooth_id", how="left")
    ldf_tb = ldf_tb.join(ldf_tb_imt, left_on="tollbooth_imt_id", right_on="tollbooth_id", how="left")

    ldf_stretch = ldf_stretch.join(ldf_tb_stretch, on="stretch_id")
    ldf_stretch_tb_in = ldf_stretch.select("stretch_id", "tollbooth_id_in", "stretch_manage").rename({"tollbooth_id_in": "tollbooth_id"})
    ldf_stretch_tb_out = ldf_stretch.select("stretch_id", "tollbooth_id_out", "stretch_manage").rename({"tollbooth_id_out": "tollbooth_id"})
    ldf_stretch = pl.concat([ldf_stretch_tb_in, ldf_stretch_tb_out]).unique()
    ldf_stretch = ldf_stretch.join(ldf_tb, on="tollbooth_id", how="left").unique()
    ldf_stretch = ldf_stretch.sort("stretch_id")
    ldf_stretch = ldf_stretch.select("stretch_id", "tollbooth_id", "tollbooth_imt_id", "stretch_manage", "manage", "manage_imt")
    ldf_stretch.sink_csv("./reports/tollbooth_stretch_manage.csv")


def stretch_sts(year: int):
    data_model = DataModel(year, DataStage.stg)
    data_model_sts = DataModel(year - 1, DataStage.prd)

    lf_stretch = (
        pl.scan_parquet(data_model.stretchs.parquet)
        .select("stretch_id", "stretch_name")
    )
    lf_sts = (
        pl.scan_parquet(data_model_sts.tb_sts.parquet)
        .select("tollbooth_id", "tollbooth_name", "stretch_name", "status")
        .rename({"tollbooth_id": "tollbooth_sts_id"})
    )
    lf_tbsts_stretch_id = (
        pl.scan_parquet(data_model.tb_sts_stretch_id.parquet)
        .select("stretch_id", "tollbooth_sts_id", "tollbooth_id")
    )
    lf_tollbooth = (
        pl.scan_parquet(data_model.tollbooths.parquet)
        .select("tollbooth_id", "tollbooth_name")
    )
    lf_sts = (
        lf_sts
        .join(lf_tbsts_stretch_id, on="tollbooth_sts_id", how="left")
        .join(lf_stretch, on="stretch_id", how="left")
        .join(lf_tollbooth, on="tollbooth_id", how="left")
        .select(
            "tollbooth_sts_id", "tollbooth_name", "stretch_name", "status", "stretch_id",
            "stretch_name_right", "tollbooth_name_right"
        )
        .unique()
        .sort("tollbooth_sts_id")
    )
    lf_sts.sink_csv(f"./reports/stretch_sts_{data_model_sts.attr.get("year")}.csv")


def tb_imt_stretch_id(year: int):
    data_model = DataModel(year, DataStage.stg)

    ldf_tb_imt_stretch_id = pl.scan_parquet(data_model.tb_imt_stretch_id.parquet)
    ldf_stretch = (
        pl.scan_parquet(data_model.stretchs.parquet)
        .select("stretch_id", "stretch_name")
    )
    ldf_tollbooth = (
        pl.scan_parquet(data_model.tollbooths.parquet)
        .select("tollbooth_id", "tollbooth_name")
    )
    ldf_tb_imt = (
        pl.scan_parquet(data_model.tb_imt.parquet)
        .select("tollbooth_id", "tollbooth_name")
        .rename({"tollbooth_name": "tollbooth_imt_name", "tollbooth_id": "tollbooth_imt_id"})
    )
    
    ldf_tb_imt_stretch_id = (
        ldf_tb_imt_stretch_id
        .join(ldf_stretch, on="stretch_id")
        .join(ldf_tollbooth, left_on="tollbooth_id_in", right_on="tollbooth_id")
        .join(ldf_tollbooth, left_on="tollbooth_id_out", right_on="tollbooth_id")
        .join(ldf_tb_imt, left_on="tollbooth_imt_id_in", right_on="tollbooth_imt_id")
        .join(ldf_tb_imt, left_on="tollbooth_imt_id_out", right_on="tollbooth_imt_id")
        .select(
            "stretch_id", "stretch_name",
            "tollbooth_imt_id_in", "tollbooth_imt_name", 
            "tollbooth_imt_id_out",	"tollbooth_imt_name_right",
            "tollbooth_id_in", "tollbooth_name", 
            "tollbooth_id_out", "tollbooth_name_right", 
        )
    )
    ldf_tb_imt_stretch_id = ldf_tb_imt_stretch_id.sort("stretch_id")
    ldf_tb_imt_stretch_id.sink_csv(f"./reports/tb_imt_stretch_id_{year}.csv")


def stretch_length(year:int):
    data_model = DataModel(year, DataStage.stg)
    
    ldf_osm_distance = pl.scan_parquet(data_model.osm_tb_distance.parquet)
    ldf_stretch = (
        pl.scan_parquet(data_model.stretchs.parquet)
        .select("stretch_id", "stretch_name", "stretch_length_km")
        .join(
            ldf_osm_distance,
            on="stretch_id"
        )
        .sort("stretch_id")
    )
    ldf_stretch.sink_csv("./reports/stretch_length.csv")


def road_manage_length(year: int):
    data_model = DataModel(year, DataStage.stg)

    lf_road = (
        pl.scan_parquet(data_model.roads.parquet)
        .select("road_id", "road_name", "road_length_km", "end_contract_date")
    )
    lf_stretch = (
        pl.scan_parquet(data_model.stretchs.parquet)
        .select("stretch_id", "stretch_length_km", "road_id")
    )
    lf_tollbooth = (
        pl.scan_parquet(data_model.tollbooths.parquet)
        .select("tollbooth_id", "manage", "parent_manage")
    )
    lf_tb_stretch_id = (
        pl.scan_parquet(data_model.tb_stretch_id.parquet)
        .select("stretch_id", "tollbooth_id_in", "tollbooth_id_out")
        .join(lf_stretch, on="stretch_id", how="left")
    )
    lf_tb_stretch_grp_in = (
        lf_tb_stretch_id
        .group_by("road_id").agg(pl.col("tollbooth_id_in"))
    )
    lf_tb_stretch_grp_out = (
        lf_tb_stretch_id
        .group_by("road_id").agg(pl.col("tollbooth_id_out"))
    )
    lf_tb_stretch_grp = (
        lf_tb_stretch_grp_in.join(lf_tb_stretch_grp_out, on="road_id")
    )
    lf_stretch_length = (
        lf_stretch
        .group_by("road_id").agg(pl.col("stretch_length_km").sum())
    )
    lf_tb_stretch = (
        lf_tb_stretch_id.join(lf_tollbooth, left_on="tollbooth_id_out", right_on="tollbooth_id")
        .group_by("road_id", "manage").agg(pl.col("stretch_id").count().alias("road_manage_total"))
        .join(lf_stretch_length, on="road_id", how="left")
        .join(lf_road, on="road_id", how="left")
        .join(lf_tb_stretch_grp, on="road_id", how="left")
        .with_columns(
            tollbooths=pl.col("tollbooth_id_in").list.concat("tollbooth_id_out").list.unique().list.sort().cast(pl.List(pl.String)).list.join(",")
        )
        .sort("road_id")
        .select(
            "road_id", "road_name", "manage", "end_contract_date", 
            "road_length_km", "stretch_length_km", "tollbooths"
        )
    )
    lf_tb_stretch.sink_csv(os.path.join(output_filepath, f"road_manage_length_{year}.csv"))


def manage_data(from_year: int, to_year: int):
    data_model = DataModel(to_year, DataStage.stg)

    lf_road = (
        pl.scan_parquet(data_model.roads.parquet)
        .select("road_id", "road_length_km", "bond_code")
    )
    lf_stretch = (
        pl.scan_parquet(data_model.stretchs.parquet)
        .select("stretch_id", "road_id", "manage", "stretch_length_km")
        .rename({"manage": "stretch_manage"})
    )
    lf_tollbooth = (
        pl.scan_parquet(data_model.tollbooths.parquet)
        .select("tollbooth_id", "tollbooth_name", "parent_manage", "state", "gate_to", "status")
    )
    lf_tb_stretch_id = (
        pl.scan_parquet(data_model.tb_stretch_id.parquet)
        .select("stretch_id", "tollbooth_id_out")
        .join(lf_stretch, on="stretch_id", how="left")
        .join(lf_tollbooth, left_on="tollbooth_id_out", right_on="tollbooth_id")
    )
    lf_tb_remove_dup = (
        lf_tollbooth.select("tollbooth_name", "parent_manage", "state", "status", "gate_to")
        .unique()
    )
    lf_tollbooth_bridge = (
        lf_tb_remove_dup
        .filter(pl.col("gate_to") == "bridge")
        .group_by("parent_manage")
        .len("bridges")
    )
    lf_tollbooth_tunnel = (
        lf_tb_remove_dup
        .filter(pl.col("gate_to") == "tunnel")
        .group_by("parent_manage")
        .len("tunnels")
    )
    lf_tollbooth_int_bridge = (
        lf_tb_remove_dup
        .filter(pl.col("gate_to") == "international_bridge")
        .group_by("parent_manage")
        .len("int_bridges")
    )
    lf_tollbooth_open_tb = (
        lf_tollbooth
        .filter(pl.col("status") == "open")
        .group_by("parent_manage")
        .len("open_tb")
    )
    lf_tollbooth_closed_tb = (
        lf_tollbooth
        .filter(pl.col("status") == "closed")
        .group_by("parent_manage")
        .len("closed_tb")
    )
    lf_stretch_state_manage = (
        lf_tb_stretch_id
        .filter((pl.col("stretch_manage") == "state") | (pl.col("stretch_manage") == "federal_state"))
        .group_by("parent_manage")
        .agg(pl.col("stretch_length_km").sum().alias("state_stretch_use"))
        .with_columns(
            (pl.col("state_stretch_use") / pl.col("state_stretch_use").sum()).round(5)
        )
    )
    lf_stretch_federal_manage = (
        lf_tb_stretch_id
        .filter((pl.col("stretch_manage") == "federal") | (pl.col("stretch_manage") == "federal_state"))
        .group_by("parent_manage")
        .agg(pl.col("stretch_length_km").sum().alias("federal_stretch_use"))
        .with_columns(
            (pl.col("federal_stretch_use") / pl.col("federal_stretch_use").sum()).round(5)
        )
    )
    lf_tb_stretch = (
        lf_tb_stretch_id
        .join(lf_road, on="road_id", how="left")
        .select("road_id", "parent_manage", "road_length_km", "bond_code")
        .unique()
    )
    lf_manage_bond = (
        lf_tb_stretch
        .group_by("parent_manage").agg(pl.col("bond_code").count().alias("road_bonds"))
    )
    lf_manage_length = (
        lf_tb_stretch
        .group_by("parent_manage").agg(pl.col("road_length_km").sum().round(2).alias("total_km"))
        
    )
    lf_manage_roads = (
        lf_tb_stretch
        .group_by("parent_manage").agg(pl.col("road_id").len().alias("total_roads"))
    )
    lf_manage_state = (
        lf_tb_stretch_id
        .select("parent_manage", "state")
        .unique()
        .group_by("parent_manage").agg(pl.col("state").len().alias("states"))
    )
    lf_manage = (
        lf_manage_length
        .join(lf_manage_roads, on="parent_manage", how="left")
        .join(lf_manage_state, on="parent_manage", how="left")
        .join(lf_tollbooth_bridge, on="parent_manage", how="left")
        .join(lf_tollbooth_tunnel, on="parent_manage", how="left")
        .join(lf_tollbooth_int_bridge, on="parent_manage", how="left")
        .join(lf_tollbooth_open_tb, on="parent_manage", how="left")
        .join(lf_tollbooth_closed_tb, on="parent_manage", how="left")
        .join(lf_manage_bond, on="parent_manage", how="left")
        .join(lf_stretch_state_manage, on="parent_manage", how="left")
        .join(lf_stretch_federal_manage, on="parent_manage", how="left")
    )
    revenue_cols = defaultdict(list)
    years = range(from_year, to_year)
    for vehicle_type in _VEHICLE_TYPE_DICT:
        if vehicle_type in ["extra_axle", "all"]:
            continue
        else:
            try:
                lf_growth = pl.scan_csv(f"./reports/growth_rate_{vehicle_type}_{from_year}_{to_year}.csv")
                lf_growth.collect_schema()
            except FileNotFoundError as e:
                print(e)
                print("Try to run the report --growth-rate {vehicle_type} first.")
            else:
                lf_km_cost_mean = (
                    lf_growth
                    .select("stretch_id", "km_cost", "parent_tb_manage", "gate_to")
                    .unique()
                    .filter(pl.col("km_cost").is_not_null())
                    .group_by("parent_tb_manage")
                    .agg(
                        pl.col("km_cost").mean()
                        .round(2)
                        .alias(f"km_cost_mean_{vehicle_type}")
                    )
                    .rename({"parent_tb_manage": "parent_manage"})
                )
                lf_km_cost_median = (
                    lf_growth
                    .select("stretch_id", "km_cost", "parent_tb_manage", "gate_to")
                    .unique()
                    .filter(pl.col("km_cost").is_not_null())
                    .group_by("parent_tb_manage")
                    .agg(
                        pl.col("km_cost").median()
                        .alias(f"km_cost_median_{vehicle_type}")
                    )
                    .rename({"parent_tb_manage": "parent_manage"})
                )
                lf_cagr_inflation_mean = (
                    lf_growth
                    .select("stretch_id", f"toll_cagr_growth_rate_{from_year}_{to_year}", "parent_tb_manage")
                    .unique()
                    .filter(pl.col(f"toll_cagr_growth_rate_{from_year}_{to_year}").is_not_null())
                    .group_by("parent_tb_manage")
                    .agg(
                        pl.col(f"toll_cagr_growth_rate_{from_year}_{to_year}").mean()
                        .round(2)
                        .alias(f"toll_cagr_growth_rate_{vehicle_type}")
                    )
                    .rename({"parent_tb_manage": "parent_manage"})
                )
                lf_last_year_inflation_mean = (
                    lf_growth
                    .select("stretch_id", f"toll_growth_rate_{to_year}", "parent_tb_manage")
                    .unique()
                    .filter(pl.col(f"toll_growth_rate_{to_year}").is_not_null())
                    .group_by("parent_tb_manage")
                    .agg(
                        pl.col(f"toll_growth_rate_{to_year}").mean()
                        .round(2)
                        .alias(f"toll_last_year_growth_rate_{vehicle_type}")
                    )
                    .rename({"parent_tb_manage": "parent_manage"})
                )
                lf_manage = (
                    lf_manage
                    .join(lf_km_cost_mean, on="parent_manage", how="left")
                    .join(lf_km_cost_median, on="parent_manage", how="left")
                    .join(lf_cagr_inflation_mean, on="parent_manage", how="left")
                    .join(lf_last_year_inflation_mean, on="parent_manage", how="left")
                )
                for year in years:
                    lf_toll_tdpa = (
                        lf_growth
                        .select("stretch_id", f"toll_round_{year}", f"tdpa_round_{year}", "parent_tb_manage")
                        .unique()
                        .filter(pl.col(f"tdpa_round_{year}").is_not_null())
                        .with_columns(
                            (pl.col(f"toll_round_{year}") * pl.col(f"tdpa_round_{year}") * 365).alias(f"revenue_from_{vehicle_type}_{year}")
                        )
                        .group_by("parent_tb_manage")
                        .agg(pl.col(f"revenue_from_{vehicle_type}_{year}").sum())
                        .rename({"parent_tb_manage": "parent_manage"})
                        .select(pl.exclude("stretch_id"))
                    )
                    lf_manage = lf_manage.join(lf_toll_tdpa, on="parent_manage", how="left")
                    revenue_cols[year].append(f"revenue_from_{vehicle_type}_{year}")
    
    for year in years:
        lf_manage = (
            lf_manage
            .with_columns(
                pl.sum_horizontal(revenue_cols[year]).alias(f"total_revenue_{year}"),
            )
        )
        lf_sts_coverage = (
            lf_growth
            .select("stretch_id", "parent_tb_manage", f"tdpa_round_{year}", f"toll_round_{year}")
            .filter(pl.col("parent_tb_manage").is_not_null())
            .unique()
            .group_by("parent_tb_manage")
            .agg(
                (pl.col(f"tdpa_round_{year}").count() / pl.col(f"toll_round_{year}").count() * 100).round().alias(f"tdpa_booth_coverage_{year}")
            )
            .rename({"parent_tb_manage": "parent_manage"})
            .select(pl.exclude("stretch_id"))
        )
        lf_manage = lf_manage.join(lf_sts_coverage, on="parent_manage", how="left")

    lf_manage = lf_manage.sort("total_km", "parent_manage", descending=True)
    lf_manage.sink_csv(os.path.join(output_filepath, f"manage_road_data_{from_year}_{to_year}.csv"))


def revenue(from_year: int, to_year: int):
    lf_dict = {}
    range_keys = range(from_year, to_year + 1)

    for year in range_keys:
        data_model = DataModel(year, DataStage.stg)
        lf_dict[year] = (
            pl.scan_parquet(data_model.manager_revenue.parquet)
            .with_columns(
                pl.sum_horizontal(data_model.manager_revenue.model.numeric_cols()).alias(f"anual_revenue_{year}")
            )
            .select("stretch_name", f"anual_revenue_{year}")
        )

    lf_base = lf_dict[to_year]
    prev_year = to_year - 1
    while from_year <= prev_year:
        lf_base = (
            lf_base
            .join(lf_dict[prev_year], on="stretch_name", how="full")
            .with_columns(
                pl.when(pl.col("stretch_name_right").is_not_null()).then(pl.col("stretch_name_right")).otherwise(pl.col("stretch_name")).alias("stretch_name")
            )
            .select(pl.exclude("stretch_name_right"))
        )
        prev_year = prev_year - 1
    
    lf_alias = pl.DataFrame({
        "name": [
            "acatzingo_ciudad_mendoza",
            "aero_los_cabos_san_jose_del_cabo_cabo_san_lucas",
            "aeropuerto_los_cabos_san_jose_del_cabo_cabo_san_lucas",
            "monterrey_nvo_laredo",
            "puente_nuevo_amanecer_reynosa_pharr",
            "salina_cruz_la_ventosa",
            "lib_noreste_de_queretaro",
            "cd_mendoza_cordoba",
            "la_rumorosa_tecate",
        ],
        "alias": [
            "acatzingo_cd_mendoza",
            "san_jose_del_cabo_san_lucas",
            "san_jose_del_cabo_san_lucas",
            "monterrey_nuevo_laredo",
            "puente_reynosa_pharr",
            "salina_cruz_tehuantepec_la_ventosa",
            "libramiento_noreste_de_queretaro",
            "ciudad_mendoza_cordoba",
            "tecate_la_rumorosa"
        ]
    }).lazy()
    
    
    # Merge numeric rows by stretch_name using sum
    numeric_cols = [col for col in lf_base.collect_schema().names() if col != "stretch_name"]
    lf_base = (
        lf_base
        .unique()
        .join(lf_alias, left_on="stretch_name", right_on="name", how="left")
        .with_columns(
            pl.when(pl.col("alias").is_null())
            .then(pl.col("stretch_name"))
            .otherwise(pl.col("alias"))
            .alias("stretch_name")
        )
        .group_by("stretch_name")
        .agg([pl.col(col).sum().alias(col) for col in numeric_cols])
        .sort("stretch_name")
    )
    lf_check = (
        lf_base
        .select(numeric_cols)
        .with_columns(
            [pl.col(col).sum().alias(col) for col in numeric_cols]
        )
        .unique()
    )
    print(lf_check.collect())
    growth_rate_columns = []
    growth_rate_expr = []
    growth_rate(range_keys, "anual_revenue", "anual_revenue", growth_rate_columns, growth_rate_expr)
    lf_base = (
        lf_base
        .with_columns(growth_rate_expr)
    )
    columns = [f"anual_revenue_{range_keys[0]}"]
    for year in range_keys[1:]:
        columns.append(f"anual_revenue_growth_rate_{year}")
        columns.append(f"anual_revenue_{year}")
    columns.reverse()

    lf_base = lf_base.select(["stretch_name"] + columns)
    lf_base.sink_csv(f"./reports/capufe_revenue_{from_year}_{to_year}.csv")


def state_report(year: int):
    lf = pl.scan_csv(f"./data/tables/{year}/inegi_state_data.csv")
    lf = (
        lf
        .with_columns(
            (pl.col("pob_femenina") / pl.col("pob_total")).round(2).alias("ratio_female"),
            (pl.col("pob_masculina") / pl.col("pob_total")).round(2).alias("ratio_male"),
            (pl.col("pob_total") / pl.col("pob_total").sum()).round(2).alias("ratio_population"),
            (pl.col("pob_total") / pl.col("total_viviendas_habitadas")).round(2).alias("ratio_dwell")
        )
        .select("nomgeo", "ratio_female", "ratio_male", "ratio_population", "ratio_dwell")
    )
    lf.sink_csv(f"./reports/state_data_{year}.csv")


if __name__ == "__main__":
    output_filepath = "reports/"

    parser = argparse.ArgumentParser()
    parser.add_argument("--from-year", required=False, type=int)
    parser.add_argument("--to-year", required=True, type=int)
    parser.add_argument("--growth-rate", required=False, choices=tuple(_VEHICLE_TYPE_DICT))
    parser.add_argument("--tb-update-date", required=False, action="store_true")
    parser.add_argument("--tb-names", required=False, action="store_true")
    parser.add_argument("--stretch-names", required=False, action="store_true")
    parser.add_argument("--tb-stretch-rel", required=False, action="store_true")
    parser.add_argument("--tb-wo-stretch", required=False, action="store_true")
    parser.add_argument("--mx-projects", required=False, action="store_true")
    parser.add_argument("--toll-ref", required=False, action="store_true")
    parser.add_argument("--tollbooth-stretch-manage", required=False, action="store_true")
    parser.add_argument("--stretch-sts", required=False, action="store_true")
    parser.add_argument("--tb-imt-stretch-id", required=False, action="store_true")
    parser.add_argument("--stretch-length", required=False, action="store_true")
    parser.add_argument("--road-manage", required=False, action="store_true")
    parser.add_argument("--manage-data", required=False, action="store_true")
    parser.add_argument("--revenue", required=False, action="store_true")
    parser.add_argument("--state-report", required=False, action="store_true")

    args = parser.parse_args()
    if args.growth_rate:
        if args.from_year is not None:
            from_year = args.from_year
        else:
            from_year = 2021
        growth_rate_report(from_year=from_year, to_year=args.to_year, vehicle_type=args.growth_rate)
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
    elif args.mx_projects:
        mx_projects_report()
    elif args.toll_ref:
        toll_ref(year=2026)
    elif args.tollbooth_stretch_manage:
        tollbooth_stretch_manage(year=2025)
    elif args.stretch_sts:
        stretch_sts(year=2025)
    elif args.tb_imt_stretch_id:
        tb_imt_stretch_id(year=2025)
    elif args.stretch_length:
        stretch_length(year=2025)
    elif args.road_manage:
        road_manage_length(year=args.to_year)
    elif args.manage_data:
        if args.from_year is not None:
            from_year = args.from_year
        else:
            from_year = 2021
        manage_data(from_year=from_year, to_year=args.to_year)
    elif args.revenue:
        revenue(from_year=args.from_year, to_year=args.to_year)
    elif args.state_report:
        state_report(year=args.to_year)
