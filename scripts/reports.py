import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import polars as pl
import polars_ds as plds
import argparse
from collections import defaultdict

from tb_map_editor.data_files import DataModel, DataStage
from tb_map_editor.utils.tools import tb_stretch_id_imt


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


def growth_rate_exprs(start, end, prefix_col: str, result_prefix_col: str):
    range_keys = range(start, end + 1)
    growth_rate_columns = []
    growth_rate_exp = []
    
    def growth_rate():
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

    def cum_growth_rate():
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
        
    def cagr_growth_rate():
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
    
    def round():
        for year in range_keys:
            result_col_name = f"{result_prefix_col}_round_{year}"
            growth_rate_exp.append(
                pl.col(f"{prefix_col}_{year}").round().alias(result_col_name)
            )
            growth_rate_columns.append(result_col_name)

    growth_rate()
    cum_growth_rate()
    cagr_growth_rate()
    round()

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
       "stretch_id", "stretch_name", "tollbooth_name", "state", "tb_manage",
       "parent_tb_manage", "stretch_length_km", "stretch_manage", "road_name",
       "start_contract_date", "end_contract_date", "operation_date", "bond_issuance_date",
       "farac", "bond_issuance_date", "km_cost", "operation_contract_days",
       "end_start_contract_days"
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
        .select("tollbooth_id", "tollbooth_name", "state", "manage", "parent_manage")
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
        data_model.tbsts_stretch_id.parquet
    ).select("stretch_id", "tollbooth_sts_id")

    ldf_tbsts_stretch_id = ldf_tbsts_stretch_id.join(
        ldf_sts, left_on="tollbooth_sts_id", right_on="tollbooth_id", how="full"
    ).select(pl.exclude("tollbooth_id"))

    ldf_toll_sts = ldf_toll.join(
        ldf_tbsts_stretch_id, left_on="stretch_id", right_on="stretch_id", how="left"
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
    ldf_tb = pl.scan_parquet(data_model.tollbooths.parquet).select("tollbooth_id", "manage")
    ldf_operator = pl.scan_csv("data/tables/area_operators_mx.csv", separator="|").select("short_name", "toll_ref")
    
    ldf_tb = ldf_tb.join(ldf_operator, left_on="manage", right_on="short_name").select(pl.exclude("manage"))
    ldf_toll = ldf_toll.join(ldf_stretch_id, on="stretch_id", how="left")
    ldf_toll = ldf_toll.join(
        ldf_tb, left_on="tollbooth_id_out", right_on="tollbooth_id", how="left"
    ).select("stretch_id", "stretch_name", "toll_ref", "toll_ref_right")
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

    ldf_stretch = (
        pl.scan_parquet(data_model.stretchs.parquet)
        .select("stretch_id", "stretch_name")
    )
    ldf_sts = (
        pl.scan_parquet(data_model_sts.tb_sts.parquet)
        .select("tollbooth_id", "tollbooth_name", "stretch_name", "status")
    )
    ldf_tbsts_stretch_id = (
        pl.scan_parquet(data_model.tbsts_stretch_id.parquet)
        .select("stretch_id", "tollbooth_sts_id")
    )

    ldf_sts = ldf_sts.join(ldf_tbsts_stretch_id, left_on="tollbooth_id", right_on="tollbooth_sts_id", how="left")
    ldf_sts = ldf_sts.join(ldf_stretch, on="stretch_id", how="left").unique()
    ldf_sts = ldf_sts.sort("tollbooth_id")
    ldf_sts.sink_csv(f"./reports/stretch_sts_{data_model_sts.attr.get("year")}.csv")


def tb_imt_stretch_id(year: int):
    data_model = DataModel(year, DataStage.stg)

    ldf_tb_imt_stretch_id = pl.scan_parquet(data_model.tb_imt_stretch_id.parquet)
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
        .join(ldf_tollbooth, left_on="tollbooth_id_in", right_on="tollbooth_id")
        .join(ldf_tollbooth, left_on="tollbooth_id_out", right_on="tollbooth_id")
        .join(ldf_tb_imt, left_on="tollbooth_imt_id_in", right_on="tollbooth_imt_id")
        .join(ldf_tb_imt, left_on="tollbooth_imt_id_out", right_on="tollbooth_imt_id")
        .select(
            "stretch_id", 
            "tollbooth_imt_id_in", "tollbooth_imt_name", 
            "tollbooth_imt_id_out",	"tollbooth_imt_name_right",
            "tollbooth_id_in", "tollbooth_name", 
            "tollbooth_id_out", "tollbooth_name_right", 
        )
    )
    ldf_tb_imt_stretch_id = ldf_tb_imt_stretch_id.sort("stretch_id")
    ldf_tb_imt_stretch_id.sink_csv(f"./reports/tb_imt_stretch_id_{year}.csv")


def complete_stretch_gaps(year: int, stretch_cols: str):
    data_model = DataModel(year, DataStage.stg)
    toll_columns = [
        "motorbike", "car", "car_axle",
        "bus_2_axle", "bus_3_axle", "bus_4_axle", "truck_2_axle",
        "truck_3_axle", "truck_4_axle", "truck_5_axle", "truck_6_axle", 
        "truck_7_axle", "truck_8_axle", "truck_9_axle", "load_axle"
    ]
    ldf_tb_imt_stretch_id = (
        pl.scan_parquet(data_model.tb_imt_stretch_id.parquet)
        .filter(pl.col("stretch_id").is_null())
        .select(pl.exclude("stretch_id"))
    )
    ldf_toll_imt = (
        pl.scan_parquet(data_model.tb_toll_imt.parquet)
        .select(["tollbooth_id_in", "tollbooth_id_out"]+toll_columns)
        .rename({"tollbooth_id_in": "tollbooth_imt_id_in", "tollbooth_id_out": "tollbooth_imt_id_out"})
    )
    ldf_stretch = (
        pl.scan_parquet(data_model.stretchs.parquet)
        .select("stretch_id", "stretch_name")
    )
    ldf_stretch_toll = (
        pl.scan_parquet(data_model.stretchs_toll.parquet)
        .select(
            ["stretch_id"] + toll_columns
        )
    )
    ldf_tb_imt = (
        pl.scan_parquet(data_model.tb_imt.parquet)
        .select("tollbooth_id", "tollbooth_name", "area", "subarea")
        .rename({"tollbooth_id": "tollbooth_imt_id"})
    )
    ldf_tb_imt_stretch_id = (
        ldf_toll_imt
        .join(ldf_tb_imt_stretch_id, on=["tollbooth_imt_id_out", "tollbooth_imt_id_in"])
        .join(ldf_tb_imt, left_on="tollbooth_imt_id_in", right_on="tollbooth_imt_id")
        .rename({"tollbooth_name": "tollbooth_name_in"})
        .join(ldf_tb_imt, left_on="tollbooth_imt_id_out", right_on="tollbooth_imt_id")
        .rename({"tollbooth_name": "tollbooth_name_out"})
    )

    ldf_stretch_toll = tb_stretch_id_imt(ldf_tb_imt_stretch_id, ldf_stretch_toll)
    ldf_stretch_toll = ldf_stretch_toll.select(
       "stretch_id", "tollbooth_id_in", "tollbooth_id_out",
       "tollbooth_imt_id_in", "tollbooth_imt_id_out", 
       "tollbooth_name_in", "tollbooth_name_out", "area", "subarea",
    )
    ldf_stretch_toll = ldf_stretch_toll.with_columns(
        (
            pl.when(pl.col("tollbooth_name_out") != pl.col("tollbooth_name_in"))
            .then(pl.col("tollbooth_name_out") + "_" + pl.col("tollbooth_name_in"))
            .otherwise(pl.col("tollbooth_name_out"))
            .alias("tollbooth_name")
        )
    )
    ldf_stretch_toll = ldf_stretch_toll.select(pl.exclude("tollbooth_name_in", "tollbooth_name_out"))
    ldf_stretch_toll = ldf_stretch_toll.join(ldf_stretch, on="stretch_id")
    ldf_stretch_toll = ldf_stretch_toll.with_columns(
        pl.col("area").replace("n_d", None),
        pl.col("subarea").replace("n_d", None)
    )
    ldf_stretch_toll = ldf_stretch_toll.with_columns(
        plds.str_jw("area", "stretch_name").alias("score_area"),
        plds.str_jw("subarea", "stretch_name").alias("score_sub"),
        (plds.str_jw("tollbooth_name", "stretch_name")).alias("score_tb"),
        (plds.str_lcs_subseq_dist("tollbooth_name", "stretch_name")).alias("score_tb_lcs"),
    )
    ldf_stretch_toll = ldf_stretch_toll.with_columns(
        pl.mean_horizontal("score_area", "score_sub", "score_tb", "score_tb_lcs").alias("score_best")
    )
    ldf_stretch_toll = ldf_stretch_toll.filter(
       pl.col("score_best") == pl.col("score_best").max().over("tollbooth_imt_id_in", "tollbooth_imt_id_out")
    )
    ldf_stretch_toll = ldf_stretch_toll.filter(pl.col("score_best") > 0.35)
    if stretch_cols == "stretch_cols":
        ldf_stretch_toll = ldf_stretch_toll.select("stretch_id", "tollbooth_id_in", "tollbooth_id_out")
        ldf_tb_stretch_id = (
            pl.scan_parquet(data_model.tb_stretch_id.parquet)
            .select("stretch_id", "tollbooth_id_in", "tollbooth_id_out")
        )
        ldf_stretch_toll = ldf_stretch_toll.cast({"tollbooth_id_in": pl.UInt32, "tollbooth_id_out": pl.UInt32})
        ldf_stretch_toll = pl.concat([ldf_tb_stretch_id, ldf_stretch_toll])
    
    ldf_stretch_toll = ldf_stretch_toll.sort("stretch_id")
    ldf_stretch_toll.sink_csv(f"./reports/complete_stretch_gaps_{year}.csv")


if __name__ == "__main__":
    output_filepath = "reports/"

    parser = argparse.ArgumentParser()
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
    parser.add_argument("--complete-stretch-gaps", required=False, type=str, choices=("calc_cols", "stretch_cols"))
    parser.add_argument("--tb-imt-stretch-id", required=False, action="store_true")

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
    elif args.mx_projects:
        mx_projects_report()
    elif args.toll_ref:
        toll_ref(year=2026)
    elif args.tollbooth_stretch_manage:
        tollbooth_stretch_manage(year=2025)
    elif args.stretch_sts:
        stretch_sts(year=2025)
    elif args.complete_stretch_gaps:
        complete_stretch_gaps(year=2025, stretch_cols=args.complete_stretch_gaps)
    elif args.tb_imt_stretch_id:
        tb_imt_stretch_id(year=2025)
