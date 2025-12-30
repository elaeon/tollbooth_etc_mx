import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import polars as pl
import polars_h3 as plh3
import argparse

from tb_map_editor.data_files import DataPathSchema


def sts_catalog():
    from_year = 2018
    to_year = 2024

    def concat_data():
        df_dict = {}
        for year in range(from_year, to_year + 1):
            data_path = DataPathSchema(year)
            df_dict[year] = pl.read_parquet(
                data_path.tollbooths_sts.parquet, columns=["tollbooth_name", "way", "lat", "lon"]
            ).with_columns(
                pl.lit(year).alias("year")
            )
        df_tbsts = pl.concat(df_dict.values())
        return df_tbsts

    df_tbsts = concat_data()
    hex_resolution = 10
    df_tbsts = df_tbsts.with_columns(
        h3_cell=plh3.latlng_to_cell("lat", "lon", hex_resolution)
    )
    df_h = df_tbsts.select(
        "h3_cell", "tollbooth_name"
    ).group_by("h3_cell", "tollbooth_name").len(name="tb_len").select("h3_cell").group_by("h3_cell").len().filter(pl.col("len") > 1)
    df_exceptions = df_h.join(df_tbsts, on="h3_cell").select("h3_cell", "tollbooth_name", "way", "year").unique().with_columns(pl.lit("bad").alias("status"))

    df_tbsts_id = df_tbsts.select(
        "h3_cell", "tollbooth_name", "way"
    ).unique(maintain_order=True).with_columns(pl.lit("ok").alias("status"))

    df_tbsts_id = df_tbsts_id.join(df_exceptions, on=["h3_cell", "tollbooth_name", "way"], how="left").filter(pl.col("status_right").is_null())
    df_tbsts_id = df_tbsts_id.with_row_index("tollboothsts_id", 1).with_columns(pl.lit(from_year).alias("ref_year"))
    
    df_exceptions = df_exceptions.group_by("h3_cell", "tollbooth_name", "way").agg(pl.min("year"))

    df_last_row = df_tbsts_id.tail(1)
    start_index = df_last_row.row(0, named=True)["tollboothsts_id"]

    df_exceptions = df_exceptions.sort("h3_cell", "year")
    df_except_index = df_exceptions.select("h3_cell", "way").unique(maintain_order=True).with_row_index("tollboothsts_id", start_index + 1)
    df_exceptions = df_exceptions.join(df_except_index, on=["h3_cell", "way"])
    df_exceptions = df_exceptions.rename({"year": "ref_year"}).select("tollboothsts_id", "h3_cell", "tollbooth_name", "way", "ref_year")
    
    df_tbsts_id = pl.concat([
        df_tbsts_id.select("tollboothsts_id", "h3_cell", "tollbooth_name", "way", "ref_year"), 
        df_exceptions.sort("h3_cell")
    ])
    df_tbsts_id.write_parquet(DataPathSchema(to_year).tbsts_id.parquet)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=False, type=int)
    parser.add_argument("--sts-merge-except", help="merge into tbsts catalog with the exptions file", required=False, action="store_true")
    parser.add_argument("--sts-catalog", help="generate tollbooth sts id catalog", required=False, action="store_true")
    args = parser.parse_args()
    if args.sts_catalog:
        sts_catalog()
    