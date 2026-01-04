import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import logging
import polars as pl
import polars_h3 as plh3

from tb_map_editor.data_files import DataModel
from tb_map_editor.utils.connector import sqlite_url

import argparse


_log = logging.getLogger(__name__)
_log.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
_log.addHandler(handler)


def insert_data_from_parquet(ldf, model_name: str):
    for i_batch, df in enumerate(ldf.collect_batches(chunk_size=100), 1):
        _log.debug(f"saving row: {i_batch}")
        df.write_database(
            table_name=model_name,
            connection=sqlite_url,
            if_table_exists="append",
            engine="adbc"
        )
        
    _log.info(f"saved data in {sqlite_url}")


def insert_tb_from_data(data_model: DataModel):
    parquet_file = data_model.tollbooths.parquet
    model_name = data_model.tollbooths.model.name()
    ldf_tb = pl.scan_parquet(parquet_file).select(pl.exclude("function"))
    insert_data_from_parquet(ldf_tb, model_name)


def insert_tb_sts_from_data(data_model: DataModel):
    parquet_file = data_model.tollbooths_sts.parquet
    model_name = data_model.tollbooths_sts.model.name()
    ldf_tb_sts = pl.scan_parquet(parquet_file)
    insert_data_from_parquet(ldf_tb_sts, model_name)


def insert_tb_imt_from_data(data_model: DataModel):
    parquet_file = data_model.tb_imt.parquet
    model_name = data_model.tb_imt.model.name()
    ldf_tb_imt = pl.scan_parquet(parquet_file)
    next_year = data_model.attr.get("year") + 1
    actual_data_model = DataModel(next_year)
    ldf_tollbooth = pl.scan_parquet(actual_data_model.tollbooths.parquet)

    hex_resolution = 9
    hex_resolution_name = "h3_cell"
    ldf_tb_imt = ldf_tb_imt.with_columns(
        plh3.latlng_to_cell("lat", "lon", hex_resolution).alias(hex_resolution_name)
    )
    ldf_tollbooth = ldf_tollbooth.with_columns(
        plh3.latlng_to_cell("lat", "lon", hex_resolution).alias(hex_resolution_name)
    )
    ldf_tb_imt = ldf_tb_imt.join(ldf_tollbooth.select(hex_resolution_name, "state"), on=hex_resolution_name, how="left")
    ldf_tb_imt = ldf_tb_imt.select(pl.exclude(hex_resolution_name)).unique()
    with pl.Config(tbl_rows=-1):
        ldf_tb_unq = ldf_tb_imt.group_by("tollbooth_imt_id").first()
        insert_data_from_parquet(ldf_tb_unq, model_name)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--new-tb", help="insert-tb", required=False, action='store_true')
    parser.add_argument("--new-tb-imt", help="insert tb imt", required=False, action="store_true")
    parser.add_argument("--new-tb-sts", help="insert-tb-sts-catalog", required=False, action='store_true')
    parser.add_argument("--year", help="year for tb-sts", required=True, type=int)
    args = parser.parse_args()
    data_model = DataModel(args.year)
    if args.new_tb:
        insert_tb_from_data(data_model)
    elif args.new_tb_sts:
        insert_tb_sts_from_data(data_model)
    elif args.new_tb_imt:
        insert_tb_imt_from_data(data_model)
    else:
        parser.print_help()
