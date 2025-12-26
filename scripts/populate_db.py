import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import logging
import polars as pl

from tb_map_editor.model import Tollbooth, TollboothSts, TollboothStsData, TmpTb
from tb_map_editor.data_files import DataPathSchema
from tb_map_editor.schemas import tollbooth_schema, tollbooth_sts_schema, tollbooth_sts_data_schema
from tb_map_editor.utils.connector import sqlite_url

import argparse


_log = logging.getLogger(__name__)
_log.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
_log.addHandler(handler)


def insert_tb_from_csv(year: int):
    data_path = DataPathSchema(year)
    ldf = pl.scan_csv(data_path.tollbooths_catalog.path, schema=data_path.tollbooths_catalog.schema)
    for i_batch, df in enumerate(ldf.collect_batches(chunk_size=100), 1):
        _log.debug(f"saving row: {i_batch}")
        df.write_database(
            table_name=Tollbooth.__name__.lower(),
            connection=sqlite_url,
            if_table_exists="append",
            engine="adbc"
        )
        
    _log.info(f"saved data in {sqlite_url}")


def insert_tb_sts_from_csv(year: int):
    data_path = DataPathSchema(year)
    ldf = pl.scan_csv(data_path.tollbooths_sts_catalog.path, schema=data_path.tollbooths_sts_catalog.schema)
    print(tollbooth_sts_schema)
    for i_batch, df in enumerate(ldf.collect_batches(chunk_size=100), 1):
        _log.debug(f"saving row: {i_batch}")
        df.write_database(
            table_name=TollboothSts.__name__.lower(),
            connection=sqlite_url,
            if_table_exists="append",
            engine="adbc"
        )
        
    _log.info(f"saved data in {sqlite_url}")


def insert_tb_sts_data_from_csv(year: int):
    data_path = DataPathSchema(year)
    ldf = pl.scan_csv(data_path.tollbooths_sts_data.path, schema=data_path.tollbooths_sts_data.schema)
    for i_batch, df in enumerate(ldf.collect_batches(chunk_size=100), 1):
        df = df.with_columns(pl.lit(year).alias("info_year"))
        _log.debug(f"saving row: {i_batch}")
        df.write_database(
            table_name=TollboothStsData.__name__.lower(),
            connection=sqlite_url,
            if_table_exists="append",
            engine="adbc"
        )
        
    _log.info(f"saved data in {sqlite_url}")


def insert_tmp_tb(filename: str):
    ldf = pl.scan_csv(filename, schema=TmpTb.dict_schema())
    for i_batch, df in enumerate(ldf.collect_batches(chunk_size=100), 1):
        _log.debug(f"saving row: {i_batch}")
        df.write_database(
            table_name=TmpTb.__name__.lower(),
            connection=sqlite_url,
            if_table_exists="append",
            engine="adbc"
        )
        
    _log.info(f"saved data in {sqlite_url}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--new-tb", help="insert-tb", required=False, action='store_true')
    parser.add_argument("--new-tb-sts", help="insert-tb-sts-catalog", required=False, action='store_true')
    parser.add_argument("--new-tb-sts-data", help="insert-tb-sts-data", required=False, action='store_true')
    parser.add_argument("--year", help="year for tb-sts", required=False, type=int)
    parser.add_argument("--insert-tmp-tb", required=False, type=str)
    args = parser.parse_args()
    if args.new_tb:
        insert_tb_from_csv(args.year)
    elif args.new_tb_sts:
        insert_tb_sts_from_csv(args.year)
    elif args.new_tb_sts_data:
        insert_tb_sts_data_from_csv(args.year)
    elif args.insert_tmp_tb:
        insert_tmp_tb(args.insert_tmp_tb)
    else:
        parser.print_help()
