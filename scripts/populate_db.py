import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import logging
import polars as pl
import polars_h3 as plh3
import sqlite3

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
    parquet_file = data_model.tb_sts.parquet
    model_name = data_model.tb_sts.model.name()
    ldf_tb_sts = pl.scan_parquet(parquet_file)
    ldf_tb_sts = ldf_tb_sts.with_columns(
        pl.lit(data_model.attr.get("year")).alias("info_year")
    )
    insert_data_from_parquet(ldf_tb_sts, model_name)


def insert_tb_imt_from_data(data_model: DataModel, source: str):
    if source == "delta":
        parquet_file = data_model.tb_imt_delta.parquet
    else:
        parquet_file = data_model.tb_imt.parquet
    model_name = data_model.tb_imt.model.name()
    ldf_tb_imt = pl.scan_parquet(parquet_file)
    insert_data_from_parquet(ldf_tb_imt, model_name)


def insert_tb_from_db(data_model: DataModel):
    query = """
        SELECT * FROM tollbooth
    """
    conn = sqlite3.connect(sqlite_url.replace("sqlite:///", ""))
    df = pl.read_database(query, connection=conn)
    df.write_parquet(data_model.tollbooths.parquet)
    _log.info(f"Saved data in {data_model.tollbooths.parquet}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--new-tb", help="insert-tb", required=False, action='store_true')
    parser.add_argument("--new-tb-imt", help="insert tb imt", required=False, type=str)
    parser.add_argument("--new-tb-sts", help="insert-tb-sts-catalog", required=False, action='store_true')
    parser.add_argument("--export-tb", action="store_true")
    parser.add_argument("--year", help="model year", required=True, type=int)
    args = parser.parse_args()
    data_model = DataModel(args.year)
    if args.new_tb:
        insert_tb_from_data(data_model)
    elif args.new_tb_sts:
        insert_tb_sts_from_data(data_model)
    elif args.new_tb_imt:
        insert_tb_imt_from_data(data_model, args.new_tb_imt)
    elif args.export_tb:
        insert_tb_from_db(data_model)
    else:
        parser.print_help()
