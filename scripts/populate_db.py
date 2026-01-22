import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import logging
import polars as pl
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
    ldf_tb = pl.scan_parquet(parquet_file)
    insert_data_from_parquet(ldf_tb, model_name)


def insert_tb_sts_from_data(data_model: DataModel):
    parquet_file = data_model.tb_sts.parquet
    model_name = data_model.tb_sts.model.name()
    ldf_tb_sts = pl.scan_parquet(parquet_file)
    #ldf_tb_sts = ldf_tb_sts.with_columns(
    #    pl.lit(data_model.attr.get("year")).alias("info_year")
    #)
    insert_data_from_parquet(ldf_tb_sts, model_name)


def insert_tb_imt_from_data(data_model: DataModel):
    parquet_file = data_model.tb_imt.parquet
    model_name = data_model.tb_imt.model.name()
    ldf_tb_imt = pl.scan_parquet(parquet_file)
    insert_data_from_parquet(ldf_tb_imt, model_name)


def insert_tb_from_db(data_model: DataModel):
    query = f"""
        SELECT * FROM tollbooth WHERE info_year={data_model.attr.get("year")}
    """
    conn = sqlite3.connect(sqlite_url.replace("sqlite:///", ""))
    df = pl.read_database(query, connection=conn)
    df.write_parquet(data_model.tollbooths.parquet)
    _log.info(f"Saved data in {data_model.tollbooths.parquet}")


def insert_tb_stretch_from_data(data_model: DataModel):
    ldf_tb_stretch = pl.scan_parquet(data_model.tb_stretch_id.parquet)
    model_name = data_model.tb_stretch_id.model.name()
    insert_data_from_parquet(ldf_tb_stretch, model_name)


def insert_stretch_from_data(data_model: DataModel):
    ldf_stretch = pl.scan_parquet(data_model.stretchs.parquet)
    model_name = data_model.stretchs.model.name()
    insert_data_from_parquet(ldf_stretch, model_name)


def insert_road_from_data(data_model: DataModel):
    ldf_road = pl.scan_parquet(data_model.roads.parquet)
    model_name = data_model.roads.model.name()
    insert_data_from_parquet(ldf_road, model_name)


def insert_stretch_toll_from_data(data_model: DataModel):
    ldf_stretch_toll = pl.scan_parquet(data_model.stretchs_toll.parquet)
    model_name = data_model.stretchs_toll.model.name()
    insert_data_from_parquet(ldf_stretch_toll, model_name)


def clean_db():
    table_parameter = "{table_parameter}"
    drop_table_query = f"DROP TABLE {table_parameter};"
    get_tables_query = "SELECT name FROM sqlite_schema WHERE type='table';"

    def get_tables(conn):
        cur = conn.cursor()
        cur.execute(get_tables_query)
        tables = cur.fetchall()
        cur.close()
        return tables

    def delete_tables(conn, tables):
        cur = conn.cursor()
        for table, in tables:
            _log.info(f"Delete table {table}")
            sql = drop_table_query.replace(table_parameter, table)
            cur.execute(sql)
        cur.close()

    conn = sqlite3.connect(sqlite_url.replace("sqlite:///", ""))
    tables = get_tables(conn)
    delete_tables(conn, tables)

    _log.info(f"Cleaned data in {sqlite_url}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--new-tb", help="insert-tb", required=False, action='store_true')
    parser.add_argument("--new-tb-imt", help="insert tb imt", required=False, action="store_true")
    parser.add_argument("--new-tb-sts", help="insert-tb-sts-catalog", required=False, action='store_true')
    parser.add_argument("--new-tb-stretch", required=False, action="store_true")
    parser.add_argument("--new-stretch", required=False, action="store_true")
    parser.add_argument("--new-road", required=False, action="store_true")
    parser.add_argument("--new-stretch-toll", required=False, action="store_true")
    parser.add_argument("--export-tb", action="store_true")
    parser.add_argument("--year", help="model year", required=False, type=int)
    parser.add_argument("--clean-db", required=False, action="store_true")
    args = parser.parse_args()
    data_model = DataModel(args.year)
    if args.new_tb:
        insert_tb_from_data(data_model)
    elif args.new_tb_sts:
        insert_tb_sts_from_data(data_model)
    elif args.new_tb_imt:
        insert_tb_imt_from_data(data_model)
    elif args.export_tb:
        insert_tb_from_db(data_model)
    elif args.new_tb_stretch:
        insert_tb_stretch_from_data(data_model)
    elif args.new_stretch:
        insert_stretch_from_data(data_model)
    elif args.new_road:
        insert_road_from_data(data_model)
    elif args.new_stretch_toll:
        insert_stretch_toll_from_data(data_model)
    elif args.clean_db:
        clean_db()
    else:
        parser.print_help()
