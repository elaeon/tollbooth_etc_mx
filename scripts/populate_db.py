import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import logging
import polars as pl

from tb_map_editor.model import Tollbooth
from tb_map_editor.data_files import file_path
from tb_map_editor.schemas import tollbooth_schema
from tb_map_editor.utils.connector import sqlite_url

import argparse


_log = logging.getLogger(__name__)
_log.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
_log.addHandler(handler)


def insert_tb_from_csv():
    ldf = pl.scan_csv(file_path["tollbooth"], schema=tollbooth_schema)
    df = pl.DataFrame(schema=tollbooth_schema)
    df.write_database(
        table_name=Tollbooth.__name__.lower(),
        connection=sqlite_url,
        if_table_exists="replace",
        engine="adbc"
    )
    for i_batch, df in enumerate(ldf.collect_batches(chunk_size=100), 1):
        _log.debug(f"saving row: {i_batch}")
        df.write_database(
            table_name=Tollbooth.__name__.lower(),
            connection=sqlite_url,
            if_table_exists="append",
            engine="adbc"
        )
        
    _log.info(f"saved data in {sqlite_url}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--new-tb", help="insert-tb", required=False, action='store_true')
    args = parser.parse_args()
    if args.new_tb:
        insert_tb_from_csv()
