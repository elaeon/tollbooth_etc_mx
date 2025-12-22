import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import polars as pl
import argparse

from tb_map_editor.data_files import DataPath
from tb_map_editor.schemas import tollbooth_sts_schema, tollbooth_sts_data_schema


def sts_catalog(year):
    data_path = DataPath(year)
    schema = tollbooth_sts_schema.copy()
    schema.update(tollbooth_sts_data_schema)
    del schema["tollboothsts_id"]
    ldf = pl.scan_csv(data_path.tollbooth_sts_full, schema=schema)
    index_ldf = ldf.with_row_index("tollboothsts_id", offset=1)
    index_ldf.select(
        "tollboothsts_id", "tollbooth_name", "way", "highway", "km", "coords"
        ).collect().write_csv(data_path.tollbooth_sts_catalog)
    index_ldf.select(pl.exclude("tollbooth_name", "way", "highway", "km", "coords", "index")
        ).collect().write_csv(data_path.tollbooth_sts_data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", help="data year", required=True, type=int)
    parser.add_argument("--sts-catalog", help="generate tollbooth sts catalog", required=False, action='store_true')
    args = parser.parse_args()
    if args.sts_catalog:
        sts_catalog(args.year)
    