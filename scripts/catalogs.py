import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import polars as pl
import argparse

from tb_map_editor.data_files import file_path
from tb_map_editor.schemas import tollbooth_sts_schema, tollbooth_sts_data_schema


def sts_catalog():
    schema = tollbooth_sts_schema.copy()
    schema.update(tollbooth_sts_data_schema)
    del schema["tollboothsts_id"]
    ldf = pl.scan_csv(file_path["tollbooth_sts"], schema=schema)
    index_ldf = ldf.with_row_index("tollboothsts_id", offset=1)
    index_ldf.select(
        "tollboothsts_id", "tollbooth_name", "way", "highway", "km", "coords"
        ).collect().write_csv("./data/tables/tollbooths_sts_catalog.csv")
    index_ldf.select(pl.exclude("tollbooth_name", "way", "highway", "km", "coords", "index")
        ).collect().write_csv("./data/tables/tollbooths_sts_data.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sts-catalog", help="generate tollbooth sts catalog", required=False, action='store_true')
    args = parser.parse_args()
    if args.sts_catalog:
        sts_catalog()
    