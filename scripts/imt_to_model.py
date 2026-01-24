import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import polars as pl
import argparse

from tb_map_editor.data_files import DataModel


def tb_imt_delta(base_year:int, next_year: int):
    base_data_model = DataModel(base_year)
    next_data_model = DataModel(next_year)
    ldf_tb_imt = pl.scan_parquet(base_data_model.tb_imt.parquet)
    ldf_tb_imt_up = pl.scan_parquet(next_data_model.tb_imt.parquet)
    ldf_tb_imt_new = ldf_tb_imt_up.join(ldf_tb_imt, on="tollbooth_imt_id", how="anti")
    ldf_tb_imt_new = ldf_tb_imt_new.with_columns(
        pl.lit("new").alias("status")
    )
    ldf_tb_imt_closed = ldf_tb_imt.join(ldf_tb_imt_up, on="tollbooth_imt_id", how="anti")
    ldf_tb_imt_closed = ldf_tb_imt_closed.with_columns(
        pl.lit("closed").alias("status")
    )
    pl.concat(
        [ldf_tb_imt_new, ldf_tb_imt_closed], how="vertical"
    ).sink_parquet(next_data_model.tb_imt_delta.parquet)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--tb-imt-delta", required=False, type=int)
    args = parser.parse_args()
    if args.tb_imt_delta:
        tb_imt_delta(args.year, args.tb_imt_delta)
