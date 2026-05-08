import polars as pl

from src.model import Stretch

assert Stretch.dict_schema() == {
    "stretch_id": pl.UInt32,
    "stretch_name": pl.String,
    "stretch_length_km": pl.Float64,
    "sct_id_via": pl.UInt16,
    "road_id": pl.UInt16,
    "manage": pl.String,
    "way": pl.String,
    "info_year": pl.UInt16,
}
