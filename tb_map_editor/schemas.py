import polars as pl
from .model import Tollbooth, TollboothSts, TollboothStsData


strechs_tolls_2025_schema = {
    "strech_id": pl.UInt16,
    "motorbike": pl.Float32,
    "car": pl.Float32,
    "car-axle": pl.Float32,
    "bus-2axle": pl.Float32,
    "bus-3axle": pl.Float32,
    "bus-4axle": pl.Float32,
    "truck-2axle": pl.Float32,
    "truck-3axle": pl.Float32,
    "truck-4axle": pl.Float32,
    "truck-5axle": pl.Float32,
    "truck-6axle": pl.Float32,
    "truck-7axle": pl.Float32,
    "truck-8axle": pl.Float32,
    "truck-9axle": pl.Float32,
    "load-axle": pl.Float32,
    "toll_ref": pl.String,
    "motorbike-axle": pl.Float32,
    "car-rush-hour": pl.Float32,
    "car-evening-hour": pl.Float32,
    "pedestrian": pl.Float32,
    "valid_from": pl.Date
}

strechs_tolls_2024_schema = {
    "car-rush-hour-2": pl.Float32,
    "car-evening-hour-2": pl.Float32,
    "car-morning-night-hour-2": pl.Float32
}
strechs_tolls_2024_schema.update(strechs_tolls_2025_schema)
strechs_tolls_2023_schema = strechs_tolls_2025_schema.copy()
strechs_tolls_2022_schema = strechs_tolls_2025_schema.copy()
strechs_tolls_2021_schema = strechs_tolls_2025_schema.copy()

strechs_schema = {
    "strech_id": pl.UInt16,
    "strech_name": pl.String,
    "strech_length_km": pl.Float32,
    "sct_idVia": pl.UInt16,
    "road_id": pl.UInt16,
    "strech_type": pl.String,
    "managed_by": pl.String
}


tollbooth_schema = Tollbooth.dict_schema()


tollbooth_strech_schema = {
    "tollbooth_id": pl.UInt16,
    "name": pl.String,
    "coords": pl.String,
    "status": pl.String,
    "strech_id": pl.UInt16,
    "type": pl.String,
    "descr": pl.String,
    "highway": pl.String
}

roads_schema = {
    "road_id": pl.UInt16,
    "road_name": pl.String,
    "road_date": pl.Date,
    "project_mx_id": pl.UInt16,
    "fonandin": pl.String,
    "length": pl.Float32,
    "notes": pl.String
}

tollbooth_sts_schema = TollboothSts.dict_schema()
tollbooth_sts_data_schema = TollboothStsData.dict_schema()
