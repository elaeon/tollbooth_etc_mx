import polars as pl

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


tollbooths_schema = {
    "tollbooth_id": pl.String,
    "direction": pl.String,
    "legacy_id": pl.UInt16,
    "tollbooth_name": pl.String,
    "coords": pl.String,
    "status": pl.String,
    "strech_id": pl.UInt16,
    "state": pl.String,
    "place": pl.String,
    "lines": pl.UInt8,
    "type": pl.String,
    "highway": pl.String,
    "operator": pl.String,
    "gate_to": pl.String,
}


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

tollbooth_sts_schema = {
    "index": pl.String,
    "tollbooth_name": pl.String,
    "way": pl.String,
    "highway": pl.String,
    "km": pl.Float32,
    "coords": pl.String,
    "tdpa": pl.UInt32,
    "motorbike": pl.Float32,
    "car": pl.Float32,
    "car-axle": pl.Float32,
    "bus": pl.Float32,
    "truck-2axle": pl.Float32,
    "truck-3axle": pl.Float32,
    "truck-4axle": pl.Float32,
    "truck-5axle": pl.Float32,
    "truck-6axle": pl.Float32,
    "truck-7axle": pl.Float32,
    "truck-8axle": pl.Float32,
    "truck-9axle": pl.Float32,
    "not_classified_vehicle": pl.Float32,
    "vta": pl.UInt64,
    "jan": pl.Float32,
    "feb": pl.Float32,
    "mar": pl.Float32,
    "apr": pl.Float32,
    "may": pl.Float32,
    "jun": pl.Float32,
    "jul": pl.Float32,
    "ago": pl.Float32,
    "sep": pl.Float32,
    "oct": pl.Float32,
    "nov": pl.Float32,
    "dec": pl.Float32,
}