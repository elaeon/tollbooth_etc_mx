import polars as pl


def tb_stretch_id_imt(ldf_toll_imt, ldf_stretch_toll):
    ldf_stretch_toll = ldf_stretch_toll.with_columns(
        pl.all().fill_null(strategy="zero"),
    ).with_columns(
        pl.col("car").cast(pl.String)
    )
    ldf_toll_imt = ldf_toll_imt.with_columns(
        pl.col("car").cast(pl.String)
    )
    ldf_stretch = ldf_toll_imt.join(
        ldf_stretch_toll, on=[
            "motorbike", "car", "bus_2_axle", "bus_3_axle", "bus_4_axle",
            "truck_2_axle", "truck_3_axle", "truck_4_axle", "truck_5_axle",
            "truck_6_axle", "truck_7_axle", "truck_8_axle", "truck_9_axle"
        ]
    )
    return ldf_stretch