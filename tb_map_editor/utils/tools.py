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


def find_closest_tb(ldf_neighbour):
    i = 0
    while i < 2:
        # Step 1: Find closest neighbour for each origin (tollbooth_id)
        ldf_neighbour_closest = ldf_neighbour.filter(
            pl.col("distance") == pl.col("distance").min().over("tollbooth_id")
        )
        # Step 2: Ensure each neighbour_id is matched to at most one tollbooth_id 
        #         (if multiple tollbooths have same closest neighbour, keep the one with smallest distance)
        ldf_neighbour_unique = ldf_neighbour_closest.filter(
            pl.col("distance") == pl.col("distance").min().over("neighbour_id")
        )
        # Step 3: Get each tollbooth and neighbour left without a close match
        ldf_ids_unmatched_tb = ldf_neighbour.join(
            ldf_neighbour_unique, on=["tollbooth_id"], how="anti"
        )
        ldf_ids_unmatched_nb = ldf_neighbour.join(
            ldf_neighbour_unique, on=["neighbour_id"], how="anti"
        )
        ldf_imt_ids_unmatched = ldf_ids_unmatched_tb.join(
            ldf_ids_unmatched_nb, on=["tollbooth_id", "neighbour_id"]
        ).select(pl.exclude("distance_right"))
        # Step 4: Do another search with the remaining relations
        ldf_neighbour = ldf_imt_ids_unmatched
        i = i + 1
        yield ldf_neighbour_unique
