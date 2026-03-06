import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import polars as pl
import polars_h3 as plh3
import argparse
import requests
import time

from tb_map_editor.data_files import DataModel, DataStage


def _tollbooth_neightbours(ldf: pl.LazyFrame, hex_resolution:int = 8):
    
    ldf = ldf.with_columns(
        plh3.latlng_to_cell("lat", "lng", hex_resolution).alias("h3_cell"),
    )
    ldf = ldf.with_columns(
        plh3.grid_disk("h3_cell", 1).alias("h3_disk")
    )
    ldf = ldf.explode("h3_disk")
    ldf_neighbour = ldf.join(
        ldf, left_on="h3_disk", right_on="h3_cell"
    )
    ldf_neighbour = ldf_neighbour.with_columns(
        plh3.great_circle_distance("lat", "lng", "lat_right", "lng_right").alias("distance"),
        (pl.col("scope") + "-" + pl.col("scope_right")).alias("scope")
    )
    ldf_neighbour = ldf_neighbour.select("tollbooth_id", "tollbooth_id_right", "distance", "scope").filter(pl.col("tollbooth_id") != pl.col("tollbooth_id_right")).unique()
    ldf_neighbour = ldf_neighbour.rename({"tollbooth_id_right": "neighbour_id"})
    
    return ldf_neighbour


def tollbooth_neighbours(year: int):
    data_model = DataModel(year, DataStage.stg)
    data_model_stg = DataModel(year, DataStage.stg)
    data_model_sts = DataModel(year - 1, DataStage.prd)

    ldf_tb = pl.scan_parquet(
        data_model.tollbooths.parquet
    ).select("tollbooth_id", "lat", "lng")
    ldf_tb_imt = pl.scan_parquet(
        data_model_stg.tb_imt.parquet
    ).select("tollbooth_id", "lat", "lng")
    ldf_tb_sts = pl.scan_parquet(
        data_model_sts.tb_sts.parquet
    ).select(["tollbooth_id", "lat", "lng"]).cast({"tollbooth_id": pl.UInt16})
    ldf_tb = ldf_tb.with_columns(
        pl.lit("local").alias("scope")
    )
    ldf_tb_imt = ldf_tb_imt.with_columns(
        pl.lit("imt").alias("scope")
    )
    ldf_tb_sts = ldf_tb_sts.with_columns(
        pl.lit("sts").alias("scope")
    )
    ldf_imt = pl.concat([ldf_tb, ldf_tb_imt])
    ldf_sts = pl.concat([ldf_tb, ldf_tb_sts])
    ldf_neighbour_imt = _tollbooth_neightbours(ldf_imt)
    ldf_neighbour_sts = _tollbooth_neightbours(ldf_sts, hex_resolution=4)

    ldf_neighbour_imt = ldf_neighbour_imt.filter(pl.col("scope") != "imt-local")
    ldf_neighbour_sts = ldf_neighbour_sts.filter((pl.col("scope") != "sts-local") & (pl.col("scope") != "local-local"))
    ldf_neighbour = pl.concat([ldf_neighbour_imt, ldf_neighbour_sts])
    ldf_neighbour = ldf_neighbour.with_columns(
        info_year=pl.lit(year)
    )
    ldf_neighbour.sink_parquet(data_model.tb_neighbour.parquet)
    print(ldf_neighbour.group_by("scope").len().collect())
    print(f"Saved file in: {data_model.tb_neighbour.parquet}")


def get_tollbooths_osm(year: int, country_name: str) -> pl.DataFrame:
    query = f'''
    [out:json];
    area["name"="{country_name}"]->.searchArea;
    (
    node["barrier"="toll_booth"](area.searchArea);
    );
    out center;
    '''
    response = requests.post("https://overpass-api.de/api/interpreter", data=query)
    print(response.text)
    data = response.json()
    
    # Parse the results into a dataframe
    tollbooths = [
        {
            "osm_id": elem.get("id"),
            "lat": elem["center"]["lat"] if "center" in elem else elem.get("lat"),
            "lng": elem["center"]["lon"] if "center" in elem else elem.get("lon"),
            "name": elem.get("tags", {}).get("name", "")
        }
        for elem in data.get("elements", [])
    ]
    
    df = pl.DataFrame(tollbooths)
    df.write_csv("./tmp_data/osm_tb.csv")


def get_osm_routing_distance(lat_in: float, lng_in: float, lat_out: float, lng_out: float) -> float:
    """
    Get routing distance (in kilometers) between two points using OSRM API (OpenStreetMap Routing).
    Returns float distance in kilometers. Returns -1 if failed.
    """
    url = f"http://router.project-osrm.org/route/v1/driving/{lng_in},{lat_in};{lng_out},{lat_out}?overview=false"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        routes = data.get("routes", [])
        if routes:
            # OSRM returns distance in meters
            return routes[0]["distance"] / 1000.0
        else:
            return -1
    except Exception as e:
        print(f"Error fetching routing distance: {e}")
        return -1


def tb_distance(year: int):
    data_model = DataModel(year, DataStage.stg)

    ldf_tollbooth = (
        pl.scan_parquet(data_model.tollbooths.parquet)
        .select("tollbooth_id", "lat", "lng")
    )
    ldf_tb_stretch_id = (
        pl.scan_parquet(data_model.tb_stretch_id.parquet)
        .select("stretch_id", "tollbooth_id_in", "tollbooth_id_out")
        .filter(
            pl.col("tollbooth_id_in") != pl.col("tollbooth_id_out")
        )
        .join(
            ldf_tollbooth,
            left_on="tollbooth_id_in",
            right_on="tollbooth_id"
        )
        .rename({"lat": "lat_in", "lng": "lng_in"})
        .join(
            ldf_tollbooth,
            left_on="tollbooth_id_out",
            right_on="tollbooth_id"
        )
        .rename({"lat": "lat_out", "lng": "lng_out"})
    )

    def save_batch(distance_tb: dict, batch_num: int) -> dict:
        data = {"key": [], "distance": []}
        for k, v in distance_tb.items():
            data["key"].append(k)
            data["distance"].append(v)

        df_distance = pl.DataFrame(data)
        df_distance = (
            df_distance.with_columns(
                pl.col("key").str.split("-").list.to_struct(fields=fields)
            )
            .unnest("key")
        )
        df_distance.write_csv(f"./tmp_data/distance/tb_distance_{batch_num}.csv")
        return {}
        
    fields = ["stretch_id", "tollbooth_id_in", "tollbooth_id_out"]
    batch_i = 1
    batch_size = 50
    while True:
        distance_tb = {}
        for row in ldf_tb_stretch_id.collect().iter_rows(named=True):
            key = f"{row[fields[0]]}-{row[fields[1]]}-{row[fields[2]]}"
            distance = get_osm_routing_distance(row["lat_in"], row["lng_in"], row["lat_out"], row["lng_in"])
            distance_tb[key] = distance
            print(key, distance)
            time.sleep(1)
    
            if len(distance_tb) % batch_size == 0:
                distance_tb = save_batch(distance_tb, batch_i)
                batch_i += 1
        save_batch(distance_tb, batch_i)
        break


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tollbooth-neighbours", required=False, action="store_true")
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--get-tb-osm", required=False, type=str)
    parser.add_argument("--distance", required=False, action="store_true")

    args = parser.parse_args()
    if args.tollbooth_neighbours:
        tollbooth_neighbours(args.year)
    elif args.get_tb_osm:
        get_tollbooths_osm(args.year, args.get_tb_osm)
    elif args.distance:
        tb_distance(args.year)
