import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

import polars as pl
import polars_h3 as plh3
import argparse

from tb_map_editor.data_files import DataModel, DataStage


def tollbooth_neighbours(year: int):
    data_model = DataModel(year, DataStage.prd)

    hex_resolution = 8
    ldf_tb = pl.scan_parquet(data_model.tollbooths.parquet)
    ldf_tb = ldf_tb.with_columns(
        plh3.latlng_to_cell("lat", "lng", hex_resolution).alias("h3_cell"),
    )
    ldf_tb = ldf_tb.with_columns(
        plh3.grid_disk("h3_cell", 1).alias("h3_disk")
    )
    ldf_tb = ldf_tb.explode("h3_disk")
    ldf_neighbour = ldf_tb.join(
        ldf_tb, left_on="h3_disk", right_on="h3_cell"
    ).select("tollbooth_id", "tollbooth_id_right").filter(pl.col("tollbooth_id") != pl.col("tollbooth_id_right")).unique()
    ldf_neighbour = ldf_neighbour.rename({"tollbooth_id_right": "neighbour_id"})

    ldf_neighbour.sink_parquet(data_model.tb_neighbour.parquet)
    print(f"Saved file in: {data_model.tb_neighbour.parquet}")


def get_tollbooths_osm(country_name: str) -> pl.DataFrame:
    import requests

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



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tollbooth-neighbours", required=False, action="store_true")
    parser.add_argument("--year", required=False, type=int)
    parser.add_argument("--get-tb-osm", required=False, type=str)
    args = parser.parse_args()
    if args.tollbooth_neighbours:
        tollbooth_neighbours(args.year)
    elif args.get_tb_osm:
        get_tollbooths_osm(args.get_tb_osm)
