import logging
import pdfplumber
import polars as pl
import sys
import re
from collections import defaultdict
import argparse
from schemas import tollbooth_sts_schema


_log = logging.getLogger(__name__)
_log.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
_log.addHandler(handler)


_sts_cols_map = {
    "m": "motorbike",
    "a": "car",
    "ar": "car-axle",
    "b": "bus",
    "c2": "truck-2axle",
    "c3": "truck-3axle",
    "c4": "truck-4axle",
    "c5": "truck-5axle",
    "c6": "truck-6axle",
    "c7": "truck-7axle",
    "c8": "truck-8axle",
    "c9": "truck-9axle",
    "vnc": "not_classified_vehicle",
    "ene": "jan",
    "abr": "apr",
    "dic": "dec"
}


def extract_index(page_text):
    scope_index = None
    lines_dict = defaultdict(dict)
    index_pat = re.compile(r"^(?P<index>\d{3}(?:-(\d+)){0,1})\s(carretera)")
    tollbooth_pat = re.compile(r"caseta:(?P<tollbooth_name>(.*))")
    strech_pat = re.compile(r"^movimiento:(?P<way>(.*?))\s\b(mex|slp|chih|ver|nl|km)\b")
    road_pat = re.compile(r"(?P<highway>\b(mex|ver|slp|chih|nl)\b[-]{0,1}\w*[-]*\w*){0,1}\s*km:(?P<km>\d+\.\d+)")
    for line in page_text.split("\n"):
        line = line.lower().replace("(", "").replace(")", "")
        match_index = re.search(index_pat, line)
        match_tb = re.search(tollbooth_pat, line)
        match_strech = re.search(strech_pat, line)
        match_road = re.search(road_pat, line)
        if match_index is not None:
            index_dict = match_index.groupdict()
            if scope_index is not None and scope_index != index_dict["index"]:
                if "way" not in lines_dict[scope_index]:
                    raise Exception("no way found")
            scope_index = index_dict["index"]
            if match_tb is not None:
                lines_dict[scope_index].update(
                    {
                        "tollbooth_name": 
                        re.sub(r"\s*-\s*", "-", match_tb.groupdict()["tollbooth_name"].replace("coordenadas", "").strip())
                    }
                )
        if match_strech is not None and scope_index is not None:
            lines_dict[scope_index]["way"] = re.sub(r"\s*-\s*", "-", match_strech["way"].strip())
            _log.debug(line)
        if match_road is not None and scope_index is not None:
            lines_dict[scope_index].update(match_road.groupdict())

        _log.debug(f"{scope_index}, {lines_dict.get(scope_index)}")
    return lines_dict


def lines_dict_to_df(page_text):
    lines_dict = extract_index(page_text)
    columns = []
    for k, v in lines_dict.items():
        d = {"index": k}
        d.update(v)
        columns.append(d)
    df = pl.DataFrame(columns, orient="row")
    return df


def fill_list(small_list, total_size):
    num_spaces = total_size - len(small_list)
    filled_list = []
    for _ in range(num_spaces):
        filled_list.append("")
    filled_list.extend(small_list)
    return filled_list


def main(year, from_page, to_page):
    prev_year = year - 1
    file_path = f"./datos_viales/{year}/33_PC_DV{year}.pdf"
    with pdfplumber.open(file_path) as pdf:
        all_df = []
        for page_num, page in enumerate(pdf.pages, 1):
            if page_num >= from_page:
                extracted_tables = page.extract_tables()
                page_text = page.extract_text(keep_blank_chars=True)
                df_index = lines_dict_to_df(page_text)
                assert df_index.columns == ['index', 'tollbooth_name', 'way', 'highway', 'km']
                dfs = []
                for table_num, table in enumerate(extracted_tables, 1):
                    tdpa = table[1][0]
                    transport_type = list(map(str.lower, table[1][1].split(" ")))
                    vta = table[1][2]
                    volume_month = list(map(str.lower, table[1][3].split(" ")))
                    transport_sts = table[2][1].split(" ")
                    volume_sts = table[2][3].split(" ")
                    if len(volume_sts) < len(volume_month):
                        _log.info(volume_sts)
                        volume_sts = fill_list(volume_sts, len(volume_month))
                        _log.info(volume_sts)
                    header = ["tdpa"] + transport_type + ["vta"] + volume_month
                    body = [tdpa.replace(",", "")] + transport_sts + [vta.replace(",", "")] + volume_sts
                    try:
                        dfs.append(pl.DataFrame([body], schema=header, orient="row"))
                    except pl.exceptions.ShapeError:
                        _log.info(f"Dataframe shape error in page: {page_num}, table: {table_num}")
                df_sts = pl.concat(dfs)
                df = pl.concat([df_index, df_sts], how="horizontal").rename(_sts_cols_map).cast(tollbooth_sts_schema, strict=False)
                _log.info(f"page: {page_num}, df shape: {df.shape}")
                all_df.append(df)
                if page_num == to_page:
                    break
        df_full = pl.concat(all_df).write_csv(f"./data/tables/{prev_year}/tollbooths_sts.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", help="filename year", required=True, type=int)
    parser.add_argument("--from-page", help="parse from page", required=False, type=int, default=55)
    parser.add_argument("--to-page", help="parse to page", required=False, type=int, default=200)
    args = parser.parse_args()
    main(year=args.year, from_page=args.from_page, to_page=args.to_page)
