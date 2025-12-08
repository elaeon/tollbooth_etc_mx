import logging
import pdfplumber
import polars as pl
import sys
import re
from collections import defaultdict
import argparse


def extract_index(page_text):
    scope_index = None
    lines_dict = defaultdict(dict)
    index_pat = re.compile(r"^(?P<index>\d{3}(?:-(\d+)){0,1})")
    tollbooth_pat = re.compile(r"caseta:(?P<tollbooth_name>(.*))")
    strech_pat = re.compile(r"^movimiento:(?P<direction>\w+(\s\w+)*(?:-\w+(\s\w+)*))\s(mex){0,1}")
    road_pat = re.compile(r"(?P<road>mex(?:-(\w+))+?)\s*km:(?P<place>\d+\.\d+)")
    for i, line in enumerate(page_text.split("\n"), 1):
        line = line.lower()
        match_index = re.search(index_pat, line)
        match_tb = re.search(tollbooth_pat, line)
        match_strech = re.search(strech_pat, line)
        match_road = re.search(road_pat, line)
        if match_index is not None:
            index_dict = match_index.groupdict()
            scope_index = index_dict["index"]
            if match_tb is not None:
                lines_dict[scope_index].update(
                    {"tollbooth_name": match_tb.groupdict()["tollbooth_name"].replace("coordenadas", "")}
                )
        elif match_strech is not None and scope_index is not None:
            lines_dict[scope_index].update(match_strech.groupdict())
            if match_road is not None:
                lines_dict[scope_index].update(match_road.groupdict())

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

    
def main():
    year = 2025
    file_path = f"./datos_viales/{year}/33_DV{year}_PlazasCobro{year}.pdf"
    with pdfplumber.open(file_path) as pdf:
        for i, page in enumerate(pdf.pages, 1):
            if i >= 55:
                extracted_tables = page.extract_tables()
                page_text = page.extract_text()
                df_index = lines_dict_to_df(page_text)
                dfs = []
                for table in extracted_tables:
                    tdpa = table[1][0]
                    transport_type = table[1][1].split(" ")
                    vta = table[1][2]
                    volume_month = table[1][3].split(" ")
                    transport_sts = table[2][1].split(" ")
                    volume_sts = table[2][3].split(" ")
                    header = ["tdpa"] + transport_type + ["vta"] + volume_month
                    body = [tdpa] + transport_sts + [vta] + volume_sts
                    dfs.append(pl.DataFrame([body], schema=header, orient="row"))
                df_sts = pl.concat(dfs)
                #print(df)
                #print(df_index)
                df = pl.concat([df_index, df_sts], how="horizontal")
                #print(df)
                if i == 60:
                    break


if __name__ == "__main__":
    main()
