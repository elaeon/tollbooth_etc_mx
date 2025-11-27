import logging
import pdfplumber
import polars as pl
import sys
from collections import defaultdict
import argparse


_log = logging.getLogger(__name__)
_log.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
_log.addHandler(handler)

_headers = ["tollbooth_id", "tollbooth_name", "motorbike", "car", "car-axle", "bus-2axle", "bus-3axle", "bus-4axle", 
"truck-2axle", "truck-3axle", "truck-4axle", "truck-5axle", "truck-6axle", 
"truck-7axle", "truck-8axle", "truck-9axle", "load-axle"
]
_old_headers = ["via_principal"] + _headers


def cast(df):
    float_type = pl.Float32
    int_type = pl.Int16

    df_cast = df.with_columns([
        pl.when(pl.col("tollbooth_id").str.split("-").list.len() == 2).then(pl.col("tollbooth_id").str.split("-").list.get(-1).str.replace(" ", "").replace("", None)).otherwise(pl.col("tollbooth_id").replace("", None)).cast(int_type),
        pl.col("motorbike").str.replace("\\$", "").str.replace_all(" ", "").str.replace_all(",", "").replace("-", None),#.cast(float_type),
        pl.col("car").str.replace("\\$", "").str.replace_all(" ", "").str.replace_all(",", "").replace("-", None).cast(float_type, strict=False),
        pl.col("car-axle").str.replace("\\$", "").str.replace_all(" ", "").str.replace_all(",", "").replace({"-": None, "": None}),
        pl.col("bus-2axle").str.replace("\\$", "").str.replace_all(" ", "").str.replace_all(",", "").replace("-", None).cast(float_type, strict=False),
        pl.col("bus-3axle").str.replace("\\$", "").str.replace_all(" ", "").str.replace_all(",", "").replace("-", None).cast(float_type, strict=False),
        pl.col("bus-4axle").str.replace("\\$", "").str.replace_all(" ", "").str.replace_all(",", "").replace("-", None).cast(float_type, strict=False),
        pl.col("truck-2axle").str.replace("\\$", "").str.replace_all(" ", "").str.replace_all(",", "").replace("-", None).cast(float_type, strict=False),
        pl.col("truck-3axle").str.replace("\\$", "").str.replace_all(" ", "").str.replace_all(",", "").replace("-", None).cast(float_type, strict=False),
        pl.col("truck-4axle").str.replace("\\$", "").str.replace_all(" ", "").str.replace_all(",", "").replace("-", None).cast(float_type, strict=False),
        pl.col("truck-5axle").str.replace("\\$", "").str.replace_all(" ", "").str.replace_all(",", "").replace("-", None).cast(float_type, strict=False),
        pl.col("truck-6axle").str.replace("\\$", "").str.replace_all(" ", "").str.replace_all(",", "").replace("-", None).cast(float_type, strict=False),
        pl.col("truck-7axle").str.replace("\\$", "").str.replace_all(" ", "").str.replace_all(",", "").replace("-", None).cast(float_type, strict=False),
        pl.col("truck-8axle").str.replace("\\$", "").str.replace_all(" ", "").str.replace_all(",", "").replace("-", None).cast(float_type, strict=False),
        pl.col("truck-9axle").str.replace("\\$", "").str.replace_all(" ", "").str.replace_all(",", "").replace("-", None).cast(float_type, strict=False),
        pl.when(pl.col("load-axle").eq(".")).then(pl.col("load-axle").replace(".", None)).otherwise(
           pl.col("load-axle").str.replace("\\$", "").str.replace_all(" ", "").str.replace_all(",", "").replace({"-": None, "": None})
        ).cast(float_type, strict=False),
    ])
    return df_cast


def main(year):
    pdf_path =  f"./tarifas/pase/tarifas_pase_{year}.pdf"

    # Extract tables from each page of the PDF
    with pdfplumber.open(pdf_path) as pdf:
        dfs = []
        dfs_remaind = defaultdict(list)
        for i, page in enumerate(pdf.pages, 1):
            #if i != 13:
            #    continue
            extracted_tables = page.extract_tables()
            _log.info(f"page {i}, tables: {len(extracted_tables)}")
            if len(extracted_tables) > 0:
                tables_indexes = return_tables_index(extracted_tables)
                _log.info(tables_indexes)
                if len(tables_indexes) > 0:
                    _fix_extracted_tables = []
                    fix_tables = defaultdict(list)
                    for i_table, row in tables_indexes.items():
                        add_table_to_dict(extracted_tables[i_table][:row[0][0]], fix_tables, i_table)
                        for i_row, j_row in row:
                            if i_row != j_row:
                                add_table_to_dict(extracted_tables[i_table][i_row+1:j_row], fix_tables, i_table)
                        add_table_to_dict(extracted_tables[i_table][row[-1][1]+1:], fix_tables, i_table)
                    
                    for i_table, table in enumerate(extracted_tables):
                        if not i_table in fix_tables:
                            _fix_extracted_tables.append(table)
                        else:
                            for fix_table in fix_tables[i_table]:
                                _fix_extracted_tables.append(fix_table)
                    extracted_tables = _fix_extracted_tables

            for table in extracted_tables:
                if table:  # Only add non-empty tables
                    index = find_toll(table)
                    df = pl.DataFrame(table[index:])
                    df = df.transpose()
                    df = df.with_columns(pl.col("column_0").str.replace(" ", "").replace("", None))
                    if df.filter(pl.col("column_0").is_null()).height == df.height:
                        df = df.drop("column_0")
                        start_from = 1
                    else:
                        start_from = 0

                    _map_headers, _map_headers_no_toll = map_headers(year, start_from)
                    
                    if i < 16:
                        if len(df.columns) < len(_map_headers):
                            df = df.rename(_map_headers_no_toll)
                            columns = df.columns
                            df = df.with_columns(pl.lit("").alias("tollbooth_id"))
                            empty_headers = ["tollbooth_id"]
                            if year < 2024:
                                df = df.with_columns(pl.lit("").alias("via_principal"))
                                empty_headers = ["via_principal"] + empty_headers
                                
                            df = df.select(empty_headers + columns)
                        else:
                            df = df.rename(_map_headers)
                        print(df)
                        if df.shape[1] > 18:
                            dfs_remaind[str(df.shape)+str(i)].append(df)
                        else:
                            df = cast(df)
                            dfs.append(df)
                    else:
                        if df.height > 0:
                            dfs_remaind[str(df.shape)+str(i)].append(df)
                
            _log.info(f"proccessed page {i}")

        for k, v in dfs_remaind.items():
            csv_path = f"tarifas_{year}_{k}.csv"
            pl.concat(v).write_csv(csv_path)
            
        df_all = pl.concat(dfs)
        df_all = df_all.with_columns(pl.lit("pase").alias("toll_ref"))

        # Save to CSV
        csv_path = f"tarifas_{year}.csv"
        df_all.write_csv(csv_path)


def map_headers(year, start_from=0):
    if year >= 2024:
        _map_headers = {f"column_{i}": name for i, name in enumerate(_headers, start_from)}
        _map_headers_no_toll = {f"column_{i}": name for i, name in enumerate(_headers[1:], start_from)}
    else:
        _map_headers = {f"column_{i}": name for i, name in enumerate(_old_headers, start_from)}
        _map_headers_no_toll = {f"column_{i}": name for i, name in enumerate(_old_headers[2:], start_from)}

    return _map_headers, _map_headers_no_toll


def add_table_to_dict(table, tables_dict, key):
    if len(table) > 0:
        tables_dict[key].append(table)
    

def find_toll(table):
    for i, row in enumerate(table):
        if isinstance(row[0], str) and len(row[0]) > 100:
            continue
        for item in row:
            if isinstance(item, str) and item.startswith("$"):
                return i


def return_tables_index(tables):
    indexes = defaultdict(list)
    for i, table in enumerate(tables):
        for j, row in enumerate(table):
            if all([e is None or e == "" for e in row]):
                indexes[i].append(j)
    index_range = {}
    for k, v in indexes.items():
        if len(v) == 1:
            index_range[k] = [(v[0], v[0])]
        else:
            index_range[k] = list(zip(v, v[1:]))
    return index_range


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", help="filename year", required=True, type=int)
    args = parser.parse_args()
    
    main(args.year)
