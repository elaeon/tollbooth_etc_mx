import argparse
import ast
import os
import sys

import polars as pl

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from tb_map_editor.data_files import DataModel, DataStage


REPORTS_PY = os.path.join(os.path.dirname(__file__), "reports.py")
OPERATORS_CSV = "data/tables/area_operators_mx.csv"

NON_REPORT_ARGS = {"--from-year", "--to-year", "--to-year-sts"}


def _fmt(value) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def _scan(path: str):
    if not os.path.exists(path):
        return None
    return pl.scan_parquet(path)


def _rows(path: str):
    lf = _scan(path)
    if lf is None:
        return None
    return lf.select(pl.len()).collect().item()


def count_states(dm: DataModel):
    lf = _scan(dm.tollbooths.parquet)
    if lf is None:
        return None
    return lf.select(pl.col("state").drop_nulls().n_unique()).collect().item()


def count_tdpa_segmentos(dm: DataModel):
    lf = _scan(dm.tb_sts_stretch_id.parquet)
    if lf is None:
        return None
    rows = lf.select(pl.col("stretch_id").n_unique()).collect().item()
    unique = lf.select(pl.col("tollbooth_sts_id").drop_nulls().n_unique()).collect().item()
    return f"{unique:,} / {rows:,}"


def count_linked_plazas(dm: DataModel):
    lf = _scan(dm.tb_stretch_id.parquet)
    if lf is None:
        return None
    return lf.select(pl.col("tollbooth_id_out").drop_nulls().n_unique()).collect().item()


def count_operators():
    if not os.path.exists(OPERATORS_CSV):
        return None
    return pl.scan_csv(OPERATORS_CSV, separator="|").select(pl.len()).collect().item()


def count_reports() -> int:
    """Count CLI report flags in scripts/reports.py by parsing add_argument calls."""
    with open(REPORTS_PY) as fh:
        tree = ast.parse(fh.read())

    total = 0
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and getattr(node.func, "attr", None) == "add_argument"):
            continue
        if not node.args or not isinstance(node.args[0], ast.Constant):
            continue
        flag = node.args[0].value
        if not isinstance(flag, str) or not flag.startswith("--"):
            continue
        if flag in NON_REPORT_ARGS:
            continue
        total += 1
    return total


def build_table(year: int) -> str:
    dm = DataModel(year, DataStage.stg)
    dm_p = DataModel(year, DataStage.prd)

    rows = [
        ("Estados", count_states(dm)),
        ("Carreteras de cuota", _rows(dm.roads.parquet)),
        ("Estaciones de conteo TDPA", _rows(dm_p.tb_sts.parquet)),
        ("Operadoras de plazas de cobro", count_operators()),
        ("Plazas de cobro", _rows(dm.tollbooths.parquet)),
        ("Reportes de analisis de datos", count_reports()),
        ("Segmentos con TDPA", count_tdpa_segmentos(dm)),
        ("Segmentos vinculadas a plazas", count_linked_plazas(dm)),
        ("Segmentos con distancias", _rows(dm.osm_tb_distance.parquet)),
        ("Segmentos de cuota", _rows(dm.stretchs.parquet)),
        ("Segmentos de cuota con tarifa", _rows(dm.stretchs_toll.parquet)),
    ]

    lines = [
        f"## Totales del dataset ({year})",
        "",
        "| Indicador | Registros |",
        "|---|---:|",
    ]
    for label, value in rows:
        lines.append(f"| {label} | {_fmt(value)} |")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Genera la tabla de totales del dataset en markdown.")
    parser.add_argument("--year", required=True, type=int, help="Año a examinar (ej: 2025)")
    parser.add_argument("--output", required=False, type=str, help="Ruta de archivo de salida. Si se omite, imprime a stdout.")
    args = parser.parse_args()

    table = build_table(args.year)
    if args.output:
        with open(args.output, "w") as fh:
            fh.write(table)
        print(f"Saved result in {args.output}")
    else:
        print(table)


if __name__ == "__main__":
    main()
