"""
Parsea el output de playwright-cli eval con tablas de tarifas y genera CSV.

Uso:
  playwright-cli eval "..." > /tmp/tarifas_raw.json
  python3 scripts/parse_tarifas.py /tmp/tarifas_raw.json <url> <output.csv>
"""

import csv
import json
import re
import sys
from datetime import date
from pathlib import Path


def clean_title(raw: str) -> str:
    """Extrae el nombre de la autopista del texto largo del acordeón."""
    # Busca "para la/el <Nombre>" o "en el/la <Nombre>"
    m = re.search(r"(?:para|en) (?:la|el|los|las)\s+(.+?)(?:\s{2,}|\n|$)", raw, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # Si no, toma la primera línea no vacía
    for line in raw.splitlines():
        line = line.strip()
        if line:
            return line
    return raw.strip()


def parse_eval_output(raw_text: str) -> list[dict]:
    """Extrae el JSON del output de playwright-cli eval."""
    # El output tiene formato: ### Result\n"<json_string>"\n### Ran...
    # La línea con el resultado es un string JSON válido (comillas incluidas),
    # hay que parsearlo con json.loads para obtener el string interno, luego
    # volver a parsearlo para obtener la lista de tablas.
    m = re.search(r"### Result\n(.+?)\n###", raw_text, re.DOTALL)
    if not m:
        json_outer = raw_text.strip()
    else:
        json_outer = m.group(1).strip()
    # json_outer es un string JSON: "[{...}]" (con comillas externas)
    inner = json.loads(json_outer)  # devuelve el string interno
    return json.loads(inner)        # devuelve la lista de tablas


def tables_to_csv(tables: list[dict], url: str, output_path: str) -> int:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    today = date.today().isoformat()
    rows_out = []

    HEADER_SKIP = {"CLASIFICACIÓN", "CLASIFICACION", "TIPO", "CATEGORÍA", "CATEGORIA",
                   "TIPO DE VEHÍCULO", "TIPO DE VEHICULO", "DESCRIPCION", "DESCRIPCIÓN"}

    def is_matrix(rows):
        """Tabla multi-caseta: sin 'tarifa' en header, 4+ cols, cols 2+ con valores numéricos."""
        if len(rows) < 2 or len(rows[0]) < 4:
            return False
        if any("tarifa" in h.lower() for h in rows[0]):
            return False
        return sum(1 for v in rows[1][2:] if re.search(r"\d", v)) >= 2

    for table in tables:
        autopista = clean_title(table["title"])
        rows = table["rows"]

        if is_matrix(rows):
            header = rows[0]
            # Detect orientation: rows=tramos when col1 looks like a vehicle type
            # OR when col0 header contains tramo/autopista keywords
            vehicle_kw = {"moto", "auto", "pickup", "camion", "autobus", "bus"}
            tramo_kw = {"tramo", "autopista", "caseta", "plaza", "ruta"}
            rows_are_tramos = (
                (len(header) > 1 and any(kw in header[1].lower() for kw in vehicle_kw))
                or any(kw in header[0].lower() for kw in tramo_kw)
            )
            for row in rows[1:]:
                if rows_are_tramos:
                    # rows=tramos, cols=vehicle_types
                    tramo = row[0].strip()
                    for col_i in range(1, len(header)):
                        if col_i >= len(row):
                            continue
                        tarifa_raw = row[col_i].strip()
                        if not tarifa_raw or tarifa_raw in ("-", "NA"):
                            continue
                        tarifa = re.sub(r"[^\d.]", "", tarifa_raw)
                        if not tarifa:
                            continue
                        rows_out.append({
                            "url_fuente": url,
                            "autopista": f"{autopista} - {tramo}",
                            "clasificacion": header[col_i],
                            "tarifa_mxn": tarifa,
                            "fecha_extraccion": today,
                        })
                else:
                    # rows=vehicle_types, cols=casetas (REANL-style)
                    tipo = row[0].strip()
                    ejes = row[1].strip() if len(row) > 1 else ""
                    clasificacion = f"{tipo} ({ejes} ejes)" if ejes and ejes not in ("-", "") else tipo
                    for col_i in range(2, len(header)):
                        if col_i >= len(row):
                            continue
                        tarifa_raw = row[col_i].strip()
                        if not tarifa_raw or tarifa_raw in ("-", "NA"):
                            continue
                        tarifa = re.sub(r"[^\d.]", "", tarifa_raw)
                        if not tarifa:
                            continue
                        rows_out.append({
                            "url_fuente": url,
                            "autopista": f"{autopista} - {header[col_i]}",
                            "clasificacion": clasificacion,
                            "tarifa_mxn": tarifa,
                            "fecha_extraccion": today,
                        })
            continue

        # Detect column indices from header row
        desc_col, tarifa_col = 0, 1
        if rows:
            header = rows[0]
            for i, h in enumerate(header):
                if "tarifa" in h.lower():
                    tarifa_col = i
                    desc_col = next(
                        (j for j in range(i - 1, -1, -1)
                         if "desc" in header[j].lower()),
                        i - 1
                    )
                    break
        for row in rows:
            if len(row) <= tarifa_col:
                continue
            clasificacion = row[desc_col].strip()
            tarifa_raw = row[tarifa_col].strip()
            if not clasificacion and not tarifa_raw:
                continue
            if clasificacion.upper() in HEADER_SKIP:
                continue
            tarifa = re.sub(r"[^\d.]", "", tarifa_raw)
            if not tarifa:
                continue
            rows_out.append({
                "url_fuente": url,
                "autopista": autopista,
                "clasificacion": clasificacion,
                "tarifa_mxn": tarifa,
                "fecha_extraccion": today,
            })

    fieldnames = ["url_fuente", "autopista", "clasificacion", "tarifa_mxn", "fecha_extraccion"]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_out)

    return len(rows_out)


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Uso: parse_tarifas.py <raw_json_file> <url> <output.csv>")
        sys.exit(1)

    raw_file, url, output_path = sys.argv[1], sys.argv[2], sys.argv[3]
    raw_text = Path(raw_file).read_text(encoding="utf-8")
    tables = parse_eval_output(raw_text)
    count = tables_to_csv(tables, url, output_path)
    print(f"Guardado {count} filas en {output_path}")
