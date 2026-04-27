"""Transforma data/tarifas/tarifas_*.csv (formato largo) a un único CSV columnar."""
from __future__ import annotations

import argparse
import csv
import re
import sys
import unicodedata
from pathlib import Path

INPUT_DIR = Path("data/tarifas")
OUTPUT_FILE = Path("data/tarifas_columnar.csv")

OUTPUT_COLS = [
    "filename", "autopista", "tramo",
    "motorbike", "car", "car_axle",
    "bus_2_axle", "bus_3_axle", "bus_4_axle",
    "truck_2_axle", "truck_3_axle", "truck_4_axle", "truck_5_axle",
    "truck_6_axle", "truck_7_axle", "truck_8_axle", "truck_9_axle", "truck_10_axle",
    "load_axle", "motorbike_axle",
    "car_rush_hour", "car_evening_hour",
    "pedestrian", "residente",
]


def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def normalize(s: str) -> str:
    return strip_accents(s).lower().strip()


def split_tramo_vehiculo(clasificacion: str) -> tuple[str, str]:
    """Devuelve (tramo, vehiculo) a partir del campo clasificacion."""
    c = clasificacion.strip()
    if "|" in c:
        idx = c.rfind("|")
        left, right = c[:idx].strip(), c[idx + 1:].strip()
        # Algunos operadores (e.g. AUNORTE) invierten el orden: '<dirección+horario> | <OD pair>'.
        # Si el lado derecho es un par Origen→Destino, lo tratamos como tramo.
        if "→" in right or "->" in right:
            return right, left
        return left, right
    if "→" in c or "->" in c:
        # Par OD sin tipo de vehículo: el string es el tramo, asumimos auto.
        return c, "__OD_CAR__"
    return "", c


def extract_axles(s: str) -> list[int]:
    """Extrae números de eje de una etiqueta. Normalizado (lowercase, sin acentos)."""
    # rangos: '2-4', '5-6', '7-9', '2 a 4', '2/4'
    m = re.search(r"(\d+)\s*[-–/a]\s*(\d+)", s)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        if 1 <= a <= b <= 11:
            return list(range(a, b + 1))

    # listas explícitas: '2, 3 y 4', '7,8,9', '2 y 3'
    nums = [int(n) for n in re.findall(r"\d+", s)]
    nums = [n for n in nums if 1 <= n <= 11]
    if nums:
        return sorted(set(nums))
    return []


def classify(vehiculo_raw: str, context: str) -> list[str]:
    """Devuelve la lista de columnas de salida a las que mapea esta etiqueta.

    Lista vacía → no mapeable.
    """
    s = normalize(vehiculo_raw)
    if not s:
        return []

    # OD pair sin vehículo → asumir car
    if s == normalize("__OD_CAR__"):
        return ["car"]

    # 1. Residente (antes de car/moto)
    if re.search(r"residente", s):
        return ["residente"]

    # 2. Peatón
    if re.search(r"\bpeat", s):
        return ["pedestrian"]

    # 3. Eje excedente / adicional / sencillo / extra (antes de bus/truck/car)
    is_eje_exc = bool(
        re.search(r"\bejes?\b.*\b(exc|excedente|adicional|sencillo|extra|ligero|pesado|doble|carga)\b", s)
        or re.search(r"^\s*e\.?\s*e\.?\s*[ac]\b", s)
        or re.search(r"^\s*ee[ac]?\b", s)
        or re.search(r"\bligero\s*\+\s*\d", s)
        or re.search(r"\bauto\s*\+\s*\d", s)
        or re.search(r"\bauto\s+\d+\s*eje", s)
        or re.search(r"\bligero\s+\d+\s*eje", s)
        or re.search(r"^\s*excedente\b", s)
    )
    if is_eje_exc:
        if re.search(r"\bmoto", s):
            return ["motorbike_axle"]
        # EEC / E.E.C / etiquetas de carga
        if re.search(r"\beec\b|^\s*e\.?\s*e\.?\s*c\b|\bcamion(?!et)|\bcarga\b|\bpesado\b|\bc\b", s):
            return ["load_axle"]
        if s == "eje excedente" and context in ["https://autopistasdecuota.com/ruta/chihuahua", "https://autopistasdecuota.com/ruta/durango-coahuila", "https://autopistasdecuota.com/ruta/san-luis-potosi"]:
            return ["load_axle"]
        # default: eje ligero/auto/sencillo/excedente sin contexto
        return ["car_axle"]

    # 4. Hora pico / nocturna / regular (común en autopistas urbanas con tarifa por horario)
    if re.search(r"horario\s*pico|hora\s*pico|rush", s):
        return ["car_rush_hour"]
    if re.search(r"nocturn|evening|horario\s*valle|hora\s*valle", s):
        return ["car_evening_hour"]
    if re.search(r"horario\s*regular", s):
        return ["car"]

    # 5. Autobús (B2, B3, B4, "Autobús N ejes", "Bus N ejes", "NB - Autobús de N ejes")
    is_bus = bool(
        re.search(r"autobus|\bbus\b", s)
        or re.search(r"^\s*b\d", s)
        or re.search(r"^\s*\d+\s*b\b", s)
    )
    if is_bus:
        axles = extract_axles(s)
        if not axles:
            axles = [2, 3, 4]  # default genérico para "Autobús" sin más info
        cols = [f"bus_{n}_axle" for n in axles if 2 <= n <= 4]
        return cols

    # 6. Camión / carga / pesado / trailer (todos los rangos)
    is_truck = bool(
        re.search(r"\bcamion(?!et)|\bcarga\b|\bpesado\b|\btrailer\b", s)
        or re.search(r"^\s*c\d", s)
        or re.search(r"^\s*\d+\s*e\b", s)
        or (re.search(r"^\d a \d", s) and context in ["https://casmexico.com/mapasytarifas"])
    )
    if is_truck:
        if re.search(r"camion$", s) and (context in ["https://autopistasdecuota.com/ruta/chihuahua", "https://autopistasdecuota.com/ruta/durango-coahuila", "https://autopistasdecuota.com/ruta/san-luis-potosi"]):
            s = "camion 2 ejes"
        axles = extract_axles(s)
        cols = [f"truck_{n}_axle" for n in axles if 2 <= n <= 10]
        return cols

    # 7. Auto / Pick-Up / Ligero / Camioneta
    if re.search(r"\bautomovil(es)?\b|\bauto\b|pick[\s\-]*up|\bligero\b|\bcamioneta(s)?\b|\bautos\b", s):
        return ["car"]
    if re.search(r"^\s*a\b", s):
        return ["car"]

    # 8. Moto
    if re.search(r"motocicleta|\bmoto\b|\bmotos\b", s):
        return ["motorbike"]
    if re.search(r"^\s*m\b", s):
        return ["motorbike"]

    return []


def to_number(raw: str) -> str:
    """Devuelve la tarifa como string limpio (mantiene decimales si los hay)."""
    s = raw.strip()
    if not s:
        return ""
    try:
        f = float(s)
    except ValueError:
        return s
    return f"{f:g}"


def process_files(input_dir: Path, warn_unmappable: bool = True, warn_duplicate: bool = True, only_file: str | None = None) -> tuple[dict, int, int]:
    """Lee CSVs y agrega en un dict (autopista, tramo) -> {col: tarifa}.

    Devuelve (sink, total_rows, skipped_rows).
    """
    sink: dict[tuple[str, str, str], dict[str, str]] = {}
    total = 0
    skipped = 0

    if only_file:
        files = [input_dir / only_file] if (input_dir / only_file).exists() else []
    else:
        files = sorted(input_dir.glob("tarifas_*.csv"))
    if not files:
        print(f"warn: no se encontraron archivos en {input_dir}", file=sys.stderr)
        return sink, total, skipped

    for path in files:
        with path.open(encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                total += 1
                autopista = (row.get("autopista") or "").strip()
                clasif = row.get("clasificacion") or ""
                tarifa = to_number(row.get("tarifa_mxn") or "")
                if not autopista or not clasif or not tarifa:
                    skipped += 1
                    continue
                if row.get("url_fuente") in ["https://www.mronoreste.mx/"]:
                    clasif = re.sub(r"Eje Excedente \|", "| Eje Excedente", clasif)
                tramo, vehiculo = split_tramo_vehiculo(clasif)
                cols = classify(vehiculo, context=(row.get("url_fuente") or "").strip())
                if not cols:
                    if warn_unmappable:
                        print(
                            f"warn: clasificación no mapeable en {path.name}: "
                            f"autopista={autopista!r} clasificacion={clasif!r}",
                            file=sys.stderr,
                        )
                    skipped += 1
                    continue
                key = (path.name, autopista, tramo)
                bucket = sink.setdefault(key, {})
                for col in cols:
                    prev = bucket.get(col)
                    if prev is not None and prev != tarifa:
                        if warn_duplicate:
                            print(
                                f"warn: valor duplicado en {path.name} para "
                                f"{key} columna={col}: {prev} -> {tarifa}",
                                file=sys.stderr,
                            )
                    else:
                        bucket[col] = tarifa
    return sink, total, skipped


def write_output(sink: dict, output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(OUTPUT_COLS)
        for (filename, autopista, tramo), bucket in sorted(sink.items()):
            key = [filename, autopista, tramo]
            row = key + [bucket.get(c, "") for c in OUTPUT_COLS[len(key):]]
            writer.writerow(row)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-warn-unmappable", dest="warn_unmappable", action="store_false",
                        help="Silencia warnings de clasificación no mapeable")
    parser.add_argument("--no-warn-duplicate", dest="warn_duplicate", action="store_false",
                        help="Silencia warnings de valor duplicado")
    parser.add_argument("--file", metavar="NOMBRE",
                        help="Procesa solo este archivo (ej: tarifas_viaducto.csv)")
    args = parser.parse_args()

    sink, total, skipped = process_files(INPUT_DIR, args.warn_unmappable, args.warn_duplicate, args.file)
    write_output(sink, OUTPUT_FILE)
    print(
        f"escritas {len(sink)} filas en {OUTPUT_FILE} "
        f"(input: {total} filas, omitidas: {skipped})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
