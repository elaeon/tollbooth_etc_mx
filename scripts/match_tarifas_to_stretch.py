"""Asigna stretch_id a cada fila de data/tarifas_columnar.csv mediante matching difuso
contra data/tables/2026/{roads.csv,stretchs.csv}."""
from __future__ import annotations

import csv
import re
import sys
import unicodedata
from pathlib import Path

from rapidfuzz import fuzz

ROADS_FILE = Path("data/tables/2026/roads.csv")
STRETCH_FILE = Path("data/tables/2026/stretchs.csv")
TARIFAS_FILE = Path("data/tarifas_columnar.csv")
OUTPUT_FILE = Path("data/tarifas_with_stretch_id.csv")
THRESHOLD = 0.5

_PREFIX_NOISE = re.compile(r"^(?:autopista|plaza\s+de\s+cobro|caseta:?)\s+", re.IGNORECASE)
_DASH_VARIANTS = re.compile(r"[‐-―]")  # –, —, ‐, etc.
_NON_WORD = re.compile(r"[^\w\s-]+")
_WS = re.compile(r"\s+")
_TOKEN = re.compile(r"[a-z0-9]+")
_SUFIX_NOISE = re.compile(r"tarifas\s+con\s+iva")
_COL_ORDER = [
    "stretch_id", "motorbike", "car", "car_axle",
    "bus_2_axle", "bus_3_axle", "bus_4_axle", "truck_2_axle",
    "truck_3_axle", "truck_4_axle", "truck_5_axle", "truck_6_axle",
    "truck_7_axle", "truck_8_axle", "truck_9_axle", "truck_10_axle",
    "load_axle", "motorbike_axle", "car_rush_hour", "car_evening_hour",
    "pedestrian", "residente", 
    "autopista", "tramo", "stretch_name", "road_name", "match_score", "filename"
]

def normalize(s: str) -> str:
    s = s or ""
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s = s.lower()
    s = _DASH_VARIANTS.sub("-", s)
    s = _PREFIX_NOISE.sub("", s).strip()
    s = _NON_WORD.sub(" ", s)
    s = _WS.sub(" ", s).strip()
    s = _SUFIX_NOISE.sub("", s)
    return s


def tokenize(s: str) -> set[str]:
    return set(_TOKEN.findall(s))


def load_targets() -> list[dict]:
    with ROADS_FILE.open(encoding="utf-8") as f:
        roads = {r["road_id"]: r["road_name"] for r in csv.DictReader(f)}
    targets: list[dict] = []
    with STRETCH_FILE.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            road_id = r["road_id"]
            road_name = roads.get(road_id, "")
            stretch_name = r["stretch_name"]
            target_text = normalize(f"{road_name} {stretch_name}")
            targets.append({
                "stretch_id": r["stretch_id"],
                "road_id": road_id,
                "road_name": road_name,
                "stretch_name": stretch_name,
                "target_text": target_text,
                "target_tokens": tokenize(target_text),
            })
    return targets


def best_match(search: str, search_tokens: set[str], targets: list[dict]) -> tuple[dict | None, float]:
    if not search:
        return None, 0.0
    best: dict | None = None
    best_score = 0.0
    for t in targets:
        if not (search_tokens & t["target_tokens"]):
            continue
        score = fuzz.ratio(search, t["target_text"]) / 100
        if score > best_score:
            best_score = score
            best = t
    return best, best_score


def main() -> int:
    targets = load_targets()
    print(f"loaded {len(targets)} stretches")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with TARIFAS_FILE.open(encoding="utf-8", newline="") as fin:
        reader = csv.DictReader(fin)
        total = 0
        below = 0
        rows_out = []
        for row in reader:
            total += 1
            search = normalize(f"{row.get('autopista','')} {row.get('tramo','')}")
            tokens = tokenize(search)
            best, score = best_match(search, tokens, targets)
            if best is None or score < THRESHOLD:
                below += 1
                print(
                    f"warn: sin match (score={score:.2f}) "
                    f"autopista={row.get('autopista','')!r} tramo={row.get('tramo','')!r}",
                    file=sys.stderr,
                )
                rows_out.append({**row, "stretch_id": None, "road_id": "", "stretch_name": "", "road_name": "", "match_score": f"{score:.3f}"})
            else:
                rows_out.append({**row, "stretch_id": int(best["stretch_id"]), "road_id": best["road_id"], "stretch_name": best.get("stretch_name"), "road_name": best.get("road_name"), "match_score": f"{score:.3f}"})

    rows_out.sort(key=lambda r: (r["stretch_id"] is None, r["stretch_id"] or 0))

    with OUTPUT_FILE.open("w", encoding="utf-8", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=_COL_ORDER, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows_out)
    print(f"escritas {total} filas en {OUTPUT_FILE} ({below} bajo umbral {THRESHOLD})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
