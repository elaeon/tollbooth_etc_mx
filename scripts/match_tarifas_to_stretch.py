"""Asigna stretch_id a cada fila de data/tarifas_columnar.csv mediante matching difuso
contra data/tables/2026/{roads.csv,stretchs.csv}."""
from __future__ import annotations

import csv
import re
import sys
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path

ROADS_FILE = Path("data/tables/2026/roads.csv")
STRETCH_FILE = Path("data/tables/2026/stretchs.csv")
TARIFAS_FILE = Path("data/tarifas_columnar.csv")
OUTPUT_FILE = Path("data/tarifas_with_stretch_id.csv")
THRESHOLD = 0.50

_PREFIX_NOISE = re.compile(r"^(?:autopista|plaza\s+de\s+cobro:?)\s+", re.IGNORECASE)
_DASH_VARIANTS = re.compile(r"[‐-―]")  # –, —, ‐, etc.
_NON_WORD = re.compile(r"[^\w\s-]+")
_WS = re.compile(r"\s+")
_TOKEN = re.compile(r"[a-z0-9]+")


def normalize(s: str) -> str:
    s = s or ""
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s = s.lower()
    s = _DASH_VARIANTS.sub("-", s)
    s = _PREFIX_NOISE.sub("", s).strip()
    s = _NON_WORD.sub(" ", s)
    s = _WS.sub(" ", s).strip()
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
    matcher = SequenceMatcher(autojunk=False)
    matcher.set_seq2(search)
    for t in targets:
        if not (search_tokens & t["target_tokens"]):
            continue
        matcher.set_seq1(t["target_text"])
        score = matcher.ratio()
        if score > best_score:
            best_score = score
            best = t
    return best, best_score


def main() -> int:
    targets = load_targets()
    print(f"loaded {len(targets)} stretches")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with TARIFAS_FILE.open(encoding="utf-8", newline="") as fin, \
         OUTPUT_FILE.open("w", encoding="utf-8", newline="") as fout:
        reader = csv.DictReader(fin)
        out_cols = list(reader.fieldnames or []) + ["stretch_id", "road_id", "match_score"]
        writer = csv.DictWriter(fout, fieldnames=out_cols)
        writer.writeheader()
        total = 0
        below = 0
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
                writer.writerow({
                    **row,
                    "stretch_id": "",
                    "road_id": "",
                    "match_score": f"{score:.3f}",
                })
            else:
                writer.writerow({
                    **row,
                    "stretch_id": best["stretch_id"],
                    "road_id": best["road_id"],
                    "match_score": f"{score:.3f}",
                })
    print(f"escritas {total} filas en {OUTPUT_FILE} ({below} bajo umbral {THRESHOLD})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
