"""Asigna stretch_id a cada fila de un CSV de tarifas mediante matching difuso
contra reports/toll_ref_{year}.csv."""
from __future__ import annotations

import argparse
import csv
import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Callable

from rapidfuzz import fuzz

DEFAULT_INPUT = Path("data/tarifas_columnar.csv")
DEFAULT_OUTPUT = Path("data/tarifas_with_stretch_id.csv")
THRESHOLD = 0.65

# Abreviaturas comunes en stretch_name y tramo
# Equivalencias semánticas: texto de tarifas → nombre canónico en toll_ref
# (aplicadas después de normalize(), antes de expand_abbrevs())
# Ordenadas de más larga a más corta para evitar reemplazos parciales.
TERM_ALIASES: dict[str, str] = {
    "tramo 0 norte sur": "ent aut urbana nte",
    "tramo 0 sur norte": "ent aut urbana nte",
    "ent mexico puebla": "san martin texmelucan",
    "ent mex puebla": "san martin texmelucan",
    "tramo 0": "ent aut urbana nte",
    "viaducto mineria observatorio": "mineria",
    "peri centro": "",
}

ABBREVS = {
    "ent": "entronque",
    "lib": "libramiento",
    "libr": "libramiento",
    "libto": "libramiento",
    "blvd": "boulevard",
    "cd": "ciudad",
    "mty": "monterrey",
    "gdl": "guadalajara",
    "qro": "queretaro",
    "slp": "san luis potosi",
    "hgo": "hidalgo",
    "nvo": "nuevo",
    "gral": "general",
    "nte": "norte",
    "ote": "oriente",
    "pte": "poniente",
    "veb": "entrada viaducto bicentenario",
    "vb": "viaducto bicentenario",
}

_PREFIX_NOISE = re.compile(
    r"^(?:autopista|plaza\s+de\s+cobro|caseta:?|cto\s+ext\s+mexiquense|circuito\s+exterior\s+mexiquense|amozoc\s+perote)\s+",
    re.IGNORECASE,
)
_DASH_VARIANTS = re.compile(r"[‐-―]")  # –, —, ‐, etc.
_NON_WORD = re.compile(r"[^\w\s]+")
_WS = re.compile(r"\s+")
_TOKEN = re.compile(r"[a-z0-9]+")
_SUFIX_NOISE = re.compile(r"tarifas\s+con\s+iva")
_EXTRA_COLS = ["stretch_id", "stretch_name", "match_score"]


def normalize(s: str) -> str:
    s = s or ""
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s = s.lower()
    s = s.replace("_", " ")
    s = _DASH_VARIANTS.sub(" ", s)
    s = _PREFIX_NOISE.sub("", s).strip()
    s = _PREFIX_NOISE.sub("", s).strip()
    s = _NON_WORD.sub(" ", s)
    s = _WS.sub(" ", s).strip()
    s = _SUFIX_NOISE.sub("", s).strip()
    s = re.sub(r"\b([a-z]) ([0-9][a-z0-9]*)\b", r"\1\2", s)
    for alias, replacement in TERM_ALIASES.items():
        if alias in s:
            s = s.replace(alias, replacement)
    return _WS.sub(" ", s).strip()


def expand_abbrevs(s: str) -> str:
    return " ".join(ABBREVS.get(tok, tok) for tok in s.split())


def tokenize(s: str) -> set[str]:
    return set(_TOKEN.findall(s))


def load_targets(year) -> tuple[list[dict], dict[str, list[dict]]]:
    targets: list[dict] = []
    by_file: dict[str, list[dict]] = defaultdict(list)
    ref_file = Path(f"reports/toll_ref_{year}.csv")
    with ref_file.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            stretch_name = r["stretch_name"]
            tollbooth_name = r.get("tollbooth_name", "").strip()
            toll_ref = r.get("toll_ref", "").strip()
            target_text = expand_abbrevs(normalize(stretch_name))
            target_text_alt = expand_abbrevs(normalize(tollbooth_name)) if tollbooth_name else ""
            target_combined = (target_text + " " + target_text_alt).strip() if target_text_alt else ""
            toll_ref_norm = expand_abbrevs(normalize(toll_ref.replace("tarifas_", "").replace(".csv", ""))) if toll_ref else ""
            t: dict = {
                "stretch_id": r["stretch_id"],
                "stretch_name": stretch_name,
                "target_text": target_text,
                "target_text_alt": target_text_alt,
                "target_combined": target_combined,
                "target_tokens": tokenize(target_text) | tokenize(target_text_alt) | tokenize(toll_ref_norm),
            }
            targets.append(t)
            if toll_ref:
                by_file[toll_ref].append(t)
    return targets, by_file


def _score_pool(
    search: str,
    search_rev: str,
    search_tokens: set[str],
    pool: list[dict],
    scorer: Callable[[str, str], float],
) -> tuple[dict | None, float]:
    best: dict | None = None
    best_score = 0.0
    try_rev = search_rev != search
    for t in pool:
        if not (search_tokens & t["target_tokens"]):
            continue
        s1 = scorer(search, t["target_text"]) / 100
        s2 = fuzz.ratio(search, t["target_text_alt"]) / 100 if t["target_text_alt"] else 0.0
        score = max(s1, s2)
        if try_rev:
            r1 = scorer(search_rev, t["target_text"]) / 100 * 0.98
            r2 = fuzz.ratio(search_rev, t["target_text_alt"]) / 100 * 0.98 if t["target_text_alt"] else 0.0
            score = max(score, r1, r2)
        if t["target_combined"]:
            s_comb = fuzz.token_set_ratio(search, t["target_combined"]) / 100 * 0.95
            score = max(score, s_comb)
        if scorer is fuzz.ratio and score < THRESHOLD:
            s_tsr = fuzz.token_set_ratio(search, t["target_text"]) / 100 * 0.9
            score = max(score, s_tsr)
        if score > best_score:
            best_score = score
            best = t
    return best, best_score


def best_match(
    search: str,
    search_tokens: set[str],
    candidates: list[dict],
    targets_all: list[dict],
    scorer: Callable[[str, str], float],
) -> tuple[dict | None, float]:
    """Busca en candidates (pre-filtrados por filename); si score < THRESHOLD,
    intenta fallback contra todos los targets."""
    if not search:
        return None, 0.0
    search_rev = " ".join(reversed(search.split()))
    best, score = _score_pool(search, search_rev, search_tokens, candidates, scorer)
    if score < THRESHOLD and candidates is not targets_all:
        best_fb, score_fb = _score_pool(search, search_rev, search_tokens, targets_all, scorer)
        if score_fb > score:
            return best_fb, score_fb
    return best, score


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Match tarifas CSV rows to stretch_id.")
    p.add_argument("input", nargs="?", type=Path, default=DEFAULT_INPUT,
                    help="CSV de entrada con columnas autopista, tramo, filename")
    p.add_argument("-o", "--output", type=Path, default=DEFAULT_OUTPUT)
    p.add_argument("-y", "--year", type=int, default=2026)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    input_file: Path = args.input
    output_file: Path = args.output
    year: int = args.year

    targets, by_file = load_targets(year)
    print(f"loaded {len(targets)} stretches ({len(by_file)} grupos por filename)")

    output_file.parent.mkdir(parents=True, exist_ok=True)
    rows_out: list[dict] = []
    input_fieldnames: list[str] = []
    below = 0

    with input_file.open(encoding="utf-8", newline="") as fin:
        reader = csv.DictReader(fin)
        input_fieldnames = list(reader.fieldnames or [])
        for row in reader:
            tramo = row.get("tramo", "").strip()
            autopista = row.get("autopista", "").strip()
            filename = row.get("filename", "")

            if tramo:
                # Con tramo: fuzz.ratio para preservar dirección
                search = expand_abbrevs(normalize(tramo))
                scorer = fuzz.ratio
            else:
                # Sin tramo: token_set_ratio sobre autopista (más flexible)
                search = expand_abbrevs(normalize(autopista))
                scorer = fuzz.token_set_ratio

            tokens = tokenize(search)
            candidates = by_file.get(filename) or targets
            best, score = best_match(search, tokens, candidates, targets, scorer)

            if best is None or score < THRESHOLD:
                below += 1
                print(
                    f"warn: sin match (score={score:.2f}) "
                    f"autopista={autopista!r} tramo={tramo!r}",
                    file=sys.stderr,
                )
                rows_out.append({**row, "stretch_id": None, "stretch_name": "", "match_score": f"{score:.3f}"})
            else:
                rows_out.append({
                    **row,
                    "stretch_id": int(best["stretch_id"]),
                    "stretch_name": best["stretch_name"],
                    "match_score": f"{score:.3f}",
                })

    # Dedup: por stretch_id conservar la fila con mayor score
    seen: dict[str, int] = {}  # stretch_id → índice en rows_out con mejor score
    dedup = 0
    for i, row in enumerate(rows_out):
        sid = str(row["stretch_id"])
        if sid in ("None", ""):
            continue
        if sid not in seen:
            seen[sid] = i
        else:
            prev_i = seen[sid]
            if float(row["match_score"]) > float(rows_out[prev_i]["match_score"]):
                rows_out[prev_i]["stretch_id"] = None
                seen[sid] = i
            else:
                row["stretch_id"] = None
            dedup += 1
            print(f"warn: stretch_id={sid} duplicado, descartando el de menor score", file=sys.stderr)

    total = len(rows_out)
    rows_out.sort(key=lambda r: (r["stretch_id"] is None, r["stretch_id"] or 0))

    col_order = _EXTRA_COLS + [c for c in input_fieldnames if c not in _EXTRA_COLS]
    with output_file.open("w", encoding="utf-8", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=col_order, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows_out)
    print(
        f"escritas {total} filas en {output_file} "
        f"({below} bajo umbral {THRESHOLD}, {dedup} duplicados resueltos)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
