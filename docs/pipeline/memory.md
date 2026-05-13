# Pipeline Visualization — Memory & Insights

## What was built

A fully self-contained pipeline DAG visualization for `tollbooth_etc_mx`.

| File | Purpose |
|---|---|
| `generate.py` | Imports `src/model.py` to extract live schemas, then writes both output files |
| `pipeline.json` | Graph data (nodes + edges). Can be parsed independently |
| `pipeline.html` | Self-contained visualization — JSON is embedded inline, no server needed |

Run to regenerate both:
```sh
uv run python docs/pipeline/generate.py
```

---

## Node types and colors (light theme)

| Type | Color | Meaning |
|---|---|---|
| `source` | blue `#3b82f6` | Raw input files — `data/pub/{year}/` CSVs |
| `task` | amber `#f59e0b` | Staging pipeline tasks (Prefect tasks) |
| `staging` | emerald `#10b981` | Staging parquet outputs — `data/staging/{year}/` |
| `report_task` | red `#dc2626` | Report generation functions in `reports.py` |
| `report` | dark red `#7f1d1d` | Final CSV outputs in `reports/` |

User preference: **all report-related nodes should be red** (tasks and outputs).  
User preference: **light background palette** (changed from dark to light theme).

---

## Graph topology

```
source → task → staging → (later staging task) → staging
                staging → report_task → report output
```

The staging sub-DAG has 6 execution groups (from `staging_flow.py`):

| Group | Tasks |
|---|---|
| 0 | `dv_cleaner` |
| 1 | `pub_osm`, `raw_inflation` |
| 2 | all pub/raw loaders (parallel) |
| 3 | `neighbours` (waits for all above) |
| 4 | `map_tb_id`, `tb_sts` (parallel) |
| 5 | `imt_stretch_id`, `sts_stretch_id` (parallel) |

The report DAG has 2 groups (from `report_flow.py`):
- Group 0 (parallel): `growth_rate`, `toll_update_date`, `revenue`
- Group 1 (depends on growth_rate CSVs): `manage_data`

The remaining reports in `reports.py` are **standalone** (not wired into the Prefect report flow): `tb_names`, `stretch_names`, `tb_stretch_rel`, `tb_wo_stretch`, `toll_ref`, `tollbooth_stretch_manage`, `stretch_length`, `road_manage`, `stretch_sts`, `state_report`.

---

## Data model mapping

Schemas come from `src/model.py` via `cls.dict_schema()`. Each `DataModel` descriptor maps a name to a model class and a filename:

```python
# DataStage.pub  → data/pub/{year}/
# DataStage.stg  → data/staging/{year}/
DataModel(year, stage).tollbooth  # PathModel with .csv / .parquet / .schema
```

`LAST_YEAR = 2026` is hard-coded in `generate.py` for path display. Update it when a new year's data is added.

---

## Known simplifications / caveats

- **Cross-year staging reads**: `tollbooth_neighbours`, `tb_sts`, `tb_names`, `stretch_names`, `stretch_sts` read `tb_sts` from `year - 1`. The graph shows `stg_tb_sts` as a single node — no separate "prev year" node. This is intentional to keep the graph readable.
- **`toll_ref` pub read**: `toll_ref()` opens `data_model_pub.tb_stretch_id.parquet` (a parquet under `data/pub/`). The graph maps this to `src_tb_stretch_id` (the CSV source) for simplicity.
- **`mx_projects_report`** is not in the graph — it reads raw CSV files outside the standard data model and is not wired to a CLI flag in `run.py`.

---

## How to add a new report

1. In `generate.py`, add a `report_task` node in the NODES list:
   ```python
   dict(id="report_foo", label="foo", type="report_task",
        path="src/scripts/reports.py :: foo_report",
        model=None, model_path=None, schema={}),
   ```
2. Add a `report` output node:
   ```python
   dict(id="out_foo", label="foo_{year}.csv", type="report",
        path="reports/foo_{year}.csv", model=None, model_path=None, schema={}),
   ```
3. Add edges from staging nodes → `report_foo` → `out_foo` in EDGES.
4. Run `uv run python docs/pipeline/generate.py`.

## How to add a new staging task

Same pattern: add `source` → `task` → `staging` nodes and their edges. If the model is new, add it to `src/model.py` first — `generate.py` will pick up the schema automatically via `_schema(model.NewModel)`.

---

## Tech stack

- **Cytoscape.js** `3.29.2` — graph rendering
- **dagre** `0.8.5` + **cytoscape-dagre** `2.5.0` — top-to-bottom DAG layout
- All loaded from CDN, no build step needed.

Layout config: `rankDir: TB`, `nodeSep: 40`, `rankSep: 60`.

Clicking a node highlights its connected edges and shows path, model class, and schema in the right panel. Clicking the background clears the selection.
