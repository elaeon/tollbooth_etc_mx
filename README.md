## Running the pipeline

After cloning, install dependencies with `uv sync`. 
Download the [raw data](https://zenodo.org/records/19097759) from Zenodo and unpacked inside tollbooth_etc_mx/

The `tb-pipeline` command is then available via `uv run`:

```sh
uv run tb-pipeline --from-year YEAR --to-year YEAR [--flow FLOW] [--from-step STEP] [--tasks TASK ...]
```


**Required arguments**

| Argument | Description |
|---|---|
| `--from-year` | First year to process |
| `--to-year` | Last year to process (inclusive) |

**Optional arguments**

| Argument | Description |
|---|---|
| `--flow staging\|reports\|all` | Run a Prefect flow. `staging` processes per-year data in a parallel DAG; `reports` runs the cross-year report flow; `all` runs both in sequence |
| `--from-step STEP` | Resume a flow from a specific step, skipping everything before it (requires `--flow`) |
| `--tasks TASK [TASK ...]` | Run individual tasks directly without a flow |

**Examples**

```sh
# Full staging + report pipeline for 2024–2026
uv run tb-pipeline --from-year 2024 --to-year 2026 --flow all

# Only the staging flow
uv run tb-pipeline --from-year 2024 --to-year 2026 --flow staging

# Resume staging from the neighbours step
uv run tb-pipeline --from-year 2025 --to-year 2026 --flow staging --from-step neighbours

# Run individual tasks without a flow
uv run tb-pipeline --from-year 2025 --to-year 2025 --tasks pub_tb dv_cleaner
```

---

## Contributions

Welcome! Whether you're improving data quality, reporting issues, or enhancing the analysis, your help makes this project better.

## Dataset summary

| Indicador | Registros |
|---|---:|
| Estados | 32 |
| Carreteras de cuota | 244 |
| Estaciones de conteo TDPA | 860 |
| Operadoras de plazas de cobro | 110 |
| Plazas de cobro | 1,367 |
| Reportes de analisis de datos | 16 |
| Segmentos con TDPA | 856 / 1,461 |
| Segmentos vinculadas a plazas | 1,188 |
| Segmentos con distancias | 799 |
| Segmentos de cuota | 1,463 |
| Segmentos de cuota con tarifa | 1,437 |

### How to Contribute

#### **Data Contributions**
- **Data Corrections**: Report errors in toll booth names, GPS coordinates, or operational details
- **New Data**: Share updated pricing, new routes, or seasonal changes
- **Location Updates**: Correct or update highway names, jurisdiction boundaries, or stretch information
- **Missing Information**: Add data for toll booths with incomplete records

#### **Report & Analysis Contributions**
- **Bug Reports**: Found an issue in the data processing or calculations? [Open an issue](../../issues)
- **Code Improvements**: Submit pull requests to improve data pipelines or add functionality
- **Documentation**: Help improve guides, examples, or technical documentation

#### **Quality Assurance**
- **Data Validation**: Review and verify toll data accuracy

### Getting Started

1. **Fork or Clone** the repository
2. **Identify** what you'd like to improve (data, code, or docs)
3. **Make Changes** locally
4. **Submit** a pull request with a clear description of your changes
5. **Discuss** with maintainers to refine and merge your contribution

### Guidelines

- Keep commit messages clear and descriptive
- Include references to related issues when applicable
- Test changes locally before submitting

### Questions?

Open an [issue](../../issues) to ask questions or suggest improvements.

## Citing

If your publication uses reports or data from this repository, please take a moment to cite it.

You can download the full report on [Zenodo](https://zenodo.org/records/18682704)

---

## Credits

**Original Data Sources:**
See [resources.csv](https://github.com/elaeon/tollbooth_etc_mx/blob/main/resources.csv)


## Data & Report Access

The processed toll booth data and analysis reports are available for download and use in research, journalism, and policy work.

---

**Thank you for your interest in this project! Your contributions help improve toll infrastructure transparency in Mexico.** 🚗✨
