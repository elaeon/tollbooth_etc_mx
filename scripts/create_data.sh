uv run scripts/dv_cleaner.py --year 2025 --from-page 55
uv run scripts/dv_cleaner.py --year 2024 --from-page 55
uv run scripts/dv_cleaner.py --year 2023 --from-page 54
uv run scripts/dv_cleaner.py --year 2022 --from-page 53
uv run scripts/dv_cleaner.py --year 2021 --from-page 51
uv run scripts/dv_cleaner.py --year 2020 --from-page 51
uv run scripts/dv_cleaner.py --year 2019 --from-page 51

uv run scripts/catalogs.py --year 2024 --catalog tb
uv run scripts/catalogs.py --year 2025 --catalog tb
uv run scripts/catalogs.py --year 2025 --catalog road
uv run scripts/catalogs.py --year 2025 --catalog stretch
uv run scripts/catalogs.py --year 2021 --catalog stretch_toll
uv run scripts/catalogs.py --year 2022 --catalog stretch_toll
uv run scripts/catalogs.py --year 2023 --catalog stretch_toll
uv run scripts/catalogs.py --year 2024 --catalog stretch_toll
uv run scripts/catalogs.py --year 2025 --catalog stretch_toll

uv run scripts/imt_to_model.py --year 2024 --plazas
uv run scripts/imt_to_model.py --year 2025 --plazas
uv run scripts/imt_to_model.py --year 2020 --tarifas
uv run scripts/imt_to_model.py --year 2021 --tarifas
uv run scripts/imt_to_model.py --year 2022 --tarifas
uv run scripts/imt_to_model.py --year 2023 --tarifas
uv run scripts/imt_to_model.py --year 2024 --tarifas
uv run scripts/imt_to_model.py --year 2025 --tarifas

uv run scripts/join_tollbooths.py --year 2024 --tb-imt-tb-id 2024
uv run scripts/join_tollbooths.py --year 2025 --tb-imt-tb-id 2025

uv run scripts/join_tollbooths.py --year 2024 --tb-stretch-id-imt 2020
uv run scripts/join_tollbooths.py --year 2020 --tb-stretch-id-imt-delta 2021 --pivot-year 2024
uv run scripts/join_tollbooths.py --year 2021 --tb-stretch-id-imt-delta 2022 --pivot-year 2024
uv run scripts/join_tollbooths.py --year 2022 --tb-stretch-id-imt-delta 2023 --pivot-year 2024
uv run scripts/join_tollbooths.py --year 2023 --tb-stretch-id-imt-delta 2024 --pivot-year 2024
uv run scripts/join_tollbooths.py --year 2024 --tb-stretch-id-imt-delta 2025 --pivot-year 2025

uv run scripts/join_tollbooths.py --year 2025 --tb-stretch-id-patch
