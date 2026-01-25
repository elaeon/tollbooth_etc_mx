#!/bin/bash

# Usage: ./create_data.sh [build|clean]
# - build: Execute all data processing commands sequentially
# - clean: Clean up temporary data (not implemented yet)

set -e  # Exit on error

build() {
    echo "Starting build process..."

    echo "Step 1: Running dv_cleaner..."
    uv run scripts/dv_cleaner.py --year 2025 --from-page 55
    uv run scripts/dv_cleaner.py --year 2024 --from-page 55
    uv run scripts/dv_cleaner.py --year 2023 --from-page 54
    uv run scripts/dv_cleaner.py --year 2022 --from-page 53
    uv run scripts/dv_cleaner.py --year 2021 --from-page 51
    uv run scripts/dv_cleaner.py --year 2020 --from-page 51
    uv run scripts/dv_cleaner.py --year 2019 --from-page 51

    echo "Step 2: Running stage for tb..."
    uv run scripts/stage.py --year 2024 --pub-to-stg tb
    uv run scripts/stage.py --year 2025 --pub-to-stg tb
    uv run scripts/stage.py --year 2025 --pub-to-stg road

    uv run scripts/stage.py --year 2024 --pub-to-stg stretch
    uv run scripts/stage.py --year 2025 --pub-to-stg stretch

    uv run scripts/stage.py --year 2021 --pub-to-stg stretch_toll
    uv run scripts/stage.py --year 2022 --pub-to-stg stretch_toll
    uv run scripts/stage.py --year 2023 --pub-to-stg stretch_toll
    uv run scripts/stage.py --year 2024 --pub-to-stg stretch_toll
    uv run scripts/stage.py --year 2025 --pub-to-stg stretch_toll

    echo "Step 3: Running stage for imt..."
    uv run scripts/stage.py --year 2024 --raw-to-stg tb_imt
    uv run scripts/stage.py --year 2025 --raw-to-stg tb_imt
    uv run scripts/stage.py --year 2020 --raw-to-stg tb_toll_imt
    uv run scripts/stage.py --year 2021 --raw-to-stg tb_toll_imt
    uv run scripts/stage.py --year 2022 --raw-to-stg tb_toll_imt
    uv run scripts/stage.py --year 2023 --raw-to-stg tb_toll_imt
    uv run scripts/stage.py --year 2024 --raw-to-stg tb_toll_imt
    uv run scripts/stage.py --year 2025 --raw-to-stg tb_toll_imt

    echo "Step 4: Running populate_db for tollbooths and stretches..."
    uv run scripts/populate_db.py --year 2025 --new-tb
    uv run scripts/populate_db.py --year 2025 --new-tb-imt
    uv run scripts/populate_db.py --year 2025 --new-road
    uv run scripts/populate_db.py --year 2025 --new-stretch
    uv run scripts/populate_db.py --year 2025 --new-stretch-toll
    uv run scripts/populate_db.py --year 2024 --new-stretch-toll
    uv run scripts/populate_db.py --year 2023 --new-stretch-toll

    echo "Step 5: Running populate_db for STS..."
    uv run scripts/populate_db.py --year 2018 --new-tb-sts
    uv run scripts/populate_db.py --year 2019 --new-tb-sts
    uv run scripts/populate_db.py --year 2020 --new-tb-sts
    uv run scripts/populate_db.py --year 2021 --new-tb-sts
    uv run scripts/populate_db.py --year 2022 --new-tb-sts
    uv run scripts/populate_db.py --year 2023 --new-tb-sts
    uv run scripts/populate_db.py --year 2024 --new-tb-sts

    echo "Step 6: Running join_tollbooths and populate_db for mapping..."
    uv run scripts/join_tollbooths.py --year 2024 --map-tb-imt 2024
    uv run scripts/join_tollbooths.py --year 2025 --map-tb-imt 2025

    uv run scripts/populate_db.py --year 2024 --new-map-tb-imt
    uv run scripts/populate_db.py --year 2025 --new-map-tb-imt

    echo "Step 7: Running join_tollbooths for tb-stretch-id mappings..."
    uv run scripts/join_tollbooths.py --year 2024 --tb-stretch-id-imt 2021
    uv run scripts/join_tollbooths.py --year 2021 --tb-stretch-id-imt-delta 2022 --pivot-year 2024
    uv run scripts/join_tollbooths.py --year 2022 --tb-stretch-id-imt-delta 2023 --pivot-year 2024
    uv run scripts/join_tollbooths.py --year 2023 --tb-stretch-id-imt-delta 2024 --pivot-year 2024
    uv run scripts/join_tollbooths.py --year 2024 --tb-stretch-id-imt-delta 2025 --pivot-year 2025

    uv run scripts/join_tollbooths.py --year 2025 --tb-stretch-id-patch

    echo "Build process completed successfully!"
}

clean() {
    echo "Clean functionality is not available yet."
    uv run scripts/populate_db.py --clean-db
    uv run scripts/stage.py --year 2021 --clean
    exit 1
}

# Main script logic
if [ $# -eq 0 ]; then
    echo "Usage: $0 [build|clean]"
    echo ""
    echo "Commands:"
    echo "  build  - Execute all data processing commands sequentially"
    echo "  clean  - Clean up temporary data (not available yet)"
    exit 1
fi

case "$1" in
    build)
        build
        ;;
    clean)
        clean
        ;;
    *)
        echo "Error: Unknown command '$1'"
        echo "Usage: $0 [build|clean]"
        exit 1
        ;;
esac
