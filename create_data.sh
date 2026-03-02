#!/bin/bash

# Usage: ./create_data.sh [build|clean]
# - build: Execute all data processing commands sequentially
# - clean: Clean up temporary data (not implemented yet)

set -e  # Exit on error

build() {
    echo "Starting build process..."

    echo "Build process completed successfully!"
}

tb() {
    local year="$1"
    uv run scripts/stage.py --year "$year" --pub-to-stg tb --normalize
    uv run scripts/tollbooth_cluster.py --year "$year" --tollbooth-neighbours
    uv run scripts/join_tollbooths.py --year "$year" --map-tb-id
    uv run scripts/populate_db.py --year "$year" --delete-table tollbooth
    uv run scripts/populate_db.py --year "$year" --new-tb
    uv run scripts/populate_db.py --year "$year" --delete-table tbneighbour
    uv run scripts/populate_db.py --year "$year" --insert-tb-neighbours
}

tb_imt() {
    local year="$1"
    uv run scripts/stage.py --year "$year" --raw-to-stg tb_imt
    uv run scripts/populate_db.py --year "$year" --delete-table tbimt
    uv run scripts/populate_db.py --year "$year" --new-tb-imt
}

stretch() {
    local year="$1"
    uv run scripts/stage.py --year "$year" --pub-to-stg stretch --normalize
    uv run scripts/populate_db.py --year "$year" --delete-table stretch
    uv run scripts/populate_db.py --year "$year" --new-stretch
}

road() {
    local year="$1"
    uv run scripts/stage.py --year "$year" --pub-to-stg road --normalize
    uv run scripts/populate_db.py --year "$year" --delete-table road
    uv run scripts/populate_db.py --year "$year" --new-road
}

stretch_toll() {
    local year="$1"
    uv run scripts/stage.py --year "$year" --pub-to-stg stretch_toll
    uv run scripts/populate_db.py --year "$year" --delete-table stretchtoll
    uv run scripts/populate_db.py --year "$year" --new-stretch-toll
}

stretch_toll_imt() {
    local year="$1"
    uv run scripts/stage.py --year "$year" --raw-to-stg tb_toll_imt
}

tb_stretch_id() {
    local year="$1"
    uv run scripts/stage.py --pub-to-stg tb_stretch_id --year "$year"
    uv run scripts/populate_db.py --year "$year" --delete-table tbstretchid
    uv run scripts/populate_db.py --year "$year" --new-tb-stretch
}

build_tb_stretch_id() {
    local year="$1"
    uv run scripts/join_tollbooths.py --year "$year" --tb-stretch-id-imt "$year"
    uv run scripts/populate_db.py --year "$year" --delete-table tbstretchid
    uv run scripts/populate_db.py --year "$year" --new-tb-stretch
}

build_sts() {
    uv run scripts/dv_cleaner.py --year 2025 --from-page 55
    uv run scripts/dv_cleaner.py --year 2024 --from-page 55
    uv run scripts/dv_cleaner.py --year 2023 --from-page 54
    uv run scripts/dv_cleaner.py --year 2022 --from-page 53
    uv run scripts/dv_cleaner.py --year 2021 --from-page 51
    uv run scripts/dv_cleaner.py --year 2020 --from-page 51
    uv run scripts/dv_cleaner.py --year 2019 --from-page 51

    uv run scripts/stage.py --year 2018 --stg-to-prod tb_sts
    uv run scripts/stage.py --year 2019 --stg-to-prod tb_sts
    uv run scripts/stage.py --year 2020 --stg-to-prod tb_sts
    uv run scripts/stage.py --year 2021 --stg-to-prod tb_sts
    uv run scripts/stage.py --year 2022 --stg-to-prod tb_sts
    uv run scripts/stage.py --year 2023 --stg-to-prod tb_sts
    uv run scripts/stage.py --year 2024 --stg-to-prod tb_sts

    # uv run scripts/populate_db.py --year 2024 --new-tb-sts
}

clean() {
    echo "Clean functionality is not available yet."
    uv run scripts/populate_db.py --clean-db
    exit 1
}

# Main script logic
if [ $# -eq 0 ]; then
    echo "Usage: $0 [build|tb|tb_stretch_id|clean]"
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
    road)
        if [ -z "$2" ]; then
            echo "Error: tb requires a year argument."
            exit 1
        fi
        road "$2"
        ;;
    stretch)
        if [ -z "$2" ]; then
            echo "Error: tb requires a year argument."
            exit 1
        fi
        stretch "$2"
        ;;
    tb)
        if [ -z "$2" ]; then
            echo "Error: tb requires a year argument."
            exit 1
        fi
        tb "$2"
        ;;
    tb_stretch_id)
        if [ -z "$2" ]; then
            echo "Error: tb_stretch_id requires a year argument."
            exit 1
        fi
        tb_stretch_id "$2"
        ;;
    clean)
        clean
        ;;
    *)
        echo "Error: Unknown command '$1'"
        echo "Usage: $0 [build|tb <year>|tb_stretch_id <year>|clean]"
        exit 1
        ;;
esac
