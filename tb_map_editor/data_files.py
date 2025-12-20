import os

year = 2025
prev_year = year - 1


file_path = {
    "tollbooth": os.path.abspath("./data/tables/tollbooths_catalog.csv"),
    "tollbooth_sts": os.path.abspath(f"./data/tables/tollbooths_sts_catalog.csv"),
    "tollbooth_sts_data": os.path.abspath(f"./data/tables/tollbooths_sts_data.csv"),
}
