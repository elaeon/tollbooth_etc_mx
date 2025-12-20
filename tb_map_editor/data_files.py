import os
from dataclasses import dataclass


@dataclass
class DataPath:
    tollbooth_catalog: str = os.path.abspath("./data/tables/tollbooths_catalog.csv")

    def __init__(self, year: int):
        self.year:int = year
    
    @property
    def tollbooth_sts_catalog(self):
        return os.path.abspath(f"./data/tables/{self.year}/tollbooths_sts_catalog.csv")
    
    @property
    def tollbooth_sts_data(self):
        return os.path.abspath(f"./data/tables/{self.year}/tollbooths_sts_data.csv")

    @property
    def strech_toll(self):
        pass


file_path = {
    "tollbooth": os.path.abspath("./data/tables/tollbooths_catalog.csv"),
    "tollbooth_sts": os.path.abspath(f"./data/tables/tollbooths_sts_catalog.csv"),
    "tollbooth_sts_data": os.path.abspath(f"./data/tables/tollbooths_sts_data.csv"),
}
