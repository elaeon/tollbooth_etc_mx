import os
from dataclasses import dataclass

_data_folder = "./data/tables/"


def _build_path(filename, year:int | None = None) -> str:
    if year is None:
        path = os.path.abspath(os.path.join(_data_folder, filename))
    else:
        path = os.path.abspath(os.path.join(_data_folder, f"{year}/{filename}"))
    return path


@dataclass
class DataPath:
    tollbooths_catalog: str = _build_path("tollbooths_catalog.csv")
    strechs_catalog: str = _build_path("strechs_catalog.csv")
    roads: str = _build_path("roads.csv")

    def __init__(self, year: int):
        self.year:int = year
    
    def _build_path(self, filename: str) -> str:
        return _build_path(filename, self.year)
    
    @property
    def tollbooths_sts_catalog(self) -> str:
        return self._build_path("tollbooths_sts_catalog.csv")
    
    @property
    def tollbooths_sts_data(self) -> str:
        return self._build_path("tollbooths_sts_data.csv")

    @property
    def tollbooths_sts_full(self) -> str:
        return self._build_path("tollbooths_sts.csv")
    
    @property
    def strechs_toll(self) -> str:
        return self._build_path("strechs_toll.csv")

    @property
    def strechs_data(self) -> str:
        return self._build_path("strechs_data.csv")
