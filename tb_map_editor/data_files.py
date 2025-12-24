import os
from dataclasses import dataclass

from . import schemas

_data_folder = "./data/tables/"


def _build_path(filename, year:int | None = None) -> str:
    if year is None:
        path = os.path.abspath(os.path.join(_data_folder, filename))
    else:
        path = os.path.abspath(os.path.join(_data_folder, f"{year}/{filename}"))
    return path


@dataclass
class PathSchema:
    def __init__(self, path: str, schema: dict):
        self.path: str = path
        self.schema: dict = schema


@dataclass
class DataPathSchema:
    tollbooths_catalog: PathSchema = PathSchema(_build_path("tollbooths_catalog.csv"), schemas.tollbooth_schema)
    strechs_catalog: PathSchema = PathSchema(_build_path("strechs_catalog.csv"), schemas.strechs_schema)
    roads: PathSchema = PathSchema(_build_path("roads.csv"), schemas.roads_schema)

    def __init__(self, year: int):
        self.year:int = year
    
    def _build_path(self, filename: str) -> str:
        return _build_path(filename, self.year)
    
    @property
    def tollbooths_sts_catalog(self) -> PathSchema:
        return PathSchema(self._build_path("tollbooths_sts_catalog.csv"), schemas.tollbooth_sts_schema)
    
    @property
    def tollbooths_sts_data(self) -> PathSchema:
        return PathSchema(self._build_path("tollbooths_sts_data.csv"), schemas.tollbooth_sts_data_schema)

    @property
    def tollbooths_sts_full(self) -> PathSchema:
        return PathSchema(self._build_path("tollbooths_sts.csv"), schemas.tollbooth_sts_full_schema)
    
    @property
    def strechs_toll(self) -> PathSchema:
        return PathSchema(
            self._build_path("strechs_toll.csv"), getattr(schemas, f"strechs_tolls_{self.year}_schema")
        )

    @property
    def strechs_data(self) -> PathSchema:
        return PathSchema(self._build_path("strechs_data.csv"), schemas.strechs_data_schema)
