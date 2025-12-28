import os
from dataclasses import dataclass

from . import schemas
from . import model

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


class DataPathSchema:

    def __init__(self, year: int):
        self.year:int = year
    
    def _build_path(self, filename: str) -> str:
        return _build_path(filename, self.year)
    
    @property
    def tollbooths(self) -> PathSchema:
        return PathSchema(self._build_path("tollbooths.csv"), schemas.tollbooth_schema)
    
    @property
    def strechs(self) -> PathSchema:
        return PathSchema(self._build_path("strechs.csv"), schemas.strechs_schema)
    
    @property
    def roads(self) -> PathSchema:
        return PathSchema(self._build_path("roads.csv"), schemas.roads_schema)

    @property
    def tollbooths_sts(self) -> PathSchema:
        return PathSchema(self._build_path("tollbooths_sts.csv"), schemas.tollbooth_sts_schema)
    
    @property
    def strechs_toll(self) -> PathSchema:
        return PathSchema(
            self._build_path("strechs_toll.csv"), getattr(schemas, f"strechs_tolls_{self.year}_schema")
        )

    @property
    def strechs_data(self) -> PathSchema:
        return PathSchema(self._build_path("strechs_data.csv"), schemas.strechs_data_schema)
    
    @property
    def tb_imt_tb_id(self) -> PathSchema:
        return PathSchema(_build_path("tb_imt_tb_id.csv"), model.TbImtTb.dict_schema)
