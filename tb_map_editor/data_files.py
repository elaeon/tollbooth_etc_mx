import os
from dataclasses import dataclass

from . import model

_data_folder = "./data/tables/"


def build_path(filename, attr:dict | None = None) -> str:
    if attr is None:
        path = os.path.abspath(os.path.join(_data_folder, filename))
    else:
        levels = list(attr.values())
        levels.append(filename)
        path = os.path.abspath(os.path.join(_data_folder, "/".join(map(str, levels))))
    return path


@dataclass
class PathSchema:
    def __init__(self, name: str, schema: dict, attr: dict | None = None):
        self.name: str = name
        self.schema: dict = schema
        self.attr: dict | None = attr

    @property
    def csv(self) -> str:
        return build_path(f"{self.name}.csv", self.attr)
    
    @property
    def parquet(self) -> str:
        return build_path(f"{self.name}.parquet", self.attr)


class DataPathSchema:
    def __init__(self, year: int):
        self.attr = {"year": year}
    
    @property
    def tollbooths(self) -> PathSchema:
        return PathSchema("tollbooths", model.Tollbooth.dict_schema(), self.attr)
    
    @property
    def strechs(self) -> PathSchema:
        return PathSchema("strechs", model.Strech.dict_schema(), self.attr)
    
    @property
    def roads(self) -> PathSchema:
        return PathSchema("roads", model.Road.dict_schema(), self.attr)

    @property
    def tollbooths_sts(self) -> PathSchema:
        return PathSchema("tollbooths_sts", model.TollboothSts.dict_schema(), self.attr)
    
    @property
    def strechs_toll(self) -> PathSchema:
        return PathSchema("strechs_toll", model.StrechToll.dict_schema(), self.attr)
        
    @property
    def tb_imt_tb_id(self) -> PathSchema:
        return PathSchema("tb_imt_tb_id", model.TbImtTb.dict_schema())

    @property
    def tb_strech_id(self) -> PathSchema:
        return PathSchema("tb_strech_id", model.TbStrech.dict_schema())
    
    @property
    def tbsts_strech_id(self) -> PathSchema:
        return PathSchema("tbsts_strech_id", model.TbstsStrech.dict_schema())
    