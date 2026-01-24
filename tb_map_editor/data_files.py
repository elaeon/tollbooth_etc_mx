import os
from dataclasses import dataclass

from . import model
from .model import TbModel


def build_path(filename, attr:dict, folder: str) -> str:
    levels = list(attr.values())
    levels.append(filename)
    path = os.path.abspath(os.path.join(folder, "/".join(map(str, levels))))
    return path


@dataclass
class DataStage:
    raw: str = "./data/raw/"
    stg: str = "./data/staging/"
    prd: str = "./data/production/"
    pub: str = "./data/tables/"


@dataclass
class PathModel:
    def __init__(self, filename: str, model: TbModel, attr: dict, folder: str):
        self.filename: str = filename
        self.model: TbModel = model
        self.attr: dict = attr
        self.folder: str = folder

    @property
    def csv(self) -> str:
        return build_path(f"{self.filename}.csv", self.attr, folder=self.folder)
    
    @property
    def parquet(self) -> str:
        return build_path(f"{self.filename}.parquet", self.attr, folder=self.folder)

    @property
    def schema(self) -> dict:
        return self.model.dict_schema()
    
    @property
    def str_normalize(self):
        return self.model.str_normalize()


class DataModel:
    def __init__(self, year: int, stage: DataStage):
        self.attr = {"year": year}
        self.stage = stage
    
    @property
    def tollbooths(self) -> PathModel:
        return PathModel("tollbooths", model.Tollbooth, self.attr, self.stage)
    
    @property
    def stretchs(self) -> PathModel:
        return PathModel("stretchs", model.Stretch, self.attr, self.stage)
    
    @property
    def roads(self) -> PathModel:
        return PathModel("roads", model.Road, self.attr, self.stage)

    @property
    def tb_sts(self) -> PathModel:
        return PathModel("tb_sts", model.TbSts, self.attr, self.stage)
    
    @property
    def stretchs_toll(self) -> PathModel:
        return PathModel("stretchs_toll", model.StretchToll, self.attr, self.stage)
        
    @property
    def map_tb_imt(self) -> PathModel:
        return PathModel("map_tb_imt", model.MapTbImt, self.attr, self.stage)

    @property
    def tb_stretch_id(self) -> PathModel:
        return PathModel("tb_stretch_id", model.TbStretchId, self.attr, self.stage)
    
    @property
    def tb_stretch_id_patch(self) -> PathModel:
        return PathModel("tb_stretch_id_patch", model.TbStretchId, self.attr, self.stage)
    
    @property
    def tb_stretch_id_patched(self) -> PathModel:
        return PathModel("tb_stretch_id_patched", model.TbStretchId, self.attr, self.stage)
    
    @property
    def tbsts_stretch_id(self) -> PathModel:
        return PathModel("tbsts_stretch_id", model.TbStsStretchId, self.attr, self.stage)
    
    @property
    def tbsts_id(self) -> PathModel:
        return PathModel("tbsts_id", model.TbStsId, self.attr, self.stage)
    
    @property
    def tb_imt(self) -> PathModel:
        return PathModel("tb_imt", model.TbImt, self.attr, self.stage)
    
    @property
    def tb_imt_delta(self) -> PathModel:
        return PathModel("tb_imt_delta", model.TbImt, self.attr, self.stage)

    @property
    def tb_toll_imt(self) -> PathModel:
        return PathModel("tb_toll_imt", model.TbTollImt, self.attr, self.stage)
