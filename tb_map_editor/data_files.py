import os
from dataclasses import dataclass

from . import model
from .model import TbModel

_data_folder = "./data/tables/"
_tmp_data_folder = "./tmp_data/"


def build_path(filename, attr:dict | None = None, folder: str = _data_folder) -> str:
    if attr is None:
        path = os.path.abspath(os.path.join(folder, filename))
    else:
        levels = list(attr.values())
        levels.append(filename)
        path = os.path.abspath(os.path.join(folder, "/".join(map(str, levels))))
    return path


@dataclass
class PathModel:
    def __init__(self, filename: str, model: TbModel, attr: dict | None = None, tmp_data: bool = False):
        self.filename: str = filename
        self.model: TbModel = model
        self.attr: dict | None = attr
        self.folder: str = _tmp_data_folder if tmp_data is True else _data_folder

    @property
    def csv(self) -> str:
        return build_path(f"{self.filename}.csv", self.attr, folder=self.folder)
    
    @property
    def parquet(self) -> str:
        return build_path(f"{self.filename}.parquet", self.attr, folder=self.folder)


class DataModel:
    def __init__(self, year: int):
        self.attr = {"year": year}
    
    @property
    def tollbooths(self) -> PathModel:
        return PathModel("tollbooths", model.Tollbooth, self.attr)
    
    @property
    def stretchs(self) -> PathModel:
        return PathModel("strechs", model.Stretch, self.attr)
    
    @property
    def roads(self) -> PathModel:
        return PathModel("roads", model.Road, self.attr)

    @property
    def tb_sts(self) -> PathModel:
        return PathModel("tb_sts", model.TbSts, self.attr)
    
    @property
    def stretchs_toll(self) -> PathModel:
        return PathModel("stretchs_toll", model.StretchToll, self.attr)
        
    @property
    def tb_imt_tb_id(self) -> PathModel:
        return PathModel("tb_imt_tb_id", model.TbImtTb)

    @property
    def tb_stretch_id(self) -> PathModel:
        return PathModel("tb_strech_id", model.TbStretch)
    
    @property
    def tbsts_stretch_id(self) -> PathModel:
        return PathModel("tbsts_strech_id", model.TbstsStretch)
    
    @property
    def tbsts_id(self) -> PathModel:
        return PathModel("tbsts_id", model.TbstsId, self.attr)
    
    @property
    def tb_imt(self) -> PathModel:
        return PathModel("tb_imt", model.TbImt, self.attr, tmp_data=True)
    
    @property
    def tb_toll_imt(self) -> PathModel:
        return PathModel("tb_toll_imt", model.TbTollImt, self.attr, tmp_data=True)
