import os
from dataclasses import dataclass
from typing import Any, overload

from . import model
from .model import TbModel


def build_path(filename, attr: dict, folder: str) -> str:
    levels = list(attr.values())
    levels.append(filename)
    base_dir = "tollbooth_etc_mx"
    index = os.getcwd().find(base_dir)
    if index > -1:
        base_path = os.getcwd()[:index+len(base_dir)]
    else:
        raise Exception("data folder not found.")
    path = os.path.abspath(os.path.join(base_path, folder, "/".join(map(str, levels))))
    return path


@dataclass
class DataStage:
    stg: str = "data/staging/"
    pub: str = "data/tables/"


@dataclass
class PathModel:
    def __init__(self, filename: str, model: type[TbModel], attr: dict, stage: str, name: str | None = None):
        self.filename: str = filename
        self.model: type[TbModel] = model
        self.attr: dict = attr
        self.stage: str = stage
        self.name: str | None = name

    @property
    def csv(self) -> str:
        return build_path(f"{self.filename}.csv", self.attr, folder=self.stage)

    @property
    def parquet(self) -> str:
        return build_path(f"{self.filename}.parquet", self.attr, folder=self.stage)

    @property
    def schema(self) -> dict:
        return self.model.dict_schema()

    @property
    def str_normalize(self):
        return self.model.str_normalize()


class ModelDescriptor:
    """Descriptor that returns itself on class access and a PathModel on instance access.

    DataModel.tollbooth         → ModelDescriptor (with .name = task step name)
    DataModel(year, s).tollbooth → PathModel
    """

    def __init__(self, filename: str, model_cls: type[TbModel], task_name: str | None = None):
        self.filename = filename
        self.model_cls = model_cls
        self._task_name = task_name
        self._attr_name: str = ""

    def __set_name__(self, owner: type, name: str) -> None:
        self._attr_name = name

    @property
    def name(self) -> str:
        return self._task_name if self._task_name is not None else self._attr_name

    def __call__(self, dm: "DataModel") -> PathModel:
        return PathModel(self.filename, self.model_cls, dm.attr, dm.stage, self._task_name)

    @overload
    def __get__(self, obj: None, objtype: Any) -> "ModelDescriptor": ...
    @overload
    def __get__(self, obj: "DataModel", objtype: Any) -> PathModel: ...
    def __get__(self, obj: "DataModel | None", objtype: Any = None) -> "ModelDescriptor | PathModel":
        if obj is None:
            return self
        return self(obj)


class DataModel:
    tollbooth            = ModelDescriptor("tollbooths",           model.Tollbooth,       "pub_tb")
    stretch              = ModelDescriptor("stretchs",             model.Stretch,          "pub_stretch")
    road                 = ModelDescriptor("roads",                model.Road,             "pub_road")
    stretch_toll         = ModelDescriptor("stretchs_toll",        model.StretchToll,      "pub_stretch_toll")
    tb_stretch_id        = ModelDescriptor("tb_stretch_id",        model.TbStretchId,      "pub_tb_stretch_id")
    tb_stretch_id_patch  = ModelDescriptor("tb_stretch_id_patch",  model.TbStretchId)
    tb_sts_no_id         = ModelDescriptor("tb_sts_no_id",         model.TbSts,            "dv_cleaner")
    tb_sts               = ModelDescriptor("tb_sts",               model.TbSts,            "tb_sts")
    tb_sts_stretch_id    = ModelDescriptor("tb_sts_stretch_id",    model.TbStsStretchId,   "sts_stretch_id")
    tb_sts_stretch_id_patch = ModelDescriptor("tb_sts_stretch_id_patch", model.TbStsStretchId)
    tb_imt               = ModelDescriptor("tb_imt",               model.TbImt,            "raw_tb_imt")
    tb_toll_imt          = ModelDescriptor("tb_toll_imt",          model.TbTollImt,        "raw_tb_toll_imt")
    tb_neighbour         = ModelDescriptor("tb_neighbour",         model.TbNeighbour,      "neighbours")
    map_tb_id            = ModelDescriptor("map_tb_id",            model.MapTbId,          "map_tb_id")
    inflation            = ModelDescriptor("inflation",            model.Inflation,        "raw_inflation")
    tb_imt_stretch_id    = ModelDescriptor("tb_imt_stretch_id",    model.TbImtStretchId,   "imt_stretch_id")
    osm_tb_distance      = ModelDescriptor("osm_tb_distance",      model.OsmTbDistance,    "pub_osm")
    manager_revenue      = ModelDescriptor("manager_revenue",      model.ManagerRevenue,   "raw_manager_revenue")

    def __init__(self, year: int, stage: str):
        self.attr = {"year": year}
        self.stage: str = stage
