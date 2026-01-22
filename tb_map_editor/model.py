from sqlmodel import Field, SQLModel, UniqueConstraint
from pydantic_core import CoreSchema, core_schema
from pydantic import GetCoreSchemaHandler
from typing import Any, get_args

import polars as pl


class UInt16(int):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_after_validator_function(cls, handler(int))

    @staticmethod
    def polars_dtype():
        return pl.UInt16


class UInt32(int):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_after_validator_function(cls, handler(int))

    @staticmethod
    def polars_dtype():
        return pl.UInt32
    

class UInt64(int):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_after_validator_function(cls, handler(int))

    @staticmethod
    def polars_dtype():
        return pl.UInt64


class Float32(float):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_after_validator_function(cls, handler(float))

    @staticmethod
    def polars_dtype():
        return pl.Float32
    

class Float64(float):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_after_validator_function(cls, handler(float))

    @staticmethod
    def polars_dtype():
        return pl.Float64


class String(str):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_after_validator_function(cls, handler(str))

    @staticmethod
    def polars_dtype():
        return pl.String


class Date(str):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_after_validator_function(cls, handler(str))

    @staticmethod
    def polars_dtype():
        return pl.Date


class Schema:
    _polar_dtype_attr = "polars_dtype"

    @staticmethod
    def _get_polars_dtype(field_type):
        if hasattr(field_type.annotation, Schema._polar_dtype_attr):
            polar_type = getattr(field_type.annotation, Schema._polar_dtype_attr)()
        else:
            for arg in get_args(field_type.annotation):
                if hasattr(arg, Schema._polar_dtype_attr):
                    polar_type = getattr(arg, Schema._polar_dtype_attr)()
                    break
        return polar_type

    @classmethod
    def dict_schema(cls) -> dict:
        dict_schema = {
            field_name: cls._get_polars_dtype(field_type)
            for field_name, field_type in cls.model_fields.items()
        }
        return dict_schema


class TbModel(SQLModel, Schema, table=False):
    @classmethod
    def name(cls):
        return cls.__name__.lower()
    
    def get_not_null_fields(self) -> dict:
        fields_value = {}
        for field_name in self.dict_schema():
            value = getattr(self, field_name, None)
            if value is not None:
                fields_value[field_name] = value
        return fields_value

    @classmethod
    def get_columns(cls, columns: list[str]) -> list:
        return [getattr(cls, column) for column in columns]


class Tollbooth(TbModel, table=True):
    tollbooth_id: UInt16 | None = Field(default=None, primary_key=True)
    legacy_id: UInt16 | None = Field(default=None)
    tollbooth_name: String | None = Field(default=None, index=True)
    lat: Float64 | None
    lng: Float64 | None
    status: String
    state: String
    place: String | None
    lines: UInt16 | None
    type: String
    manage: String | None
    gate_to: String | None
    info_year: UInt16 = Field(primary_key=True)

    @classmethod
    def online_empty_fields(cls, exclude_fields: set | None = None) -> dict:
        fields = {}
        if exclude_fields is None:
            exclude_fields = {"tollbooth_id", "legacy_id", "lat", "lng"}
        for field, _ in cls.model_fields.items():
            if field not in exclude_fields:
                fields[field] = None
        return fields

    def online_filled_fields(self, exclude_fields: set | None = None) -> dict:
        fields = {}
        for field in Tollbooth.online_empty_fields(exclude_fields):
            fields[field] = getattr(self, field)
        return fields


class TbSts(TbModel, table=True):
    index: String = Field(primary_key=True)
    tollbooth_name: String
    way: String
    highway: String | None = Field(default=None)
    km: Float64 | None = Field(default=None)
    lat: Float64
    lng: Float64
    tdpa: UInt32
    motorbike: Float64 | None
    car: Float64 | None
    car_axle: Float64 | None
    bus: Float64 | None
    truck_2_axle: Float64 | None
    truck_3_axle: Float64 | None
    truck_4_axle: Float64 | None
    truck_5_axle: Float64 | None
    truck_6_axle: Float64 | None
    truck_7_axle: Float64 | None
    truck_8_axle: Float64 | None
    truck_9_axle: Float64 | None
    not_classified_vehicle: Float64
    vta: UInt64
    jan: Float64 | None
    feb: Float64 | None
    mar: Float64 | None
    apr: Float64 | None
    may: Float64 | None
    jun: Float64 | None
    jul: Float64 | None
    ago: Float64 | None
    sep: Float64 | None
    oct: Float64 | None
    nov: Float64 | None
    dec: Float64 | None
    info_year: UInt16 = Field(primary_key=True)


class Road(TbModel, table=True):
    road_id: UInt16 | None = Field(default=None, primary_key=True)
    road_name: String
    operation_date: Date | None
    project_mx_id: String | None
    fonadin_ref: String | None
    road_length_km: Float64 | None
    bond_code: String | None
    bond_issuance_date: String | None
    bond_terms_years: String | None
    notes: String | None
    info_year: UInt16 = Field(primary_key=True)


class Stretch(TbModel, table=True):
    stretch_id: UInt32 | None = Field(default=None, primary_key=True)
    stretch_name: String = Field(index=True)
    stretch_length_km: Float64 | None
    sct_id_via: UInt16 | None
    road_id: UInt16 | None = Field(default=None, foreign_key="road.road_id")
    manage: String | None
    way: String | None
    info_year: UInt16 = Field(primary_key=True)


class MapTbImt(TbModel, table=True):
    tollbooth_id: UInt32 = Field(foreign_key="tollbooth.tollbooth_id", primary_key=True)
    tollbooth_imt_id: UInt32 = Field(foreign_key="tbimt.tollbooth_id", primary_key=True)
    distance: Float64
    info_year: UInt16 = Field(primary_key=True)


class TbStretchId(TbModel, table=True):
    stretch_id: UInt32 = Field(foreign_key="stretch.stretch_id", primary_key=True)
    tollbooth_id_a: UInt32 | None = Field(default=None, foreign_key="tollbooth.tollbooth_id", primary_key=True)
    tollbooth_id_b: UInt32 | None = Field(default=None, foreign_key="tollbooth.tollbooth_id", primary_key=True)
    info_year: UInt16 = Field(primary_key=True)


class StretchToll(TbModel, table=True):
    stretch_id: UInt32 = Field(foreign_key="stretch.stretch_id", primary_key=True)
    motorbike: Float64 | None
    car: Float64 | None
    car_axle: Float64 | None
    bus_2_axle: Float64 | None
    bus_3_axle: Float64 | None
    bus_4_axle: Float64 | None
    truck_2_axle: Float64 | None
    truck_3_axle: Float64 | None
    truck_4_axle: Float64 | None
    truck_5_axle: Float64 | None
    truck_6_axle: Float64 | None
    truck_7_axle: Float64 | None
    truck_8_axle: Float64 | None
    truck_9_axle: Float64 | None
    load_axle: Float64 | None
    truck_10_axle: Float64 | None
    toll_ref: String
    motorbike_axle: Float64 | None
    car_rush_hour: Float64 | None
    car_evening_hour: Float64 | None
    pedestrian: Float64 | None
    bicycle: Float64 | None
    car_rush_hour_2: Float64 | None
    car_evening_hour_2: Float64 | None
    car_morning_night: Float64 | None
    info_year: UInt16 = Field(primary_key=True)


class TbImt(TbModel, table=True):
    tollbooth_id: UInt16 = Field(primary_key=True)
    tollbooth_name: String
    area: String | None
    subarea: String | None
    type: String | None
    function: String | None
    calirepr: String | None
    lat: Float64 | None
    lng: Float64 | None
    info_year: UInt16 = Field(primary_key=True)


class TbTollImt(TbModel, table=True):
    tollbooth_id_a: UInt16 = Field(foreign_key="tbimt.tollbooth_id", primary_key=True)
    tollbooth_id_b: UInt16 = Field(foreign_key="tbimt.tollbooth_id", primary_key=True)
    nombre_sal: String | None
    nombre_ent: String | None
    motorbike: Float64 | None
    car: Float64 | None
    car_axle: Float64 | None
    bus_2_axle: Float64 | None
    bus_3_axle: Float64 | None
    bus_4_axle: Float64 | None
    truck_2_axle: Float64 | None
    truck_3_axle: Float64 | None
    truck_4_axle: Float64 | None
    truck_5_axle: Float64 | None
    truck_6_axle: Float64 | None
    truck_7_axle: Float64 | None
    truck_8_axle: Float64 | None
    truck_9_axle: Float64 | None
    load_axle: Float64 | None
    info_year: UInt16 = Field(primary_key=True)


class TbStsId(TbModel, table=True):
    tollbooth_id: UInt32 | None = Field(default=None, primary_key=True)
    historic_id: UInt32
    h3_cell: UInt64
    tollbooth_name: String
    way: String
    info_year: UInt16


class TbStsStretchId(TbModel, table=True):
    tollboothsts_id: UInt32 = Field(foreign_key="tbstsid.tollbooth_id", primary_key=True)
    stretch_id: UInt32 = Field(foreign_key="stretch.stretch_id", primary_key=True)
    info_year: UInt16 = Field(default=None, primary_key=True)
