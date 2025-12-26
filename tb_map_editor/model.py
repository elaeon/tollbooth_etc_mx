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
    

class String(str):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_after_validator_function(cls, handler(str))

    @staticmethod
    def polars_dtype():
        return pl.String


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


class Tollbooth(SQLModel, Schema, table=True):
    tollbooth_id: UInt16 | None = Field(default=None, primary_key=True)
    legacy_id: UInt16 | None = Field(default=None)
    tollbooth_name: String | None = Field(default=None, index=True)
    coords: String | None
    status: String
    state: String
    place: String
    lines: UInt16
    type: String
    highway: String | None = Field(default=None)
    managed_by: String | None = Field(default=None)
    gate_to: String | None = Field(default=None)


class TollboothSts(SQLModel, Schema, table=True):
    tollboothsts_id: UInt32 | None = Field(default=None, primary_key=True)
    tollbooth_name: String
    way: String
    highway: String | None = Field(default=None)
    km: Float32 | None = Field(default=None)
    coords: String


class TollboothStsData(SQLModel, Schema, table=True):
    tollboothsts_id: UInt32 = Field(foreign_key="tollboothsts.tollboothsts_id", primary_key=True)
    tdpa: UInt32
    motorbike: Float32 | None = Field(default=None)
    car: Float32 | None = Field(default=None)
    car_axle: Float32 | None = Field(default=None)
    bus: Float32 | None = Field(default=None)
    truck_2_axle: Float32 | None = Field(default=None)
    truck_3_axle: Float32 | None = Field(default=None)
    truck_4_axle: Float32 | None = Field(default=None)
    truck_5_axle: Float32 | None = Field(default=None)
    truck_6_axle: Float32 | None = Field(default=None)
    truck_7_axle: Float32 | None = Field(default=None)
    truck_8_axle: Float32 | None = Field(default=None)
    truck_9_axle: Float32 | None = Field(default=None)
    not_classified_vehicle: Float32
    vta: UInt64
    jan: Float32 | None = Field(default=None)
    feb: Float32 | None = Field(default=None)
    mar: Float32 | None = Field(default=None)
    apr: Float32 | None = Field(default=None)
    may: Float32 | None = Field(default=None)
    jun: Float32 | None = Field(default=None)
    jul: Float32 | None = Field(default=None)
    ago: Float32 | None = Field(default=None)
    sep: Float32 | None = Field(default=None)
    oct: Float32 | None = Field(default=None)
    nov: Float32 | None = Field(default=None)
    dec: Float32 | None = Field(default=None)
    info_year: UInt16

    @classmethod
    def dict_schema(cls):
        exclude = {"info_year"}
        dict_schema = {
            field_name: cls._get_polars_dtype(field_type)
            for field_name, field_type in cls.model_fields.items()
            if field_name not in exclude
        }
        return dict_schema


class Strech(SQLModel, Schema, table=True):
    strech_id: UInt16 | None = Field(default=None, primary_key=True)
    strech_name: String


class TmpTb(SQLModel, Schema, table=True):
    id: UInt32 | None = Field(default=None, primary_key=True)
    name: String
    lat: Float32
    lon: Float32
    valid: bool | None = Field(default=True)

    @classmethod
    def dict_schema(cls):
        exclude = {"valid"}
        dict_schema = {
            field_name: cls._get_polars_dtype(field_type)
            for field_name, field_type in cls.model_fields.items()
            if field_name not in exclude
        }
        return dict_schema
