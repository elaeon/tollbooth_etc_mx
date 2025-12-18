from sqlmodel import Field, SQLModel
from pydantic_core import CoreSchema, core_schema
from pydantic import GetCoreSchemaHandler
from typing import Any
import polars as pl


class UInt16(int):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_after_validator_function(cls, handler(int))

    @staticmethod
    def polar_type():
        return pl.UInt16


class String(str):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_after_validator_function(cls, handler(str))

    @staticmethod
    def polar_type():
        return pl.UInt16


class Tollbooth(SQLModel, table=True):
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

    @classmethod
    def dict_schema(cls):
        dict_schema = {
            field_name: field_type.annotation.polar_type()
            for field_name, field_type in cls.model_fields.items()
        }
        return dict_schema
