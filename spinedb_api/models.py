#!/usr/bin/env python
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "python-dateutil",
#   "pandas>=2",
#   "pydantic>=2",
# ]
# ///
######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Database API contributors
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""Write JSON schema for JSON blob in SpineDB"""

from datetime import datetime, timedelta
from typing import Annotated, ClassVar, Literal, Optional, TypeAlias
from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
from pydantic import BeforeValidator, PlainSerializer, PlainValidator, WithJsonSchema, model_validator
from pydantic.dataclasses import dataclass
from typing_extensions import NotRequired, Self, TypedDict
from .compat.converters import parse_duration, to_duration


def from_timestamp(ts: str | pd.Timestamp | datetime) -> datetime:
    match ts:
        case str():
            return datetime.fromisoformat(ts)
        case pd.Timestamp():
            return ts.to_pydatetime()
        case _:
            return ts


Floats: TypeAlias = list[float]
Integers: TypeAlias = list[int]
Strings: TypeAlias = list[str]
Booleans: TypeAlias = list[bool]

Datetime: TypeAlias = Annotated[datetime, BeforeValidator(from_timestamp)]
Datetimes: TypeAlias = list[Datetime]
RelativeDelta: TypeAlias = Annotated[
    relativedelta,
    PlainValidator(parse_duration),
    PlainSerializer(to_duration),
]
Timedeltas: TypeAlias = list[RelativeDelta]

# nullable variant of arrays
NullableIntegers: TypeAlias = list[int | None]
NullableFloats: TypeAlias = list[float | None]
NullableStrings: TypeAlias = list[str | None]
NullableBooleans: TypeAlias = list[bool | None]
NullableDurations: TypeAlias = list[RelativeDelta | None]

AnyType: TypeAlias = str | int | float | bool | RelativeDelta
NullableAnyTypes: TypeAlias = list[AnyType | None]

# sets of types used to define array schemas below
IndexTypes: TypeAlias = Integers | Floats | Strings | Datetimes | Timedeltas
NullableValueTypes: TypeAlias = (
    NullableIntegers | NullableFloats | NullableStrings | NullableBooleans | NullableDurations
)

# names of types used in the schema
ValueTypeNames: TypeAlias = Literal[
    "str",
    "int",
    "float",
    "bool",
    "date_time",
    "duration",
    "time_period",
]
IndexValueTypeNames: TypeAlias = Literal["str", "int", "float", "date_time", "duration", "time_period"]
SpecialTypeNames: TypeAlias = Literal["duration"]

type_map: dict[type, ValueTypeNames] = {
    str: "str",
    int: "int",
    np.int8: "int",
    np.int16: "int",
    np.int32: "int",
    np.int64: "int",
    float: "float",
    np.float16: "float",
    np.float32: "float",
    np.float64: "float",
    # np.float128: "float",  # not available on macos
    bool: "bool",
    np.bool: "bool",
    datetime: "date_time",
    pd.Timestamp: "date_time",
    timedelta: "duration",
    pd.Timedelta: "duration",
    relativedelta: "duration",
    pd.DateOffset: "duration",
}


class _ConvertsIndexByValueType:

    @model_validator(mode="after")
    def convert_to_final_type(self) -> Self:
        match getattr(self, "value_type"):
            case "date_time":
                super().__setattr__("values", list(map(datetime.fromisoformat, getattr(self, "values"))))
            case "duration":
                super().__setattr__("values", list(map(parse_duration, getattr(self, "values"))))
        return self


class _ConvertsByValueType:

    @model_validator(mode="after")
    def convert_to_final_type(self) -> Self:
        if getattr(self, "value_type") == "duration":
            super().__setattr__("values", list(map(parse_duration, getattr(self, "values"))))
        return self


@dataclass(frozen=True)
class RunLengthIndex(_ConvertsIndexByValueType):
    """Run length encoded array

    NOTE: this is not supported by PyArrow, if we use it, we will have
    to convert to a supported format.

    """

    name: str
    run_len: Integers
    values: IndexTypes
    value_type: IndexValueTypeNames
    metadata: Optional[str] = None
    type: Literal["run_length_index"] = "run_length_index"


@dataclass(frozen=True)
class RunEndIndex(_ConvertsIndexByValueType):
    """Run end encoded array"""

    name: str
    run_end: Integers
    values: IndexTypes
    value_type: IndexValueTypeNames
    metadata: Optional[str] = None
    type: Literal["run_end_index"] = "run_end_index"


@dataclass(frozen=True)
class DictEncodedIndex(_ConvertsIndexByValueType):
    """Dictionary encoded array"""

    name: str
    indices: Integers
    values: IndexTypes
    value_type: IndexValueTypeNames
    metadata: Optional[str] = None
    type: Literal["dict_encoded_index"] = "dict_encoded_index"


@dataclass(frozen=True)
class ArrayIndex(_ConvertsIndexByValueType):
    """Any array that is an index, e.g. a sequence, timestamps, labels"""

    name: str
    values: IndexTypes
    value_type: IndexValueTypeNames
    metadata: Optional[str] = None
    type: Literal["array_index"] = "array_index"


@dataclass(frozen=True)
class RunLengthArray(_ConvertsByValueType):
    """Run length encoded array

    NOTE: this is not supported by PyArrow, if we use it, we will have
    to convert to a supported format.

    """

    name: str
    run_len: Integers
    values: NullableValueTypes
    value_type: ValueTypeNames
    metadata: Optional[str] = None
    type: Literal["run_length_array"] = "run_length_array"


@dataclass(frozen=True)
class RunEndArray(_ConvertsByValueType):
    """Run end encoded array"""

    name: str
    run_end: Integers
    values: NullableValueTypes
    value_type: ValueTypeNames
    metadata: Optional[str] = None
    type: Literal["run_end_array"] = "run_end_array"


@dataclass(frozen=True)
class DictEncodedArray(_ConvertsByValueType):
    """Dictionary encoded array"""

    name: str
    indices: NullableIntegers
    values: NullableValueTypes
    value_type: ValueTypeNames
    metadata: Optional[str] = None
    type: Literal["dict_encoded_array"] = "dict_encoded_array"


@dataclass(frozen=True)
class Array(_ConvertsByValueType):
    """Array"""

    name: str
    values: NullableValueTypes
    value_type: ValueTypeNames
    metadata: Optional[str] = None
    type: Literal["array"] = "array"


@dataclass(frozen=True)
class AnyArray:
    """Array with mixed types"""

    name: str
    values: NullableAnyTypes
    special_types: dict[int, SpecialTypeNames]
    value_type: Literal["any"] = "any"
    metadata: Optional[str] = None
    type: Literal["any_array"] = "any_array"

    @model_validator(mode="after")
    def convert_to_final_type(self) -> Self:
        special_types = getattr(self, "special_types")
        values = getattr(self, "values")
        for row, value_type in special_types.items():
            match value_type:
                case "duration":
                    values[row] = parse_duration(values[row])
                case _:
                    raise ValueError(f"unknown special type {value_type}")
        return self


# NOTE: To add run-length encoding to the schema, add it to the
# following type union following which, we need to implement a
# converter to a compatible pyarrow array type
AllArrays: TypeAlias = RunEndIndex | DictEncodedIndex | ArrayIndex | RunEndArray | DictEncodedArray | Array | AnyArray
Table: TypeAlias = list[AllArrays]


class ArrayAsDict(TypedDict):
    name: str
    values: list
    value_type: str
    type: str
    metadata: NotRequired[Optional[str]]
    indices: NotRequired[list]
    run_end: NotRequired[Integers]
    run_len: NotRequired[Integers]
    special_types: NotRequired[dict[int, SpecialTypeNames]]


def dict_to_array(data: ArrayAsDict) -> AllArrays:
    match data["type"]:
        case "array":
            return Array(
                name=data["name"], value_type=data["value_type"], values=data["values"], metadata=data.get("metadata")
            )
        case "array_index":
            return ArrayIndex(
                name=data["name"], value_type=data["value_type"], values=data["values"], metadata=data.get("metadata")
            )
        case "dict_encoded_array":
            return DictEncodedArray(
                name=data["name"],
                value_type=data["value_type"],
                indices=data.get("indices", []),
                values=data["values"],
                metadata=data.get("metadata"),
            )
        case "dict_encoded_index":
            return DictEncodedIndex(
                name=data["name"],
                value_type=data["value_type"],
                indices=data.get("indices", []),
                values=data["values"],
                metadata=data.get("metadata"),
            )
        case "run_end_array":
            return RunEndArray(
                name=data["name"],
                value_type=data["value_type"],
                run_end=data.get("run_end", []),
                values=data["values"],
                metadata=data.get("metadata"),
            )
        case "run_end_index":
            return RunEndIndex(
                name=data["name"],
                value_type=data["value_type"],
                run_end=data.get("run_end", []),
                values=data["values"],
                metadata=data.get("metadata"),
            )
        case "any_array":
            return AnyArray(
                name=data["name"],
                values=data["values"],
                special_types=data["special_types"],
                metadata=data.get("metadata"),
            )
        case _:
            raise ValueError(f"{data['type']}: unknown array type")


if __name__ == "__main__":
    from argparse import ArgumentParser
    import json
    from pathlib import Path
    from pydantic import RootModel

    parser = ArgumentParser(__doc__)
    parser.add_argument("json_file", help="Path of JSON schema file to write")
    opts = parser.parse_args()

    schema = RootModel[Table].model_json_schema(mode="serialization")
    Path(opts.json_file).write_text(json.dumps(schema, indent=2))
