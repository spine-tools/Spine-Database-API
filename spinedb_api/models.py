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

from datetime import datetime
from datetime import timedelta
from types import NoneType
from typing import Annotated
from typing import Literal
from typing import TypeAlias
from typing import TypedDict

from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
from pydantic import BeforeValidator
from pydantic import Field
from pydantic import PlainSerializer
from pydantic import PlainValidator
from pydantic import RootModel
from pydantic import TypeAdapter
from pydantic import WithJsonSchema
from pydantic import model_validator
from pydantic.dataclasses import dataclass
from typing_extensions import NotRequired
from typing_extensions import Self

from .compat.converters import to_duration
from .compat.converters import to_relativedelta
from .helpers import FormatMetadata
from .helpers import TimeSeriesMetadata


def from_timestamp(ts: str | pd.Timestamp | datetime) -> datetime:
    match ts:
        # NOTE: subtype of datetime, has to be before
        case pd.Timestamp():
            return ts.to_pydatetime()
        case datetime():
            return ts
        case str():
            return datetime.fromisoformat(ts)
        case _:
            raise ValueError(f"{ts}: could not coerce to `datetime`")


def validate_relativedelta(value: str | pd.DateOffset | timedelta | relativedelta) -> relativedelta:
    match value:
        case relativedelta():
            return value
        case str() | pd.DateOffset() | timedelta():
            return to_relativedelta(value)
        case _:
            raise ValueError(f"{value}: cannot coerce `{type(value)}` to `relativedelta`")


# types
class TimePeriod(str):
    """Wrapper type necessary for data migration.

    This is necessary to discriminate from regular strings during
    during DB migration.  In the future if the migration script
    doesn't need to be supported, this type can be removed, and the
    `TimePeriod_` annotation below can just use `str`.  Something like
    this:

    .. sourcecode:: python

       TimePeriod_: TypeAlias = Annotated[
           str,
           WithJsonSchema(
               {"type": "string", "format": "time_period"},
               mode="serialization"
           ),
       ]

    """

    def __init__(self, value) -> None:
        if not isinstance(value, str):
            raise ValueError(f"{type(value)}: non-string values cannot be a TimePeriod")
        super().__init__()


# annotations for validation
Datetime: TypeAlias = Annotated[datetime, BeforeValidator(from_timestamp)]
RelativeDelta: TypeAlias = Annotated[
    relativedelta,
    PlainValidator(validate_relativedelta),
    PlainSerializer(to_duration, when_used="json"),
    WithJsonSchema({"type": "string", "format": "duration"}, mode="serialization"),
]
TimePeriod_: TypeAlias = Annotated[
    TimePeriod,
    PlainValidator(TimePeriod),
    PlainSerializer(str),
    WithJsonSchema({"type": "string", "format": "time_period"}, mode="serialization"),
]

# non-nullable arrays
Floats: TypeAlias = list[float]
Integers: TypeAlias = list[int]
Strings: TypeAlias = list[str]
Booleans: TypeAlias = list[bool]
Datetimes: TypeAlias = list[Datetime]
Durations: TypeAlias = list[RelativeDelta]
TimePeriods_: TypeAlias = list[TimePeriod_]

# nullable variant of arrays
NullableIntegers: TypeAlias = list[int | None]
NullableFloats: TypeAlias = list[float | None]
NullableStrings: TypeAlias = list[str | None]
NullableBooleans: TypeAlias = list[bool | None]
NullableDatetimes: TypeAlias = list[Datetime | None]
NullableDurations: TypeAlias = list[RelativeDelta | None]
NullableTimePeriods_: TypeAlias = list[TimePeriod_ | None]

# sets of types used to define array schemas below
IndexTypes: TypeAlias = Integers | Floats | Strings | Booleans | Datetimes | Durations | TimePeriods_
NullableTypes: TypeAlias = (
    NullableIntegers
    | NullableFloats
    | NullableStrings
    | NullableBooleans
    | NullableDatetimes
    | NullableDurations
    | NullableTimePeriods_
)

# names of types used in the schema
NullTypeName: TypeAlias = Literal["null"]
TypeNames: TypeAlias = Literal["int", "float", "str", "bool", "date_time", "duration", "time_period"]
SpecialTypeNames: TypeAlias = Literal["duration"]  # FIXME: remove

typename_map: dict[NullTypeName | TypeNames, type | TypeAlias] = {
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "date_time": Datetime,
    "duration": RelativeDelta,
    "time_period": TimePeriod,
    "null": NoneType,
}
type_map: dict[type, TypeNames] = {
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
                super().__setattr__(
                    "values", [datetime.fromisoformat(x) if x is not None else x for x in getattr(self, "values")]
                )
            case "duration":
                super().__setattr__(
                    "values", [parse_duration(x) if x is not None else x for x in getattr(self, "values")]
                )
        return self


class _ConvertsByValueType:

    @model_validator(mode="after")
    def convert_to_final_type(self) -> Self:
        if getattr(self, "value_type") == "duration":
            super().__setattr__("values", [parse_duration(x) if x is not None else x for x in getattr(self, "values")])
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
    metadata: Optional[Metadata] = None
    type: Literal["run_length_index"] = "run_length_index"


@dataclass(frozen=True)
class RunEndIndex(_ConvertsIndexByValueType):
    """Run end encoded array"""

    name: str
    run_end: Integers
    values: IndexTypes
    value_type: IndexValueTypeNames
    metadata: Optional[Metadata] = None
    type: Literal["run_end_index"] = "run_end_index"


@dataclass(frozen=True)
class DictEncodedIndex(_ConvertsIndexByValueType):
    """Dictionary encoded array"""

    name: str
    indices: Integers
    values: IndexTypes
    value_type: IndexValueTypeNames
    metadata: Optional[Metadata] = None
    type: Literal["dict_encoded_index"] = "dict_encoded_index"


@dataclass(frozen=True)
class ArrayIndex(_ConvertsIndexByValueType):
    """Any array that is an index, e.g. a sequence, timestamps, labels"""

    name: str
    values: IndexTypes
    value_type: IndexValueTypeNames
    metadata: Optional[Metadata] = None
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
    metadata: Optional[Metadata] = None
    type: Literal["run_length_array"] = "run_length_array"


@dataclass(frozen=True)
class RunEndArray(_ConvertsByValueType):
    """Run end encoded array"""

    name: str
    run_end: Integers
    values: NullableValueTypes
    value_type: ValueTypeNames
    metadata: Optional[Metadata] = None
    type: Literal["run_end_array"] = "run_end_array"


@dataclass(frozen=True)
class DictEncodedArray(_ConvertsByValueType):
    """Dictionary encoded array"""

    name: str
    indices: NullableIntegers
    values: NullableValueTypes
    value_type: ValueTypeNames
    metadata: Optional[Metadata] = None
    type: Literal["dict_encoded_array"] = "dict_encoded_array"


@dataclass(frozen=True)
class Array(_ConvertsByValueType):
    """Array"""

    name: str
    values: NullableValueTypes
    value_type: ValueTypeNames
    metadata: Optional[Metadata] = None
    type: Literal["array"] = "array"


@dataclass(frozen=True)
class AnyArray:
    """Array with mixed types"""

    name: str
    values: NullableAnyTypes
    special_types: dict[int, SpecialTypeNames]
    value_type: Literal["any"] = "any"
    metadata: Optional[Metadata] = None
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
