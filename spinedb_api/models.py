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


@dataclass(frozen=True)
class RunLengthIndex:
    """Run length encoded array

    NOTE: this is not supported by PyArrow, if we use it, we will have
    to convert to a supported format.

    """

    name: str
    run_len: Integers
    values: IndexTypes
    value_type: TypeNames
    metadata: str = ""
    type: Literal["run_length_index"] = "run_length_index"


@dataclass(frozen=True)
class RunLengthArray:
    """Run length encoded array

    NOTE: this is not supported by PyArrow, if we use it, we will have
    to convert to a supported format.

    """

    name: str
    run_len: Integers
    values: NullableTypes
    value_type: TypeNames
    metadata: str = ""
    type: Literal["run_length_array"] = "run_length_array"


@dataclass(frozen=True)
class RunEndIndex:
    """Run end encoded array"""

    name: str
    run_end: Integers
    values: IndexTypes
    value_type: TypeNames
    metadata: str = ""
    type: Literal["run_end_index"] = "run_end_index"


@dataclass(frozen=True)
class RunEndArray:
    """Run end encoded array"""

    name: str
    run_end: Integers
    values: NullableTypes
    value_type: TypeNames
    metadata: str = ""
    type: Literal["run_end_array"] = "run_end_array"


@dataclass(frozen=True)
class DictEncodedIndex:
    """Dictionary encoded array"""

    name: str
    indices: Integers
    values: IndexTypes
    value_type: TypeNames
    metadata: str = ""
    type: Literal["dict_encoded_index"] = "dict_encoded_index"


@dataclass(frozen=True)
class DictEncodedArray:
    """Dictionary encoded array"""

    name: str
    indices: NullableIntegers
    values: NullableTypes
    value_type: TypeNames
    metadata: str = ""
    type: Literal["dict_encoded_array"] = "dict_encoded_array"


@dataclass(frozen=True)
class ArrayIndex:
    """Any array that is an index, e.g. a sequence, timestamps, labels"""

    name: str
    values: IndexTypes
    value_type: TypeNames
    metadata: str = ""
    type: Literal["array_index"] = "array_index"


@dataclass(frozen=True)
class Array:
    """Array"""

    name: str
    values: NullableTypes
    value_type: TypeNames
    metadata: str = ""
    type: Literal["array"] = "array"


AnyType: TypeAlias = str | int | float | bool | RelativeDelta | Datetime
NullableAnyTypes: TypeAlias = list[AnyType | None]


@dataclass(frozen=True)
class AnyArray:
    """Array with mixed types"""

    name: str
    values: NullableAnyTypes
    value_types: list[TypeNames | NullTypeName]
    metadata: str = ""
    type: Literal["any_array"] = "any_array"

    @model_validator(mode="after")
    def convert_to_final_type(self) -> Self:
        if len(self.values) != len(self.value_types):
            raise ValueError("mismatching values and value_types")

        for i in range(len(self.values)):
            val = self.values[i]
            typ = self.value_types[i]
            self.values[i] = TypeAdapter(typename_map[typ]).validate_python(val)
        return self


# NOTE: To add run-length encoding to the schema, add it to the
# following type union following which, we need to implement a
# converter to a compatible pyarrow array type
AllArrays: TypeAlias = RunEndIndex | DictEncodedIndex | ArrayIndex | RunEndArray | DictEncodedArray | Array | AnyArray
Table: TypeAlias = list[Annotated[AllArrays, Field(discriminator="type")]]


def from_json(json_str: str, type_: type[Table | AllArrays] = Table):
    """Generic wrapper for JSON parsing."""
    return TypeAdapter(type_).validate_json(json_str)


def from_dict(value: dict, type_: type[Table | AllArrays] = Table):
    """Generic wrapper for converting from a dictionary."""
    return TypeAdapter(type_).validate_python(value)


def to_json(obj: Table | AllArrays) -> str:
    """Generic wrapper to serialise to JSON."""
    # FIXME: check why the equivalent: TypeAdapter(obj).dump_json() isn't working
    return RootModel[type(obj)](obj).model_dump_json()


class ArrayAsDict(TypedDict):
    name: str
    type: str
    values: list
    value_type: TypeNames
    value_types: NotRequired[list[TypeNames | NullTypeName]]
    metadata: NotRequired[str]
    indices: NotRequired[list]
    run_end: NotRequired[Integers]
    run_len: NotRequired[Integers]


def dict_to_array(data: ArrayAsDict) -> AllArrays:
    """Wrapper to read structured dictionary as an array."""
    match data["type"]:
        case "array":
            type_ = Array
        case "array_index":
            type_ = ArrayIndex
        case "dict_encoded_array":
            type_ = DictEncodedArray
        case "dict_encoded_index":
            type_ = DictEncodedIndex
        case "run_end_array":
            type_ = RunEndArray
        case "run_end_index":
            type_ = RunEndIndex
        case "any_array":
            type_ = AnyArray
        case _:
            raise ValueError(f"{data['type']}: unknown array type")

    return TypeAdapter(type_).validate_python(data)


if __name__ == "__main__":
    from argparse import ArgumentParser
    import json
    from pathlib import Path

    parser = ArgumentParser(__doc__)
    parser.add_argument("json_file", help="Path of JSON schema file to write")
    opts = parser.parse_args()

    schema = TypeAdapter(Table).json_schema(mode="serialization")
    Path(opts.json_file).write_text(json.dumps(schema))
