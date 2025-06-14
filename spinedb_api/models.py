#!/usr/bin/env python
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "python-dateutil",
#   "pandas>=2",
#   "pydantic>=2",
# ]
# ///

"""Write JSON schema for JSON blob in SpineDB"""

from datetime import datetime, timedelta
import re
from types import NoneType
from typing import Annotated, Literal, NotRequired, TypeAlias, TypedDict

from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
from pydantic import Field as field
from pydantic import PlainSerializer, PlainValidator, WithJsonSchema
from pydantic.dataclasses import dataclass

from .compat.converters import parse_duration, to_duration


class TimePattern_(str):
    pass


Floats: TypeAlias = list[float]
Integers: TypeAlias = list[int]
Strings: TypeAlias = list[str]
Booleans: TypeAlias = list[bool]

Datetimes: TypeAlias = list[datetime]
RelativeDelta: TypeAlias = Annotated[
    relativedelta,
    PlainValidator(parse_duration),
    PlainSerializer(to_duration),
    WithJsonSchema({"type": "string", "format": "duration"}, mode="serialization"),
]
Timedeltas: TypeAlias = list[RelativeDelta]

time_pat_re = r"(Y|M|D|WD|h|m|s)[0-9]+-[0-9]+"
TimePattern: TypeAlias = Annotated[
    TimePattern_,
    PlainValidator(TimePattern_),
    PlainSerializer(str),
    WithJsonSchema({"type": "string", "pattern": time_pat_re}, mode="serialization"),
]
TimePatterns: TypeAlias = list[TimePattern]

# nullable variant of arrays
NullableIntegers: TypeAlias = list[int | None]
NullableFloats: TypeAlias = list[float | None]
NullableStrings: TypeAlias = list[str | None]
NullableBooleans: TypeAlias = list[bool | None]

AnyType: TypeAlias = str | int | float | bool
NullableAnyTypes: TypeAlias = list[AnyType | None]

# sets of types used to define array schemas below
IndexTypes: TypeAlias = Integers | Strings | Datetimes | Timedeltas | TimePatterns
NullableValueTypes: TypeAlias = NullableIntegers | NullableFloats | NullableStrings | NullableBooleans

# names of types used in the schema
ValueTypeNames: TypeAlias = Literal[
    "string",
    "integer",
    "number",
    "boolean",
    "date-time",
    "duration",
    "time-pattern",
]
IndexValueTypeNames: TypeAlias = Literal["string", "integer", "date-time", "duration", "time-pattern"]

type_map: dict[type, ValueTypeNames] = {
    str: "string",
    int: "integer",
    np.int8: "integer",
    np.int16: "integer",
    np.int32: "integer",
    np.int64: "integer",
    float: "number",
    np.float16: "number",
    np.float32: "number",
    np.float64: "number",
    # np.float128: "number",  # not available on macos
    bool: "boolean",
    np.bool: "boolean",
    datetime: "date-time",
    pd.Timestamp: "date-time",
    timedelta: "duration",
    pd.Timedelta: "duration",
    relativedelta: "duration",
    pd.DateOffset: "duration",
    TimePattern_: "time-pattern",
}


class _TypeInferMixin:
    def __post_init__(self):
        if getattr(self, "value_type") != "any":
            value_type, *_ = set(map(type, getattr(self, "values"))) - {NoneType}
            # NOTE: have to do it like this since inherited dataclasses are frozen
            typename = type_map[value_type]
            super().__setattr__("value_type", typename)


@dataclass(frozen=True)
class RunLengthIndex(_TypeInferMixin):
    """Run length encoded array

    NOTE: this is not supported by PyArrow, if we use it, we will have
    to convert to a supported format.

    """

    name: str
    run_len: Integers
    values: IndexTypes
    value_type: IndexValueTypeNames = field(init=False)
    type: Literal["run_length_index"] = "run_length_index"


@dataclass(frozen=True)
class RunEndIndex(_TypeInferMixin):
    """Run end encoded array"""

    name: str
    run_end: Integers
    values: IndexTypes
    value_type: IndexValueTypeNames = field(init=False)
    type: Literal["run_end_index"] = "run_end_index"


@dataclass(frozen=True)
class DictEncodedIndex(_TypeInferMixin):
    """Dictionary encoded array"""

    name: str
    indices: Integers
    values: IndexTypes
    value_type: IndexValueTypeNames = field(init=False)
    type: Literal["dict_encoded_index"] = "dict_encoded_index"


@dataclass(frozen=True)
class ArrayIndex(_TypeInferMixin):
    """Any array that is an index, e.g. a sequence, timestamps, labels"""

    name: str
    values: IndexTypes
    value_type: IndexValueTypeNames = field(init=False)
    type: Literal["array_index"] = "array_index"


@dataclass(frozen=True)
class RunLengthArray(_TypeInferMixin):
    """Run length encoded array

    NOTE: this is not supported by PyArrow, if we use it, we will have
    to convert to a supported format.

    """

    name: str
    run_len: Integers
    values: NullableValueTypes
    value_type: ValueTypeNames = field(init=False)
    type: Literal["run_length_array"] = "run_length_array"


@dataclass(frozen=True)
class RunEndArray(_TypeInferMixin):
    """Run end encoded array"""

    name: str
    run_end: Integers
    values: NullableValueTypes
    value_type: ValueTypeNames = field(init=False)
    type: Literal["run_end_array"] = "run_end_array"


@dataclass(frozen=True)
class DictEncodedArray(_TypeInferMixin):
    """Dictionary encoded array"""

    name: str
    indices: NullableIntegers
    values: NullableValueTypes
    value_type: ValueTypeNames = field(init=False)
    type: Literal["dict_encoded_array"] = "dict_encoded_array"


@dataclass(frozen=True)
class Array(_TypeInferMixin):
    """Array"""

    name: str
    values: NullableValueTypes
    value_type: ValueTypeNames = field(init=False)
    type: Literal["array"] = "array"


@dataclass(frozen=True)
class AnyArray(_TypeInferMixin):
    """Array with mixed types"""

    name: str
    values: NullableAnyTypes
    value_type: Literal["any"] = "any"
    type: Literal["any_array"] = "any_array"


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
    indices: NotRequired[list]
    run_end: NotRequired[Integers]
    run_len: NotRequired[Integers]


def dict_to_array(data: ArrayAsDict) -> AllArrays:
    match data["type"]:
        case "array":
            return Array(name=data["name"], values=data["values"])
        case "array_index":
            return ArrayIndex(name=data["name"], values=data["values"])
        case "dict_encoded_array":
            return DictEncodedArray(name=data["name"], indices=data.get("indices", []), values=data["values"])
        case "dict_encoded_index":
            return DictEncodedIndex(name=data["name"], indices=data.get("indices", []), values=data["values"])
        case "run_end_array":
            return RunEndArray(name=data["name"], run_end=data.get("run_end", []), values=data["values"])
        case "run_end_index":
            return RunEndIndex(name=data["name"], run_end=data.get("run_end", []), values=data["values"])
        case "any_array":
            return AnyArray(name=data["name"], values=data["values"])
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
