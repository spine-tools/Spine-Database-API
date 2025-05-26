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
from typing import Annotated, Literal, TypeAlias
from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
from pydantic.dataclasses import Field as field
from pydantic.dataclasses import dataclass
from pydantic.types import StringConstraints

Floats: TypeAlias = list[float]
Integers: TypeAlias = list[int]
Strings: TypeAlias = list[str]
Booleans: TypeAlias = list[bool]

Datetimes: TypeAlias = list[datetime]
Timedeltas: TypeAlias = list[timedelta]

time_pat_re = r"(Y|M|D|WD|h|m|s)[0-9]+-[0-9]+"
TimePattern: TypeAlias = Annotated[str, StringConstraints(pattern=time_pat_re)]
TimePatterns: TypeAlias = list[TimePattern]

# nullable variant of arrays
NullableIntegers: TypeAlias = list[int | None]
NullableFloats: TypeAlias = list[float | None]
NullableStrings: TypeAlias = list[str | None]
NullableBooleans: TypeAlias = list[bool | None]
NullableDatetimes: TypeAlias = list[datetime | None]
NullableTimedeltas: TypeAlias = list[timedelta | None]
NullableTimePatterns: TypeAlias = list[TimePattern | None]

# sets of types used to define array schemas below
IndexTypes: TypeAlias = Integers | Strings | Datetimes | Timedeltas | TimePatterns
ValueTypes: TypeAlias = Integers | Strings | Floats | Booleans | Datetimes | Timedeltas | TimePatterns
NullableValueTypes: TypeAlias = (
    NullableIntegers
    | NullableStrings
    | NullableFloats
    | NullableBooleans
    | NullableDatetimes
    | NullableTimedeltas
    | NullableTimePatterns
)

# names of types used in the schema
ValueTypeNames: TypeAlias = Literal[
    "string",
    "integer",
    "number",
    "boolean",
    "bytes",
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
    TimePattern: "time-pattern",
    bytes: "string",
}


class _TypeInferMixin:
    def __post_init__(self):
        value_type, *_ = set(map(type, getattr(self, "values"))) - {type(None)}
        # NOTE: have to do it like this since inherited dataclasses are frozen
        super().__setattr__("value_type", type_map[value_type])


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


# NOTE: To add run-length encoding to the schema, add it to the
# following type union following which, we need to implement a
# converter to a compatible pyarrow array type
Table: TypeAlias = list[RunEndIndex | DictEncodedIndex | ArrayIndex | RunEndArray | DictEncodedArray | Array]


if __name__ == "__main__":
    from argparse import ArgumentParser
    import json
    from pathlib import Path
    from pydantic import RootModel

    parser = ArgumentParser(__doc__)
    parser.add_argument("json_file", help="Path of JSON schema file to write")
    opts = parser.parse_args()

    schema = RootModel[Table].model_json_schema()
    Path(opts.json_file).write_text(json.dumps(schema, indent=2))
