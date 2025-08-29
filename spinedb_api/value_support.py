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
from collections import defaultdict
from collections.abc import Sequence
import json
import re
from typing import Any, Literal, Optional, TypeAlias
import pyarrow
from .exception import ParameterValueFormatError

ArrowTypeNames: TypeAlias = Literal[
    "str",
    "int",
    "float",
    "bool",
    "date_time",
    "duration",
    "null",
]
JSONValue = bool | float | str | list | dict

_INTERVAL_REGEXP = re.compile(r"(Y|M|D|WD|h|m|s)")


def load_db_value(db_value: bytes) -> Optional[JSONValue]:
    """
    Parses a binary blob into a JSON object.

    If the result is a dict, adds the "type" property to it.

    :meta private:

    Args:
        db_value: The binary blob.

    Returns:
        The parsed parameter value.
    """
    if db_value is None:
        return None
    try:
        parsed = json.loads(db_value)
    except json.JSONDecodeError as err:
        raise ParameterValueFormatError(f"Could not decode the value: {err}") from err
    return parsed


def validate_time_period(time_period: str) -> None:
    """
    Checks if a time period has the right format.

    Args:
        time_period: The time period to check. Generally assumed to be a union of interval intersections.

    Raises:
        ParameterValueFormatError: If the given string doesn't comply with time period spec.
    """
    union_dlm = ","
    intersection_dlm = ";"
    range_dlm = "-"
    for intersection_str in time_period.split(union_dlm):
        for interval_str in intersection_str.split(intersection_dlm):
            m = _INTERVAL_REGEXP.match(interval_str)
            if m is None:
                raise ParameterValueFormatError(
                    f"Invalid interval {interval_str}, it should start with either Y, M, D, WD, h, m, or s."
                )
            key = m.group(0)
            lower_upper_str = interval_str[len(key) :]
            lower_upper = lower_upper_str.split(range_dlm)
            if len(lower_upper) != 2:
                raise ParameterValueFormatError(
                    f"Invalid interval bounds {lower_upper_str}, it should be two integers separated by dash (-)."
                )
            lower_str, upper_str = lower_upper
            try:
                lower = int(lower_str)
            except Exception as error:
                raise ParameterValueFormatError(f"Invalid lower bound {lower_str}, must be an integer.") from error
            try:
                upper = int(upper_str)
            except Exception as error:
                raise ParameterValueFormatError(f"Invalid upper bound {upper_str}, must be an integer.") from error
            if lower > upper:
                raise ParameterValueFormatError(f"Lower bound {lower} can't be higher than upper bound {upper}.")


def to_union_array(arr: Sequence[Any | None]):
    type_map = defaultdict(list)
    offsets = []
    for item in arr:
        item_t = type(item)
        offsets.append(len(type_map[item_t]))
        type_map[item_t].append(item)

    _types = list(type_map)
    types = pyarrow.array((_types.index(type(i)) for i in arr), type=pyarrow.int8())
    uarr = pyarrow.UnionArray.from_dense(
        types,
        pyarrow.array(offsets, type=pyarrow.int32()),
        list(map(pyarrow.array, type_map.values())),
    )
    return uarr
