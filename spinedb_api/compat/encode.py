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
"""Encode Python sequences into Array types supported by JSON blobs"""

from datetime import datetime
import enum
from itertools import chain
from types import NoneType
from typing import Any, Sequence, TypeVar
from dateutil.relativedelta import relativedelta
import pandas as pd
from ..models import (
    AnyArray,
    Array,
    ArrayIndex,
    DictEncodedArray,
    DictEncodedIndex,
    RunEndArray,
    RunEndIndex,
    Table,
    type_map,
)


def convert_records_to_columns(recs: list[dict[str, Any]]) -> dict[str, list]:
    nrows = len(recs)
    columns: dict[str, list] = {k: [None] * nrows for k in chain.from_iterable(recs)}
    for i, rec in enumerate(recs):
        for col in rec:
            columns[col][i] = rec[col]
    return columns


_sentinel = enum.Enum("_sentinel", "value")
SENTINEL = _sentinel.value

re_t = TypeVar("re_t", RunEndArray, RunEndIndex)


def re_encode(name: str, vals: list, array_t: type[re_t]) -> re_t:
    last = SENTINEL
    values, run_end = [], []
    for idx, val in enumerate(vals, start=1):
        if last != val:
            values.append(val)
            run_end.append(idx)
        else:
            run_end[-1] = idx
        last = val
    return array_t(name=name, values=values, run_end=run_end)


de_t = TypeVar("de_t", DictEncodedArray, DictEncodedIndex)


def de_encode(name: str, value_type: str, vals: list, array_t: type[de_t]) -> de_t:
    # not using list(set(...)) to preserve order
    values = list(dict.fromkeys(vals))
    indices = list(map(values.index, vals))
    return array_t(name=name, value_type=value_type, values=values, indices=indices)


def is_any_w_none(arr: Sequence) -> tuple[bool, bool]:
    all_types = set(map(type, arr))
    has_none = NoneType in all_types
    return len(all_types - {NoneType}) > 1, has_none


def to_array(name: str, col: list):
    any_type, has_none = is_any_w_none(col)
    if any_type:
        return AnyArray(name=name, values=col)

    match name, col, has_none:
        case "value", list(), _:
            return Array(name=name, value_type=type_map[type(col[0])], values=col)
        case _, [float(), *_], _:
            return Array(name=name, value_type="float", values=col)
        case _, [bool(), *_], _:
            return Array(name=name, value_type="bool", values=col)
        case _, [int(), *_], True:
            return Array(name=name, value_type="int", values=col)
        case _, [int(), *_], False:
            return ArrayIndex(name=name, value_type="int", values=col)
        case _, [pd.Timestamp() | datetime(), *_], False:
            return ArrayIndex(name=name, value_type="date_time", values=col)
        case _, [relativedelta(), *_], False:
            return ArrayIndex(name=name, value_type="duration", values=col)
        case _, [str(), *_], True:
            return de_encode(name, "str", col, DictEncodedArray)
        case _, [str(), *_], False:
            return de_encode(name, "str", col, DictEncodedIndex)
        case _, _, _:
            raise NotImplementedError(f"{name}: unknown column type {type(col[0])} ({has_none=})")


def to_table(columns: dict[str, list]) -> Table:
    return [to_array(name, col) for name, col in columns.items()]
