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
from typing import Any, Sequence, cast

from ..models import TypeNames, type_map, ArrayAsDict


def convert_records_to_columns(recs: list[dict[str, Any]]) -> dict[str, list]:
    nrows = len(recs)
    columns: dict[str, list] = {k: [None] * nrows for k in chain.from_iterable(recs)}
    for i, rec in enumerate(recs):
        for col in rec:
            columns[col][i] = rec[col]
    return columns


def types_w_none(arr: Sequence) -> tuple[list[type], bool]:
    all_types = set(map(type, arr))
    has_none = NoneType in all_types
    return list(all_types - {NoneType}), has_none


_sentinel = enum.Enum("_sentinel", "value")
SENTINEL = _sentinel.value


def _get_value_type(values) -> TypeNames:
    types_, _ = types_w_none(values)
    if len(types_) > 1:
        raise ValueError(f"array has mixed types: {types_}")
    return cast(TypeNames, type_map[types_[0]])


def re_encode(name: str, vals: list) -> ArrayAsDict:
    last = SENTINEL
    values, run_end = [], []
    for idx, val in enumerate(vals, start=1):
        if last != val:
            values.append(val)
            run_end.append(idx)
        else:
            run_end[-1] = idx
        last = val

    res: ArrayAsDict = {
        "name": name,
        "run_end": run_end,
        "values": values,
        "value_type": _get_value_type(values),
        "type": "run_end_array",
    }
    return res


def de_encode(name: str, vals: list) -> ArrayAsDict:
    # not using list(set(...)) to preserve order
    values = list(dict.fromkeys(vals))
    indices = list(map(values.index, vals))

    res: ArrayAsDict = {
        "name": name,
        "indices": indices,
        "values": values,
        "value_type": _get_value_type(values),
        "type": "dict_encoded_array",
    }
    return res
