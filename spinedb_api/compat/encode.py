"""Encode Python sequences into Array types supported by JSON blobs"""

import enum
from itertools import chain
from types import NoneType
from typing import Any, Sequence, TypeVar

from dateutil.relativedelta import relativedelta
import pandas as pd

from spinedb_api.models import (
    AnyArray,
    Array,
    ArrayIndex,
    RunEndArray,
    RunEndIndex,
    DictEncodedArray,
    DictEncodedIndex,
    TimePattern_,
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


def de_encode(name: str, vals: list, array_t: type[de_t]) -> de_t:
    # not using list(set(...)) to preserve order
    values = list(dict.fromkeys(vals))
    indices = list(map(values.index, vals))
    return array_t(name=name, values=values, indices=indices)


def is_any_w_none(arr: Sequence) -> tuple[bool, bool]:
    all_types = set(map(type, arr))
    has_none = NoneType in all_types
    return len(all_types - {NoneType}) > 1, has_none


def column_to_array(name: str, col: list):
    any_type, has_none = is_any_w_none(col)
    if any_type:
        return AnyArray(name=name, values=col)

    match name, col, has_none:
        case "value", list(), _:
            return Array(name=name, values=values)
        # TODO: separate str, ts, dt offset, and tp
        case _, [float() | bool(), *_], _:
            return Array(name=name, values=values)
        case _, [int(), *_], True:
            return Array(name=name, values=values)
        case _, [int() | pd.Timestamp() | relativedelta() | TimePattern_(), *_], False:
            return ArrayIndex(name=name, values=values)
        case _, [str(), *_], True:
            return de_encode(name, col, DictEncodedArray)
        case _, [str(), *_], False:
            return de_encode(name, col, DictEncodedIndex)
        case _, _, _:
            raise NotImplementedError(f"{name}: unknown column type {type(col[0])} ({has_none=})")
