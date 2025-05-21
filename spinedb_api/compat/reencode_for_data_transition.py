#!/usr/bin/env python
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "pandas[performance]>=2",
#   "pyarrow>=17",
#   "pydantic>=2",
# ]
# ///

"""Reencode old map type JSON to new table/tables type JSON"""

from argparse import ArgumentParser
from datetime import datetime, timedelta
import json
from pathlib import Path
import re
from typing import Any, Callable, Iterable, TypeAlias
from warnings import warn
import numpy as np
import pandas as pd
from pydantic import RootModel
from rich.pretty import pprint  # noqa: F401, keep for debugging
from spinedb_api.models import Array, ArrayIndex, DictEncodedArray, DictEncodedIndex, Table


def json_loads_ts(json_str: str | bytes):
    return pd.Series(json.loads(json_str)["data"])


# Regex pattern to indentify numerical sequences encoded as string
SEQ_PAT = re.compile(r"^(t|p)([0-9]+)$")
# Regex pattern to identify a number encoded as a string
FREQ_PAT = re.compile("^[0-9]+$")
# Regex pattern to duration strings
DUR_PAT = re.compile(r"([0-9]+) *(Y|M|W|D|h|min|s)")


def _normalise_freq(freq: int | str):
    """Normalise integer/string to frequency.

    The frequency value is as understood by `pandas.Timedelta`.  Note
    that ambiguous values such as month or year are still retained
    with the intention to handle later in the pipeline.

    """
    if isinstance(freq, int):
        return str(freq) + "min"
    if FREQ_PAT.match(freq):
        # If frequency is an integer, the implied unit is "minutes"
        return freq + "min"
    # not very robust yet
    return (
        freq.replace("years", "Y")
        .replace("year", "Y")
        .replace("months", "M")
        .replace("month", "M")
        .replace("weeks", "W")
        .replace("week", "W")
        .replace("days", "D")
        .replace("day", "D")
        .replace("hours", "h")
        .replace("hour", "h")
        .replace("minutes", "min")
        .replace("minute", "min")
        .replace("seconds", "s")
        .replace("second", "s")
    )


_to_numpy_time_units = {
    "Y": "Y",
    "M": "M",
    "W": "W",
    "D": "D",
    "h": "h",
    "min": "m",
    "s": "s",
}


def _low_res_datetime(start: str, freq: str, periods: int) -> pd.DatetimeIndex:
    """Create pd.DatetimeIndex with lower time resolution.

    The default resolution of pd.date_time is [ns], which puts
    boundaries on allowed start- and end-dates due to limited storage
    capacity. Choosing a resolution of [s] instead opens up that range
    considerably.

    "For nanosecond resolution, the time span that can be represented
    using a 64-bit integer is limited to approximately 584 years."  -
    https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timestamp-limitations

    You can check the available ranges with `pd.Timestamp.min` and
    `pd.Timestamp.max`.

    """
    if re_match := DUR_PAT.match(_normalise_freq(freq)):
        number_str, unit = re_match.groups()
    else:
        raise ValueError(f"invalid frequency: {freq!r}")

    start_date_np = np.datetime64(start, "s")
    freq_np = np.timedelta64(int(number_str), _to_numpy_time_units[unit])
    freq_pd = pd.Timedelta(freq_np)

    date_array = np.arange(start_date_np, start_date_np + periods * freq_np, freq_np)
    date_array_with_frequency = pd.DatetimeIndex(date_array, freq=freq_pd, dtype="datetime64[s]")

    return date_array_with_frequency


def _to_dateoffset(val: str) -> pd.DateOffset:
    if (m := DUR_PAT.match(val)) is None:
        raise ValueError(f"{val}: bad duration value")
    num_str, freq = m.groups()
    num = int(num_str)
    match freq:
        case "Y":
            return pd.DateOffset(years=num)
        case "M":
            return pd.DateOffset(months=num)
        case "W":
            return pd.DateOffset(weeks=num)
        case "D":
            return pd.DateOffset(days=num)
        case "h":
            return pd.DateOffset(hours=num)
        case "min":
            return pd.DateOffset(minutes=num)
        case "s":
            return pd.DateOffset(seconds=num)
        case _:
            # should not get here
            raise ValueError(f"{val}: unknown duration")


def _atoi(val: str) -> int | str:
    """Convert string to number if it matches `t0001` or `p2001`.

    If a match is found, also override the name to "time" or "period"
    respectively.

    """
    if m := SEQ_PAT.match(val):
        return int(m.group(2))
    else:
        return val


_FmtIdx: TypeAlias = Callable[[str, str | Any], dict[str, Any]]


def _formatter(index_type: str) -> _FmtIdx:
    """Get a function that formats the values of a name value pair.

    The name is the column name.  The function returned depends on the
    `index_type`.  An unknown `index_type` returns a noop formatter,
    but it also issues a warning.  A noop formatter can be requested
    explicitly by passing the type "noop"; no warning is issued in
    this case.

    Index types:
    ============

    - "date_time" :: converts value to `datetime`

    - "duration" :: converts string to `pandas.DateOffset`; this
      allows for ambiguous units like month or year.

    - "str" :: convert the value to integer if it matches `t0001` or
      `p2002`, and the name to "time" and "period" respectively;
      without a match it is a noop.

    - "float" | "time_pattern" | "noop" :: noop

    - fallback :: noop with a warning

    """
    match index_type:
        case "date_time" | "datetime":
            return lambda name, key: {name: pd.Timestamp(key)}
        case "duration":
            return lambda name, key: {name: _to_dateoffset(_normalise_freq(key))}
        case "str":
            # don't use lambda, can't add type hints
            def _atoi_dict(name: str, val: str) -> dict[str, int | str]:
                return {name: _atoi(val)}

            return _atoi_dict
        case "time_pattern" | "timepattern":
            return lambda name, key: {name: TimePattern(key)}
        case "float" | "noop":
            return lambda name, key: {name: key}
        case _:  # fallback to noop w/ a warning
            warn(f"{index_type}: unknown type, fallback to noop formatter")
            return lambda name, key: {name: key}


def make_records(
    json_doc: dict | int | float | str,
    idx_lvls: dict,
    res: list[dict],
    *,
    lvlname_base: str = "default",
) -> list[dict]:
    """Parse parameter value into a list of records

    Spine db stores parameter_value as JSON.  After the JSON blob has
    been decoded to a Python dict, this function can transform it into
    a list of records (dict) like a table.  These records can then be
    consumed by Pandas to create a dataframe.

    The parsing logic works recursively by traversing depth first.
    Each call incrementally accumulates a cell/level of a record in
    the `idx_lvls` dictionary, once the traversal reaches a leaf node,
    the final record is appended to the list `res`.  The final result
    is also returned by the function, allowing for composition.

    If at any level, the index level name is missing, a default base
    name can be provided by setting a default `lvlname_base`.  The
    level name is derived by concatenating the base name with depth
    level.

    """
    lvlname = lvlname_base + f"{len(idx_lvls)}"

    # NOTE: The private functions below are closures, defined early in
    # the function such that they have the original arguments to
    # `make_records` available to them, but nothing more.  They either
    # help with some computation, raise a warning, or are helpers to
    # append to the result.
    _msg_assert = "for the type checker: rest of the function expects `json_doc` to be a dict"

    def _uniquify_index_name(default: str) -> str:
        assert isinstance(json_doc, dict), _msg_assert
        index_name = json_doc.get("index_name", default)
        return index_name + f"{len(idx_lvls)}" if index_name in idx_lvls else index_name

    def _from_pairs(data: Iterable[Iterable], fmt: _FmtIdx):
        index_name = _uniquify_index_name(lvlname)
        for key, val in data:
            _lvls = {**idx_lvls, **fmt(index_name, key)}
            make_records(val, _lvls, res, lvlname_base=lvlname_base)

    def _deprecated(var: str, val: Any):
        assert isinstance(json_doc, dict), _msg_assert
        index_name = json_doc.get("index_name", lvlname)
        msg = f"{index_name}: {var}={val} is deprecated, handle in model, defaulting to time index from 0001-01-01."
        warn(msg, DeprecationWarning)

    def _time_index(idx: dict, length: int):
        start = idx.get("start", "0001-01-01T00:00:00")
        resolution = idx.get("resolution", "1h")
        return _low_res_datetime(start=start, freq=resolution, periods=length)

    def _append_arr(arr: Iterable, fmt: _FmtIdx):
        index_name = _uniquify_index_name("i")
        for value in arr:
            res.append({**idx_lvls, **fmt(index_name, value)})

    match json_doc:
        # maps
        case {"data": dict() as data, "type": "map"}:
            # NOTE: is "index_type" mandatory?  In case it's not, we
            # check for it separately, and fallback in a way that
            # raises a warning but doesn't crash; same for the
            # 2-column array variant below.
            index_type = json_doc.get("index_type", "undefined-index_type-in-map")
            _from_pairs(data.items(), _formatter(index_type))
        case {"data": dict() as data, "index_type": index_type}:
            # NOTE: relies on other types not having "index_type";
            # same for the 2-column array variant below.
            _from_pairs(data.items(), _formatter(index_type))
        case {"data": [[_, _], *_] as data, "type": "map"}:
            index_type = json_doc.get("index_type", "undefined-index_type-in-map")
            _from_pairs(data, _formatter(index_type))
        case {"data": [[_, _], *_] as data, "index_type": index_type}:
            _from_pairs(data, _formatter(index_type))
        # time series
        case {"data": dict() as data, "type": "time_series"}:
            _from_pairs(data.items(), _formatter("date_time"))
        case {"data": [[str(), float() | int()], *_] as data, "type": "time_series"}:
            _from_pairs(data, _formatter("date_time"))
        case {
            "data": [float() | int(), *_] as data,
            "type": "time_series",
            "index": dict() as idx,
        }:
            match idx:
                case {"ignore_year": ignore_year}:
                    _deprecated("ignore_year", ignore_year)
                case {"repeat": repeat}:
                    _deprecated("repeat", repeat)

            index = _time_index(idx, len(data))
            _from_pairs(zip(index, data), _formatter("noop"))
        case {"type": "time_series", "data": [float() | int(), *_] as data}:
            msg = "array-like 'time_series' without time-stamps, relies on 'ignore_year' and 'repeat' implicitly"
            warn(msg, DeprecationWarning)
            updated = {**json_doc, "index": {"ignore_year": True, "repeat": True}}
            make_records(updated, idx_lvls, res, lvlname_base=lvlname_base)
        # time_pattern
        case {"type": "time_pattern", "data": dict() as data}:
            _from_pairs(data.items(), _formatter("time_pattern"))
        # arrays
        case {
            "type": "array",
            "value_type": value_type,
            "data": [str() | float() | int(), *_] as data,
        }:
            _append_arr(data, _formatter(value_type))
        case {"type": "array", "data": [float() | int(), *_] as data}:
            _append_arr(data, _formatter("float"))
        # date_time | duration
        case {
            "type": "date_time" | "duration" as data_t,
            "data": str() | int() as data,
        }:
            _fmt = _formatter(data_t)
            res.append({**idx_lvls, **_fmt("value", data)})
        # values
        case int() | float() | str() | bool() as data:
            _fmt = _formatter("noop")
            res.append({**idx_lvls, **_fmt("value", data)})
        case _:
            raise ValueError(f"match not found: {json_doc}")
    return res


def json_loads_multi_dim_ts(json_str: str | bytes):
    recs = make_records(json.loads(json_str), {}, [])
    ts = pd.DataFrame((r.values() for r in recs), columns=recs[0].keys())
    # ts["time"] = ts["time"].str.strip("t").astype(int)
    # ts["period"] = ts["period"].str.strip("p").astype(int)

    # assuming last column as value column
    *idx_cols, value_col = ts.columns.to_list()
    ts = ts.astype({col: "category" for col, _ in ts.dtypes.items() if col != value_col}).set_index(idx_cols)[value_col]
    return ts


def to_df(json_doc: dict):
    data = make_records(json_doc, {}, [])
    # NOTE: don't use pyarrow, difficult to support mixed types
    # tbl = pa.Table.from_pylist(data)
    # df = tbl.to_pandas(types_mapper=pd.ArrowDtype)
    df = pd.DataFrame.from_records(data)
    return df


class _sentinel:
    pass


SENTINEL = _sentinel()


def series_to_col(
    col: pd.Series,
) -> ArrayIndex | DictEncodedIndex | Array | DictEncodedArray:
    match col.name, col.dtype.type:
        case "value", t if issubclass(t, str) or t in (object, np.object_):
            col = col.astype("category")
            return DictEncodedArray(
                name=col.name,
                values=col.cat.categories,
                indices=col.cat.codes,
            )
        case "value", t if issubclass(t, int):
            return Array(name=col.name, values=col.to_list())
        case _, t if issubclass(t, (bool, float, bytes)):
            return Array(name=col.name, values=col.to_list())
        case _, t if issubclass(t, str) or t in (object, np.object_):
            col = col.astype("category")
            return DictEncodedIndex(
                name=col.name,
                values=col.cat.categories.to_list(),
                indices=col.cat.codes.to_list(),
            )
        case _, t if issubclass(t, (int, datetime, timedelta)) or t is object:
            return ArrayIndex(name=col.name, values=col.to_list())
        case n, t:
            raise NotImplementedError(f"{n}: unknown type {t}")


def to_tables(df: pd.DataFrame) -> Table:
    if df.empty:
        return []
    return [series_to_col(col) for _, col in df.items()]


def transition_data(old_json_bytes: str) -> str:
    df = to_df(json.loads(old_json_bytes))
    tbls = to_tables(df)
    new_json_bytes = RootModel[Table](tbls).model_dump_json().encode()

    return new_json_bytes


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser(__doc__)
    parser.add_argument("old_json")
    parser.add_argument("new_json")
    opts = parser.parse_args()

    df = to_df(json.loads(Path(opts.old_json).read_text()))
    tbls = to_tables(df)
    # NOTE: using pydantic is optional here, we can also write our own
    # JSON serialisation if we want, probably all we need is `asdict(...)`.
    json_blob = RootModel[Table](tbls).model_dump_json(indent=2)
    Path(opts.new_json).write_text(json_blob)
