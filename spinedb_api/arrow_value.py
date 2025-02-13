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
"""
Apache Arrow - Spine interoperability layer.


.. warning::

  This is highly experimental API.

"""
from collections import defaultdict
from collections.abc import Callable, Iterable
import datetime
from typing import Any, Optional, SupportsFloat, Union
from dateutil import relativedelta
import numpy
import pyarrow
from .parameter_value import (
    NUMPY_DATETIME_DTYPE,
    TIME_SERIES_DEFAULT_RESOLUTION,
    TIME_SERIES_DEFAULT_START,
    ParameterValueFormatError,
    duration_to_relativedelta,
    load_db_value,
)

_DATA_TYPE_TO_ARROW_TYPE = {
    "date_time": pyarrow.timestamp("s"),
    "duration": pyarrow.duration("us"),
    "float": pyarrow.float64(),
    "str": pyarrow.string(),
    "null": pyarrow.null(),
}

_ARROW_TYPE_TO_DATA_TYPE = dict(zip(_DATA_TYPE_TO_ARROW_TYPE.values(), _DATA_TYPE_TO_ARROW_TYPE.keys()))

_DATA_CONVERTER = {
    "date_time": lambda data: numpy.array(data, dtype="datetime64[s]"),
}


def from_database(db_value: bytes, value_type: str) -> Any:
    """Parses a database value."""
    if db_value is None:
        return None
    loaded = load_db_value(db_value, value_type)
    if isinstance(loaded, dict):
        return from_dict(loaded, value_type)
    if isinstance(loaded, SupportsFloat) and not isinstance(loaded, bool):
        return float(loaded)
    return loaded


def from_dict(loaded_value: dict, value_type: str) -> pyarrow.RecordBatch:
    """Converts a value dict to parsed value."""
    if value_type == "array":
        data_type = loaded_value.get("value_type", "float")
        data = loaded_value["data"]
        if data_type in _DATA_CONVERTER:
            data = _DATA_CONVERTER[data_type](data)
        arrow_type = _DATA_TYPE_TO_ARROW_TYPE[data_type]
        y_array = pyarrow.array(data, type=arrow_type)
        x_array = pyarrow.array(range(0, len(y_array)), type=pyarrow.int64())
        return pyarrow.RecordBatch.from_arrays([x_array, y_array], names=[loaded_value.get("index_name", "i"), "value"])
    if value_type == "map":
        return crawled_to_record_batch(crawl_map_uneven, loaded_value)
    if value_type == "time_series":
        return crawled_to_record_batch(crawl_time_series, loaded_value)
    raise NotImplementedError(f"unknown value type {value_type}")


def to_database(parsed_value: Any) -> tuple[bytes, str]:
    """Converts parsed value into database value."""
    raise NotImplementedError()


def type_of_loaded(loaded_value: Any) -> str:
    """Infer the type of loaded value."""
    if isinstance(loaded_value, dict):
        return loaded_value["type"]
    elif isinstance(loaded_value, str):
        return "str"
    elif isinstance(loaded_value, bool):
        return "bool"
    elif isinstance(loaded_value, SupportsFloat):
        return "float"
    elif isinstance(loaded_value, datetime.datetime):
        return "date_time"
    elif loaded_value is None:
        return "null"
    raise RuntimeError(f"unknown type")


CrawlTuple = tuple[list, list, list, dict[str, dict[str, str]], int]


def crawled_to_record_batch(
    crawl: Callable[[dict, Optional[list[tuple[str, Any]]], Optional[list[str]]], CrawlTuple], loaded_value: dict
) -> pyarrow.RecordBatch:
    typed_xs, ys, index_names, index_metadata, depth = crawl(loaded_value)
    if not ys:
        return pyarrow.RecordBatch.from_arrays(
            [
                pyarrow.array([], _DATA_TYPE_TO_ARROW_TYPE[loaded_value["index_type"]]),
                pyarrow.array([], pyarrow.null()),
            ],
            names=index_names + ["value"],
        )
    x_arrays = []
    for i in range(depth):
        x_arrays.append(build_x_array(typed_xs, i))
    arrays = x_arrays + [build_y_array(ys)]
    array_names = index_names + ["value"]
    return pyarrow.RecordBatch.from_arrays(arrays, schema=make_schema(arrays, array_names, index_metadata))


def make_schema(
    arrays: Iterable[pyarrow.Array], array_names: Iterable[str], array_metadata: dict[str, dict[str, str]]
) -> pyarrow.Schema:
    fields = []
    for array, name in zip(arrays, array_names):
        fields.append(pyarrow.field(name, array.type, metadata=array_metadata.get(name)))
    return pyarrow.schema(fields)


def crawl_map_uneven(
    loaded_value: dict, root_index: Optional[list[tuple[str, Any]]] = None, root_index_names: Optional[list[str]] = None
) -> CrawlTuple:
    if root_index is None:
        root_index = []
        root_index_names = []
    depth = len(root_index) + 1
    typed_xs = []
    ys = []
    max_nested_depth = 0
    index_names = root_index_names + [loaded_value.get("index_name", f"col_{depth}")]
    index_metadata = {}
    deepest_nested_index_names = []
    index_type = loaded_value["index_type"]
    data = loaded_value["data"]
    if isinstance(data, dict):
        data = data.items()
    for x, y in data:
        index = root_index + [(index_type, x)]
        if isinstance(y, dict):
            y_is_scalar = False
            y_type = y["type"]
            if y_type == "date_time":
                y = datetime.datetime.fromisoformat(y["data"])
                y_is_scalar = True
            if not y_is_scalar:
                if y_type == "map":
                    crawl_nested = crawl_map_uneven
                elif y_type == "time_series":
                    crawl_nested = crawl_time_series
                else:
                    raise RuntimeError(f"unknown nested type {y_type}")
                nested_xs, nested_ys, nested_index_names, nested_index_metadata, nested_depth = crawl_nested(
                    y, index, index_names
                )
                typed_xs += nested_xs
                ys += nested_ys
                deepest_nested_index_names = collect_nested_index_names(nested_index_names, deepest_nested_index_names)
                index_metadata.update(nested_index_metadata)
                max_nested_depth = max(max_nested_depth, nested_depth)
                continue
        typed_xs.append(index)
        ys.append(y)
    index_names = index_names if not deepest_nested_index_names else deepest_nested_index_names
    return typed_xs, ys, index_names, index_metadata, depth if max_nested_depth == 0 else max_nested_depth


def crawl_time_series(
    loaded_value: dict, root_index: Optional[list[tuple[str, Any]]] = None, root_index_names: Optional[list[str]] = None
) -> CrawlTuple:
    if root_index is None:
        root_index = []
        root_index_names = []
    typed_xs = []
    ys = []
    data = loaded_value["data"]
    index_name = loaded_value.get("index_name", "t")
    if isinstance(data, list) and data and not isinstance(data[0], list):
        loaded_index = loaded_value.get("index", {})
        start = numpy.datetime64(loaded_index.get("start", TIME_SERIES_DEFAULT_START))
        resolution = loaded_index.get("resolution", TIME_SERIES_DEFAULT_RESOLUTION)
        data = zip(time_stamps(start, resolution, len(data)), data)
        for x, y in data:
            index = root_index + [("date_time", x)]
            typed_xs.append(index)
            ys.append(y)
        ignore_year = loaded_index.get("ignore_year", False)
        repeat = loaded_index.get("repeat", False)
    else:
        if isinstance(data, dict):
            data = data.items()
        for x, y in data:
            index = root_index + [("date_time", datetime.datetime.fromisoformat(x))]
            typed_xs.append(index)
            ys.append(y)
        ignore_year = False
        repeat = False
    metadata = {
        index_name: {
            "ignore_year": "true" if ignore_year else "false",
            "repeat": "true" if repeat else "false",
        }
    }
    index_names = root_index_names + [index_name]
    return typed_xs, ys, index_names, metadata, len(root_index) + 1


def time_series_resolution(resolution: Union[str, list[str]]) -> list[relativedelta]:
    """Parses time series resolution string."""
    if isinstance(resolution, str):
        resolution = [duration_to_relativedelta(resolution)]
    else:
        resolution = list(map(duration_to_relativedelta, resolution))
    if not resolution:
        raise ParameterValueFormatError("Resolution cannot be empty or zero.")
    return resolution


def time_stamps(start, resolution, count):
    resolution_as_deltas = time_series_resolution(resolution)
    cycle_count = -(-count // len(resolution_as_deltas))
    deltas = [start.tolist()] + (cycle_count * resolution_as_deltas)[: count - 1]
    np_deltas = numpy.array(deltas)
    return np_deltas.cumsum().astype(NUMPY_DATETIME_DTYPE)


def collect_nested_index_names(index_names1, index_names2):
    if len(index_names1) > len(index_names2):
        longer = index_names1
    else:
        longer = index_names2
    for name1, name2 in zip(index_names1, index_names2):
        if name1 != name2:
            raise RuntimeError(f"index name mismatch")
    return longer


def build_x_array(uneven_data, i):
    by_type = defaultdict(list)
    types_and_offsets = []
    for row in uneven_data:
        try:
            data_type, x = row[i]
        except IndexError:
            x = None
            data_type = "null"
        x_list = by_type[data_type]
        x_list.append(x)
        types_and_offsets.append((data_type, len(x_list) - 1))
    return union_array(by_type, types_and_offsets)


def build_y_array(y_list):
    by_type = defaultdict(list)
    types_and_offsets = []
    for y in y_list:
        data_type = type_of_loaded(y)
        y_list = by_type[data_type]
        y_list.append(y)
        types_and_offsets.append((data_type, len(y_list) - 1))
    return union_array(by_type, types_and_offsets)


def union_array(by_type, types_and_offsets):
    if len(by_type) == 1:
        data_type, data = next(iter(by_type.items()))
        if data_type in _DATA_CONVERTER:
            data = _DATA_CONVERTER[data_type](data)
        return pyarrow.array(data, type=_DATA_TYPE_TO_ARROW_TYPE[data_type])
    arrays = []
    for type_, ys in by_type.items():
        if type_ in _DATA_CONVERTER:
            ys = _DATA_CONVERTER[type_](ys)
        arrow_type = _DATA_TYPE_TO_ARROW_TYPE[type_]
        array = pyarrow.array(ys, type=arrow_type)
        arrays.append(array)
    type_index = {y_type: i for i, y_type in enumerate(by_type)}
    type_ids = []
    value_offsets = []
    for type_, offset in types_and_offsets:
        type_ids.append(type_index[type_])
        value_offsets.append(offset)
    types = pyarrow.array(type_ids, type=pyarrow.int8())
    offsets = pyarrow.array(value_offsets, type=pyarrow.int32())
    return pyarrow.UnionArray.from_dense(types, offsets, arrays, field_names=list(by_type))
