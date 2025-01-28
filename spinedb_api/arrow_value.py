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


.. note::

  This is highly experimental API.

"""
from collections import defaultdict
import datetime
from typing import SupportsFloat
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


def from_database(db_value, value_type):
    """Parses a database value.

    Args:
        db_value (bytes): binary blob from database
        value_type (string, optional): value type

    Returns:
        Any: parsed value
    """
    if db_value is None:
        return None
    loaded = load_db_value(db_value, value_type)
    if isinstance(loaded, dict):
        return from_dict(loaded, value_type)
    if isinstance(loaded, SupportsFloat) and not isinstance(loaded, bool):
        return float(loaded)
    return loaded


def from_dict(loaded_value, value_type):
    """Converts a value dict to parsed value.

    Args:
        loaded_value (dict): value dict
        value_type (str): value type

    Returns:
        pyarrow.RecordBatch: parsed value
    """
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
        return map_to_struct_array(loaded_value)
    raise NotImplementedError(f"unknown value type {value_type}")


def to_database(parsed_value):
    """Converts parsed value into database value.

    Args:
        parsed_value (Any): parsed value

    Returns:
        tuple: database value and its type
    """
    raise NotImplementedError()


def type_of_loaded(loaded_value):
    """Infer the type of loaded value.

    Args:
        loaded_value (Any): loaded value

    Returns:
        str: value type
    """
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


def map_to_struct_array(loaded_value):
    typed_xs, ys, index_names, depth = crawl_map_uneven(loaded_value)
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
    return pyarrow.RecordBatch.from_arrays(x_arrays + [build_y_array(ys)], names=index_names + ["value"])


def crawl_map_uneven(loaded_value, root_index=None, root_index_names=None):
    if root_index is None:
        root_index = []
        root_index_names = []
    depth = len(root_index) + 1
    typed_xs = []
    ys = []
    max_nested_depth = 0
    index_names = root_index_names + [loaded_value.get("index_name", "x")]
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
                    nested_xs, nested_ys, nested_index_names, nested_depth = crawl_map_uneven(y, index, index_names)
                elif y_type == "time_series":
                    nested_xs, nested_ys, nested_index_names, nested_depth = crawl_time_series(y, index, index_names)
                else:
                    raise RuntimeError(f"unknown nested type {y_type}")
                typed_xs += nested_xs
                ys += nested_ys
                deepest_nested_index_names = collect_nested_index_names(nested_index_names, deepest_nested_index_names)
                max_nested_depth = max(max_nested_depth, nested_depth)
                continue
        typed_xs.append(index)
        ys.append(y)
    index_names = index_names if not deepest_nested_index_names else deepest_nested_index_names
    return typed_xs, ys, index_names, depth if max_nested_depth == 0 else max_nested_depth


def crawl_time_series(loaded_value, root_index=None, root_index_names=None):
    if root_index is None:
        root_index = []
        root_index_names = []
    typed_xs = []
    ys = []
    data = loaded_value["data"]
    if isinstance(data, list) and data and not isinstance(data[0], list):
        loaded_index = loaded_value["index"]
        start = numpy.datetime64(loaded_index.get("start", TIME_SERIES_DEFAULT_START))
        resolution = loaded_index.get("resolution", TIME_SERIES_DEFAULT_RESOLUTION)
        data = zip(time_stamps(start, resolution, len(data)), data)
        for x, y in data:
            index = root_index + [("date_time", x)]
            typed_xs.append(index)
            ys.append(y)
    else:
        if isinstance(data, dict):
            data = data.items()
        for x, y in data:
            index = root_index + [("date_time", datetime.datetime.fromisoformat(x))]
            typed_xs.append(index)
            ys.append(y)
    index_names = root_index_names + [loaded_value.get("index_name", "t")]
    return typed_xs, ys, index_names, len(root_index) + 1


def time_series_resolution(resolution):
    """
    Parses time series resolution string.

    Args:
        resolution (str or list of str): resolution or a list thereof

    Returns:
        list of relativedelta: resolution
    """
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
