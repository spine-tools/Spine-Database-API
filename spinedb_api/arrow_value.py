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
import datetime
from itertools import chain, tee
import json
from typing import Any, Callable, Iterable, Optional, Sequence, SupportsFloat, TypeAlias, TypeVar, Union
from dateutil import relativedelta
import numpy
import pyarrow
from . import parameter_value as legacy_value
from .compat.data_transition import transition_data
from .exception import SpineDBAPIError
from .models import AllArrays, AnyType, ArrayAsDict, SpecialTypeNames, ValueTypeNames, dict_to_array
from .parameter_value import (
    NUMPY_DATETIME_DTYPE,
    TIME_SERIES_DEFAULT_RESOLUTION,
    TIME_SERIES_DEFAULT_START,
    ParameterValueFormatError,
    duration_to_relativedelta,
    load_db_value,
    validate_time_period,
)

_DATA_TYPE_TO_ARROW_TYPE: dict[ValueTypeNames, pyarrow.DataType] = {
    "date_time": pyarrow.timestamp("s"),
    "duration": pyarrow.duration("us"),
    "float": pyarrow.float64(),
    "str": pyarrow.string(),
    "null": pyarrow.null(),
}

_ARROW_TYPE_TO_DATA_TYPE: dict[pyarrow.DataType, ValueTypeNames] = dict(
    zip(_DATA_TYPE_TO_ARROW_TYPE.values(), _DATA_TYPE_TO_ARROW_TYPE.keys())
)

_DATA_CONVERTER = {
    "date_time": lambda data: numpy.array(data, dtype="datetime64[s]"),
}


TABLE_TYPE = "table"

Value: TypeAlias = float | str | bool | datetime.datetime | relativedelta.relativedelta | pyarrow.RecordBatch | None


def from_database(db_value: bytes, value_type: str) -> Value:
    """Parses a database value."""
    if db_value is None:
        return None
    loaded = load_db_value(db_value, value_type)
    if isinstance(loaded, list) and len(loaded) > 0 and isinstance(loaded[0], dict):
        return to_record_batch(loaded)
    if isinstance(loaded, dict):
        return from_dict(loaded, value_type)
    if isinstance(loaded, SupportsFloat) and not isinstance(loaded, bool):
        return float(loaded)
    return loaded


def with_column_as_time_period(record_batch: pyarrow.RecordBatch, column: int | str) -> pyarrow.RecordBatch:
    """Creates a shallow copy of record_batch with additional metadata marking a column's data type as time_period.

    Also, validates that the column contains strings compatible with the time period specification.
    """
    for period in record_batch.column(column):
        validate_time_period(period.as_py())
    column_name = column if isinstance(column, str) else record_batch.column_names[column]
    metadata = {column_name: json.dumps({"format": "time_period"})}
    return record_batch.replace_schema_metadata(metadata)


def to_record_batch(loaded_value: list[ArrayAsDict]) -> pyarrow.RecordBatch:
    metadata = {}
    cols = {col.name: to_arrow(col, metadata) for col in map(dict_to_array, loaded_value)}
    return pyarrow.record_batch(cols, metadata=metadata if metadata else None)


def to_union_array(arr: Sequence[AnyType | None]):
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


def to_arrow(col: AllArrays, metadata: dict) -> pyarrow.Array:
    if col.metadata:
        metadata[col.name] = col.metadata
    match col.type:
        case "array" | "array_index":
            return pyarrow.array(col.values)
        case "dict_encoded_array" | "dict_encoded_index":
            return pyarrow.DictionaryArray.from_arrays(col.indices, col.values)
        case "run_end_array" | "run_end_index":
            return pyarrow.RunEndEncodedArray.from_arrays(col.run_end, col.values)
        case "any_array":
            return to_union_array(col.values)
        case _:
            raise NotImplementedError(f"{col.type}: column type")


def merge_schemas(schemas: Iterable[pyarrow.Schema]) -> pyarrow.Schema:
    # overwrites earlier keys
    s1, s2 = tee(schemas)
    fields = list(dict.fromkeys(chain.from_iterable(s1)))
    metadata = {k: v for k, v in chain.from_iterable(sc.metadata.items() for sc in s2)}
    return pyarrow.schema(fields, metadata)


RecBatchTable_t = TypeVar("RecBatchTable_t", pyarrow.RecordBatch, pyarrow.Table)


def replace_schema(batch: RecBatchTable_t, schema) -> RecBatchTable_t:
    rows = batch.shape[0]
    data = [batch[field.name] if field in batch.schema else pyarrow.nulls(rows).cast(field.type) for field in schema]
    match batch:
        case pyarrow.RecordBatch():
            return pyarrow.record_batch(data, schema=schema)
        case pyarrow.Table():
            return pyarrow.table(data, schema=schema)
        case _:
            raise ValueError(f"{type(batch)}: unknown type")


def concat_w_missing(data: Iterable[RecBatchTable_t]) -> RecBatchTable_t:
    d1, d2, d3 = tee(data, 3)
    item = next(d1)
    schema = merge_schemas(batch.schema for batch in d2)
    match item:
        case pyarrow.RecordBatch():
            return pyarrow.concat_batches(replace_schema(batch, schema) for batch in d3)
        case pyarrow.Table():
            return pyarrow.concat_tables(replace_schema(batch, schema) for batch in d3)
        case _:
            raise ValueError(f"{type(item)}: unknown type")


def from_dict(loaded_value: dict, value_type: str) -> pyarrow.RecordBatch:
    """Converts a value dict to parsed value."""
    match value_type:
        case "array":
            data_type = loaded_value.get("value_type", "float")
            data = loaded_value["data"]
            if data_type in _DATA_CONVERTER:
                data = _DATA_CONVERTER[data_type](data)
            arrow_type = _DATA_TYPE_TO_ARROW_TYPE[data_type]
            y_array = pyarrow.array(data, type=arrow_type)
            x_array = pyarrow.array(range(0, len(y_array)), type=pyarrow.int64())
            return pyarrow.RecordBatch.from_arrays(
                [x_array, y_array], names=[loaded_value.get("index_name", "i"), "value"]
            )
        case "map":
            return crawled_to_record_batch(crawl_map_uneven, loaded_value)
        case "time_series":
            return crawled_to_record_batch(crawl_time_series, loaded_value)
        case _:
            raise NotImplementedError(f"unknown value type {value_type}")


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


def to_database(parsed_value: Value) -> tuple[bytes, Optional[str]]:
    """Converts parsed value into database value."""
    match parsed_value:
        case legacy_value.Map():
            blob, value_type = parsed_value.to_database()
            return transition_data(blob), value_type
        case pyarrow.RecordBatch():
            return json.dumps(to_list(parsed_value)).encode(), TABLE_TYPE
        case None:
            return legacy_value.UNPARSED_NULL_VALUE, None
        case bool():
            return json.dumps(parsed_value).encode(), legacy_value.BOOLEAN_VALUE_TYPE
        case float():
            return json.dumps(parsed_value).encode(), legacy_value.FLOAT_VALUE_TYPE
        case str():
            return json.dumps(parsed_value).encode(), legacy_value.STRING_VALUE_TYPE
        case _:
            raise NotImplementedError("unsupported value type")


def to_list(loaded_value: pyarrow.RecordBatch) -> list[dict]:
    arrays = []
    metadata = loaded_value.schema.metadata
    for i_column, (name, column) in enumerate(zip(loaded_value.column_names, loaded_value.columns)):
        is_value_column = i_column == loaded_value.num_columns - 1
        base_data = {
            "name": name,
        }
        if metadata is not None and (name_bytes := name.encode()) in metadata:
            base_data["metadata"] = metadata[name_bytes].decode()
        match column:
            case pyarrow.RunEndEncodedArray():
                arrays.append(
                    {
                        **base_data,
                        "type": "run_end_array" if is_value_column else "run_end_index",
                        "run_end": column.run_ends.to_pylist(),
                        "values": column.values.to_pylist(),
                        "value_type": _ARROW_TYPE_TO_DATA_TYPE[column.type.value_type],
                    }
                )
            case pyarrow.DictionaryArray():
                arrays.append(
                    {
                        **base_data,
                        "type": "dict_encoded_array" if is_value_column else "dict_encoded_index",
                        "indices": column.indices.to_pylist(),
                        "values": column.dictionary.to_pylist(),
                        "value_type": _ARROW_TYPE_TO_DATA_TYPE[column.type.value_type],
                    }
                )
            case pyarrow.UnionArray():
                if not is_value_column:
                    raise SpineDBAPIError("union array cannot be index")
                value_list, special_types = _union_array_values_to_list(column)
                arrays.append(
                    {
                        **base_data,
                        "type": "any_array",
                        "values": value_list,
                        "value_type": "any",
                        "special_types": special_types,
                    }
                )
            case pyarrow.TimestampArray():
                arrays.append(
                    {
                        **base_data,
                        "type": "array" if is_value_column else "array_index",
                        "values": [t.as_py().isoformat() for t in column],
                        "value_type": "date_time",
                    }
                )
            case pyarrow.MonthDayNanoIntervalArray():
                arrays.append(
                    {
                        **base_data,
                        "type": "array" if is_value_column else "array_index",
                        "values": [_month_day_nano_interval_to_duration(dt) for dt in column],
                        "value_type": "duration",
                    }
                )
            case _:
                arrays.append(
                    {
                        **base_data,
                        "type": "array" if is_value_column else "array_index",
                        "values": column.to_pylist(),
                        "value_type": _array_value_type(column, is_value_column),
                    }
                )
    return arrays


def _union_array_values_to_list(column: pyarrow.UnionArray) -> tuple[list, dict[int, SpecialTypeNames]]:
    values = []
    special_types = {}
    for i, x in enumerate(column):
        match x.value:
            case pyarrow.MonthDayNanoIntervalScalar():
                special_types[i] = "duration"
                values.append(_month_day_nano_interval_to_duration(x))
            case _:
                values.append(x.as_py())
    return values, special_types


def _array_value_type(column: pyarrow.Array, is_value_column: bool) -> str:
    match column:
        case pyarrow.FloatingPointArray():
            return "float"
        case pyarrow.IntegerArray():
            return "int"
        case pyarrow.StringArray() | pyarrow.LargeStringArray():
            return "str"
        case pyarrow.BooleanArray():
            if not is_value_column:
                raise SpineDBAPIError("boolean array cannot be index")
            return "bool"
        case pyarrow.MonthDayNanoIntervalArray():
            if is_value_column:
                raise SpineDBAPIError("duration array cannot be value")
            return "duration"
        case _:
            raise SpineDBAPIError(f"unsupported column type {type(column).__name__}")


_ZERO_DURATION = "P0D"


def _month_day_nano_interval_to_duration(dt: pyarrow.MonthDayNanoIntervalScalar) -> str:
    duration = "P"
    months, days, nanoseconds = dt.as_py()
    years = months // 12
    if years:
        duration = duration + f"{years}Y"
        months -= years * 12
    if months:
        duration = duration + f"{months}M"
    if days:
        duration = duration + f"{days}D"
    if not nanoseconds:
        return duration if duration != "P" else _ZERO_DURATION
    duration = duration + "T"
    seconds = nanoseconds // 1000000000
    hours = seconds // 3600
    if hours:
        duration = duration + f"{hours}H"
        seconds -= hours * 3600
    minutes = seconds // 60
    if minutes:
        duration = duration + f"{minutes}M"
        seconds -= minutes * 60
    if seconds:
        duration += f"{seconds}S"
    return duration if duration != "PT" else _ZERO_DURATION
