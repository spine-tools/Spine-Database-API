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
import datetime
import json
from typing import Any, SupportsFloat, TypeAlias
from dateutil import relativedelta
import pyarrow
from .exception import SpineDBAPIError
from .helpers import time_period_format_specification, time_series_metadata
from .models import AllArrays, ArrayAsDict, Metadata, dict_to_array
from .value_support import load_db_value, to_union_array, validate_time_period

Value: TypeAlias = float | str | bool | datetime.datetime | relativedelta.relativedelta | pyarrow.RecordBatch | None


def from_database(db_value: bytes, value_type: str) -> Value:
    """Parses a database value."""
    if db_value is None:
        return None
    loaded = load_db_value(db_value)
    if isinstance(loaded, list) and len(loaded) > 0 and isinstance(loaded[0], dict):
        return to_record_batch(loaded)
    if isinstance(loaded, SupportsFloat) and not isinstance(loaded, bool):
        return float(loaded)
    return loaded


def with_column_as_time_period(record_batch: pyarrow.RecordBatch, column: int | str) -> pyarrow.RecordBatch:
    """Creates a shallow copy of record_batch with additional metadata marking a column's data type as time_period.

    Also, validates that the column contains strings compatible with the time period specification.
    """
    for period in record_batch.column(column):
        validate_time_period(period.as_py())
    return with_field_metadata(time_period_format_specification(), record_batch, column)


def with_column_as_time_stamps(
    record_batch: pyarrow.RecordBatch, column: int | str, ignore_year: bool, repeat: bool
) -> pyarrow.RecordBatch:
    if not pyarrow.types.is_timestamp(record_batch.column(column).type):
        raise SpineDBAPIError("column is not time stamp column")
    return with_field_metadata(time_series_metadata(ignore_year, repeat), record_batch, column)


def with_field_metadata(
    metadata: Metadata | dict[str, Any], record_batch: pyarrow.RecordBatch, column: int | str
) -> pyarrow.RecordBatch:
    column_i = column if isinstance(column, int) else record_batch.column_names.index(column)
    new_fields = []
    for i in range(record_batch.num_columns):
        field = record_batch.field(i)
        if i == column_i:
            field = field.with_metadata({key: json.dumps(value) for key, value in metadata.items()})
        new_fields.append(field)
    return pyarrow.record_batch(record_batch.columns, schema=pyarrow.schema(new_fields))


def load_field_metadata(field: pyarrow.Field) -> dict[str, Any] | None:
    metadata = field.metadata
    if metadata is None:
        return None
    return {key.decode(): json.loads(value) for key, value in metadata.items()}


def to_record_batch(loaded_value: list[ArrayAsDict]) -> pyarrow.RecordBatch:
    columns = list(map(dict_to_array, loaded_value))
    arrow_columns = {column.name: to_arrow(column) for column in columns}
    record_batch = pyarrow.record_batch(arrow_columns)
    for column in columns:
        if column.metadata:
            record_batch = with_field_metadata(column.metadata, record_batch, column.name)
    return record_batch


def to_arrow(column: AllArrays) -> pyarrow.Array:
    match column.type:
        case "array" | "array_index":
            return pyarrow.array(column.values)
        case "dict_encoded_array" | "dict_encoded_index":
            return pyarrow.DictionaryArray.from_arrays(column.indices, column.values)
        case "run_end_array" | "run_end_index":
            return pyarrow.RunEndEncodedArray.from_arrays(column.run_end, column.values)
        case "any_array":
            return to_union_array(column.values)
        case _:
            raise NotImplementedError(f"{column.type}: column type")
