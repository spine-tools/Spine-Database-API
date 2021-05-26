######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

from itertools import accumulate, chain, cycle
import json
from json.decoder import JSONDecodeError
from numbers import Number
import dateutil.parser
from dateutil.relativedelta import relativedelta
from spinedb_api.parameter_value import duration_to_relativedelta


class LightParameterValue:
    """Class to parse parameter values from database to dictionaries, but not beyond.

    Used by ExportMapping, to avoid converting from database to one of our complex types just to retrieve a dict in
    the end.
    """

    def __init__(self, db_value, value_type=None):
        """
        Args:
            db_value (str): Value directly from the database
            value_type (str, NoneType): Value type
        """
        self._db_value = db_value
        self._value = None
        self._data = None
        self._dimension_count = None
        self.type = value_type if value_type in ("map", "time_series", "time_pattern", "array") else "single_value"

    @property
    def value(self):
        if self._value is None:
            try:
                self._value = json.loads(self._db_value)
            except (TypeError, JSONDecodeError):
                self._value = None
        return self._value

    @property
    def data(self):
        if self._data is None:
            self._data = _get_value_data(self.value, self.type)
        return self._data

    @property
    def dimension_count(self):
        if self._dimension_count is None:
            self._dimension_count = _get_dimension_count(self.data, self.type)
        return self._dimension_count

    def to_single_value(self):
        if self.type == "single_value":
            return self.data
        return self.type

    def to_dict(self):
        if self.type == "single_value":
            return {None: self.data}
        return _indexed_to_dict(self.data, self.type)

    def similar(self, other):
        return self.type == other.type and self.dimension_count == other.dimension_count


def _array_to_dict(data):
    return dict(enumerate(data))


def _time_pattern_to_dict(data):
    return data


def _time_series_to_dict(data):
    if isinstance(data, dict):
        return data
    return dict(data)


def _non_map_to_dict(data, type_):
    to_dict = {"array": _array_to_dict, "time_pattern": _time_pattern_to_dict, "time_series": _time_series_to_dict}.get(
        type_
    )
    if to_dict is None:
        raise ValueError(f"Unknown value type {type_}")
    return to_dict(data)


def _indexed_to_dict(data, type_):
    if type_ == "single_value":
        return data
    if type_ != "map":
        return _non_map_to_dict(data, type_)
    if isinstance(data, dict):
        data = data.items()
    return {k: _indexed_to_dict(_get_value_data(v), _get_value_type(v)) for k, v in data}


def _get_time_series_data(data, index):
    if isinstance(data, dict):
        return data
    if isinstance(data, list):
        first = next(iter(data), None)
        if first is None:
            return ()
        if isinstance(first, list):
            return data
        start = index.get("start", "0001-01-01T00:00:00")
        try:
            start = dateutil.parser.parse(start)
        except ValueError:
            raise ValueError(f'Invalid time-series start, expected a string in isoformat, got {start}')
        resolution = index.get("resolution", "1h")
        if not isinstance(resolution, list):
            resolution = [resolution]
        resolutions = cycle(map(duration_to_relativedelta, resolution))
        offsets = accumulate(chain((relativedelta(0),), resolutions))
        return {(start + offset).isoformat(): val for offset, val in zip(offsets, data)}
    raise ValueError(f"Invalid time series format, expected JSON array or object, got {data}")


def _get_value_data(value, value_type=None):
    if not isinstance(value, dict):
        if isinstance(value, Number) and not isinstance(value, bool):
            return float(value)
        return value
    try:
        if value_type is None:
            value_type = value["type"]
        if value_type != "time_series":
            return value["data"]
        return _get_time_series_data(value["data"], value.get("index", {}))
    except KeyError as key:
        raise ValueError(f"Invalid parameter value, '{key}' missing: {value}")


def _get_value_type(value):
    if not isinstance(value, dict):
        return "single_value"
    try:
        value_type = value["type"]
    except KeyError:
        raise ValueError(f"Invalid parameter value, 'type' missing: {value}")
    if value_type in ("duration", "date_time"):
        return "single_value"
    return value_type


def _get_dimension_count(data, value_type):
    if value_type == "single_value":
        return 0
    if value_type != "map":
        return 1
    if isinstance(data, dict):
        data = data.items()
    return 1 + max((_get_dimension_count(_get_value_data(v), _get_value_type(v)) for _, v in data), default=0)
