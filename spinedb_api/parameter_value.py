######################################################################################################################
# Copyright (C) 2017 - 2019 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Support utilities and classes to deal with Spine data (relationship)
parameter values.

The `from_database` function reads the database's value format returning
a float, Datatime, Duration, TimePattern, TimeSeriesFixedResolution
TimeSeriesVariableResolution or Map objects.

The above objects can be converted back to the database format by the `to_database` free function
or by their `to_database` member functions.

Individual datetimes are represented as datetime objects from the standard Python library.
Individual time steps are represented as relativedelta objects from the dateutil package.
Datetime indexes (as returned by TimeSeries.indexes()) are represented as
numpy.ndarray arrays holding numpy.datetime64 objects.

:author: A. Soininen (VTT)
:date:   3.6.2019
"""

from collections.abc import Iterable, Sequence
from copy import copy
from datetime import datetime
import dateutil.parser
from dateutil.relativedelta import relativedelta
import json
from json.decoder import JSONDecodeError
from numbers import Number
import re
import numpy as np
from .exception import ParameterValueFormatError


# Defaulting to seconds precision in numpy.
_NUMPY_DATETIME_DTYPE = "datetime64[s]"
_NUMPY_DATETIME64_UNIT = "s"
# Default start time guess, actual value not currently given in the JSON specification.
_TIME_SERIES_DEFAULT_START = "0001-01-01T00:00:00"
# Default resolution if it is omitted from the index entry.
_TIME_SERIES_DEFAULT_RESOLUTION = "1h"
# Default unit if resolution is given as a number instead of a string.
_TIME_SERIES_PLAIN_INDEX_UNIT = "m"


def duration_to_relativedelta(duration):
    """
    Converts a duration to a relativedelta object.

    Args:
        duration (str): a duration specification

    Returns:
        a relativedelta object corresponding to the given duration
    """
    try:
        count, abbreviation, full_unit = re.split("\\s|([a-z]|[A-Z])", duration, maxsplit=1)
        count = int(count)
    except ValueError:
        raise ParameterValueFormatError(f'Could not parse duration "{duration}"')
    unit = abbreviation if abbreviation is not None else full_unit
    if unit in ["s", "second", "seconds"]:
        return relativedelta(seconds=count)
    if unit in ["m", "minute", "minutes"]:
        return relativedelta(minutes=count)
    if unit in ["h", "hour", "hours"]:
        return relativedelta(hours=count)
    if unit in ["D", "day", "days"]:
        return relativedelta(days=count)
    if unit in ["M", "month", "months"]:
        return relativedelta(months=count)
    if unit in ["Y", "year", "years"]:
        return relativedelta(years=count)
    raise ParameterValueFormatError(f'Could not parse duration "{duration}"')


def relativedelta_to_duration(delta):
    """
    Converts a relativedelta to duration.

    Args:
        delta (relativedelta): the relativedelta to convert

    Returns:
        a duration string
    """
    if delta.seconds > 0:
        seconds = delta.seconds
        seconds += 60 * delta.minutes
        seconds += 60 * 60 * delta.hours
        seconds += 60 * 60 * 24 * delta.days
        # Skipping months and years since dateutil does not use them here
        # and they wouldn't make much sense anyway.
        return f"{seconds}s"
    if delta.minutes > 0:
        minutes = delta.minutes
        minutes += 60 * delta.hours
        minutes += 60 * 24 * delta.days
        return f"{minutes}m"
    if delta.hours > 0:
        hours = delta.hours
        hours += 24 * delta.days
        return f"{hours}h"
    if delta.days > 0:
        return f"{delta.days}D"
    if delta.months > 0:
        months = delta.months
        months += 12 * delta.years
        return f"{months}M"
    if delta.years > 0:
        return f"{delta.years}Y"
    return "0h"


def from_database(database_value):
    """
    Converts a (relationship) parameter value from its database representation to a Python object.

    Args:
        database_value (str): a value in the database; a JSON string or None

    Returns:
        the encoded (relationship) parameter value
    """
    if database_value is None:
        return None
    try:
        value = json.loads(database_value)
    except JSONDecodeError as err:
        raise ParameterValueFormatError(f"Could not decode the value: {err}")
    if isinstance(value, dict):
        return _from_dict(value)
    if isinstance(value, bool):
        return value
    if isinstance(value, Number):
        return float(value)
    return value

def to_database(value):
    """
    Converts a value object into its database representation.

    Args:
        value: a value to convert

    Returns:
        value's database representation as a string
    """
    if hasattr(value, "to_database"):
        return value.to_database()
    return json.dumps(value)


def _from_dict(value_dict):
    """
    Converts complex a (relationship) parameter value from its dictionary representation to a Python object.

    Args:
        value_dict (dict): value's dictionary; a parsed JSON object

    Returns:
        the encoded (relationship) parameter value
    """
    try:
        value_type = value_dict["type"]
        if value_type == "date_time":
            return _datetime_from_database(value_dict["data"])
        if value_type == "duration":
            return _duration_from_database(value_dict["data"])
        if value_type == "map":
            return _map_from_database(value_dict)
        if value_type == "time_pattern":
            return _time_pattern_from_database(value_dict)
        if value_type == "time_series":
            return _time_series_from_database(value_dict)
        if value_type == "array":
            return _array_from_database(value_dict)
        raise ParameterValueFormatError(f'Unknown parameter value type "{value_type}"')
    except KeyError as error:
        raise ParameterValueFormatError(f'"{error.args[0]}" is missing in the parameter value description')


def _break_dictionary(data):
    """Converts {"index": value} style dictionary into (list(indexes), numpy.ndarray(values)) tuple."""
    indexes = list()
    values = np.empty(len(data))
    for index, (key, value) in enumerate(data.items()):
        indexes.append(key)
        values[index] = value
    return indexes, values


def _datetime_from_database(value):
    """Converts a datetime database value into a DateTime object."""
    try:
        stamp = dateutil.parser.parse(value)
    except ValueError:
        raise ParameterValueFormatError(f'Could not parse datetime from "{value}"')
    return DateTime(stamp)


def _duration_from_database(value):
    """Converts a duration database value into a Duration object."""
    if isinstance(value, (str, int)):
        # Set default unit to minutes if value is a plain number.
        if not isinstance(value, str):
            value = f"{value}m"
        value = [duration_to_relativedelta(value)]
    elif isinstance(value, Sequence):  # It is a list of durations.
        # Set default unit to minutes for plain numbers in value.
        value = [v if isinstance(v, str) else f"{v}m" for v in value]
        value = [duration_to_relativedelta(v) for v in value]
    else:
        raise ParameterValueFormatError("Duration value is of unsupported type")
    return Duration(value)


def _time_series_from_database(value):
    """Converts a time series database value into a time series object."""
    data = value["data"]
    if isinstance(data, dict):
        return _time_series_from_dictionary(value)
    if isinstance(data, list):
        if isinstance(data[0], Sequence):
            return _time_series_from_two_columns(value)
        return _time_series_from_single_column(value)
    raise ParameterValueFormatError("Unrecognized time series format")


def _variable_resolution_time_series_info_from_index(value):
    """Returns ignore_year and repeat from index if present or their default values."""
    if "index" in value:
        data_index = value["index"]
        try:
            ignore_year = bool(data_index.get("ignore_year", False))
        except ValueError:
            raise ParameterValueFormatError(f'Could not decode ignore_year from "{data_index["ignore_year"]}"')
        try:
            repeat = bool(data_index.get("repeat", False))
        except ValueError:
            raise ParameterValueFormatError(f'Could not decode repeat from "{data_index["repeat"]}"')
    else:
        ignore_year = False
        repeat = False
    return ignore_year, repeat


def _time_series_from_dictionary(value):
    """Converts a dictionary style time series into a TimeSeriesVariableResolution object."""
    data = value["data"]
    stamps = list()
    values = np.empty(len(data))
    for index, (stamp, series_value) in enumerate(data.items()):
        try:
            stamp = np.datetime64(stamp, _NUMPY_DATETIME64_UNIT)
        except ValueError:
            raise ParameterValueFormatError(f'Could not decode time stamp "{stamp}"')
        stamps.append(stamp)
        values[index] = series_value
    stamps = np.array(stamps)
    ignore_year, repeat = _variable_resolution_time_series_info_from_index(value)
    return TimeSeriesVariableResolution(stamps, values, ignore_year, repeat)


def _time_series_from_single_column(value):
    """Converts a compact JSON formatted time series into a TimeSeriesFixedResolution object."""
    if "index" in value:
        value_index = value["index"]
        start = value_index["start"] if "start" in value_index else _TIME_SERIES_DEFAULT_START
        resolution = value_index["resolution"] if "resolution" in value_index else _TIME_SERIES_DEFAULT_RESOLUTION
        if "ignore_year" in value_index:
            try:
                ignore_year = bool(value_index["ignore_year"])
            except ValueError:
                raise ParameterValueFormatError(
                    f'Could not decode ignore_year value "{value_index["ignore_year"]}"'
                )
        else:
            ignore_year = "start" not in value_index
        if "repeat" in value_index:
            try:
                repeat = bool(value_index["repeat"])
            except ValueError:
                raise ParameterValueFormatError(f'Could not decode repeat value "{value_index["ignore_year"]}"')
        else:
            repeat = "start" not in value_index
    else:
        start = _TIME_SERIES_DEFAULT_START
        resolution = _TIME_SERIES_DEFAULT_RESOLUTION
        ignore_year = True
        repeat = True
    if isinstance(resolution, str) or not isinstance(resolution, Sequence):
        # Always work with lists to simplify the code.
        resolution = [resolution]
    relativedeltas = list()
    for duration in resolution:
        if not isinstance(duration, str):
            duration = str(duration) + _TIME_SERIES_PLAIN_INDEX_UNIT
        relativedeltas.append(duration_to_relativedelta(duration))
    try:
        start = dateutil.parser.parse(start)
    except ValueError:
        raise ParameterValueFormatError(f'Could not decode start value "{start}"')
    values = np.array(value["data"])
    return TimeSeriesFixedResolution(start, relativedeltas, values, ignore_year, repeat)


def _time_series_from_two_columns(value):
    """Converts a two column style time series into a TimeSeriesVariableResolution object."""
    data = value["data"]
    stamps = list()
    values = np.empty(len(data))
    for index, element in enumerate(data):
        if not isinstance(element, Sequence) or len(element) != 2:
            raise ParameterValueFormatError("Invalid value in time series array")
        try:
            stamp = np.datetime64(element[0], _NUMPY_DATETIME64_UNIT)
        except ValueError:
            raise ParameterValueFormatError(f'Could not decode time stamp "{element[0]}"')
        stamps.append(stamp)
        values[index] = element[1]
    stamps = np.array(stamps)
    ignore_year, repeat = _variable_resolution_time_series_info_from_index(value)
    return TimeSeriesVariableResolution(stamps, values, ignore_year, repeat)


def _time_pattern_from_database(value):
    """Converts a time pattern database value into a TimePattern object."""
    patterns, values = _break_dictionary(value["data"])
    return TimePattern(patterns, values)


def _map_from_database(value):
    """Converts a map from its database representation to a Map object."""
    index_type = _map_index_type_from_database(value["index_type"])
    data = value["data"]
    if isinstance(data, dict):
        indexes = _map_indexes_from_database(data.keys(), index_type)
        values = _map_values_from_database(data.values())
    elif isinstance(data, Sequence):
        if not data:
            indexes = list()
            values = list()
        else:
            indexes_in_db = list()
            values_in_db = list()
            for row in data:
                if not isinstance(row, Sequence) or len(row) != 2:
                    raise ParameterValueFormatError('"data" is not a nested two column array.')
                indexes_in_db.append(row[0])
                values_in_db.append(row[1])
            indexes = _map_indexes_from_database(indexes_in_db, index_type)
            values = _map_values_from_database(values_in_db)
    else:
        raise ParameterValueFormatError('"data" attribute is not a dict or array.')
    return Map(indexes, values, index_type)


def _map_index_type_from_database(index_type_in_db):
    """Returns the type corresponding to index_type string."""
    index_type = {"str": str, "date_time": DateTime, "duration": Duration, "float": float}.get(index_type_in_db, None)
    if index_type is None:
        raise ParameterValueFormatError(f'Unknown index_type "{index_type_in_db}".')
    return index_type


def _map_index_type_to_database(index_type):
    """Returns the string corresponding to given index type."""
    if issubclass(index_type, str):
        return "str"
    if issubclass(index_type, float):
        return "float"
    if index_type == DateTime:
        return "date_time"
    if index_type == Duration:
        return "duration"
    raise ParameterValueFormatError(f'Unknown index type "{index_type.__name__}".')


def _map_indexes_from_database(indexes_in_db, index_type):
    """Converts map's indexes from their database format."""
    try:
        indexes = [index_type(index) for index in indexes_in_db]
    except ValueError as error:
        raise ParameterValueFormatError(
            f'Failed to read index of type "{_map_index_type_to_database(index_type)}": {error}'
        )
    else:
        return indexes


def _map_index_to_database(index):
    """Converts a single map index to database format."""
    if hasattr(index, "value_to_database_data"):
        return index.value_to_database_data()
    return index


def _map_value_to_database(value):
    """Converts a single map value to database format."""
    if hasattr(value, "to_dict"):
        return value.to_dict()
    return value


def _map_values_from_database(values_in_db):
    """Converts map's values from their database format."""
    if not values_in_db:
        return list()
    values = list()
    for value_in_db in values_in_db:
        value = _from_dict(value_in_db) if isinstance(value_in_db, dict) else value_in_db
        if not isinstance(value, (float, Duration, Map, str, DateTime)):
            raise ParameterValueFormatError(f'Unsupported value type for Map: "{type(value).__name__}".')
        values.append(value)
    return values


def _array_from_database(value_dict):
    """Converts a parsed dict to a Python list."""
    value_type_id = value_dict.get("value_type", "float")
    value_type = {
        "float": float,
        "str": str,
        "date_time": DateTime,
        "duration": Duration,
        "time_period": str
    }.get(value_type_id, None)
    if value_type is None:
        raise ParameterValueFormatError(f'Unsupported value type for Array: "{value_type_id}".')
    try:
        data = [value_type(x) for x in value_dict["data"]]
    except (TypeError, ParameterValueFormatError):
        raise ParameterValueFormatError('Failed to read values for Array.')
    else:
        return Array(data, value_type)


class DateTime:
    """
    A single datetime value.

    Attributes:
        value (DataTime or str or datetime.datetime): a timestamp
    """

    def __init__(self, value=None):
        if value is None:
            value = datetime(year=2000, month=1, day=1)
        elif isinstance(value, str):
            try:
                value = dateutil.parser.parse(value)
            except ValueError:
                raise ParameterValueFormatError(f'Could not parse datetime from "{value}"')
        elif isinstance(value, DateTime):
            value = copy(value._value)
        elif not isinstance(value, datetime):
            raise ParameterValueFormatError(f'"{type(value).__name__}" cannot be converted to DateTime.')
        self._value = value

    def __eq__(self, other):
        """Returns True if other is equal to this object."""
        if not isinstance(other, DateTime):
            return NotImplemented
        return self._value == other._value

    def __hash__(self):
        return hash(self._value)

    def __str__(self):
        return str(self._value)

    def value_to_database_data(self):
        """Returns the database representation of the duration."""
        return self._value.isoformat()

    def to_dict(self):
        """Retturns the database representation of this object."""
        return {"type": "date_time", "data": self.value_to_database_data()}

    def to_database(self):
        """Returns the database representation of this object as JSON."""
        return json.dumps(self.to_dict())

    @property
    def value(self):
        """Returns the value as a datetime object."""
        return self._value


class Duration:
    """
    This class represents a duration in time.

    Durations are always handled as variable durations, that is, as lists of relativedeltas.

    Attributes:
        value (str, relativedelta, list): the time step(s)
    """

    def __init__(self, value=None):
        if value is None:
            value = [relativedelta(hours=1)]
        elif isinstance(value, str):
            value = [duration_to_relativedelta(value)]
        elif isinstance(value, relativedelta):
            value = [value]
        elif isinstance(value, Iterable):
            for index, element in enumerate(value):
                if isinstance(element, str):
                    value[index] = duration_to_relativedelta(element)
        elif isinstance(value, Duration):
            value = copy(value._value)
        else:
            raise ParameterValueFormatError(f'Could not parse duration from "{value}"')
        self._value = value

    def __eq__(self, other):
        """Returns True if other is equal to this object."""
        if not isinstance(other, Duration):
            return NotImplemented
        return self._value == other._value

    def __hash__(self):
        return hash(tuple(self._value))

    def __str__(self):
        return ", ".join(relativedelta_to_duration(delta) for delta in self._value)

    def to_text(self):
        """Returns a comma separated str representation of the duration"""
        return ", ".join(relativedelta_to_duration(delta) for delta in self.value)

    def value_to_database_data(self):
        """Returns the 'data' attribute part of Duration's database representation."""
        if len(self._value) == 1:
            return relativedelta_to_duration(self._value[0])
        return [relativedelta_to_duration(v) for v in self._value]

    def to_dict(self):
        """Returns the database representation of the duration."""
        return {"type": "duration", "data": self.value_to_database_data()}

    def to_database(self):
        """Returns the database representation of the duration as JSON."""
        return json.dumps(self.to_dict())

    @property
    def value(self):
        """Returns the duration as a list of relativedeltas."""
        return self._value


class _Indexes(np.ndarray):
    """
    A subclass of numpy.ndarray that keeps a lookup dictionary from elements to positions.
    Used by methods get_value and set_value of IndexedValue, to avoid something like

        position = indexes.index(element)

    which might be too slow compared to dictionary lookup.
    """

    def __new__(cls, other):
        obj = np.asarray(other).view(cls)
        obj.position_lookup = {index: k for k, index in enumerate(other)}
        return obj

    def __array_finalize__(self, obj):
        if obj is None:
            return
        # pylint: disable=attribute-defined-outside-init
        self.position_lookup = getattr(obj, 'position_lookup', {})

    def __setitem__(self, position, index):
        old_index = self.__getitem__(position)
        self.position_lookup[index] = self.position_lookup.pop(old_index)
        super().__setitem__(position, index)

    def __eq__(self, other):
        return np.all(super().__eq__(other))

    def __bool__(self):
        return np.size(self) != 0


class IndexedValue:
    """
    An abstract base class for indexed values.
    """

    def __init__(self):
        self._indexes = None

    def __len__(self):
        """Returns the number of values."""
        raise NotImplementedError()

    @property
    def indexes(self):
        """Returns the indexes."""
        return self._indexes

    @indexes.setter
    def indexes(self, indexes):
        """Sets the indexes."""
        self._indexes = _Indexes(indexes)

    def to_database(self):
        """Return the database representation of the value."""
        raise NotImplementedError()

    @property
    def values(self):
        """Returns the data values."""
        raise NotImplementedError()

    def get_value(self, index):
        """Returns the value at the given index."""
        pos = self.indexes.position_lookup.get(index)
        if pos is None:
            return None
        return self.values[pos]

    def set_value(self, index, value):
        """Sets the value at the given index."""
        pos = self.indexes.position_lookup.get(index)
        if pos is not None:
            self.values[pos] = value


class Array(IndexedValue):
    """An one dimensional array with zero based indexing."""

    def __init__(self, values, value_type=None):
        """
        Args:
            values (Sequence): array's values
            value_type (Type, optional): array element type; will be deduced from the array if not given
                and defaults to float if ``values`` is empty
        """
        super().__init__()
        if value_type is None:
            value_type = type(values[0]) if values else float
        if any(not isinstance(x, value_type) for x in values):
            raise ParameterValueFormatError("Not all array's values are of the same type.")
        self.indexes = range(len(values))
        self._values = list(values)
        self._value_type = value_type

    def __eq__(self, other):
        if not isinstance(other, Array):
            return NotImplemented
        return self._values == other._values

    def __len__(self):
        """See base class."""
        return len(self._values)

    def index_type(self):
        """Returns"""

    def to_database(self):
        """See base class."""
        value_type_id = {
            float: "float",
            str: "str",  # String could also mean time_period but we don't have any way to distinguish that, yet.
            DateTime: "date_time",
            Duration: "duration"
        }.get(self._value_type)
        if value_type_id is None:
            raise ParameterValueFormatError(f"Cannot write unsupported array value type: {self._value_type.__name__}")
        if value_type_id in ("float", "str"):
            data = self._values
        else:
            data = [x.value_to_database_data() for x in self._values]
        return json.dumps({"type": "array", "value_type": value_type_id, "data": data})

    @property
    def value_type(self):
        """Returns the type of array's elements."""
        return self._value_type

    @property
    def values(self):
        """See base class."""
        return self._values


class IndexedNumberArray(IndexedValue):
    """
    An abstract base class for indexed floats.

    The indexes and numbers are stored in numpy.ndarrays.
    """

    def __init__(self, values):
        """
        Args:
            values (Sequence): array's values; index handling should be implemented by subclasses
        """
        super().__init__()
        if not isinstance(values, np.ndarray) or not values.dtype == np.dtype(float):
            values = np.array(values, dtype=float)
        self._values = values

    def __len__(self):
        """Returns the length of the index"""
        return len(self.values)

    def to_database(self):
        """Return the database representation of the value."""
        raise NotImplementedError()

    @property
    def values(self):
        """Returns the data values as numpy.ndarray."""
        return self._values


class TimeSeries(IndexedNumberArray):
    """
    An abstract base class for time series.

    Attributes:
        values (Sequence): an array of values
        ignore_year (bool): True if the year should be ignored in the time stamps
        repeat (bool): True if the series should be repeated from the beginning
    """

    def __init__(self, values, ignore_year, repeat):
        if len(values) < 1:
            raise ParameterValueFormatError("Time series too short. Must have one or more values")
        super().__init__(values)
        self._ignore_year = ignore_year
        self._repeat = repeat

    @property
    def ignore_year(self):
        """Returns True if the year should be ignored."""
        return self._ignore_year

    @ignore_year.setter
    def ignore_year(self, ignore_year):
        self._ignore_year = bool(ignore_year)

    @property
    def repeat(self):
        """Returns True if the series should be repeated."""
        return self._repeat

    @repeat.setter
    def repeat(self, repeat):
        self._repeat = bool(repeat)

    def to_database(self):
        """Return the database representation of the value."""
        raise NotImplementedError()


class TimePattern(IndexedNumberArray):
    """
    Represents a time pattern (relationship) parameter value.

    Attributes:
        indexes (list): a list of time pattern strings
        values (Sequence): an array of values corresponding to the time patterns
    """

    def __init__(self, indexes, values):
        if len(indexes) != len(values):
            raise ParameterValueFormatError("Length of values does not match length of indexes")
        if not indexes:
            raise ParameterValueFormatError("Empty time pattern not allowed")
        super().__init__(values)
        self.indexes = indexes

    def __eq__(self, other):
        """Returns True if other is equal to this object."""
        if not isinstance(other, TimePattern):
            return NotImplemented
        return self._indexes == other._indexes and np.all(self._values == other._values)

    def to_database(self):
        """Returns the database representation of this time pattern."""
        data = dict()
        for index, value in zip(self._indexes, self._values):
            data[index] = value
        return json.dumps({"type": "time_pattern", "data": data})


class TimeSeriesFixedResolution(TimeSeries):
    """
    A time series with fixed durations between the time stamps.

    When getting the indexes the durations are applied cyclically.

    Currently, there is no support for the `ignore_year` and `repeat` options
    other than having getters for their values.

    Attributes:
        start (str or datetime or datetime64): the first time stamp
        resolution (str, relativedelta, list): duration(s) between the time stamps
        values (Sequence): data values at each time stamp
        ignore_year (bool): whether or not the time-series should apply to any year
        repeat (bool): whether or not the time series should repeat cyclically
    """

    def __init__(self, start, resolution, values, ignore_year, repeat):
        super().__init__(values, ignore_year, repeat)
        self._start = None
        self._resolution = None
        self.start = start
        self.resolution = resolution

    def __eq__(self, other):
        """Returns True if other is equal to this object."""
        if not isinstance(other, TimeSeriesFixedResolution):
            return NotImplemented
        return (
            self._start == other._start
            and self._resolution == other._resolution
            and np.all(self._values == other._values)
            and self._ignore_year == other._ignore_year
            and self._repeat == other._repeat
        )

    @property
    def indexes(self):
        """Returns the time stamps as a numpy.ndarray of numpy.datetime64 objects."""
        if self._indexes is None:
            step_index = 0
            step_cycle_index = 0
            full_cycle_duration = sum(self._resolution, relativedelta())
            stamps = np.empty(len(self), dtype=_NUMPY_DATETIME_DTYPE)
            stamps[0] = self._start
            for stamp_index in range(1, len(self._values)):
                if step_index >= len(self._resolution):
                    step_index = 0
                    step_cycle_index += 1
                current_cycle_duration = sum(self._resolution[: step_index + 1], relativedelta())
                duration_from_start = step_cycle_index * full_cycle_duration + current_cycle_duration
                stamps[stamp_index] = self._start + duration_from_start
                step_index += 1
            self.indexes = np.array(stamps, dtype=_NUMPY_DATETIME_DTYPE)
        return IndexedValue.indexes.fget(self)

    @indexes.setter
    def indexes(self, indexes):
        """Sets the indexes."""
        # Needed because we redefine the setter
        self._indexes = _Indexes(indexes)

    @property
    def start(self):
        """Returns the start index."""
        return self._start

    @start.setter
    def start(self, start):
        """
        Sets the start datetime.

        Args:
            start (datetime or datetime64 or str): the start of the series
        """
        if isinstance(start, str):
            try:
                self._start = dateutil.parser.parse(start)
            except ValueError:
                raise ParameterValueFormatError(f'Cannot parse start time "{start}"')
        elif isinstance(start, np.datetime64):
            self._start = start.tolist()
        else:
            self._start = start
        self._indexes = None

    @property
    def resolution(self):
        """Returns the resolution as list of durations."""
        return self._resolution

    @resolution.setter
    def resolution(self, resolution):
        """
        Sets the resolution.

        Args:
            resolution (str, relativedelta, list): resolution or a list thereof
        """
        if isinstance(resolution, str):
            resolution = [duration_to_relativedelta(resolution)]
        elif not isinstance(resolution, Sequence):
            resolution = [resolution]
        else:
            for i in range(len(resolution)):
                if isinstance(resolution[i], str):
                    resolution[i] = duration_to_relativedelta(resolution[i])
        if not resolution:
            raise ParameterValueFormatError("Resolution cannot be zero.")
        self._resolution = resolution
        self._indexes = None

    def to_database(self):
        """Returns the value in its database representation."""
        if len(self._resolution) > 1:
            resolution_as_json = [relativedelta_to_duration(step) for step in self._resolution]
        else:
            resolution_as_json = relativedelta_to_duration(self._resolution[0])
        return json.dumps(
            {
                "type": "time_series",
                "index": {
                    "start": str(self._start),
                    "resolution": resolution_as_json,
                    "ignore_year": self._ignore_year,
                    "repeat": self._repeat,
                },
                "data": self._values.tolist(),
            }
        )


class TimeSeriesVariableResolution(TimeSeries):
    """
    A class representing time series data with variable time step.

    Attributes:
        indexes (Sequence): time stamps as numpy.datetime64 objects
        values (Sequence): the values corresponding to the time stamps
        ignore_year (bool): True if the stamp year should be ignored
        repeat (bool): True if the series should be repeated from the beginning
    """

    def __init__(self, indexes, values, ignore_year, repeat):
        super().__init__(values, ignore_year, repeat)
        if len(indexes) != len(values):
            raise ParameterValueFormatError("Length of values does not match length of indexes")
        if not isinstance(indexes, np.ndarray):
            date_times = np.empty(len(indexes), dtype=_NUMPY_DATETIME_DTYPE)
            for i, index in enumerate(indexes):
                if isinstance(index, DateTime):
                    date_times[i] = np.datetime64(index.value, _NUMPY_DATETIME64_UNIT)
                else:
                    date_times[i] = np.datetime64(index, _NUMPY_DATETIME64_UNIT)
            indexes = date_times
        self.indexes = indexes

    def __eq__(self, other):
        """Returns True if other is equal to this object."""
        if not isinstance(other, TimeSeriesVariableResolution):
            return NotImplemented
        return (
            self._indexes == other._indexes
            and np.all(self._values == other._values)
            and self._ignore_year == other._ignore_year
            and self._repeat == other._repeat
        )

    def to_database(self):
        """Returns the value in its database representation"""
        database_value = {"type": "time_series"}
        data = dict()
        for index, value in zip(self._indexes, self._values):
            data[str(index)] = float(value)
        database_value["data"] = data
        # Add "index" entry only if its contents are not set to their default values.
        if self._ignore_year:
            if "index" not in database_value:
                database_value["index"] = dict()
            database_value["index"]["ignore_year"] = self._ignore_year
        if self._repeat:
            if "index" not in database_value:
                database_value["index"] = dict()
            database_value["index"]["repeat"] = self._repeat
        return json.dumps(database_value)


class Map(IndexedValue):
    """
    A nested general purpose indexed value.
    """

    def __init__(self, indexes, values, index_type=None):
        """
        Args:
            indexes (Sequence): map's indexes
            values (Sequence): map's values
            index_type (type or NoneType): index type or None to deduce from indexes
        """
        if not indexes and index_type is None:
            raise ParameterValueFormatError("Cannot deduce index type from empty indexes list.")
        if indexes and index_type is not None and not isinstance(indexes[0], index_type):
            raise ParameterValueFormatError('Type of index does not match "index_type" argument.')
        if len(indexes) != len(values):
            raise ParameterValueFormatError("Length of values does not match length of indexes")
        super().__init__()
        self.indexes = indexes
        self._index_type = index_type if index_type is not None else type(indexes[0])
        self._values = values

    def __eq__(self, other):
        if not isinstance(other, Map):
            return NotImplemented
        return other._indexes == self._indexes and other._values == self._values

    def __len__(self):
        """Returns the length of map."""
        return len(self._indexes)

    @property
    def values(self):
        """Map's values."""
        return self._values

    def is_nested(self):
        """Returns True if any of the values is also a map."""
        return any(isinstance(value, Map) for value in self._values)

    def value_to_database_data(self):
        """Returns map's database representation's 'data' dictionary."""
        data = list()
        for index, value in zip(self._indexes, self._values):
            index_in_db = _map_index_to_database(index)
            value_in_db = _map_value_to_database(value)
            data.append([index_in_db, value_in_db])
        return data

    def to_dict(self):
        """Returns map's database representation."""
        return {
            "type": "map",
            "index_type": _map_index_type_to_database(self._index_type),
            "data": self.value_to_database_data(),
        }

    def to_database(self):
        """Return map's database representation as JSON."""
        return json.dumps(self.to_dict())


# List of scalar types that are supported by the spinedb_api
SUPPORTED_TYPES = (Duration, DateTime, float, str)
