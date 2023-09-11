######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
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

"""

from collections.abc import Sequence
from copy import copy
from datetime import datetime
import json
from json.decoder import JSONDecodeError
from numbers import Number
import re
import dateutil.parser
from dateutil.relativedelta import relativedelta
import numpy as np
from .exception import ParameterValueFormatError

# Defaulting to seconds precision in numpy.
_NUMPY_DATETIME_DTYPE = "datetime64[s]"
NUMPY_DATETIME64_UNIT = "s"
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


def load_db_value(db_value, value_type=None):
    """
    Loads a database parameter value into a Python object using JSON.
    Adds the "type" property to dicts representing complex types.

    Args:
        db_value (bytes, optional): a value in the database
        value_type (str, optional): the type in case of complex ones

    Returns:
        Any: the parsed parameter value
    """
    if db_value is None:
        return None
    try:
        parsed = json.loads(db_value)
    except JSONDecodeError as err:
        raise ParameterValueFormatError(f"Could not decode the value: {err}") from err
    if isinstance(parsed, dict):
        return {"type": value_type, **parsed}
    return parsed


def dump_db_value(parsed_value):
    """
    Dumps a Python object into a database parameter value using JSON.
    Extracts the "type" property from dicts representing complex types.

    Args:
        parsed_value (Any): the Python object

    Returns:
        str: the database parameter value
        str: the type
    """
    value_type = parsed_value.pop("type") if isinstance(parsed_value, dict) else None
    db_value = json.dumps(parsed_value).encode("UTF8")
    if isinstance(parsed_value, dict) and value_type is not None:
        parsed_value["type"] = value_type
    return db_value, value_type


def from_database(database_value, value_type=None):
    """
    Converts a parameter value from its database representation into an encoded Python object.

    Args:
        database_value (bytes, optional): a value in the database
        value_type (str, optional): the type in case of complex ones

    Returns:
        Any: the encoded parameter value
    """
    parsed = load_db_value(database_value, value_type)
    if isinstance(parsed, dict):
        return from_dict(parsed)
    if isinstance(parsed, bool):
        return parsed
    if isinstance(parsed, Number):
        return float(parsed)
    return parsed


def from_database_to_single_value(database_value, value_type):
    """
    Converts a value from its database representation into a single value.

    Indexed values get converted to their type string.

    Args:
        database_value (bytes): a value in the database
        value_type (str, optional): value's type

    Returns:
        Any: single-value representation
    """
    if value_type is None or value_type not in ("map", "time_series", "time_pattern", "array"):
        return from_database(database_value, value_type)
    return value_type


def from_database_to_dimension_count(database_value, value_type):
    """
    Counts dimensions of value's database representation

    Args:
        database_value (bytes): a value in the database
        value_type (str, optional): value's type

    Returns:
        int: number of dimensions
    """

    if value_type in {"time_series", "time_pattern", "array"}:
        return 1
    if value_type == "map":
        map_value = from_database(database_value, value_type)
        return map_dimensions(map_value)
    return 0


def to_database(parsed_value):
    """
    Converts an encoded Python object into its database representation.

    Args:
        value: the value to convert. It can be the result of either ``load_db_value`` or ``from_database```.

    Returns:
        bytes: value's database representation as bytes
        str: the value type
    """
    if hasattr(parsed_value, "to_database"):
        return parsed_value.to_database()
    db_value = json.dumps(parsed_value).encode("UTF8")
    return db_value, None


def from_dict(value_dict):
    """
    Converts a complex (relationship) parameter value from its dictionary representation to a Python object.

    Args:
        value_dict (dict): value's dictionary; a parsed JSON object with the "type" key

    Returns:
        the encoded (relationship) parameter value
    """
    value_type = value_dict["type"]
    try:
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


def fix_conflict(new, old, on_conflict="merge"):
    """Resolves conflicts between parameter values:

    Args:
        new (any): new parameter value to write
        old (any): existing parameter value in the db
        on_conflict (str): conflict resolution strategy:
            - 'merge': Merge indexes if possible, otherwise replace
            - 'replace': Replace old with new
            - 'keep': keep old

    Returns:
        any: a parameter value with conflicts resolved
    """
    funcs = {"keep": lambda new, old: old, "replace": lambda new, old: new, "merge": merge}
    func = funcs.get(on_conflict)
    if func is None:
        raise RuntimeError(
            f"Invalid conflict resolution strategy {on_conflict}, valid strategies are {', '.join(funcs)}"
        )
    return func(new, old)


def merge(value, other):
    """Merges other into value, returns the result.
    Args:
        value (tuple): recipient value and type
        other (tuple): other value and type

    Returns:
        tuple: value and type of merged value
    """
    parsed_value = from_database(*value)
    if not hasattr(parsed_value, "merge"):
        return value
    parsed_other = from_database(*other)
    return to_database(parsed_value.merge(parsed_other))


def merge_parsed(parsed_value, parsed_other):
    if not hasattr(parsed_value, "merge"):
        return parsed_value
    return parsed_value.merge(parsed_other)


def _break_dictionary(data):
    """Converts {"index": value} style dictionary into (list(indexes), numpy.ndarray(values)) tuple."""
    if not isinstance(data, dict):
        raise ParameterValueFormatError(
            f"expected data to be in dictionary format, instead got '{type(data).__name__}'"
        )
    indexes, values = zip(*data.items())
    return list(indexes), np.array(values)


def _datetime_from_database(value):
    """Converts a datetime database value into a DateTime object."""
    try:
        stamp = dateutil.parser.parse(value)
    except ValueError:
        raise ParameterValueFormatError(f'Could not parse datetime from "{value}"')
    return DateTime(stamp)


def _duration_from_database(value):
    """
    Converts a duration database value into a Duration object.

    The deprecated 'variable durations' will be converted to Arrays.
    """
    if isinstance(value, (str, int)):
        # Set default unit to minutes if value is a plain number.
        if not isinstance(value, str):
            value = f"{value}m"
    elif isinstance(value, Sequence):
        # This type of 'variable duration' is deprecated. We make an Array instead.
        # Set default unit to minutes for plain numbers in value.
        value = [v if isinstance(v, str) else f"{v}m" for v in value]
        return Array([Duration(v) for v in value])
    else:
        raise ParameterValueFormatError("Duration value is of unsupported type")
    return Duration(value)


def _time_series_from_database(value_dict):
    """Converts a time series database value into a time series object.

    Args:
        value_dict (dict): time series dictionary

    Returns:
        TimeSeries: restored time series
    """
    data = value_dict["data"]
    if isinstance(data, dict):
        return _time_series_from_dictionary(value_dict)
    if isinstance(data, list):
        if isinstance(data[0], Sequence):
            return _time_series_from_two_columns(value_dict)
        return _time_series_from_single_column(value_dict)
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


def _time_series_from_dictionary(value_dict):
    """Converts a dictionary style time series into a TimeSeriesVariableResolution object.

    Args:
        value_dict (dict): time series dictionary

    Returns:
        TimeSeriesVariableResolution: restored time series
    """
    data = value_dict["data"]
    stamps = list()
    values = np.empty(len(data))
    for index, (stamp, series_value) in enumerate(data.items()):
        try:
            stamp = np.datetime64(stamp, NUMPY_DATETIME64_UNIT)
        except ValueError:
            raise ParameterValueFormatError(f'Could not decode time stamp "{stamp}"')
        stamps.append(stamp)
        values[index] = series_value
    stamps = np.array(stamps)
    ignore_year, repeat = _variable_resolution_time_series_info_from_index(value_dict)
    return TimeSeriesVariableResolution(stamps, values, ignore_year, repeat, value_dict.get("index_name", ""))


def _time_series_from_single_column(value_dict):
    """Converts a time series dictionary into a TimeSeriesFixedResolution object.

    Args:
        value_dict (dict): time series dictionary

    Returns:
        TimeSeriesFixedResolution: restored time series
    """
    if "index" in value_dict:
        value_index = value_dict["index"]
        start = value_index["start"] if "start" in value_index else _TIME_SERIES_DEFAULT_START
        resolution = value_index["resolution"] if "resolution" in value_index else _TIME_SERIES_DEFAULT_RESOLUTION
        if "ignore_year" in value_index:
            try:
                ignore_year = bool(value_index["ignore_year"])
            except ValueError:
                raise ParameterValueFormatError(f'Could not decode ignore_year value "{value_index["ignore_year"]}"')
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
    values = np.array(value_dict["data"])
    return TimeSeriesFixedResolution(
        start, relativedeltas, values, ignore_year, repeat, value_dict.get("index_name", "")
    )


def _time_series_from_two_columns(value_dict):
    """Converts a two column style time series into a TimeSeriesVariableResolution object.

    Args:
        value_dict (dict): time series dictionary

    Returns:
        TimeSeriesVariableResolution: restored time series
    """
    data = value_dict["data"]
    stamps = list()
    values = np.empty(len(data))
    for index, element in enumerate(data):
        if not isinstance(element, Sequence) or len(element) != 2:
            raise ParameterValueFormatError("Invalid value in time series array")
        try:
            stamp = np.datetime64(element[0], NUMPY_DATETIME64_UNIT)
        except ValueError:
            raise ParameterValueFormatError(f'Could not decode time stamp "{element[0]}"')
        stamps.append(stamp)
        values[index] = element[1]
    stamps = np.array(stamps)
    ignore_year, repeat = _variable_resolution_time_series_info_from_index(value_dict)
    return TimeSeriesVariableResolution(stamps, values, ignore_year, repeat, value_dict.get("index_name", ""))


def _time_pattern_from_database(value_dict):
    """Converts a time pattern database value into a TimePattern object.

    Args:
        value_dict (dict): time pattern dictionary

    Returns:
        TimePattern: restored time pattern
    """
    patterns, values = _break_dictionary(value_dict["data"])
    return TimePattern(patterns, values, value_dict.get("index_name", "p"))


def _map_from_database(value_dict):
    """Converts a map from its database representation to a Map object.

    Args:
        value_dict (dict): Map dictionary

    Returns:
        Map: restored Map
    """
    index_type = _map_index_type_from_database(value_dict["index_type"])
    index_name = value_dict.get("index_name", "x")
    data = value_dict["data"]
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
    return Map(indexes, values, index_type, index_name)


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
        return dict(type=value.type_(), **value.to_dict())
    return value


def _map_values_from_database(values_in_db):
    """Converts map's values from their database format."""
    if not values_in_db:
        return list()
    values = list()
    for value_in_db in values_in_db:
        value = from_dict(value_in_db) if isinstance(value_in_db, dict) else value_in_db
        if isinstance(value, int):
            value = float(value)
        elif value is not None and not isinstance(value, (float, bool, Duration, IndexedValue, str, DateTime)):
            raise ParameterValueFormatError(f'Unsupported value type for Map: "{type(value).__name__}".')
        values.append(value)
    return values


def _array_from_database(value_dict):
    """Converts a value dictionary to Array.

    Args:
          value_dict (dict): array dictionary

    Returns:
          Array: Array value
    """
    value_type_id = value_dict.get("value_type", "float")
    value_type = {"float": float, "str": str, "date_time": DateTime, "duration": Duration, "time_period": str}.get(
        value_type_id, None
    )
    if value_type is None:
        raise ParameterValueFormatError(f'Unsupported value type for Array: "{value_type_id}".')
    try:
        data = [value_type(x) for x in value_dict["data"]]
    except (TypeError, ParameterValueFormatError) as error:
        raise ParameterValueFormatError(f'Failed to read values for Array: {error}')
    else:
        index_name = value_dict.get("index_name", "i")
        return Array(data, value_type, index_name)


class ListValueRef:
    def __init__(self, list_value_id):
        self._list_value_id = list_value_id

    @staticmethod
    def type_():
        return "list_value_ref"

    def to_database(self):
        """Returns the database representation of this object as JSON."""
        return json.dumps(self._list_value_id).encode("UTF8"), self.type_()


class DateTime:
    """A single datetime value."""

    VALUE_TYPE = "single value"

    def __init__(self, value=None):
        """
        Args:
            value (DataTime or str or datetime.datetime): a timestamp
        """
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

    def __lt__(self, other):
        if not isinstance(other, DateTime):
            return NotImplemented
        return self._value < other._value

    def __hash__(self):
        return hash(self._value)

    def __str__(self):
        return self._value.isoformat()

    def value_to_database_data(self):
        """Returns the database representation of the datetime."""
        return self._value.isoformat()

    def to_dict(self):
        """Returns the database representation of this object."""
        return {"data": self.value_to_database_data()}

    @staticmethod
    def type_():
        return "date_time"

    def to_database(self):
        """Returns the database representation of this object as JSON."""
        return json.dumps(self.to_dict()).encode("UTF8"), self.type_()

    @property
    def value(self):
        """Returns the value as a datetime object."""
        return self._value


class Duration:
    """
    This class represents a duration in time.

    Durations are always handled as relativedeltas.
    """

    VALUE_TYPE = "single value"

    def __init__(self, value=None):
        """
        Args:
            value (str or relativedelta): the time step
        """
        if value is None:
            value = relativedelta(hours=1)
        elif isinstance(value, str):
            value = duration_to_relativedelta(value)
        elif isinstance(value, Duration):
            value = copy(value._value)
        if not isinstance(value, relativedelta):
            raise ParameterValueFormatError(f'Could not parse duration from "{value}"')
        self._value = value

    def __eq__(self, other):
        """Returns True if other is equal to this object."""
        if not isinstance(other, Duration):
            return NotImplemented
        return self._value == other._value

    def __hash__(self):
        return hash(self._value)

    def __str__(self):
        return str(relativedelta_to_duration(self._value))

    def value_to_database_data(self):
        """Returns the 'data' attribute part of Duration's database representation."""
        return relativedelta_to_duration(self._value)

    def to_dict(self):
        """Returns the database representation of the duration."""
        return {"data": self.value_to_database_data()}

    @staticmethod
    def type_():
        return "duration"

    def to_database(self):
        """Returns the database representation of the duration as JSON."""
        return json.dumps(self.to_dict()).encode("UTF8"), self.type_()

    @property
    def value(self):
        """Returns the duration as a :class:`relativedelta`."""
        return self._value


class _Indexes(np.ndarray):
    """
    A subclass of numpy.ndarray that keeps a lookup dictionary from elements to positions.
    Used by methods get_value and set_value of IndexedValue, to avoid something like

        position = indexes.index(element)

    which might be too slow compared to dictionary lookup.
    """

    def __new__(cls, other, dtype=None):
        obj = np.asarray(other, dtype=dtype).view(cls)
        obj.position_lookup = {index: k for k, index in enumerate(other)}
        return obj

    def __array_finalize__(self, obj):
        if obj is None:
            return
        # pylint: disable=attribute-defined-outside-init
        self.position_lookup = getattr(obj, 'position_lookup', {})

    def __setitem__(self, position, index):
        old_index = self.__getitem__(position)
        self.position_lookup[index] = self.position_lookup.pop(old_index, '')
        super().__setitem__(position, index)

    def __eq__(self, other):
        return np.all(super().__eq__(other))

    def __bool__(self):
        return np.size(self) != 0


class IndexedValue:
    """
    An abstract base class for indexed values.

    Attributes:
        index_name (str): index name
    """

    VALUE_TYPE = NotImplemented

    def __init__(self, index_name):
        """
        Args:
            index_name (str): index name
        """
        self._indexes = None
        self._values = None
        self.index_name = index_name

    def __bool__(self):
        # NOTE: Use self.indexes rather than self._indexes, otherwise TimeSeriesFixedResolution gives wrong result
        return bool(self.indexes)

    def __len__(self):
        """Returns the number of values."""
        return len(self.indexes)

    @staticmethod
    def type_():
        """Returns a type identifier string.

        Returns:
            str: type identifier
        """
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
        return json.dumps(self.to_dict()).encode("UTF8"), self.type_()

    @property
    def values(self):
        """Returns the data values."""
        return self._values

    @values.setter
    def values(self, values):
        """Sets the values."""
        self._values = values

    def get_nearest(self, index):
        pos = np.searchsorted(self.indexes, index)
        return self.values[pos]

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

    def to_dict(self):
        """Converts the value to a Python dictionary.

        Returns:
            dict(): mapping from indexes to values
        """
        raise NotImplementedError()

    def merge(self, other):
        if not isinstance(other, type(self)):
            return self
        new_indexes = np.unique(np.concatenate((self.indexes, other.indexes)))
        new_indexes.sort(kind='mergesort')
        _merge = lambda value, other: other if value is None else merge_parsed(value, other)
        new_values = [_merge(self.get_value(index), other.get_value(index)) for index in new_indexes]
        self.indexes = new_indexes
        self.values = new_values
        return self


class Array(IndexedValue):
    """A one dimensional array with zero based indexing."""

    VALUE_TYPE = "array"
    DEFAULT_INDEX_NAME = "i"

    def __init__(self, values, value_type=None, index_name=""):
        """
        Args:
            values (Sequence): array's values
            value_type (Type, optional): array element type; will be deduced from the array if not given
                and defaults to float if ``values`` is empty
            index_name (str): index name
        """
        super().__init__(index_name if index_name else Array.DEFAULT_INDEX_NAME)
        if value_type is None:
            value_type = type(values[0]) if values else float
            if value_type == int:
                try:
                    values = [float(x) for x in values]
                except ValueError:
                    raise ParameterValueFormatError("Cannot convert array's values to float.")
                value_type = float
        if any(not isinstance(x, value_type) for x in values):
            try:
                values = [value_type(x) for x in values]
            except ValueError:
                raise ParameterValueFormatError("Not all array's values are of the same type.")
        self.indexes = range(len(values))
        self.values = list(values)
        self._value_type = value_type

    def __eq__(self, other):
        if not isinstance(other, Array):
            return NotImplemented
        return np.array_equal(self._values, other._values) and self.index_name == other.index_name

    @staticmethod
    def type_():
        return "array"

    def to_dict(self):
        """See base class."""
        value_type_id = {
            float: "float",
            str: "str",  # String could also mean time_period but we don't have any way to distinguish that, yet.
            DateTime: "date_time",
            Duration: "duration",
        }.get(self._value_type)
        if value_type_id is None:
            raise ParameterValueFormatError(f"Cannot write unsupported array value type: {self._value_type.__name__}")
        if value_type_id in ("float", "str"):
            data = self._values
        else:
            data = [x.value_to_database_data() for x in self._values]
        value_dict = {"value_type": value_type_id, "data": data}
        if self.index_name != "i":
            value_dict["index_name"] = self.index_name
        return value_dict

    @property
    def value_type(self):
        """Returns the type of array's elements."""
        return self._value_type


class IndexedNumberArray(IndexedValue):
    """
    An abstract base class for indexed floats.

    The indexes and numbers are stored in numpy.ndarrays.
    """

    def __init__(self, index_name, values):
        """
        Args:
            index_name (str): index name
            values (Sequence): array's values; index handling should be implemented by subclasses
        """
        super().__init__(index_name)
        self.values = values

    @IndexedValue.values.setter
    def values(self, values):
        """Sets the values."""
        if not isinstance(values, np.ndarray) or not values.dtype == np.dtype(float):
            values = np.array(values, dtype=float)
        self._values = values

    @staticmethod
    def type_():
        raise NotImplementedError()

    def to_dict(self):
        """Return the database representation of the value."""
        raise NotImplementedError()


class TimeSeries(IndexedNumberArray):
    """An abstract base class for time series."""

    VALUE_TYPE = "time series"
    DEFAULT_INDEX_NAME = "t"

    def __init__(self, values, ignore_year, repeat, index_name=""):
        """
        Args:
            values (Sequence): an array of values
            ignore_year (bool): True if the year should be ignored in the time stamps
            repeat (bool): True if the series should be repeated from the beginning
            index_name (str): index name
        """
        if len(values) < 1:
            raise ParameterValueFormatError("Time series too short. Must have one or more values")
        super().__init__(index_name if index_name else TimeSeries.DEFAULT_INDEX_NAME, values)
        self._ignore_year = ignore_year
        self._repeat = repeat

    def __len__(self):
        """Returns the number of values."""
        return len(self._values)

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

    @staticmethod
    def type_():
        return "time_series"

    def to_dict(self):
        """Return the database representation of the value."""
        raise NotImplementedError()


def _check_time_pattern_index(union_str):
    """
    Checks if a time pattern index has the right format.

    Args:
        union_str (str): The time pattern index to check. Generally assumed to be a union of interval intersections.

    Raises:
        ParameterValueFormatError: If the given string doesn't comply with time pattern spec.
    """
    if not union_str:
        # We accept empty strings so we can add empty rows in the parameter value editor UI
        return
    union_dlm = ","
    intersection_dlm = ";"
    range_dlm = "-"
    regexp = r"(Y|M|D|WD|h|m|s)"
    for intersection_str in union_str.split(union_dlm):
        for interval_str in intersection_str.split(intersection_dlm):
            m = re.match(regexp, interval_str)
            if m is None:
                raise ParameterValueFormatError(
                    f"Invalid interval {interval_str}, it should start with either Y, M, D, WD, h, m, or s."
                )
            key = m.group(0)
            lower_upper_str = interval_str[len(key) :]
            lower_upper = lower_upper_str.split(range_dlm)
            if len(lower_upper) != 2:
                raise ParameterValueFormatError(
                    f"Invalid interval bounds {lower_upper_str}, it should be two integers separated by dash (-)."
                )
            lower_str, upper_str = lower_upper
            try:
                lower = int(lower_str)
            except:
                raise ParameterValueFormatError(f"Invalid lower bound {lower_str}, must be an integer.")
            try:
                upper = int(upper_str)
            except:
                raise ParameterValueFormatError(f"Invalid upper bound {upper_str}, must be an integer.")
            if lower > upper:
                raise ParameterValueFormatError(f"Lower bound {lower} can't be higher than upper bound {upper}.")


class _TimePatternIndexes(_Indexes):
    """An array of *checked* time pattern indexes."""

    def __array_finalize__(self, obj):
        """Checks indexes when building the array."""
        for x in obj:
            _check_time_pattern_index(x)
        super().__array_finalize__(obj)

    def __eq__(self, other):
        return list(self) == list(other)

    def __setitem__(self, position, index):
        """Checks indexes when setting and item."""
        _check_time_pattern_index(index)
        super().__setitem__(position, index)


class TimePattern(IndexedNumberArray):
    """Represents a time pattern (relationship) parameter value."""

    VALUE_TYPE = "time pattern"
    DEFAULT_INDEX_NAME = "p"

    def __init__(self, indexes, values, index_name=""):
        """
        Args:
            indexes (list): a list of time pattern strings
            values (Sequence): an array of values corresponding to the time patterns
            index_name (str): index name
        """
        if len(indexes) != len(values):
            raise ParameterValueFormatError("Length of values does not match length of indexes")
        if not indexes:
            raise ParameterValueFormatError("Empty time pattern not allowed")
        super().__init__(index_name if index_name else TimePattern.DEFAULT_INDEX_NAME, values)
        self.indexes = indexes

    def __eq__(self, other):
        """Returns True if other is equal to this object."""
        if not isinstance(other, TimePattern):
            return NotImplemented
        return (
            self._indexes == other._indexes
            and np.all(self._values == other._values)
            and self.index_name == other.index_name
        )

    @IndexedNumberArray.indexes.setter
    def indexes(self, indexes):
        """Sets the indexes."""
        self._indexes = _TimePatternIndexes(indexes, dtype=np.object_)

    @staticmethod
    def type_():
        return "time_pattern"

    def to_dict(self):
        """Returns the database representation of this time pattern."""
        value_dict = {"data": dict(zip(self._indexes, self._values))}
        if self.index_name != "p":
            value_dict["index_name"] = self.index_name
        return value_dict


class TimeSeriesFixedResolution(TimeSeries):
    """
    A time series with fixed durations between the time stamps.

    When getting the indexes the durations are applied cyclically.

    Currently, there is no support for the `ignore_year` and `repeat` options
    other than having getters for their values.
    """

    _memoized_indexes = {}

    def __init__(self, start, resolution, values, ignore_year, repeat, index_name=""):
        """
        Args:
            start (str or datetime or datetime64): the first time stamp
            resolution (str, relativedelta, list): duration(s) between the time stamps
            values (Sequence): data values at each time stamp
            ignore_year (bool): whether or not the time-series should apply to any year
            repeat (bool): whether or not the time series should repeat cyclically
            index_name (str): index name
        """
        super().__init__(values, ignore_year, repeat, index_name)
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
            and np.array_equal(self._values, other._values, equal_nan=True)
            and self._ignore_year == other._ignore_year
            and self._repeat == other._repeat
            and self.index_name == other.index_name
        )

    def _get_memoized_indexes(self):
        key = (self.start, tuple(self.resolution), len(self))
        memoized_indexes = self._memoized_indexes.get(key)
        if memoized_indexes is not None:
            return memoized_indexes
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
        memoized_indexes = self._memoized_indexes[key] = np.array(stamps, dtype=_NUMPY_DATETIME_DTYPE)
        return memoized_indexes

    @property
    def indexes(self):
        """Returns the time stamps as a numpy.ndarray of numpy.datetime64 objects."""
        if self._indexes is None:
            self.indexes = self._get_memoized_indexes()
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

    def to_dict(self):
        """Returns the value in its database representation."""
        if len(self._resolution) > 1:
            resolution_as_json = [relativedelta_to_duration(step) for step in self._resolution]
        else:
            resolution_as_json = relativedelta_to_duration(self._resolution[0])
        value_dict = {
            "index": {
                "start": str(self._start),
                "resolution": resolution_as_json,
                "ignore_year": self._ignore_year,
                "repeat": self._repeat,
            },
            "data": self._values.tolist(),
        }
        if self.index_name != "t":
            value_dict["index_name"] = self.index_name
        return value_dict


class TimeSeriesVariableResolution(TimeSeries):
    """A class representing time series data with variable time steps."""

    def __init__(self, indexes, values, ignore_year, repeat, index_name=""):
        """
        Args:
            indexes (Sequence): time stamps as numpy.datetime64 objects
            values (Sequence): the values corresponding to the time stamps
            ignore_year (bool): True if the stamp year should be ignored
            repeat (bool): True if the series should be repeated from the beginning
            index_name (str): index name
        """
        super().__init__(values, ignore_year, repeat, index_name)
        if len(indexes) != len(values):
            raise ParameterValueFormatError("Length of values does not match length of indexes")
        if not isinstance(indexes, np.ndarray):
            date_times = np.empty(len(indexes), dtype=_NUMPY_DATETIME_DTYPE)
            for i, index in enumerate(indexes):
                if isinstance(index, DateTime):
                    date_times[i] = np.datetime64(index.value, NUMPY_DATETIME64_UNIT)
                else:
                    try:
                        date_times[i] = np.datetime64(index, NUMPY_DATETIME64_UNIT)
                    except ValueError:
                        raise ParameterValueFormatError(
                            f'Cannot convert "{index}" of type {type(index).__name__} to time stamp.'
                        )
            indexes = date_times
        self.indexes = indexes

    def __eq__(self, other):
        """Returns True if other is equal to this object."""
        if not isinstance(other, TimeSeriesVariableResolution):
            return NotImplemented
        return (
            np.array_equal(self._indexes, other._indexes)
            and np.array_equal(self._values, other._values, equal_nan=True)
            and self._ignore_year == other._ignore_year
            and self._repeat == other._repeat
            and self.index_name == other.index_name
        )

    def to_dict(self):
        """Returns the value in its database representation"""
        value_dict = dict()
        value_dict["data"] = {str(index): float(value) for index, value in zip(self._indexes, self._values)}
        # Add "index" entry only if its contents are not set to their default values.
        if self._ignore_year:
            value_dict.setdefault("index", dict())["ignore_year"] = self._ignore_year
        if self._repeat:
            value_dict.setdefault("index", dict())["repeat"] = self._repeat
        if self.index_name != "t":
            value_dict["index_name"] = self.index_name
        return value_dict


class Map(IndexedValue):
    """A nested general purpose indexed value."""

    VALUE_TYPE = "map"
    DEFAULT_INDEX_NAME = "x"

    def __init__(self, indexes, values, index_type=None, index_name=""):
        """
        Args:
            indexes (Sequence): map's indexes
            values (Sequence): map's values
            index_type (type or NoneType): index type or None to deduce from indexes
            index_name (str): index name
        """
        if not indexes and index_type is None:
            raise ParameterValueFormatError("Cannot deduce index type from empty indexes list.")
        if indexes and index_type is not None and not isinstance(indexes[0], index_type):
            raise ParameterValueFormatError('Type of index does not match "index_type" argument.')
        if len(indexes) != len(values):
            raise ParameterValueFormatError("Length of values does not match length of indexes")
        super().__init__(index_name if index_name else Map.DEFAULT_INDEX_NAME)
        self.indexes = indexes
        self._index_type = index_type if index_type is not None else type(indexes[0])
        self._values = values

    def __eq__(self, other):
        if not isinstance(other, Map):
            return NotImplemented
        return other._indexes == self._indexes and other._values == self._values and self.index_name == other.index_name

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

    @staticmethod
    def type_():
        return "map"

    def to_dict(self):
        """Returns map's database representation."""
        value_dict = {
            "index_type": _map_index_type_to_database(self._index_type),
            "data": self.value_to_database_data(),
        }
        if self.index_name != "x":
            value_dict["index_name"] = self.index_name
        return value_dict


def map_dimensions(map_):
    """Counts Map's dimensions.

    Args:
        map_ (Map): a Map

    Returns:
        int: number of dimensions
    """
    nested = 0
    for v in map_.values:
        if isinstance(v, Map):
            nested = max(nested, map_dimensions(v))
        elif isinstance(v, IndexedValue):
            nested = max(nested, 1)
    return 1 + nested


def convert_leaf_maps_to_specialized_containers(map_):
    """
    Converts suitable leaf maps to corresponding specialized containers.

    Currently supported conversions:

    - index_type: :class:`DateTime`, all values ``float`` -> :class"`TimeSeries`

    Args:
        map_ (Map): a map to process

    Returns:
        IndexedValue: a map with leaves converted or specialized container if map was convertible in itself
    """
    converted_container = _try_convert_to_container(map_)
    if converted_container is not None:
        return converted_container
    new_values = list()
    for _, value in zip(map_.indexes, map_.values):
        if isinstance(value, Map):
            converted = convert_leaf_maps_to_specialized_containers(value)
            new_values.append(converted)
        else:
            new_values.append(value)
    return Map(map_.indexes, new_values, index_name=map_.index_name)


def convert_containers_to_maps(value):
    """
    Converts indexed values into maps.

    if ``value`` is :class:`Map` converts leaf values into Maps recursively.

    Args:
        value (IndexedValue): a value to convert

    Returns:
        Map: converted Map
    """
    if isinstance(value, Map):
        if not value:
            return value
        new_values = list()
        for _, x in zip(value.indexes, value.values):
            if isinstance(x, IndexedValue):
                new_values.append(convert_containers_to_maps(x))
            else:
                new_values.append(x)
        return Map(list(value.indexes), new_values, index_name=value.index_name)
    if isinstance(value, IndexedValue):
        if not value:
            if isinstance(value, TimeSeries):
                return Map([], [], DateTime, index_name=TimeSeries.DEFAULT_INDEX_NAME)
            return Map([], [], str)
        return Map(list(value.indexes), list(value.values), index_name=value.index_name)
    return value


def convert_map_to_table(map_, make_square=True, row_this_far=None, empty=None):
    """
    Converts :class:`Map` into list of rows recursively.

    Args:
        map_ (Map): map to convert
        make_square (bool): if True, append None to shorter rows, otherwise leave the row as is
        row_this_far (list, optional): current row; used for recursion
        empty (Any, optional): object to fill empty cells with

    Returns:
        list of list: map's rows
    """
    if row_this_far is None:
        row_this_far = list()
    rows = list()
    for index, value in zip(map_.indexes, map_.values):
        if not isinstance(value, Map):
            rows.append(row_this_far + [index, value])
        else:
            rows += convert_map_to_table(value, False, row_this_far + [index])
    if make_square:
        max_length = 0
        for row in rows:
            max_length = max(max_length, len(row))
        equal_length_rows = list()
        for row in rows:
            equal_length_row = row + (max_length - len(row)) * [empty]
            equal_length_rows.append(equal_length_row)
        return equal_length_rows
    return rows


def convert_map_to_dict(map_):
    """
    Converts :class:`Map` to nested dictionaries.

    Args:
        map_ (Map): map to convert

    Returns:
        dict: Map as a dict
    """
    d = dict()
    for index, x in zip(map_.indexes, map_.values):
        if isinstance(x, Map):
            x = convert_map_to_dict(x)
        d[index] = x
    return d


def _try_convert_to_container(map_):
    """
    Tries to convert a map to corresponding specialized container.

    Args:
        map_ (Map): a map to convert

    Returns:
        TimeSeriesVariableResolution or None: converted Map or None if the map couldn't be converted
    """
    if not map_:
        return None
    stamps = list()
    values = list()
    for index, value in zip(map_.indexes, map_.values):
        if not isinstance(index, DateTime) or not isinstance(value, float):
            return None
        stamps.append(index)
        values.append(value)
    return TimeSeriesVariableResolution(stamps, values, False, False, index_name=map_.index_name)


# List of scalar types that are supported by the spinedb_api
SUPPORTED_TYPES = (Duration, DateTime, float, str)


def join_value_and_type(db_value, db_type):
    """Joins database value and type into a string.
    The resulting string is a JSON string.
    In case of complex types (duration, date_time, time_series, time_pattern, array, map),
    the type is just added as top-level key.

    Args:
        db_value (bytes): database value
        db_type (str, optional): value type

    Returns:
        str: parameter value as JSON with an additional `type` field.
    """
    try:
        parsed = load_db_value(db_value, db_type)
    except ParameterValueFormatError:
        parsed = None
    return json.dumps(parsed)


def split_value_and_type(value_and_type):
    """Splits the given string into value and type.
    The string must be the result of calling ``join_value_and_type`` or have the same form.

    Args:
        value_and_type (str)

    Returns:
        bytes
        str or NoneType
    """
    try:
        parsed = json.loads(value_and_type)
    except (TypeError, json.JSONDecodeError):
        parsed = value_and_type
    return dump_db_value(parsed)
