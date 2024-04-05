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
Parameter values in a Spine DB can be of different types (see :ref:`parameter_value_format`).
For each of these types, this module provides a Python class to represent values of that type.

.. list-table:: Parameter value type and Python class
   :header-rows: 1

   * - type
     - Python class
   * - ``date_time``
     - :class:`DateTime`
   * - ``duration``
     - :class:`Duration`
   * - ``array``
     - :class:`Array`
   * - ``time_pattern``
     - :class:`TimePattern`
   * - ``time_series``
     - :class:`TimeSeriesFixedResolution` and :class:`TimeSeriesVariableResolution`
   * - ``map``
     - :class:`Map`

The module also provides the functions :func:`to_database` and :func:`from_database`
to translate between instances of the above classes and their DB representation (namely, the `value` and `type` fields
that would go in the ``parameter_value`` table).

For example, to write a Python object into a parameter value in the DB::

    # Create the Python object
    parsed_value = TimeSeriesFixedResolution(
        datetime("2023-01-01T00:00"),   # start
        "1D",                           # resolution
        [9, 8, 7, 6, 5, 4, 3, 2, 1, 0], # values
        ignore_year=False,
        repeat=False,
    )
    # Translate it to value and type
    value, type_ = to_database(parsed_value)
    # Add a parameter_value to the DB with that value and type
    with DatabaseMapping(url) as db_map:
        db_map.add_parameter_value_item(
            entity_class_name="cat",
            entity_byname=("Tom",),
            parameter_definition_name="number_of_lives",
            alternative_name="Base",
            value=value,
            type=type_,
        )
        db_map.commit_session("Tom is living one day at a time")

Similarly, to read a parameter value from the DB into a Python object::

    # Get the parameter_value from the DB
    with DatabaseMapping(url) as db_map:
        pval_item = db_map.get_parameter_value_item(
            entity_class_name="cat",
            entity_byname=("Tom",),
            parameter_definition_name="number_of_lives",
            alternative_name="Base",
        )
    # Obtain value and type
    value, type_ = pval_item["value"], pval_item["type"]
    # Translate value and type to a Python object
    parsed_value = from_database(value, type_)

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


def from_database(value, type_=None):
    """
    Converts a parameter value from the DB into a Python object.

    Args:
        value (bytes or None): the `value` field from the ``parameter_value`` table.
        type_ (str, optional): the `type` field from the ``parameter_value`` table.

    Returns:
        :class:`ParameterValue`, float, str, bool or None: a Python object representing the parameter value.
    """
    parsed = load_db_value(value, type_)
    if isinstance(parsed, dict):
        return from_dict(parsed)
    if isinstance(parsed, bool):
        return parsed
    if isinstance(parsed, Number):
        return float(parsed)
    return parsed


def to_database(parsed_value):
    """
    Converts a Python object representing a parameter value into their DB representation.

    Args:
        parsed_value (any): the Python object.

    Returns:
        tuple(bytes,str): the `value` and `type` fields that would go in the ``parameter_value`` table.
    """
    if hasattr(parsed_value, "to_database"):
        return parsed_value.to_database()
    db_value = json.dumps(parsed_value).encode("UTF8")
    return db_value, None


def duration_to_relativedelta(duration):
    """
    Converts a duration to a relativedelta object.

    :meta private:

    Args:
        duration (str): a duration string.

    Returns:
        :class:`~dateutil.relativedelta.relativedelta`: a relativedelta object corresponding to the given duration.
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

    :meta private:

    Args:
        delta (:class:`~dateutil.relativedelta.relativedelta`): the relativedelta to convert.

    Returns:
        str: a duration string
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


def load_db_value(db_value, type_=None):
    """
    Parses a database representation of a parameter value (value and type) into a Python object, using JSON.
    If the result is a dict, adds the "type" property to it.

    :meta private:

    Args:
        db_value (bytes, optional): the database value.
        type_ (str, optional): the value type.

    Returns:
        any: the parsed parameter value
    """
    if db_value is None:
        return None
    try:
        parsed = json.loads(db_value)
    except JSONDecodeError as err:
        raise ParameterValueFormatError(f"Could not decode the value: {err}") from err
    if isinstance(parsed, dict):
        return {"type": type_, **parsed}
    return parsed


def dump_db_value(parsed_value):
    """
    Unparses a Python object into a database representation of a parameter value (value and type), using JSON.
    If the given object is a dict, extracts the "type" property from it.

    :meta private:

    Args:
        parsed_value (any): a Python object, typically obtained by calling :func:`load_db_value`.

    Returns:
        tuple(str,str): database representation (value and type).
    """
    value_type = parsed_value.pop("type") if isinstance(parsed_value, dict) else None
    db_value = json.dumps(parsed_value).encode("UTF8")
    if isinstance(parsed_value, dict) and value_type is not None:
        parsed_value["type"] = value_type
    return db_value, value_type


def from_database_to_single_value(database_value, value_type):
    """
    Same as :func:`from_database`, but in the case of indexed types it returns just the type as a string.

    :meta private:

    Args:
        database_value (bytes): the database value
        value_type (str, optional): the value type

    Returns:
        :class:`ParameterValue`, float, str, bool or None: the encoded parameter value or its type.
    """
    if value_type is None or value_type not in ("map", "time_series", "time_pattern", "array"):
        return from_database(database_value, value_type)
    return value_type


def from_database_to_dimension_count(database_value, value_type):
    """
    Counts the dimensions in a database representation of a parameter value (value and type).

    :meta private:

    Args:
        database_value (bytes): the database value
        value_type (str, optional): the value type

    Returns:
        int: number of dimensions
    """

    if value_type in {"time_series", "time_pattern", "array"}:
        return 1
    if value_type == "map":
        map_value = from_database(database_value, value_type)
        return map_dimensions(map_value)
    return 0


def from_dict(value):
    """
    Converts a dictionary representation of a parameter value into an encoded parameter value.

    :meta private:

    Args:
        value (dict): the value dictionary including the "type" key.

    Returns:
        :class:`ParameterValue`, float, str, bool or None: the encoded parameter value.
    """
    value_type = value["type"]
    try:
        if value_type == "date_time":
            return _datetime_from_database(value["data"])
        if value_type == "duration":
            return _duration_from_database(value["data"])
        if value_type == "map":
            return _map_from_database(value)
        if value_type == "time_pattern":
            return _time_pattern_from_database(value)
        if value_type == "time_series":
            return _time_series_from_database(value)
        if value_type == "array":
            return _array_from_database(value)
        raise ParameterValueFormatError(f'Unknown parameter value type "{value_type}"')
    except KeyError as error:
        raise ParameterValueFormatError(f'"{error.args[0]}" is missing in the parameter value description')


def fix_conflict(new, old, on_conflict="merge"):
    """Resolves conflicts between parameter values:

    :meta private:

    Args:
        new (:class:`ParameterValue`, float, str, bool or None): new parameter value to be written.
        old (:class:`ParameterValue`, float, str, bool or None): an existing parameter value in the db.
        on_conflict (str): conflict resolution strategy:
            - 'merge': Merge indexes if possible, otherwise replace.
            - 'replace': Replace old with new.
            - 'keep': Keep old.

    Returns:
        :class:`ParameterValue`, float, str, bool or None: a new parameter value with conflicts resolved.
    """
    funcs = {"keep": lambda new, old: old, "replace": lambda new, old: new, "merge": merge}
    func = funcs.get(on_conflict)
    if func is None:
        raise RuntimeError(
            f"Invalid conflict resolution strategy {on_conflict}, valid strategies are {', '.join(funcs)}"
        )
    return func(new, old)


def merge(value, other):
    """Merges the DB representation of two parameter values.

    :meta private:

    Args:
        value (tuple(bytes,str)): recipient value and type.
        other (tuple(bytes,str)): other value and type.

    Returns:
        tuple(bytes,str): the DB representation of the merged value.
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
    return TimePattern(patterns, values, value_dict.get("index_name", TimePattern.DEFAULT_INDEX_NAME))


def _map_from_database(value_dict):
    """Converts a map from its database representation to a Map object.

    Args:
        value_dict (dict): Map dictionary

    Returns:
        Map: restored Map
    """
    index_type = _map_index_type_from_database(value_dict["index_type"])
    index_name = value_dict.get("index_name", Map.DEFAULT_INDEX_NAME)
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
        raise ParameterValueFormatError(f"Failed to read values for Array: {error}")
    else:
        index_name = value_dict.get("index_name", Array.DEFAULT_INDEX_NAME)
        return Array(data, value_type, index_name)


class ParameterValue:
    """Base for all classes representing parameter values."""

    VALUE_TYPE = NotImplemented

    def to_dict(self):
        """Returns a dictionary representation of this parameter value.

        :meta private:

        Returns:
            dict: a dictionary including the "type" key.
        """
        raise NotImplementedError()

    @staticmethod
    def type_():
        """Returns the type of the parameter value represented by this object.

        Returns:
            str
        """
        raise NotImplementedError()

    def to_database(self):
        """Returns the DB representation of this object. Equivalent to calling :func:`to_database` with it.

        Returns:
            tuple(bytes,str): the `value` and `type` fields that would go in the ``parameter_value`` table.
        """
        return json.dumps(self.to_dict()).encode("UTF8"), self.type_()


class ListValueRef:
    def __init__(self, list_value_id):
        self._list_value_id = list_value_id

    @staticmethod
    def type_():
        return "list_value_ref"

    def to_database(self):
        return json.dumps(self._list_value_id).encode("UTF8"), self.type_()


class DateTime(ParameterValue):
    """A parameter value of type 'date_time'. A point in time."""

    VALUE_TYPE = "single value"

    def __init__(self, value=None):
        """
        Args:
            value (:class:`DateTime` or str or :class:`~datetime.datetime`): the `date_time` value.
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
        """Returns the database representation of the datetime.

        :meta private:
        """
        return self._value.isoformat()

    def to_dict(self):
        return {"data": self.value_to_database_data()}

    @staticmethod
    def type_():
        """See base class

        :meta private:
        """
        return "date_time"

    @property
    def value(self):
        """The value.

        Returns:
            :class:`~datetime.datetime`
        """
        return self._value


class Duration(ParameterValue):
    """
    A parameter value of type 'duration'. An extension of time.
    """

    VALUE_TYPE = "single value"

    def __init__(self, value=None):
        """
        Args:
            value (str or :class:`Duration` or :class:`~dateutil.dateutil.relativedelta`): the `duration` value.
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
        if not isinstance(other, Duration):
            return NotImplemented
        return self._value == other._value

    def __hash__(self):
        return hash(self._value)

    def __str__(self):
        return str(relativedelta_to_duration(self._value))

    def value_to_database_data(self):
        """Returns the 'data' property of this object's database representation.

        :meta private:
        """
        return relativedelta_to_duration(self._value)

    def to_dict(self):
        return {"data": self.value_to_database_data()}

    @staticmethod
    def type_():
        """See base class

        :meta private:
        """
        return "duration"

    @property
    def value(self):
        """The value.

        Returns
            :class:`~dateutil.dateutil.relativedelta`
        """
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
        self.position_lookup = getattr(obj, "position_lookup", {})

    def __setitem__(self, position, index):
        old_index = self.__getitem__(position)
        self.position_lookup[index] = self.position_lookup.pop(old_index, "")
        super().__setitem__(position, index)

    def __eq__(self, other):
        return len(self) == len(other) and np.all(super().__eq__(other))

    def __bool__(self):
        return np.size(self) != 0


class IndexedValue(ParameterValue):
    """
    Base for all classes representing indexed parameter values.
    """

    DEFAULT_INDEX_NAME = NotImplemented

    def __init__(self, values, value_type=None, index_name=""):
        """
        :meta private:

        Args:
            index_name (str): a label for the index.
        """
        self._value_type = value_type
        self._indexes = None
        self._values = None
        self.values = values
        self.index_name = index_name if index_name else self.DEFAULT_INDEX_NAME

    def __bool__(self):
        # NOTE: Use self.indexes rather than self._indexes, otherwise TimeSeriesFixedResolution gives wrong result
        return bool(self.indexes)

    def __len__(self):
        return len(self.indexes)

    @staticmethod
    def type_():
        """See base class

        :meta private:
        """
        raise NotImplementedError()

    @property
    def indexes(self):
        """The indexes.

        Returns:
            :class:`~numpy.ndarray`
        """
        return self._indexes

    @indexes.setter
    def indexes(self, indexes):
        """Sets the indexes.

        Args:
            indexes (:class:`~numpy.ndarray`)
        """
        self._indexes = _Indexes(indexes)

    @property
    def values(self):
        """The values.

        Returns:
            :class:`~numpy.ndarray`
        """
        return self._values

    @values.setter
    def values(self, values):
        """Sets the values.

        Args:
            values (:class:`~numpy.ndarray`)
        """
        if isinstance(self._value_type, np.dtype) and (
            not isinstance(values, np.ndarray) or not values.dtype == self._value_type
        ):
            values = np.array(values, dtype=self._value_type)
        self._values = values

    @property
    def value_type(self):
        """The type of the values.

        Returns:
            type:
        """
        return self._value_type

    def get_nearest(self, index):
        """Returns the value at the nearest index to the given one.

        Args:
            index (any): The index.

        Returns:
            any: The value.
        """
        pos = np.searchsorted(self.indexes, index)
        return self.values[pos]

    def get_value(self, index):
        """Returns the value at the given index.

        Args:
            index (any): The index.

        Returns:
            any: The value.
        """
        pos = self.indexes.position_lookup.get(index)
        if pos is None:
            return None
        return self.values[pos]

    def set_value(self, index, value):
        """Sets the value at the given index.

        Args:
            index (any): The index.
            value (any): The value.
        """
        pos = self.indexes.position_lookup.get(index)
        if pos is not None:
            self.values[pos] = value

    def to_dict(self):
        raise NotImplementedError()

    def merge(self, other):
        if not isinstance(other, type(self)):
            return self
        new_indexes = np.unique(np.concatenate((self.indexes, other.indexes)))
        new_indexes.sort(kind="mergesort")
        _merge = lambda value, other: other if value is None else merge_parsed(value, other)
        new_values = [_merge(self.get_value(index), other.get_value(index)) for index in new_indexes]
        self.indexes = new_indexes
        self.values = new_values
        return self


class Array(IndexedValue):
    """A parameter value of type 'array'. A one dimensional array with zero based indexing."""

    VALUE_TYPE = "array"
    DEFAULT_INDEX_NAME = "i"

    def __init__(self, values, value_type=None, index_name=""):
        """
        Args:
            values (Sequence): the array values.
            value_type (type, optional): the type of the values; if not given, it will be deduced from `values`.
                Defaults to float if `values` is empty.
            index_name (str): the name you would give to the array index in your application.
        """
        if value_type is None:
            value_type = type(values[0]) if values else float
        if value_type == int:
            value_type = float
            try:
                values = [value_type(x) for x in values]
            except ValueError:
                raise ParameterValueFormatError("Cannot convert array's values to float.")
        if not all(isinstance(x, value_type) for x in values):
            try:
                values = [value_type(x) for x in values]
            except ValueError:
                raise ParameterValueFormatError("Not all array's values are of the same type.")
        super().__init__(values, value_type=value_type, index_name=index_name)
        self.indexes = range(len(values))

    def __eq__(self, other):
        if not isinstance(other, Array):
            return NotImplemented
        try:
            return np.array_equal(self._values, other._values, equal_nan=True) and self.index_name == other.index_name
        except TypeError:
            return np.array_equal(self._values, other._values) and self.index_name == other.index_name

    @staticmethod
    def type_():
        return "array"

    def to_dict(self):
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
        if self.index_name != self.DEFAULT_INDEX_NAME:
            value_dict["index_name"] = self.index_name
        return value_dict


class _TimePatternIndexes(_Indexes):
    """An array of *checked* time pattern indexes."""

    @staticmethod
    def _check_index(union_str):
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

    def __array_finalize__(self, obj):
        """Checks indexes when building the array."""
        for x in obj:
            self._check_index(x)
        super().__array_finalize__(obj)

    def __eq__(self, other):
        return list(self) == list(other)

    def __setitem__(self, position, index):
        """Checks indexes when setting and item."""
        self._check_index(index)
        super().__setitem__(position, index)


class TimePattern(IndexedValue):
    """A parameter value of type 'time_pattern'.
    A mapping from time patterns strings to numerical values.
    """

    VALUE_TYPE = "time pattern"
    DEFAULT_INDEX_NAME = "p"

    def __init__(self, indexes, values, index_name=""):
        """
        Args:
            indexes (list): the time pattern strings.
            values (Sequence): the values associated to different patterns.
            index_name (str): index name
        """
        if len(indexes) != len(values):
            raise ParameterValueFormatError("Length of values does not match length of indexes")
        if not indexes:
            raise ParameterValueFormatError("Empty time pattern not allowed")
        super().__init__(values, value_type=np.dtype(float), index_name=index_name)
        self.indexes = indexes

    def __eq__(self, other):
        if not isinstance(other, TimePattern):
            return NotImplemented
        return (
            self._indexes == other._indexes
            and np.all(self._values == other._values)
            and self.index_name == other.index_name
        )

    @IndexedValue.indexes.setter
    def indexes(self, indexes):
        self._indexes = _TimePatternIndexes(indexes, dtype=np.object_)

    @staticmethod
    def type_():
        return "time_pattern"

    def to_dict(self):
        value_dict = {"data": dict(zip(self._indexes, self._values))}
        if self.index_name != self.DEFAULT_INDEX_NAME:
            value_dict["index_name"] = self.index_name
        return value_dict


class TimeSeries(IndexedValue):
    """Base for all classes representing 'time_series' parameter values."""

    VALUE_TYPE = "time series"
    DEFAULT_INDEX_NAME = "t"

    def __init__(self, values, ignore_year, repeat, index_name=""):
        """
        :meta private:

        Args:
            values (Sequence): the values in the time-series.
            ignore_year (bool): True if the year should be ignored.
            repeat (bool): True if the series is repeating.
            index_name (str): index name.
        """
        if len(values) < 1:
            raise ParameterValueFormatError("Time series too short. Must have one or more values")
        super().__init__(values, value_type=np.dtype(float), index_name=index_name)
        self._ignore_year = ignore_year
        self._repeat = repeat

    def __len__(self):
        return len(self._values)

    @property
    def ignore_year(self):
        """Whether the year should be ignored.

        Returns:
            bool:
        """
        return self._ignore_year

    @ignore_year.setter
    def ignore_year(self, ignore_year):
        """Sets the ignore_year property.

        Args:
            bool: new value.
        """
        self._ignore_year = bool(ignore_year)

    @property
    def repeat(self):
        """Whether the series is repeating.

        Returns:
            bool:
        """
        return self._repeat

    @repeat.setter
    def repeat(self, repeat):
        """Sets the repeat property.

        Args:
            bool: new value.
        """
        self._repeat = bool(repeat)

    @staticmethod
    def type_():
        return "time_series"

    def to_dict(self):
        raise NotImplementedError()


class TimeSeriesFixedResolution(TimeSeries):
    """
    A parameter value of type 'time_series'.
    A mapping from time stamps to numerical values, with fixed durations between the time stamps.

    When getting the indexes the durations are applied cyclically.

    Currently, there is no support for the `ignore_year` and `repeat` options
    other than having getters for their values.
    """

    _memoized_indexes = {}

    def __init__(self, start, resolution, values, ignore_year, repeat, index_name=""):
        """
        Args:
            start (str or :class:`~datetime.datetime` or :class:`~numpy.datetime64`): the first time stamp
            resolution (str, :class:`~dateutil.relativedelta.relativedelta`, list): duration(s) between the time stamps.
            values (Sequence): the values in the time-series.
            ignore_year (bool): True if the year should be ignored.
            repeat (bool): True if the series is repeating.
            index_name (str): index name.
        """
        super().__init__(values, ignore_year, repeat, index_name)
        self._start = None
        self._resolution = None
        self.start = start
        self.resolution = resolution

    def __eq__(self, other):
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
        cycle_count = -(-len(self) // len(self.resolution))
        resolution = (cycle_count * self.resolution)[: len(self) - 1]
        resolution.insert(0, self._start)
        resolution_arr = np.array(resolution)
        memoized_indexes = self._memoized_indexes[key] = resolution_arr.cumsum().astype(_NUMPY_DATETIME_DTYPE)
        return memoized_indexes

    @property
    def indexes(self):
        if self._indexes is None:
            self.indexes = self._get_memoized_indexes()
        return IndexedValue.indexes.fget(self)

    @indexes.setter
    def indexes(self, indexes):
        # Needed because we redefine the setter
        self._indexes = _Indexes(indexes)

    @property
    def start(self):
        """Returns the start index.

        Returns:
            :class:`~numpy.datetime64`:
        """
        return self._start

    @start.setter
    def start(self, start):
        """
        Sets the start index.

        Args:
            start (:class:`~datetime.datetime` or :class:`~numpy.datetime64` or str): the start of the series
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
        """Returns the resolution as list of durations.

        Returns:
            list(:class:`Duration`):
        """
        return self._resolution

    @resolution.setter
    def resolution(self, resolution):
        """
        Sets the resolution.

        Args:
            resolution (str, :class:`~.dateutil.relativedelta.relativedelta`, list): resolution or a list thereof
        """
        if isinstance(resolution, str):
            resolution = [duration_to_relativedelta(resolution)]
        elif not isinstance(resolution, Sequence):
            resolution = [resolution]
        else:
            for i, r in enumerate(resolution):
                if isinstance(r, str):
                    resolution[i] = duration_to_relativedelta(r)
        if not resolution:
            raise ParameterValueFormatError("Resolution cannot be zero.")
        self._resolution = resolution
        self._indexes = None

    def to_dict(self):
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
        if self.index_name != self.DEFAULT_INDEX_NAME:
            value_dict["index_name"] = self.index_name
        return value_dict


class TimeSeriesVariableResolution(TimeSeries):
    """A parameter value of type 'time_series'.
    A mapping from time stamps to numerical values with arbitrary time steps.
    """

    def __init__(self, indexes, values, ignore_year, repeat, index_name=""):
        """
        Args:
            indexes (Sequence(:class:`~numpy.datetime64`)): the time stamps.
            values (Sequence): the value for each time stamp.
            ignore_year (bool): True if the year should be ignored.
            repeat (bool): True if the series is repeating.
            index_name (str): index name.
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
        value_dict = dict()
        value_dict["data"] = {str(index): float(value) for index, value in zip(self._indexes, self._values)}
        # Add "index" entry only if its contents are not set to their default values.
        if self._ignore_year:
            value_dict.setdefault("index", dict())["ignore_year"] = self._ignore_year
        if self._repeat:
            value_dict.setdefault("index", dict())["repeat"] = self._repeat
        if self.index_name != self.DEFAULT_INDEX_NAME:
            value_dict["index_name"] = self.index_name
        return value_dict


class Map(IndexedValue):
    """A parameter value of type 'map'. A mapping from key to value, where the values can be other instances
    of :class:`ParameterValue`.
    """

    VALUE_TYPE = "map"
    DEFAULT_INDEX_NAME = "x"

    def __init__(self, indexes, values, index_type=None, index_name=""):
        """
        Args:
            indexes (Sequence): the indexes in the map.
            values (Sequence): the value for each index.
            index_type (type or NoneType): index type or None to deduce from ``indexes``.
            index_name (str): index name.
        """
        if not indexes and index_type is None:
            raise ParameterValueFormatError("Cannot deduce index type from empty indexes list.")
        if indexes and index_type is not None and not isinstance(indexes[0], index_type):
            raise ParameterValueFormatError('Type of index does not match "index_type" argument.')
        if len(indexes) != len(values):
            raise ParameterValueFormatError("Length of values does not match length of indexes")
        super().__init__(values, index_name=index_name)
        self.indexes = indexes
        self._index_type = index_type if index_type is not None else type(indexes[0])
        self._values = values

    def __eq__(self, other):
        if not isinstance(other, Map):
            return NotImplemented
        return other._indexes == self._indexes and other._values == self._values and self.index_name == other.index_name

    @property
    def index_type(self):
        return self._index_type

    def is_nested(self):
        """Whether any of the values is also a map.

        Returns:
            bool:
        """
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
        value_dict = {
            "index_type": _map_index_type_to_database(self._index_type),
            "data": self.value_to_database_data(),
        }
        if self.index_name != self.DEFAULT_INDEX_NAME:
            value_dict["index_name"] = self.index_name
        return value_dict


def map_dimensions(map_):
    """Counts the dimensions in a map.

    :meta private:

    Args:
        map_ (:class:`Map`): the map to process.

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
    Converts leafs to specialized containers.

    Current conversion rules:

    - If the ``index_type`` is a :class:`DateTime` and all ``values`` are float,
      then the leaf is converted to a :class:`TimeSeries`.

    :meta private:

    Args:
        map_ (:class:`Map`): a map to process.

    Returns:
        :class:`IndexedValue`: a new map with leaves converted.
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

    If ``value`` is a :class:`Map` then converts leaf values into maps recursively.

    :meta private:

    Args:
        value (:class:`IndexedValue`): an indexed value to convert.

    Returns:
        :class:`Map`: converted Map
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

    :meta private:

    Args:
        map_ (:class:`Map`): map to convert.
        make_square (bool): if True, then pad rows with None so they all have the same length.
        row_this_far (list, optional): current row; used for recursion.
        empty (any, optional): object to fill empty cells with.

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
    Converts a :class:`Map` to a nested dictionary.

    :meta private:

    Args:
        map_ (:class:`Map`): map to convert

    Returns:
        dict:
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

    :meta private:

    Args:
        db_value (bytes): database value
        db_type (str, optional): value type

    Returns:
        str: parameter value as JSON with an additional ``type`` field.
    """
    try:
        parsed = load_db_value(db_value, db_type)
    except ParameterValueFormatError:
        parsed = None
    return json.dumps(parsed)


def split_value_and_type(value_and_type):
    """Splits the given string into value and type.

    :meta private:

    Args:
        value_and_type (str): a string joining value and type, as obtained by calling :func:`join_value_and_type`.

    Returns:
        tuple(bytes,str): database value and type.
    """
    try:
        parsed = json.loads(value_and_type)
    except (TypeError, json.JSONDecodeError):
        parsed = value_and_type
    return dump_db_value(parsed)


def deep_copy_value(value):
    """Copies a value.
    The operation is deep meaning that nested Maps will be copied as well.

    :meta private:

    Args:
        value (Any): value to copy

    Returns:
        Any: deep-copied value
    """
    if isinstance(value, (Number, str)) or value is None:
        return value
    if isinstance(value, Array):
        return Array(value.values, value.value_type, value.index_name)
    if isinstance(value, DateTime):
        return DateTime(value)
    if isinstance(value, Duration):
        return Duration(value)
    if isinstance(value, Map):
        return deep_copy_map(value)
    if isinstance(value, TimePattern):
        return TimePattern(value.indexes.copy(), value.values.copy(), value.index_name)
    if isinstance(value, TimeSeriesFixedResolution):
        return TimeSeriesFixedResolution(
            value.start, value.resolution, value.values.copy(), value.ignore_year, value.repeat, value.index_name
        )
    if isinstance(value, TimeSeriesVariableResolution):
        return TimeSeriesVariableResolution(
            value.indexes.copy(), value.values.copy(), value.ignore_year, value.repeat, value.index_name
        )
    raise ValueError("unknown value")


def deep_copy_map(value):
    """Deep copies a Map value.

    :meta private:

    Args:
        value (Map): Map to copy

    Returns:
        Map: deep-copied Map
    """
    xs = value.indexes.copy()
    ys = [deep_copy_value(y) for y in value.values]
    return Map(xs, ys, index_type=value.index_type, index_name=value.index_name)
