#############################################################################
# Copyright (C) 2017 - 2019 VTT Technical Research Centre of Finland
#
# This file is part of Spine Database API.
#
# Spine Spine Database API is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#############################################################################

"""
Support utilities and classes to deal with Spine data (relationship)
parameter values.

The `from_database` function reads the database's value format returning
a float, Datatime, Duration, TimePattern, TimeSeriesFixedResolution
or TimeSeriesVariableResolution objects.

The above objects can be converted back to the database format by the `to_database` free function
or by their `to_database` member functions.

Individual datetimes are represented as datetime objects from the standard Python library.
Individual time steps are represented as relativedelta objects from the dateutil package.
Datetime indexes (as returned by TimeSeries.indexes()) are represented as
numpy.array arrays holding numpy.datetime64 objects.

:author: A. Soininen (VTT)
:date:   3.6.2019
"""

from collections.abc import Iterable, Sequence
import json
from json.decoder import JSONDecodeError
import re
import dateutil.parser
from dateutil.relativedelta import relativedelta
import numpy as np
from exception import ParameterValueFormatError


# Defaulting to seconds precision in numpy.
_NUMPY_DATETIME_DTYPE = "datetime64[s]"
# Default start time guess, actual value not currently given in the JSON specification.
_TIME_SERIES_DEFAULT_START = "0001-01-01T00:00:00"
_TIME_SERIES_DEFAULT_RESOLUTION = "1h"


def duration_to_relativedelta(duration):
    """
    Converts a duration to a relativedelta object.

    Args:
        duration (str): a duration specification

    Returns:
        a relativedelta object corresponding to the given duration
    """
    count, abbreviation, full_unit = re.split("\\s|([a-z]|[A-Z])", duration, maxsplit=1)
    try:
        count = int(count)
    except ValueError:
        raise ParameterValueFormatError(
            'Could not parse duration "{}"'.format(duration)
        )
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
    raise ParameterValueFormatError('Could not parse duration "{}"'.format(duration))


def relativedelta_to_duration(delta):
    """
    Converts a relativedelta to duration.

    Args:
        delta (relativedelta): the relativedelta to convert

    Returns:
        a duration string
    """
    if delta.seconds > 0:
        return "{}s".format(delta.seconds)
    if delta.minutes > 0:
        return "{}m".format(delta.minutes)
    if delta.hours > 0:
        return "{}h".format(delta.hours)
    if delta.days > 0:
        return "{}D".format(delta.days)
    if delta.months > 0:
        return "{}M".format(delta.months)
    if delta.years > 0:
        return "{}Y".format(delta.years)
    raise ParameterValueFormatError("Zero relativedelta")


def from_database(database_value):
    """
    Converts a (relationship) parameter value from its database representation to a Python object.

    Args:
        database_value (str): a value in the database

    Returns:
        the encoded (relationship) parameter value
    """
    try:
        value = json.loads(database_value)
    except JSONDecodeError:
        raise ParameterValueFormatError("Could not decode the value")
    if isinstance(value, dict):
        try:
            value_type = value["type"]
            if value_type == "date_time":
                return _datetime_from_database(value["data"])
            if value_type == "duration":
                return _duration_from_database(value["data"])
            if value_type == "time_pattern":
                return _time_pattern_from_database(value)
            if value_type == "time_series":
                return _time_series_from_database(value)
            raise ParameterValueFormatError(
                'Unknown parameter value type "{}"'.format(value_type)
            )
        except KeyError as error:
            raise ParameterValueFormatError(
                "{} is missing in the parameter value description".format(error.args[0])
            )
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


def _break_dictionary(data):
    """Converts {"index": value} style dictionary into (list(indexes), list(values)) tuple."""
    indexes = list()
    values = list()
    for key, value in data.items():
        indexes.append(key)
        values.append(value)
    return indexes, values


def _datetime_from_database(value):
    """Converts a datetime database value into a DateTime object."""
    try:
        stamp = dateutil.parser.parse(value)
    except ValueError:
        raise ParameterValueFormatError(
            'Could not parse datetime from "{}"'.format(value)
        )
    return DateTime(stamp)


def _duration_from_database(value):
    """Converts a duration database value into a Duration object."""
    if isinstance(value, (str, int)):
        # Set default unit to minutes if value is a plain number.
        if not isinstance(value, str):
            value = "{}m".format(value)
        value = duration_to_relativedelta(value)
    elif isinstance(value, Sequence):  # It is a list of durations.
        # Set default unit to minutes for plain numbers in value.
        value = [v if isinstance(v, str) else "{}m".format(v) for v in value]
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
            ignore_year = bool(data_index["ignore_year"])
        except ValueError:
            raise ParameterValueFormatError(
                'Could not decode ignore_year from "{}"'.format(
                    data_index["ignore_year"]
                )
            )
        try:
            repeat = bool(data_index["repeat"])
        except ValueError:
            raise ParameterValueFormatError(
                'Could not decode repeat from "{}"'.format(data_index["repeat"])
            )
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
            stamp = np.datetime64(stamp)
        except ValueError:
            raise ParameterValueFormatError(
                'Could not decode time stamp "{}"'.format(stamp)
            )
        stamps.append(stamp)
        values[index] = series_value
    stamps = np.array(stamps)
    ignore_year, repeat = _variable_resolution_time_series_info_from_index(value)
    return TimeSeriesVariableResolution(stamps, values, ignore_year, repeat)


def _time_series_from_single_column(value):
    """Converts a compact JSON formatted time series into a TimeSeriesFixedResolution object."""
    if "index" in value:
        value_index = value["index"]
        start = (
            value_index["start"]
            if "start" in value_index
            else _TIME_SERIES_DEFAULT_START
        )
        resolution = (
            value_index["resolution"]
            if "resolution" in value_index
            else _TIME_SERIES_DEFAULT_RESOLUTION
        )
        if "ignore_year" in value_index:
            try:
                ignore_year = bool(value_index["ignore_year"])
            except ValueError:
                raise ParameterValueFormatError(
                    'Could not decode ignore_year value "{}"'.format(
                        value_index["ignore_year"]
                    )
                )
        else:
            ignore_year = "start" not in value_index
        if "repeat" in value_index:
            try:
                repeat = bool(value_index["repeat"])
            except ValueError:
                raise ParameterValueFormatError(
                    'Could not decode repeat value "{}"'.format(
                        value_index["ignore_year"]
                    )
                )
        else:
            repeat = "start" not in value_index
    else:
        start = _TIME_SERIES_DEFAULT_START
        resolution = _TIME_SERIES_DEFAULT_RESOLUTION
        ignore_year = True
        repeat = True
    if isinstance(resolution, str):
        resolution = [duration_to_relativedelta(resolution)]
    elif isinstance(resolution, Iterable):
        resolution = [duration_to_relativedelta(step) for step in resolution]
    else:
        raise ParameterValueFormatError(
            'Could not decode resolution "{}"'.format(resolution)
        )
    try:
        start = dateutil.parser.parse(start)
    except ValueError:
        raise ParameterValueFormatError(
            'Could not decode start value "{}"'.format(start)
        )
    values = np.array(value["data"])
    return TimeSeriesFixedResolution(start, resolution, values, ignore_year, repeat)


def _time_series_from_two_columns(value):
    """Converts a two column style time series into a TimeSeriesVariableResolution object."""
    data = value["data"]
    stamps = list()
    values = np.empty(len(data))
    for index, element in enumerate(data):
        if not isinstance(element, Sequence) or len(element) != 2:
            raise ParameterValueFormatError("Invalid value in time series array")
        try:
            stamp = np.datetime64(element[0])
        except ValueError:
            raise ParameterValueFormatError(
                'Could not decode time stamp "{}"'.format(element[0])
            )
        stamps.append(stamp)
        values[index] = element[1]
    stamps = np.array(stamps)
    ignore_year, repeat = _variable_resolution_time_series_info_from_index(value)
    return TimeSeriesVariableResolution(stamps, values, ignore_year, repeat)


def _time_pattern_from_database(value):
    """Converts a time pattern database value into a TimePattern object."""
    patterns, values = _break_dictionary(value["data"])
    return TimePattern(patterns, values)


class DateTime:
    """
    A single datetime value.

    Attributes:
        value (datetime.datetime): a timestamp
    """

    def __init__(self, value):
        self._value = value

    def to_database(self):
        """Returns the database representation of this object."""
        return json.dumps({"type": "date_time", "data": self._value.isoformat()})

    @property
    def value(self):
        """Returns the value as a datetime object."""
        return self._value


class Duration:
    """
    This class represents a duration in time.

    Attributes:
        value (relativedelta, list): a time step as a relativedelta or as list thereof
    """

    def __init__(self, value):
        self._value = value

    def to_database(self):
        """Returns the database representation of the duration."""
        if isinstance(self._value, Iterable):
            value = [relativedelta_to_duration(v) for v in self._value]
        else:
            value = relativedelta_to_duration(self._value)
        return json.dumps({"type": "duration", "data": value})

    @property
    def value(self):
        """Returns the duration as a relativedelta."""
        return self._value


class IndexedValue:
    """
    An abstract base class for indexed values.

    Attributes:
        values (numpy.array): the data array
    """

    def __init__(self, values):
        self._values = values

    def __len__(self):
        """Returns the length of the index"""
        return len(self.values)

    @property
    def indexes(self):
        """Returns the indexes as a numpy.array."""
        raise NotImplementedError()

    def to_database(self):
        """Return the database representation of the value."""
        raise NotImplementedError()

    @property
    def values(self):
        """Returns the data values as numpy.array."""
        return self._values


class TimeSeries(IndexedValue):
    """
    An abstract base class for time series.

    Attributes:
        values (numpy.array): an array of values
        ignore_year (bool): True if the year should be ignored in the time stamps
        repeat (bool): True if the series should be repeated from the beginning
    """

    def __init__(self, values, ignore_year, repeat):
        super().__init__(values)
        self._ignore_year = ignore_year
        self._repeat = repeat

    @property
    def indexes(self):
        """Returns the indexes as a numpy.array."""
        raise NotImplementedError()

    @property
    def ignore_year(self):
        """Returns True if the year should be ignored."""
        return self._ignore_year

    @property
    def repeat(self):
        """Returns True if the series should be repeated."""
        return self._repeat

    def to_database(self):
        """Return the database representation of the value."""
        raise NotImplementedError()


class TimePattern(IndexedValue):
    """
    Represents a time pattern (relationship) parameter value.

    Attributes:
        indexes (list): a list of time pattern strings
        values (list): a list of values corresponding to the time patterns
    """

    def __init__(self, indexes, values):
        if len(indexes) != len(values):
            raise RuntimeError("Length of values does not match length of indexes")
        super().__init__(values)
        self._indexes = indexes

    def to_database(self):
        """Returns the database representation of this time pattern."""
        data = dict()
        for index, value in zip(self._indexes, self._values):
            data[index] = value
        return json.dumps({"type": "time_pattern", "data": data})

    @property
    def indexes(self):
        """Returns the indexes."""
        return self._indexes


class TimeSeriesFixedResolution(TimeSeries):
    """
    A time series with fixed durations between the time stamps.

    When getting the indexes the durations are applied cyclically.

    Currently, there is no support for the `ignore_year` and `repeat` options
    other than having getters for their values.

    Attributes:
        start (str): the first time stamp as an ISO8601 string
        resolution (list): duration(s) between the time time stamps as a list of relativedeltas
        values (numpy.array): data values at each time stamp
        ignore_year (bool): whether or not the time-series should apply to any year
        repeat (bool): whether or not the time series should repeat cyclically
    """

    def __init__(self, start, resolution, values, ignore_year, repeat):
        super().__init__(values, ignore_year, repeat)
        self._start = start
        self._resolution = resolution

    @property
    def indexes(self):
        """Returns the time stamps as a numpy.array of numpy.datetime64 objects."""
        step_index = 0
        step_cycle_index = 0
        full_cycle_duration = sum(self._resolution, relativedelta())
        stamps = np.empty(len(self), dtype=_NUMPY_DATETIME_DTYPE)
        stamps[0] = self._start
        for stamp_index in range(1, len(self._values)):
            if step_index >= len(self._resolution):
                step_index = 0
                step_cycle_index += 1
            current_cycle_duration = sum(
                self._resolution[: step_index + 1], relativedelta()
            )
            duration_from_start = (
                step_cycle_index * full_cycle_duration + current_cycle_duration
            )
            stamps[stamp_index] = self._start + duration_from_start
            step_index += 1
        return np.array(stamps, dtype=_NUMPY_DATETIME_DTYPE)

    @property
    def start(self):
        """Returns the start index."""
        return self._start

    @property
    def resolution(self):
        """Returns the resolution as list of durations."""
        return self._resolution

    def to_database(self):
        """Returns the value in its database representation."""
        if len(self._resolution) > 1:
            resolution_as_json = [
                relativedelta_to_duration(step) for step in self._resolution
            ]
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
        indexes (numpy.array): time stamps as numpy.datetime64 objects
        values (numpy.array): the values corresponding to the time stamps
        ignore_year (bool): True if the stamp year should be ignored
        repeat (bool): True if the series should be repeated from the beginning
    """

    def __init__(self, indexes, values, ignore_year, repeat):
        super().__init__(values, ignore_year, repeat)
        if len(indexes) != len(values):
            raise RuntimeError("Length of values does not match length of indexes")
        self._indexes = indexes

    @property
    def indexes(self):
        """Returns the indexes."""
        return self._indexes

    def to_database(self):
        """Returns the value in its database representation"""
        database_value = {"type": "time_series"}
        data = dict()
        for index, value in zip(self._indexes, self._values):
            try:
                data[str(index)] = float(value)
            except ValueError:
                raise ParameterValueFormatError(
                    'Failed to convert "{}" to a float'.format(value)
                )
        database_value["data"] = data
        # Add "index" entry only if its contents are not set to their default values.
        if self._ignore_year:
            if "index" not in database_value:
                database_value["index"] = dict()
            database_value["index"]["ignore_year"] = self._ignore_year
        if self._repeat:
            if "index" not in database_value:
                database_value["index"] = dict()
            database_value["index"]["repeat"] = self._repeat()
        return json.dumps(database_value)
