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

Individual datetimes are represented as datetime objects from the standard Python library.
Individual time steps are represented as relativedelta objects from the dateutils package.
Lists of datetimes (as time series time stamps or indices) are represented as
numpy.array arrays holding numpy.datetime64 objects.

This module is currently missing proper handling of time patterns
which are represented as strings in the database format.

:author: A. Soininen (VTT)
:date:   3.6.2019
"""

from collections import Iterable
from datetime import datetime
import json
from json.decoder import JSONDecodeError
import re
from dateutil.relativedelta import relativedelta
import numpy as np

# Defaulting to seconds precision in numpy.
_NUMPY_DATETIME_DTYPE = "datetime64[s]"


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
        raise ParameterValueError('Could not parse duration "{}"'.format(duration))
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
    raise ParameterValueError('Could not parse duration "{}"'.format(duration))


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
    raise ParameterValueError("Zero relativedelta")

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
        raise ParameterValueError("Could not decode the value")
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
            raise ParameterValueError(
                'Unknown parameter value type "{}"'.format(value_type)
            )
        except KeyError as error:
            raise ParameterValueError(
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
        stamp = datetime.fromisoformat(value)
    except ValueError:
        raise ParameterValueError(
            'Could not parse datetime from "{}"'.format(value)
        )
    return DateTime(stamp)


def _duration_from_database(value):
    """Converts a duration database value into a Duration object."""
    if isinstance(value, str) or isinstance(value, int):
        # Set default unit to minutes if value is a plain number.
        if not isinstance(value, str):
            value = "{}m".format(value)
        value = duration_to_relativedelta(value)
    elif isinstance(value, Iterable):  # It is a list of durations.
        # Set default unit to minutes for plain numbers in value.
        value = [v if isinstance(v, str) else "{}m".format(v) for v in value]
        value = [duration_to_relativedelta(v) for v in value]
    else:
        raise ParameterValueError("Duration value is of unsupported type")
    return Duration(value)


def _time_series_from_database(value):
    """Converts a time series database value into a time series object."""
    data = value["data"]
    if "index" in value:
        value_index = value["index"]
        start = (
            value_index["start"] if "start" in value_index else "0001-01-01T00:00:00"
        )
        try:
            start = datetime.fromisoformat(start)
        except ValueError:
            raise ParameterValueError("Could not decode start value {}".format(start))
        resolution = (
            value_index["resolution"] if "resolution" in value_index else "1 hour"
        )
        resolution = duration_to_relativedelta(resolution)
        if "ignore_year" in value_index:
            ignore_year = value_index["ignore_year"]
        else:
            ignore_year = not "start" in value_index
        repeat = value_index["repeat"]
        return TimeSeriesFixedStep(start, resolution, data, ignore_year, repeat)
    if isinstance(data, dict):
        stamps = list()
        values = list()
        for key, value in data.items():
            try:
                stamp = np.datetime64(key)
            except ValueError:
                raise ParameterValueError(
                    'Could not decode time stamp "{}"'.format(stamp)
                )
            stamps.append(stamp)
            values.append(value)
        values = np.array(values)
        return TimeSeriesVariableStep(stamps, values)
    if isinstance(data[0], list):
        # Generalized index
        pass


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
        value (step, list): a time step as a single string or as list of strings
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
        """Returns the duration as a string."""
        return self._value


class IndexedValue:
    """
    An abstract base class for indexed values.

    Attributes:
        values (numpy.array): the data array
    """

    def __init__(self, values):
        self._values = values

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


class IndexedValueFixedStep(IndexedValue):
    """
    Holds data with fixed step index.

    Attributes:
        start: the first index
        step: the difference between consecutive indexes
        values (numpy.array): data values for each time step
    """

    def __init__(self, start, step, values):
        super().__init__(values)
        self._start = start
        self._step = step

    def to_database(self):
        """Returns the value as in its database representation."""
        raise NotImplementedError(
            "Database format for generalized indexes does not exist yet."
        )

    @property
    def start(self):
        """Returns the start index."""
        return self._start

    @property
    def length(self):
        """Returns the length of the data array."""
        return len(self._values)

    @property
    def step(self):
        """Returns the step size."""
        return self._step

    @property
    def indexes(self):
        """"Returns the time stamps as numpy.array of numpy.datetime64 values."""
        end = self._start + self.length * self._step
        return np.arange(self._start, end, self._step)


class IndexedValueVariableStep(IndexedValue):
    """
    Holds data with generalized index.

    Attributes:
        indexes (numpy.array): time stamps as a numpy.datetime64 array
        values (numpy.array): values as a numpy array
    """

    def __init__(self, indexes, values):
        if len(indexes) != len(values):
            raise RuntimeError("Length of values does not match length of indexes")
        super().__init__(values)
        self._indexes = indexes

    def to_database(self):
        """Returns the value in its database representation"""
        data = dict()
        for index, value in zip(self._indexes, self._values):
            try:
                data[str(index)] = float(value)
            except ValueError:
                raise ParameterValueError(
                    'Failed to convert "{}" to a float'.format(value)
                )
        return json.dumps(data)

    @property
    def indexes(self):
        """Returns the indexes."""
        return self._indexes


class TimePattern(IndexedValueVariableStep):
    """
    Represents a time pattern (relationship) parameter value.

    Attributes:
        indexes (list): a list of time pattern strings
        values (list): a list of values corresponding to the time patterns
    """

    def __init__(self, indexes, values):
        super().__init__(indexes, values)

    def to_database(self):
        """Returns the database representation of this time pattern."""
        data = dict()
        for index, value in zip(self._indexes, self._values):
            data[index] = value
        return json.dumps({"type": "time_pattern", "data": data})


class TimeSeriesFixedStep(IndexedValueFixedStep):
    """
    A time series with fixed durations between the time stamps.

    Currently, there is no support for the `ignore_year` and `repeat` options
    other than having getters for their values.

    Attributes:
        start (str): the first time stamp as an ISO8601 string
        step (str, list): duration(s) between the time time stamps
        values (numpy.array): data values at each time stamp
        ignore_year (bool): whether or not the time-series should apply to any year
        repeat (bool): whether or not the time series should repeat cyclically
    """

    def __init__(self, start, step, values, ignore_year, repeat):
        super().__init__(start, step, values)
        self._ignore_year = ignore_year
        self._repeat = repeat

    @property
    def ignore_year(self):
        """Returns True if the year should be ignored."""
        return self._ignore_year

    @property
    def indexes(self):
        """Returns the time stamps as a numpy.array of numpy.datetime64 objects."""
        stamps = [self._start + i * self._step for i in range(self.length)]
        return np.array(stamps, dtype=_NUMPY_DATETIME_DTYPE)

    @property
    def repeat(self):
        """Returns True if this time series should be repeated."""
        return self._repeat

    def to_database(self):
        """Returns the value in its database representation."""
        return json.dumps(
            {
                "type": "time_series",
                "index": {
                    "start": str(self._start),
                    "resolution": self._step,
                    "ignore_year": self._ignore_year,
                    "repeat": self._repeat,
                },
                "data": self._values.tolist(),
            }
        )


class TimeSeriesVariableStep(IndexedValueVariableStep):
    """
    A class representing time series data with variable time step.
    """

    def __init__(self, indexes, values):
        super().__init__(indexes, values)

    def to_database(self):
        """Returns the value in its database representation"""
        data = dict()
        for index, value in zip(self._indexes, self._values):
            try:
                data[str(index)] = float(value)
            except ValueError:
                raise ParameterValueError(
                    'Failed to convert "{}" to a float'.format(value)
                )
        return json.dumps(data)


class ParameterValueError(Exception):
    """
    An exception raised when encoding/decoding a value fails.

    Attributes:
        message (str): an error message
    """

    def __init__(self, message):
        super().__init__()
        self._message = message

    @property
    def message(self):
        """Returns a message explaining the error."""
        return self._message
