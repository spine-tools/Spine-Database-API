######################################################################################################################
# Copyright (C) 2019 Spine project consortium
# This file is part of Spine Toolbox.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Spine Toolbox application main file.

:author: A. Soininen (VTT)
:date:   3.6.2019
"""

from collections import Iterable
from enum import auto, Enum, unique
import json
from json.decoder import JSONDecodeError
import numpy
import re


def time_series_as_numpy(data):
    """
    Converts a Spine time series into numpy arrays.

    Args:
        data (dict): A dict representation of the time series

    Returns:
        A tuple of (timestamps, values)
    """
    stamps = list()
    values = list()
    for key, value in data.items():
        stamps.append(numpy.datetime64(key))
        values.append(value)
    return numpy.array(stamps), numpy.array(values)


def timedelta(resolution):
    """Converts a duration into numpy.timedelta64 object."""
    if "S" in resolution or "second" in resolution:
        duration_type = "s"
    elif "H" in resolution or "hour":
        duration_type = "h"
    elif "d" in resolution or "day" in resolution:
        duration_type = "D"
    elif "m" in resolution or "month" in resolution:
        duration_type = "M"
    elif "y" in resolution or "Y" in resolution or "year" in resolution:
        duration_type = "Y"
    else:
        # Everything else is minutes.
        duration_type = "m"
    number = int(re.split("\\s|[a-zA-Z]]", resolution, maxsplit=1)[0])
    print(number)
    print(duration_type)
    return numpy.timedelta64(number, duration_type)



def dumps(s):
    """
    Convert numpy arrays into Spine time series.

    Args:
        s (iterable): an iterable of two numpy arrays

    Returns:
        A string representetation of the time series
    """
    data = dict()
    for stamp, value in zip(s[0], s[1]):
        data[str(stamp)] = value
    return json.dumps(data)


class ParameterType(Enum):
    """An enumeration for different value types."""
    SINGLE_VALUE = auto()
    TIME_PATTERN = auto()
    TIME_SERIES = auto()


class ValueDecodeError(Exception):
    """
    An exception raised when decoding a value fails.

    Attributes:
        expression (str): the string that could not be decoded
    """

    def __init__(self, expression):
        super().__init__()
        self._message = "Failed to decode expression {}".format(expression)

    @property
    def message(self):
        """Returns a message explaining the error."""
        return self._message


class ParameterValue:
    """
    A class to convert the JSON representation of (relationship) parameter values into other types.

    Currently supports conversion to numpy.

    Attributes:
        raw_value (str): Parameter's value as a JSON string
    """

    def __init__(self, raw_value):
        self._raw = raw_value
        value = json.loads(raw_value)
        if isinstance(value, dict):
            self._type = ParameterType.TIME_SERIES
            if "metadata" in value:
                metadata = value["metadata"]
                time = numpy.datetime64(metadata["start"])
                resolution = timedelta(metadata["resolution"])
                data = value["data"]
                times = list()
                for _ in range(len(data)):
                    times.append(numpy.datetime64(time))
                    time += resolution
                self._value = (numpy.array(times), numpy.array(data))
            else:
                self._value = time_series_as_numpy(value)
        else:
            self._type = ParameterType.SINGLE_VALUE
            self._value = value

    def __str__(self):
        """"Returns the JSON representation of the value."""
        return str(self._raw)

    def is_single_value(self):
        """Returns True if the value is a single value, False otherwise."""
        return self._type == ParameterType.SINGLE_VALUE

    def is_time_series(self):
        """Returns True if the value is a time series, False otherwise."""
        return self._type == ParameterType.TIME_SERIES

    def is_time_pattern(self):
        """Returns True if the value is a time pattern, False otherwise."""
        return self._type == ParameterType.TIME_PATTERN

    def as_numpy(self):
        """
        Returns the value as a numpy object.

        The return type depends on the value type:
            singe value: a number
            time series: a tuple (numpy.array(dtype=numpy.datetime64), numpy.array())
            time pattern: not yet implemented
        """
        return self._value

    @property
    def raw(self):
        """Gets the JSON string."""
        return self._raw

    @raw.setter
    def raw(self, raw_value):
        """Sets the JSON string"""
        self._value = None
        self._raw = raw_value

    def set(self, value):
        """
        Sets the value.

        Accepted inputs types:
            single value: a number
            time series: a tuple (numpy.array(dtype=numpy.datetime64), numpy.array())
            time patter: not yet implemented
        """
        try:
            if isinstance(value, Iterable) and len(value) == 2:
                self._raw = dumps(value)
            else:
                self._raw = json.dumps(value)
            self._value = value
        except JSONDecodeError:
            raise ValueDecodeError(value)