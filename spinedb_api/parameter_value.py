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

import json
import numpy
import re
from json.decoder import JSONDecodeError


def resolution_to_timedelta(resolution):
    """Converts a duration into numpy.timedelta64 object."""
    if "S" in resolution or "second" in resolution:
        duration_type = "s"
    elif "M" in resolution or "minute" in resolution:
        duration_type = "m"
    elif "H" in resolution or "hour" in resolution:
        duration_type = "h"
    elif "d" in resolution or "day" in resolution:
        duration_type = "D"
    elif "m" in resolution or "month" in resolution:
        duration_type = "M"
    elif "y" in resolution or "Y" in resolution or "year" in resolution:
        duration_type = "Y"
    else:
        raise RuntimeError("Unrecognized time duration format: {}".format(resolution))
    number = int(re.split("\\s|[a-z]|[A-Z]", resolution, maxsplit=1)[0])
    return numpy.timedelta64(number, duration_type)


def from_json(value):
    """
    Converts a (relationship) parameter value JSON string to a Python object.

    Single values are converted to floats,
    time series into VariableTimeSeries or FixedTimeSteps objects.

    Args:
        value (str): a JSON string to decode

    Returns:
        the encoded (relationship) parameter value
    """
    try:
        value = json.loads(value)
    except JSONDecodeError:
        raise ValueDecodeError(value)
    if isinstance(value, dict):
        if "metadata" in value:
            metadata = value["metadata"]
            start = metadata["start"]
            resolution = metadata["resolution"]
            values = numpy.array(value["data"])
            return FixedTimeSteps(start, len(values), resolution, values)
        stamps = list()
        values = list()
        for key, value in value.items():
            stamps.append(numpy.datetime64(key))
            values.append(value)
        values = numpy.array(values)
        return VariableTimeSteps(stamps, values)
    return value


class VariableTimeSteps:
    """
    Holds variable time step time series information.

    Attributes:
        stamps (numpy.array): time stamps as a numpy.datetime64 array
        values (numpy.array): values as a numpy array
    """

    def __init__(self, stamps, values):
        self._stamps = stamps
        self._values = values

    def as_json(self):
        """Returns the value as a JSON string"""
        data = dict()
        for stamp, value in zip(self._stamps, self._values):
            try:
                value = float(value)
            except ValueError:
                raise ValueEncodeError()
            data[str(stamp)] = value
        return json.dumps(data)

    @property
    def stamps(self):
        return self._stamps

    @property
    def values(self):
        return self._values


class FixedTimeSteps:
    """
    Holds fixed time step time series information.

    Attributes:
        start (numpy.datetime64): start time for the series
        length (int): number of steps in the series
        resolution (numpy.timedelta64): time difference between two time steps
        values (numpy.array): data values for each time step
    """

    def __init__(self, start, length, resolution, values):
        self._start = start
        self._length = length
        self._resolution = resolution
        self._values = values

    def as_json(self):
        """Returns the value as a JSON string."""
        return json.dumps(
            {
                "metadata": {
                    "start": self.start,
                    "resolution": self.resolution,
                    "length": self.length,
                },
                "data": self.values.tolist(),
            }
        )

    @property
    def start(self):
        return self._start

    @property
    def length(self):
        return self._length

    @property
    def resolution(self):
        return self._resolution

    @property
    def stamps(self):
        """"Returns the time stamps as numpy.array of numpy.datetime64 values."""
        start = numpy.datetime64(self._start)
        resolution = resolution_to_timedelta(self._resolution)
        end = start + (self._length + 0.5) * resolution
        return numpy.arange(start, end, resolution)

    @property
    def values(self):
        return self._values

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

class ValueEncodeError(Exception):
    """An exception raised when encoding a value fails."""