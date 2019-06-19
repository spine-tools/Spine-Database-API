######################################################################################################################
# Copyright (C) 2017 - 2019 Spine project consortium
# This file is part of Spine Toolbox.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Tests for the parameter_value module.

:authors: A. Soininen (VTT)
:date:   7.6.2019
"""

import json
import unittest
import numpy.testing
from spinedb_api.parameter_value import (
    FixedTimeSteps,
    from_json,
    resolution_to_timedelta,
    VariableTimeSteps,
)


class TestParameterValue(unittest.TestCase):
    """Test for the free functions and classes in parameter_value."""

    def test_resolution_to_timedelta_seconds(self):
        delta = resolution_to_timedelta("7S")
        self.assertEqual(delta, numpy.timedelta64(7, "s"))
        delta = resolution_to_timedelta("1 second")
        self.assertEqual(delta, numpy.timedelta64(1, "s"))
        delta = resolution_to_timedelta("7 seconds")
        self.assertEqual(delta, numpy.timedelta64(7, "s"))

    def test_resolution_to_timedelta_minutes(self):
        delta = resolution_to_timedelta("7M")
        self.assertEqual(delta, numpy.timedelta64(7, "m"))
        delta = resolution_to_timedelta("1 minute")
        self.assertEqual(delta, numpy.timedelta64(1, "m"))
        delta = resolution_to_timedelta("7 minutes")
        self.assertEqual(delta, numpy.timedelta64(7, "m"))

    def test_resolution_to_timedelta_hours(self):
        delta = resolution_to_timedelta("7H")
        self.assertEqual(delta, numpy.timedelta64(7, "h"))
        delta = resolution_to_timedelta("1 hour")
        self.assertEqual(delta, numpy.timedelta64(1, "h"))
        delta = resolution_to_timedelta("7 hours")
        self.assertEqual(delta, numpy.timedelta64(7, "h"))

    def test_resolution_to_timedelta_days(self):
        delta = resolution_to_timedelta("7d")
        self.assertEqual(delta, numpy.timedelta64(7, "D"))
        delta = resolution_to_timedelta("1 day")
        self.assertEqual(delta, numpy.timedelta64(1, "D"))
        delta = resolution_to_timedelta("7 days")
        self.assertEqual(delta, numpy.timedelta64(7, "D"))

    def test_resolution_to_timedelta_months(self):
        delta = resolution_to_timedelta("7m")
        self.assertEqual(delta, numpy.timedelta64(7, "M"))
        delta = resolution_to_timedelta("1 month")
        self.assertEqual(delta, numpy.timedelta64(1, "M"))
        delta = resolution_to_timedelta("7 months")
        self.assertEqual(delta, numpy.timedelta64(7, "M"))

    def test_resolution_to_timedelta_years(self):
        delta = resolution_to_timedelta("7y")
        self.assertEqual(delta, numpy.timedelta64(7, "Y"))
        delta = resolution_to_timedelta("7Y")
        self.assertEqual(delta, numpy.timedelta64(7, "Y"))
        delta = resolution_to_timedelta("1 year")
        self.assertEqual(delta, numpy.timedelta64(1, "Y"))
        delta = resolution_to_timedelta("7 years")
        self.assertEqual(delta, numpy.timedelta64(7, "Y"))

    def test_from_json_VariableTimeSeries(self):
        releases = '{"1977-05-25": 4, "1980-05-21": 5, "1983-05-25": 6}'
        time_series = from_json(releases)
        numpy.testing.assert_equal(
            time_series.stamps,
            numpy.array(
                [
                    numpy.datetime64("1977-05-25"),
                    numpy.datetime64("1980-05-21"),
                    numpy.datetime64("1983-05-25"),
                ],
                dtype="datetime64[D]",
            ),
        )
        numpy.testing.assert_equal(time_series.values, numpy.array([4, 5, 6]))

    def test_VariableTimeSteps_to_json(self):
        dates = numpy.array(
            ["1999-05-19", "2002-05-16", "2005-05-19"], dtype="datetime64[D]"
        )
        episodes = numpy.array([1, 2, 3], dtype=float)
        value = VariableTimeSteps(dates, episodes)
        as_json = value.as_json()
        releases = json.loads(as_json)
        self.assertEqual(releases, {"1999-05-19": 1, "2002-05-16": 2, "2005-05-19": 3})

    def test_from_json_FixedTimeSteps(self):
        days_of_our_lives = '{"metadata": {"start": "2019-03-23", "length": 3, "resolution": "1 day"}, "data": [7.0, 5.0, 8.1]}'
        time_series = from_json(days_of_our_lives)
        numpy.testing.assert_equal(
            time_series.stamps,
            numpy.array(
                [
                    numpy.datetime64("2019-03-23"),
                    numpy.datetime64("2019-03-24"),
                    numpy.datetime64("2019-03-25"),
                ],
                dtype="datetime64[D]",
            ),
        )
        numpy.testing.assert_equal(time_series.values, numpy.array([7.0, 5.0, 8.1]))

    def test_FixedTimeSteps_to_json(self):
        values = numpy.array([3, 2, 4], dtype=float)
        value = FixedTimeSteps("2007-06", 3, "1 months", values)
        as_json = value.as_json()
        releases = json.loads(as_json)
        self.assertEqual(
            releases,
            {
                "metadata": {"start": "2007-06", "length": 3, "resolution": "1 months"},
                "data": [3, 2, 4],
            },
        )


if __name__ == "__main__":
    unittest.main()
