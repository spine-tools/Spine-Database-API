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
from dateutil.relativedelta import relativedelta
import numpy.testing
from spinedb_api.parameter_value import (
    duration_to_relativedelta,
    from_database,
    TimeSeriesFixedStep,
    TimeSeriesVariableStep,
)


class TestParameterValue(unittest.TestCase):
    """Test for the free functions and classes in parameter_value."""

    def test_duration_to_relativedelta_seconds(self):
        delta = duration_to_relativedelta("7s")
        self.assertEqual(delta, relativedelta(seconds=7))
        delta = duration_to_relativedelta("1 second")
        self.assertEqual(delta, relativedelta(seconds=1))
        delta = duration_to_relativedelta("7 seconds")
        self.assertEqual(delta, relativedelta(seconds=7))

    def test_duration_to_relativedelta_minutes(self):
        delta = duration_to_relativedelta("7m")
        self.assertEqual(delta, relativedelta(minutes=7))
        delta = duration_to_relativedelta("1 minute")
        self.assertEqual(delta, relativedelta(minutes=1))
        delta = duration_to_relativedelta("7 minutes")
        self.assertEqual(delta, relativedelta(minutes=7))

    def test_duration_to_relativedelta_hours(self):
        delta = duration_to_relativedelta("7h")
        self.assertEqual(delta, relativedelta(hours=7))
        delta = duration_to_relativedelta("1 hour")
        self.assertEqual(delta, relativedelta(hours=1))
        delta = duration_to_relativedelta("7 hours")
        self.assertEqual(delta, relativedelta(hours=7))

    def test_duration_to_relativedelta_days(self):
        delta = duration_to_relativedelta("7D")
        self.assertEqual(delta, relativedelta(days=7))
        delta = duration_to_relativedelta("1 day")
        self.assertEqual(delta, relativedelta(days=1))
        delta = duration_to_relativedelta("7 days")
        self.assertEqual(delta, relativedelta(days=7))

    def test_duration_to_relativedelta_months(self):
        delta = duration_to_relativedelta("7M")
        self.assertEqual(delta, relativedelta(months=7))
        delta = duration_to_relativedelta("1 month")
        self.assertEqual(delta, relativedelta(months=1))
        delta = duration_to_relativedelta("7 months")
        self.assertEqual(delta, relativedelta(months=7))

    def test_duration_to_relativedelta_years(self):
        delta = duration_to_relativedelta("7Y")
        self.assertEqual(delta, relativedelta(years=7))
        delta = duration_to_relativedelta("7Y")
        self.assertEqual(delta, relativedelta(years=7))
        delta = duration_to_relativedelta("1 year")
        self.assertEqual(delta, relativedelta(years=1))
        delta = duration_to_relativedelta("7 years")
        self.assertEqual(delta, relativedelta(years=7))

    def test_from_database_TimeSeriesVariableStep(self):
        releases = '''{
                          "type": "time_series",
                          "data": {
                              "1977-05-25": 4,
                              "1980-05-21": 5,
                              "1983-05-25": 6
                          }
                      }'''
        time_series = from_database(releases)
        numpy.testing.assert_equal(
            time_series.indexes,
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

    def test_TimeSeriesVariableStep_to_database(self):
        dates = numpy.array(
            ["1999-05-19", "2002-05-16", "2005-05-19"], dtype="datetime64[D]"
        )
        episodes = numpy.array([1, 2, 3], dtype=float)
        value = TimeSeriesVariableStep(dates, episodes)
        as_json = value.to_database()
        releases = json.loads(as_json)
        self.assertEqual(releases, {"1999-05-19": 1, "2002-05-16": 2, "2005-05-19": 3})

    def test_from_database_TimeSeriesFixedStep(self):
        days_of_our_lives = """{
                                   "type": "time_series",
                                   "index": {
                                       "start": "2019-03-23",
                                       "resolution": "1 day",
                                       "ignore_year": false,
                                       "repeat": false
                                   },
                                   "data": [7.0, 5.0, 8.1]
                               }"""
        time_series = from_database(days_of_our_lives)
        numpy.testing.assert_equal(
            time_series.indexes,
            numpy.array(
                [
                    numpy.datetime64("2019-03-23"),
                    numpy.datetime64("2019-03-24"),
                    numpy.datetime64("2019-03-25"),
                ],
                dtype="datetime64[s]",
            ),
        )
        numpy.testing.assert_equal(time_series.values, numpy.array([7.0, 5.0, 8.1]))
        self.assertEqual(time_series.start, "2019-03-23")
        self.assertEqual(time_series.step, "1 day")
        self.assertFalse(time_series.ignore_year)
        self.assertFalse(time_series.repeat)

    def test_TimeSeriesFixedStep_to_database(self):
        values = numpy.array([3, 2, 4], dtype=float)
        value = TimeSeriesFixedStep("2007-06", "1 months", values, True, True)
        as_json = value.to_database()
        releases = json.loads(as_json)
        self.assertEqual(
            releases,
            {
                "type": "time_series",
                "index": {"start": "2007-06", "resolution": "1 months", "ignore_year": True, "repeat": True},
                "data": [3, 2, 4],
            },
        )


if __name__ == "__main__":
    unittest.main()
