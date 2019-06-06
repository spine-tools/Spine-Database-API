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
from spinedb_api.parameter_value import dumps, time_series_as_numpy


class TestParameterValue(unittest.TestCase):
    """Test for the free functions and classes in parameter_value."""

    def test_time_series_as_numpy(self):
        releases = {"1977-05-25": 4, "1980-05-21": 5, "1983-05-25": 6}
        converted = time_series_as_numpy(releases)
        self.assertEqual(len(converted), 2)
        numpy.testing.assert_equal(
            converted[0],
            numpy.array(
                ["1977-05-25", "1980-05-21", "1983-05-25"], dtype="datetime64[D]"
            ),
        )
        numpy.testing.assert_equal(converted[1], numpy.array([4, 5, 6]))

    def test_dumps(self):
        dates = numpy.array(["1999-05-19", "2002-05-16", "2005-05-19"], dtype="datetime64[D]")
        episodes = numpy.array([1, 2, 3], dtype=float)
        as_numpy = (dates, episodes)
        as_json = dumps(as_numpy)
        releases = json.loads(as_json)
        self.assertEqual(releases, {"1999-05-19": 1, "2002-05-16": 2, "2005-05-19": 3})


if __name__ == "__main__":
    unittest.main()
