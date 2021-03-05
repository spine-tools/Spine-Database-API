######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
"""
Contains unit tests for the ``pivot`` module.

:author: A. Soininen (VTT)
:date:   1.2.2021
"""
import unittest
from spinedb_api.export_mapping.pivot import make_pivot


class TestPivot(unittest.TestCase):
    def test_pivot(self):
        table = [
            ["A", "a", "1", -1.1],
            ["A", "a", "2", -2.2],
            ["A", "b", "1", -3.3],
            ["A", "b", "2", -4.4],
            ["A", "b", "3", -5.5],
            ["B", "a", "2", -6.6],
            ["B", "b", "2", -7.7],
            ["B", "c", "3", -8.8],
            ["C", "a", "1", -9.9],
        ]
        pivot_table = list(make_pivot(table, 3, [0], [1], [2]))
        expected = [
            [None, "1", "2", "3"],
            ["A", -1.1, -2.2, None],
            ["A", -3.3, -4.4, -5.5],
            ["B", None, -6.6, None],
            ["B", None, -7.7, None],
            ["B", None, None, -8.8],
            ["C", -9.9, None, None],
        ]
        self.assertEqual(pivot_table, expected)

    def test_half_pivot(self):
        table = [
            ["A", "a", "1", -1.1],
            ["A", "a", "2", -2.2],
            ["A", "b", "1", -3.3],
            ["A", "b", "2", -4.4],
            ["A", "b", "3", -5.5],
            ["B", "a", "2", -6.6],
            ["B", "b", "2", -7.7],
            ["B", "c", "3", -8.8],
            ["C", "a", "1", -9.9],
        ]
        pivot_table = list(make_pivot(table, 3, [], [], [0, 1, 2]))
        expected = [
            ["A", "A", "A", "A", "A", "B", "B", "B", "C"],
            ["a", "a", "b", "b", "b", "a", "b", "c", "a"],
            ["1", "2", "1", "2", "3", "2", "2", "3", "1"],
            [-1.1, -2.2, -3.3, -4.4, -5.5, -6.6, -7.7, -8.8, -9.9],
        ]
        self.assertEqual(pivot_table, expected)


if __name__ == "__main__":
    unittest.main()