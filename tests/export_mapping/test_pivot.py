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
Contains unit tests for the ``pivot`` module.

"""
import unittest
import numpy
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
        pivot_table = list(make_pivot(table, ["H", "h", "#", "xx"], 3, [0], [1], [2]))
        expected = [
            ["H", "1", "2", "3"],
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
        pivot_table = list(make_pivot(table, ["H", "h", "#", "xx"], 3, [], [], [0, 1, 2]))
        expected = [
            ["H", "A", "A", "A", "A", "A", "B", "B", "B", "C"],
            ["h", "a", "a", "b", "b", "b", "a", "b", "c", "a"],
            ["#", "1", "2", "1", "2", "3", "2", "2", "3", "1"],
            [None, -1.1, -2.2, -3.3, -4.4, -5.5, -6.6, -7.7, -8.8, -9.9],
        ]
        self.assertEqual(pivot_table, expected)

    def test_pivot_group_concat(self):
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
        pivot_table = list(make_pivot(table, ["H", "h", "#", "xx"], 3, [0], [1], [2], "concat"))
        expected = [
            ["H", "1", "2", "3"],
            ["A", "-1.1,-3.3", "-2.2,-4.4", "-5.5"],
            ["B", "", "-6.6,-7.7", "-8.8"],
            ["C", "-9.9", "", ""],
        ]
        self.assertEqual(pivot_table, expected)

    def test_pivot_group_sum(self):
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
        pivot_table = list(make_pivot(table, ["H", "h", "#", "xx"], 3, [0], [1], [2], "sum"))
        expected = [
            ["H", "1", "2", "3"],
            ["A", -1.1 - 3.3, -2.2 - 4.4, -5.5],
            ["B", numpy.nan, -6.6 - 7.7, -8.8],
            ["C", -9.9, numpy.nan, numpy.nan],
        ]
        self.assertEqual(pivot_table, expected)

    def test_pivot_group_mean(self):
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
        pivot_table = list(make_pivot(table, ["H", "h", "#", "xx"], 3, [0], [1], [2], "mean"))
        expected = [
            ["H", "1", "2", "3"],
            ["A", (-1.1 - 3.3) / 2.0, (-2.2 - 4.4) / 2.0, -5.5],
            ["B", numpy.nan, (-6.6 - 7.7) / 2.0, -8.8],
            ["C", -9.9, numpy.nan, numpy.nan],
        ]
        self.assertEqual(pivot_table, expected)

    def test_pivot_group_min(self):
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
        pivot_table = list(make_pivot(table, ["H", "h", "#", "xx"], 3, [0], [1], [2], "min"))
        expected = [
            ["H", "1", "2", "3"],
            ["A", -3.3, -4.4, -5.5],
            ["B", numpy.nan, -7.7, -8.8],
            ["C", -9.9, numpy.nan, numpy.nan],
        ]
        self.assertEqual(pivot_table, expected)

    def test_pivot_group_max(self):
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
        pivot_table = list(make_pivot(table, ["H", "h", "#", "xx"], 3, [0], [1], [2], "max"))
        expected = [
            ["H", "1", "2", "3"],
            ["A", -1.1, -2.2, -5.5],
            ["B", numpy.nan, -6.6, -8.8],
            ["C", -9.9, numpy.nan, numpy.nan],
        ]
        self.assertEqual(pivot_table, expected)

    def test_pivot_group_one_or_none(self):
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
        pivot_table = list(make_pivot(table, ["H", "h", "#", "xx"], 3, [0], [1], [2], "one_or_none"))
        expected = [["H", "1", "2", "3"], ["A", None, None, -5.5], ["B", None, None, -8.8], ["C", -9.9, None, None]]
        self.assertEqual(pivot_table, expected)

    def test_Nones_in_regular_keys(self):
        table = [
            ["A", "a", "1", -1.1],
            ["A", "a", "2", -2.2],
            ["A", "b", "1", -3.3],
            ["A", "b", "2", -4.4],
            ["A", "b", "3", -5.5],
            [None, "a", "2", -6.6],
            [None, "b", "2", -7.7],
            [None, "c", "3", -8.8],
            ["C", "a", "1", -9.9],
        ]
        pivot_table = list(make_pivot(table, None, 3, [0], [1], [2]))
        expected = [
            [None, "1", "2", "3"],
            [None, None, -6.6, None],
            [None, None, -7.7, None],
            [None, None, None, -8.8],
            ["A", -1.1, -2.2, None],
            ["A", -3.3, -4.4, -5.5],
            ["C", -9.9, None, None],
        ]
        self.assertEqual(pivot_table, expected)

    def test_Nones_in_pivot_keys(self):
        table = [
            ["A", "a", "1", -1.1],
            ["A", "a", None, -2.2],
            ["A", "b", "1", -3.3],
            ["A", "b", None, -4.4],
            ["A", "b", "3", -5.5],
            ["B", "a", None, -6.6],
            ["B", "b", None, -7.7],
            ["B", "c", "3", -8.8],
            ["C", "a", "1", -9.9],
        ]
        pivot_table = list(make_pivot(table, None, 3, [0], [1], [2]))
        expected = [
            [None, None, "1", "3"],
            ["A", -2.2, -1.1, None],
            ["A", -4.4, -3.3, -5.5],
            ["B", -6.6, None, None],
            ["B", -7.7, None, None],
            ["B", None, None, -8.8],
            ["C", None, -9.9, None],
        ]
        self.assertEqual(pivot_table, expected)


if __name__ == "__main__":
    unittest.main()
