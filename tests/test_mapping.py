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
Unit tests for :mod:`spinedb_api.mapping`.

"""
import unittest
from spinedb_api.mapping import Mapping, Position, value_index, unflatten


class TestMapping(unittest.TestCase):
    def test_value_index(self):
        mapping = Mapping(0)
        self.assertEqual(value_index(mapping.flatten()), 0)
        mapping.position = Position.hidden
        self.assertEqual(value_index(mapping.flatten()), -1)
        mapping.child = Mapping(0)
        self.assertEqual(value_index(mapping.flatten()), 1)
        mapping.child.position = Position.hidden
        self.assertEqual(value_index(mapping.flatten()), -1)
        mapping.position = 0
        self.assertEqual(value_index(mapping.flatten()), 0)

    def test_non_pivoted_columns(self):
        root_mapping = unflatten([Mapping(5), Mapping(Position.hidden)])
        self.assertEqual(root_mapping.non_pivoted_columns(), [5])

    def test_non_pivoted_columns_when_non_tail_mapping_is_pivoted(self):
        root_mapping = unflatten([Mapping(5), Mapping(Position.hidden), Mapping(-1), Mapping(13), Mapping(23)])
        self.assertEqual(root_mapping.non_pivoted_columns(), [5, 13])

    def test_is_pivoted_returns_true_when_position_is_pivoted(self):
        mapping = Mapping(-1)
        self.assertTrue(mapping.is_pivoted())

    def test_is_pivoted_returns_false_when_all_mappings_are_non_pivoted(self):
        mappings = [Mapping(0), Mapping(1)]
        root = unflatten(mappings)
        self.assertFalse(root.is_pivoted())


if __name__ == "__main__":
    unittest.main()
