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
Unit tests for :mod:`spinedb_api.mapping`.

:author: A. Soininen (VTT)
:date:   12.5.2021
"""
import unittest
from spinedb_api.mapping import Mapping, Position, value_index


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


if __name__ == "__main__":
    unittest.main()
