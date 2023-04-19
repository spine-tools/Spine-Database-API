######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Unit tests for helpers.py.

"""


import unittest
from spinedb_api.helpers import compare_schemas, create_new_spine_database


class TestHelpers(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_same_schema(self):
        """Test that importing object class works"""
        engine1 = create_new_spine_database('sqlite://')
        engine2 = create_new_spine_database('sqlite://')
        self.assertTrue(compare_schemas(engine1, engine2))

    def test_different_schema(self):
        """Test that importing object class works"""
        engine1 = create_new_spine_database('sqlite://')
        engine2 = create_new_spine_database('sqlite://')
        engine2.execute("drop table entity_type")
        self.assertFalse(compare_schemas(engine1, engine2))


if __name__ == "__main__":
    unittest.main()
