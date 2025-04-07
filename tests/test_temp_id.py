######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Database API contributors
# This file is part of Spine Database API.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
import unittest
from spinedb_api.temp_id import TempId


class TestTempId(unittest.TestCase):
    def test_item_type(self):
        id_lookup = {}
        temp_id = TempId.new_unique("my item type", id_lookup)
        self.assertEqual(temp_id.item_type, "my item type")

    def test_private_id(self):
        id_lookup = {}
        temp_id = TempId(-23, "my item type", id_lookup)
        self.assertEqual(temp_id.private_id, -23)

    def test_id_lookup(self):
        id_lookup = {}
        temp_id = TempId(-23, "my item type", id_lookup)
        self.assertEqual(id_lookup, {-23: temp_id})


if __name__ == "__main__":
    unittest.main()
