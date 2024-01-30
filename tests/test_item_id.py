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
import unittest

from spinedb_api.item_id import IdFactory, IdMap


class TestIdFactory(unittest.TestCase):
    def test_ids_are_negative_and_consecutive(self):
        factory = IdFactory()
        self.assertEqual(factory.next_id(), -1)
        self.assertEqual(factory.next_id(), -2)


class TestIdMap(unittest.TestCase):
    def test_add_item_id(self):
        id_map = IdMap()
        id_map.add_item_id(-2)
        self.assertIsNone(id_map.db_id(-2))

    def test_remove_item_id(self):
        id_map = IdMap()
        id_map.set_db_id(-2, 3)
        id_map.remove_item_id(-2)
        self.assertRaises(KeyError, id_map.item_id, 3)
        self.assertRaises(KeyError, id_map.db_id, -2)

    def test_set_db_id(self):
        id_map = IdMap()
        id_map.set_db_id(-2, 3)
        self.assertEqual(id_map.db_id(-2), 3)
        self.assertEqual(id_map.item_id(3), -2)

    def test_remove_db_id_using_db_id(self):
        id_map = IdMap()
        id_map.set_db_id(-2, 3)
        id_map.remove_db_id(3)
        self.assertIsNone(id_map.db_id(-2))
        self.assertRaises(KeyError, id_map.item_id, 3)

    def test_remove_db_id_using_item_id(self):
        id_map = IdMap()
        id_map.set_db_id(-2, 3)
        id_map.remove_db_id(-2)
        self.assertIsNone(id_map.db_id(-2))
        self.assertRaises(KeyError, id_map.item_id, 3)

    def test_item_id(self):
        id_map = IdMap()
        id_map.set_db_id(-2, 3)
        self.assertEqual(id_map.item_id(3), -2)
        self.assertRaises(KeyError, id_map.item_id, 99)

    def test_db_id(self):
        id_map = IdMap()
        id_map.set_db_id(-2, 3)
        self.assertEqual(id_map.db_id(-2), 3)
        self.assertRaises(KeyError, id_map.db_id, -99)


if __name__ == '__main__':
    unittest.main()
