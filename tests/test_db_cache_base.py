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
import unittest

from spinedb_api.db_mapping_base import MappedItemBase, DatabaseMappingBase


class TestDBMapping(DatabaseMappingBase):
    @property
    def item_types(self):
        return ["cutlery"]

    @staticmethod
    def item_factory(item_type):
        if item_type == "cutlery":
            return MappedItemBase
        raise RuntimeError(f"unknown item_type '{item_type}'")


class TestDBCacheBase(unittest.TestCase):
    def test_rolling_back_new_item_invalidates_its_id(self):
        db_map = TestDBMapping()
        mapped_table = db_map.mapped_table("cutlery")
        item = mapped_table.add_item({}, new=True)
        self.assertTrue(item.is_id_valid)
        self.assertIn("id", item)
        id_ = item["id"]
        db_map.rollback()
        self.assertFalse(item.is_id_valid)
        self.assertEqual(item["id"], id_)


class TestTableCache(unittest.TestCase):
    def test_readding_item_with_invalid_id_creates_new_id(self):
        db_map = TestDBMapping()
        mapped_table = db_map.mapped_table("cutlery")
        item = mapped_table.add_item({}, new=True)
        id_ = item["id"]
        db_map.rollback()
        self.assertFalse(item.is_id_valid)
        mapped_table.add_item(item, new=True)
        self.assertTrue(item.is_id_valid)
        self.assertNotEqual(item["id"], id_)


class TestMappedItemBase(unittest.TestCase):
    def test_id_is_valid_initially(self):
        db_map = TestDBMapping()
        item = MappedItemBase(db_map, "cutlery")
        self.assertTrue(item.is_id_valid)

    def test_id_can_be_invalidated(self):
        db_map = TestDBMapping()
        item = MappedItemBase(db_map, "cutlery")
        item.invalidate_id()
        self.assertFalse(item.is_id_valid)

    def test_setting_new_id_validates_it(self):
        db_map = TestDBMapping()
        item = MappedItemBase(db_map, "cutlery")
        item.invalidate_id()
        self.assertFalse(item.is_id_valid)
        item["id"] = 23
        self.assertTrue(item.is_id_valid)


if __name__ == '__main__':
    unittest.main()
