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
from spinedb_api import DatabaseMapping
from spinedb_api.db_mapping_base import DatabaseMappingBase, MappedItemBase
from tests.mock_helpers import AssertSuccessTestCase


class TestMappedTable(unittest.TestCase):
    def test_readding_item_with_invalid_id_creates_new_id(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            mapped_table = db_map.mapped_table("entity_class")
            item = mapped_table.add_item({"name": "Object"})
            id_ = item["id"]
            db_map._rollback()
            self.assertFalse(item.has_valid_id)
            mapped_table.add_item(item)
            self.assertTrue(item.has_valid_id)
            self.assertNotEqual(item["id"], id_)


class TestMappedItemBase(unittest.TestCase):
    def test_id_is_valid_initially(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            item = MappedItemBase(db_map)
            self.assertTrue(item.has_valid_id)

    def test_id_can_be_invalidated(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            item = MappedItemBase(db_map)
            item.invalidate_id()
            self.assertFalse(item.has_valid_id)

    def test_setting_new_id_validates_it(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            item = MappedItemBase(db_map)
            item.invalidate_id()
            self.assertFalse(item.has_valid_id)
            item["id"] = 23
            self.assertTrue(item.has_valid_id)


class TestPublicItem(AssertSuccessTestCase):
    def test_contains_operator(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            item = self._assert_success(db_map.add_scenario_item(name="my scenario"))
            self.assertIn("name", item)


if __name__ == "__main__":
    unittest.main()
