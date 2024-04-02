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

from spinedb_api import apply_execution_filter, DatabaseMapping


class TestExecutionFilter(unittest.TestCase):
    def test_import_alternative_after_applying_execution_filter(self):
        execution = {
            "execution_item": "Importing importer",
            "scenarios": ["low_on_steam", "wasting_my_time"],
            "timestamp": "2023-09-06T01:23:45",
        }
        with DatabaseMapping("sqlite:///", create=True) as db_map:
            apply_execution_filter(db_map, execution)
            alternative_name = db_map.get_import_alternative_name()
            self.assertEqual(alternative_name, "low_on_steam_wasting_my_time__Importing importer@2023-09-06T01:23:45")
            alternatives = {item["name"] for item in db_map.mapped_table("alternative").valid_values()}
            self.assertIn(alternative_name, alternatives)
            scenarios = {item["name"] for item in db_map.mapped_table("scenario").valid_values()}
            self.assertEqual(scenarios, {"low_on_steam", "wasting_my_time"})


if __name__ == "__main__":
    unittest.main()
