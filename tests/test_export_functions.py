######################################################################################################################
# Copyright (C) 2017 - 2020 Spine project consortium
# This file is part of Spine Toolbox.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Unit tests for export_functions.

:authors: A. Soininen (VTT)
:date:    29.6.2020
"""

from os import remove
import os.path
from tempfile import TemporaryDirectory
import unittest
from spinedb_api import (
    create_new_spine_database,
    DiffDatabaseMapping,
    export_alternatives,
    export_scenarios,
    export_scenario_alternatives,
    import_alternatives,
    import_scenarios,
    import_scenario_alternatives,
)


class TestExportFunctions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._temp_dir = TemporaryDirectory()

    def setUp(self):
        self._database_file_path = os.path.join(self._temp_dir.name, "test_export_functions.sqlite")
        db_url = "sqlite:///" + self._database_file_path
        create_new_spine_database(db_url)
        self._db_map = DiffDatabaseMapping(db_url, username="UnitTest")

    def tearDown(self):
        self._db_map.connection.close()
        remove(self._database_file_path)

    def test_export_alternatives(self):
        import_alternatives(self._db_map, [("alternative", "Description")])
        exported = export_alternatives(self._db_map, None)
        self.assertEqual(exported, [("Base", "Base alternative"), ("alternative", "Description")])

    def test_export_scenarios(self):
        import_scenarios(self._db_map, [("scenario", "Description")])
        exported = export_scenarios(self._db_map, None)
        self.assertEqual(exported, [("scenario", "Description")])

    def test_export_scenario_alternatives(self):
        import_alternatives(self._db_map, ["alternative"])
        import_scenarios(self._db_map, ["scenario"])
        import_scenario_alternatives(self._db_map, (("scenario", (("alternative", 23),),),))
        exported = export_scenario_alternatives(self._db_map, None)
        self.assertEqual(exported, [("scenario", [("alternative", 23)])])

    def test_export_multiple_scenario_alternatives(self):
        import_alternatives(self._db_map, ["alternative1"])
        import_alternatives(self._db_map, ["alternative2"])
        import_scenarios(self._db_map, ["scenario"])
        import_scenario_alternatives(self._db_map, (("scenario", (("alternative1", 23), ("alternative2", 5)),),))
        exported = export_scenario_alternatives(self._db_map, None)
        self.assertEqual(exported, [("scenario", [("alternative1", 23), ("alternative2", 5)])])


if __name__ == '__main__':
    unittest.main()
