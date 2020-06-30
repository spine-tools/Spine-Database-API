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
    export_data,
    export_scenarios,
    export_scenario_alternatives,
    import_alternatives,
    import_object_classes,
    import_object_parameter_values,
    import_object_parameters,
    import_objects,
    import_parameter_value_lists,
    import_relationship_classes,
    import_relationship_parameter_values,
    import_relationship_parameters,
    import_relationships,
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

    def test_export_data(self):
        import_object_classes(self._db_map, ["object_class"])
        import_object_parameters(self._db_map, [("object_class", "object_parameter")])
        import_objects(self._db_map, [("object_class", "object")])
        import_object_parameter_values(self._db_map, [("object_class", "object", "object_parameter", 2.3)])
        import_relationship_classes(self._db_map, [("relationship_class", ["object_class"])])
        import_relationship_parameters(self._db_map, [("relationship_class", "relationship_parameter")])
        import_relationships(self._db_map, [("relationship_class", ["object"])])
        import_relationship_parameter_values(self._db_map, [("relationship_class", ["object"], "relationship_parameter", 3.14)])
        import_parameter_value_lists(self._db_map, [("value_list", ["5.5"])])
        import_alternatives(self._db_map, ["alternative"])
        import_scenarios(self._db_map, ["scenario"])
        import_scenario_alternatives(self._db_map, [("scenario", ["alternative"])])
        exported = export_data(self._db_map)
        self.assertEqual(len(exported), 12)
        self.assertIn("object_classes", exported)
        self.assertEqual(exported["object_classes"], [("object_class", None, None)])
        self.assertIn("object_parameters", exported)
        self.assertEqual(exported["object_parameters"], [("object_class", "object_parameter", None, None, None)])
        self.assertIn("objects", exported)
        self.assertEqual(exported["objects"], [("object_class", "object", None)])
        self.assertIn("object_parameter_values", exported)
        self.assertEqual(exported["object_parameter_values"], [("object_class", "object", "object_parameter", 2.3)])
        self.assertIn("relationship_classes", exported)
        self.assertEqual(exported["relationship_classes"], [("relationship_class", ["object_class"], None)])
        self.assertIn("relationship_parameters", exported)
        self.assertEqual(exported["relationship_parameters"], [("relationship_class", "relationship_parameter", None, None, None)])
        self.assertIn("relationships", exported)
        self.assertEqual(exported["relationships"], [("relationship_class", ["object"])])
        self.assertIn("relationship_parameter_values", exported)
        self.assertEqual(exported["relationship_parameter_values"], [("relationship_class", ["object"], "relationship_parameter", 3.14)])
        self.assertIn("parameter_value_lists", exported)
        self.assertEqual(exported["parameter_value_lists"], [("value_list", ["5.5"])])
        self.assertIn("alternatives", exported)
        self.assertEqual(exported["alternatives"], [("Base", "Base alternative"), ("alternative", None)])
        self.assertIn("scenarios", exported)
        self.assertEqual(exported["scenarios"], [("scenario", None)])
        self.assertIn("scenario_alternatives", exported)
        self.assertEqual(exported["scenario_alternatives"], [("scenario", [("alternative", 1)])])


if __name__ == '__main__':
    unittest.main()
