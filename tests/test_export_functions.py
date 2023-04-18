######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# This file is part of Spine Database API.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Unit tests for export_functions.

"""

import unittest
from spinedb_api import (
    DiffDatabaseMapping,
    export_alternatives,
    export_data,
    export_scenarios,
    export_scenario_alternatives,
    export_tools,
    export_features,
    export_tool_features,
    export_tool_feature_methods,
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
    import_tools,
    import_features,
    import_tool_features,
    import_tool_feature_methods,
)


class TestExportFunctions(unittest.TestCase):
    def setUp(self):
        db_url = "sqlite://"
        self._db_map = DiffDatabaseMapping(db_url, username="UnitTest", create=True)

    def tearDown(self):
        self._db_map.connection.close()

    def test_export_tools(self):
        import_tools(self._db_map, [("tool", "Description")])
        exported = export_tools(self._db_map)
        self.assertEqual(exported, [("tool", "Description")])

    def test_export_features(self):
        import_object_classes(self._db_map, ["object_class1", "object_class2"])
        import_parameter_value_lists(self._db_map, [['value_list', 'value1'], ['value_list', 'value2']])
        import_object_parameters(self._db_map, [["object_class1", "parameter1", "value1", "value_list"]])
        import_features(self._db_map, [["object_class1", "parameter1", "Description"]])
        exported = export_features(self._db_map)
        self.assertEqual(exported, [("object_class1", "parameter1", "value_list", "Description")])

    def test_export_tool_features(self):
        import_object_classes(self._db_map, ["object_class1", "object_class2"])
        import_parameter_value_lists(self._db_map, [['value_list', 'value1'], ['value_list', 'value2']])
        import_object_parameters(self._db_map, [["object_class1", "parameter1", "value1", "value_list"]])
        import_features(self._db_map, [["object_class1", "parameter1", "Description"]])
        import_tools(self._db_map, ["tool1"])
        import_tool_features(self._db_map, [["tool1", "object_class1", "parameter1"]])
        exported = export_tool_features(self._db_map)
        self.assertEqual(exported, [("tool1", "object_class1", "parameter1", False)])

    def test_export_tool_feature_methods(self):
        import_object_classes(self._db_map, ["object_class1", "object_class2"])
        import_parameter_value_lists(self._db_map, [['value_list', 'value1'], ['value_list', 'value2']])
        import_object_parameters(self._db_map, [["object_class1", "parameter1", "value1", "value_list"]])
        import_features(self._db_map, [["object_class1", "parameter1", "Description"]])
        import_tools(self._db_map, ["tool1"])
        import_tool_features(self._db_map, [["tool1", "object_class1", "parameter1"]])
        import_tool_feature_methods(self._db_map, [["tool1", "object_class1", "parameter1", "value2"]])
        exported = export_tool_feature_methods(self._db_map)
        self.assertEqual(exported, [("tool1", "object_class1", "parameter1", "value2")])

    def test_export_alternatives(self):
        import_alternatives(self._db_map, [("alternative", "Description")])
        exported = export_alternatives(self._db_map)
        self.assertEqual(exported, [("Base", "Base alternative"), ("alternative", "Description")])

    def test_export_scenarios(self):
        import_scenarios(self._db_map, [("scenario", False, "Description")])
        exported = export_scenarios(self._db_map)
        self.assertEqual(exported, [("scenario", False, "Description")])

    def test_export_scenario_alternatives(self):
        import_alternatives(self._db_map, ["alternative"])
        import_scenarios(self._db_map, ["scenario"])
        import_scenario_alternatives(self._db_map, (("scenario", "alternative"),))
        exported = export_scenario_alternatives(self._db_map)
        self.assertEqual(exported, [("scenario", "alternative", None)])

    def test_export_multiple_scenario_alternatives(self):
        import_alternatives(self._db_map, ["alternative1"])
        import_alternatives(self._db_map, ["alternative2"])
        import_scenarios(self._db_map, ["scenario"])
        import_scenario_alternatives(self._db_map, (("scenario", "alternative1"),))
        import_scenario_alternatives(self._db_map, (("scenario", "alternative2", "alternative1"),))
        exported = export_scenario_alternatives(self._db_map)
        self.assertEqual(
            set(exported), {("scenario", "alternative2", "alternative1"), ("scenario", "alternative1", None)}
        )

    def test_export_data(self):
        import_object_classes(self._db_map, ["object_class"])
        import_object_parameters(self._db_map, [("object_class", "object_parameter")])
        import_objects(self._db_map, [("object_class", "object")])
        import_object_parameter_values(self._db_map, [("object_class", "object", "object_parameter", 2.3)])
        import_relationship_classes(self._db_map, [("relationship_class", ["object_class"])])
        import_relationship_parameters(self._db_map, [("relationship_class", "relationship_parameter")])
        import_relationships(self._db_map, [("relationship_class", ["object"])])
        import_relationship_parameter_values(
            self._db_map, [("relationship_class", ["object"], "relationship_parameter", 3.14)]
        )
        import_parameter_value_lists(self._db_map, [("value_list", "5.5"), ("value_list", "6.4")])
        import_alternatives(self._db_map, ["alternative"])
        import_scenarios(self._db_map, ["scenario"])
        import_scenario_alternatives(self._db_map, [("scenario", "alternative")])
        exported = export_data(self._db_map)
        self.assertEqual(len(exported), 12)
        self.assertIn("object_classes", exported)
        self.assertEqual(exported["object_classes"], [("object_class", None, None)])
        self.assertIn("object_parameters", exported)
        self.assertEqual(exported["object_parameters"], [("object_class", "object_parameter", None, None, None)])
        self.assertIn("objects", exported)
        self.assertEqual(exported["objects"], [("object_class", "object", None)])
        self.assertIn("object_parameter_values", exported)
        self.assertEqual(
            exported["object_parameter_values"], [("object_class", "object", "object_parameter", 2.3, "Base")]
        )
        self.assertIn("relationship_classes", exported)
        self.assertEqual(exported["relationship_classes"], [("relationship_class", ("object_class",), None, None)])
        self.assertIn("relationship_parameters", exported)
        self.assertEqual(
            exported["relationship_parameters"], [("relationship_class", "relationship_parameter", None, None, None)]
        )
        self.assertIn("relationships", exported)
        self.assertEqual(exported["relationships"], [("relationship_class", ("object",))])
        self.assertIn("relationship_parameter_values", exported)
        self.assertEqual(
            exported["relationship_parameter_values"],
            [("relationship_class", ("object",), "relationship_parameter", 3.14, "Base")],
        )
        self.assertIn("parameter_value_lists", exported)
        self.assertEqual(exported["parameter_value_lists"], [("value_list", "5.5"), ("value_list", "6.4")])
        self.assertIn("alternatives", exported)
        self.assertEqual(exported["alternatives"], [("Base", "Base alternative"), ("alternative", None)])
        self.assertIn("scenarios", exported)
        self.assertEqual(exported["scenarios"], [("scenario", False, None)])
        self.assertIn("scenario_alternatives", exported)
        self.assertEqual(exported["scenario_alternatives"], [("scenario", "alternative", None)])


if __name__ == '__main__':
    unittest.main()
