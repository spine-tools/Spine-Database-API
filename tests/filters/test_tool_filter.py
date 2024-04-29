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

"""
Unit tests for ``tool_entity_filter`` module.

"""
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from sqlalchemy.engine.url import URL
from spinedb_api import (
    create_new_spine_database,
    DatabaseMapping,
    import_object_classes,
    import_relationship_classes,
    import_object_parameter_values,
    import_object_parameters,
    import_objects,
    import_relationships,
    import_relationship_parameter_values,
    import_relationship_parameters,
    import_parameter_value_lists,
    SpineDBAPIError,
)


@unittest.skip("obsolete, but need to adapt into the scenario filter")
class TestToolEntityFilter(unittest.TestCase):
    _db_url = None
    _temp_dir = None

    @classmethod
    def setUpClass(cls):
        cls._temp_dir = TemporaryDirectory()
        cls._db_url = URL("sqlite", database=Path(cls._temp_dir.name, "test_tool_filter_mapping.sqlite").as_posix())

    def setUp(self):
        create_new_spine_database(self._db_url)
        self._db_map = DatabaseMapping(self._db_url)

    def tearDown(self):
        self._db_map.close()

    def _build_data_with_tools(self):
        import_object_classes(self._db_map, ["object_class"])
        import_objects(
            self._db_map,
            [
                ("object_class", "object1"),
                ("object_class", "object2"),
                ("object_class", "object3"),
                ("object_class", "object4"),
            ],
        )
        import_parameter_value_lists(
            self._db_map, [("methods", "methodA"), ("methods", "methodB"), ("methods", "methodC")]
        )
        import_object_parameters(
            self._db_map,
            [
                ("object_class", "parameter1", "methodA", "methods"),
                ("object_class", "parameter2", "methodC", "methods"),
            ],
        )
        import_object_parameter_values(
            self._db_map,
            [
                ("object_class", "object1", "parameter1", "methodA"),
                ("object_class", "object2", "parameter1", "methodB"),
                ("object_class", "object3", "parameter1", "methodC"),
                ("object_class", "object4", "parameter1", "methodB"),
                ("object_class", "object2", "parameter2", "methodA"),
                ("object_class", "object3", "parameter2", "methodC"),
            ],
        )
        import_tools(self._db_map, ["tool1", "tool2"])
        import_features(self._db_map, [("object_class", "parameter1"), ("object_class", "parameter2")])
        import_tool_features(
            self._db_map,
            [("tool1", "object_class", "parameter1", False), ("tool2", "object_class", "parameter1", False)],
        )

    def test_non_existing_tool_filter_raises(self):
        self._build_data_with_tools()
        self._db_map.commit_session("Add test data")
        self.assertRaises(SpineDBAPIError, apply_tool_filter_to_entity_sq, self._db_map, "notool")

    def test_tool_feature_no_filter(self):
        self._build_data_with_tools()
        self._db_map.commit_session("Add test data")
        apply_tool_filter_to_entity_sq(self._db_map, "tool1")
        entities = self._db_map.query(self._db_map.entity_sq).all()
        self.assertEqual(len(entities), 4)
        names = [x.name for x in entities]
        self.assertIn("object1", names)
        self.assertIn("object2", names)
        self.assertIn("object3", names)
        self.assertIn("object4", names)

    def test_tool_feature_required(self):
        self._build_data_with_tools()
        import_tool_features(self._db_map, [("tool1", "object_class", "parameter2", True)])
        self._db_map.commit_session("Add test data")
        apply_tool_filter_to_entity_sq(self._db_map, "tool1")
        entities = self._db_map.query(self._db_map.entity_sq).all()
        self.assertEqual(len(entities), 2)
        names = [x.name for x in entities]
        self.assertIn("object2", names)
        self.assertIn("object3", names)

    def test_tool_feature_method(self):
        self._build_data_with_tools()
        import_tool_feature_methods(
            self._db_map,
            [("tool1", "object_class", "parameter1", "methodB"), ("tool2", "object_class", "parameter1", "methodC")],
        )
        self._db_map.commit_session("Add test data")
        apply_tool_filter_to_entity_sq(self._db_map, "tool1")
        entities = self._db_map.query(self._db_map.entity_sq).all()
        self.assertEqual(len(entities), 2)
        names = [x.name for x in entities]
        self.assertIn("object2", names)
        self.assertIn("object4", names)

    def test_tool_feature_required_and_method(self):
        self._build_data_with_tools()
        import_tool_features(self._db_map, [("tool1", "object_class", "parameter2", True)])
        import_tool_feature_methods(
            self._db_map,
            [("tool1", "object_class", "parameter1", "methodB"), ("tool2", "object_class", "parameter1", "methodC")],
        )
        self._db_map.commit_session("Add test data")
        apply_tool_filter_to_entity_sq(self._db_map, "tool1")
        entities = self._db_map.query(self._db_map.entity_sq).all()
        self.assertEqual(len(entities), 1)
        self.assertEqual(entities[0].name, "object2")

    def test_tool_filter_config(self):
        config = tool_filter_config("tool name")
        self.assertEqual(config, {"type": "tool_filter", "tool": "tool name"})

    def test_tool_filter_from_dict(self):
        self._build_data_with_tools()
        import_tool_features(self._db_map, [("tool1", "object_class", "parameter2", True)])
        self._db_map.commit_session("Add test data")
        config = tool_filter_config("tool1")
        tool_filter_from_dict(self._db_map, config)
        entities = self._db_map.query(self._db_map.entity_sq).all()
        self.assertEqual(len(entities), 2)
        names = [x.name for x in entities]
        self.assertIn("object2", names)
        self.assertIn("object3", names)

    def test_tool_filter_config_to_shorthand(self):
        config = tool_filter_config("tool name")
        shorthand = tool_filter_config_to_shorthand(config)
        self.assertEqual(shorthand, "tool:tool name")

    def test_tool_filter_shorthand_to_config(self):
        config = tool_filter_shorthand_to_config("tool:tool name")
        self.assertEqual(config, {"type": "tool_filter", "tool": "tool name"})

    def test_object_activity_control_filter(self):
        import_object_classes(self._db_map, ["node", "unit"])
        import_relationship_classes(self._db_map, [["node__unit", ["node", "unit"]]])
        import_objects(self._db_map, [("node", "node1"), ("node", "node2"), ("unit", "unita"), ("unit", "unitb")])
        import_relationships(
            self._db_map,
            [
                ["node__unit", ["node1", "unita"]],
                ["node__unit", ["node1", "unitb"]],
                ["node__unit", ["node2", "unita"]],
            ],
        )
        import_parameter_value_lists(self._db_map, [("boolean", True), ("boolean", False)])
        import_object_parameters(self._db_map, [("node", "is_active", True, "boolean")])
        import_relationship_parameters(self._db_map, [("node__unit", "x")])
        import_object_parameter_values(self._db_map, [("node", "node1", "is_active", False)])
        import_relationship_parameter_values(
            self._db_map,
            [
                ["node__unit", ["node1", "unita"], "x", 5],
                ["node__unit", ["node1", "unitb"], "x", 7],
                ["node__unit", ["node2", "unita"], "x", 11],
            ],
        )
        import_tools(self._db_map, ["obj_act_ctrl"])
        import_features(self._db_map, [("node", "is_active")])
        import_tool_features(self._db_map, [("obj_act_ctrl", "node", "is_active", False)])
        import_tool_feature_methods(self._db_map, [("obj_act_ctrl", "node", "is_active", True)])
        self._db_map.commit_session("Add obj act ctrl filter")
        apply_tool_filter_to_entity_sq(self._db_map, "obj_act_ctrl")
        objects = self._db_map.query(self._db_map.object_sq).all()
        self.assertEqual(len(objects), 3)
        object_names = [x.name for x in objects]
        self.assertTrue("node1" not in object_names)
        self.assertTrue("node2" in object_names)
        self.assertTrue("unita" in object_names)
        self.assertTrue("unitb" in object_names)
        relationships = self._db_map.query(self._db_map.wide_relationship_sq).all()
        self.assertEqual(len(relationships), 1)
        relationship_object_names = relationships[0].object_name_list.split(",")
        self.assertTrue("node1" not in relationship_object_names)
        ent_pvals = self._db_map.query(self._db_map.entity_parameter_value_sq).all()
        self.assertEqual(len(ent_pvals), 1)
        pval_object_names = ent_pvals[0].object_name_list.split(",")
        self.assertTrue("node1" not in pval_object_names)


if __name__ == "__main__":
    unittest.main()
