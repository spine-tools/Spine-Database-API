######################################################################################################################
# Copyright (C) 2017 - 2019 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Unit tests for import_functions.py.

:author: P. Vennström (VTT)
:date:   17.12.2018
"""

import os.path
from tempfile import TemporaryDirectory
import unittest

from spinedb_api.diff_db_mapping import DiffDatabaseMapping
from spinedb_api.helpers import create_new_spine_database
from spinedb_api.import_functions import (
    import_alternatives,
    import_object_classes,
    import_object_parameter_values,
    import_object_parameters,
    import_objects,
    import_relationship_classes,
    import_relationship_parameter_values,
    import_relationship_parameters,
    import_relationships,
    import_scenario_alternatives,
    import_scenarios,
    import_parameter_value_lists,
    import_tools,
    import_features,
    import_tool_features,
    import_tool_feature_methods,
    import_data,
)
from spinedb_api.parameter_value import from_database


def create_diff_db_map(directory):
    file_name = os.path.join(directory, "test_import_functions.sqlite")
    db_url = "sqlite:///" + file_name
    create_new_spine_database(db_url)
    return DiffDatabaseMapping(db_url, username="UnitTest")


class TestIntegrationImportData(unittest.TestCase):
    def test_import_data_integration(self):
        with TemporaryDirectory() as temp_dir:
            database_file = os.path.join(temp_dir, "test_import_data_integration.sqlite")
            database_url = "sqlite:///" + database_file
            create_new_spine_database(database_url)
            db_map = DiffDatabaseMapping(database_url, username="IntegrationTest")

            object_c = ["example_class", "other_class"]  # 2 items
            objects = [["example_class", "example_object"], ["other_class", "other_object"]]  # 2 items
            relationship_c = [["example_rel_class", ["example_class", "other_class"]]]  # 1 item
            relationships = [["example_rel_class", ["example_object", "other_object"]]]  # 1 item
            obj_parameters = [["example_class", "example_parameter"]]  # 1 item
            rel_parameters = [["example_rel_class", "rel_parameter"]]  # 1 item
            object_p_values = [["example_class", "example_object", "example_parameter", 3.14]]  # 1 item
            rel_p_values = [["example_rel_class", ["example_object", "other_object"], "rel_parameter", 2.718]]  # 1
            alternatives = [['example_alternative', 'An example']]
            scenarios = [['example_scenario', True, 'An example']]
            scenario_alternatives = [['example_scenario', 'example_alternative']]

            num_imports, errors = import_data(
                db_map,
                object_classes=object_c,
                relationship_classes=relationship_c,
                object_parameters=obj_parameters,
                relationship_parameters=rel_parameters,
                objects=objects,
                relationships=relationships,
                object_parameter_values=object_p_values,
                relationship_parameter_values=rel_p_values,
                alternatives=alternatives,
                scenarios=scenarios,
                scenario_alternatives=scenario_alternatives,
            )
            db_map.connection.close()
        self.assertEqual(num_imports, 13)
        self.assertFalse(errors)


class TestImportObjectClass(unittest.TestCase):
    def test_import_object_class(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            _, errors = import_object_classes(db_map, ["new_class"])
            self.assertFalse(errors)
            self.assertIn("new_class", [oc.name for oc in db_map.query(db_map.object_class_sq)])
            db_map.connection.close()


class TestImportObject(unittest.TestCase):
    def test_import_valid_objects(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_object_classes(db_map, ["object_class"])
            _, errors = import_objects(db_map, [["object_class", "new_object"]])
            self.assertFalse(errors)
            self.assertIn("new_object", [o.name for o in db_map.query(db_map.object_sq)])
            db_map.connection.close()

    def test_import_object_with_invalid_object_class_name(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            _, errors = import_objects(db_map, [["nonexistent_class", "new_object"]])
            self.assertTrue(errors)
            db_map.connection.close()

    def test_import_two_objects_with_same_name(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_object_classes(db_map, ["object_class1", "object_class2"])
            _, errors = import_objects(db_map, [["object_class1", "object"], ["object_class2", "object"]])
            self.assertFalse(errors)
            objects = {
                o.class_name: o.name
                for o in db_map.query(
                    db_map.object_sq.c.name.label("name"), db_map.object_class_sq.c.name.label("class_name")
                )
            }
            expected = {"object_class1": "object", "object_class2": "object"}
            self.assertEqual(objects, expected)
            db_map.connection.close()

    def test_import_existing_object(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_object_classes(db_map, ["object_class"])
            import_objects(db_map, [["object_class", "object"]])
            self.assertIn("object", [o.name for o in db_map.query(db_map.object_sq)])
            _, errors = import_objects(db_map, [["object_class", "object"]])
            self.assertFalse(errors)
            self.assertIn("object", [o.name for o in db_map.query(db_map.object_sq)])
            db_map.connection.close()


class TestImportRelationshipClass(unittest.TestCase):
    def test_import_valid_relationship_class(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_object_classes(db_map, ["object_class1", "object_class2"])
            _, errors = import_relationship_classes(
                db_map, [["relationship_class", ["object_class1", "object_class2"]]]
            )
            self.assertFalse(errors)
            relationship_classes = {
                rc.name: rc.object_class_name_list for rc in db_map.query(db_map.wide_relationship_class_sq)
            }
            expected = {"relationship_class": "object_class1,object_class2"}
            self.assertEqual(relationship_classes, expected)
            db_map.connection.close()

    def test_import_relationship_class_with_invalid_object_class_name(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_object_classes(db_map, ["object_class"])
            _, errors = import_relationship_classes(db_map, [["relationship_class", ["object_class", "nonexistent"]]])
            self.assertTrue(errors)
            self.assertFalse([rc for rc in db_map.query(db_map.wide_relationship_class_sq)])
            db_map.connection.close()

    def test_import_relationship_class_name_twice(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_object_classes(db_map, ["object_class1", "object_class2"])
            _, errors = import_relationship_classes(
                db_map, [["new_rc", ["object_class1", "object_class2"]], ["new_rc", ["object_class1", "object_class2"]]]
            )
            self.assertFalse(errors)
            relationship_classes = {
                rc.name: rc.object_class_name_list for rc in db_map.query(db_map.wide_relationship_class_sq)
            }
            expected = {"new_rc": "object_class1,object_class2"}
            self.assertEqual(relationship_classes, expected)
            db_map.connection.close()

    def test_import_existing_relationship_class(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_object_classes(db_map, ["object_class1", "object_class2"])
            import_relationship_classes(db_map, [["rc", ["object_class1", "object_class2"]]])
            _, errors = import_relationship_classes(db_map, [["rc", ["object_class1", "object_class2"]]])
            self.assertFalse(errors)
            db_map.connection.close()

    def test_import_relationship_class_with_one_object_class_as_None(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_object_classes(db_map, ["object_class1"])
            _, errors = import_relationship_classes(db_map, [["new_rc", ["object_class", None]]])
            self.assertTrue(errors)
            self.assertFalse([rc for rc in db_map.query(db_map.wide_relationship_class_sq)])
            db_map.connection.close()


class TestImportObjectClassParameter(unittest.TestCase):
    def test_import_valid_object_class_parameter(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_object_classes(db_map, ["object_class"])
            _, errors = import_object_parameters(db_map, [["object_class", "new_parameter"]])
            self.assertFalse(errors)
            self.assertIn("new_parameter", [p.name for p in db_map.query(db_map.parameter_definition_sq)])
            db_map.connection.close()

    def test_import_parameter_with_invalid_object_class_name(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            _, errors = import_object_parameters(db_map, [["nonexistent_object_class", "new_parameter"]])
            self.assertTrue(errors)
            db_map.connection.close()

    def test_import_object_class_parameter_name_twice(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_object_classes(db_map, ["object_class1", "object_class2"])
            _, errors = import_object_parameters(
                db_map, [["object_class1", "new_parameter"], ["object_class2", "new_parameter"]]
            )
            self.assertFalse(errors)
            definitions = {
                definition.object_class_name: definition.parameter_name
                for definition in db_map.query(db_map.object_parameter_definition_sq)
            }
            expected = {"object_class1": "new_parameter", "object_class2": "new_parameter"}
            self.assertEqual(definitions, expected)
            db_map.connection.close()

    def test_import_existing_object_class_parameter(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_object_classes(db_map, ["object_class"])
            import_object_parameters(db_map, [["object_class", "parameter"]])
            self.assertIn("parameter", [p.name for p in db_map.query(db_map.parameter_definition_sq)])
            _, errors = import_object_parameters(db_map, [["object_class", "parameter"]])
            self.assertIn("parameter", [p.name for p in db_map.query(db_map.parameter_definition_sq)])
            self.assertFalse(errors)
            db_map.connection.close()


class TestImportRelationshipClassParameter(unittest.TestCase):
    def test_import_valid_relationship_class_parameter(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_object_classes(db_map, ["object_class1", "object_class2"])
            import_relationship_classes(db_map, [["relationship_class", ["object_class1", "object_class2"]]])
            _, errors = import_relationship_parameters(db_map, [["relationship_class", "new_parameter"]])
            self.assertFalse(errors)
            definitions = {
                d.class_name: d.name
                for d in db_map.query(
                    db_map.relationship_parameter_definition_sq.c.parameter_name.label("name"),
                    db_map.relationship_class_sq.c.name.label("class_name"),
                )
            }
            expected = {"relationship_class": "new_parameter"}
            self.assertEqual(definitions, expected)
            db_map.connection.close()

    def test_import_parameter_with_invalid_relationship_class_name(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            _, errors = import_relationship_parameters(db_map, [["nonexistent_relationship_class", "new_parameter"]])
            self.assertTrue(errors)
            db_map.connection.close()

    def test_import_relationship_class_parameter_name_twice(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_object_classes(db_map, ["object_class1", "object_class2"])
            import_relationship_classes(
                db_map,
                [
                    ["relationship_class1", ["object_class1", "object_class2"]],
                    ["relationship_class2", ["object_class2", "object_class1"]],
                ],
            )
            _, errors = import_relationship_parameters(
                db_map, [["relationship_class1", "new_parameter"], ["relationship_class2", "new_parameter"]]
            )
            self.assertFalse(errors)
            definitions = {
                d.class_name: d.name
                for d in db_map.query(
                    db_map.relationship_parameter_definition_sq.c.parameter_name.label("name"),
                    db_map.relationship_class_sq.c.name.label("class_name"),
                )
            }
            expected = {"relationship_class1": "new_parameter", "relationship_class2": "new_parameter"}
            self.assertEqual(definitions, expected)
            db_map.connection.close()

    def test_import_existing_relationship_class_parameter(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_object_classes(db_map, ["object_class1", "object_class2"])
            import_relationship_classes(db_map, [["relationship_class", ["object_class1", "object_class2"]]])
            import_relationship_parameters(db_map, [["relationship_class", "new_parameter"]])
            _, errors = import_relationship_parameters(db_map, [["relationship_class", "new_parameter"]])
            self.assertFalse(errors)
            db_map.connection.close()


class TestImportRelationship(unittest.TestCase):
    @staticmethod
    def populate(db_map):
        import_object_classes(db_map, ["object_class1", "object_class2"])
        import_objects(db_map, [["object_class1", "object1"], ["object_class2", "object2"]])

    def test_import_valid_relationship(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            import_relationship_classes(db_map, [["relationship_class", ["object_class1", "object_class2"]]])
            _, errors = import_relationships(db_map, [["relationship_class", ["object1", "object2"]]])
            self.assertFalse(errors)
            self.assertIn("relationship_class_object1__object2", [r.name for r in db_map.query(db_map.relationship_sq)])
            db_map.connection.close()

    def test_import_valid_relationship_with_object_name_in_multiple_classes(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            import_objects(db_map, [["object_class1", "duplicate"], ["object_class2", "duplicate"]])
            import_relationship_classes(db_map, [["relationship_class", ["object_class1", "object_class2"]]])
            _, errors = import_relationships(db_map, [["relationship_class", ["duplicate", "object2"]]])
            self.assertFalse(errors)
            self.assertIn(
                "relationship_class_duplicate__object2", [r.name for r in db_map.query(db_map.relationship_sq)]
            )
            db_map.connection.close()

    def test_import_relationship_with_invalid_class_name(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            _, errors = import_relationships(db_map, [["nonexistent_relationship_class", ["object1", "object2"]]])
            self.assertTrue(errors)
            self.assertFalse([r.name for r in db_map.query(db_map.relationship_sq)])
            db_map.connection.close()

    def test_import_relationship_with_invalid_object_name(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            import_relationship_classes(db_map, [["relationship_class", ["object_class1", "object_class2"]]])
            _, errors = import_relationships(db_map, [["relationship_class", ["nonexistent_object", "object2"]]])
            self.assertTrue(errors)
            self.assertFalse([r.name for r in db_map.query(db_map.relationship_sq)])
            db_map.connection.close()

    def test_import_existing_relationship(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            import_relationship_classes(db_map, [["relationship_class", ["object_class1", "object_class2"]]])
            import_relationships(db_map, [["relationship_class", ["object1", "object2"]]])
            self.assertIn("relationship_class_object1__object2", [r.name for r in db_map.query(db_map.relationship_sq)])
            _, errors = import_relationships(db_map, [["relationship_class", ["object1", "object2"]]])
            self.assertFalse(errors)
            self.assertIn("relationship_class_object1__object2", [r.name for r in db_map.query(db_map.relationship_sq)])
            db_map.connection.close()

    def test_import_relationship_with_one_None_object(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            import_relationship_classes(db_map, [["relationship_class", ["object_class1", "object_class2"]]])
            _, errors = import_relationships(db_map, [["relationship_class", [None, "object2"]]])
            self.assertTrue(errors)
            self.assertFalse([r.name for r in db_map.query(db_map.relationship_sq)])
            db_map.connection.close()


class TestImportParameterValue(unittest.TestCase):
    @staticmethod
    def populate(db_map):
        import_object_classes(db_map, ["object_class1", "object_class2"])
        import_objects(db_map, [["object_class1", "object1"], ["object_class2", "object2"]])
        import_object_parameters(db_map, [["object_class1", "parameter"]])

    @staticmethod
    def populate_with_relationship(db_map):
        TestImportParameterValue.populate(db_map)
        import_relationship_classes(db_map, [["relationship_class", ["object_class1", "object_class2"]]])
        import_relationship_parameters(db_map, [["relationship_class", "parameter"]])
        import_relationships(db_map, [["relationship_class", ["object1", "object2"]]])

    def test_import_valid_object_parameter_value(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            _, errors = import_object_parameter_values(db_map, [["object_class1", "object1", "parameter", 1]])
            self.assertFalse(errors)
            values = {v.object_name: v.value for v in db_map.query(db_map.object_parameter_value_sq)}
            expected = {"object1": "1"}
            self.assertEqual(values, expected)
            db_map.connection.close()

    def test_import_valid_object_parameter_value_string(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            _, errors = import_object_parameter_values(
                db_map, [["object_class1", "object1", "parameter", "value_string"]]
            )
            self.assertFalse(errors)
            values = {v.object_name: v.value for v in db_map.query(db_map.object_parameter_value_sq)}
            expected = {"object1": '"value_string"'}
            self.assertEqual(values, expected)
            db_map.connection.close()

    def test_import_valid_object_parameter_value_with_duplicate_object_name(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            import_objects(db_map, [["object_class1", "duplicate_object"], ["object_class2", "duplicate_object"]])
            _, errors = import_object_parameter_values(db_map, [["object_class1", "duplicate_object", "parameter", 1]])
            self.assertFalse(errors)
            values = {
                v.object_class_name: {v.object_name: v.value} for v in db_map.query(db_map.object_parameter_value_sq)
            }
            expected = {"object_class1": {"duplicate_object": "1"}}
            self.assertEqual(values, expected)
            db_map.connection.close()

    def test_import_valid_object_parameter_value_with_duplicate_parameter_name(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            import_object_parameters(db_map, [["object_class2", "parameter"]])
            _, errors = import_object_parameter_values(db_map, [["object_class1", "object1", "parameter", 1]])
            self.assertFalse(errors)
            values = {
                v.object_class_name: {v.object_name: v.value} for v in db_map.query(db_map.object_parameter_value_sq)
            }
            expected = {"object_class1": {"object1": "1"}}
            self.assertEqual(values, expected)
            db_map.connection.close()

    def test_import_object_parameter_value_with_invalid_object(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_object_classes(db_map, ["object_class"])
            import_object_parameters(db_map, [["object_class", "parameter"]])
            _, errors = import_object_parameter_values(db_map, [["object_class", "nonexistent_object", "parameter", 1]])
            self.assertTrue(errors)
            self.assertFalse([v for v in db_map.query(db_map.object_parameter_value_sq)])
            db_map.connection.close()

    def test_import_object_parameter_value_with_invalid_parameter(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_object_classes(db_map, ["object_class"])
            import_objects(db_map, ["object_class", "object"])
            _, errors = import_object_parameter_values(db_map, [["object_class", "object", "nonexistent_parameter", 1]])
            self.assertTrue(errors)
            self.assertFalse([v for v in db_map.query(db_map.object_parameter_value_sq)])
            db_map.connection.close()

    def test_import_existing_object_parameter_value_update_the_value(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            import_object_parameter_values(db_map, [["object_class1", "object1", "parameter", "initial_value"]])
            _, errors = import_object_parameter_values(db_map, [["object_class1", "object1", "parameter", "new_value"]])
            self.assertFalse(errors)
            values = {v.object_name: v.value for v in db_map.query(db_map.object_parameter_value_sq)}
            expected = {"object1": '"new_value"'}
            self.assertEqual(values, expected)
            db_map.connection.close()

    def test_import_duplicate_object_parameter_value(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            _, errors = import_object_parameter_values(
                db_map,
                [
                    ["object_class1", "object1", "parameter", "first"],
                    ["object_class1", "object1", "parameter", "second"],
                ],
            )
            self.assertTrue(errors)
            values = {v.object_name: v.value for v in db_map.query(db_map.object_parameter_value_sq)}
            expected = {"object1": '"first"'}
            self.assertEqual(values, expected)
            db_map.connection.close()

    def test_import_object_parameter_value_with_alternative(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            import_alternatives(db_map, ["alternative"])
            count, errors = import_object_parameter_values(
                db_map, [["object_class1", "object1", "parameter", 1, "alternative"]]
            )
            self.assertFalse(errors)
            self.assertEqual(count, 1)
            values = {
                v.object_name: (v.value, v.alternative_name)
                for v in db_map.query(
                    db_map.object_parameter_value_sq, db_map.alternative_sq.c.name.label("alternative_name")
                )
                .filter(db_map.object_parameter_value_sq.c.alternative_id == db_map.alternative_sq.c.id)
                .all()
            }
            expected = {"object1": ("1", "alternative")}
            self.assertEqual(values, expected)
            db_map.connection.close()

    def test_import_object_parameter_value_fails_with_nonexistent_alternative(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            count, errors = import_object_parameter_values(
                db_map, [["object_class1", "object1", "parameter", 1, "nonexistent_alternative"]]
            )
            self.assertTrue(errors)
            self.assertEqual(count, 0)
            db_map.connection.close()

    def test_import_valid_relationship_parameter_value(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate_with_relationship(db_map)
            _, errors = import_relationship_parameter_values(
                db_map, [["relationship_class", ["object1", "object2"], "parameter", 1]]
            )
            self.assertFalse(errors)
            values = {v.object_name_list: v.value for v in db_map.query(db_map.relationship_parameter_value_sq)}
            expected = {"object1,object2": "1"}
            self.assertEqual(values, expected)
            db_map.connection.close()

    def test_import_valid_relationship_parameter_value_with_duplicate_parameter_name(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate_with_relationship(db_map)
            import_relationship_classes(db_map, [["relationship_class2", ["object_class2", "object_class1"]]])
            import_relationship_parameters(db_map, [["relationship_class2", "parameter"]])
            _, errors = import_relationship_parameter_values(
                db_map, [["relationship_class", ["object1", "object2"], "parameter", 1]]
            )
            self.assertFalse(errors)
            values = {v.object_name_list: v.value for v in db_map.query(db_map.relationship_parameter_value_sq)}
            expected = {"object1,object2": "1"}
            self.assertEqual(values, expected)
            db_map.connection.close()

    def test_import_valid_relationship_parameter_value_with_duplicate_object_name(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate_with_relationship(db_map)
            import_objects(db_map, [["object_class1", "duplicate_object"], ["object_class2", "duplicate_object"]])
            import_relationships(db_map, [["relationship_class", ["duplicate_object", "duplicate_object"]]])
            _, errors = import_relationship_parameter_values(
                db_map, [["relationship_class", ["duplicate_object", "duplicate_object"], "parameter", 1]]
            )
            self.assertFalse(errors)
            values = {v.object_name_list: v.value for v in db_map.query(db_map.relationship_parameter_value_sq)}
            expected = {"duplicate_object,duplicate_object": "1"}
            self.assertEqual(values, expected)
            db_map.connection.close()

    def test_import_relationship_parameter_value_with_invalid_object(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate_with_relationship(db_map)
            _, errors = import_relationship_parameter_values(
                db_map, [["relationship_class", ["nonexistent_object", "object2"], "parameter", 1]]
            )
            self.assertTrue(errors)
            self.assertFalse([v for v in db_map.query(db_map.relationship_parameter_value_sq)])
            db_map.connection.close()

    def test_import_relationship_parameter_value_with_invalid_relationship_class(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate_with_relationship(db_map)
            _, errors = import_relationship_parameter_values(
                db_map, [["nonexistent_class", ["object1", "object2"], "parameter", 1]]
            )
            self.assertTrue(errors)
            self.assertFalse([v for v in db_map.query(db_map.relationship_parameter_value_sq)])
            db_map.connection.close()

    def test_import_relationship_parameter_value_with_invalid_parameter(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate_with_relationship(db_map)
            _, errors = import_relationship_parameter_values(
                db_map, [["relationship_class", ["object1", "object2"], "nonexistent_parameter", 1]]
            )
            self.assertTrue(errors)
            self.assertFalse([v for v in db_map.query(db_map.relationship_parameter_value_sq)])
            db_map.connection.close()

    def test_import_existing_relationship_parameter_value(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate_with_relationship(db_map)
            import_relationship_parameter_values(
                db_map, [["relationship_class", ["object1", "object2"], "parameter", "initial_value"]]
            )
            _, errors = import_relationship_parameter_values(
                db_map, [["relationship_class", ["object1", "object2"], "parameter", "new_value"]]
            )
            self.assertFalse(errors)
            values = {v.object_name_list: v.value for v in db_map.query(db_map.relationship_parameter_value_sq)}
            expected = {"object1,object2": '"new_value"'}
            self.assertEqual(values, expected)
            db_map.connection.close()

    def test_import_duplicate_relationship_parameter_value(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate_with_relationship(db_map)
            _, errors = import_relationship_parameter_values(
                db_map,
                [
                    ["relationship_class", ["object1", "object2"], "parameter", "first"],
                    ["relationship_class", ["object1", "object2"], "parameter", "second"],
                ],
            )
            self.assertTrue(errors)
            values = {v.object_name_list: v.value for v in db_map.query(db_map.relationship_parameter_value_sq)}
            expected = {"object1,object2": '"first"'}
            self.assertEqual(values, expected)
            db_map.connection.close()

    def test_import_relationship_parameter_value_with_alternative(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate_with_relationship(db_map)
            import_alternatives(db_map, ["alternative"])
            count, errors = import_relationship_parameter_values(
                db_map, [["relationship_class", ["object1", "object2"], "parameter", 1, "alternative"]]
            )
            self.assertFalse(errors)
            self.assertEqual(count, 1)
            values = {
                v.object_name_list: (v.value, v.alternative_name)
                for v in db_map.query(
                    db_map.relationship_parameter_value_sq, db_map.alternative_sq.c.name.label("alternative_name")
                )
                .filter(db_map.relationship_parameter_value_sq.c.alternative_id == db_map.alternative_sq.c.id)
                .all()
            }
            expected = {"object1,object2": ("1", "alternative")}
            self.assertEqual(values, expected)
            db_map.connection.close()

    def test_import_relationship_parameter_value_fails_with_nonexistent_alternative(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            count, errors = import_relationship_parameter_values(
                db_map, [["relationship_class", ["object1", "object2"], "parameter", 1, "alternative"]]
            )
            self.assertTrue(errors)
            self.assertEqual(count, 0)
            db_map.connection.close()


class TestImportAlternative(unittest.TestCase):
    def test_single_alternative(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            count, errors = import_alternatives(db_map, ["alternative"])
            self.assertEqual(count, 1)
            self.assertFalse(errors)
            alternatives = [a.name for a in db_map.query(db_map.alternative_sq)]
            self.assertEqual(len(alternatives), 2)
            self.assertIn("Base", alternatives)
            self.assertIn("alternative", alternatives)
            db_map.connection.close()

    def test_alternative_description(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            count, errors = import_alternatives(db_map, [["alternative", "description"]])
            self.assertEqual(count, 1)
            self.assertFalse(errors)
            alternatives = {a.name: a.description for a in db_map.query(db_map.alternative_sq)}
            expected = {"Base": "Base alternative", "alternative": "description"}
            self.assertEqual(alternatives, expected)
            db_map.connection.close()

    def test_update_alternative_description(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            count, errors = import_alternatives(db_map, [["Base", "new description"]])
            self.assertEqual(count, 1)
            self.assertFalse(errors)
            alternatives = {a.name: a.description for a in db_map.query(db_map.alternative_sq)}
            expected = {"Base": "new description"}
            self.assertEqual(alternatives, expected)
            db_map.connection.close()


class TestImportScenario(unittest.TestCase):
    def test_single_scenario(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            count, errors = import_scenarios(db_map, ["scenario"])
            self.assertEqual(count, 1)
            self.assertFalse(errors)
            scenarios = {s.name: s.description for s in db_map.query(db_map.scenario_sq)}
            self.assertEqual(scenarios, {"scenario": None})
            db_map.connection.close()

    def test_scenario_with_description(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            count, errors = import_scenarios(db_map, [["scenario", False, "description"]])
            self.assertEqual(count, 1)
            self.assertFalse(errors)
            scenarios = {s.name: s.description for s in db_map.query(db_map.scenario_sq)}
            self.assertEqual(scenarios, {"scenario": "description"})
            db_map.connection.close()

    def test_update_scenario_description(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_scenarios(db_map, [["scenario", False, "initial description"]])
            count, errors = import_scenarios(db_map, [["scenario", False, "new description"]])
            self.assertEqual(count, 1)
            self.assertFalse(errors)
            scenarios = {s.name: s.description for s in db_map.query(db_map.scenario_sq)}
            self.assertEqual(scenarios, {"scenario": "new description"})
            db_map.connection.close()


class TestImportScenarioAlternative(unittest.TestCase):
    def test_single_scenario_alternative_import(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_alternatives(db_map, ["alternative"])
            import_scenarios(db_map, ["scenario"])
            count, errors = import_scenario_alternatives(db_map, [["scenario", "alternative"]])
            self.assertEqual(count, 1)
            self.assertFalse(errors)
            scenario_alternatives = self.scenario_alternatives(db_map)
            self.assertEqual(scenario_alternatives, {"scenario": {"alternative": 1}})
            db_map.connection.close()

    def test_scenario_alternative_import_multiple_without_before_alternatives(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_alternatives(db_map, ["alternative1"])
            import_alternatives(db_map, ["alternative2"])
            import_scenarios(db_map, ["scenario"])
            count, errors = import_scenario_alternatives(
                db_map, [["scenario", "alternative1"], ["scenario", "alternative2"]]
            )
            self.assertEqual(count, 2)
            self.assertFalse(errors)
            scenario_alternatives = self.scenario_alternatives(db_map)
            self.assertEqual(scenario_alternatives, {"scenario": {"alternative1": 1, "alternative2": 2}})
            db_map.connection.close()

    def test_scenario_alternative_import_multiple_with_before_alternatives(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_alternatives(db_map, ["alternative1"])
            import_alternatives(db_map, ["alternative2"])
            import_alternatives(db_map, ["alternative3"])
            import_scenarios(db_map, ["scenario"])
            count, errors = import_scenario_alternatives(
                db_map,
                [
                    ["scenario", "alternative1"],
                    ["scenario", "alternative3"],
                    ["scenario", "alternative2", "alternative3"],
                ],
            )
            self.assertEqual(count, 3)
            self.assertFalse(errors)
            scenario_alternatives = self.scenario_alternatives(db_map)
            self.assertEqual(
                scenario_alternatives, {"scenario": {"alternative1": 1, "alternative2": 2, "alternative3": 3}}
            )
            db_map.connection.close()

    def test_fails_with_nonexistent_scenario(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_alternatives(db_map, ["alternative"])
            count, errors = import_scenario_alternatives(db_map, [["nonexistent_scenario", "alternative"]])
            self.assertEqual(count, 0)
            self.assertTrue(errors)
            db_map.connection.close()

    def test_fails_with_nonexistent_alternative(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_scenarios(db_map, ["scenario"])
            count, errors = import_scenario_alternatives(db_map, [["scenario", "nonexistent_alternative"]])
            self.assertEqual(count, 0)
            self.assertTrue(errors)
            db_map.connection.close()

    def test_fails_with_nonexistent_before_alternative(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_alternatives(db_map, ["alternative"])
            import_scenarios(db_map, ["scenario"])
            count, errors = import_scenario_alternatives(
                db_map, [["scenario", "alternative", "nonexistent_alternative"]]
            )
            self.assertEqual(count, 0)
            self.assertTrue(errors)
            db_map.connection.close()

    @staticmethod
    def scenario_alternatives(db_map):
        scenario_alternative_qry = (
            db_map.query(
                db_map.scenario_sq.c.name.label("scenario_name"),
                db_map.alternative_sq.c.name.label("alternative_name"),
                db_map.scenario_alternative_sq.c.rank,
            )
            .filter(db_map.scenario_alternative_sq.c.scenario_id == db_map.scenario_sq.c.id)
            .filter(db_map.scenario_alternative_sq.c.alternative_id == db_map.alternative_sq.c.id)
        )
        scenario_alternatives = dict()
        for scenario_alternative in scenario_alternative_qry:
            alternative_rank = scenario_alternatives.setdefault(scenario_alternative.scenario_name, dict())
            alternative_rank[scenario_alternative.alternative_name] = scenario_alternative.rank
        return scenario_alternatives


class TestImportTool(unittest.TestCase):
    def test_single_tool(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            count, errors = import_tools(db_map, ["tool"])
            self.assertEqual(count, 1)
            self.assertFalse(errors)
            tools = [x.name for x in db_map.query(db_map.tool_sq)]
            self.assertEqual(len(tools), 1)
            self.assertIn("tool", tools)
            db_map.connection.close()

    def test_tool_description(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            count, errors = import_tools(db_map, [["tool", "description"]])
            self.assertEqual(count, 1)
            self.assertFalse(errors)
            tools = {x.name: x.description for x in db_map.query(db_map.tool_sq)}
            expected = {"tool": "description"}
            self.assertEqual(tools, expected)
            db_map.connection.close()

    def test_update_tool_description(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            count, errors = import_tools(db_map, [["tool", "description"]])
            count, errors = import_tools(db_map, [["tool", "new description"]])
            self.assertEqual(count, 1)
            self.assertFalse(errors)
            tools = {x.name: x.description for x in db_map.query(db_map.tool_sq)}
            expected = {"tool": "new description"}
            self.assertEqual(tools, expected)
            db_map.connection.close()


class TestImportFeature(unittest.TestCase):
    @staticmethod
    def populate(db_map):
        import_object_classes(db_map, ["object_class1", "object_class2"])
        import_parameter_value_lists(
            db_map, [['value_list', 'value1'], ['value_list', 'value2'], ['value_list', 'value3']]
        )
        import_object_parameters(
            db_map, [["object_class1", "parameter1", "value1", "value_list"], ["object_class1", "parameter2"]]
        )

    def test_single_feature(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            count, errors = import_features(db_map, [["object_class1", "parameter1"]])
            self.assertEqual(count, 1)
            self.assertFalse(errors)
            features = [
                (x.entity_class_name, x.parameter_definition_name, x.parameter_value_list_name)
                for x in db_map.query(db_map.ext_feature_sq)
            ]
            self.assertEqual(len(features), 1)
            self.assertIn(("object_class1", "parameter1", "value_list"), features)
            db_map.connection.close()

    def test_feature_for_parameter_without_value_list(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            count, errors = import_features(db_map, [["object_class1", "parameter2"]])
            self.assertEqual(count, 0)
            self.assertTrue(errors)
            db_map.connection.close()

    def test_feature_description(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            count, errors = import_features(db_map, [["object_class1", "parameter1", "description"]])
            self.assertEqual(count, 1)
            self.assertFalse(errors)
            features = {
                (x.entity_class_name, x.parameter_definition_name, x.parameter_value_list_name): x.description
                for x in db_map.query(db_map.ext_feature_sq)
            }
            expected = {("object_class1", "parameter1", "value_list"): "description"}
            self.assertEqual(features, expected)
            db_map.connection.close()

    def test_update_feature_description(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            count, errors = import_features(db_map, [["object_class1", "parameter1", "description"]])
            count, errors = import_features(db_map, [["object_class1", "parameter1", "new description"]])
            self.assertEqual(count, 1)
            self.assertFalse(errors)
            features = {
                (x.entity_class_name, x.parameter_definition_name, x.parameter_value_list_name): x.description
                for x in db_map.query(db_map.ext_feature_sq)
            }
            expected = {("object_class1", "parameter1", "value_list"): "new description"}
            self.assertEqual(features, expected)
            db_map.connection.close()


class TestImportToolFeature(unittest.TestCase):
    @staticmethod
    def populate(db_map):
        import_object_classes(db_map, ["object_class1", "object_class2"])
        import_parameter_value_lists(
            db_map, [['value_list', 'value1'], ['value_list', 'value2'], ['value_list', 'value3']]
        )
        import_object_parameters(
            db_map, [["object_class1", "parameter1", "value1", "value_list"], ["object_class1", "parameter2"]]
        )
        import_features(db_map, [["object_class1", "parameter1"]])
        import_tools(db_map, ["tool1"])

    def test_single_tool_feature(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            count, errors = import_tool_features(db_map, [["tool1", "object_class1", "parameter1"]])
            self.assertEqual(count, 1)
            self.assertFalse(errors)
            tool_features = [
                (x.tool_name, x.entity_class_name, x.parameter_definition_name, x.required)
                for x in db_map.query(db_map.ext_tool_feature_sq)
            ]
            self.assertEqual(len(tool_features), 1)
            self.assertIn(("tool1", "object_class1", "parameter1", False), tool_features)
            db_map.connection.close()

    def test_tool_feature_with_non_feature_parameter(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            count, errors = import_tool_features(db_map, [["tool1", "object_class1", "parameter2"]])
            self.assertEqual(count, 0)
            self.assertTrue(errors)
            db_map.connection.close()

    def test_tool_feature_with_non_existing_tool(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            count, errors = import_tool_features(db_map, [["non_existing_tool", "object_class1", "parameter1"]])
            self.assertEqual(count, 0)
            self.assertTrue(errors)
            db_map.connection.close()

    def test_tool_feature_required(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            count, errors = import_tool_features(db_map, [["tool1", "object_class1", "parameter1", True]])
            self.assertEqual(count, 1)
            self.assertFalse(errors)
            tool_features = [
                (x.tool_name, x.entity_class_name, x.parameter_definition_name, x.required)
                for x in db_map.query(db_map.ext_tool_feature_sq)
            ]
            self.assertEqual(len(tool_features), 1)
            self.assertIn(("tool1", "object_class1", "parameter1", True), tool_features)
            db_map.connection.close()

    def test_update_tool_feature_required(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            import_tool_features(db_map, [["tool1", "object_class1", "parameter1"]])
            count, errors = import_tool_features(db_map, [["tool1", "object_class1", "parameter1", True]])
            self.assertEqual(count, 1)
            self.assertFalse(errors)
            tool_features = [
                (x.tool_name, x.entity_class_name, x.parameter_definition_name, x.required)
                for x in db_map.query(db_map.ext_tool_feature_sq)
            ]
            self.assertEqual(len(tool_features), 1)
            self.assertIn(("tool1", "object_class1", "parameter1", True), tool_features)
            db_map.connection.close()


class TestImportToolFeatureMethod(unittest.TestCase):
    @staticmethod
    def populate(db_map):
        import_object_classes(db_map, ["object_class1", "object_class2"])
        import_parameter_value_lists(
            db_map, [['value_list', 'value1'], ['value_list', 'value2'], ['value_list', 'value3']]
        )
        import_object_parameters(
            db_map, [["object_class1", "parameter1", "value1", "value_list"], ["object_class1", "parameter2"]]
        )
        import_features(db_map, [["object_class1", "parameter1"]])
        import_tools(db_map, ["tool1"])
        import_tool_features(db_map, [["tool1", "object_class1", "parameter1"]])

    def test_a_couple_of_tool_feature_method(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            count, errors = import_tool_feature_methods(
                db_map,
                [
                    ["tool1", "object_class1", "parameter1", "value2"],
                    ["tool1", "object_class1", "parameter1", "value3"],
                ],
            )
            self.assertEqual(count, 2)
            self.assertFalse(errors)
            tool_feature_methods = [
                (x.tool_name, x.entity_class_name, x.parameter_definition_name, from_database(x.method))
                for x in db_map.query(db_map.ext_tool_feature_method_sq)
            ]
            self.assertEqual(len(tool_feature_methods), 2)
            self.assertIn(("tool1", "object_class1", "parameter1", "value2"), tool_feature_methods)
            self.assertIn(("tool1", "object_class1", "parameter1", "value3"), tool_feature_methods)
            db_map.connection.close()

    def test_tool_feature_method_with_non_feature_parameter(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            count, errors = import_tool_feature_methods(db_map, [["tool1", "object_class1", "parameter2", "method"]])
            self.assertEqual(count, 0)
            self.assertTrue(errors)
            db_map.connection.close()

    def test_tool_feature_method_with_non_existing_tool(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            count, errors = import_tool_feature_methods(
                db_map, [["non_existing_tool", "object_class1", "parameter1", "value2"]]
            )
            self.assertEqual(count, 0)
            self.assertTrue(errors)
            db_map.connection.close()

    def test_tool_feature_method_with_invalid_method(self):
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            self.populate(db_map)
            count, errors = import_tool_feature_methods(
                db_map, [["tool1", "object_class1", "parameter1", "invalid_method"]],
            )
            self.assertEqual(count, 0)
            self.assertTrue(errors)
            db_map.connection.close()


if __name__ == "__main__":
    unittest.main()
