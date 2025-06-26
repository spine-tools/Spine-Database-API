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
""" Unit tests for import_functions.py. """
import unittest
import pytest
from spinedb_api.db_mapping import DatabaseMapping
from spinedb_api.exception import NothingToCommit
from spinedb_api.helpers import DisplayStatus
from spinedb_api.import_functions import (
    import_alternatives,
    import_data,
    import_display_modes,
    import_entities,
    import_entity_class_display_modes,
    import_entity_classes,
    import_metadata,
    import_object_classes,
    import_object_metadata,
    import_object_parameter_value_metadata,
    import_object_parameter_values,
    import_object_parameters,
    import_objects,
    import_parameter_definitions,
    import_parameter_types,
    import_parameter_value_lists,
    import_parameter_values,
    import_relationship_classes,
    import_relationship_metadata,
    import_relationship_parameter_value_metadata,
    import_relationship_parameter_values,
    import_relationship_parameters,
    import_relationships,
    import_scenario_alternatives,
    import_scenarios,
)
from spinedb_api.parameter_value import (
    Map,
    TimePattern,
    TimeSeriesFixedResolution,
    dump_db_value,
    from_database,
    to_database,
)
from spinedb_api.spine_db_server import _unparse_value
from tests.mock_helpers import AssertSuccessTestCase


def assert_imports(result: tuple[int, list[str]]) -> int:
    count, errors = result
    assert not errors
    return count


def assert_import_equivalent(test, obs, exp, strict=True):
    """Helper function to assert that two dictionaries will have the same effect if passed to ``import_data()``"""
    if strict:
        test.assertEqual(obs.keys(), exp.keys())
    for key in obs:
        obs_vals = []
        for val in obs[key]:
            if val not in obs_vals:
                obs_vals.append(val)
        exp_vals = []
        for val in exp[key]:
            if val not in exp_vals:
                exp_vals.append(val)
        _assert_same_elements(test, obs_vals, exp_vals)


def _assert_same_elements(test, obs_vals, exp_vals):
    if isinstance(obs_vals, (tuple, list)) and isinstance(exp_vals, (tuple, list)):
        test.assertEqual(len(obs_vals), len(exp_vals))
        for k, exp_val in enumerate(exp_vals):
            try:
                obs_val = obs_vals[k]
            except IndexError:
                obs_val = None
            _assert_same_elements(test, obs_val, exp_val)
        return
    test.assertEqual(obs_vals, exp_vals)


class TestIntegrationImportData(AssertSuccessTestCase):
    def test_import_data_integration(self):
        with DatabaseMapping("sqlite://", username="IntegrationTest", create=True) as db_map:
            object_c = ["example_class", "other_class"]  # 2 items
            objects = [["example_class", "example_object"], ["other_class", "other_object"]]  # 2 items
            relationship_c = [["example_rel_class", ["example_class", "other_class"]]]  # 1 item
            relationships = [["example_rel_class", ["example_object", "other_object"]]]  # 1 item
            obj_parameters = [["example_class", "example_parameter"]]  # 1 item
            rel_parameters = [["example_rel_class", "rel_parameter"]]  # 1 item
            object_p_values = [["example_class", "example_object", "example_parameter", 3.14]]  # 1 item
            rel_p_values = [["example_rel_class", ["example_object", "other_object"], "rel_parameter", 2.718]]  # 1
            alternatives = [["example_alternative", "An example"]]
            scenarios = [["example_scenario", True, "An example"]]
            scenario_alternatives = [["example_scenario", "example_alternative"]]

            num_imports = self._assert_imports(
                import_data(
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
            )
        self.assertEqual(num_imports, 13)


class TestImportObjectClass(AssertSuccessTestCase):
    def test_import_object_class(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_object_classes(db_map, ["new_class"]))
            db_map.commit_session("test")
            self.assertEqual(["new_class"], [oc.name for oc in db_map.query(db_map.object_class_sq)])


class TestImportObject(AssertSuccessTestCase):
    def test_import_valid_objects(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_object_classes(db_map, ["object_class"])
            _, errors = import_objects(db_map, [["object_class", "new_object"]])
            self.assertFalse(errors)
            db_map.commit_session("test")
            self.assertIn("new_object", [o.name for o in db_map.query(db_map.object_sq)])

    def test_import_object_with_invalid_object_class_name(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            _, errors = import_objects(db_map, [["nonexistent_class", "new_object"]])
            self.assertTrue(errors)

    def test_import_two_objects_with_same_name(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_object_classes(db_map, ["object_class1", "object_class2"])
            _, errors = import_objects(db_map, [["object_class1", "object"], ["object_class2", "object"]])
            self.assertFalse(errors)
            db_map.commit_session("test")
            objects = {
                o.class_name: o.name
                for o in db_map.query(
                    db_map.object_sq.c.name.label("name"), db_map.object_class_sq.c.name.label("class_name")
                ).join(db_map.object_class_sq, db_map.object_sq.c.class_id == db_map.object_class_sq.c.id)
            }
            expected = {"object_class1": "object", "object_class2": "object"}
            self.assertEqual(objects, expected)

    def test_import_existing_object(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_object_classes(db_map, ["object_class"])
            import_objects(db_map, [["object_class", "object"]])
            db_map.commit_session("test")
            self.assertIn("object", [o.name for o in db_map.query(db_map.object_sq)])
            _, errors = import_objects(db_map, [["object_class", "object"]])
            self.assertFalse(errors)
            self.assertIn("object", [o.name for o in db_map.query(db_map.object_sq)])


class TestImportRelationshipClass(AssertSuccessTestCase):
    def test_import_valid_relationship_class(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_object_classes(db_map, ["object_class1", "object_class2"])
            _, errors = import_relationship_classes(
                db_map, [["relationship_class", ["object_class1", "object_class2"]]]
            )
            self.assertFalse(errors)
            db_map.commit_session("test")
            relationship_classes = {
                rc.name: rc.object_class_name_list for rc in db_map.query(db_map.wide_relationship_class_sq)
            }
            expected = {"relationship_class": "object_class1,object_class2"}
            self.assertEqual(relationship_classes, expected)

    def test_import_relationship_class_with_invalid_object_class_name(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_object_classes(db_map, ["object_class"])
            _, errors = import_relationship_classes(db_map, [["relationship_class", ["object_class", "nonexistent"]]])
            self.assertTrue(errors)
            db_map.commit_session("test")
            self.assertFalse(db_map.query(db_map.wide_relationship_class_sq).all())

    def test_import_relationship_class_name_twice(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_object_classes(db_map, ["object_class1", "object_class2"])
            _, errors = import_relationship_classes(
                db_map, [["new_rc", ["object_class1", "object_class2"]], ["new_rc", ["object_class1", "object_class2"]]]
            )
            self.assertFalse(errors)
            db_map.commit_session("test")
            relationship_classes = {
                rc.name: rc.object_class_name_list for rc in db_map.query(db_map.wide_relationship_class_sq)
            }
            expected = {"new_rc": "object_class1,object_class2"}
            self.assertEqual(relationship_classes, expected)

    def test_import_existing_relationship_class(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_object_classes(db_map, ["object_class1", "object_class2"])
            import_relationship_classes(db_map, [["rc", ["object_class1", "object_class2"]]])
            _, errors = import_relationship_classes(db_map, [["rc", ["object_class1", "object_class2"]]])
            self.assertFalse(errors)

    def test_import_relationship_class_with_one_object_class_as_None(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_object_classes(db_map, ["object_class1"])
            _, errors = import_relationship_classes(db_map, [["new_rc", ["object_class", None]]])
            self.assertTrue(errors)
            db_map.commit_session("test")
            self.assertFalse([rc for rc in db_map.query(db_map.wide_relationship_class_sq)])


class TestImportObjectClassParameter(AssertSuccessTestCase):
    def test_import_valid_object_class_parameter(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_object_classes(db_map, ["object_class"])
            _, errors = import_object_parameters(db_map, [["object_class", "new_parameter"]])
            self.assertFalse(errors)
            db_map.commit_session("test")
            self.assertIn("new_parameter", [p.name for p in db_map.query(db_map.parameter_definition_sq)])

    def test_import_parameter_with_invalid_object_class_name(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            _, errors = import_object_parameters(db_map, [["nonexistent_object_class", "new_parameter"]])
            self.assertTrue(errors)

    def test_import_object_class_parameter_name_twice(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_object_classes(db_map, ["object_class1", "object_class2"])
            _, errors = import_object_parameters(
                db_map, [["object_class1", "new_parameter"], ["object_class2", "new_parameter"]]
            )
            self.assertFalse(errors)
            db_map.commit_session("test")
            definitions = {
                definition.object_class_name: definition.parameter_name
                for definition in db_map.query(db_map.object_parameter_definition_sq)
            }
            expected = {"object_class1": "new_parameter", "object_class2": "new_parameter"}
            self.assertEqual(definitions, expected)

    def test_import_existing_object_class_parameter(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_object_classes(db_map, ["object_class"])
            import_object_parameters(db_map, [["object_class", "parameter"]])
            db_map.commit_session("test")
            self.assertIn("parameter", [p.name for p in db_map.query(db_map.parameter_definition_sq)])
            _, errors = import_object_parameters(db_map, [["object_class", "parameter"]])
            self.assertIn("parameter", [p.name for p in db_map.query(db_map.parameter_definition_sq)])
            self.assertFalse(errors)

    def test_import_object_class_parameter_with_None_default_value_and_db_server_unparsing(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_object_classes(db_map, ["object_class"])
            _, errors = import_object_parameters(
                db_map, [["object_class", "parameter", [None, None]]], unparse_value=_unparse_value
            )
            self.assertEqual(errors, [])
            db_map.commit_session("Add test data.")
            parameters = db_map.query(db_map.object_parameter_definition_sq).all()
            self.assertEqual(len(parameters), 1)
            self.assertIsNone(parameters[0].default_value)
            self.assertIsNone(parameters[0].default_type)


class TestImportRelationshipClassParameter(AssertSuccessTestCase):
    def test_import_valid_relationship_class_parameter(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_object_classes(db_map, ["object_class1", "object_class2"])
            import_relationship_classes(db_map, [["relationship_class", ["object_class1", "object_class2"]]])
            _, errors = import_relationship_parameters(db_map, [["relationship_class", "new_parameter"]])
            self.assertFalse(errors)
            db_map.commit_session("test")
            definitions = {
                d.class_name: d.name
                for d in db_map.query(
                    db_map.relationship_parameter_definition_sq.c.parameter_name.label("name"),
                    db_map.relationship_class_sq.c.name.label("class_name"),
                ).join(
                    db_map.relationship_class_sq,
                    db_map.relationship_parameter_definition_sq.c.relationship_class_id
                    == db_map.relationship_class_sq.c.id,
                )
            }
            expected = {"relationship_class": "new_parameter"}
            self.assertEqual(definitions, expected)

    def test_import_parameter_with_invalid_relationship_class_name(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            _, errors = import_relationship_parameters(db_map, [["nonexistent_relationship_class", "new_parameter"]])
            self.assertTrue(errors)

    def test_import_relationship_class_parameter_name_twice(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
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
            db_map.commit_session("test")
            definitions = {
                d.class_name: d.name
                for d in db_map.query(
                    db_map.relationship_parameter_definition_sq.c.parameter_name.label("name"),
                    db_map.relationship_class_sq.c.name.label("class_name"),
                ).join(
                    db_map.relationship_class_sq,
                    db_map.relationship_parameter_definition_sq.c.relationship_class_id
                    == db_map.relationship_class_sq.c.id,
                )
            }
            expected = {"relationship_class1": "new_parameter", "relationship_class2": "new_parameter"}
            self.assertEqual(definitions, expected)

    def test_import_existing_relationship_class_parameter(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_object_classes(db_map, ["object_class1", "object_class2"])
            import_relationship_classes(db_map, [["relationship_class", ["object_class1", "object_class2"]]])
            import_relationship_parameters(db_map, [["relationship_class", "new_parameter"]])
            _, errors = import_relationship_parameters(db_map, [["relationship_class", "new_parameter"]])
            self.assertFalse(errors)


class TestImportEntityClasses(AssertSuccessTestCase):
    def test_import_object_class_with_all_optional_data(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(
                import_entity_classes(
                    db_map,
                    (
                        ("Object", (), "The test class.", 23, True),
                        ("Relation", ("Object",), "The test relationship.", 5, False),
                    ),
                )
            )
            entity_classes = db_map.get_entity_class_items()
            self.assertEqual(len(entity_classes), 2)
            data = (
                (
                    row["name"],
                    row["dimension_name_list"],
                    row["description"],
                    row["display_icon"],
                    row["active_by_default"],
                )
                for row in entity_classes
            )
            expected = (
                ("Object", (), "The test class.", 23, True),
                ("Relation", ("Object",), "The test relationship.", 5, False),
            )
            self.assertCountEqual(data, expected)


class TestImportEntity(AssertSuccessTestCase):
    def test_import_multi_d_entity_twice(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_data(
                db_map,
                entity_classes=(
                    ("object_class1",),
                    ("object_class2",),
                    ("relationship_class", ("object_class1", "object_class2")),
                ),
                entities=(
                    ("object_class1", "object1"),
                    ("object_class2", "object2"),
                    ("relationship_class", ("object1", "object2")),
                ),
            )
            count, errors = import_data(db_map, entities=(("relationship_class", ("object1", "object2")),))
            self.assertEqual(count, 0)
            self.assertEqual(errors, [])

    def test_multidimensional_entities(self):
        data = {
            "entity_classes": [
                ("Unit",),
                ("Node",),
                ("Unit__Node", ("Unit", "Node")),
                ("Node__Unit", ("Node", "Unit")),
                ("Unit__Node__Node__Unit", ("Unit__Node", "Node__Unit")),
            ],
            "entities": [
                ("Unit", "u"),
                ("Node", "n"),
                ("Node__Unit", ("n", "u")),
                ("Unit__Node", ("u", "n")),
                ("Unit__Node__Node__Unit", ("u", "n", "n", "u")),
            ],
        }
        with DatabaseMapping("sqlite://", create=True) as db_map:
            count, errors = import_data(db_map, **data)
            self.assertEqual(errors, [])
            self.assertEqual(count, 10)
            classes = db_map.find_entity_classes()
            self.assertCountEqual(
                [c["name"] for c in classes], ["Unit", "Node", "Unit__Node", "Node__Unit", "Unit__Node__Node__Unit"]
            )
            entities = db_map.find_entities()
            self.assertCountEqual([e["name"] for e in entities], ["u", "n", "u__n", "n__u", "u__n__n__u"])

    def test_multidimensional_entities_in_superclasses(self):
        data = {
            "entity_classes": [
                ["node", [], None, None, True],
                ["unit", [], None, None, True],
                ["unit_flow", [], None, None, True],
                ["node__unit", ["node", "unit"], None, None, True],
                ["unit__node", ["unit", "node"], None, None, True],
                ["unit_flow__unit_flow", ["unit_flow", "unit_flow"], None, None, True],
            ],
            "superclass_subclasses": [
                ["unit_flow", "node__unit"],
                ["unit_flow", "unit__node"],
            ],
            "entities": [
                ["node", "n1", None],
                ["node", "n2", None],
                ["unit", "u1", None],
                ["unit", "u2", None],
                ["node__unit", ["n1", "u1"], None],
                ["node__unit", ["n2", "u2"], None],
                ["unit__node", ["u1", "n2"], None],
                ["unit__node", ["u2", "n1"], None],
                ["unit_flow__unit_flow", ["n1", "u1", "n2", "u2"], None],
                ["unit_flow__unit_flow", ["u1", "n2", "u2", "n1"], None],
                ["unit_flow__unit_flow", ["n1", "u1", "u1", "n2"], None],
                ["unit_flow__unit_flow", ["u2", "n1", "n2", "u2"], None],
            ],
        }
        with DatabaseMapping("sqlite://", create=True) as db_map:
            count, errors = import_data(db_map, **data)
            self.assertEqual(errors, [])
            classes = {c["name"]: c["dimension_name_list"] for c in db_map.find_entity_classes()}
            self.assertEqual(len(classes), 6)
            self.assertEqual(
                [
                    classes["node"],
                    classes["unit"],
                    classes["unit_flow"],
                    classes["node__unit"],
                    classes["unit__node"],
                    classes["unit_flow__unit_flow"],
                ],
                [(), (), (), ("node", "unit"), ("unit", "node"), ("unit_flow", "unit_flow")],
            )
            superclasses = db_map.find_superclass_subclasses()
            self.assertEqual(len(superclasses), 2)
            subclasses = {}
            for superclass in superclasses:
                subclasses.setdefault(superclass["superclass_name"], []).append(superclass["subclass_name"])
            self.assertEqual(len(subclasses), 1)
            self.assertCountEqual(subclasses["unit_flow"], ["node__unit", "unit__node"])
            entities = {e["name"]: e for e in db_map.find_entities()}
            self.assertEqual(len(entities), 12)
            self.assertIn("n1", entities)
            self.assertIn("n2", entities)
            self.assertIn("u1", entities)
            self.assertIn("u2", entities)
            self.assertIn("n1__u1", entities)
            self.assertIn("n2__u2", entities)
            self.assertIn("u1__n2", entities)
            self.assertIn("u2__n1", entities)
            self.assertEqual(entities["n1__u1__n2__u2"]["entity_byname"], ("n1", "u1", "n2", "u2"))
            self.assertEqual(entities["n1__u1__n2__u2"]["dimension_name_list"], ("unit_flow", "unit_flow"))
            self.assertEqual(entities["u1__n2__u2__n1"]["entity_byname"], ("u1", "n2", "u2", "n1"))
            self.assertEqual(entities["u1__n2__u2__n1"]["dimension_name_list"], ("unit_flow", "unit_flow"))
            self.assertEqual(entities["n1__u1__u1__n2"]["entity_byname"], ("n1", "u1", "u1", "n2"))
            self.assertEqual(entities["n1__u1__u1__n2"]["dimension_name_list"], ("unit_flow", "unit_flow"))
            self.assertEqual(entities["u2__n1__n2__u2"]["entity_byname"], ("u2", "n1", "n2", "u2"))
            self.assertEqual(entities["u2__n1__n2__u2"]["dimension_name_list"], ("unit_flow", "unit_flow"))

    def test_nonexistent_multidimensional_entities_in_superclasses_get_rejected(self):
        data = {
            "entity_classes": [
                ["node", [], None, None, True],
                ["unit", [], None, None, True],
                ["unit_flow", [], None, None, True],
                ["node__unit", ["node", "unit"], None, None, True],
                ["unit__node", ["unit", "node"], None, None, True],
                ["unit_flow__unit_flow", ["unit_flow", "unit_flow"], None, None, True],
            ],
            "superclass_subclasses": [
                ["unit_flow", "node__unit"],
                ["unit_flow", "unit__node"],
            ],
            "entities": [
                ["node", "n1", None],
                ["node", "n2", None],
                ["unit", "u1", None],
                ["unit", "u2", None],
                ["node__unit", ["n1", "u1"], None],
                ["node__unit", ["n2", "u2"], None],
                ["unit__node", ["u1", "n2"], None],
                ["unit__node", ["u2", "n1"], None],
                ["unit_flow__unit_flow", ["n1", "u2", "n2", "u2"], None],
                ["unit_flow__unit_flow", ["n2", "u2", "n1", "u2"], None],
                ["unit_flow__unit_flow", ["u1", "n1", "u2", "n1"], None],
                ["unit_flow__unit_flow", ["u1", "n2", "u1", "n1"], None],
                ["unit_flow__unit_flow", ["n1", "u2", "u1", "n2"], None],
                ["unit_flow__unit_flow", ["n1", "u1", "u1", "n1"], None],
                ["unit_flow__unit_flow", ["u1", "n1", "n2", "u2"], None],
                ["unit_flow__unit_flow", ["u2", "n1", "n2", "u1"], None],
            ],
        }
        with DatabaseMapping("sqlite://", create=True) as db_map:
            count, errors = import_data(db_map, **data)
            self.assertEqual(
                errors,
                [
                    "non-existent elements in byname ('n1', 'u2', 'n2', 'u2') for class " "unit_flow__unit_flow",
                    "non-existent elements in byname ('n2', 'u2', 'n1', 'u2') for class " "unit_flow__unit_flow",
                    "non-existent elements in byname ('u1', 'n1', 'u2', 'n1') for class " "unit_flow__unit_flow",
                    "non-existent elements in byname ('u1', 'n2', 'u1', 'n1') for class " "unit_flow__unit_flow",
                    "non-existent elements in byname ('n1', 'u2', 'u1', 'n2') for class " "unit_flow__unit_flow",
                    "non-existent elements in byname ('n1', 'u1', 'u1', 'n1') for class " "unit_flow__unit_flow",
                    "non-existent elements in byname ('u1', 'n1', 'n2', 'u2') for class " "unit_flow__unit_flow",
                    "non-existent elements in byname ('u2', 'n1', 'n2', 'u1') for class " "unit_flow__unit_flow",
                ],
            )
            classes = {c["name"]: c["dimension_name_list"] for c in db_map.find_entity_classes()}
            self.assertEqual(len(classes), 6)
            self.assertEqual(
                [
                    classes["node"],
                    classes["unit"],
                    classes["unit_flow"],
                    classes["node__unit"],
                    classes["unit__node"],
                    classes["unit_flow__unit_flow"],
                ],
                [(), (), (), ("node", "unit"), ("unit", "node"), ("unit_flow", "unit_flow")],
            )
            superclasses = db_map.find_superclass_subclasses()
            self.assertEqual(len(superclasses), 2)
            subclasses = {}
            for superclass in superclasses:
                subclasses.setdefault(superclass["superclass_name"], []).append(superclass["subclass_name"])
            self.assertEqual(len(subclasses), 1)
            self.assertCountEqual(subclasses["unit_flow"], ["node__unit", "unit__node"])
            entities = {e["name"]: e for e in db_map.find_entities()}
            self.assertEqual(len(entities), 8)
            self.assertIn("n1", entities)
            self.assertIn("n2", entities)
            self.assertIn("u1", entities)
            self.assertIn("u2", entities)
            self.assertIn("n1__u1", entities)
            self.assertIn("n2__u2", entities)
            self.assertIn("u1__n2", entities)
            self.assertIn("u2__n1", entities)

    def test_import_subclass_with_duplicate_entity_names(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="node")
            db_map.add_entity_class(name="unit")
            db_map.add_entity_class(name="node__unit", dimension_name_list=("node", "unit"))
            db_map.add_entity_class(name="unit__node", dimension_name_list=("unit", "node"))
            db_map.add_entity_class(name="flow")
            db_map.add_superclass_subclass(superclass_name="flow", subclass_name="node__unit")
            db_map.add_superclass_subclass(superclass_name="flow", subclass_name="unit__node")
            db_map.add_entity_class(name="flow__flow", dimension_name_list=("flow", "flow"))
            db_map.add_entity(entity_class_name="node", name="a")
            db_map.add_entity(entity_class_name="unit", name="a")
            count, errors = import_entities(db_map, (("node__unit", ("a", "a")), ("unit__node", ("a", "a"))))
            self.assertEqual(errors, [])
            self.assertEqual(count, 2)
            entities = db_map.find_entities(entity_class_name="node__unit")
            self.assertEqual(entities, [])
            entities = db_map.find_entities(entity_class_name="unit__node")
            self.assertEqual(len(entities), 1)
            self.assertEqual(entities[0]["entity_class_name"], "unit__node")
            self.assertEqual(entities[0]["entity_byname"], ("a", "a"))
            count, errors = import_entities(db_map, (("flow__flow", ("a", "a", "a", "a")),))
            self.assertEqual(errors, [])
            self.assertEqual(count, 1)
            entities = db_map.find_entities(entity_class_name="flow__flow")
            self.assertEqual(len(entities), 1)
            self.assertEqual(entities[0]["entity_class_name"], "flow__flow")
            self.assertEqual(entities[0]["dimension_name_list"], ("flow", "flow"))

    def test_entity_location(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_entity_classes(db_map, [("Object",)]))
            self._assert_imports(
                import_entities(db_map, [("Object", "shape", None, (2.3, 3.2, 55.0, "polyhedron", "{}"))])
            )
            shape = db_map.entity(entity_class_name="Object", name="shape")
            self.assertEqual(shape["lat"], 2.3)
            self.assertEqual(shape["lon"], 3.2)
            self.assertEqual(shape["alt"], 55.0)
            self.assertEqual(shape["shape_name"], "polyhedron")
            self.assertEqual(shape["shape_blob"], "{}")


class TestImportRelationship(AssertSuccessTestCase):
    def populate(self, db_map):
        self._assert_imports(import_object_classes(db_map, ["object_class1", "object_class2"]))
        self._assert_imports(import_objects(db_map, [["object_class1", "object1"], ["object_class2", "object2"]]))

    def test_import_relationships(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_object_classes(db_map, ("object_class",)))
            self._assert_imports(import_objects(db_map, (("object_class", "object"),)))
            self._assert_imports(import_relationship_classes(db_map, (("relationship_class", ("object_class",)),)))
            self._assert_imports(import_relationships(db_map, (("relationship_class", ("object",)),)))
            db_map.commit_session("test")
            self.assertIn("object__", [r.name for r in db_map.query(db_map.relationship_sq)])

    def test_import_valid_relationship(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            self._assert_imports(
                import_relationship_classes(db_map, [["relationship_class", ["object_class1", "object_class2"]]])
            )
            self._assert_imports(import_relationships(db_map, [["relationship_class", ["object1", "object2"]]]))
            db_map.commit_session("test")
            self.assertIn("object1__object2", [r.name for r in db_map.query(db_map.relationship_sq)])

    def test_import_valid_relationship_with_object_name_in_multiple_classes(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            self._assert_imports(
                import_objects(db_map, [["object_class1", "duplicate"], ["object_class2", "duplicate"]])
            )
            self._assert_imports(
                import_relationship_classes(db_map, [["relationship_class", ["object_class1", "object_class2"]]])
            )
            self._assert_imports(import_relationships(db_map, [["relationship_class", ["duplicate", "object2"]]]))
            db_map.commit_session("test")
            self.assertIn("duplicate__object2", [r.name for r in db_map.query(db_map.relationship_sq)])

    def test_import_relationship_with_invalid_class_name(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            _, errors = import_relationships(db_map, [["nonexistent_relationship_class", ["object1", "object2"]]])
            self.assertTrue(errors)
            db_map.commit_session("test")
            self.assertFalse([r.name for r in db_map.query(db_map.relationship_sq)])

    def test_import_relationship_with_invalid_object_name(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            self._assert_imports(
                import_relationship_classes(db_map, [["relationship_class", ["object_class1", "object_class2"]]])
            )
            _, errors = import_relationships(db_map, [["relationship_class", ["nonexistent_object", "object2"]]])
            self.assertTrue(errors)
            db_map.commit_session("test")
            self.assertFalse([r.name for r in db_map.query(db_map.relationship_sq)])

    def test_import_existing_relationship(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            self._assert_imports(
                import_relationship_classes(db_map, [["relationship_class", ["object_class1", "object_class2"]]])
            )
            self._assert_imports(import_relationships(db_map, [["relationship_class", ["object1", "object2"]]]))
            db_map.commit_session("test")
            self.assertIn("object1__object2", [r.name for r in db_map.query(db_map.relationship_sq)])
            self._assert_imports(import_relationships(db_map, [["relationship_class", ["object1", "object2"]]]))
            self.assertIn("object1__object2", [r.name for r in db_map.query(db_map.relationship_sq)])

    def test_import_relationship_with_one_None_object(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            import_relationship_classes(db_map, [["relationship_class", ["object_class1", "object_class2"]]])
            _, errors = import_relationships(db_map, [["relationship_class", [None, "object2"]]])
            self.assertTrue(errors)
            db_map.commit_session("test")
            self.assertFalse([r.name for r in db_map.query(db_map.relationship_sq)])

    def test_import_multi_d_entity_with_elements_from_superclass(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(
                import_data(
                    db_map,
                    entity_classes=[
                        ["object_class1", []],
                        ["object_class2", []],
                        ["superclass", []],
                        ["relationship_class1", ["superclass", "superclass"]],
                    ],
                    superclass_subclasses=[["superclass", "object_class1"], ["superclass", "object_class2"]],
                    entities=[["object_class1", "object1"], ["object_class2", "object2"]],
                )
            )
            self._assert_imports(import_data(db_map, entities=[["relationship_class1", ["object1", "object2"]]]))
            db_map.commit_session("test")
            entities = {
                tuple(r.element_name_list.split(",")) if r.element_name_list else r.name: r.name
                for r in db_map.query(db_map.wide_entity_sq)
            }
            self.assertTrue("object1" in entities)
            self.assertTrue("object2" in entities)
            self.assertTrue(("object1", "object2") in entities)
            self.assertEqual(len(entities), 3)

    def test_import_multi_d_entity_with_elements_from_superclass_fails_with_wrong_dimension_count(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(
                import_data(
                    db_map,
                    entity_classes=[
                        ["object_class1", []],
                        ["object_class2", []],
                        ["superclass", []],
                        ["relationship_class1", ["superclass", "superclass"]],
                    ],
                    superclass_subclasses=[["superclass", "object_class1"], ["superclass", "object_class2"]],
                    entities=[["object_class1", "object1"], ["object_class2", "object2"]],
                )
            )
            _, errors = import_data(db_map, entities=[["relationship_class1", ["object1"]]])
            self.assertEqual(len(errors), 1)
            self.assertIn("too few elements", errors[0])
            _, errors = import_data(db_map, entities=[["relationship_class1", ["object1", "object2", "object1"]]])
            self.assertEqual(len(errors), 1)
            self.assertIn("too many elements", errors[0])

    def test_import_multi_d_entity_with_multi_d_elements(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            self._assert_imports(
                import_data(
                    db_map,
                    entity_classes=[
                        ["relationship_class1", ["object_class1", "object_class2"]],
                        ["relationship_class2", ["object_class2", "object_class1"]],
                        ["meta_relationship_class", ["relationship_class1", "relationship_class2"]],
                    ],
                    entities=[
                        ["relationship_class1", ["object1", "object2"]],
                        ["relationship_class2", ["object2", "object1"]],
                    ],
                )
            )
            self._assert_imports(
                import_data(
                    db_map, entities=[["meta_relationship_class", ["object1", "object2", "object2", "object1"]]]
                )
            )
            db_map.commit_session("test")
            entities = {
                tuple(r.element_name_list.split(",")) if r.element_name_list else r.name: r.name
                for r in db_map.query(db_map.wide_entity_sq)
            }
            self.assertTrue("object1" in entities)
            self.assertTrue("object2" in entities)
            self.assertTrue(("object1", "object2") in entities)
            self.assertTrue(("object2", "object1") in entities)
            self.assertTrue((entities["object1", "object2"], entities["object2", "object1"]) in entities)
            self.assertEqual(len(entities), 5)

    def test_import_multi_d_entity_with_multi_d_elements_from_superclass(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            self._assert_imports(
                import_data(
                    db_map,
                    entity_classes=[
                        ["relationship_class1", ["object_class1", "object_class2"]],
                        ["relationship_class2", ["object_class2", "object_class1"]],
                        ["superclass", []],
                    ],
                    superclass_subclasses=[
                        ["superclass", "relationship_class1"],
                        ["superclass", "relationship_class2"],
                    ],
                )
            )
            self._assert_imports(
                import_data(
                    db_map,
                    entity_classes=[["meta_relationship_class", ["superclass", "superclass"]]],
                    entities=[
                        ["relationship_class1", ["object1", "object2"]],
                        ["relationship_class2", ["object2", "object1"]],
                    ],
                )
            )
            self._assert_imports(
                import_data(
                    db_map, entities=[["meta_relationship_class", ["object1", "object2", "object2", "object1"]]]
                )
            )
            db_map.commit_session("test")
            entities = {
                tuple(r.element_name_list.split(",")) if r.element_name_list else r.name: r.name
                for r in db_map.query(db_map.wide_entity_sq)
            }
            self.assertTrue("object1" in entities)
            self.assertTrue("object2" in entities)
            self.assertTrue(("object1", "object2") in entities)
            self.assertTrue(("object2", "object1") in entities)
            self.assertTrue((entities["object1", "object2"], entities["object2", "object1"]) in entities)
            self.assertEqual(len(entities), 5)

    def test_import_multi_d_entity_with_multi_d_elements_from_superclass_fails_with_wrong_dimension_count(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            self._assert_imports(
                import_data(
                    db_map,
                    entity_classes=[
                        ["relationship_class1", ["object_class1", "object_class2"]],
                        ["relationship_class2", ["object_class2", "object_class1"]],
                        ["superclass", []],
                    ],
                    superclass_subclasses=[
                        ["superclass", "relationship_class1"],
                        ["superclass", "relationship_class2"],
                    ],
                )
            )
            self._assert_imports(
                import_data(
                    db_map,
                    entity_classes=[["meta_relationship_class", ["superclass", "superclass"]]],
                    entities=[
                        ["relationship_class1", ["object1", "object2"]],
                        ["relationship_class2", ["object2", "object1"]],
                    ],
                )
            )
            _, errors = import_data(db_map, entities=[["meta_relationship_class", ["object1", "object2", "object2"]]])
            self.assertEqual(len(errors), 1)
            self.assertIn("too few elements", errors[0])
            _, errors = import_data(
                db_map, entities=[["meta_relationship_class", ["object1", "object2", "object2", "object1", "object1"]]]
            )
            self.assertEqual(len(errors), 1)
            self.assertIn("too many elements", errors[0])


class TestImportParameterDefinition(AssertSuccessTestCase):
    def test_import_object_parameter_definition(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_object_classes(db_map, ["my_object_class"]))
            count = self._assert_imports(import_object_parameters(db_map, (("my_object_class", "my_parameter"),)))
            self.assertEqual(count, 1)
            db_map.commit_session("Add test data.")
            parameter_definitions = [row._asdict() for row in db_map.query(db_map.object_parameter_definition_sq)]
            self.assertEqual(
                parameter_definitions,
                [
                    {
                        "default_type": None,
                        "default_value": None,
                        "description": None,
                        "entity_class_id": 1,
                        "entity_class_name": "my_object_class",
                        "id": 1,
                        "object_class_id": 1,
                        "object_class_name": "my_object_class",
                        "parameter_name": "my_parameter",
                        "value_list_id": None,
                        "value_list_name": None,
                    }
                ],
            )

    def test_import_object_parameter_definition_with_value_list(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_object_classes(db_map, ["my_object_class"]))
            self._assert_imports(import_parameter_value_lists(db_map, (("my_list", 99.0),)))
            count = self._assert_imports(
                import_object_parameters(db_map, (("my_object_class", "my_parameter", None, "my_list"),))
            )
            self.assertEqual(count, 1)
            db_map.commit_session("Add test data.")
            parameter_definitions = [row._asdict() for row in db_map.query(db_map.object_parameter_definition_sq)]
            self.assertEqual(
                parameter_definitions,
                [
                    {
                        "default_type": None,
                        "default_value": b"null",
                        "description": None,
                        "entity_class_id": 1,
                        "entity_class_name": "my_object_class",
                        "id": 1,
                        "object_class_id": 1,
                        "object_class_name": "my_object_class",
                        "parameter_name": "my_parameter",
                        "value_list_id": 1,
                        "value_list_name": "my_list",
                    }
                ],
            )

    def test_import_object_parameter_definition_with_default_value_from_value_list(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_object_classes(db_map, ["my_object_class"]))
            self._assert_imports(import_parameter_value_lists(db_map, (("my_list", 99.0),)))
            count = self._assert_imports(
                import_object_parameters(db_map, (("my_object_class", "my_parameter", 99.0, "my_list"),))
            )
            self.assertEqual(count, 1)
            db_map.commit_session("Add test data.")
            parameter_definitions = [row._asdict() for row in db_map.query(db_map.object_parameter_definition_sq)]
            self.assertEqual(
                parameter_definitions,
                [
                    {
                        "default_type": "float",
                        "default_value": b"99.0",
                        "description": None,
                        "entity_class_id": 1,
                        "entity_class_name": "my_object_class",
                        "id": 1,
                        "object_class_id": 1,
                        "object_class_name": "my_object_class",
                        "parameter_name": "my_parameter",
                        "value_list_id": 1,
                        "value_list_name": "my_list",
                    }
                ],
            )

    def test_import_object_parameter_definition_with_default_value_from_value_list_fails_gracefully(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_object_classes(db_map, ["my_object_class"]))
            self._assert_imports(import_parameter_value_lists(db_map, (("my_list", 99.0),)))
            count, errors = import_object_parameters(db_map, (("my_object_class", "my_parameter", 23.0, "my_list"),))
            self.assertEqual(errors, ["default value 23.0 of my_parameter is not in my_list"])
            self.assertEqual(count, 0)


class TestImportParameterType(AssertSuccessTestCase):
    def test_basic_types_without_rank(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_entity_classes(db_map, ["Gadget"]))
            self._assert_imports(import_parameter_definitions(db_map, [("Gadget", "width")]))
            self._assert_imports(import_parameter_types(db_map, [("Gadget", "width", "float")]))
            self._assert_imports(import_parameter_types(db_map, [("Gadget", "width", "bool")]))
            self._assert_imports(import_parameter_types(db_map, [("Gadget", "width", "str")]))
            self._assert_imports(import_parameter_types(db_map, [("Gadget", "width", "array")]))
            self._assert_imports(import_parameter_types(db_map, [("Gadget", "width", "time_pattern")]))
            self._assert_imports(import_parameter_types(db_map, [("Gadget", "width", "time_series")]))
            db_map.commit_session("Add parameter with types.")
            definition = db_map.query(db_map.parameter_definition_sq).one()
            types = (
                db_map.query(db_map.parameter_type_sq)
                .filter(db_map.parameter_type_sq.c.parameter_definition_id == definition.id)
                .all()
            )
            self.assertEqual(len(types), 6)
            expected_types = ["float", "bool", "str", "array", "time_pattern", "time_series"]
            self.assertCountEqual([t.type for t in types], expected_types)
            expected_ranks = {"float": 0, "bool": 0, "str": 0, "array": 1, "time_pattern": 1, "time_series": 1}
            for t in types:
                with self.subTest(type=t.type):
                    self.assertEqual(t.rank, expected_ranks[t.type])

    def test_map_with_intrinsic_rank(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_entity_classes(db_map, ["Gadget"]))
            self._assert_imports(import_parameter_definitions(db_map, [("Gadget", "width")]))
            self._assert_imports(import_parameter_types(db_map, [("Gadget", "width", "1d_map")]))
            self._assert_imports(import_parameter_types(db_map, [("Gadget", "width", "2d_map")]))
            self._assert_imports(import_parameter_types(db_map, [("Gadget", "width", "23d_map")]))
            db_map.commit_session("Add parameter with types.")
            definition = db_map.query(db_map.parameter_definition_sq).one()
            types = (
                db_map.query(db_map.parameter_type_sq)
                .filter(db_map.parameter_type_sq.c.parameter_definition_id == definition.id)
                .all()
            )
            self.assertEqual(len(types), 3)
            expected_types = ["map", "map", "map"]
            self.assertCountEqual([t.type for t in types], expected_types)
            expected_ranks = {1, 2, 23}
            self.assertEqual({t.rank for t in types}, expected_ranks)

    def test_map_with_explicit_rank(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_entity_classes(db_map, ["Gadget"]))
            self._assert_imports(import_parameter_definitions(db_map, [("Gadget", "width")]))
            self._assert_imports(import_parameter_types(db_map, [("Gadget", "width", "map", 23)]))
            db_map.commit_session("Add parameter with types.")
            definition = db_map.query(db_map.parameter_definition_sq).one()
            parameter_type = (
                db_map.query(db_map.parameter_type_sq)
                .filter(db_map.parameter_type_sq.c.parameter_definition_id == definition.id)
                .one()
            )
            self.assertEqual(parameter_type.type, "map")
            self.assertEqual(parameter_type.rank, 23)

    def test_map_rank_reading_fails(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_entity_classes(db_map, ["Gadget"]))
            self._assert_imports(import_parameter_definitions(db_map, [("Gadget", "width")]))
            count, errors = import_parameter_types(db_map, [("Gadget", "width", "xd_map")])
            self.assertEqual(errors, ["Failed to read rank from type 'xd_map' for parameter width in class Gadget"])
            self.assertEqual(count, 0)
            count, errors = import_parameter_types(db_map, [("Gadget", "width", "map")])
            self.assertEqual(errors, ["Missing rank for map type for parameter width in class Gadget"])
            self.assertEqual(count, 0)


class TestImportParameterValue(AssertSuccessTestCase):
    def populate(self, db_map):
        self._assert_imports(import_object_classes(db_map, ["object_class1", "object_class2"]))
        self._assert_imports(import_objects(db_map, [["object_class1", "object1"], ["object_class2", "object2"]]))
        self._assert_imports(import_object_parameters(db_map, [["object_class1", "parameter"]]))

    def populate_with_relationship(self, db_map):
        self.populate(db_map)
        self._assert_imports(
            import_relationship_classes(db_map, [["relationship_class", ["object_class1", "object_class2"]]])
        )
        self._assert_imports(import_relationship_parameters(db_map, [["relationship_class", "parameter"]]))
        self._assert_imports(import_relationships(db_map, [["relationship_class", ["object1", "object2"]]]))

    def test_import_valid_object_parameter_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            self._assert_imports(import_object_parameter_values(db_map, [["object_class1", "object1", "parameter", 1]]))
            db_map.commit_session("test")
            values = {v.object_name: v.value for v in db_map.query(db_map.object_parameter_value_sq)}
            expected = {"object1": b"1"}
            self.assertEqual(values, expected)

    def test_import_valid_object_parameter_value_string(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            self._assert_imports(
                import_object_parameter_values(db_map, [["object_class1", "object1", "parameter", "value_string"]])
            )
            db_map.commit_session("test")
            values = {v.object_name: v.value for v in db_map.query(db_map.object_parameter_value_sq)}
            expected = {"object1": b'"value_string"'}
            self.assertEqual(values, expected)

    def test_import_valid_object_parameter_value_with_duplicate_object_name(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            self._assert_imports(
                import_objects(db_map, [["object_class1", "duplicate_object"], ["object_class2", "duplicate_object"]])
            )
            self._assert_imports(
                import_object_parameter_values(db_map, [["object_class1", "duplicate_object", "parameter", 1]])
            )
            db_map.commit_session("test")
            values = {
                v.object_class_name: {v.object_name: v.value} for v in db_map.query(db_map.object_parameter_value_sq)
            }
            expected = {"object_class1": {"duplicate_object": b"1"}}
            self.assertEqual(values, expected)

    def test_import_valid_object_parameter_value_with_duplicate_parameter_name(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            self._assert_imports(import_object_parameters(db_map, [["object_class2", "parameter"]]))
            self._assert_imports(import_object_parameter_values(db_map, [["object_class1", "object1", "parameter", 1]]))
            db_map.commit_session("test")
            values = {
                v.object_class_name: {v.object_name: v.value} for v in db_map.query(db_map.object_parameter_value_sq)
            }
            expected = {"object_class1": {"object1": b"1"}}
            self.assertEqual(values, expected)

    def test_import_object_parameter_value_with_invalid_object(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_object_classes(db_map, ["object_class"]))
            self._assert_imports(import_object_parameters(db_map, [["object_class", "parameter"]]))
            _, errors = import_object_parameter_values(db_map, [["object_class", "nonexistent_object", "parameter", 1]])
            self.assertTrue(errors)
            db_map.commit_session("test")
            self.assertFalse(db_map.query(db_map.object_parameter_value_sq).all())

    def test_import_object_parameter_value_with_invalid_parameter(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_object_classes(db_map, ["object_class"]))
            self._assert_imports(import_objects(db_map, [["object_class", "object"]]))
            _, errors = import_object_parameter_values(db_map, [["object_class", "object", "nonexistent_parameter", 1]])
            self.assertTrue(errors)
            db_map.commit_session("test")
            self.assertFalse(db_map.query(db_map.object_parameter_value_sq).all())

    def test_import_existing_object_parameter_value_update_the_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            self._assert_imports(
                import_object_parameter_values(db_map, [["object_class1", "object1", "parameter", "initial_value"]])
            )
            self._assert_imports(
                import_object_parameter_values(db_map, [["object_class1", "object1", "parameter", "new_value"]])
            )
            db_map.commit_session("test")
            values = {v.object_name: v.value for v in db_map.query(db_map.object_parameter_value_sq)}
            expected = {"object1": b'"new_value"'}
            self.assertEqual(values, expected)

    def test_import_existing_object_parameter_value_on_conflict_keep(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            initial_value = {"type": "time_series", "data": [("2000-01-01T01:00", "1"), ("2000-01-01T02:00", "2")]}
            new_value = {"type": "time_series", "data": [("2000-01-01T02:00", "3"), ("2000-01-01T03:00", "4")]}
            self._assert_imports(
                import_object_parameter_values(db_map, [["object_class1", "object1", "parameter", initial_value]])
            )
            _, errors = import_object_parameter_values(
                db_map, [["object_class1", "object1", "parameter", new_value]], on_conflict="keep"
            )
            self.assertFalse(errors)
            db_map.commit_session("test")
            pv = db_map.query(db_map.object_parameter_value_sq).filter_by(object_name="object1").first()
            value = from_database(pv.value, pv.type)
            self.assertEqual(["2000-01-01T01:00:00", "2000-01-01T02:00:00"], [str(x) for x in value.indexes])
            self.assertEqual([1.0, 2.0], list(value.values))

    def test_import_existing_object_parameter_value_on_conflict_replace(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            initial_value = {"type": "time_series", "data": [("2000-01-01T01:00", "1"), ("2000-01-01T02:00", "2")]}
            new_value = {"type": "time_series", "data": [("2000-01-01T02:00", "3"), ("2000-01-01T03:00", "4")]}
            self._assert_imports(
                import_object_parameter_values(db_map, [["object_class1", "object1", "parameter", initial_value]])
            )
            self._assert_imports(
                import_object_parameter_values(
                    db_map, [["object_class1", "object1", "parameter", new_value]], on_conflict="replace"
                )
            )
            db_map.commit_session("test")
            pv = db_map.query(db_map.object_parameter_value_sq).filter_by(object_name="object1").first()
            value = from_database(pv.value, pv.type)
            self.assertEqual(["2000-01-01T02:00:00", "2000-01-01T03:00:00"], [str(x) for x in value.indexes])
            self.assertEqual([3.0, 4.0], list(value.values))

    def test_import_existing_object_parameter_value_on_conflict_merge(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            initial_value = {"type": "time_series", "data": [("2000-01-01T01:00", "1"), ("2000-01-01T02:00", "2")]}
            new_value = {"type": "time_series", "data": [("2000-01-01T02:00", "3"), ("2000-01-01T03:00", "4")]}
            self._assert_imports(
                import_object_parameter_values(db_map, [["object_class1", "object1", "parameter", initial_value]])
            )
            self._assert_imports(
                import_object_parameter_values(
                    db_map, [["object_class1", "object1", "parameter", new_value]], on_conflict="merge"
                )
            )
            db_map.commit_session("test")
            pv = db_map.query(db_map.object_parameter_value_sq).filter_by(object_name="object1").first()
            value = from_database(pv.value, pv.type)
            self.assertEqual(
                ["2000-01-01T01:00:00", "2000-01-01T02:00:00", "2000-01-01T03:00:00"], [str(x) for x in value.indexes]
            )
            self.assertEqual([1.0, 3.0, 4.0], list(value.values))

    def test_import_existing_object_parameter_value_on_conflict_merge_map(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            initial_value = {
                "type": "map",
                "index_type": "str",
                "data": {
                    "xxx": {"type": "time_series", "data": [("2000-01-01T01:00", "1"), ("2000-01-01T02:00", "2")]}
                },
            }
            new_value = {
                "type": "map",
                "index_type": "str",
                "data": {
                    "xxx": {"type": "time_series", "data": [("2000-01-01T02:00", "3"), ("2000-01-01T03:00", "4")]}
                },
            }
            self._assert_imports(
                import_object_parameter_values(db_map, [["object_class1", "object1", "parameter", initial_value]])
            )
            self._assert_imports(
                import_object_parameter_values(
                    db_map, [["object_class1", "object1", "parameter", new_value]], on_conflict="merge"
                )
            )
            db_map.commit_session("test")
            pv = db_map.query(db_map.object_parameter_value_sq).filter_by(object_name="object1").first()
            map_ = from_database(pv.value, pv.type)
            self.assertEqual(["xxx"], [str(x) for x in map_.indexes])
            ts = map_.get_value("xxx")
            self.assertEqual(
                ["2000-01-01T01:00:00", "2000-01-01T02:00:00", "2000-01-01T03:00:00"], [str(x) for x in ts.indexes]
            )
            self.assertEqual([1.0, 3.0, 4.0], list(ts.values))

    def test_import_same_as_existing_value_on_conflict_merge_time_pattern(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_parameter_definition_item(name="X", entity_class_name="Object"))
            self._assert_success(db_map.add_entity_item(name="widget", entity_class_name="Object"))
            value = TimePattern(["D2-3", "D11-12"], [1.1, 2.2])
            db_value, value_type = to_database(value)
            self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="Object",
                    entity_byname=("widget",),
                    parameter_definition_name="X",
                    alternative_name="Base",
                    value=db_value,
                    type=value_type,
                )
            )
            self._assert_imports(
                import_parameter_values(db_map, [["Object", "widget", "X", value, "Base"]], on_conflict="merge")
            )
            value_item = db_map.get_parameter_value_item(
                entity_class_name="Object",
                entity_byname=("widget",),
                parameter_definition_name="X",
                alternative_name="Base",
            )
            self.assertTrue(value_item)
            self.assertEqual(value_item["parsed_value"], value)

    def test_import_existing_map_on_conflict_merge(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_parameter_definition_item(name="X", entity_class_name="Object"))
            self._assert_success(db_map.add_entity_item(name="widget", entity_class_name="Object"))
            value = Map(["T1", "T2"], [1.1, 1.2])
            db_value, value_type = to_database(value)
            self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="Object",
                    entity_byname=("widget",),
                    parameter_definition_name="X",
                    alternative_name="Base",
                    value=db_value,
                    type=value_type,
                )
            )
            extended_value = Map(["T3", "T4"], [1.3, 1.4])
            self._assert_imports(
                import_parameter_values(
                    db_map, [["Object", "widget", "X", extended_value, "Base"]], on_conflict="merge"
                )
            )
            value_item = db_map.get_parameter_value_item(
                entity_class_name="Object",
                entity_byname=("widget",),
                parameter_definition_name="X",
                alternative_name="Base",
            )
            self.assertTrue(value_item)
            merged_value = Map(["T1", "T2", "T3", "T4"], [1.1, 1.2, 1.3, 1.4])
            self.assertEqual(value_item["parsed_value"], merged_value)

    def test_import_duplicate_object_parameter_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            _, errors = import_object_parameter_values(
                db_map,
                [
                    ["object_class1", "object1", "parameter", "first"],
                    ["object_class1", "object1", "parameter", "second"],
                ],
            )
            self.assertTrue(errors)
            db_map.commit_session("test")
            values = {v.object_name: v.value for v in db_map.query(db_map.object_parameter_value_sq)}
            expected = {"object1": b'"first"'}
            self.assertEqual(values, expected)

    def test_import_object_parameter_value_with_alternative(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            self._assert_imports(import_alternatives(db_map, ["alternative"]))
            count = self._assert_imports(
                import_object_parameter_values(db_map, [["object_class1", "object1", "parameter", 1, "alternative"]])
            )
            self.assertEqual(count, 1)
            db_map.commit_session("test")
            values = {
                v.object_name: (v.value, v.alternative_name)
                for v in db_map.query(db_map.object_parameter_value_sq).all()
            }
            expected = {"object1": (b"1", "alternative")}
            self.assertEqual(values, expected)

    def test_import_object_parameter_value_fails_with_nonexistent_alternative(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            count, errors = import_object_parameter_values(
                db_map, [["object_class1", "object1", "parameter", 1, "nonexistent_alternative"]]
            )
            self.assertTrue(errors)
            self.assertEqual(count, 0)

    def test_import_parameter_values_from_committed_value_list(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_data(db_map, parameter_value_lists=(("values_1", 5.0),)))
            db_map.commit_session("test")
            count = self._assert_imports(
                import_data(
                    db_map,
                    object_classes=("object_class",),
                    object_parameters=(("object_class", "parameter", None, "values_1"),),
                    objects=(("object_class", "my_object"),),
                    object_parameter_values=(("object_class", "my_object", "parameter", 5.0),),
                )
            )
            self.assertEqual(count, 4)
            db_map.commit_session("test")
            values = db_map.query(db_map.object_parameter_value_sq).all()
            value = values[0]
            self.assertEqual(from_database(value.value, value.type), 5.0)

    def test_valid_object_parameter_value_from_value_list(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_parameter_value_lists(db_map, (("values_1", 5.0),)))
            self._assert_imports(import_object_classes(db_map, ("object_class",)))
            self._assert_imports(import_object_parameters(db_map, (("object_class", "parameter", None, "values_1"),)))
            self._assert_imports(import_objects(db_map, (("object_class", "my_object"),)))
            count = self._assert_imports(
                import_object_parameter_values(db_map, (("object_class", "my_object", "parameter", 5.0),))
            )
            self.assertEqual(count, 1)
            db_map.commit_session("test")
            values = db_map.query(db_map.object_parameter_value_sq).all()
            self.assertEqual(len(values), 1)
            value = values[0]
            self.assertEqual(from_database(value.value, value.type), 5.0)

    def test_non_existent_object_parameter_value_from_value_list_fails_gracefully(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_parameter_value_lists(db_map, (("values_1", 5.0),)))
            self._assert_imports(import_object_classes(db_map, ("object_class",)))
            self._assert_imports(import_object_parameters(db_map, (("object_class", "parameter", None, "values_1"),)))
            self._assert_imports(import_objects(db_map, (("object_class", "my_object"),)))
            count, errors = import_object_parameter_values(db_map, (("object_class", "my_object", "parameter", 2.3),))
            self.assertEqual(count, 0)
            self.assertEqual(len(errors), 1)

    def test_import_valid_relationship_parameter_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate_with_relationship(db_map)
            self._assert_imports(
                import_relationship_parameter_values(
                    db_map, [["relationship_class", ["object1", "object2"], "parameter", 1]]
                )
            )
            db_map.commit_session("test")
            values = {v.object_name_list: v.value for v in db_map.query(db_map.relationship_parameter_value_sq)}
            expected = {"object1,object2": b"1"}
            self.assertEqual(values, expected)

    def test_import_valid_relationship_parameter_value_with_duplicate_parameter_name(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate_with_relationship(db_map)
            self._assert_imports(
                import_relationship_classes(db_map, [["relationship_class2", ["object_class2", "object_class1"]]])
            )
            self._assert_imports(import_relationship_parameters(db_map, [["relationship_class2", "parameter"]]))
            self._assert_imports(
                import_relationship_parameter_values(
                    db_map, [["relationship_class", ["object1", "object2"], "parameter", 1]]
                )
            )
            db_map.commit_session("test")
            values = {v.object_name_list: v.value for v in db_map.query(db_map.relationship_parameter_value_sq)}
            expected = {"object1,object2": b"1"}
            self.assertEqual(values, expected)

    def test_import_valid_relationship_parameter_value_with_duplicate_object_name(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate_with_relationship(db_map)
            self._assert_imports(
                import_objects(db_map, [["object_class1", "duplicate_object"], ["object_class2", "duplicate_object"]])
            )
            self._assert_imports(
                import_relationships(db_map, [["relationship_class", ["duplicate_object", "duplicate_object"]]])
            )
            self._assert_imports(
                import_relationship_parameter_values(
                    db_map, [["relationship_class", ["duplicate_object", "duplicate_object"], "parameter", 1]]
                )
            )
            db_map.commit_session("test")
            values = {v.object_name_list: v.value for v in db_map.query(db_map.relationship_parameter_value_sq)}
            expected = {"duplicate_object,duplicate_object": b"1"}
            self.assertEqual(values, expected)

    def test_import_relationship_parameter_value_with_invalid_object(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate_with_relationship(db_map)
            _, errors = import_relationship_parameter_values(
                db_map, [["relationship_class", ["nonexistent_object", "object2"], "parameter", 1]]
            )
            self.assertTrue(errors)
            db_map.commit_session("test")
            self.assertFalse(db_map.query(db_map.relationship_parameter_value_sq).all())

    def test_import_relationship_parameter_value_with_invalid_relationship_class(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate_with_relationship(db_map)
            _, errors = import_relationship_parameter_values(
                db_map, [["nonexistent_class", ["object1", "object2"], "parameter", 1]]
            )
            self.assertTrue(errors)
            db_map.commit_session("test")
            self.assertFalse(db_map.query(db_map.relationship_parameter_value_sq).all())

    def test_import_relationship_parameter_value_with_invalid_parameter(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate_with_relationship(db_map)
            _, errors = import_relationship_parameter_values(
                db_map, [["relationship_class", ["object1", "object2"], "nonexistent_parameter", 1]]
            )
            self.assertTrue(errors)
            db_map.commit_session("test")
            self.assertFalse(db_map.query(db_map.relationship_parameter_value_sq).all())

    def test_import_existing_relationship_parameter_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate_with_relationship(db_map)
            self._assert_imports(
                import_relationship_parameter_values(
                    db_map, [["relationship_class", ["object1", "object2"], "parameter", "initial_value"]]
                )
            )
            self._assert_imports(
                import_relationship_parameter_values(
                    db_map, [["relationship_class", ["object1", "object2"], "parameter", "new_value"]]
                )
            )
            db_map.commit_session("test")
            values = {v.object_name_list: v.value for v in db_map.query(db_map.relationship_parameter_value_sq)}
            expected = {"object1,object2": b'"new_value"'}
            self.assertEqual(values, expected)

    def test_import_duplicate_relationship_parameter_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate_with_relationship(db_map)
            _, errors = import_relationship_parameter_values(
                db_map,
                [
                    ["relationship_class", ["object1", "object2"], "parameter", "first"],
                    ["relationship_class", ["object1", "object2"], "parameter", "second"],
                ],
            )
            self.assertTrue(errors)
            db_map.commit_session("test")
            values = {v.object_name_list: v.value for v in db_map.query(db_map.relationship_parameter_value_sq)}
            expected = {"object1,object2": b'"first"'}
            self.assertEqual(values, expected)

    def test_import_relationship_parameter_value_with_alternative(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate_with_relationship(db_map)
            self._assert_imports(import_alternatives(db_map, ["alternative"]))
            count = self._assert_imports(
                import_relationship_parameter_values(
                    db_map, [["relationship_class", ["object1", "object2"], "parameter", 1, "alternative"]]
                )
            )
            self.assertEqual(count, 1)
            db_map.commit_session("test")
            values = {
                v.object_name_list: (v.value, v.alternative_name)
                for v in db_map.query(db_map.relationship_parameter_value_sq).all()
            }
            expected = {"object1,object2": (b"1", "alternative")}
            self.assertEqual(values, expected)

    def test_import_relationship_parameter_value_fails_with_nonexistent_alternative(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            count, errors = import_relationship_parameter_values(
                db_map, [["relationship_class", ["object1", "object2"], "parameter", 1, "alternative"]]
            )
            self.assertTrue(errors)
            self.assertEqual(count, 0)

    def test_valid_relationship_parameter_value_from_value_list(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_parameter_value_lists(db_map, (("values_1", 5.0),)))
            self._assert_imports(import_object_classes(db_map, ("object_class",)))
            self._assert_imports(import_objects(db_map, (("object_class", "my_object"),)))
            self._assert_imports(import_relationship_classes(db_map, (("relationship_class", ("object_class",)),)))
            self._assert_imports(
                import_relationship_parameters(db_map, (("relationship_class", "parameter", None, "values_1"),))
            )
            self._assert_imports(import_relationships(db_map, (("relationship_class", ("my_object",)),)))
            count = self._assert_imports(
                import_relationship_parameter_values(
                    db_map, (("relationship_class", ("my_object",), "parameter", 5.0),)
                )
            )
            self.assertEqual(count, 1)
            db_map.commit_session("test")
            values = db_map.query(db_map.relationship_parameter_value_sq).all()
            self.assertEqual(len(values), 1)
            value = values[0]
            self.assertEqual(from_database(value.value, value.type), 5.0)

    def test_non_existent_relationship_parameter_value_from_value_list_fails_gracefully(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_parameter_value_lists(db_map, (("values_1", 5.0),)))
            self._assert_imports(import_object_classes(db_map, ("object_class",)))
            self._assert_imports(import_objects(db_map, (("object_class", "my_object"),)))
            self._assert_imports(import_relationship_classes(db_map, (("relationship_class", ("object_class",)),)))
            self._assert_imports(
                import_relationship_parameters(db_map, (("relationship_class", "parameter", None, "values_1"),))
            )
            self._assert_imports(import_relationships(db_map, (("relationship_class", ("my_object",)),)))
            count, errors = import_relationship_parameter_values(
                db_map, (("relationship_class", ("my_object",), "parameter", 2.3),)
            )
            self.assertEqual(len(errors), 1)
            self.assertEqual(count, 0)

    def test_unparse_value_imports_fields_correctly(self):
        with DatabaseMapping("sqlite:///", create=True) as db_map:
            data = {
                "entity_classes": [("A", (), None, None, False)],
                "entities": [("A", "aa", None)],
                "parameter_definitions": [("A", "test1", None, None, None)],
                "parameter_values": [
                    (
                        "A",
                        "aa",
                        "test1",
                        {
                            "type": "time_series",
                            "index": {
                                "start": "2000-01-01 00:00:00",
                                "resolution": "1h",
                                "ignore_year": False,
                                "repeat": False,
                            },
                            "data": [0.0, 1.0, 2.0, 4.0, 8.0, 0.0],
                        },
                        "Base",
                    )
                ],
                "alternatives": [("Base", "Base alternative")],
            }

            count = self._assert_imports(import_data(db_map, **data, unparse_value=dump_db_value))
            self.assertEqual(count, 4)
            db_map.commit_session("add test data")
            value = db_map.query(db_map.entity_parameter_value_sq).one()
            self.assertEqual(value.type, "time_series")
            self.assertEqual(value.parameter_name, "test1")
            self.assertEqual(value.alternative_name, "Base")
            self.assertEqual(value.entity_class_name, "A")
            self.assertEqual(value.entity_name, "aa")

            time_series = from_database(value.value, value.type)
            expected_result = TimeSeriesFixedResolution(
                "2000-01-01 00:00:00", "1h", [0.0, 1.0, 2.0, 4.0, 8.0, 0.0], False, False
            )
            self.assertEqual(time_series, expected_result)


class TestImportParameterValueList(AssertSuccessTestCase):
    def test_list_with_single_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            count = self._assert_imports(import_parameter_value_lists(db_map, (("list_1", 23.0),)))
            self.assertEqual(count, 2)
            db_map.commit_session("test")
            value_lists = db_map.query(db_map.parameter_value_list_sq).all()
            list_values = db_map.query(db_map.list_value_sq).all()
            self.assertEqual(len(value_lists), 1)
            self.assertEqual(len(list_values), 1)
            self.assertEqual(value_lists[0].name, "list_1")
            self.assertEqual(from_database(list_values[0].value, list_values[0].type), 23.0)
            self.assertEqual(list_values[0].index, 0)

    def test_import_twelfth_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            n_values = 11
            initial_list = tuple(("list_1", 1.1 * i) for i in range(1, n_values + 1))
            count = self._assert_imports(import_parameter_value_lists(db_map, initial_list))
            self.assertEqual(count, n_values + 1)
            count = self._assert_imports(import_parameter_value_lists(db_map, (("list_1", 23.0),)))
            self.assertEqual(count, 1)
            db_map.commit_session("test")
            value_lists = db_map.query(db_map.parameter_value_list_sq).all()
            self.assertEqual(len(value_lists), 1)
            self.assertEqual(value_lists[0].name, "list_1")
            list_values = db_map.query(db_map.list_value_sq).all()
            self.assertEqual(len(list_values), n_values + 1)
            expected = {i: 1.1 * (i + 1) for i in range(n_values)}
            expected[len(expected)] = 23.0
            for row in list_values:
                self.assertEqual(from_database(row.value, row.type), expected[row.index])


class TestImportListValue:
    def test_reimporting_existing_list_values_should_not_cause_updates(self, tmp_path):
        url = "sqlite:///" + str(tmp_path / "db.sqlite")
        with DatabaseMapping(url, create=True) as db_map:
            db_map.add_parameter_value_list(name="enumeration")
            db_map.add_list_value(parameter_value_list_name="enumeration", parsed_value="yes", index=1)
            db_map.add_list_value(parameter_value_list_name="enumeration", parsed_value="no", index=2)
            db_map.commit_session("Add a value list and list values")
        with DatabaseMapping(url) as db_map:
            assert_imports(import_parameter_value_lists(db_map, (("enumeration", "yes"), ("enumeration", "no"))))
            with pytest.raises(NothingToCommit):
                db_map.commit_session("Do nothing.")


class TestImportAlternative(AssertSuccessTestCase):
    def test_single_alternative(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            count = self._assert_imports(import_alternatives(db_map, ["alternative"]))
            self.assertEqual(count, 1)
            db_map.commit_session("test")
            alternatives = [a.name for a in db_map.query(db_map.alternative_sq)]
            self.assertEqual(len(alternatives), 2)
            self.assertIn("Base", alternatives)
            self.assertIn("alternative", alternatives)

    def test_alternative_description(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            count = self._assert_imports(import_alternatives(db_map, [["alternative", "description"]]))
            self.assertEqual(count, 1)
            db_map.commit_session("test")
            alternatives = {a.name: a.description for a in db_map.query(db_map.alternative_sq)}
            expected = {"Base": "Base alternative", "alternative": "description"}
            self.assertEqual(alternatives, expected)

    def test_update_alternative_description(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            count = self._assert_imports(import_alternatives(db_map, [["Base", "new description"]]))
            self.assertEqual(count, 1)
            db_map.commit_session("test")
            alternatives = {a.name: a.description for a in db_map.query(db_map.alternative_sq)}
            expected = {"Base": "new description"}
            self.assertEqual(alternatives, expected)


class TestImportScenario(AssertSuccessTestCase):
    def test_single_scenario(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            count = self._assert_imports(import_scenarios(db_map, ["scenario"]))
            self.assertEqual(count, 1)
            db_map.commit_session("test")
            scenarios = {s.name: s.description for s in db_map.query(db_map.scenario_sq)}
            self.assertEqual(scenarios, {"scenario": None})

    def test_import_single_scenario_as_tuple(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            count = self._assert_imports(import_scenarios(db_map, [("scenario",)]))
            self.assertEqual(count, 1)
            db_map.commit_session("test")
            scenarios = {s.name: s.description for s in db_map.query(db_map.scenario_sq)}
            self.assertEqual(scenarios, {"scenario": None})

    def test_scenario_with_description(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            count = self._assert_imports(import_scenarios(db_map, [["scenario", False, "description"]]))
            self.assertEqual(count, 1)
            db_map.commit_session("test")
            scenarios = {s.name: s.description for s in db_map.query(db_map.scenario_sq)}
            self.assertEqual(scenarios, {"scenario": "description"})

    def test_update_scenario_description(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_scenarios(db_map, [["scenario", False, "initial description"]]))
            count = self._assert_imports(import_scenarios(db_map, [["scenario", False, "new description"]]))
            self.assertEqual(count, 1)
            db_map.commit_session("test")
            scenarios = {s.name: s.description for s in db_map.query(db_map.scenario_sq)}
            self.assertEqual(scenarios, {"scenario": "new description"})
        db_map.close()


class TestImportScenarioAlternative(AssertSuccessTestCase):
    def test_single_scenario_alternative_import(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_data(db_map, scenarios=["scenario"], alternatives=["alternative"]))
            count = self._assert_imports(import_scenario_alternatives(db_map, [["scenario", "alternative"]]))
            self.assertEqual(count, 1)
            scenario_alternatives = self.scenario_alternatives(db_map)
            self.assertEqual(scenario_alternatives, {"scenario": {"alternative": 1}})

    def test_missing_scenario_gets_created(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            count, errors = import_data(db_map, scenario_alternatives=[["scen", "Base"]])
            self.assertEqual(errors, ["no scenario matching {'name': 'scen'}"])
            self.assertEqual(count, 0)

    def test_scenario_alternative_import_multiple_without_before_alternatives(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(
                import_data(db_map, scenarios=["scenario"], alternatives=["alternative1", "alternative2"])
            )
            count = self._assert_imports(
                import_scenario_alternatives(db_map, [["scenario", "alternative1"], ["scenario", "alternative2"]])
            )
            self.assertEqual(count, 2)
            scenario_alternatives = self.scenario_alternatives(db_map)
            self.assertEqual(scenario_alternatives, {"scenario": {"alternative1": 1, "alternative2": 2}})

    def test_scenario_alternative_import_multiple_with_before_alternatives(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(
                import_data(
                    db_map, scenarios=["scenario"], alternatives=["alternative1", "alternative2", "alternative3"]
                )
            )
            count = self._assert_imports(
                import_scenario_alternatives(
                    db_map,
                    [
                        ["scenario", "alternative1"],
                        ["scenario", "alternative3"],
                        ["scenario", "alternative2", "alternative3"],
                    ],
                )
            )
            self.assertEqual(count, 3)
            scenario_alternatives = self.scenario_alternatives(db_map)
            self.assertEqual(
                scenario_alternatives, {"scenario": {"alternative1": 1, "alternative2": 2, "alternative3": 3}}
            )

    def test_fails_with_nonexistent_before_alternative(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_data(db_map, scenarios=["scenario"], alternatives=["alternative"]))
            count, errors = import_scenario_alternatives(
                db_map, [["scenario", "alternative", "nonexistent_alternative"]]
            )
            self.assertEqual(
                errors,
                [
                    "can't insert alternative 'alternative' before 'nonexistent_alternative' "
                    "because the latter is not in scenario 'scenario'"
                ],
            )
            self.assertEqual(count, 0)
            scenario_alternatives = self.scenario_alternatives(db_map)
            self.assertEqual(scenario_alternatives, {})

    def test_importing_existing_scenario_alternative_does_not_alter_scenario_alternatives(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(
                import_data(db_map, scenarios=["scenario"], alternatives=["alternative1", "alternative2"])
            )
            count = self._assert_imports(
                import_scenario_alternatives(
                    db_map,
                    [["scenario", "alternative2", "alternative1"], ["scenario", "alternative1"]],
                )
            )
            self.assertEqual(count, 2)
            scenario_alternatives = self.scenario_alternatives(db_map)
            self.assertEqual(scenario_alternatives, {"scenario": {"alternative1": 2, "alternative2": 1}})
            count = self._assert_imports(
                import_scenario_alternatives(
                    db_map,
                    [["scenario", "alternative1"]],
                )
            )
            self.assertEqual(count, 0)

    def test_import_scenario_alternatives_in_arbitrary_order(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            count = self._assert_imports(import_scenarios(db_map, [("A (1)", False, "")]))
            self.assertEqual(count, 1)
            count = self._assert_imports(
                import_alternatives(db_map, [("Base", "Base alternative"), ("b", ""), ("c", ""), ("d", "")])
            )
            self.assertEqual(count, 3)
            count = self._assert_imports(
                import_scenario_alternatives(
                    db_map, [("A (1)", "c", "d"), ("A (1)", "d", None), ("A (1)", "Base", "b"), ("A (1)", "b", "c")]
                )
            )
            self.assertEqual(count, 4)
            scenario_alternatives = self.scenario_alternatives(db_map)
            self.assertEqual(scenario_alternatives, {"A (1)": {"Base": 1, "b": 2, "c": 3, "d": 4}})

    def test_insert_scenario_alternative_in_the_middle_of_other_alternatives(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(
                import_data(
                    db_map, scenarios=["scenario"], alternatives=["alternative1", "alternative2", "alternative3"]
                )
            )
            count = self._assert_imports(
                import_scenario_alternatives(
                    db_map,
                    [["scenario", "alternative2", "alternative1"], ["scenario", "alternative1"]],
                )
            )
            self.assertEqual(count, 2)
            scenario_alternatives = self.scenario_alternatives(db_map)
            self.assertEqual(scenario_alternatives, {"scenario": {"alternative1": 2, "alternative2": 1}})
            count = self._assert_imports(
                import_scenario_alternatives(db_map, [["scenario", "alternative3", "alternative1"]])
            )
            self.assertEqual(count, 2)
            scenario_alternatives = self.scenario_alternatives(db_map)
            self.assertEqual(
                scenario_alternatives, {"scenario": {"alternative1": 3, "alternative2": 1, "alternative3": 2}}
            )

    def test_import_inconsistent_scenario_alternatives(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(
                import_data(
                    db_map, scenarios=["scenario"], alternatives=["alternative1", "alternative2", "alternative3"]
                )
            )
            count = self._assert_imports(
                import_scenario_alternatives(
                    db_map,
                    [["scenario", "alternative3", "alternative1"], ["scenario", "alternative1"]],
                )
            )
            self.assertEqual(count, 2)
            scenario_alternatives = self.scenario_alternatives(db_map)
            self.assertEqual(scenario_alternatives, {"scenario": {"alternative1": 2, "alternative3": 1}})
            count = self._assert_imports(
                import_scenario_alternatives(
                    db_map,
                    [
                        ["scenario", "alternative3", "alternative2"],
                        ["scenario", "alternative2", "alternative1"],
                        ["scenario", "alternative1"],
                    ],
                )
            )
            self.assertEqual(count, 2)
            scenario_alternatives = self.scenario_alternatives(db_map)
            self.assertEqual(
                scenario_alternatives, {"scenario": {"alternative1": 3, "alternative2": 2, "alternative3": 1}}
            )

    @staticmethod
    def scenario_alternatives(db_map):
        db_map.commit_session("test")
        scenario_alternative_qry = (
            db_map.query(
                db_map.scenario_sq.c.name.label("scenario_name"),
                db_map.alternative_sq.c.name.label("alternative_name"),
                db_map.scenario_alternative_sq.c.rank,
            )
            .filter(db_map.scenario_alternative_sq.c.scenario_id == db_map.scenario_sq.c.id)
            .filter(db_map.scenario_alternative_sq.c.alternative_id == db_map.alternative_sq.c.id)
        )
        scenario_alternatives = {}
        for scenario_alternative in scenario_alternative_qry:
            alternative_rank = scenario_alternatives.setdefault(scenario_alternative.scenario_name, {})
            alternative_rank[scenario_alternative.alternative_name] = scenario_alternative.rank
        return scenario_alternatives


class TestImportMetadata(AssertSuccessTestCase):
    def test_import_metadata(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            count = self._assert_imports(
                import_metadata(db_map, ['{"name": "John", "age": 17}', '{"name": "Charly", "age": 90}'])
            )
            self.assertEqual(count, 4)
            db_map.commit_session("test")
            metadata = [(x.name, x.value) for x in db_map.query(db_map.metadata_sq)]
            self.assertEqual(len(metadata), 4)
            self.assertIn(("name", "John"), metadata)
            self.assertIn(("name", "Charly"), metadata)
            self.assertIn(("age", "17"), metadata)
            self.assertIn(("age", "90"), metadata)

    def test_import_metadata_with_duplicate_entry(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            count = self._assert_imports(
                import_metadata(db_map, ['{"name": "John", "age": 17}', '{"name": "Charly", "age": 17}'])
            )
            self.assertEqual(count, 3)
            db_map.commit_session("test")
            metadata = [(x.name, x.value) for x in db_map.query(db_map.metadata_sq)]
            self.assertEqual(len(metadata), 3)
            self.assertIn(("name", "John"), metadata)
            self.assertIn(("name", "Charly"), metadata)
            self.assertIn(("age", "17"), metadata)

    def test_import_metadata_with_nested_dict(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            count = self._assert_imports(
                import_metadata(db_map, ['{"name": "John", "info": {"age": 17, "city": "LA"}}'])
            )
            db_map.commit_session("test")
            metadata = [(x.name, x.value) for x in db_map.query(db_map.metadata_sq)]
            self.assertEqual(count, 2)
            self.assertEqual(len(metadata), 2)
            self.assertIn(("name", "John"), metadata)
            self.assertIn(("info", "{'age': 17, 'city': 'LA'}"), metadata)

    def test_import_metadata_with_nested_list(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            count = self._assert_imports(
                import_metadata(db_map, ['{"contributors": [{"name": "John"}, {"name": "Charly"}]}'])
            )
            db_map.commit_session("test")
            metadata = [(x.name, x.value) for x in db_map.query(db_map.metadata_sq)]
            self.assertEqual(count, 2)
            self.assertEqual(len(metadata), 2)
            self.assertIn(("contributors", "{'name': 'John'}"), metadata)
            self.assertIn(("contributors", "{'name': 'Charly'}"), metadata)

    def test_import_unformatted_metadata(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            count = self._assert_imports(import_metadata(db_map, ["not a JSON object"]))
            db_map.commit_session("test")
            metadata = [(x.name, x.value) for x in db_map.query(db_map.metadata_sq)]
            self.assertEqual(count, 1)
            self.assertEqual(len(metadata), 1)
            self.assertIn(("unnamed", "not a JSON object"), metadata)


class TestImportEntityMetadata(AssertSuccessTestCase):
    def populate(self, db_map):
        self._assert_imports(import_object_classes(db_map, ["object_class1", "object_class2"]))
        self._assert_imports(import_relationship_classes(db_map, [("rel_cls1", ("object_class1", "object_class2"))]))
        self._assert_imports(import_objects(db_map, [("object_class1", "object1"), ("object_class2", "object2")]))
        self._assert_imports(import_relationships(db_map, [("rel_cls1", ("object1", "object2"))]))
        self._assert_imports(import_object_parameters(db_map, [("object_class1", "param1")]))
        self._assert_imports(import_relationship_parameters(db_map, [("rel_cls1", "param2")]))
        self._assert_imports(import_object_parameter_values(db_map, [("object_class1", "object1", "param1", "value1")]))
        self._assert_imports(
            import_relationship_parameter_values(db_map, [("rel_cls1", ("object1", "object2"), "param2", "value2")])
        )
        self._assert_imports(
            import_metadata(db_map, ['{"co-author": "John", "age": 17}', '{"co-author": "Charly", "age": 90}'])
        )

    def test_import_object_metadata(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            count = self._assert_imports(
                import_object_metadata(
                    db_map,
                    [
                        ("object_class1", "object1", '{"co-author": "John", "age": 90}'),
                        ("object_class1", "object1", '{"co-author": "Charly", "age": 17}'),
                    ],
                )
            )
            self.assertEqual(count, 4)
            db_map.commit_session("test")
            metadata = [
                (x.entity_name, x.metadata_name, x.metadata_value) for x in db_map.query(db_map.ext_entity_metadata_sq)
            ]
            self.assertEqual(len(metadata), 4)
            self.assertIn(("object1", "co-author", "John"), metadata)
            self.assertIn(("object1", "age", "90"), metadata)
            self.assertIn(("object1", "co-author", "Charly"), metadata)
            self.assertIn(("object1", "age", "17"), metadata)

    def test_import_relationship_metadata(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.populate(db_map)
            count = self._assert_imports(
                import_relationship_metadata(
                    db_map,
                    [
                        ("rel_cls1", ("object1", "object2"), '{"co-author": "John", "age": 90}'),
                        ("rel_cls1", ("object1", "object2"), '{"co-author": "Charly", "age": 17}'),
                    ],
                )
            )
            self.assertEqual(count, 4)
            db_map.commit_session("test")
            metadata = [(x.metadata_name, x.metadata_value) for x in db_map.query(db_map.ext_entity_metadata_sq)]
            self.assertEqual(len(metadata), 4)
            self.assertIn(("co-author", "John"), metadata)
            self.assertIn(("age", "90"), metadata)
            self.assertIn(("co-author", "Charly"), metadata)
            self.assertIn(("age", "17"), metadata)


class TestImportParameterValueMetadata(AssertSuccessTestCase):
    def _import_metadata(self, db_map):
        self._assert_imports(import_metadata(db_map, ['{"co-author": "John", "age": 17}']))

    def test_import_object_parameter_value_metadata(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._import_metadata(db_map)
            self._assert_imports(import_object_classes(db_map, ["object_class"]))
            self._assert_imports(import_object_parameters(db_map, [("object_class", "param")]))
            self._assert_imports(import_objects(db_map, [("object_class", "object")]))
            self._assert_imports(import_object_parameter_values(db_map, [("object_class", "object", "param", "value")]))
            count = self._assert_imports(
                import_object_parameter_value_metadata(
                    db_map, [("object_class", "object", "param", '{"co-author": "John", "age": 17}')]
                )
            )
            self.assertEqual(count, 2)
            db_map.commit_session("test")
            metadata = db_map.query(db_map.ext_parameter_value_metadata_sq).all()
            self.assertEqual(len(metadata), 2)
            self.assertEqual(
                metadata[0]._asdict(),
                {
                    "alternative_name": "Base",
                    "entity_name": "object",
                    "id": 1,
                    "metadata_id": 1,
                    "metadata_name": "co-author",
                    "metadata_value": "John",
                    "parameter_name": "param",
                    "parameter_value_id": 1,
                    "commit_id": 2,
                },
            )
            self.assertEqual(
                metadata[1]._asdict(),
                {
                    "alternative_name": "Base",
                    "entity_name": "object",
                    "id": 2,
                    "metadata_id": 2,
                    "metadata_name": "age",
                    "metadata_value": "17",
                    "parameter_name": "param",
                    "parameter_value_id": 1,
                    "commit_id": 2,
                },
            )

    def test_import_relationship_parameter_value_metadata(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._import_metadata(db_map)
            self._assert_imports(import_object_classes(db_map, ["object_class"]))
            self._assert_imports(import_objects(db_map, [("object_class", "object")]))
            self._assert_imports(import_relationship_classes(db_map, (("relationship_class", ("object_class",)),)))
            self._assert_imports(import_relationships(db_map, (("relationship_class", ("object",)),)))
            self._assert_imports(import_relationship_parameters(db_map, (("relationship_class", "param"),)))
            self._assert_imports(
                import_relationship_parameter_values(db_map, (("relationship_class", ("object",), "param", "value"),))
            )
            count = self._assert_imports(
                import_relationship_parameter_value_metadata(
                    db_map, (("relationship_class", ("object",), "param", '{"co-author": "John", "age": 17}'),)
                )
            )
            self.assertEqual(count, 2)
            db_map.commit_session("test")
            metadata = db_map.query(db_map.ext_parameter_value_metadata_sq).all()
            self.assertEqual(len(metadata), 2)
            self.assertEqual(
                metadata[0]._asdict(),
                {
                    "alternative_name": "Base",
                    "entity_name": "object__",
                    "id": 1,
                    "metadata_id": 1,
                    "metadata_name": "co-author",
                    "metadata_value": "John",
                    "parameter_name": "param",
                    "parameter_value_id": 1,
                    "commit_id": 2,
                },
            )
            self.assertEqual(
                metadata[1]._asdict(),
                {
                    "alternative_name": "Base",
                    "entity_name": "object__",
                    "id": 2,
                    "metadata_id": 2,
                    "metadata_name": "age",
                    "metadata_value": "17",
                    "parameter_name": "param",
                    "parameter_value_id": 1,
                    "commit_id": 2,
                },
            )


class TestImportEntityClassDisplayMode(AssertSuccessTestCase):
    def test_import_single_display_mode(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            count = self._assert_imports(import_display_modes(db_map, (("disp_mode", "Some desc."),)))
            self.assertEqual(count, 1)
            db_map.commit_session("test")
            display_modes = {a.name: a.description for a in db_map.query(db_map.display_mode_sq)}
        self.assertEqual(len(display_modes), 1)
        self.assertIn("disp_mode", display_modes.keys())
        self.assertIn("Some desc.", display_modes.values())


class TestImportEntityClassDisplayModeEntityClass(AssertSuccessTestCase):
    def test_import_single_entity_class_display_mode(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            count = self._assert_imports(import_entity_classes(db_map, (("ent_cls", ()),)))
            self.assertEqual(count, 1)
            count = self._assert_imports(import_display_modes(db_map, (("disp_mode", "Some desc."),)))
            self.assertEqual(count, 1)
            count = self._assert_imports(import_entity_class_display_modes(db_map, (("disp_mode", "ent_cls", 98),)))
            self.assertEqual(count, 1)
            db_map.commit_session("test")
            display_modes = db_map.query(db_map.entity_class_display_mode_sq).all()
        self.assertEqual(len(display_modes), 1)
        self.assertEqual(
            display_modes[0]._asdict(),
            {
                "id": 1,
                "display_mode_id": 1,
                "entity_class_id": 1,
                "display_order": 98,
                "display_status": DisplayStatus.visible.name,
                "display_font_color": None,
                "display_background_color": None,
            },
        )


if __name__ == "__main__":
    unittest.main()
