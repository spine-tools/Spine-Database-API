######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Database API contributors
# This file is part of Spine Database API.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
"""Unit tests for export_functions."""
import pathlib
from tempfile import TemporaryDirectory
import unittest
from spinedb_api import (
    DatabaseMapping,
    export_alternatives,
    export_data,
    export_entity_classes,
    export_scenario_alternatives,
    export_scenarios,
    import_alternatives,
    import_data,
    import_display_modes,
    import_entity_class_display_modes,
    import_object_classes,
    import_object_parameter_values,
    import_object_parameters,
    import_objects,
    import_parameter_value_lists,
    import_relationship_classes,
    import_relationship_parameter_values,
    import_relationship_parameters,
    import_relationships,
    import_scenario_alternatives,
    import_scenarios,
)
from spinedb_api.export_functions import export_parameter_types, export_parameter_values
from spinedb_api.helpers import DisplayStatus
from tests.mock_helpers import AssertSuccessTestCase


class TestExportFunctions(AssertSuccessTestCase):

    def test_export_alternatives(self):
        with DatabaseMapping("sqlite://", username="UnitTest", create=True) as db_map:
            self._assert_imports(import_alternatives(db_map, [("alternative", "Description")]))
            exported = export_alternatives(db_map)
            self.assertEqual(exported, [("Base", "Base alternative"), ("alternative", "Description")])

    def test_export_scenarios(self):
        with DatabaseMapping("sqlite://", username="UnitTest", create=True) as db_map:
            self._assert_imports(import_scenarios(db_map, [("scenario", False, "Description")]))
            exported = export_scenarios(db_map)
            self.assertEqual(exported, [("scenario", False, "Description")])

    def test_export_scenario_alternatives(self):
        with DatabaseMapping("sqlite://", username="UnitTest", create=True) as db_map:
            self._assert_imports(import_alternatives(db_map, ["alternative"]))
            self._assert_imports(import_scenarios(db_map, ["scenario"]))
            self._assert_imports(import_scenario_alternatives(db_map, (("scenario", "alternative"),)))
            exported = export_scenario_alternatives(db_map)
            self.assertEqual(exported, [("scenario", "alternative", None)])

    def test_export_scenario_alternatives_from_existing_database(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + str(pathlib.Path(temp_dir, "db.sqlite"))
            with DatabaseMapping(url, create=True) as db_map:
                db_map.add_scenario(name="My Scenario")
                db_map.add_scenario_alternative(scenario_name="My Scenario", alternative_name="Base", rank=0)
                db_map.commit_session("Add test data.")
            with DatabaseMapping(url) as db_map:
                self.assertEqual(export_scenario_alternatives(db_map), [("My Scenario", "Base", None)])

    def test_export_multiple_scenario_alternatives(self):
        with DatabaseMapping("sqlite://", username="UnitTest", create=True) as db_map:
            self._assert_imports(import_alternatives(db_map, ["alternative1"]))
            self._assert_imports(import_alternatives(db_map, ["alternative2"]))
            self._assert_imports(import_scenarios(db_map, ["scenario"]))
            self._assert_imports(import_scenario_alternatives(db_map, (("scenario", "alternative1"),)))
            self._assert_imports(import_scenario_alternatives(db_map, (("scenario", "alternative2", "alternative1"),)))
            exported = export_scenario_alternatives(db_map)
            self.assertEqual(
                set(exported), {("scenario", "alternative2", "alternative1"), ("scenario", "alternative1", None)}
            )

    def test_export_entity_classes(self):
        with DatabaseMapping("sqlite://", username="UnitTest", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_entity_class_item(name="Relation", dimension_name_list=("Object",)))
            exported = export_entity_classes(db_map)
            expected = (("Object", (), None, None, True), ("Relation", ("Object",), None, None, True))
            self.assertCountEqual(exported, expected)

    def test_export_multidimensional_entities(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Unit")
            db_map.add_entity_class(name="Node")
            db_map.add_entity_class(dimension_name_list=("Unit", "Node"))
            db_map.add_entity_class(dimension_name_list=("Node", "Unit"))
            db_map.add_entity_class(dimension_name_list=("Unit__Node", "Node__Unit"))
            db_map.add_entity(name="u", entity_class_name="Unit")
            db_map.add_entity(name="n", entity_class_name="Node")
            db_map.add_entity(element_name_list=("u", "n"), entity_class_name="Unit__Node")
            db_map.add_entity(element_name_list=("n", "u"), entity_class_name="Node__Unit")
            db_map.add_entity(element_name_list=("u__n", "n__u"), entity_class_name="Unit__Node__Node__Unit")
            data = export_data(db_map)
            self.assertEqual(len(data), 3)
            self.assertCountEqual(data["alternatives"], [("Base", "Base alternative")])
            self.assertCountEqual(
                data["entity_classes"],
                [
                    ("Unit", (), None, None, True),
                    ("Node", (), None, None, True),
                    ("Unit__Node", ("Unit", "Node"), None, None, True),
                    ("Node__Unit", ("Node", "Unit"), None, None, True),
                    ("Unit__Node__Node__Unit", ("Unit__Node", "Node__Unit"), None, None, True),
                ],
            )
            self.assertCountEqual(
                data["entities"],
                [
                    ("Unit", "u", None),
                    ("Node", "n", None),
                    ("Unit__Node", ("u", "n"), None),
                    ("Node__Unit", ("n", "u"), None),
                    ("Unit__Node__Node__Unit", ("u", "n", "n", "u"), None),
                ],
            )

    def test_export_entities_with_location(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Object")
            db_map.add_entity(
                entity_class_name="Object",
                name="shape",
                lat=2.3,
                lon=3.2,
                alt=55.0,
                shape_name="pentadron",
                shape_blob="{}",
            )
            data = export_data(db_map)
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_data(db_map, **data))
            shape = db_map.entity(entity_class_name="Object", name="shape")
            self.assertEqual(shape["lat"], 2.3)
            self.assertEqual(shape["lon"], 3.2)
            self.assertEqual(shape["alt"], 55.0)
            self.assertEqual(shape["shape_name"], "pentadron")
            self.assertEqual(shape["shape_blob"], "{}")

    def test_export_single_parameter_type(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Widget"))
            self._assert_success(db_map.add_parameter_definition_item(name="q", entity_class_name="Widget"))
            self._assert_success(
                db_map.add_parameter_type_item(
                    entity_class_name="Widget", parameter_definition_name="q", rank=0, type="duration"
                )
            )
            exported = export_parameter_types(db_map)
            expected = [
                ("Widget", "q", "duration", 0),
            ]
            self.assertEqual(exported, expected)
            exported_data = export_data(db_map)
            expected_data = {
                "alternatives": [("Base", "Base alternative")],
                "entity_classes": [("Widget", (), None, None, True)],
                "parameter_definitions": [("Widget", "q", None, None, None)],
                "parameter_types": [("Widget", "q", "duration", 0)],
            }
            self.assertEqual(exported_data, expected_data)

    def test_export_entity_groups(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Object")
            db_map.add_entity(entity_class_name="Object", name="my_group")
            db_map.add_entity(entity_class_name="Object", name="vip_member")
            db_map.add_entity_group(entity_class_name="Object", group_name="my_group", member_name="vip_member")
            data = export_data(db_map)
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_data(db_map, **data))
            self.assertTrue(
                db_map.entity_group(entity_class_name="Object", group_name="my_group", member_name="vip_member")
            )

    def test_export_entity_alternatives(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_alternative(name="alt")
            db_map.add_entity_class(name="Object")
            db_map.add_entity(entity_class_name="Object", name="ghost")
            db_map.add_entity_alternative(
                entity_class_name="Object", entity_byname=("ghost",), alternative_name="alt", active=True
            )
            data = export_data(db_map)
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_data(db_map, **data))
            self.assertTrue(
                db_map.entity_alternative(entity_class_name="Object", entity_byname=("ghost",), alternative_name="alt")[
                    "active"
                ]
            )

    def test_export_superclass_subclasses(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Super")
            db_map.add_entity_class(name="Sub")
            db_map.add_superclass_subclass(superclass_name="Super", subclass_name="Sub")
            data = export_data(db_map)
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_data(db_map, **data))
            self.assertTrue(db_map.superclass_subclass(superclass_name="Super", subclass_name="Sub"))

    def test_export_parameter_value_lists(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_parameter_value_list(name="possibilities")
            db_map.add_list_value(parameter_value_list_name="possibilities", parsed_value="infinite", index=0)
            data = export_data(db_map)
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_data(db_map, **data))
            self.assertEqual(
                db_map.list_value(parameter_value_list_name="possibilities", index=0)["parsed_value"], "infinite"
            )

    def test_export_parameter_values_sorts_zero_and_multidimensional_entities(self):
        with DatabaseMapping("sqlite:///", create=True) as db_map:
            db_map.add_entity_class(name="First")
            db_map.add_parameter_definition(entity_class_name="First", name="mass")
            db_map.add_entity(entity_class_name="First", name="dim1")
            db_map.add_parameter_value(
                entity_class_name="First",
                entity_byname=("dim1",),
                parameter_definition_name="mass",
                alternative_name="Base",
                parsed_value=23.0,
            )
            db_map.add_entity_class(name="Second")
            db_map.add_entity(entity_class_name="Second", name="dim2")
            db_map.add_entity_class(dimension_name_list=["First", "Second"])
            db_map.add_parameter_definition(entity_class_name="First__Second", name="x")
            db_map.add_entity(entity_class_name="First__Second", element_name_list=["dim1", "dim2"])
            db_map.add_parameter_value(
                entity_class_name="First__Second",
                entity_byname=("dim1", "dim2"),
                parameter_definition_name="x",
                alternative_name="Base",
                parsed_value=2.3,
            )
            data = export_parameter_values(db_map)
            self.assertEqual(
                data, [("First", "dim1", "mass", 23.0, "Base"), ("First__Second", ("dim1", "dim2"), "x", 2.3, "Base")]
            )

    def test_export_data(self):
        with DatabaseMapping("sqlite://", username="UnitTest", create=True) as db_map:
            self._assert_imports(import_object_classes(db_map, ["object_class"]))
            self._assert_imports(import_object_parameters(db_map, [("object_class", "object_parameter")]))
            self._assert_imports(import_objects(db_map, [("object_class", "object")]))
            self._assert_imports(
                import_object_parameter_values(db_map, [("object_class", "object", "object_parameter", 2.3)])
            )
            self._assert_imports(import_relationship_classes(db_map, [("relationship_class", ["object_class"])]))
            self._assert_imports(
                import_relationship_classes(db_map, [("compound_class", ["relationship_class", "relationship_class"])])
            )
            self._assert_imports(
                import_relationship_parameters(db_map, [("relationship_class", "relationship_parameter")])
            )
            self._assert_imports(import_relationship_parameters(db_map, [("compound_class", "compound_parameter")]))
            self._assert_imports(import_relationships(db_map, [("relationship_class", ["object"])]))
            self._assert_imports(import_relationships(db_map, [("compound_class", ["object", "object"])]))
            self._assert_imports(
                import_relationship_parameter_values(
                    db_map, [("relationship_class", ["object"], "relationship_parameter", 3.14)]
                )
            )
            self._assert_imports(
                import_relationship_parameter_values(
                    db_map, [("compound_class", ["object", "object"], "compound_parameter", 2.71)]
                )
            )
            self._assert_imports(import_parameter_value_lists(db_map, [("value_list", "5.5"), ("value_list", "6.4")]))
            self._assert_imports(import_alternatives(db_map, ["alternative"]))
            self._assert_imports(import_scenarios(db_map, ["scenario"]))
            self._assert_imports(import_scenario_alternatives(db_map, [("scenario", "alternative")]))
            self._assert_imports(import_display_modes(db_map, ["display_mode"]))
            self._assert_imports(
                import_entity_class_display_modes(
                    db_map, (("display_mode", "object_class", 1, DisplayStatus.hidden.name),)
                )
            )
            exported = export_data(db_map)
            self.assertEqual(len(exported), 10)
            self.assertIn("entity_classes", exported)
            self.assertEqual(
                exported["entity_classes"],
                [
                    ("object_class", (), None, None, True),
                    ("relationship_class", ("object_class",), None, None, True),
                    ("compound_class", ("relationship_class", "relationship_class"), None, None, True),
                ],
            )
            self.assertIn("parameter_definitions", exported)
            self.assertEqual(
                exported["parameter_definitions"],
                [
                    ("compound_class", "compound_parameter", None, None, None),
                    ("object_class", "object_parameter", None, None, None),
                    ("relationship_class", "relationship_parameter", None, None, None),
                ],
            )
            self.assertIn("entities", exported)
            self.assertEqual(
                exported["entities"],
                [
                    ("object_class", "object", None),
                    ("relationship_class", ("object",), None),
                    ("compound_class", ("object", "object"), None),
                ],
            )
            self.assertIn("parameter_values", exported)
            self.assertEqual(
                exported["parameter_values"],
                [
                    ("compound_class", ("object", "object"), "compound_parameter", 2.71, "Base"),
                    ("object_class", "object", "object_parameter", 2.3, "Base"),
                    ("relationship_class", ("object",), "relationship_parameter", 3.14, "Base"),
                ],
            )
            self.assertIn("parameter_value_lists", exported)
            self.assertEqual(exported["parameter_value_lists"], [("value_list", "5.5"), ("value_list", "6.4")])
            self.assertIn("alternatives", exported)
            self.assertEqual(exported["alternatives"], [("Base", "Base alternative"), ("alternative", None)])
            self.assertIn("scenarios", exported)
            self.assertEqual(exported["scenarios"], [("scenario", False, None)])
            self.assertIn("scenario_alternatives", exported)
            self.assertEqual(exported["scenario_alternatives"], [("scenario", "alternative", None)])
            self.assertIn("display_modes", exported)
            self.assertEqual(exported["display_modes"], [("display_mode", None)])
            self.assertIn("entity_class_display_modes", exported)
            self.assertEqual(
                exported["entity_class_display_modes"],
                [("display_mode", "object_class", 1, DisplayStatus.hidden.name, None, None)],
            )


if __name__ == "__main__":
    unittest.main()
