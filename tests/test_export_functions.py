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
""" Unit tests for export_functions. """

import unittest
from spinedb_api import (
    DatabaseMapping,
    export_alternatives,
    export_data,
    export_entity_classes,
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
    def _assert_import_success(self, result):
        errors = result[1]
        self.assertEqual(errors, [])

    def _assert_addition_success(self, result):
        error = result[1]
        self.assertIsNone(error)

    def test_export_alternatives(self):
        with DatabaseMapping("sqlite://", username="UnitTest", create=True) as db_map:
            self._assert_import_success(import_alternatives(db_map, [("alternative", "Description")]))
            exported = export_alternatives(db_map)
            self.assertEqual(exported, [("Base", "Base alternative"), ("alternative", "Description")])

    def test_export_scenarios(self):
        with DatabaseMapping("sqlite://", username="UnitTest", create=True) as db_map:
            self._assert_import_success(import_scenarios(db_map, [("scenario", False, "Description")]))
            exported = export_scenarios(db_map)
            self.assertEqual(exported, [("scenario", False, "Description")])

    def test_export_scenario_alternatives(self):
        with DatabaseMapping("sqlite://", username="UnitTest", create=True) as db_map:
            self._assert_import_success(import_alternatives(db_map, ["alternative"]))
            self._assert_import_success(import_scenarios(db_map, ["scenario"]))
            self._assert_import_success(import_scenario_alternatives(db_map, (("scenario", "alternative"),)))
            exported = export_scenario_alternatives(db_map)
            self.assertEqual(exported, [("scenario", "alternative", None)])

    def test_export_multiple_scenario_alternatives(self):
        with DatabaseMapping("sqlite://", username="UnitTest", create=True) as db_map:
            self._assert_import_success(import_alternatives(db_map, ["alternative1"]))
            self._assert_import_success(import_alternatives(db_map, ["alternative2"]))
            self._assert_import_success(import_scenarios(db_map, ["scenario"]))
            self._assert_import_success(import_scenario_alternatives(db_map, (("scenario", "alternative1"),)))
            self._assert_import_success(
                import_scenario_alternatives(db_map, (("scenario", "alternative2", "alternative1"),))
            )
            exported = export_scenario_alternatives(db_map)
            self.assertEqual(
                set(exported), {("scenario", "alternative2", "alternative1"), ("scenario", "alternative1", None)}
            )

    def test_export_entity_classes(self):
        with DatabaseMapping("sqlite://", username="UnitTest", create=True) as db_map:
            self._assert_addition_success(db_map.add_entity_class_item(name="Object"))
            self._assert_addition_success(
                db_map.add_entity_class_item(name="Relation", dimension_name_list=("Object",))
            )
            exported = export_entity_classes(db_map)
            expected = (("Object", (), None, None, False), ("Relation", ("Object",), None, None, True))
            self.assertCountEqual(exported, expected)

    def test_export_data(self):
        with DatabaseMapping("sqlite://", username="UnitTest", create=True) as db_map:
            self._assert_import_success(import_object_classes(db_map, ["object_class"]))
            self._assert_import_success(import_object_parameters(db_map, [("object_class", "object_parameter")]))
            self._assert_import_success(import_objects(db_map, [("object_class", "object")]))
            self._assert_import_success(
                import_object_parameter_values(db_map, [("object_class", "object", "object_parameter", 2.3)])
            )
            self._assert_import_success(import_relationship_classes(db_map, [("relationship_class", ["object_class"])]))
            self._assert_import_success(
                import_relationship_parameters(db_map, [("relationship_class", "relationship_parameter")])
            )
            self._assert_import_success(import_relationships(db_map, [("relationship_class", ["object"])]))
            self._assert_import_success(
                import_relationship_parameter_values(
                    db_map, [("relationship_class", ["object"], "relationship_parameter", 3.14)]
                )
            )
            self._assert_import_success(
                import_parameter_value_lists(db_map, [("value_list", "5.5"), ("value_list", "6.4")])
            )
            self._assert_import_success(import_alternatives(db_map, ["alternative"]))
            self._assert_import_success(import_scenarios(db_map, ["scenario"]))
            self._assert_import_success(import_scenario_alternatives(db_map, [("scenario", "alternative")]))
            exported = export_data(db_map)
            self.assertEqual(len(exported), 8)
            self.assertIn("entity_classes", exported)
            self.assertEqual(
                exported["entity_classes"],
                [("object_class", (), None, None, False), ("relationship_class", ("object_class",), None, None, True)],
            )
            self.assertIn("parameter_definitions", exported)
            self.assertEqual(
                exported["parameter_definitions"],
                [
                    ("object_class", "object_parameter", None, None, None),
                    ("relationship_class", "relationship_parameter", None, None, None),
                ],
            )
            self.assertIn("entities", exported)
            self.assertEqual(
                exported["entities"], [("object_class", "object", None), ("relationship_class", ("object",), None)]
            )
            self.assertIn("parameter_values", exported)
            self.assertEqual(
                exported["parameter_values"],
                [
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


if __name__ == "__main__":
    unittest.main()
