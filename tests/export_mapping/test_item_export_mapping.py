######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
"""
Unit tests for export mappings.

:author: A. Soininen (VTT)
:date:   10.12.2020
"""

import pickle
import unittest
from spinedb_api import (
    DatabaseMapping,
    DiffDatabaseMapping,
    import_alternatives,
    import_features,
    import_object_classes,
    import_object_parameter_values,
    import_object_parameters,
    import_objects,
    import_parameter_value_lists,
    import_relationship_classes,
    import_relationships,
    import_scenario_alternatives,
    import_scenarios,
    import_tool_features,
    import_tool_feature_methods,
    import_tools,
    Map,
)
from spinedb_api.import_functions import import_object_groups
from spinedb_api.export_mapping import Position, rows, titles, object_parameter_export
from spinedb_api.export_mapping.item_export_mapping import (
    AlternativeMapping,
    FixedValueMapping,
    ExpandedParameterValueMapping,
    ExpandedParameterDefaultValueMapping,
    FeatureEntityClassMapping,
    FeatureParameterDefinitionMapping,
    from_dict,
    ObjectGroupMapping,
    ObjectMapping,
    ObjectClassMapping,
    ParameterDefaultValueMapping,
    ParameterDefaultValueIndexMapping,
    ParameterDefinitionMapping,
    ParameterIndexMapping,
    ParameterValueListMapping,
    ParameterValueListValueMapping,
    ParameterValueMapping,
    RelationshipClassMapping,
    RelationshipClassObjectClassMapping,
    RelationshipMapping,
    RelationshipObjectMapping,
    ScenarioActiveFlagMapping,
    ScenarioAlternativeMapping,
    ScenarioMapping,
    ToolMapping,
    ToolFeatureEntityClassMapping,
    ToolFeatureParameterDefinitionMapping,
    ToolFeatureRequiredFlagMapping,
    ToolFeatureMethodEntityClassMapping,
    ToolFeatureMethodMethodMapping,
    ToolFeatureMethodParameterDefinitionMapping,
    to_dict,
    unflatten,
)


class TestExportMapping(unittest.TestCase):
    def test_export_empty_table(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        object_class_mapping = ObjectClassMapping(0)
        self.assertEqual(list(rows(object_class_mapping, db_map)), [])
        db_map.connection.close()

    def test_export_single_object_class(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("object_class",))
        db_map.commit_session("Add test data.")
        object_class_mapping = ObjectClassMapping(0)
        self.assertEqual(list(rows(object_class_mapping, db_map)), [["object_class"]])
        db_map.connection.close()

    def test_export_object_classes_as_single_row(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        db_map.commit_session("Add test data.")
        object_class_mapping = ObjectClassMapping(Position.single_row)
        self.assertEqual(list(rows(object_class_mapping, db_map)), [["oc1", "oc2", "oc3"]])
        db_map.connection.close()

    def test_single_row_does_not_export_empty_data(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        import_objects(db_map, (("oc2", "o1"), ("oc2", "o2")))
        db_map.commit_session("Add test data.")
        mapping = unflatten([ObjectClassMapping(0), ObjectMapping(Position.single_row)])
        self.assertEqual(list(rows(mapping, db_map)), [["oc2", "o1", "o2"]])
        db_map.connection.close()

    def test_export_objects(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        import_objects(
            db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21"), ("oc3", "o31"), ("oc3", "o32"), ("oc3", "o33"))
        )
        db_map.commit_session("Add test data.")
        object_class_mapping = ObjectClassMapping(0)
        object_class_mapping.child = ObjectMapping(1)
        self.assertEqual(
            list(rows(object_class_mapping, db_map)),
            [["oc1", "o11"], ["oc1", "o12"], ["oc2", "o21"], ["oc3", "o31"], ["oc3", "o32"], ["oc3", "o33"]],
        )
        db_map.connection.close()

    def test_None_column_on_leaf_item_is_not_exported(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1",))
        import_objects(db_map, (("oc1", "o11"), ("oc1", "o12")))
        db_map.commit_session("Add test data.")
        object_class_mapping = ObjectClassMapping(0)
        object_class_mapping.child = ObjectMapping(Position.hidden)
        self.assertEqual(list(rows(object_class_mapping, db_map)), [["oc1"]])
        db_map.connection.close()

    def test_hidden_leaf_item_in_regular_table_valid(self):
        object_class_mapping = ObjectClassMapping(0)
        object_class_mapping.child = ObjectMapping(Position.hidden)
        self.assertEqual(object_class_mapping.check_validity(), [])

    def test_hidden_leaf_item_in_pivot_table_not_valid(self):
        object_class_mapping = ObjectClassMapping(-1)
        object_class_mapping.child = ObjectMapping(Position.hidden)
        self.assertEqual(object_class_mapping.check_validity(), ["Cannot be pivoted."])

    def test_single_row_with_hidden_column(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1",))
        import_object_parameters(db_map, (("oc1", "p11"), ("oc1", "p12")))
        import_objects(db_map, (("oc1", "o11"), ("oc1", "o12")))
        db_map.commit_session("Add test data.")
        object_class_mapping = ObjectClassMapping(Position.hidden)
        parameter_definition_mapping = ParameterDefinitionMapping(0)
        parameter_definition_mapping.child = ObjectMapping(Position.single_row)
        object_class_mapping.child = parameter_definition_mapping
        expected = [["p11", "o11", "o12"], ["p12", "o11", "o12"]]
        self.assertEqual(list(rows(object_class_mapping, db_map)), expected)
        db_map.connection.close()

    def test_export_objects_on_single_row(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        import_objects(
            db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21"), ("oc3", "o31"), ("oc3", "o32"), ("oc3", "o33"))
        )
        db_map.commit_session("Add test data.")
        object_class_mapping = ObjectClassMapping(0)
        object_class_mapping.child = ObjectMapping(Position.single_row)
        self.assertEqual(
            list(rows(object_class_mapping, db_map)),
            [["oc1", "o11", "o12"], ["oc2", "o21"], ["oc3", "o31", "o32", "o33"]],
        )
        db_map.connection.close()

    def test_object_groups(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_objects(db_map, (("oc", "o1"), ("oc", "o2"), ("oc", "o3"), ("oc", "g1"), ("oc", "g2")))
        import_object_groups(db_map, (("oc", "o1", "g1"), ("oc", "o2", "g1"), ("oc", "o3", "g2")))
        db_map.commit_session("Add test data.")
        flattened = [ObjectClassMapping(0), ObjectMapping(2), ObjectGroupMapping(1)]
        mapping = unflatten(flattened)
        self.assertEqual(list(rows(mapping, db_map)), [["oc", "o1", "g1"], ["oc", "o2", "g1"], ["oc", "o3", "g2"]])
        db_map.connection.close()

    def test_export_parameter_definitions(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2"))
        import_object_parameters(db_map, (("oc1", "p11"), ("oc1", "p12"), ("oc2", "p21")))
        import_objects(db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21")))
        db_map.commit_session("Add test data.")
        object_class_mapping = ObjectClassMapping(0)
        parameter_definition_mapping = ParameterDefinitionMapping(1)
        parameter_definition_mapping.child = ObjectMapping(2)
        object_class_mapping.child = parameter_definition_mapping
        expected = [
            ["oc1", "p11", "o11"],
            ["oc1", "p11", "o12"],
            ["oc1", "p12", "o11"],
            ["oc1", "p12", "o12"],
            ["oc2", "p21", "o21"],
        ]
        self.assertEqual(list(rows(object_class_mapping, db_map)), expected)
        db_map.connection.close()

    def test_export_single_parameter_value_when_there_are_multiple_objects(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2"))
        import_object_parameters(db_map, (("oc1", "p11"), ("oc1", "p12"), ("oc2", "p21")))
        import_objects(db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21")))
        import_object_parameter_values(db_map, (("oc1", "o11", "p12", -11.0),))
        db_map.commit_session("Add test data.")
        object_class_mapping = ObjectClassMapping(0)
        parameter_definition_mapping = ParameterDefinitionMapping(1)
        alternative_mapping = AlternativeMapping(Position.hidden)
        parameter_definition_mapping.child = alternative_mapping
        object_mapping = ObjectMapping(2)
        alternative_mapping.child = object_mapping
        value_mapping = ParameterValueMapping(3)
        object_mapping.child = value_mapping
        object_class_mapping.child = parameter_definition_mapping
        self.assertEqual(list(rows(object_class_mapping, db_map)), [["oc1", "p12", "o11", -11.0]])
        db_map.connection.close()

    def test_export_single_parameter_value_pivoted_by_object_name(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1",))
        import_object_parameters(db_map, (("oc1", "p11"), ("oc1", "p12")))
        import_objects(db_map, (("oc1", "o11"), ("oc1", "o12")))
        import_object_parameter_values(
            db_map,
            (
                ("oc1", "o11", "p11", -11.0),
                ("oc1", "o11", "p12", -12.0),
                ("oc1", "o12", "p11", -21.0),
                ("oc1", "o12", "p12", -22.0),
            ),
        )
        db_map.commit_session("Add test data.")
        object_class_mapping = ObjectClassMapping(0)
        parameter_definition_mapping = ParameterDefinitionMapping(1)
        alternative_mapping = AlternativeMapping(Position.hidden)
        parameter_definition_mapping.child = alternative_mapping
        object_mapping = ObjectMapping(-1)
        alternative_mapping.child = object_mapping
        value_mapping = ParameterValueMapping(-2)
        object_mapping.child = value_mapping
        object_class_mapping.child = parameter_definition_mapping
        expected = [[None, None, "o11", "o12"], ["oc1", "p11", -11.0, -21.0], ["oc1", "p12", -12.0, -22.0]]
        self.assertEqual(list(rows(object_class_mapping, db_map)), expected)
        db_map.connection.close()

    def test_minimum_pivot_index_need_not_be_minus_one(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_alternatives(db_map, ("alt",))
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p"),))
        import_objects(db_map, (("oc", "o"),))
        import_object_parameter_values(
            db_map,
            (
                ("oc", "o", "p", Map(["A", "B"], [-1.1, -2.2]), "Base"),
                ("oc", "o", "p", Map(["A", "B"], [-5.5, -6.6]), "alt"),
            ),
        )
        db_map.commit_session("Add test data.")
        mapping = object_parameter_export(1, 2, Position.hidden, 0, -2, 4, [3])
        expected = [
            [None, None, None, None, "Base", "alt"],
            ["o", "oc", "p", "A", -1.1, -5.5],
            ["o", "oc", "p", "B", -2.2, -6.6],
        ]
        self.assertEqual(list(rows(mapping, db_map)), expected)
        db_map.connection.close()

    def test_pivot_row_order(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1",))
        import_object_parameters(db_map, (("oc1", "p11"), ("oc1", "p12")))
        import_objects(db_map, (("oc1", "o11"), ("oc1", "o12")))
        import_object_parameter_values(
            db_map,
            (
                ("oc1", "o11", "p11", -11.0),
                ("oc1", "o11", "p12", -12.0),
                ("oc1", "o12", "p11", -21.0),
                ("oc1", "o12", "p12", -22.0),
            ),
        )
        db_map.commit_session("Add test data.")
        object_class_mapping = ObjectClassMapping(0)
        parameter_definition_mapping = ParameterDefinitionMapping(-1)
        alternative_mapping = AlternativeMapping(Position.hidden)
        object_mapping = ObjectMapping(-2)
        value_mapping = ParameterValueMapping(3)
        mappings = [
            object_class_mapping,
            parameter_definition_mapping,
            alternative_mapping,
            object_mapping,
            value_mapping,
        ]
        root = unflatten(mappings)
        expected = [
            [None, "p11", "p11", "p12", "p12"],
            [None, "o11", "o12", "o11", "o12"],
            ["oc1", -11.0, -21.0, -12.0, -22.0],
        ]
        self.assertEqual(list(rows(root, db_map)), expected)
        parameter_definition_mapping.position = -2
        object_mapping.position = -1
        expected = [
            [None, "o11", "o11", "o12", "o12"],
            [None, "p11", "p12", "p11", "p12"],
            ["oc1", -11.0, -12.0, -21.0, -22.0],
        ]
        self.assertEqual(list(rows(root, db_map)), expected)
        db_map.connection.close()

    def test_export_parameter_indexes(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p1"), ("oc", "p2")))
        import_objects(db_map, (("oc", "o1"), ("oc", "o2")))
        import_object_parameter_values(
            db_map,
            (
                ("oc", "o1", "p1", Map(["a", "b"], [5.0, 5.0])),
                ("oc", "o1", "p2", Map(["c", "d"], [5.0, 5.0])),
                ("oc", "o2", "p1", Map(["e", "f"], [5.0, 5.0])),
                ("oc", "o2", "p2", Map(["g", "h"], [5.0, 5.0])),
            ),
        )
        db_map.commit_session("Add test data.")
        object_class_mapping = ObjectClassMapping(0)
        parameter_definition_mapping = ParameterDefinitionMapping(2)
        alternative_mapping = AlternativeMapping(Position.hidden)
        parameter_definition_mapping.child = alternative_mapping
        object_mapping = ObjectMapping(1)
        alternative_mapping.child = object_mapping
        index_mapping = ParameterIndexMapping(3)
        object_mapping.child = index_mapping
        object_class_mapping.child = parameter_definition_mapping
        expected = [
            ["oc", "o1", "p1", "a"],
            ["oc", "o1", "p1", "b"],
            ["oc", "o2", "p1", "e"],
            ["oc", "o2", "p1", "f"],
            ["oc", "o1", "p2", "c"],
            ["oc", "o1", "p2", "d"],
            ["oc", "o2", "p2", "g"],
            ["oc", "o2", "p2", "h"],
        ]
        self.assertEqual(list(rows(object_class_mapping, db_map)), expected)
        db_map.connection.close()

    def test_export_nested_parameter_indexes(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p"),))
        import_objects(db_map, (("oc", "o1"), ("oc", "o2")))
        import_object_parameter_values(
            db_map,
            (
                ("oc", "o1", "p", Map(["A", "B"], [23.0, Map(["a", "b"], [-1.1, -2.2])])),
                ("oc", "o2", "p", Map(["C", "D"], [Map(["c", "d"], [-3.3, -4.4]), 2.3])),
            ),
        )
        db_map.commit_session("Add test data.")
        object_class_mapping = ObjectClassMapping(0)
        parameter_definition_mapping = ParameterDefinitionMapping(2)
        alternative_mapping = AlternativeMapping(Position.hidden)
        parameter_definition_mapping.child = alternative_mapping
        object_mapping = ObjectMapping(1)
        alternative_mapping.child = object_mapping
        index_mapping_1 = ParameterIndexMapping(3)
        index_mapping_2 = ParameterIndexMapping(4)
        index_mapping_1.child = index_mapping_2
        object_mapping.child = index_mapping_1
        object_class_mapping.child = parameter_definition_mapping
        expected = [
            ["oc", "o1", "p", "A", None],
            ["oc", "o1", "p", "B", "a"],
            ["oc", "o1", "p", "B", "b"],
            ["oc", "o2", "p", "C", "c"],
            ["oc", "o2", "p", "C", "d"],
            ["oc", "o2", "p", "D", None],
        ]
        self.assertEqual(list(rows(object_class_mapping, db_map)), expected)
        db_map.connection.close()

    def test_export_nested_map_values_only(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p"),))
        import_objects(db_map, (("oc", "o1"), ("oc", "o2")))
        import_object_parameter_values(
            db_map,
            (
                ("oc", "o1", "p", Map(["A", "B"], [23.0, Map(["a", "b"], [-1.1, -2.2])])),
                ("oc", "o2", "p", Map(["C", "D"], [Map(["c", "d"], [-3.3, -4.4]), 2.3])),
            ),
        )
        db_map.commit_session("Add test data.")
        object_class_mapping = ObjectClassMapping(Position.hidden)
        parameter_definition_mapping = ParameterDefinitionMapping(Position.hidden)
        object_mapping = ObjectMapping(Position.hidden)
        parameter_definition_mapping.child = object_mapping
        alternative_mapping = AlternativeMapping(Position.hidden)
        object_mapping.child = alternative_mapping
        index_mapping_1 = ParameterIndexMapping(Position.hidden)
        index_mapping_2 = ParameterIndexMapping(Position.hidden)
        value_mapping = ExpandedParameterValueMapping(0)
        index_mapping_2.child = value_mapping
        index_mapping_1.child = index_mapping_2
        alternative_mapping.child = index_mapping_1
        object_class_mapping.child = parameter_definition_mapping
        expected = [[23.0], [-1.1], [-2.2], [-3.3], [-4.4], [2.3]]
        self.assertEqual(list(rows(object_class_mapping, db_map)), expected)
        db_map.connection.close()

    def test_full_pivot_table(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p"),))
        import_objects(db_map, (("oc", "o1"), ("oc", "o2")))
        import_object_parameter_values(
            db_map,
            (
                ("oc", "o1", "p", Map(["A", "B"], [Map(["a", "b"], [-1.1, -2.2]), Map(["a", "b"], [-3.3, -4.4])])),
                ("oc", "o2", "p", Map(["A", "B"], [Map(["a", "b"], [-5.5, -6.6]), Map(["a", "b"], [-7.7, -8.8])])),
            ),
        )
        db_map.commit_session("Add test data.")
        object_class_mapping = ObjectClassMapping(0)
        parameter_definition_mapping = ParameterDefinitionMapping(1)
        alternative_mapping = AlternativeMapping(Position.hidden)
        parameter_definition_mapping.child = alternative_mapping
        object_mapping = ObjectMapping(-1)
        alternative_mapping.child = object_mapping
        index_mapping_1 = ParameterIndexMapping(1)
        index_mapping_2 = ParameterIndexMapping(2)
        value_mapping = ExpandedParameterValueMapping(-2)
        index_mapping_2.child = value_mapping
        index_mapping_1.child = index_mapping_2
        object_mapping.child = index_mapping_1
        object_class_mapping.child = parameter_definition_mapping
        expected = [
            [None, None, None, "o1", "o2"],
            ["oc", "A", "a", -1.1, -5.5],
            ["oc", "A", "b", -2.2, -6.6],
            ["oc", "B", "a", -3.3, -7.7],
            ["oc", "B", "b", -4.4, -8.8],
        ]
        self.assertEqual(list(rows(object_class_mapping, db_map)), expected)
        db_map.connection.close()

    def test_full_pivot_table_with_hidden_columns(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p"),))
        import_objects(db_map, (("oc", "o1"), ("oc", "o2")))
        import_object_parameter_values(
            db_map, (("oc", "o1", "p", Map(["A", "B"], [-1.1, -2.2])), ("oc", "o2", "p", Map(["A", "B"], [-5.5, -6.6])))
        )
        db_map.commit_session("Add test data.")
        mapping = object_parameter_export(0, 2, Position.hidden, -1, 3, 5, [4])
        expected = [
            [None, None, None, None, None, "o1", "o2"],
            ["oc", None, "p", "Base", "A", -1.1, -5.5],
            ["oc", None, "p", "Base", "B", -2.2, -6.6],
        ]
        self.assertEqual(list(rows(mapping, db_map)), expected)
        db_map.connection.close()

    def test_objects_as_pivot_header_for_indexed_values_with_alternatives(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_alternatives(db_map, ("alt",))
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p"),))
        import_objects(db_map, (("oc", "o1"), ("oc", "o2")))
        import_object_parameter_values(
            db_map,
            (
                ("oc", "o1", "p", Map(["A", "B"], [-1.1, -2.2]), "Base"),
                ("oc", "o1", "p", Map(["A", "B"], [-3.3, -4.4]), "alt"),
                ("oc", "o2", "p", Map(["A", "B"], [-5.5, -6.6]), "Base"),
                ("oc", "o2", "p", Map(["A", "B"], [-7.7, -8.8]), "alt"),
            ),
        )
        db_map.commit_session("Add test data.")
        mapping = object_parameter_export(0, 2, Position.hidden, -1, 3, 5, [4])
        expected = [
            [None, None, None, None, None, "o1", "o2"],
            ["oc", None, "p", "Base", "A", -1.1, -5.5],
            ["oc", None, "p", "Base", "B", -2.2, -6.6],
            ["oc", None, "p", "alt", "A", -3.3, -7.7],
            ["oc", None, "p", "alt", "B", -4.4, -8.8],
        ]
        self.assertEqual(list(rows(mapping, db_map)), expected)
        db_map.connection.close()

    def test_objects_and_indexes_as_pivot_header(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p"),))
        import_objects(db_map, (("oc", "o1"), ("oc", "o2")))
        import_object_parameter_values(
            db_map, (("oc", "o1", "p", Map(["A", "B"], [-1.1, -2.2])), ("oc", "o2", "p", Map(["A", "B"], [-3.3, -4.4])))
        )
        db_map.commit_session("Add test data.")
        mapping = object_parameter_export(0, 2, Position.hidden, -1, 3, 4, [-2])
        expected = [
            [None, None, None, None, "o1", "o1", "o2", "o2"],
            [None, None, None, None, "A", "B", "A", "B"],
            ["oc", None, "p", "Base", -1.1, -2.2, -3.3, -4.4],
        ]
        self.assertEqual(list(rows(mapping, db_map)), expected)
        db_map.connection.close()

    def test_objects_and_indexes_as_pivot_header_with_multiple_alternatives_and_parameters(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_alternatives(db_map, ("alt",))
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p1"),))
        import_object_parameters(db_map, (("oc", "p2"),))
        import_objects(db_map, (("oc", "o1"), ("oc", "o2")))
        import_object_parameter_values(
            db_map,
            (
                ("oc", "o1", "p1", Map(["A", "B"], [-1.1, -2.2]), "Base"),
                ("oc", "o1", "p1", Map(["A", "B"], [-3.3, -4.4]), "alt"),
                ("oc", "o1", "p2", Map(["A", "B"], [-5.5, -6.6]), "Base"),
                ("oc", "o1", "p2", Map(["A", "B"], [-7.7, -8.8]), "alt"),
                ("oc", "o2", "p1", Map(["A", "B"], [-9.9, -10.1]), "Base"),
                ("oc", "o2", "p1", Map(["A", "B"], [-11.1, -12.2]), "alt"),
                ("oc", "o2", "p2", Map(["A", "B"], [-13.3, -14.4]), "Base"),
                ("oc", "o2", "p2", Map(["A", "B"], [-15.5, -16.6]), "alt"),
            ),
        )
        db_map.commit_session("Add test data.")
        mapping = object_parameter_export(0, 1, Position.hidden, -1, -2, 2, [-3])
        expected = [
            [None, None, "o1", "o1", "o1", "o1", "o2", "o2", "o2", "o2"],
            [None, None, "Base", "Base", "alt", "alt", "Base", "Base", "alt", "alt"],
            [None, None, "A", "B", "A", "B", "A", "B", "A", "B"],
            ["oc", "p1", -1.1, -2.2, -3.3, -4.4, -9.9, -10.1, -11.1, -12.2],
            ["oc", "p2", -5.5, -6.6, -7.7, -8.8, -13.3, -14.4, -15.5, -16.6],
        ]
        self.assertEqual(list(rows(mapping, db_map)), expected)
        db_map.connection.close()

    def test_empty_column_while_pivoted_handled_gracefully(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_alternatives(db_map, ("alt",))
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p"),))
        import_objects(db_map, (("oc", "o"),))
        db_map.commit_session("Add test data.")
        mapping = ObjectClassMapping(0)
        definition = ParameterDefinitionMapping(1)
        value_list = ParameterValueListMapping(2)
        object_ = ObjectMapping(-1)
        value_list.child = object_
        definition.child = value_list
        mapping.child = definition
        self.assertEqual(list(rows(mapping, db_map)), [])
        db_map.connection.close()

    def test_object_classes_as_header_row_and_objects_in_columns(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        import_objects(
            db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21"), ("oc3", "o31"), ("oc3", "o32"), ("oc3", "o33"))
        )
        db_map.commit_session("Add test data.")
        object_class_mapping = ObjectClassMapping(-1)
        object_class_mapping.child = ObjectMapping(0)
        self.assertEqual(
            list(rows(object_class_mapping, db_map)),
            [["oc1", "oc2", "oc3"], ["o11", "o21", "o31"], ["o12", None, "o32"], [None, None, "o33"]],
        )
        db_map.connection.close()

    def test_object_classes_as_table_names(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        import_objects(
            db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21"), ("oc3", "o31"), ("oc3", "o32"), ("oc3", "o33"))
        )
        db_map.commit_session("Add test data.")
        object_class_mapping = ObjectClassMapping(Position.table_name)
        object_class_mapping.child = ObjectMapping(0)
        tables = dict()
        for title, title_key in titles(object_class_mapping, db_map):
            tables[title] = list(rows(object_class_mapping, db_map, title_key))
        self.assertEqual(tables, {"oc1": [["o11"], ["o12"]], "oc2": [["o21"]], "oc3": [["o31"], ["o32"], ["o33"]]})
        db_map.connection.close()

    def test_parameter_definitions_as_table_names(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        import_object_parameters(db_map, (("oc1", "p11"), ("oc2", "p21"), ("oc2", "p22")))
        import_objects(db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21"), ("oc3", "o31")))
        db_map.commit_session("Add test data.")
        object_class_mapping = ObjectClassMapping(Position.hidden)
        definition_mapping = ParameterDefinitionMapping(Position.table_name)
        object_mapping = ObjectMapping(0)
        object_class_mapping.child = definition_mapping
        definition_mapping.child = object_mapping
        tables = dict()
        for title, title_key in titles(object_class_mapping, db_map):
            tables[title] = list(rows(object_class_mapping, db_map, title_key))
        self.assertEqual(tables, {"p11": [["o11"], ["o12"]], "p21": [["o21"]], "p22": [["o21"]]})
        db_map.connection.close()

    def test_parameter_definitions_with_value_lists(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_parameter_value_lists(db_map, (("vl1", -1.0), ("vl2", -2.0)))
        import_object_parameters(db_map, (("oc", "p1", None, "vl1"), ("oc", "p2")))
        db_map.commit_session("Add test data.")
        class_mapping = ObjectClassMapping(0)
        definition_mapping = ParameterDefinitionMapping(1)
        value_list_mapping = ParameterValueListMapping(2)
        definition_mapping.child = value_list_mapping
        class_mapping.child = definition_mapping
        tables = dict()
        for title, title_key in titles(class_mapping, db_map):
            tables[title] = list(rows(class_mapping, db_map, title_key))
        self.assertEqual(tables, {None: [["oc", "p1", "vl1"]]})
        db_map.connection.close()

    def test_parameter_definitions_and_values_and_value_lists(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_parameter_value_lists(db_map, (("vl", -1.0),))
        import_object_parameters(db_map, (("oc", "p1", None, "vl"), ("oc", "p2")))
        import_objects(db_map, (("oc", "o"),))
        import_object_parameter_values(db_map, (("oc", "o", "p1", -1.0), ("oc", "o", "p2", 5.0)))
        db_map.commit_session("Add test data.")
        flattened = [
            ObjectClassMapping(0),
            ParameterDefinitionMapping(1),
            AlternativeMapping(Position.hidden),
            ParameterValueListMapping(2),
            ObjectMapping(3),
            ParameterValueMapping(4),
        ]
        mapping = unflatten(flattened)
        tables = dict()
        for title, title_key in titles(mapping, db_map):
            tables[title] = list(rows(mapping, db_map, title_key))
        self.assertEqual(tables, {None: [["oc", "p1", "vl", "o", -1.0]]})
        db_map.connection.close()

    def test_parameter_definitions_and_values_and_ignorable_value_lists(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_parameter_value_lists(db_map, (("vl", -1.0),))
        import_object_parameters(db_map, (("oc", "p1", None, "vl"), ("oc", "p2")))
        import_objects(db_map, (("oc", "o"),))
        import_object_parameter_values(db_map, (("oc", "o", "p1", -1.0), ("oc", "o", "p2", 5.0)))
        db_map.commit_session("Add test data.")
        value_list_mapping = ParameterValueListMapping(2)
        value_list_mapping.set_ignorable()
        flattened = [
            ObjectClassMapping(0),
            ParameterDefinitionMapping(1),
            AlternativeMapping(Position.hidden),
            value_list_mapping,
            ObjectMapping(3),
            ParameterValueMapping(4),
        ]
        mapping = unflatten(flattened)
        tables = dict()
        for title, title_key in titles(mapping, db_map):
            tables[title] = list(rows(mapping, db_map, title_key))
        self.assertEqual(tables, {None: [["oc", "p1", "vl", "o", -1.0], ["oc", "p2", None, "o", 5.0]]})
        db_map.connection.close()

    def test_parameter_value_lists(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_parameter_value_lists(db_map, (("vl1", -1.0), ("vl2", -2.0)))
        db_map.commit_session("Add test data.")
        value_list_mapping = ParameterValueListMapping(0)
        tables = dict()
        for title, title_key in titles(value_list_mapping, db_map):
            tables[title] = list(rows(value_list_mapping, db_map, title_key))
        self.assertEqual(tables, {None: [["vl1"], ["vl2"]]})
        db_map.connection.close()

    def test_parameter_value_list_values(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_parameter_value_lists(db_map, (("vl1", -1.0), ("vl2", -2.0)))
        db_map.commit_session("Add test data.")
        value_list_mapping = ParameterValueListMapping(Position.table_name)
        value_mapping = ParameterValueListValueMapping(0)
        value_list_mapping.child = value_mapping
        tables = dict()
        for title, title_key in titles(value_list_mapping, db_map):
            tables[title] = list(rows(value_list_mapping, db_map, title_key))
        self.assertEqual(tables, {"vl1": [[-1.0]], "vl2": [[-2.0]]})
        db_map.connection.close()

    def test_no_item_declared_as_title_gives_full_table(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        import_object_parameters(db_map, (("oc1", "p11"), ("oc2", "p21"), ("oc2", "p22")))
        import_objects(db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21"), ("oc3", "o31")))
        db_map.commit_session("Add test data.")
        object_class_mapping = ObjectClassMapping(Position.hidden)
        definition_mapping = ParameterDefinitionMapping(Position.hidden)
        object_mapping = ObjectMapping(0)
        object_class_mapping.child = definition_mapping
        definition_mapping.child = object_mapping
        tables = dict()
        for title, title_key in titles(object_class_mapping, db_map):
            tables[title] = list(rows(object_class_mapping, db_map, title_key))
        self.assertEqual(tables, {None: [["o11"], ["o12"], ["o21"], ["o21"]]})
        db_map.connection.close()

    def test_missing_values_for_alternatives(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p1"), ("oc", "p2")))
        import_alternatives(db_map, ("alt1", "alt2"))
        import_objects(db_map, (("oc", "o1"), ("oc", "o2")))
        import_object_parameter_values(
            db_map,
            (
                ("oc", "o1", "p1", -1.1, "alt1"),
                ("oc", "o1", "p1", -1.2, "alt2"),
                ("oc", "o1", "p2", -2.2, "alt1"),
                ("oc", "o2", "p2", -5.5, "alt2"),
            ),
        )
        db_map.commit_session("Add test data.")
        object_class_mapping = ObjectClassMapping(0)
        definition_mapping = ParameterDefinitionMapping(2)
        object_mapping = ObjectMapping(1)
        alternative_mapping = AlternativeMapping(3)
        value_mapping = ParameterValueMapping(4)
        object_class_mapping.child = definition_mapping
        definition_mapping.child = object_mapping
        object_mapping.child = alternative_mapping
        alternative_mapping.child = value_mapping
        expected = [
            ["oc", "o1", "p1", "alt1", -1.1],
            ["oc", "o1", "p1", "alt2", -1.2],
            ["oc", "o1", "p2", "alt1", -2.2],
            ["oc", "o2", "p2", "alt2", -5.5],
        ]
        self.assertEqual(list(rows(object_class_mapping, db_map)), expected)
        db_map.connection.close()

    def test_export_relationship_classes(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        import_relationship_classes(
            db_map, (("rc1", ("oc1",)), ("rc2", ("oc3", "oc2")), ("rc3", ("oc2", "oc3", "oc1")))
        )
        db_map.commit_session("Add test data.")
        relationship_class_mapping = RelationshipClassMapping(0)
        self.assertEqual(list(rows(relationship_class_mapping, db_map)), [["rc1"], ["rc2"], ["rc3"]])
        db_map.connection.close()

    def test_export_relationships(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2"))
        import_objects(db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21")))
        import_relationship_classes(db_map, (("rc1", ("oc1",)), ("rc2", ("oc2", "oc1"))))
        import_relationships(db_map, (("rc1", ("o11",)), ("rc2", ("o21", "o11")), ("rc2", ("o21", "o12"))))
        db_map.commit_session("Add test data.")
        relationship_class_mapping = RelationshipClassMapping(0)
        relationship_mapping = RelationshipMapping(1)
        relationship_class_mapping.child = relationship_mapping
        expected = [["rc1", "rc1_o11"], ["rc2", "rc2_o21__o11"], ["rc2", "rc2_o21__o12"]]
        self.assertEqual(list(rows(relationship_class_mapping, db_map)), expected)
        db_map.connection.close()

    def test_relationships_with_different_dimensions(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2"))
        import_objects(db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21"), ("oc2", "o22")))
        import_relationship_classes(db_map, (("rc1D", ("oc1",)), ("rc2D", ("oc1", "oc2"))))
        import_relationships(db_map, (("rc1D", ("o11",)), ("rc1D", ("o12",))))
        import_relationships(
            db_map,
            (("rc2D", ("o11", "o21")), ("rc2D", ("o11", "o22")), ("rc2D", ("o12", "o21")), ("rc2D", ("o12", "o22"))),
        )
        db_map.commit_session("Add test data.")
        relationship_class_mapping = RelationshipClassMapping(0)
        object_class_mapping1 = RelationshipClassObjectClassMapping(1)
        object_class_mapping2 = RelationshipClassObjectClassMapping(2)
        relationship_mapping = RelationshipMapping(Position.hidden)
        object_mapping1 = RelationshipObjectMapping(3)
        object_mapping2 = RelationshipObjectMapping(4)
        object_mapping1.child = object_mapping2
        relationship_mapping.child = object_mapping1
        object_class_mapping2.child = relationship_mapping
        object_class_mapping1.child = object_class_mapping2
        relationship_class_mapping.child = object_class_mapping1
        tables = dict()
        for title, title_key in titles(relationship_class_mapping, db_map):
            tables[title] = list(rows(relationship_class_mapping, db_map, title_key))
        expected = [
            ["rc1D", "oc1", None, "o11", None],
            ["rc1D", "oc1", None, "o12", None],
            ["rc2D", "oc1", "oc2", "o11", "o21"],
            ["rc2D", "oc1", "oc2", "o11", "o22"],
            ["rc2D", "oc1", "oc2", "o12", "o21"],
            ["rc2D", "oc1", "oc2", "o12", "o22"],
        ]
        self.assertEqual(tables[None], expected)
        db_map.connection.close()

    def test_default_parameter_values(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        import_object_parameters(db_map, (("oc1", "p11", 3.14), ("oc2", "p21", 14.3), ("oc2", "p22", -1.0)))
        db_map.commit_session("Add test data.")
        object_class_mapping = ObjectClassMapping(0)
        definition_mapping = ParameterDefinitionMapping(1)
        default_value_mapping = ParameterDefaultValueMapping(2)
        definition_mapping.child = default_value_mapping
        object_class_mapping.child = definition_mapping
        table = list(rows(object_class_mapping, db_map))
        self.assertEqual(table, [["oc1", "p11", 3.14], ["oc2", "p21", 14.3], ["oc2", "p22", -1.0]])
        db_map.connection.close()

    def test_indexed_default_parameter_values(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        import_object_parameters(
            db_map,
            (
                ("oc1", "p11", Map(["a", "b"], [-6.28, -3.14])),
                ("oc2", "p21", Map(["A", "B"], [1.1, 2.2])),
                ("oc2", "p22", Map(["D"], [-1.0])),
            ),
        )
        db_map.commit_session("Add test data.")
        object_class_mapping = ObjectClassMapping(0)
        definition_mapping = ParameterDefinitionMapping(1)
        index_mapping = ParameterDefaultValueIndexMapping(2)
        value_mapping = ExpandedParameterDefaultValueMapping(3)
        index_mapping.child = value_mapping
        definition_mapping.child = index_mapping
        object_class_mapping.child = definition_mapping
        table = list(rows(object_class_mapping, db_map))
        expected = [
            ["oc1", "p11", "a", -6.28],
            ["oc1", "p11", "b", -3.14],
            ["oc2", "p21", "A", 1.1],
            ["oc2", "p21", "B", 2.2],
            ["oc2", "p22", "D", -1.0],
        ]
        self.assertEqual(table, expected)
        db_map.connection.close()

    def test_replace_parameter_indexes_by_external_data(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p1"),))
        import_objects(db_map, (("oc", "o1"), ("oc", "o2")))
        import_object_parameter_values(
            db_map, (("oc", "o1", "p1", Map(["a", "b"], [5.0, -5.0])), ("oc", "o2", "p1", Map(["a", "b"], [2.0, -2.0])))
        )
        db_map.commit_session("Add test data.")
        object_class_mapping = ObjectClassMapping(0)
        parameter_definition_mapping = ParameterDefinitionMapping(2)
        alternative_mapping = AlternativeMapping(Position.hidden)
        parameter_definition_mapping.child = alternative_mapping
        object_mapping = ObjectMapping(1)
        alternative_mapping.child = object_mapping
        index_mapping = ParameterIndexMapping(3)
        value_mapping = ExpandedParameterValueMapping(4)
        index_mapping.child = value_mapping
        index_mapping.replace_data(["c", "d"])
        object_mapping.child = index_mapping
        object_class_mapping.child = parameter_definition_mapping
        expected = [
            ["oc", "o1", "p1", "c", 5.0],
            ["oc", "o1", "p1", "d", -5.0],
            ["oc", "o2", "p1", "c", 2.0],
            ["oc", "o2", "p1", "d", -2.0],
        ]
        self.assertEqual(list(rows(object_class_mapping, db_map)), expected)
        db_map.connection.close()

    def test_constant_mapping_as_title(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        db_map.commit_session("Add test data.")
        constant_mapping = FixedValueMapping(Position.table_name, "title_text")
        object_class_mapping = ObjectClassMapping(0)
        constant_mapping.child = object_class_mapping
        tables = dict()
        for title, title_key in titles(constant_mapping, db_map):
            tables[title] = list(rows(constant_mapping, db_map, title_key))
        self.assertEqual(tables, {"title_text": [["oc1"], ["oc2"], ["oc3"]]})
        db_map.connection.close()

    def test_scenario_mapping(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_scenarios(db_map, ("s1", "s2"))
        db_map.commit_session("Add test data.")
        scenario_mapping = ScenarioMapping(0)
        tables = dict()
        for title, title_key in titles(scenario_mapping, db_map):
            tables[title] = list(rows(scenario_mapping, db_map, title_key))
        self.assertEqual(tables, {None: [["s1"], ["s2"]]})
        db_map.connection.close()

    def test_scenario_active_flag_mapping(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_scenarios(db_map, (("s1", True), ("s2", False)))
        db_map.commit_session("Add test data.")
        scenario_mapping = ScenarioMapping(0)
        active_flag_mapping = ScenarioActiveFlagMapping(1)
        scenario_mapping.child = active_flag_mapping
        tables = dict()
        for title, title_key in titles(scenario_mapping, db_map):
            tables[title] = list(rows(scenario_mapping, db_map, title_key))
        self.assertEqual(tables, {None: [["s1", True], ["s2", False]]})
        db_map.connection.close()

    def test_scenario_alternative_mapping(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_alternatives(db_map, ("a1", "a2", "a3"))
        import_scenarios(db_map, ("s1", "s2", "empty"))
        import_scenario_alternatives(db_map, (("s1", "a2"), ("s1", "a1", "a2"), ("s2", "a2"), ("s2", "a3", "a2")))
        db_map.commit_session("Add test data.")
        scenario_mapping = ScenarioMapping(0)
        scenario_alternative_mapping = ScenarioAlternativeMapping(1)
        scenario_mapping.child = scenario_alternative_mapping
        tables = dict()
        for title, title_key in titles(scenario_mapping, db_map):
            tables[title] = list(rows(scenario_mapping, db_map, title_key))
        self.assertEqual(tables, {None: [["s1", "a1"], ["s1", "a2"], ["s2", "a3"], ["s2", "a2"]]})
        db_map.connection.close()

    def test_tool_mapping(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_tools(db_map, ("tool1", "tool2"))
        db_map.commit_session("Add test data.")
        tool_mapping = ToolMapping(0)
        tables = dict()
        for title, title_key in titles(tool_mapping, db_map):
            tables[title] = list(rows(tool_mapping, db_map, title_key))
        self.assertEqual(tables, {None: [["tool1"], ["tool2"]]})
        db_map.connection.close()

    def test_feature_mapping(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2"))
        import_parameter_value_lists(db_map, (("features", "feat1"), ("features", "feat2")))
        import_object_parameters(
            db_map,
            (
                ("oc1", "p1", "feat1", "features"),
                ("oc1", "p2", "feat1", "features"),
                ("oc2", "p3", "feat2", "features"),
            ),
        )
        import_features(db_map, (("oc1", "p2"), ("oc2", "p3")))
        db_map.commit_session("Add test data.")
        class_mapping = FeatureEntityClassMapping(0)
        parameter_mapping = FeatureParameterDefinitionMapping(1)
        class_mapping.child = parameter_mapping
        tables = dict()
        for title, title_key in titles(class_mapping, db_map):
            tables[title] = list(rows(class_mapping, db_map, title_key))
        self.assertEqual(tables, {None: [["oc1", "p2"], ["oc2", "p3"]]})
        db_map.connection.close()

    def test_tool_feature_mapping(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2"))
        import_parameter_value_lists(db_map, (("features", "feat1"), ("features", "feat2")))
        import_object_parameters(
            db_map,
            (
                ("oc1", "p1", "feat1", "features"),
                ("oc1", "p2", "feat1", "features"),
                ("oc2", "p3", "feat2", "features"),
            ),
        )
        import_features(db_map, (("oc1", "p1"), ("oc1", "p2"), ("oc2", "p3")))
        import_tools(db_map, ("tool1", "tool2"))
        import_tool_features(
            db_map, (("tool1", "oc1", "p1", True), ("tool1", "oc2", "p3", False), ("tool2", "oc1", "p1", True))
        )
        db_map.commit_session("Add test data.")
        mapping = unflatten(
            [
                ToolMapping(Position.table_name),
                ToolFeatureEntityClassMapping(0),
                ToolFeatureParameterDefinitionMapping(1),
                ToolFeatureRequiredFlagMapping(2),
            ]
        )
        tables = dict()
        for title, title_key in titles(mapping, db_map):
            tables[title] = list(rows(mapping, db_map, title_key))
        expected = {"tool1": [["oc1", "p1", True], ["oc2", "p3", False]], "tool2": [["oc1", "p1", True]]}
        self.assertEqual(tables, expected)
        db_map.connection.close()

    def test_tool_feature_method_mapping(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2"))
        import_parameter_value_lists(db_map, (("features", "feat1"), ("features", "feat2")))
        import_object_parameters(
            db_map,
            (
                ("oc1", "p1", "feat1", "features"),
                ("oc1", "p2", "feat1", "features"),
                ("oc2", "p3", "feat2", "features"),
            ),
        )
        import_features(db_map, (("oc1", "p1"), ("oc1", "p2"), ("oc2", "p3")))
        import_tools(db_map, ("tool1", "tool2"))
        import_tool_features(
            db_map, (("tool1", "oc1", "p1", True), ("tool1", "oc2", "p3", False), ("tool2", "oc1", "p1", True))
        )
        import_tool_feature_methods(
            db_map,
            (
                ("tool1", "oc1", "p1", "feat1"),
                ("tool1", "oc1", "p1", "feat2"),
                ("tool2", "oc1", "p1", "feat1"),
                ("tool2", "oc1", "p1", "feat2"),
            ),
        )
        db_map.commit_session("Add test data.")
        mapping = unflatten(
            [
                ToolMapping(Position.table_name),
                ToolFeatureMethodEntityClassMapping(0),
                ToolFeatureMethodParameterDefinitionMapping(1),
                ToolFeatureMethodMethodMapping(2),
            ]
        )
        tables = dict()
        for title, title_key in titles(mapping, db_map):
            tables[title] = list(rows(mapping, db_map, title_key))
        expected = {
            "tool1": [["oc1", "p1", "feat1"], ["oc1", "p1", "feat2"]],
            "tool2": [["oc1", "p1", "feat1"], ["oc1", "p1", "feat2"]],
        }
        self.assertEqual(tables, expected)
        db_map.connection.close()

    def test_count_mappings(self):
        object_class_mapping = ObjectClassMapping(2)
        parameter_definition_mapping = ParameterDefinitionMapping(0)
        object_mapping = ObjectMapping(1)
        parameter_definition_mapping.child = object_mapping
        object_class_mapping.child = parameter_definition_mapping
        self.assertEqual(object_class_mapping.count_mappings(), 3)

    def test_flatten(self):
        object_class_mapping = ObjectClassMapping(2)
        parameter_definition_mapping = ParameterDefinitionMapping(0)
        object_mapping = ObjectMapping(1)
        parameter_definition_mapping.child = object_mapping
        object_class_mapping.child = parameter_definition_mapping
        mappings = object_class_mapping.flatten()
        self.assertEqual(mappings, [object_class_mapping, parameter_definition_mapping, object_mapping])

    def test_unflatten_sets_last_mappings_child_to_none(self):
        object_class_mapping = ObjectClassMapping(2)
        object_mapping = ObjectMapping(1)
        object_class_mapping.child = object_mapping
        mapping_list = object_class_mapping.flatten()
        root = unflatten(mapping_list[:1])
        self.assertIsNone(root.child)

    def test_has_title(self):
        object_class_mapping = ObjectClassMapping(0)
        parameter_definition_mapping = ParameterDefinitionMapping(Position.table_name)
        object_mapping = ObjectMapping(1)
        parameter_definition_mapping.child = object_mapping
        object_class_mapping.child = parameter_definition_mapping
        self.assertTrue(object_class_mapping.has_title())

    def test_drop_non_positioned_tail(self):
        object_class_mapping = ObjectClassMapping(0)
        parameter_definition_mapping = ParameterDefinitionMapping(Position.hidden)
        object_mapping = ObjectMapping(1)
        alternative_mapping = AlternativeMapping(Position.hidden)
        value_mapping = ParameterValueMapping(Position.hidden)
        alternative_mapping.child = value_mapping
        object_mapping.child = alternative_mapping
        parameter_definition_mapping.child = object_mapping
        object_class_mapping.child = parameter_definition_mapping
        object_class_mapping.drop_non_positioned_tail()
        flattened = object_class_mapping.flatten()
        self.assertEqual(flattened, [object_class_mapping, parameter_definition_mapping, object_mapping])

    def test_serialization(self):
        mappings = [
            ObjectClassMapping(0),
            RelationshipClassMapping(Position.table_name),
            RelationshipClassObjectClassMapping(2),
            ParameterDefinitionMapping(1),
            ObjectMapping(-1),
            RelationshipMapping(Position.hidden),
            RelationshipObjectMapping(-1),
            AlternativeMapping(3),
            ParameterValueMapping(4),
            ParameterIndexMapping(5),
            ExpandedParameterValueMapping(6),
        ]
        expected_positions = [m.position for m in mappings]
        expected_types = [type(m) for m in mappings]
        root = unflatten(mappings)
        serialized = to_dict(root)
        deserialized = from_dict(serialized)
        self.assertEqual([type(m) for m in deserialized.flatten()], expected_types)
        self.assertEqual([m.position for m in deserialized.flatten()], expected_positions)

    def test_pickle_ignorable_mapping(self):
        mapping = unflatten([ObjectClassMapping(0), ObjectMapping(1)])
        mapping.child.set_ignorable()
        data = pickle.dumps(mapping)
        restored = pickle.loads(data)
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2"))
        db_map.commit_session("Add test data.")
        self.assertEqual(list(rows(restored, db_map)), [["oc1", None], ["oc2", None]])
        db_map.connection.close()


if __name__ == "__main__":
    unittest.main()
