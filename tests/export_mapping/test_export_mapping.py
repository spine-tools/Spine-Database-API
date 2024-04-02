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
Unit tests for export mappings.

"""

import unittest
from spinedb_api import (
    DatabaseMapping,
    import_alternatives,
    import_object_classes,
    import_object_parameter_values,
    import_object_parameters,
    import_objects,
    import_parameter_value_lists,
    import_relationship_classes,
    import_relationships,
    import_scenario_alternatives,
    import_scenarios,
    Map,
)
from spinedb_api.import_functions import import_object_groups
from spinedb_api.mapping import Position, to_dict
from spinedb_api.export_mapping import (
    rows,
    titles,
    entity_parameter_default_value_export,
    entity_parameter_value_export,
    entity_export,
)
from spinedb_api.export_mapping.export_mapping import (
    AlternativeDescriptionMapping,
    AlternativeMapping,
    drop_non_positioned_tail,
    FixedValueMapping,
    ExpandedParameterValueMapping,
    ExpandedParameterDefaultValueMapping,
    from_dict,
    EntityGroupMapping,
    EntityGroupEntityMapping,
    EntityMapping,
    EntityClassMapping,
    ParameterDefaultValueMapping,
    ParameterDefaultValueIndexMapping,
    ParameterDefinitionMapping,
    ParameterValueIndexMapping,
    ParameterValueListMapping,
    ParameterValueListValueMapping,
    ParameterValueMapping,
    ParameterValueTypeMapping,
    DimensionMapping,
    ElementMapping,
    ScenarioActiveFlagMapping,
    ScenarioAlternativeMapping,
    ScenarioDescriptionMapping,
    ScenarioMapping,
)
from spinedb_api.mapping import unflatten


class TestExportMapping(unittest.TestCase):
    def test_export_empty_table(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        object_class_mapping = EntityClassMapping(0)
        self.assertEqual(list(rows(object_class_mapping, db_map)), [])
        db_map.close()

    def test_export_single_object_class(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("object_class",))
        db_map.commit_session("Add test data.")
        object_class_mapping = EntityClassMapping(0)
        self.assertEqual(list(rows(object_class_mapping, db_map)), [["object_class"]])
        db_map.close()

    def test_export_objects(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        import_objects(
            db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21"), ("oc3", "o31"), ("oc3", "o32"), ("oc3", "o33"))
        )
        db_map.commit_session("Add test data.")
        object_class_mapping = EntityClassMapping(0)
        object_class_mapping.child = EntityMapping(1)
        self.assertEqual(
            list(rows(object_class_mapping, db_map)),
            [["oc1", "o11"], ["oc1", "o12"], ["oc2", "o21"], ["oc3", "o31"], ["oc3", "o32"], ["oc3", "o33"]],
        )
        db_map.close()

    def test_hidden_tail(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1",))
        import_objects(db_map, (("oc1", "o11"), ("oc1", "o12")))
        db_map.commit_session("Add test data.")
        object_class_mapping = EntityClassMapping(0)
        object_class_mapping.child = EntityMapping(Position.hidden)
        self.assertEqual(list(rows(object_class_mapping, db_map)), [["oc1"], ["oc1"]])
        db_map.close()

    def test_pivot_without_values(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1",))
        import_objects(db_map, (("oc1", "o11"), ("oc1", "o12")))
        db_map.commit_session("Add test data.")
        object_class_mapping = EntityClassMapping(-1)
        object_class_mapping.child = EntityMapping(Position.hidden)
        self.assertEqual(list(rows(object_class_mapping, db_map)), [])
        db_map.close()

    def test_hidden_tail_pivoted(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p1"), ("oc", "p2")))
        import_objects(db_map, (("oc", "o1"), ("oc", "o2")))
        import_object_parameter_values(db_map, (("oc", "o1", "p1", -11.0), ("oc", "o1", "p2", -12.0)))
        db_map.commit_session("Add test data.")
        root_mapping = unflatten(
            [
                EntityClassMapping(0),
                ParameterDefinitionMapping(-1),
                EntityMapping(1),
                AlternativeMapping(2),
                ParameterValueMapping(Position.hidden),
            ]
        )
        expected = [[None, None, "p1", "p2"], ["oc", "o1", "Base", "Base"]]
        self.assertEqual(list(rows(root_mapping, db_map)), expected)
        db_map.close()

    def test_hidden_leaf_item_in_regular_table_valid(self):
        object_class_mapping = EntityClassMapping(0)
        object_class_mapping.child = EntityMapping(Position.hidden)
        self.assertEqual(object_class_mapping.check_validity(), [])

    def test_hidden_leaf_item_in_pivot_table_not_valid(self):
        object_class_mapping = EntityClassMapping(-1)
        object_class_mapping.child = EntityMapping(Position.hidden)
        self.assertEqual(object_class_mapping.check_validity(), ["Cannot be pivoted."])

    def test_object_groups(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_objects(db_map, (("oc", "o1"), ("oc", "o2"), ("oc", "o3"), ("oc", "g1"), ("oc", "g2")))
        import_object_groups(db_map, (("oc", "g1", "o1"), ("oc", "g1", "o2"), ("oc", "g2", "o3")))
        db_map.commit_session("Add test data.")
        flattened = [EntityClassMapping(0), EntityGroupMapping(1)]
        mapping = unflatten(flattened)
        self.assertEqual(list(rows(mapping, db_map)), [["oc", "g1"], ["oc", "g2"]])
        db_map.close()

    def test_object_groups_with_objects(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_objects(db_map, (("oc", "o1"), ("oc", "o2"), ("oc", "o3"), ("oc", "g1"), ("oc", "g2")))
        import_object_groups(db_map, (("oc", "g1", "o1"), ("oc", "g1", "o2"), ("oc", "g2", "o3")))
        db_map.commit_session("Add test data.")
        flattened = [EntityClassMapping(0), EntityGroupMapping(1), EntityGroupEntityMapping(2)]
        mapping = unflatten(flattened)
        self.assertEqual(list(rows(mapping, db_map)), [["oc", "g1", "o1"], ["oc", "g1", "o2"], ["oc", "g2", "o3"]])
        db_map.close()

    def test_object_groups_with_parameter_values(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_objects(db_map, (("oc", "o1"), ("oc", "o2"), ("oc", "o3"), ("oc", "g1"), ("oc", "g2")))
        import_object_groups(db_map, (("oc", "g1", "o1"), ("oc", "g1", "o2"), ("oc", "g2", "o3")))
        import_object_parameters(db_map, (("oc", "p"),))
        import_object_parameter_values(
            db_map, (("oc", "o1", "p", -11.0), ("oc", "o2", "p", -12.0), ("oc", "o3", "p", -13.0))
        )
        db_map.commit_session("Add test data.")
        flattened = [
            EntityClassMapping(0),
            EntityGroupMapping(1),
            EntityGroupEntityMapping(2),
            ParameterDefinitionMapping(Position.hidden),
            AlternativeMapping(Position.hidden),
            ParameterValueMapping(3),
        ]
        mapping = unflatten(flattened)
        self.assertEqual(
            list(rows(mapping, db_map)),
            [["oc", "g1", "o1", -11.0], ["oc", "g1", "o2", -12.0], ["oc", "g2", "o3", -13.0]],
        )
        db_map.close()

    def test_export_parameter_definitions(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2"))
        import_object_parameters(db_map, (("oc1", "p11"), ("oc1", "p12"), ("oc2", "p21")))
        import_objects(db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21")))
        db_map.commit_session("Add test data.")
        object_class_mapping = EntityClassMapping(0)
        parameter_definition_mapping = ParameterDefinitionMapping(1)
        parameter_definition_mapping.child = EntityMapping(2)
        object_class_mapping.child = parameter_definition_mapping
        expected = [
            ["oc1", "p11", "o11"],
            ["oc1", "p11", "o12"],
            ["oc1", "p12", "o11"],
            ["oc1", "p12", "o12"],
            ["oc2", "p21", "o21"],
        ]
        self.assertEqual(list(rows(object_class_mapping, db_map)), expected)
        db_map.close()

    def test_export_single_parameter_value_when_there_are_multiple_objects(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2"))
        import_object_parameters(db_map, (("oc1", "p11"), ("oc1", "p12"), ("oc2", "p21")))
        import_objects(db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21")))
        import_object_parameter_values(db_map, (("oc1", "o11", "p12", -11.0),))
        db_map.commit_session("Add test data.")
        object_class_mapping = EntityClassMapping(0)
        parameter_definition_mapping = ParameterDefinitionMapping(1)
        alternative_mapping = AlternativeMapping(Position.hidden)
        parameter_definition_mapping.child = alternative_mapping
        object_mapping = EntityMapping(2)
        alternative_mapping.child = object_mapping
        value_mapping = ParameterValueMapping(3)
        object_mapping.child = value_mapping
        object_class_mapping.child = parameter_definition_mapping
        self.assertEqual(list(rows(object_class_mapping, db_map)), [["oc1", "p12", "o11", -11.0]])
        db_map.close()

    def test_export_single_parameter_value_pivoted_by_object_name(self):
        db_map = DatabaseMapping("sqlite://", create=True)
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
        object_class_mapping = EntityClassMapping(0)
        parameter_definition_mapping = ParameterDefinitionMapping(1)
        alternative_mapping = AlternativeMapping(Position.hidden)
        parameter_definition_mapping.child = alternative_mapping
        object_mapping = EntityMapping(-1)
        alternative_mapping.child = object_mapping
        value_mapping = ParameterValueMapping(-2)
        object_mapping.child = value_mapping
        object_class_mapping.child = parameter_definition_mapping
        expected = [[None, None, "o11", "o12"], ["oc1", "p11", -11.0, -21.0], ["oc1", "p12", -12.0, -22.0]]
        self.assertEqual(list(rows(object_class_mapping, db_map)), expected)
        db_map.close()

    def test_minimum_pivot_index_need_not_be_minus_one(self):
        db_map = DatabaseMapping("sqlite://", create=True)
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
        mapping = entity_parameter_value_export(
            1, 2, Position.hidden, 0, None, None, -2, Position.hidden, 4, [Position.hidden], [3]
        )
        expected = [
            [None, None, None, None, "Base", "alt"],
            ["o", "oc", "p", "A", -1.1, -5.5],
            ["o", "oc", "p", "B", -2.2, -6.6],
        ]
        self.assertEqual(list(rows(mapping, db_map)), expected)
        db_map.close()

    def test_pivot_row_order(self):
        db_map = DatabaseMapping("sqlite://", create=True)
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
        object_class_mapping = EntityClassMapping(0)
        parameter_definition_mapping = ParameterDefinitionMapping(-1)
        alternative_mapping = AlternativeMapping(Position.hidden)
        object_mapping = EntityMapping(-2)
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
        db_map.close()

    def test_export_parameter_indexes(self):
        db_map = DatabaseMapping("sqlite://", create=True)
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
        object_class_mapping = EntityClassMapping(0)
        parameter_definition_mapping = ParameterDefinitionMapping(2)
        alternative_mapping = AlternativeMapping(Position.hidden)
        parameter_definition_mapping.child = alternative_mapping
        object_mapping = EntityMapping(1)
        alternative_mapping.child = object_mapping
        index_mapping = ParameterValueIndexMapping(3)
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
        db_map.close()

    def test_export_nested_parameter_indexes(self):
        db_map = DatabaseMapping("sqlite://", create=True)
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
        object_class_mapping = EntityClassMapping(0)
        parameter_definition_mapping = ParameterDefinitionMapping(2)
        alternative_mapping = AlternativeMapping(Position.hidden)
        parameter_definition_mapping.child = alternative_mapping
        object_mapping = EntityMapping(1)
        alternative_mapping.child = object_mapping
        index_mapping_1 = ParameterValueIndexMapping(3)
        index_mapping_2 = ParameterValueIndexMapping(4)
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
        db_map.close()

    def test_export_nested_map_values_only(self):
        db_map = DatabaseMapping("sqlite://", create=True)
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
        object_class_mapping = EntityClassMapping(Position.hidden)
        parameter_definition_mapping = ParameterDefinitionMapping(Position.hidden)
        object_mapping = EntityMapping(Position.hidden)
        parameter_definition_mapping.child = object_mapping
        alternative_mapping = AlternativeMapping(Position.hidden)
        object_mapping.child = alternative_mapping
        index_mapping_1 = ParameterValueIndexMapping(Position.hidden)
        index_mapping_2 = ParameterValueIndexMapping(Position.hidden)
        value_mapping = ExpandedParameterValueMapping(0)
        index_mapping_2.child = value_mapping
        index_mapping_1.child = index_mapping_2
        alternative_mapping.child = index_mapping_1
        object_class_mapping.child = parameter_definition_mapping
        expected = [[23.0], [-1.1], [-2.2], [-3.3], [-4.4], [2.3]]
        self.assertEqual(list(rows(object_class_mapping, db_map)), expected)
        db_map.close()

    def test_full_pivot_table(self):
        db_map = DatabaseMapping("sqlite://", create=True)
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
        object_class_mapping = EntityClassMapping(0)
        parameter_definition_mapping = ParameterDefinitionMapping(1)
        alternative_mapping = AlternativeMapping(Position.hidden)
        parameter_definition_mapping.child = alternative_mapping
        object_mapping = EntityMapping(-1)
        alternative_mapping.child = object_mapping
        index_mapping_1 = ParameterValueIndexMapping(2)
        index_mapping_2 = ParameterValueIndexMapping(3)
        value_mapping = ExpandedParameterValueMapping(-2)
        index_mapping_2.child = value_mapping
        index_mapping_1.child = index_mapping_2
        object_mapping.child = index_mapping_1
        object_class_mapping.child = parameter_definition_mapping
        expected = [
            [None, None, None, None, "o1", "o2"],
            ["oc", "p", "A", "a", -1.1, -5.5],
            ["oc", "p", "A", "b", -2.2, -6.6],
            ["oc", "p", "B", "a", -3.3, -7.7],
            ["oc", "p", "B", "b", -4.4, -8.8],
        ]
        self.assertEqual(list(rows(object_class_mapping, db_map)), expected)
        db_map.close()

    def test_full_pivot_table_with_hidden_columns(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p"),))
        import_objects(db_map, (("oc", "o1"), ("oc", "o2")))
        import_object_parameter_values(
            db_map, (("oc", "o1", "p", Map(["A", "B"], [-1.1, -2.2])), ("oc", "o2", "p", Map(["A", "B"], [-5.5, -6.6])))
        )
        db_map.commit_session("Add test data.")
        mapping = entity_parameter_value_export(
            0, 2, Position.hidden, -1, None, None, 3, Position.hidden, 5, [Position.hidden], [4]
        )
        expected = [
            [None, None, None, None, None, "o1", "o2"],
            ["oc", None, "p", "Base", "A", -1.1, -5.5],
            ["oc", None, "p", "Base", "B", -2.2, -6.6],
        ]
        self.assertEqual(list(rows(mapping, db_map)), expected)
        db_map.close()

    def test_objects_as_pivot_header_for_indexed_values_with_alternatives(self):
        db_map = DatabaseMapping("sqlite://", create=True)
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
        mapping = entity_parameter_value_export(
            0, 2, Position.hidden, -1, None, None, 3, Position.hidden, 5, [Position.hidden], [4]
        )
        expected = [
            [None, None, None, None, None, "o1", "o2"],
            ["oc", None, "p", "Base", "A", -1.1, -5.5],
            ["oc", None, "p", "Base", "B", -2.2, -6.6],
            ["oc", None, "p", "alt", "A", -3.3, -7.7],
            ["oc", None, "p", "alt", "B", -4.4, -8.8],
        ]
        self.assertEqual(list(rows(mapping, db_map)), expected)
        db_map.close()

    def test_objects_and_indexes_as_pivot_header(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p"),))
        import_objects(db_map, (("oc", "o1"), ("oc", "o2")))
        import_object_parameter_values(
            db_map, (("oc", "o1", "p", Map(["A", "B"], [-1.1, -2.2])), ("oc", "o2", "p", Map(["A", "B"], [-3.3, -4.4])))
        )
        db_map.commit_session("Add test data.")
        mapping = entity_parameter_value_export(
            0, 2, Position.hidden, -1, None, None, 3, Position.hidden, 4, [Position.hidden], [-2]
        )
        expected = [
            [None, None, None, None, "o1", "o1", "o2", "o2"],
            [None, None, None, None, "A", "B", "A", "B"],
            ["oc", None, "p", "Base", -1.1, -2.2, -3.3, -4.4],
        ]
        self.assertEqual(list(rows(mapping, db_map)), expected)
        db_map.close()

    def test_objects_and_indexes_as_pivot_header_with_multiple_alternatives_and_parameters(self):
        db_map = DatabaseMapping("sqlite://", create=True)
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
        mapping = entity_parameter_value_export(
            0, 1, Position.hidden, -1, None, None, -2, Position.hidden, 2, [Position.hidden], [-3]
        )
        expected = [
            [None, None, "o1", "o1", "o1", "o1", "o2", "o2", "o2", "o2"],
            [None, None, "Base", "Base", "alt", "alt", "Base", "Base", "alt", "alt"],
            [None, None, "A", "B", "A", "B", "A", "B", "A", "B"],
            ["oc", "p1", -1.1, -2.2, -3.3, -4.4, -9.9, -10.1, -11.1, -12.2],
            ["oc", "p2", -5.5, -6.6, -7.7, -8.8, -13.3, -14.4, -15.5, -16.6],
        ]
        self.assertEqual(list(rows(mapping, db_map)), expected)
        db_map.close()

    def test_empty_column_while_pivoted_handled_gracefully(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_alternatives(db_map, ("alt",))
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p"),))
        import_objects(db_map, (("oc", "o"),))
        db_map.commit_session("Add test data.")
        mapping = EntityClassMapping(0)
        definition = ParameterDefinitionMapping(1)
        value_list = ParameterValueListMapping(2)
        object_ = EntityMapping(-1)
        value_list.child = object_
        definition.child = value_list
        mapping.child = definition
        self.assertEqual(list(rows(mapping, db_map)), [])
        db_map.close()

    def test_object_classes_as_header_row_and_objects_in_columns(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        import_objects(
            db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21"), ("oc3", "o31"), ("oc3", "o32"), ("oc3", "o33"))
        )
        db_map.commit_session("Add test data.")
        object_class_mapping = EntityClassMapping(-1)
        object_class_mapping.child = EntityMapping(0)
        self.assertEqual(
            list(rows(object_class_mapping, db_map)),
            [["oc1", "oc2", "oc3"], ["o11", "o21", "o31"], ["o12", None, "o32"], [None, None, "o33"]],
        )
        db_map.close()

    def test_object_classes_as_table_names(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        import_objects(
            db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21"), ("oc3", "o31"), ("oc3", "o32"), ("oc3", "o33"))
        )
        db_map.commit_session("Add test data.")
        object_class_mapping = EntityClassMapping(Position.table_name)
        object_class_mapping.child = EntityMapping(0)
        tables = dict()
        for title, title_key in titles(object_class_mapping, db_map):
            tables[title] = list(rows(object_class_mapping, db_map, title_key))
        self.assertEqual(tables, {"oc1": [["o11"], ["o12"]], "oc2": [["o21"]], "oc3": [["o31"], ["o32"], ["o33"]]})
        db_map.close()

    def test_object_class_and_parameter_definition_as_table_name(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        import_object_parameters(db_map, (("oc1", "p11"), ("oc2", "p21"), ("oc2", "p22")))
        import_objects(db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21"), ("oc3", "o31")))
        db_map.commit_session("Add test data.")
        object_class_mapping = EntityClassMapping(Position.table_name)
        definition_mapping = ParameterDefinitionMapping(Position.table_name)
        object_mapping = EntityMapping(0)
        object_class_mapping.child = definition_mapping
        definition_mapping.child = object_mapping
        tables = dict()
        for title, title_key in titles(object_class_mapping, db_map):
            tables[title] = list(rows(object_class_mapping, db_map, title_key))
        self.assertEqual(
            tables, {"oc1,p11": [["o11"], ["o12"]], "oc2,p21": [["o21"]], "oc2,p22": [["o21"]], "oc3": [["o31"]]}
        )
        db_map.close()

    def test_object_relationship_name_as_table_name(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2"))
        import_objects(db_map, (("oc1", "o1"), ("oc1", "o2"), ("oc2", "O")))
        import_relationship_classes(db_map, (("rc", ("oc1", "oc2")),))
        import_relationships(db_map, (("rc", ("o1", "O")), ("rc", ("o2", "O"))))
        db_map.commit_session("Add test data.")
        mappings = entity_export(0, Position.table_name, [1, 2], [Position.table_name, 3])
        tables = dict()
        for title, title_key in titles(mappings, db_map):
            tables[title] = list(rows(mappings, db_map, title_key))
        self.assertEqual(tables, {"o1__O,o1": [["rc", "oc1", "oc2", "O"]], "o2__O,o2": [["rc", "oc1", "oc2", "O"]]})
        db_map.close()

    def test_parameter_definitions_with_value_lists(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_parameter_value_lists(db_map, (("vl1", -1.0), ("vl2", -2.0)))
        import_object_parameters(db_map, (("oc", "p1", None, "vl1"), ("oc", "p2")))
        db_map.commit_session("Add test data.")
        class_mapping = EntityClassMapping(0)
        definition_mapping = ParameterDefinitionMapping(1)
        value_list_mapping = ParameterValueListMapping(2)
        definition_mapping.child = value_list_mapping
        class_mapping.child = definition_mapping
        tables = dict()
        for title, title_key in titles(class_mapping, db_map):
            tables[title] = list(rows(class_mapping, db_map, title_key))
        self.assertEqual(tables, {None: [["oc", "p1", "vl1"]]})
        db_map.close()

    def test_parameter_definitions_and_values_and_value_lists(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_parameter_value_lists(db_map, (("vl", -1.0),))
        import_object_parameters(db_map, (("oc", "p1", None, "vl"), ("oc", "p2")))
        import_objects(db_map, (("oc", "o"),))
        import_object_parameter_values(db_map, (("oc", "o", "p1", -1.0), ("oc", "o", "p2", 5.0)))
        db_map.commit_session("Add test data.")
        flattened = [
            EntityClassMapping(0),
            ParameterDefinitionMapping(1),
            AlternativeMapping(Position.hidden),
            ParameterValueListMapping(2),
            EntityMapping(3),
            ParameterValueMapping(4),
        ]
        mapping = unflatten(flattened)
        tables = dict()
        for title, title_key in titles(mapping, db_map):
            tables[title] = list(rows(mapping, db_map, title_key))
        self.assertEqual(tables, {None: [["oc", "p1", "vl", "o", -1.0]]})
        db_map.close()

    def test_parameter_definitions_and_values_and_ignorable_value_lists(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_parameter_value_lists(db_map, (("vl", -1.0),))
        import_object_parameters(db_map, (("oc", "p1", None, "vl"), ("oc", "p2")))
        import_objects(db_map, (("oc", "o"),))
        import_object_parameter_values(db_map, (("oc", "o", "p1", -1.0), ("oc", "o", "p2", 5.0)))
        db_map.commit_session("Add test data.")
        value_list_mapping = ParameterValueListMapping(2)
        value_list_mapping.set_ignorable(True)
        flattened = [
            EntityClassMapping(0),
            ParameterDefinitionMapping(1),
            AlternativeMapping(Position.hidden),
            value_list_mapping,
            EntityMapping(3),
            ParameterValueMapping(4),
        ]
        mapping = unflatten(flattened)
        tables = dict()
        for title, title_key in titles(mapping, db_map):
            tables[title] = list(rows(mapping, db_map, title_key))
        self.assertEqual(tables, {None: [["oc", "p1", "vl", "o", -1.0], ["oc", "p2", None, "o", 5.0]]})
        db_map.close()

    def test_parameter_value_lists(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_parameter_value_lists(db_map, (("vl1", -1.0), ("vl2", -2.0)))
        db_map.commit_session("Add test data.")
        value_list_mapping = ParameterValueListMapping(0)
        tables = dict()
        for title, title_key in titles(value_list_mapping, db_map):
            tables[title] = list(rows(value_list_mapping, db_map, title_key))
        self.assertEqual(tables, {None: [["vl1"], ["vl2"]]})
        db_map.close()

    def test_parameter_value_list_values(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_parameter_value_lists(db_map, (("vl1", -1.0), ("vl2", -2.0)))
        db_map.commit_session("Add test data.")
        value_list_mapping = ParameterValueListMapping(Position.table_name)
        value_mapping = ParameterValueListValueMapping(0)
        value_list_mapping.child = value_mapping
        tables = dict()
        for title, title_key in titles(value_list_mapping, db_map):
            tables[title] = list(rows(value_list_mapping, db_map, title_key))
        self.assertEqual(tables, {"vl1": [[-1.0]], "vl2": [[-2.0]]})
        db_map.close()

    def test_no_item_declared_as_title_gives_full_table(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        import_object_parameters(db_map, (("oc1", "p11"), ("oc2", "p21"), ("oc2", "p22")))
        import_objects(db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21"), ("oc3", "o31")))
        db_map.commit_session("Add test data.")
        object_class_mapping = EntityClassMapping(Position.hidden)
        definition_mapping = ParameterDefinitionMapping(Position.hidden)
        object_mapping = EntityMapping(0)
        object_class_mapping.child = definition_mapping
        definition_mapping.child = object_mapping
        tables = dict()
        for title, title_key in titles(object_class_mapping, db_map):
            tables[title] = list(rows(object_class_mapping, db_map, title_key))
        self.assertEqual(tables, {None: [["o11"], ["o12"], ["o21"], ["o21"]]})
        db_map.close()

    def test_missing_values_for_alternatives(self):
        db_map = DatabaseMapping("sqlite://", create=True)
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
        object_class_mapping = EntityClassMapping(0)
        definition_mapping = ParameterDefinitionMapping(2)
        object_mapping = EntityMapping(1)
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
        db_map.close()

    def test_export_relationship_classes(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        import_relationship_classes(
            db_map, (("rc1", ("oc1",)), ("rc2", ("oc3", "oc2")), ("rc3", ("oc2", "oc3", "oc1")))
        )
        db_map.commit_session("Add test data.")
        relationship_class_mapping = EntityClassMapping(0)
        dimension_mapping = relationship_class_mapping.child = DimensionMapping(1)
        dimension_mapping.child = DimensionMapping(2)
        self.assertEqual(
            list(rows(relationship_class_mapping, db_map)),
            [["rc1", "oc1", ""], ["rc2", "oc3", "oc2"], ["rc3", "oc2", "oc3"]],
        )
        db_map.close()

    def test_export_relationships(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2"))
        import_objects(db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21")))
        import_relationship_classes(db_map, (("rc1", ("oc1",)), ("rc2", ("oc2", "oc1"))))
        import_relationships(db_map, (("rc1", ("o11",)), ("rc2", ("o21", "o11")), ("rc2", ("o21", "o12"))))
        db_map.commit_session("Add test data.")
        relationship_class_mapping = EntityClassMapping(0)
        dimension1_mapping = relationship_class_mapping.child = DimensionMapping(1)
        dimension2_mapping = dimension1_mapping.child = DimensionMapping(2)
        relationship_mapping = dimension2_mapping.child = EntityMapping(3)
        element1_mapping = relationship_mapping.child = ElementMapping(4)
        element1_mapping.child = ElementMapping(5)
        expected = [
            ["rc1", "oc1", "", "o11__", "o11", ""],
            ["rc2", "oc2", "oc1", "o21__o11", "o21", "o11"],
            ["rc2", "oc2", "oc1", "o21__o12", "o21", "o12"],
        ]
        self.assertEqual(list(rows(relationship_class_mapping, db_map)), expected)
        db_map.close()

    def test_relationships_with_different_dimensions(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2"))
        import_objects(db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21"), ("oc2", "o22")))
        import_relationship_classes(db_map, (("rc1D", ("oc1",)), ("rc2D", ("oc1", "oc2"))))
        import_relationships(db_map, (("rc1D", ("o11",)), ("rc1D", ("o12",))))
        import_relationships(
            db_map,
            (("rc2D", ("o11", "o21")), ("rc2D", ("o11", "o22")), ("rc2D", ("o12", "o21")), ("rc2D", ("o12", "o22"))),
        )
        db_map.commit_session("Add test data.")
        relationship_class_mapping = EntityClassMapping(0)
        object_class_mapping1 = DimensionMapping(1)
        object_class_mapping2 = DimensionMapping(2)
        relationship_mapping = EntityMapping(Position.hidden)
        object_mapping1 = ElementMapping(3)
        object_mapping2 = ElementMapping(4)
        object_mapping1.child = object_mapping2
        relationship_mapping.child = object_mapping1
        object_class_mapping2.child = relationship_mapping
        object_class_mapping1.child = object_class_mapping2
        relationship_class_mapping.child = object_class_mapping1
        tables = dict()
        for title, title_key in titles(relationship_class_mapping, db_map):
            tables[title] = list(rows(relationship_class_mapping, db_map, title_key))
        expected = [
            ["rc1D", "oc1", "", "o11", ""],
            ["rc1D", "oc1", "", "o12", ""],
            ["rc2D", "oc1", "oc2", "o11", "o21"],
            ["rc2D", "oc1", "oc2", "o11", "o22"],
            ["rc2D", "oc1", "oc2", "o12", "o21"],
            ["rc2D", "oc1", "oc2", "o12", "o22"],
        ]
        self.assertEqual(tables[None], expected)
        db_map.close()

    def test_default_parameter_values(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        import_object_parameters(db_map, (("oc1", "p11", 3.14), ("oc2", "p21", 14.3), ("oc2", "p22", -1.0)))
        db_map.commit_session("Add test data.")
        object_class_mapping = EntityClassMapping(0)
        definition_mapping = ParameterDefinitionMapping(1)
        default_value_mapping = ParameterDefaultValueMapping(2)
        definition_mapping.child = default_value_mapping
        object_class_mapping.child = definition_mapping
        table = list(rows(object_class_mapping, db_map))
        self.assertEqual(table, [["oc1", "p11", 3.14], ["oc2", "p21", 14.3], ["oc2", "p22", -1.0]])
        db_map.close()

    def test_indexed_default_parameter_values(self):
        db_map = DatabaseMapping("sqlite://", create=True)
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
        object_class_mapping = EntityClassMapping(0)
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
        db_map.close()

    def test_replace_parameter_indexes_by_external_data(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p1"),))
        import_objects(db_map, (("oc", "o1"), ("oc", "o2")))
        import_object_parameter_values(
            db_map, (("oc", "o1", "p1", Map(["a", "b"], [5.0, -5.0])), ("oc", "o2", "p1", Map(["a", "b"], [2.0, -2.0])))
        )
        db_map.commit_session("Add test data.")
        object_class_mapping = EntityClassMapping(0)
        parameter_definition_mapping = ParameterDefinitionMapping(2)
        alternative_mapping = AlternativeMapping(Position.hidden)
        parameter_definition_mapping.child = alternative_mapping
        object_mapping = EntityMapping(1)
        alternative_mapping.child = object_mapping
        index_mapping = ParameterValueIndexMapping(3)
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
        db_map.close()

    def test_constant_mapping_as_title(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        db_map.commit_session("Add test data.")
        constant_mapping = FixedValueMapping(Position.table_name, "title_text")
        object_class_mapping = EntityClassMapping(0)
        constant_mapping.child = object_class_mapping
        tables = dict()
        for title, title_key in titles(constant_mapping, db_map):
            tables[title] = list(rows(constant_mapping, db_map, title_key))
        self.assertEqual(tables, {"title_text": [["oc1"], ["oc2"], ["oc3"]]})
        db_map.close()

    def test_scenario_mapping(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_scenarios(db_map, ("s1", "s2"))
        db_map.commit_session("Add test data.")
        scenario_mapping = ScenarioMapping(0)
        tables = dict()
        for title, title_key in titles(scenario_mapping, db_map):
            tables[title] = list(rows(scenario_mapping, db_map, title_key))
        self.assertEqual(tables, {None: [["s1"], ["s2"]]})
        db_map.close()

    def test_scenario_active_flag_mapping(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_scenarios(db_map, (("s1", True), ("s2", False)))
        db_map.commit_session("Add test data.")
        scenario_mapping = ScenarioMapping(0)
        active_flag_mapping = ScenarioActiveFlagMapping(1)
        scenario_mapping.child = active_flag_mapping
        tables = dict()
        for title, title_key in titles(scenario_mapping, db_map):
            tables[title] = list(rows(scenario_mapping, db_map, title_key))
        self.assertEqual(tables, {None: [["s1", True], ["s2", False]]})
        db_map.close()

    def test_scenario_alternative_mapping(self):
        db_map = DatabaseMapping("sqlite://", create=True)
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
        self.assertEqual(tables, {None: [["s1", "a1"], ["s1", "a2"], ["s2", "a2"], ["s2", "a3"]]})
        db_map.close()

    def test_header(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_objects(db_map, (("oc", "o1"),))
        db_map.commit_session("Add test data.")
        root = unflatten([EntityClassMapping(0, header="class"), EntityMapping(1, header="entity")])
        expected = [["class", "entity"], ["oc", "o1"]]
        self.assertEqual(list(rows(root, db_map)), expected)
        db_map.close()

    def test_header_without_data_still_creates_header(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        root = unflatten([EntityClassMapping(0, header="class"), EntityMapping(1, header="object")])
        expected = [["class", "object"]]
        self.assertEqual(list(rows(root, db_map)), expected)
        db_map.close()

    def test_header_in_half_pivot_table_without_data_still_creates_header(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        root = unflatten([EntityClassMapping(-1, header="class"), EntityMapping(9, header="object")])
        expected = [["class"]]
        self.assertEqual(list(rows(root, db_map)), expected)
        db_map.close()

    def test_header_in_pivot_table_without_data_still_creates_header(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        root = unflatten(
            [
                EntityClassMapping(-1, header="class"),
                ParameterDefinitionMapping(0, header="parameter"),
                EntityMapping(-2, header="object"),
                AlternativeMapping(1, header="alternative"),
                ParameterValueMapping(0),
            ]
        )
        expected = [[None, "class"], ["parameter", "alternative"]]
        self.assertEqual(list(rows(root, db_map)), expected)
        db_map.close()

    def test_disabled_empty_data_header(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        root = unflatten([EntityClassMapping(0, header="class"), EntityMapping(1, header="object")])
        expected = []
        self.assertEqual(list(rows(root, db_map, empty_data_header=False)), expected)
        db_map.close()

    def test_disabled_empty_data_header_in_pivot_table(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        root = unflatten([EntityClassMapping(-1, header="class"), EntityMapping(0)])
        expected = []
        self.assertEqual(list(rows(root, db_map, empty_data_header=False)), expected)
        db_map.close()

    def test_header_position(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_objects(db_map, (("oc", "o1"),))
        db_map.commit_session("Add test data.")
        root = unflatten([EntityClassMapping(Position.header), EntityMapping(0)])
        expected = [["oc"], ["o1"]]
        self.assertEqual(list(rows(root, db_map)), expected)
        db_map.close()

    def test_header_position_with_relationships(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2"))
        import_objects(db_map, (("oc1", "o11"), ("oc2", "o21")))
        import_relationship_classes(db_map, (("rc", ("oc1", "oc2")),))
        import_relationships(db_map, (("rc", ("o11", "o21")),))
        db_map.commit_session("Add test data.")
        root = unflatten(
            [
                EntityClassMapping(0),
                DimensionMapping(Position.header),
                DimensionMapping(Position.header),
                EntityMapping(1),
                ElementMapping(2),
                ElementMapping(3),
            ]
        )
        expected = [["", "", "oc1", "oc2"], ["rc", "o11__o21", "o11", "o21"]]
        self.assertEqual(list(rows(root, db_map)), expected)
        db_map.close()

    def test_header_position_with_relationships_but_no_data(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2"))
        import_relationship_classes(db_map, (("rc", ("oc1", "oc2")),))
        db_map.commit_session("Add test data.")
        root = unflatten(
            [
                EntityClassMapping(0),
                DimensionMapping(Position.header),
                DimensionMapping(Position.header),
                EntityMapping(1),
                ElementMapping(2),
                ElementMapping(3),
            ]
        )
        expected = [["", "", "oc1", "oc2"]]
        self.assertEqual(list(rows(root, db_map)), expected)
        db_map.close()

    def test_header_and_pivot(self):
        db_map = DatabaseMapping("sqlite://", create=True)
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
        mapping = unflatten(
            [
                EntityClassMapping(0, header="class"),
                ParameterDefinitionMapping(1, header="parameter"),
                EntityMapping(-1, header="object"),
                AlternativeMapping(-2, header="alternative"),
                ParameterValueIndexMapping(-3, header=""),
                ExpandedParameterValueMapping(2, header="value"),
            ]
        )
        expected = [
            [None, "object", "o1", "o1", "o1", "o1", "o2", "o2", "o2", "o2"],
            [None, "alternative", "Base", "Base", "alt", "alt", "Base", "Base", "alt", "alt"],
            ["class", "parameter", "A", "B", "A", "B", "A", "B", "A", "B"],
            ["oc", "p1", -1.1, -2.2, -3.3, -4.4, -9.9, -10.1, -11.1, -12.2],
            ["oc", "p2", -5.5, -6.6, -7.7, -8.8, -13.3, -14.4, -15.5, -16.6],
        ]
        self.assertEqual(list(rows(mapping, db_map)), expected)
        db_map.close()

    def test_pivot_without_left_hand_side_has_padding_column_for_headers(self):
        db_map = DatabaseMapping("sqlite://", create=True)
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
        mapping = unflatten(
            [
                EntityClassMapping(Position.header),
                ParameterDefinitionMapping(Position.hidden, header="parameter"),
                EntityMapping(-1),
                AlternativeMapping(-2, header="alternative"),
                ParameterValueIndexMapping(-3, header="index"),
                ExpandedParameterValueMapping(2, header="value"),
            ]
        )
        expected = [
            ["oc", "o1", "o1", "o1", "o1", "o2", "o2", "o2", "o2"],
            ["alternative", "Base", "Base", "alt", "alt", "Base", "Base", "alt", "alt"],
            ["index", "A", "B", "A", "B", "A", "B", "A", "B"],
            [None, -1.1, -2.2, -3.3, -4.4, -9.9, -10.1, -11.1, -12.2],
            [None, -5.5, -6.6, -7.7, -8.8, -13.3, -14.4, -15.5, -16.6],
        ]
        self.assertEqual(list(rows(mapping, db_map)), expected)
        db_map.close()

    def test_count_mappings(self):
        object_class_mapping = EntityClassMapping(2)
        parameter_definition_mapping = ParameterDefinitionMapping(0)
        object_mapping = EntityMapping(1)
        parameter_definition_mapping.child = object_mapping
        object_class_mapping.child = parameter_definition_mapping
        self.assertEqual(object_class_mapping.count_mappings(), 3)

    def test_flatten(self):
        object_class_mapping = EntityClassMapping(2)
        parameter_definition_mapping = ParameterDefinitionMapping(0)
        object_mapping = EntityMapping(1)
        parameter_definition_mapping.child = object_mapping
        object_class_mapping.child = parameter_definition_mapping
        mappings = object_class_mapping.flatten()
        self.assertEqual(mappings, [object_class_mapping, parameter_definition_mapping, object_mapping])

    def test_unflatten_sets_last_mappings_child_to_none(self):
        object_class_mapping = EntityClassMapping(2)
        object_mapping = EntityMapping(1)
        object_class_mapping.child = object_mapping
        mapping_list = object_class_mapping.flatten()
        root = unflatten(mapping_list[:1])
        self.assertIsNone(root.child)

    def test_has_titles(self):
        object_class_mapping = EntityClassMapping(0)
        parameter_definition_mapping = ParameterDefinitionMapping(Position.table_name)
        object_mapping = EntityMapping(1)
        parameter_definition_mapping.child = object_mapping
        object_class_mapping.child = parameter_definition_mapping
        self.assertTrue(object_class_mapping.has_titles())

    def test_drop_non_positioned_tail(self):
        object_class_mapping = EntityClassMapping(0)
        parameter_definition_mapping = ParameterDefinitionMapping(Position.hidden)
        object_mapping = EntityMapping(1)
        alternative_mapping = AlternativeMapping(Position.hidden)
        value_mapping = ParameterValueMapping(Position.hidden)
        alternative_mapping.child = value_mapping
        object_mapping.child = alternative_mapping
        parameter_definition_mapping.child = object_mapping
        object_class_mapping.child = parameter_definition_mapping
        tail_cut_mapping = drop_non_positioned_tail(object_class_mapping)
        flattened = tail_cut_mapping.flatten()
        self.assertEqual(flattened, [object_class_mapping, parameter_definition_mapping, object_mapping])

    def test_serialization(self):
        highlight_position = 5
        mappings = [
            EntityClassMapping(0, highlight_position=highlight_position),
            EntityClassMapping(Position.table_name),
            DimensionMapping(2),
            ParameterDefinitionMapping(1),
            EntityMapping(-1),
            EntityMapping(Position.hidden),
            ElementMapping(-1),
            AlternativeMapping(3),
            ParameterValueMapping(4),
            ParameterValueIndexMapping(5),
            ParameterValueTypeMapping(6),
            ExpandedParameterValueMapping(7),
            FixedValueMapping(8, "gaga"),
        ]
        expected_positions = [m.position for m in mappings]
        expected_types = [type(m) for m in mappings]
        root = unflatten(mappings)
        serialized = to_dict(root)
        deserialized = from_dict(serialized).flatten()
        self.assertEqual([type(m) for m in deserialized], expected_types)
        self.assertEqual([m.position for m in deserialized], expected_positions)
        for m in deserialized:
            if isinstance(m, EntityClassMapping) and m.highlight_position is not None:
                self.assertEqual(m.highlight_position, highlight_position)
                break
        else:
            self.fail("no highlight_position in deserialized mappings")
        for m in deserialized:
            if isinstance(m, FixedValueMapping):
                self.assertEqual(m.value, "gaga")
                break
        else:
            self.fail("no fixed value in deserialized mappings")

    def test_setting_ignorable_flag(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        db_map.commit_session("Add test data.")
        object_mapping = EntityMapping(1)
        root_mapping = unflatten([EntityClassMapping(0), object_mapping])
        object_mapping.set_ignorable(True)
        self.assertTrue(object_mapping.is_ignorable())
        expected = [["oc", None]]
        self.assertEqual(list(rows(root_mapping, db_map)), expected)
        db_map.close()

    def test_unsetting_ignorable_flag(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_objects(db_map, (("oc", "o1"),))
        db_map.commit_session("Add test data.")
        object_mapping = EntityMapping(1)
        root_mapping = unflatten([EntityClassMapping(0), object_mapping])
        object_mapping.set_ignorable(True)
        self.assertTrue(object_mapping.is_ignorable())
        object_mapping.set_ignorable(False)
        self.assertFalse(object_mapping.is_ignorable())
        expected = [["oc", "o1"]]
        self.assertEqual(list(rows(root_mapping, db_map)), expected)
        db_map.close()

    def test_filter(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_objects(db_map, (("oc", "o1"), ("oc", "o2")))
        db_map.commit_session("Add test data.")
        object_mapping = EntityMapping(1)
        object_mapping.filter_re = "o1"
        root_mapping = unflatten([EntityClassMapping(0), object_mapping])
        expected = [["oc", "o1"]]
        self.assertEqual(list(rows(root_mapping, db_map)), expected)
        db_map.close()

    def test_hidden_tail_filter(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2"))
        import_objects(db_map, (("oc1", "o1"), ("oc2", "o2")))
        db_map.commit_session("Add test data.")
        object_mapping = EntityMapping(Position.hidden)
        object_mapping.filter_re = "o1"
        root_mapping = unflatten([EntityClassMapping(0), object_mapping])
        expected = [["oc1"]]
        self.assertEqual(list(rows(root_mapping, db_map)), expected)
        db_map.close()

    def test_index_names(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p"),))
        import_objects(db_map, (("oc", "o"),))
        import_object_parameter_values(db_map, (("oc", "o", "p", Map(["a"], [5.0], index_name="index")),))
        db_map.commit_session("Add test data.")
        mapping = entity_parameter_value_export(
            0, 2, Position.hidden, 1, None, None, 3, Position.hidden, 5, [Position.header], [4]
        )
        expected = [["", "", "", "", "index", ""], ["oc", "o", "p", "Base", "a", 5.0]]
        self.assertEqual(list(rows(mapping, db_map)), expected)
        db_map.close()

    def test_default_value_index_names_with_nested_map(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(
            db_map, (("oc", "p", Map(["A"], [Map(["b"], [2.3], index_name="idx2")], index_name="idx1")),)
        )
        db_map.commit_session("Add test data.")
        mapping = entity_parameter_default_value_export(
            0, 1, Position.hidden, 4, [Position.header, Position.header], [2, 3]
        )
        expected = [["", "", "idx1", "idx2", ""], ["oc", "p", "A", "b", 2.3]]
        self.assertEqual(list(rows(mapping, db_map)), expected)
        db_map.close()

    def test_multiple_index_names_with_empty_database(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        mapping = entity_parameter_value_export(
            0, 4, Position.hidden, 1, [2], [3], 5, Position.hidden, 8, [Position.header, Position.header], [6, 7]
        )
        expected = [9 * [""]]
        self.assertEqual(list(rows(mapping, db_map)), expected)
        db_map.close()

    def test_parameter_default_value_type(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        import_object_parameters(db_map, (("oc1", "p11", 3.14), ("oc2", "p21", 14.3), ("oc2", "p22", -1.0)))
        db_map.commit_session("Add test data.")
        root_mapping = entity_parameter_default_value_export(0, 1, 2, 3, None, None)
        expected = [
            ["oc1", "p11", "single_value", 3.14],
            ["oc2", "p21", "single_value", 14.3],
            ["oc2", "p22", "single_value", -1.0],
        ]
        self.assertEqual(list(rows(root_mapping, db_map)), expected)
        db_map.close()

    def test_map_with_more_dimensions_than_index_mappings(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p"),))
        import_objects(db_map, (("oc", "o"),))
        import_object_parameter_values(db_map, (("oc", "o", "p", Map(["A"], [Map(["b"], [2.3])])),))
        db_map.commit_session("Add test data.")
        mapping = entity_parameter_value_export(
            0, 1, Position.hidden, 2, None, None, Position.hidden, Position.hidden, 4, [Position.hidden], [3]
        )
        expected = [["oc", "p", "o", "A", "map"]]
        self.assertEqual(list(rows(mapping, db_map)), expected)
        db_map.close()

    def test_default_map_value_with_more_dimensions_than_index_mappings(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p", Map(["A"], [Map(["b"], [2.3])])),))
        db_map.commit_session("Add test data.")
        mapping = entity_parameter_default_value_export(0, 1, Position.hidden, 3, [Position.hidden], [2])
        expected = [["oc", "p", "A", "map"]]
        self.assertEqual(list(rows(mapping, db_map)), expected)
        db_map.close()

    def test_map_with_single_value_mapping(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p"),))
        import_objects(db_map, (("oc", "o"),))
        import_object_parameter_values(db_map, (("oc", "o", "p", Map(["A"], [2.3])),))
        db_map.commit_session("Add test data.")
        mapping = entity_parameter_value_export(
            0, 1, Position.hidden, 2, None, None, Position.hidden, Position.hidden, 3, None, None
        )
        expected = [["oc", "p", "o", "map"]]
        self.assertEqual(list(rows(mapping, db_map)), expected)
        db_map.close()

    def test_default_map_value_with_single_value_mapping(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p", Map(["A"], [2.3])),))
        db_map.commit_session("Add test data.")
        mapping = entity_parameter_default_value_export(0, 1, Position.hidden, 2, None, None)
        expected = [["oc", "p", "map"]]
        self.assertEqual(list(rows(mapping, db_map)), expected)
        db_map.close()

    def test_table_gets_exported_even_without_parameter_values(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p"),))
        db_map.commit_session("Add test data.")
        mapping = entity_parameter_value_export(
            Position.header, Position.table_name, entity_position=0, value_position=1
        )
        tables = dict()
        for title, title_key in titles(mapping, db_map):
            tables[title] = list(rows(mapping, db_map, title_key))
        expected = {"p": [["oc", ""]]}
        self.assertEqual(tables, expected)
        db_map.close()

    def test_relationship_class_object_classes_parameters(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p"),))
        import_relationship_classes(db_map, (("rc", ("oc",)),))
        db_map.commit_session("Add test data")
        root_mapping = unflatten(
            [
                EntityClassMapping(0, highlight_position=0),
                DimensionMapping(1),
                ParameterDefinitionMapping(2),
            ]
        )
        expected = [["rc", "oc", "p"]]
        self.assertEqual(list(rows(root_mapping, db_map)), expected)
        db_map.close()

    def test_relationship_class_object_classes_parameters_multiple_dimensions(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2"))
        import_object_parameters(db_map, (("oc1", "p11"), ("oc1", "p12"), ("oc2", "p21")))
        import_relationship_classes(db_map, (("rc", ("oc1", "oc2")),))
        db_map.commit_session("Add test data")
        root_mapping = unflatten(
            [
                EntityClassMapping(0, highlight_position=0),
                DimensionMapping(1),
                DimensionMapping(3),
                ParameterDefinitionMapping(2),
            ]
        )
        expected = [["rc", "oc1", "p11", "oc2"], ["rc", "oc1", "p12", "oc2"]]
        self.assertEqual(list(rows(root_mapping, db_map)), expected)
        db_map.close()

    def test_highlight_relationship_objects(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        import_objects(
            db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21"), ("oc2", "o22"), ("oc3", "o31"), ("oc3", "o32"))
        )
        import_relationship_classes(db_map, (("rc", ("oc1", "oc2")),))
        import_relationships(db_map, (("rc", ("o11", "o21")), ("rc", ("o12", "o22"))))
        db_map.commit_session("Add test data")
        root_mapping = unflatten(
            [
                EntityClassMapping(0, highlight_position=0),
                DimensionMapping(1),
                DimensionMapping(2),
                EntityMapping(3),
                ElementMapping(4),
                ElementMapping(5),
            ]
        )
        expected = [
            ["rc", "oc1", "oc2", "o11__o21", "o11", "o21"],
            ["rc", "oc1", "oc2", "o12__o22", "o12", "o22"],
        ]
        self.assertEqual(list(rows(root_mapping, db_map)), expected)
        db_map.close()

    def test_export_object_parameters_while_exporting_relationships(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_object_classes(db_map, ("oc",))
            import_object_parameters(db_map, (("oc", "p"),))
            import_objects(db_map, (("oc", "o"),))
            import_object_parameter_values(db_map, (("oc", "o", "p", 23.0),))
            import_relationship_classes(db_map, (("rc", ("oc",)),))
            import_relationships(db_map, (("rc", ("o",)),))
            db_map.commit_session("Add test data")
            root_mapping = unflatten(
                [
                    EntityClassMapping(0, highlight_position=0),
                    DimensionMapping(1),
                    EntityMapping(2),
                    ElementMapping(3),
                    ParameterDefinitionMapping(4),
                    AlternativeMapping(5),
                    ParameterValueMapping(6),
                ]
            )
            expected = [["rc", "oc", "o__", "o", "p", "Base", 23.0]]
            self.assertEqual(list(rows(root_mapping, db_map)), expected)

    def test_export_default_values_of_object_parameters_while_exporting_relationships(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p", 23.0),))
        import_objects(db_map, (("oc", "o"),))
        import_relationship_classes(db_map, (("rc", ("oc",)),))
        import_relationships(db_map, (("rc", ("o",)),))
        db_map.commit_session("Add test data")
        root_mapping = unflatten(
            [
                EntityClassMapping(0, highlight_position=0),
                DimensionMapping(1),
                ParameterDefinitionMapping(2),
                ParameterDefaultValueMapping(3),
            ]
        )
        expected = [["rc", "oc", "p", 23.0]]
        self.assertEqual(list(rows(root_mapping, db_map)), expected)
        db_map.close()

    def test_export_object_parameters_while_exporting_relationships_with_multiple_parameters_and_classes2(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2", "oc3"))
        import_object_parameters(db_map, (("oc1", "p11"), ("oc1", "p12"), ("oc2", "p21"), ("oc3", "p31")))
        import_objects(db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21"), ("oc2", "o22"), ("oc3", "o31")))
        import_object_parameter_values(db_map, (("oc1", "o11", "p11", 1.1),))
        import_object_parameter_values(db_map, (("oc1", "o11", "p12", 2.2),))
        import_object_parameter_values(db_map, (("oc1", "o12", "p11", 3.3),))
        import_object_parameter_values(db_map, (("oc1", "o12", "p12", 4.4),))
        import_object_parameter_values(db_map, (("oc2", "o21", "p21", 5.5),))
        import_object_parameter_values(db_map, (("oc2", "o22", "p21", 6.6),))
        import_object_parameter_values(db_map, (("oc3", "o31", "p31", 7.7),))
        import_relationship_classes(db_map, (("rc12", ("oc1", "oc2")),))
        import_relationship_classes(db_map, (("rc23", ("oc2", "oc3")),))
        import_relationships(db_map, (("rc12", ("o11", "o21")),))
        import_relationships(db_map, (("rc12", ("o12", "o21")),))
        import_relationships(db_map, (("rc23", ("o21", "o31")),))
        db_map.commit_session("Add test data")
        root_mapping = unflatten(
            [
                EntityClassMapping(0, highlight_position=1),
                DimensionMapping(1),
                DimensionMapping(2),
                EntityMapping(3),
                ElementMapping(4),
                ElementMapping(5),
                ParameterDefinitionMapping(6),
                AlternativeMapping(7),
                ParameterValueMapping(8),
            ]
        )
        expected = [
            ["rc12", "oc1", "oc2", "o11__o21", "o11", "o21", "p21", "Base", 5.5],
            ["rc12", "oc1", "oc2", "o12__o21", "o12", "o21", "p21", "Base", 5.5],
            ["rc23", "oc2", "oc3", "o21__o31", "o21", "o31", "p31", "Base", 7.7],
        ]
        self.assertEqual(list(rows(root_mapping, db_map)), expected)
        db_map.close()

    def test_alternative_mapping_with_header_and_description(self):
        root_mapping = AlternativeMapping(0, header="alternative")
        root_mapping.child = AlternativeDescriptionMapping(1, header="description")
        with DatabaseMapping("sqlite://", create=True) as db_map:
            expected = [["alternative", "description"], ["Base", "Base alternative"]]
            self.assertEqual(list(rows(root_mapping, db_map)), expected)

    def test_fixed_value_and_alternative_mappings_with_header_and_description(self):
        root_mapping = FixedValueMapping(Position.table_name, value="Alternative")
        alternative_mapping = root_mapping.child = AlternativeMapping(0, header="alternative")
        alternative_mapping.child = AlternativeDescriptionMapping(1, header="description")
        with DatabaseMapping("sqlite://", create=True) as db_map:
            expected = [["alternative", "description"], ["Base", "Base alternative"]]
            self.assertEqual(list(rows(root_mapping, db_map)), expected)

    def test_fixed_value_and_scenario_mappings_with_header_and_description(self):
        root_mapping = FixedValueMapping(Position.table_name, value="Scenario")
        scenario_mapping = root_mapping.child = ScenarioMapping(0, header="scenario")
        scenario_mapping.child = ScenarioDescriptionMapping(1, header="description")
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_scenarios(db_map, (("scenario1", False, "Scenario with Base alternative"),))
            db_map.commit_session("Add test data.")
            expected = [["scenario", "description"], ["scenario1", "Scenario with Base alternative"]]
            self.assertEqual(list(rows(root_mapping, db_map)), expected)

    def test_rows_from_scenario_mappings_after_rows_from_alternative_mappings(self):
        root_mapping1 = FixedValueMapping(Position.table_name, value="Alternative")
        alternative_mapping = root_mapping1.child = AlternativeMapping(0, header="alternative")
        alternative_mapping.child = AlternativeDescriptionMapping(1, header="description")
        root_mapping2 = FixedValueMapping(Position.table_name, value="Scenario")
        scenario_mapping = root_mapping2.child = ScenarioMapping(0, header="scenario")
        scenario_mapping.child = ScenarioDescriptionMapping(1, header="description")
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_scenarios(db_map, (("scenario1", False, "Scenario with Base alternative"),))
            db_map.commit_session("Add test data.")
            expected1 = [["alternative", "description"], ["Base", "Base alternative"]]
            self.assertEqual(list(rows(root_mapping1, db_map)), expected1)
            expected2 = [["scenario", "description"], ["scenario1", "Scenario with Base alternative"]]
            self.assertEqual(list(rows(root_mapping2, db_map)), expected2)


if __name__ == "__main__":
    unittest.main()
