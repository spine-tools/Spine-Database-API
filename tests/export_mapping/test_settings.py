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
Unit tests for export settings.

"""

import unittest
import numpy
from spinedb_api import (
    DatabaseMapping,
    import_object_classes,
    import_object_parameters,
    import_object_parameter_values,
    import_objects,
    import_relationship_classes,
    import_relationship_parameter_values,
    import_relationship_parameters,
    import_relationships,
    TimeSeriesFixedResolution,
)
from spinedb_api.export_mapping import rows
from spinedb_api.export_mapping.settings import (
    entity_export,
    set_entity_dimensions,
    entity_parameter_value_export,
    set_parameter_dimensions,
    set_parameter_default_value_dimensions,
    entity_parameter_default_value_export,
    entity_dimension_parameter_default_value_export,
    entity_dimension_parameter_value_export,
)
from spinedb_api.export_mapping.export_mapping import (
    Position,
    EntityClassMapping,
    DimensionMapping,
    EntityMapping,
    ElementMapping,
    ExpandedParameterValueMapping,
    ParameterValueIndexMapping,
    IndexNameMapping,
    ParameterValueTypeMapping,
    ParameterValueMapping,
    ExpandedParameterDefaultValueMapping,
    ParameterDefaultValueIndexMapping,
    DefaultValueIndexNameMapping,
    ParameterDefaultValueTypeMapping,
    ParameterDefaultValueMapping,
)


class TestEntityParameterExport(unittest.TestCase):
    def test_export_with_parameter_values(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", "oc2"))
        import_objects(db_map, (("oc1", "o1"), ("oc2", "o2"), ("oc2", "o3")))
        import_relationship_classes(db_map, (("rc", ("oc1", "oc2")),))
        import_relationship_parameters(db_map, (("rc", "p"),))
        import_relationships(db_map, (("rc", ("o1", "o2")), ("rc", ("o1", "o3"))))
        import_relationship_parameter_values(
            db_map,
            (
                (
                    "rc",
                    ("o1", "o2"),
                    "p",
                    TimeSeriesFixedResolution("2022-06-22T11:00", "1h", [-1.1, -2.2], False, False),
                ),
                (
                    "rc",
                    ("o1", "o3"),
                    "p",
                    TimeSeriesFixedResolution("2022-06-22T11:00", "1h", [-3.3, -4.4], False, False),
                ),
            ),
        )
        db_map.commit_session("Add test data.")
        root_mapping = entity_parameter_value_export(
            element_positions=[-1, -2], value_position=-3, index_name_positions=[Position.hidden], index_positions=[0]
        )
        expected = [
            [None, "o1", "o1"],
            [None, "o2", "o3"],
            [numpy.datetime64("2022-06-22T11:00:00"), -1.1, -3.3],
            [numpy.datetime64("2022-06-22T12:00:00"), -2.2, -4.4],
        ]
        self.assertEqual(list(rows(root_mapping, db_map)), expected)
        db_map.close()


class TestEntityClassDimensionParameterDefaultValueExport(unittest.TestCase):
    def setUp(self):
        self._db_map = DatabaseMapping("sqlite://", create=True)

    def tearDown(self):
        self._db_map.close()

    def test_export_with_two_dimensions(self):
        import_object_classes(self._db_map, ("oc1", "oc2"))
        import_object_parameters(
            self._db_map, (("oc1", "p11", 2.3), ("oc1", "p12", 5.0), ("oc2", "p21", "shouldn't show"))
        )
        import_relationship_classes(self._db_map, (("rc", ("oc1", "oc2")),))
        import_relationship_parameters(self._db_map, (("rc", "rc_p", "dummy"),))
        self._db_map.commit_session("Add test data.")
        root_mapping = entity_dimension_parameter_default_value_export(
            entity_class_position=0,
            definition_position=1,
            dimension_positions=[2, 3],
            value_position=4,
            value_type_position=5,
            index_name_positions=None,
            index_positions=None,
            highlight_position=0,
        )
        expected = [["rc", "p11", "oc1", "oc2", 2.3, "single_value"], ["rc", "p12", "oc1", "oc2", 5.0, "single_value"]]
        self.assertEqual(list(rows(root_mapping, self._db_map)), expected)


class TestEntityElementParameterExport(unittest.TestCase):
    def setUp(self):
        self._db_map = DatabaseMapping("sqlite://", create=True)

    def tearDown(self):
        self._db_map.close()

    def test_export_with_two_dimensions(self):
        import_object_classes(self._db_map, ("oc1", "oc2"))
        import_object_parameters(self._db_map, (("oc1", "p11"), ("oc1", "p12"), ("oc2", "p21")))
        import_objects(self._db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21")))
        import_object_parameter_values(
            self._db_map,
            (
                ("oc1", "o11", "p11", 2.3),
                ("oc1", "o12", "p11", -2.3),
                ("oc1", "o12", "p12", -5.0),
                ("oc2", "o21", "p21", "shouldn't show"),
            ),
        )
        import_relationship_classes(self._db_map, (("rc", ("oc1", "oc2")),))
        import_relationship_parameters(self._db_map, (("rc", "rc_p"),))
        import_relationships(self._db_map, (("rc", ("o11", "o21")), ("rc", ("o12", "o21"))))
        import_relationship_parameter_values(self._db_map, (("rc", ("o11", "o21"), "rc_p", "dummy"),))
        self._db_map.commit_session("Add test data.")
        root_mapping = entity_dimension_parameter_value_export(
            entity_class_position=0,
            definition_position=1,
            value_list_position=Position.hidden,
            entity_position=2,
            dimension_positions=[3, 4],
            element_positions=[5, 6],
            alternative_position=7,
            value_type_position=8,
            value_position=9,
            highlight_position=0,
        )
        set_entity_dimensions(root_mapping, 2)
        expected = [
            ["rc", "p11", "o11__o21", "oc1", "oc2", "o11", "o21", "Base", "single_value", 2.3],
            ["rc", "p11", "o12__o21", "oc1", "oc2", "o12", "o21", "Base", "single_value", -2.3],
            ["rc", "p12", "o12__o21", "oc1", "oc2", "o12", "o21", "Base", "single_value", -5.0],
        ]
        self.assertEqual(list(rows(root_mapping, self._db_map)), expected)


class TestSetEntityDimensions(unittest.TestCase):
    def test_change_dimensions_from_zero_to_one(self):
        mapping = entity_export(0, 1)
        self.assertEqual(mapping.count_mappings(), 2)
        set_entity_dimensions(mapping, 1)
        self.assertEqual(mapping.count_mappings(), 4)
        flattened = mapping.flatten()
        classes = [type(mapping) for mapping in flattened]
        self.assertEqual(
            classes,
            [
                EntityClassMapping,
                DimensionMapping,
                EntityMapping,
                ElementMapping,
            ],
        )
        positions = [mapping.position for mapping in flattened]
        self.assertEqual(positions, [0, Position.hidden, 1, Position.hidden])

    def test_change_dimension_from_one_to_zero(self):
        mapping = entity_export(0, 1, [2], [3])
        self.assertEqual(mapping.count_mappings(), 4)
        set_entity_dimensions(mapping, 0)
        self.assertEqual(mapping.count_mappings(), 2)
        flattened = mapping.flatten()
        classes = [type(mapping) for mapping in flattened]
        self.assertEqual(classes, [EntityClassMapping, EntityMapping])
        positions = [mapping.position for mapping in flattened]
        self.assertEqual(positions, [0, 1])

    def test_increase_dimensions(self):
        mapping = entity_export(0, 1, [2], [3])
        self.assertEqual(mapping.count_mappings(), 4)
        set_entity_dimensions(mapping, 2)
        self.assertEqual(mapping.count_mappings(), 6)
        flattened = mapping.flatten()
        classes = [type(mapping) for mapping in flattened]
        self.assertEqual(
            classes,
            [
                EntityClassMapping,
                DimensionMapping,
                DimensionMapping,
                EntityMapping,
                ElementMapping,
                ElementMapping,
            ],
        )
        positions = [mapping.position for mapping in flattened]
        self.assertEqual(positions, [0, 2, Position.hidden, 1, 3, Position.hidden])

    def test_decrease_dimensions(self):
        mapping = entity_export(0, 1, [2, 3], [4, 5])
        self.assertEqual(mapping.count_mappings(), 6)
        set_entity_dimensions(mapping, 1)
        self.assertEqual(mapping.count_mappings(), 4)
        flattened = mapping.flatten()
        classes = [type(mapping) for mapping in flattened]
        self.assertEqual(
            classes,
            [
                EntityClassMapping,
                DimensionMapping,
                EntityMapping,
                ElementMapping,
            ],
        )
        positions = [mapping.position for mapping in flattened]
        self.assertEqual(positions, [0, 2, 1, 4])


class TestSetParameterDimensions(unittest.TestCase):
    def test_set_dimensions_from_zero_to_one(self):
        root_mapping = entity_parameter_value_export()
        set_parameter_dimensions(root_mapping, 1)
        expected_types = [
            ExpandedParameterValueMapping,
            ParameterValueIndexMapping,
            IndexNameMapping,
            ParameterValueTypeMapping,
        ]
        for expected_type, mapping in zip(expected_types, reversed(root_mapping.flatten())):
            self.assertIsInstance(mapping, expected_type)

    def test_set_default_value_dimensions_from_zero_to_one(self):
        root_mapping = entity_parameter_default_value_export()
        set_parameter_default_value_dimensions(root_mapping, 1)
        expected_types = [
            ExpandedParameterDefaultValueMapping,
            ParameterDefaultValueIndexMapping,
            DefaultValueIndexNameMapping,
            ParameterDefaultValueTypeMapping,
        ]
        for expected_type, mapping in zip(expected_types, reversed(root_mapping.flatten())):
            self.assertIsInstance(mapping, expected_type)

    def test_set_dimensions_from_one_to_zero(self):
        root_mapping = entity_parameter_value_export(index_name_positions=[0], index_positions=[1])
        set_parameter_dimensions(root_mapping, 0)
        expected_types = [ParameterValueMapping, ParameterValueTypeMapping]
        for expected_type, mapping in zip(expected_types, reversed(root_mapping.flatten())):
            self.assertIsInstance(mapping, expected_type)

    def test_set_default_value_dimensions_from_one_to_zero(self):
        root_mapping = entity_parameter_default_value_export(index_name_positions=[0], index_positions=[1])
        set_parameter_default_value_dimensions(root_mapping, 0)
        expected_types = [ParameterDefaultValueMapping, ParameterDefaultValueTypeMapping]
        for expected_type, mapping in zip(expected_types, reversed(root_mapping.flatten())):
            self.assertIsInstance(mapping, expected_type)

    def test_set_dimensions_from_one_to_two(self):
        root_mapping = entity_parameter_value_export(index_name_positions=[0], index_positions=[1])
        set_parameter_dimensions(root_mapping, 2)
        expected_types = [
            ExpandedParameterValueMapping,
            ParameterValueIndexMapping,
            IndexNameMapping,
            ParameterValueIndexMapping,
            IndexNameMapping,
            ParameterValueTypeMapping,
        ]
        for expected_type, mapping in zip(expected_types, reversed(root_mapping.flatten())):
            self.assertIsInstance(mapping, expected_type)

    def test_set_default_value_dimensions_from_one_to_two(self):
        root_mapping = entity_parameter_default_value_export(index_name_positions=[0], index_positions=[1])
        set_parameter_default_value_dimensions(root_mapping, 2)
        expected_types = [
            ExpandedParameterDefaultValueMapping,
            ParameterDefaultValueIndexMapping,
            DefaultValueIndexNameMapping,
            ParameterDefaultValueIndexMapping,
            DefaultValueIndexNameMapping,
            ParameterDefaultValueTypeMapping,
        ]
        for expected_type, mapping in zip(expected_types, reversed(root_mapping.flatten())):
            self.assertIsInstance(mapping, expected_type)

    def test_set_dimensions_from_two_to_one(self):
        root_mapping = entity_parameter_value_export(index_name_positions=[0, 2], index_positions=[1, 3])
        set_parameter_dimensions(root_mapping, 1)
        expected_types = [
            ExpandedParameterValueMapping,
            ParameterValueIndexMapping,
            IndexNameMapping,
            ParameterValueTypeMapping,
        ]
        for expected_type, mapping in zip(expected_types, reversed(root_mapping.flatten())):
            self.assertIsInstance(mapping, expected_type)

    def test_set_default_value_dimensions_from_two_to_one(self):
        root_mapping = entity_parameter_default_value_export(index_name_positions=[0, 2], index_positions=[1, 3])
        set_parameter_default_value_dimensions(root_mapping, 1)
        expected_types = [
            ExpandedParameterDefaultValueMapping,
            ParameterDefaultValueIndexMapping,
            DefaultValueIndexNameMapping,
            ParameterDefaultValueTypeMapping,
        ]
        for expected_type, mapping in zip(expected_types, reversed(root_mapping.flatten())):
            self.assertIsInstance(mapping, expected_type)


if __name__ == "__main__":
    unittest.main()
