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
"""Unit tests for export settings."""

import unittest
import numpy
from spinedb_api import (
    DatabaseMapping,
    Map,
    TimeSeriesFixedResolution,
    import_object_classes,
    import_object_parameters,
    import_relationship_classes,
    import_relationship_parameters,
)
from spinedb_api.export_mapping import rows
from spinedb_api.export_mapping.export_mapping import (
    DefaultValueIndexNameMapping,
    DimensionMapping,
    ElementMapping,
    EntityClassDescriptionMapping,
    EntityClassMapping,
    EntityDescriptionMapping,
    EntityMapping,
    EntityMetadataNameMapping,
    EntityMetadataValueMapping,
    ExpandedParameterDefaultValueMapping,
    ExpandedParameterValueMapping,
    IndexNameMapping,
    MetadataNameMapping,
    MetadataValueMapping,
    ParameterDefaultValueIndexMapping,
    ParameterDefaultValueMapping,
    ParameterDefaultValueTypeMapping,
    ParameterValueIndexMapping,
    ParameterValueMapping,
    ParameterValueTypeMapping,
    Position,
)
from spinedb_api.export_mapping.settings import (
    entity_dimension_parameter_default_value_export,
    entity_dimension_parameter_value_export,
    entity_export,
    entity_metadata_export,
    entity_parameter_default_value_export,
    entity_parameter_value_export,
    metadata_export,
    parameter_value_metadata_export,
    set_entity_dimensions,
    set_entity_elements,
    set_parameter_default_value_dimensions,
    set_parameter_dimensions,
)
from tests.mock_helpers import AssertSuccessTestCase


class TestEntityExport:
    def test_export_class_description(self, db_map):
        db_map.add_entity_class(name="cat", description="A feline creature.")
        db_map.commit_session("Add test cat.")
        root_mapping = entity_export(entity_class_position=0, entity_class_description_position=1)
        entity_mapping = next(iter(mapping for mapping in root_mapping.flatten() if isinstance(mapping, EntityMapping)))
        entity_mapping.set_ignorable(True)
        expected = [["cat", "A feline creature."]]
        assert list(rows(root_mapping, db_map, {})) == expected

    def test_export_entity_description(self, db_map):
        db_map.add_entity_class(name="cat")
        db_map.add_entity(
            entity_class_name="cat", name="Garfield", description="A ball of fur that'll empty the fridge."
        )
        db_map.commit_session("Add test cat.")
        root_mapping = entity_export(entity_class_position=0, entity_position=1, entity_description_position=2)
        expected = [["cat", "Garfield", "A ball of fur that'll empty the fridge."]]
        assert list(rows(root_mapping, db_map, {})) == expected

    def test_export_dimensions_and_elements(self, db_map):
        db_map.add_entity_class(name="cat")
        db_map.add_entity(entity_class_name="cat", name="Garfield")
        db_map.add_entity_class(name="mouse")
        db_map.add_entity(entity_class_name="mouse", name="Mickey")
        db_map.add_entity_class(dimension_name_list=("mouse", "cat"))
        db_map.add_entity(entity_class_name="mouse__cat", entity_byname=("Mickey", "Garfield"))
        db_map.commit_session("Add test cat.")
        root_mapping = entity_export(
            entity_class_position=0, dimension_positions=[1, 2], entity_position=3, element_positions=[4, 5]
        )
        expected = [["mouse__cat", "mouse", "cat", "Mickey__Garfield", "Mickey", "Garfield"]]
        assert list(rows(root_mapping, db_map, {})) == expected


class TestEntityParameterDefaultValueExport:
    def test_export_indexed_default_value(self, db_map):
        db_map.add_entity_class(name="cat")
        db_map.add_parameter_definition(
            entity_class_name="cat", name="laziness", parsed_value=Map(["Mon", "Wed"], [2.3, 3.2], index_name="weekday")
        )
        db_map.commit_session("Add test cat.")
        root_mapping = entity_parameter_default_value_export(
            entity_class_position=0,
            definition_position=1,
            value_type_position=2,
            value_position=3,
            index_name_positions=[4],
            index_positions=[5],
        )
        expected = [
            ["cat", "laziness", "1d_map", 2.3, "weekday", numpy.str_("Mon")],
            ["cat", "laziness", "1d_map", 3.2, "weekday", numpy.str_("Wed")],
        ]
        assert list(rows(root_mapping, db_map, {})) == expected


class TestEntityParameterValueExport:
    def test_export_with_indexed_parameter_values(self, db_map):
        db_map.add_entity_class(name="oc1")
        db_map.add_entity_class(name="oc2")
        db_map.add_entity(entity_class_name="oc1", name="o1")
        db_map.add_entity(entity_class_name="oc2", name="o2")
        db_map.add_entity(entity_class_name="oc2", name="o3")
        db_map.add_entity_class(name="rc", dimension_name_list=("oc1", "oc2"))
        db_map.add_parameter_definition(entity_class_name="rc", name="p")
        db_map.add_entity(entity_class_name="rc", entity_byname=("o1", "o2"))
        db_map.add_entity(entity_class_name="rc", entity_byname=("o1", "o3"))
        db_map.add_parameter_value(
            entity_class_name="rc",
            entity_byname=("o1", "o2"),
            parameter_definition_name="p",
            alternative_name="Base",
            parsed_value=TimeSeriesFixedResolution("2022-06-22T11:00", "1h", [-1.1, -2.2], False, False),
        )
        db_map.add_parameter_value(
            entity_class_name="rc",
            entity_byname=("o1", "o3"),
            parameter_definition_name="p",
            alternative_name="Base",
            parsed_value=TimeSeriesFixedResolution("2022-06-22T11:00", "1h", [-3.3, -4.4], False, False),
        )
        db_map.commit_session("Add test data.")
        root_mapping = entity_parameter_value_export(
            element_positions=[-1, -2],
            value_position=-3,
            index_name_positions=[Position.hidden],
            index_positions=[0],
        )
        expected = [
            [None, "o1", "o1"],
            [None, "o2", "o3"],
            [numpy.datetime64("2022-06-22T11:00:00"), -1.1, -3.3],
            [numpy.datetime64("2022-06-22T12:00:00"), -2.2, -4.4],
        ]
        assert list(rows(root_mapping, db_map, {})) == expected


class TestEntityDimensionParameterDefaultValueExport(AssertSuccessTestCase):
    def test_export_with_two_dimensions(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_object_classes(db_map, ("oc1", "oc2")))
            self._assert_imports(
                import_object_parameters(
                    db_map, (("oc1", "p11", 2.3), ("oc1", "p12", 5.0), ("oc2", "p21", "shouldn't show"))
                )
            )
            self._assert_imports(import_relationship_classes(db_map, (("rc", ("oc1", "oc2")),)))
            self._assert_imports(import_relationship_parameters(db_map, (("rc", "rc_p", "dummy"),)))
            db_map.commit_session("Add test data.")
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
            expected = [["rc", "p11", "oc1", "oc2", 2.3, "float"], ["rc", "p12", "oc1", "oc2", 5.0, "float"]]
            self.assertEqual(list(rows(root_mapping, db_map, {})), expected)


class TestEntityDimensionParameterExport:
    def test_export_with_two_dimensions(self, db_map):
        db_map.add_entity_class(name="oc1")
        db_map.add_entity_class(name="oc2")
        db_map.add_parameter_definition(entity_class_name="oc1", name="p11")
        db_map.add_parameter_definition(entity_class_name="oc1", name="p12")
        db_map.add_parameter_definition(entity_class_name="oc2", name="p21")
        db_map.add_entity(entity_class_name="oc1", name="o11")
        db_map.add_entity(entity_class_name="oc1", name="o12")
        db_map.add_entity(entity_class_name="oc2", name="o21")
        db_map.add_parameter_value(
            entity_class_name="oc1",
            entity_byname=("o11",),
            parameter_definition_name="p11",
            alternative_name="Base",
            parsed_value=2.3,
        )
        db_map.add_parameter_value(
            entity_class_name="oc1",
            entity_byname=("o12",),
            parameter_definition_name="p11",
            alternative_name="Base",
            parsed_value=-2.3,
        )
        db_map.add_parameter_value(
            entity_class_name="oc1",
            entity_byname=("o12",),
            parameter_definition_name="p12",
            alternative_name="Base",
            parsed_value=-5.0,
        )
        db_map.add_parameter_value(
            entity_class_name="oc2",
            entity_byname=("o21",),
            parameter_definition_name="p21",
            alternative_name="Base",
            parsed_value="shouldn't show",
        )
        db_map.add_entity_class(name="rc", dimension_name_list=("oc1", "oc2"))
        db_map.add_parameter_definition(entity_class_name="rc", name="rc_p")
        db_map.add_entity(entity_class_name="rc", entity_byname=("o11", "o21"))
        db_map.add_entity(entity_class_name="rc", entity_byname=("o12", "o21"))
        db_map.add_parameter_value(
            entity_class_name="rc",
            entity_byname=("o11", "o21"),
            parameter_definition_name="rc_p",
            alternative_name="Base",
            parsed_value="dummy",
        )
        db_map.commit_session("Add test data.")
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
            ["rc", "p11", "o11__o21", "oc1", "oc2", "o11", "o21", "Base", "float", 2.3],
            ["rc", "p11", "o12__o21", "oc1", "oc2", "o12", "o21", "Base", "float", -2.3],
            ["rc", "p12", "o12__o21", "oc1", "oc2", "o12", "o21", "Base", "float", -5.0],
        ]
        assert list(rows(root_mapping, db_map, {})) == expected


class TestMetadataExport:
    def test_metadata_is_exported(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_metadata(name="temporal", value="lat: 2.3; lon: -23.0")
            db_map.commit_session("Add test data.")
            root_mapping = metadata_export(0, 1)
            expected = [
                ["temporal", "lat: 2.3; lon: -23.0"],
            ]
            assert list(rows(root_mapping, db_map, {})) == expected


class TestEntityMetadataExport:
    def test_metadata_is_exported(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_metadata(name="sources", value="https://example.com")
            db_map.add_metadata(name="created", value="2026-02-14")
            db_map.add_entity_class(name="Widget")
            db_map.add_entity(entity_class_name="Widget", name="of_anon_source")
            db_map.add_entity(entity_class_name="Widget", name="sourced")
            db_map.add_entity(entity_class_name="Widget", name="created")
            db_map.add_entity_metadata(
                entity_class_name="Widget",
                entity_byname=("sourced",),
                metadata_name="sources",
                metadata_value="https://example.com",
            )
            db_map.add_entity_metadata(
                entity_class_name="Widget",
                entity_byname=("created",),
                metadata_name="created",
                metadata_value="2026-02-14",
            )
            db_map.commit_session("Add test data.")
            root_mapping = entity_metadata_export(0, 1, None, 2, 3)
            expected = [
                ["Widget", "sourced", "sources", "https://example.com"],
                ["Widget", "created", "created", "2026-02-14"],
            ]
            test_case = unittest.TestCase()
            test_case.assertCountEqual(list(rows(root_mapping, db_map, {})), expected)


class TestParameterValueMetadataExport:
    def test_metadata_is_exported(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_metadata(name="sources", value="https://example.com")
            db_map.add_metadata(name="created", value="2026-02-14")
            db_map.add_entity_class(name="Widget")
            db_map.add_entity(entity_class_name="Widget", name="button")
            db_map.add_entity(entity_class_name="Widget", name="checkbox")
            db_map.add_entity(entity_class_name="Widget", name="table")
            db_map.add_parameter_definition(entity_class_name="Widget", name="focus_policy")
            db_map.add_parameter_value(
                entity_class_name="Widget",
                entity_byname=("button",),
                parameter_definition_name="focus_policy",
                alternative_name="Base",
                parsed_value="skip_focus",
            )
            db_map.add_parameter_value(
                entity_class_name="Widget",
                entity_byname=("checkbox",),
                parameter_definition_name="focus_policy",
                alternative_name="Base",
                parsed_value="strong_focus",
            )
            db_map.add_parameter_value(
                entity_class_name="Widget",
                entity_byname=("table",),
                parameter_definition_name="focus_policy",
                alternative_name="Base",
                parsed_value="inhuman_focus",
            )
            db_map.add_parameter_value_metadata(
                entity_class_name="Widget",
                entity_byname=("checkbox",),
                parameter_definition_name="focus_policy",
                alternative_name="Base",
                metadata_name="created",
                metadata_value="2026-02-14",
            )
            db_map.add_parameter_value_metadata(
                entity_class_name="Widget",
                entity_byname=("table",),
                parameter_definition_name="focus_policy",
                alternative_name="Base",
                metadata_name="sources",
                metadata_value="https://example.com",
            )
            db_map.commit_session("Add test data.")
            root_mapping = parameter_value_metadata_export(0, 1, None, 2, 3, 4, 5)
            expected = [
                ["Widget", "checkbox", "focus_policy", "Base", "created", "2026-02-14"],
                ["Widget", "table", "focus_policy", "Base", "sources", "https://example.com"],
            ]
            test_case = unittest.TestCase()
            test_case.assertCountEqual(list(rows(root_mapping, db_map, {})), expected)


class TestSetEntityDimensions(unittest.TestCase):
    def test_change_dimensions_from_zero_to_one(self):
        mapping = entity_export(0, Position.hidden, 1)
        self.assertEqual(mapping.count_mappings(), 4)
        set_entity_dimensions(mapping, 1)
        self.assertEqual(mapping.count_mappings(), 6)
        flattened = mapping.flatten()
        classes = [type(mapping) for mapping in flattened]
        self.assertEqual(
            classes,
            [
                EntityClassMapping,
                EntityClassDescriptionMapping,
                DimensionMapping,
                EntityMapping,
                EntityDescriptionMapping,
                ElementMapping,
            ],
        )
        positions = [mapping.position for mapping in flattened]
        self.assertEqual(positions, [0, Position.hidden, Position.hidden, 1, Position.hidden, Position.hidden])

    def test_change_dimension_from_one_to_zero(self):
        mapping = entity_export(0, Position.hidden, 1, Position.hidden, [2], [3])
        self.assertEqual(mapping.count_mappings(), 6)
        set_entity_dimensions(mapping, 0)
        self.assertEqual(mapping.count_mappings(), 4)
        flattened = mapping.flatten()
        classes = [type(mapping) for mapping in flattened]
        self.assertEqual(
            classes, [EntityClassMapping, EntityClassDescriptionMapping, EntityMapping, EntityDescriptionMapping]
        )
        positions = [mapping.position for mapping in flattened]
        self.assertEqual(positions, [0, Position.hidden, 1, Position.hidden])

    def test_increase_dimensions(self):
        mapping = entity_export(0, Position.hidden, 1, Position.hidden, [2], [3])
        self.assertEqual(mapping.count_mappings(), 6)
        set_entity_dimensions(mapping, 2)
        self.assertEqual(mapping.count_mappings(), 8)
        flattened = mapping.flatten()
        classes = [type(mapping) for mapping in flattened]
        self.assertEqual(
            classes,
            [
                EntityClassMapping,
                EntityClassDescriptionMapping,
                DimensionMapping,
                DimensionMapping,
                EntityMapping,
                EntityDescriptionMapping,
                ElementMapping,
                ElementMapping,
            ],
        )
        positions = [mapping.position for mapping in flattened]
        self.assertEqual(positions, [0, Position.hidden, 2, Position.hidden, 1, Position.hidden, 3, Position.hidden])

    def test_decrease_dimensions(self):
        mapping = entity_export(0, Position.hidden, 1, Position.hidden, [2, 3], [4, 5])
        self.assertEqual(mapping.count_mappings(), 8)
        set_entity_dimensions(mapping, 1)
        self.assertEqual(mapping.count_mappings(), 6)
        flattened = mapping.flatten()
        classes = [type(mapping) for mapping in flattened]
        self.assertEqual(
            classes,
            [
                EntityClassMapping,
                EntityClassDescriptionMapping,
                DimensionMapping,
                EntityMapping,
                EntityDescriptionMapping,
                ElementMapping,
            ],
        )
        positions = [mapping.position for mapping in flattened]
        self.assertEqual(positions, [0, Position.hidden, 2, 1, Position.hidden, 4])


class TestSetEntityElements:
    def test_set_elements_from_zero_to_one(self):
        root_mapping = entity_metadata_export(0, 1, None, 2, 3)
        set_entity_elements(root_mapping, 1)
        assert [type(mapping) for mapping in root_mapping.flatten()] == [
            EntityClassMapping,
            EntityMapping,
            ElementMapping,
            EntityMetadataNameMapping,
            EntityMetadataValueMapping,
        ]
        assert [mapping.position for mapping in root_mapping.flatten()] == [0, 1, Position.hidden, 2, 3]

    def test_set_elements_from_one_to_zero(self):
        root_mapping = entity_metadata_export(0, 1, [2], 3, 4)
        set_entity_elements(root_mapping, 0)
        assert [type(mapping) for mapping in root_mapping.flatten()] == [
            EntityClassMapping,
            EntityMapping,
            EntityMetadataNameMapping,
            EntityMetadataValueMapping,
        ]
        assert [mapping.position for mapping in root_mapping.flatten()] == [0, 1, 3, 4]

    def test_mapping_without_entities_is_unaffected(self):
        root_mapping = metadata_export(0, 1)
        set_entity_elements(root_mapping, 1)
        assert [type(mapping) for mapping in root_mapping.flatten()] == [MetadataNameMapping, MetadataValueMapping]
        assert [mapping.position for mapping in root_mapping.flatten()] == [0, 1]


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
