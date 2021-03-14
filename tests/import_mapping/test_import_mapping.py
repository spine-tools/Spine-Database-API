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
Unit tests for import Mappings.

:author: P. Vennström (VTT)
:date:   22.02.2018
"""
import unittest
from unittest.mock import Mock
from spinedb_api.exception import InvalidMapping
from spinedb_api.mapping import Position, to_dict as mapping_to_dict
from spinedb_api.import_mapping.import_mapping import ImportMapping
from spinedb_api.import_mapping.import_mapping_compat import import_mapping_from_dict
from spinedb_api.import_mapping.generator import get_mapped_data
from spinedb_api.parameter_value import Array, DateTime, TimeSeriesVariableResolution, TimePattern, Map
from ..test_import_functions import assert_import_equivalent


class TestConvertFunctions(unittest.TestCase):
    def test_convert_functions(self):
        data = [["a", "1.2"]]
        column_convert_fns = {0: str, 1: float}
        mapping = [{"map_type": "ObjectClass", "position": 0}, {"map_type": "Object", "position": 1}]
        mapped_data, _ = get_mapped_data(data, [mapping], column_convert_fns=column_convert_fns)
        self.assertEqual(mapped_data, {'object_classes': ['a'], 'objects': [('a', 1.2)]})

    def test_convert_functions_with_error(self):
        data = [["a", "not a float"]]
        column_convert_fns = {0: str, 1: float}
        mapping = [{"map_type": "ObjectClass", "position": 0}, {"map_type": "Object", "position": 1}]
        _, errors = get_mapped_data(data, [mapping], column_convert_fns=column_convert_fns)
        self.assertEqual(len(errors), 1)


class TestPolishImportMapping(unittest.TestCase):
    def test_polish_null_mapping(self):
        mapping = ImportMapping(Position.hidden, value=None)
        table_name = "tablename"
        header = ["A", "B", "C"]
        mapping.polish(table_name, header)
        self.assertEqual(mapping.position, Position.hidden)
        self.assertIsNone(mapping.value)

    def test_polish_column_mapping(self):
        mapping = ImportMapping("B", value=None)
        table_name = "tablename"
        header = ["A", "B", "C"]
        mapping.polish(table_name, header)
        self.assertEqual(mapping.position, 1)
        self.assertIsNone(mapping.value)

    def test_polish_column_header_mapping(self):
        mapping = ImportMapping(Position.header, value=2)
        table_name = "tablename"
        header = ["A", "B", "C"]
        mapping.polish(table_name, header)
        self.assertEqual(mapping.position, Position.header)
        self.assertEqual(mapping.value, "C")

    def test_polish_column_header_mapping_str(self):
        mapping = ImportMapping(Position.header, value="2")
        table_name = "tablename"
        header = ["A", "B", "C"]
        mapping.polish(table_name, header)
        self.assertEqual(mapping.position, Position.header)
        self.assertEqual(mapping.value, "C")

    def test_polish_column_header_mapping_invalid_header(self):
        mapping = ImportMapping(Position.header, value="D")
        table_name = "tablename"
        header = ["A", "B", "C"]
        with self.assertRaises(InvalidMapping):
            mapping.polish(table_name, header)

    def test_polish_column_header_mapping_invalid_index(self):
        mapping = ImportMapping(Position.header, value=4)
        table_name = "tablename"
        header = ["A", "B", "C"]
        with self.assertRaises(InvalidMapping):
            mapping.polish(table_name, header)

    def test_polish_table_name_mapping(self):
        mapping = ImportMapping(Position.table_name)
        table_name = "tablename"
        header = ["A", "B", "C"]
        mapping.polish(table_name, header)
        self.assertEqual(mapping.position, Position.table_name)
        self.assertEqual(mapping.value, "tablename")

    def test_polish_row_header_mapping(self):
        mapping = ImportMapping(Position.header, value=None)
        table_name = "tablename"
        header = ["A", "B", "C"]
        mapping.polish(table_name, header)
        self.assertEqual(mapping.position, Position.header)
        self.assertIsNone(mapping.value)


class TestImportMappingIO(unittest.TestCase):
    def test_object_class_mapping(self):
        mapping = import_mapping_from_dict({"map_type": "ObjectClass"})
        d = mapping_to_dict(mapping)
        types = [m["map_type"] for m in d]
        expected = ['ObjectClass', 'Object', 'ObjectMetadata']
        self.assertEqual(types, expected)

    def test_relationship_class_mapping(self):
        mapping = import_mapping_from_dict({"map_type": "RelationshipClass"})
        d = mapping_to_dict(mapping)
        types = [m["map_type"] for m in d]
        expected = [
            'RelationshipClass',
            'RelationshipClassObjectClass',
            'Relationship',
            'RelationshipObject',
            'RelationshipMetadata',
        ]
        self.assertEqual(types, expected)

    def test_object_group_mapping(self):
        mapping = import_mapping_from_dict({"map_type": "ObjectGroup"})
        d = mapping_to_dict(mapping)
        types = [m["map_type"] for m in d]
        expected = ['ObjectClass', 'Object', 'ObjectGroup']
        self.assertEqual(types, expected)

    def test_alternative_mapping(self):
        mapping = import_mapping_from_dict({"map_type": "Alternative"})
        d = mapping_to_dict(mapping)
        types = [m["map_type"] for m in d]
        expected = ['Alternative']
        self.assertEqual(types, expected)

    def test_scenario_mapping(self):
        mapping = import_mapping_from_dict({"map_type": "Scenario"})
        d = mapping_to_dict(mapping)
        types = [m["map_type"] for m in d]
        expected = ['Scenario', 'ScenarioActiveFlag']
        self.assertEqual(types, expected)

    def test_scenario_alternative_mapping(self):
        mapping = import_mapping_from_dict({"map_type": "ScenarioAlternative"})
        d = mapping_to_dict(mapping)
        types = [m["map_type"] for m in d]
        expected = ['Scenario', 'ScenarioAlternative', 'ScenarioBeforeAlternative']
        self.assertEqual(types, expected)

    def test_tool_mapping(self):
        mapping = import_mapping_from_dict({"map_type": "Tool"})
        d = mapping_to_dict(mapping)
        types = [m["map_type"] for m in d]
        expected = ['Tool']
        self.assertEqual(types, expected)

    def test_tool_feature_mapping(self):
        mapping = import_mapping_from_dict({"map_type": "ToolFeature"})
        d = mapping_to_dict(mapping)
        types = [m["map_type"] for m in d]
        expected = ['Tool', 'ToolFeatureEntityClass', 'ToolFeatureParameterDefinition', 'ToolFeatureRequiredFlag']
        self.assertEqual(types, expected)

    def test_tool_feature_method_mapping(self):
        mapping = import_mapping_from_dict({"map_type": "ToolFeatureMethod"})
        d = mapping_to_dict(mapping)
        types = [m["map_type"] for m in d]
        expected = [
            'Tool',
            'ToolFeatureMethodEntityClass',
            'ToolFeatureMethodParameterDefinition',
            'ToolFeatureMethodMethod',
        ]
        self.assertEqual(types, expected)

    def test_parameter_value_list_mapping(self):
        mapping = import_mapping_from_dict({"map_type": "ParameterValueList"})
        d = mapping_to_dict(mapping)
        types = [m["map_type"] for m in d]
        expected = ['ParameterValueList', 'ParameterValueListValue']
        self.assertEqual(types, expected)


class TestImportMappingLegacy(unittest.TestCase):
    def test_ObjectClass_to_dict_from_dict(self):
        mapping = {
            "map_type": "ObjectClass",
            "name": 0,
            "objects": 1,
            "parameters": {"map_type": "parameter", "name": 2, "value": 3, "parameter_type": "single value"},
        }
        mapping = import_mapping_from_dict(mapping)
        out = mapping_to_dict(mapping)
        expected = [
            {'map_type': 'ObjectClass', 'position': 0},
            {'map_type': 'Object', 'position': 1},
            {'map_type': 'ObjectMetadata', 'position': 'hidden'},
            {'map_type': 'ParameterDefinition', 'position': 2},
            {'map_type': 'Alternative', 'position': 'hidden'},
            {'map_type': 'ParameterValueMetadata', 'position': 'hidden'},
            {'map_type': 'ParameterValue', 'position': 3},
        ]
        self.assertEqual(out, expected)

    def test_ObjectClass_object_from_dict_to_dict(self):
        mapping = {"map_type": "ObjectClass", "name": 0, "objects": 1}
        mapping = import_mapping_from_dict(mapping)
        out = mapping_to_dict(mapping)
        expected = [
            {'map_type': 'ObjectClass', 'position': 0},
            {'map_type': 'Object', 'position': 1},
            {'map_type': 'ObjectMetadata', 'position': 'hidden'},
        ]
        self.assertEqual(out, expected)

    def test_ObjectClass_object_from_dict_to_dict2(self):
        mapping = {"map_type": "ObjectClass", "name": "cls", "objects": "obj"}
        mapping = import_mapping_from_dict(mapping)
        out = mapping_to_dict(mapping)
        expected = [
            {'map_type': 'ObjectClass', 'position': 'hidden', 'value': 'cls'},
            {'map_type': 'Object', 'position': 'hidden', 'value': 'obj'},
            {'map_type': 'ObjectMetadata', 'position': 'hidden'},
        ]
        self.assertEqual(out, expected)

    def test_RelationshipClassMapping_from_dict_to_dict(self):
        mapping = {
            "map_type": "RelationshipClass",
            "name": "unit__node",
            "object_classes": [0, 1],
            "objects": [0, 1],
            "parameters": {"map_type": "parameter", "name": "pname", "value": 2},
        }
        mapping = import_mapping_from_dict(mapping)
        out = mapping_to_dict(mapping)
        expected = [
            {'map_type': 'RelationshipClass', 'position': 'hidden', 'value': 'unit__node'},
            {'map_type': 'RelationshipClassObjectClass', 'position': 0},
            {'map_type': 'RelationshipClassObjectClass', 'position': 1},
            {'map_type': 'Relationship', 'position': 'hidden', 'value': 'relationship'},
            {'map_type': 'RelationshipObject', 'position': 0},
            {'map_type': 'RelationshipObject', 'position': 1},
            {'map_type': 'RelationshipMetadata', 'position': 'hidden'},
            {'map_type': 'ParameterDefinition', 'position': 'hidden', 'value': 'pname'},
            {'map_type': 'Alternative', 'position': 'hidden'},
            {'map_type': 'ParameterValueMetadata', 'position': 'hidden'},
            {'map_type': 'ParameterValue', 'position': 2},
        ]
        self.assertEqual(out, expected)

    def test_RelationshipClassMapping_from_dict_to_dict2(self):
        mapping = {
            "map_type": "RelationshipClass",
            "name": "unit__node",
            "object_classes": ["cls", 0],
            "objects": ["obj", 0],
        }
        mapping = import_mapping_from_dict(mapping)
        out = mapping_to_dict(mapping)
        expected = [
            {'map_type': 'RelationshipClass', 'position': 'hidden', 'value': 'unit__node'},
            {'map_type': 'RelationshipClassObjectClass', 'position': 'hidden', 'value': 'cls'},
            {'map_type': 'RelationshipClassObjectClass', 'position': 0},
            {'map_type': 'Relationship', 'position': 'hidden', 'value': 'relationship'},
            {'map_type': 'RelationshipObject', 'position': 'hidden', 'value': 'obj'},
            {'map_type': 'RelationshipObject', 'position': 0},
            {'map_type': 'RelationshipMetadata', 'position': 'hidden'},
        ]
        self.assertEqual(out, expected)

    def test_RelationshipClassMapping_from_dict_to_dict3(self):
        mapping = {
            "map_type": "RelationshipClass",
            "name": "unit__node",
            "parameters": {
                "map_type": "parameter",
                "name": "pname",
                "value": 2,
                "parameter_type": "array",
                "extra_dimensions": ["dim"],
            },
        }
        mapping = import_mapping_from_dict(mapping)
        out = mapping_to_dict(mapping)
        expected = [
            {'map_type': 'RelationshipClass', 'position': 'hidden', 'value': 'unit__node'},
            {'map_type': 'RelationshipClassObjectClass', 'position': 'hidden'},
            {'map_type': 'Relationship', 'position': 'hidden', 'value': 'relationship'},
            {'map_type': 'RelationshipObject', 'position': 'hidden'},
            {'map_type': 'RelationshipMetadata', 'position': 'hidden'},
            {'map_type': 'ParameterDefinition', 'position': 'hidden', 'value': 'pname'},
            {'map_type': 'Alternative', 'position': 'hidden'},
            {'map_type': 'ParameterValueMetadata', 'position': 'hidden'},
            {'map_type': 'ParameterValueType', 'position': 'hidden', 'value': 'array'},
            {'map_type': 'ParameterValueIndex', 'position': 'hidden', 'value': 'dim'},
            {'map_type': 'ExpandedValue', 'position': 2},
        ]
        self.assertEqual(out, expected)

    def test_ObjectGroupMapping_to_dict_from_dict(self):
        mapping = {
            "map_type": "ObjectGroup",
            "name": 0,
            "groups": 1,
            "members": 2,
            "parameters": {
                "map_type": "parameter",
                "name": {"map_type": "constant", "reference": "pname"},
                "parameter_value_metadata": {"map_type": "None"},
                "parameter_type": "single value",
                "value": {"reference": 2, "map_type": "column"},
            },
        }
        mapping = import_mapping_from_dict(mapping)
        out = mapping_to_dict(mapping)
        expected = [
            {'map_type': 'ObjectClass', 'position': 0},
            {'map_type': 'Object', 'position': 1},
            {'map_type': 'ObjectGroup', 'position': 2},
            {'map_type': 'ParameterDefinition', 'position': 'hidden', 'value': 'pname'},
            {'map_type': 'Alternative', 'position': 'hidden'},
            {'map_type': 'ParameterValueMetadata', 'position': 'hidden'},
            {'map_type': 'ParameterValue', 'position': 2},
        ]
        self.assertEqual(out, expected)

    def test_Alternative_to_dict_from_dict(self):
        mapping = {"map_type": "Alternative", "name": 0}
        mapping = import_mapping_from_dict(mapping)
        out = mapping_to_dict(mapping)
        expected = [{'map_type': 'Alternative', 'position': 0}]
        self.assertEqual(out, expected)

    def test_Scenario_to_dict_from_dict(self):
        mapping = {"map_type": "Scenario", "name": 0}
        mapping = import_mapping_from_dict(mapping)
        out = mapping_to_dict(mapping)
        expected = [
            {'map_type': 'Scenario', 'position': 0},
            {'map_type': 'ScenarioActiveFlag', 'position': 'hidden', 'value': 'false'},
        ]
        self.assertEqual(out, expected)

    def test_ScenarioAlternative_to_dict_from_dict(self):
        mapping = {
            "map_type": "ScenarioAlternative",
            "scenario_name": 0,
            "alternative_name": 1,
            "before_alternative_name": 2,
        }
        mapping = import_mapping_from_dict(mapping)
        out = mapping_to_dict(mapping)
        expected = [
            {'map_type': 'Scenario', 'position': 0},
            {'map_type': 'ScenarioAlternative', 'position': 1},
            {'map_type': 'ScenarioBeforeAlternative', 'position': 2},
        ]
        self.assertEqual(out, expected)

    def test_Tool_to_dict_from_dict(self):
        mapping = {"map_type": "Tool", "name": 0}
        mapping = import_mapping_from_dict(mapping)
        out = mapping_to_dict(mapping)
        expected = [{'map_type': 'Tool', 'position': 0}]
        self.assertEqual(out, expected)

    def test_Feature_to_dict_from_dict(self):
        mapping = {"map_type": "Feature", "entity_class_name": 0, "parameter_definition_name": 1}
        mapping = import_mapping_from_dict(mapping)
        out = mapping_to_dict(mapping)
        expected = [
            {'map_type': 'FeatureEntityClass', 'position': 0},
            {'map_type': 'FeatureParameterDefinition', 'position': 1},
        ]
        self.assertEqual(out, expected)

    def test_ToolFeature_to_dict_from_dict(self):
        mapping = {"map_type": "ToolFeature", "name": 0, "entity_class_name": 1, "parameter_definition_name": 2}
        mapping = import_mapping_from_dict(mapping)
        out = mapping_to_dict(mapping)
        expected = [
            {'map_type': 'Tool', 'position': 0},
            {'map_type': 'ToolFeatureEntityClass', 'position': 1},
            {'map_type': 'ToolFeatureParameterDefinition', 'position': 2},
            {'map_type': 'ToolFeatureRequiredFlag', 'position': 'hidden', 'value': 'false'},
        ]
        self.assertEqual(out, expected)

    def test_ToolFeatureMethod_to_dict_from_dict(self):
        mapping = {
            "map_type": "ToolFeatureMethod",
            "name": 0,
            "entity_class_name": 1,
            "parameter_definition_name": 2,
            "method": 3,
        }
        mapping = import_mapping_from_dict(mapping)
        out = mapping_to_dict(mapping)
        expected = [
            {'map_type': 'Tool', 'position': 0},
            {'map_type': 'ToolFeatureMethodEntityClass', 'position': 1},
            {'map_type': 'ToolFeatureMethodParameterDefinition', 'position': 2},
            {'map_type': 'ToolFeatureMethodMethod', 'position': 3},
        ]
        self.assertEqual(out, expected)


class _XXX:
    def test_MapValueMapping_to_dict(self):
        mapping_value = RowMapping(reference=23)
        extra_dimension = ColumnMapping(reference="fifth column")
        parameter_mapping = MapValueMapping(main_value=mapping_value, extra_dimension=[extra_dimension])
        mapping_dict = parameter_mapping.to_dict()
        expected = {
            "compress": False,
            "value_type": "map",
            "main_value": {"reference": 23, "map_type": "row"},
            "extra_dimensions": [{"reference": "fifth column", "map_type": "column"}],
        }
        self.assertEqual(mapping_dict, expected)

    def test_MapValueMapping_from_dict(self):
        mapping_dict = {
            "value_type": "map",
            "main_value": {"reference": 23, "map_type": "row"},
            "extra_dimensions": [{"reference": "fifth column", "map_type": "column"}],
            "compress": True,
        }
        parameter_mapping = MapValueMapping.from_dict(mapping_dict)
        self.assertIsInstance(parameter_mapping, MapValueMapping)
        main_value = parameter_mapping.main_value
        self.assertIsInstance(main_value, RowMapping)
        self.assertEqual(main_value.reference, 23)
        extra_dimensions = parameter_mapping.extra_dimensions
        self.assertEqual(len(extra_dimensions), 1)
        dimension = extra_dimensions[0]
        self.assertIsInstance(dimension, ColumnMapping)
        self.assertEqual(dimension.reference, "fifth column")
        self.assertEqual(parameter_mapping.compress, True)

    def test_TimeSeriesOptions_to_dict(self):
        options = TimeSeriesOptions(repeat=True)
        options_dict = options.to_dict()
        self.assertEqual(options_dict, {"repeat": True, "ignore_year": False, "fixed_resolution": False})

    def test_TimeSeriesOptions_from_dict(self):
        options_dict = {"repeat": True, "ignore_year": False, "fixed_resolution": False}
        options = TimeSeriesOptions.from_dict(options_dict)
        self.assertEqual(options.repeat, True)
        self.assertEqual(options.ignore_year, False)
        self.assertEqual(options.fixed_resolution, False)

    def test_TimeSeriesValueMapping_to_dict(self):
        mapping_value = RowMapping(reference=23)
        extra_dimension = ColumnMapping(reference="fifth column")
        parameter_options = TimeSeriesOptions(repeat=True)
        parameter_mapping = TimeSeriesValueMapping(
            main_value=mapping_value, extra_dimension=[extra_dimension], options=parameter_options
        )
        mapping_dict = parameter_mapping.to_dict()
        expected = {
            "value_type": "time series",
            "main_value": {"reference": 23, "map_type": "row"},
            "extra_dimensions": [{"reference": "fifth column", "map_type": "column"}],
            "options": {"repeat": True, "ignore_year": False, "fixed_resolution": False},
        }
        self.assertEqual(mapping_dict, expected)

    def test_TimeSeriesValueMapping_from_dict(self):
        mapping_dict = {
            "value_type": "time series",
            "main_value": {"reference": 23, "map_type": "row"},
            "extra_dimensions": [{"reference": "fifth column", "map_type": "column"}],
            "options": {"repeat": True, "ignore_year": False, "fixed_resolution": False},
        }
        parameter_mapping = TimeSeriesValueMapping.from_dict(mapping_dict)
        self.assertTrue(isinstance(parameter_mapping, TimeSeriesValueMapping))
        main_value = parameter_mapping.main_value
        self.assertTrue(isinstance(main_value, RowMapping))
        self.assertEqual(main_value.reference, 23)
        extra_dimensions = parameter_mapping.extra_dimensions
        self.assertEqual(len(extra_dimensions), 1)
        dimension = extra_dimensions[0]
        self.assertTrue(isinstance(dimension, ColumnMapping))
        self.assertEqual(dimension.reference, "fifth column")
        options = parameter_mapping.options
        self.assertEqual(options.repeat, True)
        self.assertEqual(options.ignore_year, False)
        self.assertEqual(options.fixed_resolution, False)

    def test_TableNameMapping_from_dict(self):
        mapping_dict = {"map_type": "table_name", "reference": "name of the table"}
        mapping = TableNameMapping.from_dict(mapping_dict)
        self.assertEqual(mapping.reference, "name of the table")
        self.assertTrue(mapping.is_valid())
        self.assertTrue(mapping.returns_value())

    def test_TableNameMapping_to_dict(self):
        mapping = TableNameMapping("name of the table")
        mapping_dict = mapping.to_dict()
        self.assertEqual(mapping_dict, {"map_type": "table_name", "reference": "name of the table"})


def _parent_with_pivot(is_pivoted):
    parent = Mock()
    parent.is_pivoted.return_value = is_pivoted
    return parent


def _pivoted_parent():
    return _parent_with_pivot(True)


def _unpivoted_parent():
    return _parent_with_pivot(False)


@unittest.skip("Obsolete, need to find an equivalent in the new API")
class TestMappingIsValid(unittest.TestCase):
    def test_valid_mapping(self):
        mapping = ColumnMapping(reference=1)
        is_valid = mapping.is_valid()
        self.assertTrue(is_valid)

    def test_invalid_mapping(self):
        mapping = ColumnMapping()
        is_valid = mapping.is_valid()
        self.assertFalse(is_valid)

    def test_valid_parameter_mapping_definition(self):
        mapping = {"map_type": "parameter", "name": "test", "parameter_type": "definition"}
        mapping = parameter_mapping_from_dict(mapping)
        mapping.parent = _unpivoted_parent()
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)

    def test_invalid_parameter_mapping_definition(self):
        mapping = {"map_type": "parameter", "parameter_type": "definition"}
        mapping = parameter_mapping_from_dict(mapping)
        mapping.parent = _unpivoted_parent()
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)

    def test_valid_parameter_mapping_single_value(self):
        mapping = {"map_type": "parameter", "name": "test", "value": 0, "parameter_type": "single value"}
        mapping = parameter_mapping_from_dict(mapping)
        mapping.parent = _unpivoted_parent()
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)

    def test_invalid_parameter_mapping_single_value(self):
        mapping = {"map_type": "parameter", "name": "test", "value": None, "parameter_type": "single value"}
        mapping = parameter_mapping_from_dict(mapping)
        mapping.parent = _unpivoted_parent()
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)

    def test_valid_parameter_mapping_time_series(self):
        mapping = {
            "map_type": "parameter",
            "name": "test",
            "value": "test",
            "parameter_type": "time series",
            "extra_dimensions": [0],
        }
        mapping = parameter_mapping_from_dict(mapping)
        mapping.parent = _unpivoted_parent()
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)

    def test_invalid_parameter_mapping_time_series(self):
        mapping = {"map_type": "parameter", "name": "test", "value": "test", "parameter_type": "time series"}
        mapping = parameter_mapping_from_dict(mapping)
        mapping.parent = _unpivoted_parent()
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)

    def test_invalid_parameter_mapping_time_series_non_valid_extra_dim(self):
        mapping = {
            "map_type": "parameter",
            "name": "test",
            "value": "test",
            "parameter_type": "time series",
            "extra_dimensions": [None],
        }
        mapping = parameter_mapping_from_dict(mapping)
        mapping.parent = _unpivoted_parent()
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)

    def test_valid_parameter_mapping_time_pattern(self):
        mapping = {
            "map_type": "parameter",
            "name": "test",
            "value": "test",
            "parameter_type": "time pattern",
            "extra_dimensions": [0],
        }
        mapping = parameter_mapping_from_dict(mapping)
        mapping.parent = _unpivoted_parent()
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)

    def test_invalid_parameter_mapping_time_pattern(self):
        mapping = {"map_type": "parameter", "name": "test", "value": "test", "parameter_type": "time pattern"}
        mapping = parameter_mapping_from_dict(mapping)
        mapping.parent = _unpivoted_parent()
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)

    def test_valid_multidimensional_map_mapping(self):
        mapping = {
            "map_type": "parameter",
            "name": "test",
            "value": 2,
            "parameter_type": "map",
            "compress": False,
            "extra_dimensions": [0, 1],
        }
        mapping = parameter_mapping_from_dict(mapping)
        mapping.parent = _unpivoted_parent()
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)

    def test_invalid_multidimensional_map_mapping_missing_mapping_for_extra_dimension(self):
        mapping = {
            "map_type": "parameter",
            "name": "test",
            "value": 2,
            "parameter_type": "map",
            "compress": False,
            "extra_dimensions": [0, None],
        }
        mapping = parameter_mapping_from_dict(mapping)
        mapping.parent = _unpivoted_parent()
        is_valid, msg = mapping.is_valid()
        self.assertFalse(is_valid)
        self.assertTrue(msg)

    def test_valid_pivoted_parameter_mapping(self):
        mapping = {"map_type": "parameter", "name": {"map_type": "row", "reference": 0}}
        mapping = parameter_mapping_from_dict(mapping)
        mapping.parent = _pivoted_parent()
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)

    def test_invalid_pivoted_parameter_mapping(self):
        mapping = {"map_type": "parameter", "name": {"map_type": "column", "reference": 0}}
        mapping = parameter_mapping_from_dict(mapping)
        mapping.parent = _unpivoted_parent()
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)

    def test_valid_pivoted_parent_parameter_mapping(self):
        mapping = {"map_type": "parameter", "name": {"map_type": "column", "reference": 0}}
        mapping = parameter_mapping_from_dict(mapping)
        mapping.parent = _pivoted_parent()
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)

    def test_valid_object_class_mapping(self):
        mapping = {"map_type": "ObjectClass", "name": "test"}
        mapping = ObjectClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)

    def test_valid_object_class_mapping_with_object(self):
        mapping = {"map_type": "ObjectClass", "name": "test", "object": "test"}
        mapping = ObjectClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)

    def test_valid_object_class_mapping_with_object_and_parameter(self):
        mapping = {
            "map_type": "ObjectClass",
            "name": "test",
            "object": "test",
            "parameters": {"map_type": "parameter", "name": "test", "value": 0},
        }
        mapping = ObjectClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)

    def test_invalid_object_class_mapping_with_parameter_but_no_object(self):
        mapping = {
            "map_type": "ObjectClass",
            "name": "test",
            "parameters": {"map_type": "parameter", "name": "test", "value": 0},
        }
        mapping = ObjectClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)

    def test_valid_object_class_mapping_with_parameter_definition_but_no_object(self):
        mapping = {
            "map_type": "ObjectClass",
            "name": "test",
            "parameters": {"map_type": "parameter", "name": "test", "parameter_type": "definition"},
        }
        mapping = ObjectClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)

    def test_valid_relationship_class_mapping(self):
        mapping = {"map_type": "RelationshipClass", "name": "test", "object_classes": ["test"]}
        mapping = RelationshipClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)

    def test_valid_relationship_class_mapping_with_objects(self):
        mapping = {"map_type": "RelationshipClass", "name": "test", "object_classes": ["test"], "objects": ["test"]}
        mapping = RelationshipClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)

    def test_valid_relationship_class_mapping_with_invalid_objects(self):
        mapping = {"map_type": "RelationshipClass", "name": "test", "object_classes": ["test"], "objects": [None]}
        mapping = RelationshipClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)

    def test_valid_relationship_class_mapping_with_parameter_definition(self):
        mapping = {
            "map_type": "RelationshipClass",
            "name": "test",
            "object_classes": ["test"],
            "parameters": {"map_type": "parameter", "name": "test", "parameter_type": "definition"},
        }
        mapping = RelationshipClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)

    def test_invalid_relationship_class_mapping_with_parameter_and_invalid_objects(self):
        mapping = {
            "map_type": "RelationshipClass",
            "name": "test",
            "object_classes": ["test"],
            "objects": [None],
            "parameters": {"map_type": "parameter", "name": "test", "value": "test"},
        }
        mapping = RelationshipClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)

    def test_valid_pivoted_relationship_class_mapping_with_parameter(self):
        mapping = {
            "map_type": "RelationshipClass",
            "name": "test",
            "object_classes": ["test"],
            "objects": [{"map_type": "row", "reference": 0}],
            "parameters": {"map_type": "parameter", "name": "test"},
        }
        mapping = RelationshipClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)

    def test_valid_relationship_class_mapping_with_invalid_parameter(self):
        mapping = {
            "map_type": "RelationshipClass",
            "name": "test",
            "object_classes": ["test"],
            "objects": ["test"],
            "parameters": {"map_type": "parameter"},
        }
        mapping = RelationshipClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)

    def test_valid_alternative_mapping(self):
        mapping = {"map_type": "Alternative", "name": "test"}
        mapping = AlternativeMapping.from_dict(mapping)
        is_valid, msg = mapping.is_valid()
        self.assertTrue(is_valid)
        self.assertFalse(msg)

    def test_invalid_alternative_mapping_name_missing(self):
        mapping = {"map_type": "Alternative", "name": None}
        mapping = AlternativeMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)

    def test_valid_scenario_mapping(self):
        mapping = {"map_type": "Scenario", "name": "test"}
        mapping = ScenarioMapping.from_dict(mapping)
        is_valid, msg = mapping.is_valid()
        self.assertTrue(is_valid)
        self.assertFalse(msg)

    def test_invalid_scenario_mapping_name_missing(self):
        mapping = {"map_type": "Scenario", "name": None}
        mapping = ScenarioMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)

    def test_valid_scenario_alternative(self):
        mapping = {"map_type": "ScenarioAlternative", "scenario_name": "scenario", "alternatives": 0, "ranks": 1}
        mapping = ScenarioAlternativeMapping.from_dict(mapping)
        is_valid, msg = mapping.is_valid()
        self.assertTrue(is_valid)
        self.assertFalse(msg)

    def test_invalid_scenario_alternative_scenario_name_missing(self):
        mapping = {"map_type": "ScenarioAlternative", "scenario_name": None, "alternatives": 0, "ranks": 1}
        mapping = ScenarioAlternativeMapping.from_dict(mapping)
        is_valid, msg = mapping.is_valid()
        self.assertFalse(is_valid)
        self.assertTrue(msg)

    def test_valid_tool_mapping(self):
        mapping = {"map_type": "Tool", "name": "test"}
        mapping = ToolMapping.from_dict(mapping)
        is_valid, msg = mapping.is_valid()
        self.assertTrue(is_valid)
        self.assertFalse(msg)

    def test_invalid_tool_mapping_name_missing(self):
        mapping = {"map_type": "Tool", "name": None}
        mapping = ToolMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)

    def test_valid_feature_mapping(self):
        mapping = {"map_type": "Feature", "entity_class_name": "test", "parameter_definition_name": "test"}
        mapping = FeatureMapping.from_dict(mapping)
        is_valid, msg = mapping.is_valid()
        self.assertTrue(is_valid)
        self.assertFalse(msg)

    def test_invalid_feature_mapping_entity_class_name_missing(self):
        mapping = {"map_type": "Feature", "entity_class_name": None, "parameter_definition_name": "test"}
        mapping = ToolMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)

    def test_invalid_feature_mapping_parameter_definition_name_missing(self):
        mapping = {"map_type": "Feature", "entity_class_name": "test", "parameter_definition_name": None}
        mapping = ToolMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)

    def test_valid_tool_feature_mapping(self):
        mapping = {
            "map_type": "ToolFeature",
            "name": "test",
            "entity_class_name": "test",
            "parameter_definition_name": "test",
        }
        mapping = ToolFeatureMapping.from_dict(mapping)
        is_valid, msg = mapping.is_valid()
        self.assertTrue(is_valid)
        self.assertFalse(msg)

    def test_invalid_tool_feature_mapping_name_missing(self):
        mapping = {
            "map_type": "ToolFeature",
            "name": None,
            "entity_class_name": "test",
            "parameter_definition_name": "test",
        }
        mapping = ToolFeatureMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)

    def test_invalid_tool_feature_mapping_entity_class_name_missing(self):
        mapping = {
            "map_type": "ToolFeature",
            "name": "test",
            "entity_class_name": None,
            "parameter_definition_name": "test",
        }
        mapping = ToolFeatureMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)

    def test_invalid_tool_feature_mapping_parameter_definition_name_missing(self):
        mapping = {
            "map_type": "ToolFeature",
            "name": "test",
            "entity_class_name": "test",
            "parameter_definition_name": None,
        }
        mapping = ToolFeatureMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)

    def test_valid_tool_feature_method_mapping(self):
        mapping = {
            "map_type": "ToolFeatureMethod",
            "name": "test",
            "entity_class_name": "test",
            "parameter_definition_name": "test",
            "method": "test",
        }
        mapping = ToolFeatureMethodMapping.from_dict(mapping)
        is_valid, msg = mapping.is_valid()
        self.assertTrue(is_valid)
        self.assertFalse(msg)

    def test_invalid_tool_feature_method_mapping_name_missing(self):
        mapping = {
            "map_type": "ToolFeatureMethod",
            "name": None,
            "entity_class_name": "test",
            "parameter_definition_name": "test",
            "method": "test",
        }
        mapping = ToolFeatureMethodMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)

    def test_invalid_tool_feature_method_mapping_entity_class_name_missing(self):
        mapping = {
            "map_type": "ToolFeatureMethod",
            "name": "test",
            "entity_class_name": None,
            "parameter_definition_name": "test",
            "method": "test",
        }
        mapping = ToolFeatureMethodMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)

    def test_invalid_tool_feature_method_mapping_parameter_definition_name_missing(self):
        mapping = {
            "map_type": "ToolFeatureMethod",
            "name": "test",
            "entity_class_name": "test",
            "parameter_definition_name": None,
            "method": "test",
        }
        mapping = ToolFeatureMethodMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)

    def test_invalid_tool_feature_method_mapping_method_missing(self):
        mapping = {
            "map_type": "ToolFeatureMethod",
            "name": "test",
            "entity_class_name": "test",
            "parameter_definition_name": "test",
            "method": None,
        }
        mapping = ToolFeatureMethodMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)


class TestMappingIntegration(unittest.TestCase):
    # just a placeholder test for different mapping testings
    def _assert_equivalent(self, obs, exp):
        """Asserts that two dictionaries will have the same effect if passed to ``import_functions.import_data()``
        """
        assert_import_equivalent(self, obs, exp)

    def test_bad_mapping_type(self):
        """Tests that passing any other than a `dict` or a `mapping` to `get_mapped_data` raises `TypeError`.
        """
        input_data = [["object_class"], ["oc1"]]
        data = iter(input_data)
        data_header = next(data)

        with self.assertRaises(TypeError):
            mapping = [1, 2, 3]
            get_mapped_data(data, [mapping], data_header)

        with self.assertRaises(TypeError):
            mappings = [{"map_type": "ObjectClass", "name": 0}, [1, 2, 3]]
            get_mapped_data(data, mappings, data_header)

    def test_read_iterator_with_row_with_all_Nones(self):
        input_data = [
            ["object_class", "object", "parameter", "value"],
            [None, None, None, None],
            ["oc2", "obj2", "parameter_name2", 2],
        ]
        expected = {"object_classes": ["oc2"]}

        data = iter(input_data)
        data_header = next(data)

        mapping = {"map_type": "ObjectClass", "name": 0}

        out, errors = get_mapped_data(data, [mapping], data_header)
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_iterator_with_None(self):
        input_data = [["object_class", "object", "parameter", "value"], None, ["oc2", "obj2", "parameter_name2", 2]]
        expected = {"object_classes": ["oc2"]}

        data = iter(input_data)
        data_header = next(data)

        mapping = {"map_type": "ObjectClass", "name": 0}

        out, errors = get_mapped_data(data, [mapping], data_header)
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_flat_file(self):
        input_data = [
            ["object_class", "object", "parameter", "value"],
            ["oc1", "obj1", "parameter_name1", 1],
            ["oc2", "obj2", "parameter_name2", 2],
        ]
        expected = {
            "object_classes": ["oc1", "oc2"],
            "objects": [("oc1", "obj1"), ("oc2", "obj2")],
            "object_parameters": [("oc1", "parameter_name1"), ("oc2", "parameter_name2")],
            "object_parameter_values": [("oc1", "obj1", "parameter_name1", 1), ("oc2", "obj2", "parameter_name2", 2)],
        }

        data = iter(input_data)
        data_header = next(data)

        mapping = {
            "map_type": "ObjectClass",
            "name": 0,
            "objects": 1,
            "parameters": {"map_type": "parameter", "name": 2, "value": 3},
        }

        out, errors = get_mapped_data(data, [mapping], data_header)
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_flat_file_array(self):
        input_data = [
            ["object_class", "object", "parameter", "value"],
            ["oc1", "obj1", "parameter_name1", 1],
            ["oc1", "obj1", "parameter_name1", 2],
        ]
        expected = {
            "object_classes": ["oc1", "oc1"],
            "objects": [("oc1", "obj1"), ("oc1", "obj1")],
            "object_parameters": [("oc1", "parameter_name1"), ("oc1", "parameter_name1")],
            "object_parameter_values": [("oc1", "obj1", "parameter_name1", Array([1, 2]))],
        }

        data = iter(input_data)
        data_header = next(data)

        mapping = {
            "map_type": "ObjectClass",
            "name": 0,
            "objects": 1,
            "parameters": {"map_type": "parameter", "name": "parameter_name1", "value": 3, "parameter_type": "array"},
        }

        out, errors = get_mapped_data(data, [mapping], data_header)
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_flat_file_array_with_ed(self):
        input_data = [
            ["object_class", "object", "parameter", "value", "value_order"],
            ["oc1", "obj1", "parameter_name1", 1, 0],
            ["oc1", "obj1", "parameter_name1", 2, 1],
        ]
        expected = {
            "object_classes": ["oc1", "oc1"],
            "objects": [("oc1", "obj1"), ("oc1", "obj1")],
            "object_parameters": [("oc1", "parameter_name1"), ("oc1", "parameter_name1")],
            "object_parameter_values": [("oc1", "obj1", "parameter_name1", Array([1, 2]))],
        }

        data = iter(input_data)
        data_header = next(data)

        mapping = {
            "map_type": "ObjectClass",
            "name": 0,
            "objects": 1,
            "parameters": {
                "map_type": "parameter",
                "name": "parameter_name1",
                "value": 3,
                "extra_dimension": [None],
                "parameter_type": "array",
            },
        }

        out, errors = get_mapped_data(data, [mapping], data_header)
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_flat_file_with_column_name_reference(self):
        input_data = [["object", "parameter", "value"], ["obj1", "parameter_name1", 1], ["obj2", "parameter_name2", 2]]
        expected = {"object_classes": ["object"], "objects": [("object", "obj1"), ("object", "obj2")]}

        data = iter(input_data)
        data_header = next(data)

        mapping = {"map_type": "ObjectClass", "name": {"map_type": "column_name", "reference": 0}, "object": 0}

        out, errors = get_mapped_data(data, [mapping], data_header)
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_object_class_from_header_using_string_as_integral_index(self):
        input_data = [["object_class"], ["obj1"], ["obj2"]]
        expected = {"object_classes": ["object_class"], "objects": [("object_class", "obj1"), ("object_class", "obj2")]}

        data = iter(input_data)
        data_header = next(data)

        mapping = {"map_type": "ObjectClass", "name": {"map_type": "column_header", "reference": "0"}, "object": 0}

        out, errors = get_mapped_data(data, [mapping], data_header)
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_object_class_from_header_using_string_as_column_header_name(self):
        input_data = [["object_class"], ["obj1"], ["obj2"]]
        expected = {"object_classes": ["object_class"], "objects": [("object_class", "obj1"), ("object_class", "obj2")]}

        data = iter(input_data)
        data_header = next(data)

        mapping = {
            "map_type": "ObjectClass",
            "name": {"map_type": "column_header", "reference": "object_class"},
            "object": 0,
        }

        out, errors = get_mapped_data(data, [mapping], data_header)
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_with_list_of_mappings(self):
        input_data = [["object", "parameter", "value"], ["obj1", "parameter_name1", 1], ["obj2", "parameter_name2", 2]]
        expected = {"object_classes": ["object"], "objects": [("object", "obj1"), ("object", "obj2")]}

        data = iter(input_data)
        data_header = next(data)

        mapping = {"map_type": "ObjectClass", "name": {"map_type": "column_header", "reference": 0}, "object": 0}

        out, errors = get_mapped_data(data, [mapping], data_header)
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_pivoted_parameters_from_header(self):
        input_data = [["object", "parameter_name1", "parameter_name2"], ["obj1", 0, 1], ["obj2", 2, 3]]
        expected = {
            "object_classes": ["object"],
            "objects": [("object", "obj1"), ("object", "obj2")],
            "object_parameters": [("object", "parameter_name1"), ("object", "parameter_name2")],
            "object_parameter_values": [
                ("object", "obj1", "parameter_name1", 0),
                ("object", "obj1", "parameter_name2", 1),
                ("object", "obj2", "parameter_name1", 2),
                ("object", "obj2", "parameter_name2", 3),
            ],
        }

        data = iter(input_data)
        data_header = next(data)

        mapping = {
            "map_type": "ObjectClass",
            "name": {"map_type": "column_header", "reference": 0},
            "object": 0,
            "parameters": {"map_type": "parameter", "name": {"map_type": "row", "reference": -1}},
        }  # -1 to read pivot from header

        out, errors = get_mapped_data(data, [mapping], data_header)
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_pivoted_parameters_from_data(self):
        input_data = [["object", "parameter_name1", "parameter_name2"], ["obj1", 0, 1], ["obj2", 2, 3]]
        expected = {
            "object_classes": ["object"],
            "objects": [("object", "obj1"), ("object", "obj2")],
            "object_parameters": [("object", "parameter_name1"), ("object", "parameter_name2")],
            "object_parameter_values": [
                ("object", "obj1", "parameter_name1", 0),
                ("object", "obj1", "parameter_name2", 1),
                ("object", "obj2", "parameter_name1", 2),
                ("object", "obj2", "parameter_name2", 3),
            ],
        }

        data = iter(input_data)

        mapping = {
            "map_type": "ObjectClass",
            "name": "object",
            "object": 0,
            "parameters": {"map_type": "parameter", "name": {"map_type": "row", "reference": 0}},
        }

        out, errors = get_mapped_data(data, [mapping])
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_flat_file_with_extra_value_dimensions(self):
        input_data = [["object", "time", "parameter_name1"], ["obj1", "2018-01-01", 1], ["obj1", "2018-01-02", 2]]

        expected = {
            "object_classes": ["object"],
            "objects": [("object", "obj1"), ("object", "obj1")],
            "object_parameters": [("object", "parameter_name1")],
            "object_parameter_values": [
                (
                    "object",
                    "obj1",
                    "parameter_name1",
                    TimeSeriesVariableResolution(["2018-01-01", "2018-01-02"], [1, 2], False, False),
                )
            ],
        }

        data = iter(input_data)
        data_header = next(data)

        mapping = {
            "map_type": "ObjectClass",
            "name": "object",
            "object": 0,
            "parameters": {
                "map_type": "parameter",
                "name": {"map_type": "column_header", "reference": 2},
                "value": 2,
                "parameter_type": "time series",
                "extra_dimensions": [1],
            },
        }

        out, errors = get_mapped_data(data, [mapping], data_header)
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_flat_file_with_parameter_definition(self):
        input_data = [["object", "time", "parameter_name1"], ["obj1", "2018-01-01", 1], ["obj1", "2018-01-02", 2]]

        expected = {
            "object_classes": ["object"],
            "objects": [("object", "obj1"), ("object", "obj1")],
            "object_parameters": [("object", "parameter_name1")],
        }

        data = iter(input_data)
        data_header = next(data)

        mapping = {
            "map_type": "ObjectClass",
            "name": "object",
            "object": 0,
            "parameters": {
                "map_type": "parameter",
                "name": {"map_type": "column_header", "reference": 2},
                "value": 2,
                "parameter_type": "definition",
            },
        }

        out, errors = get_mapped_data(data, [mapping], data_header)
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_1dim_relationships(self):
        input_data = [["unit", "node"], ["u1", "n1"], ["u1", "n2"]]
        expected = {
            "relationship_classes": [("node_group", ["node"])],
            "relationships": [("node_group", ["n1"]), ("node_group", ["n2"])],
        }

        data = iter(input_data)
        data_header = next(data)

        mapping = {
            "map_type": "RelationshipClass",
            "name": "node_group",
            "object_classes": [{"map_type": "column_header", "reference": 1}],
            "objects": [1],
        }

        out, errors = get_mapped_data(data, [mapping], data_header)
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_relationships(self):
        input_data = [["unit", "node"], ["u1", "n1"], ["u1", "n2"]]
        expected = {
            "relationship_classes": [("unit__node", ("unit", "node"))],
            "relationships": [("unit__node", ("u1", "n1")), ("unit__node", ("u1", "n2"))],
        }

        data = iter(input_data)
        data_header = next(data)

        mapping = {
            "map_type": "RelationshipClass",
            "name": "unit__node",
            "object_classes": [
                {"map_type": "column_header", "reference": 0},
                {"map_type": "column_header", "reference": 1},
            ],
            "objects": [0, 1],
        }

        out, errors = get_mapped_data(data, [mapping], data_header)
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_relationships_with_parameters(self):
        input_data = [["unit", "node", "rel_parameter"], ["u1", "n1", 0], ["u1", "n2", 1]]
        expected = {
            "relationship_classes": [("unit__node", ("unit", "node"))],
            "relationships": [("unit__node", ("u1", "n1")), ("unit__node", ("u1", "n2"))],
            "relationship_parameters": [("unit__node", "rel_parameter")],
            "relationship_parameter_values": [
                ("unit__node", ("u1", "n1"), "rel_parameter", 0),
                ("unit__node", ("u1", "n2"), "rel_parameter", 1),
            ],
        }

        data = iter(input_data)
        data_header = next(data)

        mapping = {
            "map_type": "RelationshipClass",
            "name": "unit__node",
            "object_classes": [
                {"map_type": "column_header", "reference": 0},
                {"map_type": "column_header", "reference": 1},
            ],
            "objects": [0, 1],
            "parameters": {"map_type": "parameter", "name": {"map_type": "column_header", "reference": 2}, "value": 2},
        }

        out, errors = get_mapped_data(data, [mapping], data_header)
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_relationships_with_parameters2(self):
        input_data = [["nuts2", "Capacity", "Fueltype"], ["BE23", 268.0, "Bioenergy"], ["DE11", 14.0, "Bioenergy"]]
        expected = {
            "object_classes": ["nuts2", "fueltype"],
            "objects": [("nuts2", "BE23"), ("fueltype", "Bioenergy"), ("nuts2", "DE11"), ("fueltype", "Bioenergy")],
            "relationship_classes": [("nuts2__fueltype", ("nuts2", "fueltype"))],
            "relationships": [("nuts2__fueltype", ("BE23", "Bioenergy")), ("nuts2__fueltype", ("DE11", "Bioenergy"))],
            "relationship_parameters": [("nuts2__fueltype", "capacity")],
            "relationship_parameter_values": [
                ("nuts2__fueltype", ("BE23", "Bioenergy"), "capacity", 268.0),
                ("nuts2__fueltype", ("DE11", "Bioenergy"), "capacity", 14.0),
            ],
        }

        data = iter(input_data)
        data_header = next(data)

        mapping = {
            "map_type": "RelationshipClass",
            "name": {"map_type": "constant", "reference": "nuts2__fueltype"},
            "parameters": {
                "map_type": "parameter",
                "name": {"map_type": "constant", "reference": "capacity"},
                "parameter_type": "single value",
                "value": {"map_type": "column", "reference": 1},
            },
            "skip_columns": [],
            "read_start_row": 0,
            "objects": [{"map_type": "column", "reference": 0}, {"map_type": "column", "reference": 2}],
            "object_classes": [
                {"map_type": "constant", "reference": "nuts2"},
                {"map_type": "constant", "reference": "fueltype"},
            ],
            "import_objects": True,
        }

        out, errors = get_mapped_data(data, [mapping], data_header)
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_parameter_header_with_only_one_parameter(self):
        input_data = [["object", "parameter_name1"], ["obj1", 0], ["obj2", 2]]
        expected = {
            "object_classes": ["object"],
            "objects": [("object", "obj1"), ("object", "obj2")],
            "object_parameters": [("object", "parameter_name1")],
            "object_parameter_values": [
                ("object", "obj1", "parameter_name1", 0),
                ("object", "obj2", "parameter_name1", 2),
            ],
        }

        data = iter(input_data)
        data_header = next(data)

        mapping = {
            "map_type": "ObjectClass",
            "name": "object",
            "object": 0,
            "parameters": {"map_type": "parameter", "name": {"map_type": "row", "reference": -1}},
        }  # -1 to read pivot from header

        out, errors = get_mapped_data(data, [mapping], data_header)
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_pivoted_parameters_from_data_with_skipped_column(self):
        input_data = [["object", "parameter_name1", "parameter_name2"], ["obj1", 0, 1], ["obj2", 2, 3]]
        expected = {
            "object_classes": ["object"],
            "objects": [("object", "obj1"), ("object", "obj2")],
            "object_parameters": [("object", "parameter_name1")],
            "object_parameter_values": [
                ("object", "obj1", "parameter_name1", 0),
                ("object", "obj2", "parameter_name1", 2),
            ],
        }

        data = iter(input_data)

        mapping = {
            "map_type": "ObjectClass",
            "name": "object",
            "object": 0,
            "skip_columns": [2],
            "parameters": {"map_type": "parameter", "name": {"map_type": "row", "reference": 0}},
        }  # -1 to read pivot from header

        out, errors = get_mapped_data(data, [mapping])
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_relationships_and_import_objects(self):
        input_data = [["unit", "node"], ["u1", "n1"], ["u2", "n2"]]
        expected = {
            "relationship_classes": [("unit__node", ("unit", "node"))],
            "relationships": [("unit__node", ("u1", "n1")), ("unit__node", ("u2", "n2"))],
            "object_classes": ["unit", "node"],
            "objects": [("unit", "u1"), ("node", "n1"), ("unit", "u2"), ("node", "n2")],
        }

        data = iter(input_data)
        data_header = next(data)

        mapping = {
            "map_type": "RelationshipClass",
            "name": "unit__node",
            "object_classes": [
                {"map_type": "column_header", "reference": 0},
                {"map_type": "column_header", "reference": 1},
            ],
            "objects": [0, 1],
            "import_objects": True,
        }

        out, errors = get_mapped_data(data, [mapping], data_header)
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_relationships_parameter_values_with_extra_dimensions(self):
        input_data = [["", "a", "b"], ["", "c", "d"], ["", "e", "f"], ["a", 2, 3], ["b", 4, 5]]

        expected = {
            "relationship_classes": [("unit__node", ("unit", "node"))],
            "relationship_parameters": [("unit__node", "e"), ("unit__node", "f")],
            "relationships": [("unit__node", ("a", "c")), ("unit__node", ("b", "d"))],
            "relationship_parameter_values": [
                ("unit__node", ("a", "c"), "e", TimePattern(["a", "b"], [2, 4])),
                ("unit__node", ("b", "d"), "f", TimePattern(["a", "b"], [3, 5])),
            ],
        }

        data = iter(input_data)
        data_header = []

        mapping = {
            "map_type": "RelationshipClass",
            "name": "unit__node",
            "object_classes": ["unit", "node"],
            "objects": [{"map_type": "row", "reference": i} for i in range(2)],
            "parameters": {
                "map_type": "parameter",
                "parameter_type": "time pattern",
                "name": {"map_type": "row", "reference": 2},
                "extra_dimensions": [0],
            },
        }

        out, errors = get_mapped_data(data, [mapping], data_header)
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_data_with_read_start_row(self):
        input_data = [
            ["object_class", "object", "parameter", "value"],
            [" ", " ", " ", " "],
            ["oc1", "obj1", "parameter_name1", 1],
            ["oc2", "obj2", "parameter_name2", 2],
        ]
        expected = {
            "object_classes": ["oc1", "oc2"],
            "objects": [("oc1", "obj1"), ("oc2", "obj2")],
            "object_parameters": [("oc1", "parameter_name1"), ("oc2", "parameter_name2")],
            "object_parameter_values": [("oc1", "obj1", "parameter_name1", 1), ("oc2", "obj2", "parameter_name2", 2)],
        }

        data = iter(input_data)
        data_header = next(data)

        mapping = {
            "map_type": "ObjectClass",
            "name": 0,
            "object": 1,
            "parameters": {"map_type": "parameter", "name": 2, "value": 3},
            "read_start_row": 1,
        }

        out, errors = get_mapped_data(data, [mapping], data_header)
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_data_with_two_mappings_with_different_read_start_row(self):
        input_data = [
            ["oc1", "oc2", "parameter_class1", "parameter_class2"],
            [" ", " ", " ", " "],
            ["oc1_obj1", "oc2_obj1", 1, 3],
            ["oc1_obj2", "oc2_obj2", 2, 4],
        ]
        expected = {
            "object_classes": ["oc1", "oc2"],
            "objects": [("oc1", "oc1_obj1"), ("oc1", "oc1_obj2"), ("oc2", "oc2_obj2")],
            "object_parameters": [("oc1", "parameter_class1"), ("oc2", "parameter_class2")],
            "object_parameter_values": [
                ("oc1", "oc1_obj1", "parameter_class1", 1),
                ("oc1", "oc1_obj2", "parameter_class1", 2),
                ("oc2", "oc2_obj2", "parameter_class2", 4),
            ],
        }

        data = iter(input_data)
        data_header = next(data)

        mapping1 = {
            "map_type": "ObjectClass",
            "name": {"map_type": "column_header", "reference": 0},
            "object": 0,
            "parameters": {"map_type": "parameter", "name": {"map_type": "column_header", "reference": 2}, "value": 2},
            "read_start_row": 1,
        }
        mapping2 = {
            "map_type": "ObjectClass",
            "name": {"map_type": "column_header", "reference": 1},
            "object": 1,
            "parameters": {"map_type": "parameter", "name": {"map_type": "column_header", "reference": 3}, "value": 3},
            "read_start_row": 2,
        }

        out, errors = get_mapped_data(data, [mapping1, mapping2], data_header)
        self._assert_equivalent(out, expected)
        self.assertEqual(errors, [])

    def test_read_object_class_with_table_name_as_class_name(self):
        input_data = [["Object names"], ["object 1"], ["object 2"]]
        data = iter(input_data)
        data_header = next(data)
        mapping = {
            "map_type": "ObjectClass",
            "name": {"map_type": "table_name", "reference": "class name"},
            "object": 0,
        }
        out, errors = get_mapped_data(data, [mapping], data_header, "class name")
        expected = dict()
        expected["object_classes"] = ["class name"]
        expected["objects"] = [("class name", "object 1"), ("class name", "object 2")]
        self.assertFalse(errors)
        self._assert_equivalent(out, expected)

    def test_read_flat_map_from_columns(self):
        input_data = [["Index", "Value"], ["key1", -2], ["key2", -1]]
        data = iter(input_data)
        data_header = next(data)
        mapping = {
            "map_type": "ObjectClass",
            "name": "object_class",
            "object": "object",
            "parameters": {
                "name": "parameter",
                "parameter_type": "map",
                "value": 1,
                "compress": False,
                "extra_dimensions": [0],
            },
        }
        out, errors = get_mapped_data(data, [mapping], data_header)
        expected = dict()
        expected["object_classes"] = ["object_class"]
        expected["objects"] = [("object_class", "object")]
        expected_map = Map(["key1", "key2"], [-2, -1])
        expected["object_parameter_values"] = [("object_class", "object", "parameter", expected_map)]
        expected["object_parameters"] = [("object_class", "parameter")]
        self.assertFalse(errors)
        self._assert_equivalent(out, expected)

    def test_read_nested_map_from_columns(self):
        input_data = [["Index 1", "Index 2", "Value"], ["key11", "key12", -2], ["key21", "key22", -1]]
        data = iter(input_data)
        data_header = next(data)
        mapping = {
            "map_type": "ObjectClass",
            "name": "object_class",
            "object": "object",
            "parameters": {
                "name": "parameter",
                "parameter_type": "map",
                "value": 2,
                "compress": False,
                "extra_dimensions": [0, 1],
            },
        }
        out, errors = get_mapped_data(data, [mapping], data_header)
        expected = dict()
        expected["object_classes"] = ["object_class"]
        expected["objects"] = [("object_class", "object")]
        expected_map = Map(["key11", "key21"], [Map(["key12"], [-2]), Map(["key22"], [-1])])
        expected["object_parameter_values"] = [("object_class", "object", "parameter", expected_map)]
        expected["object_parameters"] = [("object_class", "parameter")]
        self.assertFalse(errors)
        self._assert_equivalent(out, expected)

    def test_read_uneven_nested_map_from_columns(self):
        input_data = [
            ["Index", "A", "B", "C"],
            ["key1", "key11", -2, ""],
            ["key1", "key12", -1, ""],
            ["key2", -23, "", ""],
            ["key3", -33, "", ""],
            ["key4", "key31", "key311", 50],
            ["key4", "key31", "key312", 51],
            ["key4", "key32", 66, ""],
        ]
        data = iter(input_data)
        data_header = next(data)
        mapping = {
            "map_type": "ObjectClass",
            "name": "object_class",
            "object": "object",
            "parameters": {
                "name": "parameter",
                "parameter_type": "map",
                "value": 3,
                "compress": False,
                "extra_dimensions": [0, 1, 2],
            },
        }
        out, errors = get_mapped_data(data, [mapping], data_header)
        expected = dict()
        expected["object_classes"] = ["object_class"]
        expected["objects"] = [("object_class", "object")]
        expected_map = Map(
            ["key1", "key2", "key3", "key4"],
            [
                Map(["key11", "key12"], [-2, -1]),
                -23,
                -33,
                Map(["key31", "key32"], [Map(["key311", "key312"], [50, 51]), 66]),
            ],
        )
        expected["object_parameter_values"] = [("object_class", "object", "parameter", expected_map)]
        expected["object_parameters"] = [("object_class", "parameter")]
        self.assertFalse(errors)
        self._assert_equivalent(out, expected)

    def test_read_nested_map_with_compression(self):
        input_data = [
            ["Index 1", "Time stamp", "Value"],
            ["key", DateTime("2020-09-10T08:00"), -2.0],
            ["key", DateTime("2020-09-11T08:00"), -1.0],
        ]
        data = iter(input_data)
        data_header = next(data)
        mapping = {
            "map_type": "ObjectClass",
            "name": "object_class",
            "object": "object",
            "parameters": {
                "name": "parameter",
                "parameter_type": "map",
                "value": 2,
                "compress": True,
                "extra_dimensions": [0, 1],
            },
        }
        out, errors = get_mapped_data(data, [mapping], data_header)
        expected = dict()
        expected["object_classes"] = ["object_class"]
        expected["objects"] = [("object_class", "object")]
        expected_map = Map(
            ["key"],
            [TimeSeriesVariableResolution(["2020-09-10T08:00", "2020-09-11T08:00"], [-2.0, -1.0], False, False)],
        )
        expected["object_parameter_values"] = [("object_class", "object", "parameter", expected_map)]
        expected["object_parameters"] = [("object_class", "parameter")]
        self.assertFalse(errors)
        self._assert_equivalent(out, expected)

    def test_read_alternative(self):
        input_data = [["Alternatives"], ["alternative1"], ["second_alternative"], ["last_one"]]
        data = iter(input_data)
        data_header = next(data)
        mapping = {"map_type": "Alternative", "name": 0}
        out, errors = get_mapped_data(data, [mapping], data_header)
        expected = {"alternatives": ["alternative1", "second_alternative", "last_one"]}
        self._assert_equivalent(out, expected)
        self.assertFalse(errors)

    def test_read_scenario(self):
        input_data = [["Scenarios"], ["scenario1"], ["second_scenario"], ["last_one"]]
        data = iter(input_data)
        data_header = next(data)
        mapping = {"map_type": "Scenario", "name": 0}
        out, errors = get_mapped_data(data, [mapping], data_header)
        expected = dict()
        expected["scenarios"] = [("scenario1", False), ("second_scenario", False), ("last_one", False)]
        self._assert_equivalent(out, expected)
        self.assertFalse(errors)

    def test_read_scenario_with_active_flags(self):
        input_data = [["Scenarios", "Active"], ["scenario1", 1], ["second_scenario", "f"], ["last_one", "true"]]
        data = iter(input_data)
        data_header = next(data)
        mapping = {"map_type": "Scenario", "name": 0, "active": 1}
        out, errors = get_mapped_data(data, [mapping], data_header)
        expected = dict()
        expected["scenarios"] = [("scenario1", True), ("second_scenario", False), ("last_one", True)]
        self._assert_equivalent(out, expected)
        self.assertFalse(errors)

    def test_read_scenario_alternative(self):
        input_data = [
            ["Scenario", "Alternative", "Before alternative"],
            ["scenario_A", "alternative1", "second_alternative"],
            ["scenario_A", "second_alternative", "last_one"],
            ["scenario_B", "last_one", ""],
        ]
        data = iter(input_data)
        data_header = next(data)
        mapping = {
            "map_type": "ScenarioAlternative",
            "scenario_name": 0,
            "alternative_name": 1,
            "before_alternative_name": 2,
        }
        out, errors = get_mapped_data(data, [mapping], data_header)
        expected = dict()
        expected["scenario_alternatives"] = [
            ("scenario_A", "alternative1", "second_alternative"),
            ("scenario_A", "second_alternative", "last_one"),
            ("scenario_B", "last_one", ""),
        ]
        self._assert_equivalent(out, expected)
        self.assertFalse(errors)

    def test_read_tool(self):
        input_data = [["Tools"], ["tool1"], ["second_tool"], ["last_one"]]
        data = iter(input_data)
        data_header = next(data)
        mapping = {"map_type": "Tool", "name": 0}
        out, errors = get_mapped_data(data, [mapping], data_header)
        expected = dict()
        expected["tools"] = ["tool1", "second_tool", "last_one"]
        self._assert_equivalent(out, expected)
        self.assertFalse(errors)

    def test_read_feature(self):
        input_data = [["Class", "Parameter"], ["class1", "param1"], ["class2", "param2"]]
        data = iter(input_data)
        data_header = next(data)
        mapping = {"map_type": "Feature", "entity_class_name": 0, "parameter_definition_name": 1}
        out, errors = get_mapped_data(data, [mapping], data_header)
        expected = {"features": [("class1", "param1"), ("class2", "param2")]}
        self._assert_equivalent(out, expected)
        self.assertFalse(errors)

    def test_read_tool_feature(self):
        input_data = [["Tool", "Class", "Parameter"], ["tool1", "class1", "param1"], ["tool2", "class2", "param2"]]
        data = iter(input_data)
        data_header = next(data)
        mapping = {"map_type": "ToolFeature", "name": 0, "entity_class_name": 1, "parameter_definition_name": 2}
        out, errors = get_mapped_data(data, [mapping], data_header)
        expected = dict()
        expected["tool_features"] = [("tool1", "class1", "param1", False), ("tool2", "class2", "param2", False)]
        self._assert_equivalent(out, expected)
        self.assertFalse(errors)

    def test_read_tool_feature_with_required_flag(self):
        input_data = [
            ["Tool", "Class", "Parameter", "Required"],
            ["tool1", "class1", "param1", "f"],
            ["tool2", "class2", "param2", "true"],
        ]
        data = iter(input_data)
        data_header = next(data)
        mapping = {
            "map_type": "ToolFeature",
            "name": 0,
            "entity_class_name": 1,
            "parameter_definition_name": 2,
            "required": 3,
        }
        out, errors = get_mapped_data(data, [mapping], data_header)
        expected = dict()
        expected["tool_features"] = [("tool1", "class1", "param1", False), ("tool2", "class2", "param2", True)]
        self._assert_equivalent(out, expected)
        self.assertFalse(errors)

    def test_read_tool_feature_method(self):
        input_data = [
            ["Tool", "Class", "Parameter", "Method"],
            ["tool1", "class1", "param1", "meth1"],
            ["tool2", "class2", "param2", "meth2"],
        ]
        data = iter(input_data)
        data_header = next(data)
        mapping = {
            "map_type": "ToolFeatureMethod",
            "name": 0,
            "entity_class_name": 1,
            "parameter_definition_name": 2,
            "method": 3,
        }
        out, errors = get_mapped_data(data, [mapping], data_header)
        expected = dict()
        expected["tool_feature_methods"] = [
            ("tool1", "class1", "param1", "meth1"),
            ("tool2", "class2", "param2", "meth2"),
        ]
        self._assert_equivalent(out, expected)
        self.assertFalse(errors)

    def test_read_object_group_without_parameters(self):
        input_data = [
            ["Object Class", "Group", "Object"],
            ["class_A", "group1", "object1"],
            ["class_A", "group1", "object2"],
            ["class_A", "group2", "object3"],
        ]
        data = iter(input_data)
        data_header = next(data)
        mapping = {"map_type": "ObjectGroup", "name": 0, "groups": 1, "members": 2}
        out, errors = get_mapped_data(data, [mapping], data_header)
        expected = dict()
        expected["object_classes"] = ["class_A", "class_A", "class_A"]
        expected["object_groups"] = [
            ("class_A", "group1", "object1"),
            ("class_A", "group1", "object2"),
            ("class_A", "group2", "object3"),
        ]
        self._assert_equivalent(out, expected)
        self.assertFalse(errors)

    def test_read_object_group_and_import_objects(self):
        input_data = [
            ["Object Class", "Group", "Object"],
            ["class_A", "group1", "object1"],
            ["class_A", "group1", "object2"],
            ["class_A", "group2", "object3"],
        ]
        data = iter(input_data)
        data_header = next(data)
        mapping = {"map_type": "ObjectGroup", "name": 0, "groups": 1, "members": 2, "import_objects": True}
        out, errors = get_mapped_data(data, [mapping], data_header)
        expected = dict()
        expected["object_groups"] = [
            ("class_A", "group1", "object1"),
            ("class_A", "group1", "object2"),
            ("class_A", "group2", "object3"),
        ]
        expected["object_classes"] = ["class_A", "class_A", "class_A"]
        expected["objects"] = [
            ("class_A", "group1"),
            ("class_A", "object1"),
            ("class_A", "group1"),
            ("class_A", "object2"),
            ("class_A", "group2"),
            ("class_A", "object3"),
        ]
        self._assert_equivalent(out, expected)
        self.assertFalse(errors)

    def test_read_object_group_with_parameters(self):
        input_data = [
            ["Object Class", "Group", "Object", "Speed"],
            ["class_A", "group1", "object1", 23.0],
            ["class_A", "group1", "object2", 42.0],
            ["class_A", "group2", "object3", 5.0],
        ]
        data = iter(input_data)
        data_header = next(data)
        mapping = {
            "map_type": "ObjectGroup",
            "name": 0,
            "groups": 1,
            "members": 2,
            "parameters": {"name": "speed", "parameter_type": "single value", "value": 3},
        }
        out, errors = get_mapped_data(data, [mapping], data_header)
        expected = dict()
        expected["object_groups"] = [
            ("class_A", "group1", "object1"),
            ("class_A", "group1", "object2"),
            ("class_A", "group2", "object3"),
        ]
        expected["object_classes"] = ["class_A", "class_A", "class_A"]
        expected["object_parameters"] = [("class_A", "speed"), ("class_A", "speed"), ("class_A", "speed")]
        expected["object_parameter_values"] = [
            ("class_A", "group1", "speed", 23.0),
            ("class_A", "group1", "speed", 42.0),
            ("class_A", "group2", "speed", 5.0),
        ]
        self._assert_equivalent(out, expected)
        self.assertFalse(errors)

    def test_read_parameter_definition_with_default_values_and_value_lists(self):
        input_data = [
            ["Class", "Parameter", "Default", "Value list"],
            ["class_A", "param1", 23.0, "listA"],
            ["class_A", "param2", 42.0, "listB"],
            ["class_B", "param3", 5.0, "listA"],
        ]
        data = iter(input_data)
        data_header = next(data)
        mapping = {
            "map_type": "ObjectClass",
            "name": 0,
            "parameters": {
                "name": 1,
                "map_type": "ParameterDefinition",
                "default_value": {"value_type": "single value", "main_value": 2},
                "parameter_value_list_name": 3,
            },
        }
        out, errors = get_mapped_data(data, [mapping], data_header)
        expected = dict()
        expected["object_classes"] = ["class_A", "class_A", "class_B"]
        expected["object_parameters"] = [
            ("class_A", "param1", 23.0, "listA"),
            ("class_A", "param2", 42.0, "listB"),
            ("class_B", "param3", 5.0, "listA"),
        ]
        self._assert_equivalent(out, expected)
        self.assertFalse(errors)

    def test_read_parameter_definition_with_nested_map_as_default_value(self):
        input_data = [["Index 1", "Index 2", "Value"], ["key11", "key12", -2], ["key21", "key22", -1]]
        data = iter(input_data)
        data_header = next(data)
        mapping = {
            "map_type": "ObjectClass",
            "name": "object_class",
            "parameters": {
                "name": "parameter",
                "map_type": "ParameterDefinition",
                "default_value": {"value_type": "map", "main_value": 2, "compress": False, "extra_dimensions": [0, 1]},
            },
        }
        out, errors = get_mapped_data(data, [mapping], data_header)
        expected = dict()
        expected["object_classes"] = ["object_class"]
        expected_map = Map(["key11", "key21"], [Map(["key12"], [-2]), Map(["key22"], [-1])])
        expected["object_parameters"] = [("object_class", "parameter", expected_map)]
        self.assertFalse(errors)
        self._assert_equivalent(out, expected)


@unittest.skip("Obsolete, need to find an equivalent in the new API")
class TestItemMappings(unittest.TestCase):
    def test_ObjectClassMapping_dimensions_is_always_one(self):
        mapping = ObjectClassMapping()
        self.assertEqual(mapping.dimensions, 1)

    def test_RelationshipClassMapping_dimensions_equals_number_of_object_classes(self):
        mapping_dict = {
            "map_type": "RelationshipClass",
            "name": "unit__node",
            "object_classes": ["unit", "node"],
            "objects": [{"map_type": "row", "reference": i} for i in range(2)],
            "parameters": {
                "map_type": "parameter",
                "parameter_type": "time pattern",
                "name": {"map_type": "row", "reference": 2},
                "extra_dimensions": [0],
            },
        }
        mapping = RelationshipClassMapping.from_dict(mapping_dict)
        self.assertEqual(mapping.dimensions, 2)

    def test_ObjectClassMapping_import_objects_is_always_True(self):
        mapping = ObjectClassMapping()
        self.assertTrue(mapping.import_objects)

    def test_RelationshipClassMapping_import_objects_getter_and_setter(self):
        mapping = RelationshipClassMapping()
        self.assertFalse(mapping.import_objects)
        mapping.import_objects = True
        self.assertTrue(mapping.import_objects)

    def test_ObjectClassMapping_has_fixed_dimensions(self):
        mapping = ObjectClassMapping()
        self.assertTrue(mapping.has_fixed_dimensions())

    def test_RelationshipMapping_does_not_have_fixed_dimensions(self):
        mapping = RelationshipClassMapping()
        self.assertFalse(mapping.has_fixed_dimensions())

    def test_ObjectClassMapping_has_parameters(self):
        mapping = ObjectClassMapping()
        self.assertTrue(mapping.has_parameters())

    def test_RelationshipClassMapping_has_parameters(self):
        mapping = RelationshipClassMapping()
        self.assertTrue(mapping.has_parameters())

    def test_AlternativeMapping_does_not_have_parameters(self):
        mapping = AlternativeMapping()
        self.assertFalse(mapping.has_parameters())

    def test_ScenarioMapping_does_not_have_parameters(self):
        mapping = ScenarioMapping()
        self.assertFalse(mapping.has_parameters())

    def test_ScenarioAlternativeMapping_does_not_have_parameters(self):
        mapping = ScenarioAlternativeMapping()
        self.assertFalse(mapping.has_parameters())

    def test_ToolMapping_does_not_have_parameters(self):
        mapping = ToolMapping()
        self.assertFalse(mapping.has_parameters())

    def test_FeatureMapping_does_not_have_parameters(self):
        mapping = FeatureMapping()
        self.assertFalse(mapping.has_parameters())

    def test_ToolFeatureMapping_does_not_have_parameters(self):
        mapping = ToolFeatureMapping()
        self.assertFalse(mapping.has_parameters())

    def test_ToolFeatureMethodMapping_does_not_have_parameters(self):
        mapping = ToolFeatureMethodMapping()
        self.assertFalse(mapping.has_parameters())


if __name__ == "__main__":
    unittest.main()
