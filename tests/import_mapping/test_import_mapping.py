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

""" Unit tests for import Mappings. """
import unittest
from unittest.mock import Mock
from spinedb_api.exception import InvalidMapping
from spinedb_api.mapping import Position, to_dict as mapping_to_dict, unflatten
from spinedb_api.import_mapping.import_mapping import (
    default_import_mapping,
    ImportMapping,
    EntityClassMapping,
    EntityMapping,
    check_validity,
    ParameterDefinitionMapping,
    IndexNameMapping,
    ParameterValueIndexMapping,
    ExpandedParameterValueMapping,
    ParameterValueMapping,
    ParameterValueTypeMapping,
    ParameterDefaultValueTypeMapping,
    DefaultValueIndexNameMapping,
    ParameterDefaultValueIndexMapping,
    ExpandedParameterDefaultValueMapping,
    AlternativeMapping,
)
from spinedb_api.import_mapping.import_mapping_compat import (
    import_mapping_from_dict,
    parameter_mapping_from_dict,
    parameter_value_mapping_from_dict,
)
from spinedb_api.import_mapping.type_conversion import BooleanConvertSpec, StringConvertSpec, FloatConvertSpec
from spinedb_api.import_mapping.generator import get_mapped_data
from spinedb_api.parameter_value import Array, DateTime, TimeSeriesVariableResolution, Map


class TestConvertFunctions(unittest.TestCase):
    def test_convert_functions_float(self):
        data = [["a", "1.2"]]
        column_convert_fns = {0: str, 1: FloatConvertSpec()}
        mapping = import_mapping_from_dict({"map_type": "ObjectClass"})
        mapping.position = 0
        mapping.child.value = "obj"
        mapping.flatten()[-1].child = param_def_mapping = parameter_mapping_from_dict(
            {"map_type": "ParameterDefinition"}
        )
        param_def_mapping.value = "param"
        param_def_mapping.flatten()[-1].position = 1
        mapped_data, _ = get_mapped_data(data, [mapping], column_convert_fns=column_convert_fns)
        expected = {
            "entity_classes": [("a",)],
            "entities": [("a", "obj")],
            "parameter_definitions": [("a", "param", 1.2)],
        }
        self.assertEqual(mapped_data, expected)

    def test_convert_functions_str(self):
        data = [["a", '"1111.2222"']]
        column_convert_fns = {0: str, 1: StringConvertSpec()}
        mapping = import_mapping_from_dict({"map_type": "ObjectClass"})
        mapping.position = 0
        mapping.child.value = "obj"
        mapping.flatten()[-1].child = param_def_mapping = parameter_mapping_from_dict(
            {"map_type": "ParameterDefinition"}
        )
        param_def_mapping.value = "param"
        param_def_mapping.flatten()[-1].position = 1
        mapped_data, _ = get_mapped_data(data, [mapping], column_convert_fns=column_convert_fns)
        expected = {
            "entity_classes": [("a",)],
            "entities": [("a", "obj")],
            "parameter_definitions": [("a", "param", "1111.2222")],
        }
        self.assertEqual(mapped_data, expected)

    def test_convert_functions_bool(self):
        data = [["a", "false"]]
        column_convert_fns = {0: str, 1: BooleanConvertSpec()}
        mapping = import_mapping_from_dict({"map_type": "ObjectClass"})
        mapping.position = 0
        mapping.child.value = "obj"
        mapping.flatten()[-1].child = param_def_mapping = parameter_mapping_from_dict(
            {"map_type": "ParameterDefinition"}
        )
        param_def_mapping.value = "param"
        param_def_mapping.flatten()[-1].position = 1
        mapped_data, _ = get_mapped_data(data, [mapping], column_convert_fns=column_convert_fns)
        expected = {
            "entity_classes": [("a",)],
            "entities": [("a", "obj")],
            "parameter_definitions": [("a", "param", False)],
        }
        self.assertEqual(mapped_data, expected)

    def test_convert_functions_with_error(self):
        data = [["a", "not a float"]]
        column_convert_fns = {0: str, 1: FloatConvertSpec()}
        mapping = import_mapping_from_dict({"map_type": "ObjectClass"})
        mapping.position = 0
        mapping.child.value = "obj"
        mapping.flatten()[-1].child = param_def_mapping = parameter_mapping_from_dict(
            {"map_type": "ParameterDefinition"}
        )
        param_def_mapping.value = "param"
        param_def_mapping.flatten()[-1].position = 1
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

    def test_polish_column_header_mapping_duplicates(self):
        mapping = ImportMapping(Position.header, value=3)
        table_name = "tablename"
        header = ["A", "B", "C", "A"]
        mapping.polish(table_name, header, for_preview=True)
        self.assertEqual(mapping.position, Position.header)
        self.assertEqual(mapping.value, 3)

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
        expected = ["EntityClass", "Entity", "EntityMetadata"]
        self.assertEqual(types, expected)

    def test_relationship_class_mapping(self):
        mapping = import_mapping_from_dict({"map_type": "RelationshipClass"})
        d = mapping_to_dict(mapping)
        types = [m["map_type"] for m in d]
        expected = ["EntityClass", "Dimension", "Entity", "Element", "EntityMetadata"]
        self.assertEqual(types, expected)

    def test_object_group_mapping(self):
        mapping = import_mapping_from_dict({"map_type": "ObjectGroup"})
        d = mapping_to_dict(mapping)
        types = [m["map_type"] for m in d]
        expected = ["EntityClass", "Entity", "EntityGroup"]
        self.assertEqual(types, expected)

    def test_alternative_mapping(self):
        mapping = import_mapping_from_dict({"map_type": "Alternative"})
        d = mapping_to_dict(mapping)
        types = [m["map_type"] for m in d]
        expected = ["Alternative"]
        self.assertEqual(types, expected)

    def test_scenario_mapping(self):
        mapping = import_mapping_from_dict({"map_type": "Scenario"})
        d = mapping_to_dict(mapping)
        types = [m["map_type"] for m in d]
        expected = ["Scenario", "ScenarioActiveFlag"]
        self.assertEqual(types, expected)

    def test_scenario_alternative_mapping(self):
        mapping = import_mapping_from_dict({"map_type": "ScenarioAlternative"})
        d = mapping_to_dict(mapping)
        types = [m["map_type"] for m in d]
        expected = ["Scenario", "ScenarioAlternative", "ScenarioBeforeAlternative"]
        self.assertEqual(types, expected)

    def test_tool_mapping(self):
        with self.assertRaises(ValueError):
            import_mapping_from_dict({"map_type": "Tool"})

    def test_tool_feature_mapping(self):
        with self.assertRaises(ValueError):
            import_mapping_from_dict({"map_type": "ToolFeature"})

    def test_tool_feature_method_mapping(self):
        with self.assertRaises(ValueError):
            import_mapping_from_dict({"map_type": "ToolFeatureMethod"})

    def test_parameter_value_list_mapping(self):
        mapping = import_mapping_from_dict({"map_type": "ParameterValueList"})
        d = mapping_to_dict(mapping)
        types = [m["map_type"] for m in d]
        expected = ["ParameterValueList", "ParameterValueListValue"]
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
            {"map_type": "EntityClass", "position": 0},
            {"map_type": "Entity", "position": 1},
            {"map_type": "EntityMetadata", "position": "hidden"},
            {"map_type": "ParameterDefinition", "position": 2},
            {"map_type": "Alternative", "position": "hidden"},
            {"map_type": "ParameterValueMetadata", "position": "hidden"},
            {"map_type": "ParameterValue", "position": 3},
        ]
        self.assertEqual(out, expected)

    def test_ObjectClass_object_from_dict_to_dict(self):
        mapping = {"map_type": "ObjectClass", "name": 0, "objects": 1}
        mapping = import_mapping_from_dict(mapping)
        out = mapping_to_dict(mapping)
        expected = [
            {"map_type": "EntityClass", "position": 0},
            {"map_type": "Entity", "position": 1},
            {"map_type": "EntityMetadata", "position": "hidden"},
        ]
        self.assertEqual(out, expected)

    def test_ObjectClass_object_from_dict_to_dict2(self):
        mapping = {"map_type": "ObjectClass", "name": "cls", "objects": "obj"}
        mapping = import_mapping_from_dict(mapping)
        out = mapping_to_dict(mapping)
        expected = [
            {"map_type": "EntityClass", "position": "hidden", "value": "cls"},
            {"map_type": "Entity", "position": "hidden", "value": "obj"},
            {"map_type": "EntityMetadata", "position": "hidden"},
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
            {"map_type": "EntityClass", "position": "hidden", "value": "unit__node"},
            {"map_type": "Dimension", "position": 0},
            {"map_type": "Dimension", "position": 1},
            {"map_type": "Entity", "position": "hidden"},
            {"map_type": "Element", "position": 0},
            {"map_type": "Element", "position": 1},
            {"map_type": "EntityMetadata", "position": "hidden"},
            {"map_type": "ParameterDefinition", "position": "hidden", "value": "pname"},
            {"map_type": "Alternative", "position": "hidden"},
            {"map_type": "ParameterValueMetadata", "position": "hidden"},
            {"map_type": "ParameterValue", "position": 2},
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
            {"map_type": "EntityClass", "position": "hidden", "value": "unit__node"},
            {"map_type": "Dimension", "position": "hidden", "value": "cls"},
            {"map_type": "Dimension", "position": 0},
            {"map_type": "Entity", "position": "hidden"},
            {"map_type": "Element", "position": "hidden", "value": "obj"},
            {"map_type": "Element", "position": 0},
            {"map_type": "EntityMetadata", "position": "hidden"},
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
            {"map_type": "EntityClass", "position": "hidden", "value": "unit__node"},
            {"map_type": "Dimension", "position": "hidden"},
            {"map_type": "Entity", "position": "hidden"},
            {"map_type": "Element", "position": "hidden"},
            {"map_type": "EntityMetadata", "position": "hidden"},
            {"map_type": "ParameterDefinition", "position": "hidden", "value": "pname"},
            {"map_type": "Alternative", "position": "hidden"},
            {"map_type": "ParameterValueMetadata", "position": "hidden"},
            {"map_type": "ParameterValueType", "position": "hidden", "value": "array"},
            {"map_type": "IndexName", "position": "hidden"},
            {"map_type": "ParameterValueIndex", "position": "hidden", "value": "dim"},
            {"map_type": "ExpandedValue", "position": 2},
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
            {"map_type": "EntityClass", "position": 0},
            {"map_type": "Entity", "position": 1},
            {"map_type": "EntityGroup", "position": 2},
        ]
        self.assertEqual(out, expected)

    def test_Alternative_to_dict_from_dict(self):
        mapping = {"map_type": "Alternative", "name": 0}
        mapping = import_mapping_from_dict(mapping)
        out = mapping_to_dict(mapping)
        expected = [{"map_type": "Alternative", "position": 0}]
        self.assertEqual(out, expected)

    def test_Scenario_to_dict_from_dict(self):
        mapping = {"map_type": "Scenario", "name": 0}
        mapping = import_mapping_from_dict(mapping)
        out = mapping_to_dict(mapping)
        expected = [
            {"map_type": "Scenario", "position": 0},
            {"map_type": "ScenarioActiveFlag", "position": "hidden", "value": "false"},
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
            {"map_type": "Scenario", "position": 0},
            {"map_type": "ScenarioAlternative", "position": 1},
            {"map_type": "ScenarioBeforeAlternative", "position": 2},
        ]
        self.assertEqual(out, expected)

    def test_Tool_to_dict_from_dict(self):
        mapping = {"map_type": "Tool", "name": 0}
        with self.assertRaises(ValueError):
            import_mapping_from_dict(mapping)

    def test_Feature_to_dict_from_dict(self):
        mapping = {"map_type": "Feature", "entity_class_name": 0, "parameter_definition_name": 1}
        with self.assertRaises(ValueError):
            import_mapping_from_dict(mapping)

    def test_ToolFeature_to_dict_from_dict(self):
        mapping = {"map_type": "ToolFeature", "name": 0, "entity_class_name": 1, "parameter_definition_name": 2}
        with self.assertRaises(ValueError):
            import_mapping_from_dict(mapping)

    def test_ToolFeatureMethod_to_dict_from_dict(self):
        mapping = {
            "map_type": "ToolFeatureMethod",
            "name": 0,
            "entity_class_name": 1,
            "parameter_definition_name": 2,
            "method": 3,
        }
        with self.assertRaises(ValueError):
            import_mapping_from_dict(mapping)

    def test_MapValueMapping_from_dict_to_dict(self):
        mapping_dict = {
            "value_type": "map",
            "main_value": {"reference": 23, "map_type": "row"},
            "extra_dimensions": [{"reference": "fifth column", "map_type": "column"}],
            "compress": True,
        }
        parameter_mapping = parameter_value_mapping_from_dict(mapping_dict)
        out = mapping_to_dict(parameter_mapping)
        expected = [
            {"map_type": "ParameterValueType", "position": "hidden", "value": "map", "compress": True},
            {"map_type": "IndexName", "position": "hidden"},
            {"map_type": "ParameterValueIndex", "position": "fifth column"},
            {"map_type": "ExpandedValue", "position": -24},
        ]
        self.assertEqual(out, expected)

    def test_TimeSeriesValueMapping_from_dict_to_dict(self):
        mapping_dict = {
            "value_type": "time series",
            "main_value": {"reference": 23, "map_type": "row"},
            "extra_dimensions": [{"reference": "fifth column", "map_type": "column"}],
            "options": {"repeat": True, "ignore_year": False, "fixed_resolution": False},
        }
        parameter_mapping = parameter_value_mapping_from_dict(mapping_dict)
        out = mapping_to_dict(parameter_mapping)
        expected = [
            {
                "map_type": "ParameterValueType",
                "position": "hidden",
                "value": "time_series",
                "options": {"repeat": True, "ignore_year": False, "fixed_resolution": False},
            },
            {"map_type": "IndexName", "position": "hidden"},
            {"map_type": "ParameterValueIndex", "position": "fifth column"},
            {"map_type": "ExpandedValue", "position": -24},
        ]
        self.assertEqual(out, expected)


def _parent_with_pivot(is_pivoted):
    parent = Mock()
    parent.is_pivoted.return_value = is_pivoted
    return parent


def _pivoted_parent():
    return _parent_with_pivot(True)


def _unpivoted_parent():
    return _parent_with_pivot(False)


class TestMappingIsValid(unittest.TestCase):
    def test_valid_object_class_mapping(self):
        cls_mapping = import_mapping_from_dict({"map_type": "ObjectClass"})
        issues = check_validity(cls_mapping)
        self.assertFalse(issues)

    def test_invalid_object_default_value_mapping_missing_parameter_definition(self):
        cls_mapping = import_mapping_from_dict({"map_type": "ObjectClass"})
        cls_mapping.flatten()[-1].child = parameter_mapping_from_dict({"map_type": "ParameterDefinition"})
        default_value_mapping = cls_mapping.flatten()[-1]
        cls_mapping.position = 0
        default_value_mapping.position = 1
        issues = check_validity(cls_mapping)
        self.assertTrue(issues)

    def test_valid_object_default_value_mapping_not_missing_parameter_definition(self):
        cls_mapping = import_mapping_from_dict({"map_type": "ObjectClass"})
        cls_mapping.flatten()[-1].child = param_def_mapping = parameter_mapping_from_dict(
            {"map_type": "ParameterDefinition"}
        )
        default_value_mapping = cls_mapping.flatten()[-1]
        cls_mapping.position = 0
        param_def_mapping.position = 1
        default_value_mapping.position = 2
        issues = check_validity(cls_mapping)
        self.assertFalse(issues)

    def test_invalid_object_value_list_mapping_missing_parameter_definition(self):
        cls_mapping = import_mapping_from_dict({"map_type": "ObjectClass"})
        cls_mapping.flatten()[-1].child = parameter_mapping_from_dict({"map_type": "ParameterDefinition"})
        value_list_mapping = cls_mapping.flatten()[-2]
        cls_mapping.position = 0
        value_list_mapping.position = 1
        issues = check_validity(cls_mapping)
        self.assertTrue(issues)

    def test_valid_object_value_list_mapping_not_missing_parameter_definition(self):
        cls_mapping = import_mapping_from_dict({"map_type": "ObjectClass"})
        cls_mapping.flatten()[-1].child = param_def_mapping = parameter_mapping_from_dict(
            {"map_type": "ParameterDefinition"}
        )
        value_list_mapping = cls_mapping.flatten()[-2]
        cls_mapping.position = 0
        param_def_mapping.position = 1
        value_list_mapping.position = 2
        issues = check_validity(cls_mapping)
        self.assertFalse(issues)

    def test_valid_object_default_value_mapping_hidden(self):
        cls_mapping = import_mapping_from_dict({"map_type": "ObjectClass"})
        cls_mapping.flatten()[-1].child = parameter_mapping_from_dict({"map_type": "ParameterDefinition"})
        default_value_mapping = cls_mapping.flatten()[-1]
        default_value_mapping.position = Position.hidden
        issues = check_validity(cls_mapping)
        self.assertFalse(issues)

    def test_invalid_object_parameter_value_mapping_missing_object(self):
        cls_mapping = import_mapping_from_dict({"map_type": "ObjectClass"})
        cls_mapping.flatten()[-1].child = parameter_mapping_from_dict({"map_type": "ParameterValue"})
        value_mapping = cls_mapping.flatten()[-1]
        cls_mapping.position = 0
        value_mapping.position = 1
        issues = check_validity(cls_mapping)
        self.assertTrue(issues)

    def test_invalid_object_parameter_value_mapping_missing_parameter_definition(self):
        cls_mapping = import_mapping_from_dict({"map_type": "ObjectClass"})
        object_mapping = cls_mapping.flatten()[-2]
        cls_mapping.flatten()[-1].child = parameter_mapping_from_dict({"map_type": "ParameterValue"})
        value_mapping = cls_mapping.flatten()[-1]
        cls_mapping.position = 0
        object_mapping.position = 1
        value_mapping.position = 3
        issues = check_validity(cls_mapping)
        self.assertTrue(issues)

    def test_valid_object_parameter_value_mapping(self):
        cls_mapping = import_mapping_from_dict({"map_type": "ObjectClass"})
        object_mapping = cls_mapping.flatten()[-2]
        cls_mapping.flatten()[-1].child = param_def_mapping = parameter_mapping_from_dict(
            {"map_type": "ParameterValue"}
        )
        value_mapping = cls_mapping.flatten()[-1]
        cls_mapping.position = 0
        object_mapping.position = 1
        param_def_mapping.position = 2
        value_mapping.position = 3
        issues = check_validity(cls_mapping)
        self.assertFalse(issues)

    def test_valid_object_parameter_value_mapping_hidden(self):
        cls_mapping = import_mapping_from_dict({"map_type": "ObjectClass"})
        cls_mapping.flatten()[-1].child = parameter_mapping_from_dict({"map_type": "ParameterValue"})
        value_mapping = cls_mapping.flatten()[-1]
        value_mapping.position = Position.hidden
        issues = check_validity(cls_mapping)
        self.assertFalse(issues)

    def test_valid_relationship_class_mapping(self):
        cls_mapping = import_mapping_from_dict({"map_type": "RelationshipClass"})
        issues = check_validity(cls_mapping)
        self.assertFalse(issues)

    def test_invalid_relationship_default_value_mapping_missing_parameter_definition(self):
        cls_mapping = import_mapping_from_dict({"map_type": "RelationshipClass"})
        cls_mapping.flatten()[-1].child = parameter_mapping_from_dict({"map_type": "ParameterDefinition"})
        default_value_mapping = cls_mapping.flatten()[-1]
        cls_mapping.position = 0
        default_value_mapping.position = 1
        issues = check_validity(cls_mapping)
        self.assertTrue(issues)

    def test_valid_relationship_default_value_mapping_not_missing_parameter_definition(self):
        cls_mapping = import_mapping_from_dict({"map_type": "RelationshipClass"})
        obj_cls_mapping = cls_mapping.child
        cls_mapping.flatten()[-1].child = param_def_mapping = parameter_mapping_from_dict(
            {"map_type": "ParameterDefinition"}
        )
        default_value_mapping = cls_mapping.flatten()[-1]
        cls_mapping.position = 0
        obj_cls_mapping.position = 1
        param_def_mapping.position = 2
        default_value_mapping.position = 3
        issues = check_validity(cls_mapping)
        self.assertFalse(issues)

    def test_invalid_relationship_value_list_mapping_missing_parameter_definition(self):
        cls_mapping = import_mapping_from_dict({"map_type": "RelationshipClass"})
        cls_mapping.flatten()[-1].child = parameter_mapping_from_dict({"map_type": "ParameterDefinition"})
        value_list_mapping = cls_mapping.flatten()[-2]
        cls_mapping.position = 0
        value_list_mapping.position = 1
        issues = check_validity(cls_mapping)
        self.assertTrue(issues)

    def test_valid_relationship_value_list_mapping_not_missing_parameter_definition(self):
        cls_mapping = import_mapping_from_dict({"map_type": "RelationshipClass"})
        obj_cls_mapping = cls_mapping.child
        cls_mapping.flatten()[-1].child = param_def_mapping = parameter_mapping_from_dict(
            {"map_type": "ParameterDefinition"}
        )
        value_list_mapping = cls_mapping.flatten()[-2]
        cls_mapping.position = 0
        obj_cls_mapping.position = 1
        param_def_mapping.position = 2
        value_list_mapping.position = 3
        issues = check_validity(cls_mapping)
        self.assertFalse(issues)

    def test_valid_relationship_default_value_mapping_hidden(self):
        cls_mapping = import_mapping_from_dict({"map_type": "RelationshipClass"})
        cls_mapping.flatten()[-1].child = parameter_mapping_from_dict({"map_type": "ParameterDefinition"})
        default_value_mapping = cls_mapping.flatten()[-1]
        default_value_mapping.position = Position.hidden
        issues = check_validity(cls_mapping)
        self.assertFalse(issues)

    def test_invalid_relationship_parameter_value_mapping_missing_objects(self):
        cls_mapping = import_mapping_from_dict({"map_type": "RelationshipClass"})
        cls_mapping.flatten()[-1].child = parameter_mapping_from_dict({"map_type": "ParameterValue"})
        value_mapping = cls_mapping.flatten()[-1]
        cls_mapping.position = 0
        value_mapping.position = 1
        issues = check_validity(cls_mapping)
        self.assertTrue(issues)

    def test_invalid_relationship_parameter_value_mapping_missing_parameter_definition(self):
        cls_mapping = import_mapping_from_dict({"map_type": "RelationshipClass"})
        object_mapping = cls_mapping.flatten()[-2]
        cls_mapping.flatten()[-1].child = parameter_mapping_from_dict({"map_type": "ParameterValue"})
        value_mapping = cls_mapping.flatten()[-1]
        cls_mapping.position = 0
        object_mapping.position = 1
        value_mapping.position = 3
        issues = check_validity(cls_mapping)
        self.assertTrue(issues)

    def test_valid_relationship_parameter_value_mapping(self):
        cls_mapping = import_mapping_from_dict({"map_type": "RelationshipClass"})
        obj_cls_mapping = cls_mapping.child
        object_mapping = cls_mapping.flatten()[-2]
        cls_mapping.flatten()[-1].child = param_def_mapping = parameter_mapping_from_dict(
            {"map_type": "ParameterValue"}
        )
        value_mapping = cls_mapping.flatten()[-1]
        cls_mapping.position = 0
        obj_cls_mapping.position = 1
        object_mapping.position = 2
        param_def_mapping.position = 3
        value_mapping.position = 4
        issues = check_validity(cls_mapping)
        self.assertFalse(issues)

    def test_valid_relationship_parameter_value_mapping_hidden(self):
        cls_mapping = import_mapping_from_dict({"map_type": "RelationshipClass"})
        cls_mapping.flatten()[-1].child = parameter_mapping_from_dict({"map_type": "ParameterValue"})
        value_mapping = cls_mapping.flatten()[-1]
        value_mapping.position = Position.hidden
        issues = check_validity(cls_mapping)
        self.assertFalse(issues)

    def test_valid_single_value_mapping(self):
        value_mapping = parameter_value_mapping_from_dict({"value_type": "single_value"})
        issues = check_validity(value_mapping)
        self.assertFalse(issues)

    def test_invalid_single_value_mapping_missing_parameter_definition(self):
        value_mapping = parameter_value_mapping_from_dict({"value_type": "single_value"})
        value_mapping.position = 0
        issues = check_validity(value_mapping)
        self.assertTrue(issues)

    def test_valid_array_mapping(self):
        value_mapping = parameter_value_mapping_from_dict({"value_type": "array"})
        issues = check_validity(value_mapping)
        self.assertFalse(issues)

    def test_invalid_array_mapping_missing_parameter_definition(self):
        value_mapping = parameter_value_mapping_from_dict({"value_type": "array"})
        value_mapping.flatten()[-1].position = 0
        issues = check_validity(value_mapping)
        self.assertTrue(issues)

    def test_valid_time_series_mapping(self):
        value_mapping = parameter_value_mapping_from_dict({"value_type": "time_series"})
        issues = check_validity(value_mapping)
        self.assertFalse(issues)

    def test_invalid_time_series_mapping_missing_parameter_definition(self):
        value_mapping = parameter_value_mapping_from_dict({"value_type": "time_series"})
        value_mapping.flatten()[-1].position = 0
        issues = check_validity(value_mapping)
        self.assertTrue(issues)


class TestMappingIntegration(unittest.TestCase):
    # just a placeholder test for different mapping testings

    def test_bad_mapping_type(self):
        """Tests that passing any other than a `dict` or a `mapping` to `get_mapped_data` raises `TypeError`."""
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
        expected = {"entity_classes": [("oc2",)]}

        data = iter(input_data)
        data_header = next(data)

        mapping = {"map_type": "ObjectClass", "name": 0}

        out, errors = get_mapped_data(data, [mapping], data_header)
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_iterator_with_None(self):
        input_data = [["object_class", "object", "parameter", "value"], None, ["oc2", "obj2", "parameter_name2", 2]]
        expected = {"entity_classes": [("oc2",)]}

        data = iter(input_data)
        data_header = next(data)

        mapping = {"map_type": "ObjectClass", "name": 0}

        out, errors = get_mapped_data(data, [mapping], data_header)
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_flat_file(self):
        input_data = [
            ["object_class", "object", "parameter", "value"],
            ["oc1", "obj1", "parameter_name1", 1],
            ["oc2", "obj2", "parameter_name2", 2],
        ]
        expected = {
            "entity_classes": [("oc1",), ("oc2",)],
            "entities": [("oc1", "obj1"), ("oc2", "obj2")],
            "parameter_definitions": [("oc1", "parameter_name1"), ("oc2", "parameter_name2")],
            "parameter_values": [["oc1", "obj1", "parameter_name1", 1], ["oc2", "obj2", "parameter_name2", 2]],
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
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_flat_file_array(self):
        input_data = [
            ["object_class", "object", "parameter", "value"],
            ["oc1", "obj1", "parameter_name1", 1],
            ["oc1", "obj1", "parameter_name1", 2],
        ]
        expected = {
            "entity_classes": [("oc1",)],
            "entities": [("oc1", "obj1")],
            "parameter_definitions": [("oc1", "parameter_name1")],
            "parameter_values": [["oc1", "obj1", "parameter_name1", Array([1, 2])]],
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
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_flat_file_array_with_ed(self):
        input_data = [
            ["object_class", "object", "parameter", "value", "value_order"],
            ["oc1", "obj1", "parameter_name1", 1, 0],
            ["oc1", "obj1", "parameter_name1", 2, 1],
        ]
        expected = {
            "entity_classes": [("oc1",)],
            "entities": [("oc1", "obj1")],
            "parameter_definitions": [("oc1", "parameter_name1")],
            "parameter_values": [["oc1", "obj1", "parameter_name1", Array([1, 2])]],
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
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_flat_file_with_column_name_reference(self):
        input_data = [["object", "parameter", "value"], ["obj1", "parameter_name1", 1], ["obj2", "parameter_name2", 2]]
        expected = {"entity_classes": [("object",)], "entities": [("object", "obj1"), ("object", "obj2")]}

        data = iter(input_data)
        data_header = next(data)

        mapping = {"map_type": "ObjectClass", "name": {"map_type": "column_name", "reference": 0}, "object": 0}

        out, errors = get_mapped_data(data, [mapping], data_header)
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_object_class_from_header_using_string_as_integral_index(self):
        input_data = [["object_class"], ["obj1"], ["obj2"]]
        expected = {
            "entity_classes": [("object_class",)],
            "entities": [("object_class", "obj1"), ("object_class", "obj2")],
        }

        data = iter(input_data)
        data_header = next(data)

        mapping = {"map_type": "ObjectClass", "name": {"map_type": "column_header", "reference": "0"}, "object": 0}

        out, errors = get_mapped_data(data, [mapping], data_header)
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_object_class_from_header_using_string_as_column_header_name(self):
        input_data = [["object_class"], ["obj1"], ["obj2"]]
        expected = {
            "entity_classes": [("object_class",)],
            "entities": [("object_class", "obj1"), ("object_class", "obj2")],
        }

        data = iter(input_data)
        data_header = next(data)

        mapping = {
            "map_type": "ObjectClass",
            "name": {"map_type": "column_header", "reference": "object_class"},
            "object": 0,
        }

        out, errors = get_mapped_data(data, [mapping], data_header)
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_with_list_of_mappings(self):
        input_data = [["object", "parameter", "value"], ["obj1", "parameter_name1", 1], ["obj2", "parameter_name2", 2]]
        expected = {"entity_classes": [("object",)], "entities": [("object", "obj1"), ("object", "obj2")]}

        data = iter(input_data)
        data_header = next(data)

        mapping = {"map_type": "ObjectClass", "name": {"map_type": "column_header", "reference": 0}, "object": 0}

        out, errors = get_mapped_data(data, [mapping], data_header)
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_pivoted_parameters_from_header(self):
        input_data = [["object", "parameter_name1", "parameter_name2"], ["obj1", 0, 1], ["obj2", 2, 3]]
        expected = {
            "entity_classes": [("object",)],
            "entities": [("object", "obj1"), ("object", "obj2")],
            "parameter_definitions": [("object", "parameter_name1"), ("object", "parameter_name2")],
            "parameter_values": [
                ["object", "obj1", "parameter_name1", 0],
                ["object", "obj1", "parameter_name2", 1],
                ["object", "obj2", "parameter_name1", 2],
                ["object", "obj2", "parameter_name2", 3],
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
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_empty_pivot(self):
        input_data = [["object", "parameter_name1", "parameter_name2"]]
        expected = {}

        data = iter(input_data)
        data_header = next(data)

        mapping = {
            "map_type": "ObjectClass",
            "name": {"map_type": "column_header", "reference": 0},
            "object": 0,
            "parameters": {"map_type": "parameter", "name": {"map_type": "row", "reference": -1}},
        }  # -1 to read pivot from header

        out, errors = get_mapped_data(data, [mapping], data_header)
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_pivoted_parameters_from_data(self):
        input_data = [["object", "parameter_name1", "parameter_name2"], ["obj1", 0, 1], ["obj2", 2, 3]]
        expected = {
            "entity_classes": [("object",)],
            "entities": [("object", "obj1"), ("object", "obj2")],
            "parameter_definitions": [("object", "parameter_name1"), ("object", "parameter_name2")],
            "parameter_values": [
                ["object", "obj1", "parameter_name1", 0],
                ["object", "obj1", "parameter_name2", 1],
                ["object", "obj2", "parameter_name1", 2],
                ["object", "obj2", "parameter_name2", 3],
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
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_pivoted_value_has_actual_position(self):
        """Pivoted mapping works even when last mapping has valid position in columns."""
        input_data = [
            ["object", "timestep", "value"],
            ["obj1", "T1", 11.0],
            ["obj1", "T2", 12.0],
            ["obj2", "T1", 21.0],
            ["obj2", "T2", 22.0],
        ]
        expected = {
            "entity_classes": [("timeline",)],
            "entities": [("timeline", "obj1"), ("timeline", "obj2")],
            "parameter_definitions": [("timeline", "value")],
            "alternatives": {"Base"},
            "parameter_values": [
                ["timeline", "obj1", "value", Map(["T1", "T2"], [11.0, 12.0], index_name="timestep"), "Base"],
                ["timeline", "obj2", "value", Map(["T1", "T2"], [21.0, 22.0], index_name="timestep"), "Base"],
            ],
        }
        data = iter(input_data)
        mapping_dicts = [
            {"map_type": "ObjectClass", "position": "hidden", "value": "timeline"},
            {"map_type": "Object", "position": 0},
            {"map_type": "ObjectMetadata", "position": "hidden"},
            {"map_type": "ParameterDefinition", "position": -1},
            {"map_type": "Alternative", "position": "hidden", "value": "Base"},
            {"map_type": "ParameterValueMetadata", "position": "hidden"},
            {"map_type": "ParameterValueType", "position": "hidden", "value": "map"},
            {"map_type": "IndexName", "position": "hidden", "value": "timestep"},
            {"map_type": "ParameterValueIndex", "position": 1},
            {"map_type": "ExpandedValue", "position": 2},  # This caused import to fail
        ]
        out, errors = get_mapped_data(data, [mapping_dicts])
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_import_objects_from_pivoted_data_when_they_lack_parameter_values(self):
        """Pivoted mapping works even when last mapping has valid position in columns."""
        input_data = [["object", "is_skilled", "has_powers"], ["obj1", "yes", "no"], ["obj2", None, None]]
        expected = {
            "entity_classes": [("node",)],
            "entities": [("node", "obj1"), ("node", "obj2")],
            "parameter_definitions": [("node", "is_skilled"), ("node", "has_powers")],
            "alternatives": {"Base"},
            "parameter_values": [
                ["node", "obj1", "is_skilled", "yes", "Base"],
                ["node", "obj1", "has_powers", "no", "Base"],
            ],
        }
        data = iter(input_data)
        mapping_dicts = [
            {"map_type": "ObjectClass", "position": "hidden", "value": "node"},
            {"map_type": "Object", "position": 0},
            {"map_type": "ObjectMetadata", "position": "hidden"},
            {"map_type": "ParameterDefinition", "position": -1},
            {"map_type": "Alternative", "position": "hidden", "value": "Base"},
            {"map_type": "ParameterValueMetadata", "position": "hidden"},
            {"map_type": "ParameterValue", "position": "hidden"},
        ]
        out, errors = get_mapped_data(data, [mapping_dicts])
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_import_objects_from_pivoted_data_when_they_lack_map_type_parameter_values(self):
        """Pivoted mapping works even when last mapping has valid position in columns."""
        input_data = [
            ["object", "my_index", "is_skilled", "has_powers"],
            ["obj1", "yesterday", None, "no"],
            ["obj1", "today", None, "yes"],
        ]
        expected = {
            "entity_classes": [("node",)],
            "entities": [("node", "obj1")],
            "parameter_definitions": [("node", "is_skilled"), ("node", "has_powers")],
            "alternatives": {"Base"},
            "parameter_values": [
                ["node", "obj1", "has_powers", Map(["yesterday", "today"], ["no", "yes"], index_name="period"), "Base"]
            ],
        }
        data = iter(input_data)
        mapping_dicts = [
            {"map_type": "ObjectClass", "position": "hidden", "value": "node"},
            {"map_type": "Object", "position": 0},
            {"map_type": "ObjectMetadata", "position": "hidden"},
            {"map_type": "ParameterDefinition", "position": -1},
            {"map_type": "Alternative", "position": "hidden", "value": "Base"},
            {"map_type": "ParameterValueMetadata", "position": "hidden"},
            {"map_type": "ParameterValueType", "position": "hidden", "value": "map"},
            {"map_type": "IndexName", "position": "hidden", "value": "period"},
            {"map_type": "ParameterValueIndex", "position": 1},
            {"map_type": "ExpandedValue", "position": "hidden"},
        ]
        out, errors = get_mapped_data(data, [mapping_dicts])
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_flat_file_with_extra_value_dimensions(self):
        input_data = [["object", "time", "parameter_name1"], ["obj1", "2018-01-01", 1], ["obj1", "2018-01-02", 2]]

        expected = {
            "entity_classes": [("object",)],
            "entities": [("object", "obj1")],
            "parameter_definitions": [("object", "parameter_name1")],
            "parameter_values": [
                [
                    "object",
                    "obj1",
                    "parameter_name1",
                    TimeSeriesVariableResolution(["2018-01-01", "2018-01-02"], [1, 2], False, False),
                ]
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
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_flat_file_with_parameter_definition(self):
        input_data = [["object", "time", "parameter_name1"], ["obj1", "2018-01-01", 1], ["obj1", "2018-01-02", 2]]

        expected = {
            "entity_classes": [("object",)],
            "entities": [("object", "obj1")],
            "parameter_definitions": [("object", "parameter_name1")],
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
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_1dim_relationships(self):
        input_data = [["unit", "node"], ["u1", "n1"], ["u1", "n2"]]
        expected = {
            "entity_classes": [("node_group", ("node",))],
            "entities": [("node_group", ("n1",)), ("node_group", ("n2",))],
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
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_relationships(self):
        input_data = [["unit", "node"], ["u1", "n1"], ["u1", "n2"]]
        expected = {
            "entity_classes": [("unit__node", ("unit", "node"))],
            "entities": [("unit__node", ("u1", "n1")), ("unit__node", ("u1", "n2"))],
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
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_relationships_with_parameters(self):
        input_data = [["unit", "node", "rel_parameter"], ["u1", "n1", 0], ["u1", "n2", 1]]
        expected = {
            "entity_classes": [("unit__node", ("unit", "node"))],
            "entities": [("unit__node", ("u1", "n1")), ("unit__node", ("u1", "n2"))],
            "parameter_definitions": [("unit__node", "rel_parameter")],
            "parameter_values": [
                ["unit__node", ("u1", "n1"), "rel_parameter", 0],
                ["unit__node", ("u1", "n2"), "rel_parameter", 1],
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
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_relationships_with_parameters2(self):
        input_data = [["nuts2", "Capacity", "Fueltype"], ["BE23", 268.0, "Bioenergy"], ["DE11", 14.0, "Bioenergy"]]
        expected = {
            "entity_classes": [("nuts2",), ("fueltype",), ("nuts2__fueltype", ("nuts2", "fueltype"))],
            "entities": [
                ("nuts2", "BE23"),
                ("fueltype", "Bioenergy"),
                ("nuts2__fueltype", ("BE23", "Bioenergy")),
                ("nuts2", "DE11"),
                ("nuts2__fueltype", ("DE11", "Bioenergy")),
            ],
            "parameter_definitions": [("nuts2__fueltype", "capacity")],
            "parameter_values": [
                ["nuts2__fueltype", ("BE23", "Bioenergy"), "capacity", 268.0],
                ["nuts2__fueltype", ("DE11", "Bioenergy"), "capacity", 14.0],
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
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_parameter_header_with_only_one_parameter(self):
        input_data = [["object", "parameter_name1"], ["obj1", 0], ["obj2", 2]]
        expected = {
            "entity_classes": [("object",)],
            "entities": [("object", "obj1"), ("object", "obj2")],
            "parameter_definitions": [("object", "parameter_name1")],
            "parameter_values": [
                ["object", "obj1", "parameter_name1", 0],
                ["object", "obj2", "parameter_name1", 2],
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
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_pivoted_parameters_from_data_with_skipped_column(self):
        input_data = [["object", "parameter_name1", "parameter_name2"], ["obj1", 0, 1], ["obj2", 2, 3]]
        expected = {
            "entity_classes": [("object",)],
            "entities": [("object", "obj1"), ("object", "obj2")],
            "parameter_definitions": [("object", "parameter_name1")],
            "parameter_values": [
                ["object", "obj1", "parameter_name1", 0],
                ["object", "obj2", "parameter_name1", 2],
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
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_relationships_and_import_objects(self):
        input_data = [["unit", "node"], ["u1", "n1"], ["u2", "n2"]]
        expected = {
            "entity_classes": [("unit",), ("node",), ("unit__node", ("unit", "node"))],
            "entities": [
                ("unit", "u1"),
                ("node", "n1"),
                ("unit__node", ("u1", "n1")),
                ("unit", "u2"),
                ("node", "n2"),
                ("unit__node", ("u2", "n2")),
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
            "import_objects": True,
        }

        out, errors = get_mapped_data(data, [mapping], data_header)
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_relationships_parameter_values_with_extra_dimensions(self):
        input_data = [["", "a", "b"], ["", "c", "d"], ["", "e", "f"], ["a", 2, 3], ["b", 4, 5]]

        expected = {
            "entity_classes": [("unit__node", ("unit", "node"))],
            "parameter_definitions": [("unit__node", "e"), ("unit__node", "f")],
            "entities": [("unit__node", ("a", "c")), ("unit__node", ("b", "d"))],
            "parameter_values": [
                ["unit__node", ("a", "c"), "e", Map(["a", "b"], [2, 4])],
                ["unit__node", ("b", "d"), "f", Map(["a", "b"], [3, 5])],
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
                "parameter_type": "map",
                "name": {"map_type": "row", "reference": 2},
                "extra_dimensions": [0],
            },
        }

        out, errors = get_mapped_data(data, [mapping], data_header)
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_data_with_read_start_row(self):
        input_data = [
            ["object_class", "object", "parameter", "value"],
            [" ", " ", " ", " "],
            ["oc1", "obj1", "parameter_name1", 1],
            ["oc2", "obj2", "parameter_name2", 2],
        ]
        expected = {
            "entity_classes": [("oc1",), ("oc2",)],
            "entities": [("oc1", "obj1"), ("oc2", "obj2")],
            "parameter_definitions": [("oc1", "parameter_name1"), ("oc2", "parameter_name2")],
            "parameter_values": [["oc1", "obj1", "parameter_name1", 1], ["oc2", "obj2", "parameter_name2", 2]],
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
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_data_with_two_mappings_with_different_read_start_row(self):
        input_data = [
            ["oc1", "oc2", "parameter_class1", "parameter_class2"],
            [" ", " ", " ", " "],
            ["oc1_obj1", "oc2_obj1", 1, 3],
            ["oc1_obj2", "oc2_obj2", 2, 4],
        ]
        expected = {
            "entity_classes": [("oc1",), ("oc2",)],
            "entities": [("oc1", "oc1_obj1"), ("oc1", "oc1_obj2"), ("oc2", "oc2_obj2")],
            "parameter_definitions": [("oc1", "parameter_class1"), ("oc2", "parameter_class2")],
            "parameter_values": [
                ["oc1", "oc1_obj1", "parameter_class1", 1],
                ["oc1", "oc1_obj2", "parameter_class1", 2],
                ["oc2", "oc2_obj2", "parameter_class2", 4],
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
        self.assertEqual(errors, [])
        self.assertEqual(out, expected)

    def test_read_object_class_with_table_name_as_class_name(self):
        input_data = [["Entity names"], ["object 1"], ["object 2"]]
        data = iter(input_data)
        data_header = next(data)
        mapping = {
            "map_type": "ObjectClass",
            "name": {"map_type": "table_name", "reference": "class name"},
            "object": 0,
        }
        out, errors = get_mapped_data(data, [mapping], data_header, "class name")
        expected = {
            "entity_classes": [("class name",)],
            "entities": [("class name", "object 1"), ("class name", "object 2")],
        }
        self.assertFalse(errors)
        self.assertEqual(out, expected)

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
        expected_map = Map(["key1", "key2"], [-2, -1])
        expected = {
            "entity_classes": [("object_class",)],
            "entities": [("object_class", "object")],
            "parameter_values": [["object_class", "object", "parameter", expected_map]],
            "parameter_definitions": [("object_class", "parameter")],
        }
        self.assertFalse(errors)
        self.assertEqual(out, expected)

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
        expected_map = Map(["key11", "key21"], [Map(["key12"], [-2]), Map(["key22"], [-1])])
        expected = {
            "entity_classes": [("object_class",)],
            "entities": [("object_class", "object")],
            "parameter_values": [["object_class", "object", "parameter", expected_map]],
            "parameter_definitions": [("object_class", "parameter")],
        }
        self.assertFalse(errors)
        self.assertEqual(out, expected)

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
        expected_map = Map(
            ["key1", "key2", "key3", "key4"],
            [
                Map(["key11", "key12"], [-2, -1]),
                -23,
                -33,
                Map(["key31", "key32"], [Map(["key311", "key312"], [50, 51]), 66]),
            ],
        )
        expected = {
            "entity_classes": [("object_class",)],
            "entities": [("object_class", "object")],
            "parameter_values": [["object_class", "object", "parameter", expected_map]],
            "parameter_definitions": [("object_class", "parameter")],
        }
        self.assertFalse(errors)
        self.assertEqual(out, expected)

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
        expected_map = Map(
            ["key"],
            [
                TimeSeriesVariableResolution(
                    ["2020-09-10T08:00", "2020-09-11T08:00"],
                    [-2.0, -1.0],
                    False,
                    False,
                    index_name=Map.DEFAULT_INDEX_NAME,
                )
            ],
        )
        expected = {
            "entity_classes": [("object_class",)],
            "entities": [("object_class", "object")],
            "parameter_values": [["object_class", "object", "parameter", expected_map]],
            "parameter_definitions": [("object_class", "parameter")],
        }
        self.assertFalse(errors)
        self.assertEqual(out, expected)

    def test_read_alternative(self):
        input_data = [["Alternatives"], ["alternative1"], ["second_alternative"], ["last_one"]]
        data = iter(input_data)
        data_header = next(data)
        mapping = {"map_type": "Alternative", "name": 0}
        out, errors = get_mapped_data(data, [mapping], data_header)
        expected = {"alternatives": {"alternative1", "second_alternative", "last_one"}}
        self.assertFalse(errors)
        self.assertEqual(out, expected)

    def test_read_scenario(self):
        input_data = [["Scenarios"], ["scenario1"], ["second_scenario"], ["last_one"]]
        data = iter(input_data)
        data_header = next(data)
        mapping = {"map_type": "Scenario", "name": 0}
        out, errors = get_mapped_data(data, [mapping], data_header)
        expected = {"scenarios": {("scenario1", False), ("second_scenario", False), ("last_one", False)}}
        self.assertFalse(errors)
        self.assertEqual(out, expected)

    def test_read_scenario_with_active_flags(self):
        input_data = [["Scenarios", "Active"], ["scenario1", 1], ["second_scenario", "f"], ["last_one", "true"]]
        data = iter(input_data)
        data_header = next(data)
        mapping = {"map_type": "Scenario", "name": 0, "active": 1}
        out, errors = get_mapped_data(data, [mapping], data_header)
        expected = {"scenarios": {("scenario1", True), ("second_scenario", False), ("last_one", True)}}
        self.assertFalse(errors)
        self.assertEqual(out, expected)

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
            ["scenario_A", "alternative1", "second_alternative"],
            ["scenario_A", "second_alternative", "last_one"],
            ["scenario_B", "last_one", ""],
        ]
        self.assertFalse(errors)
        self.assertEqual(out, expected)

    def test_pivoted_scenario_alternative(self):
        input_data = [["scenario_A", "scenario_B"], ["first_alternative", "Base"], ["second_alternative", ""]]
        data = iter(input_data)
        mappings = [{"map_type": "Scenario", "position": -1}, {"map_type": "ScenarioAlternative", "position": "hidden"}]
        out, errors = get_mapped_data(data, [mappings])
        expected = dict()
        expected["scenario_alternatives"] = [
            ["scenario_A", "first_alternative"],
            ["scenario_A", "second_alternative"],
            ["scenario_B", "Base"],
        ]
        self.assertFalse(errors)
        self.assertEqual(out, expected)

    def test_read_tool(self):
        input_data = [["Tools"], ["tool1"], ["second_tool"], ["last_one"]]
        data = iter(input_data)
        data_header = next(data)
        mapping = {"map_type": "Tool", "name": 0}
        with self.assertRaises(ValueError):
            get_mapped_data(data, [mapping], data_header)

    def test_read_feature(self):
        input_data = [["Class", "Parameter"], ["class1", "param1"], ["class2", "param2"]]
        data = iter(input_data)
        data_header = next(data)
        mapping = {"map_type": "Feature", "entity_class_name": 0, "parameter_definition_name": 1}
        with self.assertRaises(ValueError):
            get_mapped_data(data, [mapping], data_header)

    def test_read_tool_feature(self):
        input_data = [["Tool", "Class", "Parameter"], ["tool1", "class1", "param1"], ["tool2", "class2", "param2"]]
        data = iter(input_data)
        data_header = next(data)
        mapping = {"map_type": "ToolFeature", "name": 0, "entity_class_name": 1, "parameter_definition_name": 2}
        with self.assertRaises(ValueError):
            get_mapped_data(data, [mapping], data_header)

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
        with self.assertRaises(ValueError):
            get_mapped_data(data, [mapping], data_header)

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
        with self.assertRaises(ValueError):
            get_mapped_data(data, [mapping], data_header)

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
        expected["entity_classes"] = [("class_A",)]
        expected["entity_groups"] = {
            ("class_A", "group1", "object1"),
            ("class_A", "group1", "object2"),
            ("class_A", "group2", "object3"),
        }
        self.assertFalse(errors)
        self.assertEqual(out, expected)

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
        expected["entity_groups"] = {
            ("class_A", "group1", "object1"),
            ("class_A", "group1", "object2"),
            ("class_A", "group2", "object3"),
        }
        expected["entity_classes"] = [("class_A",)]
        expected["entities"] = [
            ("class_A", "group1"),
            ("class_A", "object1"),
            ("class_A", "object2"),
            ("class_A", "group2"),
            ("class_A", "object3"),
        ]
        self.assertFalse(errors)
        self.assertEqual(out, expected)

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
        expected["entity_classes"] = [("class_A",), ("class_B",)]
        expected["parameter_definitions"] = [
            ("class_A", "param1", 23.0, "listA"),
            ("class_A", "param2", 42.0, "listB"),
            ("class_B", "param3", 5.0, "listA"),
        ]
        self.assertFalse(errors)
        self.assertEqual(out, expected)

    def test_map_as_default_parameter_value(self):
        input_data = [["key1", -2.3], ["key2", 5.5], ["key3", 3.2]]
        data = iter(input_data)
        mapping = {
            "map_type": "ObjectClass",
            "name": "object_class",
            "parameters": {
                "name": "parameter",
                "map_type": "ParameterDefinition",
                "default_value": {"value_type": "map", "main_value": 1, "compress": False, "extra_dimensions": [0]},
            },
        }
        out, errors = get_mapped_data(data, [mapping])
        expected_map = Map(["key1", "key2", "key3"], [-2.3, 5.5, 3.2])
        expected = {
            "entity_classes": [("object_class",)],
            "parameter_definitions": [("object_class", "parameter", expected_map)],
        }
        self.assertFalse(errors)
        self.assertEqual(out, expected)

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
        expected_map = Map(["key11", "key21"], [Map(["key12"], [-2]), Map(["key22"], [-1])])
        expected = {
            "entity_classes": [("object_class",)],
            "parameter_definitions": [("object_class", "parameter", expected_map)],
        }
        self.assertFalse(errors)
        self.assertEqual(out, expected)

    def test_read_map_index_names_from_columns(self):
        input_data = [["Index 1", "Index 2", "Value"], ["key11", "key12", -2], ["key21", "key22", -1]]
        data = iter(input_data)
        data_header = next(data)
        mapping_root = unflatten(
            [
                EntityClassMapping(Position.hidden, value="object_class"),
                ParameterDefinitionMapping(Position.hidden, value="parameter"),
                EntityMapping(Position.hidden, value="object"),
                ParameterValueTypeMapping(Position.hidden, value="map"),
                IndexNameMapping(Position.header, value=0),
                ParameterValueIndexMapping(0),
                IndexNameMapping(Position.header, value=1),
                ParameterValueIndexMapping(1),
                ExpandedParameterValueMapping(2),
            ]
        )
        out, errors = get_mapped_data(data, [mapping_root], data_header)
        expected_map = Map(
            ["key11", "key21"],
            [Map(["key12"], [-2], index_name="Index 2"), Map(["key22"], [-1], index_name="Index 2")],
            index_name="Index 1",
        )
        expected = {
            "entity_classes": [("object_class",)],
            "entities": [("object_class", "object")],
            "parameter_values": [["object_class", "object", "parameter", expected_map]],
            "parameter_definitions": [("object_class", "parameter")],
        }
        self.assertFalse(errors)
        self.assertEqual(out, expected)

    def test_missing_map_index_name(self):
        input_data = [["Index 1", "Index 2", "Value"], ["key11", "key12", -2], ["key21", "key22", -1]]
        data = iter(input_data)
        data_header = next(data)
        mapping_root = unflatten(
            [
                EntityClassMapping(Position.hidden, value="object_class"),
                ParameterDefinitionMapping(Position.hidden, value="parameter"),
                EntityMapping(Position.hidden, value="object"),
                ParameterValueTypeMapping(Position.hidden, value="map"),
                IndexNameMapping(Position.hidden, value=None),
                ParameterValueIndexMapping(0),
                IndexNameMapping(Position.header, value=1),
                ParameterValueIndexMapping(1),
                ExpandedParameterValueMapping(2),
            ]
        )
        out, errors = get_mapped_data(data, [mapping_root], data_header)
        expected_map = Map(
            ["key11", "key21"],
            [Map(["key12"], [-2], index_name="Index 2"), Map(["key22"], [-1], index_name="Index 2")],
            index_name="",
        )
        expected = {
            "entity_classes": [("object_class",)],
            "entities": [("object_class", "object")],
            "parameter_values": [["object_class", "object", "parameter", expected_map]],
            "parameter_definitions": [("object_class", "parameter")],
        }
        self.assertFalse(errors)
        self.assertEqual(out, expected)

    def test_read_default_value_index_names_from_columns(self):
        input_data = [["Index 1", "Index 2", "Value"], ["key11", "key12", -2], ["key21", "key22", -1]]
        data = iter(input_data)
        data_header = next(data)
        mapping_root = unflatten(
            [
                EntityClassMapping(Position.hidden, value="object_class"),
                ParameterDefinitionMapping(Position.hidden, value="parameter"),
                ParameterDefaultValueTypeMapping(Position.hidden, value="map"),
                DefaultValueIndexNameMapping(Position.header, value=0),
                ParameterDefaultValueIndexMapping(0),
                DefaultValueIndexNameMapping(Position.header, value=1),
                ParameterDefaultValueIndexMapping(1),
                ExpandedParameterDefaultValueMapping(2),
            ]
        )
        out, errors = get_mapped_data(data, [mapping_root], data_header)
        expected_map = Map(
            ["key11", "key21"],
            [Map(["key12"], [-2], index_name="Index 2"), Map(["key22"], [-1], index_name="Index 2")],
            index_name="Index 1",
        )
        expected = {
            "entity_classes": [("object_class",)],
            "parameter_definitions": [("object_class", "parameter", expected_map)],
        }
        self.assertFalse(errors)
        self.assertEqual(out, expected)

    def test_filter_regular_expression_in_root_mapping(self):
        input_data = [["A", "p"], ["A", "q"], ["B", "r"]]
        data = iter(input_data)
        mapping_root = unflatten([EntityClassMapping(0, filter_re="B"), EntityMapping(1)])
        out, errors = get_mapped_data(data, [mapping_root])
        expected = {"entity_classes": [("B",)], "entities": [("B", "r")]}
        self.assertFalse(errors)
        self.assertEqual(out, expected)

    def test_filter_regular_expression_in_child_mapping(self):
        input_data = [["A", "p"], ["A", "q"], ["B", "r"]]
        data = iter(input_data)
        mapping_root = unflatten([EntityClassMapping(0), EntityMapping(1, filter_re="q|r")])
        out, errors = get_mapped_data(data, [mapping_root])
        expected = {"entity_classes": [("A",), ("B",)], "entities": [("A", "q"), ("B", "r")]}
        self.assertFalse(errors)
        self.assertEqual(out, expected)

    def test_filter_regular_expression_in_child_mapping_filters_parent_mappings_too(self):
        input_data = [["A", "p"], ["A", "q"], ["B", "r"]]
        data = iter(input_data)
        mapping_root = unflatten([EntityClassMapping(0), EntityMapping(1, filter_re="q")])
        out, errors = get_mapped_data(data, [mapping_root])
        expected = {"entity_classes": [("A",)], "entities": [("A", "q")]}
        self.assertFalse(errors)
        self.assertEqual(out, expected)

    def test_arrays_get_imported_to_correct_alternatives(self):
        input_data = [["Base", "y", "p1"], ["alternative", "y", "p1"]]
        data = iter(input_data)
        mapping_root = unflatten(
            [
                EntityClassMapping(Position.hidden, value="class"),
                EntityMapping(1),
                ParameterDefinitionMapping(Position.hidden, value="parameter"),
                AlternativeMapping(0),
                ParameterValueTypeMapping(Position.hidden, value="array"),
                ExpandedParameterValueMapping(2),
            ]
        )
        out, errors = get_mapped_data(data, [mapping_root])
        expected = {
            "entity_classes": [("class",)],
            "entities": [("class", "y")],
            "parameter_definitions": [("class", "parameter")],
            "alternatives": {"Base", "alternative"},
            "parameter_values": [
                ["class", "y", "parameter", Array(["p1"]), "Base"],
                ["class", "y", "parameter", Array(["p1"]), "alternative"],
            ],
        }
        self.assertFalse(errors)
        self.assertEqual(out, expected)


class TestHasFilter(unittest.TestCase):
    def test_mapping_without_filter_doesnt_have_filter(self):
        mapping = EntityClassMapping(0)
        self.assertFalse(mapping.has_filter())

    def test_hidden_mapping_without_value_doesnt_have_filter(self):
        mapping = EntityClassMapping(Position.hidden, filter_re="a")
        self.assertFalse(mapping.has_filter())

    def test_hidden_mapping_with_value_has_filter(self):
        mapping = EntityClassMapping(0, value="a", filter_re="b")
        self.assertTrue(mapping.has_filter())

    def test_mapping_without_value_has_filter(self):
        mapping = EntityClassMapping(Position.hidden, value="a", filter_re="b")
        self.assertTrue(mapping.has_filter())

    def test_mapping_with_value_but_without_filter_doesnt_have_filter(self):
        mapping = EntityClassMapping(0, value="a")
        self.assertFalse(mapping.has_filter())

    def test_child_mapping_with_filter_has_filter(self):
        mapping = EntityClassMapping(0)
        mapping.child = EntityMapping(1, filter_re="a")
        self.assertTrue(mapping.has_filter())

    def test_child_mapping_without_filter_doesnt_have_filter(self):
        mapping = EntityClassMapping(0)
        mapping.child = EntityMapping(1)
        self.assertFalse(mapping.has_filter())


class TestIsPivoted(unittest.TestCase):
    def test_pivoted_position_returns_true(self):
        mapping = AlternativeMapping(-1)
        self.assertTrue(mapping.is_pivoted())

    def test_recursively_returns_false_when_all_mappings_are_non_pivoted(self):
        mapping = unflatten([AlternativeMapping(0), ParameterValueMapping(1)])
        self.assertFalse(mapping.is_pivoted())

    def test_returns_true_when_position_is_header_and_has_child(self):
        mapping = unflatten([AlternativeMapping(Position.header), ParameterValueMapping(0)])
        self.assertTrue(mapping.is_pivoted())

    def test_returns_false_when_position_is_header_and_is_leaf(self):
        mapping = unflatten([AlternativeMapping(0), ParameterValueMapping(Position.header)])
        self.assertFalse(mapping.is_pivoted())


class TestDefaultMappings(unittest.TestCase):
    def test_mappings_are_hidden(self):
        map_types = (
            "EntityClass",
            "Alternative",
            "Scenario",
            "ScenarioAlternative",
            "EntityGroup",
            "ParameterValueList",
        )
        for map_type in map_types:
            root = default_import_mapping(map_type)
            flattened = root.flatten()
            self.assertTrue(all(m.position == Position.hidden for m in flattened))


if __name__ == "__main__":
    unittest.main()
