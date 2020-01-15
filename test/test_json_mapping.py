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
:date:   22.02.2018
"""

import unittest
from spinedb_api.json_mapping import (
    read_with_mapping,
    ObjectClassMapping,
    RelationshipClassMapping,
    Mapping,
    ParameterMapping,
    TimeSeriesOptions,
    convert_function_from_spec
)
from spinedb_api.parameter_value import TimeSeriesVariableResolution, TimePattern
from spinedb_api.exception import TypeConversionError


class TestTypeConversion(unittest.TestCase):
    def test_convert_function(self):
        convert_function = convert_function_from_spec({0: str, 1: float}, 2)
        self.assertEqual(convert_function(["a", "1.2"]), ["a", 1.2])
    
    def test_convert_function_raises_error(self):
        convert_function = convert_function_from_spec({0: str, 1: float}, 2)
        with self.assertRaises(TypeConversionError):
            convert_function(["a", "not a float"])

class TestMappingIO(unittest.TestCase):
    def test_ObjectClass_to_dict_from_dict(self):
        mapping = {
            "map_type": "ObjectClass",
            "name": 0,
            "object": 1,
            "parameters": {
                "map_type": "parameter",
                "name": 2,
                "value": 3,
                "parameter_type": "single value",
            },
        }

        map_obj = ObjectClassMapping.from_dict(mapping)
        out = map_obj.to_dict()

        expected = {
            "map_type": "ObjectClass",
            "name": {"value_reference": 0, "map_type": "column"},
            "object": {"value_reference": 1, "map_type": "column"},
            "parameters": {
                "map_type": "parameter",
                "name": {"value_reference": 2, "map_type": "column"},
                "value": {"value_reference": 3, "map_type": "column"},
                "options": None,
                "parameter_type": "single value",
            },
            "read_start_row": 0,
        }
        self.assertEqual(out, expected)

    def test_ObjectClass_object_from_dict_to_dict(self):
        mapping = {"map_type": "ObjectClass", "name": 0, "object": 1}

        map_obj = ObjectClassMapping.from_dict(mapping)
        out = map_obj.to_dict()

        expected = {
            "map_type": "ObjectClass",
            "name": {"value_reference": 0, "map_type": "column"},
            "object": {"value_reference": 1, "map_type": "column"},
            "read_start_row": 0,
        }
        self.assertEqual(out, expected)

        mapping = {"map_type": "ObjectClass", "name": "str", "object": "str"}

        map_obj = ObjectClassMapping.from_dict(mapping)
        out = map_obj.to_dict()

        expected = {"map_type": "ObjectClass", "name": "str", "object": "str", "read_start_row": 0,}
        self.assertEqual(out, expected)

    def test_RelationshipClassMapping_from_dict_to_dict(self):
        mapping = {
            "map_type": "RelationshipClass",
            "name": "unit__node",
            "object_classes": [0, 1],
            "objects": [0, 1],
            "parameters": {"map_type": "parameter", "name": "test", "value": 2},
        }
        map_obj = RelationshipClassMapping.from_dict(mapping)
        out = map_obj.to_dict()

        expected = {
            "map_type": "RelationshipClass",
            "import_objects": False,
            "name": "unit__node",
            "object_classes": [
                {"value_reference": 0, "map_type": "column_name"},
                {"value_reference": 1, "map_type": "column_name"},
            ],
            "objects": [
                {"value_reference": 0, "map_type": "column"},
                {"value_reference": 1, "map_type": "column"},
            ],
            "parameters": {
                "map_type": "parameter",
                "name": "test",
                "parameter_type": "single value",
                "options": None,
                "value": {"value_reference": 2, "map_type": "column"},
            },
            "read_start_row": 0,
        }
        self.assertEqual(out, expected)

    def test_RelationshipClassMapping_from_dict_to_dict2(self):
        mapping = {
            "map_type": "RelationshipClass",
            "name": "unit__node",
            "object_classes": ["test", 0],
            "objects": ["test", 0],
        }
        map_obj = RelationshipClassMapping.from_dict(mapping)
        out = map_obj.to_dict()

        expected = {
            "map_type": "RelationshipClass",
            "import_objects": False,
            "name": "unit__node",
            "object_classes": [
                "test",
                {"value_reference": 0, "map_type": "column_name"},
            ],
            "objects": ["test", {"value_reference": 0, "map_type": "column"}],
            "read_start_row": 0,
        }
        self.assertEqual(out, expected)

    def test_RelationshipClassMapping_from_dict_to_dict3(self):
        mapping = {
            "map_type": "RelationshipClass",
            "name": "unit__node",
            "parameters": {
                "map_type": "parameter",
                "name": "test",
                "value": 2,
                "parameter_type": "1d array",
                "extra_dimensions": ["test", 0],
            },
        }
        map_obj = RelationshipClassMapping.from_dict(mapping)
        out = map_obj.to_dict()

        expected = {
            "map_type": "RelationshipClass",
            "import_objects": False,
            "name": "unit__node",
            "parameters": {
                "map_type": "parameter",
                "name": "test",
                "parameter_type": "1d array",
                "value": {"value_reference": 2, "map_type": "column"},
                "extra_dimensions": [
                    "test",
                    {"value_reference": 0, "map_type": "column"},
                ],
                "options": None,
            },
            "read_start_row": 0,
        }
        self.assertEqual(out, expected)

    def test_TimeSeriesOptions_to_dict(self):
        options = TimeSeriesOptions(repeat=True)
        options_dict = options.to_dict()
        self.assertEqual(options_dict, {"repeat": True})

    def test_TimeSeriesOptions_from_dict(self):
        options_dict = {"repeat": True}
        options = TimeSeriesOptions.from_dict(options_dict)
        self.assertEqual(options.repeat, True)

    def test_ParameterMapping_to_dict(self):
        mapping_value = Mapping(map_type="row", value_reference=23)
        extra_dimension = Mapping(map_type="column_name", value_reference="fifth column")
        parameter_options = TimeSeriesOptions(repeat=True)
        parameter_mapping = ParameterMapping("mapping name", value=mapping_value, extra_dimensions=[extra_dimension],
                                             parameter_type="time series", options=parameter_options)
        mapping_dict = parameter_mapping.to_dict()
        expected = {
                "map_type": "parameter",
                "name": "mapping name",
                "parameter_type": "time series",
                "value": {"value_reference": 23, "map_type": "row"},
                "extra_dimensions": [
                    {"value_reference": "fifth column", "map_type": "column_name"},
                ],
                "options": {"repeat": True},
            }
        self.assertEqual(mapping_dict, expected)

    def test_ParameterMapping_from_dict(self):
        mapping_dict = {
                "map_type": "parameter",
                "name": "mapping name",
                "parameter_type": "time series",
                "value": {"value_reference": 23, "map_type": "row"},
                "extra_dimensions": [
                    {"value_reference": "fifth column", "map_type": "column_name"},
                ],
                "options": {"repeat": True},
            }
        parameter_mapping = ParameterMapping.from_dict(mapping_dict)
        self.assertEqual(parameter_mapping.name, "mapping name")
        self.assertEqual(parameter_mapping.parameter_type, "time series")
        value = parameter_mapping.value
        self.assertEqual(value.map_type, "row")
        self.assertEqual(value.value_reference, 23)
        extra_dimensions = parameter_mapping.extra_dimensions
        self.assertEqual(len(extra_dimensions), 1)
        dimension = extra_dimensions[0]
        self.assertEqual(dimension.map_type, "column_name")
        self.assertEqual(dimension.value_reference, "fifth column")
        options = parameter_mapping.options
        self.assertEqual(options.repeat, True)


class TestMappingIsValid(unittest.TestCase):
    def test_valid_mapping(self):
        mapping = Mapping(value_reference=1)
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)

    def test_invalid_mapping(self):
        mapping = Mapping()
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)
    
    def test_valid_parameter_mapping_definition(self):
        mapping = {
                "map_type": "parameter",
                "name": "test",
                "parameter_type": "definition"
            }
        mapping = ParameterMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid(False)
        self.assertTrue(is_valid)
    
    def test_invalid_parameter_mapping_definition(self):
        mapping = {
                "map_type": "parameter",
                "parameter_type": "definition"
            }
        mapping = ParameterMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid(False)
        self.assertFalse(is_valid)
    
    def test_valid_parameter_mapping_single_value(self):
        mapping = {
                "map_type": "parameter",
                "name": "test",
                "value": 0,
                "parameter_type": "single value"
            }
        mapping = ParameterMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid(False)
        self.assertTrue(is_valid)
    
    def test_invalid_parameter_mapping_single_value(self):
        mapping = {
                "map_type": "parameter",
                "name": "test",
                "value": None,
                "parameter_type": "single value"
            }
        mapping = ParameterMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid(False)
        self.assertFalse(is_valid)
    
    def test_invalid_with_extra_dim_parameter_mapping_single_value(self):
        mapping = {
                "map_type": "parameter",
                "name": "test",
                "value": "test",
                "parameter_type": "single value",
                "extra_dimensions": [0]
            }
        mapping = ParameterMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid(False)
        self.assertFalse(is_valid)
    
    def test_valid_parameter_mapping_time_series(self):
        mapping = {
                "map_type": "parameter",
                "name": "test",
                "value": "test",
                "parameter_type": "time series",
                "extra_dimensions": [0]
            }
        mapping = ParameterMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid(False)
        self.assertTrue(is_valid)
    
    def test_invalid_parameter_mapping_time_series(self):
        mapping = {
                "map_type": "parameter",
                "name": "test",
                "value": "test",
                "parameter_type": "time series",
            }
        mapping = ParameterMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid(False)
        self.assertFalse(is_valid)
    
    def test_invalid_parameter_mapping_time_series_to_many_extra_dim(self):
        mapping = {
                "map_type": "parameter",
                "name": "test",
                "value": "test",
                "parameter_type": "time series",
                "extra_dimensions": [0, 3]
            }
        mapping = ParameterMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid(False)
        self.assertFalse(is_valid)
    
    def test_invalid_parameter_mapping_time_series_non_valid_extra_dim(self):
        mapping = {
                "map_type": "parameter",
                "name": "test",
                "value": "test",
                "parameter_type": "time series",
                "extra_dimensions": [None]
            }
        mapping = ParameterMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid(False)
        self.assertFalse(is_valid)
    
    def test_valid_parameter_mapping_time_pattern(self):
        mapping = {
                "map_type": "parameter",
                "name": "test",
                "value": "test",
                "parameter_type": "time pattern",
                "extra_dimensions": [0]
            }
        mapping = ParameterMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid(False)
        self.assertTrue(is_valid)
    
    def test_invalid_parameter_mapping_time_pattern(self):
        mapping = {
                "map_type": "parameter",
                "name": "test",
                "value": "test",
                "parameter_type": "time pattern",
            }
        mapping = ParameterMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid(False)
        self.assertFalse(is_valid)
    
    def test_valid_pivoted_parameter_mapping(self):
        mapping = {
                "map_type": "parameter",
                "name": {"map_type": "row", "value_reference": 0}
            }
        mapping = ParameterMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid(False)
        self.assertTrue(is_valid)
    
    def test_invalid_pivoted_parameter_mapping(self):
        mapping = {
                "map_type": "parameter",
                "name": {"map_type": "column", "value_reference": 0}
            }
        mapping = ParameterMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid(False)
        self.assertFalse(is_valid)
    
    def test_valid_pivoted_parent_parameter_mapping(self):
        mapping = {
                "map_type": "parameter",
                "name": {"map_type": "column", "value_reference": 0}
            }
        mapping = ParameterMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid(True)
        self.assertTrue(is_valid)
    
    def test_valid_object_class_mapping(self):
        mapping = {
                "map_type": "ObjectClass",
                "name": "test"
            }
        mapping = ObjectClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)
    
    def test_valid_object_class_mapping_with_object(self):
        mapping = {
                "map_type": "ObjectClass",
                "name": "test",
                "object": "test"
            }
        mapping = ObjectClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)
    
    def test_valid_object_class_mapping_with_object_and_parameter(self):
        mapping = {
                "map_type": "ObjectClass",
                "name": "test",
                "object": "test",
                "parameters": {
                    "map_type": "parameter",
                    "name": "test",
                    "value": 0
                    }
            }
        mapping = ObjectClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)
    
    def test_invalid_object_class_mapping_with_parameter_but_no_object(self):
        mapping = {
                "map_type": "ObjectClass",
                "name": "test",
                "parameters": {
                    "map_type": "parameter",
                    "name": "test",
                    "value": 0
                    }
            }
        mapping = ObjectClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)
    
    def test_valid_object_class_mapping_with_parameter_definition_but_no_object(self):
        mapping = {
                "map_type": "ObjectClass",
                "name": "test",
                "parameters": {
                    "map_type": "parameter",
                    "name": "test",
                    "parameter_type": "definition"
                    }
            }
        mapping = ObjectClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)
    
    def test_valid_relationship_class_mapping(self):
        mapping = {
                "map_type": "RelationshipClass",
                "name": "test",
                "object_classes": ["test"]
            }
        mapping = RelationshipClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)
    
    def test_invalid_relationship_class_mapping(self):
        mapping = {
                "map_type": "RelationshipClass",
                "name": "test",
                "object_classes": []
            }
        mapping = RelationshipClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)
    
    def test_valid_relationship_class_mapping_with_objects(self):
        mapping = {
                "map_type": "RelationshipClass",
                "name": "test",
                "object_classes": ["test"],
                "objects": ["test"]
            }
        mapping = RelationshipClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertTrue(is_valid)
    
    def test_invalid_relationship_class_mapping_with_invalid_objects(self):
        mapping = {
                "map_type": "RelationshipClass",
                "name": "test",
                "object_classes": ["test"],
                "objects": [None]
            }
        mapping = RelationshipClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)
    
    def test_invalid_relationship_class_mapping_with_to_many_objects(self):
        mapping = {
                "map_type": "RelationshipClass",
                "name": "test",
                "object_classes": ["test"],
                "objects": ["test1", "test2"]
            }
        mapping = RelationshipClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)
    
    def test_valid_relationship_class_mapping_with_parameter_definition(self):
        mapping = {
                "map_type": "RelationshipClass",
                "name": "test",
                "object_classes": ["test"],
                "parameters": {
                    "map_type": "parameter",
                    "name": "test",
                    "parameter_type": "definition"
                    }
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
                "parameters": {
                    "map_type": "parameter",
                    "name": "test",
                    "value": "test"
                    }
            }
        mapping = RelationshipClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)
    
    def test_valid_pivoted_relationship_class_mapping_with_parameter(self):
        mapping = {
                "map_type": "RelationshipClass",
                "name": "test",
                "object_classes": ["test"],
                "objects": [{"map_type": "row", "value_reference": 0}],
                "parameters": {
                    "map_type": "parameter",
                    "name": "test",
                    }
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
                "parameters": {
                    "map_type": "parameter"
                    }
            }
        mapping = RelationshipClassMapping.from_dict(mapping)
        is_valid, _ = mapping.is_valid()
        self.assertFalse(is_valid)


class TestMappingIntegration(unittest.TestCase):
    # just a placeholder test for different mapping testings
    def setUp(self):
        self.empty_data = {
            "object_classes": [],
            "objects": [],
            "object_parameters": [],
            "object_parameter_values": [],
            "relationship_classes": [],
            "relationships": [],
            "relationship_parameters": [],
            "relationship_parameter_values": [],
        }

    def test_bad_mapping_type(self):
        """Tests that passing any other than a `dict` or a `mapping` to `read_with_mapping` raises `TypeError`.
        """
        input_data = [["object_class"], ["oc1"]]
        data = iter(input_data)
        data_header = next(data)
        num_cols = len(data_header)

        with self.assertRaises(TypeError):
            mapping = [1, 2, 3]
            out, errors = read_with_mapping(data, mapping, num_cols, data_header)

        with self.assertRaises(TypeError):
            mapping = [{"map_type": "ObjectClass", "name": 0}, [1, 2, 3]]
            out, errors = read_with_mapping(data, mapping, num_cols, data_header)

    def test_read_flat_file(self):
        input_data = [
            ["object_class", "object", "parameter", "value"],
            ["oc1", "obj1", "parameter_name1", 1],
            ["oc2", "obj2", "parameter_name2", 2],
        ]
        self.empty_data.update(
            {
                "object_classes": ["oc1", "oc2"],
                "objects": [("oc1", "obj1"), ("oc2", "obj2")],
                "object_parameters": [
                    ("oc1", "parameter_name1"),
                    ("oc2", "parameter_name2"),
                ],
                "object_parameter_values": [
                    ("oc1", "obj1", "parameter_name1", 1),
                    ("oc2", "obj2", "parameter_name2", 2),
                ],
            }
        )

        data = iter(input_data)
        data_header = next(data)
        num_cols = len(data_header)

        mapping = {
            "map_type": "ObjectClass",
            "name": 0,
            "object": 1,
            "parameters": {"map_type": "parameter", "name": 2, "value": 3},
        }

        out, errors = read_with_mapping(data, mapping, num_cols, data_header)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])
    
    def test_read_flat_file_1d_array(self):
        input_data = [
            ["object_class", "object", "parameter", "value"],
            ["oc1", "obj1", "parameter_name1", 1],
            ["oc1", "obj1", "parameter_name1", 2],
        ]
        self.empty_data.update(
            {
                "object_classes": ["oc1", "oc1"],
                "objects": [("oc1", "obj1"), ("oc1", "obj1")],
                "object_parameters": [
                    ("oc1", "parameter_name1"),("oc1", "parameter_name1")
                ],
                "object_parameter_values": [
                    ("oc1", "obj1", "parameter_name1", [1, 2]),
                ],
            }
        )

        data = iter(input_data)
        data_header = next(data)
        num_cols = len(data_header)

        mapping = {
            "map_type": "ObjectClass",
            "name": 0,
            "object": 1,
            "parameters": {
                "map_type": "parameter",
                "name": "parameter_name1",
                "value": 3,
                "parameter_type": "1d array"},
        }

        out, errors = read_with_mapping(data, mapping, num_cols, data_header)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    def test_read_flat_file_with_column_name_reference(self):
        input_data = [
            ["object", "parameter", "value"],
            ["obj1", "parameter_name1", 1],
            ["obj2", "parameter_name2", 2],
        ]
        self.empty_data.update(
            {
                "object_classes": ["object"],
                "objects": [("object", "obj1"), ("object", "obj2")],
            }
        )

        data = iter(input_data)
        data_header = next(data)
        num_cols = len(data_header)

        mapping = {
            "map_type": "ObjectClass",
            "name": {"map_type": "column_name", "value_reference": 0},
            "object": 0,
        }

        out, errors = read_with_mapping(data, mapping, num_cols, data_header)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    def test_read_with_list_of_mappings(self):
        input_data = [
            ["object", "parameter", "value"],
            ["obj1", "parameter_name1", 1],
            ["obj2", "parameter_name2", 2],
        ]
        self.empty_data.update(
            {
                "object_classes": ["object"],
                "objects": [("object", "obj1"), ("object", "obj2")],
            }
        )

        data = iter(input_data)
        data_header = next(data)
        num_cols = len(data_header)

        mapping = {
            "map_type": "ObjectClass",
            "name": {"map_type": "column_name", "value_reference": 0},
            "object": 0,
        }

        out, errors = read_with_mapping(data, mapping, num_cols, data_header)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    @unittest.skip("ColumnMapping will be removed")
    def test_read_columncollection_parameters(self):
        input_data = [
            ["object", "parameter_name1", "parameter_name2"],
            ["obj1", 0, 1],
            ["obj2", 2, 3],
        ]
        self.empty_data.update(
            {
                "object_classes": ["object"],
                "objects": [("object", "obj1"), ("object", "obj2")],
                "object_parameters": [
                    ("object", "parameter_name1"),
                    ("object", "parameter_name2"),
                ],
                "object_parameter_values": [
                    ("object", "obj1", "parameter_name1", 0),
                    ("object", "obj1", "parameter_name2", 1),
                    ("object", "obj2", "parameter_name1", 2),
                    ("object", "obj2", "parameter_name2", 3),
                ],
            }
        )

        data = iter(input_data)
        data_header = next(data)
        num_cols = len(data_header)

        mapping = {
            "map_type": "ObjectClass",
            "name": {"map_type": "column_name", "value_reference": 0},
            "object": 0,
            "parameters": [1, 2],
        }

        out, errors = read_with_mapping(data, mapping, num_cols, data_header)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    def test_read_pivoted_parameters_from_header(self):
        input_data = [
            ["object", "parameter_name1", "parameter_name2"],
            ["obj1", 0, 1],
            ["obj2", 2, 3],
        ]
        self.empty_data.update(
            {
                "object_classes": ["object"],
                "objects": [("object", "obj1"), ("object", "obj2")],
                "object_parameters": [
                    ("object", "parameter_name1"),
                    ("object", "parameter_name2"),
                ],
                "object_parameter_values": [
                    ("object", "obj1", "parameter_name1", 0),
                    ("object", "obj1", "parameter_name2", 1),
                    ("object", "obj2", "parameter_name1", 2),
                    ("object", "obj2", "parameter_name2", 3),
                ],
            }
        )

        data = iter(input_data)
        data_header = next(data)
        num_cols = len(data_header)

        mapping = {
            "map_type": "ObjectClass",
            "name": {"map_type": "column_name", "value_reference": 0},
            "object": 0,
            "parameters": {
                "map_type": "parameter",
                "name": {"map_type": "row", "value_reference": -1},
            },
        }  # -1 to read pivot from header

        out, errors = read_with_mapping(data, mapping, num_cols, data_header)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    def test_read_pivoted_parameters_from_data(self):
        input_data = [
            ["object", "parameter_name1", "parameter_name2"],
            ["obj1", 0, 1],
            ["obj2", 2, 3],
        ]
        self.empty_data.update(
            {
                "object_classes": ["object"],
                "objects": [("object", "obj1"), ("object", "obj2")],
                "object_parameters": [
                    ("object", "parameter_name1"),
                    ("object", "parameter_name2"),
                ],
                "object_parameter_values": [
                    ("object", "obj1", "parameter_name1", 0),
                    ("object", "obj1", "parameter_name2", 1),
                    ("object", "obj2", "parameter_name1", 2),
                    ("object", "obj2", "parameter_name2", 3),
                ],
            }
        )

        data = iter(input_data)
        # data_header = next(data)
        num_cols = len(input_data[0])

        mapping = {
            "map_type": "ObjectClass",
            "name": "object",
            "object": 0,
            "parameters": {
                "map_type": "parameter",
                "name": {"map_type": "row", "value_reference": 0},
            },
        }  # -1 to read pivot from header

        out, errors = read_with_mapping(data, mapping, num_cols)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    def test_read_flat_file_with_extra_value_dimensions(self):
        # FIXME: right now the read_with_mapping only keeps the value for
        # mappings with extra dimensions until the data spec is final.
        input_data = [
            ["object", "time", "parameter_name1"],
            ["obj1", "2018-01-01", 1],
            ["obj1", "2018-01-02", 2],
        ]
        # original test data
        # self.empty_data.update(
        #    {
        #        "object_classes": ["object"],
        #        "objects": [("object", "obj1"), ("object", "obj1")],
        #        "object_parameters": [("object", "parameter_name1")],
        #        "object_parameter_values": [
        #            ("object", "obj1", "parameter_name1", '[["scenario1", "t1", 1], ["scenario1", "t2", 2]]')
        #        ],
        #    }
        # )

        self.empty_data.update(
            {
                "object_classes": ["object"],
                "objects": [("object", "obj1"), ("object", "obj1")],
                "object_parameters": [("object", "parameter_name1")],
                "object_parameter_values": [
                    ("object", "obj1", "parameter_name1", TimeSeriesVariableResolution(["2018-01-01", "2018-01-02"], [1, 2], False, False))
                ],
            }
        )

        data = iter(input_data)
        data_header = next(data)
        num_cols = len(data_header)

        mapping = {
            "map_type": "ObjectClass",
            "name": "object",
            "object": 0,
            "parameters": {
                "map_type": "parameter",
                "name": {"map_type": "column_name", "value_reference": 2},
                "value": 2,
                "parameter_type": "time series",
                "extra_dimensions": [1],
            },
        }

        out, errors = read_with_mapping(data, mapping, num_cols, data_header)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    def test_read_1dim_relationships(self):
        input_data = [["unit", "node"], ["u1", "n1"], ["u1", "n2"]]
        self.empty_data.update(
            {
                "relationship_classes": [("node_group", ["node"])],
                "relationships": [("node_group", ["n1"]), ("node_group", ["n2"])],
            }
        )

        data = iter(input_data)
        data_header = next(data)
        num_cols = len(data_header)

        mapping = {
            "map_type": "RelationshipClass",
            "name": "node_group",
            "object_classes": [1],
            "objects": [1],
        }

        out, errors = read_with_mapping(data, mapping, num_cols, data_header)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    def test_read_relationships(self):
        input_data = [["unit", "node"], ["u1", "n1"], ["u1", "n2"]]
        self.empty_data.update(
            {
                "relationship_classes": [("unit__node", ("unit", "node"))],
                "relationships": [
                    ("unit__node", ("u1", "n1")),
                    ("unit__node", ("u1", "n2")),
                ],
            }
        )

        data = iter(input_data)
        data_header = next(data)
        num_cols = len(data_header)

        mapping = {
            "map_type": "RelationshipClass",
            "name": "unit__node",
            "object_classes": [0, 1],
            "objects": [0, 1],
        }

        out, errors = read_with_mapping(data, mapping, num_cols, data_header)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    def test_read_relationships_with_parameters(self):
        input_data = [
            ["unit", "node", "rel_parameter"],
            ["u1", "n1", 0],
            ["u1", "n2", 1],
        ]
        self.empty_data.update(
            {
                "relationship_classes": [("unit__node", ("unit", "node"))],
                "relationships": [
                    ("unit__node", ("u1", "n1")),
                    ("unit__node", ("u1", "n2")),
                ],
                "relationship_parameters": [("unit__node", "rel_parameter")],
                "relationship_parameter_values": [
                    ("unit__node", ("u1", "n1"), "rel_parameter", 0),
                    ("unit__node", ("u1", "n2"), "rel_parameter", 1),
                ],
            }
        )

        data = iter(input_data)
        data_header = next(data)
        num_cols = len(data_header)

        mapping = {
            "map_type": "RelationshipClass",
            "name": "unit__node",
            "object_classes": [0, 1],
            "objects": [0, 1],
            "parameters": {
                "map_type": "parameter",
                "name": {"map_type": "column_name", "value_reference": 2},
                "value": 2
            },
        }

        out, errors = read_with_mapping(data, mapping, num_cols, data_header)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    def test_read_parameter_header_with_only_one_parameter(self):
        input_data = [["object", "parameter_name1"], ["obj1", 0], ["obj2", 2]]
        self.empty_data.update(
            {
                "object_classes": ["object"],
                "objects": [("object", "obj1"), ("object", "obj2")],
                "object_parameters": [("object", "parameter_name1")],
                "object_parameter_values": [
                    ("object", "obj1", "parameter_name1", 0),
                    ("object", "obj2", "parameter_name1", 2),
                ],
            }
        )

        data = iter(input_data)
        data_header = next(data)
        num_cols = len(data_header)

        mapping = {
            "map_type": "ObjectClass",
            "name": "object",
            "object": 0,
            "parameters": {
                "map_type": "parameter",
                "name": {"map_type": "row", "value_reference": -1},
            },
        }  # -1 to read pivot from header

        out, errors = read_with_mapping(data, mapping, num_cols, data_header)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    def test_read_pivoted_parameters_from_data_with_skipped_column(self):
        input_data = [
            ["object", "parameter_name1", "parameter_name2"],
            ["obj1", 0, 1],
            ["obj2", 2, 3],
        ]
        self.empty_data.update(
            {
                "object_classes": ["object"],
                "objects": [("object", "obj1"), ("object", "obj2")],
                "object_parameters": [("object", "parameter_name1")],
                "object_parameter_values": [
                    ("object", "obj1", "parameter_name1", 0),
                    ("object", "obj2", "parameter_name1", 2),
                ],
            }
        )

        data = iter(input_data)
        # data_header = next(data)
        num_cols = len(input_data[0])

        mapping = {
            "map_type": "ObjectClass",
            "name": "object",
            "object": 0,
            "skip_columns": [2],
            "parameters": {
                "map_type": "parameter",
                "name": {"map_type": "row", "value_reference": 0},
            },
        }  # -1 to read pivot from header

        out, errors = read_with_mapping(data, mapping, num_cols)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    def test_read_relationships_and_save_objects(self):
        input_data = [["unit", "node"], ["u1", "n1"], ["u2", "n2"]]
        self.empty_data.update(
            {
                "relationship_classes": [("unit__node", ("unit", "node"))],
                "relationships": [
                    ("unit__node", ("u1", "n1")),
                    ("unit__node", ("u2", "n2")),
                ],
                "object_classes": ["unit", "node"],
                "objects": [
                    ("unit", "u1"),
                    ("node", "n1"),
                    ("unit", "u2"),
                    ("node", "n2"),
                ],
            }
        )

        data = iter(input_data)
        data_header = next(data)
        num_cols = len(data_header)

        mapping = {
            "map_type": "RelationshipClass",
            "name": "unit__node",
            "object_classes": [0, 1],
            "objects": [0, 1],
            "import_objects": True,
        }

        out, errors = read_with_mapping(data, mapping, num_cols, data_header)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    def test_read_relationships_parameter_values_with_extra_dimensions(self):
        # FIXME: right now the read_with_mapping only keeps the value for
        # mappings with extra dimensions until the data spec is final.
        input_data = [
            ["", "a", "b"],
            ["", "c", "d"],
            ["", "e", "f"],
            ["a", 2, 3],
            ["b", 4, 5],
        ]
        # original test
        # self.empty_data.update(
        #    {
        #        "relationship_classes": [("unit__node", ("unit", "node"))],
        #        "relationship_parameters": [("unit__node", "e"), ("unit__node", "f")],
        #        "relationships": [("unit__node", ("a", "c")), ("unit__node", ("b", "d"))],
        #        "relationship_parameter_values": [
        #            ("unit__node", ("a", "c"), "e", "[[1, 2], [2, 4]]"),
        #            ("unit__node", ("b", "d"), "f", "[[1, 3], [2, 5]]"),
        #        ],
        #    }
        # )

        

        self.empty_data.update(
            {
                "relationship_classes": [("unit__node", ("unit", "node"))],
                "relationship_parameters": [("unit__node", "e"), ("unit__node", "f")],
                "relationships": [
                    ("unit__node", ("a", "c")),
                    ("unit__node", ("b", "d")),
                ],
                "relationship_parameter_values": [
                    ("unit__node", ("a", "c"), "e", TimePattern(["a", "b"], [2, 4])),
                    ("unit__node", ("b", "d"), "f", TimePattern(["a", "b"], [3, 5])),
                ],
            }
        )

        data = iter(input_data)
        data_header = []
        num_cols = 3

        mapping = {
            "map_type": "RelationshipClass",
            "name": "unit__node",
            "object_classes": ["unit", "node"],
            "objects": [{"map_type": "row", "value_reference": i} for i in range(2)],
            "parameters": {
                "map_type": "parameter",
                "parameter_type": "time pattern",
                "name": {"map_type": "row", "value_reference": 2},
                "extra_dimensions": [0],
            },
        }

        out, errors = read_with_mapping(data, mapping, num_cols, data_header)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    def test_read_data_with_read_start_row(self):
        input_data = [
            ["object_class", "object", "parameter", "value"],
            [" ", " ", " ", " "],
            ["oc1", "obj1", "parameter_name1", 1],
            ["oc2", "obj2", "parameter_name2", 2],
        ]
        self.empty_data.update(
            {
                "object_classes": ["oc1", "oc2"],
                "objects": [("oc1", "obj1"), ("oc2", "obj2")],
                "object_parameters": [
                    ("oc1", "parameter_name1"),
                    ("oc2", "parameter_name2"),
                ],
                "object_parameter_values": [
                    ("oc1", "obj1", "parameter_name1", 1),
                    ("oc2", "obj2", "parameter_name2", 2),
                ],
            }
        )

        data = iter(input_data)
        data_header = next(data)
        num_cols = len(data_header)

        mapping = {
            "map_type": "ObjectClass",
            "name": 0,
            "object": 1,
            "parameters": {"map_type": "parameter", "name": 2, "value": 3},
            "read_start_row": 1,
        }

        out, errors = read_with_mapping(data, mapping, num_cols, data_header)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    def test_read_data_with_two_mappings_with_different_read_start_row(self):
        input_data = [
            ["oc1", "oc2", "parameter_class1", "parameter_class2"],
            [" ", " ", " ", " "],
            ["oc1_obj1", "oc2_obj1", 1, 3],
            ["oc1_obj2", "oc2_obj2", 2, 4],
        ]
        self.empty_data.update(
            {
                "object_classes": ["oc1", "oc2"],
                "objects": [("oc1", "oc1_obj1"), ("oc1", "oc1_obj2"), ("oc2", "oc2_obj2")],
                "object_parameters": [
                    ("oc1", "parameter_class1"),
                    ("oc2", "parameter_class2"),
                ],
                "object_parameter_values": [
                    ("oc1", "oc1_obj1", "parameter_class1", 1),
                    ("oc1", "oc1_obj2", "parameter_class1", 2),
                    ("oc2", "oc2_obj2", "parameter_class2", 4),
                ],
            }
        )

        data = iter(input_data)
        data_header = next(data)
        num_cols = len(data_header)

        mapping1 = {
            "map_type": "ObjectClass",
            "name": {"map_type": "column_name", "value_reference": 0},
            "object": 0,
            "parameters": {"map_type": "parameter", "name": {"map_type": "column_name", "value_reference": 2}, "value": 2},
            "read_start_row": 1,
        }
        mapping2 = {
            "map_type": "ObjectClass",
            "name": {"map_type": "column_name", "value_reference": 1},
            "object": 1,
            "parameters": {"map_type": "parameter", "name": {"map_type": "column_name", "value_reference": 3}, "value": 3},
            "read_start_row": 2,
        }

        out, errors = read_with_mapping(data, [mapping1, mapping2], num_cols, data_header)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

if __name__ == "__main__":

    unittest.main()
