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
Contains unit tests for the generator module.

"""
import unittest

from spinedb_api import Array, Map
from spinedb_api.import_mapping.generator import get_mapped_data
from spinedb_api.import_mapping.type_conversion import value_to_convert_spec


class TestGetMappedData(unittest.TestCase):
    def test_does_not_give_traceback_when_pivoted_mapping_encounters_empty_data(self):
        data_source = iter([])
        mappings = [
            [
                {"map_type": "RelationshipClass", "position": "hidden", "value": "unit__sourceNode"},
                {"map_type": "RelationshipClassObjectClass", "position": "hidden", "value": "unit"},
                {"map_type": "RelationshipClassObjectClass", "position": "hidden", "value": "node"},
                {"map_type": "Relationship", "position": "hidden", "value": "relationship"},
                {"map_type": "RelationshipObject", "position": 1},
                {"map_type": "RelationshipObject", "position": 2},
                {"map_type": "RelationshipMetadata", "position": 3},
                {"map_type": "ParameterDefinition", "position": -2},
                {"map_type": "Alternative", "position": 0},
                {"map_type": "ParameterValueMetadata", "position": "hidden"},
                {"map_type": "ParameterValueType", "position": "hidden", "value": "map"},
                {"map_type": "IndexName", "position": "hidden", "value": "constraint"},
                {"map_type": "ParameterValueIndex", "position": 4},
                {"map_type": "ExpandedValue", "position": "hidden"},
            ]
        ]
        mapped_data, errors = get_mapped_data(data_source, mappings)
        self.assertEqual(errors, [])
        self.assertEqual(mapped_data, {})

    def test_returns_appropriate_error_if_last_row_is_empty(self):
        data_source = iter([["", "T1", "T2"], ["Parameter", "5.0", "99.0"], [" "]])
        mappings = [
            [
                {"map_type": "ObjectClass", "position": "hidden", "value": "Object"},
                {"map_type": "Object", "position": "hidden", "value": "data"},
                {"map_type": "ObjectMetadata", "position": "hidden"},
                {"map_type": "ParameterDefinition", "position": 0},
                {"map_type": "Alternative", "position": "hidden", "value": "Base"},
                {"map_type": "ParameterValueMetadata", "position": "hidden"},
                {"map_type": "ParameterValueType", "position": "hidden", "value": "map"},
                {"map_type": "IndexName", "position": "hidden"},
                {"map_type": "ParameterValueIndex", "position": -1},
                {"map_type": "ExpandedValue", "position": "hidden"},
            ]
        ]
        convert_function_specs = {0: "string", 1: "float", 2: "float"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}

        mapped_data, errors = get_mapped_data(data_source, mappings, column_convert_fns=convert_functions)
        self.assertEqual(errors, ["Could not process incomplete row 2"])
        self.assertEqual(
            mapped_data,
            {
                "alternatives": {"Base"},
                "entity_classes": [("Object",)],
                "parameter_values": [["Object", "data", "Parameter", Map(["T1", "T2"], [5.0, 99.0]), "Base"]],
                "parameter_definitions": [("Object", "Parameter")],
                "entities": [("Object", "data")],
            },
        )

    def test_convert_functions_get_expanded_over_last_defined_column_in_pivoted_data(self):
        data_source = iter([["", "T1", "T2"], ["Parameter", "5.0", "99.0"]])
        mappings = [
            [
                {"map_type": "ObjectClass", "position": "hidden", "value": "Object"},
                {"map_type": "Object", "position": "hidden", "value": "data"},
                {"map_type": "ObjectMetadata", "position": "hidden"},
                {"map_type": "ParameterDefinition", "position": 0},
                {"map_type": "Alternative", "position": "hidden", "value": "Base"},
                {"map_type": "ParameterValueMetadata", "position": "hidden"},
                {"map_type": "ParameterValueType", "position": "hidden", "value": "map"},
                {"map_type": "IndexName", "position": "hidden"},
                {"map_type": "ParameterValueIndex", "position": -1},
                {"map_type": "ExpandedValue", "position": "hidden"},
            ]
        ]
        convert_function_specs = {0: "string", 1: "float"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}

        mapped_data, errors = get_mapped_data(data_source, mappings, column_convert_fns=convert_functions)
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {
                "alternatives": {"Base"},
                "entity_classes": [("Object",)],
                "parameter_values": [["Object", "data", "Parameter", Map(["T1", "T2"], [5.0, 99.0]), "Base"]],
                "parameter_definitions": [("Object", "Parameter")],
                "entities": [("Object", "data")],
            },
        )

    def test_read_start_row_skips_rows_in_pivoted_data(self):
        data_source = iter([["", "T1", "T2"], ["Parameter_1", "5.0", "99.0"], ["Parameter_2", "2.3", "23.0"]])
        mappings = [
            [
                {"map_type": "ObjectClass", "position": "hidden", "value": "klass", "read_start_row": 2},
                {"map_type": "Object", "position": "hidden", "value": "kloss"},
                {"map_type": "ObjectMetadata", "position": "hidden"},
                {"map_type": "ParameterDefinition", "position": 0},
                {"map_type": "Alternative", "position": "hidden"},
                {"map_type": "ParameterValueMetadata", "position": "hidden"},
                {"map_type": "ParameterValueType", "position": "hidden", "value": "map"},
                {"map_type": "IndexName", "position": "hidden"},
                {"map_type": "ParameterValueIndex", "position": -1},
                {"map_type": "ExpandedValue", "position": "hidden"},
            ]
        ]
        convert_function_specs = {0: "string", 1: "float", 2: "float"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}

        mapped_data, errors = get_mapped_data(data_source, mappings, column_convert_fns=convert_functions)
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {
                "entity_classes": [("klass",)],
                "parameter_values": [["klass", "kloss", "Parameter_2", Map(["T1", "T2"], [2.3, 23.0])]],
                "parameter_definitions": [("klass", "Parameter_2")],
                "entities": [("klass", "kloss")],
            },
        )

    def test_empty_pivoted_data_is_skipped(self):
        data_header = ["period", "time"]
        data_source = iter([["p2020", "t0"], ["p2020", "t1"]])
        mappings = [
            [
                {"map_type": "ObjectClass", "position": "hidden", "value": "unit"},
                {"map_type": "Object", "position": "header"},
                {"map_type": "ObjectMetadata", "position": "hidden"},
                {"map_type": "ParameterDefinition", "position": "hidden", "value": "price"},
                {"map_type": "Alternative", "position": "hidden"},
                {"map_type": "ParameterValueMetadata", "position": "hidden"},
                {"map_type": "ParameterValueType", "position": "hidden", "value": "map"},
                {"map_type": "IndexName", "position": "hidden", "value": "period"},
                {"map_type": "ParameterValueIndex", "position": 0},
                {"map_type": "IndexName", "position": "hidden", "value": "time"},
                {"map_type": "ParameterValueIndex", "position": 1},
                {"map_type": "ExpandedValue", "position": "hidden"},
            ]
        ]
        mapped_data, errors = get_mapped_data(data_source, mappings, data_header)
        self.assertEqual(errors, [])
        self.assertEqual(mapped_data, {})

    def test_map_without_values_is_ignored_and_not_interpreted_as_null(self):
        data_source = iter([["map index", "parameter_name"], ["t1", None]])
        mappings = [
            [
                {"map_type": "ObjectClass", "position": "hidden", "value": "o"},
                {"map_type": "Object", "position": "hidden", "value": "o1"},
                {"map_type": "ObjectMetadata", "position": "hidden"},
                {"map_type": "ParameterDefinition", "position": -1},
                {"map_type": "Alternative", "position": "hidden", "value": "base"},
                {"map_type": "ParameterValueMetadata", "position": "hidden"},
                {"map_type": "ParameterValueType", "position": "hidden", "value": "map"},
                {"map_type": "IndexName", "position": "hidden"},
                {"map_type": "ParameterValueIndex", "position": 0},
                {"map_type": "ExpandedValue", "position": "hidden"},
            ]
        ]
        convert_function_specs = {0: "string", 1: "float"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}
        mapped_data, errors = get_mapped_data(data_source, mappings, column_convert_fns=convert_functions)
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {
                "alternatives": {"base"},
                "entity_classes": [("o",)],
                "parameter_definitions": [("o", "parameter_name")],
                "parameter_values": [],
                "entities": [("o", "o1")],
            },
        )

    def test_import_object_works_with_multiple_relationship_object_imports(self):
        header = ["time", "relationship 1", "relationship 2", "relationship 3"]
        data_source = iter([[None, "o1", "o2", "o1"], [None, "q1", "q2", "q2"], ["t1", 11, 33, 55], ["t2", 22, 44, 66]])
        mappings = [
            [
                {"map_type": "RelationshipClass", "position": "hidden", "value": "o_to_q"},
                {"map_type": "RelationshipClassObjectClass", "position": "hidden", "value": "o"},
                {"map_type": "RelationshipClassObjectClass", "position": "hidden", "value": "q"},
                {"map_type": "Relationship", "position": "hidden", "value": "relationship"},
                {"map_type": "RelationshipObject", "position": -1, "import_objects": True},
                {"map_type": "RelationshipObject", "position": -2, "import_objects": True},
                {"map_type": "RelationshipMetadata", "position": "hidden"},
                {"map_type": "ParameterDefinition", "position": "hidden", "value": "param"},
                {"map_type": "Alternative", "position": "hidden", "value": "base"},
                {"map_type": "ParameterValueMetadata", "position": "hidden"},
                {"map_type": "ParameterValueType", "position": "hidden", "value": "map"},
                {"map_type": "IndexName", "position": "hidden", "value": "time"},
                {"map_type": "ParameterValueIndex", "position": 0},
                {"map_type": "ExpandedValue", "position": "hidden"},
            ]
        ]
        convert_function_specs = {0: "string", 1: "float", 2: "float", 3: "float"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}
        mapped_data, errors = get_mapped_data(data_source, mappings, header, column_convert_fns=convert_functions)
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {
                "alternatives": {"base"},
                "entity_classes": [("o",), ("q",), ("o_to_q", ("o", "q"))],
                "entities": [
                    ("o", "o1"),
                    ("q", "q1"),
                    ("o_to_q", ("o1", "q1")),
                    ("o", "o2"),
                    ("q", "q2"),
                    ("o_to_q", ("o2", "q2")),
                    ("o_to_q", ("o1", "q2")),
                ],
                "parameter_definitions": [("o_to_q", "param")],
                "parameter_values": [
                    ["o_to_q", ("o1", "q1"), "param", Map(["t1", "t2"], [11, 22], index_name="time"), "base"],
                    ["o_to_q", ("o2", "q2"), "param", Map(["t1", "t2"], [33, 44], index_name="time"), "base"],
                    ["o_to_q", ("o1", "q2"), "param", Map(["t1", "t2"], [55, 66], index_name="time"), "base"],
                ],
            },
        )

    def test_default_convert_function_in_column_convert_functions(self):
        data_source = iter([["", "T1", "T2"], ["Parameter_1", "5.0", "99.0"], ["Parameter_2", "2.3", "23.0"]])
        mappings = [
            [
                {"map_type": "ObjectClass", "position": "hidden", "value": "klass", "read_start_row": 2},
                {"map_type": "Object", "position": "hidden", "value": "kloss"},
                {"map_type": "ObjectMetadata", "position": "hidden"},
                {"map_type": "ParameterDefinition", "position": 0},
                {"map_type": "Alternative", "position": "hidden"},
                {"map_type": "ParameterValueMetadata", "position": "hidden"},
                {"map_type": "ParameterValueType", "position": "hidden", "value": "map"},
                {"map_type": "IndexName", "position": "hidden"},
                {"map_type": "ParameterValueIndex", "position": -1},
                {"map_type": "ExpandedValue", "position": "hidden"},
            ]
        ]
        convert_function_specs = {0: "string"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}
        mapped_data, errors = get_mapped_data(
            data_source, mappings, column_convert_fns=convert_functions, default_column_convert_fn=float
        )
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {
                "entity_classes": [("klass",)],
                "parameter_values": [["klass", "kloss", "Parameter_2", Map(["T1", "T2"], [2.3, 23.0])]],
                "parameter_definitions": [("klass", "Parameter_2")],
                "entities": [("klass", "kloss")],
            },
        )

    def test_identity_function_is_used_as_convert_function_when_no_convert_functions_given(self):
        data_source = iter([["", "T1", "T2"], ["Parameter_1", "5.0", "99.0"], ["Parameter_2", "2.3", "23.0"]])
        mappings = [
            [
                {"map_type": "ObjectClass", "position": "hidden", "value": "klass", "read_start_row": 2},
                {"map_type": "Object", "position": "hidden", "value": "kloss"},
                {"map_type": "ObjectMetadata", "position": "hidden"},
                {"map_type": "ParameterDefinition", "position": 0},
                {"map_type": "Alternative", "position": "hidden"},
                {"map_type": "ParameterValueMetadata", "position": "hidden"},
                {"map_type": "ParameterValueType", "position": "hidden", "value": "map"},
                {"map_type": "IndexName", "position": "hidden"},
                {"map_type": "ParameterValueIndex", "position": -1},
                {"map_type": "ExpandedValue", "position": "hidden"},
            ]
        ]
        mapped_data, errors = get_mapped_data(data_source, mappings)
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {
                "entity_classes": [("klass",)],
                "parameter_values": [["klass", "kloss", "Parameter_2", Map(["T1", "T2"], ["2.3", "23.0"])]],
                "parameter_definitions": [("klass", "Parameter_2")],
                "entities": [("klass", "kloss")],
            },
        )

    def test_last_convert_function_gets_used_as_default_convert_function_when_no_default_is_set(self):
        data_source = iter([["", "T1", "T2"], ["Parameter_1", "5.0", "99.0"], ["Parameter_2", "2.3", "23.0"]])
        mappings = [
            [
                {"map_type": "ObjectClass", "position": "hidden", "value": "klass", "read_start_row": 2},
                {"map_type": "Object", "position": "hidden", "value": "kloss"},
                {"map_type": "ObjectMetadata", "position": "hidden"},
                {"map_type": "ParameterDefinition", "position": 0},
                {"map_type": "Alternative", "position": "hidden"},
                {"map_type": "ParameterValueMetadata", "position": "hidden"},
                {"map_type": "ParameterValueType", "position": "hidden", "value": "map"},
                {"map_type": "IndexName", "position": "hidden"},
                {"map_type": "ParameterValueIndex", "position": -1},
                {"map_type": "ExpandedValue", "position": "hidden"},
            ]
        ]
        convert_function_specs = {0: "string", 1: "float"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}
        mapped_data, errors = get_mapped_data(data_source, mappings, column_convert_fns=convert_functions)
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {
                "entity_classes": [("klass",)],
                "parameter_values": [["klass", "kloss", "Parameter_2", Map(["T1", "T2"], [2.3, 23.0])]],
                "parameter_definitions": [("klass", "Parameter_2")],
                "entities": [("klass", "kloss")],
            },
        )

    def test_array_parameters_get_imported_correctly_when_objects_are_in_header(self):
        header = ["object_1", "object_2"]
        data_source = iter([[-1.1, 2.3], [1.1, -2.3]])
        mappings = [
            [
                {"map_type": "ObjectClass", "position": "hidden", "value": "class"},
                {"map_type": "Object", "position": "header"},
                {"map_type": "ObjectMetadata", "position": "hidden"},
                {"map_type": "ParameterDefinition", "position": "hidden", "value": "param"},
                {"map_type": "Alternative", "position": "hidden", "value": "Base"},
                {"map_type": "ParameterValueMetadata", "position": "hidden"},
                {"map_type": "ParameterValueType", "position": "hidden", "value": "array"},
                {"map_type": "IndexName", "position": "hidden"},
                {"map_type": "ParameterValueIndex", "position": "hidden"},
                {"map_type": "ExpandedValue", "position": "hidden"},
            ]
        ]
        convert_function_specs = {0: "float", 1: "float"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}

        mapped_data, errors = get_mapped_data(data_source, mappings, header, column_convert_fns=convert_functions)
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {
                "alternatives": {"Base"},
                "entity_classes": [("class",)],
                "parameter_values": [
                    ["class", "object_1", "param", Array([-1.1, 1.1]), "Base"],
                    ["class", "object_2", "param", Array([2.3, -2.3]), "Base"],
                ],
                "parameter_definitions": [("class", "param")],
                "entities": [("class", "object_1"), ("class", "object_2")],
            },
        )

    def test_arrays_get_imported_correctly_when_objects_are_in_header_and_alternatives_in_first_row(self):
        header = ["object_1", "object_2"]
        data_source = iter([["Base", "Base"], [-1.1, 2.3], [1.1, -2.3]])
        mappings = [
            [
                {"map_type": "ObjectClass", "position": "hidden", "value": "Gadget"},
                {"map_type": "Object", "position": "header"},
                {"map_type": "ObjectMetadata", "position": "hidden"},
                {"map_type": "ParameterDefinition", "position": "hidden", "value": "data"},
                {"map_type": "Alternative", "position": -1},
                {"map_type": "ParameterValueMetadata", "position": "hidden"},
                {"map_type": "ParameterValueType", "position": "hidden", "value": "array"},
                {"map_type": "IndexName", "position": "hidden"},
                {"map_type": "ParameterValueIndex", "position": "hidden"},
                {"map_type": "ExpandedValue", "position": "hidden"},
            ]
        ]
        convert_function_specs = {0: "float", 1: "float"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}

        mapped_data, errors = get_mapped_data(data_source, mappings, header, column_convert_fns=convert_functions)
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {
                "alternatives": {"Base"},
                "entity_classes": [("Gadget",)],
                "parameter_values": [
                    ["Gadget", "object_1", "data", Array([-1.1, 1.1]), "Base"],
                    ["Gadget", "object_2", "data", Array([2.3, -2.3]), "Base"],
                ],
                "parameter_definitions": [("Gadget", "data")],
                "entities": [("Gadget", "object_1"), ("Gadget", "object_2")],
            },
        )

    def test_header_position_is_ignored_in_last_mapping_if_other_mappings_are_in_header(self):
        header = ["Dimension", "parameter1", "parameter2"]
        data_source = iter([["d1", 1.1, -2.3], ["d2", -1.1, 2.3]])
        mappings = [
            [
                {"map_type": "ObjectClass", "position": "table_name"},
                {"map_type": "Object", "position": 0},
                {"map_type": "ObjectMetadata", "position": "hidden"},
                {"map_type": "ParameterDefinition", "position": "header"},
                {"map_type": "Alternative", "position": "hidden", "value": "Base"},
                {"map_type": "ParameterValueMetadata", "position": "hidden"},
                {"map_type": "ParameterValue", "position": "header"},
            ]
        ]
        convert_function_specs = {0: "string", 1: "float", 2: "float"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}

        mapped_data, errors = get_mapped_data(
            data_source, mappings, header, table_name="Data", column_convert_fns=convert_functions
        )
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {
                "alternatives": {"Base"},
                "entity_classes": [("Data",)],
                "parameter_values": [
                    ["Data", "d1", "parameter1", 1.1, "Base"],
                    ["Data", "d1", "parameter2", -2.3, "Base"],
                    ["Data", "d2", "parameter1", -1.1, "Base"],
                    ["Data", "d2", "parameter2", 2.3, "Base"],
                ],
                "parameter_definitions": [("Data", "parameter1"), ("Data", "parameter2")],
                "entities": [("Data", "d1"), ("Data", "d2")],
            },
        )


if __name__ == "__main__":
    unittest.main()
