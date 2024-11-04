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

""" Contains unit tests for the generator module. """
import unittest
from spinedb_api import Array, DateTime, Duration, Map
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

    def test_import_scenario(self):
        data_source = iter([["scen1"]])
        mappings = [
            [
                {"map_type": "Scenario", "position": 0},
            ]
        ]
        convert_function_specs = {0: "string"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}
        mapped_data, errors = get_mapped_data(data_source, mappings, column_convert_fns=convert_functions)
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {"scenarios": {("scen1",)}},
        )

    def test_importing_multidimensional_class_when_there_is_an_extra_column(self):
        header = ["3D entity class", "unit", "node", "node", "parameter", "alternative", "value", None]
        data_source = iter(
            [
                ["unit__node__node", "Dyson sphere", "Gamma Ceti", "Ring world", "flow", "Base", 23.3, None],
                7 * [None] + ["aa"],
            ]
        )
        mappings = [
            [
                {"map_type": "EntityClass", "position": 0},
                {"map_type": "Dimension", "position": "header", "value": 1},
                {"map_type": "Dimension", "position": "header", "value": 2},
                {"map_type": "Dimension", "position": "header", "value": 3},
                {"map_type": "Entity", "position": "hidden"},
                {"map_type": "Element", "position": 1, "import_entities": True},
                {"map_type": "Element", "position": 2, "import_entities": True},
                {"map_type": "Element", "position": 3, "import_entities": True},
                {"map_type": "EntityMetadata", "position": "hidden"},
                {"map_type": "ParameterDefinition", "position": 4},
                {"map_type": "Alternative", "position": 5},
                {"map_type": "ParameterValueMetadata", "position": "hidden"},
                {"map_type": "ParameterValue", "position": 6},
            ]
        ]
        convert_function_specs = {
            0: "string",
            1: "string",
            2: "string",
            3: "string",
            4: "string",
            5: "string",
            6: "float",
        }
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}
        mapped_data, errors = get_mapped_data(data_source, mappings, header, column_convert_fns=convert_functions)
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {
                "alternatives": {"Base"},
                "entities": [
                    ("unit", "Dyson sphere"),
                    ("node", "Gamma Ceti"),
                    ("node", "Ring world"),
                    ("unit__node__node", ("Dyson sphere", "Gamma Ceti", "Ring world")),
                ],
                "entity_classes": [("unit",), ("node",), ("unit__node__node", ("unit", "node", "node"))],
                "parameter_definitions": [("unit__node__node", "flow")],
                "parameter_values": [
                    ["unit__node__node", ("Dyson sphere", "Gamma Ceti", "Ring world"), "flow", 23.3, "Base"]
                ],
            },
        )

    def test_importing_empty_rows_does_unnecessarily_not_repeat_mapped_data(self):
        header = ["Generator", "HydroGenerator"]
        data_source = iter(
            [["MyHydroGenerator", "MyHydroGenerator"], ["NonHydroGenerator", None], ["OtherGenerator", None]]
        )
        mappings = [
            [
                {"map_type": "EntityClass", "position": "header", "value": 0},
                {"map_type": "Entity", "position": 1},
                {"map_type": "EntityMetadata", "position": "hidden"},
                {"map_type": "ParameterDefinition", "position": "hidden", "value": "Type"},
                {"map_type": "Alternative", "position": "hidden"},
                {"map_type": "ParameterValueMetadata", "position": "hidden"},
                {"map_type": "ParameterValue", "position": "hidden", "value": "Hydro"},
            ]
        ]
        convert_function_specs = {0: "string", 1: "string"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}
        mapped_data, errors = get_mapped_data(data_source, mappings, header, column_convert_fns=convert_functions)
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {
                "entities": [("Generator", "MyHydroGenerator")],
                "entity_classes": [("Generator",)],
                "parameter_definitions": [("Generator", "Type")],
                "parameter_values": [["Generator", "MyHydroGenerator", "Type", "Hydro"]],
            },
        )

    def test_pivoted_mapping_has_position_outside_source_bounds(self):
        data_source = iter(
            [
                ["solve", "period", "time", "A1", "A2", "A3"],
                [None, None, None, "B1", "B2", "B3"],
                [None, None, None, "C1", "C2", "C3"],
                ["y2025", "p2025", "t01", -2.1, -2.2, -2.3],
                ["y2025", "p2025", "t02", -3.1, -3.2, -3.3],
            ]
        )
        mappings = [
            [
                {"map_type": "RelationshipClass", "position": "hidden", "value": "connection__node__node"},
                {"map_type": "RelationshipClassObjectClass", "position": "hidden", "value": "connection"},
                {"map_type": "RelationshipClassObjectClass", "position": "hidden", "value": "node"},
                {"map_type": "RelationshipClassObjectClass", "position": "hidden", "value": "node"},
                {"map_type": "Relationship", "position": "hidden", "value": "relationship"},
                {"map_type": "RelationshipObject", "position": -1, "import_objects": True},
                {"map_type": "RelationshipObject", "position": -2, "import_objects": True},
                {"map_type": "RelationshipObject", "position": -3, "import_objects": True},
                {"map_type": "RelationshipMetadata", "position": "hidden"},
                {"map_type": "ParameterDefinition", "position": "hidden", "value": "flow_t"},
                {"map_type": "Alternative", "position": "hidden"},
                {"map_type": "ParameterValueMetadata", "position": "hidden"},
                {"map_type": "ParameterValueType", "position": "hidden", "value": "map"},
                {"map_type": "IndexName", "position": "hidden", "value": "solve"},
                {"map_type": "ParameterValueIndex", "position": 0},
                {"map_type": "IndexName", "position": "hidden", "value": "period"},
                {"map_type": "ParameterValueIndex", "position": 1},
                {"map_type": "IndexName", "position": "hidden", "value": "time"},
                {"map_type": "ParameterValueIndex", "position": 2},
                {"map_type": "ExpandedValue", "position": 6},
            ]
        ]
        convert_function_specs = {0: "string", 1: "string", 2: "string", 3: "float", 4: "float", 5: "float"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}
        mapped_data, errors = get_mapped_data(data_source, mappings, column_convert_fns=convert_functions)
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {
                "entities": [
                    ("connection", "A1"),
                    ("node", "B1"),
                    ("node", "C1"),
                    ("connection__node__node", ("A1", "B1", "C1")),
                    ("connection", "A2"),
                    ("node", "B2"),
                    ("node", "C2"),
                    ("connection__node__node", ("A2", "B2", "C2")),
                    ("connection", "A3"),
                    ("node", "B3"),
                    ("node", "C3"),
                    ("connection__node__node", ("A3", "B3", "C3")),
                ],
                "entity_classes": [
                    ("connection",),
                    ("node",),
                    ("connection__node__node", ("connection", "node", "node")),
                ],
                "parameter_definitions": [("connection__node__node", "flow_t")],
                "parameter_values": [
                    [
                        "connection__node__node",
                        ("A1", "B1", "C1"),
                        "flow_t",
                        Map(
                            ["y2025"],
                            [
                                Map(
                                    ["p2025"],
                                    [Map(["t01", "t02"], [-2.1, -3.1], index_name="time")],
                                    index_name="period",
                                )
                            ],
                            index_name="solve",
                        ),
                    ],
                    [
                        "connection__node__node",
                        ("A2", "B2", "C2"),
                        "flow_t",
                        Map(
                            ["y2025"],
                            [
                                Map(
                                    ["p2025"],
                                    [Map(["t01", "t02"], [-2.2, -3.2], index_name="time")],
                                    index_name="period",
                                )
                            ],
                            index_name="solve",
                        ),
                    ],
                    [
                        "connection__node__node",
                        ("A3", "B3", "C3"),
                        "flow_t",
                        Map(
                            ["y2025"],
                            [
                                Map(
                                    ["p2025"],
                                    [Map(["t01", "t02"], [-2.3, -3.3], index_name="time")],
                                    index_name="period",
                                )
                            ],
                            index_name="solve",
                        ),
                    ],
                ],
            },
        )

    def test_import_datetime_values(self):
        header = ["entity", "t"]
        data_source = iter([["o1", "2024-06-24T09:00:00"], ["o2", "Jun 24th 2024"]])
        mappings = [
            [
                {"map_type": "EntityClass", "position": "hidden", "value": "Object"},
                {"map_type": "Entity", "position": 0},
                {"map_type": "EntityMetadata", "position": "hidden"},
                {"map_type": "ParameterDefinition", "position": "header"},
                {"map_type": "Alternative", "position": "hidden", "value": "Base"},
                {"map_type": "ParameterValueMetadata", "position": "hidden"},
                {"map_type": "ParameterValue", "position": 1},
            ]
        ]
        convert_function_specs = {0: "string", 1: "datetime"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}
        mapped_data, errors = get_mapped_data(data_source, mappings, header, column_convert_fns=convert_functions)
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {
                "alternatives": {"Base"},
                "entity_classes": [("Object",)],
                "entities": [
                    ("Object", "o1"),
                    ("Object", "o2"),
                ],
                "parameter_definitions": [("Object", "t")],
                "parameter_values": [
                    ["Object", "o1", "t", DateTime("2024-06-24T09:00:00"), "Base"],
                    ["Object", "o2", "t", DateTime("2024-06-24T00:00:00"), "Base"],
                ],
            },
        )

    def test_import_durations(self):
        header = ["entity", "t"]
        data_source = iter([["o1", "23D"], ["o2", "19 days"]])
        mappings = [
            [
                {"map_type": "EntityClass", "position": "hidden", "value": "Object"},
                {"map_type": "Entity", "position": 0},
                {"map_type": "EntityMetadata", "position": "hidden"},
                {"map_type": "ParameterDefinition", "position": "header"},
                {"map_type": "Alternative", "position": "hidden", "value": "Base"},
                {"map_type": "ParameterValueMetadata", "position": "hidden"},
                {"map_type": "ParameterValue", "position": 1},
            ]
        ]
        convert_function_specs = {0: "string", 1: "duration"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}
        mapped_data, errors = get_mapped_data(data_source, mappings, header, column_convert_fns=convert_functions)
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {
                "alternatives": {"Base"},
                "entity_classes": [("Object",)],
                "entities": [
                    ("Object", "o1"),
                    ("Object", "o2"),
                ],
                "parameter_definitions": [("Object", "t")],
                "parameter_values": [
                    ["Object", "o1", "t", Duration("23D"), "Base"],
                    ["Object", "o2", "t", Duration("19D"), "Base"],
                ],
            },
        )

    def test_import_with_one_mapping_name_for_two_mappings(self):
        data_source = iter([["other_name"]])
        mappings = [
            [
                {"map_type": "Alternative", "position": "mapping_name"},
            ],
            [
                {"map_type": "Alternative", "position": 0},
            ],
        ]
        convert_function_specs = {0: "string"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}
        mapped_data, errors = get_mapped_data(
            data_source, mappings, column_convert_fns=convert_functions, mapping_names=["some_name"]
        )
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {"alternatives": {"some_name", "other_name"}},
        )

    def test_import_with_mapping_name_with_too_many_mapping_names(self):
        data_source = iter([["other_name"]])
        mappings = [
            [
                {"map_type": "Alternative", "position": "mapping_name"},
            ],
            [
                {"map_type": "Alternative", "position": 0},
            ],
        ]
        convert_function_specs = {0: "string"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}
        mapped_data, errors = get_mapped_data(
            data_source, mappings, column_convert_fns=convert_functions, mapping_names=["some_name", "other", "null"]
        )
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {"alternatives": {"some_name", "other_name"}},
        )

    def test_import_entity_alternatives_with_activity_string(self):
        header = ["entity", "alternative", "active"]
        data_source = iter([["o1", "Base", "yes"], ["o1", "alt1", "no"], ["o1", "alt2", ""]])
        mappings = [
            [
                {"map_type": "EntityClass", "position": "hidden", "value": "Object"},
                {"map_type": "Entity", "position": 0},
                {"map_type": "Alternative", "position": 1},
                {"map_type": "EntityAlternativeActivity", "position": 2},
            ]
        ]
        convert_function_specs = {0: "string", 1: "string", 2: "string"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}
        mapped_data, errors = get_mapped_data(data_source, mappings, header, column_convert_fns=convert_functions)
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {
                "alternatives": {"Base", "alt1", "alt2"},
                "entity_classes": [("Object",)],
                "entities": [
                    ("Object", "o1"),
                ],
                "entity_alternatives": [("Object", ("o1",), "Base", True), ("Object", ("o1",), "alt1", False)],
            },
        )

    def test_import_entity_alternatives_with_activity_boolean(self):
        header = ["entity", "alternative", "active"]
        data_source = iter([["o1", "Base", True], ["o1", "alt1", False], ["o1", "alt2", None]])
        mappings = [
            [
                {"map_type": "EntityClass", "position": "hidden", "value": "Object"},
                {"map_type": "Entity", "position": 0},
                {"map_type": "Alternative", "position": 1},
                {"map_type": "EntityAlternativeActivity", "position": 2},
            ]
        ]
        convert_function_specs = {0: "string", 1: "string", 2: "boolean"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}
        mapped_data, errors = get_mapped_data(data_source, mappings, header, column_convert_fns=convert_functions)
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {
                "alternatives": {"Base", "alt1", "alt2"},
                "entity_classes": [("Object",)],
                "entities": [
                    ("Object", "o1"),
                ],
                "entity_alternatives": [("Object", ("o1",), "Base", True), ("Object", ("o1",), "alt1", False)],
            },
        )

    def test_import_entity_alternatives_with_activity_integer(self):
        header = ["entity", "alternative", "active"]
        data_source = iter([["o1", "Base", 1], ["o1", "alt1", 0], ["o1", "alt2", None]])
        mappings = [
            [
                {"map_type": "EntityClass", "position": "hidden", "value": "Object"},
                {"map_type": "Entity", "position": 0},
                {"map_type": "Alternative", "position": 1},
                {"map_type": "EntityAlternativeActivity", "position": 2},
            ]
        ]
        convert_function_specs = {0: "string", 1: "string", 2: "float"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}
        mapped_data, errors = get_mapped_data(data_source, mappings, header, column_convert_fns=convert_functions)
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {
                "alternatives": {"Base", "alt1", "alt2"},
                "entity_classes": [("Object",)],
                "entities": [
                    ("Object", "o1"),
                ],
                "entity_alternatives": [("Object", ("o1",), "Base", True), ("Object", ("o1",), "alt1", False)],
            },
        )

    def test_import_entity_alternatives_errors_gracefully_when_activity_cannot_be_converted_to_bool(self):
        header = ["entity", "alternative", "active"]
        data_source = iter([["o1", "Base", "xoxoxo"]])
        mappings = [
            [
                {"map_type": "EntityClass", "position": "hidden", "value": "Object"},
                {"map_type": "Entity", "position": 0},
                {"map_type": "Alternative", "position": 1},
                {"map_type": "EntityAlternativeActivity", "position": 2},
            ]
        ]
        convert_function_specs = {0: "string", 1: "string", 2: "string"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}
        mapped_data, errors = get_mapped_data(data_source, mappings, header, column_convert_fns=convert_functions)
        self.assertEqual(
            errors,
            [
                "Can't convert xoxoxo to entity alternative activity boolean for '('o1',)' in "
                "'Object' with alternative 'Base'"
            ],
        )
        self.assertEqual(
            mapped_data,
            {
                "alternatives": {"Base"},
                "entity_classes": [("Object",)],
                "entities": [
                    ("Object", "o1"),
                ],
                "entity_alternatives": [],
            },
        )

    def test_import_entity_alternatives_with_multidimensional_entities(self):
        header = ["element 1", "element 2", "alternative", "active"]
        data_source = iter([["o1", "p1", "Base", "true"], ["o1", "p1", "alt1", "false"], ["o1", "p2", "alt1", "true"]])
        mappings = [
            [
                {"map_type": "EntityClass", "position": "hidden", "value": "Widget__Gadget"},
                {"map_type": "Dimension", "position": "hidden", "value": "Widget"},
                {"map_type": "Dimension", "position": "hidden", "value": "Gadget"},
                {"map_type": "Entity", "position": "hidden", "value": "relationship"},
                {"map_type": "Element", "position": 0, "import_objects": True},
                {"map_type": "Element", "position": 1, "import_objects": True},
                {"map_type": "Alternative", "position": 2},
                {"map_type": "EntityAlternativeActivity", "position": 3},
            ]
        ]
        convert_function_specs = {0: "string", 1: "string", 2: "string", 3: "string"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}
        mapped_data, errors = get_mapped_data(data_source, mappings, header, column_convert_fns=convert_functions)
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {
                "alternatives": {"Base", "alt1"},
                "entity_classes": [("Widget",), ("Gadget",), ("Widget__Gadget", ("Widget", "Gadget"))],
                "entities": [
                    ("Widget", "o1"),
                    ("Gadget", "p1"),
                    ("Widget__Gadget", ("o1", "p1")),
                    ("Gadget", "p2"),
                    ("Widget__Gadget", ("o1", "p2")),
                ],
                "entity_alternatives": [
                    ("Widget__Gadget", ("o1", "p1"), "Base", True),
                    ("Widget__Gadget", ("o1", "p1"), "alt1", False),
                    ("Widget__Gadget", ("o1", "p2"), "alt1", True),
                ],
            },
        )

    def test_import_parameter_types(self):
        data_source = iter(
            [
                ["Widget", "x", "float"],
                ["Widget", "x", "bool"],
                ["Gadget", "p", "time_pattern"],
                ["Gadget", "q", "str"],
                ["Gadget", "p", "date_time"],
                ["Object", "w", ""],
            ]
        )
        mappings = [
            [
                {"map_type": "EntityClass", "position": 0},
                {"map_type": "ParameterDefinition", "position": 1},
                {"map_type": "ParameterType", "position": 2},
            ]
        ]
        convert_function_specs = {0: "string", 1: "string", 2: "string"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}
        mapped_data, errors = get_mapped_data(data_source, mappings, column_convert_fns=convert_functions)
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {
                "entity_classes": [("Widget",), ("Gadget",), ("Object",)],
                "parameter_definitions": [("Widget", "x"), ("Gadget", "p"), ("Gadget", "q"), ("Object", "w")],
                "parameter_types": [
                    ("Widget", "x", "float"),
                    ("Widget", "x", "bool"),
                    ("Gadget", "p", "time_pattern"),
                    ("Gadget", "q", "str"),
                    ("Gadget", "p", "date_time"),
                ],
            },
        )

    def test_skip_first_row_when_importing_pivoted_data(self):
        data_source = iter(
            [
                [None, "alternative1", "alternative2", "alternative3"],
                ["Scenario1", "Base", "fixed_prices", None],
            ]
        )
        mappings = [
            [
                {"map_type": "Scenario", "position": 0, "read_start_row": 1},
                {"map_type": "ScenarioAlternative", "position": -1},
            ]
        ]
        convert_function_specs = {0: "string", 1: "string", 2: "string", 3: "string"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}
        mapped_data, errors = get_mapped_data(data_source, mappings, column_convert_fns=convert_functions)
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {"scenario_alternatives": [["Scenario1", "Base"], ["Scenario1", "fixed_prices"]]},
        )

    def test_leaf_mapping_with_position_on_row_is_still_considered_as_pivoted(self):
        data_source = iter(
            [
                [None, "Scenario1", "Scenario2"],
                ["Base", "Base", "Base"],
                ["alternative 1", "alt1", "alt1"],
                ["alternative 1", None, "alt2"],
            ]
        )
        mappings = [
            [
                {"map_type": "Scenario", "position": -1},
                {"map_type": "ScenarioAlternative", "position": -3},
            ]
        ]
        convert_function_specs = {0: "string", 1: "string", 2: "string"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}
        mapped_data, errors = get_mapped_data(data_source, mappings, column_convert_fns=convert_functions)
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {
                "scenario_alternatives": [
                    ["Scenario1", "Base"],
                    ["Scenario1", "alt1"],
                    ["Scenario2", "Base"],
                    ["Scenario2", "alt1"],
                    ["Scenario2", "alt2"],
                ]
            },
        )

    def test_column_header_position_while_leaf_is_hidden(self):
        header = ["Widget"]
        data_source = iter(
            [
                ["gadget"],
            ]
        )
        mappings = [
            [
                {"map_type": "EntityClass", "position": "header", "value": 0},
                {"map_type": "Entity", "position": 0},
                {"map_type": "EntityMetadata", "position": "hidden"},
            ]
        ]
        convert_function_specs = {0: "string"}
        convert_functions = {column: value_to_convert_spec(spec) for column, spec in convert_function_specs.items()}
        mapped_data, errors = get_mapped_data(data_source, mappings, header, column_convert_fns=convert_functions)
        self.assertEqual(errors, [])
        self.assertEqual(
            mapped_data,
            {
                "entity_classes": [
                    ("Widget",),
                ],
                "entities": [("Widget", "gadget")],
            },
        )


if __name__ == "__main__":
    unittest.main()
