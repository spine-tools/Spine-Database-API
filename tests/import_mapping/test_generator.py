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
Contains unit tests for the generator module.

:author: A. Soininen (VTT)
:date:   2.2.2022
"""
import unittest

from spinedb_api import Map
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
                'alternatives': ['Base', 'Base'],
                'object_classes': ['Object', 'Object'],
                'object_parameter_values': [['Object', 'data', 'Parameter', Map(["T1", "T2"], [5.0, 99.0]), 'Base']],
                'object_parameters': [['Object', 'Parameter'], ['Object', 'Parameter']],
                'objects': [('Object', 'data'), ('Object', 'data')],
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
                'alternatives': ['Base', 'Base'],
                'object_classes': ['Object', 'Object'],
                'object_parameter_values': [['Object', 'data', 'Parameter', Map(["T1", "T2"], [5.0, 99.0]), 'Base']],
                'object_parameters': [['Object', 'Parameter'], ['Object', 'Parameter']],
                'objects': [('Object', 'data'), ('Object', 'data')],
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
                'object_classes': ['klass', 'klass'],
                'object_parameter_values': [['klass', 'kloss', 'Parameter_2', Map(["T1", "T2"], [2.3, 23.0])]],
                'object_parameters': [['klass', 'Parameter_2'], ['klass', 'Parameter_2']],
                'objects': [('klass', 'kloss'), ('klass', 'kloss')],
            },
        )


if __name__ == '__main__':
    unittest.main()
