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
from spinedb_api.import_mapping.generator import get_mapped_data


class TestGetMappedData(unittest.TestCase):
    def test_does_not_give_traceback_when_pivoted_mapping_encounters_empty_data(self):
        data_source = iter([])
        mappings = [[
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
        ]]
        mapped_data, errors = get_mapped_data(data_source, mappings)
        self.assertEqual(errors, [])
        self.assertEqual(mapped_data, {})


if __name__ == '__main__':
    unittest.main()
