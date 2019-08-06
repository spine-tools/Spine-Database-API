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
from unittest.mock import MagicMock
from spinedb_api.json_mapping import read_with_mapping, ObjectClassMapping, RelationshipClassMapping


class TestMappingIO(unittest.TestCase):
    def test_ObjectClass_to_dict_from_dict(self):
        mapping = {
            "map_type": "ObjectClass",
            "name": 0,
            "object": 1,
            "parameters": {"map_type": "parameter", "name": 2, "value": 3},
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
            },
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
        }
        self.assertEqual(out, expected)

        mapping = {"map_type": "ObjectClass", "name": "str", "object": "str"}

        map_obj = ObjectClassMapping.from_dict(mapping)
        out = map_obj.to_dict()

        expected = {"map_type": "ObjectClass", "name": "str", "object": "str"}
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
            "objects": [{"value_reference": 0, "map_type": "column"}, {"value_reference": 1, "map_type": "column"}],
            "parameters": {
                "map_type": "parameter",
                "name": "test",
                "value": {"value_reference": 2, "map_type": "column"},
            },
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
            "object_classes": ["test", {"value_reference": 0, "map_type": "column_name"}],
            "objects": ["test", {"value_reference": 0, "map_type": "column"}],
        }
        self.assertEqual(out, expected)

    def test_RelationshipClassMapping_from_dict_to_dict3(self):
        mapping = mapping = {
            "map_type": "RelationshipClass",
            "name": "unit__node",
            "parameters": {"map_type": "parameter", "name": "test", "value": 2, "extra_dimensions": ["test", 0]},
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
                "value": {"value_reference": 2, "map_type": "column"},
                "extra_dimensions": ["test", {"value_reference": 0, "map_type": "column"}],
            },
        }
        self.assertEqual(out, expected)


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
                "object_parameters": [("oc1", "parameter_name1"), ("oc2", "parameter_name2")],
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

    def test_read_flat_file_with_column_name_reference(self):
        input_data = [["object", "parameter", "value"], ["obj1", "parameter_name1", 1], ["obj2", "parameter_name2", 2]]
        self.empty_data.update({"object_classes": ["object"], "objects": [("object", "obj1"), ("object", "obj2")]})

        data = iter(input_data)
        data_header = next(data)
        num_cols = len(data_header)

        mapping = {"map_type": "ObjectClass", "name": {"map_type": "column_name", "value_reference": 0}, "object": 0}

        out, errors = read_with_mapping(data, mapping, num_cols, data_header)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    def test_read_with_list_of_mappings(self):
        input_data = [["object", "parameter", "value"], ["obj1", "parameter_name1", 1], ["obj2", "parameter_name2", 2]]
        self.empty_data.update({"object_classes": ["object"], "objects": [("object", "obj1"), ("object", "obj2")]})

        data = iter(input_data)
        data_header = next(data)
        num_cols = len(data_header)

        mapping = {"map_type": "ObjectClass", "name": {"map_type": "column_name", "value_reference": 0}, "object": 0}

        out, errors = read_with_mapping(data, [mapping], num_cols, data_header)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    def test_read_columncollection_parameters(self):
        input_data = [["object", "parameter_name1", "parameter_name2"], ["obj1", 0, 1], ["obj2", 2, 3]]
        self.empty_data.update(
            {
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
        input_data = [["object", "parameter_name1", "parameter_name2"], ["obj1", 0, 1], ["obj2", 2, 3]]
        self.empty_data.update(
            {
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
        )

        data = iter(input_data)
        data_header = next(data)
        num_cols = len(data_header)

        mapping = {
            "map_type": "ObjectClass",
            "name": {"map_type": "column_name", "value_reference": 0},
            "object": 0,
            "parameters": {"map_type": "parameter", "name": {"map_type": "row", "value_reference": -1}},
        }  # -1 to read pivot from header

        out, errors = read_with_mapping(data, mapping, num_cols, data_header)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    def test_read_pivoted_parameters_from_data(self):
        input_data = [["object", "parameter_name1", "parameter_name2"], ["obj1", 0, 1], ["obj2", 2, 3]]
        self.empty_data.update(
            {
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
        )

        data = iter(input_data)
        # data_header = next(data)
        num_cols = len(input_data[0])

        mapping = {
            "map_type": "ObjectClass",
            "name": "object",
            "object": 0,
            "parameters": {"map_type": "parameter", "name": {"map_type": "row", "value_reference": 0}},
        }  # -1 to read pivot from header

        out, errors = read_with_mapping(data, mapping, num_cols)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    def test_read_flat_file_with_extra_value_dimensions(self):
        # FIXME: right now the read_with_mapping only keeps the value for
        # mappings with extra dimensions until the data spec is final.
        input_data = [["object", "time", "parameter_name1"], ["obj1", "t1", 1], ["obj1", "t2", 2]]
        # orginal test data
        #        self.empty_data.update({'object_classes': ['object'],
        #                                'objects': [('object','obj1'), ('object','obj1')],
        #                                'object_parameters': [('object','parameter_name1')],
        #                                'object_parameter_values': [('object','obj1','parameter_name1','[["scenario1", "t1", 1], ["scenario1", "t2", 2]]')]})

        self.empty_data.update(
            {
                "object_classes": ["object"],
                "objects": [("object", "obj1"), ("object", "obj1")],
                "object_parameters": [("object", "parameter_name1")],
                "object_parameter_values": [("object", "obj1", "parameter_name1", '[{"t1": 1}, {"t2": 2}]')],
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
                "map_type": "parameter_column_collection",
                "parameters": [2],
                "extra_dimensions": ["scenario1", 1],
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

        mapping = {"map_type": "RelationshipClass", "name": "node_group", "object_classes": [1], "objects": [1]}

        out, errors = read_with_mapping(data, mapping, num_cols, data_header)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    def test_read_relationships(self):
        input_data = [["unit", "node"], ["u1", "n1"], ["u1", "n2"]]
        self.empty_data.update(
            {
                "relationship_classes": [("unit__node", ("unit", "node"))],
                "relationships": [("unit__node", ("u1", "n1")), ("unit__node", ("u1", "n2"))],
            }
        )

        data = iter(input_data)
        data_header = next(data)
        num_cols = len(data_header)

        mapping = {"map_type": "RelationshipClass", "name": "unit__node", "object_classes": [0, 1], "objects": [0, 1]}

        out, errors = read_with_mapping(data, mapping, num_cols, data_header)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    def test_read_relationships_with_parameters(self):
        input_data = [["unit", "node", "rel_parameter"], ["u1", "n1", 0], ["u1", "n2", 1]]
        self.empty_data.update(
            {
                "relationship_classes": [("unit__node", ("unit", "node"))],
                "relationships": [("unit__node", ("u1", "n1")), ("unit__node", ("u1", "n2"))],
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
            "parameters": [2],
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
            "parameters": {"map_type": "parameter", "name": {"map_type": "row", "value_reference": -1}},
        }  # -1 to read pivot from header

        out, errors = read_with_mapping(data, mapping, num_cols, data_header)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    def test_read_pivoted_parameters_from_data_with_skipped_column(self):
        input_data = [["object", "parameter_name1", "parameter_name2"], ["obj1", 0, 1], ["obj2", 2, 3]]
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
            "parameters": {"map_type": "parameter", "name": {"map_type": "row", "value_reference": 0}},
        }  # -1 to read pivot from header

        out, errors = read_with_mapping(data, mapping, num_cols)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])

    def test_read_relationships_and_save_objects(self):
        input_data = [["unit", "node"], ["u1", "n1"], ["u2", "n2"]]
        self.empty_data.update(
            {
                "relationship_classes": [("unit__node", ("unit", "node"))],
                "relationships": [("unit__node", ("u1", "n1")), ("unit__node", ("u2", "n2"))],
                "object_classes": ["unit", "node"],
                "objects": [("unit", "u1"), ("node", "n1"), ("unit", "u2"), ("node", "n2")],
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
        input_data = [["", "a", "b"], ["", "c", "d"], ["", "e", "f"], [1, 2, 3], [2, 4, 5]]
        # original test
        #        self.empty_data.update({'relationship_classes': [('unit__node',('unit','node'))],
        #                                'relationship_parameters': [('unit__node', 'e'), ('unit__node', 'f')],
        #                                'relationships': [('unit__node',('a','c')),
        #                                                  ('unit__node',('b','d'))],
        #                                'relationship_parameter_values': [('unit__node',('a','c'), 'e', '[[1, 2], [2, 4]]'),
        #                                                                  ('unit__node',('b','d'), 'f', '[[1, 3], [2, 5]]')]
        #                                })

        self.empty_data.update(
            {
                "relationship_classes": [("unit__node", ("unit", "node"))],
                "relationship_parameters": [("unit__node", "e"), ("unit__node", "f")],
                "relationships": [("unit__node", ("a", "c")), ("unit__node", ("b", "d"))],
                "relationship_parameter_values": [
                    ("unit__node", ("a", "c"), "e", '[{"1": 2}, {"2": 4}]'),
                    ("unit__node", ("b", "d"), "f", '[{"1": 3}, {"2": 5}]'),
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
                "name": {"map_type": "row", "value_reference": 2},
                "extra_dimensions": [0],
            },
        }

        out, errors = read_with_mapping(data, mapping, num_cols, data_header)
        self.assertEqual(out, self.empty_data)
        self.assertEqual(errors, [])


if __name__ == "__main__":

    unittest.main()
