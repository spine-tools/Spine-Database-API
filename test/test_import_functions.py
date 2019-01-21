######################################################################################################################
# Copyright (C) 2017 - 2018 Spine project consortium
# This file is part of Spine Toolbox.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Unit tests for import_functions.py.

:author: P. Vennström (VTT)
:date:   17.12.2018
"""
#import sys
#sys.path.append('/spinedatabase_api')


import unittest
from collections import namedtuple
from unittest.mock import MagicMock
from sqlalchemy.orm import Session

from spinedatabase_api.diff_database_mapping import DiffDatabaseMapping
from spinedatabase_api.helpers import create_new_spine_database
from spinedatabase_api.import_functions import (import_object_classes,
                              import_object_parameter_values,
                              import_object_parameters, import_objects,
                              import_relationship_classes,
                              import_relationship_parameter_values,
                              import_relationship_parameters,
                              import_relationships,
                              import_data)


def create_mock_db_map():
    # dataclasses for database_api
    ObjectClass = namedtuple("ObjectClass", ["name", "id"])
    Object = namedtuple("Object", ["name", "id", "class_id"])
    Parameter = namedtuple("Parameter",
                           ["name", "id", "object_class_id", "relationship_class_id"])
    RelationshipClass = namedtuple("RelationshipClass",
                                   ["name", "id", "object_class_id_list", "object_class_name_list"])
    Relationship = namedtuple("Relationship",
                              ["name", "id", "object_id_list", "class_id"])
    ParameterValue = namedtuple("ParameterValue",
                                ["id", "parameter_id", "object_id", "relationship_id"])

    # mock data
    existing_object_classes = [ObjectClass(
        'existing_oc1', 1), ObjectClass('existing_oc2', 2)]
    existing_rel_class = [RelationshipClass('existing_rc1', 1, "1,2", "existing_oc1,existing_oc2"),
                          RelationshipClass('existing_rc2', 2, "2,1", "existing_oc2,existing_oc1")]
    existing_parameter = [Parameter("existing_p1", 1, 1, None),
                          Parameter("existing_p2", 2, None, 1),
                          Parameter("existing_p3", 3, 1, None),
                          Parameter("existing_p4", 4, None, 1)]
    existing_objects = [Object('existing_o1', 1, 1),
                        Object('existing_o2', 2, 2)]
    existing_relationship = [Relationship("existing_r1", 1, "1,2", 1)]
    existing_parameter_value = [ParameterValue(1, 1, 1, None),
                                ParameterValue(2, 2, None, 1)]

    # Mock DiffDatabaseMapping
    db_map = MagicMock()
    db_map.object_class_list.return_value = existing_object_classes
    db_map.object_list.return_value = existing_objects
    db_map.wide_relationship_class_list.return_value = existing_rel_class
    db_map.parameter_list.return_value = existing_parameter
    db_map.wide_relationship_list.return_value = existing_relationship
    db_map.object_parameter_value_list.return_value = [
        p for p in existing_parameter_value if p.object_id != None]
    db_map.relationship_parameter_value_list.return_value = [
        p for p in existing_parameter_value if p.relationship_id != None]
    query = MagicMock()
    db_map.add_object_classes.return_value = [query, []]
    db_map.add_objects.return_value = [query, []]
    db_map.add_wide_relationship_classes.return_value = [query, []]
    db_map.add_parameters.return_value = [query, []]
    db_map.add_wide_relationships.return_value = [query, []]
    db_map.add_parameter_values.return_value = [query, []]
    db_map.update_parameter_values.return_value = [query, []]
    return db_map

class TestIntegrationImportData(unittest.TestCase):

    def test_import_data_integration(self):
        """Test all import functions to an in memmory database"""
        input_db = create_new_spine_database('sqlite://')
        db_map = DiffDatabaseMapping(
            "", username='IntegrationTest', create_all=False)
        db_map.engine = input_db
        db_map.engine.connect()
        db_map.session = Session(db_map.engine, autoflush=False)
        db_map.create_mapping()
        db_map.create_diff_tables_and_mapping()
        db_map.init_next_id()

        object_c = ['example_class', 'other_class']
        obj_parameters = [['example_parameter', 'example_class']]
        relationship_c = [['example_rel_class', ['example_class', 'other_class']]]
        rel_parameters = [['rel_parameter', 'example_rel_class']]
        objects = [['example_object', 'example_class'],
                   ['other_object', 'other_class']]
        object_p_values = [['example_object', 'example_parameter', 'value', 3.14]]
        relationships = [['example_rel_class', ['example_object', 'other_object']]]
        rel_p_values = [['example_rel_class', ['example_object', 'other_object'], 'rel_parameter', 'value', 2.718]]

        num_imports, errors = import_data(
            db_map,
            object_classes=object_c,
            relationship_classes=relationship_c,
            object_parameters=obj_parameters,
            relationship_parameters=rel_parameters,
            objects=objects,
            relationships=relationships,
            object_parameter_values=object_p_values,
            relationship_parameter_values=rel_p_values
        )

        self.assertEqual(num_imports, 10)
        self.assertEqual(len(errors), 0)


class TestImportObjectClass(unittest.TestCase):

    def setUp(self):
        self.mock_db_map = create_mock_db_map()

    def tearDown(self):
        pass

    def test_import_object_class(self):
        """Test that importing object class works"""
        num_imported, errors = import_object_classes(self.mock_db_map, ["new_class"])
        self.mock_db_map.add_object_classes.assert_called_once_with(
            {'name': 'new_class'}, raise_intgr_error=False)
        self.assertEqual(len(errors), 0)


class TestImportObject(unittest.TestCase):

    def setUp(self):
        self.mock_db_map = create_mock_db_map()

    def tearDown(self):
        pass

    def test_import_valid_objects(self):
        num_imported, errors = import_objects(self.mock_db_map, [["new_object", "existing_oc1"]])
        self.mock_db_map.add_objects.assert_called_once_with(
            {'name': 'new_object', "class_id": 1}, raise_intgr_error=False)
        self.assertEqual(len(errors), 0)

    def test_import_object_with_invalid_object_class_name(self):
        num_imported, errors = import_objects(self.mock_db_map, [["new_object", "invalid_class_name"]])
        self.mock_db_map.add_objects.assert_called_once_with(raise_intgr_error=False)
        self.assertEqual(len(errors), 1)


class TestImportRelationshipClass(unittest.TestCase):

    def setUp(self):
        self.mock_db_map = create_mock_db_map()

    def tearDown(self):
        pass

    def test_import_valid_relationship_class(self):
        num_imported, errors = import_relationship_classes(
            self.mock_db_map, [["new_rc", ["existing_oc1", "existing_oc2"]]])
        self.mock_db_map.add_wide_relationship_classes.assert_called_once_with(
            {'name': 'new_rc', 'object_class_id_list': [1, 2]}, raise_intgr_error=False)
        self.assertEqual(len(errors), 0)

    def test_import_relationship_class_with_invalid_object_class_name(self):
        num_imported, errors = import_relationship_classes(
            self.mock_db_map, [["new_rc", ["existing_oc1", "invalid_oc"]]])
        self.mock_db_map.add_wide_relationship_classes.assert_called_once_with(raise_intgr_error=False)
        self.assertEqual(len(errors), 1)


class TestImportObjectClassParameter(unittest.TestCase):

    def setUp(self):
        self.mock_db_map = create_mock_db_map()

    def tearDown(self):
        pass

    def test_import_valid_object_class_parameter(self):
        num_imported, errors = import_object_parameters(self.mock_db_map, [["new_parameter", "existing_oc1"]])
        self.mock_db_map.add_parameters.assert_called_once_with(
            {'name': 'new_parameter', 'object_class_id': 1}, raise_intgr_error=False)
        self.assertEqual(len(errors), 0)

    def test_import_parameter_with_invalid_object_class_name(self):
        num_imported, errors = import_object_parameters(
            self.mock_db_map, [["new_parameter", "invalid_object_class"]])
        self.mock_db_map.add_parameters.assert_called_once_with(raise_intgr_error=False)
        self.assertEqual(len(errors), 1)


class TestImportRelationshipClassParameter(unittest.TestCase):

    def setUp(self):
        self.mock_db_map = create_mock_db_map()

    def tearDown(self):
        pass

    def test_import_valid_relationship_class_parameter(self):
        num_imported, errors = import_relationship_parameters(
            self.mock_db_map, [["new_parameter", "existing_rc1"]])
        self.mock_db_map.add_parameters.assert_called_once_with(
            {'name': 'new_parameter', 'relationship_class_id': 1}, raise_intgr_error=False)
        self.assertEqual(len(errors), 0)

    def test_import_parameter_with_invalid_relationship_class_name(self):
        num_imported, errors = import_relationship_parameters(
            self.mock_db_map, [["new_parameter", "invalid_relationship_class"]])
        self.mock_db_map.add_parameters.assert_called_once_with(raise_intgr_error=False)
        self.assertEqual(len(errors), 1)


class TestImportRelationship(unittest.TestCase):

    def setUp(self):
        self.mock_db_map = create_mock_db_map()

    def tearDown(self):
        pass

    def test_import_valid_relationship(self):
        num_imported, errors = import_relationships(
            self.mock_db_map, [["existing_rc2", ["existing_o2", "existing_o1"]]])
        self.mock_db_map.add_wide_relationships.assert_called_once_with(
            {'name': 'existing_rc2_existing_o2__existing_o1', 'class_id': 2, 'object_id_list': (2, 1)},
            raise_intgr_error=False)
        self.assertEqual(len(errors), 0)

    def test_import_relationship_with_invalid_class_name(self):
        num_imported, errors = import_relationships(
            self.mock_db_map, [["invalid_relationship_class", ["existing_o1", "existing_o2"]]])
        self.mock_db_map.add_wide_relationships.assert_called_once_with(raise_intgr_error=False)
        self.assertEqual(len(errors), 1)

    def test_import_relationship_with_invalid_object_name(self):
        num_imported, errors = import_relationships(
            self.mock_db_map, [["existing_rc1", ["none_existing_object", "existing_o2"]]])
        self.mock_db_map.add_wide_relationships.assert_called_once_with(raise_intgr_error=False)
        self.assertEqual(len(errors), 1)


class TestImportParameterValue(unittest.TestCase):

    def setUp(self):
        self.mock_db_map = create_mock_db_map()

    def tearDown(self):
        pass

    def test_import_valid_object_parameter_value(self):
        ParameterValue = namedtuple("ParameterValue",
                                    ["id", "parameter_id", "object_id", "relationship_id"])
        added = MagicMock()
        added.__iter__.return_value = {ParameterValue(3, 3, 1, None)}
        added.count.return_value = 1
        self.mock_db_map.add_parameter_values.return_value = added, []
        num_imported, errors = import_object_parameter_values(
            self.mock_db_map, [["existing_o1", "existing_p3", "value", 1]])
        self.mock_db_map.add_parameter_values.assert_called_once_with(
            {'object_id': 1, 'parameter_id': 3, 'value': 1}, raise_intgr_error=False)
        self.mock_db_map.update_parameter_values.assert_called_once_with(raise_intgr_error=False)
        self.assertEqual(len(errors), 0)

    def test_import_valid_object_parameter_json_value(self):
        ParameterValue = namedtuple("ParameterValue",
                                    ["id", "parameter_id", "object_id", "relationship_id"])
        added = MagicMock()
        added.__iter__.return_value = {ParameterValue(3, 3, 1, None)}
        added.count.return_value = 1
        self.mock_db_map.add_parameter_values.return_value = added, []
        num_imported, errors = import_object_parameter_values(
            self.mock_db_map, [["existing_o1", "existing_p3", "json", 1]])
        self.mock_db_map.add_parameter_values.assert_called_once_with(
            {'object_id': 1, 'parameter_id': 3, 'json': 1}, raise_intgr_error=False)
        self.mock_db_map.update_parameter_values.assert_called_once_with(raise_intgr_error=False)
        self.assertEqual(len(errors), 0)

    def test_import_object_parameter_value_with_invalid_object(self):
        num_imported, errors = import_object_parameter_values(
            self.mock_db_map, [["invalid_object", "existing_p3", "value", 1]])
        self.mock_db_map.add_parameter_values.assert_called_once_with(raise_intgr_error=False)
        self.mock_db_map.update_parameter_values.assert_called_once_with(raise_intgr_error=False)
        self.assertEqual(len(errors), 1)

    def test_import_object_parameter_value_with_invalid_parameter(self):
        num_imported, errors = import_object_parameter_values(
            self.mock_db_map, [["existing_o1", "invalid_parameter", "json", 1]])
        self.mock_db_map.add_parameter_values.assert_called_once_with(raise_intgr_error=False)
        self.mock_db_map.update_parameter_values.assert_called_once_with(raise_intgr_error=False)
        self.assertEqual(len(errors), 1)

    def test_import_object_parameter_value_with_invalid_field(self):
        num_imported, errors = import_object_parameter_values(
            self.mock_db_map, [["existing_o1", "existing_p3", "invalid_field", 1]])
        self.mock_db_map.add_parameter_values.assert_called_once_with(raise_intgr_error=False)
        self.mock_db_map.update_parameter_values.assert_called_once_with(raise_intgr_error=False)
        self.assertEqual(len(errors), 1)

    def test_import_existing_object_parameter_value(self):
        ParameterValue = namedtuple("ParameterValue",
                                    ["id", "parameter_id", "object_id", "relationship_id"])
        added = MagicMock()
        added.__iter__.return_value = {}
        added.count.return_value = 0
        self.mock_db_map.add_parameter_values.return_value = added, ['error']
        num_imported, errors = import_object_parameter_values(
            self.mock_db_map, [["existing_o1", "existing_p1", "value", 1]])
        self.mock_db_map.add_parameter_values.assert_called_once_with(
            {'parameter_id': 1, 'object_id': 1, 'value': 1},
            raise_intgr_error=False)
        self.mock_db_map.update_parameter_values.assert_called_once_with(
            {'id': 1, 'value': 1}, raise_intgr_error=False)
        self.assertEqual(len(errors), 1)

    def test_import_duplicate_object_parameter_value(self):
        ParameterValue = namedtuple("ParameterValue",
                                    ["id", "parameter_id", "object_id", "relationship_id"])
        added = MagicMock()
        added.__iter__.return_value = {ParameterValue(3, 3, 1, None)}
        added.count.return_value = 1
        self.mock_db_map.add_parameter_values.return_value = added, []
        num_imported, errors = import_object_parameter_values(
            self.mock_db_map,
            [["existing_o1", "existing_p3", "value", 1], ["existing_o1", "existing_p3", "value", "4"]])
        self.mock_db_map.add_parameter_values.assert_called_once_with(
            {'object_id': 1, 'parameter_id': 3, 'value': 1}, raise_intgr_error=False)
        self.mock_db_map.update_parameter_values.assert_called_once_with(raise_intgr_error=False)
        self.assertEqual(len(errors), 1)

    def test_import_valid_relationship_parameter_value(self):
        ParameterValue = namedtuple("ParameterValue",
                                    ["id", "parameter_id", "object_id", "relationship_id"])
        added = MagicMock()
        added.__iter__.return_value = {ParameterValue(3, 4, None, 1)}
        added.count.return_value = 1
        self.mock_db_map.add_parameter_values.return_value = added, []
        num_imported, errors = import_relationship_parameter_values(
            self.mock_db_map, [["existing_rc1", ["existing_o1", "existing_o2"], "existing_p4", "value", 1]])
        self.mock_db_map.add_parameter_values.assert_called_once_with(
            {'relationship_id': 1, 'parameter_id': 4, 'value': 1}, raise_intgr_error=False)
        self.mock_db_map.update_parameter_values.assert_called_once_with(raise_intgr_error=False)
        self.assertEqual(len(errors), 0)

    def test_import_valid_relationship_parameter_json_value(self):
        ParameterValue = namedtuple("ParameterValue",
                                    ["id", "parameter_id", "object_id", "relationship_id"])
        added = MagicMock()
        added.__iter__.return_value = {ParameterValue(3, 4, None, 1)}
        added.count.return_value = 1
        self.mock_db_map.add_parameter_values.return_value = added, []
        num_imported, errors = import_relationship_parameter_values(
            self.mock_db_map, [["existing_rc1", ["existing_o1", "existing_o2"], "existing_p4", "json", 1]])
        self.mock_db_map.add_parameter_values.assert_called_once_with(
            {'relationship_id': 1, 'parameter_id': 4, 'json': 1}, raise_intgr_error=False)
        self.mock_db_map.update_parameter_values.assert_called_once_with(raise_intgr_error=False)
        self.assertEqual(len(errors), 0)

    def test_import_relationship_parameter_value_with_invalid_object(self):
        num_imported, errors = import_relationship_parameter_values(
            self.mock_db_map, [["existing_rc1", ["existing_o1", "invalid_object"], "existing_p4", "json", 1]])
        self.mock_db_map.add_parameter_values.assert_called_once_with(raise_intgr_error=False)
        self.mock_db_map.update_parameter_values.assert_called_once_with(raise_intgr_error=False)
        self.assertEqual(len(errors), 1)

    def test_import_relationship_parameter_value_with_invalid_relationship_class(self):
        num_imported, errors = import_relationship_parameter_values(
            self.mock_db_map, [["invalid_rel_cls", ["existing_o1", "existing_o2"], "existing_p4", "json", 1]])
        self.mock_db_map.add_parameter_values.assert_called_once_with(raise_intgr_error=False)
        self.mock_db_map.update_parameter_values.assert_called_once_with(raise_intgr_error=False)
        self.assertEqual(len(errors), 1)

    def test_import_relationship_parameter_value_with_invalid_parameter(self):
        num_imported, errors = import_relationship_parameter_values(
            self.mock_db_map, [["existing_rc1", ["existing_o1", "existing_o2"], "invalid_param", "json", 1]])
        self.mock_db_map.add_parameter_values.assert_called_once_with(raise_intgr_error=False)
        self.mock_db_map.update_parameter_values.assert_called_once_with(raise_intgr_error=False)
        self.assertEqual(len(errors), 1)

    def test_import_existing_relationship_parameter_value(self):
        ParameterValue = namedtuple("ParameterValue",
                                    ["id", "parameter_id", "object_id", "relationship_id"])
        added = MagicMock()
        added.__iter__.return_value = {}
        added.count.return_value = 0
        self.mock_db_map.add_parameter_values.return_value = added, ['error']
        num_imported, errors = import_relationship_parameter_values(
            self.mock_db_map, [["existing_rc1", ["existing_o1", "existing_o2"], "existing_p2", "value", 1]])
        self.mock_db_map.add_parameter_values.assert_called_once_with(
            {'parameter_id': 2, 'relationship_id': 1, 'value': 1},
            raise_intgr_error=False)
        self.mock_db_map.update_parameter_values.assert_called_once_with(
            {'id': 2, 'value': 1}, raise_intgr_error=False)
        self.assertEqual(len(errors), 1)

    def test_import_duplicate_relationship_parameter_value(self):
        ParameterValue = namedtuple("ParameterValue",
                                    ["id", "parameter_id", "object_id", "relationship_id"])
        added = MagicMock()
        added.__iter__.return_value = {ParameterValue(3, 4, None, 1)}
        added.count.return_value = 1
        self.mock_db_map.add_parameter_values.return_value = added, []
        num_imported, errors = import_relationship_parameter_values(
            self.mock_db_map,
            [
                ["existing_rc1", ["existing_o1", "existing_o2"], "existing_p4", "value", 1],
                ["existing_rc1", ["existing_o1", "existing_o2"], "existing_p4", "value", 8]
            ])
        self.mock_db_map.add_parameter_values.assert_called_once_with(
            {'relationship_id': 1, 'parameter_id': 4, 'value': 1}, raise_intgr_error=False)
        self.mock_db_map.update_parameter_values.assert_called_once_with(raise_intgr_error=False)
        self.assertEqual(len(errors), 1)


if __name__ == '__main__':
    unittest.main()
