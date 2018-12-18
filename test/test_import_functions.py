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


def create_mock_db():
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

    #mock apis
    db = MagicMock()
    db.object_class_list.return_value.all.return_value = existing_object_classes
    db.object_list.return_value.all.return_value = existing_objects
    db.wide_relationship_class_list.return_value.all.return_value = existing_rel_class
    db.parameter_list.return_value.all.return_value = existing_parameter
    db.wide_relationship_list.return_value.all.return_value = existing_relationship
    db.object_parameter_value_list.return_value.all.return_value = [
        p for p in existing_parameter_value if p.object_id != None]
    db.relationship_parameter_value_list.return_value.all.return_value = [
        p for p in existing_parameter_value if p.relationship_id != None]
    return db

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
        self.mock_db = create_mock_db()

    def tearDown(self):
        pass

    def test_insert_valid_object_classes(self):
        """Test that importing object class works"""
        num_imported, errors = import_object_classes(
            self.mock_db, ["new_class"])
        self.mock_db.add_object_classes.assert_called_once_with(
            *[{'name': 'new_class'}])
        self.assertEqual(num_imported, 1)
        self.assertEqual(len(errors), 0)

    def test_insert_object_class_with_already_taken_name(self):
        """Test that importing object class with duplicate name in database doesn't import anything"""
        num_imported, errors = import_object_classes(
            self.mock_db, ["existing_oc1"])
        self.mock_db.add_object_classes.assert_not_called()
        self.assertEqual(num_imported, 0)
        self.assertEqual(len(errors), 0)

    def test_insert_two_object_classes_with_same_name(self):
        """Test that importing two object class with duplicate name only tries to import one"""
        num_imported, errors = import_object_classes(
            self.mock_db, ["new_class", "new_class"])
        self.mock_db.add_object_classes.assert_called_once_with(
            *[{'name': 'new_class'}])
        self.assertEqual(num_imported, 1)
        self.assertEqual(len(errors), 0)


class TestImportObject(unittest.TestCase):

    def setUp(self):
        self.mock_db = create_mock_db()

    def tearDown(self):
        pass

    def test_insert_valid_objects(self):
        num_imported, errors = import_objects(
            self.mock_db, [["new_object", "existing_oc1"]])
        self.mock_db.add_objects.assert_called_once_with(
            *[{'name': 'new_object', "class_id": 1}])
        self.assertEqual(num_imported, 1)
        self.assertEqual(len(errors), 0)

    def test_insert_object_with_name_existing_but_wrong_class(self):
        num_imported, errors = import_objects(
            self.mock_db, [["existing_o1", "existing_oc2"]])
        self.mock_db.add_objects.assert_not_called()
        self.assertEqual(num_imported, 0)
        self.assertEqual(len(errors), 1)

    def test_insert_object_which_already_exists(self):
        num_imported, errors = import_objects(
            self.mock_db, [["existing_o1", "existing_oc1"]])
        self.mock_db.add_objects.assert_not_called()
        self.assertEqual(num_imported, 0)
        self.assertEqual(len(errors), 0)

    def test_insert_object_with_invalid_object_class_name(self):
        num_imported, errors = import_objects(
            self.mock_db, [["new_object", "invalid_class_name"]])
        self.mock_db.add_objects.assert_not_called()
        self.assertEqual(num_imported, 0)
        self.assertEqual(len(errors), 1)

    def test_insert_two_duplicate_objects(self):
        num_imported, errors = import_objects(
            self.mock_db, [["new_object", "existing_oc1"], ["new_object", "existing_oc1"]])
        self.mock_db.add_objects.assert_called_once_with(
            *[{'name': 'new_object', "class_id": 1}])
        self.assertEqual(num_imported, 1)
        self.assertEqual(len(errors), 0)

    def test_insert_two_objects_with_same_name_but_different_class(self):
        num_imported, errors = import_objects(
            self.mock_db, [["new_object", "existing_oc1"], ["new_object", "existing_oc2"]])
        self.mock_db.add_objects.assert_called_once_with(
            *[{'name': 'new_object', "class_id": 1}])
        self.assertEqual(num_imported, 1)
        self.assertEqual(len(errors), 1)


class TestImportRelationshipClass(unittest.TestCase):

    def setUp(self):
        self.mock_db = create_mock_db()

    def tearDown(self):
        pass

    def test_insert_valid_relationship_class(self):
        num_imported, errors = import_relationship_classes(
            self.mock_db, [["new_rc", ["existing_oc1", "existing_oc2"]]])
        self.mock_db.add_wide_relationship_classes.assert_called_once_with(
            *[{'name': 'new_rc', 'object_class_id_list': [1, 2]}])
        self.assertEqual(num_imported, 1)
        self.assertEqual(len(errors), 0)

    def test_insert_relationship_class_with_already_taken_name_same_object_classes(self):
        num_imported, errors = import_relationship_classes(
            self.mock_db, [["existing_rc1", ["existing_oc1", "existing_oc2"]]])
        self.mock_db.add_wide_relationship_classes.assert_not_called()
        self.assertEqual(num_imported, 0)
        self.assertEqual(len(errors), 0)

    def test_insert_relationship_class_with_already_taken_name_different_object_classes(self):
        num_imported, errors = import_relationship_classes(
            self.mock_db, [["existing_rc1", ["existing_oc2", "existing_oc1"]]])
        self.mock_db.add_wide_relationship_classes.assert_not_called()
        self.assertEqual(num_imported, 0)
        self.assertEqual(len(errors), 1)

    def test_insert_relationship_class_with_invalid_object_class_name(self):
        num_imported, errors = import_relationship_classes(
            self.mock_db, [["new_rc", ["existing_oc1", "invalid_oc"]]])
        self.mock_db.add_wide_relationship_classes.assert_not_called()
        self.assertEqual(num_imported, 0)
        self.assertEqual(len(errors), 1)

    def test_insert_two_relationships_classes_with_same_name_different_object_classes(self):
        num_imported, errors = import_relationship_classes(self.mock_db, [["new_rc", ["existing_oc1", "existing_oc2"]],
                                                                          ["new_rc", ["existing_oc2", "existing_oc2"]]])
        self.mock_db.add_wide_relationship_classes.assert_called_once_with(
            *[{'name': 'new_rc', 'object_class_id_list': [1, 2]}])
        self.assertEqual(num_imported, 1)
        self.assertEqual(len(errors), 1)

    def test_insert_two_relationships_classes_with_same_name_same_object_classes(self):
        num_imported, errors = import_relationship_classes(self.mock_db, [["new_rc", ["existing_oc1", "existing_oc2"]],
                                                                          ["new_rc", ["existing_oc1", "existing_oc2"]]])
        self.mock_db.add_wide_relationship_classes.assert_called_once_with(
            *[{'name': 'new_rc', 'object_class_id_list': [1, 2]}])
        self.assertEqual(num_imported, 1)
        self.assertEqual(len(errors), 0)


class TestImportObjectClassParameter(unittest.TestCase):

    def setUp(self):
        self.mock_db = create_mock_db()

    def tearDown(self):
        pass

    def test_insert_valid_object_class_parameter(self):
        num_imported, errors = import_object_parameters(
            self.mock_db, [["new_parameter", "existing_oc1"]])
        self.mock_db.add_parameters.assert_called_once_with(
            *[{'name': 'new_parameter', 'object_class_id': 1}])
        self.assertEqual(num_imported, 1)
        self.assertEqual(len(errors), 0)

    def test_insert_parameter_with_already_taken_name_same_object_class(self):
        num_imported, errors = import_object_parameters(
            self.mock_db, [["existing_p1", "existing_oc1"]])
        self.mock_db.add_parameters.assert_not_called()
        self.assertEqual(num_imported, 0)
        self.assertEqual(len(errors), 0)

    def test_insert_parameter_with_already_taken_name_different_object_class(self):
        num_imported, errors = import_object_parameters(
            self.mock_db, [["existing_p1", "existing_oc2"]])
        self.mock_db.add_parameters.assert_not_called()
        self.assertEqual(num_imported, 0)
        self.assertEqual(len(errors), 1)

    def test_insert_parameter_with_invalid_object_class_name(self):
        num_imported, errors = import_object_parameters(
            self.mock_db, [["new_parameter", "invalid_object_class"]])
        self.mock_db.add_parameters.assert_not_called()
        self.assertEqual(num_imported, 0)
        self.assertEqual(len(errors), 1)

    def test_insert_two_parameters_with_same_name_same_object_class(self):
        num_imported, errors = import_object_parameters(
            self.mock_db, [["new_parameter", "existing_oc1"], ["new_parameter", "existing_oc1"]])
        self.mock_db.add_parameters.assert_called_once_with(
            *[{'name': 'new_parameter', 'object_class_id': 1}])
        self.assertEqual(num_imported, 1)
        self.assertEqual(len(errors), 0)

    def test_insert_two_parameters_with_same_name_different_object_class(self):
        num_imported, errors = import_object_parameters(
            self.mock_db, [["new_parameter", "existing_oc1"], ["new_parameter", "existing_oc2"]])
        self.mock_db.add_parameters.assert_called_once_with(
            *[{'name': 'new_parameter', 'object_class_id': 1}])
        self.assertEqual(num_imported, 1)
        self.assertEqual(len(errors), 1)


class TestImportRelationshipClassParameter(unittest.TestCase):

    def setUp(self):
        self.mock_db = create_mock_db()

    def tearDown(self):
        pass

    def test_insert_valid_relationship_class_parameter(self):
        num_imported, errors = import_relationship_parameters(
            self.mock_db, [["new_parameter", "existing_rc1"]])
        self.mock_db.add_parameters.assert_called_once_with(
            *[{'name': 'new_parameter', 'relationship_class_id': 1}])
        self.assertEqual(num_imported, 1)
        self.assertEqual(len(errors), 0)

    def test_insert_parameter_with_already_taken_name_same_relationship_class(self):
        num_imported, errors = import_relationship_parameters(
            self.mock_db, [["existing_p2", "existing_rc1"]])
        self.mock_db.add_parameters.assert_not_called()
        self.assertEqual(num_imported, 0)
        self.assertEqual(len(errors), 0)

    def test_insert_parameter_with_already_taken_name_different_relationship_class(self):
        num_imported, errors = import_relationship_parameters(
            self.mock_db, [["existing_p2", "existing_rc2"]])
        self.mock_db.add_parameters.assert_not_called()
        self.assertEqual(num_imported, 0)
        self.assertEqual(len(errors), 1)

    def test_insert_parameter_with_invalid_relationship_class_name(self):
        num_imported, errors = import_relationship_parameters(
            self.mock_db, [["new_parameter", "invalid_relationship_class"]])
        self.mock_db.add_parameters.assert_not_called()
        self.assertEqual(num_imported, 0)
        self.assertEqual(len(errors), 1)

    def test_insert_two_parameters_with_same_name_same_relationship_class(self):
        num_imported, errors = import_relationship_parameters(
            self.mock_db, [["new_parameter", "existing_rc1"], ["new_parameter", "existing_rc1"]])
        self.mock_db.add_parameters.assert_called_once_with(
            *[{'name': 'new_parameter', 'relationship_class_id': 1}])
        self.assertEqual(num_imported, 1)
        self.assertEqual(len(errors), 0)

    def test_insert_two_parameters_with_same_name_different_relationship_class(self):
        num_imported, errors = import_relationship_parameters(
            self.mock_db, [["new_parameter", "existing_rc1"], ["new_parameter", "existing_rc2"]])
        self.mock_db.add_parameters.assert_called_once_with(
            *[{'name': 'new_parameter', 'relationship_class_id': 1}])
        self.assertEqual(num_imported, 1)
        self.assertEqual(len(errors), 1)


class TestImportRelationship(unittest.TestCase):

    def setUp(self):
        self.mock_db = create_mock_db()

    def tearDown(self):
        pass

    def test_insert_valid_relationship(self):
        num_imported, errors = import_relationships(
            self.mock_db, [["existing_rc2", ["existing_o2", "existing_o1"]]])
        self.mock_db.add_wide_relationships.assert_called_once_with(
            *[{'name': 'existing_rc2__existing_o2_existing_o1', 'class_id': 2, 'object_id_list': (2, 1)}])
        self.assertEqual(num_imported, 1)
        self.assertEqual(len(errors), 0)

    def test_insert_existing_relationship(self):
        num_imported, errors = import_relationships(
            self.mock_db, [["existing_rc1", ["existing_o1", "existing_o2"]]])
        self.mock_db.add_wide_relationships.assert_not_called()
        self.assertEqual(num_imported, 0)
        self.assertEqual(len(errors), 0)

    def test_insert_relationship_with_invalid_object_classes(self):
        num_imported, errors = import_relationships(
            self.mock_db, [["existing_rc1", ["existing_o1", "existing_o1"]]])
        self.mock_db.add_wide_relationships.assert_not_called()
        self.assertEqual(num_imported, 0)
        self.assertEqual(len(errors), 1)

    def test_insert_relationship_with_invalid_object_name(self):
        num_imported, errors = import_relationships(
            self.mock_db, [["existing_rc1", ["none_existing_object", "existing_o2"]]])
        self.mock_db.add_wide_relationships.assert_not_called()
        self.assertEqual(num_imported, 0)
        self.assertEqual(len(errors), 1)

    def test_insert_two_relationships_class_with_same_objects_and_class(self):
        num_imported, errors = import_relationships(self.mock_db, [["existing_rc2", ["existing_o2", "existing_o1"]],
                                                                   ["existing_rc2", ["existing_o2", "existing_o1"]]])
        self.mock_db.add_wide_relationships.assert_called_once_with(
            *[{'name': 'existing_rc2__existing_o2_existing_o1', 'class_id': 2, 'object_id_list': (2, 1)}])
        self.assertEqual(num_imported, 1)
        self.assertEqual(len(errors), 0)


class TestImportParameterValue(unittest.TestCase):

    def setUp(self):
        self.mock_db = create_mock_db()

    def tearDown(self):
        pass

    def test_insert_valid_object_parameter_value(self):
        num_imported, errors = import_object_parameter_values(
            self.mock_db, [["existing_o1", "existing_p3", "value", 1]])
        self.mock_db.add_parameter_values.assert_called_once_with(
            *[{'object_id': 1, 'parameter_id': 3, 'value': 1}])
        self.mock_db.update_parameter_values.assert_not_called()
        self.assertEqual(num_imported, 1)
        self.assertEqual(len(errors), 0)

    def test_insert_valid_object_parameter_value_json(self):
        num_imported, errors = import_object_parameter_values(
            self.mock_db, [["existing_o1", "existing_p3", "json", 1]])
        self.mock_db.add_parameter_values.assert_called_once_with(
            *[{'object_id': 1, 'parameter_id': 3, 'json': 1}])
        self.mock_db.update_parameter_values.assert_not_called()
        self.assertEqual(num_imported, 1)
        self.assertEqual(len(errors), 0)

    def test_insert_invalid_field_object_parameter_value(self):
        num_imported, errors = import_object_parameter_values(
            self.mock_db, [["existing_o1", "existing_p3", "invalid_field", 1]])
        self.mock_db.add_parameter_values.assert_not_called()
        self.mock_db.update_parameter_values.assert_not_called()
        self.assertEqual(num_imported, 0)
        self.assertEqual(len(errors), 1)

    def test_insert_valid_object_parameter_value_with_existing_value(self):
        num_imported, errors = import_object_parameter_values(
            self.mock_db, [["existing_o1", "existing_p1", "value", 1]])
        self.mock_db.add_parameter_values.assert_not_called()
        self.mock_db.update_parameter_values.assert_called_once_with(
            *[{'id': 1, 'value': 1}])
        self.assertEqual(num_imported, 1)
        self.assertEqual(len(errors), 0)

    def test_insert_valid_relationship_parameter_value_with_existing_value(self):
        num_imported, errors = import_relationship_parameter_values(self.mock_db, [
                                                                    ["existing_rc1", ["existing_o1", "existing_o2"], "existing_p2", "value", 1]])
        self.mock_db.add_parameter_values.assert_not_called()
        self.mock_db.update_parameter_values.assert_called_once_with(
            *[{'id': 2, 'value': 1}])
        self.assertEqual(num_imported, 1)
        self.assertEqual(len(errors), 0)

    def test_insert_valid_relationship_parameter(self):
        num_imported, errors = import_relationship_parameter_values(self.mock_db, [
                                                                    ["existing_rc1", ["existing_o1", "existing_o2"], "existing_p4", "value", 1]])
        self.mock_db.add_parameter_values.assert_called_once_with(
            *[{'relationship_id': 1, 'parameter_id': 4, 'value': 1}])
        self.mock_db.update_parameter_values.assert_not_called()
        self.assertEqual(num_imported, 1)
        self.assertEqual(len(errors), 0)

    def test_insert_relationship_parameter_with_invalid_object_classes(self):
        num_imported, errors = import_relationship_parameter_values(self.mock_db, [
                                                                    ["existing_rc1", ["existing_o2", "existing_o1"], "existing_p4", "value", 1]])
        self.mock_db.add_parameter_values.assert_not_called()
        self.mock_db.update_parameter_values.assert_not_called()
        self.assertEqual(num_imported, 0)
        self.assertEqual(len(errors), 1)

    def test_insert_relationship_parameter_with_none_existing_object_name(self):
        num_imported, errors = import_relationship_parameter_values(self.mock_db, [
                                                                    ["existing_rc1", ["invalid_object", "existing_o2"], "existing_p4", "value", 1]])
        self.mock_db.add_parameter_values.assert_not_called()
        self.mock_db.update_parameter_values.assert_not_called()
        self.assertEqual(num_imported, 0)
        self.assertEqual(len(errors), 1)

    def test_insert_object_parameter_value_with_invalid_object_class(self):
        num_imported, errors = import_object_parameter_values(
            self.mock_db, [["existing_o2", "existing_p3", "value", 1]])
        self.mock_db.add_parameter_values.assert_not_called()
        self.mock_db.update_parameter_values.assert_not_called
        self.assertEqual(num_imported, 0)
        self.assertEqual(len(errors), 1)

    def test_insert_object_parameter_value_with_none_existing_object_name(self):
        num_imported, errors = import_object_parameter_values(
            self.mock_db, [["invalid_object", "existing_p1", "value", 1]])
        self.mock_db.add_parameter_values.assert_not_called()
        self.mock_db.update_parameter_values.assert_not_called
        self.assertEqual(num_imported, 0)
        self.assertEqual(len(errors), 1)


if __name__ == '__main__':
    unittest.main()
