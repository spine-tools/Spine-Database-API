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
Unit tests for DiffDatabaseMapping class.

:author: P. Vennström (VTT)
:date:   29.11.2018
"""

import os
import unittest
import logging
import sys
from spinedatabase_api.diff_database_mapping import DiffDatabaseMapping, SpineIntegrityError
from spinedatabase_api.helpers import create_new_spine_database
from sqlalchemy.util import KeyedTuple
from unittest import mock
from sqlalchemy.orm import Session

class TestDiffDatabaseMapping(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Overridden method. Runs once before all tests in this class."""
        logging.basicConfig(stream=sys.stderr, level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s: %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')
        try:
            os.remove("temp.sqlite")
        except OSError:
            pass
        db_url = 'sqlite:///temp.sqlite'
        engine = create_new_spine_database(db_url)
        cls.db_map = DiffDatabaseMapping(db_url, username='UnitTest')

    @classmethod
    def tearDownClass(cls):
        """Overridden method. Runs once after all tests in this class."""
        try:
            os.remove("temp.sqlite")
        except OSError:
            pass

    def setUp(self):
        """Overridden method. Runs before each test. Makes instances of TreeViewForm and GraphViewForm classes.
        """
        # Set logging level to Error to silence "Logging level: All messages" print
        logging.disable(level=logging.ERROR)  # Disable logging
        self.db_map.reset_mapping()
        self.db_map.session.query(self.db_map.NextId).delete(synchronize_session=False)
        logging.disable(level=logging.NOTSET)  # Enable logging

    def tearDown(self):
        """Overridden method. Runs after each test.
        Use this to free resources after a test if needed.
        """
        pass

    def test_insert_many_objects_and_commit(self):
        """Tests inserting many objects into db"""
        c_id = self.db_map.add_object_classes({'name': 'testclass'}).first().id
        self.db_map.add_objects(*[{'name': str(i), 'class_id': c_id} for i in range(1001)])
        self.db_map.commit_session('test_commit')
        self.assertEqual(self.db_map.session.query(self.db_map.Object).count(), 1001)

    def test_insert_and_retrieve_many_objects(self):
        """Tests inserting many objects into db and retrieving them."""
        c_id = self.db_map.add_object_classes({'name': 'testclass'}).first().id
        objects = self.db_map.add_objects(*[{'name': str(i), 'class_id': c_id} for i in range(1001)])
        self.assertEqual(objects.count(), 1001)

    @unittest.skip("TODO")
    def test_check_wide_relationship_with_repeated_object(self):
        """Tests that checking valid relationship with one repeated object doesn't throw an error"""
        check_rel = {"name": 'unique_name', 'object_id_list': [1, 1], 'class_id': 1}
        self.db_map.check_wide_relationship(check_rel, [], {1: [1, 1]}, {1:1, 1:1})

    def test_add_object_classes(self):
        """Test that adding object classes works."""
        try:
            self.db_map.add_object_classes({'name': 'fish'}, {'name': 'dog'})
        except SpineIntegrityError:
            self.fail("add_object_classes() raised SpineIntegrityError unexpectedly")
        object_classes = self.db_map.session.query(self.db_map.DiffObjectClass).all()
        self.assertEqual(len(object_classes), 2)
        self.assertEqual(object_classes[0].name, 'fish')
        self.assertEqual(object_classes[1].name, 'dog')

    def test_add_object_classes_with_same_name(self):
        """Test that adding two object classes with the same name only adds one of them."""
        self.db_map.add_object_classes(
            {'name': 'fish'}, {'name': 'fish'}, raise_intgr_error=False)
        object_classes = self.db_map.session.query(self.db_map.DiffObjectClass).all()
        self.assertEqual(len(object_classes), 1)
        self.assertEqual(object_classes[0].name, 'fish')

    def test_add_object_class_with_same_name_as_existing_one(self):
        """Test that adding an object class with an already taken name raises an integrity error."""
        with mock.patch.object(DiffDatabaseMapping, "object_class_list") as mock_object_class_list:
            mock_object_class_list.return_value = [KeyedTuple([1, 'fish'], labels=["id", "name"])]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_object_classes({'name': 'fish'})

    def test_add_objects(self):
        """Test that adding objects works."""
        with mock.patch.object(DiffDatabaseMapping, "object_class_list") as mock_object_class_list:
            mock_object_class_list.return_value = [KeyedTuple([1, 'fish'], labels=["id", "name"])]
            try:
                self.db_map.add_objects(
                    {'name': 'nemo', 'class_id': 1}, {'name': 'dory', 'class_id': 1})
            except SpineIntegrityError:
                self.fail("add_objects() raised SpineIntegrityError unexpectedly")
        objects = self.db_map.session.query(self.db_map.DiffObject).all()
        self.assertEqual(len(objects), 2)
        self.assertEqual(objects[0].name, 'nemo')
        self.assertEqual(objects[0].class_id, 1)
        self.assertEqual(objects[1].name, 'dory')
        self.assertEqual(objects[1].class_id, 1)

    def test_add_objects_with_same_name(self):
        """Test that adding two objects with the same name only adds one of them."""
        with mock.patch.object(DiffDatabaseMapping, "object_class_list") as mock_object_class_list:
            mock_object_class_list.return_value = [KeyedTuple([1, 'fish'], labels=["id", "name"])]
            self.db_map.add_objects(
                {'name': 'nemo', 'class_id': 1}, {'name': 'nemo', 'class_id': 1}, raise_intgr_error=False)
        objects = self.db_map.session.query(self.db_map.DiffObject).all()
        self.assertEqual(len(objects), 1)
        self.assertEqual(objects[0].name, 'nemo')
        self.assertEqual(objects[0].class_id, 1)

    def test_add_object_with_same_name_as_existing_one(self):
        """Test that adding an object with an already taken name raises an integrity error."""
        with mock.patch.object(DiffDatabaseMapping, "object_class_list") as mock_object_class_list, \
                mock.patch.object(DiffDatabaseMapping, "object_list") as mock_object_list:
            mock_object_class_list.return_value = [KeyedTuple([1, 'fish'], labels=["id", "name"])]
            mock_object_list.return_value = [KeyedTuple([1, 1, 'nemo'], labels=["id", "class_id", "name"])]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_objects({'name': 'nemo', 'class_id': 1})

    def test_add_object_with_invalid_class(self):
        """Test that adding an object with a non existing class raises an integrity error."""
        with mock.patch.object(DiffDatabaseMapping, "object_class_list") as mock_object_class_list:
            mock_object_class_list.return_value = [KeyedTuple([1, 'fish'], labels=["id", "name"])]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_objects({'name': 'pluto', 'class_id': 2})

    def test_add_relationship_classes(self):
        """Test that adding relationship classes works."""
        with mock.patch.object(DiffDatabaseMapping, "object_class_list") as mock_object_class_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_class_list"):
            mock_object_class_list.return_value = [
                KeyedTuple([1, 'fish'], labels=["id", "name"]),
                KeyedTuple([2, 'dog'], labels=["id", "name"])
            ]
            try:
                self.db_map.add_wide_relationship_classes(
                    {'name': 'fish__dog', 'object_class_id_list': [1, 2]},
                    {'name': 'fishy_doggy', 'object_class_id_list': [1, 2]})
            except SpineIntegrityError:
                self.fail("add_wide_relationship_classes() raised SpineIntegrityError unexpectedly")
        relationship_classes = self.db_map.session.query(self.db_map.DiffRelationshipClass).all()
        self.assertEqual(len(relationship_classes), 4)
        self.assertEqual(relationship_classes[0].name, 'fish__dog')
        self.assertEqual(relationship_classes[0].object_class_id, 1)
        self.assertEqual(relationship_classes[1].name, 'fish__dog')
        self.assertEqual(relationship_classes[1].object_class_id, 2)
        self.assertEqual(relationship_classes[2].name, 'fishy_doggy')
        self.assertEqual(relationship_classes[2].object_class_id, 1)
        self.assertEqual(relationship_classes[3].name, 'fishy_doggy')
        self.assertEqual(relationship_classes[3].object_class_id, 2)

    def test_add_relationship_classes_with_same_name(self):
        """Test that adding two relationship classes with the same name only adds one of them."""
        with mock.patch.object(DiffDatabaseMapping, "object_class_list") as mock_object_class_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_class_list"):
            mock_object_class_list.return_value = [
                KeyedTuple([1, 'fish'], labels=["id", "name"]),
                KeyedTuple([2, 'dog'], labels=["id", "name"])
            ]
            self.db_map.add_wide_relationship_classes(
                {'name': 'dog__fish', 'object_class_id_list': [1, 2]},
                {'name': 'dog__fish', 'object_class_id_list': [1, 2]},
                raise_intgr_error=False)
        relationship_classes = self.db_map.session.query(self.db_map.DiffRelationshipClass).all()
        self.assertEqual(len(relationship_classes), 2)
        self.assertEqual(relationship_classes[0].name, 'dog__fish')
        self.assertEqual(relationship_classes[0].object_class_id, 1)
        self.assertEqual(relationship_classes[1].name, 'dog__fish')
        self.assertEqual(relationship_classes[1].object_class_id, 2)

    def test_add_relationship_class_with_same_name_as_existing_one(self):
        """Test that adding a relationship class with an already taken name raises an integrity error."""
        with mock.patch.object(DiffDatabaseMapping, "object_class_list") as mock_object_class_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_class_list") as mock_wide_rel_cls_list:
            mock_object_class_list.return_value = [
                KeyedTuple([1, 'fish'], labels=["id", "name"]),
                KeyedTuple([2, 'dog'], labels=["id", "name"])
            ]
            mock_wide_rel_cls_list.return_value = [
                KeyedTuple([1, '1,2', 'fish__dog'], labels=["id", "object_class_id_list", "name"])
            ]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_wide_relationship_classes({'name': 'fish__dog', 'object_class_id_list': [1, 2]})

    def test_add_relationship_class_with_invalid_object_class(self):
        """Test that adding a relationship class with a non existing object class raises an integrity error."""
        with mock.patch.object(DiffDatabaseMapping, "object_class_list") as mock_object_class_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_class_list"):
            mock_object_class_list.return_value = [KeyedTuple([1, 'fish'], labels=["id", "name"])]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_wide_relationship_classes({'name': 'fish__dog', 'object_class_id_list': [1, 2]})

    def test_add_relationships(self):
        """Test that adding relationships works."""
        with mock.patch.object(DiffDatabaseMapping, "object_list") as mock_object_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_class_list") as mock_wide_rel_cls_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_list"):
            mock_object_list.return_value = [
                KeyedTuple([1, 10, 'nemo'], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 10, 'dory'], labels=["id", "class_id", "name"]),
                KeyedTuple([3, 20, 'pluto'], labels=["id", "class_id", "name"]),
                KeyedTuple([4, 20, 'scooby'], labels=["id", "class_id", "name"])
            ]
            mock_wide_rel_cls_list.return_value = [
                KeyedTuple([1, '10,20', 'fish__dog'], labels=["id", "object_class_id_list", "name"]),
                KeyedTuple([2, '20,10', 'dog__fish'], labels=["id", "object_class_id_list", "name"])
            ]
            try:
                self.db_map.add_wide_relationships(
                    {'name': 'nemo__pluto', 'class_id': 1, 'object_id_list': [1, 3]},
                    {'name': 'scooby_dory', 'class_id': 2, 'object_id_list': [4, 2]})
            except SpineIntegrityError:
                self.fail("add_wide_relationships() raised SpineIntegrityError unexpectedly")
        relationships = self.db_map.session.query(self.db_map.DiffRelationship).all()
        self.assertEqual(len(relationships), 4)
        self.assertEqual(relationships[0].name, 'nemo__pluto')
        self.assertEqual(relationships[0].class_id, 1)
        self.assertEqual(relationships[0].object_id, 1)
        self.assertEqual(relationships[1].name, 'nemo__pluto')
        self.assertEqual(relationships[1].class_id, 1)
        self.assertEqual(relationships[1].object_id, 3)
        self.assertEqual(relationships[2].name, 'scooby_dory')
        self.assertEqual(relationships[2].class_id, 2)
        self.assertEqual(relationships[2].object_id, 4)
        self.assertEqual(relationships[3].name, 'scooby_dory')
        self.assertEqual(relationships[3].class_id, 2)
        self.assertEqual(relationships[3].object_id, 2)

    def test_add_identical_relationships(self):
        """Test that adding two relationships with the same class and same objects only adds the first one.
        """
        with mock.patch.object(DiffDatabaseMapping, "object_list") as mock_object_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_class_list") as mock_wide_rel_cls_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_list"):
            mock_object_list.return_value = [
                KeyedTuple([1, 10, 'nemo'], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 20, 'pluto'], labels=["id", "class_id", "name"])
            ]
            mock_wide_rel_cls_list.return_value = [
                KeyedTuple([1, '10,20', 'fish__dog'], labels=["id", "object_class_id_list", "name"])
            ]
            self.db_map.add_wide_relationships(
                {'name': 'nemo__pluto', 'class_id': 1, 'object_id_list': [1, 2]},
                {'name': 'nemoy__plutoy', 'class_id': 1, 'object_id_list': [1, 2]},
                raise_intgr_error=False)
        relationships = self.db_map.session.query(self.db_map.DiffRelationship).all()
        self.assertEqual(len(relationships), 2)
        self.assertEqual(relationships[0].name, 'nemo__pluto')
        self.assertEqual(relationships[0].class_id, 1)
        self.assertEqual(relationships[0].object_id, 1)
        self.assertEqual(relationships[1].name, 'nemo__pluto')
        self.assertEqual(relationships[1].class_id, 1)
        self.assertEqual(relationships[1].object_id, 2)

    def test_add_relationship_identical_to_existing_one(self):
        """Test that adding a relationship with the same class and same objects as an existing one
        raises an integrity error.
        """
        with mock.patch.object(DiffDatabaseMapping, "object_list") as mock_object_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_class_list") as mock_wide_rel_cls_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_list") as mock_wide_rel_list:
            mock_object_list.return_value = [
                KeyedTuple([1, 10, 'nemo'], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 20, 'pluto'], labels=["id", "class_id", "name"])
            ]
            mock_wide_rel_cls_list.return_value = [
                KeyedTuple([1, '10,20', 'fish__dog'], labels=["id", "object_class_id_list", "name"])
            ]
            mock_wide_rel_list.return_value = [
                KeyedTuple([1, 1, '1,2', 'nemo__pluto'], labels=["id", "class_id", "object_id_list", "name"])
            ]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_wide_relationships({'name': 'nemoy__plutoy', 'class_id': 1, 'object_id_list': [1, 2]})

    def test_add_relationship_with_invalid_class(self):
        """Test that adding a relationship with an invalid class raises an integrity error.
        """
        with mock.patch.object(DiffDatabaseMapping, "object_list") as mock_object_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_class_list") as mock_wide_rel_cls_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_list"):
            mock_object_list.return_value = [
                KeyedTuple([1, 10, 'nemo'], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 20, 'pluto'], labels=["id", "class_id", "name"])
            ]
            mock_wide_rel_cls_list.return_value = [
                KeyedTuple([1, '10,20', 'fish__dog'], labels=["id", "object_class_id_list", "name"])
            ]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_wide_relationships({'name': 'nemo__pluto', 'class_id': 2, 'object_id_list': [1, 2]})

    def test_add_relationship_with_invalid_object(self):
        """Test that adding a relationship with an invalid object raises an integrity error.
        """
        with mock.patch.object(DiffDatabaseMapping, "object_list") as mock_object_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_class_list") as mock_wide_rel_cls_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_list"):
            mock_object_list.return_value = [
                KeyedTuple([1, 10, 'nemo'], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 20, 'pluto'], labels=["id", "class_id", "name"])
            ]
            mock_wide_rel_cls_list.return_value = [
                KeyedTuple([1, '10,20', 'fish__dog'], labels=["id", "object_class_id_list", "name"])
            ]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_wide_relationships({'name': 'nemo__pluto', 'class_id': 1, 'object_id_list': [1, 3]})

    def test_add_parameters(self):
        """Test that adding parameters works."""
        with mock.patch.object(DiffDatabaseMapping, "object_class_list") as mock_object_class_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_class_list") as mock_wide_rel_cls_list:
            mock_object_class_list.return_value = [KeyedTuple([1, 'fish'], labels=["id", "name"])]
            mock_wide_rel_cls_list.return_value = [
                KeyedTuple([10, '1,2', 'fish__dog'], labels=["id", "object_class_id_list", "name"])
            ]
            try:
                self.db_map.add_parameters(
                    {'name': 'color', 'object_class_id': 1},
                    {'name': 'relative_speed', 'relationship_class_id': 10})
            except SpineIntegrityError:
                self.fail("add_parameters() raised SpineIntegrityError unexpectedly")
        parameters = self.db_map.session.query(self.db_map.DiffParameter).all()
        self.assertEqual(len(parameters), 2)
        self.assertEqual(parameters[0].name, 'color')
        self.assertEqual(parameters[0].object_class_id, 1)
        self.assertIsNone(parameters[0].relationship_class_id)
        self.assertEqual(parameters[1].name, 'relative_speed')
        self.assertIsNone(parameters[1].object_class_id)
        self.assertEqual(parameters[1].relationship_class_id, 10)

    def test_add_parameters_with_same_name(self):
        """Test that adding two parameters with the same name only adds one of them."""
        with mock.patch.object(DiffDatabaseMapping, "object_class_list") as mock_object_class_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_class_list") as mock_wide_rel_cls_list:
            mock_object_class_list.return_value = [KeyedTuple([1, 'fish'], labels=["id", "name"])]
            mock_wide_rel_cls_list.return_value = [
                KeyedTuple([10, '1,2', 'fish__dog'], labels=["id", "object_class_id_list", "name"])
            ]
            self.db_map.add_parameters(
                {'name': 'color', 'object_class_id': 1},
                {'name': 'color', 'relationship_class_id': 10},
                raise_intgr_error=False)
        parameters = self.db_map.session.query(self.db_map.DiffParameter).all()
        self.assertEqual(len(parameters), 1)
        self.assertEqual(parameters[0].name, 'color')
        self.assertEqual(parameters[0].object_class_id, 1)
        self.assertIsNone(parameters[0].relationship_class_id)

    def test_add_parameter_with_same_name_as_existing_one(self):
        """Test that adding parameters with an already taken name raises and integrity error."""
        with mock.patch.object(DiffDatabaseMapping, "object_class_list") as mock_object_class_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_class_list"), \
                mock.patch.object(DiffDatabaseMapping, "parameter_list") as mock_parameter_list:
            mock_object_class_list.return_value = [KeyedTuple([1, 'fish'], labels=["id", "name"])]
            mock_parameter_list.return_value = [
                KeyedTuple([1, 1, 'color'], labels=["id", "object_class_id", "name"])
            ]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_parameters({'name': 'color', 'object_class_id': 2})

    def test_add_parameter_with_invalid_class(self):
        """Test that adding parameters with an invalid (object or relationship) class raises and integrity error."""
        with mock.patch.object(DiffDatabaseMapping, "object_class_list") as mock_object_class_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_class_list") as mock_wide_rel_cls_list:
            mock_object_class_list.return_value = [KeyedTuple([1, 'fish'], labels=["id", "name"])]
            mock_wide_rel_cls_list.return_value = [
                KeyedTuple([10, '1,2', 'fish__dog'], labels=["id", "object_class_id_list", "name"])
            ]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_parameters({'name': 'color', 'object_class_id': 2})
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_parameters({'name': 'color', 'relationship_class_id': 9})

    def test_add_parameter_for_both_object_and_relationship_class(self):
        """Test that adding parameters associated to both and object and relationship class
        raises and integrity error."""
        with mock.patch.object(DiffDatabaseMapping, "object_class_list") as mock_object_class_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_class_list") as mock_wide_rel_cls_list:
            mock_object_class_list.return_value = [KeyedTuple([1, 'fish'], labels=["id", "name"])]
            mock_wide_rel_cls_list.return_value = [
                KeyedTuple([10, '1,2', 'fish__dog'], labels=["id", "object_class_id_list", "name"])
            ]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_parameters(
                    {'name': 'color', 'object_class_id': 1, 'relationship_class_id': 10})

    def test_add_parameter_values(self):
        """Test that adding parameter values works."""
        with mock.patch.object(DiffDatabaseMapping, "object_list") as mock_object_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_list") as mock_wide_rel_list, \
                mock.patch.object(DiffDatabaseMapping, "parameter_list") as mock_parameter_list:
            mock_object_list.return_value = [
                KeyedTuple([1, 10, 'nemo'], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 20, 'pluto'], labels=["id", "class_id", "name"])
            ]
            mock_wide_rel_list.return_value = [
                KeyedTuple([1, 100, '1,2', 'nemo__pluto'], labels=["id", "class_id", "object_id_list", "name"])
            ]
            mock_parameter_list.return_value = [
                KeyedTuple(
                    [1, 10, None, 'color'], labels=["id", "object_class_id", "relationship_class_id", "name"]),
                KeyedTuple(
                    [2, None, 100, 'rel_speed'], labels=["id", "object_class_id", "relationship_class_id", "name"])
            ]
            try:
                self.db_map.add_parameter_values(
                    {'parameter_id': 1, 'object_id': 1, 'value': 'orange'},
                    {'parameter_id': 2, 'relationship_id': 1, 'value': 125})
            except SpineIntegrityError:
                self.fail("add_parameter_values() raised SpineIntegrityError unexpectedly")
        parameter_values = self.db_map.session.query(self.db_map.DiffParameterValue).all()
        self.assertEqual(len(parameter_values), 2)
        self.assertEqual(parameter_values[0].parameter_definition_id, 1)
        self.assertEqual(parameter_values[0].object_id, 1)
        self.assertIsNone(parameter_values[0].relationship_id)
        self.assertEqual(parameter_values[0].value, 'orange')
        self.assertEqual(parameter_values[1].parameter_definition_id, 2)
        self.assertIsNone(parameter_values[1].object_id)
        self.assertEqual(parameter_values[1].relationship_id, 1)
        self.assertEqual(parameter_values[1].value, '125')

    def test_add_parameter_value_for_both_object_and_relationship(self):
        """Test that adding a parameter value for both an object and a relationship raises an
        integrity error."""
        with mock.patch.object(DiffDatabaseMapping, "object_list") as mock_object_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_list") as mock_wide_rel_list, \
                mock.patch.object(DiffDatabaseMapping, "parameter_list") as mock_parameter_list:
            mock_object_list.return_value = [
                KeyedTuple([1, 10, 'nemo'], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 20, 'pluto'], labels=["id", "class_id", "name"])
            ]
            mock_wide_rel_list.return_value = [
                KeyedTuple([1, 100, '1,2', 'nemo__pluto'], labels=["id", "class_id", "object_id_list", "name"])
            ]
            mock_parameter_list.return_value = [
                KeyedTuple(
                    [1, 10, None, 'color'], labels=["id", "object_class_id", "relationship_class_id", "name"]),
                KeyedTuple(
                    [2, None, 100, 'rel_speed'], labels=["id", "object_class_id", "relationship_class_id", "name"])
            ]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_parameter_values(
                    {'parameter_id': 1, 'object_id': 1, 'relationship_id': 1, 'value': 'orange'})

    def test_add_parameter_value_with_invalid_object_or_relationship(self):
        """Test that adding a parameter value with an invalid object or relationship raises an
        integrity error."""
        with mock.patch.object(DiffDatabaseMapping, "object_list") as mock_object_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_list") as mock_wide_rel_list, \
                mock.patch.object(DiffDatabaseMapping, "parameter_list") as mock_parameter_list:
            mock_object_list.return_value = [
                KeyedTuple([1, 10, 'nemo'], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 20, 'pluto'], labels=["id", "class_id", "name"])
            ]
            mock_wide_rel_list.return_value = [
                KeyedTuple([1, 100, '1,2', 'nemo__pluto'], labels=["id", "class_id", "object_id_list", "name"])
            ]
            mock_parameter_list.return_value = [
                KeyedTuple(
                    [1, 10, None, 'color'], labels=["id", "object_class_id", "relationship_class_id", "name"]),
                KeyedTuple(
                    [2, None, 100, 'rel_speed'], labels=["id", "object_class_id", "relationship_class_id", "name"])
            ]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_parameter_values({'parameter_id': 1, 'object_id': 3, 'value': 'orange'})
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_parameter_values({'parameter_id': 2, 'relationship_id': 2, 'value': 125})

    def test_add_parameter_value_with_object_or_relationship_of_invalid_class(self):
        """Test that adding a parameter value with an object or relationship invalid for
        the parameter class raises an integrity error."""
        with mock.patch.object(DiffDatabaseMapping, "object_list") as mock_object_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_list") as mock_wide_rel_list, \
                mock.patch.object(DiffDatabaseMapping, "parameter_list") as mock_parameter_list:
            mock_object_list.return_value = [
                KeyedTuple([1, 10, 'nemo'], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 20, 'pluto'], labels=["id", "class_id", "name"])
            ]
            mock_wide_rel_list.return_value = [
                KeyedTuple([1, 100, '1,2', 'nemo__pluto'], labels=["id", "class_id", "object_id_list", "name"]),
                KeyedTuple([2, 200, '2,1', 'pluto__nemo'], labels=["id", "class_id", "object_id_list", "name"])
            ]
            mock_parameter_list.return_value = [
                KeyedTuple(
                    [1, 10, None, 'color'], labels=["id", "object_class_id", "relationship_class_id", "name"]),
                KeyedTuple(
                    [2, None, 100, 'rel_speed'], labels=["id", "object_class_id", "relationship_class_id", "name"])
            ]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_parameter_values({'parameter_id': 1, 'object_id': 2, 'value': 'orange'})
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_parameter_values({'parameter_id': 2, 'relationship_id': 2, 'value': 125})

    def test_add_same_parameter_value_twice(self):
        """Test that adding a parameter value twice only adds the first one."""
        with mock.patch.object(DiffDatabaseMapping, "object_list") as mock_object_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_list") as mock_wide_rel_list, \
                mock.patch.object(DiffDatabaseMapping, "parameter_list") as mock_parameter_list:
            mock_object_list.return_value = [
                KeyedTuple([1, 10, 'nemo'], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 20, 'pluto'], labels=["id", "class_id", "name"])
            ]
            mock_wide_rel_list.return_value = [
                KeyedTuple([1, 100, '1,2', 'nemo__pluto'], labels=["id", "class_id", "object_id_list", "name"])
            ]
            mock_parameter_list.return_value = [
                KeyedTuple(
                    [1, 10, None, 'color'], labels=["id", "object_class_id", "relationship_class_id", "name"])
            ]
            self.db_map.add_parameter_values(
                {'parameter_id': 1, 'object_id': 1, 'value': 'orange'},
                {'parameter_id': 1, 'object_id': 1, 'value': 'blue'},
                raise_intgr_error=False)
        parameter_values = self.db_map.session.query(self.db_map.DiffParameterValue).all()
        self.assertEqual(len(parameter_values), 1)
        self.assertEqual(parameter_values[0].parameter_definition_id, 1)
        self.assertEqual(parameter_values[0].object_id, 1)
        self.assertIsNone(parameter_values[0].relationship_id)
        self.assertEqual(parameter_values[0].value, 'orange')

    def test_add_existing_parameter_value(self):
        """Test that adding an existing parameter value raises an integrity error."""
        with mock.patch.object(DiffDatabaseMapping, "object_list") as mock_object_list, \
                mock.patch.object(DiffDatabaseMapping, "wide_relationship_list") as mock_wide_rel_list, \
                mock.patch.object(DiffDatabaseMapping, "parameter_list") as mock_parameter_list, \
                mock.patch.object(DiffDatabaseMapping, "parameter_value_list") as mock_parameter_value_list:
            mock_object_list.return_value = [
                KeyedTuple([1, 10, 'nemo'], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 20, 'pluto'], labels=["id", "class_id", "name"])
            ]
            mock_wide_rel_list.return_value = [
                KeyedTuple([1, 100, '1,2', 'nemo__pluto'], labels=["id", "class_id", "object_id_list", "name"])
            ]
            mock_parameter_list.return_value = [
                KeyedTuple(
                    [1, 10, None, 'color'], labels=["id", "object_class_id", "relationship_class_id", "name"])
            ]
            mock_parameter_value_list.return_value = [
                KeyedTuple(
                    [1, 1, 1, None, 'orange'],
                    labels=["id", "parameter_definition_id", "object_id", "relationship_id", "value"])
            ]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_parameter_values({'parameter_id': 1, 'object_id': 1, 'value': 'blue'})



if __name__ == '__main__':
    unittest.main()
