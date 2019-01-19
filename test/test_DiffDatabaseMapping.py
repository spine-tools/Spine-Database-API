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
#import sys
#sys.path.append('/spinedatabase_api')

from spinedatabase_api.diff_database_mapping import DiffDatabaseMapping, SpineIntegrityError
from spinedatabase_api.helpers import create_new_spine_database
from sqlalchemy.util import KeyedTuple
import unittest
from unittest import mock
import logging
import sys
from sqlalchemy.orm import Session

class TestDiffDatabaseMapping(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Overridden method. Runs once before all tests in this class."""
        logging.basicConfig(stream=sys.stderr, level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s: %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')
        engine = create_new_spine_database('sqlite://')
        cls.db_map = DiffDatabaseMapping("", username='UnitTest', create_all=False)
        cls.db_map.engine = engine
        cls.db_map.engine.connect()
        cls.db_map.session = Session(cls.db_map.engine, autoflush=False)
        cls.db_map.create_mapping()
        cls.db_map.create_diff_tables_and_mapping()
        cls.db_map.init_next_id()

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


if __name__ == '__main__':
    unittest.main()
