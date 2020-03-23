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
Unit tests for DiffDatabaseMapping class.

:author: P. Vennström (VTT)
:date:   29.11.2018
"""

import os
import unittest
from unittest import mock
import logging
import sys
from sqlalchemy.util import KeyedTuple
from spinedb_api.diff_database_mapping import DiffDatabaseMapping
from spinedb_api.exception import SpineIntegrityError
from spinedb_api.helpers import create_new_spine_database


def create_query_wrapper(db_map):
    def query_wrapper(*args, orig_query=db_map.query, **kwargs):
        arg = args[0]
        if isinstance(arg, mock.Mock):
            return arg.value
        return orig_query(*args, **kwargs)

    return query_wrapper


class TestDiffDatabaseMappingRemove(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Overridden method. Runs once before all tests in this class."""
        logging.basicConfig(
            stream=sys.stderr,
            level=logging.DEBUG,
            format="%(asctime)s %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        try:
            os.remove("temp.sqlite")
        except OSError:
            pass
        db_url = "sqlite:///temp.sqlite"
        create_new_spine_database(db_url)
        cls.db_map = DiffDatabaseMapping(db_url, username="UnitTest")

    @classmethod
    def tearDownClass(cls):
        """Overridden method. Runs once after all tests in this class."""
        try:
            os.remove("temp.sqlite")
        except OSError:
            pass

    def setUp(self):
        """Overridden method. Runs before each test.
        """
        # Set logging level to Error to silence "Logging level: All messages" print
        logging.disable(level=logging.ERROR)  # Disable logging
        self.db_map._reset_mapping()
        self.db_map._reset_diff_mapping()
        if self.db_map.has_pending_changes():
            self.db_map.commit_session("")
        self.db_map.session.query(self.db_map.NextId).delete(synchronize_session=False)
        self.query_wrapper = create_query_wrapper(self.db_map)
        logging.disable(level=logging.NOTSET)  # Enable logging

    def tearDown(self):
        """Overridden method. Runs after each test.
        Use this to free resources after a test if needed.
        """

    def test_remove_relationship(self):
        """Test adding and removing an relationship and commiting"""
        self.db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self.db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
        self.db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 2})
        ids, _ = self.db_map.add_wide_relationships({"name": "remove_me", "class_id": 3, "object_id_list": [1, 2]})
        self.db_map.remove_items(relationship_ids=ids)
        self.assertEqual(len(self.db_map.wide_relationship_list().all()), 0)
        self.db_map.commit_session("delete")
        self.assertEqual(len(self.db_map.wide_relationship_list().all()), 0)

    def test_remove_relationship_from_commited_session(self):
        """Test removing an relationship from an commited session"""
        self.db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self.db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
        self.db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 2})
        ids, _ = self.db_map.add_wide_relationships({"name": "remove_me", "class_id": 3, "object_id_list": [1, 2]})
        self.db_map.commit_session("add")
        self.assertEqual(len(self.db_map.wide_relationship_list().all()), 1)
        self.db_map.remove_items(relationship_ids=ids)
        self.assertEqual(len(self.db_map.wide_relationship_list().all()), 0)
        self.db_map.commit_session("")
        self.assertEqual(len(self.db_map.wide_relationship_list().all()), 0)

    def test_remove_object(self):
        """Test adding and removing an relationship and commiting"""
        self.db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        ids, _ = self.db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 2})
        self.db_map.remove_items(object_ids=ids)
        self.assertEqual(len(self.db_map.wide_relationship_list().all()), 0)
        self.db_map.commit_session("delete")
        self.assertEqual(len(self.db_map.wide_relationship_list().all()), 0)

    def test_remove_object_from_commited_session(self):
        """Test removing an relationship from an commited session"""
        self.db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        ids, _ = self.db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 2})
        self.db_map.commit_session("add")
        self.assertEqual(len(self.db_map.object_list().all()), 2)
        self.db_map.remove_items(object_ids=ids)
        self.assertEqual(len(self.db_map.object_list().all()), 0)
        self.db_map.commit_session("")
        self.assertEqual(len(self.db_map.object_list().all()), 0)

    def test_remove_relationship_class(self):
        """Test adding and removing an relationship and commiting"""
        self.db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        ids, _ = self.db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
        self.db_map.remove_items(relationship_class_ids=ids)
        self.assertEqual(len(self.db_map.wide_relationship_class_list().all()), 0)
        self.db_map.commit_session("delete")
        self.assertEqual(len(self.db_map.wide_relationship_class_list().all()), 0)

    def test_remove_relationship_class_from_commited_session(self):
        """Test removing an relationship from an commited session"""
        self.db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        ids, _ = self.db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
        self.db_map.commit_session("add")
        self.assertEqual(len(self.db_map.wide_relationship_class_list().all()), 1)
        self.db_map.remove_items(relationship_class_ids=ids)
        self.assertEqual(len(self.db_map.wide_relationship_class_list().all()), 0)
        self.db_map.commit_session("")
        self.assertEqual(len(self.db_map.wide_relationship_class_list().all()), 0)

    def test_remove_object_class(self):
        """Test adding and removing an relationship and commiting"""
        ids, _ = self.db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self.db_map.remove_items(object_class_ids=ids)
        self.assertEqual(len(self.db_map.object_class_list().all()), 0)
        self.db_map.commit_session("delete")
        self.assertEqual(len(self.db_map.object_class_list().all()), 0)

    def test_remove_object_class_from_commited_session(self):
        """Test removing an relationship from an commited session"""
        ids, _ = self.db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self.db_map.commit_session("add")
        self.assertEqual(len(self.db_map.object_class_list().all()), 2)
        self.db_map.remove_items(object_class_ids=ids)
        self.assertEqual(len(self.db_map.object_class_list().all()), 0)
        self.db_map.commit_session("")
        self.assertEqual(len(self.db_map.object_class_list().all()), 0)

    def test_remove_parameter_value(self):
        """Test adding and removing an parameter value and commiting"""
        self.db_map.add_object_classes({"name": "oc1", "id": 1}, strict=True)
        self.db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, strict=True)
        self.db_map.add_parameter_definitions({"name": "param", "id": 1, "object_class_id": 1}, strict=True)
        self.db_map.add_parameter_values(
            {"value": "0", "id": 1, "parameter_definition_id": 1, "object_id": 1, "object_class_id": 1, "alternative_id": 1}, strict=True
        )
        self.assertEqual(len(self.db_map.parameter_value_list().all()), 1)
        self.db_map.remove_items(parameter_value_ids=[1])
        self.assertEqual(len(self.db_map.parameter_value_list().all()), 0)
        self.db_map.commit_session("delete")
        self.assertEqual(len(self.db_map.parameter_value_list().all()), 0)

    def test_remove_parameter_value_from_commited_session(self):
        """Test adding and commiting a parmaeter value and then removing it"""
        self.db_map.add_object_classes({"name": "oc1", "id": 1}, strict=True)
        self.db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, strict=True)
        self.db_map.add_parameter_definitions({"name": "param", "id": 1, "object_class_id": 1}, strict=True)
        self.db_map.add_parameter_values(
            {"value": "0", "id": 1, "parameter_definition_id": 1, "object_id": 1, "object_class_id": 1, "alternative_id": 1}, strict=True
        )
        self.db_map.commit_session("add")
        self.assertEqual(len(self.db_map.parameter_value_list().all()), 1)
        self.db_map.remove_items(parameter_value_ids=[1])
        self.assertEqual(len(self.db_map.parameter_value_list().all()), 0)
        self.db_map.commit_session("delete")
        self.assertEqual(len(self.db_map.parameter_value_list().all()), 0)

    def test_remove_object_with_parameter_value(self):
        """Test adding and removing an parameter value and commiting"""
        self.db_map.add_object_classes({"name": "oc1", "id": 1}, strict=True)
        self.db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, strict=True)
        self.db_map.add_parameter_definitions({"name": "param", "id": 1, "object_class_id": 1}, strict=True)
        self.db_map.add_parameter_values(
            {"value": "0", "id": 1, "parameter_definition_id": 1, "object_id": 1, "object_class_id": 1, "alternative_id": 1}, strict=True
        )
        self.assertEqual(len(self.db_map.parameter_value_list().all()), 1)
        self.db_map.remove_items(object_ids=[1])
        self.assertEqual(len(self.db_map.parameter_value_list().all()), 0)
        self.db_map.commit_session("delete")
        self.assertEqual(len(self.db_map.parameter_value_list().all()), 0)

    def test_remove_object_with_parameter_value_from_commited_session(self):
        """Test adding and commiting a parmaeter value and then removing it"""
        self.db_map.add_object_classes({"name": "oc1", "id": 1}, strict=True)
        self.db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, strict=True)
        self.db_map.add_parameter_definitions({"name": "param", "id": 1, "object_class_id": 1}, strict=True)
        self.db_map.add_parameter_values(
            {"value": "0", "id": 1, "parameter_definition_id": 1, "object_id": 1, "object_class_id": 1, "alternative_id": 1}, strict=True
        )
        self.db_map.commit_session("add")
        self.assertEqual(len(self.db_map.parameter_value_list().all()), 1)
        self.db_map.remove_items(object_ids=[1])
        self.assertEqual(len(self.db_map.parameter_value_list().all()), 0)
        self.db_map.commit_session("delete")
        self.assertEqual(len(self.db_map.parameter_value_list().all()), 0)


class TestDiffDatabaseMappingAdd(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Overridden method. Runs once before all tests in this class."""
        logging.basicConfig(
            stream=sys.stderr,
            level=logging.DEBUG,
            format="%(asctime)s %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        try:
            os.remove("temp.sqlite")
        except OSError:
            pass
        db_url = "sqlite:///temp.sqlite"
        create_new_spine_database(db_url)
        cls.db_map = DiffDatabaseMapping(db_url, username="UnitTest")

    @classmethod
    def tearDownClass(cls):
        """Overridden method. Runs once after all tests in this class."""
        try:
            os.remove("temp.sqlite")
        except OSError:
            pass

    def setUp(self):
        """Overridden method. Runs before each test.
        """
        # Set logging level to Error to silence "Logging level: All messages" print
        logging.disable(level=logging.ERROR)  # Disable logging
        self.db_map._reset_mapping()
        self.db_map._reset_diff_mapping()
        self.db_map.session.query(self.db_map.NextId).delete(synchronize_session=False)
        self.query_wrapper = create_query_wrapper(self.db_map)
        logging.disable(level=logging.NOTSET)  # Enable logging

    def tearDown(self):
        """Overridden method. Runs after each test.
        Use this to free resources after a test if needed.
        """

    def test_add_and_retrieve_many_objects(self):
        """Tests add many objects into db and retrieving them."""
        ids, _ = self.db_map.add_object_classes({"name": "testclass"})
        class_id = next(iter(ids))
        added = self.db_map.add_objects(*[{"name": str(i), "class_id": class_id} for i in range(1001)])[0]
        self.assertEqual(len(added), 1001)
        self.db_map.commit_session("test_commit")
        self.assertEqual(self.db_map.session.query(self.db_map.Entity).count(), 1001)

    @unittest.skip("TODO")
    def test_check_wide_relationship_with_repeated_object(self):
        """Tests that checking valid relationship with one repeated object doesn't throw an error"""

    def test_add_object_classes(self):
        """Test that adding object classes works."""
        self.db_map.add_object_classes({"name": "fish"}, {"name": "dog"})
        object_classes = (
            self.db_map.session.query(self.db_map.DiffEntityClass)
            .filter(self.db_map.DiffEntityClass.type_id == self.db_map.object_class_type)
            .all()
        )
        self.assertEqual(len(object_classes), 2)
        self.assertEqual(object_classes[0].name, "fish")
        self.assertEqual(object_classes[1].name, "dog")

    def test_add_object_class_with_invalid_name(self):
        """Test that adding object classes with empty name raises error"""
        with self.assertRaises(SpineIntegrityError):
            self.db_map.add_object_classes({"name": ""}, strict=True)

    def test_add_object_classes_with_same_name(self):
        """Test that adding two object classes with the same name only adds one of them."""
        self.db_map.add_object_classes({"name": "fish"}, {"name": "fish"})
        object_classes = (
            self.db_map.session.query(self.db_map.DiffEntityClass)
            .filter(self.db_map.DiffEntityClass.type_id == self.db_map.object_class_type)
            .all()
        )
        self.assertEqual(len(object_classes), 1)
        self.assertEqual(object_classes[0].name, "fish")

    def test_add_object_class_with_same_name_as_existing_one(self):
        """Test that adding an object class with an already taken name raises an integrity error."""
        with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
            DiffDatabaseMapping, "object_class_sq"
        ) as mock_object_class_sq:
            mock_query.side_effect = self.query_wrapper
            mock_object_class_sq.value = [KeyedTuple([1, "fish"], labels=["id", "name"])]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_object_classes({"name": "fish"}, strict=True)

    def test_add_objects(self):
        """Test that adding objects works."""

        with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
            DiffDatabaseMapping, "object_class_sq"
        ) as mock_object_class_sq:
            mock_query.side_effect = self.query_wrapper
            mock_object_class_sq.value = [KeyedTuple([1, "fish"], labels=["id", "name"])]
            self.db_map.add_objects({"name": "nemo", "class_id": 1}, {"name": "dory", "class_id": 1})
        objects = (
            self.db_map.session.query(self.db_map.DiffEntity)
            .filter(self.db_map.DiffEntity.type_id == self.db_map.object_entity_type)
            .all()
        )
        self.assertEqual(len(objects), 2)
        self.assertEqual(objects[0].name, "nemo")
        self.assertEqual(objects[0].class_id, 1)
        self.assertEqual(objects[1].name, "dory")
        self.assertEqual(objects[1].class_id, 1)

    def test_add_object_with_invalid_name(self):
        """Test that adding object classes with empty name raises error"""
        self.db_map.add_object_classes({"name": "fish"})
        with self.assertRaises(SpineIntegrityError):
            self.db_map.add_objects({"name": "", "class_id": 1}, strict=True)

    def test_add_objects_with_same_name(self):
        """Test that adding two objects with the same name only adds one of them."""

        with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
            DiffDatabaseMapping, "object_class_sq"
        ) as mock_object_class_sq:
            mock_query.side_effect = self.query_wrapper
            mock_object_class_sq.value = [KeyedTuple([1, "fish"], labels=["id", "name"])]
            self.db_map.add_objects({"name": "nemo", "class_id": 1}, {"name": "nemo", "class_id": 1})
        objects = (
            self.db_map.session.query(self.db_map.DiffEntity)
            .filter(self.db_map.DiffEntity.type_id == self.db_map.object_entity_type)
            .all()
        )
        self.assertEqual(len(objects), 1)
        self.assertEqual(objects[0].name, "nemo")
        self.assertEqual(objects[0].class_id, 1)

    def test_add_object_with_same_name_as_existing_one(self):
        """Test that adding an object with an already taken name raises an integrity error."""
        with mock.patch.object(DiffDatabaseMapping, "object_class_list") as mock_object_class_list, mock.patch.object(
            DiffDatabaseMapping, "object_list"
        ) as mock_object_list:
            mock_object_class_list.return_value = [KeyedTuple([1, "fish"], labels=["id", "name"])]
            mock_object_list.return_value = [KeyedTuple([1, 1, "nemo"], labels=["id", "class_id", "name"])]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_objects({"name": "nemo", "class_id": 1}, strict=True)

    def test_add_object_with_invalid_class(self):
        """Test that adding an object with a non existing class raises an integrity error."""
        with mock.patch.object(DiffDatabaseMapping, "object_class_list") as mock_object_class_list:
            mock_object_class_list.return_value = [KeyedTuple([1, "fish"], labels=["id", "name"])]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_objects({"name": "pluto", "class_id": 2}, strict=True)

    def test_add_relationship_classes(self):
        """Test that adding relationship classes works."""
        self.db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self.db_map.add_wide_relationship_classes(
            {"name": "rc1", "object_class_id_list": [1, 2]}, {"name": "rc2", "object_class_id_list": [2, 1]}
        )
        rel_ent_clss = self.db_map.session.query(self.db_map.DiffRelationshipEntityClass).all()
        rel_clss = (
            self.db_map.session.query(self.db_map.DiffEntityClass)
            .filter(self.db_map.DiffEntityClass.type_id == self.db_map.relationship_class_type)
            .all()
        )
        self.assertEqual(len(rel_ent_clss), 4)
        self.assertEqual(rel_clss[0].name, "rc1")
        self.assertEqual(rel_ent_clss[0].member_class_id, 1)
        self.assertEqual(rel_ent_clss[1].member_class_id, 2)
        self.assertEqual(rel_clss[1].name, "rc2")
        self.assertEqual(rel_ent_clss[2].member_class_id, 2)
        self.assertEqual(rel_ent_clss[3].member_class_id, 1)

    def test_add_relationship_classes_with_invalid_name(self):
        """Test that adding object classes with empty name raises error"""
        self.db_map.add_object_classes({"name": "fish"})
        with self.assertRaises(SpineIntegrityError):
            self.db_map.add_wide_relationship_classes({"name": "", "object_class_id_list": [1]}, strict=True)

    def test_add_relationship_classes_with_same_name(self):
        """Test that adding two relationship classes with the same name only adds one of them."""
        self.db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self.db_map.add_wide_relationship_classes(
            {"name": "rc1", "object_class_id_list": [1, 2]}, {"name": "rc1", "object_class_id_list": [1, 2]}
        )
        relationship_members = self.db_map.session.query(self.db_map.DiffRelationshipEntityClass).all()
        relationships = (
            self.db_map.session.query(self.db_map.DiffEntityClass)
            .filter(self.db_map.DiffEntityClass.type_id == self.db_map.relationship_class_type)
            .all()
        )
        self.assertEqual(len(relationship_members), 2)
        self.assertEqual(len(relationships), 1)
        self.assertEqual(relationships[0].name, "rc1")
        self.assertEqual(relationship_members[0].member_class_id, 1)
        self.assertEqual(relationship_members[1].member_class_id, 2)

    def test_add_relationship_class_with_same_name_as_existing_one(self):
        """Test that adding a relationship class with an already taken name raises an integrity error."""
        with mock.patch.object(DiffDatabaseMapping, "object_class_list") as mock_object_class_list, mock.patch.object(
            DiffDatabaseMapping, "wide_relationship_class_list"
        ) as mock_wide_rel_cls_list:
            mock_object_class_list.return_value = [
                KeyedTuple([1, "fish"], labels=["id", "name"]),
                KeyedTuple([2, "dog"], labels=["id", "name"]),
            ]
            mock_wide_rel_cls_list.return_value = [
                KeyedTuple([1, "1,2", "fish__dog"], labels=["id", "object_class_id_list", "name"])
            ]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_wide_relationship_classes(
                    {"name": "fish__dog", "object_class_id_list": [1, 2]}, strict=True
                )

    def test_add_relationship_class_with_invalid_object_class(self):
        """Test that adding a relationship class with a non existing object class raises an integrity error."""
        with mock.patch.object(DiffDatabaseMapping, "object_class_list") as mock_object_class_list, mock.patch.object(
            DiffDatabaseMapping, "wide_relationship_class_list"
        ):
            mock_object_class_list.return_value = [KeyedTuple([1, "fish"], labels=["id", "name"])]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_wide_relationship_classes(
                    {"name": "fish__dog", "object_class_id_list": [1, 2]}, strict=True
                )

    def test_add_relationships(self):
        """Test that adding relationships works."""
        self.db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self.db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
        self.db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 2})
        self.db_map.add_wide_relationships({"name": "nemo__pluto", "class_id": 3, "object_id_list": [1, 2]})

        rel_ents = self.db_map.session.query(self.db_map.DiffRelationshipEntity).all()
        relationships = (
            self.db_map.session.query(self.db_map.DiffEntity)
            .filter(self.db_map.DiffEntity.type_id == self.db_map.relationship_entity_type)
            .all()
        )
        self.assertEqual(len(rel_ents), 2)
        self.assertEqual(len(relationships), 1)
        self.assertEqual(relationships[0].name, "nemo__pluto")
        self.assertEqual(rel_ents[0].entity_class_id, 3)
        self.assertEqual(rel_ents[0].member_id, 1)
        self.assertEqual(rel_ents[1].entity_class_id, 3)
        self.assertEqual(rel_ents[1].member_id, 2)

    def test_add_relationship_with_invalid_name(self):
        """Test that adding object classes with empty name raises error"""
        self.db_map.add_object_classes({"name": "oc1"}, strict=True)
        self.db_map.add_wide_relationship_classes({"name": "rc1", "object_class_id_list": [1]}, strict=True)
        self.db_map.add_objects({"name": "o1", "class_id": 1}, strict=True)
        with self.assertRaises(SpineIntegrityError):
            self.db_map.add_wide_relationships({"name": "", "class_id": 1, "object_id_list": [1]}, strict=True)

    def test_add_identical_relationships(self):
        """Test that adding two relationships with the same class and same objects only adds the first one.
        """
        self.db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self.db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
        self.db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 2})
        self.db_map.add_wide_relationships(
            {"name": "nemo__pluto", "class_id": 3, "object_id_list": [1, 2]},
            {"name": "nemo__pluto_duplicate", "class_id": 3, "object_id_list": [1, 2]},
        )
        relationships = self.db_map.session.query(self.db_map.DiffRelationship).all()
        self.assertEqual(len(relationships), 1)

    def test_add_relationship_identical_to_existing_one(self):
        """Test that adding a relationship with the same class and same objects as an existing one
        raises an integrity error.
        """
        with mock.patch.object(DiffDatabaseMapping, "object_list") as mock_object_list, mock.patch.object(
            DiffDatabaseMapping, "wide_relationship_class_list"
        ) as mock_wide_rel_cls_list, mock.patch.object(
            DiffDatabaseMapping, "wide_relationship_list"
        ) as mock_wide_rel_list:
            mock_object_list.return_value = [
                KeyedTuple([1, 10, "nemo"], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 20, "pluto"], labels=["id", "class_id", "name"]),
            ]
            mock_wide_rel_cls_list.return_value = [
                KeyedTuple([1, "10,20", "fish__dog"], labels=["id", "object_class_id_list", "name"])
            ]
            mock_wide_rel_list.return_value = [
                KeyedTuple([1, 1, "1,2", "nemo__pluto"], labels=["id", "class_id", "object_id_list", "name"])
            ]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_wide_relationships(
                    {"name": "nemoy__plutoy", "class_id": 1, "object_id_list": [1, 2]}, strict=True
                )

    def test_add_relationship_with_invalid_class(self):
        """Test that adding a relationship with an invalid class raises an integrity error.
        """
        with mock.patch.object(DiffDatabaseMapping, "object_list") as mock_object_list, mock.patch.object(
            DiffDatabaseMapping, "wide_relationship_class_list"
        ) as mock_wide_rel_cls_list, mock.patch.object(DiffDatabaseMapping, "wide_relationship_list"):
            mock_object_list.return_value = [
                KeyedTuple([1, 10, "nemo"], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 20, "pluto"], labels=["id", "class_id", "name"]),
            ]
            mock_wide_rel_cls_list.return_value = [
                KeyedTuple([1, "10,20", "fish__dog"], labels=["id", "object_class_id_list", "name"])
            ]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_wide_relationships(
                    {"name": "nemo__pluto", "class_id": 2, "object_id_list": [1, 2]}, strict=True
                )

    def test_add_relationship_with_invalid_object(self):
        """Test that adding a relationship with an invalid object raises an integrity error.
        """
        with mock.patch.object(DiffDatabaseMapping, "object_list") as mock_object_list, mock.patch.object(
            DiffDatabaseMapping, "wide_relationship_class_list"
        ) as mock_wide_rel_cls_list, mock.patch.object(DiffDatabaseMapping, "wide_relationship_list"):
            mock_object_list.return_value = [
                KeyedTuple([1, 10, "nemo"], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 20, "pluto"], labels=["id", "class_id", "name"]),
            ]
            mock_wide_rel_cls_list.return_value = [
                KeyedTuple([1, "10,20", "fish__dog"], labels=["id", "object_class_id_list", "name"])
            ]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_wide_relationships(
                    {"name": "nemo__pluto", "class_id": 1, "object_id_list": [1, 3]}, strict=True
                )

    def test_add_parameter_definitions(self):
        """Test that adding parameter definitions works."""
        self.db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self.db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
        self.db_map.add_parameter_definitions(
            {"name": "color", "object_class_id": 1, "description": "test1"},
            {"name": "relative_speed", "relationship_class_id": 3, "description": "test2"},
        )
        parameter_definitions = self.db_map.session.query(self.db_map.DiffParameterDefinition).all()
        self.assertEqual(len(parameter_definitions), 2)
        self.assertEqual(parameter_definitions[0].name, "color")
        self.assertEqual(parameter_definitions[0].entity_class_id, 1)
        self.assertEqual(parameter_definitions[0].description, "test1")
        self.assertEqual(parameter_definitions[1].name, "relative_speed")
        self.assertEqual(parameter_definitions[1].entity_class_id, 3)
        self.assertEqual(parameter_definitions[1].description, "test2")

    def test_add_parameter_with_invalid_name(self):
        """Test that adding object classes with empty name raises error"""
        self.db_map.add_object_classes({"name": "oc1"}, strict=True)
        with self.assertRaises(SpineIntegrityError):
            self.db_map.add_parameter_definitions({"name": "", "object_class_id": 1}, strict=True)

    def test_add_parameter_definitions_with_same_name(self):
        """Test that adding two parameter_definitions with the same name adds both of them."""
        self.db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self.db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
        self.db_map.add_parameter_definitions(
            {"name": "color", "object_class_id": 1}, {"name": "color", "relationship_class_id": 3}
        )
        parameter_definitions = self.db_map.session.query(self.db_map.DiffParameterDefinition).all()
        self.assertEqual(len(parameter_definitions), 2)
        self.assertEqual(parameter_definitions[0].name, "color")
        self.assertEqual(parameter_definitions[1].name, "color")
        self.assertEqual(parameter_definitions[0].entity_class_id, 1)

    def test_add_parameter_with_same_name_as_existing_one(self):
        """Test that adding parameter_definitions with an already taken name raises and integrity error."""
        self.db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self.db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
        self.db_map.add_parameter_definitions(
            {"name": "color", "object_class_id": 1}, {"name": "color", "relationship_class_id": 3}
        )
        with self.assertRaises(SpineIntegrityError):
            self.db_map.add_parameter_definitions({"name": "color", "object_class_id": 1}, strict=True)

    def test_add_parameter_with_invalid_class(self):
        """Test that adding parameter_definitions with an invalid (object or relationship) class raises and integrity error."""
        self.db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self.db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
        with self.assertRaises(SpineIntegrityError):
            self.db_map.add_parameter_definitions({"name": "color", "object_class_id": 3}, strict=True)
        with self.assertRaises(SpineIntegrityError):
            self.db_map.add_parameter_definitions({"name": "color", "relationship_class_id": 1}, strict=True)

    def test_add_parameter_for_both_object_and_relationship_class(self):
        """Test that adding parameter_definitions associated to both and object and relationship class
        raises and integrity error."""
        with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
            DiffDatabaseMapping, "object_class_sq"
        ) as mock_object_class_sq, mock.patch.object(
            DiffDatabaseMapping, "wide_relationship_class_sq"
        ) as mock_wide_rel_cls_sq:
            mock_query.side_effect = self.query_wrapper
            mock_object_class_sq.value = [KeyedTuple([1, "fish"], labels=["id", "name"])]
            mock_wide_rel_cls_sq.value = [
                KeyedTuple([10, "1,2", "fish__dog"], labels=["id", "object_class_id_list", "name"])
            ]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_parameter_definitions(
                    {"name": "color", "object_class_id": 1, "relationship_class_id": 10}, strict=True
                )

    def test_add_parameter_values(self):
        """Test that adding parameter values works."""

        with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
            DiffDatabaseMapping, "object_sq"
        ) as mock_object_sq, mock.patch.object(
            DiffDatabaseMapping, "wide_relationship_sq"
        ) as mock_wide_rel_sq, mock.patch.object(
            DiffDatabaseMapping, "parameter_definition_sq"
        ) as mock_parameter_definition_sq:
            mock_query.side_effect = self.query_wrapper
            mock_object_sq.value = [
                KeyedTuple([1, 10, "nemo"], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 20, "pluto"], labels=["id", "class_id", "name"]),
            ]
            mock_wide_rel_sq.value = [
                KeyedTuple([2, 100, "1,2", "nemo__pluto"], labels=["id", "class_id", "object_id_list", "name"])
            ]
            mock_parameter_definition_sq.value = [
                KeyedTuple(
                    [1, 10, None, "color", None],
                    labels=["id", "object_class_id", "relationship_class_id", "name", "parameter_value_list_id"],
                ),
                KeyedTuple(
                    [2, None, 100, "rel_speed", None],
                    labels=["id", "object_class_id", "relationship_class_id", "name", "parameter_value_list_id"],
                ),
            ]
            self.db_map.add_parameter_values(
                {"parameter_definition_id": 1, "object_id": 1, "object_class_id": 10, "value": '"orange"', "alternative_id": 1},
                {"parameter_definition_id": 2, "relationship_id": 2, "relationship_class_id": 100, "value": "125", "alternative_id": 1},
            )
        parameter_values = self.db_map.session.query(self.db_map.DiffParameterValue).all()
        self.assertEqual(len(parameter_values), 2)
        self.assertEqual(parameter_values[0].parameter_definition_id, 1)
        self.assertEqual(parameter_values[0].entity_id, 1)
        self.assertEqual(parameter_values[0].value, '"orange"')
        self.assertEqual(parameter_values[1].parameter_definition_id, 2)
        self.assertEqual(parameter_values[1].entity_id, 2)
        self.assertEqual(parameter_values[1].value, "125")

    def test_add_parameter_value_for_both_object_and_relationship(self):
        """Test that adding a parameter value for both an object and a relationship raises an
        integrity error."""
        with mock.patch.object(DiffDatabaseMapping, "object_list") as mock_object_list, mock.patch.object(
            DiffDatabaseMapping, "wide_relationship_list"
        ) as mock_wide_rel_list, mock.patch.object(
            DiffDatabaseMapping, "parameter_definition_list"
        ) as mock_parameter_definition_list:
            mock_object_list.return_value = [
                KeyedTuple([1, 10, "nemo"], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 20, "pluto"], labels=["id", "class_id", "name"]),
            ]
            mock_wide_rel_list.return_value = [
                KeyedTuple([1, 100, "1,2", "nemo__pluto"], labels=["id", "class_id", "object_id_list", "name"])
            ]
            mock_parameter_definition_list.return_value = [
                KeyedTuple(
                    [1, 10, None, "color", None],
                    labels=["id", "object_class_id", "relationship_class_id", "name", "parameter_value_list_id"],
                ),
                KeyedTuple(
                    [2, None, 100, "rel_speed", None],
                    labels=["id", "object_class_id", "relationship_class_id", "name", "parameter_value_list_id"],
                ),
            ]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_parameter_values(
                    {"parameter_definition_id": 1, "object_id": 1, "relationship_id": 1, "value": "orange"}, strict=True
                )

    def test_add_parameter_value_with_invalid_object_or_relationship(self):
        """Test that adding a parameter value with an invalid object or relationship raises an
        integrity error."""
        with mock.patch.object(DiffDatabaseMapping, "object_list") as mock_object_list, mock.patch.object(
            DiffDatabaseMapping, "wide_relationship_list"
        ) as mock_wide_rel_list, mock.patch.object(
            DiffDatabaseMapping, "parameter_definition_list"
        ) as mock_parameter_definition_list:
            mock_object_list.return_value = [
                KeyedTuple([1, 10, "nemo"], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 20, "pluto"], labels=["id", "class_id", "name"]),
            ]
            mock_wide_rel_list.return_value = [
                KeyedTuple([1, 100, "1,2", "nemo__pluto"], labels=["id", "class_id", "object_id_list", "name"])
            ]
            mock_parameter_definition_list.return_value = [
                KeyedTuple(
                    [1, 10, None, "color", None],
                    labels=["id", "object_class_id", "relationship_class_id", "name", "parameter_value_list_id"],
                ),
                KeyedTuple(
                    [2, None, 100, "rel_speed", None],
                    labels=["id", "object_class_id", "relationship_class_id", "name", "parameter_value_list_id"],
                ),
            ]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_parameter_values(
                    {"parameter_definition_id": 1, "object_id": 3, "value": "orange"}, strict=True
                )
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_parameter_values(
                    {"parameter_definition_id": 2, "relationship_id": 2, "value": "125"}, strict=True
                )

    def test_add_parameter_value_with_object_or_relationship_of_invalid_class(self):
        """Test that adding a parameter value with an object or relationship invalid for
        the parameter class raises an integrity error."""
        with mock.patch.object(DiffDatabaseMapping, "object_list") as mock_object_list, mock.patch.object(
            DiffDatabaseMapping, "wide_relationship_list"
        ) as mock_wide_rel_list, mock.patch.object(
            DiffDatabaseMapping, "parameter_definition_list"
        ) as mock_parameter_definition_list:
            mock_object_list.return_value = [
                KeyedTuple([1, 10, "nemo"], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 20, "pluto"], labels=["id", "class_id", "name"]),
            ]
            mock_wide_rel_list.return_value = [
                KeyedTuple([1, 100, "1,2", "nemo__pluto"], labels=["id", "class_id", "object_id_list", "name"]),
                KeyedTuple([2, 200, "2,1", "pluto__nemo"], labels=["id", "class_id", "object_id_list", "name"]),
            ]
            mock_parameter_definition_list.return_value = [
                KeyedTuple(
                    [1, 10, None, "color", None],
                    labels=["id", "object_class_id", "relationship_class_id", "name", "parameter_value_list_id"],
                ),
                KeyedTuple(
                    [2, None, 100, "rel_speed", None],
                    labels=["id", "object_class_id", "relationship_class_id", "name", "parameter_value_list_id"],
                ),
            ]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_parameter_values(
                    {"parameter_definition_id": 1, "object_id": 2, "value": "orange"}, strict=True
                )
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_parameter_values(
                    {"parameter_definition_id": 2, "relationship_id": 2, "value": "125"}, strict=True
                )

    def test_add_same_parameter_value_twice(self):
        """Test that adding a parameter value twice only adds the first one."""

        with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
            DiffDatabaseMapping, "object_sq"
        ) as mock_object_sq, mock.patch.object(
            DiffDatabaseMapping, "wide_relationship_sq"
        ) as mock_wide_rel_sq, mock.patch.object(
            DiffDatabaseMapping, "parameter_definition_sq"
        ) as mock_parameter_definition_sq:
            mock_query.side_effect = self.query_wrapper
            mock_object_sq.value = [
                KeyedTuple([1, 10, "nemo"], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 20, "pluto"], labels=["id", "class_id", "name"]),
            ]
            mock_wide_rel_sq.value = [
                KeyedTuple([1, 100, "1,2", "nemo__pluto"], labels=["id", "class_id", "object_id_list", "name"])
            ]
            mock_parameter_definition_sq.value = [
                KeyedTuple(
                    [1, 10, None, "color", None],
                    labels=["id", "object_class_id", "relationship_class_id", "name", "parameter_value_list_id"],
                )
            ]
            self.db_map.add_parameter_values(
                {"parameter_definition_id": 1, "object_id": 1, "object_class_id": 10, "value": '"orange"', "alternative_id": 1},
                {"parameter_definition_id": 1, "object_id": 1, "object_class_id": 10, "value": '"blue"', "alternative_id": 1},
            )
        parameter_values = self.db_map.session.query(self.db_map.DiffParameterValue).all()
        self.assertEqual(len(parameter_values), 1)
        self.assertEqual(parameter_values[0].parameter_definition_id, 1)
        self.assertEqual(parameter_values[0].entity_id, 1)
        self.assertEqual(parameter_values[0].value, '"orange"')

    def test_add_existing_parameter_value(self):
        """Test that adding an existing parameter value raises an integrity error."""

        with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
            DiffDatabaseMapping, "object_sq"
        ) as mock_object_sq, mock.patch.object(
            DiffDatabaseMapping, "wide_relationship_sq"
        ) as mock_wide_rel_sq, mock.patch.object(
            DiffDatabaseMapping, "parameter_definition_sq"
        ) as mock_parameter_definition_sq, mock.patch.object(
            DiffDatabaseMapping, "parameter_value_sq"
        ) as mock_parameter_value_sq:
            mock_query.side_effect = self.query_wrapper
            mock_object_sq.value = [
                KeyedTuple([1, 10, "nemo"], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 20, "pluto"], labels=["id", "class_id", "name"]),
            ]
            mock_wide_rel_sq.value = [
                KeyedTuple([1, 100, "1,2", "nemo__pluto"], labels=["id", "class_id", "object_id_list", "name"])
            ]
            mock_parameter_definition_sq.value = [
                KeyedTuple(
                    [1, 10, None, "color", None],
                    labels=["id", "object_class_id", "relationship_class_id", "name", "parameter_value_list_id"],
                )
            ]
            mock_parameter_value_sq.value = [
                KeyedTuple(
                    [1, 1, 1, None, "orange", 1],
                    labels=["id", "parameter_definition_id", "object_id", "relationship_id", "value", "alternative_id"],
                )
            ]
            with self.assertRaises(SpineIntegrityError):
                self.db_map.add_parameter_values(
                    {"parameter_definition_id": 1, "object_id": 1, "value": "blue", "alternative_id": 1}, strict=True
                )


class TestDiffDatabaseMappingUpdate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Overridden method. Runs once before all tests in this class."""
        logging.basicConfig(
            stream=sys.stderr,
            level=logging.DEBUG,
            format="%(asctime)s %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        try:
            os.remove("temp.sqlite")
        except OSError:
            pass
        db_url = "sqlite:///temp.sqlite"
        create_new_spine_database(db_url)
        cls.db_map = DiffDatabaseMapping(db_url, username="UnitTest")

    @classmethod
    def tearDownClass(cls):
        """Overridden method. Runs once after all tests in this class."""
        try:
            os.remove("temp.sqlite")
        except OSError:
            pass

    def setUp(self):
        """Overridden method. Runs before each test.
        """
        # Set logging level to Error to silence "Logging level: All messages" print
        logging.disable(level=logging.ERROR)  # Disable logging
        self.db_map._reset_mapping()
        self.db_map._reset_diff_mapping()
        self.db_map.session.query(self.db_map.NextId).delete(synchronize_session=False)
        self.query_wrapper = create_query_wrapper(self.db_map)
        logging.disable(level=logging.NOTSET)  # Enable logging

    def tearDown(self):
        """Overridden method. Runs after each test.
        Use this to free resources after a test if needed.
        """

    def test_update_object_classes(self):
        """Test that updating object classes works."""
        self.db_map.add_object_classes({"id": 1, "name": "fish"}, {"id": 2, "name": "dog"})
        ids, intgr_error_log = self.db_map.update_object_classes({"id": 1, "name": "octopus"}, {"id": 2, "name": "god"})
        sq = self.db_map.object_class_sq
        object_classes = {x.id: x.name for x in self.db_map.query(sq).filter(sq.c.id.in_(ids))}
        self.assertEqual(intgr_error_log, [])
        self.assertEqual(object_classes[1], "octopus")
        self.assertEqual(object_classes[2], "god")

    def test_update_objects(self):
        """Test that updating objects works."""
        with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
            DiffDatabaseMapping, "object_class_sq"
        ) as mock_object_class_sq:
            mock_query.side_effect = self.query_wrapper
            mock_object_class_sq.value = [KeyedTuple([1, "fish"], labels=["id", "name"])]
            self.db_map.add_objects({"id": 1, "name": "nemo", "class_id": 1}, {"id": 2, "name": "dory", "class_id": 1})
            ids, intgr_error_log = self.db_map.update_objects(
                {"id": 1, "name": "klaus"}, {"id": 2, "name": "squidward"}
            )
        sq = self.db_map.object_sq
        objects = {x.id: x.name for x in self.db_map.query(sq).filter(sq.c.id.in_(ids))}
        self.assertEqual(intgr_error_log, [])
        self.assertEqual(objects[1], "klaus")
        self.assertEqual(objects[2], "squidward")

    def test_update_objects_not_commited(self):
        """Test that updating objects works."""
        self.db_map.add_object_classes({"id": 1, "name": "some_class"})
        self.db_map.add_objects({"id": 1, "name": "nemo", "class_id": 1})
        ids, intgr_error_log = self.db_map.update_objects({"id": 1, "name": "klaus"})
        sq = self.db_map.object_sq
        objects = {x.id: x.name for x in self.db_map.query(sq).filter(sq.c.id.in_(ids))}
        self.assertEqual(intgr_error_log, [])
        self.assertEqual(objects[1], "klaus")
        self.assertEqual(self.db_map.object_list(id_list=[1]).first().name, "klaus")
        self.db_map.commit_session("update")
        self.assertEqual(self.db_map.object_list(id_list=[1]).first().name, "klaus")

    def test_update_committed_object(self):
        """Test that updating objects works."""
        self.db_map.add_object_classes({"id": 1, "name": "some_class"})
        self.db_map.add_objects({"id": 1, "name": "nemo", "class_id": 1})
        self.db_map.commit_session("update")
        ids, intgr_error_log = self.db_map.update_objects({"id": 1, "name": "klaus"})
        sq = self.db_map.object_sq
        objects = {x.id: x.name for x in self.db_map.query(sq).filter(sq.c.id.in_(ids))}
        self.assertEqual(intgr_error_log, [])
        self.assertEqual(objects[1], "klaus")
        self.assertEqual(self.db_map.object_list(id_list=[1]).first().name, "klaus")
        self.db_map.commit_session("update")
        self.assertEqual(self.db_map.object_list(id_list=[1]).first().name, "klaus")

    def test_update_relationship_classes(self):
        """Test that updating relationship classes works."""
        self.db_map.add_object_classes({"name": "dog", "id": 1}, {"name": "fish", "id": 2})
        self.db_map.add_wide_relationship_classes(
            {"id": 3, "name": "dog__fish", "object_class_id_list": [1, 2]},
            {"id": 4, "name": "fish__dog", "object_class_id_list": [2, 1]},
        )
        ids, intgr_error_log = self.db_map.update_wide_relationship_classes(
            {"id": 3, "name": "god__octopus"}, {"id": 4, "name": "octopus__dog"}
        )
        sq = self.db_map.wide_relationship_class_sq
        rel_clss = {x.id: x.name for x in self.db_map.query(sq).filter(sq.c.id.in_(ids))}
        self.assertEqual(intgr_error_log, [])
        self.assertEqual(rel_clss[3], "god__octopus")
        self.assertEqual(rel_clss[4], "octopus__dog")

    def test_update_relationships(self):
        """Test that updating relationships works."""
        self.db_map.add_object_classes({"name": "fish", "id": 1}, {"name": "dog", "id": 2})
        self.db_map.add_wide_relationship_classes({"name": "fish__dog", "id": 3, "object_class_id_list": [1, 2]})
        self.db_map.add_objects(
            {"name": "nemo", "id": 1, "class_id": 1},
            {"name": "pluto", "id": 2, "class_id": 2},
            {"name": "scooby", "id": 3, "class_id": 2},
        )
        self.db_map.add_wide_relationships({"id": 4, "name": "nemo__pluto", "class_id": 3, "object_id_list": [1, 2]})
        ids, intgr_error_log = self.db_map.update_wide_relationships(
            {"id": 4, "name": "nemo__scooby", "object_id_list": [1, 3]}
        )
        sq = self.db_map.wide_relationship_sq
        rels = {
            x.id: {"name": x.name, "object_id_list": x.object_id_list}
            for x in self.db_map.query(sq).filter(sq.c.id.in_(ids))
        }
        self.assertEqual(intgr_error_log, [])
        self.assertEqual(rels[4]["name"], "nemo__scooby")
        self.assertEqual(rels[4]["object_id_list"], "1,3")


if __name__ == "__main__":
    unittest.main()
