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
import os.path
from tempfile import TemporaryDirectory
import unittest
from unittest import mock
from sqlalchemy.engine.url import make_url
from sqlalchemy.util import KeyedTuple
from spinedb_api.diff_db_mapping import DiffDatabaseMapping
from spinedb_api.exception import SpineIntegrityError
from spinedb_api.helpers import create_new_spine_database
from spinedb_api import import_functions


def create_query_wrapper(db_map):
    def query_wrapper(*args, orig_query=db_map.query, **kwargs):
        arg = args[0]
        if isinstance(arg, mock.Mock):
            return arg.value
        return orig_query(*args, **kwargs)

    return query_wrapper


def create_diff_db_map(directory):
    file_name = os.path.join(directory, "test_DiffDatabaseMapping.json")
    db_url = "sqlite:///" + file_name
    create_new_spine_database(db_url)
    return DiffDatabaseMapping(db_url, username="UnitTest")


class TestDiffDatabaseMappingConstruction(unittest.TestCase):
    _db_url = None
    _temp_dir = None

    @classmethod
    def setUpClass(cls):
        cls._temp_dir = TemporaryDirectory()
        cls._db_url = "sqlite:///" + os.path.join(cls._temp_dir.name, "test_database_mapping.sqlite")
        create_new_spine_database(cls._db_url)
        db_map = DiffDatabaseMapping(cls._db_url)
        try:
            db_map.add_tools({"name": "object_activity_control", "id": 1})
            db_map.commit_session("Add tool.")
        finally:
            db_map.connection.close()

    def test_construction_with_filters(self):
        db_url = self._db_url + "?spinedbfilter=fltr1&spinedbfilter=fltr2"
        with mock.patch("spinedb_api.diff_db_mapping.apply_filter_stack") as mock_apply:
            with mock.patch(
                "spinedb_api.diff_db_mapping.load_filters", return_value=[{"fltr1": "config1", "fltr2": "config2"}]
            ) as mock_load:
                db_map = DiffDatabaseMapping(db_url)
                db_map.connection.close()
                mock_load.assert_called_once_with(["fltr1", "fltr2"])
                mock_apply.assert_called_once_with(db_map, [{"fltr1": "config1", "fltr2": "config2"}])

    def test_construction_with_sqlalchemy_url_and_filters(self):
        db_url = self._db_url + "?spinedbfilter=fltr1&spinedbfilter=fltr2"
        sa_url = make_url(db_url)
        with mock.patch("spinedb_api.diff_db_mapping.apply_filter_stack") as mock_apply:
            with mock.patch(
                "spinedb_api.diff_db_mapping.load_filters", return_value=[{"fltr1": "config1", "fltr2": "config2"}]
            ) as mock_load:
                db_map = DiffDatabaseMapping(sa_url)
                db_map.connection.close()
                mock_load.assert_called_once_with(["fltr1", "fltr2"])
                mock_apply.assert_called_once_with(db_map, [{"fltr1": "config1", "fltr2": "config2"}])

    def test_shorthand_filter_query_works(self):
        url = self._db_url + "?spinedbfilter=cfg%3Atool%3Aobject_activity_control"
        try:
            db_map = DiffDatabaseMapping(url)
        except:
            self.fail("DiffDatabaseMapping.__init__() should not raise.")
        else:
            db_map.connection.close()


class TestDiffDatabaseMappingRemove(unittest.TestCase):
    def test_cascade_remove_relationship(self):
        """Test adding and removing a relationship and committing"""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
            db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 2})
            ids, _ = db_map.add_wide_relationships({"name": "remove_me", "class_id": 3, "object_id_list": [1, 2]})
            db_map.cascade_remove_items(relationship=ids)
            self.assertEqual(len(db_map.query(db_map.wide_relationship_sq).all()), 0)
            db_map.commit_session("delete")
            self.assertEqual(len(db_map.query(db_map.wide_relationship_sq).all()), 0)
            db_map.connection.close()

    def test_cascade_remove_relationship_from_commited_session(self):
        """Test removing a relationship from a committed session"""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
            db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 2})
            ids, _ = db_map.add_wide_relationships({"name": "remove_me", "class_id": 3, "object_id_list": [1, 2]})
            db_map.commit_session("add")
            self.assertEqual(len(db_map.query(db_map.wide_relationship_sq).all()), 1)
            db_map.cascade_remove_items(relationship=ids)
            self.assertEqual(len(db_map.query(db_map.wide_relationship_sq).all()), 0)
            db_map.commit_session("")
            self.assertEqual(len(db_map.query(db_map.wide_relationship_sq).all()), 0)
            db_map.connection.close()

    def test_remove_object(self):
        """Test adding and removing an object and committing"""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            ids, _ = db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 2})
            db_map.remove_items(object=ids)
            self.assertEqual(len(db_map.query(db_map.wide_relationship_sq).all()), 0)
            db_map.commit_session("delete")
            self.assertEqual(len(db_map.query(db_map.wide_relationship_sq).all()), 0)
            db_map.connection.close()

    def test_remove_object_from_commited_session(self):
        """Test removing an object from a committed session"""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            ids, _ = db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 2})
            db_map.commit_session("add")
            self.assertEqual(len(db_map.query(db_map.object_sq).all()), 2)
            db_map.remove_items(object=ids)
            self.assertEqual(len(db_map.query(db_map.object_sq).all()), 0)
            db_map.commit_session("")
            self.assertEqual(len(db_map.query(db_map.object_sq).all()), 0)
            db_map.connection.close()

    def test_remove_entity_group(self):
        """Test adding and removing an entity group and committing"""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1})
            db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 1})
            ids, _ = db_map.add_entity_groups({"entity_id": 1, "entity_class_id": 1, "member_id": 2})
            db_map.remove_items(entity_group=ids)
            self.assertEqual(len(db_map.query(db_map.entity_group_sq).all()), 0)
            db_map.commit_session("delete")
            self.assertEqual(len(db_map.query(db_map.entity_group_sq).all()), 0)
            db_map.connection.close()

    def test_remove_entity_group_from_committed_session(self):
        """Test removing an entity group from a committed session"""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1})
            db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 1})
            ids, _ = db_map.add_entity_groups({"entity_id": 1, "entity_class_id": 1, "member_id": 2})
            db_map.commit_session("add")
            self.assertEqual(len(db_map.query(db_map.entity_group_sq).all()), 1)
            db_map.remove_items(entity_group=ids)
            self.assertEqual(len(db_map.query(db_map.entity_group_sq).all()), 0)
            db_map.commit_session("delete")
            self.assertEqual(len(db_map.query(db_map.entity_group_sq).all()), 0)
            db_map.connection.close()

    def test_cascade_remove_relationship_class(self):
        """Test adding and removing a relationship class and committing"""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            ids, _ = db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
            db_map.cascade_remove_items(relationship_class=ids)
            self.assertEqual(len(db_map.query(db_map.wide_relationship_class_sq).all()), 0)
            db_map.commit_session("delete")
            self.assertEqual(len(db_map.query(db_map.wide_relationship_class_sq).all()), 0)
            db_map.connection.close()

    def test_cascade_remove_relationship_class_from_committed_session(self):
        """Test removing a relationship class from a committed session"""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            ids, _ = db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
            db_map.commit_session("add")
            self.assertEqual(len(db_map.query(db_map.wide_relationship_class_sq).all()), 1)
            db_map.cascade_remove_items(relationship_class=ids)
            self.assertEqual(len(db_map.query(db_map.wide_relationship_class_sq).all()), 0)
            db_map.commit_session("")
            self.assertEqual(len(db_map.query(db_map.wide_relationship_class_sq).all()), 0)
            db_map.connection.close()

    def test_remove_object_class(self):
        """Test adding and removing an object class and committing"""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            ids, _ = db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            db_map.remove_items(object_class=ids)
            self.assertEqual(len(db_map.query(db_map.object_class_sq).all()), 0)
            db_map.commit_session("delete")
            self.assertEqual(len(db_map.query(db_map.object_class_sq).all()), 0)
            db_map.connection.close()

    def test_remove_object_class_from_committed_session(self):
        """Test removing an object class from a committed session"""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            ids, _ = db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            db_map.commit_session("add")
            self.assertEqual(len(db_map.query(db_map.object_class_sq).all()), 2)
            db_map.remove_items(object_class=ids)
            self.assertEqual(len(db_map.query(db_map.object_class_sq).all()), 0)
            db_map.commit_session("")
            self.assertEqual(len(db_map.query(db_map.object_class_sq).all()), 0)
            db_map.connection.close()

    def test_remove_parameter_value(self):
        """Test adding and removing a parameter value and committing"""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1}, strict=True)
            db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, strict=True)
            db_map.add_parameter_definitions({"name": "param", "id": 1, "object_class_id": 1}, strict=True)
            db_map.add_parameter_values(
                {
                    "value": "0",
                    "id": 1,
                    "parameter_definition_id": 1,
                    "object_id": 1,
                    "object_class_id": 1,
                    "alternative_id": 1,
                },
                strict=True,
            )
            self.assertEqual(len(db_map.query(db_map.parameter_value_sq).all()), 1)
            db_map.remove_items(parameter_value=[1])
            self.assertEqual(len(db_map.query(db_map.parameter_value_sq).all()), 0)
            db_map.commit_session("delete")
            self.assertEqual(len(db_map.query(db_map.parameter_value_sq).all()), 0)
            db_map.connection.close()

    def test_remove_parameter_value_from_committed_session(self):
        """Test adding and committing a parameter value and then removing it"""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1}, strict=True)
            db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, strict=True)
            db_map.add_parameter_definitions({"name": "param", "id": 1, "object_class_id": 1}, strict=True)
            db_map.add_parameter_values(
                {
                    "value": "0",
                    "id": 1,
                    "parameter_definition_id": 1,
                    "object_id": 1,
                    "object_class_id": 1,
                    "alternative_id": 1,
                },
                strict=True,
            )
            db_map.commit_session("add")
            self.assertEqual(len(db_map.query(db_map.parameter_value_sq).all()), 1)
            db_map.remove_items(parameter_value=[1])
            self.assertEqual(len(db_map.query(db_map.parameter_value_sq).all()), 0)
            db_map.commit_session("delete")
            self.assertEqual(len(db_map.query(db_map.parameter_value_sq).all()), 0)
            db_map.connection.close()

    def test_cascade_remove_object_removes_parameter_value_as_well(self):
        """Test adding and removing a parameter value and committing"""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1}, strict=True)
            db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, strict=True)
            db_map.add_parameter_definitions({"name": "param", "id": 1, "object_class_id": 1}, strict=True)
            db_map.add_parameter_values(
                {
                    "value": "0",
                    "id": 1,
                    "parameter_definition_id": 1,
                    "object_id": 1,
                    "object_class_id": 1,
                    "alternative_id": 1,
                },
                strict=True,
            )
            self.assertEqual(len(db_map.query(db_map.parameter_value_sq).all()), 1)
            db_map.cascade_remove_items(object={1})
            self.assertEqual(len(db_map.query(db_map.parameter_value_sq).all()), 0)
            db_map.commit_session("delete")
            self.assertEqual(len(db_map.query(db_map.parameter_value_sq).all()), 0)
            db_map.connection.close()

    def test_cascade_remove_object_from_committed_session_removes_parameter_value_as_well(self):
        """Test adding and committing a paramater value and then removing it"""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1}, strict=True)
            db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, strict=True)
            db_map.add_parameter_definitions({"name": "param", "id": 1, "object_class_id": 1}, strict=True)
            db_map.add_parameter_values(
                {
                    "value": "0",
                    "id": 1,
                    "parameter_definition_id": 1,
                    "object_id": 1,
                    "object_class_id": 1,
                    "alternative_id": 1,
                },
                strict=True,
            )
            db_map.commit_session("add")
            self.assertEqual(len(db_map.query(db_map.parameter_value_sq).all()), 1)
            db_map.cascade_remove_items(object={1})
            self.assertEqual(len(db_map.query(db_map.parameter_value_sq).all()), 0)
            db_map.commit_session("delete")
            self.assertEqual(len(db_map.query(db_map.parameter_value_sq).all()), 0)
            db_map.connection.close()


class TestDiffDatabaseMappingAdd(unittest.TestCase):
    def test_add_and_retrieve_many_objects(self):
        """Tests add many objects into db and retrieving them."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            ids, _ = db_map.add_object_classes({"name": "testclass"})
            class_id = next(iter(ids))
            added = db_map.add_objects(*[{"name": str(i), "class_id": class_id} for i in range(1001)])[0]
            self.assertEqual(len(added), 1001)
            db_map.commit_session("test_commit")
            self.assertEqual(db_map.query(db_map.entity_sq).count(), 1001)
            db_map.connection.close()

    def test_add_object_classes(self):
        """Test that adding object classes works."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "fish"}, {"name": "dog"})
            diff_table = db_map._diff_table("entity_class")
            object_classes = db_map.query(diff_table).filter(diff_table.c.type_id == db_map.object_class_type).all()
            self.assertEqual(len(object_classes), 2)
            self.assertEqual(object_classes[0].name, "fish")
            self.assertEqual(object_classes[1].name, "dog")
            db_map.connection.close()

    def test_add_object_class_with_invalid_name(self):
        """Test that adding object classes with empty name raises error"""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            with self.assertRaises(SpineIntegrityError):
                db_map.add_object_classes({"name": ""}, strict=True)
            db_map.connection.close()

    def test_add_object_classes_with_same_name(self):
        """Test that adding two object classes with the same name only adds one of them."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "fish"}, {"name": "fish"})
            diff_table = db_map._diff_table("entity_class")
            object_classes = db_map.query(diff_table).filter(diff_table.c.type_id == db_map.object_class_type).all()
            self.assertEqual(len(object_classes), 1)
            self.assertEqual(object_classes[0].name, "fish")
            db_map.connection.close()

    def test_add_object_class_with_same_name_as_existing_one(self):
        """Test that adding an object class with an already taken name raises an integrity error."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            query_wrapper = create_query_wrapper(db_map)
            with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
                DiffDatabaseMapping, "object_class_sq"
            ) as mock_object_class_sq:
                mock_query.side_effect = query_wrapper
                mock_object_class_sq.value = [KeyedTuple([1, "fish"], labels=["id", "name"])]
                with self.assertRaises(SpineIntegrityError):
                    db_map.add_object_classes({"name": "fish"}, strict=True)
            db_map.connection.close()

    def test_add_objects(self):
        """Test that adding objects works."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            query_wrapper = create_query_wrapper(db_map)
            with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
                DiffDatabaseMapping, "object_class_sq"
            ) as mock_object_class_sq:
                mock_query.side_effect = query_wrapper
                mock_object_class_sq.value = [KeyedTuple([1, "fish"], labels=["id", "name"])]
                db_map.add_objects({"name": "nemo", "class_id": 1}, {"name": "dory", "class_id": 1})
            diff_table = db_map._diff_table("entity")
            objects = db_map.query(diff_table).filter(diff_table.c.type_id == db_map.object_entity_type).all()
            self.assertEqual(len(objects), 2)
            self.assertEqual(objects[0].name, "nemo")
            self.assertEqual(objects[0].class_id, 1)
            self.assertEqual(objects[1].name, "dory")
            self.assertEqual(objects[1].class_id, 1)
            db_map.connection.close()

    def test_add_object_with_invalid_name(self):
        """Test that adding object classes with empty name raises error"""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "fish"})
            with self.assertRaises(SpineIntegrityError):
                db_map.add_objects({"name": "", "class_id": 1}, strict=True)
            db_map.connection.close()

    def test_add_objects_with_same_name(self):
        """Test that adding two objects with the same name only adds one of them."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            query_wrapper = create_query_wrapper(db_map)
            with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
                DiffDatabaseMapping, "object_class_sq"
            ) as mock_object_class_sq:
                mock_query.side_effect = query_wrapper
                mock_object_class_sq.value = [KeyedTuple([1, "fish"], labels=["id", "name"])]
                db_map.add_objects({"name": "nemo", "class_id": 1}, {"name": "nemo", "class_id": 1})
            diff_table = db_map._diff_table("entity")
            objects = db_map.query(diff_table).filter(diff_table.c.type_id == db_map.object_entity_type).all()
            self.assertEqual(len(objects), 1)
            self.assertEqual(objects[0].name, "nemo")
            self.assertEqual(objects[0].class_id, 1)
            db_map.connection.close()

    def test_add_object_with_same_name_as_existing_one(self):
        """Test that adding an object with an already taken name raises an integrity error."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            query_wrapper = create_query_wrapper(db_map)
            with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
                DiffDatabaseMapping, "object_class_sq"
            ) as mock_object_class_sq, mock.patch.object(DiffDatabaseMapping, "object_sq") as mock_object_sq:
                mock_query.side_effect = query_wrapper
                mock_object_class_sq.return_value = [KeyedTuple([1, "fish"], labels=["id", "name"])]
                mock_object_sq.return_value = [KeyedTuple([1, 1, "nemo"], labels=["id", "class_id", "name"])]
                with self.assertRaises(SpineIntegrityError):
                    db_map.add_objects({"name": "nemo", "class_id": 1}, strict=True)
            db_map.connection.close()

    def test_add_object_with_invalid_class(self):
        """Test that adding an object with a non existing class raises an integrity error."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            query_wrapper = create_query_wrapper(db_map)
            with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
                DiffDatabaseMapping, "object_class_sq"
            ) as mock_object_class_sq:
                mock_query.side_effect = query_wrapper
                mock_object_class_sq.return_value = [KeyedTuple([1, "fish"], labels=["id", "name"])]
                with self.assertRaises(SpineIntegrityError):
                    db_map.add_objects({"name": "pluto", "class_id": 2}, strict=True)
            db_map.connection.close()

    def test_add_relationship_classes(self):
        """Test that adding relationship classes works."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            db_map.add_wide_relationship_classes(
                {"name": "rc1", "object_class_id_list": [1, 2]}, {"name": "rc2", "object_class_id_list": [2, 1]}
            )
            diff_table = db_map._diff_table("relationship_entity_class")
            rel_ent_clss = db_map.query(diff_table).all()
            diff_table = db_map._diff_table("entity_class")
            rel_clss = db_map.query(diff_table).filter(diff_table.c.type_id == db_map.relationship_class_type).all()
            self.assertEqual(len(rel_ent_clss), 4)
            self.assertEqual(rel_clss[0].name, "rc1")
            self.assertEqual(rel_ent_clss[0].member_class_id, 1)
            self.assertEqual(rel_ent_clss[1].member_class_id, 2)
            self.assertEqual(rel_clss[1].name, "rc2")
            self.assertEqual(rel_ent_clss[2].member_class_id, 2)
            self.assertEqual(rel_ent_clss[3].member_class_id, 1)
            db_map.connection.close()

    def test_add_relationship_classes_with_invalid_name(self):
        """Test that adding object classes with empty name raises error"""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "fish"})
            with self.assertRaises(SpineIntegrityError):
                db_map.add_wide_relationship_classes({"name": "", "object_class_id_list": [1]}, strict=True)
            db_map.connection.close()

    def test_add_relationship_classes_with_same_name(self):
        """Test that adding two relationship classes with the same name only adds one of them."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            db_map.add_wide_relationship_classes(
                {"name": "rc1", "object_class_id_list": [1, 2]}, {"name": "rc1", "object_class_id_list": [1, 2]}
            )
            diff_table = db_map._diff_table("relationship_entity_class")
            relationship_members = db_map.query(diff_table).all()
            diff_table = db_map._diff_table("entity_class")
            relationships = (
                db_map.query(diff_table).filter(diff_table.c.type_id == db_map.relationship_class_type).all()
            )
            self.assertEqual(len(relationship_members), 2)
            self.assertEqual(len(relationships), 1)
            self.assertEqual(relationships[0].name, "rc1")
            self.assertEqual(relationship_members[0].member_class_id, 1)
            self.assertEqual(relationship_members[1].member_class_id, 2)
            db_map.connection.close()

    def test_add_relationship_class_with_same_name_as_existing_one(self):
        """Test that adding a relationship class with an already taken name raises an integrity error."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            query_wrapper = create_query_wrapper(db_map)
            with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
                DiffDatabaseMapping, "object_class_sq"
            ) as mock_object_class_sq, mock.patch.object(
                DiffDatabaseMapping, "wide_relationship_class_sq"
            ) as mock_wide_rel_cls_sq:
                mock_query.side_effect = query_wrapper
                mock_object_class_sq.return_value = [
                    KeyedTuple([1, "fish"], labels=["id", "name"]),
                    KeyedTuple([2, "dog"], labels=["id", "name"]),
                ]
                mock_wide_rel_cls_sq.return_value = [
                    KeyedTuple([1, "1,2", "fish__dog"], labels=["id", "object_class_id_list", "name"])
                ]
                with self.assertRaises(SpineIntegrityError):
                    db_map.add_wide_relationship_classes(
                        {"name": "fish__dog", "object_class_id_list": [1, 2]}, strict=True
                    )
            db_map.connection.close()

    def test_add_relationship_class_with_invalid_object_class(self):
        """Test that adding a relationship class with a non existing object class raises an integrity error."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            query_wrapper = create_query_wrapper(db_map)
            with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
                DiffDatabaseMapping, "object_class_sq"
            ) as mock_object_class_sq, mock.patch.object(DiffDatabaseMapping, "wide_relationship_class_sq"):
                mock_query.side_effect = query_wrapper
                mock_object_class_sq.return_value = [KeyedTuple([1, "fish"], labels=["id", "name"])]
                with self.assertRaises(SpineIntegrityError):
                    db_map.add_wide_relationship_classes(
                        {"name": "fish__dog", "object_class_id_list": [1, 2]}, strict=True
                    )
            db_map.connection.close()

    def test_add_relationships(self):
        """Test that adding relationships works."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
            db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 2})
            db_map.add_wide_relationships({"name": "nemo__pluto", "class_id": 3, "object_id_list": [1, 2]})
            diff_table = db_map._diff_table("relationship_entity")
            rel_ents = db_map.query(diff_table).all()
            diff_table = db_map._diff_table("entity")
            relationships = (
                db_map.query(diff_table).filter(diff_table.c.type_id == db_map.relationship_entity_type).all()
            )
            self.assertEqual(len(rel_ents), 2)
            self.assertEqual(len(relationships), 1)
            self.assertEqual(relationships[0].name, "nemo__pluto")
            self.assertEqual(rel_ents[0].entity_class_id, 3)
            self.assertEqual(rel_ents[0].member_id, 1)
            self.assertEqual(rel_ents[1].entity_class_id, 3)
            self.assertEqual(rel_ents[1].member_id, 2)
            db_map.connection.close()

    def test_add_relationship_with_invalid_name(self):
        """Test that adding object classes with empty name raises error"""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1"}, strict=True)
            db_map.add_wide_relationship_classes({"name": "rc1", "object_class_id_list": [1]}, strict=True)
            db_map.add_objects({"name": "o1", "class_id": 1}, strict=True)
            with self.assertRaises(SpineIntegrityError):
                db_map.add_wide_relationships({"name": "", "class_id": 1, "object_id_list": [1]}, strict=True)
            db_map.connection.close()

    def test_add_identical_relationships(self):
        """Test that adding two relationships with the same class and same objects only adds the first one.
        """
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
            db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 2})
            db_map.add_wide_relationships(
                {"name": "nemo__pluto", "class_id": 3, "object_id_list": [1, 2]},
                {"name": "nemo__pluto_duplicate", "class_id": 3, "object_id_list": [1, 2]},
            )
            diff_table = db_map._diff_table("relationship")
            relationships = db_map.query(diff_table).all()
            self.assertEqual(len(relationships), 1)
            db_map.connection.close()

    def test_add_relationship_identical_to_existing_one(self):
        """Test that adding a relationship with the same class and same objects as an existing one
        raises an integrity error.
        """
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            query_wrapper = create_query_wrapper(db_map)
            with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
                DiffDatabaseMapping, "object_sq"
            ) as mock_object_sq, mock.patch.object(
                DiffDatabaseMapping, "wide_relationship_class_sq"
            ) as mock_wide_rel_cls_sq, mock.patch.object(
                DiffDatabaseMapping, "wide_relationship_sq"
            ) as mock_wide_rel_sq:
                mock_query.side_effect = query_wrapper
                mock_object_sq.return_value = [
                    KeyedTuple([1, 10, "nemo"], labels=["id", "class_id", "name"]),
                    KeyedTuple([2, 20, "pluto"], labels=["id", "class_id", "name"]),
                ]
                mock_wide_rel_cls_sq.return_value = [
                    KeyedTuple([1, "10,20", "fish__dog"], labels=["id", "object_class_id_list", "name"])
                ]
                mock_wide_rel_sq.return_value = [
                    KeyedTuple([1, 1, "1,2", "nemo__pluto"], labels=["id", "class_id", "object_id_list", "name"])
                ]
                with self.assertRaises(SpineIntegrityError):
                    db_map.add_wide_relationships(
                        {"name": "nemoy__plutoy", "class_id": 1, "object_id_list": [1, 2]}, strict=True
                    )
            db_map.connection.close()

    def test_add_relationship_with_invalid_class(self):
        """Test that adding a relationship with an invalid class raises an integrity error.
        """
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            query_wrapper = create_query_wrapper(db_map)
            with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
                DiffDatabaseMapping, "object_sq"
            ) as mock_object_sq, mock.patch.object(
                DiffDatabaseMapping, "wide_relationship_class_sq"
            ) as mock_wide_rel_cls_sq, mock.patch.object(
                DiffDatabaseMapping, "wide_relationship_sq"
            ):
                mock_query.side_effect = query_wrapper
                mock_object_sq.return_value = [
                    KeyedTuple([1, 10, "nemo"], labels=["id", "class_id", "name"]),
                    KeyedTuple([2, 20, "pluto"], labels=["id", "class_id", "name"]),
                ]
                mock_wide_rel_cls_sq.return_value = [
                    KeyedTuple([1, "10,20", "fish__dog"], labels=["id", "object_class_id_list", "name"])
                ]
                with self.assertRaises(SpineIntegrityError):
                    db_map.add_wide_relationships(
                        {"name": "nemo__pluto", "class_id": 2, "object_id_list": [1, 2]}, strict=True
                    )
            db_map.connection.close()

    def test_add_relationship_with_invalid_object(self):
        """Test that adding a relationship with an invalid object raises an integrity error.
        """
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            query_wrapper = create_query_wrapper(db_map)
            with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
                DiffDatabaseMapping, "object_sq"
            ) as mock_object_sq, mock.patch.object(
                DiffDatabaseMapping, "wide_relationship_class_sq"
            ) as mock_wide_rel_cls_sq, mock.patch.object(
                DiffDatabaseMapping, "wide_relationship_sq"
            ):
                mock_query.side_effect = query_wrapper
                mock_object_sq.return_value = [
                    KeyedTuple([1, 10, "nemo"], labels=["id", "class_id", "name"]),
                    KeyedTuple([2, 20, "pluto"], labels=["id", "class_id", "name"]),
                ]
                mock_wide_rel_cls_sq.return_value = [
                    KeyedTuple([1, "10,20", "fish__dog"], labels=["id", "object_class_id_list", "name"])
                ]
                with self.assertRaises(SpineIntegrityError):
                    db_map.add_wide_relationships(
                        {"name": "nemo__pluto", "class_id": 1, "object_id_list": [1, 3]}, strict=True
                    )
            db_map.connection.close()

    def test_add_entity_groups(self):
        """Test that adding group entities works."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1})
            db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 1})
            db_map.add_entity_groups({"entity_id": 1, "entity_class_id": 1, "member_id": 2})
            diff_table = db_map._diff_table("entity_group")
            entity_groups = db_map.query(diff_table).all()
            self.assertEqual(len(entity_groups), 1)
            self.assertEqual(entity_groups[0].entity_id, 1)
            self.assertEqual(entity_groups[0].entity_class_id, 1)
            self.assertEqual(entity_groups[0].member_id, 2)
            db_map.connection.close()

    def test_add_entity_groups_with_invalid_class(self):
        """Test that adding group entities with an invalid class fails."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1})
            db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 1})
            with self.assertRaises(SpineIntegrityError):
                db_map.add_entity_groups({"entity_id": 1, "entity_class_id": 2, "member_id": 2}, strict=True)
            db_map.connection.close()

    def test_add_entity_groups_with_invalid_entity(self):
        """Test that adding group entities with an invalid entity fails."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1})
            db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 1})
            with self.assertRaises(SpineIntegrityError):
                db_map.add_entity_groups({"entity_id": 3, "entity_class_id": 2, "member_id": 2}, strict=True)
            db_map.connection.close()

    def test_add_entity_groups_with_invalid_member(self):
        """Test that adding group entities with an invalid member fails."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1})
            db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 1})
            with self.assertRaises(SpineIntegrityError):
                db_map.add_entity_groups({"entity_id": 1, "entity_class_id": 2, "member_id": 3}, strict=True)
            db_map.connection.close()

    def test_add_repeated_entity_groups(self):
        """Test that adding repeated group entities fails."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1})
            db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 1})
            db_map.add_entity_groups({"entity_id": 1, "entity_class_id": 2, "member_id": 2})
            with self.assertRaises(SpineIntegrityError):
                db_map.add_entity_groups({"entity_id": 1, "entity_class_id": 2, "member_id": 2}, strict=True)
            db_map.connection.close()

    def test_add_parameter_definitions(self):
        """Test that adding parameter definitions works."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
            db_map.add_parameter_definitions(
                {"name": "color", "object_class_id": 1, "description": "test1"},
                {"name": "relative_speed", "relationship_class_id": 3, "description": "test2"},
            )
            diff_table = db_map._diff_table("parameter_definition")
            parameter_definitions = db_map.query(diff_table).all()
            self.assertEqual(len(parameter_definitions), 2)
            self.assertEqual(parameter_definitions[0].name, "color")
            self.assertEqual(parameter_definitions[0].entity_class_id, 1)
            self.assertEqual(parameter_definitions[0].description, "test1")
            self.assertEqual(parameter_definitions[1].name, "relative_speed")
            self.assertEqual(parameter_definitions[1].entity_class_id, 3)
            self.assertEqual(parameter_definitions[1].description, "test2")
            db_map.connection.close()

    def test_add_parameter_with_invalid_name(self):
        """Test that adding object classes with empty name raises error"""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1"}, strict=True)
            with self.assertRaises(SpineIntegrityError):
                db_map.add_parameter_definitions({"name": "", "object_class_id": 1}, strict=True)
            db_map.connection.close()

    def test_add_parameter_definitions_with_same_name(self):
        """Test that adding two parameter_definitions with the same name adds both of them."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
            db_map.add_parameter_definitions(
                {"name": "color", "object_class_id": 1}, {"name": "color", "relationship_class_id": 3}
            )
            diff_table = db_map._diff_table("parameter_definition")
            parameter_definitions = db_map.query(diff_table).all()
            self.assertEqual(len(parameter_definitions), 2)
            self.assertEqual(parameter_definitions[0].name, "color")
            self.assertEqual(parameter_definitions[1].name, "color")
            self.assertEqual(parameter_definitions[0].entity_class_id, 1)
            db_map.connection.close()

    def test_add_parameter_with_same_name_as_existing_one(self):
        """Test that adding parameter_definitions with an already taken name raises and integrity error."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
            db_map.add_parameter_definitions(
                {"name": "color", "object_class_id": 1}, {"name": "color", "relationship_class_id": 3}
            )
            with self.assertRaises(SpineIntegrityError):
                db_map.add_parameter_definitions({"name": "color", "object_class_id": 1}, strict=True)
            db_map.connection.close()

    def test_add_parameter_with_invalid_class(self):
        """Test that adding parameter_definitions with an invalid (object or relationship) class raises and integrity error."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
            with self.assertRaises(SpineIntegrityError):
                db_map.add_parameter_definitions({"name": "color", "object_class_id": 3}, strict=True)
            with self.assertRaises(SpineIntegrityError):
                db_map.add_parameter_definitions({"name": "color", "relationship_class_id": 1}, strict=True)
            db_map.connection.close()

    def test_add_parameter_for_both_object_and_relationship_class(self):
        """Test that adding parameter_definitions associated to both and object and relationship class
        raises and integrity error."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            query_wrapper = create_query_wrapper(db_map)
            with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
                DiffDatabaseMapping, "object_class_sq"
            ) as mock_object_class_sq, mock.patch.object(
                DiffDatabaseMapping, "wide_relationship_class_sq"
            ) as mock_wide_rel_cls_sq:
                mock_query.side_effect = query_wrapper
                mock_object_class_sq.value = [KeyedTuple([1, "fish"], labels=["id", "name"])]
                mock_wide_rel_cls_sq.value = [
                    KeyedTuple([10, "1,2", "fish__dog"], labels=["id", "object_class_id_list", "name"])
                ]
                with self.assertRaises(SpineIntegrityError):
                    db_map.add_parameter_definitions(
                        {"name": "color", "object_class_id": 1, "relationship_class_id": 10}, strict=True
                    )
            db_map.connection.close()

    def test_add_parameter_values(self):
        """Test that adding parameter values works."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_functions.import_object_classes(db_map, ["fish", "dog"])
            import_functions.import_relationship_classes(db_map, [("fish_dog", ["fish", "dog"])])
            import_functions.import_objects(db_map, [("fish", "nemo"), ("dog", "pluto")])
            import_functions.import_relationships(db_map, [("fish_dog", ("nemo", "pluto"))])
            import_functions.import_object_parameters(db_map, [("fish", "color")])
            import_functions.import_relationship_parameters(db_map, [("fish_dog", "rel_speed")])
            color_id = (
                db_map.parameter_definition_list().filter(db_map.parameter_definition_sq.c.name == "color").first().id
            )
            rel_speed_id = (
                db_map.parameter_definition_list()
                .filter(db_map.parameter_definition_sq.c.name == "rel_speed")
                .first()
                .id
            )
            nemo_row = db_map.object_list().filter(db_map.entity_sq.c.name == "nemo").first()
            nemo__pluto_row = db_map.wide_relationship_list().filter().first()
            db_map.add_parameter_values(
                {
                    "parameter_definition_id": color_id,
                    "entity_id": nemo_row.id,
                    "entity_class_id": nemo_row.class_id,
                    "value": '"orange"',
                    "alternative_id": 1,
                },
                {
                    "parameter_definition_id": rel_speed_id,
                    "entity_id": nemo__pluto_row.id,
                    "entity_class_id": nemo__pluto_row.class_id,
                    "value": "125",
                    "alternative_id": 1,
                },
            )
            diff_table = db_map._diff_table("parameter_value")
            parameter_values = db_map.query(diff_table).all()
            self.assertEqual(len(parameter_values), 2)
            self.assertEqual(parameter_values[0].parameter_definition_id, 1)
            self.assertEqual(parameter_values[0].entity_id, 1)
            self.assertEqual(parameter_values[0].value, '"orange"')
            self.assertEqual(parameter_values[1].parameter_definition_id, 2)
            self.assertEqual(parameter_values[1].entity_id, 3)
            self.assertEqual(parameter_values[1].value, "125")
            db_map.connection.close()

    def test_add_parameter_value_for_both_object_and_relationship(self):
        """Test that adding a parameter value for both an object and a relationship raises an
        integrity error."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            query_wrapper = create_query_wrapper(db_map)
            with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
                DiffDatabaseMapping, "object_sq"
            ) as mock_object_sq, mock.patch.object(
                DiffDatabaseMapping, "wide_relationship_sq"
            ) as mock_wide_rel_sq, mock.patch.object(
                DiffDatabaseMapping, "parameter_definition_sq"
            ) as mock_parameter_definition_sq:
                mock_query.side_effect = query_wrapper
                mock_object_sq.return_value = [
                    KeyedTuple([1, 10, "nemo"], labels=["id", "class_id", "name"]),
                    KeyedTuple([2, 20, "pluto"], labels=["id", "class_id", "name"]),
                ]
                mock_wide_rel_sq.return_value = [
                    KeyedTuple([1, 100, "1,2", "nemo__pluto"], labels=["id", "class_id", "object_id_list", "name"])
                ]
                mock_parameter_definition_sq.return_value = [
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
                    db_map.add_parameter_values(
                        {"parameter_definition_id": 1, "object_id": 1, "relationship_id": 1, "value": "orange"},
                        strict=True,
                    )
            db_map.connection.close()

    def test_add_parameter_value_with_invalid_object_or_relationship(self):
        """Test that adding a parameter value with an invalid object or relationship raises an
        integrity error."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            query_wrapper = create_query_wrapper(db_map)
            with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
                DiffDatabaseMapping, "object_sq"
            ) as mock_object_sq, mock.patch.object(
                DiffDatabaseMapping, "wide_relationship_sq"
            ) as mock_wide_rel_sq, mock.patch.object(
                DiffDatabaseMapping, "parameter_definition_sq"
            ) as mock_parameter_definition_sq:
                mock_query.side_effect = query_wrapper
                mock_object_sq.return_value = [
                    KeyedTuple([1, 10, "nemo"], labels=["id", "class_id", "name"]),
                    KeyedTuple([2, 20, "pluto"], labels=["id", "class_id", "name"]),
                ]
                mock_wide_rel_sq.return_value = [
                    KeyedTuple([1, 100, "1,2", "nemo__pluto"], labels=["id", "class_id", "object_id_list", "name"])
                ]
                mock_parameter_definition_sq.return_value = [
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
                    db_map.add_parameter_values(
                        {"parameter_definition_id": 1, "object_id": 3, "value": "orange"}, strict=True
                    )
                with self.assertRaises(SpineIntegrityError):
                    db_map.add_parameter_values(
                        {"parameter_definition_id": 2, "relationship_id": 2, "value": "125"}, strict=True
                    )
            db_map.connection.close()

    def test_add_parameter_value_with_object_or_relationship_of_invalid_class(self):
        """Test that adding a parameter value with an object or relationship invalid for
        the parameter class raises an integrity error."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            query_wrapper = create_query_wrapper(db_map)
            with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
                DiffDatabaseMapping, "object_sq"
            ) as mock_object_sq, mock.patch.object(
                DiffDatabaseMapping, "wide_relationship_sq"
            ) as mock_wide_rel_sq, mock.patch.object(
                DiffDatabaseMapping, "parameter_definition_sq"
            ) as mock_parameter_definition_sq:
                mock_query.side_effect = query_wrapper
                mock_object_sq.return_value = [
                    KeyedTuple([1, 10, "nemo"], labels=["id", "class_id", "name"]),
                    KeyedTuple([2, 20, "pluto"], labels=["id", "class_id", "name"]),
                ]
                mock_wide_rel_sq.return_value = [
                    KeyedTuple([1, 100, "1,2", "nemo__pluto"], labels=["id", "class_id", "object_id_list", "name"]),
                    KeyedTuple([2, 200, "2,1", "pluto__nemo"], labels=["id", "class_id", "object_id_list", "name"]),
                ]
                mock_parameter_definition_sq.return_value = [
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
                    db_map.add_parameter_values(
                        {"parameter_definition_id": 1, "object_id": 2, "value": "orange"}, strict=True
                    )
                with self.assertRaises(SpineIntegrityError):
                    db_map.add_parameter_values(
                        {"parameter_definition_id": 2, "relationship_id": 2, "value": "125"}, strict=True
                    )
            db_map.connection.close()

    def test_add_same_parameter_value_twice(self):
        """Test that adding a parameter value twice only adds the first one."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            import_functions.import_object_classes(db_map, ["fish"])
            import_functions.import_objects(db_map, [("fish", "nemo")])
            import_functions.import_object_parameters(db_map, [("fish", "color")])
            color_id = (
                db_map.parameter_definition_list().filter(db_map.parameter_definition_sq.c.name == "color").first().id
            )
            nemo_row = db_map.object_list().filter(db_map.entity_sq.c.name == "nemo").first()
            db_map.add_parameter_values(
                {
                    "parameter_definition_id": color_id,
                    "entity_id": nemo_row.id,
                    "entity_class_id": nemo_row.class_id,
                    "value": '"orange"',
                    "alternative_id": 1,
                },
                {
                    "parameter_definition_id": color_id,
                    "entity_id": nemo_row.id,
                    "entity_class_id": nemo_row.class_id,
                    "value": '"blue"',
                    "alternative_id": 1,
                },
            )
            diff_table = db_map._diff_table("parameter_value")
            parameter_values = db_map.query(diff_table).all()
            self.assertEqual(len(parameter_values), 1)
            self.assertEqual(parameter_values[0].parameter_definition_id, 1)
            self.assertEqual(parameter_values[0].entity_id, 1)
            self.assertEqual(parameter_values[0].value, '"orange"')
            db_map.connection.close()

    def test_add_existing_parameter_value(self):
        """Test that adding an existing parameter value raises an integrity error."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            query_wrapper = create_query_wrapper(db_map)
            with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
                DiffDatabaseMapping, "entity_sq"
            ) as mock_entity_sq, mock.patch.object(
                DiffDatabaseMapping, "parameter_definition_sq"
            ) as mock_parameter_definition_sq, mock.patch.object(
                DiffDatabaseMapping, "parameter_value_sq"
            ) as mock_parameter_value_sq:
                mock_query.side_effect = query_wrapper
                mock_entity_sq.value = [
                    KeyedTuple([1, 10, "nemo"], labels=["id", "class_id", "name"]),
                    KeyedTuple([2, 20, "pluto"], labels=["id", "class_id", "name"]),
                    KeyedTuple([3, 100, "nemo__pluto"], labels=["id", "class_id", "name"]),
                ]
                mock_parameter_definition_sq.value = [
                    KeyedTuple(
                        [1, 10, "color", None], labels=["id", "entity_class_id", "name", "parameter_value_list_id"]
                    )
                ]
                mock_parameter_value_sq.value = [
                    KeyedTuple(
                        [1, 1, 1, "orange", 1],
                        labels=["id", "parameter_definition_id", "entity_id", "value", "alternative_id"],
                    )
                ]
                with self.assertRaises(SpineIntegrityError):
                    db_map.add_parameter_values(
                        {"parameter_definition_id": 1, "entity_id": 1, "value": "blue", "alternative_id": 1},
                        strict=True,
                    )
            db_map.connection.close()


class TestDiffDatabaseMappingUpdate(unittest.TestCase):
    def test_update_object_classes(self):
        """Test that updating object classes works."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"id": 1, "name": "fish"}, {"id": 2, "name": "dog"})
            ids, intgr_error_log = db_map.update_object_classes({"id": 1, "name": "octopus"}, {"id": 2, "name": "god"})
            sq = db_map.object_class_sq
            object_classes = {x.id: x.name for x in db_map.query(sq).filter(sq.c.id.in_(ids))}
            self.assertEqual(intgr_error_log, [])
            self.assertEqual(object_classes[1], "octopus")
            self.assertEqual(object_classes[2], "god")
            db_map.connection.close()

    def test_update_objects(self):
        """Test that updating objects works."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            query_wrapper = create_query_wrapper(db_map)
            with mock.patch.object(DiffDatabaseMapping, "query") as mock_query, mock.patch.object(
                DiffDatabaseMapping, "object_class_sq"
            ) as mock_object_class_sq:
                mock_query.side_effect = query_wrapper
                mock_object_class_sq.value = [KeyedTuple([1, "fish"], labels=["id", "name"])]
                db_map.add_objects({"id": 1, "name": "nemo", "class_id": 1}, {"id": 2, "name": "dory", "class_id": 1})
                ids, intgr_error_log = db_map.update_objects({"id": 1, "name": "klaus"}, {"id": 2, "name": "squidward"})
            sq = db_map.object_sq
            objects = {x.id: x.name for x in db_map.query(sq).filter(sq.c.id.in_(ids))}
            self.assertEqual(intgr_error_log, [])
            self.assertEqual(objects[1], "klaus")
            self.assertEqual(objects[2], "squidward")
            db_map.connection.close()

    def test_update_objects_not_commited(self):
        """Test that updating objects works."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"id": 1, "name": "some_class"})
            db_map.add_objects({"id": 1, "name": "nemo", "class_id": 1})
            ids, intgr_error_log = db_map.update_objects({"id": 1, "name": "klaus"})
            sq = db_map.object_sq
            objects = {x.id: x.name for x in db_map.query(sq).filter(sq.c.id.in_(ids))}
            self.assertEqual(intgr_error_log, [])
            self.assertEqual(objects[1], "klaus")
            self.assertEqual(db_map.query(db_map.object_sq).filter_by(id=1).first().name, "klaus")
            db_map.commit_session("update")
            self.assertEqual(db_map.query(db_map.object_sq).filter_by(id=1).first().name, "klaus")
            db_map.connection.close()

    def test_update_committed_object(self):
        """Test that updating objects works."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"id": 1, "name": "some_class"})
            db_map.add_objects({"id": 1, "name": "nemo", "class_id": 1})
            db_map.commit_session("update")
            ids, intgr_error_log = db_map.update_objects({"id": 1, "name": "klaus"})
            sq = db_map.object_sq
            objects = {x.id: x.name for x in db_map.query(sq).filter(sq.c.id.in_(ids))}
            self.assertEqual(intgr_error_log, [])
            self.assertEqual(objects[1], "klaus")
            self.assertEqual(db_map.query(db_map.object_sq).filter_by(id=1).first().name, "klaus")
            db_map.commit_session("update")
            self.assertEqual(db_map.query(db_map.object_sq).filter_by(id=1).first().name, "klaus")
            db_map.connection.close()

    def test_update_relationship_classes(self):
        """Test that updating relationship classes works."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "dog", "id": 1}, {"name": "fish", "id": 2})
            db_map.add_wide_relationship_classes(
                {"id": 3, "name": "dog__fish", "object_class_id_list": [1, 2]},
                {"id": 4, "name": "fish__dog", "object_class_id_list": [2, 1]},
            )
            ids, intgr_error_log = db_map.update_wide_relationship_classes(
                {"id": 3, "name": "god__octopus"}, {"id": 4, "name": "octopus__dog"}
            )
            sq = db_map.wide_relationship_class_sq
            rel_clss = {x.id: x.name for x in db_map.query(sq).filter(sq.c.id.in_(ids))}
            self.assertEqual(intgr_error_log, [])
            self.assertEqual(rel_clss[3], "god__octopus")
            self.assertEqual(rel_clss[4], "octopus__dog")
            db_map.connection.close()

    def test_update_relationships(self):
        """Test that updating relationships works."""
        with TemporaryDirectory() as temp_dir:
            db_map = create_diff_db_map(temp_dir)
            db_map.add_object_classes({"name": "fish", "id": 1}, {"name": "dog", "id": 2})
            db_map.add_wide_relationship_classes({"name": "fish__dog", "id": 3, "object_class_id_list": [1, 2]})
            db_map.add_objects(
                {"name": "nemo", "id": 1, "class_id": 1},
                {"name": "pluto", "id": 2, "class_id": 2},
                {"name": "scooby", "id": 3, "class_id": 2},
            )
            db_map.add_wide_relationships({"id": 4, "name": "nemo__pluto", "class_id": 3, "object_id_list": [1, 2]})
            ids, intgr_error_log = db_map.update_wide_relationships(
                {"id": 4, "name": "nemo__scooby", "object_id_list": [1, 3]}
            )
            sq = db_map.wide_relationship_sq
            rels = {
                x.id: {"name": x.name, "object_id_list": x.object_id_list}
                for x in db_map.query(sq).filter(sq.c.id.in_(ids))
            }
            self.assertEqual(intgr_error_log, [])
            self.assertEqual(rels[4]["name"], "nemo__scooby")
            self.assertEqual(rels[4]["object_id_list"], "1,3")
            db_map.connection.close()


if __name__ == "__main__":
    unittest.main()
