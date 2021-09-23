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
Unit tests for DatabaseMapping class.

:author: A. Soininen
:date:   2.7.2020
"""
import unittest
from unittest.mock import patch
from sqlalchemy.engine.url import URL
from spinedb_api import DatabaseMapping


class TestDatabaseMappingBase(unittest.TestCase):
    _db_map = None
    _db_url = "sqlite://"

    @classmethod
    def setUpClass(cls):
        cls._db_map = DatabaseMapping(cls._db_url, create=True)

    @classmethod
    def tearDownClass(cls):
        cls._db_map.connection.close()

    def test_construction_with_filters(self):
        db_url = self._db_url + "?spinedbfilter=fltr1&spinedbfilter=fltr2"
        with patch("spinedb_api.db_mapping.apply_filter_stack") as mock_apply:
            with patch(
                "spinedb_api.db_mapping.load_filters", return_value=[{"fltr1": "config1", "fltr2": "config2"}]
            ) as mock_load:
                db_map = DatabaseMapping(db_url, create=True)
                db_map.connection.close()
                mock_load.assert_called_once_with(["fltr1", "fltr2"])
                mock_apply.assert_called_once_with(db_map, [{"fltr1": "config1", "fltr2": "config2"}])

    def test_construction_with_sqlalchemy_url_and_filters(self):
        sa_url = URL("sqlite")
        sa_url.query = {"spinedbfilter": ["fltr1", "fltr2"]}
        with patch("spinedb_api.db_mapping.apply_filter_stack") as mock_apply:
            with patch(
                "spinedb_api.db_mapping.load_filters", return_value=[{"fltr1": "config1", "fltr2": "config2"}]
            ) as mock_load:
                db_map = DatabaseMapping(sa_url, create=True)
                db_map.connection.close()
                mock_load.assert_called_once_with(["fltr1", "fltr2"])
                mock_apply.assert_called_once_with(db_map, [{"fltr1": "config1", "fltr2": "config2"}])

    def test_entity_class_type_sq(self):
        columns = ["id", "name", "commit_id"]
        self.assertEqual(len(self._db_map.entity_class_type_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.entity_class_type_sq.c, column_name))

    def test_entity_type_sq(self):
        columns = ["id", "name", "commit_id"]
        self.assertEqual(len(self._db_map.entity_type_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.entity_type_sq.c, column_name))

    def test_entity_sq(self):
        columns = ["id", "type_id", "class_id", "name", "description", "commit_id"]
        self.assertEqual(len(self._db_map.entity_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.entity_sq.c, column_name))

    def test_object_class_sq(self):
        columns = ["id", "name", "description", "display_order", "display_icon", "hidden", "commit_id"]
        self.assertEqual(len(self._db_map.object_class_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.object_class_sq.c, column_name))

    def test_object_sq(self):
        columns = ["id", "class_id", "name", "description", "commit_id"]
        self.assertEqual(len(self._db_map.object_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.object_sq.c, column_name))

    def test_relationship_class_sq(self):
        columns = ["id", "dimension", "object_class_id", "name", "description", "hidden", "commit_id"]
        self.assertEqual(len(self._db_map.relationship_class_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.relationship_class_sq.c, column_name))

    def test_relationship_sq(self):
        columns = ["id", "dimension", "object_id", "class_id", "name", "commit_id"]
        self.assertEqual(len(self._db_map.relationship_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.relationship_sq.c, column_name))

    def test_entity_group_sq(self):
        columns = ["id", "entity_id", "entity_class_id", "member_id"]
        self.assertEqual(len(self._db_map.entity_group_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.entity_group_sq.c, column_name))

    def test_parameter_definition_sq(self):
        columns = [
            "id",
            "name",
            "description",
            "entity_class_id",
            "object_class_id",
            "relationship_class_id",
            "default_value",
            "default_type",
            "commit_id",
            "parameter_value_list_id",
        ]
        self.assertEqual(len(self._db_map.parameter_definition_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.parameter_definition_sq.c, column_name))

    def test_parameter_value_sq(self):
        columns = [
            "id",
            "parameter_definition_id",
            "entity_class_id",
            "entity_id",
            "object_class_id",
            "relationship_class_id",
            "object_id",
            "relationship_id",
            "value",
            "type",
            "commit_id",
            "alternative_id",
        ]
        self.assertEqual(len(self._db_map.parameter_value_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.parameter_value_sq.c, column_name))

    def test_parameter_value_list_sq(self):
        columns = ["id", "name", "value_index", "value", "commit_id"]
        self.assertEqual(len(self._db_map.parameter_value_list_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.parameter_value_list_sq.c, column_name))

    def test_ext_object_sq(self):
        columns = ["id", "class_id", "class_name", "name", "description"]
        self.assertEqual(len(self._db_map.ext_object_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.ext_object_sq.c, column_name))

    def test_ext_relationship_class_sq(self):
        columns = ["id", "name", "description", "dimension", "object_class_id", "object_class_name"]
        self.assertEqual(len(self._db_map.ext_relationship_class_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.ext_relationship_class_sq.c, column_name))

    def test_wide_relationship_class_sq(self):
        columns = ["id", "name", "description", "object_class_id_list", "object_class_name_list"]
        self.assertEqual(len(self._db_map.wide_relationship_class_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.wide_relationship_class_sq.c, column_name))

    def test_ext_relationship_sq(self):
        columns = [
            "id",
            "name",
            "class_id",
            "class_name",
            "dimension",
            "object_id",
            "object_name",
            "object_class_id",
            "object_class_name",
        ]
        data = self._db_map.ext_relationship_sq.c
        self.assertEqual(len(self._db_map.ext_relationship_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.ext_relationship_sq.c, column_name))

    def test_wide_relationship_sq(self):
        columns = [
            "id",
            "name",
            "class_id",
            "class_name",
            "object_id_list",
            "object_name_list",
            "object_class_id_list",
            "object_class_name_list",
        ]
        self.assertEqual(len(self._db_map.wide_relationship_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.wide_relationship_sq.c, column_name))

    def test_object_parameter_definition_sq(self):
        columns = [
            "id",
            "entity_class_id",
            "entity_class_name",
            "object_class_id",
            "object_class_name",
            "parameter_name",
            "value_list_id",
            "value_list_name",
            "default_value",
            "default_type",
            "description",
        ]
        self.assertEqual(len(self._db_map.object_parameter_definition_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.object_parameter_definition_sq.c, column_name))

    def test_relationship_parameter_definition_sq(self):
        columns = [
            "id",
            "entity_class_id",
            "entity_class_name",
            "relationship_class_id",
            "relationship_class_name",
            "object_class_id_list",
            "object_class_name_list",
            "parameter_name",
            "value_list_id",
            "value_list_name",
            "default_value",
            "default_type",
            "description",
        ]
        self.assertEqual(len(self._db_map.relationship_parameter_definition_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.relationship_parameter_definition_sq.c, column_name))

    def test_object_parameter_value_sq(self):
        columns = [
            "id",
            "entity_class_id",
            "object_class_id",
            "object_class_name",
            "entity_id",
            "object_id",
            "object_name",
            "parameter_id",
            "parameter_name",
            "alternative_id",
            "alternative_name",
            "value",
            "type",
        ]
        self.assertEqual(len(self._db_map.object_parameter_value_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.object_parameter_value_sq.c, column_name))

    def test_relationship_parameter_value_sq(self):
        columns = [
            "id",
            "entity_class_id",
            "relationship_class_id",
            "relationship_class_name",
            "object_class_id_list",
            "object_class_name_list",
            "entity_id",
            "relationship_id",
            "object_id_list",
            "object_name_list",
            "parameter_id",
            "parameter_name",
            "alternative_id",
            "alternative_name",
            "value",
            "type",
        ]
        self.assertEqual(len(self._db_map.relationship_parameter_value_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.relationship_parameter_value_sq.c, column_name))

    def test_wide_parameter_value_list_sq(self):
        columns = ["id", "name", "value_index_list", "value_list"]
        self.assertEqual(len(self._db_map.wide_parameter_value_list_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.wide_parameter_value_list_sq.c, column_name))


if __name__ == "__main__":
    unittest.main()
