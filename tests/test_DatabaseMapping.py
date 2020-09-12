######################################################################################################################
# Copyright (C) 2017 - 2020 Spine project consortium
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
import os.path
from tempfile import TemporaryDirectory
import unittest
from spinedb_api.helpers import create_new_spine_database
from spinedb_api import DatabaseMapping


class TestDatabaseMappingBase(unittest.TestCase):
    _db_map = None

    @classmethod
    def setUpClass(cls):
        cls._temp_dir = TemporaryDirectory()
        db_url = "sqlite:///" + os.path.join(cls._temp_dir.name, "test_database_mapping.sqlite")
        engine = create_new_spine_database(db_url)
        cls._db_map = DatabaseMapping(db_url)

    @classmethod
    def tearDownClass(cls):
        cls._db_map.connection.close()

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
            "data_type",
            "entity_class_id",
            "object_class_id",
            "relationship_class_id",
            "default_value",
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
            "commit_id",
            "alternative_id",
        ]
        self.assertEqual(len(self._db_map.parameter_value_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.parameter_value_sq.c, column_name))

    def test_parameter_tag_sq(self):
        columns = ["id", "tag", "description", "commit_id"]
        self.assertEqual(len(self._db_map.parameter_tag_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.parameter_tag_sq.c, column_name))

    def test_parameter_definition_tag_sq(self):
        columns = ["id", "parameter_definition_id", "parameter_tag_id", "commit_id"]
        self.assertEqual(len(self._db_map.parameter_definition_tag_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.parameter_definition_tag_sq.c, column_name))

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
        columns = ["id", "name", "description", "object_class_id", "object_class_name"]
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
            "object_id",
            "object_name",
            "object_class_id",
            "object_class_name",
        ]
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
            "object_class_id",
            "object_class_name",
            "parameter_name",
            "value_list_id",
            "value_list_name",
            "parameter_tag_id_list",
            "parameter_tag_list",
            "default_value",
            "description",
        ]
        self.assertEqual(len(self._db_map.object_parameter_definition_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.object_parameter_definition_sq.c, column_name))

    def test_relationship_parameter_definition_sq(self):
        columns = [
            "id",
            "entity_class_id",
            "relationship_class_id",
            "relationship_class_name",
            "object_class_id_list",
            "object_class_name_list",
            "parameter_name",
            "value_list_id",
            "value_list_name",
            "parameter_tag_id_list",
            "parameter_tag_list",
            "default_value",
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
        ]
        self.assertEqual(len(self._db_map.relationship_parameter_value_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.relationship_parameter_value_sq.c, column_name))

    def test_ext_parameter_definition_tag_sq(self):
        columns = ["parameter_definition_id", "parameter_tag_id", "parameter_tag"]
        self.assertEqual(len(self._db_map.ext_parameter_definition_tag_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.ext_parameter_definition_tag_sq.c, column_name))

    def test_wide_parameter_definition_tag_sq(self):
        columns = ["id", "parameter_tag_id_list", "parameter_tag_list"]
        self.assertEqual(len(self._db_map.wide_parameter_definition_tag_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.wide_parameter_definition_tag_sq.c, column_name))

    def test_wide_parameter_value_list_sq(self):
        columns = ["id", "name", "value_list"]
        self.assertEqual(len(self._db_map.wide_parameter_value_list_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.wide_parameter_value_list_sq.c, column_name))


if __name__ == "__main__":
    unittest.main()
