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
Unit tests for the ``filters.filter_stacks`` module.

:author: A. Soininen
:date:   7.10.2020
"""
from json import dump
import os.path
from tempfile import TemporaryDirectory
import unittest
from spinedb_api import (
    append_filter_config,
    apply_filter_stack,
    create_new_spine_database,
    DatabaseMapping,
    DiffDatabaseMapping,
    export_object_classes,
    filtered_database_map,
    import_object_classes,
    load_filters,
)


class TestLoadFilters(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._dir = TemporaryDirectory()

    def test_no_config_files(self):
        stack = load_filters([])
        self.assertEqual(stack, [])

    def test_single_config(self):
        path = os.path.join(self._dir.name, "config.json")
        with open(path, "w") as out_file:
            dump({}, out_file)
        stack = load_filters([path])
        self.assertEqual(stack, [{}])

    def test_config_ordering(self):
        path1 = os.path.join(self._dir.name, "config1.json")
        with open(path1, "w") as out_file:
            dump({"first": 1}, out_file)
        path2 = os.path.join(self._dir.name, "config2.json")
        with open(path2, "w") as out_file:
            dump({"second": 2}, out_file)
        stack = load_filters([path1, path2])
        self.assertEqual(stack, [{"first": 1}, {"second": 2}])


class TestApplyFilterStack(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._dir = TemporaryDirectory()
        path = os.path.join(cls._dir.name, "database.sqlite")
        cls._db_url = "sqlite:///" + path
        create_new_spine_database(cls._db_url)
        db_map = DiffDatabaseMapping(cls._db_url)
        import_object_classes(db_map, ("object_class",))
        db_map.commit_session("Add test data.")
        db_map.connection.close()

    def test_empty_stack(self):
        db_map = DatabaseMapping(self._db_url)
        try:
            apply_filter_stack(db_map, [])
            object_classes = export_object_classes(db_map)
            self.assertEqual(object_classes, [("object_class", None, None)])
        except:
            raise
        finally:
            db_map.connection.close()

    def test_single_renaming_filter(self):
        db_map = DatabaseMapping(self._db_url)
        try:
            stack = [{"type": "renamer", "name_map": {"object_class": "renamed_once"}}]
            apply_filter_stack(db_map, stack)
            object_classes = export_object_classes(db_map)
            self.assertEqual(object_classes, [("renamed_once", None, None)])
        except:
            raise
        finally:
            db_map.connection.close()

    def test_two_renaming_filters(self):
        db_map = DatabaseMapping(self._db_url)
        try:
            stack = [
                {"type": "renamer", "name_map": {"object_class": "renamed_once"}},
                {"type": "renamer", "name_map": {"renamed_once": "renamed_twice"}},
            ]
            apply_filter_stack(db_map, stack)
            object_classes = export_object_classes(db_map)
            self.assertEqual(object_classes, [("renamed_twice", None, None)])
        except:
            raise
        finally:
            db_map.connection.close()


class TestFilteredDatabaseMap(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._dir = TemporaryDirectory()
        path = os.path.join(cls._dir.name, "database.sqlite")
        cls._db_url = "sqlite:///" + path
        create_new_spine_database(cls._db_url)
        db_map = DiffDatabaseMapping(cls._db_url)
        import_object_classes(db_map, ("object_class",))
        db_map.commit_session("Add test data.")
        db_map.connection.close()

    def test_without_filters(self):
        db_map = filtered_database_map(DatabaseMapping, self._db_url)
        try:
            object_classes = export_object_classes(db_map)
            self.assertEqual(object_classes, [("object_class", None, None)])
        except:
            raise
        finally:
            db_map.connection.close()

    def test_single_renaming_filter(self):
        path = os.path.join(self._dir.name, "config.json")
        with open(path, "w") as out_file:
            dump({"type": "renamer", "name_map": {"object_class": "renamed_once"}}, out_file)
        url = append_filter_config(self._db_url, path)
        db_map = filtered_database_map(DatabaseMapping, url)
        try:
            object_classes = export_object_classes(db_map)
            self.assertEqual(object_classes, [("renamed_once", None, None)])
        except:
            raise
        finally:
            db_map.connection.close()

    def test_two_renaming_filters(self):
        path1 = os.path.join(self._dir.name, "config1.json")
        with open(path1, "w") as out_file:
            dump({"type": "renamer", "name_map": {"object_class": "renamed_once"}}, out_file)
        url = append_filter_config(self._db_url, path1)
        path2 = os.path.join(self._dir.name, "config2.json")
        with open(path2, "w") as out_file:
            dump({"type": "renamer", "name_map": {"renamed_once": "renamed_twice"}}, out_file)
        url = append_filter_config(url, path2)
        db_map = filtered_database_map(DatabaseMapping, url)
        try:
            object_classes = export_object_classes(db_map)
            self.assertEqual(object_classes, [("renamed_twice", None, None)])
        except:
            raise
        finally:
            db_map.connection.close()


if __name__ == "__main__":
    unittest.main()
