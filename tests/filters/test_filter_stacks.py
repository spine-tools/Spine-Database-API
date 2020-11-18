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
from sqlalchemy.engine.url import URL
from spinedb_api import (
    append_filter_config,
    apply_filter_stack,
    DatabaseMapping,
    DiffDatabaseMapping,
    export_object_classes,
    import_object_classes,
    load_filters,
)
from spinedb_api.filters.renamer import entity_class_renamer_config


class TestLoadFilters(unittest.TestCase):
    _dir = None

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

    def test_config_dict_passes_through(self):
        filters = [entity_class_renamer_config(object_class="renamed")]
        stack = load_filters(filters)
        self.assertEqual(stack, [entity_class_renamer_config(object_class="renamed")])

    def test_mixture_of_files_and_shorthands(self):
        path1 = os.path.join(self._dir.name, "config1.json")
        with open(path1, "w") as out_file:
            dump({"first": 1}, out_file)
        path2 = os.path.join(self._dir.name, "config2.json")
        with open(path2, "w") as out_file:
            dump({"second": 2}, out_file)
        stack = load_filters([path1, {"middle": -2}, path2])
        self.assertEqual(stack, [{"first": 1}, {"middle": -2}, {"second": 2}])


class TestApplyFilterStack(unittest.TestCase):
    _db_url = URL("sqlite")
    _dir = None

    @classmethod
    def setUpClass(cls):
        cls._dir = TemporaryDirectory()
        cls._db_url.database = os.path.join(cls._dir.name, ".json")
        db_map = DiffDatabaseMapping(cls._db_url, create=True)
        import_object_classes(db_map, ("object_class",))
        db_map.commit_session("Add test data.")
        db_map.connection.close()

    def test_empty_stack(self):
        db_map = DatabaseMapping(self._db_url)
        try:
            apply_filter_stack(db_map, [])
            object_classes = export_object_classes(db_map)
            self.assertEqual(object_classes, [("object_class", None, None)])
        finally:
            db_map.connection.close()

    def test_single_renaming_filter(self):
        db_map = DatabaseMapping(self._db_url)
        try:
            stack = [entity_class_renamer_config(object_class="renamed_once")]
            apply_filter_stack(db_map, stack)
            object_classes = export_object_classes(db_map)
            self.assertEqual(object_classes, [("renamed_once", None, None)])
        finally:
            db_map.connection.close()

    def test_two_renaming_filters(self):
        db_map = DatabaseMapping(self._db_url)
        try:
            stack = [
                entity_class_renamer_config(object_class="renamed_once"),
                entity_class_renamer_config(renamed_once="renamed_twice"),
            ]
            apply_filter_stack(db_map, stack)
            object_classes = export_object_classes(db_map)
            self.assertEqual(object_classes, [("renamed_twice", None, None)])
        finally:
            db_map.connection.close()


class TestFilteredDatabaseMap(unittest.TestCase):
    _db_url = URL("sqlite")
    _dir = None
    _engine = None

    @classmethod
    def setUpClass(cls):
        cls._dir = TemporaryDirectory()
        cls._db_url.database = os.path.join(cls._dir.name, "TestFilteredDatabaseMap.json")
        db_map = DiffDatabaseMapping(cls._db_url, create=True)
        import_object_classes(db_map, ("object_class",))
        db_map.commit_session("Add test data.")
        db_map.connection.close()

    def test_without_filters(self):
        db_map = DatabaseMapping(self._db_url, self._engine)
        try:
            object_classes = export_object_classes(db_map)
            self.assertEqual(object_classes, [("object_class", None, None)])
        finally:
            db_map.connection.close()

    def test_single_renaming_filter(self):
        path = os.path.join(self._dir.name, "config.json")
        with open(path, "w") as out_file:
            dump(entity_class_renamer_config(object_class="renamed_once"), out_file)
        url = append_filter_config(str(self._db_url), path)
        db_map = DatabaseMapping(url, self._engine)
        try:
            object_classes = export_object_classes(db_map)
            self.assertEqual(object_classes, [("renamed_once", None, None)])
        finally:
            db_map.connection.close()

    def test_two_renaming_filters(self):
        path1 = os.path.join(self._dir.name, "config1.json")
        with open(path1, "w") as out_file:
            dump(entity_class_renamer_config(object_class="renamed_once"), out_file)
        url = append_filter_config(str(self._db_url), path1)
        path2 = os.path.join(self._dir.name, "config2.json")
        with open(path2, "w") as out_file:
            dump(entity_class_renamer_config(renamed_once="renamed_twice"), out_file)
        url = append_filter_config(url, path2)
        db_map = DatabaseMapping(url, self._engine)
        try:
            object_classes = export_object_classes(db_map)
            self.assertEqual(object_classes, [("renamed_twice", None, None)])
        finally:
            db_map.connection.close()

    def test_config_embedded_to_url(self):
        config = entity_class_renamer_config(object_class="renamed_once")
        url = append_filter_config(str(self._db_url), config)
        db_map = DatabaseMapping(url, self._engine)
        try:
            object_classes = export_object_classes(db_map)
            self.assertEqual(object_classes, [("renamed_once", None, None)])
        finally:
            db_map.connection.close()


if __name__ == "__main__":
    unittest.main()
