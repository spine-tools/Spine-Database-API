######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Database API contributors
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
"""
Unit tests for the ``filters.tools`` module.

"""
import os.path
from tempfile import TemporaryDirectory
import unittest
from sqlalchemy.engine.url import URL
from spinedb_api import (
    append_filter_config,
    clear_filter_configs,
    DatabaseMapping,
    export_entity_classes,
    import_object_classes,
    pop_filter_configs,
)
from spinedb_api.filters.tools import (
    apply_filter_stack,
    ensure_filtering,
    filter_configs,
    filter_config,
    load_filters,
    name_from_dict,
    store_filter,
)
from spinedb_api.filters.alternative_filter import alternative_filter_config, alternative_names_from_dict
from spinedb_api.filters.renamer import entity_class_renamer_config
from spinedb_api.filters.scenario_filter import scenario_filter_config, scenario_name_from_dict


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
            store_filter({}, out_file)
        stack = load_filters([path])
        self.assertEqual(stack, [{}])

    def test_config_ordering(self):
        path1 = os.path.join(self._dir.name, "config1.json")
        with open(path1, "w") as out_file:
            store_filter({"first": 1}, out_file)
        path2 = os.path.join(self._dir.name, "config2.json")
        with open(path2, "w") as out_file:
            store_filter({"second": 2}, out_file)
        stack = load_filters([path1, path2])
        self.assertEqual(stack, [{"first": 1}, {"second": 2}])

    def test_config_dict_passes_through(self):
        filters = [entity_class_renamer_config(object_class="renamed")]
        stack = load_filters(filters)
        self.assertEqual(stack, [entity_class_renamer_config(object_class="renamed")])

    def test_mixture_of_files_and_shorthands(self):
        path1 = os.path.join(self._dir.name, "config1.json")
        with open(path1, "w") as out_file:
            store_filter({"first": 1}, out_file)
        path2 = os.path.join(self._dir.name, "config2.json")
        with open(path2, "w") as out_file:
            store_filter({"second": 2}, out_file)
        stack = load_filters([path1, {"middle": -2}, path2])
        self.assertEqual(stack, [{"first": 1}, {"middle": -2}, {"second": 2}])


class TestApplyFilterStack(unittest.TestCase):
    _db_url = URL("sqlite")
    _dir = None

    @classmethod
    def setUpClass(cls):
        cls._dir = TemporaryDirectory()
        cls._db_url.database = os.path.join(cls._dir.name, ".json")
        db_map = DatabaseMapping(cls._db_url, create=True)
        import_object_classes(db_map, ("object_class",))
        db_map.commit_session("Add test data.")
        db_map.close()

    def test_empty_stack(self):
        db_map = DatabaseMapping(self._db_url)
        try:
            apply_filter_stack(db_map, [])
            object_classes = export_entity_classes(db_map)
            self.assertEqual(object_classes, [("object_class", (), None, None, False)])
        finally:
            db_map.close()

    def test_single_renaming_filter(self):
        db_map = DatabaseMapping(self._db_url)
        try:
            stack = [entity_class_renamer_config(object_class="renamed_once")]
            apply_filter_stack(db_map, stack)
            object_classes = export_entity_classes(db_map)
            self.assertEqual(object_classes, [("renamed_once", (), None, None, False)])
        finally:
            db_map.close()

    def test_two_renaming_filters(self):
        db_map = DatabaseMapping(self._db_url)
        try:
            stack = [
                entity_class_renamer_config(object_class="renamed_once"),
                entity_class_renamer_config(renamed_once="renamed_twice"),
            ]
            apply_filter_stack(db_map, stack)
            object_classes = export_entity_classes(db_map)
            self.assertEqual(object_classes, [("renamed_twice", (), None, None, False)])
        finally:
            db_map.close()


class TestFilteredDatabaseMap(unittest.TestCase):
    _db_url = URL("sqlite")
    _dir = None
    _engine = None

    @classmethod
    def setUpClass(cls):
        cls._dir = TemporaryDirectory()
        cls._db_url.database = os.path.join(cls._dir.name, "TestFilteredDatabaseMap.json")
        db_map = DatabaseMapping(cls._db_url, create=True)
        import_object_classes(db_map, ("object_class",))
        db_map.commit_session("Add test data.")
        db_map.close()

    def test_without_filters(self):
        db_map = DatabaseMapping(self._db_url, self._engine)
        try:
            object_classes = export_entity_classes(db_map)
            self.assertEqual(object_classes, [("object_class", (), None, None, False)])
        finally:
            db_map.close()

    def test_single_renaming_filter(self):
        path = os.path.join(self._dir.name, "config.json")
        with open(path, "w") as out_file:
            store_filter(entity_class_renamer_config(object_class="renamed_once"), out_file)
        url = append_filter_config(str(self._db_url), path)
        db_map = DatabaseMapping(url, self._engine)
        try:
            object_classes = export_entity_classes(db_map)
            self.assertEqual(object_classes, [("renamed_once", (), None, None, False)])
        finally:
            db_map.close()

    def test_two_renaming_filters(self):
        path1 = os.path.join(self._dir.name, "config1.json")
        with open(path1, "w") as out_file:
            store_filter(entity_class_renamer_config(object_class="renamed_once"), out_file)
        url = append_filter_config(str(self._db_url), path1)
        path2 = os.path.join(self._dir.name, "config2.json")
        with open(path2, "w") as out_file:
            store_filter(entity_class_renamer_config(renamed_once="renamed_twice"), out_file)
        url = append_filter_config(url, path2)
        db_map = DatabaseMapping(url, self._engine)
        try:
            object_classes = export_entity_classes(db_map)
            self.assertEqual(object_classes, [("renamed_twice", (), None, None, False)])
        finally:
            db_map.close()

    def test_config_embedded_to_url(self):
        config = entity_class_renamer_config(object_class="renamed_once")
        url = append_filter_config(str(self._db_url), config)
        db_map = DatabaseMapping(url, self._engine)
        try:
            object_classes = export_entity_classes(db_map)
            self.assertEqual(object_classes, [("renamed_once", (), None, None, False)])
        finally:
            db_map.close()


class TestAppendFilterConfig(unittest.TestCase):
    def test_append_to_simple_url(self):
        url = append_filter_config(r"sqlite:///C:\dbs\database.sqlite", r"F:\fltr\a.json")
        self.assertEqual(url, r"sqlite:///C:\dbs\database.sqlite?spinedbfilter=F%3A%5Cfltr%5Ca.json")

    def test_append_to_existing_filters(self):
        url = append_filter_config(r"sqlite:///C:\dbs\database.sqlite", r"F:\fltr\a.json")
        url = append_filter_config(url, r"F:\fltr\b.json")
        self.assertEqual(
            url,
            r"sqlite:///C:\dbs\database.sqlite?spinedbfilter=F%3A%5Cfltr%5Ca.json&spinedbfilter=F%3A%5Cfltr%5Cb.json",
        )

    def test_append_to_remote_database_url(self):
        url = append_filter_config(r"mysql+pymysql://username:password@remote.fi/database_name", r"F:\fltr\a.json")
        self.assertEqual(
            url, r"mysql+pymysql://username:password@remote.fi/database_name?spinedbfilter=F%3A%5Cfltr%5Ca.json"
        )


class TestFilterConfigs(unittest.TestCase):
    def test_empty_query(self):
        url = r"sqlite:///C:\dbs\database.sqlite"
        filters = filter_configs(url)
        self.assertEqual(filters, list())

    def test_single_query(self):
        url = append_filter_config(r"sqlite:///C:\dbs\database.sqlite", r"F:\fltr\a.json")
        filters = filter_configs(url)
        self.assertEqual(filters, [r"F:\fltr\a.json"])

    def test_filter_list_is_ordered(self):
        url = append_filter_config(r"sqlite:///C:\dbs\database.sqlite", r"F:\fltr\a.json")
        url = append_filter_config(url, r"F:\fltr\b.json")
        filters = filter_configs(url)
        self.assertEqual(filters, [r"F:\fltr\a.json", r"F:\fltr\b.json"])


class TestPopFilterConfigs(unittest.TestCase):
    def test_pop_from_empty_query(self):
        url = r"sqlite:///C:\dbs\database.sqlite"
        filters, popped = pop_filter_configs(url)
        self.assertEqual(filters, list())
        self.assertEqual(popped, url)

    def test_pop_single_query(self):
        url = append_filter_config(r"sqlite:///C:\dbs\database.sqlite", r"F:\fltr\a.json")
        filters, popped = pop_filter_configs(url)
        self.assertEqual(filters, [r"F:\fltr\a.json"])
        self.assertEqual(popped, r"sqlite:///C:\dbs\database.sqlite")

    def test_filter_list_is_ordered(self):
        url = append_filter_config(r"sqlite:///C:\dbs\database.sqlite", r"F:\fltr\a.json")
        url = append_filter_config(url, r"F:\fltr\b.json")
        filters, popped = pop_filter_configs(url)
        self.assertEqual(filters, [r"F:\fltr\a.json", r"F:\fltr\b.json"])
        self.assertEqual(popped, r"sqlite:///C:\dbs\database.sqlite")

    def test_pop_from_remote_url(self):
        url = append_filter_config(r"mysql+pymysql://username:password@remote.fi/database_name", r"F:\fltr\a.json")
        filters, popped = pop_filter_configs(url)
        self.assertEqual(filters, [r"F:\fltr\a.json"])
        self.assertEqual(popped, r"mysql+pymysql://username:password@remote.fi/database_name")


class TestClearFilterConfigs(unittest.TestCase):
    def test_without_queries(self):
        url = r"sqlite:///C:\dbs\database.sqlite"
        cleared = clear_filter_configs(url)
        self.assertEqual(cleared, url)

    def test_pop_single_query(self):
        url = append_filter_config(r"sqlite:///C:\dbs\database.sqlite", r"F:\fltr\a.json")
        cleared = clear_filter_configs(url)
        self.assertEqual(cleared, r"sqlite:///C:\dbs\database.sqlite")


class TestEnsureFiltering(unittest.TestCase):
    def test_ensure_filtering_adds_fallback_alternative(self):
        filtered = ensure_filtering("sqlite:///home/unittest/db.sqlite", fallback_alternative="fallback")
        config = filter_configs(filtered)
        self.assertEqual(len(config), 1)
        self.assertEqual(alternative_names_from_dict(config[0]), ["fallback"])

    def test_ensure_filtering_returns_original_url_if_alternative_exists(self):
        url = append_filter_config("sqlite:///home/unittest/db.sqlite", alternative_filter_config(["alternative"]))
        filtered = ensure_filtering(url, fallback_alternative="fallback")
        config = filter_configs(filtered)
        self.assertEqual(len(config), 1)
        self.assertEqual(alternative_names_from_dict(config[0]), ["alternative"])

    def test_ensure_filtering_returns_original_url_if_scenario_exists(self):
        url = append_filter_config("sqlite:///home/unittest/db.sqlite", scenario_filter_config("scenario"))
        filtered = ensure_filtering(url, fallback_alternative="fallback")
        config = filter_configs(filtered)
        self.assertEqual(len(config), 1)
        self.assertEqual(scenario_name_from_dict(config[0]), "scenario")

    def test_works_with_configs_stored_on_disk(self):
        with TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "filter.json")
            with open(path, "w") as out:
                store_filter(scenario_filter_config("scenario"), out)
            url = append_filter_config("sqlite:///home/unittest/db.sqlite", path)
            filtered = ensure_filtering(url, fallback_alternative="fallback")
            config = load_filters(filter_configs(filtered))
            self.assertEqual(len(config), 1)
            self.assertEqual(scenario_name_from_dict(config[0]), "scenario")


class TestNameFromDict(unittest.TestCase):
    def test_get_scenario_name(self):
        config = filter_config("scenario_filter", "scenario_name")
        self.assertEqual(name_from_dict(config), "scenario_name")

    def test_get_tool_name(self):
        with self.assertRaises(KeyError):
            _ = filter_config("tool_filter", "tool_name")

    def test_returns_none_if_name_not_found(self):
        config = entity_class_renamer_config(name="rename")
        self.assertIsNone(name_from_dict(config))


if __name__ == "__main__":
    unittest.main()
