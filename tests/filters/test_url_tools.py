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
Unit tests for the ``filters.url_tools`` module.

:author: A. Soininen
:date:   7.10.2020
"""
import unittest
from spinedb_api import append_filter_config, clear_filter_configs, pop_filter_configs
from spinedb_api.filters.url_tools import ensure_filtering, filter_configs
from spinedb_api.filters.alternative_filter import alternative_filter_config, alternative_names_from_dict
from spinedb_api.filters.scenario_filter import scenario_filter_config, scenario_name_from_dict


class TestAppendFilterConfig(unittest.TestCase):
    def test_append_to_simple_url(self):
        url = append_filter_config(r"sqlite:///C:\dbs\database.sqlite", r"F:\fltr\a.json")
        self.assertEqual(url, r"sqlite:///C:\dbs\database.sqlite?spinedbfilter=F%3A%5Cfltr%5Ca.json")

    def test_append_to_existing_filters(self):
        url = append_filter_config("sqlite:///C:\dbs\database.sqlite", r"F:\fltr\a.json")
        url = append_filter_config(url, r"F:\fltr\b.json")
        self.assertEqual(
            url,
            r"sqlite:///C:\dbs\database.sqlite?spinedbfilter=F%3A%5Cfltr%5Ca.json&spinedbfilter=F%3A%5Cfltr%5Cb.json",
        )


class TestFilterConfigs(unittest.TestCase):
    def test_empty_query(self):
        url = "sqlite:///C:\dbs\database.sqlite"
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
        url = "sqlite:///C:\dbs\database.sqlite"
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


class TestClearFilterConfigs(unittest.TestCase):
    def test_without_queries(self):
        url = "sqlite:///C:\dbs\database.sqlite"
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


if __name__ == "__main__":
    unittest.main()
