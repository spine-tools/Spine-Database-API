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
"""Unit tests for helpers.py."""

import pathlib
from tempfile import TemporaryDirectory
import unittest
from sqlalchemy import text
from spinedb_api import DatabaseMapping
from spinedb_api.helpers import (
    compare_schemas,
    copy_database,
    create_new_spine_database,
    fix_name_ambiguity,
    get_head_alembic_version,
    group_consecutive,
    name_from_dimensions,
    name_from_elements,
    remove_credentials_from_url,
    string_to_bool,
    vacuum,
)
from tests.mock_helpers import AssertSuccessTestCase


class TestNameFromElements(unittest.TestCase):
    def test_single_element(self):
        self.assertEqual(name_from_elements(("a",)), "a__")

    def test_multiple_elements(self):
        self.assertEqual(name_from_elements(("a", "b")), "a__b")


class TestNameFromDimensions(unittest.TestCase):
    def test_single_dimension(self):
        self.assertEqual(name_from_dimensions(("a",)), "a__")

    def test_multiple_dimension(self):
        self.assertEqual(name_from_dimensions(("a", "b")), "a__b")


class TestCreateNewSpineEngine(unittest.TestCase):
    def test_same_schema(self):
        engine1 = create_new_spine_database("sqlite://")
        engine2 = create_new_spine_database("sqlite://")
        self.assertTrue(compare_schemas(engine1, engine2))

    def test_different_schema(self):
        engine1 = create_new_spine_database("sqlite://")
        engine2 = create_new_spine_database("sqlite://")
        with engine2.begin() as connection:
            connection.execute(text("drop table entity"))
        self.assertFalse(compare_schemas(engine1, engine2))


class TestRemoveCredentialsFromUrl(unittest.TestCase):
    def test_url_without_credentials_is_returned_as_is(self):
        url = "mysql://example.com/db"
        sanitized = remove_credentials_from_url(url)
        self.assertEqual(url, sanitized)

    def test_username_and_password_are_removed(self):
        url = "mysql://user:secret@example.com/db"
        sanitized = remove_credentials_from_url(url)
        self.assertEqual(sanitized, "mysql://example.com/db")

    def test_password_with_special_characters(self):
        url = "mysql://user:p@ass://word@example.com/db"
        sanitized = remove_credentials_from_url(url)
        self.assertEqual(sanitized, "mysql://example.com/db")


class TestGetHeadAlembicVersion(unittest.TestCase):
    def test_returns_latest_version(self):
        # This test must be updated each time new migration script is added.
        self.assertEqual(get_head_alembic_version(), "91f1f55aa972")


class TestStringToBool(unittest.TestCase):
    def test_truths(self):
        self.assertTrue(string_to_bool("yes"))
        self.assertTrue(string_to_bool("YES"))
        self.assertTrue(string_to_bool("y"))
        self.assertTrue(string_to_bool("true"))
        self.assertTrue(string_to_bool("t"))
        self.assertTrue(string_to_bool("1"))

    def test_falses(self):
        self.assertFalse(string_to_bool("NO"))
        self.assertFalse(string_to_bool("no"))
        self.assertFalse(string_to_bool("n"))
        self.assertFalse(string_to_bool("false"))
        self.assertFalse(string_to_bool("f"))
        self.assertFalse(string_to_bool("0"))

    def test_raises_value_error(self):
        self.assertRaises(ValueError, string_to_bool, "no truth in this")


class TestVacuum(unittest.TestCase):
    def test_vacuum(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + str(pathlib.Path(temp_dir) / "db.sqlite")
            create_new_spine_database(url)
            freed, units = vacuum(url)
            self.assertEqual(freed, 0)
            self.assertEqual(units, "bytes")


class TestCopyDatabase(AssertSuccessTestCase):
    def test_copies_correctly(self):
        with TemporaryDirectory() as temp_dir:
            source_url = "sqlite:///" + str(pathlib.Path(temp_dir) / "source.sqlite")
            with DatabaseMapping(source_url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="ForgottenAtAGasStation"))
                db_map.commit_session("Add some data.")
            target_url = "sqlite:///" + str(pathlib.Path(temp_dir) / "destination.sqlite")
            create_new_spine_database(target_url)
            copy_database(target_url, source_url)
            with DatabaseMapping(target_url) as db_map:
                entity_class = db_map.get_entity_class_item(name="ForgottenAtAGasStation")
                self.assertTrue(bool(entity_class))


class TestFixNameAmbiguity(unittest.TestCase):
    def test_empty_input_list(self):
        self.assertEqual(fix_name_ambiguity([]), [])

    def test_unique_names_are_kept(self):
        self.assertEqual(fix_name_ambiguity(["a", "b"]), ["a", "b"])

    def test_ambiguous_names_get_prefixed(self):
        self.assertEqual(fix_name_ambiguity(["a", "a"]), ["a1", "a2"])

    def test_offset(self):
        self.assertEqual(fix_name_ambiguity(["a", "a", "b"], offset=5), ["a6", "a7", "b"])

    def test_prefix(self):
        self.assertEqual(fix_name_ambiguity(["a", "b", "b"], prefix="/"), ["a", "b/1", "b/2"])


class TestGroupConsecutive(unittest.TestCase):
    def test_empty_input(self):
        self.assertEqual(list(group_consecutive([])), [])

    def test_grouping(self):
        self.assertEqual(list(group_consecutive((1, 2, 6, 3, 7, 10))), [(1, 3), (6, 7), (10, 10)])


if __name__ == "__main__":
    unittest.main()
