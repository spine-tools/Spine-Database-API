######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
"""Unit tests for helpers.py."""


import unittest
from spinedb_api.helpers import (
    compare_schemas,
    create_new_spine_database,
    name_from_dimensions,
    name_from_elements,
    remove_credentials_from_url,
)


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
        engine1 = create_new_spine_database('sqlite://')
        engine2 = create_new_spine_database('sqlite://')
        self.assertTrue(compare_schemas(engine1, engine2))

    def test_different_schema(self):
        engine1 = create_new_spine_database('sqlite://')
        engine2 = create_new_spine_database('sqlite://')
        engine2.execute("drop table entity")
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


if __name__ == "__main__":
    unittest.main()
