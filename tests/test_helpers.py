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
    query_byname,
    remove_credentials_from_url,
)
from spinedb_api.db_mapping import DatabaseMapping


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


class TestQueryByname(unittest.TestCase):
    def _assert_success(self, result):
        item, error = result
        self.assertIsNone(error)
        return item

    def test_zero_dimension_entity(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="my_class"))
            self._assert_success(db_map.add_entity_item(name="my_entity", entity_class_name="my_class"))
            db_map.commit_session("Add entity.")
            entity_row = db_map.query(db_map.wide_entity_sq).one()
            self.assertEqual(query_byname(entity_row, db_map), ("my_entity",))

    def test_dimensioned_entity(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="class_1"))
            self._assert_success(db_map.add_entity_class_item(name="class_2"))
            self._assert_success(db_map.add_entity_item(name="entity_1", entity_class_name="class_1"))
            self._assert_success(db_map.add_entity_item(name="entity_2", entity_class_name="class_2"))
            self._assert_success(
                db_map.add_entity_class_item(name="relationship", dimension_name_list=("class_1", "class_2"))
            )
            relationship = self._assert_success(
                db_map.add_entity_item(entity_class_name="relationship", element_name_list=("entity_1", "entity_2"))
            )
            db_map.commit_session("Add entities")
            entity_row = (
                db_map.query(db_map.wide_entity_sq)
                .filter(db_map.wide_entity_sq.c.id == db_map.find_db_id("entity", relationship["id"]))
                .one()
            )
            self.assertEqual(query_byname(entity_row, db_map), ("entity_1", "entity_2"))

    def test_deep_dimensioned_entity(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="class_1"))
            self._assert_success(db_map.add_entity_class_item(name="class_2"))
            self._assert_success(db_map.add_entity_item(name="entity_1", entity_class_name="class_1"))
            self._assert_success(db_map.add_entity_item(name="entity_2", entity_class_name="class_2"))
            self._assert_success(
                db_map.add_entity_class_item(name="relationship_1", dimension_name_list=("class_1", "class_2"))
            )
            relationship_1 = self._assert_success(
                db_map.add_entity_item(entity_class_name="relationship_1", element_name_list=("entity_1", "entity_2"))
            )
            self._assert_success(
                db_map.add_entity_class_item(name="relationship_2", dimension_name_list=("class_2", "class_1"))
            )
            relationship_2 = self._assert_success(
                db_map.add_entity_item(entity_class_name="relationship_2", element_name_list=("entity_2", "entity_1"))
            )
            self._assert_success(
                db_map.add_entity_class_item(
                    name="super_relationship", dimension_name_list=("relationship_1", "relationship_2")
                )
            )
            superrelationship = self._assert_success(
                db_map.add_entity_item(
                    entity_class_name="super_relationship",
                    element_name_list=(relationship_1["name"], relationship_2["name"]),
                )
            )
            db_map.commit_session("Add entities")
            entity_row = (
                db_map.query(db_map.wide_entity_sq)
                .filter(db_map.wide_entity_sq.c.id == db_map.find_db_id("entity", superrelationship["id"]))
                .one()
            )
            self.assertEqual(query_byname(entity_row, db_map), ("entity_1", "entity_2", "entity_2", "entity_1"))


if __name__ == "__main__":
    unittest.main()
