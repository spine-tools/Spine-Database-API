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
import unittest
from spinedb_api import Asterisk, DatabaseMapping, SpineDBAPIError


class TestItem(unittest.TestCase):
    def test_normal_operation(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            mapped_table = db_map.mapped_table("alternative")
            alternative = db_map.item(mapped_table, name="Base")
            self.assertEqual(alternative["name"], "Base")

    def test_raises_when_find_args_are_wrong(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            with self.assertRaisesRegex(SpineDBAPIError, "no alternative matching {'name': 'non-existent'}"):
                mapped_table = db_map.mapped_table("alternative")
                db_map.item(mapped_table, name="non-existent")

    def test_does_not_return_removed_item(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            mapped_table = db_map.mapped_table("alternative")
            db_map.item(mapped_table, name="Base").remove()
            with self.assertRaisesRegex(SpineDBAPIError, "alternative matching {'name': 'Base'} has been removed"):
                db_map.item(mapped_table, name="Base")


class TestAdd(unittest.TestCase):
    def test_normal_operation(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            mapped_table = db_map.mapped_table("alternative")
            alternative = db_map.add(mapped_table, name="new", description="New alternative.")
            self.assertEqual(alternative["name"], "new")
            self.assertEqual(alternative["description"], "New alternative.")
            alternative_in_mapping = db_map.alternative(name="new")
            self.assertEqual(alternative, alternative_in_mapping)

    def test_raises_when_item_is_wrong(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            with self.assertRaisesRegex(SpineDBAPIError, "missing name"):
                mapped_table = db_map.mapped_table("alternative")
                db_map.add(mapped_table, entity_class="new")


class TestAddByType(unittest.TestCase):
    def test_normal_operation(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            added = db_map.add_by_type("alternative", name="new")
            self.assertEqual(added.item_type, "alternative")
            self.assertEqual(added["name"], "new")
            item = db_map.alternative(name="new")
            self.assertEqual(item["name"], "new")


class TestApplyManyByType(unittest.TestCase):
    def test_apply_add(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.apply_many_by_type("alternative", "add", [{"name": "new"}])
            item = db_map.alternative(name="new")
            self.assertEqual(item["name"], "new")


class TestFind(unittest.TestCase):
    def test_returns_empty_list_when_no_items_exist(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            mapped_table = db_map.mapped_table("entity")
            found = db_map.find(mapped_table)
            self.assertEqual(found, [])

    def test_returns_found_items(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            mapped_table = db_map.mapped_table("alternative")
            found = db_map.find(mapped_table, name="Base")
            self.assertEqual(len(found), 1)
            self.assertEqual(found[0]["name"], "Base")

    def test_returns_all_items(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_alternative(name="new")
            mapped_table = db_map.mapped_table("alternative")
            found = db_map.find(mapped_table)
            self.assertEqual(len(found), 2)
            self.assertCountEqual([i["name"] for i in found], ["Base", "new"])

    def test_find_doesnt_find_removed_items(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_scenario(name="Scenario").remove()
            scenario_table = db_map.mapped_table("scenario")
            found = db_map.find(scenario_table)
            self.assertEqual(found, [])

    def test_asterisk_works_with_bynames(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Object")
            db_map.add_entity(name="o1", entity_class_name="Object")
            db_map.add_entity(name="o2", entity_class_name="Object")
            db_map.add_entity(name="o3", entity_class_name="Object")
            db_map.add_entity_class(dimension_name_list=["Object", "Object"])
            db_map.add_entity(entity_class_name="Object__Object", entity_byname=("o1", "o1"))
            db_map.add_entity(entity_class_name="Object__Object", entity_byname=("o1", "o2"))
            db_map.add_entity(entity_class_name="Object__Object", entity_byname=("o1", "o3"))
            db_map.add_entity(entity_class_name="Object__Object", entity_byname=("o3", "o2"))
            db_map.add_entity(entity_class_name="Object__Object", entity_byname=("o3", "o1"))
            self.assertCountEqual(
                [
                    i["entity_byname"]
                    for i in db_map.find_entities(
                        entity_class_name="Object__Object", entity_byname=(Asterisk, Asterisk)
                    )
                ],
                [("o1", "o1"), ("o1", "o2"), ("o1", "o3"), ("o3", "o2"), ("o3", "o1")],
            )
            self.assertCountEqual(
                [
                    i["entity_byname"]
                    for i in db_map.find_entities(entity_class_name="Object__Object", entity_byname=("o2", Asterisk))
                ],
                [],
            )
            self.assertCountEqual(
                [
                    i["entity_byname"]
                    for i in db_map.find_entities(entity_class_name="Object__Object", entity_byname=("o3", Asterisk))
                ],
                [("o3", "o2"), ("o3", "o1")],
            )
            self.assertCountEqual(
                [
                    i["entity_byname"]
                    for i in db_map.find_entities(entity_class_name="Object__Object", entity_byname=(Asterisk, "o2"))
                ],
                [
                    ("o1", "o2"),
                    ("o3", "o2"),
                ],
            )


class TestFindByType(unittest.TestCase):
    def test_returns_entities_of_class(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Object")
            db_map.add_entities([{"name": "gadget"}, {"name": "widget"}], entity_class_name="Object")
            db_map.add_entity_class(name="Subject")
            db_map.add_entities([{"name": "mockup"}, {"name": "slush"}], entity_class_name="Subject")
            entities = db_map.find_by_type("entity", entity_class_name="Object")
            self.assertCountEqual([entity["name"] for entity in entities], ["widget", "gadget"])


class TestUpdate(unittest.TestCase):
    def test_update_alternative_description(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            alternative = db_map.add_alternative(name="new")
            mapped_table = db_map.mapped_table("alternative")
            updated = db_map.update(mapped_table, name="new", description="Updated description.")
            self.assertEqual(updated["name"], "new")
            self.assertEqual(updated["description"], "Updated description.")
            self.assertEqual(alternative["name"], "new")
            self.assertEqual(alternative["description"], "Updated description.")


class TestAddOrUpdate(unittest.TestCase):
    def test_adds_alternative_if_it_doesnt_exist(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            mapped_table = db_map.mapped_table("alternative")
            alternative = db_map.add_or_update(mapped_table, name="new", description="New alternative.")
            self.assertEqual(alternative["name"], "new")
            self.assertEqual(alternative["description"], "New alternative.")

    def test_updates_existing_alternative(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            added = db_map.add_alternative(name="new", description="New alternative.")
            mapped_table = db_map.mapped_table("alternative")
            updated = db_map.add_or_update(mapped_table, name="new", description="Modified description.")
            self.assertEqual(updated["name"], "new")
            self.assertEqual(updated["description"], "Modified description.")
            self.assertEqual(added["name"], "new")
            self.assertEqual(added["description"], "Modified description.")


class TestAddOrUpdateByType(unittest.TestCase):
    def test_adds_alternative_if_it_doesnt_exist(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            alternative = db_map.add_or_update_by_type("alternative", name="new", description="New alternative.")
            self.assertEqual(alternative["name"], "new")
            self.assertEqual(alternative["description"], "New alternative.")


class TestRemove(unittest.TestCase):
    def test_removes_item(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_alternative(name="new")
            mapped_table = db_map.mapped_table("alternative")
            db_map.remove(mapped_table, name="new")
            alternatives = db_map.find_alternatives(name="new")
            self.assertEqual(alternatives, [])

    def test_raises_when_items_doesnt_exist(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            mapped_table = db_map.mapped_table("alternative")
            with self.assertRaisesRegex(SpineDBAPIError, "no alternative matching {'name': 'non-existent'}"):
                db_map.remove(mapped_table, name="non-existent")


class TestRestore(unittest.TestCase):
    def test_restore_by_id(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            item = db_map.add_scenario(name="new")
            item.remove()
            self.assertFalse(item.is_valid())
            mapped_table = db_map.mapped_table("scenario")
            db_map.restore(mapped_table, id=item["id"])
            self.assertTrue(item.is_valid())

    def test_restore_by_unique_key(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            item = db_map.add_scenario(name="new")
            item.remove()
            self.assertFalse(item.is_valid())
            mapped_table = db_map.mapped_table("scenario")
            db_map.restore(mapped_table, name="new")
            self.assertTrue(item.is_valid())

    def test_raises_if_id_is_not_found(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            item = db_map.add_scenario(name="new")
            item.remove()
            self.assertFalse(item.is_valid())
            mapped_table = db_map.mapped_table("scenario")
            with self.assertRaisesRegex(SpineDBAPIError, "failed to restore item"):
                db_map.restore(mapped_table, id=99)

    def test_raises_if_unique_key_is_wrong(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            item = db_map.add_scenario(name="new")
            item.remove()
            self.assertFalse(item.is_valid())
            mapped_table = db_map.mapped_table("scenario")
            with self.assertRaisesRegex(SpineDBAPIError, "no scenario matching {'name': 'non-existent'}"):
                db_map.restore(mapped_table, name="non-existent")


if __name__ == "__main__":
    unittest.main()
