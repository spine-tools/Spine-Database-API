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
""" Unit tests for DatabaseMapping class. """
import os.path
from tempfile import TemporaryDirectory
import unittest
import threading
import multiprocessing
from unittest import mock
from unittest.mock import patch
from sqlalchemy.engine.url import make_url, URL
from sqlalchemy.util import KeyedTuple
from spinedb_api import (
    DatabaseMapping,
    import_functions,
    from_database,
    to_database,
    SpineDBAPIError,
    SpineIntegrityError,
)
from spinedb_api.helpers import Asterisk, name_from_elements
from tests.custom_db_mapping import CustomDatabaseMapping


def create_query_wrapper(db_map):
    def query_wrapper(*args, orig_query=db_map.query, **kwargs):
        arg = args[0]
        if isinstance(arg, mock.Mock):
            return arg.value
        return orig_query(*args, **kwargs)

    return query_wrapper


IN_MEMORY_DB_URL = "sqlite://"


class TestDatabaseMappingConstruction(unittest.TestCase):
    def test_construction_with_filters(self):
        db_url = IN_MEMORY_DB_URL + "?spinedbfilter=fltr1&spinedbfilter=fltr2"
        with mock.patch("spinedb_api.db_mapping.apply_filter_stack") as mock_apply:
            with mock.patch(
                "spinedb_api.db_mapping.load_filters", return_value=[{"fltr1": "config1", "fltr2": "config2"}]
            ) as mock_load:
                db_map = CustomDatabaseMapping(db_url, create=True)
                db_map.close()
                mock_load.assert_called_once_with(["fltr1", "fltr2"])
                mock_apply.assert_called_once_with(db_map, [{"fltr1": "config1", "fltr2": "config2"}])

    def test_construction_with_sqlalchemy_url_and_filters(self):
        db_url = IN_MEMORY_DB_URL + "/?spinedbfilter=fltr1&spinedbfilter=fltr2"
        sa_url = make_url(db_url)
        with mock.patch("spinedb_api.db_mapping.apply_filter_stack") as mock_apply:
            with mock.patch(
                "spinedb_api.db_mapping.load_filters", return_value=[{"fltr1": "config1", "fltr2": "config2"}]
            ) as mock_load:
                db_map = CustomDatabaseMapping(sa_url, create=True)
                db_map.close()
                mock_load.assert_called_once_with(["fltr1", "fltr2"])
                mock_apply.assert_called_once_with(db_map, [{"fltr1": "config1", "fltr2": "config2"}])

    def test_shorthand_filter_query_works(self):
        with TemporaryDirectory() as temp_dir:
            url = URL("sqlite")
            url.database = os.path.join(temp_dir, "test_shorthand_filter_query_works.json")
            out_db_map = CustomDatabaseMapping(url, create=True)
            out_db_map.add_scenarios({"name": "scen1"})
            out_db_map.add_scenario_alternatives({"scenario_name": "scen1", "alternative_name": "Base", "rank": 1})
            out_db_map.commit_session("Add scen.")
            out_db_map.close()
            try:
                db_map = CustomDatabaseMapping(url)
            except:
                self.fail("CustomDatabaseMapping.__init__() should not raise.")
            else:
                db_map.close()


class AssertSuccessTestCase(unittest.TestCase):
    def _assert_success(self, result):
        item, error = result
        self.assertIsNone(error)
        return item


class TestDatabaseMapping(AssertSuccessTestCase):
    def test_active_by_default_is_initially_false_for_zero_dimensional_entity_class(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            item = self._assert_success(db_map.add_entity_class_item(name="Entity"))
            self.assertFalse(item["active_by_default"])

    def test_active_by_default_is_initially_false_for_multi_dimensional_entity_class(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class_item(name="Dimension")
            item = self._assert_success(db_map.add_entity_class_item(name="Entity", dimension_name_list=("Dimension",)))
            self.assertTrue(item["active_by_default"])

    def test_read_active_by_default_from_database(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "database.sqlite")
            with DatabaseMapping(url, create=True) as out_db_map:
                self._assert_success(out_db_map.add_entity_class_item(name="HiddenStuff", active_by_default=False))
                self._assert_success(out_db_map.add_entity_class_item(name="VisibleStuff", active_by_default=True))
                out_db_map.commit_session("Add entity classes.")
                entity_classes = out_db_map.query(out_db_map.wide_entity_class_sq).all()
                self.assertEqual(len(entity_classes), 2)
                activities = ((row.name, row.active_by_default) for row in entity_classes)
                expected = (("HiddenStuff", False), ("VisibleStuff", True))
                self.assertCountEqual(activities, expected)
            with DatabaseMapping(url) as db_map:
                entity_classes = db_map.get_entity_class_items()
                self.assertEqual(len(entity_classes), 2)
                active_by_default = {c["name"]: c["active_by_default"] for c in entity_classes}
                expected = {"HiddenStuff": False, "VisibleStuff": True}
                for name, activity in active_by_default.items():
                    expected_activity = expected.pop(name)
                    with self.subTest(class_name=name):
                        self.assertEqual(activity, expected_activity)

    def test_commit_parameter_value(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "database.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_item("entity_class", name="fish", description="It swims."))
                self._assert_success(
                    db_map.add_item(
                        "entity", entity_class_name="fish", name="Nemo", description="Peacefully swimming away."
                    )
                )
                self._assert_success(db_map.add_item("parameter_definition", entity_class_name="fish", name="color"))
                value, type_ = to_database("mainly orange")
                self._assert_success(
                    db_map.add_item(
                        "parameter_value",
                        entity_class_name="fish",
                        entity_byname=("Nemo",),
                        parameter_definition_name="color",
                        alternative_name="Base",
                        value=value,
                        type=type_,
                    )
                )
                db_map.commit_session("Added data")
            with DatabaseMapping(url) as db_map:
                color = db_map.get_item(
                    "parameter_value",
                    entity_class_name="fish",
                    entity_byname=("Nemo",),
                    parameter_definition_name="color",
                    alternative_name="Base",
                )
                value = from_database(color["value"], color["type"])
                self.assertEqual(value, "mainly orange")

    def test_commit_multidimensional_parameter_value(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "database.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_item("entity_class", name="fish", description="It swims."))
                self._assert_success(db_map.add_item("entity_class", name="cat", description="Eats fish."))
                self._assert_success(
                    db_map.add_item(
                        "entity_class",
                        name="fish__cat",
                        dimension_name_list=("fish", "cat"),
                        description="A fish getting eaten by a cat?",
                    )
                )
                self._assert_success(
                    db_map.add_item("entity", entity_class_name="fish", name="Nemo", description="Lost (soon).")
                )
                self._assert_success(
                    db_map.add_item(
                        "entity", entity_class_name="cat", name="Felix", description="The wonderful wonderful cat."
                    )
                )
                self._assert_success(
                    db_map.add_item("entity", entity_class_name="fish__cat", element_name_list=("Nemo", "Felix"))
                )
                self._assert_success(
                    db_map.add_item("parameter_definition", entity_class_name="fish__cat", name="rate")
                )
                value, type_ = to_database(0.23)
                self._assert_success(
                    db_map.add_item(
                        "parameter_value",
                        entity_class_name="fish__cat",
                        entity_byname=("Nemo", "Felix"),
                        parameter_definition_name="rate",
                        alternative_name="Base",
                        value=value,
                        type=type_,
                    )
                )
                db_map.commit_session("Added data")
            with DatabaseMapping(url) as db_map:
                color = db_map.get_item(
                    "parameter_value",
                    entity_class_name="fish__cat",
                    entity_byname=("Nemo", "Felix"),
                    parameter_definition_name="rate",
                    alternative_name="Base",
                )
                value = from_database(color["value"], color["type"])
                self.assertEqual(value, 0.23)

    def test_updating_entity_name_updates_the_name_in_parameter_value_too(self):
        with DatabaseMapping(IN_MEMORY_DB_URL, create=True) as db_map:
            self._assert_success(db_map.add_item("entity_class", name="fish", description="It swims."))
            self._assert_success(
                db_map.add_item(
                    "entity", entity_class_name="fish", name="Nemo", description="Peacefully swimming away."
                )
            )
            self._assert_success(db_map.add_item("parameter_definition", entity_class_name="fish", name="color"))
            value, type_ = to_database("mainly orange")
            self._assert_success(
                db_map.add_item(
                    "parameter_value",
                    entity_class_name="fish",
                    entity_byname=("Nemo",),
                    parameter_definition_name="color",
                    alternative_name="Base",
                    value=value,
                    type=type_,
                )
            )
            color = db_map.get_item(
                "parameter_value",
                entity_class_name="fish",
                entity_byname=("Nemo",),
                parameter_definition_name="color",
                alternative_name="Base",
            )
            self.assertIsNotNone(color)
            fish = db_map.get_item("entity", entity_class_name="fish", name="Nemo")
            self.assertIsNotNone(fish)
            fish.update(name="NotNemo")
            self.assertEqual(fish["name"], "NotNemo")
            not_color_anymore = db_map.get_item(
                "parameter_value",
                entity_class_name="fish",
                entity_byname=("Nemo",),
                parameter_definition_name="color",
                alternative_name="Base",
            )
            self.assertEqual(not_color_anymore, {})
            color = db_map.get_item(
                "parameter_value",
                entity_class_name="fish",
                entity_byname=("NotNemo",),
                parameter_definition_name="color",
                alternative_name="Base",
            )
            self.assertIsNotNone(color)

    def test_update_entity_metadata_by_changing_its_entity(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="my_class"))
            db_map.add_entity_item(name="entity_1", entity_class_name="my_class")
            entity_2 = self._assert_success(db_map.add_entity_item(name="entity_2", entity_class_name="my_class"))
            metadata_value = '{"sources": [], "contributors": []}'
            metadata = self._assert_success(db_map.add_metadata_item(name="my_metadata", value=metadata_value))
            entity_metadata = self._assert_success(
                db_map.add_entity_metadata_item(
                    metadata_name="my_metadata",
                    metadata_value=metadata_value,
                    entity_class_name="my_class",
                    entity_byname=("entity_1",),
                )
            )
            entity_metadata.update(entity_byname=("entity_2",))
            self.assertEqual(
                entity_metadata._extended(),
                {
                    "entity_class_name": "my_class",
                    "entity_byname": ("entity_2",),
                    "entity_id": entity_2["id"],
                    "id": entity_metadata["id"],
                    "metadata_id": metadata["id"],
                    "metadata_name": "my_metadata",
                    "metadata_value": metadata_value,
                },
            )
            db_map.commit_session("Add initial data.")
            entity_sq = (
                db_map.query(
                    db_map.entity_sq.c.id.label("entity_id"),
                    db_map.entity_class_sq.c.name.label("entity_class_name"),
                    db_map.entity_sq.c.name.label("entity_name"),
                )
                .join(db_map.entity_class_sq, db_map.entity_class_sq.c.id == db_map.entity_sq.c.class_id)
                .subquery()
            )
            metadata_records = (
                db_map.query(
                    db_map.entity_metadata_sq.c.id,
                    entity_sq.c.entity_class_name,
                    entity_sq.c.entity_name,
                    db_map.metadata_sq.c.name.label("metadata_name"),
                    db_map.metadata_sq.c.value.label("metadata_value"),
                )
                .join(entity_sq, entity_sq.c.entity_id == db_map.entity_metadata_sq.c.entity_id)
                .join(db_map.metadata_sq, db_map.metadata_sq.c.id == db_map.entity_metadata_sq.c.metadata_id)
                .all()
            )
            self.assertEqual(len(metadata_records), 1)
            self.assertEqual(
                dict(**metadata_records[0]),
                {
                    "id": 1,
                    "entity_class_name": "my_class",
                    "entity_name": "entity_2",
                    "metadata_name": "my_metadata",
                    "metadata_value": metadata_value,
                },
            )

    def test_update_parameter_value_metadata_by_changing_its_parameter(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            entity_class = self._assert_success(db_map.add_entity_class_item(name="my_class"))
            self._assert_success(db_map.add_parameter_definition_item(name="x", entity_class_name="my_class"))
            self._assert_success(db_map.add_parameter_definition_item(name="y", entity_class_name="my_class"))
            self._assert_success(db_map.add_entity_item(name="my_entity", entity_class_name="my_class"))
            value, value_type = to_database(2.3)
            self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="my_class",
                    entity_byname=("my_entity",),
                    parameter_definition_name="x",
                    alternative_name="Base",
                    value=value,
                    type=value_type,
                )
            )
            value, value_type = to_database(-2.3)
            y = self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="my_class",
                    entity_byname=("my_entity",),
                    parameter_definition_name="y",
                    alternative_name="Base",
                    value=value,
                    type=value_type,
                )
            )
            metadata_value = '{"sources": [], "contributors": []}'
            metadata = self._assert_success(db_map.add_metadata_item(name="my_metadata", value=metadata_value))
            value_metadata = self._assert_success(
                db_map.add_parameter_value_metadata_item(
                    metadata_name="my_metadata",
                    metadata_value=metadata_value,
                    entity_class_name="my_class",
                    entity_byname=("my_entity",),
                    parameter_definition_name="x",
                    alternative_name="Base",
                )
            )
            value_metadata.update(parameter_definition_name="y")
            self.assertEqual(
                value_metadata._extended(),
                {
                    "entity_class_name": "my_class",
                    "entity_byname": ("my_entity",),
                    "alternative_name": "Base",
                    "parameter_definition_name": "y",
                    "parameter_value_id": y["id"],
                    "id": value_metadata["id"],
                    "metadata_id": metadata["id"],
                    "metadata_name": "my_metadata",
                    "metadata_value": metadata_value,
                },
            )
            db_map.commit_session("Add initial data.")
            parameter_sq = (
                db_map.query(
                    db_map.parameter_value_sq.c.id.label("value_id"),
                    db_map.entity_class_sq.c.name.label("entity_class_name"),
                    db_map.entity_sq.c.name.label("entity_name"),
                    db_map.parameter_definition_sq.c.name.label("parameter_definition_name"),
                    db_map.alternative_sq.c.name.label("alternative_name"),
                )
                .join(
                    db_map.entity_class_sq, db_map.entity_class_sq.c.id == db_map.parameter_value_sq.c.entity_class_id
                )
                .join(db_map.entity_sq, db_map.entity_sq.c.id == db_map.parameter_value_sq.c.entity_id)
                .join(
                    db_map.parameter_definition_sq,
                    db_map.parameter_definition_sq.c.id == db_map.parameter_value_sq.c.parameter_definition_id,
                )
                .join(db_map.alternative_sq, db_map.alternative_sq.c.id == db_map.parameter_value_sq.c.alternative_id)
                .subquery("parameter_sq")
            )
            metadata_records = (
                db_map.query(
                    db_map.parameter_value_metadata_sq.c.id,
                    parameter_sq.c.entity_class_name,
                    parameter_sq.c.entity_name,
                    parameter_sq.c.parameter_definition_name,
                    parameter_sq.c.alternative_name,
                    db_map.metadata_sq.c.name.label("metadata_name"),
                    db_map.metadata_sq.c.value.label("metadata_value"),
                )
                .join(parameter_sq, parameter_sq.c.value_id == db_map.parameter_value_metadata_sq.c.parameter_value_id)
                .join(db_map.metadata_sq, db_map.metadata_sq.c.id == db_map.parameter_value_metadata_sq.c.metadata_id)
                .all()
            )
            self.assertEqual(len(metadata_records), 1)
            self.assertEqual(
                dict(**metadata_records[0]),
                {
                    "id": 1,
                    "entity_class_name": "my_class",
                    "entity_name": "my_entity",
                    "parameter_definition_name": "y",
                    "alternative_name": "Base",
                    "metadata_name": "my_metadata",
                    "metadata_value": metadata_value,
                },
            )

    def test_fetch_more(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            alternatives = db_map.fetch_more("alternative")
            expected = [{"id": 1, "name": "Base", "description": "Base alternative", "commit_id": 1}]
            self.assertEqual([a.resolve() for a in alternatives], expected)

    def test_fetch_more_after_commit_and_refresh(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_item("entity_class", name="Widget"))
            self._assert_success(db_map.add_item("entity", entity_class_name="Widget", name="gadget"))
            db_map.commit_session("Add test data.")
            db_map.refresh_session()
            entities = db_map.fetch_more("entity")
            self.assertEqual([(x["entity_class_name"], x["name"]) for x in entities], [("Widget", "gadget")])

    def test_has_external_commits_returns_true_initially(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.assertTrue(db_map.has_external_commits())

    def test_has_external_commits_returns_true_when_another_db_mapping_has_made_commits(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                with DatabaseMapping(url) as other_db_map:
                    self._assert_success(other_db_map.add_item("entity_class", name="cc"))
                    other_db_map.commit_session("Added a class")
                self.assertTrue(db_map.has_external_commits())

    def test_has_external_commits_returns_false_after_commit_session(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                with DatabaseMapping(url) as other_db_map:
                    other_db_map.add_item("entity_class", name="cc")
                    other_db_map.commit_session("Added a class")
                self._assert_success(db_map.add_item("entity_class", name="omega"))
                db_map.commit_session("Added a class")
                self.assertFalse(db_map.has_external_commits())

    def test_get_items_gives_commits(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            items = db_map.get_items("commit")
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0].item_type, "commit")

    def test_fetch_entities_that_refer_to_unfetched_entities(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="dog"))
                self._assert_success(db_map.add_entity_class_item(name="cat"))
                self._assert_success(db_map.add_entity_class_item(name="dog__cat", dimension_name_list=("dog", "cat")))
                self._assert_success(db_map.add_entity_item(name="Pulgoso", entity_class_name="dog"))
                self._assert_success(db_map.add_entity_item(name="Sylvester", entity_class_name="cat"))
                self._assert_success(db_map.add_entity_item(name="Tom", entity_class_name="cat"))
                db_map.commit_session("Arf!")
            with DatabaseMapping(url) as db_map:
                # Remove the entity in the middle and add a multi-D one referring to the third entity.
                # The multi-D one will go in the middle.
                db_map.get_entity_item(name="Sylvester", entity_class_name="cat").remove()
                self._assert_success(
                    db_map.add_entity_item(element_name_list=("Pulgoso", "Tom"), entity_class_name="dog__cat")
                )
                db_map.commit_session("Meow!")
            with DatabaseMapping(url) as db_map:
                # The ("Pulgoso", "Tom") entity will be fetched before "Tom".
                # What happens?
                entities = db_map.get_items("entity")
                self.assertEqual(len(entities), 3)

    def test_committing_scenario_alternatives(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                item = self._assert_success(db_map.add_alternative_item(name="alt1"))
                self.assertIsNotNone(item)
                item = self._assert_success(db_map.add_alternative_item(name="alt2"))
                self.assertIsNotNone(item)
                item = self._assert_success(db_map.add_scenario_item(name="my_scenario"))
                self.assertIsNotNone(item)
                item = self._assert_success(
                    db_map.add_scenario_alternative_item(scenario_name="my_scenario", alternative_name="alt1", rank=0)
                )
                self.assertIsNotNone(item)
                item = self._assert_success(
                    db_map.add_scenario_alternative_item(scenario_name="my_scenario", alternative_name="alt2", rank=1)
                )
                self.assertIsNotNone(item)
                db_map.commit_session("Add test data.")
            with DatabaseMapping(url) as db_map:
                scenario_alternatives = db_map.get_items("scenario_alternative")
                self.assertEqual(len(scenario_alternatives), 2)
                self.assertEqual(scenario_alternatives[0]["scenario_name"], "my_scenario")
                self.assertEqual(scenario_alternatives[0]["alternative_name"], "alt1")
                self.assertEqual(scenario_alternatives[0]["rank"], 0)
                self.assertEqual(scenario_alternatives[1]["scenario_name"], "my_scenario")
                self.assertEqual(scenario_alternatives[1]["alternative_name"], "alt2")
                self.assertEqual(scenario_alternatives[1]["rank"], 1)

    def test_committing_entity_class_items_doesnt_add_commit_ids_to_them(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="my_class"))
            db_map.commit_session("Add class.")
            classes = db_map.get_entity_class_items()
            self.assertEqual(len(classes), 1)
            self.assertNotIn("commit_id", classes[0]._extended())

    def test_committing_superclass_subclass_items_doesnt_add_commit_ids_to_them(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="high"))
            self._assert_success(db_map.add_entity_class_item(name="low"))
            self._assert_success(db_map.add_superclass_subclass_item(superclass_name="high", subclass_name="low"))
            db_map.commit_session("Add class hierarchy.")
            classes = db_map.get_superclass_subclass_items()
            self.assertEqual(len(classes), 1)
            self.assertNotIn("commit_id", classes[0]._extended())

    def test_committing_entity_group_items_doesnt_add_commit_ids_to_them(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="my_class"))
            self._assert_success(db_map.add_entity_item(name="element", entity_class_name="my_class"))
            self._assert_success(db_map.add_entity_item(name="container", entity_class_name="my_class"))
            self._assert_success(
                db_map.add_entity_group_item(
                    group_name="container", member_name="element", entity_class_name="my_class"
                )
            )
            db_map.commit_session("Add entity group.")
            groups = db_map.get_entity_group_items()
            self.assertEqual(len(groups), 1)
            self.assertNotIn("commit_id", groups[0]._extended())

    def test_commit_parameter_value_coincidentally_called_is_active(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_parameter_value_list_item(name="booleans"))
            value, value_type = to_database(True)
            self._assert_success(
                db_map.add_list_value_item(parameter_value_list_name="booleans", value=value, type=value_type, index=0)
            )
            self._assert_success(db_map.add_entity_class_item(name="my_class"))
            self._assert_success(
                db_map.add_parameter_definition_item(
                    name="is_active", entity_class_name="my_class", parameter_value_list_name="booleans"
                )
            )
            self._assert_success(db_map.add_entity_item(name="widget1", entity_class_name="my_class"))
            self._assert_success(db_map.add_entity_item(name="widget2", entity_class_name="my_class"))
            self._assert_success(db_map.add_entity_item(name="no_is_active", entity_class_name="my_class"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_class_name="my_class", entity_byname=("widget1",), alternative_name="Base", active=False
                )
            )
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_class_name="my_class", entity_byname=("widget2",), alternative_name="Base", active=False
                )
            )
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_class_name="my_class", entity_byname=("no_is_active",), alternative_name="Base", active=False
                )
            )
            value, value_type = to_database(True)
            self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="my_class",
                    parameter_definition_name="is_active",
                    entity_byname=("widget1",),
                    alternative_name="Base",
                    value=value,
                    type=value_type,
                )
            )
            self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="my_class",
                    parameter_definition_name="is_active",
                    entity_byname=("widget2",),
                    alternative_name="Base",
                    value=value,
                    type=value_type,
                )
            )
            db_map.commit_session("Add test data to see if this crashes.")
            entity_names = {entity["id"]: entity["name"] for entity in db_map.query(db_map.wide_entity_sq)}
            alternative_names = {
                alternative["id"]: alternative["name"] for alternative in db_map.query(db_map.alternative_sq)
            }
            expected = {
                ("widget1", "Base"): True,
                ("widget2", "Base"): True,
                ("no_is_active", "Base"): False,
            }
            in_database = {}
            entity_alternatives = db_map.query(db_map.entity_alternative_sq)
            for entity_alternative in entity_alternatives:
                entity_name = entity_names[entity_alternative["entity_id"]]
                alternative_name = alternative_names[entity_alternative["alternative_id"]]
                in_database[(entity_name, alternative_name)] = entity_alternative["active"]
            self.assertEqual(in_database, expected)
            self.assertEqual(db_map.query(db_map.parameter_value_sq).all(), [])

    def test_commit_default_value_for_parameter_called_is_active(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_parameter_value_list_item(name="booleans"))
            value, value_type = to_database(True)
            self._assert_success(
                db_map.add_list_value_item(parameter_value_list_name="booleans", value=value, type=value_type, index=0)
            )
            self._assert_success(db_map.add_entity_class_item(name="Widget"))
            self._assert_success(
                db_map.add_parameter_definition_item(
                    name="is_active",
                    entity_class_name="Widget",
                    parameter_value_list_name="booleans",
                    default_value=value,
                    default_type=value_type,
                )
            )
            self._assert_success(db_map.add_entity_class_item(name="Gadget"))
            self._assert_success(
                db_map.add_parameter_definition_item(
                    name="is_active",
                    entity_class_name="Gadget",
                    parameter_value_list_name="booleans",
                    default_value=value,
                    default_type=value_type,
                )
            )
            self._assert_success(db_map.add_entity_class_item(name="NoIsActiveDefault"))
            self._assert_success(
                db_map.add_parameter_definition_item(
                    name="is_active", entity_class_name="NoIsActiveDefault", parameter_value_list_name="booleans"
                )
            )
            db_map.commit_session("Add test data to see if this crashes")
            active_by_defaults = {
                entity_class["name"]: entity_class["active_by_default"]
                for entity_class in db_map.query(db_map.wide_entity_class_sq)
            }
            self.assertEqual(active_by_defaults, {"Widget": True, "Gadget": True, "NoIsActiveDefault": False})
            defaults = [
                from_database(definition["default_value"], definition["default_type"])
                for definition in db_map.query(db_map.parameter_definition_sq)
            ]
            self.assertEqual(defaults, 3 * [None])

    def test_remove_items_by_asterisk(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_alternative_item(name="alt_1"))
            self._assert_success(db_map.add_alternative_item(name="alt_2"))
            db_map.commit_session("Add alternatives.")
            alternatives = db_map.get_alternative_items()
            self.assertEqual(len(alternatives), 3)
            db_map.remove_items("alternative", Asterisk)
            db_map.commit_session("Remove all alternatives.")
            alternatives = db_map.get_alternative_items()
            self.assertEqual(alternatives, [])

    def test_reset_purging(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                db_map.add_entity_class_item(name="Widget")
                db_map.purge_items("entity_class")
                with DatabaseMapping(url) as another_db_map:
                    another_db_map.add_entity_class_item(name="Gadget")
                    another_db_map.commit_session("Add another entity class.")
                db_map.reset_purging()
                entity_classes = db_map.get_entity_class_items()
                self.assertEqual(len(entity_classes), 1)
                self.assertEqual(entity_classes[0]["name"], "Gadget")

    def test_restored_entity_class_item_has_display_icon_field(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            entity_class = self._assert_success(db_map.add_entity_class_item(name="Gadget"))
            db_map.purge_items("entity_class")
            entity_class.restore()
            item = db_map.get_entity_class_item(name="Gadget")
            self.assertIsNone(item["display_icon"])

    def test_trying_to_restore_item_whose_parent_is_removed_fails(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            entity_class = self._assert_success(db_map.add_entity_class_item(name="Object"))
            entity = self._assert_success(db_map.add_entity_item(name="knife", entity_class_name="Object"))
            entity_class.remove()
            self.assertFalse(entity.is_valid())
            entity.restore()
            self.assertFalse(entity.is_valid())
            entity_class.restore()
            self.assertTrue(entity.is_valid())


class TestDatabaseMappingLegacy(unittest.TestCase):
    """'Backward compatibility' tests, i.e. pre-entity tests converted to work with the entity structure."""

    _db_map = None

    @classmethod
    def setUpClass(cls):
        cls._db_map = CustomDatabaseMapping(IN_MEMORY_DB_URL, create=True)

    @classmethod
    def tearDownClass(cls):
        cls._db_map.close()

    def test_construction_with_filters(self):
        db_url = IN_MEMORY_DB_URL + "?spinedbfilter=fltr1&spinedbfilter=fltr2"
        with patch("spinedb_api.db_mapping.apply_filter_stack") as mock_apply:
            with patch(
                "spinedb_api.db_mapping.load_filters", return_value=[{"fltr1": "config1", "fltr2": "config2"}]
            ) as mock_load:
                db_map = CustomDatabaseMapping(db_url, create=True)
                db_map.close()
                mock_load.assert_called_once_with(["fltr1", "fltr2"])
                mock_apply.assert_called_once_with(db_map, [{"fltr1": "config1", "fltr2": "config2"}])

    def test_construction_with_sqlalchemy_url_and_filters(self):
        sa_url = URL("sqlite")
        sa_url.query = {"spinedbfilter": ["fltr1", "fltr2"]}
        with patch("spinedb_api.db_mapping.apply_filter_stack") as mock_apply:
            with patch(
                "spinedb_api.db_mapping.load_filters", return_value=[{"fltr1": "config1", "fltr2": "config2"}]
            ) as mock_load:
                db_map = CustomDatabaseMapping(sa_url, create=True)
                db_map.close()
                mock_load.assert_called_once_with(["fltr1", "fltr2"])
                mock_apply.assert_called_once_with(db_map, [{"fltr1": "config1", "fltr2": "config2"}])

    def test_entity_sq(self):
        columns = ["id", "class_id", "name", "description", "commit_id"]
        self.assertEqual(len(self._db_map.entity_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.entity_sq.c, column_name))

    def test_object_class_sq(self):
        columns = ["id", "name", "description", "display_order", "display_icon", "hidden"]
        self.assertEqual(len(self._db_map.object_class_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.object_class_sq.c, column_name))

    def test_object_sq(self):
        columns = ["id", "class_id", "name", "description", "commit_id"]
        self.assertEqual(len(self._db_map.object_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.object_sq.c, column_name))

    def test_relationship_class_sq(self):
        columns = ["id", "dimension", "object_class_id", "name", "description", "display_icon", "hidden"]
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
            "default_value",
            "default_type",
            "list_value_id",
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
            "value",
            "type",
            "list_value_id",
            "commit_id",
            "alternative_id",
        ]
        self.assertEqual(len(self._db_map.parameter_value_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.parameter_value_sq.c, column_name))

    def test_parameter_value_list_sq(self):
        columns = ["id", "name", "commit_id"]
        self.assertEqual(len(self._db_map.parameter_value_list_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.parameter_value_list_sq.c, column_name))

    def test_ext_object_sq(self):
        columns = ["id", "class_id", "class_name", "name", "description", "group_id", "commit_id"]
        self.assertEqual(len(self._db_map.ext_object_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.ext_object_sq.c, column_name))

    def test_ext_relationship_class_sq(self):
        columns = [
            "id",
            "name",
            "description",
            "display_icon",
            "dimension",
            "object_class_id",
            "object_class_name",
        ]
        self.assertEqual(len(self._db_map.ext_relationship_class_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.ext_relationship_class_sq.c, column_name))

    def test_wide_relationship_class_sq(self):
        columns = ["id", "name", "description", "display_icon", "object_class_id_list", "object_class_name_list"]
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
            "commit_id",
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
            "commit_id",
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
        columns = ["id", "name", "value_index_list", "value_id_list", "commit_id"]
        self.assertEqual(len(self._db_map.wide_parameter_value_list_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.wide_parameter_value_list_sq.c, column_name))

    def test_get_import_alternative_returns_base_alternative_by_default(self):
        alternative_name = self._db_map.get_import_alternative_name()
        self.assertEqual(alternative_name, "Base")


class TestDatabaseMappingQueries(unittest.TestCase):
    def setUp(self):
        self._db_map = CustomDatabaseMapping(IN_MEMORY_DB_URL, create=True)

    def tearDown(self):
        self._db_map.close()

    def create_object_classes(self):
        obj_classes = ["class1", "class2"]
        import_functions.import_object_classes(self._db_map, obj_classes)
        return obj_classes

    def create_objects(self):
        objects = [("class1", "obj11"), ("class1", "obj12"), ("class2", "obj21")]
        import_functions.import_objects(self._db_map, objects)
        return objects

    def create_relationship_classes(self):
        relationship_classes = [("rel1", ["class1"]), ("rel2", ["class1", "class2"])]
        import_functions.import_relationship_classes(self._db_map, relationship_classes)
        return relationship_classes

    def create_relationships(self):
        relationships = [("rel1", ["obj11"]), ("rel2", ["obj11", "obj21"])]
        import_functions.import_relationships(self._db_map, relationships)
        return relationships

    def test_commit_sq_hides_pending_commit(self):
        commits = self._db_map.query(self._db_map.commit_sq).all()
        self.assertEqual(len(commits), 1)

    def test_alternative_sq(self):
        import_functions.import_alternatives(self._db_map, (("alt1", "test alternative"),))
        self._db_map.commit_session("test")
        alternative_rows = self._db_map.query(self._db_map.alternative_sq).all()
        expected_names_and_descriptions = {"Base": "Base alternative", "alt1": "test alternative"}
        self.assertEqual(len(alternative_rows), len(expected_names_and_descriptions))
        for row in alternative_rows:
            self.assertTrue(row.name in expected_names_and_descriptions)
            self.assertEqual(row.description, expected_names_and_descriptions[row.name])
            expected_names_and_descriptions.pop(row.name)
        self.assertEqual(expected_names_and_descriptions, {})

    def test_scenario_sq(self):
        import_functions.import_scenarios(self._db_map, (("scen1", True, "test scenario"),))
        self._db_map.commit_session("test")
        scenario_rows = self._db_map.query(self._db_map.scenario_sq).all()
        self.assertEqual(len(scenario_rows), 1)
        self.assertEqual(scenario_rows[0].name, "scen1")
        self.assertEqual(scenario_rows[0].description, "test scenario")
        self.assertTrue(scenario_rows[0].active)

    def test_ext_linked_scenario_alternative_sq(self):
        import_functions.import_scenarios(self._db_map, (("scen1", True),))
        import_functions.import_alternatives(self._db_map, ("alt1", "alt2", "alt3"))
        import_functions.import_scenario_alternatives(self._db_map, (("scen1", "alt2"),))
        import_functions.import_scenario_alternatives(self._db_map, (("scen1", "alt3"),))
        import_functions.import_scenario_alternatives(self._db_map, (("scen1", "alt1"),))
        self._db_map.commit_session("test")
        scenario_alternative_rows = self._db_map.query(self._db_map.ext_linked_scenario_alternative_sq).all()
        self.assertEqual(len(scenario_alternative_rows), 3)
        expected_befores = {"alt2": "alt3", "alt3": "alt1", "alt1": None}
        expected_ranks = {"alt2": 1, "alt3": 2, "alt1": 3}
        for row in scenario_alternative_rows:
            self.assertEqual(row.scenario_name, "scen1")
            self.assertIn(row.alternative_name, expected_befores)
            self.assertEqual(row.rank, expected_ranks[row.alternative_name])
            expected_before_alternative = expected_befores.pop(row.alternative_name)
            self.assertEqual(row.before_alternative_name, expected_before_alternative)
            if expected_before_alternative is not None:
                self.assertIsNotNone(row.before_alternative_id)
                self.assertEqual(row.before_rank, expected_ranks[row.before_alternative_name])
            else:
                self.assertIsNone(row.before_alternative_id)
                self.assertIsNone(row.before_rank)
        self.assertEqual(expected_befores, {})

    def test_entity_class_sq(self):
        obj_classes = self.create_object_classes()
        relationship_classes = self.create_relationship_classes()
        self._db_map.commit_session("test")
        results = self._db_map.query(self._db_map.entity_class_sq).all()
        # Check that number of results matches total entities
        self.assertEqual(len(results), len(obj_classes) + len(relationship_classes))
        # Check result values
        for row, class_name in zip(results, obj_classes + [rel[0] for rel in relationship_classes]):
            self.assertEqual(row.name, class_name)

    def test_entity_sq(self):
        self.create_object_classes()
        objects = self.create_objects()
        self.create_relationship_classes()
        relationships = self.create_relationships()
        self._db_map.commit_session("test")
        entity_rows = self._db_map.query(self._db_map.entity_sq).all()
        self.assertEqual(len(entity_rows), len(objects) + len(relationships))
        object_names = [o[1] for o in objects]
        relationship_names = [name_from_elements(r[1]) for r in relationships]
        for row, expected_name in zip(entity_rows, object_names + relationship_names):
            self.assertEqual(row.name, expected_name)

    def test_object_class_sq_picks_object_classes_only(self):
        obj_classes = self.create_object_classes()
        self.create_relationship_classes()
        self._db_map.commit_session("test")
        class_rows = self._db_map.query(self._db_map.object_class_sq).all()
        self.assertEqual(len(class_rows), len(obj_classes))
        for row, expected_name in zip(class_rows, obj_classes):
            self.assertEqual(row.name, expected_name)

    def test_object_sq_picks_objects_only(self):
        self.create_object_classes()
        objects = self.create_objects()
        self.create_relationship_classes()
        self.create_relationships()
        self._db_map.commit_session("test")
        object_rows = self._db_map.query(self._db_map.object_sq).all()
        self.assertEqual(len(object_rows), len(objects))
        for row, expected_object in zip(object_rows, objects):
            self.assertEqual(row.name, expected_object[1])

    def test_wide_relationship_class_sq(self):
        self.create_object_classes()
        relationship_classes = self.create_relationship_classes()
        self._db_map.commit_session("test")
        class_rows = self._db_map.query(self._db_map.wide_relationship_class_sq).all()
        self.assertEqual(len(class_rows), 2)
        for row, relationship_class in zip(class_rows, relationship_classes):
            self.assertEqual(row.name, relationship_class[0])
            self.assertEqual(row.object_class_name_list, ",".join(relationship_class[1]))

    def test_wide_relationship_sq(self):
        self.create_object_classes()
        self.create_objects()
        relationship_classes = self.create_relationship_classes()
        object_classes = {rel_class[0]: rel_class[1] for rel_class in relationship_classes}
        relationships = self.create_relationships()
        self._db_map.commit_session("test")
        relationship_rows = self._db_map.query(self._db_map.wide_relationship_sq).all()
        self.assertEqual(len(relationship_rows), 2)
        for row, relationship in zip(relationship_rows, relationships):
            self.assertEqual(row.name, name_from_elements(relationship[1]))
            self.assertEqual(row.class_name, relationship[0])
            self.assertEqual(row.object_class_name_list, ",".join(object_classes[relationship[0]]))
            self.assertEqual(row.object_name_list, ",".join(relationship[1]))

    def test_parameter_definition_sq_for_object_class(self):
        self.create_object_classes()
        import_functions.import_object_parameters(self._db_map, (("class1", "par1"),))
        self._db_map.commit_session("test")
        definition_rows = self._db_map.query(self._db_map.parameter_definition_sq).all()
        self.assertEqual(len(definition_rows), 1)
        self.assertEqual(definition_rows[0].name, "par1")
        self.assertIsNotNone(definition_rows[0].entity_class_id)

    def test_parameter_definition_sq_for_relationship_class(self):
        self.create_object_classes()
        self.create_relationship_classes()
        import_functions.import_relationship_parameters(self._db_map, (("rel1", "par1"),))
        self._db_map.commit_session("test")
        definition_rows = self._db_map.query(self._db_map.parameter_definition_sq).all()
        self.assertEqual(len(definition_rows), 1)
        self.assertEqual(definition_rows[0].name, "par1")
        self.assertIsNotNone(definition_rows[0].entity_class_id)

    def test_entity_parameter_definition_sq_for_object_class(self):
        self.create_object_classes()
        self.create_relationship_classes()
        import_functions.import_object_parameters(self._db_map, (("class1", "par1"),))
        self._db_map.commit_session("test")
        definition_rows = self._db_map.query(self._db_map.entity_parameter_definition_sq).all()
        self.assertEqual(len(definition_rows), 1)
        self.assertEqual(definition_rows[0].parameter_name, "par1")
        self.assertEqual(definition_rows[0].entity_class_name, "class1")
        self.assertEqual(definition_rows[0].object_class_name, "class1")
        self.assertIsNone(definition_rows[0].relationship_class_id)
        self.assertIsNone(definition_rows[0].relationship_class_name)
        self.assertIsNone(definition_rows[0].object_class_id_list)
        self.assertIsNone(definition_rows[0].object_class_name_list)

    def test_entity_parameter_definition_sq_for_relationship_class(self):
        object_classes = self.create_object_classes()
        self.create_relationship_classes()
        import_functions.import_relationship_parameters(self._db_map, (("rel2", "par1"),))
        self._db_map.commit_session("test")
        definition_rows = self._db_map.query(self._db_map.entity_parameter_definition_sq).all()
        self.assertEqual(len(definition_rows), 1)
        self.assertEqual(definition_rows[0].parameter_name, "par1")
        self.assertEqual(definition_rows[0].entity_class_name, "rel2")
        self.assertIsNotNone(definition_rows[0].relationship_class_id)
        self.assertEqual(definition_rows[0].relationship_class_name, "rel2")
        self.assertIsNotNone(definition_rows[0].object_class_id_list)
        self.assertEqual(definition_rows[0].object_class_name_list, ",".join(object_classes))
        self.assertIsNone(definition_rows[0].object_class_name)

    def test_entity_parameter_definition_sq_with_multiple_relationship_classes_but_single_parameter(self):
        self.create_object_classes()
        self.create_relationship_classes()
        obj_parameter_definitions = [("class1", "par1a"), ("class1", "par1b")]
        rel_parameter_definitions = [("rel1", "rpar1a")]
        import_functions.import_object_parameters(self._db_map, obj_parameter_definitions)
        import_functions.import_relationship_parameters(self._db_map, rel_parameter_definitions)
        self._db_map.commit_session("test")
        results = self._db_map.query(self._db_map.entity_parameter_definition_sq).all()
        # Check that number of results matches total entities
        self.assertEqual(len(results), len(obj_parameter_definitions) + len(rel_parameter_definitions))
        # Check result values
        for row, par_def in zip(results, obj_parameter_definitions + rel_parameter_definitions):
            self.assertTupleEqual((row.entity_class_name, row.parameter_name), par_def)

    def test_entity_parameter_values(self):
        self.create_object_classes()
        self.create_objects()
        self.create_relationship_classes()
        self.create_relationships()
        obj_parameter_definitions = [("class1", "par1a"), ("class1", "par1b"), ("class2", "par2a")]
        rel_parameter_definitions = [("rel1", "rpar1a"), ("rel2", "rpar2a")]
        import_functions.import_object_parameters(self._db_map, obj_parameter_definitions)
        import_functions.import_relationship_parameters(self._db_map, rel_parameter_definitions)
        object_parameter_values = [
            ("class1", "obj11", "par1a", 123),
            ("class1", "obj11", "par1b", 333),
            ("class2", "obj21", "par2a", "empty"),
        ]
        _, errors = import_functions.import_object_parameter_values(self._db_map, object_parameter_values)
        self.assertFalse(errors)
        relationship_parameter_values = [("rel1", ["obj11"], "rpar1a", 1.1), ("rel2", ["obj11", "obj21"], "rpar2a", 42)]
        _, errors = import_functions.import_relationship_parameter_values(self._db_map, relationship_parameter_values)
        self.assertFalse(errors)
        self._db_map.commit_session("test")
        results = self._db_map.query(self._db_map.entity_parameter_value_sq).all()
        # Check that number of results matches total entities
        self.assertEqual(len(results), len(object_parameter_values) + len(relationship_parameter_values))
        # Check result values
        for row, par_val in zip(results, object_parameter_values + relationship_parameter_values):
            self.assertEqual(row.entity_class_name, par_val[0])
            if row.object_name:  # This is an object parameter
                self.assertEqual(row.object_name, par_val[1])
            else:  # This is a relationship parameter
                self.assertEqual(row.object_name_list, ",".join(par_val[1]))
            self.assertEqual(row.parameter_name, par_val[2])
            self.assertEqual(from_database(row.value, row.type), par_val[3])

    def test_wide_parameter_value_list_sq(self):
        _, errors = import_functions.import_parameter_value_lists(
            self._db_map, (("list1", "value1"), ("list1", "value2"), ("list2", "valueA"))
        )
        self.assertEqual(errors, [])
        self._db_map.commit_session("test")
        value_lists = self._db_map.query(self._db_map.wide_parameter_value_list_sq).all()
        self.assertEqual(len(value_lists), 2)
        self.assertEqual(value_lists[0].name, "list1")
        self.assertEqual(value_lists[1].name, "list2")

    def test_filter_query_accepts_multiple_criteria(self):
        classes, errors = self._db_map.add_entity_classes({"name": "Real"}, {"name": "Fake"})
        self.assertEqual(errors, [])
        self.assertEqual(len(classes), 2)
        self.assertEqual(classes[0]["name"], "Real")
        self.assertEqual(classes[1]["name"], "Fake")
        real_class_id = classes[0]["id"]
        fake_class_id = classes[1]["id"]
        _, errors = self._db_map.add_entities(
            {"name": "entity 1", "class_id": real_class_id},
            {"name": "entity_2", "class_id": real_class_id},
            {"name": "entity_1", "class_id": fake_class_id},
        )
        self.assertEqual(errors, [])
        self._db_map.commit_session("Add test data")
        sq = self._db_map.wide_entity_class_sq
        real_class_id = self._db_map.query(sq).filter(sq.c.name == "Real").one().id
        sq = self._db_map.wide_entity_sq
        entity = self._db_map.query(sq).filter(sq.c.name == "entity 1", sq.c.class_id == 1).one()
        self.assertEqual(entity.name, "entity 1")
        self.assertEqual(entity.class_id, real_class_id)


class TestDatabaseMappingGet(unittest.TestCase):
    def setUp(self):
        self._db_map = CustomDatabaseMapping(IN_MEMORY_DB_URL, create=True)

    def tearDown(self):
        self._db_map.close()

    def test_get_entity_class_items_check_fields(self):
        import_functions.import_data(self._db_map, entity_classes=(("fish",),))
        with self.assertRaises(SpineDBAPIError):
            self._db_map.get_entity_class_item(entity_class_name="fish")
        with self.assertRaises(SpineDBAPIError):
            self._db_map.get_entity_class_item(name=("fish",))
        self._db_map.get_entity_class_item(name="fish")

    def test_get_entity_alternative_items(self):
        import_functions.import_data(
            self._db_map,
            entity_classes=(("fish",),),
            entities=(("fish", "Nemo"),),
            entity_alternatives=(("fish", "Nemo", "Base", True),),
        )
        ea_item = self._db_map.get_entity_alternative_item(
            alternative_name="Base", entity_class_name="fish", entity_byname=("Nemo",)
        )
        self.assertIsNotNone(ea_item)
        ea_items = self._db_map.get_entity_alternative_items(
            alternative_name="Base", entity_class_name="fish", entity_byname=("Nemo",)
        )
        self.assertEqual(len(ea_items), 1)
        self.assertEqual(ea_items[0], ea_item)


class TestDatabaseMappingAdd(unittest.TestCase):
    def setUp(self):
        self._db_map = CustomDatabaseMapping(IN_MEMORY_DB_URL, create=True)

    def tearDown(self):
        self._db_map.close()

    def test_add_and_retrieve_many_objects(self):
        """Tests add many objects into db and retrieving them."""
        items, _ = self._db_map.add_object_classes({"name": "testclass"})
        class_id = next(iter(items))["id"]
        added = self._db_map.add_objects(*[{"name": str(i), "class_id": class_id} for i in range(1001)])[0]
        self.assertEqual(len(added), 1001)
        self._db_map.commit_session("test_commit")
        self.assertEqual(self._db_map.query(self._db_map.entity_sq).count(), 1001)

    def test_add_object_classes(self):
        """Test that adding object classes works."""
        self._db_map.add_object_classes({"name": "fish"}, {"name": "dog"})
        self._db_map.commit_session("add")
        object_classes = self._db_map.query(self._db_map.object_class_sq).all()
        self.assertEqual(len(object_classes), 2)
        self.assertEqual(object_classes[0].name, "fish")
        self.assertEqual(object_classes[1].name, "dog")

    def test_add_object_class_with_invalid_name(self):
        """Test that adding object classes with empty name raises error"""
        with self.assertRaises(SpineIntegrityError):
            self._db_map.add_object_classes({"name": ""}, strict=True)

    def test_add_object_classes_with_same_name(self):
        """Test that adding two object classes with the same name only adds one of them."""
        self._db_map.add_object_classes({"name": "fish"}, {"name": "fish"})
        self._db_map.commit_session("add")
        object_classes = self._db_map.query(self._db_map.object_class_sq).all()
        self.assertEqual(len(object_classes), 1)
        self.assertEqual(object_classes[0].name, "fish")

    def test_add_object_class_with_same_name_as_existing_one(self):
        """Test that adding an object class with an already taken name raises an integrity error."""
        self._db_map.add_object_classes({"name": "fish"}, {"name": "fish"})
        with self.assertRaises(SpineIntegrityError):
            self._db_map.add_object_classes({"name": "fish"}, strict=True)

    def test_add_objects(self):
        """Test that adding objects works."""
        self._db_map.add_object_classes({"name": "fish", "id": 1})
        self._db_map.add_objects({"name": "nemo", "class_id": 1}, {"name": "dory", "class_id": 1})
        self._db_map.commit_session("add")
        objects = self._db_map.query(self._db_map.object_sq).all()
        self.assertEqual(len(objects), 2)
        self.assertEqual(objects[0].name, "nemo")
        self.assertEqual(objects[0].class_id, 1)
        self.assertEqual(objects[1].name, "dory")
        self.assertEqual(objects[1].class_id, 1)

    def test_add_object_with_invalid_name(self):
        """Test that adding object classes with empty name raises error"""
        self._db_map.add_object_classes({"name": "fish"})
        with self.assertRaises(SpineIntegrityError):
            self._db_map.add_objects({"name": "", "entity_class_name": "fish"}, strict=True)

    def test_add_objects_with_same_name(self):
        """Test that adding two objects with the same name only adds one of them."""
        self._db_map.add_object_classes({"name": "fish", "id": 1})
        self._db_map.add_objects({"name": "nemo", "class_id": 1}, {"name": "nemo", "class_id": 1})
        self._db_map.commit_session("add")
        objects = self._db_map.query(self._db_map.object_sq).all()
        self.assertEqual(len(objects), 1)
        self.assertEqual(objects[0].name, "nemo")
        self.assertEqual(objects[0].class_id, 1)

    def test_add_object_with_same_name_as_existing_one(self):
        """Test that adding an object with an already taken name raises an integrity error."""
        self._db_map.add_object_classes({"name": "fish"})
        self._db_map.add_objects({"name": "nemo", "class_id": 1})
        with self.assertRaises(SpineIntegrityError):
            self._db_map.add_objects({"name": "nemo", "class_id": 1}, strict=True)

    def test_add_object_with_invalid_class(self):
        """Test that adding an object with a non existing class raises an integrity error."""
        self._db_map.add_object_classes({"name": "fish"})
        with self.assertRaises(SpineIntegrityError):
            self._db_map.add_objects({"name": "pluto", "class_id": 2}, strict=True)

    def test_add_relationship_classes(self):
        """Test that adding relationship classes works."""
        self._db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self._db_map.add_wide_relationship_classes(
            {"name": "rc1", "object_class_id_list": [1, 2]}, {"name": "rc2", "object_class_id_list": [2, 1]}
        )
        self._db_map.commit_session("add")
        table = self._db_map.get_table("entity_class_dimension")
        ent_cls_dims = self._db_map.query(table).all()
        rel_clss = self._db_map.query(self._db_map.wide_relationship_class_sq).all()
        self.assertEqual(len(ent_cls_dims), 4)
        self.assertEqual(rel_clss[0].name, "rc1")
        self.assertEqual(ent_cls_dims[0].dimension_id, 1)
        self.assertEqual(ent_cls_dims[1].dimension_id, 2)
        self.assertEqual(rel_clss[1].name, "rc2")
        self.assertEqual(ent_cls_dims[2].dimension_id, 2)
        self.assertEqual(ent_cls_dims[3].dimension_id, 1)

    def test_add_relationship_classes_with_invalid_name(self):
        """Test that adding object classes with empty name raises error"""
        self._db_map.add_object_classes({"name": "fish"})
        with self.assertRaises(SpineIntegrityError):
            self._db_map.add_wide_relationship_classes({"name": "", "object_class_id_list": [1]}, strict=True)

    def test_add_relationship_classes_with_same_name(self):
        """Test that adding two relationship classes with the same name only adds one of them."""
        self._db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self._db_map.add_wide_relationship_classes(
            {"name": "rc1", "object_class_id_list": [1, 2]},
            {"name": "rc1", "object_class_id_list": [1, 2]},
            strict=False,
        )
        self._db_map.commit_session("add")
        table = self._db_map.get_table("entity_class_dimension")
        ecs_dims = self._db_map.query(table).all()
        relationship_classes = self._db_map.query(self._db_map.wide_relationship_class_sq).all()
        self.assertEqual(len(ecs_dims), 2)
        self.assertEqual(len(relationship_classes), 1)
        self.assertEqual(relationship_classes[0].name, "rc1")
        self.assertEqual(ecs_dims[0].dimension_id, 1)
        self.assertEqual(ecs_dims[1].dimension_id, 2)

    def test_add_relationship_class_with_same_name_as_existing_one(self):
        """Test that adding a relationship class with an already taken name raises an integrity error."""
        query_wrapper = create_query_wrapper(self._db_map)
        with mock.patch.object(CustomDatabaseMapping, "query") as mock_query, mock.patch.object(
            CustomDatabaseMapping, "object_class_sq"
        ) as mock_object_class_sq, mock.patch.object(
            CustomDatabaseMapping, "wide_relationship_class_sq"
        ) as mock_wide_rel_cls_sq:
            mock_query.side_effect = query_wrapper
            mock_object_class_sq.return_value = [
                KeyedTuple([1, "fish"], labels=["id", "name"]),
                KeyedTuple([2, "dog"], labels=["id", "name"]),
            ]
            mock_wide_rel_cls_sq.return_value = [
                KeyedTuple([1, "1,2", "fish__dog"], labels=["id", "object_class_id_list", "name"])
            ]
            with self.assertRaises(SpineIntegrityError):
                self._db_map.add_wide_relationship_classes(
                    {"name": "fish__dog", "object_class_id_list": [1, 2]}, strict=True
                )

    def test_add_relationship_class_with_invalid_object_class(self):
        """Test that adding a relationship class with a non existing object class raises an integrity error."""
        query_wrapper = create_query_wrapper(self._db_map)
        with mock.patch.object(CustomDatabaseMapping, "query") as mock_query, mock.patch.object(
            CustomDatabaseMapping, "object_class_sq"
        ) as mock_object_class_sq, mock.patch.object(CustomDatabaseMapping, "wide_relationship_class_sq"):
            mock_query.side_effect = query_wrapper
            mock_object_class_sq.return_value = [KeyedTuple([1, "fish"], labels=["id", "name"])]
            with self.assertRaises(SpineIntegrityError):
                self._db_map.add_wide_relationship_classes(
                    {"name": "fish__dog", "object_class_id_list": [1, 2]}, strict=True
                )

    def test_add_relationships(self):
        """Test that adding relationships works."""
        self._db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self._db_map.add_wide_relationship_classes({"name": "rc1", "object_class_id_list": [1, 2], "id": 3})
        self._db_map.add_objects({"name": "o1", "class_id": 1, "id": 1}, {"name": "o2", "class_id": 2, "id": 2})
        self._db_map.add_wide_relationships({"name": "nemo__pluto", "class_id": 3, "object_id_list": [1, 2]})
        self._db_map.commit_session("add")
        ent_els = self._db_map.query(self._db_map.get_table("entity_element")).all()
        relationships = self._db_map.query(self._db_map.wide_relationship_sq).all()
        self.assertEqual(len(ent_els), 2)
        self.assertEqual(len(relationships), 1)
        self.assertEqual(relationships[0].name, "nemo__pluto")
        self.assertEqual(ent_els[0].entity_class_id, 3)
        self.assertEqual(ent_els[0].element_id, 1)
        self.assertEqual(ent_els[1].entity_class_id, 3)
        self.assertEqual(ent_els[1].element_id, 2)

    def test_add_relationship_with_invalid_name(self):
        """Test that adding object classes with empty name raises error"""
        self._db_map.add_object_classes({"name": "oc1", "id": 1}, strict=True)
        self._db_map.add_wide_relationship_classes({"name": "rc1", "object_class_id_list": [1]}, strict=True)
        self._db_map.add_objects({"name": "o1", "class_id": 1}, strict=True)
        with self.assertRaises(SpineIntegrityError):
            self._db_map.add_wide_relationships({"name": "", "class_id": 2, "object_id_list": [1]}, strict=True)

    def test_add_identical_relationships(self):
        """Test that adding two relationships with the same class and same objects only adds the first one."""
        self._db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self._db_map.add_wide_relationship_classes({"name": "rc1", "object_class_id_list": [1, 2], "id": 3})
        self._db_map.add_objects({"name": "o1", "class_id": 1, "id": 1}, {"name": "o2", "class_id": 2, "id": 2})
        self._db_map.add_wide_relationships(
            {"name": "nemo__pluto", "class_id": 3, "object_id_list": [1, 2]},
            {"name": "nemo__pluto_duplicate", "class_id": 3, "object_id_list": [1, 2]},
        )
        self._db_map.commit_session("add")
        relationships = self._db_map.query(self._db_map.wide_relationship_sq).all()
        self.assertEqual(len(relationships), 1)

    def test_add_relationship_identical_to_existing_one(self):
        """Test that adding a relationship with the same class and same objects as an existing one
        raises an integrity error.
        """
        query_wrapper = create_query_wrapper(self._db_map)
        with mock.patch.object(CustomDatabaseMapping, "query") as mock_query, mock.patch.object(
            CustomDatabaseMapping, "object_sq"
        ) as mock_object_sq, mock.patch.object(
            CustomDatabaseMapping, "wide_relationship_class_sq"
        ) as mock_wide_rel_cls_sq, mock.patch.object(
            CustomDatabaseMapping, "wide_relationship_sq"
        ) as mock_wide_rel_sq:
            mock_query.side_effect = query_wrapper
            mock_object_sq.return_value = [
                KeyedTuple([1, 10, "nemo"], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 20, "pluto"], labels=["id", "class_id", "name"]),
            ]
            mock_wide_rel_cls_sq.return_value = [
                KeyedTuple([1, "10,20", "fish__dog"], labels=["id", "object_class_id_list", "name"])
            ]
            mock_wide_rel_sq.return_value = [
                KeyedTuple([1, 1, "1,2", "nemo__pluto"], labels=["id", "class_id", "object_id_list", "name"])
            ]
            with self.assertRaises(SpineIntegrityError):
                self._db_map.add_wide_relationships(
                    {"name": "nemoy__plutoy", "class_id": 1, "object_id_list": [1, 2]}, strict=True
                )

    def test_add_relationship_with_invalid_class(self):
        """Test that adding a relationship with an invalid class raises an integrity error."""
        query_wrapper = create_query_wrapper(self._db_map)
        with mock.patch.object(CustomDatabaseMapping, "query") as mock_query, mock.patch.object(
            CustomDatabaseMapping, "object_sq"
        ) as mock_object_sq, mock.patch.object(
            CustomDatabaseMapping, "wide_relationship_class_sq"
        ) as mock_wide_rel_cls_sq, mock.patch.object(
            CustomDatabaseMapping, "wide_relationship_sq"
        ):
            mock_query.side_effect = query_wrapper
            mock_object_sq.return_value = [
                KeyedTuple([1, 10, "nemo"], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 20, "pluto"], labels=["id", "class_id", "name"]),
            ]
            mock_wide_rel_cls_sq.return_value = [
                KeyedTuple([1, "10,20", "fish__dog"], labels=["id", "object_class_id_list", "name"])
            ]
            with self.assertRaises(SpineIntegrityError):
                self._db_map.add_wide_relationships(
                    {"name": "nemo__pluto", "class_id": 2, "object_id_list": [1, 2]}, strict=True
                )

    def test_add_relationship_with_invalid_object(self):
        """Test that adding a relationship with an invalid object raises an integrity error."""
        query_wrapper = create_query_wrapper(self._db_map)
        with mock.patch.object(CustomDatabaseMapping, "query") as mock_query, mock.patch.object(
            CustomDatabaseMapping, "object_sq"
        ) as mock_object_sq, mock.patch.object(
            CustomDatabaseMapping, "wide_relationship_class_sq"
        ) as mock_wide_rel_cls_sq, mock.patch.object(
            CustomDatabaseMapping, "wide_relationship_sq"
        ):
            mock_query.side_effect = query_wrapper
            mock_object_sq.return_value = [
                KeyedTuple([1, 10, "nemo"], labels=["id", "class_id", "name"]),
                KeyedTuple([2, 20, "pluto"], labels=["id", "class_id", "name"]),
            ]
            mock_wide_rel_cls_sq.return_value = [
                KeyedTuple([1, "10,20", "fish__dog"], labels=["id", "object_class_id_list", "name"])
            ]
            with self.assertRaises(SpineIntegrityError):
                self._db_map.add_wide_relationships(
                    {"name": "nemo__pluto", "class_id": 1, "object_id_list": [1, 3]}, strict=True
                )

    def test_add_entity_groups(self):
        """Test that adding group entities works."""
        self._db_map.add_object_classes({"name": "oc1", "id": 1})
        self._db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 1})
        self._db_map.add_entity_groups({"entity_id": 1, "entity_class_id": 1, "member_id": 2})
        self._db_map.commit_session("add")
        table = self._db_map.get_table("entity_group")
        entity_groups = self._db_map.query(table).all()
        self.assertEqual(len(entity_groups), 1)
        self.assertEqual(entity_groups[0].entity_id, 1)
        self.assertEqual(entity_groups[0].entity_class_id, 1)
        self.assertEqual(entity_groups[0].member_id, 2)

    def test_add_entity_groups_with_invalid_class(self):
        """Test that adding group entities with an invalid class fails."""
        self._db_map.add_object_classes({"name": "oc1", "id": 1})
        self._db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 1})
        with self.assertRaises(SpineIntegrityError):
            self._db_map.add_entity_groups({"entity_id": 1, "entity_class_id": 2, "member_id": 2}, strict=True)

    def test_add_entity_groups_with_invalid_entity(self):
        """Test that adding group entities with an invalid entity fails."""
        self._db_map.add_object_classes({"name": "oc1", "id": 1})
        self._db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 1})
        with self.assertRaises(SpineIntegrityError):
            self._db_map.add_entity_groups({"entity_id": 3, "entity_class_id": 2, "member_id": 2}, strict=True)

    def test_add_entity_groups_with_invalid_member(self):
        """Test that adding group entities with an invalid member fails."""
        self._db_map.add_object_classes({"name": "oc1", "id": 1})
        self._db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 1})
        with self.assertRaises(SpineIntegrityError):
            self._db_map.add_entity_groups({"entity_id": 1, "entity_class_id": 2, "member_id": 3}, strict=True)

    def test_add_repeated_entity_groups(self):
        """Test that adding repeated group entities fails."""
        self._db_map.add_object_classes({"name": "oc1", "id": 1})
        self._db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 1})
        self._db_map.add_entity_groups({"entity_id": 1, "entity_class_id": 2, "member_id": 2})
        with self.assertRaises(SpineIntegrityError):
            self._db_map.add_entity_groups({"entity_id": 1, "entity_class_id": 2, "member_id": 2}, strict=True)

    def test_add_parameter_definitions(self):
        """Test that adding parameter definitions works."""
        self._db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self._db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
        self._db_map.add_parameter_definitions(
            {"name": "color", "object_class_id": 1, "description": "test1"},
            {"name": "relative_speed", "relationship_class_id": 3, "description": "test2"},
        )
        self._db_map.commit_session("add")
        table = self._db_map.get_table("parameter_definition")
        parameter_definitions = self._db_map.query(table).all()
        self.assertEqual(len(parameter_definitions), 2)
        self.assertEqual(parameter_definitions[0].name, "color")
        self.assertEqual(parameter_definitions[0].entity_class_id, 1)
        self.assertEqual(parameter_definitions[0].description, "test1")
        self.assertEqual(parameter_definitions[1].name, "relative_speed")
        self.assertEqual(parameter_definitions[1].entity_class_id, 3)
        self.assertEqual(parameter_definitions[1].description, "test2")

    def test_add_parameter_with_invalid_name(self):
        """Test that adding object classes with empty name raises error"""
        self._db_map.add_object_classes({"name": "oc1"}, strict=True)
        with self.assertRaises(SpineIntegrityError):
            self._db_map.add_parameter_definitions({"name": "", "object_class_id": 1}, strict=True)

    def test_add_parameter_definitions_with_same_name(self):
        """Test that adding two parameter_definitions with the same name adds both of them."""
        self._db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self._db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
        self._db_map.add_parameter_definitions(
            {"name": "color", "object_class_id": 1}, {"name": "color", "relationship_class_id": 3}
        )
        self._db_map.commit_session("add")
        table = self._db_map.get_table("parameter_definition")
        parameter_definitions = self._db_map.query(table).all()
        self.assertEqual(len(parameter_definitions), 2)
        self.assertEqual(parameter_definitions[0].name, "color")
        self.assertEqual(parameter_definitions[1].name, "color")
        self.assertEqual(parameter_definitions[0].entity_class_id, 1)

    def test_add_parameter_with_same_name_as_existing_one(self):
        """Test that adding parameter_definitions with an already taken name raises and integrity error."""
        self._db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self._db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
        self._db_map.add_parameter_definitions(
            {"name": "color", "object_class_id": 1}, {"name": "color", "relationship_class_id": 3}
        )
        with self.assertRaises(SpineIntegrityError):
            self._db_map.add_parameter_definitions({"name": "color", "object_class_id": 1}, strict=True)

    def test_add_parameter_values(self):
        """Test that adding parameter values works."""
        import_functions.import_object_classes(self._db_map, ["fish", "dog"])
        import_functions.import_relationship_classes(self._db_map, [("fish_dog", ["fish", "dog"])])
        import_functions.import_objects(self._db_map, [("fish", "nemo"), ("dog", "pluto")])
        import_functions.import_relationships(self._db_map, [("fish_dog", ("nemo", "pluto"))])
        import_functions.import_object_parameters(self._db_map, [("fish", "color")])
        import_functions.import_relationship_parameters(self._db_map, [("fish_dog", "rel_speed")])
        self._db_map.commit_session("add")
        color_id = (
            self._db_map.query(self._db_map.parameter_definition_sq)
            .filter(self._db_map.parameter_definition_sq.c.name == "color")
            .first()
            .id
        )
        rel_speed_id = (
            self._db_map.query(self._db_map.parameter_definition_sq)
            .filter(self._db_map.parameter_definition_sq.c.name == "rel_speed")
            .first()
            .id
        )
        nemo_row = self._db_map.query(self._db_map.object_sq).filter(self._db_map.object_sq.c.name == "nemo").first()
        nemo__pluto_row = self._db_map.query(self._db_map.wide_relationship_sq).first()
        self._db_map.add_parameter_values(
            {
                "parameter_definition_id": color_id,
                "entity_id": nemo_row.id,
                "entity_class_id": nemo_row.class_id,
                "value": b'"orange"',
                "alternative_id": 1,
            },
            {
                "parameter_definition_id": rel_speed_id,
                "entity_id": nemo__pluto_row.id,
                "entity_class_id": nemo__pluto_row.class_id,
                "value": b"125",
                "alternative_id": 1,
            },
        )
        self._db_map.commit_session("add")
        table = self._db_map.get_table("parameter_value")
        parameter_values = self._db_map.query(table).all()
        self.assertEqual(len(parameter_values), 2)
        self.assertEqual(parameter_values[0].parameter_definition_id, 1)
        self.assertEqual(parameter_values[0].entity_id, 1)
        self.assertEqual(parameter_values[0].value, b'"orange"')
        self.assertEqual(parameter_values[1].parameter_definition_id, 2)
        self.assertEqual(parameter_values[1].entity_id, 3)
        self.assertEqual(parameter_values[1].value, b"125")

    def test_add_parameter_value_with_invalid_object_or_relationship(self):
        """Test that adding a parameter value with an invalid object or relationship raises an
        integrity error."""
        import_functions.import_object_classes(self._db_map, ["fish", "dog"])
        import_functions.import_relationship_classes(self._db_map, [("fish_dog", ["fish", "dog"])])
        import_functions.import_objects(self._db_map, [("fish", "nemo"), ("dog", "pluto")])
        import_functions.import_relationships(self._db_map, [("fish_dog", ("nemo", "pluto"))])
        import_functions.import_object_parameters(self._db_map, [("fish", "color")])
        import_functions.import_relationship_parameters(self._db_map, [("fish_dog", "rel_speed")])
        _, errors = self._db_map.add_parameter_values(
            {"parameter_definition_id": 1, "object_id": 3, "value": b'"orange"', "alternative_id": 1}, strict=False
        )
        self.assertEqual([str(e) for e in errors], ["invalid entity_class_id for parameter_value"])
        _, errors = self._db_map.add_parameter_values(
            {"parameter_definition_id": 2, "relationship_id": 2, "value": b"125", "alternative_id": 1}, strict=False
        )
        self.assertEqual([str(e) for e in errors], ["invalid entity_class_id for parameter_value"])

    def test_add_same_parameter_value_twice(self):
        """Test that adding a parameter value twice only adds the first one."""
        import_functions.import_object_classes(self._db_map, ["fish"])
        import_functions.import_objects(self._db_map, [("fish", "nemo")])
        import_functions.import_object_parameters(self._db_map, [("fish", "color")])
        self._db_map.commit_session("add")
        color_id = (
            self._db_map.query(self._db_map.parameter_definition_sq)
            .filter(self._db_map.parameter_definition_sq.c.name == "color")
            .first()
            .id
        )
        nemo_row = self._db_map.query(self._db_map.object_sq).filter(self._db_map.entity_sq.c.name == "nemo").first()
        self._db_map.add_parameter_values(
            {
                "parameter_definition_id": color_id,
                "entity_id": nemo_row.id,
                "entity_class_id": nemo_row.class_id,
                "value": b'"orange"',
                "alternative_id": 1,
            },
            {
                "parameter_definition_id": color_id,
                "entity_id": nemo_row.id,
                "entity_class_id": nemo_row.class_id,
                "value": b'"blue"',
                "alternative_id": 1,
            },
        )
        self._db_map.commit_session("add")
        table = self._db_map.get_table("parameter_value")
        parameter_values = self._db_map.query(table).all()
        self.assertEqual(len(parameter_values), 1)
        self.assertEqual(parameter_values[0].parameter_definition_id, 1)
        self.assertEqual(parameter_values[0].entity_id, 1)
        self.assertEqual(parameter_values[0].value, b'"orange"')

    def test_add_existing_parameter_value(self):
        """Test that adding an existing parameter value raises an integrity error."""
        import_functions.import_object_classes(self._db_map, ["fish"])
        import_functions.import_objects(self._db_map, [("fish", "nemo")])
        import_functions.import_object_parameters(self._db_map, [("fish", "color")])
        import_functions.import_object_parameter_values(self._db_map, [("fish", "nemo", "color", "orange")])
        self._db_map.commit_session("add")
        _, errors = self._db_map.add_parameter_values(
            {
                "parameter_definition_id": 1,
                "entity_class_id": 1,
                "entity_id": 1,
                "value": b'"blue"',
                "alternative_id": 1,
            },
            strict=False,
        )
        self.assertEqual(
            [str(e) for e in errors],
            [
                "there's already a parameter_value with "
                "{'entity_class_name': 'fish', 'parameter_definition_name': 'color', "
                "'entity_byname': ('nemo',), 'alternative_name': 'Base'}"
            ],
        )

    def test_add_alternative(self):
        items, errors = self._db_map.add_alternatives({"name": "my_alternative"})
        self.assertEqual(errors, [])
        self.assertEqual(len(items), 1)
        self._db_map.commit_session("Add test data.")
        alternatives = self._db_map.query(self._db_map.alternative_sq).all()
        self.assertEqual(len(alternatives), 2)
        self.assertEqual(
            dict(alternatives[0]), {"id": 1, "name": "Base", "description": "Base alternative", "commit_id": 1}
        )
        self.assertEqual(
            dict(alternatives[1]), {"id": 2, "name": "my_alternative", "description": None, "commit_id": 2}
        )

    def test_add_scenario(self):
        items, errors = self._db_map.add_scenarios({"name": "my_scenario"})
        self.assertEqual(errors, [])
        self.assertEqual(len(items), 1)
        self._db_map.commit_session("Add test data.")
        scenarios = self._db_map.query(self._db_map.scenario_sq).all()
        self.assertEqual(len(scenarios), 1)
        self.assertEqual(
            dict(scenarios[0]),
            {"id": 1, "name": "my_scenario", "description": None, "active": False, "commit_id": 2},
        )

    def test_add_scenario_alternative(self):
        import_functions.import_scenarios(self._db_map, ("my_scenario",))
        self._db_map.commit_session("Add test data.")
        items, errors = self._db_map.add_scenario_alternatives({"scenario_id": 1, "alternative_id": 1, "rank": 0})
        self.assertEqual(errors, [])
        self.assertEqual(len(items), 1)
        self._db_map.commit_session("Add test data.")
        scenario_alternatives = self._db_map.query(self._db_map.scenario_alternative_sq).all()
        self.assertEqual(len(scenario_alternatives), 1)
        self.assertEqual(
            dict(scenario_alternatives[0]),
            {"id": 1, "scenario_id": 1, "alternative_id": 1, "rank": 0, "commit_id": 3},
        )

    def test_add_metadata(self):
        items, errors = self._db_map.add_metadata({"name": "test name", "value": "test_add_metadata"}, strict=False)
        self.assertEqual(errors, [])
        self.assertEqual(len(items), 1)
        self._db_map.commit_session("Add metadata")
        metadata = self._db_map.query(self._db_map.metadata_sq).all()
        self.assertEqual(len(metadata), 1)
        self.assertEqual(
            dict(metadata[0]), {"name": "test name", "id": 1, "value": "test_add_metadata", "commit_id": 2}
        )

    def test_add_metadata_that_exists_does_not_add_it(self):
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        self._db_map.commit_session("Add test data.")
        items, _ = self._db_map.add_metadata({"name": "title", "value": "My metadata."}, strict=False)
        self.assertEqual(items, [])
        metadata = self._db_map.query(self._db_map.metadata_sq).all()
        self.assertEqual(len(metadata), 1)
        self.assertEqual(dict(metadata[0]), {"name": "title", "id": 1, "value": "My metadata.", "commit_id": 2})

    def test_add_entity_metadata_for_object(self):
        import_functions.import_object_classes(self._db_map, ("fish",))
        import_functions.import_objects(self._db_map, (("fish", "leviathan"),))
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        self._db_map.commit_session("Add test data.")
        items, errors = self._db_map.add_entity_metadata({"entity_id": 1, "metadata_id": 1}, strict=False)
        self.assertEqual(errors, [])
        self.assertEqual(len(items), 1)
        self._db_map.commit_session("Add entity metadata")
        entity_metadata = self._db_map.query(self._db_map.ext_entity_metadata_sq).all()
        self.assertEqual(len(entity_metadata), 1)
        self.assertEqual(
            dict(entity_metadata[0]),
            {
                "entity_id": 1,
                "entity_name": "leviathan",
                "metadata_name": "title",
                "metadata_value": "My metadata.",
                "metadata_id": 1,
                "id": 1,
                "commit_id": 3,
            },
        )

    def test_add_entity_metadata_for_relationship(self):
        import_functions.import_object_classes(self._db_map, ("my_object_class",))
        import_functions.import_objects(self._db_map, (("my_object_class", "my_object"),))
        import_functions.import_relationship_classes(self._db_map, (("my_relationship_class", ("my_object_class",)),))
        import_functions.import_relationships(self._db_map, (("my_relationship_class", ("my_object",)),))
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        self._db_map.commit_session("Add test data.")
        items, errors = self._db_map.add_entity_metadata({"entity_id": 2, "metadata_id": 1}, strict=False)
        self.assertEqual(errors, [])
        self.assertEqual(len(items), 1)
        self._db_map.commit_session("Add entity metadata")
        entity_metadata = self._db_map.query(self._db_map.ext_entity_metadata_sq).all()
        self.assertEqual(len(entity_metadata), 1)
        self.assertEqual(
            dict(entity_metadata[0]),
            {
                "entity_id": 2,
                "entity_name": "my_object__",
                "metadata_name": "title",
                "metadata_value": "My metadata.",
                "metadata_id": 1,
                "id": 1,
                "commit_id": 3,
            },
        )

    def test_add_entity_metadata_doesnt_raise_with_empty_cache(self):
        items, errors = self._db_map.add_entity_metadata({"entity_id": 1, "metadata_id": 1}, strict=False)
        self.assertEqual(items, [])
        self.assertEqual(len(errors), 1)

    def test_add_ext_entity_metadata_for_object(self):
        import_functions.import_object_classes(self._db_map, ("fish",))
        import_functions.import_objects(self._db_map, (("fish", "leviathan"),))
        self._db_map.commit_session("Add test data.")
        items, errors = self._db_map.add_ext_entity_metadata(
            {"entity_id": 1, "metadata_name": "key", "metadata_value": "object metadata"}, strict=False
        )
        self.assertEqual(errors, [])
        self.assertEqual(len(items), 1)
        self._db_map.commit_session("Add entity metadata")
        entity_metadata = self._db_map.query(self._db_map.ext_entity_metadata_sq).all()
        self.assertEqual(len(entity_metadata), 1)
        self.assertEqual(
            dict(entity_metadata[0]),
            {
                "entity_id": 1,
                "entity_name": "leviathan",
                "metadata_name": "key",
                "metadata_value": "object metadata",
                "metadata_id": 1,
                "id": 1,
                "commit_id": 3,
            },
        )

    def test_adding_ext_entity_metadata_for_object_reuses_existing_metadata_names_and_values(self):
        import_functions.import_object_classes(self._db_map, ("fish",))
        import_functions.import_objects(self._db_map, (("fish", "leviathan"),))
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        self._db_map.commit_session("Add test data.")
        items, errors = self._db_map.add_ext_entity_metadata(
            {"entity_id": 1, "metadata_name": "title", "metadata_value": "My metadata."}, strict=False
        )
        self.assertEqual(errors, [])
        self.assertEqual(len(items), 1)
        self._db_map.commit_session("Add entity metadata")
        metadata = self._db_map.query(self._db_map.metadata_sq).all()
        self.assertEqual(len(metadata), 1)
        self.assertEqual(dict(metadata[0]), {"id": 1, "name": "title", "value": "My metadata.", "commit_id": 2})
        entity_metadata = self._db_map.query(self._db_map.ext_entity_metadata_sq).all()
        self.assertEqual(len(entity_metadata), 1)
        self.assertEqual(
            dict(entity_metadata[0]),
            {
                "entity_id": 1,
                "entity_name": "leviathan",
                "metadata_name": "title",
                "metadata_value": "My metadata.",
                "metadata_id": 1,
                "id": 1,
                "commit_id": 3,
            },
        )

    def test_add_parameter_value_metadata(self):
        import_functions.import_object_classes(self._db_map, ("fish",))
        import_functions.import_objects(self._db_map, (("fish", "leviathan"),))
        import_functions.import_object_parameters(self._db_map, (("fish", "paranormality"),))
        import_functions.import_object_parameter_values(self._db_map, (("fish", "leviathan", "paranormality", 3.9),))
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        self._db_map.commit_session("Add test data.")
        items, errors = self._db_map.add_parameter_value_metadata(
            {"parameter_value_id": 1, "metadata_id": 1}, strict=False
        )
        self.assertEqual(errors, [])
        self.assertEqual(len(items), 1)
        self._db_map.commit_session("Add value metadata")
        value_metadata = self._db_map.query(self._db_map.ext_parameter_value_metadata_sq).all()
        self.assertEqual(len(value_metadata), 1)
        self.assertEqual(
            dict(value_metadata[0]),
            {
                "alternative_name": "Base",
                "entity_name": "leviathan",
                "parameter_value_id": 1,
                "parameter_name": "paranormality",
                "metadata_name": "title",
                "metadata_value": "My metadata.",
                "metadata_id": 1,
                "id": 1,
                "commit_id": 3,
            },
        )

    def test_add_parameter_value_metadata_doesnt_raise_with_empty_cache(self):
        items, errors = self._db_map.add_parameter_value_metadata(
            {"parameter_value_id": 1, "metadata_id": 1, "alternative_id": 1}
        )
        self.assertEqual(len(items), 0)
        self.assertEqual(len(errors), 1)

    def test_add_ext_parameter_value_metadata(self):
        import_functions.import_object_classes(self._db_map, ("fish",))
        import_functions.import_objects(self._db_map, (("fish", "leviathan"),))
        import_functions.import_object_parameters(self._db_map, (("fish", "paranormality"),))
        import_functions.import_object_parameter_values(self._db_map, (("fish", "leviathan", "paranormality", 3.9),))
        self._db_map.commit_session("Add test data.")
        items, errors = self._db_map.add_ext_parameter_value_metadata(
            {"parameter_value_id": 1, "metadata_name": "key", "metadata_value": "parameter metadata"}, strict=False
        )
        self.assertEqual(errors, [])
        self.assertEqual(len(items), 1)
        self._db_map.commit_session("Add value metadata")
        value_metadata = self._db_map.query(self._db_map.ext_parameter_value_metadata_sq).all()
        self.assertEqual(len(value_metadata), 1)
        self.assertEqual(
            dict(value_metadata[0]),
            {
                "alternative_name": "Base",
                "entity_name": "leviathan",
                "parameter_value_id": 1,
                "parameter_name": "paranormality",
                "metadata_name": "key",
                "metadata_value": "parameter metadata",
                "metadata_id": 1,
                "id": 1,
                "commit_id": 3,
            },
        )

    def test_add_ext_parameter_value_metadata_reuses_existing_metadata(self):
        import_functions.import_object_classes(self._db_map, ("fish",))
        import_functions.import_objects(self._db_map, (("fish", "leviathan"),))
        import_functions.import_object_parameters(self._db_map, (("fish", "paranormality"),))
        import_functions.import_object_parameter_values(self._db_map, (("fish", "leviathan", "paranormality", 3.9),))
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        self._db_map.commit_session("Add test data.")
        items, errors = self._db_map.add_ext_parameter_value_metadata(
            {"parameter_value_id": 1, "metadata_name": "title", "metadata_value": "My metadata."}, strict=False
        )
        self.assertEqual(errors, [])
        self.assertEqual(len(items), 1)
        self._db_map.commit_session("Add value metadata")
        metadata = self._db_map.query(self._db_map.metadata_sq).all()
        self.assertEqual(len(metadata), 1)
        self.assertEqual(dict(metadata[0]), {"id": 1, "name": "title", "value": "My metadata.", "commit_id": 2})
        value_metadata = self._db_map.query(self._db_map.ext_parameter_value_metadata_sq).all()
        self.assertEqual(len(value_metadata), 1)
        self.assertEqual(
            dict(value_metadata[0]),
            {
                "alternative_name": "Base",
                "entity_name": "leviathan",
                "parameter_value_id": 1,
                "parameter_name": "paranormality",
                "metadata_name": "title",
                "metadata_value": "My metadata.",
                "metadata_id": 1,
                "id": 1,
                "commit_id": 3,
            },
        )

    def test_add_entity_to_a_class_with_abstract_dimensions(self):
        import_functions.import_entity_classes(
            self._db_map, (("fish", ()), ("dog", ()), ("animal", ()), ("two_animals", ("animal", "animal")))
        )
        import_functions.import_superclass_subclasses(self._db_map, (("animal", "fish"), ("animal", "dog")))
        import_functions.import_entities(self._db_map, (("fish", "Nemo"), ("dog", "Pulgoso")))
        self._db_map.commit_session("Add test data.")
        item, error = self._db_map.add_item(
            "entity", entity_class_name="two_animals", element_name_list=("Nemo", "Pulgoso")
        )
        self.assertTrue(item)
        self.assertFalse(error)
        self._db_map.commit_session("Add test data.")
        entities = self._db_map.query(self._db_map.wide_entity_sq).all()
        self.assertEqual(len(entities), 3)
        self.assertIn("Nemo,Pulgoso", {x.element_name_list for x in entities})


class TestDatabaseMappingUpdate(unittest.TestCase):
    def setUp(self):
        self._db_map = CustomDatabaseMapping(IN_MEMORY_DB_URL, create=True)

    def tearDown(self):
        self._db_map.close()

    def test_update_object_classes(self):
        """Test that updating object classes works."""
        self._db_map.add_object_classes({"id": 1, "name": "fish"}, {"id": 2, "name": "dog"})
        items, intgr_error_log = self._db_map.update_object_classes(
            {"id": 1, "name": "octopus"}, {"id": 2, "name": "god"}
        )
        ids = {x.resolve()["id"] for x in items}
        self._db_map.commit_session("test commit")
        sq = self._db_map.object_class_sq
        object_classes = {x.id: x.name for x in self._db_map.query(sq).filter(sq.c.id.in_(ids))}
        self.assertEqual(intgr_error_log, [])
        self.assertEqual(object_classes[1], "octopus")
        self.assertEqual(object_classes[2], "god")

    def test_update_objects(self):
        """Test that updating objects works."""
        self._db_map.add_object_classes({"id": 1, "name": "fish"})
        self._db_map.add_objects({"id": 1, "name": "nemo", "class_id": 1}, {"id": 2, "name": "dory", "class_id": 1})
        items, intgr_error_log = self._db_map.update_objects({"id": 1, "name": "klaus"}, {"id": 2, "name": "squidward"})
        ids = {x.resolve()["id"] for x in items}
        self._db_map.commit_session("test commit")
        sq = self._db_map.object_sq
        objects = {x.id: x.name for x in self._db_map.query(sq).filter(sq.c.id.in_(ids))}
        self.assertEqual(intgr_error_log, [])
        self.assertEqual(objects[1], "klaus")
        self.assertEqual(objects[2], "squidward")

    def test_update_committed_object(self):
        """Test that updating objects works."""
        self._db_map.add_object_classes({"id": 1, "name": "some_class"})
        self._db_map.add_objects({"id": 1, "name": "nemo", "class_id": 1})
        self._db_map.commit_session("update")
        items, intgr_error_log = self._db_map.update_objects({"id": 1, "name": "klaus"})
        ids = {x.resolve()["id"] for x in items}
        self._db_map.commit_session("test commit")
        sq = self._db_map.object_sq
        objects = {x.id: x.name for x in self._db_map.query(sq).filter(sq.c.id.in_(ids))}
        self.assertEqual(intgr_error_log, [])
        self.assertEqual(objects[1], "klaus")
        self.assertEqual(self._db_map.query(self._db_map.object_sq).filter_by(id=1).first().name, "klaus")

    def test_update_relationship_classes(self):
        """Test that updating relationship classes works."""
        self._db_map.add_object_classes({"name": "dog", "id": 1}, {"name": "fish", "id": 2})
        self._db_map.add_wide_relationship_classes(
            {"id": 3, "name": "dog__fish", "object_class_id_list": [1, 2]},
            {"id": 4, "name": "fish__dog", "object_class_id_list": [2, 1]},
        )
        items, intgr_error_log = self._db_map.update_wide_relationship_classes(
            {"id": 3, "name": "god__octopus"}, {"id": 4, "name": "octopus__dog"}
        )
        ids = {x.resolve()["id"] for x in items}
        self._db_map.commit_session("test commit")
        sq = self._db_map.wide_relationship_class_sq
        rel_clss = {x.id: x.name for x in self._db_map.query(sq).filter(sq.c.id.in_(ids))}
        self.assertEqual(intgr_error_log, [])
        self.assertEqual(rel_clss[3], "god__octopus")
        self.assertEqual(rel_clss[4], "octopus__dog")

    def test_update_committed_relationship_class(self):
        _ = import_functions.import_object_classes(self._db_map, ("object_class_1",))
        _ = import_functions.import_relationship_classes(self._db_map, (("my_class", ("object_class_1",)),))
        self._db_map.commit_session("Add test data")
        items, errors = self._db_map.update_wide_relationship_classes({"id": 2, "name": "renamed"})
        updated_ids = {x.resolve()["id"] for x in items}
        self.assertEqual(errors, [])
        self.assertEqual(updated_ids, {2})
        self._db_map.commit_session("Update data.")
        classes = self._db_map.query(self._db_map.wide_relationship_class_sq).all()
        self.assertEqual(len(classes), 1)
        self.assertEqual(classes[0].name, "renamed")

    def test_update_relationship_class_does_not_update_member_class_id(self):
        import_functions.import_object_classes(self._db_map, ("object_class_1", "object_class_2"))
        import_functions.import_relationship_classes(self._db_map, (("my_class", ("object_class_1",)),))
        self._db_map.commit_session("Add test data")
        items, errors = self._db_map.update_wide_relationship_classes(
            {"id": 3, "name": "renamed", "object_class_id_list": [2]}
        )
        self.assertEqual([str(err) for err in errors], ["can't modify dimensions of an entity class"])
        self.assertEqual(len(items), 1)
        self._db_map.commit_session("Update data.")
        classes = self._db_map.query(self._db_map.wide_relationship_class_sq).all()
        self.assertEqual(len(classes), 1)
        self.assertEqual(classes[0].name, "renamed")
        self.assertEqual(classes[0].object_class_name_list, "object_class_1")

    def test_update_relationships(self):
        """Test that updating relationships works."""
        self._db_map.add_object_classes({"name": "fish", "id": 1}, {"name": "dog", "id": 2})
        self._db_map.add_wide_relationship_classes({"name": "fish__dog", "id": 3, "object_class_id_list": [1, 2]})
        self._db_map.add_objects(
            {"name": "nemo", "id": 1, "class_id": 1},
            {"name": "pluto", "id": 2, "class_id": 2},
            {"name": "scooby", "id": 3, "class_id": 2},
        )
        self._db_map.add_wide_relationships(
            {"id": 4, "name": "nemo__pluto", "class_id": 3, "object_id_list": [1, 2], "object_class_id_list": [1, 2]}
        )
        items, intgr_error_log = self._db_map.update_wide_relationships(
            {"id": 4, "name": "nemo__scooby", "class_id": 3, "object_id_list": [1, 3], "object_class_id_list": [1, 2]}
        )
        ids = {x.resolve()["id"] for x in items}
        self._db_map.commit_session("test commit")
        sq = self._db_map.wide_relationship_sq
        rels = {
            x.id: {"name": x.name, "object_id_list": x.object_id_list}
            for x in self._db_map.query(sq).filter(sq.c.id.in_(ids))
        }
        self.assertEqual(intgr_error_log, [])
        self.assertEqual(rels[4]["name"], "nemo__scooby")
        self.assertEqual(rels[4]["object_id_list"], "1,3")

    def test_update_committed_relationship(self):
        import_functions.import_object_classes(self._db_map, ("object_class_1", "object_class_2"))
        import_functions.import_objects(
            self._db_map,
            (("object_class_1", "object_11"), ("object_class_1", "object_12"), ("object_class_2", "object_21")),
        )
        import_functions.import_relationship_classes(
            self._db_map, (("my_class", ("object_class_1", "object_class_2")),)
        )
        import_functions.import_relationships(self._db_map, (("my_class", ("object_11", "object_21")),))
        self._db_map.commit_session("Add test data")
        items, errors = self._db_map.update_wide_relationships({"id": 4, "name": "renamed", "object_id_list": [2, 3]})
        updated_ids = {x.resolve()["id"] for x in items}
        self.assertEqual(errors, [])
        self.assertEqual(updated_ids, {4})
        self._db_map.commit_session("Update data.")
        relationships = self._db_map.query(self._db_map.wide_relationship_sq).all()
        self.assertEqual(len(relationships), 1)
        self.assertEqual(relationships[0].name, "renamed")
        self.assertEqual(relationships[0].object_name_list, "object_12,object_21")

    def test_update_parameter_value_by_id_only(self):
        import_functions.import_object_classes(self._db_map, ("object_class1",))
        import_functions.import_object_parameters(self._db_map, (("object_class1", "parameter1"),))
        import_functions.import_objects(self._db_map, (("object_class1", "object1"),))
        import_functions.import_object_parameter_values(
            self._db_map, (("object_class1", "object1", "parameter1", "something"),)
        )
        self._db_map.commit_session("Populate with initial data.")
        items, errors = self._db_map.update_parameter_values({"id": 1, "value": b"something else"})
        updated_ids = {x.resolve()["id"] for x in items}
        self.assertEqual(errors, [])
        self.assertEqual(updated_ids, {1})
        self._db_map.commit_session("Update data.")
        pvals = self._db_map.query(self._db_map.parameter_value_sq).all()
        self.assertEqual(len(pvals), 1)
        pval = pvals[0]
        self.assertEqual(pval.value, b"something else")

    def test_update_parameter_value_to_an_uncommitted_list_value(self):
        import_functions.import_object_classes(self._db_map, ("object_class1",))
        import_functions.import_parameter_value_lists(self._db_map, (("values_1", 5.0),))
        import_functions.import_object_parameters(self._db_map, (("object_class1", "parameter1", None, "values_1"),))
        import_functions.import_objects(self._db_map, (("object_class1", "object1"),))
        import_functions.import_object_parameter_values(
            self._db_map, (("object_class1", "object1", "parameter1", 5.0),)
        )
        self._db_map.commit_session("Update data.")
        import_functions.import_parameter_value_lists(self._db_map, (("values_1", 7.0),))
        value, type_ = to_database(7.0)
        items, errors = self._db_map.update_parameter_values({"id": 1, "value": value, "type": type_})
        self.assertEqual(errors, [])
        self.assertEqual(len(items), 1)
        self._db_map.commit_session("Update data.")
        pvals = self._db_map.query(self._db_map.parameter_value_sq).all()
        self.assertEqual(from_database(pvals[0].value, pvals[0].type), 7.0)

    def test_update_parameter_definition_by_id_only(self):
        import_functions.import_object_classes(self._db_map, ("object_class1",))
        import_functions.import_object_parameters(self._db_map, (("object_class1", "parameter1"),))
        self._db_map.commit_session("Populate with initial data.")
        items, errors = self._db_map.update_parameter_definitions({"id": 1, "name": "parameter2"})
        updated_ids = {x.resolve()["id"] for x in items}
        self.assertEqual(errors, [])
        self.assertEqual(updated_ids, {1})
        self._db_map.commit_session("Update data.")
        pdefs = self._db_map.query(self._db_map.parameter_definition_sq).all()
        self.assertEqual(len(pdefs), 1)
        self.assertEqual(pdefs[0].name, "parameter2")

    def test_update_parameter_definition_value_list(self):
        import_functions.import_parameter_value_lists(self._db_map, (("my_list", 99.0),))
        import_functions.import_object_classes(self._db_map, ("object_class",))
        import_functions.import_object_parameters(self._db_map, (("object_class", "my_parameter"),))
        self._db_map.commit_session("Populate with initial data.")
        items, errors = self._db_map.update_parameter_definitions(
            {"id": 1, "name": "my_parameter", "parameter_value_list_id": 1}
        )
        updated_ids = {x.resolve()["id"] for x in items}
        self.assertEqual(errors, [])
        self.assertEqual(updated_ids, {1})
        self._db_map.commit_session("Update data.")
        pdefs = self._db_map.query(self._db_map.parameter_definition_sq).all()
        self.assertEqual(len(pdefs), 1)
        self.assertEqual(
            dict(pdefs[0]),
            {
                "commit_id": 3,
                "default_type": None,
                "default_value": None,
                "description": None,
                "entity_class_id": 1,
                "id": 1,
                "list_value_id": None,
                "name": "my_parameter",
                "parameter_value_list_id": 1,
            },
        )

    def test_update_parameter_definition_value_list_when_values_exist_gives_error(self):
        import_functions.import_parameter_value_lists(self._db_map, (("my_list", 99.0),))
        import_functions.import_object_classes(self._db_map, ("object_class",))
        import_functions.import_objects(self._db_map, (("object_class", "my_object"),))
        import_functions.import_object_parameters(self._db_map, (("object_class", "my_parameter"),))
        import_functions.import_object_parameter_values(
            self._db_map, (("object_class", "my_object", "my_parameter", 23.0),)
        )
        self._db_map.commit_session("Populate with initial data.")
        items, errors = self._db_map.update_parameter_definitions(
            {"id": 1, "name": "my_parameter", "parameter_value_list_id": 1}
        )
        self.assertEqual(
            list(map(str, errors)),
            ["can't modify the parameter value list of a parameter that already has values"],
        )
        self.assertEqual(items, [])

    def test_update_parameter_definitions_default_value_that_is_not_on_value_list_gives_error(self):
        import_functions.import_parameter_value_lists(self._db_map, (("my_list", 99.0),))
        import_functions.import_object_classes(self._db_map, ("object_class",))
        import_functions.import_objects(self._db_map, (("object_class", "my_object"),))
        import_functions.import_object_parameters(self._db_map, (("object_class", "my_parameter", None, "my_list"),))
        self._db_map.commit_session("Populate with initial data.")
        items, errors = self._db_map.update_parameter_definitions(
            {"id": 1, "name": "my_parameter", "default_value": to_database(23.0)[0]}
        )
        updated_ids = {x["id"] for x in items}
        self.assertEqual(list(map(str, errors)), ["default value 23.0 of my_parameter is not in my_list"])
        self.assertEqual(updated_ids, set())

    def test_update_parameter_definition_value_list_when_default_value_not_on_the_list_exists_gives_error(self):
        import_functions.import_parameter_value_lists(self._db_map, (("my_list", 99.0),))
        import_functions.import_object_classes(self._db_map, ("object_class",))
        import_functions.import_objects(self._db_map, (("object_class", "my_object"),))
        import_functions.import_object_parameters(self._db_map, (("object_class", "my_parameter", 23.0),))
        self._db_map.commit_session("Populate with initial data.")
        items, errors = self._db_map.update_parameter_definitions(
            {"id": 1, "name": "my_parameter", "parameter_value_list_id": 1}
        )
        updated_ids = {x["id"] for x in items}
        self.assertEqual(list(map(str, errors)), ["default value 23.0 of my_parameter is not in my_list"])
        self.assertEqual(updated_ids, set())

    def test_update_object_metadata(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"),))
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        import_functions.import_object_metadata(self._db_map, (("my_class", "my_object", '{"title": "My metadata."}'),))
        self._db_map.commit_session("Add test data")
        items, errors = self._db_map.update_ext_entity_metadata(
            {"id": 1, "metadata_name": "key_2", "metadata_value": "new value"}
        )
        self.assertEqual(errors, [])
        self.assertEqual(len(items), 2)
        self._db_map.remove_unused_metadata()
        self._db_map.commit_session("Update data")
        metadata_entries = self._db_map.query(self._db_map.metadata_sq).all()
        self.assertEqual(len(metadata_entries), 1)
        self.assertEqual(dict(metadata_entries[0]), {"id": 1, "name": "key_2", "value": "new value", "commit_id": 3})
        entity_metadata_entries = self._db_map.query(self._db_map.entity_metadata_sq).all()
        self.assertEqual(len(entity_metadata_entries), 1)
        self.assertEqual(dict(entity_metadata_entries[0]), {"id": 1, "entity_id": 1, "metadata_id": 1, "commit_id": 3})

    def test_update_object_metadata_reuses_existing_metadata(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"), ("my_class", "extra_object")))
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}', '{"key 2": "metadata value 2"}'))
        import_functions.import_object_metadata(
            self._db_map,
            (
                ("my_class", "my_object", '{"title": "My metadata."}'),
                ("my_class", "extra_object", '{"key 2": "metadata value 2"}'),
            ),
        )
        self._db_map.commit_session("Add test data")
        items, errors = self._db_map.update_ext_entity_metadata(
            *[{"id": 1, "metadata_name": "key 2", "metadata_value": "metadata value 2"}]
        )
        ids = {x.resolve()["id"] for x in items}
        self.assertEqual(errors, [])
        self.assertEqual(ids, {1})
        self._db_map.remove_unused_metadata()
        self._db_map.commit_session("Update data")
        metadata_entries = self._db_map.query(self._db_map.metadata_sq).all()
        self.assertEqual(len(metadata_entries), 1)
        self.assertEqual(
            dict(metadata_entries[0]), {"id": 2, "name": "key 2", "value": "metadata value 2", "commit_id": 2}
        )
        entity_metadata_entries = self._db_map.query(self._db_map.entity_metadata_sq).all()
        self.assertEqual(len(entity_metadata_entries), 2)
        self.assertEqual(dict(entity_metadata_entries[0]), {"id": 1, "entity_id": 1, "metadata_id": 2, "commit_id": 3})
        self.assertEqual(dict(entity_metadata_entries[1]), {"id": 2, "entity_id": 2, "metadata_id": 2, "commit_id": 2})

    def test_update_object_metadata_keeps_metadata_still_in_use(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_objects(self._db_map, (("my_class", "object_1"), ("my_class", "object_2")))
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        import_functions.import_object_metadata(
            self._db_map,
            (
                ("my_class", "object_1", '{"title": "My metadata."}'),
                ("my_class", "object_2", '{"title": "My metadata."}'),
            ),
        )
        self._db_map.commit_session("Add test data")
        items, errors = self._db_map.update_ext_entity_metadata(
            *[{"id": 1, "metadata_name": "new key", "metadata_value": "new value"}]
        )
        self.assertEqual(errors, [])
        self.assertEqual(len(items), 2)
        self._db_map.commit_session("Update data")
        metadata_entries = self._db_map.query(self._db_map.metadata_sq).all()
        self.assertEqual(len(metadata_entries), 2)
        self.assertEqual(dict(metadata_entries[0]), {"id": 1, "name": "title", "value": "My metadata.", "commit_id": 2})
        self.assertEqual(dict(metadata_entries[1]), {"id": 2, "name": "new key", "value": "new value", "commit_id": 3})
        entity_metadata_entries = self._db_map.query(self._db_map.entity_metadata_sq).all()
        self.assertEqual(len(entity_metadata_entries), 2)
        self.assertEqual(dict(entity_metadata_entries[0]), {"id": 1, "entity_id": 1, "metadata_id": 2, "commit_id": 3})
        self.assertEqual(dict(entity_metadata_entries[1]), {"id": 2, "entity_id": 2, "metadata_id": 1, "commit_id": 2})

    def test_update_parameter_value_metadata(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_object_parameters(self._db_map, (("my_class", "my_parameter"),))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"),))
        import_functions.import_object_parameter_values(
            self._db_map, (("my_class", "my_object", "my_parameter", 99.0),)
        )
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        import_functions.import_object_parameter_value_metadata(
            self._db_map, (("my_class", "my_object", "my_parameter", '{"title": "My metadata."}'),)
        )
        self._db_map.commit_session("Add test data")
        items, errors = self._db_map.update_ext_parameter_value_metadata(
            {"id": 1, "metadata_name": "key_2", "metadata_value": "new value"}
        )
        self.assertEqual(errors, [])
        self.assertEqual(len(items), 2)
        self._db_map.remove_unused_metadata()
        self._db_map.commit_session("Update data")
        metadata_entries = self._db_map.query(self._db_map.metadata_sq).all()
        self.assertEqual(len(metadata_entries), 1)
        self.assertEqual(dict(metadata_entries[0]), {"id": 1, "name": "key_2", "value": "new value", "commit_id": 3})
        value_metadata_entries = self._db_map.query(self._db_map.parameter_value_metadata_sq).all()
        self.assertEqual(len(value_metadata_entries), 1)
        self.assertEqual(
            dict(value_metadata_entries[0]), {"id": 1, "parameter_value_id": 1, "metadata_id": 1, "commit_id": 3}
        )

    def test_update_parameter_value_metadata_will_not_delete_shared_entity_metadata(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_object_parameters(self._db_map, (("my_class", "my_parameter"),))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"),))
        import_functions.import_object_parameter_values(
            self._db_map, (("my_class", "my_object", "my_parameter", 99.0),)
        )
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        import_functions.import_object_metadata(self._db_map, (("my_class", "my_object", '{"title": "My metadata."}'),))
        import_functions.import_object_parameter_value_metadata(
            self._db_map, (("my_class", "my_object", "my_parameter", '{"title": "My metadata."}'),)
        )
        self._db_map.commit_session("Add test data")
        items, errors = self._db_map.update_ext_parameter_value_metadata(
            *[{"id": 1, "metadata_name": "key_2", "metadata_value": "new value"}]
        )
        self.assertEqual(errors, [])
        self.assertEqual(len(items), 2)
        self._db_map.commit_session("Update data")
        metadata_entries = self._db_map.query(self._db_map.metadata_sq).all()
        self.assertEqual(len(metadata_entries), 2)
        self.assertEqual(dict(metadata_entries[0]), {"id": 1, "name": "title", "value": "My metadata.", "commit_id": 2})
        self.assertEqual(dict(metadata_entries[1]), {"id": 2, "name": "key_2", "value": "new value", "commit_id": 3})
        value_metadata_entries = self._db_map.query(self._db_map.parameter_value_metadata_sq).all()
        self.assertEqual(len(value_metadata_entries), 1)
        self.assertEqual(
            dict(value_metadata_entries[0]), {"id": 1, "parameter_value_id": 1, "metadata_id": 2, "commit_id": 3}
        )
        entity_metadata_entries = self._db_map.query(self._db_map.entity_metadata_sq).all()
        self.assertEqual(len(entity_metadata_entries), 1)
        self.assertEqual(dict(entity_metadata_entries[0]), {"id": 1, "entity_id": 1, "metadata_id": 1, "commit_id": 2})

    def test_update_metadata(self):
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        self._db_map.commit_session("Add test data.")
        items, errors = self._db_map.update_metadata(*({"id": 1, "name": "author", "value": "Prof. T. Est"},))
        ids = {x.resolve()["id"] for x in items}
        self.assertEqual(errors, [])
        self.assertEqual(ids, {1})
        self._db_map.commit_session("Update data")
        metadata_records = self._db_map.query(self._db_map.metadata_sq).all()
        self.assertEqual(len(metadata_records), 1)
        self.assertEqual(
            dict(metadata_records[0]), {"id": 1, "name": "author", "value": "Prof. T. Est", "commit_id": 3}
        )


class TestDatabaseMappingRemoveMixin(unittest.TestCase):
    def setUp(self):
        self._db_map = CustomDatabaseMapping(IN_MEMORY_DB_URL, create=True)

    def tearDown(self):
        self._db_map.close()

    def _assert_import(self, result):
        error = result[1]
        self.assertEqual(error, [])

    def test_remove_object_class(self):
        """Test adding and removing an object class and committing"""
        items, _ = self._db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self.assertEqual(len(items), 2)
        self._db_map.remove_items("object_class", 1, 2)
        with self.assertRaises(SpineDBAPIError):
            # Nothing to commit
            self._db_map.commit_session("delete")
        self.assertEqual(len(self._db_map.query(self._db_map.object_class_sq).all()), 0)

    def test_remove_object_class_from_committed_session(self):
        """Test removing an object class from a committed session"""
        items, _ = self._db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self._db_map.commit_session("add")
        self.assertEqual(len(self._db_map.query(self._db_map.object_class_sq).all()), 2)
        self._db_map.remove_items("object_class", *{x["id"] for x in items})
        self._db_map.commit_session("Add test data.")
        self.assertEqual(len(self._db_map.query(self._db_map.object_class_sq).all()), 0)

    def test_remove_object(self):
        """Test adding and removing an object and committing"""
        self._db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        items, _ = self._db_map.add_objects(
            {"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 2}
        )
        self._db_map.remove_items("object", *{x["id"] for x in items})
        self._db_map.commit_session("delete")
        self.assertEqual(len(self._db_map.query(self._db_map.object_sq).all()), 0)

    def test_remove_object_from_committed_session(self):
        """Test removing an object from a committed session"""
        self._db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        items, _ = self._db_map.add_objects(
            {"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 2}
        )
        self._db_map.commit_session("add")
        self.assertEqual(len(self._db_map.query(self._db_map.object_sq).all()), 2)
        self._db_map.remove_items("object", *{x["id"] for x in items})
        self._db_map.commit_session("Add test data.")
        self.assertEqual(len(self._db_map.query(self._db_map.object_sq).all()), 0)

    def test_remove_entity_group(self):
        """Test adding and removing an entity group and committing"""
        self._db_map.add_object_classes({"name": "oc1", "id": 1})
        self._db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 1})
        items, _ = self._db_map.add_entity_groups({"entity_id": 1, "entity_class_id": 1, "member_id": 2})
        self._db_map.remove_items("entity_group", *{x["id"] for x in items})
        self._db_map.commit_session("delete")
        self.assertEqual(len(self._db_map.query(self._db_map.entity_group_sq).all()), 0)

    def test_remove_entity_group_from_committed_session(self):
        """Test removing an entity group from a committed session"""
        self._db_map.add_object_classes({"name": "oc1", "id": 1})
        self._db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 1})
        self._db_map.add_entity_groups({"entity_id": 1, "entity_class_id": 1, "member_id": 2})
        self._db_map.commit_session("add")
        self.assertEqual(len(self._db_map.query(self._db_map.entity_group_sq).all()), 1)
        self._db_map.remove_items("entity_group", 1)
        self._db_map.commit_session("delete")
        self.assertEqual(len(self._db_map.query(self._db_map.entity_group_sq).all()), 0)

    def test_cascade_remove_relationship_class(self):
        """Test adding and removing a relationship class and committing"""
        self._db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        items, _ = self._db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
        self._db_map.remove_items("relationship_class", *{x["id"] for x in items})
        self._db_map.commit_session("delete")
        self.assertEqual(len(self._db_map.query(self._db_map.wide_relationship_class_sq).all()), 0)

    def test_cascade_remove_relationship_class_from_committed_session(self):
        """Test removing a relationship class from a committed session"""
        self._db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        items, _ = self._db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
        self._db_map.commit_session("add")
        self.assertEqual(len(self._db_map.query(self._db_map.wide_relationship_class_sq).all()), 1)
        self._db_map.remove_items("relationship_class", *{x["id"] for x in items})
        self._db_map.commit_session("remove")
        self.assertEqual(len(self._db_map.query(self._db_map.wide_relationship_class_sq).all()), 0)

    def test_cascade_remove_relationship(self):
        """Test adding and removing a relationship and committing"""
        self._db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self._db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
        self._db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 2})
        items, _ = self._db_map.add_wide_relationships(
            {"id": 3, "name": "remove_me", "class_id": 3, "object_id_list": [1, 2]}
        )
        self._db_map.remove_items("relationship", *{x["id"] for x in items})
        self.assertEqual(len(self._db_map.query(self._db_map.wide_relationship_sq).all()), 0)
        self._db_map.commit_session("delete")
        self.assertEqual(len(self._db_map.query(self._db_map.wide_relationship_sq).all()), 0)

    def test_cascade_remove_relationship_from_committed_session(self):
        """Test removing a relationship from a committed session"""
        self._db_map.add_object_classes({"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
        self._db_map.add_wide_relationship_classes({"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
        self._db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 2})
        items, _ = self._db_map.add_wide_relationships(
            {"id": 3, "name": "remove_me", "class_id": 3, "object_id_list": [1, 2]}
        )
        self._db_map.commit_session("add")
        self.assertEqual(len(self._db_map.query(self._db_map.wide_relationship_sq).all()), 1)
        self._db_map.remove_items("relationship", *{x["id"] for x in items})
        self._db_map.commit_session("Add test data.")
        self.assertEqual(len(self._db_map.query(self._db_map.wide_relationship_sq).all()), 0)

    def test_remove_parameter_value(self):
        """Test adding and removing a parameter value and committing"""
        self._db_map.add_object_classes({"name": "oc1", "id": 1}, strict=True)
        self._db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, strict=True)
        self._db_map.add_parameter_definitions({"name": "param", "id": 1, "object_class_id": 1}, strict=True)
        self._db_map.add_parameter_values(
            {
                "value": b"0",
                "id": 1,
                "parameter_definition_id": 1,
                "object_id": 1,
                "object_class_id": 1,
                "alternative_id": 1,
            },
            strict=True,
        )
        self._db_map.commit_session("add")
        self.assertEqual(len(self._db_map.query(self._db_map.parameter_value_sq).all()), 1)
        self._db_map.remove_items("parameter_value", 1)
        self._db_map.commit_session("delete")
        self.assertEqual(len(self._db_map.query(self._db_map.parameter_value_sq).all()), 0)

    def test_remove_parameter_value_from_committed_session(self):
        """Test adding and committing a parameter value and then removing it"""
        self._db_map.add_object_classes({"name": "oc1", "id": 1}, strict=True)
        self._db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, strict=True)
        self._db_map.add_parameter_definitions({"name": "param", "id": 1, "object_class_id": 1}, strict=True)
        self._db_map.add_parameter_values(
            {
                "value": b"0",
                "id": 1,
                "parameter_definition_id": 1,
                "object_id": 1,
                "object_class_id": 1,
                "alternative_id": 1,
            },
            strict=True,
        )
        self._db_map.commit_session("add")
        self.assertEqual(len(self._db_map.query(self._db_map.parameter_value_sq).all()), 1)
        self._db_map.remove_items("parameter_value", 1)
        self._db_map.commit_session("delete")
        self.assertEqual(len(self._db_map.query(self._db_map.parameter_value_sq).all()), 0)

    def test_cascade_remove_object_removes_parameter_value_as_well(self):
        """Test adding and removing a parameter value and committing"""
        self._db_map.add_object_classes({"name": "oc1", "id": 1}, strict=True)
        self._db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, strict=True)
        self._db_map.add_parameter_definitions({"name": "param", "id": 1, "object_class_id": 1}, strict=True)
        self._db_map.add_parameter_values(
            {
                "value": b"0",
                "id": 1,
                "parameter_definition_id": 1,
                "object_id": 1,
                "object_class_id": 1,
                "alternative_id": 1,
            },
            strict=True,
        )
        self._db_map.commit_session("add")
        self.assertEqual(len(self._db_map.query(self._db_map.parameter_value_sq).all()), 1)
        self._db_map.remove_items("object", 1)
        self._db_map.commit_session("delete")
        self.assertEqual(len(self._db_map.query(self._db_map.parameter_value_sq).all()), 0)

    def test_cascade_remove_object_from_committed_session_removes_parameter_value_as_well(self):
        """Test adding and committing a paramater value and then removing it"""
        self._db_map.add_object_classes({"name": "oc1", "id": 1}, strict=True)
        self._db_map.add_objects({"name": "o1", "id": 1, "class_id": 1}, strict=True)
        self._db_map.add_parameter_definitions({"name": "param", "id": 1, "object_class_id": 1}, strict=True)
        self._db_map.add_parameter_values(
            {
                "value": b"0",
                "id": 1,
                "parameter_definition_id": 1,
                "object_id": 1,
                "object_class_id": 1,
                "alternative_id": 1,
            },
            strict=True,
        )
        self._db_map.commit_session("add")
        self.assertEqual(len(self._db_map.query(self._db_map.parameter_value_sq).all()), 1)
        self._db_map.remove_items("object", 1)
        self._db_map.commit_session("delete")
        self.assertEqual(len(self._db_map.query(self._db_map.parameter_value_sq).all()), 0)

    def test_cascade_remove_metadata_removes_corresponding_entity_and_value_metadata(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"),))
        import_functions.import_object_parameters(self._db_map, (("my_class", "my_parameter"),))
        import_functions.import_object_parameter_values(
            self._db_map, (("my_class", "my_object", "my_parameter", 99.0),)
        )
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        import_functions.import_object_metadata(self._db_map, (("my_class", "my_object", '{"title": "My metadata."}'),))
        import_functions.import_object_parameter_value_metadata(
            self._db_map, (("my_class", "my_object", "my_parameter", '{"title": "My metadata."}'),)
        )
        self._db_map.commit_session("Add test data.")
        metadata = self._db_map.query(self._db_map.metadata_sq).all()
        self.assertEqual(len(metadata), 1)
        self._db_map.remove_items("metadata", metadata[0].id)
        self._db_map.commit_session("Remove test data.")
        self.assertEqual(len(self._db_map.query(self._db_map.metadata_sq).all()), 0)
        self.assertEqual(len(self._db_map.query(self._db_map.parameter_value_metadata_sq).all()), 0)
        self.assertEqual(len(self._db_map.query(self._db_map.entity_metadata_sq).all()), 0)
        self.assertEqual(len(self._db_map.query(self._db_map.object_sq).all()), 1)
        self.assertEqual(len(self._db_map.query(self._db_map.object_parameter_definition_sq).all()), 1)

    def test_cascade_remove_entity_metadata_removes_corresponding_metadata(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"),))
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        import_functions.import_object_metadata(self._db_map, (("my_class", "my_object", '{"title": "My metadata."}'),))
        self._db_map.commit_session("Add test data.")
        entity_metadata = self._db_map.query(self._db_map.entity_metadata_sq).all()
        self.assertEqual(len(entity_metadata), 1)
        self._db_map.remove_items("entity_metadata", entity_metadata[0].id)
        self._db_map.remove_unused_metadata()
        self._db_map.commit_session("Remove test data.")
        self.assertEqual(len(self._db_map.query(self._db_map.metadata_sq).all()), 0)
        self.assertEqual(len(self._db_map.query(self._db_map.entity_metadata_sq).all()), 0)
        self.assertEqual(len(self._db_map.query(self._db_map.object_sq).all()), 1)

    def test_cascade_remove_entity_metadata_leaves_metadata_used_by_value_intact(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"),))
        import_functions.import_object_parameters(self._db_map, (("my_class", "my_parameter"),))
        import_functions.import_object_parameter_values(
            self._db_map, (("my_class", "my_object", "my_parameter", 99.0),)
        )
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        self._assert_import(
            import_functions.import_object_metadata(
                self._db_map, (("my_class", "my_object", '{"title": "My metadata."}'),)
            )
        )
        import_functions.import_object_parameter_value_metadata(
            self._db_map, (("my_class", "my_object", "my_parameter", '{"title": "My metadata."}'),)
        )
        self._db_map.commit_session("Add test data.")
        entity_metadata = self._db_map.query(self._db_map.entity_metadata_sq).all()
        self.assertEqual(len(entity_metadata), 1)
        self._db_map.remove_items("entity_metadata", entity_metadata[0].id)
        self._db_map.commit_session("Remove test data.")
        self.assertEqual(len(self._db_map.query(self._db_map.metadata_sq).all()), 1)
        self.assertEqual(len(self._db_map.query(self._db_map.entity_metadata_sq).all()), 0)
        self.assertEqual(len(self._db_map.query(self._db_map.parameter_value_metadata_sq).all()), 1)

    def test_cascade_remove_value_metadata_leaves_metadata_used_by_entity_intact(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"),))
        import_functions.import_object_parameters(self._db_map, (("my_class", "my_parameter"),))
        import_functions.import_object_parameter_values(
            self._db_map, (("my_class", "my_object", "my_parameter", 99.0),)
        )
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        import_functions.import_object_metadata(self._db_map, (("my_class", "my_object", '{"title": "My metadata."}'),))
        import_functions.import_object_parameter_value_metadata(
            self._db_map, (("my_class", "my_object", "my_parameter", '{"title": "My metadata."}'),)
        )
        self._db_map.commit_session("Add test data.")
        parameter_value_metadata = self._db_map.query(self._db_map.parameter_value_metadata_sq).all()
        self.assertEqual(len(parameter_value_metadata), 1)
        self._db_map.remove_items("parameter_value_metadata", parameter_value_metadata[0].id)
        self._db_map.commit_session("Remove test data.")
        self.assertEqual(len(self._db_map.query(self._db_map.metadata_sq).all()), 1)
        self.assertEqual(len(self._db_map.query(self._db_map.entity_metadata_sq).all()), 1)
        self.assertEqual(len(self._db_map.query(self._db_map.parameter_value_metadata_sq).all()), 0)

    def test_cascade_remove_object_removes_its_metadata(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"),))
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        import_functions.import_object_metadata(self._db_map, (("my_class", "my_object", '{"title": "My metadata."}'),))
        self._db_map.commit_session("Add test data.")
        self._db_map.remove_items("object", 1)
        self._db_map.remove_unused_metadata()
        self._db_map.commit_session("Remove test data.")
        self.assertEqual(len(self._db_map.query(self._db_map.metadata_sq).all()), 0)
        self.assertEqual(len(self._db_map.query(self._db_map.entity_metadata_sq).all()), 0)
        self.assertEqual(len(self._db_map.query(self._db_map.object_sq).all()), 0)

    def test_cascade_remove_relationship_removes_its_metadata(self):
        import_functions.import_object_classes(self._db_map, ("my_object_class",))
        import_functions.import_objects(self._db_map, (("my_object_class", "my_object"),))
        import_functions.import_relationship_classes(self._db_map, (("my_class", ("my_object_class",)),))
        import_functions.import_relationships(self._db_map, (("my_class", ("my_object",)),))
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        import_functions.import_relationship_metadata(
            self._db_map, (("my_class", ("my_object",), '{"title": "My metadata."}'),)
        )
        self._db_map.commit_session("Add test data.")
        self._db_map.remove_items("relationship", 2)
        self._db_map.remove_unused_metadata()
        self._db_map.commit_session("Remove test data.")
        self.assertEqual(len(self._db_map.query(self._db_map.metadata_sq).all()), 0)
        self.assertEqual(len(self._db_map.query(self._db_map.entity_metadata_sq).all()), 0)
        self.assertEqual(len(self._db_map.query(self._db_map.relationship_sq).all()), 0)

    def test_cascade_remove_parameter_value_removes_its_metadata(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"),))
        import_functions.import_object_parameters(self._db_map, (("my_class", "my_parameter"),))
        import_functions.import_object_parameter_values(
            self._db_map, (("my_class", "my_object", "my_parameter", 99.0),)
        )
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        import_functions.import_object_parameter_value_metadata(
            self._db_map, (("my_class", "my_object", "my_parameter", '{"title": "My metadata."}'),)
        )
        self._db_map.commit_session("Add test data.")
        self._db_map.remove_items("parameter_value", 1)
        self._db_map.remove_unused_metadata()
        self._db_map.commit_session("Remove test data.")
        self.assertEqual(len(self._db_map.query(self._db_map.metadata_sq).all()), 0)
        self.assertEqual(len(self._db_map.query(self._db_map.entity_metadata_sq).all()), 0)
        self.assertEqual(len(self._db_map.query(self._db_map.parameter_value_sq).all()), 0)

    def test_remove_works_when_entity_groups_are_present(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"),))
        import_functions.import_objects(self._db_map, (("my_class", "my_group"),))
        import_functions.import_object_groups(self._db_map, (("my_class", "my_group", "my_object"),))
        self._db_map.commit_session("Add test data.")
        self._db_map.remove_items("object", 1)  # This shouldn't raise an exception
        self._db_map.commit_session("Remove object.")
        objects = self._db_map.query(self._db_map.object_sq).all()
        self.assertEqual(len(objects), 1)
        self.assertEqual(objects[0].name, "my_group")

    def test_remove_object_class2(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        self._db_map.commit_session("Add test data.")
        my_class = self._db_map.query(self._db_map.object_class_sq).one_or_none()
        self.assertIsNotNone(my_class)
        self._db_map.remove_items("object_class", my_class.id)
        self._db_map.commit_session("Remove object class.")
        my_class = self._db_map.query(self._db_map.object_class_sq).one_or_none()
        self.assertIsNone(my_class)

    def test_remove_relationship_class2(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_relationship_classes(self._db_map, (("my_relationship_class", ("my_class",)),))
        self._db_map.commit_session("Add test data.")
        my_class = self._db_map.query(self._db_map.relationship_class_sq).one_or_none()
        self.assertIsNotNone(my_class)
        self._db_map.remove_items("relationship_class", my_class.id)
        self._db_map.commit_session("Remove relationship class.")
        my_class = self._db_map.query(self._db_map.relationship_class_sq).one_or_none()
        self.assertIsNone(my_class)

    def test_remove_object2(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"),))
        self._db_map.commit_session("Add test data.")
        my_object = self._db_map.query(self._db_map.object_sq).one_or_none()
        self.assertIsNotNone(my_object)
        self._db_map.remove_items("object", my_object.id)
        self._db_map.commit_session("Remove object.")
        my_object = self._db_map.query(self._db_map.object_sq).one_or_none()
        self.assertIsNone(my_object)

    def test_remove_relationship2(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"),))
        import_functions.import_relationship_classes(self._db_map, (("my_relationship_class", ("my_class",)),))
        import_functions.import_relationships(self._db_map, (("my_relationship_class", ("my_object",)),))
        self._db_map.commit_session("Add test data.")
        my_relationship = self._db_map.query(self._db_map.relationship_sq).one_or_none()
        self.assertIsNotNone(my_relationship)
        self._db_map.remove_items("relationship", 2)
        self._db_map.commit_session("Remove relationship.")
        my_relationship = self._db_map.query(self._db_map.relationship_sq).one_or_none()
        self.assertIsNone(my_relationship)

    def test_remove_parameter_value2(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"),))
        import_functions.import_object_parameters(self._db_map, (("my_class", "my_parameter"),))
        import_functions.import_object_parameter_values(
            self._db_map, (("my_class", "my_object", "my_parameter", 23.0),)
        )
        self._db_map.commit_session("Add test data.")
        my_value = self._db_map.query(self._db_map.object_parameter_value_sq).one_or_none()
        self.assertIsNotNone(my_value)
        self._db_map.remove_items("parameter_value", my_value.id)
        self._db_map.commit_session("Remove parameter value.")
        my_parameter = self._db_map.query(self._db_map.object_parameter_value_sq).one_or_none()
        self.assertIsNone(my_parameter)


class TestDatabaseMappingCommitMixin(unittest.TestCase):
    def setUp(self):
        self._db_map = CustomDatabaseMapping(IN_MEMORY_DB_URL, create=True)

    def tearDown(self):
        self._db_map.close()

    def test_commit_message(self):
        """Tests that commit comment ends up in the database."""
        self._db_map.add_object_classes({"name": "testclass"})
        self._db_map.commit_session("test commit")
        self.assertEqual(self._db_map.query(self._db_map.commit_sq).all()[-1].comment, "test commit")
        self._db_map.close()

    def test_commit_session_raise_with_empty_comment(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        self.assertRaisesRegex(SpineDBAPIError, "Commit message cannot be empty.", self._db_map.commit_session, "")

    def test_commit_session_raise_when_nothing_to_commit(self):
        self.assertRaisesRegex(SpineDBAPIError, "Nothing to commit.", self._db_map.commit_session, "No changes.")

    def test_rollback_addition(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        self._db_map.commit_session("test commit")
        import_functions.import_object_classes(self._db_map, ("second_class",))
        entity_class_names = {x["name"] for x in self._db_map.mapped_table("entity_class").valid_values()}
        self.assertEqual(entity_class_names, {"my_class", "second_class"})
        self._db_map.rollback_session()
        entity_class_names = {x["name"] for x in self._db_map.mapped_table("entity_class").valid_values()}
        self.assertEqual(entity_class_names, {"my_class"})
        with self.assertRaises(SpineDBAPIError):
            # Nothing to commit
            self._db_map.commit_session("test commit")

    def test_rollback_removal(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        self._db_map.commit_session("test commit")
        self._db_map.remove_items("entity_class", 1)
        entity_class_names = {x["name"] for x in self._db_map.mapped_table("entity_class").valid_values()}
        self.assertEqual(entity_class_names, set())
        self._db_map.rollback_session()
        entity_class_names = {x["name"] for x in self._db_map.mapped_table("entity_class").valid_values()}
        self.assertEqual(entity_class_names, {"my_class"})
        with self.assertRaises(SpineDBAPIError):
            # Nothing to commit
            self._db_map.commit_session("test commit")

    def test_rollback_update(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        self._db_map.commit_session("test commit")
        self._db_map.get_item("entity_class", name="my_class").update(name="new_name")
        entity_class_names = {x["name"] for x in self._db_map.mapped_table("entity_class").valid_values()}
        self.assertEqual(entity_class_names, {"new_name"})
        self._db_map.rollback_session()
        entity_class_names = {x["name"] for x in self._db_map.mapped_table("entity_class").valid_values()}
        self.assertEqual(entity_class_names, {"my_class"})
        with self.assertRaises(SpineDBAPIError):
            # Nothing to commit
            self._db_map.commit_session("test commit")

    def test_refresh_addition(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        self._db_map.commit_session("test commit")
        import_functions.import_object_classes(self._db_map, ("second_class",))
        entity_class_names = {x["name"] for x in self._db_map.mapped_table("entity_class").valid_values()}
        self.assertEqual(entity_class_names, {"my_class", "second_class"})
        self._db_map.refresh_session()
        self._db_map.fetch_all()
        entity_class_names = {x["name"] for x in self._db_map.mapped_table("entity_class").valid_values()}
        self.assertEqual(entity_class_names, {"my_class", "second_class"})

    def test_refresh_removal(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        self._db_map.commit_session("test commit")
        self._db_map.remove_items("entity_class", 1)
        entity_class_names = {x["name"] for x in self._db_map.mapped_table("entity_class").valid_values()}
        self.assertEqual(entity_class_names, set())
        self._db_map.refresh_session()
        self._db_map.fetch_all()
        entity_class_names = {x["name"] for x in self._db_map.mapped_table("entity_class").valid_values()}
        self.assertEqual(entity_class_names, set())

    def test_refresh_update(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        self._db_map.commit_session("test commit")
        self._db_map.get_item("entity_class", name="my_class").update(name="new_name")
        entity_class_names = {x["name"] for x in self._db_map.mapped_table("entity_class").valid_values()}
        self.assertEqual(entity_class_names, {"new_name"})
        self._db_map.refresh_session()
        self._db_map.fetch_all()
        entity_class_names = {x["name"] for x in self._db_map.mapped_table("entity_class").valid_values()}
        self.assertEqual(entity_class_names, {"new_name"})

    def test_cascade_remove_unfetched(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"),))
        self._db_map.commit_session("test commit")
        self._db_map.reset()
        self._db_map.remove_items("entity_class", 1)
        self._db_map.commit_session("test commit")
        ents = self._db_map.query(self._db_map.entity_sq).all()
        self.assertEqual(ents, [])


class TestDatabaseMappingConcurrent(AssertSuccessTestCase):
    @unittest.skipIf(os.name == "nt", "Needs fixing")
    def test_concurrent_commit_threading(self):
        self._do_test_concurrent_commit(threading.Thread)

    @unittest.skipIf(os.name == "nt", "Needs fixing")
    def test_concurrent_commit_multiprocessing(self):
        self._do_test_concurrent_commit(multiprocessing.Process)

    def _do_test_concurrent_commit(self, make_concurrent):
        def _commit_on_thread(db_map, msg):
            db_map.commit_session(msg)

        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "database.sqlite")
            with CustomDatabaseMapping(url, create=True) as db_map1:
                with CustomDatabaseMapping(url) as db_map2:
                    db_map1.add_entity_class_item(name="dog")
                    db_map1.add_entity_class_item(name="cat")
                    db_map2.add_entity_class_item(name="cat")
                    c1 = make_concurrent(target=_commit_on_thread, args=(db_map1, "one"))
                    c2 = make_concurrent(target=_commit_on_thread, args=(db_map2, "two"))
                    c2.start()
                    c1.start()
                    c1.join()
                    c2.join()
            with CustomDatabaseMapping(url) as db_map:
                commit_msgs = {x["comment"] for x in db_map.query(db_map.commit_sq)}
                entity_class_names = [x["name"] for x in db_map.query(db_map.entity_class_sq)]
                self.assertEqual(commit_msgs, {"Create the database", "one", "two"})
                self.assertEqual(len(entity_class_names), 2)
                self.assertEqual(set(entity_class_names), {"cat", "dog"})

    def test_uncommitted_mapped_items_take_id_from_externally_committed_items(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "database.sqlite")
            with CustomDatabaseMapping(url, create=True) as db_map1:
                db_map1.add_entity_class_item(name="widget")
                db_map1.add_entity_class_item(name="gadget")
                with CustomDatabaseMapping(url) as db_map2:
                    # Add the same classes in different order
                    db_map2.add_entity_class_item(name="gadget")
                    db_map2.add_entity_class_item(name="widget")
                    db_map2.commit_session("No comment")
                    committed_resolved_entity_classes = [x.resolve() for x in db_map2.get_items("entity_class")]
                    committed_resolved_id_by_name = {x["name"]: x["id"] for x in committed_resolved_entity_classes}
                # Verify that the uncommitted classes are now seen as 'committed'
                uncommitted_entity_classes = db_map1.get_items("entity_class")
                uncommitted_resolved_entity_classes = [x.resolve() for x in uncommitted_entity_classes]
                uncommitted_resolved_id_by_name = {x["name"]: x["id"] for x in uncommitted_resolved_entity_classes}
                self.assertEqual(committed_resolved_id_by_name, uncommitted_resolved_id_by_name)
                for mapped_item in uncommitted_entity_classes:
                    self.assertTrue(mapped_item.is_committed())
                with self.assertRaises(SpineDBAPIError):
                    db_map1.commit_session("No comment")

    def test_committed_mapped_items_take_id_from_externally_committed_items(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "database.sqlite")
            with CustomDatabaseMapping(url, create=True) as db_map0:
                # Add widget before gadget
                db_map0.add_entity_class_item(name="widget")
                db_map0.add_entity_class_item(name="gadget")
                db_map0.commit_session("No comment")
            with CustomDatabaseMapping(url) as db_map1:
                with CustomDatabaseMapping(url) as db_map2:
                    # Purge, then add *gadget* before *widget* (swap the order)
                    # Also add an entity
                    db_map2.purge_items("entity_class")
                    db_map2.add_entity_class_item(name="gadget")
                    db_map2.add_entity_class_item(name="widget")
                    db_map2.add_entity_item(entity_class_name="gadget", name="phone")
                    db_map2.commit_session("No comment")
                # Check that we see the entity added by the other mapping
                phone = db_map1.get_entity_item(entity_class_name="gadget", name="phone")
                self.assertIsNotNone(phone)

    def test_fetching_entities_after_external_change_has_renamed_their_classes(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="Widget"))
                self._assert_success(db_map.add_entity_class_item(name="Gadget"))
                self._assert_success(db_map.add_entity_item(entity_class_name="Widget", name="smart_watch"))
                widget = db_map.get_entity_item(entity_class_name="Widget", name="smart_watch")
                self.assertEqual(widget["name"], "smart_watch")
                db_map.commit_session("Add initial data.")
                with DatabaseMapping(url) as shadow_db_map:
                    widget_class = shadow_db_map.get_entity_class_item(name="Widget")
                    widget_class.update(name="NotAWidget")
                    gadget_class = shadow_db_map.get_entity_class_item(name="Gadget")
                    gadget_class.update(name="Widget")
                    widget_class.update(name="Gadget")
                    shadow_db_map.commit_session("Swap Widget and Gadget to cause mayhem.")
                db_map.refresh_session()
                gadget = db_map.get_entity_item(entity_class_name="Gadget", name="smart_watch")
                self.assertEqual(gadget["name"], "smart_watch")

    def test_additive_commit_from_another_db_map_gets_fetched(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                items = db_map.get_items("entity")
                self.assertEqual(len(items), 0)
                with DatabaseMapping(url) as shadow_db_map:
                    self._assert_success(shadow_db_map.add_entity_class_item(name="my_class"))
                    self._assert_success(shadow_db_map.add_entity_item(name="my_entity", entity_class_name="my_class"))
                    shadow_db_map.commit_session("Add entity.")
                db_map.refresh_session()
                items = db_map.get_items("entity")
                self.assertEqual(len(items), 1)
                self.assertEqual(
                    items[0].resolve(),
                    {
                        "id": 1,
                        "name": "my_entity",
                        "description": None,
                        "class_id": 1,
                        "element_name_list": None,
                        "element_id_list": (),
                        "commit_id": 2,
                    },
                )

    def test_restoring_entity_whose_db_id_has_been_replaced_by_external_db_modification(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="my_class"))
                item = self._assert_success(db_map.add_entity_item(entity_class_name="my_class", name="my_entity"))
                original_id = item["id"]
                db_map.commit_session("Add initial data.")
                items = db_map.fetch_more("entity")
                self.assertEqual(len(items), 1)
                db_map.remove_item("entity", original_id)
                db_map.commit_session("Removed entity.")
                self.assertEqual(len(db_map.get_entity_items()), 0)
                with DatabaseMapping(url) as shadow_db_map:
                    self._assert_success(
                        shadow_db_map.add_entity_item(entity_class_name="my_class", name="other_entity")
                    )
                    shadow_db_map.commit_session("Add entity with different name, probably reusing previous id.")
                # db_map.refresh_session()
                items = db_map.fetch_more("entity")
                self.assertEqual(len(items), 1)
                self.assertEqual(items[0]["name"], "other_entity")
                all_items = db_map.get_entity_items()
                self.assertEqual(len(all_items), 1)
                restored_item = db_map.restore_item("entity", original_id)
                self.assertEqual(restored_item["name"], "my_entity")
                all_items = db_map.get_entity_items()
                self.assertEqual(len(all_items), 2)

    def test_cunning_ways_to_make_external_changes(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="interesting_class"))
                self._assert_success(db_map.add_entity_class_item(name="filler_class"))
                self._assert_success(
                    db_map.add_parameter_definition_item(name="quality", entity_class_name="interesting_class")
                )
                self._assert_success(
                    db_map.add_parameter_definition_item(name="quantity", entity_class_name="filler_class")
                )
                self._assert_success(
                    db_map.add_entity_item(name="object_of_interest", entity_class_name="interesting_class")
                )
                value, value_type = to_database(2.3)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        parameter_definition_name="quality",
                        entity_class_name="interesting_class",
                        entity_byname=("object_of_interest",),
                        alternative_name="Base",
                        value=value,
                        type=value_type,
                    )
                )
                db_map.commit_session("Add initial data")
                removed_item = db_map.get_entity_item(name="object_of_interest", entity_class_name="interesting_class")
                removed_item.remove()
                db_map.commit_session("Remove object of interest")
                with DatabaseMapping(url) as shadow_db_map:
                    self._assert_success(
                        shadow_db_map.add_entity_item(name="other_entity", entity_class_name="interesting_class")
                    )
                    self._assert_success(shadow_db_map.add_entity_item(name="filler", entity_class_name="filler_class"))
                    value, value_type = to_database(-2.3)
                    self._assert_success(
                        shadow_db_map.add_parameter_value_item(
                            parameter_definition_name="quantity",
                            entity_class_name="filler_class",
                            entity_byname=("filler",),
                            alternative_name="Base",
                            value=value,
                            type=value_type,
                        )
                    )
                    value, value_type = to_database(99.9)
                    self._assert_success(
                        shadow_db_map.add_parameter_value_item(
                            parameter_definition_name="quality",
                            entity_class_name="interesting_class",
                            entity_byname=("other_entity",),
                            alternative_name="Base",
                            value=value,
                            type=value_type,
                        )
                    )
                    shadow_db_map.commit_session("Add entities.")
                entity_items = db_map.get_entity_items()
                self.assertEqual(len(entity_items), 2)
                unique_values = {(x["name"], x["entity_class_name"]) for x in entity_items}
                self.assertIn(("other_entity", "interesting_class"), unique_values)
                self.assertIn(("filler", "filler_class"), unique_values)
                value_items = db_map.get_parameter_value_items()
                self.assertEqual(len(value_items), 2)
                self.assertTrue(removed_item.is_committed())
                unique_values = {
                    (
                        x["entity_class_name"],
                        x["parameter_definition_name"],
                        x["entity_name"],
                        x["alternative_name"],
                        x["value"],
                        x["type"],
                    )
                    for x in value_items
                }
                self.assertIn(("filler_class", "quantity", "filler", "Base", *to_database(-2.3)), unique_values)
                self.assertIn(
                    ("interesting_class", "quality", "other_entity", "Base", *to_database(99.9)), unique_values
                )

    def test_update_entity_metadata_externally(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="my_class"))
                self._assert_success(db_map.add_entity_item(name="my_entity", entity_class_name="my_class"))
                metadata_value = '{"sources": [], "contributors": []}'
                self._assert_success(db_map.add_metadata_item(name="my_metadata", value=metadata_value))
                self._assert_success(
                    db_map.add_entity_metadata_item(
                        metadata_name="my_metadata",
                        metadata_value=metadata_value,
                        entity_class_name="my_class",
                        entity_byname=("my_entity",),
                    )
                )
                db_map.commit_session("Add initial data.")
                with DatabaseMapping(url) as shadow_db_map:
                    self._assert_success(
                        shadow_db_map.add_entity_item(name="other_entity", entity_class_name="my_class")
                    )
                    metadata_item = shadow_db_map.get_entity_metadata_item(
                        metadata_name="my_metadata",
                        metadata_value=metadata_value,
                        entity_class_name="my_class",
                        entity_byname=("my_entity",),
                    )
                    self.assertTrue(metadata_item)
                    metadata_item.update(entity_byname=("other_entity",))
                    shadow_db_map.commit_session("Move entity metadata to another entity")
                metadata_items = db_map.get_entity_metadata_items()
                self.assertEqual(len(metadata_items), 2)
                self.assertNotEqual(metadata_items[0]["id"], metadata_items[1]["id"])
                unique_values = {
                    (x["entity_class_name"], x["entity_byname"], x["metadata_name"], x["metadata_value"])
                    for x in metadata_items
                }
                self.assertIn(("my_class", ("my_entity",), "my_metadata", metadata_value), unique_values)
                self.assertIn(("my_class", ("other_entity",), "my_metadata", metadata_value), unique_values)

    def test_update_parameter_value_metadata_externally(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="my_class"))
                self._assert_success(db_map.add_parameter_definition_item(name="x", entity_class_name="my_class"))
                self._assert_success(db_map.add_entity_item(name="my_entity", entity_class_name="my_class"))
                value, value_type = to_database(2.3)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="my_class",
                        entity_byname=("my_entity",),
                        parameter_definition_name="x",
                        alternative_name="Base",
                        value=value,
                        type=value_type,
                    )
                )
                metadata_value = '{"sources": [], "contributors": []}'
                self._assert_success(db_map.add_metadata_item(name="my_metadata", value=metadata_value))
                self._assert_success(
                    db_map.add_parameter_value_metadata_item(
                        metadata_name="my_metadata",
                        metadata_value=metadata_value,
                        entity_class_name="my_class",
                        entity_byname=("my_entity",),
                        parameter_definition_name="x",
                        alternative_name="Base",
                    )
                )
                db_map.commit_session("Add initial data.")
                with DatabaseMapping(url) as shadow_db_map:
                    self._assert_success(
                        shadow_db_map.add_entity_item(name="other_entity", entity_class_name="my_class")
                    )
                    value, value_type = to_database(5.0)
                    self._assert_success(
                        shadow_db_map.add_parameter_value_item(
                            entity_class_name="my_class",
                            entity_byname=("other_entity",),
                            parameter_definition_name="x",
                            alternative_name="Base",
                            value=value,
                            type=value_type,
                        )
                    )
                    metadata_item = shadow_db_map.get_parameter_value_metadata_item(
                        metadata_name="my_metadata",
                        metadata_value=metadata_value,
                        entity_class_name="my_class",
                        entity_byname=("my_entity",),
                        parameter_definition_name="x",
                        alternative_name="Base",
                    )
                    self.assertTrue(metadata_item)
                    metadata_item.update(entity_byname=("other_entity",))
                    shadow_db_map.commit_session("Move parameter value metadata to another entity")
                metadata_items = db_map.get_parameter_value_metadata_items()
                self.assertEqual(len(metadata_items), 2)
                self.assertNotEqual(metadata_items[0]["id"], metadata_items[1]["id"])
                unique_values = {
                    (
                        x["entity_class_name"],
                        x["parameter_definition_name"],
                        x["entity_byname"],
                        x["metadata_name"],
                        x["alternative_name"],
                        x["metadata_value"],
                    )
                    for x in metadata_items
                }
                self.assertIn(("my_class", "x", ("my_entity",), "my_metadata", "Base", metadata_value), unique_values)
                self.assertIn(
                    ("my_class", "x", ("other_entity",), "my_metadata", "Base", metadata_value), unique_values
                )

    def test_update_entity_alternative_externally(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="my_class"))
                self._assert_success(db_map.add_entity_item(name="my_entity", entity_class_name="my_class"))
                self._assert_success(
                    db_map.add_entity_alternative_item(
                        entity_byname=("my_entity",),
                        entity_class_name="my_class",
                        alternative_name="Base",
                        active=False,
                    )
                )
                db_map.commit_session("Add initial data.")
                with DatabaseMapping(url) as shadow_db_map:
                    self._assert_success(
                        shadow_db_map.add_entity_item(name="other_entity", entity_class_name="my_class")
                    )
                    entity_alternative = shadow_db_map.get_entity_alternative_item(
                        entity_class_name="my_class", entity_byname=("my_entity",), alternative_name="Base"
                    )
                    self.assertTrue(entity_alternative)
                    entity_alternative.update(entity_byname=("other_entity",))
                    shadow_db_map.commit_session("Move entity alternative to another entity.")
                entity_alternatives = db_map.get_entity_alternative_items()
                self.assertEqual(len(entity_alternatives), 2)
                self.assertNotEqual(entity_alternatives[0]["id"], entity_alternatives[1]["id"])
                unique_values = {
                    (x["entity_class_name"], x["entity_name"], x["alternative_name"]) for x in entity_alternatives
                }
                self.assertIn(("my_class", "my_entity", "Base"), unique_values)
                self.assertIn(("my_class", "other_entity", "Base"), unique_values)

    def test_update_superclass_subclass_externally(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="ceiling"))
                self._assert_success(db_map.add_entity_class_item(name="floor"))
                self._assert_success(db_map.add_entity_class_item(name="soil"))
                self._assert_success(
                    db_map.add_superclass_subclass_item(superclass_name="ceiling", subclass_name="floor")
                )
                db_map.commit_session("Add initial data.")
                with DatabaseMapping(url) as shadow_db_map:
                    superclass_subclass = shadow_db_map.get_superclass_subclass_item(subclass_name="floor")
                    superclass_subclass.update(subclass_name="soil")
                    shadow_db_map.commit_session("Changes subclass to another one.")
                superclass_subclasses = db_map.get_superclass_subclass_items()
                self.assertEqual(len(superclass_subclasses), 2)
                self.assertNotEqual(superclass_subclasses[0]["id"], superclass_subclasses[1]["id"])
                unique_values = {(x["superclass_name"], x["subclass_name"]) for x in superclass_subclasses}
                self.assertIn(("ceiling", "floor"), unique_values)
                self.assertIn(("ceiling", "soil"), unique_values)

    def test_adding_same_parameters_values_to_different_entities_externally(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="my_class"))
                self._assert_success(db_map.add_parameter_definition_item(name="x", entity_class_name="my_class"))
                my_entity = self._assert_success(db_map.add_entity_item(name="my_entity", entity_class_name="my_class"))
                value, value_type = to_database(2.3)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="my_class",
                        entity_byname=("my_entity",),
                        parameter_definition_name="x",
                        alternative_name="Base",
                        value=value,
                        type=value_type,
                    )
                )
                db_map.commit_session("Add initial data.")
                my_entity.remove()
                db_map.commit_session("Remove entity.")
                with DatabaseMapping(url) as shadow_db_map:
                    self._assert_success(
                        shadow_db_map.add_entity_item(name="other_entity", entity_class_name="my_class")
                    )
                    self._assert_success(
                        shadow_db_map.add_parameter_value_item(
                            entity_class_name="my_class",
                            entity_byname=("other_entity",),
                            parameter_definition_name="x",
                            alternative_name="Base",
                            value=value,
                            type=value_type,
                        )
                    )
                    shadow_db_map.commit_session("Add another entity.")
                values = db_map.get_parameter_value_items()
                self.assertEqual(len(values), 1)
                unique_value = (
                    values[0]["entity_class_name"],
                    values[0]["parameter_definition_name"],
                    values[0]["entity_name"],
                    values[0]["alternative_name"],
                )
                value_and_type = (values[0]["value"], values[0]["type"])
                self.assertEqual(unique_value, ("my_class", "x", "other_entity", "Base"))
                self.assertEqual(value_and_type, (value, value_type))

    def test_committing_changed_purged_entity_has_been_overwritten_by_external_change(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="my_class"))
                self._assert_success(db_map.add_entity_item(name="ghost", entity_class_name="my_class"))
                db_map.commit_session("Add soon-to-be-removed entity.")
                db_map.purge_items("entity")
                db_map.commit_session("Purge entities.")
                with DatabaseMapping(url) as shadow_db_map:
                    self._assert_success(
                        shadow_db_map.add_entity_item(name="other_entity", entity_class_name="my_class")
                    )
                    shadow_db_map.commit_session("Add another entity that steals ghost's id.")
                db_map.do_fetch_all("entity")
                self._assert_success(db_map.add_entity_item(name="dirty_entity", entity_class_name="my_class"))
                db_map.commit_session("Add still uncommitted entity.")
                entities = db_map.query(db_map.wide_entity_sq).all()
                self.assertEqual(len(entities), 2)

    def test_db_items_prevail_if_mapped_items_are_committed(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="my_class"))
                db_map.commit_session("Add some data")
            with DatabaseMapping(url, create=True) as db_map:
                db_map.purge_items("entity_class")
                db_map.commit_session("Purge all")
                with DatabaseMapping(url) as shadow_db_map:
                    self._assert_success(shadow_db_map.add_entity_class_item(name="my_class"))
                    shadow_db_map.commit_session("Add same class")
                entity_class_item = db_map.get_entity_class_item(name="my_class")
                self.assertTrue(entity_class_item)
                self.assertEqual(entity_class_item["name"], "my_class")

    def test_remove_items_then_refresh_then_readd(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="my_class"))
                self._assert_success(db_map.add_entity_class_item(name="new_class"))
                db_map.commit_session("Add some data")
            with DatabaseMapping(url, create=True) as db_map:
                db_map.fetch_all("entity_class")
                with DatabaseMapping(url) as shadow_db_map:
                    shadow_db_map.purge_items("entity_class")
                    self._assert_success(shadow_db_map.add_entity_class_item(name="new_class"))
                    shadow_db_map.commit_session("Purge then add new class back")
                db_map.refresh_session()
                entity_class_names = [x["name"] for x in db_map.get_entity_class_items()]
                self.assertIn("new_class", entity_class_names)
                self.assertNotIn("my_class", entity_class_names)

    def test_remove_items_then_refresh_then_readd2(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="xxx"))
                self._assert_success(db_map.add_entity_class_item(name="yyy"))
                self._assert_success(db_map.add_entity_class_item(name="zzz"))
                db_map.commit_session("Add some data")
            with DatabaseMapping(url, create=True) as db_map:
                db_map.fetch_all("entity_class")
                with DatabaseMapping(url) as shadow_db_map:
                    shadow_db_map.purge_items("entity_class")
                    self._assert_success(shadow_db_map.add_entity_class_item(name="zzz"))
                    self._assert_success(shadow_db_map.add_entity_class_item(name="www"))
                    shadow_db_map.commit_session("Purge then add one old class and one new class")
                db_map.refresh_session()
                entity_class_names = [x["name"] for x in db_map.get_entity_class_items()]
                self.assertEqual(len(entity_class_names), 2)
                self.assertEqual(set(entity_class_names), {"zzz", "www"})


if __name__ == "__main__":
    unittest.main()
