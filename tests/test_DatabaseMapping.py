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
from collections import namedtuple
import os.path
from tempfile import TemporaryDirectory
import threading
import unittest
from unittest import mock
from unittest.mock import patch
from dateutil.relativedelta import relativedelta
from sqlalchemy.engine.url import URL, make_url
from spinedb_api import (
    DatabaseMapping,
    SpineDBAPIError,
    SpineIntegrityError,
    append_filter_config,
    from_database,
    import_functions,
    to_database,
)
from spinedb_api.db_mapping_base import PublicItem, Status
from spinedb_api.exception import NothingToCommit
from spinedb_api.filters.scenario_filter import scenario_filter_config
from spinedb_api.helpers import Asterisk, DisplayStatus, create_new_spine_database, name_from_elements
from spinedb_api.parameter_value import Duration, type_for_scalar
from tests.mock_helpers import AssertSuccessTestCase

ObjectRow = namedtuple("ObjectRow", ["id", "class_id", "name"])
ObjectClassRow = namedtuple("ObjectClassRow", ["id", "name"])
RelationshipRow = namedtuple("RelationshipRow", ["id", "object_class_id_list", "name"])


def create_query_wrapper(db_map):
    def query_wrapper(*args, orig_query=db_map.query, **kwargs):
        arg = args[0]
        if isinstance(arg, mock.Mock):
            return arg.value
        return orig_query(*args, **kwargs)

    return query_wrapper


IN_MEMORY_DB_URL = "sqlite://"


class TestDatabaseMappingConstruction(AssertSuccessTestCase):
    def test_construction_with_filters(self):
        db_url = IN_MEMORY_DB_URL + "?spinedbfilter=fltr1&spinedbfilter=fltr2"
        with mock.patch("spinedb_api.db_mapping.apply_filter_stack") as mock_apply:
            with mock.patch(
                "spinedb_api.db_mapping.load_filters", return_value=[{"fltr1": "config1", "fltr2": "config2"}]
            ) as mock_load:
                db_map = DatabaseMapping(db_url, create=True)
                mock_load.assert_called_once_with(["fltr1", "fltr2"])
                mock_apply.assert_called_once_with(db_map, [{"fltr1": "config1", "fltr2": "config2"}])

    def test_construction_with_sqlalchemy_url_and_filters(self):
        db_url = IN_MEMORY_DB_URL + "/?spinedbfilter=fltr1&spinedbfilter=fltr2"
        sa_url = make_url(db_url)
        with mock.patch("spinedb_api.db_mapping.apply_filter_stack") as mock_apply:
            with mock.patch(
                "spinedb_api.db_mapping.load_filters", return_value=[{"fltr1": "config1", "fltr2": "config2"}]
            ) as mock_load:
                db_map = DatabaseMapping(sa_url, create=True)
                mock_load.assert_called_once_with(("fltr1", "fltr2"))
                mock_apply.assert_called_once_with(db_map, [{"fltr1": "config1", "fltr2": "config2"}])

    def test_shorthand_filter_query_works(self):
        with TemporaryDirectory() as temp_dir:
            url = URL.create("sqlite", database=os.path.join(temp_dir, "test_shorthand_filter_query_works.json"))
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_imports(db_map.add_items("scenario", {"name": "scen1"}))
                self._assert_imports(
                    db_map.add_items(
                        "scenario_alternative", {"scenario_name": "scen1", "alternative_name": "Base", "rank": 1}
                    )
                )
                db_map.commit_session("Add scen.")
            try:
                DatabaseMapping(url)
            except:
                self.fail("DatabaseMapping.__init__() should not raise.")


class TestDatabaseMapping(AssertSuccessTestCase):
    def test_get_item_without_fetching(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                db_map.add_metadata_item(name="Title", value="The four horsemen")
                db_map.commit_session("Add test data.")
            with DatabaseMapping(url) as db_map:
                self.assertEqual(db_map.get_item("metadata", name="Title", value="The four horsemen", fetch=False), {})

    def test_rolling_back_new_item_invalidates_its_id(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            mapped_table = db_map.mapped_table("entity_class")
            item = mapped_table.add_item({"name": "Object"})
            self.assertTrue(item.has_valid_id)
            self.assertIn("id", item)
            id_ = item["id"]
            db_map._rollback()
            self.assertFalse(item.has_valid_id)
            self.assertEqual(item["id"], id_)

    def test_active_by_default_is_initially_true_for_zero_dimensional_entity_class(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            item = self._assert_success(db_map.add_entity_class_item(name="Entity"))
            self.assertTrue(item["active_by_default"])

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
            self.assertEqual(color["entity_byname"], ("Nemo",))
            fish = db_map.get_item("entity", entity_class_name="fish", name="Nemo")
            self.assertNotEqual(fish, {})
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
            self.assertEqual(color["entity_byname"], ("NotNemo",))

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
                entity_metadata.extended(),
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
                metadata_records[0]._asdict(),
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
                value_metadata.extended(),
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
                metadata_records[0]._asdict(),
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
            self.assertNotIn("commit_id", classes[0].extended())

    def test_committing_superclass_subclass_items_doesnt_add_commit_ids_to_them(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="high"))
            self._assert_success(db_map.add_entity_class_item(name="low"))
            self._assert_success(db_map.add_superclass_subclass_item(superclass_name="high", subclass_name="low"))
            db_map.commit_session("Add class hierarchy.")
            classes = db_map.get_superclass_subclass_items()
            self.assertEqual(len(classes), 1)
            self.assertNotIn("commit_id", classes[0].extended())

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
            self.assertNotIn("commit_id", groups[0].extended())

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
            entity_names = {entity.id: entity.name for entity in db_map.query(db_map.wide_entity_sq)}
            alternative_names = {
                alternative.id: alternative.name for alternative in db_map.query(db_map.alternative_sq)
            }
            expected = {
                ("widget1", "Base"): True,
                ("widget2", "Base"): True,
                ("no_is_active", "Base"): False,
            }
            in_database = {}
            entity_alternatives = db_map.query(db_map.entity_alternative_sq)
            for entity_alternative in entity_alternatives:
                entity_name = entity_names[entity_alternative.entity_id]
                alternative_name = alternative_names[entity_alternative.alternative_id]
                in_database[(entity_name, alternative_name)] = entity_alternative.active
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
                entity_class.name: entity_class.active_by_default
                for entity_class in db_map.query(db_map.wide_entity_class_sq)
            }
            self.assertEqual(active_by_defaults, {"Widget": True, "Gadget": True, "NoIsActiveDefault": False})
            defaults = [
                from_database(definition.default_value, definition.default_type)
                for definition in db_map.query(db_map.parameter_definition_sq)
            ]
            self.assertEqual(defaults, [True, True, None])

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

    def test_get_parameter_value_from_wrong_alternative_fails_graciously(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_alternative_item(name="extra alternative"))
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_entity_item(name="knife", entity_class_name="Object"))
            self._assert_success(db_map.add_parameter_definition_item(entity_class_name="Object", name="x"))
            db_value, value_type = to_database(2.3)
            self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="Object",
                    entity_byname=("knife",),
                    parameter_definition_name="x",
                    alternative_name="Base",
                    value=db_value,
                    type=value_type,
                )
            )
            gotten_value = db_map.get_parameter_value_item(
                entity_class_name="Object",
                entity_byname=("knife",),
                parameter_definition_name="x",
                alternative_name="extra alternative",
            )
            self.assertEqual(gotten_value, {})

    def test_get_parameter_value_with_list_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_parameter_value_list_item(name="Enumeration"))
            db_value, value_type = to_database(2.3)
            self._assert_success(
                db_map.add_list_value_item(
                    parameter_value_list_name="Enumeration", value=db_value, type=value_type, index=0
                )
            )
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_entity_item(name="knife", entity_class_name="Object"))
            self._assert_success(
                db_map.add_parameter_definition_item(
                    entity_class_name="Object", name="x", parameter_value_list_name="Enumeration"
                )
            )
            self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="Object",
                    entity_byname=("knife",),
                    parameter_definition_name="x",
                    alternative_name="Base",
                    value=db_value,
                    type=value_type,
                )
            )
            gotten_value = db_map.get_parameter_value_item(
                entity_class_name="Object",
                entity_byname=("knife",),
                parameter_definition_name="x",
                alternative_name="Base",
            )
            self.assertEqual(gotten_value["parsed_value"], 2.3)

    def test_get_parameter_value_with_list_value_from_disk(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_parameter_value_list_item(name="Enumeration"))
                value, value_type = to_database(2.3)
                self._assert_success(
                    db_map.add_list_value_item(
                        parameter_value_list_name="Enumeration", value=value, type=value_type, index=0
                    )
                )
                self._assert_success(db_map.add_entity_class_item(name="Object"))
                self._assert_success(db_map.add_entity_item(name="knife", entity_class_name="Object"))
                self._assert_success(
                    db_map.add_parameter_definition_item(
                        entity_class_name="Object", name="x", parameter_value_list_name="Enumeration"
                    )
                )
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="Object",
                        entity_byname=("knife",),
                        parameter_definition_name="x",
                        alternative_name="Base",
                        value=value,
                        type=value_type,
                    )
                )
                db_map.commit_session("Add parameter value.")
            with DatabaseMapping(url) as db_map:
                value = db_map.get_parameter_value_item(
                    entity_class_name="Object",
                    entity_byname=("knife",),
                    parameter_definition_name="x",
                    alternative_name="Base",
                )
                self.assertNotEqual(value, {})
                self.assertEqual(value["parsed_value"], 2.3)

    def test_nonexistent_parameter_value_with_list_value_does_not_traceback(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_alternative_item(name="extra alternative"))
                self._assert_success(db_map.add_parameter_value_list_item(name="Enumeration"))
                value, value_type = to_database(2.3)
                self._assert_success(
                    db_map.add_list_value_item(
                        parameter_value_list_name="Enumeration", value=value, type=value_type, index=0
                    )
                )
                self._assert_success(db_map.add_entity_class_item(name="Object"))
                self._assert_success(db_map.add_entity_item(name="knife", entity_class_name="Object"))
                self._assert_success(
                    db_map.add_parameter_definition_item(
                        entity_class_name="Object", name="x", parameter_value_list_name="Enumeration"
                    )
                )
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="Object",
                        entity_byname=("knife",),
                        parameter_definition_name="x",
                        alternative_name="Base",
                        value=value,
                        type=value_type,
                    )
                )
                db_map.commit_session("Add parameter value.")
            with DatabaseMapping(url) as db_map:
                value = db_map.get_parameter_value_item(
                    entity_class_name="Object",
                    entity_byname=("knife",),
                    parameter_definition_name="x",
                    alternative_name="extra alternative",
                )
                self.assertEqual(value, {})

    def test_add_entity_class_by_dimension_names(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Fish"))
            self._assert_success(db_map.add_entity_class_item(name="Cat"))
            entity_class = self._assert_success(db_map.add_entity_class_item(dimension_name_list=("Fish", "Cat")))
            self.assertEqual(entity_class["name"], "Fish__Cat")

    def test_add_entity_by_element_names(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Fish"))
            self._assert_success(db_map.add_entity_item(name="Nemo", entity_class_name="Fish"))
            self._assert_success(db_map.add_entity_class_item(name="Cat"))
            self._assert_success(db_map.add_entity_item(name="Jerry", entity_class_name="Cat"))
            relation = self._assert_success(db_map.add_entity_class_item(dimension_name_list=("Fish", "Cat")))
            entity = self._assert_success(
                db_map.add_entity_item(entity_class_name=relation["name"], element_name_list=("Nemo", "Jerry"))
            )
            self.assertEqual(entity["name"], "Nemo__Jerry")

    def test_add_alternative_without_name_gives_error(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            item, error = db_map.add_alternative_item()
            self.assertEqual(error, "missing name")
            self.assertIsNone(item)

    def test_byname_versus_element_name_list(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="unit"))
            self._assert_success(db_map.add_entity_class_item(name="node"))
            unit_node = self._assert_success(db_map.add_entity_class_item(dimension_name_list=("unit", "node")))
            unit_node_unit_node = self._assert_success(
                db_map.add_entity_class_item(dimension_name_list=(unit_node["name"], unit_node["name"]))
            )
            self._assert_success(db_map.add_entity_item(name="U", entity_class_name="unit"))
            self._assert_success(db_map.add_entity_item(name="N", entity_class_name="node"))
            u_n = self._assert_success(
                db_map.add_entity_item(element_name_list=("U", "N"), entity_class_name=unit_node["name"])
            )
            u_n_u_n = self._assert_success(
                db_map.add_entity_item(
                    element_name_list=(u_n["name"], u_n["name"]), entity_class_name=unit_node_unit_node["name"]
                )
            )
            self.assertEqual(u_n_u_n["element_name_list"], ("U__N", "U__N"))
            self.assertEqual(u_n_u_n["entity_byname"], ("U", "N", "U", "N"))

    def test_get_parameter_definition_item_has_value_list_name(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_parameter_value_list_item(name="Values"))
                self._assert_success(db_map.add_entity_class_item(name="Object"))
                definition_item = self._assert_success(
                    db_map.add_parameter_definition_item(
                        name="x", entity_class_name="Object", parameter_value_list_name="Values"
                    )
                )
                self.assertIn("parameter_value_list_name", definition_item)
                self.assertEqual(definition_item["parameter_value_list_name"], "Values")
                db_map.commit_session("Add test data.")
            with DatabaseMapping(url) as db_map:
                definition_item = db_map.get_parameter_definition_item(name="x", entity_class_name="Object")
                self.assertIn("parameter_value_list_name", definition_item)
                self.assertEqual(definition_item["parameter_value_list_name"], "Values")

    def test_get_parameter_definition_item_without_value_list_(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="Object"))
                definition_item = self._assert_success(
                    db_map.add_parameter_definition_item(name="x", entity_class_name="Object")
                )
                self.assertIsNone(definition_item["parameter_value_list_name"])
                db_map.commit_session("Add test data.")
            with DatabaseMapping(url) as db_map:
                definition_item = db_map.get_parameter_definition_item(name="x", entity_class_name="Object")
                self.assertIsNone(definition_item["parameter_value_list_name"])

    def test_get_non_existent_parameter_definition_item_without_value_list_returns_empty_dict(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            definition_item = db_map.get_parameter_definition_item(
                name="x", entity_class_name="Object", parameter_value_list_name=None
            )
            self.assertEqual(definition_item, {})

    def test_remove_parameter_value_list_removes_parameter_definition(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            list_item = self._assert_success(db_map.add_parameter_value_list_item(name="Values"))
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            definition_item = self._assert_success(
                db_map.add_parameter_definition_item(
                    name="x", entity_class_name="Object", parameter_value_list_name="Values"
                )
            )
            list_item.remove()
            self.assertFalse(definition_item.is_valid())

    def test_adding_scenario_with_same_name_as_previously_removed_one(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                scenario = self._assert_success(db_map.add_scenario_item(name="high lows"))
                db_map.commit_session("Add 'high lows' scenario")
                scenario.remove()
                self._assert_success(db_map.add_scenario_item(name="high lows", description="Readded scenario"))
                db_map.commit_session("Readd 'high lows' scenario")
            with DatabaseMapping(url) as db_map:
                scenario = db_map.get_scenario_item(name="high lows")
                self.assertNotEqual(scenario, {})
                self.assertEqual(scenario["name"], "high lows")
                self.assertEqual(scenario["description"], "Readded scenario")

    def test_restoring_original_item_fails_after_it_has_been_removed_and_replaced(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            scenario = self._assert_success(db_map.add_scenario_item(name="high lows"))
            scenario.remove()
            self._assert_success(db_map.add_scenario_item(name="high lows"))
            with self.assertRaisesRegex(
                SpineDBAPIError, "restoring would create a conflict with another item with same unique values"
            ):
                scenario.restore()

    def test_restoring_original_item_succeeds_after_readded_item_has_been_removed(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            original_scenario = self._assert_success(db_map.add_scenario_item(name="high lows"))
            original_scenario.remove()
            readded_scenario = self._assert_success(db_map.add_scenario_item(name="high lows"))
            readded_scenario.remove()
            self.assertIsNotNone(original_scenario.restore())

    def test_restoring_original_item_restores_referrers_after_readded_item_has_been_removed(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            original_scenario = self._assert_success(db_map.add_scenario_item(name="high lows"))
            scenario_alternative = self._assert_success(
                db_map.add_scenario_alternative_item(scenario_name="high lows", alternative_name="Base", rank=1)
            )
            original_scenario.remove()
            self.assertFalse(scenario_alternative.is_valid())
            readded_scenario = self._assert_success(db_map.add_scenario_item(name="high lows"))
            readded_scenario.remove()
            self.assertIsNotNone(original_scenario.restore())
            self.assertTrue(scenario_alternative.is_valid)

    def test_shuffle_scenario_alternatives(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            scenario = self._assert_success(db_map.add_scenario_item(name="scenario"))
            base = db_map.get_alternative_item(name="Base")
            next_level = self._assert_success(db_map.add_alternative_item(name="Next level"))
            scenario_alternative_1 = self._assert_success(
                db_map.add_scenario_alternative_item(scenario_id=scenario["id"], alternative_id=base["id"], rank=1)
            )
            scenario_alternative_2 = self._assert_success(
                db_map.add_scenario_alternative_item(
                    scenario_id=scenario["id"], alternative_id=next_level["id"], rank=2
                )
            )
            removed, error = db_map.remove_item("scenario_alternative", scenario_alternative_2["id"])
            self.assertIsNone(error)
            self.assertIsInstance(removed, PublicItem)
            removed, error = db_map.remove_item("scenario_alternative", scenario_alternative_1["id"])
            self.assertIsNone(error)
            self.assertIsInstance(removed, PublicItem)
            scenario_alternative_3 = self._assert_success(
                db_map.add_scenario_alternative_item(
                    scenario_id=scenario["id"], alternative_id=next_level["id"], rank=1
                )
            )
            scenario_alternative_4 = self._assert_success(
                db_map.add_scenario_alternative_item(scenario_id=scenario["id"], alternative_id=base["id"], rank=2)
            )
            removed, error = db_map.remove_item("scenario_alternative", scenario_alternative_4["id"])
            self.assertIsNone(
                error,
            )
            self.assertIsInstance(removed, PublicItem)
            removed, error = db_map.remove_item("scenario_alternative", scenario_alternative_3["id"])
            self.assertIsNone(error)
            self.assertIsInstance(removed, PublicItem)

    def test_double_remove_does_not_invoke_remove_callbacks(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            scenario = db_map.add_scenario(name="Scenario")
            remove_callback = mock.MagicMock()
            scenario.add_remove_callback(remove_callback)
            scenario.remove()
            remove_callback.assert_called_once()
            remove_callback.reset_mock()
            scenario.remove()
            remove_callback.assert_not_called()

    def test_get_items_returns_nothing_when_item_is_removed(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            entity_class = db_map.add_entity_class(name="Object")
            entity_class.remove()
            classes = db_map.get_items("entity_class")
            self.assertEqual(len(classes), 0)

    def test_get_items_with_skip_removed_set_to_false(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            alternative = db_map.get_alternative_item(name="Base")
            alternative.remove()
            alternatives = db_map.get_items("alternative", skip_removed=False)
            self.assertEqual(len(alternatives), 1)
            self.assertEqual(alternatives[0]["name"], "Base")
            self.assertFalse(alternatives[0].is_valid())

    def test_get_items_with_existing_item_and_skip_removed_set_to_false(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            alternatives = db_map.get_items("alternative", skip_removed=False)
            self.assertEqual(len(alternatives), 1)
            self.assertEqual(alternatives[0]["name"], "Base")

    def test_get_items_with_skip_removed_set_to_true(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            alternative = db_map.get_alternative_item(name="Base")
            alternative.remove()
            alternatives = db_map.get_items("alternative", skip_removed=True)
            self.assertEqual(len(alternatives), 0)

    def test_get_items_with_skip_removed_after_readding_item(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            alternative = db_map.get_alternative_item(name="Base")
            alternative.remove()
            self._assert_success(db_map.add_alternative_item(name="Base"))
            alternatives = db_map.get_items("alternative", skip_removed=False)
            self.assertEqual(len(alternatives), 2)
            self.assertEqual(alternatives[0]["name"], "Base")
            self.assertFalse(alternatives[0].is_valid())
            self.assertEqual(alternatives[1]["name"], "Base")
            self.assertTrue(alternatives[1].is_valid())

    def test_get_item_returns_nothing_when_item_is_removed(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            entity_class = db_map.add_entity_class(name="Object")
            entity_class.remove()
            self.assertEqual(db_map.get_item("entity_class", name="Object"), {})

    def test_get_item_with_skip_removed_set_to_false(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.get_alternative_item(name="Base").remove()
            alternative = db_map.get_item("alternative", name="Base", skip_removed=False)
            self.assertEqual(alternative["name"], "Base")
            self.assertFalse(alternative.is_valid())

    def test_get_item_with_existing_item_and_skip_removed_set_to_false(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.get_alternative_item(name="Base")
            alternative = db_map.get_item("alternative", name="Base", skip_removed=True)
            self.assertEqual(alternative["name"], "Base")

    def test_get_item_with_skip_removed_set_to_true(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.get_alternative_item(name="Base").remove()
            alternative = db_map.get_item("alternative", name="Base", skip_removed=True)
            self.assertEqual(alternative, {})

    def test_get_item_with_skip_removed_after_readding_item(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            alternative = db_map.get_alternative_item(name="Base")
            alternative.remove()
            self._assert_success(db_map.add_alternative_item(name="Base"))
            alternative = db_map.get_item("alternative", name="Base", skip_removed=False)
            self.assertEqual(alternative["name"], "Base")
            self.assertTrue(alternative.is_valid())

    def test_entity_item_active_in_scenario(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                # Add data
                import_functions.import_scenarios(db_map, ("scen1",))
                db_map.commit_session("Add test data.")
                import_functions.import_scenarios(db_map, ("scen2",))
                db_map.commit_session("Add test data.")
                import_functions.import_alternatives(db_map, ("alt1",))
                db_map.commit_session("Add test data.")
                import_functions.import_alternatives(db_map, ("alt2",))
                db_map.commit_session("Add test data.")
                import_functions.import_alternatives(db_map, ("alt3",))
                db_map.commit_session("Add test data.")
                items, errors = db_map.add_items(
                    "scenario_alternative", {"scenario_id": 1, "alternative_id": 1, "rank": 0}
                )
                self.assertEqual(errors, [])
                self.assertEqual(len(items), 1)
                items, errors = db_map.add_items(
                    "scenario_alternative", {"scenario_id": 1, "alternative_id": 2, "rank": 1}
                )
                self.assertEqual(errors, [])
                self.assertEqual(len(items), 1)
                items, errors = db_map.add_items(
                    "scenario_alternative", {"scenario_id": 2, "alternative_id": 3, "rank": 0}
                )
                self.assertEqual(errors, [])
                self.assertEqual(len(items), 1)
                items, errors = db_map.add_items(
                    "scenario_alternative", {"scenario_id": 2, "alternative_id": 2, "rank": 1}
                )
                self.assertEqual(errors, [])
                self.assertEqual(len(items), 1)
                items, errors = db_map.add_items(
                    "scenario_alternative", {"scenario_id": 2, "alternative_id": 1, "rank": 2}
                )
                self.assertEqual(errors, [])
                self.assertEqual(len(items), 1)

                db_map.commit_session("Add test data.")
                scenario_alternatives = db_map.query(db_map.scenario_alternative_sq).all()
                self.assertEqual(len(scenario_alternatives), 5)
                self.assertEqual(
                    scenario_alternatives[0]._asdict(),
                    {"id": 1, "scenario_id": 1, "alternative_id": 1, "rank": 0, "commit_id": 7},
                )
                import_functions.import_scenarios(db_map, ("scen1",))
                items, errors = db_map.add_items("entity_class", {"id": 1, "name": "class"})
                self.assertEqual(errors, [])
                self.assertEqual(len(items), 1)
                entity_items, errors = db_map.add_items(
                    "entity",
                    {"class_id": 1, "id": 1, "name": "entity1"},
                    {"class_id": 1, "id": 2, "name": "entity2"},
                )
                self.assertEqual(errors, [])
                self.assertEqual(len(entity_items), 2)
                db_map.commit_session("Add test data.")
                items, errors = db_map.add_items(
                    "entity_alternative",
                    {"alternative_id": 1, "entity_class_name": "class", "entity_byname": ("entity1",), "active": False},
                    {"alternative_id": 2, "entity_class_name": "class", "entity_byname": ("entity1",), "active": True},
                    {"alternative_id": 3, "entity_class_name": "class", "entity_byname": ("entity1",), "active": True},
                    {"alternative_id": 1, "entity_class_name": "class", "entity_byname": ("entity2",), "active": True},
                    {"alternative_id": 2, "entity_class_name": "class", "entity_byname": ("entity2",), "active": False},
                    {"alternative_id": 3, "entity_class_name": "class", "entity_byname": ("entity2",), "active": False},
                )
                self.assertEqual(errors, [])
                self.assertEqual(len(items), 6)
                # Actual tests
                active = db_map.item_active_in_scenario(entity_items[0], 1)
                self.assertTrue(active)
                active = db_map.item_active_in_scenario(entity_items[0], 2)
                self.assertFalse(active)
                active = db_map.item_active_in_scenario(entity_items[1], 1)
                self.assertFalse(active)
                active = db_map.item_active_in_scenario(entity_items[1], 2)
                self.assertTrue(active)

    def test_remove_items(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            alternative_1 = self._assert_success(db_map.add_alternative_item(name="alt 1"))
            alternative_2 = self._assert_success(db_map.add_alternative_item(name="alt 2"))
            self.assertTrue(alternative_1.is_valid())
            self.assertTrue(alternative_2.is_valid())
            removed_items, errors = db_map.remove_items("alternative", alternative_1["id"], alternative_2["id"])
            self.assertTrue(all(not error for error in errors))
            self.assertCountEqual(removed_items, [alternative_1, alternative_2])
            self.assertFalse(alternative_1.is_valid())
            self.assertFalse(alternative_2.is_valid())

    def test_restore_items(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            alternative_1 = self._assert_success(db_map.add_alternative_item(name="alt 1"))
            alternative_2 = self._assert_success(db_map.add_alternative_item(name="alt 2"))
            self.assertTrue(alternative_1.is_valid())
            self.assertTrue(alternative_2.is_valid())
            removed_items, errors = db_map.remove_items("alternative", alternative_1["id"], alternative_2["id"])
            self.assertTrue(all(not error for error in errors))
            self.assertCountEqual(removed_items, [alternative_1, alternative_2])
            self.assertFalse(alternative_1.is_valid())
            self.assertFalse(alternative_2.is_valid())
            restored_items, errors = db_map.restore_items("alternative", alternative_1["id"], alternative_2["id"])
            self.assertTrue(all(not error for error in errors))
            self.assertCountEqual(removed_items, [alternative_1, alternative_2])
            self.assertTrue(alternative_1.is_valid())
            self.assertTrue(alternative_2.is_valid())

    def test_remove_value_list_after_fetch_more_then_recreate_it(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_parameter_value_list_item(name="yes_no"))
                value, value_type = to_database("yes")
                self._assert_success(
                    db_map.add_list_value_item(
                        parameter_value_list_name="yes_no", index=0, value=value, type=value_type
                    )
                )
                db_map.commit_session("Add value list.")
            with DatabaseMapping(url) as db_map:
                db_map.fetch_more("parameter_value_list")
                value_list = db_map.get_parameter_value_list_item(name="yes_no")
                value_list.remove()
                new_value_list = self._assert_success(db_map.add_parameter_value_list_item(name="yes_no"))
                self._assert_success(
                    db_map.add_list_value_item(
                        parameter_value_list_name="yes_no", index=0, value=value, type=value_type
                    )
                )
                db_map.commit_session("Readd value list.")
                self.assertIsNone(new_value_list.mapped_item.replaced_item_waiting_for_removal)
                list_values = db_map.get_list_value_items()
                self.assertEqual(len(list_values), 1)
                self.assertEqual(list_values[0]["parameter_value_list_name"], "yes_no")
                self.assertEqual(from_database(list_values[0]["value"], list_values[0]["type"]), "yes")
                self.assertIsNone(list_values[0].mapped_item.replaced_item_waiting_for_removal)

    def test_add_referrer_called_only_once_for_fetched_items(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_parameter_value_list_item(name="list of values"))
                value, value_type = to_database("yes")
                self._assert_success(
                    db_map.add_list_value_item(
                        parameter_value_list_name="list of values", index=0, value=value, type=value_type
                    )
                )
                db_map.commit_session("Add value list.")
            with DatabaseMapping(url) as db_map:
                value_list = db_map.get_parameter_value_list_item(name="list of values")
                db_map.get_list_value_item(parameter_value_list_name="list_of_values", index=0)
                self.assertEqual(len(value_list.mapped_item._referrers), 1)

    def test_remove_scenario_alternative_from_middle(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_scenario_item(name="Scenario"))
            self._assert_success(db_map.add_alternative_item(name="alt1"))
            self._assert_success(
                db_map.add_scenario_alternative_item(scenario_name="Scenario", alternative_name="Base", rank=0)
            )
            self._assert_success(
                db_map.add_scenario_alternative_item(scenario_name="Scenario", alternative_name="alt1", rank=1)
            )
            db_map.commit_session("Add scenario with two alternatives")
            scenario_alternatives = db_map.query(db_map.scenario_alternative_sq).all()
            self.assertEqual(len(scenario_alternatives), 2)
            db_map.get_scenario_alternative_item(scenario_name="Scenario", alternative_name="alt1", rank=1).remove()
            db_map.get_scenario_alternative_item(scenario_name="Scenario", alternative_name="Base", rank=0).remove()
            self._assert_success(
                db_map.add_scenario_alternative_item(scenario_name="Scenario", alternative_name="alt1", rank=0)
            )
            db_map.commit_session("Remove first alternative from scenario")
            scenario_alternatives = db_map.query(db_map.scenario_alternative_sq).all()
            self.assertEqual(len(scenario_alternatives), 1)

    def test_add_parameter_definition_item_adds_a_single_entry_to_the_mapped_table(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Gadget"))
            self._assert_success(db_map.add_parameter_definition_item(name="typeless", entity_class_name="Gadget"))
            self.assertEqual(len(db_map.mapped_table("parameter_definition")), 1)

    def test_add_item_without_check(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            entity_class = self._assert_success(db_map.add_entity_class_item(name="Gadget"))
            item = db_map.mapped_table("parameter_definition").add_item(
                {"entity_class_id": entity_class["id"], "name": "y"}
            )
            self.assertTrue(item.is_valid())
            self.assertEqual(item["entity_class_name"], "Gadget")

    def test_add_parameter_type(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Widget"))
            definition = self._assert_success(
                db_map.add_parameter_definition_item(name="typed", entity_class_name="Widget")
            )
            self._assert_success(
                db_map.add_parameter_type_item(
                    entity_class_name="Widget", parameter_definition_name="typed", type="float", rank=0
                )
            )
            self._assert_success(
                db_map.add_parameter_type_item(
                    entity_class_name="Widget", parameter_definition_name="typed", type="str", rank=0
                )
            )
            self._assert_success(
                db_map.add_parameter_type_item(
                    entity_class_name="Widget", parameter_definition_name="typed", type="bool", rank=0
                )
            )
            self._assert_success(
                db_map.add_parameter_type_item(
                    entity_class_name="Widget", parameter_definition_name="typed", type="duration", rank=0
                )
            )
            self._assert_success(
                db_map.add_parameter_type_item(
                    entity_class_name="Widget", parameter_definition_name="typed", type="date_time", rank=0
                )
            )
            self._assert_success(
                db_map.add_parameter_type_item(
                    entity_class_name="Widget", parameter_definition_name="typed", type="array", rank=1
                )
            )
            self._assert_success(
                db_map.add_parameter_type_item(
                    entity_class_name="Widget", parameter_definition_name="typed", type="time_pattern", rank=1
                )
            )
            self._assert_success(
                db_map.add_parameter_type_item(
                    entity_class_name="Widget", parameter_definition_name="typed", type="time_series", rank=1
                )
            )
            self._assert_success(
                db_map.add_parameter_type_item(
                    entity_class_name="Widget", parameter_definition_name="typed", type="map", rank=1
                )
            )
            self._assert_success(
                db_map.add_parameter_type_item(
                    entity_class_name="Widget", parameter_definition_name="typed", type="map", rank=2
                )
            )
            db_map.commit_session("Ensure data goes to db.")
            expected_types = (
                "array",
                "bool",
                "date_time",
                "duration",
                "float",
                "1d_map",
                "2d_map",
                "str",
                "time_pattern",
                "time_series",
            )
            self.assertEqual(definition["parameter_type_list"], expected_types)
            self.assertEqual(len(definition["parameter_type_id_list"]), len(expected_types))

    def test_cannot_add_invalid_parameter_type(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Widget"))
            self._assert_success(db_map.add_parameter_definition_item(name="typed", entity_class_name="Widget"))
            item, error = db_map.add_parameter_type_item(
                entity_class_name="Widget", parameter_definition_name="typed", rank=1, type="GIBBERISH"
            )
            self.assertEqual(error, "invalid type for parameter_type")
            self.assertFalse(bool(item))

    def test_creating_parameter_definition_with_types(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Widget"))
            definition = self._assert_success(
                db_map.add_parameter_definition_item(
                    name="typed", entity_class_name="Widget", parameter_type_list=("duration", "23d_map")
                )
            )
            self.assertEqual(definition["parameter_type_list"], ("duration", "23d_map"))
            types = db_map.get_parameter_type_items()
            self.assertEqual(len(types), 2)
            self.assertEqual([(t["type"], t["rank"]) for t in types], [("duration", 0), ("map", 23)])

    def test_add_type_to_parameter_by_update(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Widget"))
            definition = self._assert_success(
                db_map.add_parameter_definition_item(name="typed", entity_class_name="Widget")
            )
            self.assertEqual(definition["parameter_type_list"], tuple())
            updated_item = definition.update(parameter_type_list=("bool",))
            self.assertIsNotNone(updated_item)
            self.assertEqual(definition["parameter_type_list"], ("bool",))
            types = db_map.get_parameter_type_items()
            self.assertEqual(len(types), 1)
            self.assertEqual([(t["type"], t["rank"]) for t in types], [("bool", 0)])

    def test_remove_type_from_parameter_by_update(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Widget"))
            definition = self._assert_success(
                db_map.add_parameter_definition_item(name="typed", entity_class_name="Widget")
            )
            self.assertEqual(definition["parameter_type_list"], tuple())
            updated_item = definition.update(parameter_type_list=("bool",))
            self.assertIsNotNone(updated_item)
            self.assertEqual(definition["parameter_type_list"], ("bool",))
            types = db_map.get_parameter_type_items()
            self.assertEqual(len(types), 1)
            self.assertEqual([(t["type"], t["rank"]) for t in types], [("bool", 0)])

    def test_modify_parameter_types_by_update(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Widget"))
            definition = self._assert_success(
                db_map.add_parameter_definition_item(
                    name="typed", entity_class_name="Widget", parameter_type_list=("3d_map", "str", "array")
                )
            )
            self.assertEqual(definition["parameter_type_list"], ("array", "3d_map", "str"))
            updated_item = definition.update(parameter_type_list=("time_series", "23d_map", "str"))
            self.assertIsNotNone(updated_item)
            self.assertEqual(definition["parameter_type_list"], ("23d_map", "str", "time_series"))
            types = db_map.get_parameter_type_items()
            self.assertEqual(len(types), 3)
            self.assertCountEqual(
                [(t["type"], t["rank"]) for t in types], [("map", 23), ("str", 0), ("time_series", 1)]
            )

    def test_modify_parameter_types_by_update_via_mapped_table(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Widget"))
            definition = self._assert_success(
                db_map.add_parameter_definition_item(
                    name="typed", entity_class_name="Widget", parameter_type_list=("3d_map", "str", "array")
                )
            )
            self.assertEqual(definition["parameter_type_list"], ("array", "3d_map", "str"))
            parameter_table = db_map.mapped_table("parameter_definition")
            target_item = parameter_table.find_item_by_id(definition["id"])
            merged_item, updated_fields = target_item.merge({"parameter_type_list": ("time_series", "23d_map", "str")})
            parameter_table.update_item(merged_item, target_item, updated_fields)
            self.assertEqual(definition["parameter_type_list"], ("23d_map", "str", "time_series"))
            types = db_map.get_parameter_type_items()
            self.assertEqual(len(types), 3)
            self.assertCountEqual(
                [(t["type"], t["rank"]) for t in types], [("str", 0), ("time_series", 1), ("map", 23)]
            )

    def test_non_updating_something_else_than_parameter_type_list(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Widget"))
            reused_description = "This won't actually change."
            definition = self._assert_success(
                db_map.add_parameter_definition_item(
                    name="typed",
                    entity_class_name="Widget",
                    description=reused_description,
                    parameter_type_list=("array",),
                )
            )
            updated_item = definition.update(description=reused_description)
            self.assertIsNone(updated_item)

    def test_parameter_type_list_is_included_in_asdict(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            entity_class = self._assert_success(db_map.add_entity_class_item(name="Widget"))
            definition = self._assert_success(
                db_map.add_parameter_definition_item(
                    name="typeless",
                    entity_class_name="Widget",
                    description="This is not-a-gadget.",
                )
            )
            self.assertEqual(
                definition._asdict(),
                {
                    "name": "typeless",
                    "id": definition["id"],
                    "entity_class_id": entity_class["id"],
                    "description": "This is not-a-gadget.",
                    "parameter_type_list": (),
                    "default_type": None,
                    "default_value": None,
                    "list_value_id": None,
                    "parameter_value_list_id": None,
                },
            )

    def test_read_parameter_types_from_database(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db_sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="Widget"))
                self._assert_success(
                    db_map.add_parameter_definition_item(
                        name="typed", entity_class_name="Widget", parameter_type_list=("3d_map", "str", "array")
                    )
                )
                db_map.commit_session("Add parameter with types.")
            with DatabaseMapping(url) as db_map:
                definition = db_map.get_parameter_definition_item(entity_class_name="Widget", name="typed")
                self.assertEqual(definition["parameter_type_list"], ("array", "3d_map", "str"))

    def test_set_parameter_value_to_null(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Unit"))
            self._assert_success(db_map.add_parameter_definition_item(name="is_SI", entity_class_name="Unit"))
            self._assert_success(db_map.add_entity_item(name="gram", entity_class_name="Unit"))
            value, value_type = to_database(None)
            value_item = self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="Unit",
                    entity_byname=("gram",),
                    parameter_definition_name="is_SI",
                    alternative_name="Base",
                    value=value,
                    type=value_type,
                )
            )
            self.assertIsNone(value_item["parsed_value"])
            self.assertIsNone(value_item["type"])

    def test_parameter_default_type_cannot_be_none_if_default_value_is_non_null(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Unit"))
            value, _ = to_database(2.3)
            definition, error = db_map.add_parameter_definition_item(
                name="is_SI", entity_class_name="Unit", default_value=value, default_type=None
            )
            self.assertEqual(error, "invalid default_type for parameter_definition")
            self.assertIsNone(definition)

    def test_parameter_type_cannot_be_none_if_value_is_non_null(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Unit"))
            self._assert_success(db_map.add_parameter_definition_item(name="is_SI", entity_class_name="Unit"))
            self._assert_success(db_map.add_entity_item(name="gram", entity_class_name="Unit"))
            value, _ = to_database(2.3)
            value_item, error = db_map.add_parameter_value_item(
                entity_class_name="Unit",
                entity_byname=("gram",),
                parameter_definition_name="is_SI",
                alternative_name="Base",
                value=value,
                type=None,
            )
            self.assertEqual(error, "'type' is missing")
            self.assertIsNone(value_item)

    def test_parameter_definition_callbacks_get_called_on_default_value_update(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="bear"))
            definition = self._assert_success(
                db_map.add_parameter_definition_item(name="is_hibernating", entity_class_name="bear")
            )
            callback = mock.MagicMock(return_value=True)
            definition.add_update_callback(callback)
            value, value_type = to_database(2.3)
            definition.update(default_value=value, default_type=value_type)
            callback.assert_called_once_with(definition)

    def test_adding_fractional_duration_converts_time_units_correctly(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            duration = Duration(value=relativedelta(hours=1.5))
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="Object"))
                self._assert_success(db_map.add_entity_item(name="object", entity_class_name="Object"))
                self._assert_success(db_map.add_parameter_definition_item(name="count", entity_class_name="Object"))
                value, value_type = to_database(duration)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="Object",
                        entity_byname=("object",),
                        parameter_definition_name="count",
                        alternative_name="Base",
                        value=value,
                        type=value_type,
                    )
                )
                db_map.commit_session("Add test data")
            with DatabaseMapping(url) as db_map:
                value_item = db_map.get_parameter_value_item(
                    entity_class_name="Object",
                    entity_byname=("object",),
                    parameter_definition_name="count",
                    alternative_name="Base",
                )
                self.assertEqual(value_item["parsed_value"], Duration("90m"))

    def test_add_indirectly_purged_values_back(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            duration = Duration(value=relativedelta(hours=1.5))
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="Object"))
                self._assert_success(db_map.add_entity_item(name="object", entity_class_name="Object"))
                self._assert_success(db_map.add_parameter_definition_item(name="count", entity_class_name="Object"))
                value, value_type = to_database(duration)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="Object",
                        entity_byname=("object",),
                        parameter_definition_name="count",
                        alternative_name="Base",
                        value=value,
                        type=value_type,
                    )
                )
                db_map.commit_session("Add test data")
            with DatabaseMapping(url) as db_map:
                db_map.purge_items("entity")
                self._assert_success(db_map.add_entity_item(name="object", entity_class_name="Object"))
                value, value_type = to_database(duration)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="Object",
                        entity_byname=("object",),
                        parameter_definition_name="count",
                        alternative_name="Base",
                        value=value,
                        type=value_type,
                    )
                )
                value_item = db_map.get_parameter_value_item(
                    entity_class_name="Object",
                    entity_byname=("object",),
                    parameter_definition_name="count",
                    alternative_name="Base",
                )
                self.assertTrue(value_item)
                db_map.commit_session("Add purged data back.")
            with DatabaseMapping(url) as db_map:
                value_item = db_map.get_parameter_value_item(
                    entity_class_name="Object",
                    entity_byname=("object",),
                    parameter_definition_name="count",
                    alternative_name="Base",
                )
                self.assertTrue(value_item)
                self.assertEqual(value_item["parsed_value"], Duration("90m"))

    def test_add_purged_values_back(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            duration = Duration(value=relativedelta(hours=1.5))
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="Object"))
                self._assert_success(db_map.add_entity_item(name="object", entity_class_name="Object"))
                self._assert_success(db_map.add_parameter_definition_item(name="count", entity_class_name="Object"))
                value, value_type = to_database(duration)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="Object",
                        entity_byname=("object",),
                        parameter_definition_name="count",
                        alternative_name="Base",
                        value=value,
                        type=value_type,
                    )
                )
                db_map.commit_session("Add test data")
            with DatabaseMapping(url) as db_map:
                db_map.purge_items("parameter_value")
                db_map.purge_items("entity")
                db_map.commit_session("Purge items.")
                self._assert_success(db_map.add_entity_item(name="object", entity_class_name="Object"))
                value, value_type = to_database(duration)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="Object",
                        entity_byname=("object",),
                        parameter_definition_name="count",
                        alternative_name="Base",
                        value=value,
                        type=value_type,
                    )
                )
                value_item = db_map.get_parameter_value_item(
                    entity_class_name="Object",
                    entity_byname=("object",),
                    parameter_definition_name="count",
                    alternative_name="Base",
                )
                self.assertTrue(value_item)
                db_map.commit_session("Add purged data back.")
            with DatabaseMapping(url) as db_map:
                value_item = db_map.get_parameter_value_item(
                    entity_class_name="Object",
                    entity_byname=("object",),
                    parameter_definition_name="count",
                    alternative_name="Base",
                )
                self.assertTrue(value_item)
                self.assertEqual(value_item["parsed_value"], Duration("90m"))

    def test_add_purged_alternative_back_then_commit_and_add_other_items_back(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            duration = Duration(value=relativedelta(hours=1.5))
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="Object"))
                self._assert_success(db_map.add_entity_item(name="object", entity_class_name="Object"))
                self._assert_success(db_map.add_parameter_definition_item(name="count", entity_class_name="Object"))
                value, value_type = to_database(duration)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="Object",
                        entity_byname=("object",),
                        parameter_definition_name="count",
                        alternative_name="Base",
                        value=value,
                        type=value_type,
                    )
                )
                db_map.commit_session("Add test data")
            with DatabaseMapping(url) as db_map:
                db_map.purge_items("parameter_value")
                db_map.purge_items("entity")
                db_map.purge_items("alternative")
                self._assert_success(db_map.add_alternative_item(name="Base"))
                db_map.commit_session("Purge items but add alternative back.")
                self._assert_success(db_map.add_entity_item(name="object", entity_class_name="Object"))
                value, value_type = to_database(duration)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="Object",
                        entity_byname=("object",),
                        parameter_definition_name="count",
                        alternative_name="Base",
                        value=value,
                        type=value_type,
                    )
                )
                db_map.commit_session("Add data back.")
            with DatabaseMapping(url) as db_map:
                alternative_item = db_map.get_alternative_item(
                    name="Base",
                )
                self.assertTrue(alternative_item)
                self.assertEqual(alternative_item["name"], "Base")
                value_item = db_map.get_parameter_value_item(
                    entity_class_name="Object",
                    entity_byname=("object",),
                    parameter_definition_name="count",
                    alternative_name="Base",
                )
                self.assertTrue(value_item)
                self.assertEqual(value_item["parsed_value"], Duration("90m"))

    def test_invalid_color_values_get_rejected_in_entity_class_display_mode(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_display_mode_item(name="important"))
            _, error = db_map.add_entity_class_display_mode_item(
                entity_class_name="Object", display_mode_name="important", display_font_color=""
            )
            self.assertEqual(error, "invalid display_font_color for entity_class_display_mode")
            _, error = db_map.add_entity_class_display_mode_item(
                entity_class_name="Object", display_mode_name="important", display_font_color="ff"
            )
            self.assertEqual(error, "invalid display_font_color for entity_class_display_mode")
            _, error = db_map.add_entity_class_display_mode_item(
                entity_class_name="Object", display_mode_name="important", display_font_color="gggggg"
            )
            self.assertEqual(error, "invalid display_font_color for entity_class_display_mode")
            _, error = db_map.add_entity_class_display_mode_item(
                entity_class_name="Object", display_mode_name="important", display_background_color=""
            )
            self.assertEqual(error, "invalid display_background_color for entity_class_display_mode")
            _, error = db_map.add_entity_class_display_mode_item(
                entity_class_name="Object", display_mode_name="important", display_background_color="ff"
            )
            self.assertEqual(error, "invalid display_background_color for entity_class_display_mode")
            _, error = db_map.add_entity_class_display_mode_item(
                entity_class_name="Object",
                display_mode_name="important",
                display_background_color="gggggg",
            )
            self.assertEqual(error, "invalid display_background_color for entity_class_display_mode")

    def test_missing_unique_key_in_non_first_unique_key_set_gets_caught(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_scenario_item(name="Keys"))
            self._assert_success(db_map.add_alternative_item(name="ctrl"))
            _, error = db_map.add_scenario_alternative_item(scenario_name="Keys", alternative_name="ctrl")
            self.assertEqual(error, "missing rank")

    def test_list_values_get_removed_in_cascade_even_when_they_havent_been_fetched(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_parameter_value_list_item(name="Enum"))
                value, value_type = to_database("yes!")
                self._assert_success(
                    db_map.add_list_value_item(parameter_value_list_name="Enum", index=1, value=value, type=value_type)
                )
                db_map.commit_session("Add value list")
            with DatabaseMapping(url) as db_map:
                value_list = db_map.get_parameter_value_list_item(name="Enum")
                value_list.remove()
                db_map.commit_session("Remove value list")
            with DatabaseMapping(url) as db_map:
                list_value_rows = db_map.query(db_map.list_value_sq).all()
                self.assertEqual(len(list_value_rows), 0)

    def test_scenario_alternative_ids_dont_get_messed_up(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_scenario_item(name="Keys"))
            self._assert_success(db_map.add_alternative_item(name="ctrl"))
            self._assert_success(db_map.add_alternative_item(name="alt"))
            original_base = self._assert_success(
                db_map.add_scenario_alternative_item(scenario_name="Keys", alternative_name="Base", rank=1)
            )
            original_ctrl = self._assert_success(
                db_map.add_scenario_alternative_item(scenario_name="Keys", alternative_name="ctrl", rank=2)
            )
            original_alt = self._assert_success(
                db_map.add_scenario_alternative_item(scenario_name="Keys", alternative_name="alt", rank=3)
            )
            db_map.commit_session("Add scenario")
            original_base.remove()
            original_ctrl.remove()
            original_alt.remove()
            shuffled_alt = self._assert_success(
                db_map.add_scenario_alternative_item(scenario_name="Keys", alternative_name="alt", rank=1)
            )
            shuffled_alt.remove()
            db_map.commit_session("Shuffled scenario alternatives")
            scenario_alternatives = db_map.get_scenario_alternative_items()
            self.assertEqual(len(scenario_alternatives), 0)

    def test_swapping_scenario_alternatives_then_deleting_highest_rank_doesnt_violate_unique_constraints(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_scenario_item(name="Keys"))
            self._assert_success(db_map.add_alternative_item(name="alt"))
            self._assert_success(db_map.add_alternative_item(name="ctrl"))
            original_alt = self._assert_success(
                db_map.add_scenario_alternative_item(scenario_name="Keys", alternative_name="alt", rank=1)
            )
            original_ctrl = self._assert_success(
                db_map.add_scenario_alternative_item(scenario_name="Keys", alternative_name="ctrl", rank=2)
            )
            db_map.commit_session("Add scenario")
            original_alt.remove()
            original_ctrl.remove()
            self._assert_success(
                db_map.add_scenario_alternative_item(scenario_name="Keys", alternative_name="ctrl", rank=1)
            )
            shuffled_alt = self._assert_success(
                db_map.add_scenario_alternative_item(scenario_name="Keys", alternative_name="alt", rank=2)
            )
            shuffled_alt.remove()
            db_map.commit_session("Shuffled scenario alternatives")
            scenario_alternatives = db_map.get_scenario_alternative_items()
            self.assertEqual(len(scenario_alternatives), 1)

    def test_restore_item_whose_db_id_has_been_invalidated_by_id(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_alternative_item(name="alt"))
                db_map.commit_session("Add alternative")
            with DatabaseMapping(url) as db_map:
                alternative_in_db = db_map.get_alternative_item(name="alt")
                self.assertNotEqual(alternative_in_db, {})
                alternative_in_db.remove()
                replacement_alternative = self._assert_success(db_map.add_alternative_item(name="alt"))
                db_map.commit_session("Replace alternative")
                replacement_alternative.remove()
                alternative_in_db.restore()
                db_map.commit_session("No net changes")
            with DatabaseMapping(url) as db_map:
                restored_alternative = db_map.get_alternative_item(name="alt")
                self.assertEqual(restored_alternative["name"], "alt")

    def test_do_fetch_more_in_chunks(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="Widget"))
                self._assert_success(db_map.add_entity_item(name="widget1", entity_class_name="Widget"))
                self._assert_success(db_map.add_entity_class_item(name="Gadget"))
                self._assert_success(db_map.add_entity_item(name="gadget1", entity_class_name="Gadget"))
                db_map.commit_session("Add data.")
            with DatabaseMapping(url) as db_map:
                widgets = db_map.get_items("entity", entity_class_name="Widget")
                self.assertEqual(len(widgets), 1)
                gadgets = db_map.get_items("entity", entity_class_name="Gadget")
                self.assertEqual(len(gadgets), 1)

    def test_add_parameter_definition_to_database_with_parameter_types_does_not_raise_key_error(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Key"))
            repeat_rate = self._assert_success(
                db_map.add_parameter_definition_item(
                    name="repeat rate", entity_class_name="Key", parameter_type_list=("float",)
                )
            )
            self.assertEqual(repeat_rate["parameter_type_list"], ("float",))
            is_useful = self._assert_success(
                db_map.add_parameter_definition_item(name="is useful", entity_class_name="Key")
            )
            self.assertNotEqual(is_useful, {})

    def test_non_existent_elements_in_multidimensional_entity_addition_get_caught(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="A"))
            self._assert_success(db_map.add_entity_class_item(name="B"))
            a_b_class = self._assert_success(db_map.add_entity_class_item(dimension_name_list=("A", "B")))
            b_a_class = self._assert_success(db_map.add_entity_class_item(dimension_name_list=("B", "A")))
            abba_class = self._assert_success(
                db_map.add_entity_class_item(dimension_name_list=(a_b_class["name"], b_a_class["name"]))
            )
            item, error = db_map.add_entity_item(
                entity_byname=("a", "b", "b", "a"), entity_class_name=abba_class["name"]
            )
            self.assertEqual(error, "non-existent elements in byname ('a', 'b') for class A__B")
            self.assertIsNone(item)
            item, error = db_map.add_entity_item(entity_byname=("a", "b"), entity_class_name=a_b_class["name"])
            self.assertEqual(error, "non-existent elements in byname ('a', 'b') for class A__B")
            self.assertIsNone(item)
            self._assert_success(db_map.add_entity_item(name="a", entity_class_name="A"))
            item, error = db_map.add_entity_item(entity_byname=("a", "b"), entity_class_name=a_b_class["name"])
            self.assertEqual(error, "non-existent elements in byname ('a', 'b') for class A__B")
            self.assertIsNone(item)
            self._assert_success(db_map.add_entity_item(name="b", entity_class_name="B"))
            self._assert_success(db_map.add_entity_item(entity_byname=("a", "b"), entity_class_name=a_b_class["name"]))
            item, error = db_map.add_entity_item(
                entity_byname=("a", "b", "b", "a"), entity_class_name=abba_class["name"]
            )
            self.assertEqual(error, "non-existent elements in byname ('a', 'b', 'b', 'a') for class A__B__B__A")
            self.assertIsNone(item)
            self._assert_success(db_map.add_entity_item(entity_byname=("b", "a"), entity_class_name=b_a_class["name"]))
            self._assert_success(
                db_map.add_entity_item(entity_byname=("a", "b", "b", "a"), entity_class_name=abba_class["name"])
            )

    def test_becoming_referrer_when_reference_has_been_filtered_out(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="Object"))
                self._assert_success(db_map.add_entity_item(name="widget", entity_class_name="Object"))
                self._assert_success(
                    db_map.add_entity_alternative_item(
                        entity_class_name="Object", entity_byname=("widget",), alternative_name="Base", active=False
                    )
                )
                self._assert_success(db_map.add_scenario_item(name="HideWidget"))
                self._assert_success(
                    db_map.add_scenario_alternative_item(scenario_name="HideWidget", alternative_name="Base", rank=1)
                )
                db_map.commit_session("Add test data")
            filtered_url = append_filter_config(url, scenario_filter_config("HideWidget"))
            with DatabaseMapping(filtered_url) as db_map:
                db_map.fetch_all("entity_alternative")
                entity_alternatives = db_map.get_entity_alternative_items()
                self.assertEqual(len(entity_alternatives), 0)

    def test_add_parameter_definition_by_entity_class_id(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            object_class = self._assert_success(db_map.add_entity_class_item(name="Object"))
            x = self._assert_success(db_map.add_parameter_definition_item(name="x", entity_class_id=object_class["id"]))
            self.assertEqual(x["entity_class_name"], "Object")

    def test_removing_item_by_database_id(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_parameter_value_list_item(name="my_enum"))
                db_map.commit_session("Add value list.")
                list_id = db_map.query(db_map.parameter_value_list_sq).first().id
            with DatabaseMapping(url) as db_map:
                self._assert_success(db_map.remove_item("parameter_value_list", list_id))
                db_map.commit_session("Remove value list")
                value_lists = db_map.query(db_map.parameter_value_list_sq).all()
                self.assertEqual(value_lists, [])

    def test_with_block_reminder_exception(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        with self.assertRaises(
            SpineDBAPIError, msg="session is None; did you forget to use the DB map inside a 'with' block?"
        ):
            db_map.query(db_map.entity_sq)

    def test_create_parameter_type_table(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.fetch_all("parameter_type")

    def test_add_list_value_item_with_parsed_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_parameter_value_list_item(name="my list"))
            list_value = self._assert_success(
                db_map.add_list_value_item(parameter_value_list_name="my list", parsed_value=2.3, index=0)
            )
            self.assertTrue(list_value.mapped_item.has_value_been_parsed())
            value = from_database(list_value["value"], list_value["type"])
            self.assertEqual(value, 2.3)
            self.assertEqual(list_value["parsed_value"], 2.3)

    def test_add_parameter_definition_item_with_parsed_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Ring"))
            definition = self._assert_success(
                db_map.add_parameter_definition_item(name="radius", entity_class_name="Ring", parsed_value=2.3)
            )
            self.assertTrue(definition.mapped_item.has_value_been_parsed())
            value = from_database(definition["default_value"], definition["default_type"])
            self.assertEqual(value, 2.3)
            self.assertEqual(definition["parsed_value"], 2.3)

    def test_add_parameter_value_item_with_parsed_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Ring"))
            self._assert_success(db_map.add_parameter_definition_item(name="radius", entity_class_name="Ring"))
            self._assert_success(db_map.add_entity_item(name="master", entity_class_name="Ring"))
            parameter_value = self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="Ring",
                    parameter_definition_name="radius",
                    entity_byname=("master",),
                    alternative_name="Base",
                    parsed_value=2.3,
                )
            )
            self.assertTrue(parameter_value.mapped_item.has_value_been_parsed())
            value = from_database(parameter_value["value"], parameter_value["type"])
            self.assertEqual(value, 2.3)
            self.assertEqual(parameter_value["parsed_value"], 2.3)

    def test_update_parameter_definition_with_parsed_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            definition = self._assert_success(
                db_map.add_parameter_definition_item(name="y", entity_class_name="Object")
            )
            self.assertIsNone(definition["default_value"])
            definition.update(parsed_value=2.3)
            self.assertEqual(definition["parsed_value"], 2.3)

    def test_update_parameter_value_with_parsed_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_parameter_definition_item(name="y", entity_class_name="Object"))
            self._assert_success(db_map.add_entity_item(name="spoon", entity_class_name="Object"))
            value_item = self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="Object",
                    parameter_definition_name="y",
                    entity_byname=("spoon",),
                    alternative_name="Base",
                    parsed_value=None,
                )
            )
            self.assertIsNone(value_item["parsed_value"])
            value_item.update(parsed_value=2.3)
            self.assertEqual(value_item["parsed_value"], 2.3)

    def test_update_list_value_with_parsed_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_parameter_value_list_item(name="my values"))
            value_item = self._assert_success(
                db_map.add_list_value_item(parameter_value_list_name="my values", parsed_value=None, index=0)
            )
            self.assertIsNone(value_item["parsed_value"])
            value_item.update(parsed_value=2.3)
            self.assertEqual(value_item["parsed_value"], 2.3)

    def test_entity_class_bynames(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            item = self._assert_success(db_map.add_entity_class_item(name="Object"))
            self.assertEqual(item["entity_class_byname"], ("Object",))
            self._assert_success(db_map.add_entity_class_item(name="Subject"))
            item = self._assert_success(db_map.add_entity_class_item(dimension_name_list=("Subject", "Object")))
            self.assertEqual(item["entity_class_byname"], ("Subject", "Object"))
            self._assert_success(db_map.add_entity_class_item(dimension_name_list=("Object", "Subject")))
            item = self._assert_success(
                db_map.add_entity_class_item(dimension_name_list=("Subject__Object", "Object__Subject"))
            )
            self.assertEqual(item["entity_class_byname"], ("Subject", "Object", "Object", "Subject"))

    def test_entity_class_byname_is_in_extended_item(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_entity_item(name="fork", entity_class_name="Object"))
            self._assert_success(db_map.add_entity_class_item(name="Subject"))
            self._assert_success(db_map.add_entity_item(name="apple", entity_class_name="Subject"))
            self._assert_success(db_map.add_entity_class_item(dimension_name_list=("Object", "Subject")))
            self._assert_success(
                db_map.add_entity_item(element_name_list=("fork", "apple"), entity_class_name="Object__Subject")
            )
            class_item = db_map.get_entity_class_item(name="Object__Subject")
            item_dict = class_item.extended()
            self.assertIn("entity_class_byname", item_dict)
            self.assertEqual(item_dict["entity_class_byname"], ("Object", "Subject"))

    def test_get_parameter_definition_items_resolves_list_values(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_parameter_value_list_item(name="Color"))
                self._assert_success(
                    db_map.add_list_value_item(parameter_value_list_name="Color", parsed_value="blue", index=1)
                )
                self._assert_success(db_map.add_entity_class_item(name="Cat"))
                definition = self._assert_success(
                    db_map.add_parameter_definition_item(
                        name="color", entity_class_name="Cat", parameter_value_list_name="Color", parsed_value="blue"
                    )
                )
                db_map.commit_session("Add parameter definition with list value as default value.")
                self.assertEqual(definition["default_type"], "str")
                self.assertEqual(definition._asdict()["default_type"], "str")
            with DatabaseMapping(url) as db_map:
                definition = db_map.get_parameter_definition_item(entity_class_name="Cat", name="color")
                self.assertEqual(definition._asdict()["default_type"], "str")

    def test_add_item_unchecked_returns_public_item(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            item = self._assert_success(db_map.add_item("alternative", check=False, name="new"))
            self.assertIsInstance(item, PublicItem)
            self.assertEqual(item["name"], "new")

    def test_add_items_by_id_after_reset(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            widget = db_map.add_entity_class(name="Widget")
            db_map.commit_session("Add initial data.")
            spoon = db_map.add_entity(name="spoon", class_id=widget["id"].db_id)
            db_map.commit_session("Add initial data.")
            db_map.reset()
            with self.assertRaisesRegex(SpineDBAPIError, "there's already a entity_class with \\{'name': 'Widget'\\}"):
                db_map.add_entity_class(id=widget["id"].db_id, name="Widget")
            with self.assertRaisesRegex(
                SpineDBAPIError, "there's already a entity with \\{'entity_class_name': 'Widget', 'name': 'spoon'\\}"
            ):
                db_map.add_entity(id=spoon["id"].db_id, name="spoon", class_id=widget["id"].db_id)
            with self.assertRaises(NothingToCommit):
                db_map.commit_session("Add initial data.")

    def test_subclass_dimensions_must_match(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Object")
            db_map.add_entity_class(dimension_name_list=("Object",))
            db_map.add_entity_class(name="SuperObject")
            db_map.add_superclass_subclass(superclass_name="SuperObject", subclass_name="Object")
            with self.assertRaisesRegex(
                SpineDBAPIError, "subclass has different dimension count to existing subclasses"
            ):
                db_map.add_superclass_subclass(superclass_name="SuperObject", subclass_name="Object__")

    def test_subclass_cannot_be_superclass(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Object")
            db_map.add_entity_class(name="Subclass")
            db_map.add_superclass_subclass(superclass_name="Subclass", subclass_name="Object")
            db_map.add_entity_class(name="Superclass")
            with self.assertRaisesRegex(SpineDBAPIError, "subclass or any of its dimensions cannot be a superclass"):
                db_map.add_superclass_subclass(superclass_name="Superclass", subclass_name="Subclass")

    def test_none_of_subclass_dimensions_cannot_be_superclass(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Object")
            db_map.add_entity_class(name="Subclass")
            db_map.add_superclass_subclass(superclass_name="Subclass", subclass_name="Object")
            db_map.add_entity_class(name="Superclass")
            with self.assertRaisesRegex(SpineDBAPIError, "subclass or any of its dimensions cannot be a superclass"):
                db_map.add_superclass_subclass(superclass_name="Superclass", subclass_name="Subclass")

    def test_subclass_dimensions_must_match_on_subclass_update(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Object")
            db_map.add_entity_class(name="Relationship1", dimension_name_list=("Object",))
            db_map.add_entity_class(name="Relationship2", dimension_name_list=("Object",))
            db_map.add_entity_class(name="SuperObject")
            db_map.add_superclass_subclass(superclass_name="SuperObject", subclass_name="Relationship1")
            superclass_definition = db_map.add_superclass_subclass(
                superclass_name="SuperObject", subclass_name="Relationship2"
            )
            with self.assertRaisesRegex(
                SpineDBAPIError, "subclass has different dimension count to existing subclasses"
            ):
                db_map.update_superclass_subclass(id=superclass_definition["id"], subclass_name="Object")

    def test_subclass_dimensions_must_match_on_superclass_update(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Object")
            db_map.add_entity_class(name="Relationship", dimension_name_list=("Object",))
            db_map.add_entity_class(name="SuperObject1")
            superclass_definition = db_map.add_superclass_subclass(
                superclass_name="SuperObject1", subclass_name="Object"
            )
            db_map.add_entity_class(name="SuperObject2")
            db_map.add_superclass_subclass(superclass_name="SuperObject2", subclass_name="Relationship")
            with self.assertRaisesRegex(
                SpineDBAPIError, "subclass has different dimension count to existing subclasses"
            ):
                db_map.update_superclass_subclass(id=superclass_definition["id"], superclass_name="SuperObject2")

    def test_duplicate_entity_names_in_subclass(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="A")
            db_map.add_entity_class(name="B")
            db_map.add_entity_class(name="C")
            db_map.add_superclass_subclass(superclass_name="C", subclass_name="A")
            db_map.add_superclass_subclass(superclass_name="C", subclass_name="B")
            db_map.add_entity_class(dimension_name_list=("C", "C"))
            db_map.add_entity(entity_class_name="A", name="a")
            with self.assertRaisesRegex(
                SpineDBAPIError, "there's already a entity with {'entity_class_name': 'C', 'name': 'a'}"
            ):
                db_map.add_entity(entity_class_name="B", name="a")
            entity = db_map.add_entity(entity_class_name="C__C", entity_byname=("a", "a"))
            self.assertEqual(entity["entity_byname"], ("a", "a"))
            self.assertEqual(entity["dimension_name_list"], ("C", "C"))
            entity = db_map.entity(entity_class_name="C", entity_byname=("a",))
            self.assertEqual(entity["entity_class_name"], "A")

    def test_duplicate_entity_names_in_deep_subclass(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="node")
            db_map.add_entity_class(name="unit")
            db_map.add_entity_class(name="node__unit", dimension_name_list=("node", "unit"))
            db_map.add_entity_class(name="unit__node", dimension_name_list=("unit", "node"))
            db_map.add_entity_class(name="flow")
            db_map.add_superclass_subclass(superclass_name="flow", subclass_name="node__unit")
            db_map.add_superclass_subclass(superclass_name="flow", subclass_name="unit__node")
            db_map.add_entity_class(name="flow__flow", dimension_name_list=("flow", "flow"))
            db_map.add_entity(entity_class_name="node", name="a")
            db_map.add_entity(entity_class_name="unit", name="a")
            db_map.add_entity(entity_class_name="node__unit", entity_byname=("a", "a"))
            with self.assertRaisesRegex(
                SpineDBAPIError,
                "there's already a entity with {'entity_class_name': 'flow', 'entity_byname': \\('a', 'a'\\)}",
            ):
                db_map.add_entity(entity_class_name="unit__node", entity_byname=("a", "a"))
            entity = db_map.add_entity(entity_class_name="flow__flow", entity_byname=("a", "a", "a", "a"))
            self.assertEqual(entity["entity_byname"], ("a", "a", "a", "a"))
            entity = db_map.entity(entity_class_name="flow", entity_byname=("a", "a"))
            self.assertEqual(entity["entity_class_name"], "node__unit")

    def test_creating_superclass_with_entities_fails(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Superclass")
            db_map.add_entity(name="object", entity_class_name="Superclass")
            db_map.add_entity_class(name="Subclass")
            with self.assertRaisesRegex(SpineDBAPIError, "cannot turn a class that has entities into superclass"):
                db_map.add_superclass_subclass(superclass_name="Superclass", subclass_name="Subclass")

    def test_updating_to_superclass_with_entities_fails(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Superclass")
            db_map.add_entity_class(name="Subclass")
            superclass_subclass = db_map.add_superclass_subclass(superclass_name="Superclass", subclass_name="Subclass")
            db_map.add_entity_class(name="ClassWithEntities")
            db_map.add_entity(name="object", entity_class_name="ClassWithEntities")
            with self.assertRaisesRegex(SpineDBAPIError, "cannot turn a class that has entities into superclass"):
                superclass_subclass.update(superclass_name="ClassWithEntities")

    def test_creating_subclass_with_entities_fails(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Superclass")
            db_map.add_entity_class(name="Subclass")
            db_map.add_entity(name="object", entity_class_name="Subclass")
            with self.assertRaisesRegex(
                SpineDBAPIError, "can't set or modify the superclass for a class that already has entities"
            ):
                db_map.add_superclass_subclass(superclass_name="Superclass", subclass_name="Subclass")

    def test_updating_to_subclass_with_entities_fails(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Superclass")
            db_map.add_entity_class(name="Subclass")
            superclass_subclass = db_map.add_superclass_subclass(superclass_name="Superclass", subclass_name="Subclass")
            db_map.add_entity_class(name="ClassWithEntities")
            db_map.add_entity(name="object", entity_class_name="ClassWithEntities")
            with self.assertRaisesRegex(
                SpineDBAPIError, "can't set or modify the superclass for a class that already has entities"
            ):
                superclass_subclass.update(subclass_name="ClassWithEntities")

    def test_creating_superclass_with_non_zero_dimension_count_fails(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Widget")
            db_map.add_entity_class(name="Superclass", dimension_name_list=("Widget",))
            db_map.add_entity_class(name="Subclass")
            with self.assertRaisesRegex(SpineDBAPIError, "superclass cannot have more than zero dimensions"):
                db_map.add_superclass_subclass(superclass_name="Superclass", subclass_name="Subclass")

    def test_updating_to_superclass_with_non_zero_dimension_count_fails(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Superclass")
            db_map.add_entity_class(name="Subclass")
            superclass_subclass = db_map.add_superclass_subclass(superclass_name="Superclass", subclass_name="Subclass")
            db_map.add_entity_class(name="Widget")
            db_map.add_entity_class(name="InvalidSuperclass", dimension_name_list=("Widget",))
            with self.assertRaisesRegex(SpineDBAPIError, "superclass cannot have more than zero dimensions"):
                superclass_subclass.update(superclass_name="InvalidSuperclass")

    def test_add_entities_to_superclass_fails(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Superclass")
            db_map.add_entity_class(name="Subclass")
            db_map.add_superclass_subclass(superclass_name="Superclass", subclass_name="Subclass")
            with self.assertRaisesRegex(SpineDBAPIError, "an entity class that is a superclass cannot have entities"):
                db_map.add_entity(name="object", entity_class_name="Superclass")

    def test_move_entity_to_superclass_fails(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Superclass")
            db_map.add_entity_class(name="Subclass")
            db_map.add_superclass_subclass(superclass_name="Superclass", subclass_name="Subclass")
            db_map.add_entity_class(name="Widget")
            entity = db_map.add_entity(name="object", entity_class_name="Widget")
            with self.assertRaisesRegex(SpineDBAPIError, "an entity class that is a superclass cannot have entities"):
                entity.update(entity_class_name="Superclass")

    def test_reset_resets_purging(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                db_map.add_parameter_value_list(name="enum")
                db_map.commit_session("Add test data")
            with DatabaseMapping(url) as db_map:
                db_map.purge_items("parameter_value_list")
                db_map.reset()
                items = db_map.find_parameter_value_lists()
                self.assertEqual(len(items), 1)
                self.assertEqual(items[0]["name"], "enum")

    def test_data_is_fetched_again_after_reset(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            create_new_spine_database(url)
            with DatabaseMapping(url) as db_map:
                self.assertTrue(db_map.has_external_commits())
                db_map.fetch_all("alternative")
                self.assertFalse(db_map.has_external_commits())
                db_map.reset()
                self.assertTrue(db_map.has_external_commits())

    def test_purge_parameter_definition_with_default_value_from_value_list(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                db_map.add_parameter_value_list(name="possibilities")
                db_map.add_list_value(parameter_value_list_name="possibilities", parsed_value="infinite", index=0)
                db_map.add_entity_class(name="Object")
                db_map.add_parameter_definition(
                    entity_class_name="Object",
                    name="X",
                    parameter_value_list_name="possibilities",
                    parsed_value="infinite",
                )
                db_map.commit_session("Add test data.")
            with DatabaseMapping(url) as db_map:
                db_map.purge_items("parameter_value_list")
                db_map.purge_items("parameter_definition")
                db_map.commit_session("Purge.")
                definitions = db_map.find_parameter_definitions()
                self.assertEqual(definitions, [])
                list_values = db_map.find_list_values()
                self.assertEqual(list_values, [])
                value_lists = db_map.find_parameter_value_lists()
                self.assertEqual(value_lists, [])
            with DatabaseMapping(url) as db_map:
                self.assertEqual(db_map.query(db_map.parameter_value_list_sq).all(), [])
                self.assertEqual(db_map.query(db_map.list_value_sq).all(), [])
                self.assertEqual(db_map.query(db_map.parameter_definition_sq).all(), [])

    def test_add_entity_with_location_data(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Object")
            lat_lon_entity = db_map.add_entity(entity_class_name="Object", name="lat_lon", lat=2.3, lon=3.2)
            self.assertEqual(lat_lon_entity["lat"], 2.3)
            self.assertEqual(lat_lon_entity["lon"], 3.2)
            self.assertEqual(lat_lon_entity["alt"], None)
            self.assertIsNone(lat_lon_entity["shape_name"])
            self.assertIsNone(lat_lon_entity["shape_blob"])
            lat_lon_alt_entity = db_map.add_entity(
                entity_class_name="Object", name="lat_lon_alt", lat=2.3, lon=3.2, alt=55.0
            )
            self.assertEqual(lat_lon_alt_entity["lat"], 2.3)
            self.assertEqual(lat_lon_alt_entity["lon"], 3.2)
            self.assertEqual(lat_lon_alt_entity["alt"], 55.0)
            self.assertIsNone(lat_lon_alt_entity["shape_name"])
            self.assertIsNone(lat_lon_alt_entity["shape_blob"])
            shape_blob_and_name_entity = db_map.add_entity(
                entity_class_name="Object", name="name_shape_blob", shape_name="island", shape_blob="{}"
            )
            self.assertIsNone(shape_blob_and_name_entity["lat"])
            self.assertIsNone(shape_blob_and_name_entity["lon"])
            self.assertIsNone(shape_blob_and_name_entity["alt"])
            self.assertEqual(shape_blob_and_name_entity["shape_name"], "island")
            self.assertEqual(shape_blob_and_name_entity["shape_blob"], "{}")
            entity = db_map.add_entity(
                entity_class_name="Object",
                name="all_data",
                lat=2.3,
                lon=-3.2,
                alt=55.0,
                shape_name="island",
                shape_blob="{}",
            )
            self.assertEqual(entity["lat"], 2.3)
            self.assertEqual(entity["lon"], -3.2)
            self.assertEqual(entity["alt"], 55.0)
            self.assertEqual(entity["shape_name"], "island")
            self.assertEqual(entity["shape_blob"], "{}")

    def test_add_entity_with_incomplete_location_data_raises(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Object")
            with self.assertRaisesRegex(SpineDBAPIError, "cannot set latitude without longitude"):
                db_map.add_entity(entity_class_name="Object", name="gadget", lat=2.3)
            with self.assertRaisesRegex(SpineDBAPIError, "cannot set longitude without latitude"):
                db_map.add_entity(entity_class_name="Object", name="gadget", lon=3.2)
            with self.assertRaisesRegex(SpineDBAPIError, "cannot set altitude without latitude and longitude"):
                db_map.add_entity(entity_class_name="Object", name="gadget", alt=55.0)
            with self.assertRaisesRegex(SpineDBAPIError, "cannot set shape_name without shape_blob"):
                db_map.add_entity(entity_class_name="Object", name="gadget", shape_name="island")
            with self.assertRaisesRegex(SpineDBAPIError, "cannot set shape_blob without shape_name"):
                db_map.add_entity(entity_class_name="Object", name="gadget", shape_blob="{}}")

    def test_location_data_available_from_database(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                db_map.add_entity_class(name="Object")
                db_map.add_entity(entity_class_name="Object", name="no_location")
                db_map.add_entity(
                    entity_class_name="Object",
                    name="locations_and_shapes",
                    lat=2.3,
                    lon=3.2,
                    alt=55.0,
                    shape_name="hexagon",
                    shape_blob="{}",
                )
                db_map.commit_session("Add test data.")
            with DatabaseMapping(url) as db_map:
                no_location = db_map.entity(entity_class_name="Object", name="no_location")
                self.assertIsNone(no_location["lat"])
                self.assertIsNone(no_location["lon"])
                self.assertIsNone(no_location["alt"])
                self.assertIsNone(no_location["shape_name"])
                self.assertIsNone(no_location["shape_blob"])
                with_location = db_map.entity(entity_class_name="Object", name="locations_and_shapes")
                self.assertEqual(with_location["lat"], 2.3)
                self.assertEqual(with_location["lon"], 3.2)
                self.assertEqual(with_location["alt"], 55.0)
                self.assertEqual(with_location["shape_name"], "hexagon")
                self.assertEqual(with_location["shape_blob"], "{}")

    def test_entity_location_data_available_in_asdict(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            entity_class = db_map.add_entity_class(name="Object")
            no_location = db_map.add_entity(entity_class_name="Object", name="no_location")
            yes_location = db_map.add_entity(
                entity_class_name="Object",
                name="yes_location",
                lat=2.3,
                lon=3.2,
                alt=55.0,
                shape_name="hexagon",
                shape_blob="{}",
            )
            self.assertEqual(
                no_location._asdict(),
                {
                    "class_id": entity_class["id"],
                    "id": no_location["id"],
                    "name": "no_location",
                    "element_id_list": (),
                    "description": None,
                    "lat": None,
                    "lon": None,
                    "alt": None,
                    "shape_name": None,
                    "shape_blob": None,
                },
            )
            self.assertEqual(
                yes_location._asdict(),
                {
                    "class_id": entity_class["id"],
                    "id": yes_location["id"],
                    "name": "yes_location",
                    "element_id_list": (),
                    "description": None,
                    "lat": 2.3,
                    "lon": 3.2,
                    "alt": 55.0,
                    "shape_name": "hexagon",
                    "shape_blob": "{}",
                },
            )

    def test_entity_location_is_removed_in_cascade_with_entity(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Object")
            entity = db_map.add_entity(entity_class_name="Object", name="cube", lat=2.3, lon=3.2)
            locations = db_map.find_entity_locations(lat=2.3, lon=3.2)
            self.assertEqual(len(locations), 1)
            self.assertTrue(locations[0].is_valid())
            entity.remove()
            self.assertFalse(locations[0].is_valid())

    def test_entity_location_is_removed_when_all_its_data_is_set_to_none(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Object")
            lat_lon_entity = db_map.add_entity(entity_class_name="Object", name="point", lat=2.3, lon=3.2)
            locations = db_map.find_entity_locations(lat=2.3, lon=3.2)
            self.assertEqual(len(locations), 1)
            self.assertTrue(locations[0].is_valid())
            lat_lon_entity.update(lat=None, lon=None)
            self.assertFalse(locations[0].is_valid())
            shape_entity = db_map.add_entity(
                entity_class_name="Object", name="polygon", shape_name="hexagon", shape_blob="{}"
            )
            locations = db_map.find_entity_locations(shape_name="hexagon", shape_blob="{}")
            self.assertEqual(len(locations), 1)
            self.assertTrue(locations[0].is_valid())
            shape_entity.update(shape_name=None, shape_blob=None)
            self.assertFalse(locations[0].is_valid())

    def test_entity_location_is_not_removed_when_some_of_its_data_is_set_to_none(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Object")
            locationed_entity1 = db_map.add_entity(
                entity_class_name="Object", name="soon_polygon", lat=2.3, lon=3.2, shape_name="hexagon", shape_blob="{}"
            )
            locations = db_map.find_entity_locations(lat=2.3, lon=3.2)
            self.assertEqual(len(locations), 1)
            self.assertTrue(locations[0].is_valid())
            locationed_entity1.update(lat=None, lon=None)
            self.assertTrue(locations[0].is_valid())
            self.assertEqual(locations[0]["entity_byname"], ("soon_polygon",))
            self.assertIsNone(locations[0]["lat"])
            self.assertIsNone(locations[0]["lon"])
            self.assertIsNone(locations[0]["alt"])
            self.assertEqual(locations[0]["shape_name"], "hexagon")
            self.assertEqual(locations[0]["shape_blob"], "{}")
            locationed_entity2 = db_map.add_entity(
                entity_class_name="Object",
                name="soon_point",
                lat=2.3,
                lon=3.2,
                alt=55.0,
                shape_name="hexagon",
                shape_blob="{}",
            )
            locations = db_map.find_entity_locations(lat=2.3, lon=3.2)
            self.assertEqual(len(locations), 1)
            self.assertTrue(locations[0].is_valid())
            locationed_entity2.update(shape_name=None, shape_blob=None)
            self.assertTrue(locations[0].is_valid())
            self.assertEqual(locations[0]["entity_byname"], ("soon_point",))
            self.assertEqual(locations[0]["lat"], 2.3)
            self.assertEqual(locations[0]["lon"], 3.2)
            self.assertEqual(locations[0]["alt"], 55.0)
            self.assertIsNone(locations[0]["shape_name"])
            self.assertIsNone(locations[0]["shape_blob"])

    def test_updating_entitys_location_data_adds_location_item(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Object")
            entity = db_map.add_entity(entity_class_name="Object", name="soon_to_have_location")
            self.assertEqual(db_map.find_entity_locations(), [])
            entity.update(lat=2.3, lon=3.2)
            locations = db_map.find_entity_locations()
            self.assertEqual(len(locations), 1)
            self.assertEqual(locations[0]["entity_byname"], ("soon_to_have_location",))
            self.assertEqual(locations[0]["lat"], 2.3)
            self.assertEqual(locations[0]["lon"], 3.2)
            self.assertIsNone(locations[0]["alt"])
            self.assertIsNone(locations[0]["shape_name"])
            self.assertIsNone(locations[0]["shape_blob"])
            entity.update(lat=None, lon=None)
            self.assertEqual(db_map.find_entity_locations(), [])
            entity.update(shape_name="polygon", shape_blob="{}")
            locations = db_map.find_entity_locations()
            self.assertEqual(len(locations), 1)
            self.assertEqual(locations[0]["entity_byname"], ("soon_to_have_location",))
            self.assertIsNone(locations[0]["lat"])
            self.assertIsNone(locations[0]["lon"])
            self.assertIsNone(locations[0]["alt"])
            self.assertEqual(locations[0]["shape_name"], "polygon")
            self.assertEqual(locations[0]["shape_blob"], "{}")

    def test_updating_location_of_entity_from_database(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                db_map.add_entity_class(name="Unit")
                db_map.add_entity(entity_class_name="Unit", name="mana_source")
                db_map.commit_session("Add test data.")
            with DatabaseMapping(url) as db_map:
                entity = db_map.entity(entity_class_name="Unit", name="mana_source")
                entity.update(lat=2.3, lon=3.2)
                self.assertEqual(entity["lat"], 2.3)
                self.assertEqual(entity["lon"], 3.2)

    def test_update_fetched_entitys_location_by_single_field(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                db_map.add_entity_class(name="Unit")
                db_map.add_entity(
                    entity_class_name="Unit",
                    name="mana_source",
                    lat=2.3,
                    lon=3.2,
                    alt=55.0,
                    shape_name="diagon",
                    shape_blob="{}",
                )
                db_map.commit_session("Add test data.")
            with DatabaseMapping(url) as db_map:
                entity = db_map.entity(entity_class_name="Unit", name="mana_source")
                self.assertEqual(entity["lat"], 2.3)
                self.assertEqual(entity["lon"], 3.2)
                self.assertEqual(entity["alt"], 55.0)
                self.assertEqual(entity["shape_name"], "diagon")
                self.assertEqual(entity["shape_blob"], "{}")
                entity.update(lat=-2.3)
                self.assertEqual(entity["lat"], -2.3)
                self.assertEqual(entity["lon"], 3.2)
                self.assertEqual(entity["alt"], 55.0)
                self.assertEqual(entity["shape_name"], "diagon")
                self.assertEqual(entity["shape_blob"], "{}")
                entity.update(lon=-3.2)
                self.assertEqual(entity["lat"], -2.3)
                self.assertEqual(entity["lon"], -3.2)
                self.assertEqual(entity["alt"], 55.0)
                self.assertEqual(entity["shape_name"], "diagon")
                self.assertEqual(entity["shape_blob"], "{}")
                entity.update(alt=-55.0)
                self.assertEqual(entity["lat"], -2.3)
                self.assertEqual(entity["lon"], -3.2)
                self.assertEqual(entity["alt"], -55.0)
                self.assertEqual(entity["shape_name"], "diagon")
                self.assertEqual(entity["shape_blob"], "{}")
                entity.update(shape_name="polygram")
                self.assertEqual(entity["lat"], -2.3)
                self.assertEqual(entity["lon"], -3.2)
                self.assertEqual(entity["alt"], -55.0)
                self.assertEqual(entity["shape_name"], "polygram")
                self.assertEqual(entity["shape_blob"], "{}")
                entity.update(shape_blob='{"feature": {}}')
                self.assertEqual(entity["lat"], -2.3)
                self.assertEqual(entity["lon"], -3.2)
                self.assertEqual(entity["alt"], -55.0)
                self.assertEqual(entity["shape_name"], "polygram")
                self.assertEqual(entity["shape_blob"], '{"feature": {}}')

    def test_update_entitys_location_with_half_new_data(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                db_map.add_entity_class(name="Unit")
                db_map.add_entity(
                    entity_class_name="Unit",
                    name="mana_source",
                    lat=2.3,
                    lon=3.2,
                    alt=55.0,
                    shape_name="diagon",
                    shape_blob="{}",
                )
                db_map.commit_session("Add test data.")
            with DatabaseMapping(url) as db_map:
                entity = db_map.entity(entity_class_name="Unit", name="mana_source")
                entity.update(lat=-2.3, lon=3.2)
                self.assertEqual(entity["lat"], -2.3)
                self.assertEqual(entity["lon"], 3.2)
                self.assertEqual(entity["alt"], 55.0)
                self.assertEqual(entity["shape_name"], "diagon")
                self.assertEqual(entity["shape_blob"], "{}")
                entity.update(lat=-2.3, lon=-3.2)
                self.assertEqual(entity["lat"], -2.3)
                self.assertEqual(entity["lon"], -3.2)
                self.assertEqual(entity["alt"], 55.0)
                self.assertEqual(entity["shape_name"], "diagon")
                self.assertEqual(entity["shape_blob"], "{}")
                entity.update(lat=-2.3, lon=-3.2, alt=-55.0)
                self.assertEqual(entity["lat"], -2.3)
                self.assertEqual(entity["lon"], -3.2)
                self.assertEqual(entity["alt"], -55.0)
                self.assertEqual(entity["shape_name"], "diagon")
                self.assertEqual(entity["shape_blob"], "{}")
                entity.update(shape_name="polygram", shape_blob="{}")
                self.assertEqual(entity["lat"], -2.3)
                self.assertEqual(entity["lon"], -3.2)
                self.assertEqual(entity["alt"], -55.0)
                self.assertEqual(entity["shape_name"], "polygram")
                self.assertEqual(entity["shape_blob"], "{}")
                entity.update(shape_name="polygram", shape_blob='{"feature": {}}')
                self.assertEqual(entity["lat"], -2.3)
                self.assertEqual(entity["lon"], -3.2)
                self.assertEqual(entity["alt"], -55.0)
                self.assertEqual(entity["shape_name"], "polygram")
                self.assertEqual(entity["shape_blob"], '{"feature": {}}')

    def test_updating_entitys_location_data_with_missing_data_raises_exception(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Object")
            entity = db_map.add_entity(entity_class_name="Object", name="soon_to_have_location")
            with self.assertRaisesRegex(SpineDBAPIError, "latitude cannot be set if longitude is None"):
                entity.update(lat=2.3)
            with self.assertRaisesRegex(SpineDBAPIError, "longitude cannot be set if latitude is None"):
                entity.update(lon=3.2)
            with self.assertRaisesRegex(SpineDBAPIError, "altitude cannot be set if latitude and longitude are None"):
                entity.update(alt=55.0)
            with self.assertRaisesRegex(SpineDBAPIError, "shape_name cannot be set if shape_blob is None"):
                entity.update(shape_name="monogon")
            with self.assertRaisesRegex(SpineDBAPIError, "shape_blob cannot be set if shape_name is None"):
                entity.update(shape_blob="{}}")

    def test_removing_entity_location_sets_entitys_location_id_to_none(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Object")
            sphere = db_map.add_entity(entity_class_name="Object", name="sphere", lat=2.3, lon=3.2)
            location = db_map.entity_location(entity_class_name="Object", entity_byname=("sphere",))
            location.remove()
            self.assertIsNone(sphere["lat"])
            self.assertIsNone(sphere["lon"])

    def test_updating_manually_removed_entity_location_via_entity_restores_location(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Object")
            sphere = db_map.add_entity(entity_class_name="Object", name="sphere", lat=2.3, lon=3.2)
            location = db_map.entity_location(entity_class_name="Object", entity_byname=("sphere",))
            location.remove()
            self.assertFalse(location.is_valid())
            sphere.update(shape_name="metagon", shape_blob="{}")
            self.assertTrue(location.is_valid())
            self.assertIsNone(location["lat"])
            self.assertIsNone(location["lon"])
            self.assertIsNone(location["alt"])
            self.assertEqual(location["shape_name"], "metagon")
            self.assertEqual(location["shape_blob"], "{}")

    def test_restoring_entity_restores_its_location(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Object")
            sphere = db_map.add_entity(entity_class_name="Object", name="sphere", lat=2.3, lon=3.2)
            location = db_map.entity_location(entity_class_name="Object", entity_byname=("sphere",))
            sphere.remove()
            self.assertFalse(location.is_valid())
            sphere.restore()
            self.assertTrue(location.is_valid())
            self.assertEqual(sphere["lat"], 2.3)
            self.assertEqual(sphere["lon"], 3.2)

    def test_purge_entity_location_sets_entitys_location_data_to_none(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Object")
            entity = db_map.add_entity(entity_class_name="Object", name="mouse", lat=2.3, lon=3.2)
            db_map.purge_items("entity_location")
            self.assertIsNone(entity["lat"])
            self.assertIsNone(entity["lon"])
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Object")
            entity = db_map.add_entity(entity_class_name="Object", name="mouse", lat=2.3, lon=3.2)
            self.assertEqual(entity["lat"], 2.3)
            self.assertEqual(entity["lon"], 3.2)
            db_map.purge_items("entity_location")
            self.assertIsNone(entity["lat"])
            self.assertIsNone(entity["lon"])
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                db_map.add_entity_class(name="Object")
                db_map.add_entity(entity_class_name="Object", name="mouse", lat=2.3, lon=3.2)
                db_map.commit_session("Add test data.")
            with DatabaseMapping(url) as db_map:
                db_map.purge_items("entity_location")
                entity = db_map.entity(entity_class_name="Object", name="mouse")
                self.assertIsNone(entity["lat"])
                self.assertIsNone(entity["lon"])

    def test_fetching_after_refresh_session_should_not_fail(self):
        with TemporaryDirectory() as temp_dir:
            target_url = "sqlite:///" + os.path.join(temp_dir, "target.sqlite")
            with DatabaseMapping(target_url, create=True) as db_map:
                db_map.add_entity_class(name="unit")
                db_map.add_entity(entity_class_name="unit", name="power_plant_a")
                db_map.add_entity_class(name="node")
                db_map.add_entity(entity_class_name="node", name="fuel_node")
                db_map.add_entity_class(name="unit__to_node", dimension_name_list=("unit", "node"))
                db_map.add_parameter_definition(entity_class_name="unit__to_node", name="unit_capacity")
                db_map.add_parameter_definition(entity_class_name="unit__to_node", name="vom_cost")
                db_map.add_entity(
                    entity_class_name="unit__to_node",
                    entity_byname=("power_plant_a", "fuel_node"),
                )
                db_map.add_parameter_value(
                    entity_class_name="unit__to_node",
                    entity_byname=("power_plant_a", "fuel_node"),
                    parameter_definition_name="vom_cost",
                    parsed_value=55.0,
                    alternative_name="Base",
                )
                db_map.commit_session("Add base data")
            with DatabaseMapping(target_url) as db_map:
                db_map.add_parameter_value(
                    entity_class_name="unit__to_node",
                    entity_byname=("power_plant_a", "fuel_node"),
                    parameter_definition_name="unit_capacity",
                    parsed_value=2.3,
                    alternative_name="Base",
                )
                db_map.refresh_session()
                db_map.fetch_all("parameter_value")
                value = db_map.parameter_value(
                    entity_class_name="unit__to_node",
                    entity_byname=("power_plant_a", "fuel_node"),
                    parameter_definition_name="vom_cost",
                    alternative_name="Base",
                )
                self.assertEqual(value["parsed_value"], 55.0)

    def test_refresh_session_should_not_destroy_added_value(self):
        with TemporaryDirectory() as temp_dir:
            target_url = "sqlite:///" + os.path.join(temp_dir, "target.sqlite")
            with DatabaseMapping(target_url, create=True) as db_map:
                db_map.add_entity_class(name="unit")
                db_map.add_entity(entity_class_name="unit", name="power_plant_a")
                db_map.add_entity_class(name="node")
                db_map.add_entity(entity_class_name="node", name="fuel_node")
                db_map.add_entity_class(name="unit__to_node", dimension_name_list=("unit", "node"))
                db_map.add_parameter_definition(entity_class_name="unit__to_node", name="unit_capacity")
                db_map.add_parameter_definition(entity_class_name="unit__to_node", name="vom_cost")
                db_map.add_entity(
                    entity_class_name="unit__to_node",
                    entity_byname=("power_plant_a", "fuel_node"),
                )
                db_map.add_parameter_value(
                    entity_class_name="unit__to_node",
                    entity_byname=("power_plant_a", "fuel_node"),
                    parameter_definition_name="vom_cost",
                    parsed_value=55.0,
                    alternative_name="Base",
                )
                db_map.commit_session("Add base data")
            with DatabaseMapping(target_url) as db_map:
                db_map.add_parameter_value(
                    entity_class_name="unit__to_node",
                    entity_byname=("power_plant_a", "fuel_node"),
                    parameter_definition_name="unit_capacity",
                    parsed_value=2.3,
                    alternative_name="Base",
                )
                db_map.refresh_session()
                db_map.commit_session("Add value.")
            with DatabaseMapping(target_url) as db_map:
                value_item = db_map.parameter_value(
                    entity_class_name="unit__to_node",
                    entity_byname=("power_plant_a", "fuel_node"),
                    parameter_definition_name="unit_capacity",
                    alternative_name="Base",
                )
                self.assertEqual(value_item["parsed_value"], 2.3)

    def test_existing_entity_items_location_data_in_asdict(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                db_map.add_entity_class(name="Object")
                db_map.add_entity(entity_class_name="Object", name="nothing")
                db_map.add_entity(
                    entity_class_name="Object",
                    name="anything",
                    lat=2.3,
                    lon=3.2,
                    alt=55.0,
                    shape_name="blob",
                    shape_blob="{}",
                )
                db_map.commit_session("Add test data")
            with DatabaseMapping(url) as db_map:
                entity_dict = db_map.entity(entity_class_name="Object", name="nothing")._asdict()
                self.assertIsNone(entity_dict["lat"])
                self.assertIsNone(entity_dict["lon"])
                self.assertIsNone(entity_dict["alt"])
                self.assertIsNone(entity_dict["shape_name"])
                self.assertIsNone(entity_dict["shape_blob"])
                entity_dict = db_map.entity(entity_class_name="Object", name="anything")._asdict()
                self.assertEqual(entity_dict["lat"], 2.3)
                self.assertEqual(entity_dict["lon"], 3.2)
                self.assertEqual(entity_dict["alt"], 55.0)
                self.assertEqual(entity_dict["shape_name"], "blob")
                self.assertEqual(entity_dict["shape_blob"], "{}")

    def test_updating_entity_location_does_not_update_its_parameter_values(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_entity_class(name="Object")
            db_map.add_parameter_definition(entity_class_name="Object", name="X")
            fork = db_map.add_entity(entity_class_name="Object", name="fork")
            x = db_map.add_parameter_value(
                entity_class_name="Object",
                entity_byname=("fork",),
                parameter_definition_name="X",
                alternative_name="Base",
                parsed_value=2.3,
            )
            update_callback = mock.MagicMock()
            x.add_update_callback(update_callback)
            fork.update(lat=2.3, lon=3.2)
            update_callback.assert_not_called()

    def test_dirty_ids(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_parameter_value_list(name="list in db")
            renamed_list = db_map.add_parameter_value_list(name="another list")
            db_map.commit_session("Add value list.")
            new_list = db_map.add_parameter_value_list(name="new list")
            renamed_list.update(name="renamed list")
            self.assertCountEqual(db_map.dirty_ids("parameter_value_list"), [new_list["id"], renamed_list["id"]])

    def test_mapped_table_error_message(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            with self.assertRaisesRegex(SpineDBAPIError, "Invalid item type 'anon' - maybe you meant 'scenario'?"):
                db_map.mapped_table("anon")

    def test_add_errors(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            with self.assertRaisesRegex(SpineDBAPIError, "missing name"):
                db_map.add_entity_class()
            with self.assertRaisesRegex(
                SpineDBAPIError, "invalid type for 'name' of 'entity_class' - got int, expected str"
            ):
                db_map.add_entity_class(name=23)
            with self.assertRaisesRegex(SpineDBAPIError, "no entity_class matching {'name': 'Subject'}"):
                db_map.add_entity(entity_class_name="Subject", name="pleb")
            db_map.add_entity_class(name="Object")
            db_map.add_entity(entity_class_name="Object", name="spoon")
            db_map.add_entity_class(dimension_name_list=["Object", "Object"])
            with self.assertRaisesRegex(
                SpineDBAPIError, "non-existent elements in byname \\('spoon', 'anon'\\) for class Object__Object"
            ):
                db_map.add_entity(entity_class_name="Object__Object", entity_byname=["spoon", "anon"])

    def test_update_errors(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            with self.assertRaisesRegex(
                SpineDBAPIError, "no alternative matching {'name': 'not base', 'description': 'Does not exist.'}"
            ):
                db_map.update_alternative(name="not base", description="Does not exist.")
            with self.assertRaisesRegex(
                SpineDBAPIError, "invalid type for 'description' of 'alternative' - got int, expected str"
            ):
                db_map.update_alternative(name="Base", description=23)
            db_map.add_entity_class(name="Object")
            db_map.add_entity(entity_class_name="Object", name="knife")
            fork = db_map.add_entity(entity_class_name="Object", name="fork")
            db_map.add_entity_class(dimension_name_list=["Object", "Object"])
            relationship = db_map.add_entity(
                entity_class_name="Object__Object",
                entity_byname=(
                    "fork",
                    "knife",
                ),
            )
            with self.assertRaisesRegex(
                SpineDBAPIError, "non-existent elements in byname \\('knife', 'anon'\\) for class Object__Object"
            ):
                db_map.update_entity(id=relationship["id"], entity_byname=("knife", "anon"))

    def test_find_fetched_entity_classes(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                db_map.add_entity_class(name="asset")
                db_map.add_entity_class(name="group")
                db_map.add_entity_class(dimension_name_list=["asset", "group"])
                db_map.commit_session("Add test data.")
            with DatabaseMapping(url) as db_map:
                classes = db_map.find_entity_classes(name="asset__group")
                self.assertEqual(len(classes), 1)
                self.assertEqual(classes[0]["name"], "asset__group")

    def test_fetch_all_returns_public_items(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                db_map.add_entity_class(name="Object")
                db_map.add_scenario(name="my scenario")
                db_map.commit_session("Add test data.")
            with DatabaseMapping(url) as db_map:
                classes_and_scenarios = db_map.fetch_all("entity_class", "scenario")
                self.assertEqual(len(classes_and_scenarios), 2)
                classes = [item["name"] for item in classes_and_scenarios if item.item_type == "entity_class"]
                self.assertEqual(len(classes), 1)
                self.assertEqual(classes[0], "Object")
                scenarios = [item["name"] for item in classes_and_scenarios if item.item_type == "scenario"]
                self.assertEqual(len(scenarios), 1)
                self.assertEqual(scenarios[0], "my scenario")

    def test_set_parameter_definitions_list_value_to_none(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_parameter_value_list(name="Choices")
            db_map.add_list_value(parameter_value_list_name="Choices", parsed_value="yes", index=0)
            db_map.add_entity_class(name="Object")
            definition = db_map.add_parameter_definition(
                entity_class_name="Object", name="y", parameter_value_list_name="Choices", parsed_value="yes"
            )
            db_map.update_parameter_definition(id=definition["id"], default_type=None, default_value=None)
            self.assertIsNone(definition["default_type"])
            self.assertIsNone(definition["default_value"])

    def test_set_parameter_definitions_list_value_to_none_unchecked(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_parameter_value_list(name="Choices")
            db_map.add_list_value(parameter_value_list_name="Choices", parsed_value="yes", index=0)
            db_map.add_entity_class(name="Object")
            definition = db_map.add_parameter_definition(
                entity_class_name="Object", name="y", parameter_value_list_name="Choices", parsed_value="yes"
            )
            db_map.update_parameter_definition_item(
                id=definition["id"], default_type=None, default_value=None, check=False
            )
            self.assertIsNone(definition["default_type"])
            self.assertIsNone(definition["default_value"])

    def test_deleting_multidimensional_entities_deletes_their_parameter_values_too(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                db_map.add_entity_class(name="Object")
                db_map.add_entity(entity_class_name="Object", name="gadget")
                db_map.add_entity_class(name="Subject")
                db_map.add_entity(entity_class_name="Subject", name="widget")
                db_map.add_entity_class(dimension_name_list=("Subject", "Object"))
                db_map.add_parameter_definition(entity_class_name="Subject__Object", name="bonkiness")
                db_map.add_entity(entity_class_name="Subject__Object", entity_byname=("widget", "gadget"))
                db_map.add_parameter_value(
                    entity_class_name="Subject__Object",
                    entity_byname=("widget", "gadget"),
                    parameter_definition_name="bonkiness",
                    alternative_name="Base",
                    parsed_value=2.3,
                )
                db_map.commit_session("Add test data.")
            with DatabaseMapping(url) as db_map:
                entity_item = db_map.entity(entity_class_name="Subject__Object", entity_byname=("widget", "gadget"))
                entity_item.remove()
                db_map.commit_session("Remove the entity.")
            with DatabaseMapping(url) as db_map:
                values = db_map.find_parameter_values()
                self.assertEqual(values, [])

    def test_deleting_element_from_multidimensional_entity_deletes_all_parameter_values_too(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                db_map.add_entity_class(name="Object")
                db_map.add_entity(entity_class_name="Object", name="gadget")
                db_map.add_entity_class(name="Subject")
                db_map.add_entity(entity_class_name="Subject", name="widget")
                db_map.add_entity_class(dimension_name_list=("Subject", "Object"))
                db_map.add_parameter_definition(entity_class_name="Subject__Object", name="bonkiness")
                db_map.add_entity(entity_class_name="Subject__Object", entity_byname=("widget", "gadget"))
                db_map.add_parameter_value(
                    entity_class_name="Subject__Object",
                    entity_byname=("widget", "gadget"),
                    parameter_definition_name="bonkiness",
                    alternative_name="Base",
                    parsed_value=2.3,
                )
                db_map.commit_session("Add test data.")
            with DatabaseMapping(url) as db_map:
                entity_item = db_map.entity(entity_class_name="Object", name="gadget")
                entity_item.remove()
                db_map.commit_session("Remove the entity.")
            with DatabaseMapping(url) as db_map:
                values = db_map.find_parameter_values()
                self.assertEqual(values, [])

    def test_add_metadata(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_metadata(name="Title", value="Catalogue of things that should be")
            metadata_record = db_map.metadata(name="Title", value="Catalogue of things that should be")
            self.assertEqual(metadata_record["name"], "Title")
            self.assertEqual(metadata_record["value"], "Catalogue of things that should be")

    def test_add_metadata_items(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_metadata_items([{"name": "Title", "value": "Catalogue of things that should be"}])
            metadata_record = db_map.metadata(name="Title", value="Catalogue of things that should be")
            self.assertEqual(metadata_record["name"], "Title")
            self.assertEqual(metadata_record["value"], "Catalogue of things that should be")

    def test_before_alternative(self):
        with DatabaseMapping("sqlite:///", create=True) as db_map:
            db_map.add_scenario(name="scenario 1")
            base_scenario_alternative = db_map.add_scenario_alternative(
                scenario_name="scenario 1", alternative_name="Base", rank=99
            )
            self.assertIsNone(base_scenario_alternative["before_alternative_name"])
            db_map.add_alternative(name="alt 1")
            alt1_scenario_alternative = db_map.add_scenario_alternative(
                scenario_name="scenario 1", alternative_name="alt 1", rank=50
            )
            self.assertEqual(alt1_scenario_alternative["before_alternative_name"], "Base")
            self.assertIsNone(base_scenario_alternative["before_alternative_name"])
            db_map.add_alternative(name="alt 2")
            alt2_scenario_alternative = db_map.add_scenario_alternative(
                scenario_name="scenario 1", alternative_name="alt 2", rank=150
            )
            self.assertIsNone(alt2_scenario_alternative["before_alternative_name"])
            self.assertEqual(alt1_scenario_alternative["before_alternative_name"], "Base")
            self.assertEqual(base_scenario_alternative["before_alternative_name"], "alt 2")
        with DatabaseMapping("sqlite:///", create=True) as db_map:
            db_map.add_scenario(name="scenario 1")
            db_map.add_scenario(name="uninteresting scenario")
            db_map.add_alternative(name="alt 1")
            base_scenario_alternative = db_map.add_scenario_alternative(
                scenario_name="scenario 1", alternative_name="Base", rank=99
            )
            alt1_scenario_alternative = db_map.add_scenario_alternative(
                scenario_name="scenario 1", alternative_name="alt 1", rank=50
            )
            db_map.add_alternative(name="uninteresting alternative")
            db_map.add_scenario_alternative(
                scenario_name="uninteresting scenario", alternative_name="uninteresting alternative", rank=75
            )
            self.assertEqual(alt1_scenario_alternative["before_alternative_name"], "Base")
            self.assertIsNone(base_scenario_alternative["before_alternative_name"])
        with DatabaseMapping("sqlite:///", create=True) as db_map:
            db_map.add_scenario(name="scenario 1")
            db_map.add_alternative(name="alt 1")
            alternative2 = db_map.add_alternative(name="alt 2")
            base_scenario_alternative = db_map.add_scenario_alternative(
                scenario_name="scenario 1", alternative_name="Base", rank=99
            )
            alt1_scenario_alternative = db_map.add_scenario_alternative(
                scenario_name="scenario 1", alternative_name="alt 1", rank=50
            )
            db_map.add_scenario_alternative = db_map.add_scenario_alternative(
                scenario_name="scenario 1", alternative_name="alt 2", rank=150
            )
            alternative2.remove()
            self.assertEqual(alt1_scenario_alternative["before_alternative_name"], "Base")
            self.assertIsNone(base_scenario_alternative["before_alternative_name"])

    def test_before_alternative_id(self):
        with DatabaseMapping("sqlite:///", create=True) as db_map:
            db_map.add_scenario(name="scenario 1")
            base_scenario_alternative = db_map.add_scenario_alternative(
                scenario_name="scenario 1", alternative_name="Base", rank=1
            )
            self.assertIsNone(base_scenario_alternative["before_alternative_id"])
            another_alternative = db_map.add_alternative(name="alt 1")
            db_map.add_scenario_alternative(scenario_name="scenario 1", alternative_name="alt 1", rank=2)
            self.assertEqual(base_scenario_alternative["before_alternative_id"], another_alternative["id"])


class TestDatabaseMappingLegacy(unittest.TestCase):
    """'Backward compatibility' tests, i.e. pre-entity tests converted to work with the entity structure."""

    def test_construction_with_filters(self):
        db_url = IN_MEMORY_DB_URL + "?spinedbfilter=fltr1&spinedbfilter=fltr2"
        with patch("spinedb_api.db_mapping.apply_filter_stack") as mock_apply:
            with patch(
                "spinedb_api.db_mapping.load_filters", return_value=[{"fltr1": "config1", "fltr2": "config2"}]
            ) as mock_load:
                db_map = DatabaseMapping(db_url, create=True)
                mock_load.assert_called_once_with(["fltr1", "fltr2"])
                mock_apply.assert_called_once_with(db_map, [{"fltr1": "config1", "fltr2": "config2"}])

    def test_construction_with_sqlalchemy_url_and_filters(self):
        sa_url = URL.create("sqlite", query={"spinedbfilter": ["fltr1", "fltr2"]})
        with patch("spinedb_api.db_mapping.apply_filter_stack") as mock_apply:
            with patch(
                "spinedb_api.db_mapping.load_filters", return_value=[{"fltr1": "config1", "fltr2": "config2"}]
            ) as mock_load:
                with DatabaseMapping(sa_url, create=True) as db_map:
                    mock_load.assert_called_once_with(("fltr1", "fltr2"))
                    mock_apply.assert_called_once_with(db_map, [{"fltr1": "config1", "fltr2": "config2"}])

    def test_entity_sq(self):
        columns = ["id", "class_id", "name", "description", "commit_id"]
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.assertEqual(len(db_map.entity_sq.c), len(columns))
            for column_name in columns:
                self.assertTrue(hasattr(db_map.entity_sq.c, column_name))

    def test_object_class_sq(self):
        columns = ["id", "name", "description", "display_order", "display_icon", "hidden"]
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.assertEqual(len(db_map.object_class_sq.c), len(columns))
            for column_name in columns:
                self.assertTrue(hasattr(db_map.object_class_sq.c, column_name))

    def test_object_sq(self):
        columns = ["id", "class_id", "name", "description", "commit_id"]
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.assertEqual(len(db_map.object_sq.c), len(columns))
            for column_name in columns:
                self.assertTrue(hasattr(db_map.object_sq.c, column_name))

    def test_relationship_class_sq(self):
        columns = ["id", "dimension", "object_class_id", "name", "description", "display_icon", "hidden"]
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.assertEqual(len(db_map.relationship_class_sq.c), len(columns))
            for column_name in columns:
                self.assertTrue(hasattr(db_map.relationship_class_sq.c, column_name))

    def test_relationship_sq(self):
        columns = ["id", "dimension", "object_id", "class_id", "name", "commit_id"]
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.assertEqual(len(db_map.relationship_sq.c), len(columns))
            for column_name in columns:
                self.assertTrue(hasattr(db_map.relationship_sq.c, column_name))

    def test_entity_group_sq(self):
        columns = ["id", "entity_id", "entity_class_id", "member_id"]
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.assertEqual(len(db_map.entity_group_sq.c), len(columns))
            for column_name in columns:
                self.assertTrue(hasattr(db_map.entity_group_sq.c, column_name))

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
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.assertEqual(len(db_map.parameter_definition_sq.c), len(columns))
            for column_name in columns:
                self.assertTrue(hasattr(db_map.parameter_definition_sq.c, column_name))

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
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.assertEqual(len(db_map.parameter_value_sq.c), len(columns))
            for column_name in columns:
                self.assertTrue(hasattr(db_map.parameter_value_sq.c, column_name))

    def test_parameter_value_list_sq(self):
        columns = ["id", "name", "commit_id"]
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.assertEqual(len(db_map.parameter_value_list_sq.c), len(columns))
            for column_name in columns:
                self.assertTrue(hasattr(db_map.parameter_value_list_sq.c, column_name))

    def test_ext_object_sq(self):
        columns = ["id", "class_id", "class_name", "name", "description", "group_id", "commit_id"]
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.assertEqual(len(db_map.ext_object_sq.c), len(columns))
            for column_name in columns:
                self.assertTrue(hasattr(db_map.ext_object_sq.c, column_name))

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
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.assertEqual(len(db_map.ext_relationship_class_sq.c), len(columns))
            for column_name in columns:
                self.assertTrue(hasattr(db_map.ext_relationship_class_sq.c, column_name))

    def test_wide_relationship_class_sq(self):
        columns = ["id", "name", "description", "display_icon", "object_class_id_list", "object_class_name_list"]
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.assertEqual(len(db_map.wide_relationship_class_sq.c), len(columns))
            for column_name in columns:
                self.assertTrue(hasattr(db_map.wide_relationship_class_sq.c, column_name))

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
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.assertEqual(len(db_map.ext_relationship_sq.c), len(columns))
            for column_name in columns:
                self.assertTrue(hasattr(db_map.ext_relationship_sq.c, column_name))

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
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.assertEqual(len(db_map.wide_relationship_sq.c), len(columns))
            for column_name in columns:
                self.assertTrue(hasattr(db_map.wide_relationship_sq.c, column_name))

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
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.assertEqual(len(db_map.object_parameter_definition_sq.c), len(columns))
            for column_name in columns:
                self.assertTrue(hasattr(db_map.object_parameter_definition_sq.c, column_name))

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
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.assertEqual(len(db_map.relationship_parameter_definition_sq.c), len(columns))
            for column_name in columns:
                self.assertTrue(hasattr(db_map.relationship_parameter_definition_sq.c, column_name))

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
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.assertEqual(len(db_map.object_parameter_value_sq.c), len(columns))
            for column_name in columns:
                self.assertTrue(hasattr(db_map.object_parameter_value_sq.c, column_name))

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
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.assertEqual(len(db_map.relationship_parameter_value_sq.c), len(columns))
            for column_name in columns:
                self.assertTrue(hasattr(db_map.relationship_parameter_value_sq.c, column_name))

    def test_wide_parameter_value_list_sq(self):
        columns = ["id", "name", "value_index_list", "value_id_list", "commit_id"]
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.assertEqual(len(db_map.wide_parameter_value_list_sq.c), len(columns))
            for column_name in columns:
                self.assertTrue(hasattr(db_map.wide_parameter_value_list_sq.c, column_name))

    def test_get_import_alternative_returns_base_alternative_by_default(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            alternative_name = db_map.get_import_alternative_name()
            self.assertEqual(alternative_name, "Base")


class TestDatabaseMappingQueries(AssertSuccessTestCase):

    def _assert_import(self, result):
        import_count, errors = result
        self.assertEqual(errors, [])
        return import_count

    def create_object_classes(self, db_map):
        obj_classes = ["class1", "class2"]
        self._assert_import(import_functions.import_object_classes(db_map, obj_classes))
        return obj_classes

    def create_objects(self, db_map):
        objects = [("class1", "obj11"), ("class1", "obj12"), ("class2", "obj21")]
        self._assert_import(import_functions.import_entities(db_map, objects))
        return objects

    def create_relationship_classes(self, db_map):
        relationship_classes = [("rel1", ["class1"]), ("rel2", ["class1", "class2"])]
        self._assert_import(import_functions.import_entity_classes(db_map, relationship_classes))
        return relationship_classes

    def create_relationships(self, db_map):
        relationships = [("rel1", ["obj11"]), ("rel2", ["obj11", "obj21"])]
        self._assert_import(import_functions.import_entities(db_map, relationships))
        return relationships

    def test_commit_sq_hides_pending_commit(self):
        with DatabaseMapping(IN_MEMORY_DB_URL, create=True) as db_map:
            commits = db_map.query(db_map.commit_sq).all()
        self.assertEqual(len(commits), 1)

    def test_alternative_sq(self):
        with DatabaseMapping(IN_MEMORY_DB_URL, create=True) as db_map:
            self._assert_import(import_functions.import_alternatives(db_map, (("alt1", "test alternative"),)))
            db_map.commit_session("test")
            alternative_rows = db_map.query(db_map.alternative_sq).all()
        expected_names_and_descriptions = {"Base": "Base alternative", "alt1": "test alternative"}
        self.assertEqual(len(alternative_rows), len(expected_names_and_descriptions))
        for row in alternative_rows:
            self.assertTrue(row.name in expected_names_and_descriptions)
            self.assertEqual(row.description, expected_names_and_descriptions[row.name])
            expected_names_and_descriptions.pop(row.name)
        self.assertEqual(expected_names_and_descriptions, {})

    def test_scenario_sq(self):
        with DatabaseMapping(IN_MEMORY_DB_URL, create=True) as db_map:
            self._assert_import(import_functions.import_scenarios(db_map, (("scen1", True, "test scenario"),)))
            db_map.commit_session("test")
            scenario_rows = db_map.query(db_map.scenario_sq).all()
        self.assertEqual(len(scenario_rows), 1)
        self.assertEqual(scenario_rows[0].name, "scen1")
        self.assertEqual(scenario_rows[0].description, "test scenario")
        self.assertTrue(scenario_rows[0].active)

    def test_display_mode_sq(self):
        with DatabaseMapping(IN_MEMORY_DB_URL, create=True) as db_map:
            import_functions.import_display_modes(db_map, (("disp_mode", "Some desc."),))
            db_map.commit_session("test")
            disp_mode_rows = db_map.query(db_map.display_mode_sq).all()
        self.assertEqual(len(disp_mode_rows), 1)
        self.assertEqual(disp_mode_rows[0].name, "disp_mode")
        self.assertEqual(disp_mode_rows[0].description, "Some desc.")

    def test_entity_class_display_mode_sq(self):
        with DatabaseMapping(IN_MEMORY_DB_URL, create=True) as db_map:
            import_functions.import_entity_classes(
                db_map,
                (("ent_cls",)),
            )
            import_functions.import_display_modes(db_map, (("disp_mode", "Some desc."),))
            import_functions.import_entity_class_display_modes(
                db_map, (("disp_mode", "ent_cls", 1, DisplayStatus.greyed_out.name, "deadbe", "efdead"),)
            )
            db_map.commit_session("test")
            disp_mode_rows = db_map.query(db_map.entity_class_display_mode_sq).all()
        self.assertEqual(len(disp_mode_rows), 1)
        self.assertEqual(disp_mode_rows[0].display_mode_id, 1)
        self.assertEqual(disp_mode_rows[0].entity_class_id, 1)
        self.assertEqual(disp_mode_rows[0].display_order, 1)
        self.assertEqual(disp_mode_rows[0].display_status, DisplayStatus.greyed_out.name)
        self.assertEqual(disp_mode_rows[0].display_font_color, "deadbe")
        self.assertEqual(disp_mode_rows[0].display_background_color, "efdead")

    def test_ext_linked_scenario_alternative_sq(self):
        with DatabaseMapping(IN_MEMORY_DB_URL, create=True) as db_map:
            self._assert_import(import_functions.import_scenarios(db_map, (("scen1", True),)))
            self._assert_import(import_functions.import_alternatives(db_map, ("alt1", "alt2", "alt3")))
            self._assert_import(import_functions.import_scenario_alternatives(db_map, (("scen1", "alt2"),)))
            self._assert_import(import_functions.import_scenario_alternatives(db_map, (("scen1", "alt3"),)))
            self._assert_import(import_functions.import_scenario_alternatives(db_map, (("scen1", "alt1"),)))
            db_map.commit_session("test")
            scenario_alternative_rows = db_map.query(db_map.ext_linked_scenario_alternative_sq).all()
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
        with DatabaseMapping(IN_MEMORY_DB_URL, create=True) as db_map:
            obj_classes = self.create_object_classes(db_map)
            relationship_classes = self.create_relationship_classes(db_map)
            db_map.commit_session("test")
            results = db_map.query(db_map.entity_class_sq).all()
        # Check that number of results matches total entities
        self.assertEqual(len(results), len(obj_classes) + len(relationship_classes))
        # Check result values
        for row, class_name in zip(results, obj_classes + [rel[0] for rel in relationship_classes]):
            self.assertEqual(row.name, class_name)

    def test_entity_sq(self):
        with DatabaseMapping(IN_MEMORY_DB_URL, create=True) as db_map:
            self.create_object_classes(db_map)
            objects = self.create_objects(db_map)
            self.create_relationship_classes(db_map)
            relationships = self.create_relationships(db_map)
            db_map.commit_session("test")
            entity_rows = db_map.query(db_map.entity_sq).all()
        self.assertEqual(len(entity_rows), len(objects) + len(relationships))
        object_names = [o[1] for o in objects]
        relationship_names = [name_from_elements(r[1]) for r in relationships]
        for row, expected_name in zip(entity_rows, object_names + relationship_names):
            self.assertEqual(row.name, expected_name)

    def test_object_class_sq_picks_object_classes_only(self):
        with DatabaseMapping(IN_MEMORY_DB_URL, create=True) as db_map:
            obj_classes = self.create_object_classes(db_map)
            self.create_relationship_classes(db_map)
            db_map.commit_session("test")
            class_rows = db_map.query(db_map.object_class_sq).all()
        self.assertEqual(len(class_rows), len(obj_classes))
        for row, expected_name in zip(class_rows, obj_classes):
            self.assertEqual(row.name, expected_name)

    def test_object_sq_picks_objects_only(self):
        with DatabaseMapping(IN_MEMORY_DB_URL, create=True) as db_map:
            self.create_object_classes(db_map)
            objects = self.create_objects(db_map)
            self.create_relationship_classes(db_map)
            self.create_relationships(db_map)
            db_map.commit_session("test")
            object_rows = db_map.query(db_map.object_sq).all()
        self.assertEqual(len(object_rows), len(objects))
        for row, expected_object in zip(object_rows, objects):
            self.assertEqual(row.name, expected_object[1])

    def test_wide_relationship_class_sq(self):
        with DatabaseMapping(IN_MEMORY_DB_URL, create=True) as db_map:
            self.create_object_classes(db_map)
            relationship_classes = self.create_relationship_classes(db_map)
            db_map.commit_session("test")
            class_rows = db_map.query(db_map.wide_relationship_class_sq).all()
        self.assertEqual(len(class_rows), 2)
        for row, relationship_class in zip(class_rows, relationship_classes):
            self.assertEqual(row.name, relationship_class[0])
            self.assertEqual(row.object_class_name_list, ",".join(relationship_class[1]))

    def test_wide_relationship_sq(self):
        with DatabaseMapping(IN_MEMORY_DB_URL, create=True) as db_map:
            self.create_object_classes(db_map)
            self.create_objects(db_map)
            relationship_classes = self.create_relationship_classes(db_map)
            object_classes = {rel_class[0]: rel_class[1] for rel_class in relationship_classes}
            relationships = self.create_relationships(db_map)
            db_map.commit_session("test")
            relationship_rows = db_map.query(db_map.wide_relationship_sq).all()
        self.assertEqual(len(relationship_rows), 2)
        for row, relationship in zip(relationship_rows, relationships):
            self.assertEqual(row.name, name_from_elements(relationship[1]))
            self.assertEqual(row.class_name, relationship[0])
            self.assertEqual(row.object_class_name_list, ",".join(object_classes[relationship[0]]))
            self.assertEqual(row.object_name_list, ",".join(relationship[1]))

    def test_parameter_definition_sq_for_object_class(self):
        with DatabaseMapping(IN_MEMORY_DB_URL, create=True) as db_map:
            self.create_object_classes(db_map)
            self._assert_import(import_functions.import_object_parameters(db_map, (("class1", "par1"),)))
            db_map.commit_session("test")
            definition_rows = db_map.query(db_map.parameter_definition_sq).all()
        self.assertEqual(len(definition_rows), 1)
        self.assertEqual(definition_rows[0].name, "par1")
        self.assertIsNotNone(definition_rows[0].entity_class_id)

    def test_parameter_definition_sq_for_relationship_class(self):
        with DatabaseMapping(IN_MEMORY_DB_URL, create=True) as db_map:
            self.create_object_classes(db_map)
            self.create_relationship_classes(db_map)
            self._assert_import(import_functions.import_relationship_parameters(db_map, (("rel1", "par1"),)))
            db_map.commit_session("test")
            definition_rows = db_map.query(db_map.parameter_definition_sq).all()
        self.assertEqual(len(definition_rows), 1)
        self.assertEqual(definition_rows[0].name, "par1")
        self.assertIsNotNone(definition_rows[0].entity_class_id)

    def test_entity_parameter_definition_sq_for_object_class(self):
        with DatabaseMapping(IN_MEMORY_DB_URL, create=True) as db_map:
            self.create_object_classes(db_map)
            self.create_relationship_classes(db_map)
            self._assert_import(import_functions.import_object_parameters(db_map, (("class1", "par1"),)))
            db_map.commit_session("test")
            definition_rows = db_map.query(db_map.entity_parameter_definition_sq).all()
        self.assertEqual(len(definition_rows), 1)
        self.assertEqual(definition_rows[0].parameter_name, "par1")
        self.assertEqual(definition_rows[0].entity_class_name, "class1")
        self.assertEqual(definition_rows[0].object_class_name, "class1")
        self.assertIsNone(definition_rows[0].relationship_class_id)
        self.assertIsNone(definition_rows[0].relationship_class_name)
        self.assertIsNone(definition_rows[0].object_class_id_list)
        self.assertIsNone(definition_rows[0].object_class_name_list)

    def test_entity_parameter_definition_sq_for_relationship_class(self):
        with DatabaseMapping(IN_MEMORY_DB_URL, create=True) as db_map:
            object_classes = self.create_object_classes(db_map)
            self.create_relationship_classes(db_map)
            self._assert_import(import_functions.import_relationship_parameters(db_map, (("rel2", "par1"),)))
            db_map.commit_session("test")
            definition_rows = db_map.query(db_map.entity_parameter_definition_sq).all()
        self.assertEqual(len(definition_rows), 1)
        self.assertEqual(definition_rows[0].parameter_name, "par1")
        self.assertEqual(definition_rows[0].entity_class_name, "rel2")
        self.assertIsNotNone(definition_rows[0].relationship_class_id)
        self.assertEqual(definition_rows[0].relationship_class_name, "rel2")
        self.assertIsNotNone(definition_rows[0].object_class_id_list)
        self.assertEqual(definition_rows[0].object_class_name_list, ",".join(object_classes))
        self.assertIsNone(definition_rows[0].object_class_name)

    def test_entity_parameter_definition_sq_with_multiple_relationship_classes_but_single_parameter(self):
        with DatabaseMapping(IN_MEMORY_DB_URL, create=True) as db_map:
            self.create_object_classes(db_map)
            self.create_relationship_classes(db_map)
            obj_parameter_definitions = [("class1", "par1a"), ("class1", "par1b")]
            rel_parameter_definitions = [("rel1", "rpar1a")]
            self._assert_import(import_functions.import_object_parameters(db_map, obj_parameter_definitions))
            self._assert_import(import_functions.import_relationship_parameters(db_map, rel_parameter_definitions))
            db_map.commit_session("test")
            results = db_map.query(db_map.entity_parameter_definition_sq).all()
        # Check that number of results matches total entities
        self.assertEqual(len(results), len(obj_parameter_definitions) + len(rel_parameter_definitions))
        # Check result values
        for row, par_def in zip(results, obj_parameter_definitions + rel_parameter_definitions):
            self.assertTupleEqual((row.entity_class_name, row.parameter_name), par_def)

    def test_entity_parameter_values(self):
        with DatabaseMapping(IN_MEMORY_DB_URL, create=True) as db_map:
            self.create_object_classes(db_map)
            self.create_objects(db_map)
            self.create_relationship_classes(db_map)
            self.create_relationships(db_map)
            obj_parameter_definitions = [("class1", "par1a"), ("class1", "par1b"), ("class2", "par2a")]
            rel_parameter_definitions = [("rel1", "rpar1a"), ("rel2", "rpar2a")]
            self._assert_import(import_functions.import_object_parameters(db_map, obj_parameter_definitions))
            self._assert_import(import_functions.import_relationship_parameters(db_map, rel_parameter_definitions))
            object_parameter_values = [
                ("class1", "obj11", "par1a", 123),
                ("class1", "obj11", "par1b", 333),
                ("class2", "obj21", "par2a", "empty"),
            ]
            self._assert_import(import_functions.import_object_parameter_values(db_map, object_parameter_values))
            relationship_parameter_values = [
                ("rel1", ["obj11"], "rpar1a", 1.1),
                ("rel2", ["obj11", "obj21"], "rpar2a", 42),
            ]
            self._assert_import(
                import_functions.import_relationship_parameter_values(db_map, relationship_parameter_values)
            )
            db_map.commit_session("test")
            results = db_map.query(db_map.entity_parameter_value_sq).all()
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
        with DatabaseMapping(IN_MEMORY_DB_URL, create=True) as db_map:
            self._assert_import(
                import_functions.import_parameter_value_lists(
                    db_map, (("list1", "value1"), ("list1", "value2"), ("list2", "valueA"))
                )
            )
            db_map.commit_session("test")
            value_lists = db_map.query(db_map.wide_parameter_value_list_sq).all()
        self.assertEqual(len(value_lists), 2)
        self.assertEqual(value_lists[0].name, "list1")
        self.assertEqual(value_lists[1].name, "list2")

    def test_filter_query_accepts_multiple_criteria(self):
        with DatabaseMapping(IN_MEMORY_DB_URL, create=True) as db_map:
            classes = self._assert_import(db_map.add_items("entity_class", {"name": "Real"}, {"name": "Fake"}))
            self.assertEqual(len(classes), 2)
            self.assertEqual(classes[0]["name"], "Real")
            self.assertEqual(classes[1]["name"], "Fake")
            real_class_id = classes[0]["id"]
            fake_class_id = classes[1]["id"]
            self._assert_import(
                db_map.add_items(
                    "entity",
                    {"name": "entity 1", "class_id": real_class_id},
                    {"name": "entity_2", "class_id": real_class_id},
                    {"name": "entity_1", "class_id": fake_class_id},
                )
            )
            db_map.commit_session("Add test data")
            sq = db_map.wide_entity_class_sq
            real_class_id = db_map.query(sq).filter(sq.c.name == "Real").one().id
            sq = db_map.wide_entity_sq
            entity = db_map.query(sq).filter(sq.c.name == "entity 1", sq.c.class_id == 1).one()
        self.assertEqual(entity.name, "entity 1")
        self.assertEqual(entity.class_id, real_class_id)

    def test_wide_parameter_definition_sq(self):
        with DatabaseMapping(IN_MEMORY_DB_URL, create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Gadget"))
            self._assert_success(db_map.add_parameter_definition_item(name="typeless", entity_class_name="Gadget"))
            self._assert_success(db_map.add_parameter_definition_item(name="typed", entity_class_name="Gadget"))
            self._assert_success(
                db_map.add_parameter_type_item(
                    entity_class_name="Gadget", parameter_definition_name="typed", rank=0, type=type_for_scalar(1.0)
                )
            )
            self._assert_success(
                db_map.add_parameter_type_item(
                    entity_class_name="Gadget", parameter_definition_name="typed", rank=0, type=type_for_scalar("high")
                )
            )
            self._assert_success(
                db_map.add_parameter_type_item(
                    entity_class_name="Gadget", parameter_definition_name="typed", rank=0, type=type_for_scalar(True)
                )
            )
            db_map.commit_session("Add parameter definitions with types")
            definitions = db_map.query(db_map.wide_parameter_definition_sq).all()
        self.assertEqual(len(definitions), 2)
        self.assertCountEqual([item.name for item in definitions], ["typed", "typeless"])
        for definition in definitions:
            if definition.name == "typed":
                self.assertCountEqual(definition.parameter_type_list.split(","), ("str", "float", "bool"))
            elif definition.name == "typeless":
                self.assertEqual(definition.parameter_type_list, None)


class TestDatabaseMappingGet(unittest.TestCase):
    def test_get_entity_class_items_check_fields(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_data(db_map, entity_classes=(("fish",),))
            with self.assertRaises(SpineDBAPIError):
                db_map.get_entity_class_item(entity_class_name="fish")
            with self.assertRaises(SpineDBAPIError):
                db_map.get_entity_class_item(name=("fish",))
            db_map.get_entity_class_item(name="fish")

    def test_get_entity_alternative_items(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_data(
                db_map,
                entity_classes=(("fish",),),
                entities=(("fish", "Nemo"),),
                entity_alternatives=(("fish", "Nemo", "Base", True),),
            )
            ea_item = db_map.get_entity_alternative_item(
                alternative_name="Base", entity_class_name="fish", entity_byname=("Nemo",)
            )
            self.assertIsNotNone(ea_item)
            ea_items = db_map.get_entity_alternative_items(
                alternative_name="Base", entity_class_name="fish", entity_byname=("Nemo",)
            )
            self.assertEqual(len(ea_items), 1)
            self.assertEqual(ea_items[0], ea_item)


class TestDatabaseMappingAdd(AssertSuccessTestCase):
    def test_add_and_retrieve_many_objects(self):
        """Tests add many objects into db and retrieving them."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            items, _ = db_map.add_items("entity_class", {"name": "testclass"})
            class_id = next(iter(items))["id"]
            added = db_map.add_items("entity", *[{"name": str(i), "class_id": class_id} for i in range(1001)])[0]
            self.assertEqual(len(added), 1001)
            db_map.commit_session("test_commit")
            self.assertEqual(db_map.query(db_map.entity_sq).count(), 1001)

    def test_add_object_classes(self):
        """Test that adding object classes works."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "fish"}, {"name": "dog"})
            db_map.commit_session("add")
            object_classes = db_map.query(db_map.object_class_sq).all()
            self.assertEqual(len(object_classes), 2)
            self.assertEqual(object_classes[0].name, "fish")
            self.assertEqual(object_classes[1].name, "dog")

    def test_add_object_class_with_invalid_name(self):
        """Test that adding object classes with empty name raises error"""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            with self.assertRaises(SpineIntegrityError):
                db_map.add_items("entity_class", {"name": ""}, strict=True)

    def test_add_object_classes_with_same_name(self):
        """Test that adding two object classes with the same name only adds one of them."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "fish"}, {"name": "fish"})
            db_map.commit_session("add")
            object_classes = db_map.query(db_map.object_class_sq).all()
            self.assertEqual(len(object_classes), 1)
            self.assertEqual(object_classes[0].name, "fish")

    def test_add_object_class_with_same_name_as_existing_one(self):
        """Test that adding an object class with an already taken name raises an integrity error."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "fish"}, {"name": "fish"})
            with self.assertRaises(SpineIntegrityError):
                db_map.add_items("entity_class", {"name": "fish"}, strict=True)

    def test_add_objects(self):
        """Test that adding objects works."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "fish", "id": 1})
            db_map.add_items("entity", {"name": "nemo", "class_id": 1}, {"name": "dory", "class_id": 1})
            db_map.commit_session("add")
            objects = db_map.query(db_map.object_sq).all()
            self.assertEqual(len(objects), 2)
            self.assertEqual(objects[0].name, "nemo")
            self.assertEqual(objects[0].class_id, 1)
            self.assertEqual(objects[1].name, "dory")
            self.assertEqual(objects[1].class_id, 1)

    def test_add_object_with_invalid_name(self):
        """Test that adding object classes with empty name raises error"""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "fish"})
            with self.assertRaises(SpineIntegrityError):
                db_map.add_items("entity", {"name": "", "entity_class_name": "fish"}, strict=True)

    def test_add_objects_with_same_name(self):
        """Test that adding two objects with the same name only adds one of them."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "fish", "id": 1})
            db_map.add_items("entity", {"name": "nemo", "class_id": 1}, {"name": "nemo", "class_id": 1})
            db_map.commit_session("add")
            objects = db_map.query(db_map.object_sq).all()
            self.assertEqual(len(objects), 1)
            self.assertEqual(objects[0].name, "nemo")
            self.assertEqual(objects[0].class_id, 1)

    def test_add_object_with_same_name_as_existing_one(self):
        """Test that adding an object with an already taken name raises an integrity error."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "fish"})
            db_map.add_items("entity", {"name": "nemo", "class_id": 1})
            with self.assertRaises(SpineIntegrityError):
                db_map.add_items("entity", {"name": "nemo", "class_id": 1}, strict=True)

    def test_add_object_with_invalid_class(self):
        """Test that adding an object with a non existing class raises an integrity error."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "fish"})
            with self.assertRaises(SpineIntegrityError):
                db_map.add_items("entity", {"name": "pluto", "class_id": 2}, strict=True)

    def test_add_relationship_classes(self):
        """Test that adding relationship classes works."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            db_map.add_items(
                "entity_class",
                {"name": "rc1", "object_class_id_list": [1, 2]},
                {"name": "rc2", "object_class_id_list": [2, 1]},
            )
            db_map.commit_session("add")
            table = db_map.get_table("entity_class_dimension")
            ent_cls_dims = db_map.query(table).all()
            rel_clss = db_map.query(db_map.wide_relationship_class_sq).all()
            self.assertEqual(len(ent_cls_dims), 4)
            self.assertEqual(rel_clss[0].name, "rc1")
            self.assertEqual(ent_cls_dims[0].dimension_id, 1)
            self.assertEqual(ent_cls_dims[1].dimension_id, 2)
            self.assertEqual(rel_clss[1].name, "rc2")
            self.assertEqual(ent_cls_dims[2].dimension_id, 2)
            self.assertEqual(ent_cls_dims[3].dimension_id, 1)

    def test_add_relationship_classes_with_invalid_name(self):
        """Test that adding object classes with empty name raises error"""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "fish"})
            with self.assertRaises(SpineIntegrityError):
                db_map.add_items("entity_class", {"name": "", "object_class_id_list": [1]}, strict=True)

    def test_add_relationship_classes_with_same_name(self):
        """Test that adding two relationship classes with the same name only adds one of them."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            db_map.add_items(
                "entity_class",
                {"name": "rc1", "object_class_id_list": [1, 2]},
                {"name": "rc1", "object_class_id_list": [1, 2]},
                strict=False,
            )
            db_map.commit_session("add")
            table = db_map.get_table("entity_class_dimension")
            ecs_dims = db_map.query(table).all()
            relationship_classes = db_map.query(db_map.wide_relationship_class_sq).all()
            self.assertEqual(len(ecs_dims), 2)
            self.assertEqual(len(relationship_classes), 1)
            self.assertEqual(relationship_classes[0].name, "rc1")
            self.assertEqual(ecs_dims[0].dimension_id, 1)
            self.assertEqual(ecs_dims[1].dimension_id, 2)

    def test_add_relationship_class_with_same_name_as_existing_one(self):
        """Test that adding a relationship class with an already taken name raises an integrity error."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            query_wrapper = create_query_wrapper(db_map)
            with (
                mock.patch.object(DatabaseMapping, "query") as mock_query,
                mock.patch.object(DatabaseMapping, "object_class_sq") as mock_object_class_sq,
                mock.patch.object(DatabaseMapping, "wide_relationship_class_sq") as mock_wide_rel_cls_sq,
            ):
                mock_query.side_effect = query_wrapper
                mock_object_class_sq.return_value = [
                    ObjectClassRow(1, "fish"),
                    ObjectClassRow(2, "dog"),
                ]
                WideObjectClassRow = namedtuple("WideObjectClassRow", ["id", "object_class_id_list", "name"])
                mock_wide_rel_cls_sq.return_value = [WideObjectClassRow(1, "1,2", "fish__dog")]
                with self.assertRaises(SpineIntegrityError):
                    db_map.add_items("entity_class", {"name": "fish__dog", "object_class_id_list": [1, 2]}, strict=True)

    def test_add_relationship_class_with_invalid_object_class(self):
        """Test that adding a relationship class with a non existing object class raises an integrity error."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            query_wrapper = create_query_wrapper(db_map)
            with (
                mock.patch.object(DatabaseMapping, "query") as mock_query,
                mock.patch.object(DatabaseMapping, "object_class_sq") as mock_object_class_sq,
                mock.patch.object(DatabaseMapping, "wide_relationship_class_sq"),
            ):
                mock_query.side_effect = query_wrapper
                mock_object_class_sq.return_value = [ObjectClassRow(1, "fish")]
                with self.assertRaises(SpineIntegrityError):
                    db_map.add_items("entity_class", {"name": "fish__dog", "object_class_id_list": [1, 2]}, strict=True)

    def test_add_relationships(self):
        """Test that adding relationships works."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            db_map.add_items("entity_class", {"name": "rc1", "object_class_id_list": [1, 2], "id": 3})
            db_map.add_items("entity", {"name": "o1", "class_id": 1, "id": 1}, {"name": "o2", "class_id": 2, "id": 2})
            db_map.add_items("entity", {"name": "nemo__pluto", "class_id": 3, "object_id_list": [1, 2]})
            db_map.commit_session("add")
            ent_els = db_map.query(db_map.get_table("entity_element")).all()
            relationships = db_map.query(db_map.wide_relationship_sq).all()
            self.assertEqual(len(ent_els), 2)
            self.assertEqual(len(relationships), 1)
            self.assertEqual(relationships[0].name, "nemo__pluto")
            self.assertEqual(ent_els[0].entity_class_id, 3)
            self.assertEqual(ent_els[0].element_id, 1)
            self.assertEqual(ent_els[1].entity_class_id, 3)
            self.assertEqual(ent_els[1].element_id, 2)

    def test_add_relationship_with_invalid_name(self):
        """Test that adding object classes with empty name raises error"""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1}, strict=True)
            db_map.add_items("entity_class", {"name": "rc1", "object_class_id_list": [1]}, strict=True)
            db_map.add_items("entity", {"name": "o1", "class_id": 1}, strict=True)
            with self.assertRaises(SpineIntegrityError):
                db_map.add_items("entity", {"name": "", "class_id": 2, "object_id_list": [1]}, strict=True)

    def test_add_identical_relationships(self):
        """Test that adding two relationships with the same class and same objects only adds the first one."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            db_map.add_items("entity_class", {"name": "rc1", "object_class_id_list": [1, 2], "id": 3})
            db_map.add_items("entity", {"name": "o1", "class_id": 1, "id": 1}, {"name": "o2", "class_id": 2, "id": 2})
            db_map.add_items(
                "entity",
                {"name": "nemo__pluto", "class_id": 3, "object_id_list": [1, 2]},
                {"name": "nemo__pluto_duplicate", "class_id": 3, "object_id_list": [1, 2]},
            )
            db_map.commit_session("add")
            relationships = db_map.query(db_map.wide_relationship_sq).all()
            self.assertEqual(len(relationships), 1)

    def test_add_relationship_identical_to_existing_one(self):
        """Test that adding a relationship with the same class and same objects as an existing one
        raises an integrity error.
        """
        with DatabaseMapping("sqlite://", create=True) as db_map:
            query_wrapper = create_query_wrapper(db_map)
            with (
                mock.patch.object(DatabaseMapping, "query") as mock_query,
                mock.patch.object(DatabaseMapping, "object_sq") as mock_object_sq,
                mock.patch.object(DatabaseMapping, "wide_relationship_class_sq") as mock_wide_rel_cls_sq,
                mock.patch.object(DatabaseMapping, "wide_relationship_sq") as mock_wide_rel_sq,
            ):
                mock_query.side_effect = query_wrapper
                mock_object_sq.return_value = [
                    ObjectRow(1, 10, "nemo"),
                    ObjectRow(2, 20, "pluto"),
                ]
                RelationshipClassRow = namedtuple("RelationshipClassRow", ["id", "object_class_id_list", "name"])
                mock_wide_rel_cls_sq.return_value = [RelationshipClassRow(1, "10,20", "fish__dog")]
                WideRelationshipClassRow = namedtuple(
                    "WideRelationshipClassRow", ["id", "class_id", "object_id_list", "name"]
                )
                mock_wide_rel_sq.return_value = [WideRelationshipClassRow(1, 1, "1,2", "nemo__pluto")]
                with self.assertRaises(SpineIntegrityError):
                    db_map.add_items(
                        "entity", {"name": "nemoy__plutoy", "class_id": 1, "object_id_list": [1, 2]}, strict=True
                    )

    def test_add_relationship_with_invalid_class(self):
        """Test that adding a relationship with an invalid class raises an integrity error."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            query_wrapper = create_query_wrapper(db_map)
            with (
                mock.patch.object(DatabaseMapping, "query") as mock_query,
                mock.patch.object(DatabaseMapping, "object_sq") as mock_object_sq,
                mock.patch.object(DatabaseMapping, "wide_relationship_class_sq") as mock_wide_rel_cls_sq,
                mock.patch.object(DatabaseMapping, "wide_relationship_sq"),
            ):
                mock_query.side_effect = query_wrapper
                mock_object_sq.return_value = [
                    ObjectRow(1, 10, "nemo"),
                    ObjectRow(2, 20, "pluto"),
                ]
                mock_wide_rel_cls_sq.return_value = [RelationshipRow(1, "10,20", "fish__dog")]
                with self.assertRaises(SpineIntegrityError):
                    db_map.add_items(
                        "entity", {"name": "nemo__pluto", "class_id": 2, "object_id_list": [1, 2]}, strict=True
                    )

    def test_add_relationship_with_invalid_object(self):
        """Test that adding a relationship with an invalid object raises an integrity error."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            query_wrapper = create_query_wrapper(db_map)
            with (
                mock.patch.object(DatabaseMapping, "query") as mock_query,
                mock.patch.object(DatabaseMapping, "object_sq") as mock_object_sq,
                mock.patch.object(DatabaseMapping, "wide_relationship_class_sq") as mock_wide_rel_cls_sq,
                mock.patch.object(DatabaseMapping, "wide_relationship_sq"),
            ):
                mock_query.side_effect = query_wrapper
                mock_object_sq.return_value = [
                    ObjectRow(1, 10, "nemo"),
                    ObjectRow(2, 20, "pluto"),
                ]
                mock_wide_rel_cls_sq.return_value = [RelationshipRow(1, "10,20", "fish__dog")]
                with self.assertRaises(SpineIntegrityError):
                    db_map.add_items(
                        "entity", {"name": "nemo__pluto", "class_id": 1, "object_id_list": [1, 3]}, strict=True
                    )

    def test_add_entity_groups(self):
        """Test that adding group entities works."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1})
            db_map.add_items("entity", {"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 1})
            db_map.add_items("entity_group", {"entity_id": 1, "entity_class_id": 1, "member_id": 2})
            db_map.commit_session("add")
            table = db_map.get_table("entity_group")
            entity_groups = db_map.query(table).all()
            self.assertEqual(len(entity_groups), 1)
            self.assertEqual(entity_groups[0].entity_id, 1)
            self.assertEqual(entity_groups[0].entity_class_id, 1)
            self.assertEqual(entity_groups[0].member_id, 2)

    def test_add_entity_groups_with_invalid_class(self):
        """Test that adding group entities with an invalid class fails."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1})
            db_map.add_items("entity", {"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 1})
            with self.assertRaises(SpineIntegrityError):
                db_map.add_items("entity_group", {"entity_id": 1, "entity_class_id": 2, "member_id": 2}, strict=True)

    def test_add_entity_groups_with_invalid_entity(self):
        """Test that adding group entities with an invalid entity fails."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1})
            db_map.add_items("entity", {"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 1})
            with self.assertRaises(SpineIntegrityError):
                db_map.add_items("entity_group", {"entity_id": 3, "entity_class_id": 2, "member_id": 2}, strict=True)

    def test_add_entity_groups_with_invalid_member(self):
        """Test that adding group entities with an invalid member fails."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1})
            db_map.add_items("entity", {"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 1})
            with self.assertRaises(SpineIntegrityError):
                db_map.add_items("entity_group", {"entity_id": 1, "entity_class_id": 2, "member_id": 3}, strict=True)

    def test_add_repeated_entity_groups(self):
        """Test that adding repeated group entities fails."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1})
            db_map.add_items("entity", {"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 1})
            db_map.add_items("entity_group", {"entity_id": 1, "entity_class_id": 2, "member_id": 2})
            with self.assertRaises(SpineIntegrityError):
                db_map.add_items("entity_group", {"entity_id": 1, "entity_class_id": 2, "member_id": 2}, strict=True)

    def test_add_parameter_definitions(self):
        """Test that adding parameter definitions works."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            db_map.add_items("entity_class", {"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
            db_map.add_items(
                "parameter_definition",
                {"name": "color", "object_class_id": 1, "description": "test1"},
                {"name": "relative_speed", "relationship_class_id": 3, "description": "test2"},
            )
            db_map.commit_session("add")
            table = db_map.get_table("parameter_definition")
            parameter_definitions = db_map.query(table).all()
            self.assertEqual(len(parameter_definitions), 2)
            self.assertEqual(parameter_definitions[0].name, "color")
            self.assertEqual(parameter_definitions[0].entity_class_id, 1)
            self.assertEqual(parameter_definitions[0].description, "test1")
            self.assertEqual(parameter_definitions[1].name, "relative_speed")
            self.assertEqual(parameter_definitions[1].entity_class_id, 3)
            self.assertEqual(parameter_definitions[1].description, "test2")

    def test_add_parameter_with_invalid_name(self):
        """Test that adding object classes with empty name raises error"""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1"}, strict=True)
            with self.assertRaises(SpineIntegrityError):
                db_map.add_items("parameter_definition", {"name": "", "object_class_id": 1}, strict=True)

    def test_add_parameter_definitions_with_same_name(self):
        """Test that adding two parameter_definitions with the same name adds both of them."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            db_map.add_items("entity_class", {"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
            db_map.add_items(
                "parameter_definition",
                {"name": "color", "object_class_id": 1},
                {"name": "color", "relationship_class_id": 3},
            )
            db_map.commit_session("add")
            table = db_map.get_table("parameter_definition")
            parameter_definitions = db_map.query(table).all()
            self.assertEqual(len(parameter_definitions), 2)
            self.assertEqual(parameter_definitions[0].name, "color")
            self.assertEqual(parameter_definitions[1].name, "color")
            self.assertEqual(parameter_definitions[0].entity_class_id, 1)

    def test_add_parameter_with_same_name_as_existing_one(self):
        """Test that adding parameter_definitions with an already taken name raises and integrity error."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            db_map.add_items("entity_class", {"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
            db_map.add_items(
                "parameter_definition",
                {"name": "color", "object_class_id": 1},
                {"name": "color", "relationship_class_id": 3},
            )
            with self.assertRaises(SpineIntegrityError):
                db_map.add_items("parameter_definition", {"name": "color", "object_class_id": 1}, strict=True)

    def test_add_parameter_values(self):
        """Test that adding parameter values works."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ["fish", "dog"])
            import_functions.import_relationship_classes(db_map, [("fish_dog", ["fish", "dog"])])
            import_functions.import_objects(db_map, [("fish", "nemo"), ("dog", "pluto")])
            import_functions.import_relationships(db_map, [("fish_dog", ("nemo", "pluto"))])
            import_functions.import_object_parameters(db_map, [("fish", "color")])
            import_functions.import_relationship_parameters(db_map, [("fish_dog", "rel_speed")])
            db_map.commit_session("add")
            color_id = (
                db_map.query(db_map.parameter_definition_sq)
                .filter(db_map.parameter_definition_sq.c.name == "color")
                .first()
                .id
            )
            rel_speed_id = (
                db_map.query(db_map.parameter_definition_sq)
                .filter(db_map.parameter_definition_sq.c.name == "rel_speed")
                .first()
                .id
            )
            nemo_row = db_map.query(db_map.object_sq).filter(db_map.object_sq.c.name == "nemo").first()
            nemo__pluto_row = db_map.query(db_map.wide_relationship_sq).first()
            value1, value_type_1 = to_database("orange")
            value2, value_type_2 = to_database(125)
            db_map.add_items(
                "parameter_value",
                {
                    "parameter_definition_id": color_id,
                    "entity_id": nemo_row.id,
                    "entity_class_id": nemo_row.class_id,
                    "value": value1,
                    "type": value_type_1,
                    "alternative_id": 1,
                },
                {
                    "parameter_definition_id": rel_speed_id,
                    "entity_id": nemo__pluto_row.id,
                    "entity_class_id": nemo__pluto_row.class_id,
                    "value": value2,
                    "type": value_type_2,
                    "alternative_id": 1,
                },
            )
            db_map.commit_session("add")
            table = db_map.get_table("parameter_value")
            parameter_values = db_map.query(table).all()
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
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ["fish", "dog"])
            import_functions.import_relationship_classes(db_map, [("fish_dog", ["fish", "dog"])])
            import_functions.import_objects(db_map, [("fish", "nemo"), ("dog", "pluto")])
            import_functions.import_relationships(db_map, [("fish_dog", "nemo", "pluto")])
            import_functions.import_object_parameters(db_map, [("fish", "color")])
            import_functions.import_relationship_parameters(db_map, [("fish_dog", "rel_speed")])
            _, errors = db_map.add_items(
                "parameter_value",
                {"parameter_definition_id": 1, "object_id": 3, "parsed_value": "orange", "alternative_id": 1},
                strict=False,
            )
            self.assertEqual([str(e) for e in errors], ["invalid entity_class_id for parameter_value"])
            _, errors = db_map.add_items(
                "parameter_value",
                {"parameter_definition_id": 2, "relationship_id": 2, "parsed_value": 125, "alternative_id": 1},
                strict=False,
            )
            self.assertEqual([str(e) for e in errors], ["invalid entity_class_id for parameter_value"])

    def test_add_same_parameter_value_twice(self):
        """Test that adding a parameter value twice only adds the first one."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ["fish"])
            import_functions.import_objects(db_map, [("fish", "nemo")])
            import_functions.import_object_parameters(db_map, [("fish", "color")])
            db_map.commit_session("add")
            color_id = (
                db_map.query(db_map.parameter_definition_sq)
                .filter(db_map.parameter_definition_sq.c.name == "color")
                .first()
                .id
            )
            nemo_row = db_map.query(db_map.object_sq).filter(db_map.object_sq.c.name == "nemo").first()
            value1, type1 = to_database("orange")
            value2, type2 = to_database("blue")
            db_map.add_items(
                "parameter_value",
                {
                    "parameter_definition_id": color_id,
                    "entity_id": nemo_row.id,
                    "entity_class_id": nemo_row.class_id,
                    "value": value1,
                    "type": type1,
                    "alternative_id": 1,
                },
                {
                    "parameter_definition_id": color_id,
                    "entity_id": nemo_row.id,
                    "entity_class_id": nemo_row.class_id,
                    "value": value2,
                    "type": type2,
                    "alternative_id": 1,
                },
            )
            db_map.commit_session("add")
            table = db_map.get_table("parameter_value")
            parameter_values = db_map.query(table).all()
            self.assertEqual(len(parameter_values), 1)
            self.assertEqual(parameter_values[0].parameter_definition_id, 1)
            self.assertEqual(parameter_values[0].entity_id, 1)
            self.assertEqual(parameter_values[0].value, b'"orange"')

    def test_add_existing_parameter_value(self):
        """Test that adding an existing parameter value raises an integrity error."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ["fish"])
            import_functions.import_objects(db_map, [("fish", "nemo")])
            import_functions.import_object_parameters(db_map, [("fish", "color")])
            import_functions.import_object_parameter_values(db_map, [("fish", "nemo", "color", "orange")])
            value, value_type = to_database("blue")
            db_map.commit_session("add")
            _, errors = db_map.add_items(
                "parameter_value",
                {
                    "parameter_definition_id": 1,
                    "entity_class_id": 1,
                    "entity_id": 1,
                    "value": value,
                    "type": value_type,
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
        with DatabaseMapping("sqlite://", create=True) as db_map:
            items, errors = db_map.add_items("alternative", {"name": "my_alternative"})
            self.assertEqual(errors, [])
            self.assertEqual(len(items), 1)
            db_map.commit_session("Add test data.")
            alternatives = db_map.query(db_map.alternative_sq).all()
            self.assertEqual(len(alternatives), 2)
            self.assertEqual(
                alternatives[0]._asdict(), {"id": 1, "name": "Base", "description": "Base alternative", "commit_id": 1}
            )
            self.assertEqual(
                alternatives[1]._asdict(), {"id": 2, "name": "my_alternative", "description": None, "commit_id": 2}
            )

    def test_add_scenario(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            items, errors = db_map.add_items("scenario", {"name": "my_scenario"})
            self.assertEqual(errors, [])
            self.assertEqual(len(items), 1)
            db_map.commit_session("Add test data.")
            scenarios = db_map.query(db_map.scenario_sq).all()
            self.assertEqual(len(scenarios), 1)
            self.assertEqual(
                scenarios[0]._asdict(),
                {"id": 1, "name": "my_scenario", "description": None, "active": False, "commit_id": 2},
            )

    def test_add_scenario_alternative(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_scenarios(db_map, ("my_scenario",))
            db_map.commit_session("Add test data.")
            items, errors = db_map.add_items("scenario_alternative", {"scenario_id": 1, "alternative_id": 1, "rank": 0})
            self.assertEqual(errors, [])
            self.assertEqual(len(items), 1)
            db_map.commit_session("Add test data.")
            scenario_alternatives = db_map.query(db_map.scenario_alternative_sq).all()
            self.assertEqual(len(scenario_alternatives), 1)
            self.assertEqual(
                scenario_alternatives[0]._asdict(),
                {"id": 1, "scenario_id": 1, "alternative_id": 1, "rank": 0, "commit_id": 3},
            )

    def test_add_metadata(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            items, errors = db_map.add_items(
                "metadata", {"name": "test name", "value": "test_add_metadata"}, strict=False
            )
            self.assertEqual(errors, [])
            self.assertEqual(len(items), 1)
            db_map.commit_session("Add metadata")
            metadata = db_map.query(db_map.metadata_sq).all()
            self.assertEqual(len(metadata), 1)
            self.assertEqual(
                metadata[0]._asdict(), {"name": "test name", "id": 1, "value": "test_add_metadata", "commit_id": 2}
            )

    def test_add_metadata_that_exists_does_not_add_it(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_metadata(db_map, ('{"title": "My metadata."}',))
            db_map.commit_session("Add test data.")
            items, _ = db_map.add_items("metadata", {"name": "title", "value": "My metadata."}, strict=False)
            self.assertEqual(items, [])
            metadata = db_map.query(db_map.metadata_sq).all()
            self.assertEqual(len(metadata), 1)
            self.assertEqual(metadata[0]._asdict(), {"name": "title", "id": 1, "value": "My metadata.", "commit_id": 2})

    def test_add_entity_metadata_for_object(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("fish",))
            import_functions.import_objects(db_map, (("fish", "leviathan"),))
            import_functions.import_metadata(db_map, ('{"title": "My metadata."}',))
            db_map.commit_session("Add test data.")
            items, errors = db_map.add_items("entity_metadata", {"entity_id": 1, "metadata_id": 1}, strict=False)
            self.assertEqual(errors, [])
            self.assertEqual(len(items), 1)
            db_map.commit_session("Add entity metadata")
            entity_metadata = db_map.query(db_map.ext_entity_metadata_sq).all()
            self.assertEqual(len(entity_metadata), 1)
            self.assertEqual(
                entity_metadata[0]._asdict(),
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
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_object_class",))
            import_functions.import_objects(db_map, (("my_object_class", "my_object"),))
            import_functions.import_relationship_classes(db_map, (("my_relationship_class", ("my_object_class",)),))
            import_functions.import_relationships(db_map, (("my_relationship_class", ("my_object",)),))
            import_functions.import_metadata(db_map, ('{"title": "My metadata."}',))
            db_map.commit_session("Add test data.")
            items, errors = db_map.add_items("entity_metadata", {"entity_id": 2, "metadata_id": 1}, strict=False)
            self.assertEqual(errors, [])
            self.assertEqual(len(items), 1)
            db_map.commit_session("Add entity metadata")
            entity_metadata = db_map.query(db_map.ext_entity_metadata_sq).all()
            self.assertEqual(len(entity_metadata), 1)
            self.assertEqual(
                entity_metadata[0]._asdict(),
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
        with DatabaseMapping("sqlite://", create=True) as db_map:
            items, errors = db_map.add_items("entity_metadata", {"entity_id": 1, "metadata_id": 1}, strict=False)
            self.assertEqual(items, [])
            self.assertEqual(len(errors), 1)

    def test_add_ext_entity_metadata_for_object(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("fish",))
            import_functions.import_objects(db_map, (("fish", "leviathan"),))
            db_map.commit_session("Add test data.")
            items, errors = db_map.add_ext_entity_metadata(
                {"entity_id": 1, "metadata_name": "key", "metadata_value": "object metadata"}, strict=False
            )
            self.assertEqual(errors, [])
            self.assertEqual(len(items), 1)
            db_map.commit_session("Add entity metadata")
            entity_metadata = db_map.query(db_map.ext_entity_metadata_sq).all()
            self.assertEqual(len(entity_metadata), 1)
            self.assertEqual(
                entity_metadata[0]._asdict(),
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
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("fish",))
            import_functions.import_objects(db_map, (("fish", "leviathan"),))
            import_functions.import_metadata(db_map, ('{"title": "My metadata."}',))
            db_map.commit_session("Add test data.")
            items, errors = db_map.add_ext_entity_metadata(
                {"entity_id": 1, "metadata_name": "title", "metadata_value": "My metadata."}, strict=False
            )
            self.assertEqual(errors, [])
            self.assertEqual(len(items), 1)
            db_map.commit_session("Add entity metadata")
            metadata = db_map.query(db_map.metadata_sq).all()
            self.assertEqual(len(metadata), 1)
            self.assertEqual(metadata[0]._asdict(), {"id": 1, "name": "title", "value": "My metadata.", "commit_id": 2})
            entity_metadata = db_map.query(db_map.ext_entity_metadata_sq).all()
            self.assertEqual(len(entity_metadata), 1)
            self.assertEqual(
                entity_metadata[0]._asdict(),
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
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("fish",))
            import_functions.import_objects(db_map, (("fish", "leviathan"),))
            import_functions.import_object_parameters(db_map, (("fish", "paranormality"),))
            import_functions.import_object_parameter_values(db_map, (("fish", "leviathan", "paranormality", 3.9),))
            import_functions.import_metadata(db_map, ('{"title": "My metadata."}',))
            db_map.commit_session("Add test data.")
            items, errors = db_map.add_items(
                "parameter_value_metadata", {"parameter_value_id": 1, "metadata_id": 1}, strict=False
            )
            self.assertEqual(errors, [])
            self.assertEqual(len(items), 1)
            db_map.commit_session("Add value metadata")
            value_metadata = db_map.query(db_map.ext_parameter_value_metadata_sq).all()
            self.assertEqual(len(value_metadata), 1)
            self.assertEqual(
                value_metadata[0]._asdict(),
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
        with DatabaseMapping("sqlite://", create=True) as db_map:
            items, errors = db_map.add_items(
                "parameter_value_metadata", {"parameter_value_id": 1, "metadata_id": 1, "alternative_id": 1}
            )
            self.assertEqual(len(items), 0)
            self.assertEqual(len(errors), 1)

    def test_add_ext_parameter_value_metadata(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("fish",))
            import_functions.import_objects(db_map, (("fish", "leviathan"),))
            import_functions.import_object_parameters(db_map, (("fish", "paranormality"),))
            import_functions.import_object_parameter_values(db_map, (("fish", "leviathan", "paranormality", 3.9),))
            db_map.commit_session("Add test data.")
            items, errors = db_map.add_ext_parameter_value_metadata(
                {"parameter_value_id": 1, "metadata_name": "key", "metadata_value": "parameter metadata"}, strict=False
            )
            self.assertEqual(errors, [])
            self.assertEqual(len(items), 1)
            db_map.commit_session("Add value metadata")
            value_metadata = db_map.query(db_map.ext_parameter_value_metadata_sq).all()
            self.assertEqual(len(value_metadata), 1)
            self.assertEqual(
                value_metadata[0]._asdict(),
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
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("fish",))
            import_functions.import_objects(db_map, (("fish", "leviathan"),))
            import_functions.import_object_parameters(db_map, (("fish", "paranormality"),))
            import_functions.import_object_parameter_values(db_map, (("fish", "leviathan", "paranormality", 3.9),))
            import_functions.import_metadata(db_map, ('{"title": "My metadata."}',))
            db_map.commit_session("Add test data.")
            items, errors = db_map.add_ext_parameter_value_metadata(
                {"parameter_value_id": 1, "metadata_name": "title", "metadata_value": "My metadata."}, strict=False
            )
            self.assertEqual(errors, [])
            self.assertEqual(len(items), 1)
            db_map.commit_session("Add value metadata")
            metadata = db_map.query(db_map.metadata_sq).all()
            self.assertEqual(len(metadata), 1)
            self.assertEqual(metadata[0]._asdict(), {"id": 1, "name": "title", "value": "My metadata.", "commit_id": 2})
            value_metadata = db_map.query(db_map.ext_parameter_value_metadata_sq).all()
            self.assertEqual(len(value_metadata), 1)
            self.assertEqual(
                value_metadata[0]._asdict(),
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
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_entity_classes(
                db_map, (("fish",), ("dog",), ("animal",), ("two_animals", ("animal", "animal")))
            )
            import_functions.import_superclass_subclasses(db_map, (("animal", "fish"), ("animal", "dog")))
            import_functions.import_entities(db_map, (("fish", "Nemo"), ("dog", "Pulgoso")))
            db_map.commit_session("Add test data.")
            item, error = db_map.add_item(
                "entity", entity_class_name="two_animals", element_name_list=("Nemo", "Pulgoso")
            )
            self.assertTrue(item)
            self.assertFalse(error)
            db_map.commit_session("Add test data.")
            entities = db_map.query(db_map.wide_entity_sq).all()
            self.assertEqual(len(entities), 3)
            self.assertIn("Nemo,Pulgoso", {x.element_name_list for x in entities})


class TestDatabaseMappingUpdate(AssertSuccessTestCase):
    def test_update_object_classes(self):
        """Test that updating object classes works."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"id": 1, "name": "fish"}, {"id": 2, "name": "dog"})
            items, intgr_error_log = db_map.update_items(
                "entity_class", {"id": 1, "name": "octopus"}, {"id": 2, "name": "god"}
            )
            ids = {x.resolve()["id"] for x in items}
            db_map.commit_session("test commit")
            sq = db_map.object_class_sq
            object_classes = {x.id: x.name for x in db_map.query(sq).filter(sq.c.id.in_(ids))}
            self.assertEqual(intgr_error_log, [])
            self.assertEqual(object_classes[1], "octopus")
            self.assertEqual(object_classes[2], "god")

    def test_update_objects(self):
        """Test that updating objects works."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"id": 1, "name": "fish"})
            db_map.add_items(
                "entity", {"id": 1, "name": "nemo", "class_id": 1}, {"id": 2, "name": "dory", "class_id": 1}
            )
            items, intgr_error_log = db_map.update_items(
                "entity", {"id": 1, "name": "klaus"}, {"id": 2, "name": "squidward"}
            )
            ids = {x.resolve()["id"] for x in items}
            db_map.commit_session("test commit")
            sq = db_map.object_sq
            objects = {x.id: x.name for x in db_map.query(sq).filter(sq.c.id.in_(ids))}
            self.assertEqual(intgr_error_log, [])
            self.assertEqual(objects[1], "klaus")
            self.assertEqual(objects[2], "squidward")

    def test_update_committed_object(self):
        """Test that updating objects works."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"id": 1, "name": "some_class"})
            db_map.add_items("entity", {"id": 1, "name": "nemo", "class_id": 1})
            db_map.commit_session("update")
            items, intgr_error_log = db_map.update_items("entity", {"id": 1, "name": "klaus"})
            ids = {x.resolve()["id"] for x in items}
            db_map.commit_session("test commit")
            sq = db_map.object_sq
            objects = {x.id: x.name for x in db_map.query(sq).filter(sq.c.id.in_(ids))}
            self.assertEqual(intgr_error_log, [])
            self.assertEqual(objects[1], "klaus")
            self.assertEqual(db_map.query(db_map.object_sq).filter_by(id=1).first().name, "klaus")

    def test_update_relationship_classes(self):
        """Test that updating relationship classes works."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "dog", "id": 1}, {"name": "fish", "id": 2})
            db_map.add_items(
                "entity_class",
                {"id": 3, "name": "dog__fish", "object_class_id_list": [1, 2]},
                {"id": 4, "name": "fish__dog", "object_class_id_list": [2, 1]},
            )
            items, intgr_error_log = db_map.update_items(
                "entity_class", {"id": 3, "name": "god__octopus"}, {"id": 4, "name": "octopus__dog"}
            )
            ids = {x.resolve()["id"] for x in items}
            db_map.commit_session("test commit")
            sq = db_map.wide_relationship_class_sq
            rel_clss = {x.id: x.name for x in db_map.query(sq).filter(sq.c.id.in_(ids))}
            self.assertEqual(intgr_error_log, [])
            self.assertEqual(rel_clss[3], "god__octopus")
            self.assertEqual(rel_clss[4], "octopus__dog")

    def test_update_committed_relationship_class(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("object_class_1",))
            import_functions.import_relationship_classes(db_map, (("my_class", ("object_class_1",)),))
            db_map.commit_session("Add test data")
            items, errors = db_map.update_items("entity_class", {"id": 2, "name": "renamed"})
            updated_ids = {x.resolve()["id"] for x in items}
            self.assertEqual(errors, [])
            self.assertEqual(updated_ids, {2})
            db_map.commit_session("Update data.")
            classes = db_map.query(db_map.wide_relationship_class_sq).all()
            self.assertEqual(len(classes), 1)
            self.assertEqual(classes[0].name, "renamed")

    def test_update_relationship_class_does_not_update_member_class_id(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("object_class_1", "object_class_2"))
            import_functions.import_relationship_classes(db_map, (("my_class", ("object_class_1",)),))
            db_map.commit_session("Add test data")
            items, errors = db_map.update_items(
                "entity_class", {"id": 3, "name": "renamed", "object_class_id_list": [2]}
            )
            self.assertEqual([str(err) for err in errors], ["can't modify dimensions of an entity class"])
            self.assertEqual(len(items), 0)

    def test_update_relationships(self):
        """Test that updating relationships works."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "fish", "id": 1}, {"name": "dog", "id": 2})
            db_map.add_items("entity_class", {"name": "fish__dog", "id": 3, "object_class_id_list": [1, 2]})
            db_map.add_items(
                "entity",
                {"name": "nemo", "id": 1, "class_id": 1},
                {"name": "pluto", "id": 2, "class_id": 2},
                {"name": "scooby", "id": 3, "class_id": 2},
            )
            db_map.add_items(
                "entity",
                {
                    "id": 4,
                    "name": "nemo__pluto",
                    "class_id": 3,
                    "object_id_list": [1, 2],
                    "object_class_id_list": [1, 2],
                },
            )
            items, intgr_error_log = db_map.update_items(
                "entity",
                {
                    "id": 4,
                    "name": "nemo__scooby",
                    "class_id": 3,
                    "object_id_list": [1, 3],
                    "object_class_id_list": [1, 2],
                },
            )
            ids = {x.resolve()["id"] for x in items}
            db_map.commit_session("test commit")
            sq = db_map.wide_relationship_sq
            rels = {
                x.id: {"name": x.name, "object_id_list": x.object_id_list}
                for x in db_map.query(sq).filter(sq.c.id.in_(ids))
            }
            self.assertEqual(intgr_error_log, [])
            self.assertEqual(rels[4]["name"], "nemo__scooby")
            self.assertEqual(rels[4]["object_id_list"], "1,3")

    def test_update_committed_relationship(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("object_class_1", "object_class_2"))
            import_functions.import_objects(
                db_map,
                (("object_class_1", "object_11"), ("object_class_1", "object_12"), ("object_class_2", "object_21")),
            )
            import_functions.import_relationship_classes(db_map, (("my_class", ("object_class_1", "object_class_2")),))
            import_functions.import_relationships(db_map, (("my_class", ("object_11", "object_21")),))
            db_map.commit_session("Add test data")
            items, errors = db_map.update_items("entity", {"id": 4, "name": "renamed", "object_id_list": [2, 3]})
            updated_ids = {x.resolve()["id"] for x in items}
            self.assertEqual(errors, [])
            self.assertEqual(updated_ids, {4})
            db_map.commit_session("Update data.")
            relationships = db_map.query(db_map.wide_relationship_sq).all()
            self.assertEqual(len(relationships), 1)
            self.assertEqual(relationships[0].name, "renamed")
            self.assertEqual(relationships[0].object_name_list, "object_12,object_21")

    def test_update_parameter_value_by_id_only(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("object_class1",))
            import_functions.import_object_parameters(db_map, (("object_class1", "parameter1"),))
            import_functions.import_objects(db_map, (("object_class1", "object1"),))
            import_functions.import_object_parameter_values(
                db_map, (("object_class1", "object1", "parameter1", "something"),)
            )
            db_map.commit_session("Populate with initial data.")
            items, errors = db_map.update_items("parameter_value", {"id": 1, "parsed_value": "something else"})
            updated_ids = {x.resolve()["id"] for x in items}
            self.assertEqual(errors, [])
            self.assertEqual(updated_ids, {1})
            db_map.commit_session("Update data.")
            pvals = db_map.query(db_map.parameter_value_sq).all()
            self.assertEqual(len(pvals), 1)
            pval = pvals[0]
            expected_value, expected_type = to_database("something else")
            self.assertEqual(pval.value, expected_value)
            self.assertEqual(pval.type, expected_type)

    def test_update_parameter_value_to_an_uncommitted_list_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("object_class1",))
            import_functions.import_parameter_value_lists(db_map, (("values_1", 5.0),))
            import_functions.import_object_parameters(db_map, (("object_class1", "parameter1", None, "values_1"),))
            import_functions.import_objects(db_map, (("object_class1", "object1"),))
            import_functions.import_object_parameter_values(db_map, (("object_class1", "object1", "parameter1", 5.0),))
            db_map.commit_session("Update data.")
            import_functions.import_parameter_value_lists(db_map, (("values_1", 7.0),))
            value, type_ = to_database(7.0)
            items, errors = db_map.update_items("parameter_value", {"id": 1, "value": value, "type": type_})
            self.assertEqual(errors, [])
            self.assertEqual(len(items), 1)
            db_map.commit_session("Update data.")
            pvals = db_map.query(db_map.parameter_value_sq).all()
            self.assertEqual(from_database(pvals[0].value, pvals[0].type), 7.0)

    def test_update_parameter_definition_by_id_only(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("object_class1",))
            import_functions.import_object_parameters(db_map, (("object_class1", "parameter1"),))
            db_map.commit_session("Populate with initial data.")
            items, errors = db_map.update_items("parameter_definition", {"id": 1, "name": "parameter2"})
            updated_ids = {x.resolve()["id"] for x in items}
            self.assertEqual(errors, [])
            self.assertEqual(updated_ids, {1})
            db_map.commit_session("Update data.")
            pdefs = db_map.query(db_map.parameter_definition_sq).all()
            self.assertEqual(len(pdefs), 1)
            self.assertEqual(pdefs[0].name, "parameter2")

    def test_update_parameter_definition_value_list(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_parameter_value_lists(db_map, (("my_list", 99.0),))
            import_functions.import_object_classes(db_map, ("object_class",))
            import_functions.import_object_parameters(db_map, (("object_class", "my_parameter"),))
            db_map.commit_session("Populate with initial data.")
            items, errors = db_map.update_items(
                "parameter_definition", {"id": 1, "name": "my_parameter", "parameter_value_list_id": 1}
            )
            updated_ids = {x.resolve()["id"] for x in items}
            self.assertEqual(errors, [])
            self.assertEqual(updated_ids, {1})
            db_map.commit_session("Update data.")
            pdefs = db_map.query(db_map.parameter_definition_sq).all()
            self.assertEqual(len(pdefs), 1)
            self.assertEqual(
                pdefs[0]._asdict(),
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
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_parameter_value_lists(db_map, (("my_list", 99.0),))
            import_functions.import_object_classes(db_map, ("object_class",))
            import_functions.import_objects(db_map, (("object_class", "my_object"),))
            import_functions.import_object_parameters(db_map, (("object_class", "my_parameter"),))
            import_functions.import_object_parameter_values(
                db_map, (("object_class", "my_object", "my_parameter", 23.0),)
            )
            db_map.commit_session("Populate with initial data.")
            items, errors = db_map.update_items(
                "parameter_definition", {"id": 1, "name": "my_parameter", "parameter_value_list_id": 1}
            )
            self.assertEqual(
                list(map(str, errors)),
                ["can't modify the parameter value list of a parameter that already has values"],
            )
            self.assertEqual(items, [])

    def test_update_parameter_definitions_default_value_that_is_not_on_value_list_gives_error(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_parameter_value_lists(db_map, (("my_list", 99.0),))
            import_functions.import_object_classes(db_map, ("object_class",))
            import_functions.import_objects(db_map, (("object_class", "my_object"),))
            import_functions.import_object_parameters(db_map, (("object_class", "my_parameter", None, "my_list"),))
            db_map.commit_session("Populate with initial data.")
            items, errors = db_map.update_items(
                "parameter_definition", {"id": 1, "name": "my_parameter", "default_value": to_database(23.0)[0]}
            )
            updated_ids = {x["id"] for x in items}
            self.assertEqual(list(map(str, errors)), ["default value 23.0 of my_parameter is not in my_list"])
            self.assertEqual(updated_ids, set())

    def test_update_parameter_definition_value_list_when_default_value_not_on_the_list_exists_gives_error(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_parameter_value_lists(db_map, (("my_list", 99.0),))
            import_functions.import_object_classes(db_map, ("object_class",))
            import_functions.import_objects(db_map, (("object_class", "my_object"),))
            import_functions.import_object_parameters(db_map, (("object_class", "my_parameter", 23.0),))
            db_map.commit_session("Populate with initial data.")
            items, errors = db_map.update_items(
                "parameter_definition", {"id": 1, "name": "my_parameter", "parameter_value_list_id": 1}
            )
            updated_ids = {x["id"] for x in items}
            self.assertEqual(list(map(str, errors)), ["default value 23.0 of my_parameter is not in my_list"])
            self.assertEqual(updated_ids, set())

    def test_update_object_metadata(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            import_functions.import_objects(db_map, (("my_class", "my_object"),))
            import_functions.import_metadata(db_map, ('{"title": "My metadata."}',))
            import_functions.import_object_metadata(db_map, (("my_class", "my_object", '{"title": "My metadata."}'),))
            db_map.commit_session("Add test data")
            items, errors = db_map.update_ext_entity_metadata(
                {"id": 1, "metadata_name": "key_2", "metadata_value": "new value"}
            )
            self.assertEqual(errors, [])
            self.assertEqual(len(items), 2)
            db_map.remove_unused_metadata()
            db_map.commit_session("Update data")
            metadata_entries = db_map.query(db_map.metadata_sq).all()
            self.assertEqual(len(metadata_entries), 1)
            self.assertEqual(
                metadata_entries[0]._asdict(), {"id": 1, "name": "key_2", "value": "new value", "commit_id": 3}
            )
            entity_metadata_entries = db_map.query(db_map.entity_metadata_sq).all()
            self.assertEqual(len(entity_metadata_entries), 1)
            self.assertEqual(
                entity_metadata_entries[0]._asdict(), {"id": 1, "entity_id": 1, "metadata_id": 1, "commit_id": 3}
            )

    def test_update_object_metadata_reuses_existing_metadata(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            import_functions.import_objects(db_map, (("my_class", "my_object"), ("my_class", "extra_object")))
            import_functions.import_metadata(db_map, ('{"title": "My metadata."}', '{"key 2": "metadata value 2"}'))
            import_functions.import_object_metadata(
                db_map,
                (
                    ("my_class", "my_object", '{"title": "My metadata."}'),
                    ("my_class", "extra_object", '{"key 2": "metadata value 2"}'),
                ),
            )
            db_map.commit_session("Add test data")
            items, errors = db_map.update_ext_entity_metadata(
                *[{"id": 1, "metadata_name": "key 2", "metadata_value": "metadata value 2"}]
            )
            ids = {x.resolve()["id"] for x in items}
            self.assertEqual(errors, [])
            self.assertEqual(ids, {1})
            db_map.remove_unused_metadata()
            db_map.commit_session("Update data")
            metadata_entries = db_map.query(db_map.metadata_sq).all()
            self.assertEqual(len(metadata_entries), 1)
            self.assertEqual(
                metadata_entries[0]._asdict(), {"id": 2, "name": "key 2", "value": "metadata value 2", "commit_id": 2}
            )
            entity_metadata_entries = db_map.query(db_map.entity_metadata_sq).all()
            self.assertEqual(len(entity_metadata_entries), 2)
            self.assertEqual(
                entity_metadata_entries[0]._asdict(), {"id": 1, "entity_id": 1, "metadata_id": 2, "commit_id": 3}
            )
            self.assertEqual(
                entity_metadata_entries[1]._asdict(), {"id": 2, "entity_id": 2, "metadata_id": 2, "commit_id": 2}
            )

    def test_update_object_metadata_keeps_metadata_still_in_use(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            import_functions.import_objects(db_map, (("my_class", "object_1"), ("my_class", "object_2")))
            import_functions.import_metadata(db_map, ('{"title": "My metadata."}',))
            import_functions.import_object_metadata(
                db_map,
                (
                    ("my_class", "object_1", '{"title": "My metadata."}'),
                    ("my_class", "object_2", '{"title": "My metadata."}'),
                ),
            )
            db_map.commit_session("Add test data")
            items, errors = db_map.update_ext_entity_metadata(
                *[{"id": 1, "metadata_name": "new key", "metadata_value": "new value"}]
            )
            self.assertEqual(errors, [])
            self.assertEqual(len(items), 2)
            db_map.commit_session("Update data")
            metadata_entries = db_map.query(db_map.metadata_sq).all()
            self.assertEqual(len(metadata_entries), 2)
            self.assertEqual(
                metadata_entries[0]._asdict(), {"id": 1, "name": "title", "value": "My metadata.", "commit_id": 2}
            )
            self.assertEqual(
                metadata_entries[1]._asdict(), {"id": 2, "name": "new key", "value": "new value", "commit_id": 3}
            )
            entity_metadata_entries = db_map.query(db_map.entity_metadata_sq).all()
            self.assertEqual(len(entity_metadata_entries), 2)
            self.assertEqual(
                entity_metadata_entries[0]._asdict(), {"id": 1, "entity_id": 1, "metadata_id": 2, "commit_id": 3}
            )
            self.assertEqual(
                entity_metadata_entries[1]._asdict(), {"id": 2, "entity_id": 2, "metadata_id": 1, "commit_id": 2}
            )

    def test_update_parameter_value_metadata(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            import_functions.import_object_parameters(db_map, (("my_class", "my_parameter"),))
            import_functions.import_objects(db_map, (("my_class", "my_object"),))
            import_functions.import_object_parameter_values(db_map, (("my_class", "my_object", "my_parameter", 99.0),))
            import_functions.import_metadata(db_map, ('{"title": "My metadata."}',))
            import_functions.import_object_parameter_value_metadata(
                db_map, (("my_class", "my_object", "my_parameter", '{"title": "My metadata."}'),)
            )
            db_map.commit_session("Add test data")
            items, errors = db_map.update_ext_parameter_value_metadata(
                {"id": 1, "metadata_name": "key_2", "metadata_value": "new value"}
            )
            self.assertEqual(errors, [])
            self.assertEqual(len(items), 2)
            db_map.remove_unused_metadata()
            db_map.commit_session("Update data")
            metadata_entries = db_map.query(db_map.metadata_sq).all()
            self.assertEqual(len(metadata_entries), 1)
            self.assertEqual(
                metadata_entries[0]._asdict(), {"id": 1, "name": "key_2", "value": "new value", "commit_id": 3}
            )
            value_metadata_entries = db_map.query(db_map.parameter_value_metadata_sq).all()
            self.assertEqual(len(value_metadata_entries), 1)
            self.assertEqual(
                value_metadata_entries[0]._asdict(),
                {"id": 1, "parameter_value_id": 1, "metadata_id": 1, "commit_id": 3},
            )

    def test_update_parameter_value_metadata_will_not_delete_shared_entity_metadata(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            import_functions.import_object_parameters(db_map, (("my_class", "my_parameter"),))
            import_functions.import_objects(db_map, (("my_class", "my_object"),))
            import_functions.import_object_parameter_values(db_map, (("my_class", "my_object", "my_parameter", 99.0),))
            import_functions.import_metadata(db_map, ('{"title": "My metadata."}',))
            import_functions.import_object_metadata(db_map, (("my_class", "my_object", '{"title": "My metadata."}'),))
            import_functions.import_object_parameter_value_metadata(
                db_map, (("my_class", "my_object", "my_parameter", '{"title": "My metadata."}'),)
            )
            db_map.commit_session("Add test data")
            items, errors = db_map.update_ext_parameter_value_metadata(
                *[{"id": 1, "metadata_name": "key_2", "metadata_value": "new value"}]
            )
            self.assertEqual(errors, [])
            self.assertEqual(len(items), 2)
            db_map.commit_session("Update data")
            metadata_entries = db_map.query(db_map.metadata_sq).all()
            self.assertEqual(len(metadata_entries), 2)
            self.assertEqual(
                metadata_entries[0]._asdict(), {"id": 1, "name": "title", "value": "My metadata.", "commit_id": 2}
            )
            self.assertEqual(
                metadata_entries[1]._asdict(), {"id": 2, "name": "key_2", "value": "new value", "commit_id": 3}
            )
            value_metadata_entries = db_map.query(db_map.parameter_value_metadata_sq).all()
            self.assertEqual(len(value_metadata_entries), 1)
            self.assertEqual(
                value_metadata_entries[0]._asdict(),
                {"id": 1, "parameter_value_id": 1, "metadata_id": 2, "commit_id": 3},
            )
            entity_metadata_entries = db_map.query(db_map.entity_metadata_sq).all()
            self.assertEqual(len(entity_metadata_entries), 1)
            self.assertEqual(
                entity_metadata_entries[0]._asdict(), {"id": 1, "entity_id": 1, "metadata_id": 1, "commit_id": 2}
            )

    def test_update_metadata(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_metadata(db_map, ('{"title": "My metadata."}',))
            db_map.commit_session("Add test data.")
            items, errors = db_map.update_items("metadata", *({"id": 1, "name": "author", "value": "Prof. T. Est"},))
            ids = {x.resolve()["id"] for x in items}
            self.assertEqual(errors, [])
            self.assertEqual(ids, {1})
            db_map.commit_session("Update data")
            metadata_records = db_map.query(db_map.metadata_sq).all()
            self.assertEqual(len(metadata_records), 1)
            self.assertEqual(
                metadata_records[0]._asdict(), {"id": 1, "name": "author", "value": "Prof. T. Est", "commit_id": 3}
            )


class TestDatabaseMappingRemoveMixin(AssertSuccessTestCase):
    def test_remove_object_class(self):
        """Test adding and removing an object class and committing"""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            items, _ = db_map.add_items("entity_class", {"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            self.assertEqual(len(items), 2)
            db_map.remove_items("object_class", 1, 2)
            with self.assertRaises(SpineDBAPIError):
                # Nothing to commit
                db_map.commit_session("delete")
            self.assertEqual(len(db_map.query(db_map.object_class_sq).all()), 0)

    def test_remove_object_class_from_committed_session(self):
        """Test removing an object class from a committed session"""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            items, _ = db_map.add_items("entity_class", {"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            db_map.commit_session("add")
            self.assertEqual(len(db_map.query(db_map.object_class_sq).all()), 2)
            db_map.remove_items("object_class", *{x["id"] for x in items})
            db_map.commit_session("Add test data.")
            self.assertEqual(len(db_map.query(db_map.object_class_sq).all()), 0)

    def test_remove_object(self):
        """Test adding and removing an object and committing"""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            items, _ = db_map.add_items(
                "entity", {"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 2}
            )
            db_map.remove_items("object", *{x["id"] for x in items})
            db_map.commit_session("delete")
            self.assertEqual(len(db_map.query(db_map.object_sq).all()), 0)

    def test_remove_object_from_committed_session(self):
        """Test removing an object from a committed session"""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            items, _ = db_map.add_items(
                "entity", {"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 2}
            )
            db_map.commit_session("add")
            self.assertEqual(len(db_map.query(db_map.object_sq).all()), 2)
            db_map.remove_items("object", *{x["id"] for x in items})
            db_map.commit_session("Add test data.")
            self.assertEqual(len(db_map.query(db_map.object_sq).all()), 0)

    def test_remove_entity_group(self):
        """Test adding and removing an entity group and committing"""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1})
            db_map.add_items("entity", {"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 1})
            items, _ = db_map.add_items("entity_group", {"entity_id": 1, "entity_class_id": 1, "member_id": 2})
            db_map.remove_items("entity_group", *{x["id"] for x in items})
            db_map.commit_session("delete")
            self.assertEqual(len(db_map.query(db_map.entity_group_sq).all()), 0)

    def test_remove_entity_group_from_committed_session(self):
        """Test removing an entity group from a committed session"""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1})
            db_map.add_items("entity", {"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 1})
            db_map.add_items("entity_group", {"entity_id": 1, "entity_class_id": 1, "member_id": 2})
            db_map.commit_session("add")
            self.assertEqual(len(db_map.query(db_map.entity_group_sq).all()), 1)
            db_map.remove_items("entity_group", 1)
            db_map.commit_session("delete")
            self.assertEqual(len(db_map.query(db_map.entity_group_sq).all()), 0)

    def test_cascade_remove_relationship_class(self):
        """Test adding and removing a relationship class and committing"""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            items, _ = db_map.add_items("entity_class", {"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
            db_map.remove_items("relationship_class", *{x["id"] for x in items})
            db_map.commit_session("delete")
            self.assertEqual(len(db_map.query(db_map.wide_relationship_class_sq).all()), 0)

    def test_cascade_remove_relationship_class_from_committed_session(self):
        """Test removing a relationship class from a committed session"""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            items, _ = db_map.add_items("entity_class", {"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
            db_map.commit_session("add")
            self.assertEqual(len(db_map.query(db_map.wide_relationship_class_sq).all()), 1)
            db_map.remove_items("relationship_class", *{x["id"] for x in items})
            db_map.commit_session("remove")
            self.assertEqual(len(db_map.query(db_map.wide_relationship_class_sq).all()), 0)

    def test_cascade_remove_relationship(self):
        """Test adding and removing a relationship and committing"""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            db_map.add_items("entity_class", {"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
            db_map.add_items("entity", {"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 2})
            items, _ = db_map.add_items(
                "entity", {"id": 3, "name": "remove_me", "class_id": 3, "object_id_list": [1, 2]}
            )
            db_map.remove_items("relationship", *{x["id"] for x in items})
            self.assertEqual(len(db_map.query(db_map.wide_relationship_sq).all()), 0)
            db_map.commit_session("delete")
            self.assertEqual(len(db_map.query(db_map.wide_relationship_sq).all()), 0)

    def test_cascade_remove_relationship_from_committed_session(self):
        """Test removing a relationship from a committed session"""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1}, {"name": "oc2", "id": 2})
            db_map.add_items("entity_class", {"name": "rc1", "id": 3, "object_class_id_list": [1, 2]})
            db_map.add_items("entity", {"name": "o1", "id": 1, "class_id": 1}, {"name": "o2", "id": 2, "class_id": 2})
            items, _ = db_map.add_items(
                "entity", {"id": 3, "name": "remove_me", "class_id": 3, "object_id_list": [1, 2]}
            )
            db_map.commit_session("add")
            self.assertEqual(len(db_map.query(db_map.wide_relationship_sq).all()), 1)
            db_map.remove_items("relationship", *{x["id"] for x in items})
            db_map.commit_session("Add test data.")
            self.assertEqual(len(db_map.query(db_map.wide_relationship_sq).all()), 0)

    def test_remove_parameter_value(self):
        """Test adding and removing a parameter value and committing"""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1}, strict=True)
            db_map.add_items("entity", {"name": "o1", "id": 1, "class_id": 1}, strict=True)
            db_map.add_items("parameter_definition", {"name": "param", "id": 1, "object_class_id": 1}, strict=True)
            value, value_type = to_database(0)
            db_map.add_items(
                "parameter_value",
                {
                    "value": value,
                    "type": value_type,
                    "id": 1,
                    "parameter_definition_id": 1,
                    "object_id": 1,
                    "object_class_id": 1,
                    "alternative_id": 1,
                },
                strict=True,
            )
            db_map.commit_session("add")
            self.assertEqual(len(db_map.query(db_map.parameter_value_sq).all()), 1)
            db_map.remove_items("parameter_value", 1)
            db_map.commit_session("delete")
            self.assertEqual(len(db_map.query(db_map.parameter_value_sq).all()), 0)

    def test_remove_parameter_value_from_committed_session(self):
        """Test adding and committing a parameter value and then removing it"""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1}, strict=True)
            db_map.add_items("entity", {"name": "o1", "id": 1, "class_id": 1}, strict=True)
            db_map.add_items("parameter_definition", {"name": "param", "id": 1, "object_class_id": 1}, strict=True)
            value, value_type = to_database(0)
            db_map.add_items(
                "parameter_value",
                {
                    "value": value,
                    "type": value_type,
                    "id": 1,
                    "parameter_definition_id": 1,
                    "object_id": 1,
                    "object_class_id": 1,
                    "alternative_id": 1,
                },
                strict=True,
            )
            db_map.commit_session("add")
            self.assertEqual(len(db_map.query(db_map.parameter_value_sq).all()), 1)
            db_map.remove_items("parameter_value", 1)
            db_map.commit_session("delete")
            self.assertEqual(len(db_map.query(db_map.parameter_value_sq).all()), 0)

    def test_cascade_remove_object_removes_parameter_value_as_well(self):
        """Test adding and removing a parameter value and committing"""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1}, strict=True)
            db_map.add_items("entity", {"name": "o1", "id": 1, "class_id": 1}, strict=True)
            db_map.add_items("parameter_definition", {"name": "param", "id": 1, "object_class_id": 1}, strict=True)
            value, value_type = to_database(0)
            db_map.add_items(
                "parameter_value",
                {
                    "value": value,
                    "type": value_type,
                    "id": 1,
                    "parameter_definition_id": 1,
                    "object_id": 1,
                    "object_class_id": 1,
                    "alternative_id": 1,
                },
                strict=True,
            )
            db_map.commit_session("add")
            self.assertEqual(len(db_map.query(db_map.parameter_value_sq).all()), 1)
            db_map.remove_items("object", 1)
            db_map.commit_session("delete")
            self.assertEqual(len(db_map.query(db_map.parameter_value_sq).all()), 0)

    def test_cascade_remove_object_from_committed_session_removes_parameter_value_as_well(self):
        """Test adding and committing a paramater value and then removing it"""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "oc1", "id": 1}, strict=True)
            db_map.add_items("entity", {"name": "o1", "id": 1, "class_id": 1}, strict=True)
            db_map.add_items("parameter_definition", {"name": "param", "id": 1, "object_class_id": 1}, strict=True)
            value, value_type = to_database(0)
            db_map.add_items(
                "parameter_value",
                {
                    "value": value,
                    "type": value_type,
                    "id": 1,
                    "parameter_definition_id": 1,
                    "object_id": 1,
                    "object_class_id": 1,
                    "alternative_id": 1,
                },
                strict=True,
            )
            db_map.commit_session("add")
            self.assertEqual(len(db_map.query(db_map.parameter_value_sq).all()), 1)
            db_map.remove_items("object", 1)
            db_map.commit_session("delete")
            self.assertEqual(len(db_map.query(db_map.parameter_value_sq).all()), 0)

    def test_cascade_remove_metadata_removes_corresponding_entity_and_value_metadata(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            import_functions.import_objects(db_map, (("my_class", "my_object"),))
            import_functions.import_object_parameters(db_map, (("my_class", "my_parameter"),))
            import_functions.import_object_parameter_values(db_map, (("my_class", "my_object", "my_parameter", 99.0),))
            import_functions.import_metadata(db_map, ('{"title": "My metadata."}',))
            import_functions.import_object_metadata(db_map, (("my_class", "my_object", '{"title": "My metadata."}'),))
            import_functions.import_object_parameter_value_metadata(
                db_map, (("my_class", "my_object", "my_parameter", '{"title": "My metadata."}'),)
            )
            db_map.commit_session("Add test data.")
            metadata = db_map.query(db_map.metadata_sq).all()
            self.assertEqual(len(metadata), 1)
            db_map.remove_items("metadata", metadata[0].id)
            db_map.commit_session("Remove test data.")
            self.assertEqual(len(db_map.query(db_map.metadata_sq).all()), 0)
            self.assertEqual(len(db_map.query(db_map.parameter_value_metadata_sq).all()), 0)
            self.assertEqual(len(db_map.query(db_map.entity_metadata_sq).all()), 0)
            self.assertEqual(len(db_map.query(db_map.object_sq).all()), 1)
            self.assertEqual(len(db_map.query(db_map.object_parameter_definition_sq).all()), 1)

    def test_cascade_remove_entity_metadata_removes_corresponding_metadata(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            import_functions.import_objects(db_map, (("my_class", "my_object"),))
            import_functions.import_metadata(db_map, ('{"title": "My metadata."}',))
            import_functions.import_object_metadata(db_map, (("my_class", "my_object", '{"title": "My metadata."}'),))
            db_map.commit_session("Add test data.")
            entity_metadata = db_map.query(db_map.entity_metadata_sq).all()
            self.assertEqual(len(entity_metadata), 1)
            db_map.remove_items("entity_metadata", entity_metadata[0].id)
            db_map.remove_unused_metadata()
            db_map.commit_session("Remove test data.")
            self.assertEqual(len(db_map.query(db_map.metadata_sq).all()), 0)
            self.assertEqual(len(db_map.query(db_map.entity_metadata_sq).all()), 0)
            self.assertEqual(len(db_map.query(db_map.object_sq).all()), 1)

    def test_cascade_remove_entity_metadata_leaves_metadata_used_by_value_intact(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            import_functions.import_objects(db_map, (("my_class", "my_object"),))
            import_functions.import_object_parameters(db_map, (("my_class", "my_parameter"),))
            import_functions.import_object_parameter_values(db_map, (("my_class", "my_object", "my_parameter", 99.0),))
            import_functions.import_metadata(db_map, ('{"title": "My metadata."}',))
            self._assert_imports(
                import_functions.import_object_metadata(
                    db_map, (("my_class", "my_object", '{"title": "My metadata."}'),)
                )
            )
            import_functions.import_object_parameter_value_metadata(
                db_map, (("my_class", "my_object", "my_parameter", '{"title": "My metadata."}'),)
            )
            db_map.commit_session("Add test data.")
            entity_metadata = db_map.query(db_map.entity_metadata_sq).all()
            self.assertEqual(len(entity_metadata), 1)
            db_map.remove_items("entity_metadata", entity_metadata[0].id)
            db_map.commit_session("Remove test data.")
            self.assertEqual(len(db_map.query(db_map.metadata_sq).all()), 1)
            self.assertEqual(len(db_map.query(db_map.entity_metadata_sq).all()), 0)
            self.assertEqual(len(db_map.query(db_map.parameter_value_metadata_sq).all()), 1)

    def test_cascade_remove_value_metadata_leaves_metadata_used_by_entity_intact(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            import_functions.import_objects(db_map, (("my_class", "my_object"),))
            import_functions.import_object_parameters(db_map, (("my_class", "my_parameter"),))
            import_functions.import_object_parameter_values(db_map, (("my_class", "my_object", "my_parameter", 99.0),))
            import_functions.import_metadata(db_map, ('{"title": "My metadata."}',))
            import_functions.import_object_metadata(db_map, (("my_class", "my_object", '{"title": "My metadata."}'),))
            import_functions.import_object_parameter_value_metadata(
                db_map, (("my_class", "my_object", "my_parameter", '{"title": "My metadata."}'),)
            )
            db_map.commit_session("Add test data.")
            parameter_value_metadata = db_map.query(db_map.parameter_value_metadata_sq).all()
            self.assertEqual(len(parameter_value_metadata), 1)
            db_map.remove_items("parameter_value_metadata", parameter_value_metadata[0].id)
            db_map.commit_session("Remove test data.")
            self.assertEqual(len(db_map.query(db_map.metadata_sq).all()), 1)
            self.assertEqual(len(db_map.query(db_map.entity_metadata_sq).all()), 1)
            self.assertEqual(len(db_map.query(db_map.parameter_value_metadata_sq).all()), 0)

    def test_cascade_remove_object_removes_its_metadata(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            import_functions.import_objects(db_map, (("my_class", "my_object"),))
            import_functions.import_metadata(db_map, ('{"title": "My metadata."}',))
            import_functions.import_object_metadata(db_map, (("my_class", "my_object", '{"title": "My metadata."}'),))
            db_map.commit_session("Add test data.")
            db_map.remove_items("object", 1)
            db_map.remove_unused_metadata()
            db_map.commit_session("Remove test data.")
            self.assertEqual(len(db_map.query(db_map.metadata_sq).all()), 0)
            self.assertEqual(len(db_map.query(db_map.entity_metadata_sq).all()), 0)
            self.assertEqual(len(db_map.query(db_map.object_sq).all()), 0)

    def test_cascade_remove_relationship_removes_its_metadata(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_object_class",))
            import_functions.import_objects(db_map, (("my_object_class", "my_object"),))
            import_functions.import_relationship_classes(db_map, (("my_class", ("my_object_class",)),))
            import_functions.import_relationships(db_map, (("my_class", ("my_object",)),))
            import_functions.import_metadata(db_map, ('{"title": "My metadata."}',))
            import_functions.import_relationship_metadata(
                db_map, (("my_class", ("my_object",), '{"title": "My metadata."}'),)
            )
            db_map.commit_session("Add test data.")
            db_map.remove_items("relationship", 2)
            db_map.remove_unused_metadata()
            db_map.commit_session("Remove test data.")
            self.assertEqual(len(db_map.query(db_map.metadata_sq).all()), 0)
            self.assertEqual(len(db_map.query(db_map.entity_metadata_sq).all()), 0)
            self.assertEqual(len(db_map.query(db_map.relationship_sq).all()), 0)

    def test_cascade_remove_parameter_value_removes_its_metadata(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            import_functions.import_objects(db_map, (("my_class", "my_object"),))
            import_functions.import_object_parameters(db_map, (("my_class", "my_parameter"),))
            import_functions.import_object_parameter_values(db_map, (("my_class", "my_object", "my_parameter", 99.0),))
            import_functions.import_metadata(db_map, ('{"title": "My metadata."}',))
            import_functions.import_object_parameter_value_metadata(
                db_map, (("my_class", "my_object", "my_parameter", '{"title": "My metadata."}'),)
            )
            db_map.commit_session("Add test data.")
            db_map.remove_items("parameter_value", 1)
            db_map.remove_unused_metadata()
            db_map.commit_session("Remove test data.")
            self.assertEqual(len(db_map.query(db_map.metadata_sq).all()), 0)
            self.assertEqual(len(db_map.query(db_map.entity_metadata_sq).all()), 0)
            self.assertEqual(len(db_map.query(db_map.parameter_value_sq).all()), 0)

    def test_remove_works_when_entity_groups_are_present(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            import_functions.import_objects(db_map, (("my_class", "my_object"),))
            import_functions.import_objects(db_map, (("my_class", "my_group"),))
            import_functions.import_object_groups(db_map, (("my_class", "my_group", "my_object"),))
            db_map.commit_session("Add test data.")
            db_map.remove_items("object", 1)  # This shouldn't raise an exception
            db_map.commit_session("Remove object.")
            objects = db_map.query(db_map.object_sq).all()
            self.assertEqual(len(objects), 1)
            self.assertEqual(objects[0].name, "my_group")

    def test_remove_object_class2(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            db_map.commit_session("Add test data.")
            my_class = db_map.query(db_map.object_class_sq).one_or_none()
            self.assertIsNotNone(my_class)
            db_map.remove_items("object_class", my_class.id)
            db_map.commit_session("Remove object class.")
            my_class = db_map.query(db_map.object_class_sq).one_or_none()
            self.assertIsNone(my_class)

    def test_remove_relationship_class2(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            import_functions.import_relationship_classes(db_map, (("my_relationship_class", ("my_class",)),))
            db_map.commit_session("Add test data.")
            my_class = db_map.query(db_map.relationship_class_sq).one_or_none()
            self.assertIsNotNone(my_class)
            db_map.remove_items("relationship_class", my_class.id)
            db_map.commit_session("Remove relationship class.")
            my_class = db_map.query(db_map.relationship_class_sq).one_or_none()
            self.assertIsNone(my_class)

    def test_remove_object2(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            import_functions.import_objects(db_map, (("my_class", "my_object"),))
            db_map.commit_session("Add test data.")
            my_object = db_map.query(db_map.object_sq).one_or_none()
            self.assertIsNotNone(my_object)
            db_map.remove_items("object", my_object.id)
            db_map.commit_session("Remove object.")
            my_object = db_map.query(db_map.object_sq).one_or_none()
            self.assertIsNone(my_object)

    def test_remove_relationship2(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            import_functions.import_objects(db_map, (("my_class", "my_object"),))
            import_functions.import_relationship_classes(db_map, (("my_relationship_class", ("my_class",)),))
            import_functions.import_relationships(db_map, (("my_relationship_class", ("my_object",)),))
            db_map.commit_session("Add test data.")
            my_relationship = db_map.query(db_map.relationship_sq).one_or_none()
            self.assertIsNotNone(my_relationship)
            db_map.remove_items("relationship", 2)
            db_map.commit_session("Remove relationship.")
            my_relationship = db_map.query(db_map.relationship_sq).one_or_none()
            self.assertIsNone(my_relationship)

    def test_remove_parameter_value2(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            import_functions.import_objects(db_map, (("my_class", "my_object"),))
            import_functions.import_object_parameters(db_map, (("my_class", "my_parameter"),))
            import_functions.import_object_parameter_values(db_map, (("my_class", "my_object", "my_parameter", 23.0),))
            db_map.commit_session("Add test data.")
            my_value = db_map.query(db_map.object_parameter_value_sq).one_or_none()
            self.assertIsNotNone(my_value)
            db_map.remove_items("parameter_value", my_value.id)
            db_map.commit_session("Remove parameter value.")
            my_parameter = db_map.query(db_map.object_parameter_value_sq).one_or_none()
            self.assertIsNone(my_parameter)


class TestDatabaseMappingCommitMixin(AssertSuccessTestCase):
    def test_commit_message(self):
        """Tests that commit comment ends up in the database."""
        with DatabaseMapping("sqlite://", create=True) as db_map:
            db_map.add_items("entity_class", {"name": "testclass"})
            db_map.commit_session("test commit")
            self.assertEqual(db_map.query(db_map.commit_sq).all()[-1].comment, "test commit")

    def test_commit_session_raise_with_empty_comment(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            self.assertRaisesRegex(SpineDBAPIError, "Commit message cannot be empty.", db_map.commit_session, "")

    def test_commit_session_raise_when_nothing_to_commit(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self.assertRaisesRegex(NothingToCommit, "Nothing to commit.", db_map.commit_session, "No changes.")

    def test_rollback_addition(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            db_map.commit_session("test commit")
            import_functions.import_object_classes(db_map, ("second_class",))
            entity_class_names = {x["name"] for x in db_map.mapped_table("entity_class").valid_values()}
            self.assertEqual(entity_class_names, {"my_class", "second_class"})
            db_map.rollback_session()
            entity_class_names = {x["name"] for x in db_map.mapped_table("entity_class").valid_values()}
            self.assertEqual(entity_class_names, {"my_class"})
            with self.assertRaises(NothingToCommit):
                db_map.commit_session("test commit")

    def test_rollback_removal(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            db_map.commit_session("test commit")
            db_map.remove_items("entity_class", 1)
            entity_class_names = {x["name"] for x in db_map.mapped_table("entity_class").valid_values()}
            self.assertEqual(entity_class_names, set())
            db_map.rollback_session()
            entity_class_names = {x["name"] for x in db_map.mapped_table("entity_class").valid_values()}
            self.assertEqual(entity_class_names, {"my_class"})
            with self.assertRaises(NothingToCommit):
                db_map.commit_session("test commit")

    def test_rollback_update(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            db_map.commit_session("test commit")
            db_map.get_item("entity_class", name="my_class").update(name="new_name")
            entity_class_names = {x["name"] for x in db_map.mapped_table("entity_class").valid_values()}
            self.assertEqual(entity_class_names, {"new_name"})
            db_map.rollback_session()
            entity_class_names = {x["name"] for x in db_map.mapped_table("entity_class").valid_values()}
            self.assertEqual(entity_class_names, {"my_class"})
            with self.assertRaises(NothingToCommit):
                db_map.commit_session("test commit")

    def test_rollback_entity_update(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="my_class"))
            self._assert_success(db_map.add_entity_item(name="my_entity", entity_class_name="my_class"))
            db_map.commit_session("test commit")
            db_map.get_item("entity", name="my_entity", entity_class_name="my_class").update(name="new_name")
            entity_names = {x["name"] for x in db_map.mapped_table("entity").valid_values()}
            self.assertEqual(entity_names, {"new_name"})
            db_map.rollback_session()
            entity_names = {x["name"] for x in db_map.mapped_table("entity").valid_values()}
            self.assertEqual(entity_names, {"my_entity"})
            with self.assertRaises(NothingToCommit):
                db_map.commit_session("test commit")

    def test_refresh_addition(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            db_map.commit_session("test commit")
            import_functions.import_object_classes(db_map, ("second_class",))
            entity_class_names = {x["name"] for x in db_map.mapped_table("entity_class").valid_values()}
            self.assertEqual(entity_class_names, {"my_class", "second_class"})
            db_map.refresh_session()
            db_map.fetch_all()
            entity_class_names = {x["name"] for x in db_map.mapped_table("entity_class").valid_values()}
            self.assertEqual(entity_class_names, {"my_class", "second_class"})

    def test_refresh_removal(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            db_map.commit_session("test commit")
            db_map.remove_items("entity_class", 1)
            entity_class_names = {x["name"] for x in db_map.mapped_table("entity_class").valid_values()}
            self.assertEqual(entity_class_names, set())
            db_map.refresh_session()
            db_map.fetch_all()
            entity_class_names = {x["name"] for x in db_map.mapped_table("entity_class").valid_values()}
            self.assertEqual(entity_class_names, set())

    def test_refresh_update(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            db_map.commit_session("test commit")
            db_map.get_item("entity_class", name="my_class").update(name="new_name")
            entity_class_names = {x["name"] for x in db_map.mapped_table("entity_class").valid_values()}
            self.assertEqual(entity_class_names, {"new_name"})
            db_map.refresh_session()
            db_map.fetch_all()
            entity_class_names = {x["name"] for x in db_map.mapped_table("entity_class").valid_values()}
            self.assertEqual(entity_class_names, {"new_name"})

    def test_cascade_remove_unfetched(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            import_functions.import_object_classes(db_map, ("my_class",))
            import_functions.import_objects(db_map, (("my_class", "my_object"),))
            db_map.commit_session("test commit")
            db_map.reset()
            db_map.remove_items("entity_class", 1)
            db_map.commit_session("test commit")
            ents = db_map.query(db_map.entity_sq).all()
            self.assertEqual(ents, [])


def _commit_on_thread(db_map, msg, lock):
    with db_map:
        with lock:
            db_map.commit_session(msg)


class TestDatabaseMappingConcurrent(AssertSuccessTestCase):
    def test_concurrent_commit_threading(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "database.sqlite")
            db_map1 = DatabaseMapping(url, create=True)
            db_map2 = DatabaseMapping(url)
            with db_map1:
                self._assert_success(db_map1.add_entity_class_item(name="dog"))
                self._assert_success(db_map1.add_entity_class_item(name="cat"))
            with db_map2:
                self._assert_success(db_map2.add_entity_class_item(name="cat"))
            db_lock = threading.Lock()
            c1 = threading.Thread(target=_commit_on_thread, args=(db_map1, "one", db_lock))
            c2 = threading.Thread(target=_commit_on_thread, args=(db_map2, "two", db_lock))
            c2.start()
            c1.start()
            c1.join()
            c2.join()
            with DatabaseMapping(url) as db_map:
                commit_msgs = {x.comment for x in db_map.query(db_map.commit_sq)}
                entity_class_names = [x.name for x in db_map.query(db_map.entity_class_sq)]
                self.assertEqual(commit_msgs, {"Create the database", "one", "two"})
                self.assertCountEqual(entity_class_names, ["cat", "dog"])

    def test_uncommitted_mapped_items_take_id_from_externally_committed_items(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "database.sqlite")
            with DatabaseMapping(url, create=True) as db_map1:
                self._assert_success(db_map1.add_entity_class_item(name="widget"))
                self._assert_success(db_map1.add_entity_class_item(name="gadget"))
                with DatabaseMapping(url) as db_map2:
                    # Add the same classes in different order
                    self._assert_success(db_map2.add_entity_class_item(name="gadget"))
                    self._assert_success(db_map2.add_entity_class_item(name="widget"))
                    db_map2.commit_session("No comment")
                    committed_resolved_entity_classes = [x.resolve() for x in db_map2.get_items("entity_class")]
                    committed_resolved_id_by_name = {x["name"]: x["id"] for x in committed_resolved_entity_classes}
                # Verify that the uncommitted classes are now seen as 'committed'
                uncommitted_entity_classes = db_map1.get_items("entity_class")
                uncommitted_resolved_entity_classes = [x.resolve() for x in uncommitted_entity_classes]
                uncommitted_resolved_id_by_name = {x["name"]: x["id"] for x in uncommitted_resolved_entity_classes}
                self.assertEqual(committed_resolved_id_by_name, uncommitted_resolved_id_by_name)
                for mapped_item in uncommitted_entity_classes:
                    self.assertFalse(mapped_item.is_committed())
                    self.assertEqual(mapped_item._mapped_item.status, Status.to_update)
                db_map1.commit_session("Update classes already in database.")

    def test_committed_mapped_items_take_id_from_externally_committed_items(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "database.sqlite")
            with DatabaseMapping(url, create=True) as db_map0:
                # Add widget before gadget
                self._assert_success(db_map0.add_entity_class_item(name="widget"))
                self._assert_success(db_map0.add_entity_class_item(name="gadget"))
                db_map0.commit_session("No comment")
            with DatabaseMapping(url) as db_map1:
                with DatabaseMapping(url) as db_map2:
                    # Purge, then add *gadget* before *widget* (swap the order)
                    # Also add an entity
                    db_map2.purge_items("entity_class")
                    self._assert_success(db_map2.add_entity_class_item(name="gadget"))
                    self._assert_success(db_map2.add_entity_class_item(name="widget"))
                    self._assert_success(db_map2.add_entity_item(entity_class_name="gadget", name="phone"))
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
                        "lat": None,
                        "lon": None,
                        "alt": None,
                        "shape_name": None,
                        "shape_blob": None,
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
                self._assert_success(db_map.remove_item("entity", original_id))
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
                restored_item = self._assert_success(db_map.restore_item("entity", original_id))
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
                db_map.do_fetch_all(db_map.mapped_table("entity"))
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
            with DatabaseMapping(url) as db_map:
                db_map.purge_items("entity_class")
                db_map.commit_session("Purge all")
                with DatabaseMapping(url) as shadow_db_map:
                    self._assert_success(shadow_db_map.add_entity_class_item(name="my_class"))
                    shadow_db_map.commit_session("Add same class")
                entity_class_item = db_map.get_entity_class_item(name="my_class")
                self.assertTrue(entity_class_item)
                self.assertEqual(entity_class_item["name"], "my_class")

    def test_db_items_prevail_with_get_items_if_mapped_items_are_committed(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="my_class"))
                db_map.commit_session("Add some data")
            with DatabaseMapping(url) as db_map:
                db_map.purge_items("entity_class")
                db_map.commit_session("Purge all")
                with DatabaseMapping(url) as shadow_db_map:
                    self._assert_success(shadow_db_map.add_entity_class_item(name="my_class"))
                    shadow_db_map.commit_session("Add same class")
                entity_class_items = db_map.get_entity_class_items()
                self.assertEqual(len(entity_class_items), 1)
                self.assertEqual(entity_class_items[0]["name"], "my_class")

    def test_db_items_prevail_with_item_if_mapped_items_are_committed(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                db_map.add_entity_class(name="my_class")
                db_map.commit_session("Add some data")
            with DatabaseMapping(url) as db_map:
                db_map.purge_items("entity_class")
                db_map.commit_session("Purge all")
                with DatabaseMapping(url) as shadow_db_map:
                    shadow_db_map.add_entity_class(name="my_class")
                    shadow_db_map.commit_session("Add same class")
                entity_class_item = db_map.entity_class(name="my_class")
                self.assertEqual(entity_class_item["name"], "my_class")

    def test_db_items_prevail_with_find_if_mapped_items_are_committed(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                db_map.add_entity_class(name="my_class")
                db_map.commit_session("Add some data")
            with DatabaseMapping(url) as db_map:
                db_map.purge_items("entity_class")
                db_map.commit_session("Purge all")
                with DatabaseMapping(url) as shadow_db_map:
                    shadow_db_map.add_entity_class(name="my_class")
                    shadow_db_map.commit_session("Add same class")
                entity_class_items = db_map.find_entity_classes(name="my_class")
                self.assertEqual(len(entity_class_items), 1)
                self.assertEqual(entity_class_items[0]["name"], "my_class")

    def test_remove_items_then_refresh_then_readd(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="my_class"))
                self._assert_success(db_map.add_entity_class_item(name="new_class"))
                db_map.commit_session("Add some data")
            with DatabaseMapping(url) as db_map:
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
            with DatabaseMapping(url) as db_map:
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

    def test_refresh_after_update(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="Object"))
                value, value_type = to_database(2.3)
                self._assert_success(
                    db_map.add_parameter_definition_item(
                        name="z", entity_class_name="Object", default_value=value, default_type=value_type
                    )
                )
                db_map.commit_session("Add initial data.")
            with DatabaseMapping(url) as db_map:
                db_map.fetch_more("parameter_definition")
                definition = db_map.get_parameter_definition_item(name="z", entity_class_name="Object")
                self.assertEqual(definition["parsed_value"], 2.3)
                with DatabaseMapping(url) as db_map_2:
                    definition = db_map_2.get_parameter_definition_item(name="z", entity_class_name="Object")
                    value, value_type = to_database("yes")
                    item = definition.update(default_value=value, default_type=value_type)
                    self.assertIsNotNone(item)
                    db_map_2.commit_session("Update parameter default value.")
                db_map.refresh_session()
                db_map.fetch_more("parameter_definition")
                definition = db_map.get_parameter_definition_item(name="z", entity_class_name="Object")
                self.assertEqual(definition["parsed_value"], "yes")


if __name__ == "__main__":
    unittest.main()
