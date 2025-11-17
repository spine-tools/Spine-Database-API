######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Database API contributors
# This file is part of Spine Database API.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
import pytest
from sqlalchemy import create_engine, desc, text
from spinedb_api import DatabaseMapping, SpineDBAPIError, append_filter_config
from spinedb_api.filters.alternative_filter import alternative_filter_config
from spinedb_api.filters.renamer import entity_class_renamer_config
from spinedb_api.mapped_item_status import Status


def test_add_new_entity(tmp_path):
    url = "sqlite:///" + str(tmp_path / "db.sqlite")
    with DatabaseMapping(url, create=True) as db_map:
        db_map.add_alternative(name="other")
        db_map.add_entity_class(name="cat")
        db_map.commit_session("Add structure.")
    filtered_url = append_filter_config(url, alternative_filter_config(["other"]))
    with DatabaseMapping(filtered_url) as db_map:
        tom = db_map.add_entity(name="Tom", entity_class_name="cat")
        assert db_map.commit_session("Add Tom the first time.") == ([], [])
        assert tom.mapped_item.status == Status.committed
        assert tom["id"].db_id is not None
        latest_commit = db_map.query(db_map.commit_sq).order_by(desc(db_map.commit_sq.c.date)).first()
        assert tom["commit_id"] == latest_commit.id
    with DatabaseMapping(url) as db_map:
        tom = db_map.entity(name="Tom", entity_class_name="cat")
        assert tom["commit_id"] == latest_commit.id


def test_add_stuff_thats_been_filtered_out(tmp_path):
    url = "sqlite:///" + str(tmp_path / "db.sqlite")
    with DatabaseMapping(url, create=True) as db_map:
        db_map.add_entity_class(name="cat")
        tom = db_map.add_entity(name="Tom", entity_class_name="cat")
        db_map.add_alternative(name="other")
        db_map.add_entity_alternative(
            alternative_name="other", entity_byname=("Tom",), entity_class_name="cat", active=False
        )
        db_map.commit_session("Add Tom.")
        id_of_tom = tom["id"].db_id
        commit_id_of_tom = tom["commit_id"]
    filtered_url = append_filter_config(url, alternative_filter_config(["other"]))
    with DatabaseMapping(filtered_url) as db_map:
        tom = db_map.add_entity(name="Tom", entity_class_name="cat")
        assert db_map.commit_session("Add Tom again.") == ([], [])
        assert tom.mapped_item.status == Status.committed
        assert tom["id"].db_id == id_of_tom
        assert tom["commit_id"] == commit_id_of_tom


def test_add_stuff_that_has_been_renamed_and_doesnt_show_in_commit_log(tmp_path):
    url = "sqlite:///" + str(tmp_path / "db.sqlite")
    with DatabaseMapping(url, create=True) as db_map:
        cat = db_map.add_entity_class(name="cat")
        db_map.commit_session("Add cat class.")
        cat_id = cat["id"].db_id
    filtered_url = append_filter_config(url, entity_class_renamer_config(cat="kitty"))
    with DatabaseMapping(filtered_url) as db_map:
        cat = db_map.add_entity_class(name="cat")
        assert db_map.commit_session("Add cat again.") == ([], [])
        assert cat.mapped_item.status == Status.committed
        assert cat["id"].db_id == cat_id


def test_add_relationship_to_filtered_database(tmp_path):
    url = "sqlite:///" + str(tmp_path / "db.sqlite")
    with DatabaseMapping(url, create=True) as db_map:
        db_map.add_alternative(name="other")
        db_map.add_entity_class(name="cat")
        db_map.add_entity(name="Tom", entity_class_name="cat")
        db_map.add_entity_class(name="fish")
        db_map.add_entity(name="Nemo", entity_class_name="fish")
        db_map.commit_session("Add test data.")
    filtered_url = append_filter_config(url, alternative_filter_config(["other"]))
    with DatabaseMapping(filtered_url) as db_map:
        db_map.add_entity_class(dimension_name_list=("fish", "cat"))
        db_map.add_entity(entity_class_name="fish__cat", element_name_list=("Nemo", "Tom"))
        db_map.commit_session("Add relationship between hidden Nemo and Tom.")
    with DatabaseMapping(url) as db_map:
        nemo__tom = db_map.entity(entity_class_name="fish__cat", entity_byname=("Nemo", "Tom"))
        assert nemo__tom["element_name_list"] == ("Nemo", "Tom")


def test_update_non_filtered_entity(tmp_path):
    url = "sqlite:///" + str(tmp_path / "db.sqlite")
    with DatabaseMapping(url, create=True) as db_map:
        db_map.add_entity_class(name="cat")
        tom = db_map.add_entity(name="Tom", entity_class_name="cat")
        db_map.add_alternative(name="other")
        db_map.add_entity_alternative(
            alternative_name="other", entity_byname=("Tom",), entity_class_name="cat", active=True
        )
        db_map.commit_session("Add Tom.")
        id_of_tom = tom["id"].db_id
    filtered_url = append_filter_config(url, alternative_filter_config(["other"]))
    with DatabaseMapping(filtered_url) as db_map:
        tom = db_map.entity(name="Tom", entity_class_name="cat")
        tom.update(name="Bigglesworth")
        assert db_map.commit_session("Rename Tom.") == ([], [])
        assert tom.mapped_item.status == Status.committed
        assert tom["id"].db_id == id_of_tom
        latest_commit = db_map.query(db_map.commit_sq).order_by(desc(db_map.commit_sq.c.date)).first()
        assert tom["commit_id"] == latest_commit.id
    with DatabaseMapping(url) as db_map:
        bigglesworth = db_map.entity(name="Bigglesworth", entity_class_name="cat")
        assert bigglesworth["commit_id"] == latest_commit.id


def test_update_non_filtered_entitys_location(tmp_path):
    url = "sqlite:///" + str(tmp_path / "db.sqlite")
    with DatabaseMapping(url, create=True) as db_map:
        db_map.add_entity_class(name="cat")
        tom = db_map.add_entity(name="Tom", entity_class_name="cat")
        db_map.add_alternative(name="other")
        db_map.add_entity_alternative(
            alternative_name="other", entity_byname=("Tom",), entity_class_name="cat", active=True
        )
        db_map.commit_session("Add Tom.")
        id_of_tom = tom["id"].db_id
        commit_id_of_tom = tom["commit_id"]
    filtered_url = append_filter_config(url, alternative_filter_config(["other"]))
    with DatabaseMapping(filtered_url) as db_map:
        tom = db_map.entity(name="Tom", entity_class_name="cat")
        tom.update(lat=0.5, lon=0.23)
        assert db_map.commit_session("Place Tom.") == ([], [])
        assert tom.mapped_item.status == Status.committed
        assert tom["id"].db_id == id_of_tom
        assert tom["commit_id"] == commit_id_of_tom
    with DatabaseMapping(url) as db_map:
        tom = db_map.entity(name="Tom", entity_class_name="cat")
        assert tom["lat"] == 0.5
        assert tom["lon"] == 0.23


def test_rename_entity_to_something_that_has_been_filtered_out(tmp_path):
    url = "sqlite:///" + str(tmp_path / "db.sqlite")
    with DatabaseMapping(url, create=True) as db_map:
        db_map.add_entity_class(name="cat")
        db_map.add_entity(name="Tom", entity_class_name="cat")
        db_map.add_entity(name="Bigglesworth", entity_class_name="cat")
        db_map.add_alternative(name="other")
        db_map.add_entity_alternative(
            alternative_name="other", entity_byname=("Tom",), entity_class_name="cat", active=False
        )
        db_map.commit_session("Add Tom.")
    filtered_url = append_filter_config(url, alternative_filter_config(["other"]))
    with DatabaseMapping(filtered_url) as db_map:
        bigglesworth = db_map.entity(name="Bigglesworth", entity_class_name="cat")
        bigglesworth.update(name="Tom")
        with pytest.raises(
            SpineDBAPIError, match="^there's already a entity with \{'entity_class_name': 'cat', 'name': 'Tom'\}$"
        ):
            db_map.commit_session("Rename Bigglesworth.")
        assert bigglesworth.mapped_item.status == Status.to_update


def test_removing_entity_removes_its_relationships_in_cascade(tmp_path):
    url = "sqlite:///" + str(tmp_path / "db.sqlite")
    with DatabaseMapping(url, create=True) as db_map:
        db_map.add_alternative(name="other")
        db_map.add_entity_class(name="cat")
        db_map.add_entity(name="Tom", entity_class_name="cat")
        db_map.add_entity_class(name="fish")
        db_map.add_entity(name="Nemo", entity_class_name="fish")
        db_map.add_entity_alternative(
            entity_class_name="fish", entity_byname=("Nemo",), alternative_name="other", active=False
        )
        db_map.add_entity_class(dimension_name_list=("fish", "cat"))
        db_map.add_entity(entity_class_name="fish__cat", element_name_list=("Nemo", "Tom"))
        db_map.commit_session("Add test data.")
    filtered_url = append_filter_config(url, alternative_filter_config(["other"]))
    with DatabaseMapping(filtered_url) as db_map:
        tom = db_map.entity(entity_class_name="cat", name="Tom")
        tom.remove()
        db_map.commit_session("Remove Tom.")
        assert not tom.is_valid()
        assert tom.mapped_item.status == Status.committed
    engine = create_engine(url)
    with engine.connect() as connection:
        entity_names = {record.name for record in connection.execute(text("select name from entity"))}
        assert entity_names == {"Nemo"}
        entity_elements = connection.execute(text("select entity_id from entity_element")).all()
        assert entity_elements == []
