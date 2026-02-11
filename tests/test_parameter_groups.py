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
import pytest
from spinedb_api import DatabaseMapping, SpineDBAPIError
from spinedb_api.export_functions import export_parameter_groups
from spinedb_api.import_functions import import_parameter_groups
from spinedb_api.mapped_item_status import Status
from tests.mock_helpers import assert_imports


def test_create_group():
    with DatabaseMapping("sqlite://", create=True) as db_map:
        group = db_map.add_parameter_group(name="my group", color="ABC123", priority=23)
        assert group["name"] == "my group"
        assert group["color"] == "ABC123"
        assert group["priority"] == 23


def test_invalid_background_color_code_raises():
    with DatabaseMapping("sqlite://", create=True) as db_map:
        with pytest.raises(SpineDBAPIError, match="^invalid color for parameter_group$"):
            db_map.add_parameter_group(name="my group", color="XXX", priority=0)


def test_missing_color_raises():
    with DatabaseMapping("sqlite://", create=True) as db_map:
        with pytest.raises(SpineDBAPIError, match="^missing color"):
            db_map.add_parameter_group(name="my group", priority=23)


def test_missing_priority_raises():
    with DatabaseMapping("sqlite://", create=True) as db_map:
        with pytest.raises(SpineDBAPIError, match="^missing priority$"):
            db_map.add_parameter_group(name="my group", color="090807")


def test_associated_parameter_definition_to_group():
    with DatabaseMapping("sqlite://", create=True) as db_map:
        group = db_map.add_parameter_group(name="priority params", color="fafabc", priority=5)
        db_map.add_entity_class(name="Gadget")
        definition = db_map.add_parameter_definition(
            entity_class_name="Gadget", name="Q", parameter_group_name="priority params"
        )
        assert definition["parameter_group_id"] == group["id"]


def test_set_group_for_parameter_definition():
    with DatabaseMapping("sqlite://", create=True) as db_map:
        db_map.add_parameter_group(name="Important", color="deadaf", priority=5)
        db_map.add_entity_class(name="Gadget")
        opacity = db_map.add_parameter_definition(entity_class_name="Gadget", name="opacity")
        assert opacity["parameter_group_name"] is None
        opacity.update(parameter_group_name="Important")
        assert opacity["parameter_group_name"] == "Important"


def test_parameter_group_id_in_parameter_value():
    with DatabaseMapping("sqlite://", create=True) as db_map:
        extras = db_map.add_parameter_group(name="Extras", color="beefaf", priority=9)
        db_map.add_entity_class(name="Gadget")
        db_map.add_entity(entity_class_name="Gadget", name="watch")
        db_map.add_parameter_definition(entity_class_name="Gadget", name="price", parameter_group_name="Extras")
        watch_price = db_map.add_parameter_value(
            entity_class_name="Gadget",
            entity_byname=("watch",),
            parameter_definition_name="price",
            alternative_name="Base",
            parsed_value="too high",
        )
        assert watch_price["parameter_group_id"] == extras["id"]


def test_removing_parameter_group_removes_referring_definition():
    with DatabaseMapping("sqlite://", create=True) as db_map:
        important = db_map.add_parameter_group(name="Important", color="effecd", priority=5)
        db_map.add_entity_class(name="Gadget")
        price = db_map.add_parameter_definition(
            entity_class_name="Gadget", name="price", parameter_group_name="Important"
        )
        db_map.commit_session("Add test data.")
        important.remove()
        assert not price.is_valid()
        db_map.commit_session("Remove group")
        assert db_map.query(db_map.parameter_definition_sq).all() == []


def test_restore_removed_parameter_group_restores_referring_definition():
    with DatabaseMapping("sqlite://", create=True) as db_map:
        important = db_map.add_parameter_group(name="Important", color="effecd", priority=5)
        db_map.add_entity_class(name="Gadget")
        price = db_map.add_parameter_definition(
            entity_class_name="Gadget", name="price", parameter_group_name="Important"
        )
        db_map.commit_session("Add test data.")
        important.remove()
        assert not price.is_valid()
        important.restore()
        assert price.is_valid()
        assert price["parameter_group_name"] == important["name"]
        assert price.mapped_item.status == Status.committed


def test_import_parameter_group():
    with DatabaseMapping("sqlite://", create=True) as db_map:
        count = assert_imports(import_parameter_groups(db_map, [("My group", "010203", 5)]))
        assert count == 1
        groups = db_map.find_parameter_groups()
        assert len(groups) == 1
        assert groups[0]["name"] == "My group"
        assert groups[0]["color"] == "010203"
        assert groups[0]["priority"] == 5


def test_export_parameter_group():
    with DatabaseMapping("sqlite://", create=True) as db_map:
        db_map.add_parameter_group(name="My group", color="010203", priority=5)
        exported = export_parameter_groups(db_map)
    with DatabaseMapping("sqlite://", create=True) as db_map:
        count = assert_imports(import_parameter_groups(db_map, exported))
        assert count == 1
        groups = db_map.find_parameter_groups()
        assert len(groups) == 1
        assert groups[0]["name"] == "My group"
        assert groups[0]["color"] == "010203"
        assert groups[0]["priority"] == 5


def test_fetch_parameter_definitions_with_groups(tmp_path):
    url = "sqlite:///" + str(tmp_path / "test.db")
    with DatabaseMapping(url, create=True) as db_map:
        db_map.add_parameter_group(name="My group", color="010203", priority=5)
        db_map.add_entity_class(name="Widget")
        db_map.add_parameter_definition(entity_class_name="Widget", name="Widget", parameter_group_name="My group")
        db_map.commit_session("Add test data.")
    with DatabaseMapping(url) as db_map:
        definitions = db_map.find_parameter_definitions()
        assert len(definitions) == 1
        assert definitions[0]["parameter_group_name"] == "My group"
