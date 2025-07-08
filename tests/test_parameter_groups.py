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


def test_create_group():
    with DatabaseMapping("sqlite://", create=True) as db_map:
        group = db_map.add_parameter_group(name="my group", background_color="ABC123")
        assert group["name"] == "my group"
        assert group["background_color"] == "ABC123"


def test_invalid_background_color_code_raises():
    with DatabaseMapping("sqlite://", create=True) as db_map:
        with pytest.raises(SpineDBAPIError, match="^invalid background_color for parameter_group$"):
            db_map.add_parameter_group(name="my group", background_color="XXX")


def test_associated_parameter_definition_to_group():
    with DatabaseMapping("sqlite://", create=True) as db_map:
        group = db_map.add_parameter_group(name="priority params", background_color="fafabc")
        db_map.add_entity_class(name="Gadget")
        definition = db_map.add_parameter_definition(
            entity_class_name="Gadget", name="Q", parameter_group_name="priority params"
        )
        assert definition["parameter_group_id"] == group["id"]
