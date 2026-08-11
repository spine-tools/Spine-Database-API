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
def test_add_or_update_when_setting_parameter_type_list_to_none(db_map):
    db_map.add_entity_class(name="Gadget")
    definition = db_map.add_or_update_parameter_definition(
        entity_class_name="Gadget", name="X", parameter_type_list=None
    )
    assert definition["parameter_type_list"] == ()


def test_rolling_back_group_update_succeeds(db_map):
    db_map.add_entity_class(name="A")
    definition = db_map.add_parameter_definition(entity_class_name="A", name="P")
    db_map.add_parameter_group(name="important", color="FAAAAC", priority=1)
    db_map.commit_session("Add parameter")
    definition.update(parameter_group_name="important")
    db_map.rollback_session()
    assert definition["parameter_group_name"] is None


def test_rolling_back_value_list_update_succeeds(db_map):
    db_map.add_entity_class(name="A")
    definition = db_map.add_parameter_definition(entity_class_name="A", name="P")
    db_map.add_parameter_value_list(name="options")
    db_map.commit_session("Add parameter")
    definition.update(parameter_value_list_name="options")
    db_map.rollback_session()
    assert definition["parameter_value_list_name"] is None
