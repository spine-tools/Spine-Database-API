######################################################################################################################
# Copyright (C) 2017 - 2020 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
"""
Contains the :func:`apply_filter_stack` function.

:author: Antti Soininen (VTT)
:date:   6.10.2020
"""
from json import load
from .alternative_filter import apply_alternative_filter_to_parameter_value_sq
from .renamer import apply_renaming_to_entity_class_sq
from .scenario_filter import apply_scenario_filter_to_parameter_value_sq
from .tool_filter import apply_tool_filter_to_entity_sq
from .url_tools import pop_filter_configs


def apply_filter_stack(db_map, stack):
    """
    Applies stack of filters and manipulator to given database map.

    Args:
        db_map (DatabaseMappingBase): a database map
        stack (list): a stack of database filters and manipulators
    """
    for filter_ in stack:
        type_ = filter_["type"]
        if type_ == "alternative_filter":
            apply_alternative_filter_to_parameter_value_sq(db_map, filter_["alternatives"])
        elif type_ == "renamer":
            apply_renaming_to_entity_class_sq(db_map, filter_["name_map"])
        elif type_ == "scenario_filter":
            apply_scenario_filter_to_parameter_value_sq(db_map, filter_["scenario"])
        elif type_ == "tool_filter":
            apply_tool_filter_to_entity_sq(db_map, filter_["tool"])


def load_filters(filter_configs):
    """
    Loads filter configurations from disk and constructs a filter stack.

    Args:
        filter_configs (list of str): paths to filter configuration files

    Returns:
        list of dict: filter stack
    """
    stack = list()
    for path in filter_configs:
        with open(path) as config_file:
            stack.append(load(config_file))
    return stack
