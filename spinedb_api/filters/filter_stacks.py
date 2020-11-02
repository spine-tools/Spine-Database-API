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
Contains functions to deal with filter stacks.

:author: Antti Soininen (VTT)
:date:   6.10.2020
"""
from json import load
from .alternative_filter import alternative_filter_from_dict, ALTERNATIVE_FILTER_TYPE
from .renamer import (
    entity_class_renamer_from_dict,
    ENTITY_CLASS_RENAMER_TYPE,
    parameter_renamer_from_dict,
    PARAMETER_RENAMER_TYPE,
)
from .scenario_filter import scenario_filter_from_dict, SCENARIO_FILTER_TYPE
from .tool_filter import tool_filter_from_dict, TOOL_FILTER_TYPE


def apply_filter_stack(db_map, stack):
    """
    Applies stack of filters and manipulator to given database map.

    Args:
        db_map (DatabaseMappingBase): a database map
        stack (list): a stack of database filters and manipulators
    """
    appliers = {
        ALTERNATIVE_FILTER_TYPE: alternative_filter_from_dict,
        ENTITY_CLASS_RENAMER_TYPE: entity_class_renamer_from_dict,
        PARAMETER_RENAMER_TYPE: parameter_renamer_from_dict,
        SCENARIO_FILTER_TYPE: scenario_filter_from_dict,
        TOOL_FILTER_TYPE: tool_filter_from_dict,
    }
    for filter_ in stack:
        appliers[filter_["type"]](db_map, filter_)


def load_filters(filter_configs):
    """
    Loads filter configurations from disk as needed and constructs a filter stack.

    Args:
        filter_configs (list): list of filter config dicts and paths to filter configuration files

    Returns:
        list of dict: filter stack
    """
    stack = list()
    for config in filter_configs:
        if isinstance(config, str):
            with open(config) as config_file:
                stack.append(load(config_file))
        else:
            stack.append(config)
    return stack
