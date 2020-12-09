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
Provides functions to apply filtering based on tools to entity subqueries.

:author: M. Marin (KTH)
:date:   23.9.2020
"""
from functools import partial
from sqlalchemy import and_, or_, case, func
from ..exception import SpineDBAPIError


TOOL_FILTER_TYPE = "tool_filter"
TOOL_SHORTHAND_TAG = "tool"


def apply_tool_filter_to_entity_sq(db_map, tool):
    """
    Replaces entity subquery properties in ``db_map`` such that they return only values of given tool.
    
    Args:
        db_map (DatabaseMappingBase): a database map to alter
        tool (str or int): tool name or id
    """
    state = _ToolFilterState(db_map, tool)
    filtering = partial(_make_tool_filtered_entity_sq, state=state)
    db_map.override_entity_sq_maker(filtering)


def tool_filter_config(tool):
    """
    Creates a config dict for tool filter.

    Args:
        tool (str): tool name

    Returns:
        dict: filter configuration
    """
    return {"type": TOOL_FILTER_TYPE, "tool": tool}


def tool_filter_from_dict(db_map, config):
    """
    Applies tool filter to given database map.

    Args:
        db_map (DatabaseMappingBase): target database map
        config (dict): tool filter configuration
    """
    apply_tool_filter_to_entity_sq(db_map, config["tool"])


def tool_name_from_dict(config):
    """
    Returns tool's name from filter config.

    Args:
        config (dict): tool filter configuration

    Returns:
        str: tool name or None if ``config`` is not a valid tool filter configuration
    """
    if config["type"] != TOOL_FILTER_TYPE:
        return None
    return config["tool"]


def tool_filter_config_to_shorthand(config):
    """
    Makes a shorthand string from tool filter configuration.

    Args:
        config (dict): tool filter configuration

    Returns:
        str: a shorthand string
    """
    return TOOL_SHORTHAND_TAG + ":" + config["tool"]


def tool_filter_shorthand_to_config(shorthand):
    """
    Makes configuration dictionary out of a shorthand string.

    Args:
        shorthand (str): a shorthand string

    Returns:
        dict: tool filter configuration
    """
    _, _, tool = shorthand.partition(":")
    return tool_filter_config(tool)


class _ToolFilterState:
    """
    Internal state for :func:`_make_tool_filtered_entity_sq`

    Attributes:
        original_entity_sq (Alias): previous ``entity_sq``
        tool_id (int): id of selected tool
    """

    def __init__(self, db_map, tool):
        """
        Args:
            db_map (DatabaseMappingBase): database the state applies to
            tool (str or int): tool name or id
        """
        self.original_entity_sq = db_map.entity_sq
        self.tool_id = self._tool_id(db_map, tool)

    @staticmethod
    def _tool_id(db_map, tool):
        """
        Finds id for given tool.

        Args:
            db_map (DatabaseMappingBase): a database map
            tool (str or int): tool name or id

        Returns:
            int or NoneType: tool id
        """
        if isinstance(tool, str):
            tool_name = tool
            tool_id = db_map.query(db_map.tool_sq.c.id).filter(db_map.tool_sq.c.name == tool_name).scalar()
            if tool_id is None:
                raise SpineDBAPIError(f"Tool '{tool_name}' not found.")
            return tool_id
        tool_id = tool
        tool = db_map.query(db_map.tool_sq).filter(db_map.tool_sq.c.id == tool_id).one_or_none()
        if tool is None:
            raise SpineDBAPIError(f"Tool id {tool_id} not found.")
        return tool_id


def _make_ext_tool_feature_method_sq(db_map, state):
    """
    Returns an extended tool_feature_method subquery that has ``None`` whenever no method is specified.
    Used by ``_make_tool_filtered_entity_sq``

    Args:
        db_map (DatabaseMappingBase): a database map
        state (_ScenarioFilterState): a state bound to ``db_map``

    Returns:
        Alias: a subquery for tool_feature_method
    """
    return (
        db_map.query(
            db_map.ext_tool_feature_sq.c.tool_id,
            db_map.ext_tool_feature_sq.c.parameter_definition_id,
            db_map.ext_tool_feature_sq.c.required,
            db_map.parameter_value_list_sq.c.value.label("method"),
        )
        .outerjoin(
            db_map.tool_feature_method_sq,
            db_map.tool_feature_method_sq.c.tool_feature_id == db_map.ext_tool_feature_sq.c.id,
        )
        .outerjoin(
            db_map.parameter_value_list_sq,
            and_(
                db_map.ext_tool_feature_sq.c.parameter_value_list_id == db_map.parameter_value_list_sq.c.id,
                db_map.tool_feature_method_sq.c.method_index == db_map.parameter_value_list_sq.c.value_index,
            ),
        )
        .filter(db_map.ext_tool_feature_sq.c.tool_id == state.tool_id)
    ).subquery()


def _make_method_filter(tool_feature_method_sq, parameter_value_sq):
    return case(
        [
            (
                or_(
                    tool_feature_method_sq.c.method.is_(None),
                    parameter_value_sq.c.value == tool_feature_method_sq.c.method,
                ),
                True,
            )
        ],
        else_=False,
    )


def _make_required_filter(tool_feature_method_sq, parameter_value_sq):
    return case(
        [(or_(tool_feature_method_sq.c.required.is_(False), parameter_value_sq.c.value.isnot(None)), True)], else_=False
    )


def _make_tool_filtered_entity_sq(db_map, state):
    """
    Returns a tool filtering subquery similar to :func:`DatabaseMappingBase.entity_sq`.

    This function can be used as replacement for entity subquery maker in :class:`DatabaseMappingBase`.

    Args:
        db_map (DatabaseMappingBase): a database map
        state (_ScenarioFilterState): a state bound to ``db_map``

    Returns:
        Alias: a subquery for entity filtered by selected tool
    """
    tool_feature_method_sq = _make_ext_tool_feature_method_sq(db_map, state)
    parameter_value_sq = db_map._subquery("parameter_value")

    method_filter = _make_method_filter(tool_feature_method_sq, parameter_value_sq)
    required_filter = _make_required_filter(tool_feature_method_sq, parameter_value_sq)

    entity_filter_sq = (
        db_map.query(
            state.original_entity_sq.c.id,
            func.min(method_filter).label("method_filter"),
            func.min(required_filter).label("required_filter"),
        )
        .select_from(tool_feature_method_sq)
        .filter(tool_feature_method_sq.c.parameter_definition_id == db_map.parameter_definition_sq.c.id)
        .filter(db_map.parameter_definition_sq.c.entity_class_id == state.original_entity_sq.c.class_id)
        .outerjoin(
            parameter_value_sq,
            and_(
                parameter_value_sq.c.parameter_definition_id == db_map.parameter_definition_sq.c.id,
                parameter_value_sq.c.entity_id == state.original_entity_sq.c.id,
            ),
        )
        .group_by(state.original_entity_sq.c.id)
    ).subquery()

    accepted_entity_sq = (
        db_map.query(entity_filter_sq.c.id)
        .filter(entity_filter_sq.c.method_filter.is_(True))
        .filter(entity_filter_sq.c.required_filter.is_(True))
    ).subquery()

    return (
        db_map.query(state.original_entity_sq)
        .filter(state.original_entity_sq.c.id.in_(db_map.query(accepted_entity_sq.c.id).distinct()))
        .subquery()
    )
