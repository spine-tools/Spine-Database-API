######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
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

"""
from functools import partial
from uuid import uuid4
from sqlalchemy import and_, or_, case, func, Table, Column, ForeignKey
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
        table (Table): temporary table containing cached entity ids that passed the filter
    """

    def __init__(self, db_map, tool):
        """
        Args:
            db_map (DatabaseMappingBase): database the state applies to
            tool (str or int): tool name or id
        """
        self.original_entity_sq = db_map.entity_sq
        tool_id = self._tool_id(db_map, tool)
        table_name = "tool_filter_cache_" + uuid4().hex
        column = Column("entity_id", ForeignKey("entity.id"))
        self.table = db_map.make_temporary_table(table_name, column)
        statement = self.table.insert().from_select(["entity_id"], self.active_entity_id_sq(db_map, tool_id))
        db_map.connection.execute(statement)

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

    @staticmethod
    def active_entity_id_sq(db_map, tool_id):
        """
        Creates a subquery that returns entity ids that pass the tool filter.

        Args:
            db_map (DatabaseMappingBase): database mapping
            tool_id (int): tool identifier

        Returns:
            Alias: subquery
        """
        tool_feature_method_sq = _make_ext_tool_feature_method_sq(db_map, tool_id)

        method_filter = _make_method_filter(
            tool_feature_method_sq, db_map.parameter_value_sq, db_map.parameter_definition_sq
        )
        required_filter = _make_required_filter(tool_feature_method_sq, db_map.parameter_value_sq)

        return (
            db_map.query(db_map.entity_sq.c.id)
            .outerjoin(
                db_map.parameter_definition_sq,
                db_map.parameter_definition_sq.c.entity_class_id == db_map.entity_sq.c.class_id,
            )
            .outerjoin(
                db_map.parameter_value_sq,
                and_(
                    db_map.parameter_value_sq.c.parameter_definition_id == db_map.parameter_definition_sq.c.id,
                    db_map.parameter_value_sq.c.entity_id == db_map.entity_sq.c.id,
                ),
            )
            .outerjoin(
                tool_feature_method_sq,
                tool_feature_method_sq.c.parameter_definition_id == db_map.parameter_definition_sq.c.id,
            )
            .group_by(db_map.entity_sq.c.id)
            .having(and_(func.min(method_filter).is_(True), func.min(required_filter).is_(True)))
        ).subquery()


def _make_ext_tool_feature_method_sq(db_map, tool_id):
    """
    Returns an extended tool_feature_method subquery that has ``None`` whenever no method is specified.
    Used by ``_make_tool_filtered_entity_sq``

    Args:
        db_map (DatabaseMappingBase): a database map
        tool_id (int): tool id

    Returns:
        Alias: a subquery for tool_feature_method
    """
    return (
        db_map.query(
            db_map.ext_tool_feature_sq.c.tool_id,
            db_map.ext_tool_feature_sq.c.parameter_definition_id,
            db_map.ext_tool_feature_sq.c.required,
            db_map.list_value_sq.c.id.label("method_list_value_id"),
        )
        .outerjoin(
            db_map.tool_feature_method_sq,
            db_map.tool_feature_method_sq.c.tool_feature_id == db_map.ext_tool_feature_sq.c.id,
        )
        .outerjoin(
            db_map.list_value_sq,
            and_(
                db_map.ext_tool_feature_sq.c.parameter_value_list_id == db_map.list_value_sq.c.parameter_value_list_id,
                db_map.tool_feature_method_sq.c.method_index == db_map.list_value_sq.c.index,
            ),
        )
        .filter(db_map.ext_tool_feature_sq.c.tool_id == tool_id)
    ).subquery()


def _make_method_filter(tool_feature_method_sq, parameter_value_sq, parameter_definition_sq):
    # Filter passes if either:
    # 1) parameter definition is not a feature for the tool
    # 2) method is not specified
    # 3) value is equal to method
    # 4) value is not specified, but default value is equal to method
    return case(
        [
            (
                or_(
                    tool_feature_method_sq.c.parameter_definition_id.is_(None),
                    tool_feature_method_sq.c.method_list_value_id.is_(None),
                    parameter_value_sq.c.list_value_id == tool_feature_method_sq.c.method_list_value_id,
                    and_(
                        parameter_value_sq.c.value.is_(None),
                        parameter_definition_sq.c.list_value_id == tool_feature_method_sq.c.method_list_value_id,
                    ),
                ),
                True,
            )
        ],
        else_=False,
    )


def _make_required_filter(tool_feature_method_sq, parameter_value_sq):
    # Filter passes if either:
    # 1) parameter definition is not a feature for the tool
    # 2) value is specified
    # 3) method is not required
    return case(
        [
            (
                or_(
                    tool_feature_method_sq.c.parameter_definition_id.is_(None),
                    parameter_value_sq.c.value.isnot(None),
                    tool_feature_method_sq.c.required.is_(False),
                ),
                True,
            )
        ],
        else_=False,
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
    return (
        db_map.query(state.original_entity_sq)
        .join(state.table, state.original_entity_sq.c.id == state.table.c.entity_id)
        .subquery()
    )
