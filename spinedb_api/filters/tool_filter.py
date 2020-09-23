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
from sqlalchemy import and_, or_, case
from ..exception import SpineDBAPIError


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
        Finds id for given tool and checks its existence.

        Args:
            db_map (DatabaseMappingBase): a database map
            tool (str or int): tool name or id

        Returns:
            int: tool's id
        """
        if isinstance(tool, str):
            tool_id = db_map.query(db_map.tool_sq.c.id).filter(db_map.tool_sq.c.name == tool).scalar()
            if tool_id is None:
                raise SpineDBAPIError(f"Tool '{tool}' not found")
            return tool_id
        tool_id = tool
        id_in_db = db_map.query(db_map.tool_sq.c.id).filter(db_map.tool_sq.c.id == tool_id).scalar()
        if id_in_db is None:
            raise SpineDBAPIError(f"Tool id {tool_id} not found")
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
        [(or_(tool_feature_method_sq.c.required.is_(False), parameter_value_sq.c.value.isnot(None),), True,)],
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
    tool_feature_method_sq = _make_ext_tool_feature_method_sq(db_map, state)
    parameter_value_sq = db_map._subquery("parameter_value")

    method_filter = _make_method_filter(tool_feature_method_sq, parameter_value_sq)
    required_filter = _make_required_filter(tool_feature_method_sq, parameter_value_sq)

    rejected_entity_sq = (
        db_map.query(state.original_entity_sq.c.id,)
        .select_from(db_map.parameter_definition_sq)
        .filter(db_map.parameter_definition_sq.c.entity_class_id == state.original_entity_sq.c.class_id)
        .outerjoin(
            parameter_value_sq,
            and_(
                parameter_value_sq.c.parameter_definition_id == db_map.parameter_definition_sq.c.id,
                parameter_value_sq.c.entity_id == state.original_entity_sq.c.id,
            ),
        )
        .filter(tool_feature_method_sq.c.parameter_definition_id == db_map.parameter_definition_sq.c.id)
        .filter(tool_feature_method_sq.c.tool_id == state.tool_id)
        .filter(or_(method_filter.is_(False), required_filter.is_(False)))
    ).subquery()

    return (
        db_map.query(state.original_entity_sq)
        .filter(~state.original_entity_sq.c.id.in_(db_map.query(rejected_entity_sq.c.id).distinct()))
        .subquery()
    )
