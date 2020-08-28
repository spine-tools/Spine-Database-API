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
Provides functions to add parameter value filtering by scenarios and alternatives to database maps.

:author: Antti Soininen (VTT)
:date:   21.8.2020
"""
from functools import partial
from sqlalchemy import and_, case, exists, func, literal, literal_column, or_
from sqlalchemy.sql.expression import label
from .exception import SpineDBAPIError


def apply_alternative_value_filter(db_map, overridden_active_scenarios=None, overridden_active_alternatives=None):
    """
    Replaces parameter value subquery properties in ``db_map`` such that they return only values of active alternatives.

    By default, alternatives of active scenarios are used falling back to the Base alternative, if necessary.

    Args:
        db_map (DatabaseMappingBase): a database map to alter
        overridden_active_scenarios (Iterable of str or int, optional): scenario names or ids;
            overrides active scenarios in the database
        overridden_active_alternatives (Iterable of str or int, optional): alternative names or ids;
            overrides active scenarios in the database
    """
    state = _FilterState(db_map, overridden_active_scenarios, overridden_active_alternatives)
    filtering = partial(_make_filtered_parameter_value_sq, state=state)
    db_map.set_parameter_value_sq_maker(filtering)


class _FilterState:
    """Internal state for :func:`_make_filtered_parameter_value_sq`"""

    def __init__(self, db_map, scenarios, alternatives):
        """
        Args:
            db_map (DatabaseMappingBase): database the state applies to
            scenarios (Iterable of str or int, optional): scenario names or ids;
                overrides active scenarios in the database.
            alternatives (Iterable of str or int, optional): alternative names of ids;
                overrides active scenarios in the database
        """
        self.original_parameter_value_sq = db_map.parameter_value_sq
        self.active_scenarios = self._scenario_ids(db_map, scenarios) if scenarios is not None else None
        self.active_alternatives = self._alternative_ids(db_map, alternatives) if alternatives is not None else None

    @staticmethod
    def _scenario_ids(db_map, scenarios):
        """
        Finds ids for given scenarios.

        Args:
            db_map (DatabaseMappingBase): a database map
            scenarios (Iterable): scenario names or ids

        Returns:
            list of int: scenario_ids
        """
        names = {name for name in scenarios if isinstance(name, str)}
        names_in_db = (
            db_map.query(db_map.scenario_sq.c.id, db_map.scenario_sq.c.name)
            .filter(db_map.in_(db_map.scenario_sq.c.name, names))
            .all()
        )
        if len(names_in_db) != len(names):
            missing_names = tuple(name for name in names if name not in [i.name for i in names_in_db])
            raise SpineDBAPIError(f"Scenario(s) {missing_names} not found")
        ids = [i.id for i in names_in_db]
        given_ids = {id_ for id_ in scenarios if isinstance(id_, int)}
        ids_in_db = db_map.query(db_map.scenario_sq.c.id).filter(db_map.in_(db_map.scenario_sq.c.id, given_ids)).all()
        if len(ids_in_db) != len(given_ids):
            missing_ids = tuple(id_ for id_ in given_ids if id_ not in [i.id for i in ids_in_db])
            raise SpineDBAPIError(f"Scenario id(s) {missing_ids} not found")
        ids += [i.id for i in ids_in_db]
        return ids

    @staticmethod
    def _alternative_ids(db_map, alternatives):
        """
        Finds ids for given alternatives.

        Args:
            db_map (DatabaseMappingBase): a database map
            alternatives (Iterable): alternative names or ids

        Returns:
            list of int: alternative ids
        """
        ids = list()
        for alternative in alternatives:
            if isinstance(alternative, int):
                exists = (
                    db_map.query(db_map.alternative_sq.c.id).filter(db_map.alternative_sq.c.id == alternative).scalar()
                )
                if exists is None:
                    raise SpineDBAPIError(f"Alternative id {alternative} not found")
                ids.append(alternative)
            else:
                id_ = (
                    db_map.query(db_map.alternative_sq.c.id)
                    .filter(db_map.alternative_sq.c.name == alternative)
                    .scalar()
                )
                if id_ is None:
                    raise SpineDBAPIError(f"Alternative {alternative} not found")
                ids.append(id_)
        return ids


def _make_filtered_parameter_value_sq(db_map, state):
    """
    Returns a filtering subquery similar to :func:`DatabaseMappingBase.parameter_value_sq`.

    This function can be used as replacement for parameter value subquery maker in :class:`DatabaseMappingBase`.

    Args:
        db_map (DatabaseMappingBase): a database map
        state (_FilterState): a state bound to ``db_map``

    Returns:
        Alias: a subquery for parameter value filtered by active alternatives
    """
    if state.active_scenarios is not None:
        return _parameter_value_sq_overridden_scenario_filtered(db_map, state)
    if state.active_alternatives is not None:
        subquery = state.original_parameter_value_sq
        return db_map.query(subquery).filter(db_map.in_(subquery.c.alternative_id, state.active_alternatives)).subquery()
    return _parameter_value_sq_active_scenario_filtered(db_map, state)


def _parameter_value_sq_overridden_scenario_filtered(db_map, state):
    """
    Returns a filtering subquery similar to :func:`DatabaseMappingBase.parameter_value_sq`.

    This function uses the scenarios in ``state`` to override the active scenarios in the database.

    Args:
        db_map (DatabaseMappingBase): a database map
        state (_FilterState): a state bound to ``db_map``

    Returns:
        Alias: a subquery for parameter value filtered by active alternatives
    """
    active_alternatives_subquery = (
        db_map.query(db_map.scenario_alternative_sq.c.alternative_id, func.max(db_map.scenario_alternative_sq.c.rank))
        .filter(db_map.in_(db_map.scenario_alternative_sq.c.scenario_id, state.active_scenarios))
        .group_by(db_map.scenario_alternative_sq.c.scenario_id)
        .subquery()
    )
    return _alternative_filtered_parameter_value_sq(db_map, active_alternatives_subquery, state)


def _parameter_value_sq_active_scenario_filtered(db_map, state):
    """
    Returns a filtering subquery similar to :func:`DatabaseMappingBase.parameter_value_sq`.

    This function uses the active scenarios in the database.

    Args:
        db_map (DatabaseMappingBase): a database map
        state (_FilterState): a state bound to ``db_map``

    Returns:
        Alias: a subquery for parameter value filtered by active alternatives
    """
    active_scenarios_subquery = (
        db_map.query(db_map.scenario_sq.c.id).filter(db_map.scenario_sq.c.active == True).subquery()
    )
    active_alternatives_subquery = (
        db_map.query(db_map.scenario_alternative_sq.c.alternative_id, func.max(db_map.scenario_alternative_sq.c.rank))
        .filter(db_map.scenario_alternative_sq.c.scenario_id == active_scenarios_subquery.c.id)
        .group_by(db_map.scenario_alternative_sq.c.scenario_id)
        .subquery()
    )
    return _alternative_filtered_parameter_value_sq(db_map, active_alternatives_subquery, state)


def _alternative_filtered_parameter_value_sq(db_map, active_alternatives_subquery, state):
    """
    Returns a filtering subquery similar to :func:`DatabaseMappingBase.parameter_value_sq`.

    Args:
        db_map (DatabaseMappingBase): a database map
        active_alternatives_subquery (Alias): a subquery for active alternatives
        state (_FilterState): a state bound to ``db_map``

    Returns:
        Alias: a subquery for parameter value filtered by active alternatives
    """
    # Here we assume that Base alternative id = 1.
    which_alternative = case(
        [
            (
                func.count(active_alternatives_subquery.c.alternative_id) != 0,
                active_alternatives_subquery.c.alternative_id,
            )
        ],
        else_=1,
    )
    selected_alternatives_subquery = db_map.query(label("alternative_id", which_alternative)).subquery()
    value_subquery = state.original_parameter_value_sq
    selected_and_base_suqbuery = (
        db_map.query(value_subquery.c.parameter_definition_id, value_subquery.c.alternative_id)
        .filter(
            or_(
                value_subquery.c.alternative_id == 1,
                value_subquery.c.alternative_id == selected_alternatives_subquery.c.alternative_id,
            )
        )
        .subquery()
    )
    max_alternative_id_subquery = (
        db_map.query(
            selected_and_base_suqbuery.c.parameter_definition_id,
            func.max(selected_and_base_suqbuery.c.alternative_id).label("alternative_id"),
        )
        .group_by(selected_and_base_suqbuery.c.parameter_definition_id)
        .subquery()
    )
    filtered_suqbquery = (
        db_map.query(value_subquery)
        .filter(
            and_(
                value_subquery.c.parameter_definition_id == max_alternative_id_subquery.c.parameter_definition_id,
                value_subquery.c.alternative_id == max_alternative_id_subquery.c.alternative_id,
            )
        )
        .subquery()
    )
    return filtered_suqbquery
