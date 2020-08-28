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
Provides :func:`apply_alternative_value_filter` which adds parameter value filtering by
scenarios and alternatives to database maps.

:author: Antti Soininen (VTT)
:date:   21.8.2020
"""
from functools import partial
from sqlalchemy import and_, case, exists, func, literal, literal_column, or_
from sqlalchemy.sql.expression import label
from .exception import SpineDBAPIError


def apply_alternative_value_filter(db_map, scenario, overridden_active_alternatives=None):
    """
    Replaces parameter value subquery properties in ``db_map`` such that they return only values of active alternatives.

    By default, alternatives of active scenarios are used falling back to the Base alternative, if necessary.

    Args:
        db_map (DatabaseMappingBase): a database map to alter
        scenario (str or int, optional): scenario name or id
        overridden_active_alternatives (Iterable of str or int, optional): alternative names or ids;
            must be provided if ``scenario`` is omitted
    """
    state = _FilterState(db_map, scenario, overridden_active_alternatives)
    filtering = partial(_make_filtered_parameter_value_sq, state=state)
    db_map.override_parameter_value_sq_maker(filtering)


class _FilterState:
    """
    Internal state for :func:`_make_filtered_parameter_value_sq`

    Attributes:
        original_parameter_value_sq (Alias): previous ``parameter_value_sq``
        active_scenario (int): id of active scenario
        active_alternatives (NoneType or Iterable of int): ids of alternatives overriding the active scenario
    """

    def __init__(self, db_map, scenario, alternatives):
        """
        Args:
            db_map (DatabaseMappingBase): database the state applies to
            scenario (str or int, optional): scenario name or ids
            alternatives (Iterable of str or int, optional): alternative names of ids;
                must be provided if ``scenario`` is omitted
        """
        if scenario is None and not alternatives:
            raise SpineDBAPIError("Cannot create filter: no scenario or alternatives provided.")
        self.original_parameter_value_sq = db_map.parameter_value_sq
        self.active_scenario = self._scenario_id(db_map, scenario) if scenario is not None else None
        self.active_alternatives = self._alternative_ids(db_map, alternatives) if alternatives is not None else None

    @staticmethod
    def _scenario_id(db_map, scenario):
        """
        Finds id for given scenario and checks its existence.

        Args:
            db_map (DatabaseMappingBase): a database map
            scenario (str or int): scenario name or id

        Returns:
            int: scenario's id
        """
        if isinstance(scenario, str):
            scenario_id = db_map.query(db_map.scenario_sq.c.id).filter(db_map.scenario_sq.c.name == scenario).scalar()
            if scenario_id is None:
                raise SpineDBAPIError(f"Scenario '{scenario}' not found")
            return scenario_id
        id_in_db = db_map.query(db_map.scenario_sq.c.id).filter(db_map.scenario_sq.c.id == scenario).scalar()
        if id_in_db is None:
            raise SpineDBAPIError(f"Scenario id {scenario} not found")
        return scenario

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
        alternative_names = [name for name in alternatives if isinstance(name, str)]
        ids_from_db = (
            db_map.query(db_map.alternative_sq.c.id, db_map.alternative_sq.c.name)
            .filter(db_map.in_(db_map.alternative_sq.c.name, alternative_names))
            .all()
        )
        names_in_db = [i.name for i in ids_from_db]
        if len(alternative_names) != len(names_in_db):
            missing_names = tuple(name for name in alternative_names if name not in names_in_db)
            raise SpineDBAPIError(f"Alternative(s) {missing_names} not found")
        ids = [i.id for i in ids_from_db]
        alternative_ids = [id_ for id_ in alternatives if isinstance(id_, int)]
        ids_from_db = (
            db_map.query(db_map.alternative_sq.c.id)
            .filter(db_map.in_(db_map.alternative_sq.c.id, alternative_ids))
            .all()
        )
        ids_in_db = [i.id for i in ids_from_db]
        if len(alternative_ids) != len(ids_from_db):
            missing_ids = tuple(i for i in alternative_ids if i not in ids_in_db)
            raise SpineDBAPIError(f"Alternative id(s) {missing_ids} not found")
        ids += ids_in_db
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
    if state.active_alternatives is not None:
        subquery = state.original_parameter_value_sq
        return (
            db_map.query(subquery).filter(db_map.in_(subquery.c.alternative_id, state.active_alternatives)).subquery()
        )
    return _parameter_value_sq_active_scenario_filtered(db_map, state)


def _parameter_value_sq_active_scenario_filtered(db_map, state):
    """
    Returns a filtering subquery similar to :func:`DatabaseMappingBase.parameter_value_sq`.

    This function uses the active scenario set in the state.

    Args:
        db_map (DatabaseMappingBase): a database map
        state (_FilterState): a state bound to ``db_map``

    Returns:
        Alias: a subquery for parameter value filtered by active alternatives
    """
    active_alternatives_subquery = (
        db_map.query(db_map.scenario_alternative_sq.c.alternative_id, func.max(db_map.scenario_alternative_sq.c.rank))
        .filter(db_map.scenario_alternative_sq.c.scenario_id == state.active_scenario)
        .group_by(db_map.scenario_alternative_sq.c.scenario_id)
        .subquery()
    )
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
