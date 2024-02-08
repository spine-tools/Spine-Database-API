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
""" Provides functions to apply filtering based on scenarios to subqueries. """

from functools import partial
from sqlalchemy import and_, desc, func, or_
from ..exception import SpineDBAPIError

SCENARIO_FILTER_TYPE = "scenario_filter"
SCENARIO_SHORTHAND_TAG = "scenario"


def apply_scenario_filter_to_subqueries(db_map, scenario):
    """
    Replaces affected subqueries in ``db_map`` such that they return only values of given scenario.

    Args:
        db_map (DatabaseMapping): a database map to alter
        scenario (str or int): scenario name or id
    """
    state = _ScenarioFilterState(db_map, scenario)
    make_entity_element_sq = partial(_make_scenario_filtered_entity_element_sq, state=state)
    db_map.override_entity_element_sq_maker(make_entity_element_sq)
    make_entity_sq = partial(_make_scenario_filtered_entity_sq, state=state)
    db_map.override_entity_sq_maker(make_entity_sq)
    make_parameter_value_sq = partial(_make_scenario_filtered_parameter_value_sq, state=state)
    db_map.override_parameter_value_sq_maker(make_parameter_value_sq)
    make_alternative_sq = partial(_make_scenario_filtered_alternative_sq, state=state)
    db_map.override_alternative_sq_maker(make_alternative_sq)
    make_scenario_sq = partial(_make_scenario_filtered_scenario_sq, state=state)
    db_map.override_scenario_sq_maker(make_scenario_sq)
    make_scenario_alternative_sq = partial(_make_scenario_filtered_scenario_alternative_sq, state=state)
    db_map.override_scenario_alternative_sq_maker(make_scenario_alternative_sq)


def scenario_filter_config(scenario):
    """
    Creates a config dict for scenario filter.

    Args:
        scenario (str): scenario name

    Returns:
        dict: filter configuration
    """
    return {"type": SCENARIO_FILTER_TYPE, "scenario": scenario}


def scenario_filter_from_dict(db_map, config):
    """
    Applies scenario filter to given database map.

    Args:
        db_map (DatabaseMapping): target database map
        config (dict): scenario filter configuration
    """
    apply_scenario_filter_to_subqueries(db_map, config["scenario"])


def scenario_name_from_dict(config):
    """
    Returns scenario's name from filter config.

    Args:
        config (dict): scenario filter configuration

    Returns:
        str: scenario name or None if ``config`` is not a valid scenario filter configuration
    """
    if config["type"] != SCENARIO_FILTER_TYPE:
        return None
    return config["scenario"]


def scenario_filter_config_to_shorthand(config):
    """
    Makes a shorthand string from scenario filter configuration.

    Args:
        config (dict): scenario filter configuration

    Returns:
        str: a shorthand string
    """
    return SCENARIO_SHORTHAND_TAG + ":" + config["scenario"]


def scenario_filter_shorthand_to_config(shorthand):
    """
    Makes configuration dictionary out of a shorthand string.

    Args:
        shorthand (str): a shorthand string

    Returns:
        dict: scenario filter configuration
    """
    _, _, scenario = shorthand.partition(":")
    return scenario_filter_config(scenario)


class _ScenarioFilterState:
    """
    Internal state for :func:`_make_scenario_filtered_parameter_value_sq`.

    Attributes:
        original_entity_sq (Alias): previous ``entity_sq``
        original_alternative_sq (Alias): previous ``alternative_sq``
        original_parameter_value_sq (Alias): previous ``parameter_value_sq``
        original_scenario_alternative_sq (Alias): previous ``scenario_alternative_sq``
        original_scenario_sq (Alias): previous ``scenario_sq``
        scenario_alternative_ids (list of int): ids of selected scenario's alternatives
        scenario_id (int): id of selected scenario
    """

    def __init__(self, db_map, scenario):
        """
        Args:
            db_map (DatabaseMapping): database the state applies to
            scenario (str or int): scenario name or ids
        """
        self.original_entity_sq = db_map.entity_sq
        self.original_entity_element_sq = db_map.entity_element_sq
        self.original_parameter_value_sq = db_map.parameter_value_sq
        self.original_scenario_sq = db_map.scenario_sq
        self.original_scenario_alternative_sq = db_map.scenario_alternative_sq
        self.original_alternative_sq = db_map.alternative_sq
        self.scenario_id = self._scenario_id(db_map, scenario)
        self.scenario_alternative_ids, self.alternative_ids = self._scenario_alternative_ids(db_map)

    @staticmethod
    def _scenario_id(db_map, scenario):
        """
        Finds id for given scenario.

        Args:
            db_map (DatabaseMapping): a database map
            scenario (str or int): scenario name or id

        Returns:
            int: scenario's id
        """
        if isinstance(scenario, str):
            scenario_name = scenario
            scenario_id = (
                db_map.query(db_map.scenario_sq.c.id).filter(db_map.scenario_sq.c.name == scenario_name).scalar()
            )
            if scenario_id is None:
                raise SpineDBAPIError(f"Scenario '{scenario_name}' not found.")
            return scenario_id
        scenario_id = scenario
        scenario = db_map.query(db_map.scenario_sq).filter(db_map.scenario_sq.c.id == scenario_id).one_or_none()
        if scenario is None:
            raise SpineDBAPIError(f"Scenario id {scenario_id} not found.")
        return scenario_id

    def _scenario_alternative_ids(self, db_map):
        """
        Finds scenario alternative and alternative ids of current scenario.

        Args:
            db_map (DatabaseMapping): a database map

        Returns:
            tuple: scenario alternative ids and alternative ids
        """
        alternative_ids = []
        scenario_alternative_ids = []
        for row in db_map.query(db_map.scenario_alternative_sq).filter(
            db_map.scenario_alternative_sq.c.scenario_id == self.scenario_id
        ):
            scenario_alternative_ids.append(row.id)
            alternative_ids.append(row.alternative_id)
        return scenario_alternative_ids, alternative_ids


def _ext_entity_sq(db_map, state):
    return (
        db_map.query(
            state.original_entity_sq,
            func.row_number()
            .over(
                partition_by=[state.original_entity_sq.c.id],
                order_by=desc(db_map.scenario_alternative_sq.c.rank),
            )
            .label("desc_rank_row_number"),
            db_map.entity_alternative_sq.c.active,
            db_map.entity_class_sq.c.active_by_default,
            db_map.scenario_alternative_sq.c.scenario_id,
        )
        .outerjoin(
            db_map.entity_alternative_sq, state.original_entity_sq.c.id == db_map.entity_alternative_sq.c.entity_id
        )
        .outerjoin(db_map.entity_class_sq, state.original_entity_sq.c.class_id == db_map.entity_class_sq.c.id)
        .outerjoin(
            db_map.scenario_alternative_sq,
            db_map.entity_alternative_sq.c.alternative_id == db_map.scenario_alternative_sq.c.alternative_id,
        )
        .filter(
            or_(
                db_map.scenario_alternative_sq.c.scenario_id == None,
                db_map.scenario_alternative_sq.c.scenario_id == state.scenario_id,
            )
        )
        .filter(
            or_(
                db_map.entity_alternative_sq.c.alternative_id == None,
                db_map.entity_alternative_sq.c.alternative_id == db_map.scenario_alternative_sq.c.alternative_id,
            )
        )
    ).subquery()


def _make_scenario_filtered_entity_element_sq(db_map, state):
    """Returns a scenario filtering subquery similar to :func:`DatabaseMapping.entity_element_sq`.

    This function can be used as replacement for entity_element subquery maker in :class:`DatabaseMapping`.

    Args:
        db_map (DatabaseMapping): a database map
        state (_ScenarioFilterState): a state bound to ``db_map``

    Returns:
        Alias: a subquery for entity_element filtered by selected scenario
    """
    ext_entity_sq = _ext_entity_sq(db_map, state)
    entity_sq = ext_entity_sq.alias()
    element_sq = ext_entity_sq.alias()
    return (
        db_map.query(state.original_entity_element_sq)
        .filter(state.original_entity_element_sq.c.entity_id == entity_sq.c.id)
        .filter(state.original_entity_element_sq.c.element_id == element_sq.c.id)
        .filter(
            entity_sq.c.desc_rank_row_number == 1,
            or_(entity_sq.c.active == True, entity_sq.c.active == None),
        )
        .filter(
            element_sq.c.desc_rank_row_number == 1,
            or_(element_sq.c.active == True, and_(element_sq.c.active == None, element_sq.c.active_by_default == True)),
        )
        .subquery()
    )


def _make_scenario_filtered_entity_sq(db_map, state):
    """Returns a scenario filtering subquery similar to :func:`DatabaseMapping.entity_sq`.

    This function can be used as replacement for entity subquery maker in :class:`DatabaseMapping`.

    Args:
        db_map (DatabaseMapping): a database map
        state (_ScenarioFilterState): a state bound to ``db_map``

    Returns:
        Alias: a subquery for entity filtered by selected scenario
    """
    ext_entity_sq = _ext_entity_sq(db_map, state)
    ext_entity_element_count_sq = (
        db_map.query(
            db_map.entity_element_sq.c.entity_id,
            func.count(db_map.entity_element_sq.c.element_id).label("element_count"),
        )
        .group_by(db_map.entity_element_sq.c.entity_id)
        .subquery()
    )
    ext_entity_class_dimension_count_sq = (
        db_map.query(
            db_map.entity_class_dimension_sq.c.entity_class_id,
            func.count(db_map.entity_class_dimension_sq.c.dimension_id).label("dimension_count"),
        )
        .group_by(db_map.entity_class_dimension_sq.c.entity_class_id)
        .subquery()
    )
    return (
        db_map.query(
            ext_entity_sq.c.id,
            ext_entity_sq.c.class_id,
            ext_entity_sq.c.name,
            ext_entity_sq.c.description,
            ext_entity_sq.c.commit_id,
        )
        .filter(
            ext_entity_sq.c.desc_rank_row_number == 1,
            or_(
                ext_entity_sq.c.active == True,
                and_(ext_entity_sq.c.active == None, ext_entity_sq.c.active_by_default == True),
            ),
        )
        .outerjoin(
            ext_entity_element_count_sq,
            ext_entity_element_count_sq.c.entity_id == ext_entity_sq.c.id,
        )
        .outerjoin(
            ext_entity_class_dimension_count_sq,
            ext_entity_class_dimension_count_sq.c.entity_class_id == ext_entity_sq.c.class_id,
        )
        .filter(
            or_(
                and_(
                    ext_entity_element_count_sq.c.element_count == None,
                    ext_entity_class_dimension_count_sq.c.dimension_count == None,
                ),
                ext_entity_element_count_sq.c.element_count == ext_entity_class_dimension_count_sq.c.dimension_count,
            )
        )
        .subquery()
    )


def _make_scenario_filtered_parameter_value_sq(db_map, state):
    """
    Returns a scenario filtering subquery similar to :func:`DatabaseMapping.parameter_value_sq`.

    This function can be used as replacement for parameter value subquery maker in :class:`DatabaseMapping`.

    Args:
        db_map (DatabaseMapping): a database map
        state (_ScenarioFilterState): a state bound to ``db_map``

    Returns:
        Alias: a subquery for parameter value filtered by selected scenario
    """
    ext_parameter_value_sq = (
        db_map.query(
            state.original_parameter_value_sq,
            func.row_number()
            .over(
                partition_by=[
                    state.original_parameter_value_sq.c.parameter_definition_id,
                    state.original_parameter_value_sq.c.entity_id,
                ],
                order_by=desc(db_map.scenario_alternative_sq.c.rank),
            )  # the one with the highest rank will have row_number equal to 1, so it will 'win' in the filter below
            .label("desc_rank_row_number"),
        )
        .filter(state.original_parameter_value_sq.c.alternative_id == db_map.scenario_alternative_sq.c.alternative_id)
        .filter(db_map.scenario_alternative_sq.c.scenario_id == state.scenario_id)
    ).subquery()
    return db_map.query(ext_parameter_value_sq).filter(ext_parameter_value_sq.c.desc_rank_row_number == 1).subquery()


def _make_scenario_filtered_alternative_sq(db_map, state):
    """
    Returns an alternative filtering subquery similar to :func:`DatabaseMapping.alternative_sq`.

    This function can be used as replacement for alternative subquery maker in :class:`DatabaseMapping`.

    Args:
        db_map (DatabaseMapping): a database map
        state (_ScenarioFilterState): a state bound to ``db_map``

    Returns:
        Alias: a subquery for alternative filtered by selected scenario
    """
    alternative_sq = state.original_alternative_sq
    return db_map.query(alternative_sq).filter(alternative_sq.c.id.in_(state.alternative_ids)).subquery()


def _make_scenario_filtered_scenario_sq(db_map, state):
    """
    Returns a scenario filtering subquery similar to :func:`DatabaseMapping.scenario_sq`.

    This function can be used as replacement for scenario subquery maker in :class:`DatabaseMapping`.

    Args:
        db_map (DatabaseMapping): a database map
        state (_ScenarioFilterState): a state bound to ``db_map``

    Returns:
        Alias: a subquery for scenario filtered by selected scenario
    """
    scenario_sq = state.original_scenario_sq
    return db_map.query(scenario_sq).filter(scenario_sq.c.id == state.scenario_id).subquery()


def _make_scenario_filtered_scenario_alternative_sq(db_map, state):
    """
    Returns a scenario alternative filtering subquery similar to :func:`DatabaseMapping.scenario_alternative_sq`.

    This function can be used as replacement for scenario alternative subquery maker in :class:`DatabaseMapping`.

    Args:
        db_map (DatabaseMapping): a database map
        state (_ScenarioFilterState): a state bound to ``db_map``

    Returns:
        Alias: a subquery for scenario alternative filtered by selected scenario
    """
    scenario_alternative_sq = state.original_scenario_alternative_sq
    return (
        db_map.query(scenario_alternative_sq)
        .filter(scenario_alternative_sq.c.id.in_(state.scenario_alternative_ids))
        .subquery()
    )
