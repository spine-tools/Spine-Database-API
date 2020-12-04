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
Provides functions to apply filtering based on scenarios to parameter value subqueries.

:author: Antti Soininen (VTT)
:date:   21.8.2020
"""

from functools import partial
import datetime
from sqlalchemy import desc, func

SCENARIO_FILTER_TYPE = "scenario_filter"
SCENARIO_SHORTHAND_TAG = "scenario"


def apply_scenario_filter_to_parameter_value_sq(db_map, scenario):
    """
    Replaces parameter value subquery properties in ``db_map`` such that they return only values of given scenario.

    Args:
        db_map (DatabaseMappingBase): a database map to alter
        scenario (str or int): scenario name or id
    """
    state = _ScenarioFilterState(db_map, scenario)
    make_parameter_value_sq = partial(_make_scenario_filtered_parameter_value_sq, state=state)
    db_map.override_parameter_value_sq_maker(make_parameter_value_sq)


def apply_full_scenario_filter(db_map, scenario):
    """
    Replaces (i) parameter value subquery properties in ``db_map`` such that they return only values of given scenario,
    and (ii) the ``_create_import_alternative`` method so it creates an import alternative for the given scenario.

    Args:
        db_map (DatabaseMappingBase): a database map to alter
        scenario (str or int): scenario name or id
    """
    state = _ScenarioFilterState(db_map, scenario)
    make_parameter_value_sq = partial(_make_scenario_filtered_parameter_value_sq, state=state)
    db_map.override_parameter_value_sq_maker(make_parameter_value_sq)
    create_import_alternative = partial(_create_import_alternative, state=state)
    db_map.override_create_import_alternative(create_import_alternative)


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
        db_map (DatabaseMappingBase): target database map
        config (dict): scenario filter configuration
    """
    apply_full_scenario_filter(db_map, config["scenario"])


def scenario_name_from_dict(config):
    """
    Returns scenario's name from filter config.

    Args:
        config (dict): scenario filter configuration

    Returns:
        str: scenario name or None if ``config`` is not a valid scenario filter configuration
    """
    if not config["type"] == SCENARIO_FILTER_TYPE:
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
    Internal state for :func:`_make_scenario_filtered_parameter_value_sq` and :func:`_create_import_alternative`

    Attributes:
        original_parameter_value_sq (Alias): previous ``parameter_value_sq``
        scenario_id (int): id of selected scenario
        scenario_name (name): name of selected scenario
    """

    def __init__(self, db_map, scenario):
        """
        Args:
            db_map (DatabaseMappingBase): database the state applies to
            scenario (str or int): scenario name or ids
        """
        self.scenario = scenario
        self.original_parameter_value_sq = db_map.parameter_value_sq
        self.original_create_import_alternative = db_map._create_import_alternative
        self.scenario_id, self.scenario_name = self._scenario_id_and_name(db_map, scenario)
        self._import_alternative_name = None

    @staticmethod
    def _scenario_id_and_name(db_map, scenario):
        """
        Finds id and name for given scenario.

        Args:
            db_map (DatabaseMappingBase): a database map
            scenario (str or int): scenario name or id

        Returns:
            int or NoneType: scenario's id
            str or NoneType: scenario's name
        """
        if isinstance(scenario, str):
            scenario_name = scenario
            scenario_id = (
                db_map.query(db_map.scenario_sq.c.id).filter(db_map.scenario_sq.c.name == scenario_name).scalar()
            )
            return scenario_id, scenario_name
        scenario_id = scenario
        scenario_name = db_map.query(db_map.scenario_sq.c.name).filter(db_map.scenario_sq.c.id == scenario_id).scalar()
        return scenario_id, scenario_name


def _create_import_alternative(db_map, state):
    """
    Creates an alternative to use as default for all import operations on the given db_map.
    Associates the alternative with the scenario from the given state.

    Args:
        db_map (DatabaseMappingBase): database the state applies to
        state (_ScenarioFilterState): a state bound to ``db_map``
    """
    state.scenario_id, state.scenario_name = state._scenario_id_and_name(db_map, state.scenario)
    if state.scenario_name is None:
        state.original_create_import_alternative(db_map)
        return
    if state.scenario_id is None:
        ids = db_map._add_scenarios({"name": state.scenario_name})
        state.scenario_id = next(iter(ids))
    stamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S:%f")
    db_map._import_alternative_name = f"{state.scenario_name}_run@{stamp}"
    ids = db_map._add_alternatives({"name": db_map._import_alternative_name})
    db_map._import_alternative_id = next(iter(ids))
    max_rank = (
        db_map.query(func.max(db_map.scenario_alternative_sq.c.rank))
        .filter(db_map.scenario_alternative_sq.c.scenario_id == state.scenario_id)
        .scalar()
    )
    rank = max_rank + 1 if max_rank else 1
    db_map._add_scenario_alternatives(
        {"scenario_id": state.scenario_id, "alternative_id": db_map._import_alternative_id, "rank": rank}
    )


def _make_scenario_filtered_parameter_value_sq(db_map, state):
    """
    Returns a scenario filtering subquery similar to :func:`DatabaseMappingBase.parameter_value_sq`.

    This function can be used as replacement for parameter value subquery maker in :class:`DatabaseMappingBase`.

    Args:
        db_map (DatabaseMappingBase): a database map
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
            )
            .label("max_rank_row_number"),
        )
        .filter(state.original_parameter_value_sq.c.alternative_id == db_map.scenario_alternative_sq.c.alternative_id)
        .filter(db_map.scenario_alternative_sq.c.scenario_id == state.scenario_id)
    ).subquery()
    return db_map.query(ext_parameter_value_sq).filter(ext_parameter_value_sq.c.max_rank_row_number == 1).subquery()
