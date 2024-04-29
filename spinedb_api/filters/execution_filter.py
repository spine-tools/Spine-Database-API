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

"""
Provides functions to apply filtering based on scenarios to parameter value subqueries.

"""

import json
from functools import partial
from sqlalchemy import func
from ..exception import SpineDBAPIError

EXECUTION_FILTER_TYPE = "execution_filter"
EXECUTION_SHORTHAND_TAG = "execution"


def apply_execution_filter(db_map, execution):
    """
    Replaces the import alternative in ``db_map`` with a dedicated alternative for an execution.

    Args:
        db_map (DatabaseMapping): a database map to alter
        execution (dict): execution descriptor
    """
    state = _ExecutionFilterState(db_map, execution)
    create_import_alternative = partial(_create_import_alternative, state=state)
    db_map.override_create_import_alternative(create_import_alternative)


def execution_filter_config(execution):
    """
    Creates a config dict for execution filter.

    Args:
        execution (dict): execution descriptor

    Returns:
        dict: filter configuration
    """
    return {"type": EXECUTION_FILTER_TYPE, "execution": execution}


def execution_filter_from_dict(db_map, config):
    """
    Applies execution filter to given database map.

    Args:
        db_map (DatabaseMapping): target database map
        config (dict): execution filter configuration
    """
    apply_execution_filter(db_map, config["execution"])


def execution_descriptor_from_dict(config):
    """
    Returns execution descriptor from filter config.

    Args:
        config (dict): execution filter configuration

    Returns:
        dict: execution descriptor or None if ``config`` is not a valid execution filter configuration
    """
    if config["type"] != EXECUTION_FILTER_TYPE:
        return None
    return config["execution"]


def execution_filter_config_to_shorthand(config):
    """
    Makes a shorthand string from execution filter configuration.

    Args:
        config (dict): execution filter configuration

    Returns:
        str: a shorthand string
    """
    return EXECUTION_SHORTHAND_TAG + ":" + json.dumps(config["execution"])


def execution_filter_shorthand_to_config(shorthand):
    """
    Makes configuration dictionary out of a shorthand string.

    Args:
        shorthand (str): a shorthand string

    Returns:
        dict: execution filter configuration
    """
    _, _, execution = shorthand.partition(":")
    return execution_filter_config(json.loads(execution))


class _ExecutionFilterState:
    """
    Internal state for :func:`_create_import_alternative`

    Attributes:
        original_create_import_alternative (MethodType): previous ``_create_import_alternative``
        execution_item (str): the item that performs the execution
        scenarios (list of str): scenarios involved in the execution
        timestamp (str): timestamp of execution
    """

    def __init__(self, db_map, execution):
        """
        Args:
            db_map (DatabaseMapping): database the state applies to
            execution (dict): execution descriptor
        """
        self.original_create_import_alternative = db_map._create_import_alternative
        self.execution_item, self.scenarios, self.timestamp = self._parse_execution_descriptor(execution)

    @staticmethod
    def _parse_execution_descriptor(execution):
        """Parses data from execution descriptor.

        Args:
            execution (dict): execution descriptor

        Returns:
            tuple: execution item name, list of scenario names, timestamp string

        Raises:
            SpineDBAPIError: raised when execution descriptor is invalid
        """
        try:
            execution_item = execution["execution_item"]
            scenarios = execution["scenarios"]
            timestamp = execution["timestamp"]
        except KeyError as e:
            raise SpineDBAPIError(f"Key '{e}' not found in execution filter descriptor.") from e
        if not isinstance(scenarios, list):
            raise SpineDBAPIError("Key 'scenarios' should contain a list.")
        return execution_item, scenarios, timestamp


def _create_import_alternative(db_map, state):
    """
    Creates an alternative to use as default for all import operations on the given db_map.

    Args:
        db_map (DatabaseMapping): database the state applies to
        state (_ExecutionFilterState): a state bound to ``db_map``
    """
    execution_item = state.execution_item
    scenarios = state.scenarios
    timestamp = state.timestamp
    sep = "__" if scenarios else ""
    db_map._import_alternative_name = f"{'_'.join(scenarios)}{sep}{execution_item}@{timestamp}"
    db_map.add_item("alternative", name=db_map._import_alternative_name)
    for scen_name in scenarios:
        db_map.add_item("scenario", name=scen_name)
    for scen_name in scenarios:
        scen = db_map.get_item("scenario", name=scen_name)
        rank = len(scen["sorted_scenario_alternatives"]) + 1  # ranks are 1-based
        db_map.add_item(
            "scenario_alternative", scenario_name=scen_name, alternative_name=db_map._import_alternative_name, rank=rank
        )
