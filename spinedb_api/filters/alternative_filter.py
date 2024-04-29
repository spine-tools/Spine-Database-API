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
Provides functions to apply filtering based on alternatives to parameter value subqueries.

"""
from functools import partial
from ..exception import SpineDBAPIError


ALTERNATIVE_FILTER_TYPE = "alternative_filter"
ALTERNATIVE_FILTER_SHORTHAND_TAG = "alternatives"


def apply_alternative_filter_to_parameter_value_sq(db_map, alternatives):
    """
    Replaces parameter value subquery properties in ``db_map`` such that they return only values of given alternatives.

    Args:
        db_map (DatabaseMapping): a database map to alter
        alternatives (Iterable of str or int, optional): alternative names or ids;
    """
    state = _AlternativeFilterState(db_map, alternatives)
    filtering = partial(_make_alternative_filtered_parameter_value_sq, state=state)
    db_map.override_parameter_value_sq_maker(filtering)


def alternative_filter_config(alternatives):
    """
    Creates a config dict for alternative filter.

    Args:
        alternatives (Iterable of str): alternative names

    Returns:
        dict: filter configuration
    """
    return {"type": ALTERNATIVE_FILTER_TYPE, "alternatives": list(alternatives)}


def alternative_filter_from_dict(db_map, config):
    """
    Applies alternative filter to given database map.

    Args:
        db_map (DatabaseMapping): target database map
        config (dict): alternative filter configuration
    """
    apply_alternative_filter_to_parameter_value_sq(db_map, config["alternatives"])


def alternative_filter_config_to_shorthand(config):
    """
    Makes a shorthand string from alternative filter configuration.

    Args:
        config (dict): alternative filter configuration

    Returns:
        str: a shorthand string
    """
    shorthand = ""
    for alternative in config["alternatives"]:
        shorthand = shorthand + f":'{alternative}'"
    return ALTERNATIVE_FILTER_SHORTHAND_TAG + shorthand


def alternative_names_from_dict(config):
    """
    Returns alternatives' names from filter config.

    Args:
        config (dict): alternative filter configuration

    Returns:
        list: list of alternative names or None if ``config`` is not a valid alternative filter configuration
    """
    if not config["type"] == ALTERNATIVE_FILTER_TYPE:
        return None
    return config["alternatives"]


def alternative_filter_shorthand_to_config(shorthand):
    """
    Makes configuration dictionary out of a shorthand string.

    Args:
        shorthand (str): a shorthand string

    Returns:
        dict: alternative filter configuration
    """
    _filter_type, _separator, tokens = shorthand.partition(":'")
    alternatives = tokens.split("':'")
    alternatives[-1] = alternatives[-1][:-1]
    return alternative_filter_config(alternatives)


class _AlternativeFilterState:
    """
    Internal state for :func:`_make_alternative_filtered_parameter_value_sq`

    Attributes:
        original_parameter_value_sq (Alias): previous ``parameter_value_sq``
        alternatives (Iterable of int): ids of alternatives
    """

    def __init__(self, db_map, alternatives):
        """
        Args:
            db_map (DatabaseMapping): database the state applies to
            alternatives (Iterable of str or int): alternative names or ids;
        """
        self.original_parameter_value_sq = db_map.parameter_value_sq
        self.alternatives = self._alternative_ids(db_map, alternatives) if alternatives is not None else None

    @staticmethod
    def _alternative_ids(db_map, alternatives):
        """
        Finds ids for given alternatives.

        Args:
            db_map (DatabaseMapping): a database map
            alternatives (Iterable): alternative names or ids

        Returns:
            list of int: alternative ids
        """
        alternative_names = [name for name in alternatives if isinstance(name, str)]
        ids_from_db = (
            db_map.query(db_map.alternative_sq.c.id, db_map.alternative_sq.c.name)
            .filter(db_map.alternative_sq.c.name.in_(alternative_names))
            .all()
        )
        names_in_db = [i.name for i in ids_from_db]
        if len(alternative_names) != len(names_in_db):
            missing_names = tuple(name for name in alternative_names if name not in names_in_db)
            raise SpineDBAPIError(f"Alternative(s) {missing_names} not found")
        ids = [i.id for i in ids_from_db]
        alternative_ids = [id_ for id_ in alternatives if isinstance(id_, int)]
        ids_from_db = (
            db_map.query(db_map.alternative_sq.c.id).filter(db_map.alternative_sq.c.id.in_(alternative_ids)).all()
        )
        ids_in_db = [i.id for i in ids_from_db]
        if len(alternative_ids) != len(ids_from_db):
            missing_ids = tuple(i for i in alternative_ids if i not in ids_in_db)
            raise SpineDBAPIError(f"Alternative id(s) {missing_ids} not found")
        ids += ids_in_db
        return ids


def _make_alternative_filtered_parameter_value_sq(db_map, state):
    """
    Returns an alternative filtering subquery similar to :func:`DatabaseMapping.parameter_value_sq`.

    This function can be used as replacement for parameter value subquery maker in :class:`DatabaseMapping`.

    Args:
        db_map (DatabaseMapping): a database map
        state (_AlternativeFilterState): a state bound to ``db_map``

    Returns:
        Alias: a subquery for parameter value filtered by selected alternatives
    """
    subquery = state.original_parameter_value_sq
    return db_map.query(subquery).filter(subquery.c.alternative_id.in_(state.alternatives)).subquery()
