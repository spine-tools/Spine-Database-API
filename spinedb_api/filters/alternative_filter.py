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
This module provides the alternative filter.

Alternative filter is defined by selected alternatives.
It lets everything depending on the selected alternatives through and filters out the rest.
"""
from collections.abc import Iterable
from functools import partial
from sqlalchemy import and_, or_, select, literal, cast, Integer, union_all, func, desc
from ..exception import SpineDBAPIError
from .query_utils import filter_by_active_elements

__all__ = ("alternative_filter_config",)

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
    make_alternative_sq = partial(_make_alternative_filtered_alternative_sq, state=state)
    db_map.override_alternative_sq_maker(make_alternative_sq)
    make_scenario_alternative_sq = partial(_make_alternative_filtered_scenario_alternative_sq, state=state)
    db_map.override_scenario_alternative_sq_maker(make_scenario_alternative_sq)
    make_scenario_sq = partial(_make_alternative_filtered_scenario_sq, state=state)
    db_map.override_scenario_sq_maker(make_scenario_sq)
    make_entity_element_sq = partial(_make_alternative_filtered_entity_element_sq, state=state)
    db_map.override_entity_element_sq_maker(make_entity_element_sq)
    make_entity_sq = partial(_make_alternative_filtered_entity_sq, state=state)
    db_map.override_entity_sq_maker(make_entity_sq)
    make_entity_alternative_sq = partial(_make_alternative_filtered_entity_alternative_sq, state=state)
    db_map.override_entity_alternative_sq_maker(make_entity_alternative_sq)
    make_entity_group_sq = partial(_make_alternative_filtered_entity_group_sq, state=state)
    db_map.override_entity_group_sq_maker(make_entity_group_sq)
    make_parameter_value_sq = partial(_make_alternative_filtered_parameter_value_sq, state=state)
    db_map.override_parameter_value_sq_maker(make_parameter_value_sq)


def alternative_filter_config(alternatives: Iterable[str]) -> dict:
    """Creates a config dict for alternative filter from a list of selected alternative names."""
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
    """Internal state for :func:`_make_alternative_filtered_parameter_value_sq`."""

    def __init__(self, db_map, alternatives):
        """
        Args:
            db_map (DatabaseMapping): database the state applies to
            alternatives (Iterable of str or int): alternative names or ids;
        """
        self.original_entity_sq = db_map.entity_sq
        self.original_entity_element_sq = db_map.entity_element_sq
        self.original_entity_alternative_sq = db_map.entity_alternative_sq
        self.original_entity_group_sq = db_map.entity_group_sq
        self.original_parameter_value_sq = db_map.parameter_value_sq
        self.original_scenario_sq = db_map.scenario_sq
        self.original_scenario_alternative_sq = db_map.scenario_alternative_sq
        self.original_alternative_sq = db_map.alternative_sq
        self.alternatives = self._alternative_ids(db_map, alternatives) if alternatives is not None else None
        self.scenarios = self._scenario_ids(db_map, self.alternatives)

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

    @staticmethod
    def _scenario_ids(db_map, alternative_ids):
        """
        Finds active scenario ids.

        Arg:
            db_map (DatabaseMapping): database mapping
            alternative_ids (Iterable of int): active alternative ids

        Returns:
            list of int: active scenario ids
        """
        scenario_ids = {row.id for row in db_map.query(db_map.scenario_sq.c.id)}
        alternative_ids = set(alternative_ids)
        for scenario_alternative in db_map.query(db_map.scenario_alternative_sq):
            if scenario_alternative.alternative_id not in alternative_ids:
                scenario_ids.discard(scenario_alternative.scenario_id)
        return list(scenario_ids)


def _rank_alternative_sq(alternatives):
    if not alternatives:
        return select(literal(None).label("rank"), literal(None).label("alternative_id"))
    rank_alt_rows = list(enumerate(reversed(alternatives)))
    selects = [
        # NOTE: optimization to reduce the size of the statement:
        # make type cast only for first row, for other rows DB engine will infer
        select(cast(literal(rank), Integer).label("rank"), cast(literal(alt_id), Integer).label("alternative_id"))
        if i == 0
        else select(literal(rank), literal(alt_id))  # no type cast
        for i, (rank, alt_id) in enumerate(rank_alt_rows)
    ]
    return union_all(*selects).alias(name="rank_alternative")


def _ext_entity_sq(db_map, state):
    """Filter entities in given `db_map` by the alternatives defined by given `state`."""
    # NOTE: the 'alternatives' filter is pretty much like the 'scenario' filter,
    # except that here the alternatives are explicitly given in a list by the user (in increasing rank order)
    # (whereas in the 'scenario' filter, they are given by a scenario
    # and fetched from the scenario_alternative table...)
    # For the implementation we create a literal subquery with alternatives and their ranks
    # that we join to the entity subquery (in the same fashion as we join the scenario_alternative table
    # in the scenario filter).
    rank_alt_sq = _rank_alternative_sq(state.alternatives)
    entity_sq = (
        db_map.query(
            state.original_entity_sq,
            func.row_number()
            .over(
                partition_by=[state.original_entity_sq.c.id],
                order_by=desc(rank_alt_sq.c.rank),
            )
            .label("desc_rank_row_number"),
            state.original_entity_alternative_sq.c.active,
            db_map.entity_class_sq.c.active_by_default,
        )
        .outerjoin(
            state.original_entity_alternative_sq,
            state.original_entity_sq.c.id == state.original_entity_alternative_sq.c.entity_id,
        )
        .outerjoin(db_map.entity_class_sq, state.original_entity_sq.c.class_id == db_map.entity_class_sq.c.id)
        .outerjoin(rank_alt_sq, state.original_entity_alternative_sq.c.alternative_id == rank_alt_sq.c.alternative_id)
        .filter(
            or_(
                state.original_entity_alternative_sq.c.alternative_id == None,
                state.original_entity_alternative_sq.c.alternative_id == rank_alt_sq.c.alternative_id,
                db_map.entity_class_sq.c.active_by_default == True,
            ),
        )
    ).subquery()
    return (
        db_map.query(entity_sq)
        .filter(
            entity_sq.c.desc_rank_row_number == 1,
            or_(entity_sq.c.active == True, and_(entity_sq.c.active == None, entity_sq.c.active_by_default == True)),
        )
        .subquery()
    )


def _make_alternative_filtered_entity_alternative_sq(db_map, state):
    """
    Returns an entity alternative filtering subquery similar to :func:`DatabaseMapping.entity_alternative_sq`.

    This function can be used as replacement for entity_alternative subquery maker in :class:`DatabaseMapping`.

    Args:
        db_map (DatabaseMapping): a database map
        state (_AlternativeFilterState): a state bound to ``db_map``

    Returns:
        Alias: a subquery for entity alternatives filtered by selected alternatives
    """
    ext_entity_sq = _ext_entity_sq(db_map, state)
    return (
        db_map.query(state.original_entity_alternative_sq)
        .filter(
            state.original_entity_alternative_sq.c.alternative_id.in_(state.alternatives),
            state.original_entity_alternative_sq.c.entity_id == ext_entity_sq.c.id,
        )
        .subquery()
    )


def _make_alternative_filtered_entity_element_sq(db_map, state):
    """Returns an alternative filtering subquery similar to :func:`DatabaseMapping.entity_element_sq`.

    This function can be used as replacement for entity_element subquery maker in :class:`DatabaseMapping`.

    Args:
        db_map (DatabaseMapping): a database map
        state (_AlternativeFilterState): a state bound to ``db_map``

    Returns:
        Alias: a subquery for entity_element filtered by selected alternatives
    """
    ext_entity_sq = _ext_entity_sq(db_map, state)
    entity_sq = ext_entity_sq.alias()
    element_sq = ext_entity_sq.alias()
    return (
        db_map.query(state.original_entity_element_sq)
        .filter(
            state.original_entity_element_sq.c.entity_id == entity_sq.c.id,
            state.original_entity_element_sq.c.element_id == element_sq.c.id,
        )
        .subquery()
    )


def _make_alternative_filtered_entity_sq(db_map, state):
    """Returns an entity filtering subquery similar to :func:`DatabaseMapping.entity_sq`.

    This function can be used as replacement for entity subquery maker in :class:`DatabaseMapping`.

    Args:
        db_map (DatabaseMapping): a database map
        state (_AlternativeFilterState): a state bound to ``db_map``

    Returns:
        Alias: a subquery for entity filtered by selected alternatives
    """
    ext_entity_sq = _ext_entity_sq(db_map, state)
    filtered_by_activity = db_map.query(
        ext_entity_sq.c.id,
        ext_entity_sq.c.class_id,
        ext_entity_sq.c.name,
        ext_entity_sq.c.description,
        ext_entity_sq.c.commit_id,
    )
    return filter_by_active_elements(db_map, filtered_by_activity, ext_entity_sq).subquery()


def _make_alternative_filtered_entity_group_sq(db_map, state):
    """Returns an entity group  filtering subquery similar to :func:`DatabaseMapping.entity_group_sq`.

    This function can be used as replacement for entity group subquery maker in :class:`DatabaseMapping`.

    Args:
        db_map (DatabaseMapping): a database map
        state (_AlternativeFilterState): a state bound to ``db_map``

    Returns:
        Alias: a subquery for entity group filtered by selected alternatives
    """
    ext_entity_sq1 = _ext_entity_sq(db_map, state)
    ext_entity_sq2 = _ext_entity_sq(db_map, state)
    return (
        db_map.query(state.original_entity_group_sq)
        .filter(
            state.original_entity_group_sq.c.entity_id == ext_entity_sq1.c.id,
            state.original_entity_group_sq.c.member_id == ext_entity_sq2.c.id,
        )
        .subquery()
    )


def _make_alternative_filtered_alternative_sq(db_map, state):
    """
    Returns an alternative filtering subquery similar to :func:`DatabaseMapping.alternative_sq`.

    This function can be used as replacement for alternative subquery maker in :class:`DatabaseMapping`.

    Args:
        db_map (DatabaseMapping): a database map
        state (_AlternativeFilterState): a state bound to ``db_map``

    Returns:
        Alias: a subquery for alternative filtered by selected alternatives
    """
    alternative_sq = state.original_alternative_sq
    return db_map.query(alternative_sq).filter(alternative_sq.c.id.in_(state.alternatives)).subquery()


def _make_alternative_filtered_scenario_sq(db_map, state):
    """
    Returns a scenario filtering subquery similar to :func:`DatabaseMapping.scenario_sq`.

    This function can be used as replacement for scenario subquery maker in :class:`DatabaseMapping`.

    Args:
        db_map (DatabaseMapping): a database map
        state (_AlternativeFilterState): a state bound to ``db_map``

    Returns:
        Alias: a subquery for scenario filtered by selected alternatives
    """
    scenario_sq = state.original_scenario_sq
    return db_map.query(scenario_sq).filter(scenario_sq.c.id.in_(state.scenarios)).subquery()


def _make_alternative_filtered_scenario_alternative_sq(db_map, state):
    """
    Returns a scenario alternative filtering subquery similar to :func:`DatabaseMapping.scenario_alternative_sq`.

    This function can be used as replacement for scenario alternative subquery maker in :class:`DatabaseMapping`.

    Args:
        db_map (DatabaseMapping): a database map
        state (_AlternativeFilterState): a state bound to ``db_map``

    Returns:
        Alias: a subquery for scenario alternative filtered by selected alternatives
    """
    scenario_alternative_sq = state.original_scenario_alternative_sq
    return (
        db_map.query(scenario_alternative_sq)
        .filter(scenario_alternative_sq.c.scenario_id.in_(state.scenarios))
        .subquery()
    )


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
    ext_entity_sq = _ext_entity_sq(db_map, state)
    filtered_by_activity = (
        db_map.query(subquery)
        .join(ext_entity_sq, subquery.c.entity_id == ext_entity_sq.c.id)
        .filter(subquery.c.alternative_id.in_(state.alternatives))
    )
    return filter_by_active_elements(db_map, filtered_by_activity, ext_entity_sq).subquery()
