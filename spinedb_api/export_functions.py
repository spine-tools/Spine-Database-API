######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# This file is part of Spine Database API.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Functions for exporting data from a Spine database using entity names as references.

"""
from operator import itemgetter

from sqlalchemy.util import KeyedTuple
from .parameter_value import from_database
from .helpers import Asterisk


def export_data(
    db_map,
    entity_class_ids=Asterisk,
    entity_ids=Asterisk,
    entity_group_ids=Asterisk,
    parameter_value_list_ids=Asterisk,
    parameter_definition_ids=Asterisk,
    parameter_value_ids=Asterisk,
    alternative_ids=Asterisk,
    scenario_ids=Asterisk,
    scenario_alternative_ids=Asterisk,
    parse_value=from_database,
):
    """
    Exports data from given database into a dictionary that can be splatted into keyword arguments for ``import_data``.

    Args:
        db_map (DiffDatabaseMapping): The db to pull stuff from.
        entity_class_ids (Iterable, optional): A collection of ids to pick from the database table
        entity_ids (Iterable, optional): A collection of ids to pick from the database table
        entity_group_ids (Iterable, optional): A collection of ids to pick from the database table
        parameter_value_list_ids (Iterable, optional): A collection of ids to pick from the database table
        parameter_definition_ids (Iterable, optional): A collection of ids to pick from the database table
        parameter_value_ids (Iterable, optional): A collection of ids to pick from the database table
        alternative_ids (Iterable, optional): A collection of ids to pick from the database table
        scenario_ids (Iterable, optional): A collection of ids to pick from the database table
        scenario_alternative_ids (Iterable, optional): A collection of ids to pick from the database table

    Returns:
        dict: exported data
    """
    data = {
        "entity_classes": export_entity_classes(db_map, entity_class_ids),
        "entities": export_entities(db_map, entity_ids),
        "entity_groups": export_entity_groups(db_map, entity_group_ids),
        "parameter_value_lists": export_parameter_value_lists(
            db_map, parameter_value_list_ids, parse_value=parse_value
        ),
        "parameter_definitions": export_parameter_definitions(
            db_map, parameter_definition_ids, parse_value=parse_value
        ),
        "parameter_values": export_parameter_values(db_map, parameter_value_ids, parse_value=parse_value),
        "alternatives": export_alternatives(db_map, alternative_ids),
        "scenarios": export_scenarios(db_map, scenario_ids),
        "scenario_alternatives": export_scenario_alternatives(db_map, scenario_alternative_ids),
    }
    return {key: value for key, value in data.items() if value}


def _get_items(db_map, tablename, ids):
    if not ids:
        return ()
    _process_item = _make_item_processor(db_map, tablename)
    for item in _get_items_from_cache(db_map.cache, tablename, ids):
        yield from _process_item(item)


def _get_items_from_cache(cache, tablename, ids):
    if ids is Asterisk:
        cache.fetch_all(tablename)
        yield from cache.get(tablename, {}).values()
        return
    for id_ in ids:
        item = cache.get_item(tablename, id_) or cache.fetch_ref(tablename, id_)
        if item.is_valid():
            yield item


def _make_item_processor(db_map, tablename):
    if tablename == "parameter_value_list":
        db_map.fetch_all({"list_value"})
        return _ParameterValueListProcessor(db_map.cache.get("list_value", {}).values())
    return lambda item: (item,)


class _ParameterValueListProcessor:
    def __init__(self, value_items):
        self._value_items_by_list_id = {}
        for x in value_items:
            self._value_items_by_list_id.setdefault(x.parameter_value_list_id, []).append(x)

    def __call__(self, item):
        for list_value_item in sorted(self._value_items_by_list_id.get(item.id, ()), key=lambda x: x.index):
            yield KeyedTuple([item.name, list_value_item.value, list_value_item.type], ["name", "value", "type"])


def export_parameter_value_lists(db_map, ids=Asterisk, parse_value=from_database):
    return sorted(
        ((x.name, parse_value(x.value, x.type)) for x in _get_items(db_map, "parameter_value_list", ids)),
        key=itemgetter(0),
    )


def export_entity_classes(db_map, ids=Asterisk):
    return sorted(
        (
            (x.name, x.dimension_name_list, x.description, x.display_icon)
            for x in _get_items(db_map, "entity_class", ids)
        ),
        key=lambda x: (len(x[1]), x[0]),
    )


def export_entities(db_map, ids=Asterisk):
    return sorted(
        ((x.class_name, x.element_name_list or x.name, x.description) for x in _get_items(db_map, "entity", ids)),
        key=lambda x: (0 if isinstance(x[1], str) else len(x[1]), x[0]),
    )


def export_entity_groups(db_map, ids=Asterisk):
    return sorted((x.class_name, x.group_name, x.member_name) for x in _get_items(db_map, "entity_group", ids))


def export_parameter_definitions(db_map, ids=Asterisk, parse_value=from_database):
    return sorted(
        (
            x.entity_class_name,
            x.parameter_name,
            parse_value(x.default_value, x.default_type),
            x.value_list_name,
            x.description,
        )
        for x in _get_items(db_map, "parameter_definition", ids)
    )


def export_parameter_values(db_map, ids=Asterisk, parse_value=from_database):
    return sorted(
        (
            (
                x.entity_class_name,
                x.element_name_list or x.entity_name,
                x.parameter_name,
                parse_value(x.value, x.type),
                x.alternative_name,
            )
            for x in _get_items(db_map, "parameter_value", ids)
        ),
        key=lambda x: x[:3] + (x[-1],),
    )


def export_alternatives(db_map, ids=Asterisk):
    """
    Exports alternatives from database.

    The format is what :func:`import_alternatives` accepts as its input.

    Args:
        db_map (spinedb_api.DatabaseMapping or spinedb_api.DiffDatabaseMapping): a database map
        ids (Iterable, optional): ids of the alternatives to export

    Returns:
        Iterable: tuples of two elements: name of alternative and description
    """
    return sorted((x.name, x.description) for x in _get_items(db_map, "alternative", ids))


def export_scenarios(db_map, ids=Asterisk):
    """
    Exports scenarios from database.

    The format is what :func:`import_scenarios` accepts as its input.

    Args:
        db_map (spinedb_api.DatabaseMapping or spinedb_api.DiffDatabaseMapping): a database map
        ids (Iterable, optional): ids of the scenarios to export

    Returns:
        Iterable: tuples of two elements: name of scenario and description
    """
    return sorted((x.name, x.active, x.description) for x in _get_items(db_map, "scenario", ids))


def export_scenario_alternatives(db_map, ids=Asterisk):
    """
    Exports scenario alternatives from database.

    The format is what :func:`import_scenario_alternatives` accepts as its input.

    Args:
        db_map (spinedb_api.DatabaseMapping or spinedb_api.DiffDatabaseMapping): a database map
        ids (Iterable, optional): ids of the scenario alternatives to export

    Returns:
        Iterable: tuples of three elements: name of scenario, tuple containing one alternative name,
            and name of next alternative
    """
    return sorted(
        (
            (x.scenario_name, x.alternative_name, x.before_alternative_name)
            for x in _get_items(db_map, "scenario_alternative", ids)
        ),
        key=itemgetter(0),
    )
