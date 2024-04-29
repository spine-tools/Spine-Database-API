######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Database API contributors
# This file is part of Spine Database API.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
""" Functions for exporting data from a Spine database in a standard format. """
from operator import itemgetter

from sqlalchemy.util import KeyedTuple
from .parameter_value import from_database
from .helpers import Asterisk


def export_data(
    db_map,
    entity_class_ids=Asterisk,
    superclass_subclass_ids=Asterisk,
    entity_ids=Asterisk,
    entity_group_ids=Asterisk,
    parameter_value_list_ids=Asterisk,
    parameter_definition_ids=Asterisk,
    parameter_value_ids=Asterisk,
    alternative_ids=Asterisk,
    scenario_ids=Asterisk,
    scenario_alternative_ids=Asterisk,
    entity_alternative_ids=Asterisk,
    parse_value=from_database,
):
    """
    Exports data from a Spine DB into a standard dictionary format.
    The result can be splatted into keyword arguments for :func:`spinedb_api.import_functions.import_data`,
    to copy data from one DB to another.

    Args:
        db_map (DatabaseMapping): The db to pull data from.
        entity_class_ids (Iterable, optional): If given, only exports classes with these ids
        entity_ids (Iterable, optional): If given, only exports entities with these ids
        entity_group_ids (Iterable, optional): If given, only exports groups with these ids
        parameter_value_list_ids (Iterable, optional): If given, only exports lists with these ids
        parameter_definition_ids (Iterable, optional): If given, only exports parameter definitions with these ids
        parameter_value_ids (Iterable, optional): If given, only exports parameter values with these ids
        alternative_ids (Iterable, optional): If given, only exports alternatives with these ids
        scenario_ids (Iterable, optional): If given, only exports scenarios with these ids
        scenario_alternative_ids (Iterable, optional): If given, only exports scenario alternatives with these ids
        entity_alternative_ids (Iterable, optional): If given, only exports entity alternatives with these ids

    Returns:
        dict: exported data
    """
    data = {
        "entity_classes": export_entity_classes(db_map, entity_class_ids),
        "superclass_subclasses": export_superclass_subclasses(db_map, superclass_subclass_ids),
        "entities": export_entities(db_map, entity_ids),
        "entity_alternatives": export_entity_alternatives(db_map, entity_alternative_ids),
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
    for item in _get_items_from_db_map(db_map, tablename, ids):
        yield from _process_item(item)


def _get_items_from_db_map(db_map, tablename, ids):
    if ids is Asterisk:
        db_map.fetch_all(tablename)
        yield from db_map.mapped_table(tablename).valid_values()
        return
    for id_ in ids:
        item = db_map.get_mapped_item(tablename, id_)
        if item.is_valid():
            yield item


def _make_item_processor(db_map, tablename):
    if tablename == "parameter_value_list":
        db_map.fetch_all("list_value")
        return _ParameterValueListProcessor(db_map.mapped_table("list_value").valid_values())
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
            (x.name, x.dimension_name_list, x.description, x.display_icon, x.active_by_default)
            for x in _get_items(db_map, "entity_class", ids)
        ),
        key=lambda x: (len(x[1]), x[0]),
    )


def export_superclass_subclasses(db_map, ids=Asterisk):
    return sorted(((x.superclass_name, x.subclass_name) for x in _get_items(db_map, "superclass_subclass", ids)))


def export_entities(db_map, ids=Asterisk):
    return sorted(
        (
            (x.entity_class_name, x.element_name_list or x.name, x.description)
            for x in _get_items(db_map, "entity", ids)
        ),
        key=lambda x: (0 if isinstance(x[1], str) else len(x[1]), x[0], (x[1],) if isinstance(x[1], str) else x[1]),
    )


def export_entity_groups(db_map, ids=Asterisk):
    return sorted((x.entity_class_name, x.group_name, x.member_name) for x in _get_items(db_map, "entity_group", ids))


def export_entity_alternatives(db_map, ids=Asterisk):
    return sorted(
        (x.entity_class_name, x.entity_byname, x.alternative_name, x.active)
        for x in _get_items(db_map, "entity_alternative", ids)
    )


def export_parameter_definitions(db_map, ids=Asterisk, parse_value=from_database):
    return sorted(
        (
            x.entity_class_name,
            x.name,
            parse_value(x.default_value, x.default_type),
            x.parameter_value_list_name,
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
    return sorted((x.name, x.description) for x in _get_items(db_map, "alternative", ids))


def export_scenarios(db_map, ids=Asterisk):
    return sorted((x.name, x.active, x.description) for x in _get_items(db_map, "scenario", ids))


def export_scenario_alternatives(db_map, ids=Asterisk):
    return sorted(
        (
            (x.scenario_name, x.alternative_name, x.before_alternative_name)
            for x in _get_items(db_map, "scenario_alternative", ids)
        ),
        key=itemgetter(0),
    )
