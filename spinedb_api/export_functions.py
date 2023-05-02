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
    parameter_definition_ids=Asterisk,
    parameter_value_ids=Asterisk,
    entity_group_ids=Asterisk,
    object_class_ids=Asterisk,
    relationship_class_ids=Asterisk,
    parameter_value_list_ids=Asterisk,
    object_parameter_ids=Asterisk,
    relationship_parameter_ids=Asterisk,
    object_ids=Asterisk,
    object_group_ids=Asterisk,
    relationship_ids=Asterisk,
    object_parameter_value_ids=Asterisk,
    relationship_parameter_value_ids=Asterisk,
    alternative_ids=Asterisk,
    scenario_ids=Asterisk,
    scenario_alternative_ids=Asterisk,
    make_cache=None,
    parse_value=from_database,
):
    """
    Exports data from given database into a dictionary that can be splatted into keyword arguments for ``import_data``.

    Args:
        db_map (DiffDatabaseMapping): The db to pull stuff from.
        object_class_ids (Iterable, optional): A collection of ids to pick from the database table
        relationship_class_ids (Iterable, optional): A collection of ids to pick from the database table
        parameter_value_list_ids (Iterable, optional): A collection of ids to pick from the database table
        object_parameter_ids (Iterable, optional): A collection of ids to pick from the database table
        relationship_parameter_ids (Iterable, optional): A collection of ids to pick from the database table
        object_ids (Iterable, optional): A collection of ids to pick from the database table
        relationship_ids (Iterable, optional): A collection of ids to pick from the database table
        object_parameter_value_ids (Iterable, optional): A collection of ids to pick from the database table
        relationship_parameter_value_ids (Iterable, optional): A collection of ids to pick from the database table
        alternative_ids (Iterable, optional): A collection of ids to pick from the database table
        scenario_ids (Iterable, optional): A collection of ids to pick from the database table
        scenario_alternative_ids (Iterable, optional): A collection of ids to pick from the database table

    Returns:
        dict: exported data
    """
    data = {
        "entity_classes": export_entity_classes(db_map, entity_class_ids, make_cache=make_cache),
        "entities": export_entities(db_map, entity_ids, make_cache=make_cache),
        "entity_groups": export_entity_groups(db_map, entity_group_ids, make_cache=make_cache),
        "parameter_definitions": export_parameter_definitions(
            db_map, parameter_definition_ids, make_cache=make_cache, parse_value=parse_value
        ),
        "parameter_values": export_parameter_values(
            db_map, parameter_value_ids, make_cache=make_cache, parse_value=parse_value
        ),
        "object_classes": export_object_classes(db_map, object_class_ids, make_cache=make_cache),
        "relationship_classes": export_relationship_classes(db_map, relationship_class_ids, make_cache=make_cache),
        "parameter_value_lists": export_parameter_value_lists(
            db_map, parameter_value_list_ids, make_cache=make_cache, parse_value=parse_value
        ),
        "object_parameters": export_object_parameters(
            db_map, object_parameter_ids, make_cache=make_cache, parse_value=parse_value
        ),
        "relationship_parameters": export_relationship_parameters(
            db_map, relationship_parameter_ids, make_cache=make_cache, parse_value=parse_value
        ),
        "objects": export_objects(db_map, object_ids, make_cache=make_cache),
        "relationships": export_relationships(db_map, relationship_ids, make_cache=make_cache),
        "object_groups": export_object_groups(db_map, object_group_ids, make_cache=make_cache),
        "object_parameter_values": export_object_parameter_values(
            db_map, object_parameter_value_ids, make_cache=make_cache, parse_value=parse_value
        ),
        "relationship_parameter_values": export_relationship_parameter_values(
            db_map, relationship_parameter_value_ids, make_cache=make_cache, parse_value=parse_value
        ),
        "alternatives": export_alternatives(db_map, alternative_ids, make_cache=make_cache),
        "scenarios": export_scenarios(db_map, scenario_ids, make_cache=make_cache),
        "scenario_alternatives": export_scenario_alternatives(db_map, scenario_alternative_ids, make_cache=make_cache),
    }
    return {key: value for key, value in data.items() if value}


def _get_items(db_map, tablename, ids, make_cache):
    if not ids:
        return ()
    if make_cache is None:
        make_cache = db_map.make_cache
    cache = make_cache({tablename}, include_ancestors=True)
    _process_item = _make_item_processor(tablename, make_cache)
    for item in _get_items_from_cache(cache, tablename, ids):
        yield from _process_item(item)


def _get_items_from_cache(cache, tablename, ids):
    items = cache.get(tablename, {})
    if ids is Asterisk:
        yield from items.values()
        return
    for id_ in ids:
        item = items[id_]
        if item.is_valid():
            yield item


def _make_item_processor(tablename, make_cache):
    if tablename == "parameter_value_list":
        return _ParameterValueListProcessor(make_cache)
    return lambda item: (item,)


class _ParameterValueListProcessor:
    def __init__(self, make_cache):
        self._cache = make_cache({"list_value"})

    def __call__(self, item):
        fields = ["name", "value", "type"]
        if item.value_id_list is None:
            yield KeyedTuple([item.name, None, None], fields)
            return
        for value_id in item.value_id_list:
            val = self._cache["list_value"][value_id]
            yield KeyedTuple([item.name, val.value, val.type], fields)


def export_parameter_value_lists(db_map, ids=Asterisk, make_cache=None, parse_value=from_database):
    return sorted(
        ((x.name, parse_value(x.value, x.type)) for x in _get_items(db_map, "parameter_value_list", ids, make_cache)),
        key=itemgetter(0),
    )


def export_entity_classes(db_map, ids=Asterisk, make_cache=None):
    return sorted(
        (
            (x.name, x.dimension_name_list, x.description, x.display_icon)
            for x in _get_items(db_map, "entity_class", ids, make_cache)
        ),
        key=lambda x: (len(x[1]), x[0]),
    )


def export_entities(db_map, ids=Asterisk, make_cache=None):
    return sorted(
        (
            (x.class_name, x.element_name_list or x.name, x.description)
            for x in _get_items(db_map, "entity", ids, make_cache)
        ),
        key=lambda x: (0 if isinstance(x[1], str) else len(x[1]), x[0]),
    )


def export_entity_groups(db_map, ids=Asterisk, make_cache=None):
    return sorted(
        (x.class_name, x.group_name, x.member_name) for x in _get_items(db_map, "entity_group", ids, make_cache)
    )


def export_parameter_definitions(db_map, ids=Asterisk, make_cache=None, parse_value=from_database):
    return sorted(
        (
            x.entity_class_name,
            x.parameter_name,
            parse_value(x.default_value, x.default_type),
            x.value_list_name,
            x.description,
        )
        for x in _get_items(db_map, "parameter_definition", ids, make_cache)
    )


def export_parameter_values(db_map, ids=Asterisk, make_cache=None, parse_value=from_database):
    return sorted(
        (
            (
                x.entity_class_name,
                x.element_name_list or x.entity_name,
                x.parameter_name,
                parse_value(x.value, x.type),
                x.alternative_name,
            )
            for x in _get_items(db_map, "parameter_value", ids, make_cache)
        ),
        key=lambda x: x[:3] + (x[-1],),
    )


def export_object_classes(db_map, ids=Asterisk, make_cache=None):
    return sorted(
        (x.name, x.description, x.display_icon)
        for x in _get_items(db_map, "entity_class", ids, make_cache)
        if not x.dimension_id_list
    )


def export_relationship_classes(db_map, ids=Asterisk, make_cache=None):
    return sorted(
        (x.name, x.dimension_name_list, x.description, x.display_icon)
        for x in _get_items(db_map, "entity_class", ids, make_cache)
        if x.dimension_id_list
    )


def export_objects(db_map, ids=Asterisk, make_cache=None):
    return sorted(
        (x.class_name, x.name, x.description)
        for x in _get_items(db_map, "entity", ids, make_cache)
        if not x.element_id_list
    )


def export_relationships(db_map, ids=Asterisk, make_cache=None):
    return sorted(
        (x.class_name, x.element_name_list) for x in _get_items(db_map, "entity", ids, make_cache) if x.element_id_list
    )


def export_object_groups(db_map, ids=Asterisk, make_cache=None):
    return sorted(
        (x.class_name, x.group_name, x.member_name)
        for x in _get_items(db_map, "entity_group", ids, make_cache)
        if not x.dimension_id_list
    )


def export_object_parameters(db_map, ids=Asterisk, make_cache=None, parse_value=from_database):
    return sorted(
        (
            x.entity_class_name,
            x.parameter_name,
            parse_value(x.default_value, x.default_type),
            x.value_list_name,
            x.description,
        )
        for x in _get_items(db_map, "parameter_definition", ids, make_cache)
        if not x.dimension_id_list
    )


def export_relationship_parameters(db_map, ids=Asterisk, make_cache=None, parse_value=from_database):
    return sorted(
        (
            x.entity_class_name,
            x.parameter_name,
            parse_value(x.default_value, x.default_type),
            x.value_list_name,
            x.description,
        )
        for x in _get_items(db_map, "parameter_definition", ids, make_cache)
        if x.dimension_id_list
    )


def export_object_parameter_values(db_map, ids=Asterisk, make_cache=None, parse_value=from_database):
    return sorted(
        (
            (x.entity_class_name, x.entity_name, x.parameter_name, parse_value(x.value, x.type), x.alternative_name)
            for x in _get_items(db_map, "parameter_value", ids, make_cache)
            if not x.element_id_list
        ),
        key=lambda x: x[:3] + (x[-1],),
    )


def export_relationship_parameter_values(db_map, ids=Asterisk, make_cache=None, parse_value=from_database):
    return sorted(
        (
            (
                x.entity_class_name,
                x.element_name_list,
                x.parameter_name,
                parse_value(x.value, x.type),
                x.alternative_name,
            )
            for x in _get_items(db_map, "parameter_value", ids, make_cache)
            if x.element_id_list
        ),
        key=lambda x: x[:3] + (x[-1],),
    )


def export_alternatives(db_map, ids=Asterisk, make_cache=None):
    """
    Exports alternatives from database.

    The format is what :func:`import_alternatives` accepts as its input.

    Args:
        db_map (spinedb_api.DatabaseMapping or spinedb_api.DiffDatabaseMapping): a database map
        ids (Iterable, optional): ids of the alternatives to export

    Returns:
        Iterable: tuples of two elements: name of alternative and description
    """
    return sorted((x.name, x.description) for x in _get_items(db_map, "alternative", ids, make_cache))


def export_scenarios(db_map, ids=Asterisk, make_cache=None):
    """
    Exports scenarios from database.

    The format is what :func:`import_scenarios` accepts as its input.

    Args:
        db_map (spinedb_api.DatabaseMapping or spinedb_api.DiffDatabaseMapping): a database map
        ids (Iterable, optional): ids of the scenarios to export

    Returns:
        Iterable: tuples of two elements: name of scenario and description
    """
    return sorted((x.name, x.active, x.description) for x in _get_items(db_map, "scenario", ids, make_cache))


def export_scenario_alternatives(db_map, ids=Asterisk, make_cache=None):
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
            for x in _get_items(db_map, "scenario_alternative", ids, make_cache)
        ),
        key=itemgetter(0),
    )
