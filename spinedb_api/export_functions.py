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
"""Functions for exporting data from a Spine database in a standard format."""
from collections.abc import Callable, Iterable, Iterator
from operator import itemgetter
from typing import Any, Optional, Union
from . import DatabaseMapping
from .db_mapping_base import MappedItemBase
from .helpers import Asterisk, AsteriskType, DisplayStatus
from .parameter_value import from_database
from .temp_id import TempId

Ids = Union[Iterable[TempId], AsteriskType]


def export_data(
    db_map: DatabaseMapping,
    entity_class_ids: Ids = Asterisk,
    superclass_subclass_ids: Ids = Asterisk,
    display_mode_ids: Ids = Asterisk,
    entity_class_display_mode_ids: Ids = Asterisk,
    entity_ids: Ids = Asterisk,
    entity_group_ids: Ids = Asterisk,
    parameter_value_list_ids: Ids = Asterisk,
    parameter_definition_ids: Ids = Asterisk,
    parameter_type_ids: Ids = Asterisk,
    parameter_value_ids: Ids = Asterisk,
    alternative_ids: Ids = Asterisk,
    scenario_ids: Ids = Asterisk,
    scenario_alternative_ids: Ids = Asterisk,
    entity_alternative_ids: Ids = Asterisk,
    parse_value: Callable[[bytes, Optional[str]], Any] = from_database,
) -> dict:
    """
    Exports data from a Spine DB into a standard dictionary format.
    The result can be splatted into keyword arguments for :func:`spinedb_api.import_functions.import_data`,
    to copy data from one DB to another.

    Args:
        db_map: The db to pull data from.
        entity_class_ids: If given, only exports classes with these ids
        superclass_subclass_ids: If given, only exports superclass subclasse with these ids
        display_mode_ids: If given, only exports display modes with these ids
        entity_class_display_mode_ids: If given, only exports entity class specific display modes with these ids
        entity_ids: If given, only exports entities with these ids
        entity_group_ids: If given, only exports groups with these ids
        parameter_value_list_ids: If given, only exports lists with these ids
        parameter_definition_ids: If given, only exports parameter definitions with these ids
        parameter_type_ids: If given, only exports parameter types with these ids
        parameter_value_ids: If given, only exports parameter values with these ids
        alternative_ids: If given, only exports alternatives with these ids
        scenario_ids: If given, only exports scenarios with these ids
        scenario_alternative_ids: If given, only exports scenario alternatives with these ids
        entity_alternative_ids: If given, only exports entity alternatives with these ids
        parse_value: Callable to parse value blobs from database

    Returns:
        exported data
    """
    data = {
        "entity_classes": export_entity_classes(db_map, entity_class_ids),
        "superclass_subclasses": export_superclass_subclasses(db_map, superclass_subclass_ids),
        "display_modes": export_display_modes(db_map, display_mode_ids),
        "entity_class_display_modes": export_entity_class_display_modes(db_map, entity_class_display_mode_ids),
        "entities": export_entities(db_map, entity_ids),
        "entity_alternatives": export_entity_alternatives(db_map, entity_alternative_ids),
        "entity_groups": export_entity_groups(db_map, entity_group_ids),
        "parameter_value_lists": export_parameter_value_lists(
            db_map, parameter_value_list_ids, parse_value=parse_value
        ),
        "parameter_definitions": export_parameter_definitions(
            db_map, parameter_definition_ids, parse_value=parse_value
        ),
        "parameter_types": export_parameter_types(db_map, parameter_type_ids),
        "parameter_values": export_parameter_values(db_map, parameter_value_ids, parse_value=parse_value),
        "alternatives": export_alternatives(db_map, alternative_ids),
        "scenarios": export_scenarios(db_map, scenario_ids),
        "scenario_alternatives": export_scenario_alternatives(db_map, scenario_alternative_ids),
    }
    return {key: value for key, value in data.items() if value}


def _get_items(db_map: DatabaseMapping, tablename: str, ids: Ids) -> Iterator[dict]:
    if not ids:
        return
    if tablename == "parameter_value_list":
        db_map.fetch_all("list_value")
        process_item = _ParameterValueListProcessor(db_map.mapped_table("list_value").valid_values())
        for item in _get_items_from_db_map(db_map, tablename, ids):
            yield from process_item(item)
    else:
        yield from _get_items_from_db_map(db_map, tablename, ids)


def _get_items_from_db_map(db_map: DatabaseMapping, tablename: str, ids: Ids) -> Iterator[MappedItemBase]:
    if ids is Asterisk:
        db_map.fetch_all(tablename)
        yield from db_map.mapped_table(tablename).valid_values()
        return
    mapped_table = db_map.mapped_table(tablename)
    for id_ in ids:
        item = mapped_table.find_item_by_id(id_)
        if item.is_valid():
            yield item


class _ParameterValueListProcessor:
    def __init__(self, value_items: Iterable[MappedItemBase]):
        self._value_items_by_list_id = {}
        for x in value_items:
            self._value_items_by_list_id.setdefault(x["parameter_value_list_id"], []).append(x)

    def __call__(self, item: MappedItemBase) -> Iterator[dict]:
        for list_value_item in sorted(self._value_items_by_list_id.get(item["id"], ()), key=itemgetter("index")):
            yield {"name": item["name"], "value": list_value_item["value"], "type": list_value_item["type"]}


def export_parameter_value_lists(
    db_map: DatabaseMapping, ids: Ids = Asterisk, parse_value: Callable[[bytes, Optional[str]], Any] = from_database
) -> list[tuple[str, Any]]:
    return sorted(
        ((x["name"], parse_value(x["value"], x["type"])) for x in _get_items(db_map, "parameter_value_list", ids)),
        key=itemgetter(0),
    )


def export_entity_classes(
    db_map: DatabaseMapping, ids: Ids = Asterisk
) -> list[tuple[str, tuple[str, ...], str, int, bool]]:
    return sorted(
        (
            (x["name"], x["dimension_name_list"], x["description"], x["display_icon"], x["active_by_default"])
            for x in _get_items(db_map, "entity_class", ids)
        ),
        key=lambda x: (len(x[1]), x[0]),
    )


def export_superclass_subclasses(db_map: DatabaseMapping, ids: Ids = Asterisk) -> list[tuple[str, str]]:
    return sorted(((x["superclass_name"], x["subclass_name"]) for x in _get_items(db_map, "superclass_subclass", ids)))


def export_display_modes(db_map: DatabaseMapping, ids: Ids = Asterisk) -> list[tuple[str, str]]:
    return sorted(((x["name"], x["description"]) for x in _get_items(db_map, "display_mode", ids)))


def export_entity_class_display_modes(
    db_map: DatabaseMapping, ids: Ids = Asterisk
) -> list[tuple[str, str, int, DisplayStatus, str, str]]:
    return sorted(
        (
            x["display_mode_name"],
            x["entity_class_name"],
            x["display_order"],
            x["display_status"],
            x["display_font_color"],
            x["display_background_color"],
        )
        for x in _get_items(db_map, "entity_class_display_mode", ids)
    )


def export_entities(db_map: DatabaseMapping, ids: Ids = Asterisk) -> list[
    Union[
        tuple[str, Union[tuple[str, ...], str], str],
        tuple[
            str,
            Union[
                tuple[str, ...], str, Optional[float], Optional[float], Optional[float], Optional[str], Optional[str]
            ],
        ],
    ]
]:
    data = []
    if ids is Asterisk:
        db_map.fetch_all("entity_location")
    for entity in _get_items(db_map, "entity", ids):
        exported = (
            entity["entity_class_name"],
            entity["entity_byname"] if entity["element_name_list"] else entity["name"],
            entity["description"],
        )
        if entity["entity_location_id"] is not None:
            exported = exported + (
                (entity["lat"], entity["lon"], entity["alt"], entity["shape_name"], entity["shape_blob"]),
            )
        data.append(exported)
    return sorted(
        data,
        key=lambda x: (0 if isinstance(x[1], str) else len(x[1]), x[0], (x[1],) if isinstance(x[1], str) else x[1]),
    )


def export_entity_groups(db_map: DatabaseMapping, ids: Ids = Asterisk) -> list[tuple[str, str, str]]:
    return sorted(
        (x["entity_class_name"], x["group_name"], x["member_name"]) for x in _get_items(db_map, "entity_group", ids)
    )


def export_entity_alternatives(
    db_map: DatabaseMapping, ids: Ids = Asterisk
) -> list[tuple[str, tuple[str, ...], str, bool]]:
    return sorted(
        (x["entity_class_name"], x["entity_byname"], x["alternative_name"], x["active"])
        for x in _get_items(db_map, "entity_alternative", ids)
    )


def export_parameter_definitions(
    db_map: DatabaseMapping, ids: Ids = Asterisk, parse_value=from_database
) -> list[tuple[str, str, Any, str, str]]:
    return sorted(
        (
            x["entity_class_name"],
            x["name"],
            parse_value(x["default_value"], x["default_type"]),
            x["parameter_value_list_name"],
            x["description"],
        )
        for x in _get_items(db_map, "parameter_definition", ids)
    )


def export_parameter_types(db_map: DatabaseMapping, ids: Ids = Asterisk) -> list[tuple[str, str, str, int]]:
    return sorted(
        (x["entity_class_name"], x["parameter_definition_name"], x["type"], x["rank"])
        for x in _get_items(db_map, "parameter_type", ids)
    )


def export_parameter_values(
    db_map: DatabaseMapping, ids: Ids = Asterisk, parse_value=from_database
) -> list[tuple[str, Union[tuple[str, ...], str], str, Any, str]]:
    return sorted(
        (
            (
                x["entity_class_name"],
                x["entity_byname"] if x["element_name_list"] else x["entity_name"],
                x["parameter_name"],
                parse_value(x["value"], x["type"]),
                x["alternative_name"],
            )
            for x in _get_items(db_map, "parameter_value", ids)
        ),
        key=lambda x: x[:3] + (x[-1],),
    )


def export_alternatives(db_map: DatabaseMapping, ids: Ids = Asterisk) -> list[tuple[str, str]]:
    return sorted((x["name"], x["description"]) for x in _get_items(db_map, "alternative", ids))


def export_scenarios(db_map: DatabaseMapping, ids: Ids = Asterisk) -> list[tuple[str, bool, str]]:
    return sorted((x["name"], x["active"], x["description"]) for x in _get_items(db_map, "scenario", ids))


def export_scenario_alternatives(db_map: DatabaseMapping, ids: Ids = Asterisk) -> list[tuple[str, str, str]]:
    return sorted(
        (
            (x["scenario_name"], x["alternative_name"], x["before_alternative_name"])
            for x in _get_items(db_map, "scenario_alternative", ids)
        ),
        key=itemgetter(0),
    )
