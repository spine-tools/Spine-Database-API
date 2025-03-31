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
This module defines functions, classes and other utilities
that may be useful with :class:`.db_mapping.DatabaseMapping`.
"""
from spinedb_api.db_mapping_base import PublicItem
from spinedb_api.mapped_items import ParameterDefinitionItem
from spinedb_api.parameter_value import UNPARSED_NULL_VALUE, Map, from_database_to_dimension_count, type_for_value

# Here goes stuff that depends on `database_mapping`, `mapped_items` etc.
# and thus cannot go to `helpers` due to circular imports.


def type_check_args(value_item):
    """Generates arguments compatible for is_parameter_type_valid()

    Args:
        value_item (PublicItem or ParameterDefinitionItem or ParameterValueItem): mapped value item

    Returns:
        tuple: arguments for is_parameter_type_valid()
    """
    mapped_item = value_item.mapped_item if isinstance(value_item, PublicItem) else value_item
    database_value = mapped_item[mapped_item.value_key]
    value_type = mapped_item[mapped_item.type_key]
    parsed_value = mapped_item["parsed_value"] if mapped_item.has_value_been_parsed() else None
    if isinstance(mapped_item, ParameterDefinitionItem):
        definition = mapped_item
    else:
        # We may have a situation during cascade_restore where a value exists
        # but its definition has not yet been restored, thus skip_removed=False.
        # This 'incomplete' state is accessible from restore_callbacks.
        definition = mapped_item.db_map.get_parameter_definition_item(
            id=value_item["parameter_definition_id"], skip_removed=False
        )
    type_ids = definition["parameter_type_id_list"]
    parameter_types = ()
    if type_ids:
        type_table = mapped_item.db_map.mapped_table("parameter_type")
        for type_id in type_ids:
            type_item = type_table.find_item_by_id(type_id)
            parameter_types = parameter_types + ((type_item["type"], type_item["rank"]),)
    return parameter_types, database_value, parsed_value, value_type


def is_parameter_type_valid(parameter_types, database_value, value, value_type):
    """Tests whether given parameter type and value are valid.

    Args:
        parameter_types (Iterable): tuples of parameter types and ranks
        database_value (bytes, optional): unparsed value blob
        value (Any, optional): parsed value if available
        value_type (str, optional): value's type

    Returns:
        bool: True if value is of valid type, False otherwise
    """
    if not parameter_types or database_value is None or database_value == UNPARSED_NULL_VALUE:
        return True
    if value_type is not None:
        if not any(value_type == type_and_rank[0] for type_and_rank in parameter_types):
            return False
        if value_type != Map.TYPE:
            return True
        rank = from_database_to_dimension_count(database_value, value_type)
        return any(rank == type_and_rank[1] for type_and_rank in parameter_types if type_and_rank[0] == Map.TYPE)
    return any(type_for_value(value) == type_and_rank for type_and_rank in parameter_types)
