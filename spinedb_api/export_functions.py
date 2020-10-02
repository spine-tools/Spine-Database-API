######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
# This file is part of Spine Toolbox.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Functions for exporting data from a Spine database using entity names as references.

:author: M. Marin (KTH)
:date:   1.4.2020
"""

from .parameter_value import from_database
from .helpers import Asterisk


def export_data(
    db_map,
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
    tool_ids=Asterisk,
    feature_ids=Asterisk,
    tool_feature_ids=Asterisk,
    tool_feature_method_ids=Asterisk,
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
        tool_ids (Iterable, optional): A collection of ids to pick from the database table
        feature_ids (Iterable, optional): A collection of ids to pick from the database table
        tool_feature_ids (Iterable, optional): A collection of ids to pick from the database table
        tool_feature_method_ids (Iterable, optional): A collection of ids to pick from the database table

    Returns:
        dict: exported data
    """
    data = {
        "object_classes": export_object_classes(db_map, object_class_ids),
        "relationship_classes": export_relationship_classes(db_map, relationship_class_ids),
        "parameter_value_lists": export_parameter_value_lists(db_map, parameter_value_list_ids),
        "object_parameters": export_object_parameters(db_map, object_parameter_ids),
        "relationship_parameters": export_relationship_parameters(db_map, relationship_parameter_ids),
        "objects": export_objects(db_map, object_ids),
        "relationships": export_relationships(db_map, relationship_ids),
        "object_groups": export_object_groups(db_map, object_group_ids),
        "object_parameter_values": export_object_parameter_values(db_map, object_parameter_value_ids),
        "relationship_parameter_values": export_relationship_parameter_values(db_map, relationship_parameter_value_ids),
        "alternatives": export_alternatives(db_map, alternative_ids),
        "scenarios": export_scenarios(db_map, scenario_ids),
        "scenario_alternatives": export_scenario_alternatives(db_map, scenario_alternative_ids),
        "tools": export_tools(db_map, tool_ids),
        "features": export_features(db_map, feature_ids),
        "tool_features": export_tool_features(db_map, tool_feature_ids),
        "tool_feature_methods": export_tool_feature_methods(db_map, tool_feature_method_ids),
    }
    return {key: value for key, value in data.items() if value}


def _make_query(db_map, sq_attr, ids):
    sq = getattr(db_map, sq_attr)
    qry = db_map.query(sq)
    if ids is Asterisk:
        return qry
    return qry.filter(db_map.in_(sq.c.id, ids))


def export_object_classes(db_map, ids=Asterisk):
    return sorted((x.name, x.description, x.display_icon) for x in _make_query(db_map, "object_class_sq", ids))


def export_objects(db_map, ids=Asterisk):
    return sorted((x.class_name, x.name, x.description) for x in _make_query(db_map, "ext_object_sq", ids))


def export_relationship_classes(db_map, ids=Asterisk):
    return sorted(
        (x.name, x.object_class_name_list.split(","), x.description)
        for x in _make_query(db_map, "wide_relationship_class_sq", ids)
    )


def export_parameter_value_lists(db_map, ids=Asterisk):
    return sorted((x.name, from_database(x.value)) for x in _make_query(db_map, "parameter_value_list_sq", ids))


def export_object_parameters(db_map, ids=Asterisk):
    return sorted(
        (x.object_class_name, x.parameter_name, from_database(x.default_value), x.value_list_name, x.description)
        for x in _make_query(db_map, "object_parameter_definition_sq", ids)
    )


def export_relationship_parameters(db_map, ids=Asterisk):
    return sorted(
        (x.relationship_class_name, x.parameter_name, from_database(x.default_value), x.value_list_name, x.description)
        for x in _make_query(db_map, "relationship_parameter_definition_sq", ids)
    )


def export_relationships(db_map, ids=Asterisk):
    return sorted(
        (x.class_name, x.object_name_list.split(",")) for x in _make_query(db_map, "wide_relationship_sq", ids)
    )


def export_object_groups(db_map, ids=Asterisk):
    return sorted((x.class_name, x.group_name, x.member_name) for x in _make_query(db_map, "ext_object_group_sq", ids))


def export_object_parameter_values(db_map, ids=Asterisk):
    return sorted(
        (
            (x.object_class_name, x.object_name, x.parameter_name, from_database(x.value), x.alternative_name)
            for x in _make_query(db_map, "object_parameter_value_sq", ids)
        ),
        key=lambda x: x[:3] + (x[-1],),
    )


def export_relationship_parameter_values(db_map, ids=Asterisk):
    return sorted(
        (
            (
                x.relationship_class_name,
                x.object_name_list.split(","),
                x.parameter_name,
                from_database(x.value),
                x.alternative_name,
            )
            for x in _make_query(db_map, "relationship_parameter_value_sq", ids)
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
    return sorted((x.name, x.description) for x in _make_query(db_map, "alternative_sq", ids))


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
    return sorted((x.name, x.active, x.description) for x in _make_query(db_map, "scenario_sq", ids))


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
            (x.scenario_name, x.alternative_name, x.next_alternative_name)
            for x in _make_query(db_map, "ext_linked_scenario_alternative_sq", ids)
        ),
        key=lambda x: x[0],
    )


def export_tools(db_map, ids=Asterisk):
    return sorted((x.name, x.description) for x in _make_query(db_map, "tool_sq", ids))


def export_features(db_map, ids=Asterisk):
    return sorted(
        (x.entity_class_name, x.parameter_definition_name, x.parameter_value_list_name, x.description)
        for x in _make_query(db_map, "ext_feature_sq", ids)
    )


def export_tool_features(db_map, ids=Asterisk):
    return sorted(
        (x.tool_name, x.entity_class_name, x.parameter_definition_name, x.required)
        for x in _make_query(db_map, "ext_tool_feature_sq", ids)
    )


def export_tool_feature_methods(db_map, ids=Asterisk):
    return sorted(
        (x.tool_name, x.entity_class_name, x.parameter_definition_name, from_database(x.method))
        for x in _make_query(db_map, "ext_tool_feature_method_sq", ids)
    )


def export_expanded_parameter_values(db_map):
    """Returns two dictionaries.
    The first one maps type-annotated classes to a list of index-expanded parameter values for that class.
    The second one maps the same keys to some metadata as described below.

    Args:
        db_map (DatabaseMapping)

    Returns:
        dict (data): class key -> list of lists, where the first element in the inner list is the header,
            and each other corresponds to one expanded parameter value, with the following fields:
            - object_names: one or more object names
            - alternative_name
            - indexes: zero or more indexes
            - parameter_name
            - value
        dict (metadata): class key -> dictionary of class metadata, with keys:
            - entity_type: either "object" or "relationship"
            - class_name: the object or relationship class name
            - entity_dim_count: how many object classes in the relationship class (1, in case of object class)
            - value_type: either None, "single value", "array", "time series", "time pattern", or "map"
            - index_dim_count: either None, or the number of dimensions in the index
    """
    data = {}
    metadata = {}
    entity_parameters, entity_values = _compile_parameters_and_values(db_map)
    for entity_type in ("object", "relationship"):
        subquery = {"object": db_map.ext_object_sq, "relationship": db_map.wide_relationship_sq}[entity_type]
        get_entity_names = {"object": _get_ent_names_from_obj, "relationship": _get_ent_names_from_rel}[entity_type]
        for ent in db_map.query(subquery):
            object_class_names, object_names = get_entity_names(ent)
            alternative_values = entity_values.get(ent.id)
            base_metadata = {
                "entity_type": entity_type,
                "class_name": ent.class_name,
                "entity_dim_count": len(object_class_names),
            }
            if not alternative_values:
                class_name = ent.class_name
                metadata[class_name] = base_metadata.copy()
                header = [*object_class_names]
                class_data = data.setdefault(class_name, [header])
                item = [*object_names]
                class_data.append(item)
                continue
            for alt_name, value_type, index_dim_count, values_per_index in _unpack_alt_values(alternative_values):
                vtype = value_type.replace(" ", "_")
                suffix = f"{index_dim_count}d_{vtype}" if value_type == "map" else f"{vtype}"
                class_name = f"{ent.class_name}_{suffix}"
                class_metadata = metadata[class_name] = base_metadata.copy()
                class_metadata["value_type"] = value_type
                class_metadata["index_dim_count"] = index_dim_count
                parameter_names = (
                    entity_parameters.get(ent.class_id, {}).get(value_type, {}).get(index_dim_count, set())
                )
                parameter_names = list(parameter_names)
                index = next(iter(values_per_index), None)
                index_key = _get_index_key(index)
                header = [*object_class_names, "alternative", *index_key, *parameter_names]
                class_data = data.setdefault(class_name, [header])
                for index, given_values in values_per_index.items():
                    index_values = _get_index_values(index)
                    values = dict.fromkeys(parameter_names)
                    values.update(given_values)
                    item = [*object_names, alt_name, *index_values, *values.values()]
                    class_data.append(item)
    return data, metadata


# Utility functions for `export_expanded_parameter_values()`
def _compile_parameters_and_values(db_map):
    """Queries parameter values in given db_map and compiles results into two dictionaries.
    Used by ``export_expanded_parameter_values``.

    Args:
        db_map (DatabaseMapping)

    Returns:
        dict (parameters): class id -> value type -> index dim count -> set of parameter names
        dict (values): entity id -> alternative name -> value type -> index dim count -> parameter name -> value
    """
    parameters = {}
    values = {}
    qry = (
        db_map.query(
            db_map.parameter_value_sq.c.entity_id,
            db_map.parameter_value_sq.c.entity_class_id,
            db_map.parameter_value_sq.c.value,
            db_map.parameter_definition_sq.c.name.label("parameter_name"),
            db_map.alternative_sq.c.name.label("alternative_name"),
        )
        .filter(db_map.parameter_value_sq.c.parameter_definition_id == db_map.parameter_definition_sq.c.id)
        .filter(db_map.parameter_value_sq.c.alternative_id == db_map.alternative_sq.c.id)
    )
    for pval in qry:
        parsed_value = from_database(pval.value)
        if parsed_value is None:
            continue
        values_per_type = values.setdefault(pval.entity_id, {}).setdefault(pval.alternative_name, {})
        parameters_per_type = parameters.setdefault(pval.entity_class_id, {})
        try:
            value_type = parsed_value.VALUE_TYPE
            values_per_dim_count = values_per_type.setdefault(value_type, {})
            parameters_per_dim_count = parameters_per_type.setdefault(value_type, {})
            for index, value in parsed_value.indexed_values():
                if isinstance(index, tuple):
                    index_dim_count = len(index)
                elif index is not None:
                    index_dim_count = 1
                else:
                    index_dim_count = None
                values_per_dim_count.setdefault(index_dim_count, {}).setdefault(index, {})[pval.parameter_name] = value
                parameters_per_dim_count.setdefault(index_dim_count, set()).add(pval.parameter_name)
        except AttributeError:
            value_type = "single value"
            index_dim_count = None
            values_per_type.setdefault(value_type, {}).setdefault(index_dim_count, {}).setdefault("", {})[
                pval.parameter_name
            ] = pval.value
            parameters_per_type.setdefault(value_type, {}).setdefault(index_dim_count, set()).add(pval.parameter_name)
    return parameters, values


def _get_ent_names_from_obj(obj):
    return (obj.class_name,), (obj.name,)


def _get_ent_names_from_rel(rel):
    object_class_name_list = rel.object_class_name_list.split(",")
    object_name_list = rel.object_name_list.split(",")
    return object_class_name_list, object_name_list


def _unpack_alt_values(alternative_values):
    for alt_name, values_per_type in alternative_values.items():
        for value_type, values_per_dim_count in values_per_type.items():
            for dim_count, values_per_index in values_per_dim_count.items():
                yield alt_name, value_type, dim_count, values_per_index


def _get_index_key(index):
    if index is None:
        return ()
    if isinstance(index, tuple):
        return tuple("index" for k in range(len(index)))
    return ("index",)


def _get_index_values(index):
    if index is None:
        return ()
    if isinstance(index, tuple):
        return index
    return (index,)
