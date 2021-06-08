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
    return sorted(
        (x.name, from_database(x.value, value_type=None)) for x in _make_query(db_map, "parameter_value_list_sq", ids)
    )


def export_object_parameters(db_map, ids=Asterisk):
    return sorted(
        (
            x.object_class_name,
            x.parameter_name,
            from_database(x.default_value, x.default_type),
            x.value_list_name,
            x.description,
        )
        for x in _make_query(db_map, "object_parameter_definition_sq", ids)
    )


def export_relationship_parameters(db_map, ids=Asterisk):
    return sorted(
        (
            x.relationship_class_name,
            x.parameter_name,
            from_database(x.default_value, x.default_type),
            x.value_list_name,
            x.description,
        )
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
            (x.object_class_name, x.object_name, x.parameter_name, from_database(x.value, x.type), x.alternative_name)
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
                from_database(x.value, x.type),
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
            (x.scenario_name, x.alternative_name, x.before_alternative_name)
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
        (x.tool_name, x.entity_class_name, x.parameter_definition_name, from_database(x.method, value_type=None))
        for x in _make_query(db_map, "ext_tool_feature_method_sq", ids)
    )
