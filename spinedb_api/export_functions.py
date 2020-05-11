######################################################################################################################
# Copyright (C) 2017 - 2018 Spine project consortium
# This file is part of Spine Toolbox.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Functions for exporting data into a Spine database using entity names as references.

:author: M. Marin (KTH)
:date:   1.4.2020
"""

from .parameter_value import from_database


def export_data(
    db_map,
    object_class_ids=None,
    relationship_class_ids=None,
    parameter_value_list_ids=None,
    object_parameter_ids=None,
    relationship_parameter_ids=None,
    object_ids=None,
    relationship_ids=None,
    object_parameter_value_ids=None,
    relationship_parameter_value_ids=None,
):
    """
    Exports data from given database into a dictionary that can be splatted into keyword arguments for ``import_data``.

    Args:
        db_map (DiffDatabaseMapping): The db to pull stuff from.
        ...ids (Iterable): A collection of ids to pick from each corresponding table. ``None`` (the default) means pick them all.

    Returns:
        dict
    """
    data = {
        "object_classes": export_object_classes(db_map, object_class_ids),
        "relationship_classes": export_relationship_classes(db_map, relationship_class_ids),
        "parameter_value_lists": export_parameter_value_lists(db_map, parameter_value_list_ids),
        "object_parameters": export_object_parameters(db_map, object_parameter_ids),
        "relationship_parameters": export_relationship_parameters(db_map, relationship_parameter_ids),
        "objects": export_objects(db_map, object_ids),
        "relationships": export_relationships(db_map, relationship_ids),
        "object_parameter_values": export_object_parameter_values(db_map, object_parameter_value_ids),
        "relationship_parameter_values": export_relationship_parameter_values(db_map, relationship_parameter_value_ids),
    }
    return {key: value for key, value in data.items() if value}


def export_object_classes(db_map, ids):
    sq = db_map.object_class_sq
    return sorted((x.name, x.description, x.display_icon) for x in db_map.query(sq).filter(db_map.in_(sq.c.id, ids)))


def export_objects(db_map, ids):
    sq = db_map.ext_object_sq
    return sorted((x.class_name, x.name) for x in db_map.query(sq).filter(db_map.in_(sq.c.id, ids)))


def export_relationship_classes(db_map, ids):
    sq = db_map.wide_relationship_class_sq
    return sorted(
        (x.name, x.object_class_name_list.split(","), x.description)
        for x in db_map.query(sq).filter(db_map.in_(sq.c.id, ids))
    )


def export_parameter_value_lists(db_map, ids):
    sq = db_map.wide_parameter_value_list_sq
    return sorted((x.name, x.value_list.split(",")) for x in db_map.query(sq).filter(db_map.in_(sq.c.id, ids)))


def export_object_parameters(db_map, ids):
    sq = db_map.object_parameter_definition_sq
    return sorted(
        (x.object_class_name, x.parameter_name, from_database(x.default_value), x.value_list_name, x.description)
        for x in db_map.query(sq).filter(db_map.in_(sq.c.id, ids))
    )


def export_relationship_parameters(db_map, ids):
    sq = db_map.relationship_parameter_definition_sq
    return sorted(
        (x.relationship_class_name, x.parameter_name, from_database(x.default_value), x.value_list_name, x.description)
        for x in db_map.query(sq).filter(db_map.in_(sq.c.id, ids))
    )


def export_relationships(db_map, ids):
    sq = db_map.wide_relationship_sq
    return sorted(
        (x.class_name, x.object_name_list.split(",")) for x in db_map.query(sq).filter(db_map.in_(sq.c.id, ids))
    )


def export_object_parameter_values(db_map, ids):
    sq = db_map.object_parameter_value_sq
    return sorted(
        (x.object_class_name, x.object_name, x.parameter_name, from_database(x.value))
        for x in db_map.query(sq).filter(db_map.in_(sq.c.id, ids))
    )


def export_relationship_parameter_values(db_map, ids):
    sq = db_map.relationship_parameter_value_sq
    return sorted(
        (x.relationship_class_name, x.object_name_list.split(","), x.parameter_name, from_database(x.value))
        for x in db_map.query(sq).filter(db_map.in_(sq.c.id, ids))
    )
