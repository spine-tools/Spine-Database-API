#############################################################################
# Copyright (C) 2017 - 2018 VTT Technical Research Centre of Finland
#
# This file is part of Spine Database API.
#
# Spine Spine Database API is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#############################################################################

"""Functions for checking whether inserting data into a Spine database leads
to the violation of integrity constraints.

:author: Manuel Marin (KTH)
:date:   4.6.2019
"""

import json
from .exception import SpineIntegrityError


def check_object_class(item, current_items):
    """Check whether the insertion of an object class item
    results in the violation of an integrity constraint.

    :param dict item: An object class item to be checked.
    :param dict current_items: A dictionary mapping names to ids of object classes already in the database.

    :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.
    """
    try:
        name = item["name"]
    except KeyError:
        raise SpineIntegrityError("Missing object class name.")
    if name in current_items:
        raise SpineIntegrityError(
            "There can't be more than one object class called '{}'.".format(name), id=current_items[name]
        )


def check_object(item, current_items, object_class_ids):
    """Check whether the insertion of an object item
    results in the violation of an integrity constraint.

    :param dict item: An object item to be checked.
    :param dict current_items: A dictionary mapping tuples (class_id, name) to ids of objects
        already in the database.
    :param list object_class_ids: A list of object class ids in the database.

    :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.
    """
    try:
        class_id = item["class_id"]
    except KeyError:
        raise SpineIntegrityError("Missing object class identifier.")
    if class_id not in object_class_ids:
        raise SpineIntegrityError("Object class not found.")
    try:
        name = item["name"]
    except KeyError:
        raise SpineIntegrityError("Missing object name.")
    if (class_id, name) in current_items:
        raise SpineIntegrityError(
            "There's already an object called '{}' in the same class.".format(name), id=current_items[class_id, name]
        )


def check_wide_relationship_class(wide_item, current_items, object_class_ids):
    """Check whether the insertion of a relationship class item
    results in the violation of an integrity constraint.

    :param dict wide_item: A wide relationship class item to be checked.
    :param dict current_items: A dictionary mapping names to ids of relationship classes
        already in the database.
    :param list object_class_ids: A list of object class ids in the database.

    :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.
    """
    try:
        given_object_class_id_list = wide_item["object_class_id_list"]
    except KeyError:
        raise SpineIntegrityError("Missing object class identifiers.")
    if len(given_object_class_id_list) == 0:
        raise SpineIntegrityError("At least one object class is needed.")
    if not all([id in object_class_ids for id in given_object_class_id_list]):
        raise SpineIntegrityError("Object class not found.")
    try:
        name = wide_item["name"]
    except KeyError:
        raise SpineIntegrityError("Missing relationship class name.")
    if name in current_items:
        raise SpineIntegrityError(
            "There can't be more than one relationship class called '{}'.".format(name), id=current_items[name]
        )


def check_wide_relationship(wide_item, current_items_by_name, current_items_by_obj_lst, relationship_classes, objects):
    """Check whether the insertion of a relationship item
    results in the violation of an integrity constraint.

    :param dict wide_item: A wide relationship item to be checked.
    :param dict current_items_by_name: A dictionary mapping tuples (class_id, name) to ids of relationships
        already in the database.
    :param dict current_items_by_obj_lst: A dictionary mapping tuples (class_id, object_name_list) to ids of
        relationships already in the database.
    :param dict relationship_classes: A dictionary of wide relationship class items in the database keyed by id.
    :param dict objects: A dictionary of object items in the database keyed by id.

    :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.
    """
    try:
        name = wide_item["name"]
    except KeyError:
        raise SpineIntegrityError("Missing relationship name.")
    try:
        class_id = wide_item["class_id"]
    except KeyError:
        raise SpineIntegrityError("Missing relationship class identifier.")
    if (class_id, name) in current_items_by_name:
        raise SpineIntegrityError(
            "There's already a relationship called '{}' in the same class.".format(name),
            id=current_items_by_name[class_id, name],
        )
    try:
        object_class_id_list = relationship_classes[class_id]["object_class_id_list"]
    except KeyError:
        raise SpineIntegrityError("Relationship class not found.")
    try:
        object_id_list = wide_item["object_id_list"]
    except KeyError:
        raise SpineIntegrityError("Missing object identifier.")
    try:
        given_object_class_id_list = [objects[id]["class_id"] for id in object_id_list]
    except KeyError as e:
        raise SpineIntegrityError("Object id '{}' not found.".format(e))
    if given_object_class_id_list != object_class_id_list:
        object_name_list = [objects[id]["name"] for id in object_id_list]
        relationship_class_name = relationship_classes[class_id]["name"]
        raise SpineIntegrityError(
            "Incorrect objects '{}' for relationship class '{}'.".format(object_name_list, relationship_class_name)
        )
    join_object_id_list = ",".join([str(x) for x in object_id_list])
    if (class_id, join_object_id_list) in current_items_by_obj_lst:
        object_name_list = [objects[id]["name"] for id in object_id_list]
        relationship_class_name = relationship_classes[class_id]["name"]
        raise SpineIntegrityError(
            "There's already a relationship between objects {} in class {}.".format(
                object_name_list, relationship_class_name
            ),
            id=current_items_by_obj_lst[class_id, join_object_id_list],
        )


def check_parameter_definition(
    item, current_obj_items, current_rel_items, object_class_names, relationship_class_names, parameter_value_lists
):
    """Check whether the insertion of a parameter definition item
    results in the violation of an integrity constraint.

    :param dict item: A parameter definition item to be checked.
    :param dict current_obj_items: A dictionary mapping tuples (object_class_id, name) to ids
        of object parameter definitions already in the database.
    :param dict current_rel_items: A dictionary mapping tuples (relationship_class_id, name) to ids
        of relationship parameter definitions already in the database.
    :param dict object_class_names: A dictionary of object class names in the database keyed by id.
    :param dict relationship_class_names: A dictionary of relationship class names in the database
        keyed by id.
    :param dict parameter_value_lists: A dictionary of value-lists in the database keyed by id.

    :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.
    """
    object_class_id = item.get("object_class_id", None)
    relationship_class_id = item.get("relationship_class_id", None)
    if object_class_id and relationship_class_id:
        try:
            object_class_name = object_class_names[object_class_id]
        except KeyError:
            object_class_name = "id " + object_class_id
        try:
            relationship_class_name = relationship_class_names[relationship_class_id]
        except KeyError:
            relationship_class_name = "id " + relationship_class_id
        raise SpineIntegrityError(
            "Can't associate a parameter to both object class '{}' and relationship class '{}'.".format(
                object_class_name, relationship_class_name
            )
        )
    if object_class_id:
        if object_class_id not in object_class_names:
            raise SpineIntegrityError("Object class not found.")
        try:
            name = item["name"]
        except KeyError:
            raise SpineIntegrityError("Missing parameter name.")
        if (object_class_id, name) in current_obj_items:
            raise SpineIntegrityError(
                "There's already a parameter called '{}' in this class.".format(name),
                id=current_obj_items[object_class_id, name],
            )
    elif relationship_class_id:
        if relationship_class_id not in relationship_class_names:
            raise SpineIntegrityError("Relationship class not found.")
        try:
            name = item["name"]
        except KeyError:
            raise SpineIntegrityError("Missing parameter name.")
        if (relationship_class_id, name) in current_rel_items:
            raise SpineIntegrityError(
                "There's already a parameter called '{}' in this class.".format(name),
                id=current_rel_items[relationship_class_id, name],
            )
    else:
        raise SpineIntegrityError("Missing object class or relationship class identifier.")
    parameter_value_list_id = item.get("parameter_value_list_id")
    if parameter_value_list_id is not None and parameter_value_list_id not in parameter_value_lists:
        raise SpineIntegrityError("Invalid parameter value list.")
    default_value = item.get("default_value")
    if default_value is not None:
        try:
            json.loads(default_value)
        except json.JSONDecodeError as err:
            raise SpineIntegrityError("Couldn't decode default value '{}' as JSON: {}".format(default_value, err))


def check_parameter_value(
    item, current_obj_items, current_rel_items, parameter_definitions, objects, relationships, parameter_value_lists
):
    """Check whether the insertion of a parameter value item
    results in the violation of an integrity constraint.

    :param dict item: A parameter value item to be checked.
    :param dict current_obj_items: A dictionary mapping tuples (object_id, parameter_definition_id) to ids of
        object parameter values already in the database.
    :param dict current_rel_items: A dictionary mapping tuples (relationship_id, parameter_definition_id) to ids
        of relationship parameter values already in the database.
    :param dict parameter_definitions: A dictionary of parameter definition items in the database keyed by id.
    :param dict objects: A dictionary of object items already in the database keyed by id.
    :param dict relationships: A dictionary of relationship items in the database keyed by id.
    :param dict parameter_value_lists: A dictionary of value-lists in the database keyed by id.

    :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.
    """
    try:
        parameter_definition_id = item["parameter_definition_id"]
    except KeyError:
        raise SpineIntegrityError("Missing parameter identifier.")
    try:
        parameter_definition = parameter_definitions[parameter_definition_id]
    except KeyError:
        raise SpineIntegrityError("Parameter not found.")
    value = item.get("value")
    if value is not None:
        try:
            json.loads(value)
        except json.JSONDecodeError as err:
            raise SpineIntegrityError("Couldn't decode '{}' as JSON: {}".format(value, err))
        parameter_value_list_id = parameter_definition["parameter_value_list_id"]
        if parameter_value_list_id in parameter_value_lists:
            value_list = parameter_value_lists[parameter_value_list_id].split(",")
            if value and value not in value_list:
                valid_values = ", ".join(value_list)
                raise SpineIntegrityError(
                    "The value '{}' is not a valid value for parameter '{}' (valid values are: {})".format(
                        value, parameter_definition["name"], valid_values
                    )
                )
    object_id = item.get("object_id", None)
    relationship_id = item.get("relationship_id", None)
    if object_id and relationship_id:
        try:
            object_name = objects[object_id]["name"]
        except KeyError:
            object_name = "object id " + object_id
        try:
            relationship_name = relationships[relationship_id]["name"]
        except KeyError:
            relationship_name = "relationship id " + relationship_id
        raise SpineIntegrityError(
            "Can't associate a parameter value to both object '{}' and relationship '{}'.".format(
                object_name, relationship_name
            )
        )
    if object_id:
        try:
            object_class_id = objects[object_id]["class_id"]
        except KeyError:
            raise SpineIntegrityError("Object not found")
        if object_class_id != parameter_definition["object_class_id"]:
            object_name = objects[object_id]["name"]
            parameter_name = parameter_definition["name"]
            raise SpineIntegrityError("Incorrect object '{}' for parameter '{}'.".format(object_name, parameter_name))
        if (object_id, parameter_definition_id) in current_obj_items:
            object_name = objects[object_id]["name"]
            parameter_name = parameter_definition["name"]
            raise SpineIntegrityError(
                "The value of parameter '{}' for object '{}' is already specified.".format(parameter_name, object_name),
                id=current_obj_items[object_id, parameter_definition_id],
            )
    elif relationship_id:
        try:
            relationship_class_id = relationships[relationship_id]["class_id"]
        except KeyError:
            raise SpineIntegrityError("Relationship not found")
        if relationship_class_id != parameter_definition["relationship_class_id"]:
            relationship_name = relationships[relationship_id]["name"]
            parameter_name = parameter_definition["name"]
            raise SpineIntegrityError(
                "Incorrect relationship '{}' for parameter '{}'.".format(relationship_name, parameter_name)
            )
        if (relationship_id, parameter_definition_id) in current_rel_items:
            relationship_name = relationships[relationship_id]["name"]
            parameter_name = parameter_definition["name"]
            raise SpineIntegrityError(
                "The value of parameter '{}' for relationship '{}' is already specified.".format(
                    parameter_name, relationship_name
                ),
                id=current_rel_items[relationship_id, parameter_definition_id],
            )
    else:
        raise SpineIntegrityError("Missing object or relationship identifier.")


def check_parameter_tag(item, current_items):
    """Check whether the insertion of a parameter tag item
    results in the violation of an integrity constraint.

    :param dict item: A parameter tag item to be checked.
    :param dict current_items: A dictionary mapping tags to ids of parameter tags already in the database.

    :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.
    """
    try:
        tag = item["tag"]
    except KeyError:
        raise SpineIntegrityError("Missing parameter tag.")
    if tag in current_items:
        raise SpineIntegrityError("There can't be more than one '{}' tag.".format(tag), id=current_items[tag])


def check_parameter_definition_tag(item, current_items, parameter_names, parameter_tags):
    """Check whether the insertion of a parameter tag item
    results in the violation of an integrity constraint.

    :param dict item: A parameter tag item to be checked.
    :param dict current_items: A dictionary mapping tuples (parameter_definition_id, parameter_tag_id) to ids of
        parameter tags already in the database.
    :param dict parameter_names: A dictionary of parameter definition names in the database keyed by id.
    :param dict parameter_tags: A dictionary of parameter tags in the database keyed by id.

    :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.
    """
    try:
        parameter_definition_id = item["parameter_definition_id"]
    except KeyError:
        raise SpineIntegrityError("Missing parameter definition identifier.")
    try:
        parameter_tag_id = item["parameter_tag_id"]
    except KeyError:
        raise SpineIntegrityError("Missing parameter tag identifier.")
    try:
        parameter_name = parameter_names[parameter_definition_id]
    except KeyError:
        raise SpineIntegrityError("Parameter definition not found.")
    try:
        tag = parameter_tags[parameter_tag_id]
    except KeyError:
        raise SpineIntegrityError("Parameter tag not found.")
    if (parameter_definition_id, parameter_tag_id) in current_items:
        raise SpineIntegrityError(
            "Parameter '{0}' already has the tag '{1}'.".format(parameter_name, tag),
            id=current_items[parameter_definition_id, parameter_tag_id],
        )


def check_wide_parameter_value_list(wide_item, current_items):
    """Check whether the insertion of a parameter value-list item
    results in the violation of an integrity constraint.

    :param dict wide_item: A wide parameter value-list item to be checked.
    :param dict current_items: A dictionary mapping names to ids of parameter value-lists
        already in the database.

    :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.
    """
    try:
        name = wide_item["name"]
    except KeyError:
        raise SpineIntegrityError("Missing parameter value list name.")
    if name in current_items:
        raise SpineIntegrityError(
            "There can't be more than one parameter value_list called '{}'.".format(name), id=current_items[name]
        )
    try:
        value_list = wide_item["value_list"]
    except KeyError:
        raise SpineIntegrityError("Missing list of values.")
    if len(value_list) != len(set(value_list)):
        raise SpineIntegrityError("Values must be unique.")
    for value in value_list:
        if value is None:
            continue
        try:
            json.loads(value)
        except json.JSONDecodeError as err:
            raise SpineIntegrityError("Unable to decode value '{}' as JSON: {}".format(value, err))
