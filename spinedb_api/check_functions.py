######################################################################################################################
# Copyright (C) 2017 - 2019 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""Functions for checking whether inserting data into a Spine database leads
to the violation of integrity constraints.

:author: Manuel Marin (KTH)
:date:   4.6.2019
"""

from .parameter_value import from_database, ParameterValueFormatError
from .exception import SpineIntegrityError

# NOTE: We parse each parameter value or default value before accepting it. Is it too much?


def check_alternative(item, current_items):
    try:
        name = item["name"]
    except KeyError:
        raise SpineIntegrityError("Missing alternative name.")
    if name in current_items:
        raise SpineIntegrityError(f"There can't be more than one alternative called '{name}'.", id=current_items[name])


def check_scenario(item, current_items):
    try:
        name = item["name"]
    except KeyError:
        raise SpineIntegrityError("Missing scenario name.")
    if name in current_items:
        raise SpineIntegrityError(f"There can't be more than one scenario called '{name}'.", id=current_items[name])


def check_scenario_alternative(item, ids_by_alt_id, ids_by_rank, scenario_names, alternative_names):
    """
    Checks if given scenario alternative violates a database's integrity.

    :param dict item: a scenario alternative item for checking; must contain the following fields:

        - "scenario_id": scenario's id
        - "alternative_id": alternative's id
        - "rank": alternative's rank within the scenario

    :param dict ids_by_alt_id: a mapping from (scenario id, alternative id) tuples to scenario_alternative ids
        that already exist in the database
    :param dict ids_by_rank: a mapping from (scenario id, rank) tuples to scenario_alternative ranks
        that already exist in the database
    :param Iterable scenario_names: the names of existing scenarios in the database keyed by id
    :param Iterable alternative_names: the names of existing alternatives in the database keyed by id
    :raises SpineIntegrityError: if insertion of ``item`` would violate database's integrity
    """
    try:
        scen_id = item["scenario_id"]
    except KeyError:
        raise SpineIntegrityError("Missing scenario identifier.")
    try:
        alt_id = item["alternative_id"]
    except KeyError:
        raise SpineIntegrityError("Missing alternative identifier.")
    try:
        rank = item["rank"]
    except KeyError:
        raise SpineIntegrityError("Missing scenario alternative rank.")
    scen_name = scenario_names.get(scen_id)
    if scen_name is None:
        raise SpineIntegrityError("Scenario not found.")
    alt_name = alternative_names.get(alt_id)
    if alt_name is None:
        raise SpineIntegrityError("Alternative not found.")
    dup_id = ids_by_alt_id.get((scen_id, alt_id))
    if dup_id is not None:
        raise SpineIntegrityError(f"Alternative {alt_name} already exists in scenario {scen_name}.", id=dup_id)
    dup_id = ids_by_rank.get((scen_id, rank))
    if dup_id is not None:
        raise SpineIntegrityError(f"Rank {rank} already exists in scenario {scen_name}.", id=dup_id)


def check_object_class(item, current_items, object_class_type):
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
    if not name:
        raise SpineIntegrityError(f"Name '{name}' is not valid")
    if "type_id" in item and item["type_id"] != object_class_type:
        raise SpineIntegrityError("Object class '{}' must have correct type_id.".format(name), id=current_items[name])
    if name in current_items:
        raise SpineIntegrityError(
            "There can't be more than one object class called '{}'.".format(name), id=current_items[name]
        )


def check_object(item, current_items, object_class_ids, object_entity_type):
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
    if not name:
        raise SpineIntegrityError(f"Name '{name}' is not valid")
    if "type_id" in item and item["type_id"] != object_entity_type:
        raise SpineIntegrityError("Object '{}' must have correct type_id.".format(name), id=current_items[name])
    if (class_id, name) in current_items:
        raise SpineIntegrityError(
            "There's already an object called '{}' in the same class.".format(name), id=current_items[class_id, name]
        )


def check_wide_relationship_class(wide_item, current_items, object_class_ids, relationship_class_type):
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
    if not given_object_class_id_list:
        raise SpineIntegrityError("At least one object class is needed.")
    if not all([id_ in object_class_ids for id_ in given_object_class_id_list]):
        raise SpineIntegrityError("Object class not found.")
    try:
        name = wide_item["name"]
    except KeyError:
        raise SpineIntegrityError("Missing relationship class name.")
    if not name:
        raise SpineIntegrityError(f"Name '{name}' is not valid")
    if "type_id" in wide_item and wide_item["type_id"] != relationship_class_type:
        raise SpineIntegrityError(
            "Relationship class '{}' must have correct type_id .".format(name), id=current_items[name]
        )
    if name in current_items:
        raise SpineIntegrityError(
            "There can't be more than one relationship class called '{}'.".format(name), id=current_items[name]
        )


def check_wide_relationship(
    wide_item, current_items_by_name, current_items_by_obj_lst, relationship_classes, objects, relationship_entity_type
):
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
    if not name:
        raise SpineIntegrityError(f"Name '{name}' is not valid")
    try:
        class_id = wide_item["class_id"]
    except KeyError:
        raise SpineIntegrityError("Missing relationship class identifier.")
    if "type_id" in wide_item and wide_item["type_id"] != relationship_entity_type:
        raise SpineIntegrityError(
            "Relationship '{}' must have correct type_id .".format(name), id=current_items_by_name[class_id, name]
        )
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


def check_entity_group(item, current_items, entities):
    """Check whether the insertion of an entity group item
    results in the violation of an integrity constraint.

    :param dict item: An entity group item to be checked.
    :param dict current_items: A dictionary mapping tuples (entity_id, member_id) to ids
        of entity groups already in the database.
    :param dict entities: A dictionary mapping entity class ids, to entity ids, to entity items
        already in the db
    """
    try:
        entity_id = item["entity_id"]
    except KeyError:
        raise SpineIntegrityError("Missing entity identifier.")
    try:
        member_id = item["member_id"]
    except KeyError:
        raise SpineIntegrityError("Missing member identifier.")
    try:
        entity_class_id = item["entity_class_id"]
    except KeyError:
        raise SpineIntegrityError("Missing entity class identifier.")
    ents = entities.get(entity_class_id)
    if ents is None:
        raise SpineIntegrityError("Entity class not found.")
    entity = ents.get(entity_id)
    if not entity:
        raise SpineIntegrityError("Entity not found.")
    member = ents.get(member_id)
    if not member:
        raise SpineIntegrityError("Member not found.")
    if (entity_id, member_id) in current_items:
        raise SpineIntegrityError(
            "{0} is already a member in {1}.".format(member["name"], entity["name"]),
            id=current_items[entity_id, member_id],
        )


def check_parameter_definition(item, current_items, entity_class_ids, parameter_value_lists):
    """Check whether the insertion of a parameter definition item
    results in the violation of an integrity constraint.

    :param dict item: A parameter definition item to be checked.
    :param dict current_items: A dictionary mapping tuples (entity_class_id, name) to ids
        of parameter definitions already in the database.
    :param dict entity_class_ids: A set of entity class ids in the database.
    :param dict parameter_value_lists: A dictionary of value-lists in the database keyed by id.

    :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.
    """
    name = item.get("name")
    if not name:
        raise SpineIntegrityError("Missing parameter name.")
    entity_class_id = item.get("entity_class_id")
    if not entity_class_id:
        raise SpineIntegrityError("Missing entity class identifier.")
    if entity_class_id not in entity_class_ids:
        raise SpineIntegrityError("Entity class not found.")
    if (entity_class_id, name) in current_items:
        raise SpineIntegrityError(
            "There's already a parameter called '{}' in this class.".format(name),
            id=current_items[entity_class_id, name],
        )
    parameter_value_list_id = item.get("parameter_value_list_id")
    if parameter_value_list_id is not None and parameter_value_list_id not in parameter_value_lists:
        raise SpineIntegrityError("Invalid parameter value list.")
    default_value = item.get("default_value")
    try:
        _ = from_database(default_value)
    except ParameterValueFormatError as err:
        raise SpineIntegrityError("Invalid default value '{}': {}".format(default_value, err))


def check_parameter_value(item, current_items, parameter_definitions, entities, parameter_value_lists, alternatives):
    """Check whether the insertion of a parameter value item results in the violation of an integrity constraint.

    :param dict item: A parameter value item to be checked.
    :param dict current_items: A dictionary mapping tuples (entity_id, parameter_definition_id) to ids of
        parameter values already in the database.
    :param dict parameter_definitions: A dictionary of parameter definition items in the database keyed by id.
    :param dict entities: A dictionary of entity items already in the database keyed by id.
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
    alt_id = item.get("alternative_id")
    if alt_id not in alternatives:
        raise SpineIntegrityError("Alternative not found.")
    try:
        _ = from_database(value)
    except ParameterValueFormatError as err:
        raise SpineIntegrityError("Invalid value '{}': {}".format(value, err))
    if value is not None:
        parameter_value_list_id = parameter_definition["parameter_value_list_id"]
        value_list = parameter_value_lists.get(parameter_value_list_id)
        if value_list is not None:
            value_list = value_list.split(";")
            if value not in value_list:
                valid_values = ", ".join(value_list)
                raise SpineIntegrityError(
                    "The value '{}' is not a valid value for parameter '{}' (valid values are: {})".format(
                        value, parameter_definition["name"], valid_values
                    )
                )
    entity_id = item.get("entity_id")
    if not entity_id:
        raise SpineIntegrityError("Missing object or relationship identifier.")
    try:
        entity_class_id = entities[entity_id]["class_id"]
    except KeyError:
        raise SpineIntegrityError("Entity not found")
    if entity_class_id != parameter_definition["entity_class_id"]:
        entity_name = entities[entity_id]["name"]
        parameter_name = parameter_definition["name"]
        raise SpineIntegrityError("Incorrect entity '{}' for parameter '{}'.".format(entity_name, parameter_name))
    if (entity_id, parameter_definition_id, alt_id) in current_items:
        entity_name = entities[entity_id]["name"]
        parameter_name = parameter_definition["name"]
        raise SpineIntegrityError(
            "The value of parameter '{}' for entity '{}' is already specified.".format(parameter_name, entity_name),
            id=current_items[entity_id, parameter_definition_id, alt_id],
        )


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
    if not tag:
        raise SpineIntegrityError(f"Tag '{tag}' is not valid")
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
        try:
            _ = from_database(value)
        except ParameterValueFormatError as err:
            raise SpineIntegrityError("Invalid value '{}': {}".format(value, err))


def check_tool(item, current_items):
    try:
        name = item["name"]
    except KeyError:
        raise SpineIntegrityError("Missing tool name.")
    if name in current_items:
        raise SpineIntegrityError(f"There can't be more than one tool called '{name}'.", id=current_items[name])


def check_feature(item, current_items, parameter_definitions):
    try:
        parameter_definition_id = item["parameter_definition_id"]
    except KeyError:
        raise SpineIntegrityError("Missing parameter identifier.")
    try:
        parameter_value_list_id = item["parameter_value_list_id"]
    except KeyError:
        raise SpineIntegrityError("Missing parameter value list identifier.")
    try:
        parameter_definition = parameter_definitions[parameter_definition_id]
    except KeyError:
        raise SpineIntegrityError("Parameter not found.")
    if parameter_value_list_id is None:
        raise SpineIntegrityError(f"Parameter '{parameter_definition['name']}' doesn't have a value list.")
    if parameter_value_list_id != parameter_definition["parameter_value_list_id"]:
        raise SpineIntegrityError("Parameter definition and value list don't match.")
    if parameter_definition_id in current_items:
        raise SpineIntegrityError(
            f"There's already a feature defined for parameter '{parameter_definition['name']}'.",
            id=current_items[parameter_definition_id],
        )


def check_tool_feature(item, current_items, tools, features):
    try:
        tool_id = item["tool_id"]
    except KeyError:
        raise SpineIntegrityError("Missing tool identifier.")
    try:
        feature_id = item["feature_id"]
    except KeyError:
        raise SpineIntegrityError("Missing feature identifier.")
    try:
        parameter_value_list_id = item["parameter_value_list_id"]
    except KeyError:
        raise SpineIntegrityError("Missing parameter value list identifier.")
    try:
        tool = tools[tool_id]
    except KeyError:
        raise SpineIntegrityError("Tool not found.")
    try:
        feature = features[feature_id]
    except KeyError:
        raise SpineIntegrityError("Feature not found.")
    dup_id = current_items.get((tool_id, feature_id))
    if dup_id is not None:
        raise SpineIntegrityError(f"Tool '{tool['name']}' already has feature '{feature['name']}'.", id=dup_id)
    if parameter_value_list_id != feature["parameter_value_list_id"]:
        raise SpineIntegrityError("Feature and parameter value list don't match.")


def check_tool_feature_method(item, current_items, tool_features, parameter_value_lists):
    try:
        tool_feature_id = item["tool_feature_id"]
    except KeyError:
        raise SpineIntegrityError("Missing tool feature identifier.")
    try:
        parameter_value_list_id = item["parameter_value_list_id"]
    except KeyError:
        raise SpineIntegrityError("Missing parameter value list identifier.")
    try:
        method_index = item["method_index"]
    except KeyError:
        raise SpineIntegrityError("Missing method index.")
    try:
        tool_feature = tool_features[tool_feature_id]
    except KeyError:
        raise SpineIntegrityError("Tool feature not found.")
    try:
        parameter_value_list = parameter_value_lists[parameter_value_list_id]
    except KeyError:
        raise SpineIntegrityError("Parameter value list not found.")
    dup_id = current_items.get((tool_feature_id, method_index))
    if dup_id is not None:
        raise SpineIntegrityError("Tool feature already has the given method.", id=dup_id)
    if parameter_value_list_id != tool_feature["parameter_value_list_id"]:
        raise SpineIntegrityError("Feature and parameter value list don't match.")
    if method_index not in parameter_value_list["value_index_list"]:
        raise SpineIntegrityError("Invalid method for tool feature.")
