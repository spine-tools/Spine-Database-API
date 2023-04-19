######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
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

"""

from .parameter_value import dump_db_value, from_database, ParameterValueFormatError
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

    Args:
        item (dict: a scenario alternative item for checking; must contain the following fields:
            - "scenario_id": scenario's id
            - "alternative_id": alternative's id
            - "rank": alternative's rank within the scenario
        ids_by_alt_id (dict): a mapping from (scenario id, alternative id) tuples to scenario_alternative ids
            already in the database
        ids_by_rank (dict): a mapping from (scenario id, rank) tuples to scenario_alternative ranks already in the database
        scenario_names (Iterable): the names of existing scenarios in the database keyed by id
        alternative_names (Iterable): the names of existing alternatives in the database keyed by id

    Raises:
        SpineIntegrityError: if the insertion of the item violates an integrity constraint.
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
        raise SpineIntegrityError(f"Scenario with id {scen_id} does not have a name.")
    alt_name = alternative_names.get(alt_id)
    if alt_name is None:
        raise SpineIntegrityError(f"Alternative with id {alt_id} does not have a name.")
    dup_id = ids_by_alt_id.get((scen_id, alt_id))
    if dup_id is not None:
        raise SpineIntegrityError(f"Alternative {alt_name} already exists in scenario {scen_name}.", id=dup_id)
    dup_id = ids_by_rank.get((scen_id, rank))
    if dup_id is not None:
        raise SpineIntegrityError(
            f"Rank {rank} already exists in scenario {scen_name}. Cannot give the same rank for "
            f"alternative {alt_name}.",
            id=dup_id,
        )


def check_object_class(item, current_items, object_class_type):
    """Check whether the insertion of an object class item
    results in the violation of an integrity constraint.

    Args:
        item (dict): An object class item to be checked.
        current_items (dict): A dictionary mapping names to ids of object classes already in the database.

    Raises:
        SpineIntegrityError: if the insertion of the item violates an integrity constraint.
    """
    try:
        name = item["name"]
    except KeyError:
        raise SpineIntegrityError(
            "Python KeyError: There is no dictionary key for the object class name. Probably a bug, please report."
        )
    if not name:
        raise SpineIntegrityError("Object class name is an empty string and therefore not valid")
    if "type_id" in item and item["type_id"] != object_class_type:
        raise SpineIntegrityError(
            f"Object class '{name}' does not have a type_id of an object class.", id=current_items[name]
        )
    if name in current_items:
        raise SpineIntegrityError(f"There can't be more than one object class called '{name}'.", id=current_items[name])


def check_object(item, current_items, object_class_ids, object_entity_type):
    """Check whether the insertion of an object item
    results in the violation of an integrity constraint.

    Args:
        item (dict): An object item to be checked.
        current_items (dict): A dictionary mapping tuples (class_id, name) to ids of objects already in the database.
        object_class_ids (list): A list of object class ids in the database.

    Raises:
        SpineIntegrityError: if the insertion of the item violates an integrity constraint.
    """
    try:
        name = item["name"]
    except KeyError:
        raise SpineIntegrityError(
            "Python KeyError: There is no dictionary key for the object name. Probably a bug, please report."
        )
    if not name:
        raise SpineIntegrityError("Object name is an empty string and therefore not valid")
    if "type_id" in item and item["type_id"] != object_entity_type:
        raise SpineIntegrityError(f"Object '{name}' does not have entity type of and object", id=current_items[name])
    try:
        class_id = item["class_id"]
    except KeyError:
        raise SpineIntegrityError(f"Object '{name}' does not have an object class id.")
    if class_id not in object_class_ids:
        raise SpineIntegrityError(f"Object class id for object '{name}' not found.")
    if (class_id, name) in current_items:
        raise SpineIntegrityError(
            f"There's already an object called '{name}' in the same object class.", id=current_items[class_id, name]
        )


def check_wide_relationship_class(wide_item, current_items, object_class_ids, relationship_class_type):
    """Check whether the insertion of a relationship class item
    results in the violation of an integrity constraint.

    Args:
        wide_item (dict): A wide relationship class item to be checked.
        current_items (dict): A dictionary mapping names to ids of relationship classes already in the database.
        object_class_ids (list): A list of object class ids in the database.

    Raises:
        SpineIntegrityError: if the insertion of the item violates an integrity constraint.
    """
    try:
        name = wide_item["name"]
    except KeyError:
        raise SpineIntegrityError(
            "Python KeyError: There is no dictionary key for the relationship class name. "
            "Probably a bug, please report."
        )
    if not name:
        raise SpineIntegrityError(f"Name '{name}' is not valid")
    try:
        given_object_class_id_list = wide_item["object_class_id_list"]
    except KeyError:
        raise SpineIntegrityError(
            f"Python KeyError: There is no dictionary keys for the object class ids of relationship class '{name}'. "
            "Probably a bug, please report."
        )
    if not given_object_class_id_list:
        raise SpineIntegrityError(f"At least one object class is needed for the relationship class '{name}'.")
    if not all(id_ in object_class_ids for id_ in given_object_class_id_list):
        raise SpineIntegrityError(
            f"At least one of the object class ids of the relationship class '{name}' is not in the database."
        )
    if "type_id" in wide_item and wide_item["type_id"] != relationship_class_type:
        raise SpineIntegrityError(f"Relationship class '{name}' must have correct type_id .", id=current_items[name])
    if name in current_items:
        raise SpineIntegrityError(
            f"There can't be more than one relationship class with the name '{name}'.", id=current_items[name]
        )


def check_wide_relationship(
    wide_item, current_items_by_name, current_items_by_obj_lst, relationship_classes, objects, relationship_entity_type
):
    """Check whether the insertion of a relationship item
    results in the violation of an integrity constraint.

    Args:
        wide_item (dict): A wide relationship item to be checked.
        current_items_by_name (dict): A dictionary mapping tuples (class_id, name) to ids of
            relationships already in the database.
        current_items_by_obj_lst (dict): A dictionary mapping tuples (class_id, object_name_list) to ids
            of relationships already in the database.
        relationship_classes (dict): A dictionary of wide relationship class items in the database keyed by id.
        objects (dict): A dictionary of object items in the database keyed by id.

    Raises:
        SpineIntegrityError: if the insertion of the item violates an integrity constraint.
    """

    try:
        name = wide_item["name"]
    except KeyError:
        raise SpineIntegrityError(
            "Python KeyError: There is no dictionary key for the relationship name. Probably a bug, please report."
        )
    if not name:
        raise SpineIntegrityError("Relationship name is an empty string, which is not valid")
    try:
        class_id = wide_item["class_id"]
    except KeyError:
        raise SpineIntegrityError(
            f"Python KeyError: There is no dictionary key for the relationship class id of relationship '{name}'. "
            "Probably a bug, please report"
        )
    if "type_id" in wide_item and wide_item["type_id"] != relationship_entity_type:
        raise SpineIntegrityError(
            f"Relationship '{name}' does not have entity type of a relationship.",
            id=current_items_by_name[class_id, name],
        )
    if (class_id, name) in current_items_by_name:
        raise SpineIntegrityError(
            f"There's already a relationship called '{name}' in the same class.",
            id=current_items_by_name[class_id, name],
        )
    try:
        object_class_id_list = relationship_classes[class_id]["object_class_id_list"]
    except KeyError:
        raise SpineIntegrityError(f"There is no object class id list for relationship '{name}'")
    try:
        object_id_list = tuple(wide_item["object_id_list"])
    except KeyError:
        raise SpineIntegrityError(f"There is no object id list for relationship '{name}'")
    try:
        given_object_class_id_list = tuple(objects[id]["class_id"] for id in object_id_list)
    except KeyError:
        raise SpineIntegrityError(f"Some of the objects in relationship '{name}' are invalid.")
    if given_object_class_id_list != object_class_id_list:
        object_name_list = [objects[id]["name"] for id in object_id_list]
        relationship_class_name = relationship_classes[class_id]["name"]
        raise SpineIntegrityError(
            f"Incorrect objects '{object_name_list}' for relationship class '{relationship_class_name}'."
        )
    if (class_id, object_id_list) in current_items_by_obj_lst:
        object_name_list = [objects[id]["name"] for id in object_id_list]
        relationship_class_name = relationship_classes[class_id]["name"]
        raise SpineIntegrityError(
            "There's already a relationship between objects {} in class {}.".format(
                object_name_list, relationship_class_name
            ),
            id=current_items_by_obj_lst[class_id, object_id_list],
        )


def check_entity_group(item, current_items, entities):
    """Check whether the insertion of an entity group item
    results in the violation of an integrity constraint.

    Args:
        item (dict): An entity group item to be checked.
        current_items (dict): A dictionary mapping tuples (entity_id, member_id) to ids of entity groups
            already in the database.
        entities (dict): A dictionary mapping entity class ids, to entity ids, to entity items already in the db

    Raises:
        SpineIntegrityError: if the insertion of the item violates an integrity constraint.
    """
    try:
        entity_id = item["entity_id"]
    except KeyError:
        raise SpineIntegrityError(
            "Python KeyError: There is no dictionary key for the entity id of entity group. "
            "Probably a bug, please report."
        )
    try:
        member_id = item["member_id"]
    except KeyError:
        raise SpineIntegrityError(
            "Python KeyError: There is no dictionary key for the member id of an entity group. "
            "Probably a bug, please report."
        )
    try:
        entity_class_id = item["entity_class_id"]
    except KeyError:
        raise SpineIntegrityError(
            "Python KeyError: There is no dictionary key for the entity class id of entity group. "
            "Probably a bug, please report."
        )
    ents = entities.get(entity_class_id)
    if ents is None:
        raise SpineIntegrityError("Entity class not found for entity group.")
    entity = ents.get(entity_id)
    if not entity:
        raise SpineIntegrityError("No entity id for the entity group.")
    member = ents.get(member_id)
    if not member:
        raise SpineIntegrityError("Entity group has no members.")
    if (entity_id, member_id) in current_items:
        raise SpineIntegrityError(
            "{0} is already a member in {1}.".format(member["name"], entity["name"]),
            id=current_items[entity_id, member_id],
        )


def check_parameter_definition(item, current_items, entity_class_ids, parameter_value_lists, list_values):
    """Check whether the insertion of a parameter definition item
    results in the violation of an integrity constraint.

    Args:
        item (dict): A parameter definition item to be checked.
        current_items (dict): A dictionary mapping tuples (entity_class_id, name) to ids of parameter definitions
            already in the database.
        entity_class_ids (Iterable): A set of entity class ids in the database.
        parameter_value_lists (dict): A dictionary of value-lists in the database keyed by id.

    Raises:
        SpineIntegrityError: if the insertion of the item violates an integrity constraint.
    """
    name = item.get("name")
    if not name:
        raise SpineIntegrityError("No name provided for a parameter definition.")
    entity_class_id = item.get("entity_class_id")
    if not entity_class_id:
        raise SpineIntegrityError(f"Missing entity class id for parameter definition '{name}'.")
    if entity_class_id not in entity_class_ids:
        raise SpineIntegrityError(
            f"Entity class id for parameter definition '{name}' not found in the entity class "
            "ids of the current database."
        )
    if (entity_class_id, name) in current_items:
        raise SpineIntegrityError(
            "There's already a parameter called {0} in entity class with id {1}.".format(name, entity_class_id),
            id=current_items[entity_class_id, name],
        )
    replace_default_values_with_list_references(item, parameter_value_lists, list_values)


def check_parameter_value(
    item, current_items, parameter_definitions, entities, parameter_value_lists, list_values, alternatives
):
    """Check whether the insertion of a parameter value item results in the violation of an integrity constraint.

    Args:
        item (dict): A parameter value item to be checked.
        current_items (dict): A dictionary mapping tuples (entity_id, parameter_definition_id)
            to ids of parameter values already in the database.
        parameter_definitions (dict): A dictionary of parameter definition items in the database keyed by id.
        entities (dict): A dictionary of entity items already in the database keyed by id.
        parameter_value_lists (dict): A dictionary of value-lists in the database keyed by id.
        list_values (dict): A dictionary of list-values in the database keyed by id.
        alternatives (set)

    Raises:
        SpineIntegrityError: if the insertion of the item violates an integrity constraint.
    """
    try:
        parameter_definition_id = item["parameter_definition_id"]
    except KeyError:
        raise SpineIntegrityError("Missing parameter identifier.")
    try:
        parameter_definition = parameter_definitions[parameter_definition_id]
    except KeyError:
        raise SpineIntegrityError("Parameter not found.")
    alt_id = item.get("alternative_id")
    if alt_id not in alternatives:
        raise SpineIntegrityError("Alternative not found.")
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
    replace_parameter_values_with_list_references(item, parameter_definitions, parameter_value_lists, list_values)


def replace_default_values_with_list_references(item, parameter_value_lists, list_values):
    parameter_value_list_id = item.get("parameter_value_list_id")
    return _replace_values_with_list_references(
        "parameter_definition", item, parameter_value_list_id, parameter_value_lists, list_values
    )


def replace_parameter_values_with_list_references(item, parameter_definitions, parameter_value_lists, list_values):
    parameter_definition_id = item["parameter_definition_id"]
    parameter_definition = parameter_definitions[parameter_definition_id]
    parameter_value_list_id = parameter_definition["parameter_value_list_id"]
    return _replace_values_with_list_references(
        "parameter_value", item, parameter_value_list_id, parameter_value_lists, list_values
    )


def _replace_values_with_list_references(item_type, item, parameter_value_list_id, parameter_value_lists, list_values):
    if parameter_value_list_id is None:
        return False
    if parameter_value_list_id not in parameter_value_lists:
        raise SpineIntegrityError("Parameter value list not found.")
    value_id_list = parameter_value_lists[parameter_value_list_id]
    if value_id_list is None:
        raise SpineIntegrityError("Parameter value list is empty!")
    value_key, type_key = {
        "parameter_value": ("value", "type"),
        "parameter_definition": ("default_value", "default_type"),
    }[item_type]
    value = dict.get(item, value_key)
    value_type = dict.get(item, type_key)
    try:
        parsed_value = from_database(value, value_type)
    except ParameterValueFormatError as err:
        raise SpineIntegrityError(f"Invalid {value_key} '{value}': {err}") from None
    if parsed_value is None:
        return False
    list_value_id = next((id_ for id_ in value_id_list if list_values.get(id_) == parsed_value), None)
    if list_value_id is None:
        valid_values = ", ".join(f"{dump_db_value(list_values.get(id_))[0].decode('utf8')!r}" for id_ in value_id_list)
        raise SpineIntegrityError(
            f"Invalid {value_key} '{parsed_value}' - it should be one from the parameter value list: {valid_values}."
        )
    item[value_key] = str(list_value_id).encode("UTF8")
    item[type_key] = "list_value_ref"
    item["list_value_id"] = list_value_id
    return True


def check_parameter_value_list(item, current_items):
    """Check whether the insertion of a parameter value-list item results in the violation of an integrity constraint.

    Args:
        item (dict): A parameter value-list item to be checked.
        current_items (dict): A dictionary mapping names to ids of parameter value-lists already in the database.

    Raises:
        SpineIntegrityError: if the insertion of the item violates an integrity constraint.
    """
    try:
        name = item["name"]
    except KeyError:
        raise SpineIntegrityError("Missing parameter value list name.")
    if name in current_items:
        raise SpineIntegrityError(
            "There can't be more than one parameter value_list called '{}'.".format(name), id=current_items[name]
        )


def check_list_value(item, list_names_by_id, list_value_ids_by_index, list_value_ids_by_value):
    """Check whether the insertion of a list value item results in the violation of an integrity constraint.

    Args:
        item (dict): A list value item to be checked.
        list_names_by_id (dict): Mapping parameter value list ids to names.
        list_value_ids_by_index (dict): Mapping tuples (list id, index) to ids of existing list values.
        list_value_ids_by_value (dict): Mapping tuples (list id, type, value) to ids of existing list values.

    Raises:
        SpineIntegrityError: if the insertion of the item violates an integrity constraint.
    """
    keys = {"parameter_value_list_id", "index", "value", "type"}
    missing_keys = keys - item.keys()
    if missing_keys:
        raise SpineIntegrityError(f"Missing keys: {', '.join(missing_keys)}.")
    list_id = item["parameter_value_list_id"]
    list_name = list_names_by_id.get(list_id)
    if list_name is None:
        raise SpineIntegrityError("Unknown parameter value list identifier.")
    index = item["index"]
    type_ = item["type"]
    value = item["value"]
    dup_id = list_value_ids_by_index.get((list_id, index))
    if dup_id is not None:
        raise SpineIntegrityError(f"'{list_name}' already has the index '{index}'.", id=dup_id)
    dup_id = list_value_ids_by_value.get((list_id, type_, value))
    if dup_id is not None:
        raise SpineIntegrityError(f"'{list_name}' already has the value '{from_database(value, type_)}'.", id=dup_id)


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


def check_metadata(item, metadata):
    """Check whether the entity metadata item violates an integrity constraint.

    Args:
        item (dict): An entity metadata item to be checked.
        metadata (dict): Mapping from metadata name and value to metadata id.

    Raises:
        SpineIntegrityError: if the item violates an integrity constraint.
    """
    keys = {"name", "value"}
    missing_keys = keys - item.keys()
    if missing_keys:
        raise SpineIntegrityError(f"Missing keys: {', '.join(missing_keys)}.")


def check_entity_metadata(item, entities, metadata):
    """Check whether the entity metadata item violates an integrity constraint.

    Args:
        item (dict): An entity metadata item to be checked.
        entities (set of int): Available entity ids.
        metadata (set of int): Available metadata ids.

    Raises:
        SpineIntegrityError: if the item violates an integrity constraint.
    """
    keys = {"entity_id", "metadata_id"}
    missing_keys = keys - item.keys()
    if missing_keys:
        raise SpineIntegrityError(f"Missing keys: {', '.join(missing_keys)}.")
    if item["entity_id"] not in entities:
        raise SpineIntegrityError("Unknown entity identifier.")
    if item["metadata_id"] not in metadata:
        raise SpineIntegrityError("Unknown metadata identifier.")


def check_parameter_value_metadata(item, values, metadata):
    """Check whether the parameter value metadata item violates an integrity constraint.

    Args:
        item (dict): An entity metadata item to be checked.
        values (set of int): Available parameter value ids.
        metadata (set of int): Available metadata ids.

    Raises:
        SpineIntegrityError: if the item violates an integrity constraint.
    """
    keys = {"parameter_value_id", "metadata_id"}
    missing_keys = keys - item.keys()
    if missing_keys:
        raise SpineIntegrityError(f"Missing keys: {', '.join(missing_keys)}.")
    if item["parameter_value_id"] not in values:
        raise SpineIntegrityError("Unknown parameter value identifier.")
    if item["metadata_id"] not in metadata:
        raise SpineIntegrityError("Unknown metadata identifier.")
