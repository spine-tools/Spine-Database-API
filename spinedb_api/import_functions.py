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
Functions for importing data into a Spine database using entity names as references.

"""

import uuid
from .exception import SpineIntegrityError, SpineDBAPIError
from .check_functions import (
    check_tool,
    check_feature,
    check_tool_feature,
    check_tool_feature_method,
    check_alternative,
    check_object_class,
    check_object,
    check_wide_relationship_class,
    check_wide_relationship,
    check_entity_group,
    check_parameter_definition,
    check_parameter_value,
    check_scenario,
    check_parameter_value_list,
    check_list_value,
)
from .parameter_value import to_database, from_database, fix_conflict
from .helpers import _parse_metadata

# TODO: update docstrings


class ImportErrorLogItem:
    """Class to hold log data for import errors"""

    def __init__(self, msg="", db_type="", imported_from="", other=""):
        self.msg = msg
        self.db_type = db_type
        self.imported_from = imported_from
        self.other = other

    def __repr__(self):
        return self.msg


def import_data(db_map, make_cache=None, unparse_value=to_database, on_conflict="merge", **kwargs):
    """Imports data into a Spine database using name references (rather than id references).

    Example::

            object_c = ['example_class', 'other_class']
            obj_parameters = [['example_class', 'example_parameter']]
            relationship_c = [['example_rel_class', ['example_class', 'other_class']]]
            rel_parameters = [['example_rel_class', 'rel_parameter']]
            objects = [['example_class', 'example_object'],
                       ['other_class', 'other_object']]
            object_p_values = [['example_object_class', 'example_object', 'example_parameter', 3.14]]
            relationships = [['example_rel_class', ['example_object', 'other_object']]]
            rel_p_values = [['example_rel_class', ['example_object', 'other_object'], 'rel_parameter', 2.718]]
            object_groups = [['object_class_name', 'object_group_name', ['member_name', 'another_member_name']]]
            alternatives = [['example_alternative', 'An example']]
            scenarios = [['example_scenario', 'An example']]
            scenario_alternatives = [('scenario', 'alternative1'), ('scenario', 'alternative0', 'alternative1')]
            tools = [('tool1', 'Tool one description'), ('tool2', 'Tool two description']]

            import_data(db_map,
                        object_classes=object_c,
                        relationship_classes=relationship_c,
                        object_parameters=obj_parameters,
                        relationship_parameters=rel_parameters,
                        objects=objects,
                        relationships=relationships,
                        object_groups=object_groups,
                        object_parameter_values=object_p_values,
                        relationship_parameter_values=rel_p_values,
                        alternatives=alternatives,
                        scenarios=scenarios,
                        scenario_alternatives=scenario_alternatives
                        tools=tools)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): database mapping
        on_conflict (str): Conflict resolution strategy for ``parameter_value.fix_conflict``
        object_classes (List[str]): List of object class names
        relationship_classes (List[List[str, List(str)]):
            List of lists with relationship class names and list of object class names
        object_parameters (List[List[str, str]]):
            list of lists with object class name and parameter name
        relationship_parameters (List[List[str, str]]):
            list of lists with relationship class name and parameter name
        objects (List[List[str, str]]):
            list of lists with object class name and object name
        relationships: (List[List[str,List(String)]]):
            list of lists with relationship class name and list of object names
        object_groups (List[List/Tuple]): list/set/iterable of lists/tuples with object class name, group name,
            and member name
        object_parameter_values (List[List[str, str, str|numeric]]):
            list of lists with object name, parameter name, parameter value
        relationship_parameter_values (List[List[str, List(str), str, str|numeric]]):
            list of lists with relationship class name, list of object names, parameter name, parameter value
        alternatives (Iterable): alternative names or lists of two elements: alternative name and description
        scenarios (Iterable): scenario names or lists of two elements: scenario name and description
        scenario_alternatives (Iterable): lists of two elements: scenario name and a list of names of alternatives

    Returns:
        tuple: number of inserted/changed entities and list of ImportErrorLogItem with
            any import errors
    """
    add_items_by_tablename = {
        "alternative": db_map._add_alternatives,
        "scenario": db_map._add_scenarios,
        "scenario_alternative": db_map._add_scenario_alternatives,
        "object_class": db_map._add_object_classes,
        "relationship_class": db_map._add_wide_relationship_classes,
        "parameter_value_list": db_map._add_parameter_value_lists,
        "list_value": db_map._add_list_values,
        "parameter_definition": db_map._add_parameter_definitions,
        "feature": db_map._add_features,
        "tool": db_map._add_tools,
        "tool_feature": db_map._add_tool_features,
        "tool_feature_method": db_map._add_tool_feature_methods,
        "object": db_map._add_objects,
        "relationship": db_map._add_wide_relationships,
        "entity_group": db_map._add_entity_groups,
        "parameter_value": db_map._add_parameter_values,
        "metadata": db_map._add_metadata,
        "entity_metadata": db_map._add_entity_metadata,
        "parameter_value_metadata": db_map._add_parameter_value_metadata,
    }
    update_items_by_tablename = {
        "alternative": db_map._update_alternatives,
        "scenario": db_map._update_scenarios,
        "scenario_alternative": db_map._update_scenario_alternatives,
        "object_class": db_map._update_object_classes,
        "relationship_class": db_map._update_wide_relationship_classes,
        "parameter_value_list": db_map._update_parameter_value_lists,
        "list_value": db_map._update_list_values,
        "parameter_definition": db_map._update_parameter_definitions,
        "feature": db_map._update_features,
        "tool": db_map._update_tools,
        "tool_feature": db_map._update_tool_features,
        "object": db_map._update_objects,
        "parameter_value": db_map._update_parameter_values,
    }
    error_log = []
    num_imports = 0
    for tablename, (to_add, to_update, errors) in get_data_for_import(
        db_map, make_cache=make_cache, unparse_value=unparse_value, on_conflict=on_conflict, **kwargs
    ):
        update_items = update_items_by_tablename.get(tablename, lambda *args, **kwargs: ())
        try:
            updated = update_items(*to_update)
        except SpineDBAPIError as error:
            updated = []
            error_log.append(ImportErrorLogItem(msg=str(error), db_type=tablename))
        add_items = add_items_by_tablename[tablename]
        try:
            added = add_items(*to_add)
        except SpineDBAPIError as error:
            added = []
            error_log.append(ImportErrorLogItem(msg=str(error), db_type=tablename))
        num_imports += len(added) + len(updated)
        error_log.extend(errors)
    return num_imports, error_log


def get_data_for_import(
    db_map,
    make_cache=None,
    unparse_value=to_database,
    on_conflict="merge",
    object_classes=(),
    relationship_classes=(),
    parameter_value_lists=(),
    object_parameters=(),
    relationship_parameters=(),
    objects=(),
    relationships=(),
    object_groups=(),
    object_parameter_values=(),
    relationship_parameter_values=(),
    alternatives=(),
    scenarios=(),
    scenario_alternatives=(),
    features=(),
    tools=(),
    tool_features=(),
    tool_feature_methods=(),
    metadata=(),
    object_metadata=(),
    relationship_metadata=(),
    object_parameter_value_metadata=(),
    relationship_parameter_value_metadata=(),
):
    """Returns an iterator of data for import, that the user can call instead of `import_data`
    if they want to add and update the data by themselves.
    Especially intended to be used with the toolbox undo/redo functionality.

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): database mapping
        on_conflict (str): Conflict resolution strategy for ``parameter_value.fix_conflict``
        object_classes (List[str]): List of object class names
        relationship_classes (List[List[str, List(str)]):
            List of lists with relationship class names and list of object class names
        object_parameters (List[List[str, str]]):
            list of lists with object class name and parameter name
        relationship_parameters (List[List[str, str]]):
            list of lists with relationship class name and parameter name
        objects (List[List[str, str]]):
            list of lists with object class name and object name
        relationships: (List[List[str,List(String)]]):
            list of lists with relationship class name and list of object names
        object_groups (List[List/Tuple]): list/set/iterable of lists/tuples with object class name, group name,
            and member name
        object_parameter_values (List[List[str, str, str|numeric]]):
            list of lists with object name, parameter name, parameter value
        relationship_parameter_values (List[List[str, List(str), str, str|numeric]]):
            list of lists with relationship class name, list of object names, parameter name,
            parameter value

    Returns:
        dict(str, list)
    """
    if make_cache is None:
        make_cache = db_map.make_cache
    # NOTE: The order is important, because of references. E.g., we want to import alternatives before parameter_values
    if alternatives:
        yield ("alternative", _get_alternatives_for_import(alternatives, make_cache))
    if scenarios:
        yield ("scenario", _get_scenarios_for_import(scenarios, make_cache))
    if scenario_alternatives:
        if not scenarios:
            scenarios = (item[0] for item in scenario_alternatives)
            yield ("scenario", _get_scenarios_for_import(scenarios, make_cache))
        if not alternatives:
            alternatives = (item[1] for item in scenario_alternatives)
            yield ("alternative", _get_alternatives_for_import(alternatives, make_cache))
        yield ("scenario_alternative", _get_scenario_alternatives_for_import(scenario_alternatives, make_cache))
    if object_classes:
        yield ("object_class", _get_object_classes_for_import(db_map, object_classes, make_cache))
    if relationship_classes:
        yield ("relationship_class", _get_relationship_classes_for_import(db_map, relationship_classes, make_cache))
    if parameter_value_lists:
        yield ("parameter_value_list", _get_parameter_value_lists_for_import(db_map, parameter_value_lists, make_cache))
        yield ("list_value", _get_list_values_for_import(db_map, parameter_value_lists, make_cache, unparse_value))
    if object_parameters:
        yield (
            "parameter_definition",
            _get_object_parameters_for_import(db_map, object_parameters, make_cache, unparse_value),
        )
    if relationship_parameters:
        yield (
            "parameter_definition",
            _get_relationship_parameters_for_import(db_map, relationship_parameters, make_cache, unparse_value),
        )
    if features:
        yield ("feature", _get_features_for_import(db_map, features, make_cache))
    if tools:
        yield ("tool", _get_tools_for_import(db_map, tools, make_cache))
    if tool_features:
        yield ("tool_feature", _get_tool_features_for_import(db_map, tool_features, make_cache))
    if tool_feature_methods:
        yield (
            "tool_feature_method",
            _get_tool_feature_methods_for_import(db_map, tool_feature_methods, make_cache, unparse_value),
        )
    if objects:
        yield ("object", _get_objects_for_import(db_map, objects, make_cache))
    if relationships:
        yield ("relationship", _get_relationships_for_import(db_map, relationships, make_cache))
    if object_groups:
        yield ("entity_group", _get_object_groups_for_import(db_map, object_groups, make_cache))
    if object_parameter_values:
        yield (
            "parameter_value",
            _get_object_parameter_values_for_import(
                db_map, object_parameter_values, make_cache, unparse_value, on_conflict
            ),
        )
    if relationship_parameter_values:
        yield (
            "parameter_value",
            _get_relationship_parameter_values_for_import(
                db_map, relationship_parameter_values, make_cache, unparse_value, on_conflict
            ),
        )
    if metadata:
        yield ("metadata", _get_metadata_for_import(db_map, metadata, make_cache))
    if object_metadata:
        yield ("entity_metadata", _get_object_metadata_for_import(db_map, object_metadata, make_cache))
    if relationship_metadata:
        yield ("entity_metadata", _get_relationship_metadata_for_import(db_map, relationship_metadata, make_cache))
    if object_parameter_value_metadata:
        yield (
            "parameter_value_metadata",
            _get_object_parameter_value_metadata_for_import(db_map, object_parameter_value_metadata, make_cache),
        )
    if relationship_parameter_value_metadata:
        yield (
            "parameter_value_metadata",
            _get_relationship_parameter_value_metadata_for_import(
                db_map, relationship_parameter_value_metadata, make_cache
            ),
        )


def import_features(db_map, data, make_cache=None):
    """
    Imports features.

    Example:

        data = [('class', 'parameter'), ('another_class', 'another_parameter', 'description')]
        import_features(db_map, data)

    Args:
        db_map (DiffDatabaseMapping): mapping for database to insert into
        data (Iterable): an iterable of lists/tuples with class name, parameter name, and optionally description

    Returns:
        tuple of int and list: Number of successfully inserted features, list of errors
    """
    return import_data(db_map, features=data, make_cache=make_cache)


def _get_features_for_import(db_map, data, make_cache):
    cache = make_cache({"feature"}, include_ancestors=True)
    feature_ids = {x.parameter_definition_id: x.id for x in cache.get("feature", {}).values()}
    parameter_ids = {
        (x.entity_class_name, x.parameter_name): (x.id, x.value_list_id)
        for x in cache.get("parameter_definition", {}).values()
    }
    parameter_definitions = {
        x.id: {
            "name": x.parameter_name,
            "entity_class_id": x.entity_class_id,
            "parameter_value_list_id": x.value_list_id,
        }
        for x in cache.get("parameter_definition", {}).values()
    }
    checked = set()
    to_add = []
    to_update = []
    error_log = []
    for class_name, parameter_name, *optionals in data:
        parameter_definition_id, parameter_value_list_id = parameter_ids.get((class_name, parameter_name), (None, None))
        if parameter_definition_id in checked:
            continue
        feature_id = feature_ids.pop(parameter_definition_id, None)
        item = (
            cache["feature"][feature_id]._asdict()
            if feature_id is not None
            else {
                "parameter_definition_id": parameter_definition_id,
                "parameter_value_list_id": parameter_value_list_id,
                "description": None,
            }
        )
        item.update(dict(zip(("description",), optionals)))
        try:
            check_feature(item, feature_ids, parameter_definitions)
        except SpineIntegrityError as e:
            error_log.append(
                ImportErrorLogItem(
                    msg=f"Could not import feature '{class_name, parameter_name}': {e.msg}", db_type="feature"
                )
            )
            continue
        finally:
            if feature_id is not None:
                feature_ids[parameter_definition_id] = feature_id
        checked.add(parameter_definition_id)
        if feature_id is not None:
            item["id"] = feature_id
            to_update.append(item)
        else:
            to_add.append(item)
    return to_add, to_update, error_log


def import_tools(db_map, data, make_cache=None):
    """
    Imports tools.

    Example:

        data = ['tool', ('another_tool', 'description')]
        import_tools(db_map, data)

    Args:
        db_map (DiffDatabaseMapping): mapping for database to insert into
        data (Iterable): an iterable of tool names,
            or of lists/tuples with tool names and optional descriptions

    Returns:
        tuple of int and list: Number of successfully inserted tools, list of errors
    """
    return import_data(db_map, tools=data, make_cache=make_cache)


def _get_tools_for_import(db_map, data, make_cache):
    cache = make_cache({"tool"}, include_ancestors=True)
    tool_ids = {tool.name: tool.id for tool in cache.get("tool", {}).values()}
    checked = set()
    to_add = []
    to_update = []
    error_log = []
    for tool in data:
        if isinstance(tool, str):
            tool = (tool,)
        name, *optionals = tool
        if name in checked:
            continue
        tool_id = tool_ids.pop(name, None)
        item = cache["tool"][tool_id]._asdict() if tool_id is not None else {"name": name, "description": None}
        item.update(dict(zip(("description",), optionals)))
        try:
            check_tool(item, tool_ids)
        except SpineIntegrityError as e:
            error_log.append(ImportErrorLogItem(msg=f"Could not import tool '{name}': {e.msg}", db_type="tool"))
            continue
        finally:
            if tool_id is not None:
                tool_ids[name] = tool_id
        checked.add(name)
        if tool_id is not None:
            item["id"] = tool_id
            to_update.append(item)
        else:
            to_add.append(item)
    return to_add, to_update, error_log


def import_tool_features(db_map, data, make_cache=None):
    """
    Imports tool features.

    Example:

        data = [('tool', 'class', 'parameter'), ('another_tool', 'another_class', 'another_parameter', 'required')]
        import_tool_features(db_map, data)

    Args:
        db_map (DiffDatabaseMapping): mapping for database to insert into
        data (Iterable): an iterable of lists/tuples with tool name, class name, parameter name,
            and optionally description

    Returns:
        tuple of int and list: Number of successfully inserted tool features, list of errors
    """
    return import_data(db_map, tool_features=data, make_cache=make_cache)


def _get_tool_features_for_import(db_map, data, make_cache):
    cache = make_cache({"tool_feature"}, include_ancestors=True)
    tool_feature_ids = {(x.tool_id, x.feature_id): x.id for x in cache.get("tool_feature", {}).values()}
    tool_ids = {x.name: x.id for x in cache.get("tool", {}).values()}
    feature_ids = {
        (x.entity_class_name, x.parameter_definition_name): (x.id, x.parameter_value_list_id)
        for x in cache.get("feature", {}).values()
    }
    tools = {x.id: x._asdict() for x in cache.get("tool", {}).values()}
    features = {
        x.id: {
            "name": x.entity_class_name + "/" + x.parameter_definition_name,
            "parameter_value_list_id": x.parameter_value_list_id,
        }
        for x in cache.get("feature", {}).values()
    }
    checked = set()
    to_add = []
    to_update = []
    error_log = []
    for tool_name, class_name, parameter_name, *optionals in data:
        tool_id = tool_ids.get(tool_name)
        feature_id, parameter_value_list_id = feature_ids.get((class_name, parameter_name), (None, None))
        if (tool_id, feature_id) in checked:
            continue
        tool_feature_id = tool_feature_ids.pop((tool_id, feature_id), None)
        item = (
            cache["tool_feature"][tool_feature_id]._asdict()
            if tool_feature_id is not None
            else {
                "tool_id": tool_id,
                "feature_id": feature_id,
                "parameter_value_list_id": parameter_value_list_id,
                "required": False,
            }
        )
        item.update(dict(zip(("required",), optionals)))
        try:
            check_tool_feature(item, tool_feature_ids, tools, features)
        except SpineIntegrityError as e:
            error_log.append(
                ImportErrorLogItem(
                    msg=f"Could not import tool feature '{tool_name, class_name, parameter_name}': {e.msg}",
                    db_type="tool_feature",
                )
            )
            continue
        finally:
            if tool_feature_id is not None:
                tool_feature_ids[tool_id, feature_id] = tool_feature_id
        checked.add((tool_id, feature_id))
        if tool_feature_id is not None:
            item["id"] = tool_feature_id
            to_update.append(item)
        else:
            to_add.append(item)
    return to_add, to_update, error_log


def import_tool_feature_methods(db_map, data, make_cache=None, unparse_value=to_database):
    """
    Imports tool feature methods.

    Example:

        data = [('tool', 'class', 'parameter', 'method'), ('another_tool', 'another_class', 'another_parameter', 'another_method')]
        import_tool_features(db_map, data)

    Args:
        db_map (DiffDatabaseMapping): mapping for database to insert into
        data (Iterable): an iterable of lists/tuples with tool name, class name, parameter name, and method

    Returns:
        tuple of int and list: Number of successfully inserted tool features, list of errors
    """
    return import_data(db_map, tool_feature_methods=data, make_cache=make_cache, unparse_value=unparse_value)


def _get_tool_feature_methods_for_import(db_map, data, make_cache, unparse_value):
    cache = make_cache({"tool_feature_method"}, include_ancestors=True)
    tool_feature_method_ids = {
        (x.tool_feature_id, x.method_index): x.id for x in cache.get("tool_feature_method", {}).values()
    }
    tool_feature_ids = {
        (x.tool_name, x.entity_class_name, x.parameter_definition_name): (x.id, x.parameter_value_list_id)
        for x in cache.get("tool_feature", {}).values()
    }
    tool_features = {x.id: x._asdict() for x in cache.get("tool_feature", {}).values()}
    parameter_value_lists = {
        x.id: {"name": x.name, "value_index_list": x.value_index_list}
        for x in cache.get("parameter_value_list", {}).values()
    }
    list_values = {
        (x.parameter_value_list_id, x.index): from_database(x.value, x.type)
        for x in cache.get("list_value", {}).values()
    }
    seen = set()
    to_add = []
    error_log = []
    for tool_name, class_name, parameter_name, method in data:
        tool_feature_id, parameter_value_list_id = tool_feature_ids.get(
            (tool_name, class_name, parameter_name), (None, None)
        )
        parameter_value_list = parameter_value_lists.get(parameter_value_list_id, {})
        value_index_list = parameter_value_list.get("value_index_list", [])
        method = from_database(*unparse_value(method))
        method_index = next(
            iter(index for index in value_index_list if list_values.get((parameter_value_list_id, index)) == method),
            None,
        )
        if (tool_feature_id, method_index) in seen | tool_feature_method_ids.keys():
            continue
        item = {
            "tool_feature_id": tool_feature_id,
            "parameter_value_list_id": parameter_value_list_id,
            "method_index": method_index,
        }
        try:
            check_tool_feature_method(item, tool_feature_method_ids, tool_features, parameter_value_lists)
            to_add.append(item)
            seen.add((tool_feature_id, method_index))
        except SpineIntegrityError as e:
            error_log.append(
                ImportErrorLogItem(
                    msg=(
                        f"Could not import tool feature method '{tool_name, class_name, parameter_name, method}':"
                        f" {e.msg}"
                    ),
                    db_type="tool_feature_method",
                )
            )
    return to_add, [], error_log


def import_alternatives(db_map, data, make_cache=None):
    """
    Imports alternatives.

    Example:

        data = ['new_alternative', ('another_alternative', 'description')]
        import_alternatives(db_map, data)

    Args:
        db_map (DiffDatabaseMapping): mapping for database to insert into
        data (Iterable): an iterable of alternative names,
            or of lists/tuples with alternative names and optional descriptions

    Returns:
        tuple of int and list: Number of successfully inserted alternatives, list of errors
    """
    return import_data(db_map, alternatives=data, make_cache=make_cache)


def _get_alternatives_for_import(data, make_cache):
    cache = make_cache({"alternative"}, include_ancestors=True)
    alternative_ids = {alternative.name: alternative.id for alternative in cache.get("alternative", {}).values()}
    checked = set()
    to_add = []
    to_update = []
    error_log = []
    for alternative in data:
        if isinstance(alternative, str):
            alternative = (alternative,)
        name, *optionals = alternative
        if name in checked:
            continue
        alternative_id = alternative_ids.pop(name, None)
        item = (
            cache["alternative"][alternative_id]._asdict()
            if alternative_id is not None
            else {"name": name, "description": None}
        )
        item.update(dict(zip(("description",), optionals)))
        try:
            check_alternative(item, alternative_ids)
        except SpineIntegrityError as e:
            error_log.append(
                ImportErrorLogItem(msg=f"Could not import alternative '{name}': {e.msg}", db_type="alternative")
            )
            continue
        finally:
            if alternative_id is not None:
                alternative_ids[name] = alternative_id
        checked.add(name)
        if alternative_id is not None:
            item["id"] = alternative_id
            to_update.append(item)
        else:
            to_add.append(item)
    return to_add, to_update, error_log


def import_scenarios(db_map, data, make_cache=None):
    """
    Imports scenarios.

    Example:

        second_active = True
        third_active = False
        data = ['scenario', ('second_scenario', second_active), ('third_scenario', third_active, 'description')]
        import_scenarios(db_map, data)

    Args:
        db_map (DiffDatabaseMapping): mapping for database to insert into
        data (Iterable): an iterable of scenario names,
            or of lists/tuples with scenario names and optional descriptions

    Returns:
        tuple of int and list: Number of successfully inserted scenarios, list of errors
    """
    return import_data(db_map, scenarios=data, make_cache=make_cache)


def _get_scenarios_for_import(data, make_cache):
    cache = make_cache({"scenario"}, include_ancestors=True)
    scenario_ids = {scenario.name: scenario.id for scenario in cache.get("scenario", {}).values()}
    checked = set()
    to_add = []
    to_update = []
    error_log = []
    for scenario in data:
        if isinstance(scenario, str):
            scenario = (scenario,)
        name, *optionals = scenario
        if name in checked:
            continue
        scenario_id = scenario_ids.pop(name, None)
        item = (
            cache["scenario"][scenario_id]._asdict()
            if scenario_id is not None
            else {"name": name, "active": False, "description": None}
        )
        item.update(dict(zip(("active", "description"), optionals)))
        try:
            check_scenario(item, scenario_ids)
        except SpineIntegrityError as e:
            error_log.append(ImportErrorLogItem(msg=f"Could not import scenario '{name}': {e.msg}", db_type="scenario"))
            continue
        finally:
            if scenario_id is not None:
                scenario_ids[name] = scenario_id
        checked.add(name)
        if scenario_id is not None:
            item["id"] = scenario_id
            to_update.append(item)
        else:
            to_add.append(item)
    return to_add, to_update, error_log


def import_scenario_alternatives(db_map, data, make_cache=None):
    """
    Imports scenario alternatives.

    Example:

        data = [('scenario', 'bottom_alternative'), ('another_scenario', 'top_alternative', 'bottom_alternative')]
        import_scenario_alternatives(db_map, data)

    Args:
        db_map (DiffDatabaseMapping): mapping for database to insert into
        data (Iterable): an iterable of (scenario name, alternative name,
            and optionally, 'before' alternative name).
            Alternatives are inserted before the 'before' alternative,
            or at the end if not given.

    Returns:
        tuple of int and list: Number of successfully inserted scenario alternatives, list of errors
    """
    return import_data(db_map, scenario_alternatives=data, make_cache=make_cache)


def _get_scenario_alternatives_for_import(data, make_cache):
    cache = make_cache({"scenario_alternative"}, include_ancestors=True)
    scenario_alternative_id_lists = {x.id: x.alternative_id_list for x in cache.get("scenario", {}).values()}
    scenario_alternative_ids = {
        (x.scenario_id, x.alternative_id): x.id for x in cache.get("scenario_alternative", {}).values()
    }
    scenario_ids = {scenario.name: scenario.id for scenario in cache.get("scenario", {}).values()}
    alternative_ids = {alternative.name: alternative.id for alternative in cache.get("alternative", {}).values()}
    checked = set()
    to_add = []
    to_update = []
    error_log = []
    for scenario_name, alternative_name, *optionals in data:
        scenario_id = scenario_ids.get(scenario_name)
        if not scenario_id:
            error_log.append(
                ImportErrorLogItem(msg=f"Scenario '{scenario_name}' not found.", db_type="scenario alternative")
            )
            continue
        alternative_id = alternative_ids.get(alternative_name)
        if not alternative_id:
            error_log.append(
                ImportErrorLogItem(msg=f"Alternative '{alternative_name}' not found.", db_type="scenario alternative")
            )
            continue
        if (scenario_name, alternative_name) in checked:
            continue
        checked.add((scenario_name, alternative_name))
        if optionals and optionals[0]:
            before_alt_name = optionals[0]
            try:
                before_alt_id = alternative_ids[before_alt_name]
            except KeyError:
                error_log.append(
                    ImportErrorLogItem(msg=f"Before alternative '{before_alt_name}' not found for '{alternative_name}'")
                )
                continue
        else:
            before_alt_id = None
        orig_alt_id_list = scenario_alternative_id_lists.get(scenario_id, [])
        new_alt_id_list = [id_ for id_ in orig_alt_id_list if id_ != alternative_id]
        try:
            pos = new_alt_id_list.index(before_alt_id)
        except ValueError:
            pos = len(new_alt_id_list)
        new_alt_id_list.insert(pos, alternative_id)
        scenario_alternative_id_lists[scenario_id] = new_alt_id_list
    for scenario_id, new_alt_id_list in scenario_alternative_id_lists.items():
        for k, alt_id in enumerate(new_alt_id_list):
            id_ = scenario_alternative_ids.get((scenario_id, alt_id))
            item = {"scenario_id": scenario_id, "alternative_id": alt_id, "rank": k + 1}
            if id_ is not None:
                item["id"] = id_
                to_update.append(item)
            else:
                to_add.append(item)
    return to_add, to_update, error_log


def import_object_classes(db_map, data, make_cache=None):
    """Imports object classes.

    Example::

            data = ['new_object_class', ('another_object_class', 'description', 123456)]
            import_object_classes(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (Iterable): list/set/iterable of string object class names, or of lists/tuples with object class names,
            and optionally description and integer display icon reference

    Returns:
        tuple of int and list: Number of successfully inserted object classes, list of errors
    """
    return import_data(db_map, object_classes=data, make_cache=make_cache)


def _get_object_classes_for_import(db_map, data, make_cache):
    cache = make_cache({"object_class"}, include_ancestors=True)
    object_class_ids = {oc.name: oc.id for oc in cache.get("object_class", {}).values()}
    checked = set()
    to_add = []
    to_update = []
    error_log = []
    for object_class in data:
        if isinstance(object_class, str):
            object_class = (object_class,)
        name, *optionals = object_class
        if name in checked:
            continue
        oc_id = object_class_ids.pop(name, None)
        item = (
            cache["object_class"][oc_id]._asdict()
            if oc_id is not None
            else {"name": name, "description": None, "display_icon": None}
        )
        item["type_id"] = db_map.object_class_type
        item.update(dict(zip(("description", "display_icon"), optionals)))
        try:
            check_object_class(item, object_class_ids, db_map.object_class_type)
        except SpineIntegrityError as e:
            error_log.append(
                ImportErrorLogItem(msg=f"Could not import object class '{name}': {e.msg}", db_type="object class")
            )
            continue
        finally:
            if oc_id is not None:
                object_class_ids[name] = oc_id
        checked.add(name)
        if oc_id is not None:
            item["id"] = oc_id
            to_update.append(item)
        else:
            to_add.append(item)
    return to_add, to_update, error_log


def import_relationship_classes(db_map, data, make_cache=None):
    """Imports relationship classes.

    Example::

            data = [
                ('new_rel_class', ['object_class_1', 'object_class_2']),
                ('another_rel_class', ['object_class_3', 'object_class_4'], 'description'),
            ]
            import_relationship_classes(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with relationship class names,
            list of object class names, and optionally description

    Returns:
        (Int, List) Number of successful inserted objects, list of errors
    """
    return import_data(db_map, relationship_classes=data, make_cache=make_cache)


def _get_relationship_classes_for_import(db_map, data, make_cache):
    cache = make_cache({"relationship_class"}, include_ancestors=True)
    object_class_ids = {oc.name: oc.id for oc in cache.get("object_class", {}).values()}
    relationship_class_ids = {x.name: x.id for x in cache.get("relationship_class", {}).values()}
    checked = set()
    error_log = []
    to_add = []
    to_update = []
    for name, oc_names, *optionals in data:
        if name in checked:
            continue
        rc_id = relationship_class_ids.pop(name, None)
        item = (
            cache["relationship_class"][rc_id]._asdict()
            if rc_id is not None
            else {
                "name": name,
                "object_class_id_list": [object_class_ids.get(oc, None) for oc in oc_names],
                "description": None,
                "display_icon": None,
            }
        )
        item["type_id"] = db_map.relationship_class_type
        item.update(dict(zip(("description", "display_icon"), optionals)))
        try:
            check_wide_relationship_class(
                item, relationship_class_ids, set(object_class_ids.values()), db_map.relationship_class_type
            )
        except SpineIntegrityError as e:
            error_log.append(
                ImportErrorLogItem(
                    f"Could not import relationship class '{name}' with object classes {tuple(oc_names)}: {e.msg}",
                    db_type="relationship class",
                )
            )
            continue
        finally:
            if rc_id is not None:
                relationship_class_ids[name] = rc_id
        checked.add(name)
        if rc_id is not None:
            item["id"] = rc_id
            to_update.append(item)
        else:
            to_add.append(item)
    return to_add, to_update, error_log


def import_objects(db_map, data, make_cache=None):
    """Imports list of object by name with associated object class name into given database mapping:
    Ignores duplicate names and existing names.

    Example::

            data = [
                ('object_class_name', 'new_object'),
                ('object_class_name', 'other_object', 'description')
            ]
            import_objects(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with object name and object class name

    Returns:
        (Int, List) Number of successful inserted objects, list of errors
    """
    return import_data(db_map, objects=data, make_cache=make_cache)


def _get_objects_for_import(db_map, data, make_cache):
    cache = make_cache({"object"}, include_ancestors=True)
    object_class_ids = {oc.name: oc.id for oc in cache.get("object_class", {}).values()}
    object_ids = {(o.class_id, o.name): o.id for o in cache.get("object", {}).values()}
    checked = set()
    error_log = []
    to_add = []
    to_update = []
    for oc_name, name, *optionals in data:
        oc_id = object_class_ids.get(oc_name, None)
        if (oc_id, name) in checked:
            continue
        o_id = object_ids.pop((oc_id, name), None)
        item = (
            cache["object"][o_id]._asdict()
            if o_id is not None
            else {"name": name, "class_id": oc_id, "description": None}
        )
        item["type_id"] = db_map.object_entity_type
        item.update(dict(zip(("description",), optionals)))
        try:
            check_object(item, object_ids, set(object_class_ids.values()), db_map.object_entity_type)
        except SpineIntegrityError as e:
            error_log.append(
                ImportErrorLogItem(
                    msg=f"Could not import object '{name}' with class '{oc_name}': {e.msg}", db_type="object"
                )
            )
            continue
        finally:
            if o_id is not None:
                object_ids[oc_id, name] = o_id
        checked.add((oc_id, name))
        if o_id is not None:
            item["id"] = o_id
            to_update.append(item)
        else:
            to_add.append(item)
    return to_add, to_update, error_log


def import_object_groups(db_map, data, make_cache=None):
    """Imports list of object groups by name with associated object class name into given database mapping:
    Ignores duplicate and existing (group, member) tuples.

    Example::

            data = [
                ('object_class_name', 'object_group_name', 'member_name'),
                ('object_class_name', 'object_group_name', 'another_member_name')
            ]
            import_object_groups(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with object class name, group name,
            and member name

    Returns:
        (Int, List) Number of successful inserted objects, list of errors
    """
    return import_data(db_map, object_groups=data, make_cache=make_cache)


def _get_object_groups_for_import(db_map, data, make_cache):
    cache = make_cache({"entity_group"}, include_ancestors=True)
    object_class_ids = {oc.name: oc.id for oc in cache.get("object_class", {}).values()}
    object_ids = {(o.class_id, o.name): o.id for o in cache.get("object", {}).values()}
    objects = {}
    for obj in cache.get("object", {}).values():
        objects.setdefault(obj.class_id, dict())[obj.id] = obj._asdict()
    entity_group_ids = {(x.group_id, x.member_id): x.id for x in cache.get("entity_group", {}).values()}
    error_log = []
    to_add = []
    seen = set()
    for class_name, group_name, member_name in data:
        oc_id = object_class_ids.get(class_name)
        g_id = object_ids.get((oc_id, group_name))
        m_id = object_ids.get((oc_id, member_name))
        if (g_id, m_id) in seen | entity_group_ids.keys():
            continue
        item = {"entity_class_id": oc_id, "entity_id": g_id, "member_id": m_id}
        try:
            check_entity_group(item, entity_group_ids, objects)
            to_add.append(item)
            seen.add((g_id, m_id))
        except SpineIntegrityError as e:
            error_log.append(
                ImportErrorLogItem(
                    msg=f"Could not import object '{member_name}' into group '{group_name}': {e.msg}",
                    db_type="entity group",
                )
            )
    return to_add, [], error_log


def import_relationships(db_map, data, make_cache=None):
    """Imports relationships.

    Example::

            data = [('relationship_class_name', ('object_name1', 'object_name2'))]
            import_relationships(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with relationship class name
            and list/tuple of object names

    Returns:
        (Int, List) Number of successful inserted objects, list of errors
    """
    return import_data(db_map, relationships=data, make_cache=make_cache)


def _make_unique_relationship_name(class_id, class_name, object_names, class_id_name_tuples):
    base_name = class_name + "_" + "__".join([obj if obj is not None else "None" for obj in object_names])
    name = base_name
    while (class_id, name) in class_id_name_tuples:
        name = base_name + uuid.uuid4().hex
    return name


def _get_relationships_for_import(db_map, data, make_cache):
    cache = make_cache({"relationship"}, include_ancestors=True)
    relationships = {x.name: x for x in cache.get("relationship", {}).values()}
    relationship_ids_per_name = {(x.class_id, x.name): x.id for x in relationships.values()}
    relationship_ids_per_obj_lst = {(x.class_id, x.object_id_list): x.id for x in relationships.values()}
    relationship_classes = {
        x.id: {"object_class_id_list": x.object_class_id_list, "name": x.name}
        for x in cache.get("relationship_class", {}).values()
    }
    objects = {x.id: {"class_id": x.class_id, "name": x.name} for x in cache.get("object", {}).values()}
    object_ids = {(o["name"], o["class_id"]): o_id for o_id, o in objects.items()}
    relationship_class_ids = {rc["name"]: rc_id for rc_id, rc in relationship_classes.items()}
    object_class_id_lists = {rc_id: rc["object_class_id_list"] for rc_id, rc in relationship_classes.items()}
    error_log = []
    to_add = []
    to_update = []
    checked = set()
    for class_name, object_names, *optionals in data:
        rc_id = relationship_class_ids.get(class_name, None)
        oc_ids = object_class_id_lists.get(rc_id, [])
        o_ids = tuple(object_ids.get((name, oc_id), None) for name, oc_id in zip(object_names, oc_ids))
        if (rc_id, o_ids) in checked:
            continue
        r_id = relationship_ids_per_obj_lst.pop((rc_id, o_ids), None)
        if r_id is not None:
            r_name = cache["relationship"][r_id].name
            relationship_ids_per_name.pop((rc_id, r_name))
        item = (
            cache["relationship"][r_id]._asdict()
            if r_id is not None
            else {
                "name": _make_unique_relationship_name(rc_id, class_name, object_names, relationship_ids_per_name),
                "class_id": rc_id,
                "object_id_list": list(o_ids),
                "object_class_id_list": oc_ids,
                "type_id": db_map.relationship_entity_type,
            }
        )
        item.update(dict(zip(("description",), optionals)))
        try:
            check_wide_relationship(
                item,
                relationship_ids_per_name,
                relationship_ids_per_obj_lst,
                relationship_classes,
                objects,
                db_map.relationship_entity_type,
            )
        except SpineIntegrityError as e:
            msg = f"Could not import relationship with objects {tuple(object_names)} into '{class_name}': {e.msg}"
            error_log.append(ImportErrorLogItem(msg=msg, db_type="relationship"))
            continue
        finally:
            if r_id is not None:
                relationship_ids_per_obj_lst[rc_id, o_ids] = r_id
                relationship_ids_per_name[rc_id, r_name] = r_id
        checked.add((rc_id, o_ids))
        if r_id is not None:
            item["id"] = r_id
            to_update.append(item)
        else:
            to_add.append(item)
    return to_add, to_update, error_log


def import_object_parameters(db_map, data, make_cache=None, unparse_value=to_database):
    """Imports list of object class parameters:

    Example::

            data = [
                ('object_class_1', 'new_parameter'),
                ('object_class_2', 'other_parameter', 'default_value', 'value_list_name', 'description')
            ]
            import_object_parameters(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with object class name, parameter name,
            and optionally default value, value list name, and description

    Returns:
        (Int, List) Number of successful inserted objects, list of errors
    """
    return import_data(db_map, object_parameters=data, make_cache=make_cache, unparse_value=unparse_value)


def _get_object_parameters_for_import(db_map, data, make_cache, unparse_value):
    cache = make_cache({"parameter_definition"}, include_ancestors=True)
    parameter_ids = {
        (x.entity_class_id, x.parameter_name): x.id for x in cache.get("parameter_definition", {}).values()
    }
    object_class_names = {x.id: x.name for x in cache.get("object_class", {}).values()}
    object_class_ids = {oc_name: oc_id for oc_id, oc_name in object_class_names.items()}
    parameter_value_lists = {}
    parameter_value_list_ids = {}
    for x in cache.get("parameter_value_list", {}).values():
        parameter_value_lists[x.id] = x.value_id_list
        parameter_value_list_ids[x.name] = x.id
    list_values = {x.id: from_database(x.value, x.type) for x in cache.get("list_value", {}).values()}
    checked = set()
    error_log = []
    to_add = []
    to_update = []
    functions = [unparse_value, lambda x: (parameter_value_list_ids.get(x),), lambda x: (x,)]
    for class_name, parameter_name, *optionals in data:
        oc_id = object_class_ids.get(class_name, None)
        checked_key = (oc_id, parameter_name)
        if checked_key in checked:
            continue
        p_id = parameter_ids.pop((oc_id, parameter_name), None)
        item = (
            cache["parameter_definition"][p_id]._asdict()
            if p_id is not None
            else {
                "name": parameter_name,
                "entity_class_id": oc_id,
                "object_class_id": oc_id,
                "default_value": None,
                "default_type": None,
                "parameter_value_list_id": None,
                "description": None,
            }
        )
        optionals = [y for f, x in zip(functions, optionals) for y in f(x)]
        item.update(dict(zip(("default_value", "default_type", "parameter_value_list_id", "description"), optionals)))
        try:
            check_parameter_definition(
                item, parameter_ids, object_class_names.keys(), parameter_value_lists, list_values
            )
        except SpineIntegrityError as e:
            error_log.append(
                ImportErrorLogItem(
                    f"Could not import parameter '{parameter_name}' with class '{class_name}': {e.msg}",
                    db_type="parameter definition",
                )
            )
            continue
        finally:
            if p_id is not None:
                parameter_ids[oc_id, parameter_name] = p_id
        checked.add(checked_key)
        if p_id is not None:
            item["id"] = p_id
            to_update.append(item)
        else:
            to_add.append(item)
    return to_add, to_update, error_log


def import_relationship_parameters(db_map, data, make_cache=None, unparse_value=to_database):
    """Imports list of relationship class parameters:

    Example::

            data = [
                ('relationship_class_1', 'new_parameter'),
                ('relationship_class_2', 'other_parameter', 'default_value', 'value_list_name', 'description')
            ]
            import_relationship_parameters(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with relationship class name, parameter name,
            and optionally default value, value list name, and description

    Returns:
        (Int, List) Number of successful inserted objects, list of errors
    """
    return import_data(db_map, relationship_parameters=data, make_cache=make_cache, unparse_value=unparse_value)


def _get_relationship_parameters_for_import(db_map, data, make_cache, unparse_value):
    cache = make_cache({"parameter_definition"}, include_ancestors=True)
    parameter_ids = {
        (x.entity_class_id, x.parameter_name): x.id for x in cache.get("parameter_definition", {}).values()
    }
    relationship_class_names = {x.id: x.name for x in cache.get("relationship_class", {}).values()}
    relationship_class_ids = {rc_name: rc_id for rc_id, rc_name in relationship_class_names.items()}
    parameter_value_lists = {}
    parameter_value_list_ids = {}
    for x in cache.get("parameter_value_list", {}).values():
        parameter_value_lists[x.id] = x.value_id_list
        parameter_value_list_ids[x.name] = x.id
    list_values = {x.id: from_database(x.value, x.type) for x in cache.get("list_value", {}).values()}
    error_log = []
    to_add = []
    to_update = []
    checked = set()
    functions = [unparse_value, lambda x: (parameter_value_list_ids.get(x),), lambda x: (x,)]
    for class_name, parameter_name, *optionals in data:
        rc_id = relationship_class_ids.get(class_name, None)
        checked_key = (rc_id, parameter_name)
        if checked_key in checked:
            continue
        p_id = parameter_ids.pop((rc_id, parameter_name), None)
        item = (
            cache["parameter_definition"][p_id]._asdict()
            if p_id is not None
            else {
                "name": parameter_name,
                "entity_class_id": rc_id,
                "relationship_class_id": rc_id,
                "default_value": None,
                "default_type": None,
                "parameter_value_list_id": None,
                "description": None,
            }
        )
        optionals = [y for f, x in zip(functions, optionals) for y in f(x)]
        item.update(dict(zip(("default_value", "default_type", "parameter_value_list_id", "description"), optionals)))
        try:
            check_parameter_definition(
                item, parameter_ids, relationship_class_names.keys(), parameter_value_lists, list_values
            )
        except SpineIntegrityError as e:
            # Relationship class doesn't exists
            error_log.append(
                ImportErrorLogItem(
                    msg=f"Could not import parameter '{parameter_name}' with class '{class_name}': {e.msg}",
                    db_type="parameter definition",
                )
            )
            continue
        finally:
            if p_id is not None:
                parameter_ids[rc_id, parameter_name] = p_id
        checked.add(checked_key)
        if p_id is not None:
            item["id"] = p_id
            to_update.append(item)
        else:
            to_add.append(item)
    return to_add, to_update, error_log


def import_object_parameter_values(db_map, data, make_cache=None, unparse_value=to_database, on_conflict="merge"):
    """Imports object parameter values:

    Example::

            data = [('object_class_name', 'object_name', 'parameter_name', 123.4),
                    ('object_class_name', 'object_name', 'parameter_name2', <TimeSeries>),
                    ('object_class_name', 'object_name', 'parameter_name', <TimeSeries>, 'alternative')]
            import_object_parameter_values(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with
            object_class_name, object name, parameter name, (deserialized) parameter value,
            optional name of an alternative

    Returns:
        (Int, List) Number of successful inserted objects, list of errors
    """
    return import_data(
        db_map,
        object_parameter_values=data,
        make_cache=make_cache,
        unparse_value=unparse_value,
        on_conflict=on_conflict,
    )


def _get_object_parameter_values_for_import(db_map, data, make_cache, unparse_value, on_conflict):
    cache = make_cache({"parameter_value"}, include_ancestors=True)
    object_class_ids = {x.name: x.id for x in cache.get("object_class", {}).values()}
    parameter_value_ids = {
        (x.entity_id, x.parameter_id, x.alternative_id): x.id for x in cache.get("parameter_value", {}).values()
    }
    parameters = {
        x.id: {
            "name": x.parameter_name,
            "entity_class_id": x.entity_class_id,
            "parameter_value_list_id": x.value_list_id,
        }
        for x in cache.get("parameter_definition", {}).values()
    }
    objects = {x.id: {"class_id": x.class_id, "name": x.name} for x in cache.get("object", {}).values()}
    parameter_value_lists = {x.id: x.value_id_list for x in cache.get("parameter_value_list", {}).values()}
    list_values = {x.id: from_database(x.value, x.type) for x in cache.get("list_value", {}).values()}
    object_ids = {(o["name"], o["class_id"]): o_id for o_id, o in objects.items()}
    parameter_ids = {(p["name"], p["entity_class_id"]): p_id for p_id, p in parameters.items()}
    alternatives = {a.name: a.id for a in cache.get("alternative", {}).values()}
    alternative_ids = set(alternatives.values())
    error_log = []
    to_add = []
    to_update = []
    checked = set()
    for class_name, object_name, parameter_name, value, *optionals in data:
        oc_id = object_class_ids.get(class_name, None)
        o_id = object_ids.get((object_name, oc_id), None)
        p_id = parameter_ids.get((parameter_name, oc_id), None)
        if optionals:
            alternative_name = optionals[0]
            alt_id = alternatives.get(alternative_name)
            if not alt_id:
                error_log.append(
                    ImportErrorLogItem(
                        msg=(
                            "Could not import parameter value for "
                            f"'{object_name}', class '{class_name}', parameter '{parameter_name}': "
                            f"alternative '{alternative_name}' does not exist."
                        ),
                        db_type="parameter value",
                    )
                )
                continue
        else:
            alt_id, alternative_name = db_map.get_import_alternative(cache=cache)
            alternative_ids.add(alt_id)
        checked_key = (o_id, p_id, alt_id)
        if checked_key in checked:
            msg = (
                f"Could not import parameter value for '{object_name}', class '{class_name}', "
                f"parameter '{parameter_name}', alternative {alternative_name}: "
                "Duplicate parameter value, only first value will be considered."
            )
            error_log.append(ImportErrorLogItem(msg=msg, db_type="parameter value"))
            continue
        pv_id = parameter_value_ids.pop((o_id, p_id, alt_id), None)
        value, type_ = unparse_value(value)
        if pv_id is not None:
            current_pv = cache["parameter_value"][pv_id]
            value, type_ = fix_conflict((value, type_), (current_pv.value, current_pv.type), on_conflict)
        item = {
            "parameter_definition_id": p_id,
            "entity_class_id": oc_id,
            "entity_id": o_id,
            "object_class_id": oc_id,
            "object_id": o_id,
            "value": value,
            "type": type_,
            "alternative_id": alt_id,
        }
        try:
            check_parameter_value(
                item, parameter_value_ids, parameters, objects, parameter_value_lists, list_values, alternative_ids
            )
        except SpineIntegrityError as e:
            error_log.append(
                ImportErrorLogItem(
                    msg="Could not import parameter value for '{0}', class '{1}', parameter '{2}': {3}".format(
                        object_name, class_name, parameter_name, e.msg
                    ),
                    db_type="parameter value",
                )
            )
            continue
        finally:
            if pv_id is not None:
                parameter_value_ids[o_id, p_id, alt_id] = pv_id
        checked.add(checked_key)
        if pv_id is not None:
            item["id"] = pv_id
            to_update.append(item)
        else:
            to_add.append(item)
    return to_add, to_update, error_log


def import_relationship_parameter_values(db_map, data, make_cache=None, unparse_value=to_database, on_conflict="merge"):
    """Imports relationship parameter values:

    Example::

            data = [['example_rel_class',
                ['example_object', 'other_object'], 'rel_parameter', 2.718],
                ['example_object', 'other_object'], 'rel_parameter', 5.5, 'alternative']]
            import_relationship_parameter_values(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with
            relationship class name, list of object names, parameter name, (deserialized) parameter value,
            optional name of an alternative

    Returns:
        (Int, List) Number of successful inserted objects, list of errors
    """
    return import_data(
        db_map,
        relationship_parameter_values=data,
        make_cache=make_cache,
        unparse_value=unparse_value,
        on_conflict=on_conflict,
    )


def _get_relationship_parameter_values_for_import(db_map, data, make_cache, unparse_value, on_conflict):
    cache = make_cache({"parameter_value"}, include_ancestors=True)
    object_class_id_lists = {x.id: x.object_class_id_list for x in cache.get("relationship_class", {}).values()}
    parameter_value_ids = {
        (x.entity_id, x.parameter_id, x.alternative_id): x.id for x in cache.get("parameter_value", {}).values()
    }
    parameters = {
        x.id: {
            "name": x.parameter_name,
            "entity_class_id": x.entity_class_id,
            "parameter_value_list_id": x.value_list_id,
        }
        for x in cache.get("parameter_definition", {}).values()
    }
    relationships = {
        x.id: {"class_id": x.class_id, "name": x.name, "object_id_list": x.object_id_list}
        for x in cache.get("relationship", {}).values()
    }
    parameter_value_lists = {x.id: x.value_id_list for x in cache.get("parameter_value_list", {}).values()}
    list_values = {x.id: from_database(x.value, x.type) for x in cache.get("list_value", {}).values()}
    parameter_ids = {(p["entity_class_id"], p["name"]): p_id for p_id, p in parameters.items()}
    relationship_ids = {(r["class_id"], tuple(r["object_id_list"])): r_id for r_id, r in relationships.items()}
    object_ids = {(o.name, o.class_id): o.id for o in cache.get("object", {}).values()}
    relationship_class_ids = {oc.name: oc.id for oc in cache.get("relationship_class", {}).values()}
    alternatives = {a.name: a.id for a in cache.get("alternative", {}).values()}
    alternative_ids = set(alternatives.values())
    error_log = []
    to_add = []
    to_update = []
    checked = set()
    for class_name, object_names, parameter_name, value, *optionals in data:
        rc_id = relationship_class_ids.get(class_name, None)
        oc_ids = object_class_id_lists.get(rc_id, [])
        if len(object_names) == len(oc_ids):
            o_ids = tuple(object_ids.get((name, oc_id), None) for name, oc_id in zip(object_names, oc_ids))
        else:
            o_ids = tuple(None for _ in object_names)
        r_id = relationship_ids.get((rc_id, o_ids), None)
        p_id = parameter_ids.get((rc_id, parameter_name), None)
        if optionals:
            alternative_name = optionals[0]
            alt_id = alternatives.get(alternative_name)
            if not alt_id:
                error_log.append(
                    ImportErrorLogItem(
                        msg=(
                            "Could not import parameter value for "
                            f"'{object_names}', class '{class_name}', parameter '{parameter_name}': "
                            f"alternative {alternative_name} does not exist."
                        ),
                        db_type="parameter value",
                    )
                )
                continue
        else:
            alt_id, alternative_name = db_map.get_import_alternative(cache=cache)
            alternative_ids.add(alt_id)
        checked_key = (r_id, p_id, alt_id)
        if checked_key in checked:
            msg = (
                f"Could not import parameter value for '{object_names}', class '{class_name}', "
                f"parameter '{parameter_name}', alternative {alternative_name}: "
                "Duplicate parameter value, only first value will be considered."
            )
            error_log.append(ImportErrorLogItem(msg=msg, db_type="parameter value"))
            continue
        pv_id = parameter_value_ids.pop((r_id, p_id, alt_id), None)
        value, type_ = unparse_value(value)
        if pv_id is not None:
            current_pv = cache["parameter_value"][pv_id]
            value, type_ = fix_conflict((value, type_), (current_pv.value, current_pv.type), on_conflict)
        item = {
            "parameter_definition_id": p_id,
            "entity_class_id": rc_id,
            "entity_id": r_id,
            "relationship_class_id": rc_id,
            "relationship_id": r_id,
            "value": value,
            "type": type_,
            "alternative_id": alt_id,
        }
        try:
            check_parameter_value(
                item,
                parameter_value_ids,
                parameters,
                relationships,
                parameter_value_lists,
                list_values,
                alternative_ids,
            )
        except SpineIntegrityError as e:
            error_log.append(
                ImportErrorLogItem(
                    msg="Could not import parameter value for '{0}', class '{1}', parameter '{2}': {3}".format(
                        object_names, class_name, parameter_name, e.msg
                    ),
                    db_type="parameter value",
                )
            )
            continue
        finally:
            if pv_id is not None:
                parameter_value_ids[r_id, p_id, alt_id] = pv_id
        checked.add(checked_key)
        if pv_id is not None:
            item["id"] = pv_id
            to_update.append(item)
        else:
            to_add.append(item)
    return to_add, to_update, error_log


def import_parameter_value_lists(db_map, data, make_cache=None, unparse_value=to_database):
    """Imports list of parameter value lists:

    Example::

            data = [
                ['value_list_name', value1], ['value_list_name', value2],
                ['another_value_list_name', value3],
            ]
            import_parameter_value_lists(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with
                                 value list name, list of values

    Returns:
        (Int, List) Number of successful inserted objects, list of errors
    """
    return import_data(db_map, parameter_value_lists=data, make_cache=make_cache, unparse_value=unparse_value)


def _get_parameter_value_lists_for_import(db_map, data, make_cache):
    cache = make_cache({"parameter_value_list"}, include_ancestors=True)
    parameter_value_list_ids = {x.name: x.id for x in cache.get("parameter_value_list", {}).values()}
    error_log = []
    to_add = []
    for name in list({x[0]: None for x in data}):
        item = {"name": name}
        try:
            check_parameter_value_list(item, parameter_value_list_ids)
        except SpineIntegrityError:
            continue
        to_add.append(item)
    return to_add, [], error_log


def _get_list_values_for_import(db_map, data, make_cache, unparse_value):
    cache = make_cache({"list_value"}, include_ancestors=True)
    value_lists_by_name = {x.name: (x.id, x.value_index_list) for x in cache.get("parameter_value_list", {}).values()}
    list_value_ids_by_index = {(x.parameter_value_list_id, x.index): x.id for x in cache.get("list_value", {}).values()}
    list_value_ids_by_value = {
        (x.parameter_value_list_id, x.type, x.value): x.id for x in cache.get("list_value", {}).values()
    }
    list_names_by_id = {x.id: x.name for x in cache.get("parameter_value_list", {}).values()}
    error_log = []
    to_add = []
    to_update = []
    seen_values = set()
    max_indexes = dict()
    for list_name, value in data:
        try:
            list_id, value_index_list = value_lists_by_name.get(list_name)
        except TypeError:
            # cannot unpack non-iterable NoneType object
            error_log.append(
                ImportErrorLogItem(
                    msg=f"Could not import value for list '{list_name}': list not found", db_type="list value"
                )
            )
            continue
        val, type_ = unparse_value(value)
        if (list_id, type_, val) in seen_values:
            error_log.append(
                ImportErrorLogItem(
                    msg=f"Could not import value for list '{list_name}': "
                    "Duplicate value, only first will be considered",
                    db_type="list value",
                )
            )
            continue
        max_index = max_indexes.get(list_id)
        if max_index is not None:
            index = max_index + 1
        elif not value_index_list:
            index = 0
        else:
            index = max(value_index_list) + 1
        item = {"parameter_value_list_id": list_id, "value": val, "type": type_, "index": index}
        try:
            check_list_value(item, list_names_by_id, list_value_ids_by_index, list_value_ids_by_value)
        except SpineIntegrityError as e:
            if e.id is None:
                error_log.append(
                    ImportErrorLogItem(
                        msg=f"Could not import value '{value}' for list '{list_name}': {e.msg}", db_type="list value"
                    )
                )
            continue
        max_indexes[list_id] = index
        seen_values.add((list_id, type_, val))
        to_add.append(item)
    return to_add, to_update, error_log


def import_metadata(db_map, data, make_cache=None):
    """Imports metadata. Ignores duplicates.

    Example::

            data = ['{"name1": "value1"}', '{"name2": "value2"}']
            import_metadata(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of string metadata entries in JSON format

    Returns:
        (Int, List) Number of successful inserted objects, list of errors
    """
    return import_data(db_map, metadata=data, make_cache=make_cache)


def _get_metadata_for_import(db_map, data, make_cache):
    cache = make_cache({"metadata"}, include_ancestors=True)
    seen = {(x.name, x.value) for x in cache.get("metadata", {}).values()}
    to_add = []
    for metadata in data:
        for name, value in _parse_metadata(metadata):
            if (name, value) in seen:
                continue
            item = {"name": name, "value": value}
            seen.add((name, value))
            to_add.append(item)
    return to_add, [], []


def import_object_metadata(db_map, data, make_cache=None):
    """Imports object metadata. Ignores duplicates.

    Example::

            data = [("classA", "object1", '{"name1": "value1"}'), ("classA", "object1", '{"name2": "value2"}')]
            import_object_metadata(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of tuples with class name, object name,
            and string metadata entries in JSON format

    Returns:
        (Int, List) Number of successful inserted items, list of errors
    """
    return import_data(db_map, object_metadata=data, make_cache=make_cache)


def _get_object_metadata_for_import(db_map, data, make_cache):
    cache = make_cache({"object", "entity_metadata"}, include_ancestors=True)
    object_class_ids = {x.name: x.id for x in cache.get("object_class", {}).values()}
    metadata_ids = {(x.name, x.value): x.id for x in cache.get("metadata", {}).values()}
    object_ids = {(x.name, x.class_id): x.id for x in cache.get("object", {}).values()}
    seen = {(x.entity_id, x.metadata_id) for x in cache.get("entity_metadata", {}).values()}
    error_log = []
    to_add = []
    for class_name, object_name, metadata in data:
        oc_id = object_class_ids.get(class_name, None)
        o_id = object_ids.get((object_name, oc_id), None)
        if o_id is None:
            error_log.append(
                ImportErrorLogItem(
                    msg=f"Could not import object metadata: unknown object '{object_name}' of class '{class_name}'",
                    db_type="object metadata",
                )
            )
            continue
        for name, value in _parse_metadata(metadata):
            m_id = metadata_ids.get((name, value), None)
            if m_id is None:
                error_log.append(
                    ImportErrorLogItem(
                        msg=f"Could not import object metadata: unknown metadata '{name}': '{value}'",
                        db_type="object metadata",
                    )
                )
                continue
            unique_key = (o_id, m_id)
            if unique_key in seen:
                continue
            item = {"entity_id": o_id, "metadata_id": m_id}
            seen.add(unique_key)
            to_add.append(item)
    return to_add, [], error_log


def import_relationship_metadata(db_map, data, make_cache=None):
    """Imports relationship metadata. Ignores duplicates.

    Example::

            data = [
                ("classA", ("object1", "object2"), '{"name1": "value1"}'),
                ("classA", ("object3", "object4"), '{"name2": "value2"}')
            ]
            import_relationship_metadata(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of tuples with class name, tuple of object names,
            and string metadata entries in JSON format

    Returns:
        (Int, List) Number of successful inserted items, list of errors
    """
    return import_data(db_map, relationship_metadata=data, make_cache=make_cache)


def _get_relationship_metadata_for_import(db_map, data, make_cache):
    cache = make_cache({"relationship", "entity_metadata"}, include_ancestors=True)
    relationship_class_ids = {oc.name: oc.id for oc in cache.get("relationship_class", {}).values()}
    object_class_id_lists = {x.id: x.object_class_id_list for x in cache.get("relationship_class", {}).values()}
    metadata_ids = {(x.name, x.value): x.id for x in cache.get("metadata", {}).values()}
    object_ids = {(x.name, x.class_id): x.id for x in cache.get("object", {}).values()}
    relationship_ids = {(x.class_id, x.object_id_list): x.id for x in cache.get("relationship", {}).values()}
    seen = {(x.entity_id, x.metadata_id) for x in cache.get("entity_metadata", {}).values()}
    error_log = []
    to_add = []
    for class_name, object_names, metadata in data:
        rc_id = relationship_class_ids.get(class_name, None)
        oc_ids = object_class_id_lists.get(rc_id, [])
        o_ids = tuple(object_ids.get((name, oc_id), None) for name, oc_id in zip(object_names, oc_ids))
        r_id = relationship_ids.get((rc_id, o_ids), None)
        if r_id is None:
            error_log.append(
                ImportErrorLogItem(
                    msg="Could not import relationship metadata: unknown relationship '{0}' of class '{1}'".format(
                        object_names, class_name
                    ),
                    db_type="relationship metadata",
                )
            )
            continue
        for name, value in _parse_metadata(metadata):
            m_id = metadata_ids.get((name, value), None)
            if m_id is None:
                error_log.append(
                    ImportErrorLogItem(
                        msg=f"Could not import relationship metadata: unknown metadata '{name}': '{value}'",
                        db_type="relationship metadata",
                    )
                )
                continue
            unique_key = (r_id, m_id)
            if unique_key in seen:
                continue
            item = {"entity_id": r_id, "metadata_id": m_id}
            seen.add(unique_key)
            to_add.append(item)
    return to_add, [], error_log


def import_object_parameter_value_metadata(db_map, data, make_cache=None):
    """Imports object parameter value metadata. Ignores duplicates.

    Example::

            data = [
                ("classA", "object1", "parameterX", '{"name1": "value1"}'),
                ("classA", "object1", "parameterY", '{"name2": "value2"}', "alternativeA")
            ]
            import_object_parameter_value_metadata(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of tuples with class name, object name,
            parameter name, string metadata entries in JSON format, and optionally alternative name

    Returns:
        (Int, List) Number of successful inserted items, list of errors
    """
    return import_data(db_map, object_parameter_value_metadata=data, make_cache=make_cache)


def _get_object_parameter_value_metadata_for_import(db_map, data, make_cache):
    cache = make_cache({"parameter_value", "parameter_value_metadata"}, include_ancestors=True)
    object_class_ids = {x.name: x.id for x in cache.get("object_class", {}).values()}
    object_ids = {(x.name, x.class_id): x.id for x in cache.get("object", {}).values()}
    parameter_ids = {
        (x.parameter_name, x.entity_class_id): x.id for x in cache.get("parameter_definition", {}).values()
    }
    alternative_ids = {a.name: a.id for a in cache.get("alternative", {}).values()}
    parameter_value_ids = {
        (x.entity_id, x.parameter_id, x.alternative_id): x.id for x in cache.get("parameter_value", {}).values()
    }
    metadata_ids = {(x.name, x.value): x.id for x in cache.get("metadata", {}).values()}
    seen = {(x.parameter_value_id, x.metadata_id) for x in cache.get("parameter_value_metadata", {}).values()}
    error_log = []
    to_add = []
    for class_name, object_name, parameter_name, metadata, *optionals in data:
        oc_id = object_class_ids.get(class_name, None)
        o_id = object_ids.get((object_name, oc_id), None)
        p_id = parameter_ids.get((parameter_name, oc_id), None)
        if optionals:
            alternative_name = optionals[0]
            alt_id = alternative_ids.get(alternative_name, None)
        else:
            alt_id, alternative_name = db_map.get_import_alternative(cache=cache)
        pv_id = parameter_value_ids.get((o_id, p_id, alt_id), None)
        if pv_id is None:
            msg = (
                "Could not import object parameter value metadata: "
                "parameter {0} doesn't have a value for object {1}, alternative {2}".format(
                    parameter_name, object_name, alternative_name
                )
            )
            error_log.append(ImportErrorLogItem(msg=msg, db_type="object parameter value metadata"))
            continue
        for name, value in _parse_metadata(metadata):
            m_id = metadata_ids.get((name, value), None)
            if m_id is None:
                error_log.append(
                    ImportErrorLogItem(
                        msg=f"Could not import object parameter value metadata: unknown metadata '{name}': '{value}'",
                        db_type="object parameter value metadata",
                    )
                )
                continue
            unique_key = (pv_id, m_id)
            if unique_key in seen:
                continue
            item = {"parameter_value_id": pv_id, "metadata_id": m_id}
            seen.add(unique_key)
            to_add.append(item)
    return to_add, [], error_log


def import_relationship_parameter_value_metadata(db_map, data, make_cache=None):
    """Imports relationship parameter value metadata. Ignores duplicates.

    Example::

            data = [
                ("classA", ("object1", "object2"), "parameterX", '{"name1": "value1"}'),
                ("classA", ("object3", "object4"), "parameterY", '{"name2": "value2"}', "alternativeA")
            ]
            import_object_parameter_value_metadata(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of tuples with class name, tuple of object names,
            parameter name, string metadata entries in JSON format, and optionally alternative name

    Returns:
        (Int, List) Number of successful inserted items, list of errors
    """
    return import_data(db_map, relationship_parameter_value_metadata=data, make_cache=make_cache)


def _get_relationship_parameter_value_metadata_for_import(db_map, data, make_cache):
    cache = make_cache({"parameter_value", "parameter_value_metadata"}, include_ancestors=True)
    relationship_class_ids = {oc.name: oc.id for oc in cache.get("relationship_class", {}).values()}
    object_class_id_lists = {x.id: x.object_class_id_list for x in cache.get("relationship_class", {}).values()}
    object_ids = {(x.name, x.class_id): x.id for x in cache.get("object", {}).values()}
    relationship_ids = {(x.object_id_list, x.class_id): x.id for x in cache.get("relationship", {}).values()}
    parameter_ids = {
        (x.parameter_name, x.entity_class_id): x.id for x in cache.get("parameter_definition", {}).values()
    }
    alternative_ids = {a.name: a.id for a in cache.get("alternative", {}).values()}
    parameter_value_ids = {
        (x.entity_id, x.parameter_id, x.alternative_id): x.id for x in cache.get("parameter_value", {}).values()
    }
    metadata_ids = {(x.name, x.value): x.id for x in cache.get("metadata", {}).values()}
    seen = {(x.parameter_value_id, x.metadata_id) for x in cache.get("parameter_value_metadata", {}).values()}
    error_log = []
    to_add = []
    for class_name, object_names, parameter_name, metadata, *optionals in data:
        rc_id = relationship_class_ids.get(class_name, None)
        oc_ids = object_class_id_lists.get(rc_id, [])
        o_ids = tuple(object_ids.get((name, oc_id), None) for name, oc_id in zip(object_names, oc_ids))
        r_id = relationship_ids.get((o_ids, rc_id), None)
        p_id = parameter_ids.get((parameter_name, rc_id), None)
        if optionals:
            alternative_name = optionals[0]
            alt_id = alternative_ids.get(alternative_name, None)
        else:
            alt_id, alternative_name = db_map.get_import_alternative(cache=cache)
        pv_id = parameter_value_ids.get((r_id, p_id, alt_id), None)
        if pv_id is None:
            msg = (
                "Could not import relationship parameter value metadata: "
                "parameter '{0}' doesn't have a value for relationship '{1}', alternative '{2}'".format(
                    parameter_name, object_names, alternative_name
                )
            )
            error_log.append(ImportErrorLogItem(msg=msg, db_type="relationship parameter value metadata"))
            continue
        for name, value in _parse_metadata(metadata):
            m_id = metadata_ids.get((name, value), None)
            if m_id is None:
                msg = f"Could not import relationship parameter value metadata: unknown metadata '{name}': '{value}'"
                error_log.append(ImportErrorLogItem(msg=msg, db_type="relationship parameter value metadata"))
                continue
            unique_key = (pv_id, m_id)
            if unique_key in seen:
                continue
            item = {"parameter_value_id": pv_id, "metadata_id": m_id}
            seen.add(unique_key)
            to_add.append(item)
    return to_add, [], error_log
