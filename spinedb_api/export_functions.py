######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
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

:author: M. Marin (KTH)
:date:   1.4.2020
"""

from .parameter_value import from_database
from .helpers import Asterisk
from .db_mapping import DatabaseMapping
from .import_functions import import_data


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
    make_cache=None,
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
        "object_classes": export_object_classes(db_map, object_class_ids, make_cache=make_cache),
        "relationship_classes": export_relationship_classes(db_map, relationship_class_ids, make_cache=make_cache),
        "parameter_value_lists": export_parameter_value_lists(db_map, parameter_value_list_ids, make_cache=make_cache),
        "object_parameters": export_object_parameters(db_map, object_parameter_ids, make_cache=make_cache),
        "relationship_parameters": export_relationship_parameters(
            db_map, relationship_parameter_ids, make_cache=make_cache
        ),
        "objects": export_objects(db_map, object_ids, make_cache=make_cache),
        "relationships": export_relationships(db_map, relationship_ids, make_cache=make_cache),
        "object_groups": export_object_groups(db_map, object_group_ids, make_cache=make_cache),
        "object_parameter_values": export_object_parameter_values(
            db_map, object_parameter_value_ids, make_cache=make_cache
        ),
        "relationship_parameter_values": export_relationship_parameter_values(
            db_map, relationship_parameter_value_ids, make_cache=make_cache
        ),
        "alternatives": export_alternatives(db_map, alternative_ids, make_cache=make_cache),
        "scenarios": export_scenarios(db_map, scenario_ids, make_cache=make_cache),
        "scenario_alternatives": export_scenario_alternatives(db_map, scenario_alternative_ids, make_cache=make_cache),
        "tools": export_tools(db_map, tool_ids, make_cache=make_cache),
        "features": export_features(db_map, feature_ids, make_cache=make_cache),
        "tool_features": export_tool_features(db_map, tool_feature_ids, make_cache=make_cache),
        "tool_feature_methods": export_tool_feature_methods(db_map, tool_feature_method_ids, make_cache=make_cache),
    }
    return {key: value for key, value in data.items() if value}


def _get_items(db_map, tablename, ids, make_cache):
    if not ids:
        return ()
    if make_cache is None:
        make_cache = db_map.make_cache
    cache = make_cache({tablename})
    items = cache.get(tablename, {})
    if ids is Asterisk:
        yield from items.values()
        return
    for id_ in ids:
        yield items[id_]


def export_object_classes(db_map, ids=Asterisk, make_cache=None):
    return sorted((x.name, x.description, x.display_icon) for x in _get_items(db_map, "object_class", ids, make_cache))


def export_objects(db_map, ids=Asterisk, make_cache=None):
    return sorted((x.class_name, x.name, x.description) for x in _get_items(db_map, "object", ids, make_cache))


def export_relationship_classes(db_map, ids=Asterisk, make_cache=None):
    return sorted(
        (x.name, x.object_class_name_list.split(","), x.description, x.display_icon)
        for x in _get_items(db_map, "relationship_class", ids, make_cache)
    )


def export_parameter_value_lists(db_map, ids=Asterisk, make_cache=None):
    return sorted(
        (x.name, from_database(value, value_type=None))
        for x in _get_items(db_map, "parameter_value_list", ids, make_cache)
        for value in x.value_list.split(";")
    )


def export_object_parameters(db_map, ids=Asterisk, make_cache=None):
    return sorted(
        (
            x.object_class_name,
            x.parameter_name,
            from_database(x.default_value, x.default_type),
            x.value_list_name,
            x.description,
        )
        for x in _get_items(db_map, "parameter_definition", ids, make_cache)
        if x.object_class_id
    )


def export_relationship_parameters(db_map, ids=Asterisk, make_cache=None):
    return sorted(
        (
            x.relationship_class_name,
            x.parameter_name,
            from_database(x.default_value, x.default_type),
            x.value_list_name,
            x.description,
        )
        for x in _get_items(db_map, "parameter_definition", ids, make_cache)
        if x.relationship_class_id
    )


def export_relationships(db_map, ids=Asterisk, make_cache=None):
    return sorted(
        (x.class_name, x.object_name_list.split(",")) for x in _get_items(db_map, "relationship", ids, make_cache)
    )


def export_object_groups(db_map, ids=Asterisk, make_cache=None):
    return sorted(
        (x.class_name, x.group_name, x.member_name)
        for x in _get_items(db_map, "entity_group", ids, make_cache)
        if x.object_class_id
    )


def export_object_parameter_values(db_map, ids=Asterisk, make_cache=None):
    return sorted(
        (
            (x.object_class_name, x.object_name, x.parameter_name, from_database(x.value, x.type), x.alternative_name)
            for x in _get_items(db_map, "parameter_value", ids, make_cache)
            if x.object_id
        ),
        key=lambda x: x[:3] + (x[-1],),
    )


def export_relationship_parameter_values(db_map, ids=Asterisk, make_cache=None):
    return sorted(
        (
            (
                x.relationship_class_name,
                x.object_name_list.split(","),
                x.parameter_name,
                from_database(x.value, x.type),
                x.alternative_name,
            )
            for x in _get_items(db_map, "parameter_value", ids, make_cache)
            if x.relationship_id
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
        key=lambda x: x[0],
    )


def export_tools(db_map, ids=Asterisk, make_cache=None):
    return sorted((x.name, x.description) for x in _get_items(db_map, "tool", ids, make_cache))


def export_features(db_map, ids=Asterisk, make_cache=None):
    return sorted(
        (x.entity_class_name, x.parameter_definition_name, x.parameter_value_list_name, x.description)
        for x in _get_items(db_map, "feature", ids, make_cache)
    )


def export_tool_features(db_map, ids=Asterisk, make_cache=None):
    return sorted(
        (x.tool_name, x.entity_class_name, x.parameter_definition_name, x.required)
        for x in _get_items(db_map, "tool_feature", ids, make_cache)
    )


def export_tool_feature_methods(db_map, ids=Asterisk, make_cache=None):
    return sorted(
        (x.tool_name, x.entity_class_name, x.parameter_definition_name, from_database(x.method, value_type=None))
        for x in _get_items(db_map, "tool_feature_method", ids, make_cache)
    )


def perfect_split(input_urls, intersection_url, diff_urls):
    """Splits dbs into disjoint subsets.

    Args:
        input_urls (list(str)): List of urls to split
        intersection_url (str): A url to store the data common to all input urls
        diff_urls (list(str)): List of urls to store the differences of each input with respect to the intersection.
    """
    input_data_sets = {}
    db_names = {}
    diff_url_lookup = dict(zip(input_urls, diff_urls))
    for input_url in diff_url_lookup:
        input_db_map = DatabaseMapping(input_url)
        input_data_sets[input_url] = export_data(input_db_map)
        db_names[input_url] = input_db_map.codename
        input_db_map.connection.close()
    intersection_data = {}
    input_data_set_iter = iter(input_data_sets)
    left_url = next(iter(input_data_set_iter))
    right_urls = list(input_data_set_iter)
    left_data_set = input_data_sets[left_url]
    for key, left in left_data_set.items():
        intersection = [x for x in left if all(x in input_data_sets[url][key] for url in right_urls)]
        if intersection:
            intersection_data[key] = intersection
    diffs_data = {}
    for left_url in input_data_sets:
        left_data_set = input_data_sets[left_url]
        for key, left in left_data_set.items():
            left_diff = [
                x for x in left if all(x not in input_data_sets[url][key] for url in input_data_sets if url != left_url)
            ]
            if left_diff:
                diff_data = diffs_data.setdefault(left_url, {})
                diff_data[key] = left_diff
    if intersection_data:
        db_map_intersection = DatabaseMapping(intersection_url)
        import_data(db_map_intersection, **intersection_data)
        all_db_names = ', '.join(db_names.values())
        db_map_intersection.commit_session(f"Add intersection of {all_db_names}")
        db_map_intersection.connection.close()
    lookup = _make_lookup(intersection_data)
    for input_url, diff_data in diffs_data.items():
        diff_url = diff_url_lookup[input_url]
        diff_db_map = DatabaseMapping(diff_url)
        _add_references(diff_data, lookup)
        import_data(diff_db_map, **diff_data)
        db_name = db_names[input_url]
        other_db_names = ', '.join([name for url, name in db_names.items() if url != input_url])
        diff_db_map.commit_session(f"Add differences between {db_name} and {other_db_names}")
        diff_db_map.connection.close()


def _make_lookup(data):
    lookup = {}
    if "object_classes" in data:
        lookup["object_classes"] = {(x[0],): x for x in data["object_classes"]}
    if "relationship_classes" in data:
        lookup["relationship_classes"] = {(x[0],): x for x in data["relationship_classes"]}
    if "object_parameters" in data:
        lookup["object_parameters"] = {(x[0], x[1]): x for x in data["object_parameters"]}
    if "relationship_parameters" in data:
        lookup["relationship_parameters"] = {(x[0], x[1]): x for x in data["relationship_parameters"]}
    if "objects" in data:
        lookup["objects"] = {(x[0], x[1]): x for x in data["objects"]}
    return lookup


def _add_parameter_value_references(data, lookup, parameter_values, parameters, entities, make_ent):
    ref_parameters = []
    ref_entities = []
    self_lookup = _make_lookup(data)
    for param_val in data.get(parameter_values, []):
        param_key = (param_val[0], param_val[2])
        if param_key not in self_lookup.get(parameters, ()):
            ref_parameters.append(lookup.get(parameters, {}).get(param_key, param_key))
        ent_key = (param_val[0], make_ent(param_val[1]))
        if ent_key not in self_lookup.get(entities, ()):
            ref_entities.append(lookup.get(entities, {}).get(ent_key, ent_key))
    if ref_parameters:
        data.setdefault(parameters, []).extend(ref_parameters)
    if ref_entities:
        data.setdefault(entities, []).extend(ref_entities)


def _add_object_parameter_value_references(data, lookup):
    _add_parameter_value_references(
        data, lookup, "object_parameter_values", "object_parameters", "objects", make_ent=lambda x: x
    )


def _add_relationship_parameter_value_references(data, lookup):
    _add_parameter_value_references(
        data, lookup, "relationship_parameter_values", "relationship_parameters", "relationships", make_ent=tuple
    )


def _add_relationship_references(data, lookup):
    ref_objects = []
    ref_relationship_classes = []
    self_lookup = _make_lookup(data)
    for rel in data.get("relationships", []):
        rel_cls_key = (rel[0],)
        obj_name_lst = rel[1]
        rel_cls = self_lookup.get("relationship_classes", {}).get(rel_cls_key) or lookup.get(
            "relationship_classes", {}
        ).get(rel_cls_key)
        obj_cls_name_lst = rel_cls[1]
        for obj_key in zip(obj_cls_name_lst, obj_name_lst):
            if obj_key not in self_lookup.get("objects", ()):
                ref_objects.append(lookup.get("objects", {}).get(obj_key, obj_key))
        if rel_cls_key not in self_lookup.get("relationship_classes", ()):
            ref_relationship_classes.append(lookup.get("relationship_classes", {}).get(rel_cls_key, rel_cls_key))
    if ref_objects:
        data.setdefault("objects", []).extend(ref_objects)
    if ref_relationship_classes:
        data.setdefault("relationship_classes", []).extend(ref_relationship_classes)


def _add_references_to_classes(data, lookup, klass, referee):
    ref_classes = []
    self_lookup = _make_lookup(data)
    for obj in data.get(referee, []):
        cls_key = (obj[0],)
        if cls_key not in self_lookup.get(klass, ()):
            ref_classes.append(lookup.get(klass, {}).get(cls_key, cls_key))
    if ref_classes:
        data.setdefault(klass, []).extend(ref_classes)


def _add_object_references(data, lookup):
    _add_references_to_classes(data, lookup, "object_classes", "objects")


def _add_object_parameter_references(data, lookup):
    _add_references_to_classes(data, lookup, "object_classes", "object_parameters")


def _add_relationship_parameter_references(data, lookup):
    _add_references_to_classes(data, lookup, "relationship_classes", "relationship_parameters")


def _add_relationship_class_references(data, lookup):
    ref_classes = []
    self_lookup = _make_lookup(data)
    for rel_cls in data.get("relationship_classes", []):
        obj_cls_name_lst = rel_cls[1]
        for obj_cls in obj_cls_name_lst:
            obj_cls_key = (obj_cls,)
            if obj_cls_key not in self_lookup.get("object_classes", ()):
                ref_classes.append(lookup.get("object_classes", {}).get(obj_cls_key, obj_cls_key))
    if ref_classes:
        data.setdefault("object_classes", []).extend(ref_classes)


def _add_references(data, lookup):
    _add_object_parameter_value_references(data, lookup)
    _add_relationship_parameter_value_references(data, lookup)
    _add_relationship_references(data, lookup)
    _add_object_references(data, lookup)
    _add_object_parameter_references(data, lookup)
    _add_relationship_parameter_references(data, lookup)
    _add_relationship_class_references(data, lookup)
