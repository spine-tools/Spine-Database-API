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

"""
This module provides the :func:`perfect_split` function.
"""
from .db_mapping import DatabaseMapping
from .export_functions import export_data
from .import_functions import import_data


def perfect_split(input_urls, intersection_url, diff_urls):
    """Splits DBs into disjoint subsets.

    Args:
        input_urls (list(str)): List of urls of DBs to split.
        intersection_url (str): The url of a DB to store the data common to all input DBs (i.e., their intersection).
        diff_urls (list(str)): List of urls of DBs to store the differences between each input and the intersection.
    """
    diff_url_lookup = dict(zip(input_urls, diff_urls))
    input_data_sets = {}
    db_names = {}
    for input_url in diff_url_lookup:
        input_db_map = DatabaseMapping(input_url)
        input_data_sets[input_url] = export_data(input_db_map)
        db_names[input_url] = input_db_map.codename
        input_db_map.close()
    intersection_data = {}
    input_data_set_iter = iter(input_data_sets)
    left_url = next(iter(input_data_set_iter))
    right_urls = list(input_data_set_iter)
    left_data_set = input_data_sets[left_url]
    for tablename, left in left_data_set.items():
        intersection = [x for x in left if all(x in input_data_sets[url][tablename] for url in right_urls)]
        if intersection:
            intersection_data[tablename] = intersection
    diffs_data = {}
    for left_url in input_data_sets:
        right_urls = [url for url in input_data_sets if url != left_url]
        left_data_set = input_data_sets[left_url]
        for tablename, left in left_data_set.items():
            left_diff = [x for x in left if all(x not in input_data_sets[url][tablename] for url in right_urls)]
            if left_diff:
                diff_data = diffs_data.setdefault(left_url, {})
                diff_data[tablename] = left_diff
    if intersection_data:
        db_map_intersection = DatabaseMapping(intersection_url)
        import_data(db_map_intersection, **intersection_data)
        all_db_names = ", ".join(db_names.values())
        db_map_intersection.commit_session(f"Add intersection of {all_db_names}")
        db_map_intersection.connection.close()
    lookup = _make_lookup(intersection_data)
    for input_url, diff_data in diffs_data.items():
        diff_url = diff_url_lookup[input_url]
        diff_db_map = DatabaseMapping(diff_url)
        _add_references(diff_data, lookup)
        import_data(diff_db_map, **diff_data)
        db_name = db_names[input_url]
        other_db_names = ", ".join([name for url, name in db_names.items() if url != input_url])
        diff_db_map.commit_session(f"Add differences between {db_name} and {other_db_names}")
        diff_db_map.close()


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
