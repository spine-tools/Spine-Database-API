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
Functions for importing data into a Spine database using entity names as references.

:author: P. Vennström (VTT)
:date:   17.12.2018
"""

from .exception import SpineIntegrityError
from .check_functions import (
    check_object_class,
    check_object,
    check_wide_relationship_class,
    check_wide_relationship,
    check_parameter_definition,
    check_parameter_value,
    check_parameter_tag,
    check_parameter_definition_tag,
    check_wide_parameter_value_list,
)
from .parameter_value import to_database


class ImportErrorLogItem:
    """Class to hold log data for import errors"""

    def __init__(self, msg="", db_type="", imported_from="", other=""):
        self.msg = msg
        self.db_type = db_type
        self.imported_from = imported_from
        self.other = other


def import_data(
    db_map,
    object_classes=(),
    relationship_classes=(),
    parameter_value_lists=(),
    object_parameters=(),
    relationship_parameters=(),
    objects=(),
    relationships=(),
    object_parameter_values=(),
    relationship_parameter_values=(),
):
    """Imports data into a Spine database using name references (rather than id
    references).

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

            import_data(db_map,
                        object_classes=object_c,
                        relationship_classes=relationship_c,
                        object_parameters=obj_parameters,
                        relationship_parameters=rel_parameters,
                        objects=objects,
                        relationships=relationships,
                        object_parameter_values=object_p_values,
                        relationship_parameter_values=rel_p_values)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): database mapping
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
        object_parameter_values (List[List[str, str, str|numeric]]):
            list of lists with object name, parameter name, parameter value
        relationship_parameter_values (List[List[str, List(str), str, str|numeric]]):
            list of lists with relationship class name, list of object names, parameter name,
            parameter value

    Returns:
        number of inserted/changed entities and list of ImportErrorLogItem with
        any import errors
    """
    error_log = []
    num_imports = 0
    if object_classes:
        new, errors = import_object_classes(db_map, object_classes)
        num_imports = num_imports + new
        error_log.extend(errors)
    if relationship_classes:
        new, errors = import_relationship_classes(db_map, relationship_classes)
        num_imports = num_imports + new
        error_log.extend(errors)
    if parameter_value_lists:
        new, errors = import_parameter_value_lists(db_map, parameter_value_lists)
        num_imports = num_imports + new
        error_log.extend(errors)
    if object_parameters:
        new, errors = import_object_parameters(db_map, object_parameters)
        num_imports = num_imports + new
        error_log.extend(errors)
    if relationship_parameters:
        new, errors = import_relationship_parameters(db_map, relationship_parameters)
        num_imports = num_imports + new
        error_log.extend(errors)
    if objects:
        new, errors = import_objects(db_map, objects)
        num_imports = num_imports + new
        error_log.extend(errors)
    if relationships:
        new, errors = import_relationships(db_map, relationships)
        num_imports = num_imports + new
        error_log.extend(errors)
    if object_parameter_values:
        new, errors = import_object_parameter_values(db_map, object_parameter_values)
        num_imports = num_imports + new
        error_log.extend(errors)
    if relationship_parameter_values:
        new, errors = import_relationship_parameter_values(db_map, relationship_parameter_values)
        num_imports = num_imports + new
        error_log.extend(errors)
    return num_imports, error_log


def import_object_classes(db_map, object_classes):
    """Imports list of object classes by name into given database mapping.
    Ignores duplicate names and existing names.

    Example::

            data = ['new_object_class']
            import_objects(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        object_classes (Iterable): list/set/iterable of object class names (strings) to import

    Returns:
        (Int, List) Number of successful inserted object classes, list of errors
    """
    existing_classes = {oc.name: oc.id for oc in db_map.query(db_map.object_class_sq)}
    new_classes = []
    error_log = []
    for object_class_name in set(object_classes).difference(existing_classes):
        try:
            new_oc = {"name": object_class_name, "type_id": db_map.object_class_type}
            check_object_class(new_oc, existing_classes, db_map.object_class_type)
            new_classes.append(new_oc)
        except SpineIntegrityError as e:
            error_log.append(
                ImportErrorLogItem(
                    msg=f"Could not import object class '{object_class_name}': {e.msg}", db_type="object class"
                )
            )
    added = db_map._add_object_classes(*new_classes)
    return len(added), error_log


def import_objects(db_map, object_data):
    """Imports list of object by name with associated object class name into given database mapping:
    Ignores duplicate names and existing names.

    Example::

            data = [('object_class_name', 'new_object')]
            import_objects(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        object_data (List[List/Tuple]): list/set/iterable of lists/tuples with
                                 object name and object class name

    Returns:
        (Int, List) Number of successful inserted objects, list of errors
    """
    existing_classes = {oc.name: oc.id for oc in db_map.query(db_map.object_class_sq)}
    existing_objects = {(o.class_id, o.name): o.id for o in db_map.query(db_map.object_sq)}
    existing_class_ids = set(existing_classes.values())
    # Check that class exists for each object we want to insert
    error_log = []
    new_objects = []
    seen_objects = set()
    for oc_name, name in object_data:
        oc_id = existing_classes.get(oc_name, None)
        if (oc_id, name) in seen_objects or (oc_id, name) in existing_objects:
            continue
        db_object = {"name": name, "class_id": oc_id, "type_id": db_map.object_entity_type}
        try:
            check_object(db_object, existing_objects, existing_class_ids, db_map.object_entity_type)
            new_objects.append(db_object)
            seen_objects.add((oc_id, name))
        except SpineIntegrityError as e:
            error_log.append(
                ImportErrorLogItem(
                    msg=f"Could not import object'{name}' with class '{oc_name}': {e.msg}", db_type="object"
                )
            )
    added = db_map._add_objects(*new_objects)
    return len(added), error_log


def import_relationship_classes(db_map, relationship_classes):
    """Imports list of relationship class names with object classes:

    Example::

            data = [('new_rel_class', ['object_class_1', 'object_class_2'])]
            import_relationship_classes(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        relationship_classes (List[List/Tuple]): list/set/iterable of lists/tuples with
                                 relationship class names and list of object class names

    Returns:
        (Int, List) Number of successful inserted objects, list of errors
    """
    existing_classes = {oc.name: oc.id for oc in db_map.query(db_map.object_class_sq)}
    existing_classes_ids = set(existing_classes.values())
    existing_rel_classes = {x.name: x for x in db_map.query(db_map.wide_relationship_class_sq)}
    relationship_class_names = {name: x.id for name, x in existing_rel_classes.items()}
    seen_classes = set()
    error_log = []
    new_rc = []
    for name, oc_names in relationship_classes:
        oc_ids = tuple(existing_classes.get(oc, None) for oc in oc_names)
        if (name, oc_ids) in seen_classes:
            continue
        if name in existing_rel_classes:
            if ",".join(str(i) for i in oc_ids) == existing_rel_classes[name].object_class_id_list:
                continue
        rel_class = {"name": name, "object_class_id_list": oc_ids, "type_id": db_map.relationship_class_type}
        try:
            check_wide_relationship_class(
                rel_class, relationship_class_names, existing_classes_ids, db_map.relationship_class_type
            )
            new_rc.append(rel_class)
            relationship_class_names[name] = None
            seen_classes.add((name, oc_ids))
        except SpineIntegrityError as e:
            error_log.append(
                ImportErrorLogItem(
                    f"Could not import relationship class '{name}' with object classes class '{oc_names}': {e.msg}",
                    db_type="relationship class",
                )
            )
    added = db_map._add_wide_relationship_classes(*new_rc)
    return len(added), error_log


def import_object_parameters(db_map, parameter_data):
    """Imports list of object class parameters:

    Example::

            data = [
                ('object_class_1', 'new_parameter'),
                ('object_class_2', 'other_parameter', 'default_value', 'value_list_name')
            ]
            import_object_parameters(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        parameter_data (List[List/Tuple]): list/set/iterable of lists/tuples with
                                 object class name and parameter name

    Returns:
        (Int, List) Number of successful inserted objects, list of errors
    """

    obj_parameter_names = {
        (x.object_class_id, x.name): x.id for x in db_map.query(db_map.parameter_definition_sq) if x.object_class_id
    }
    object_class_dict = {x.id: x.name for x in db_map.query(db_map.object_class_sq)}
    existing_classes = {oc_name: oc_id for oc_id, oc_name in object_class_dict.items()}
    parameter_value_list_dict = {}
    existing_value_lists = {}
    for x in db_map.query(db_map.wide_parameter_value_list_sq):
        parameter_value_list_dict[x.id] = x.value_list
        existing_value_lists[x.name] = x.id
    seen_parameters = set()
    error_log = []
    new_parameters = []
    update_parameters = []
    for parameter in parameter_data:
        oc_name = parameter[0]
        parameter_name = parameter[1]
        oc_id = existing_classes.get(oc_name, None)
        p_id = obj_parameter_names.get((oc_id, parameter_name), None)
        param = {"name": parameter_name, "entity_class_id": oc_id}
        if len(parameter) > 2:
            param["default_value"] = to_database(parameter[2])
        if len(parameter) > 3:
            value_list_id = existing_value_lists.get(parameter[3])
            if value_list_id is not None:
                param["parameter_value_list_id"] = value_list_id
        if p_id is not None:
            # existing param
            param.update({"id": p_id})
        try:
            obj_parameter_names.pop((oc_id, parameter_name), None)
            check_parameter_definition(param, obj_parameter_names, object_class_dict.keys(), parameter_value_list_dict)
        except SpineIntegrityError as e:
            # Object class doesn't exists
            error_log.append(
                ImportErrorLogItem(
                    f"Could not import parameter '{parameter_name}' with class '{oc_name}': {e.msg}",
                    db_type="parameter_definition",
                )
            )
        checked_key = (oc_id, parameter_name)
        if checked_key not in seen_parameters:
            if p_id is not None:
                update_parameters.append(param)
            else:
                new_parameters.append(param)
            seen_parameters.add(checked_key)
    added = db_map._add_parameter_definitions(*new_parameters)
    updated = db_map._update_parameter_definitions(*update_parameters)
    return len(added) + len(updated), error_log


def import_relationship_parameters(db_map, parameter_data):
    """Imports list of relationship class parameters:

    Example::

            data = [
                ('relationship_class_1', 'new_parameter'),
                ('relationship_class_2', 'other_parameter', 'default_value', 'value_list_name')
            ]
            import_object_parameters(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        parameter_data (List[List/Tuple]): list/set/iterable of lists/tuples with
                                 relationship class name and parameter name

    Returns:
        (Int, List) Number of successful inserted objects, list of errors
    """
    rel_parameter_names = {
        (x.relationship_class_id, x.name): x.id
        for x in db_map.query(db_map.parameter_definition_sq)
        if x.relationship_class_id
    }
    relationship_class_dict = {x.id: x.name for x in db_map.query(db_map.wide_relationship_class_sq)}
    existing_classes = {rc_name: rc_id for rc_id, rc_name in relationship_class_dict.items()}
    parameter_value_list_dict = {}
    existing_value_lists = {}
    for x in db_map.query(db_map.wide_parameter_value_list_sq):
        parameter_value_list_dict[x.id] = x.value_list
        existing_value_lists[x.name] = x.id
    seen_parameters = set()

    error_log = []
    new_parameters = []
    update_parameters = []
    for parameter in parameter_data:
        rel_class_name = parameter[0]
        param_name = parameter[1]
        rc_id = existing_classes.get(rel_class_name, None)
        p_id = rel_parameter_names.get((rc_id, param_name), None)
        new_param = {"name": param_name, "entity_class_id": rc_id}
        if len(parameter) > 2:
            new_param["default_value"] = to_database(parameter[2])
        if len(parameter) > 3:
            value_list_id = existing_value_lists.get(parameter[3])
            if value_list_id is not None:
                new_param["parameter_value_list_id"] = value_list_id
        if p_id is not None:
            # existing param
            new_param.update({"id": p_id})
        try:
            rel_parameter_names.pop((rc_id, param_name), None)
            check_parameter_definition(
                new_param, rel_parameter_names, relationship_class_dict.keys(), parameter_value_list_dict
            )
        except SpineIntegrityError as e:
            # Relationship class doesn't exists
            error_log.append(
                ImportErrorLogItem(
                    msg=f"Could not import parameter '{param_name}' with class '{rel_class_name}': {e.msg}",
                    db_type="parameter_definition",
                )
            )
        checked_key = (rc_id, param_name)
        if checked_key not in seen_parameters:
            if p_id is not None:
                update_parameters.append(new_param)
            else:
                new_parameters.append(new_param)
            seen_parameters.add(checked_key)
    added = db_map._add_parameter_definitions(*new_parameters)
    updated = db_map._update_parameter_definitions(*update_parameters)
    return len(added) + len(updated), error_log


def import_relationships(db_map, relationship_data):
    """Imports list of relationships:

    Example::

            data = [('relationship_class_name', ('object_name1', 'object_name2'))]
            import_object_parameters(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        relationship_data (List[List/Tuple]): list/set/iterable of lists/tuples with
                                 relationship class name and list of object names

    Returns:
        (Int, List) Number of successful inserted objects, list of errors
    """

    relationships = {x.name: x for x in db_map.query(db_map.wide_relationship_sq)}
    relationship_names = {(x.class_id, x.name): x.id for x in relationships.values()}
    relationship_objects = {(x.class_id, x.object_id_list): x.id for x in relationships.values()}
    relationship_class_dict = {
        x.id: {"object_class_id_list": [int(y) for y in x.object_class_id_list.split(",")], "name": x.name}
        for x in db_map.query(db_map.wide_relationship_class_sq)
    }
    object_dict = {x.id: {"class_id": x.class_id, "name": x.name} for x in db_map.query(db_map.object_sq)}
    existing_objects = {(o["name"], o["class_id"]): o_id for o_id, o in object_dict.items()}
    existing_relationship_classes = {rc["name"]: rc_id for rc_id, rc in relationship_class_dict.items()}
    error_log = []
    new_relationships = []
    seen_relationships = set()
    for rel_class_name, object_names in relationship_data:
        rc_id = existing_relationship_classes.get(rel_class_name, None)
        rc_oc_id = relationship_class_dict.get(rc_id, {"object_class_id_list": []})["object_class_id_list"]
        if len(object_names) == len(rc_oc_id):
            o_ids = tuple(existing_objects.get((n, rc_oc_id[i]), None) for i, n in enumerate(object_names))
        else:
            o_ids = tuple(None for n in object_names)
        if (rc_id, o_ids) in seen_relationships:
            continue
        if (rc_id, ",".join(str(o) for o in o_ids)) in relationship_objects:
            continue
        object_names = [str(obj) for obj in object_names]
        new_rel = {
            "name": rel_class_name + "_" + "__".join(object_names),
            "class_id": rc_id,
            "object_id_list": o_ids,
            "object_class_id_list": rc_oc_id,
            "type_id": db_map.relationship_entity_type,
        }
        try:
            check_wide_relationship(
                new_rel,
                relationship_names,
                relationship_objects,
                relationship_class_dict,
                object_dict,
                db_map.relationship_entity_type,
            )
            new_relationships.append(new_rel)
            seen_relationships.add((rc_id, o_ids))
        except SpineIntegrityError as e:
            error_log.append(
                ImportErrorLogItem(
                    msg=f"Could not import relationship with entities '{object_names}' into '{rel_class_name}': {e.msg}",
                    db_type="relationship",
                )
            )

    added = db_map._add_wide_relationships(*new_relationships)
    return len(added), error_log


def import_object_parameter_values(db_map, data):
    """Imports list of object parameter values:

    Example::

            data = [('object_class_name', 'object_name', 'parameter_name', 123.4),
                    ('object_class_name', 'object_name', 'parameter_name2',
                        '{"type":"time_series", "data": [1,2,3]}')]
            import_object_parameter_values(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to
            insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with
            object_class_name, object name, parameter name, field name,
            (deserialized) parameter value

    Returns:
        (Int, List) Number of successful inserted objects, list of errors
    """

    object_class_dict = {x.name: x.id for x in db_map.query(db_map.object_class_sq)}
    object_parameter_values = {
        (x.object_id, x.parameter_id): x.id for x in db_map.query(db_map.object_parameter_value_sq)
    }
    parameter_dict = {
        x.id: {
            "name": x.name,
            "entity_class_id": x.entity_class_id,
            "parameter_value_list_id": x.parameter_value_list_id,
        }
        for x in db_map.query(db_map.parameter_definition_sq)
    }
    object_dict = {x.id: {"class_id": x.class_id, "name": x.name} for x in db_map.query(db_map.object_sq)}
    parameter_value_list_dict = {x.id: x.value_list for x in db_map.query(db_map.wide_parameter_value_list_sq)}
    existing_objects = {(o["name"], o["class_id"]): o_id for o_id, o in object_dict.items()}
    existing_parameters = {(p["name"], p["entity_class_id"]): p_id for p_id, p in parameter_dict.items()}
    error_log = []
    new_values = []
    update_values = []
    checked_new_values = set()
    for object_class, object_name, param_name, value in data:
        # get ids
        oc_id = object_class_dict.get(object_class, None)
        o_id = existing_objects.get((object_name, oc_id), None)
        p_id = existing_parameters.get((param_name, oc_id), None)
        pv_id = object_parameter_values.get((o_id, p_id), None)
        new_value = {"parameter_definition_id": p_id, "entity_id": o_id, "value": to_database(value)}
        if pv_id is not None:
            # existing value
            new_value.update({"id": pv_id})

        try:
            # check integrity
            object_parameter_values.pop((o_id, p_id), None)
            check_parameter_value(
                new_value, object_parameter_values, parameter_dict, object_dict, parameter_value_list_dict
            )
            new_value["entity_class_id"] = oc_id
        except SpineIntegrityError as e:
            error_log.append(
                ImportErrorLogItem(
                    f"Could not import parameter value for '{object_name}', class '{object_class}', parameter '{param_name}': {e.msg}",
                    db_type="parameter value",
                )
            )
            continue
        checked_key = (p_id, o_id)
        if checked_key not in checked_new_values:
            # new values
            if pv_id is not None:
                #  update
                update_values.append({"id": pv_id, "value": to_database(value)})
            else:
                # add
                new_values.append(new_value)
            # add to check new values to avoid duplicates
            checked_new_values.add(checked_key)
        else:
            # duplicate new value
            error_log.append(
                ImportErrorLogItem(
                    f"Could not import parameter value for '{object_name}', class '{object_class}', parameter '{param_name}': Duplicate value, only first will be considered",
                    "parameter_value",
                )
            )
    # add new
    added = db_map._add_parameter_values(*new_values)
    # Try and update whatever wasn't added
    updated = db_map._update_parameter_values(*update_values)
    return len(added) + len(updated), error_log


def import_relationship_parameter_values(db_map, data):
    """Imports list of object parameter values:

    Example::

            data = [['example_rel_class', ['example_object', 'other_object'], 'rel_parameter', 2.718]]
            import_relationship_parameter_values(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with
                                 relationship class name, list of object names, parameter name, field name,
                                 (deserialized) parameter value

    Returns:
        (Int, List) Number of successful inserted objects, list of errors
    """

    relationship_class_dict = {
        x.id: {"object_class_id_list": [int(y) for y in x.object_class_id_list.split(",")], "name": x.name}
        for x in db_map.query(db_map.wide_relationship_class_sq)
    }

    relationship_parameter_values = {
        (x.relationship_id, x.parameter_id): x.id for x in db_map.query(db_map.relationship_parameter_value_sq)
    }
    parameter_dict = {
        x.id: {
            "name": x.name,
            "entity_class_id": x.entity_class_id,
            "parameter_value_list_id": x.parameter_value_list_id,
        }
        for x in db_map.query(db_map.parameter_definition_sq)
    }
    object_dict = {x.id: {"class_id": x.class_id, "name": x.name} for x in db_map.query(db_map.object_sq)}
    relationship_dict = {
        x.id: {"class_id": x.class_id, "name": x.name, "object_id_list": [int(i) for i in x.object_id_list.split(",")]}
        for x in db_map.query(db_map.wide_relationship_sq)
    }
    parameter_value_list_dict = {x.id: x.value_list for x in db_map.query(db_map.wide_parameter_value_list_sq)}
    existing_objects = {(o["name"], o["class_id"]): o_id for o_id, o in object_dict.items()}
    existing_parameters = {(p["name"], p["entity_class_id"]): p_id for p_id, p in parameter_dict.items()}
    existing_relationship_classes = {oc.name: oc.id for oc in db_map.query(db_map.wide_relationship_class_sq)}
    existing_relationships = {
        (r["class_id"], tuple(r["object_id_list"])): r_id for r_id, r in relationship_dict.items()
    }

    error_log = []
    new_values = []
    update_values = []
    checked_new_values = set()
    for class_name, object_names, param_name, value in data:
        rc_id = existing_relationship_classes.get(class_name, None)
        rc_oc_id = relationship_class_dict.get(rc_id, {"object_class_id_list": []})["object_class_id_list"]
        if len(object_names) == len(rc_oc_id):
            o_ids = tuple(existing_objects.get((n, rc_oc_id[i]), None) for i, n in enumerate(object_names))
        else:
            o_ids = tuple(None for n in object_names)
        rel_key = (rc_id, o_ids)
        r_id = existing_relationships.get(rel_key, None)
        p_id = existing_parameters.get((param_name, rc_id), None)
        pv_id = relationship_parameter_values.get((r_id, p_id), None)
        new_value = {"parameter_definition_id": p_id, "entity_id": r_id, "value": to_database(value)}
        if pv_id is not None:
            # existing value
            new_value.update({"id": pv_id})

        try:
            # check integrity
            relationship_parameter_values.pop((r_id, p_id), None)
            check_parameter_value(
                new_value, relationship_parameter_values, parameter_dict, relationship_dict, parameter_value_list_dict
            )
            new_value["entity_class_id"] = rc_id
        except SpineIntegrityError as e:
            error_log.append(
                ImportErrorLogItem(
                    msg=f"Could not import parameter value for '{object_names}', class '{class_name}', parameter '{param_name}': {e.msg}",
                    db_type="parameter value",
                )
            )
            continue

        checked_key = (p_id, r_id)
        if checked_key not in checked_new_values:
            # new values
            if pv_id is not None:
                #  update
                update_values.append({"id": pv_id, "value": to_database(value)})
            else:
                # add
                new_values.append(new_value)
            # add to check new values to avoid duplicates
            checked_new_values.add(checked_key)
        else:
            # duplicate new value
            error_log.append(
                ImportErrorLogItem(
                    f"Could not import parameter value for '{object_names}', class '{class_name}', parameter '{param_name}': Duplicate parameter value only first value will be considered",
                    "parameter_value",
                )
            )

    # add new
    added = db_map._add_parameter_values(*new_values)
    # Try and update whatever wasn't added
    updated = db_map._update_parameter_values(*update_values)
    return len(added) + len(updated), error_log


def import_parameter_value_lists(db_map, data):
    """Imports list of parameter value lists:

    Example::

            data = [
                ['value_list_name', ['value1', 'value2', 'value3'],
                ['another_value_list_name', ['value5', 'value4'],
            ]
            import_parameter_value_lists(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with
                                 value list name, list of values

    Returns:
        (Int, List) Number of successful inserted objects, list of errors
    """
    existing_ids_by_name = {}
    for x in db_map.query(db_map.wide_parameter_value_list_sq):
        existing_ids_by_name[x.name] = x.id
    seen = set()
    error_log = []
    to_add = []
    to_update = []
    for name, value_list in data:
        item = {"name": name, "value_list": value_list}
        id_ = existing_ids_by_name.pop(name, None)
        try:
            check_wide_parameter_value_list(item, existing_ids_by_name)
        except SpineIntegrityError as e:
            error_log.append(
                ImportErrorLogItem(
                    f"Could not import parameter value list '{name}' with values '{value_list}': {e.msg}",
                    db_type="parameter value list",
                )
            )
            continue
        finally:
            if id_ is not None:
                # Restablish ids
                existing_ids_by_name[name] = id_
        if name in seen:
            error_log.append(
                ImportErrorLogItem(
                    f"Could not import parameter value list '{name}': Duplicate list, only first will be considered",
                    "parameter value list",
                )
            )
            continue
        seen.add(name)
        if id_ is not None:
            item["id"] = id_
            to_update.append(item)
        else:
            to_add.append(item)
    added = db_map._add_wide_parameter_value_lists(*to_add)
    updated = db_map.update_wide_parameter_value_lists(*to_update)
    return len(added) + len(updated), error_log
