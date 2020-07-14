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

from .diff_database_mapping import DiffDatabaseMapping
from .exception import SpineIntegrityError, SpineDBAPIError
from .check_functions import (
    check_object_class,
    check_object,
    check_wide_relationship_class,
    check_wide_relationship,
    check_entity_group,
    check_parameter_definition,
    check_parameter_value,
    check_parameter_tag,
    check_parameter_definition_tag,
    check_wide_parameter_value_list,
)
from .parameter_value import to_database

# TODO: Now `import_data` and `get_data_for_import` are called more openly from user provided data, we may want to
# distrust the input a little bit more and insert some try excepts


class ImportErrorLogItem:
    """Class to hold log data for import errors"""

    def __init__(self, msg="", db_type="", imported_from="", other=""):
        self.msg = msg
        self.db_type = db_type
        self.imported_from = imported_from
        self.other = other

    def __str__(self):
        return self.msg


def import_data_to_url(url, upgrade=False, **kwargs):
    db_map = DiffDatabaseMapping(url, upgrade=upgrade)
    num_imports, error_log = import_data(db_map, **kwargs)
    if num_imports:
        try:
            db_map.commit_session("Import data using `import_data_to_url`")
        except SpineDBAPIError as e:
            db_map.rollback_session()
            err_item = ImportErrorLogItem(msg=f"Error while committing changes: {e.msg}")
            error_log.append(err_item)
    return num_imports, error_log


def import_data(
    db_map,
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
    if object_groups:
        new, errors = import_object_groups(db_map, object_groups)
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


def get_data_for_import(
    db_map,
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
):
    """Returns an iterator of data for import, that the user can call instead of `import_data`
    if they want to add and update the data by themselves.
    Especially intended to be used with the toolbox undo/redo functionality.

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
        dict(str, list)
    """
    if object_classes:
        yield ("object class", _get_object_classes_for_import(db_map, object_classes))
    if relationship_classes:
        yield ("relationship class", _get_relationship_classes_for_import(db_map, relationship_classes))
    if parameter_value_lists:
        yield ("parameter value list", _get_parameter_value_lists_for_import(db_map, parameter_value_lists))
    if object_parameters:
        yield ("parameter definition", _get_object_parameters_for_import(db_map, object_parameters))
    if relationship_parameters:
        yield ("parameter definition", _get_relationship_parameters_for_import(db_map, relationship_parameters))
    if objects:
        yield ("object", _get_objects_for_import(db_map, objects))
    if relationships:
        yield ("relationship", _get_relationships_for_import(db_map, relationships))
    if object_groups:
        yield ("entity group", _get_object_groups_for_import(db_map, object_groups))
    if object_parameter_values:
        yield ("parameter value", _get_object_parameter_values_for_import(db_map, object_parameter_values))
    if relationship_parameter_values:
        yield ("parameter value", _get_relationship_parameter_values_for_import(db_map, relationship_parameter_values))


def import_object_classes(db_map, data):
    """Imports object classes.

    Example::

            data = ['new_object_class', ('another_object_class', 'description', 123456)]
            import_object_classes(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (Iterable): list/set/iterable of string object class names, or of lists/tuples with object class names,
            and optionally description and integer display icon reference

    Returns:
        (Int, List) Number of successful inserted object classes, list of errors
    """
    to_add, to_update, error_log = _get_object_classes_for_import(db_map, data)
    added = db_map._add_object_classes(*to_add)
    updated = db_map._update_object_classes(*to_update)
    return len(added) + len(updated), error_log


def _get_object_classes_for_import(db_map, data):
    object_class_ids = {oc.name: oc.id for oc in db_map.query(db_map.object_class_sq)}
    checked = set()
    to_add = []
    to_update = []
    error_log = []
    for object_class in data:
        if isinstance(object_class, str):
            name = object_class
            item = {"name": name, "type_id": db_map.object_class_type}
        else:
            name, *optionals = object_class
            item = {"name": name, "type_id": db_map.object_class_type}
            item.update(dict(zip(("description", "display_icon"), optionals)))
        if name in checked:
            continue
        oc_id = object_class_ids.pop(name, None)
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


def import_relationship_classes(db_map, data):
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
    to_add, to_update, error_log = _get_relationship_classes_for_import(db_map, data)
    added = db_map._add_wide_relationship_classes(*to_add)
    updated = db_map._update_wide_relationship_classes(*to_update)
    return len(added) + len(updated), error_log


def _get_relationship_classes_for_import(db_map, data):
    object_class_ids = {oc.name: oc.id for oc in db_map.query(db_map.object_class_sq)}
    relationship_class_ids = {x.name: x.id for x in db_map.query(db_map.wide_relationship_class_sq)}
    checked = set()
    error_log = []
    to_add = []
    to_update = []
    for name, oc_names, *optionals in data:
        ## This block is temporarliy shaded as it does not make much sense at present 
        # if name in checked:
        #     error_log.append(f"Duplicate relationship class '{name}'")
        #     continue
        oc_ids = tuple(object_class_ids.get(oc, None) for oc in oc_names)
        item = {"name": name, "object_class_id_list": list(oc_ids), "type_id": db_map.relationship_class_type}
        item.update(dict(zip(("description",), optionals)))
        rc_id = relationship_class_ids.pop(name, None)
        try:
            check_wide_relationship_class(
                item, relationship_class_ids, set(object_class_ids.values()), db_map.relationship_class_type
            )
        except SpineIntegrityError as e:
            error_log.append(
                ImportErrorLogItem(
                    f"Could not import relationship class '{name}' with object classes '{oc_names}': {e.msg}",
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


def import_objects(db_map, data):
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
    to_add, _, error_log = _get_objects_for_import(db_map, data)
    added = db_map._add_objects(*to_add)
    return len(added), error_log


def _get_objects_for_import(db_map, data):
    object_class_ids = {oc.name: oc.id for oc in db_map.query(db_map.object_class_sq)}
    object_ids = {(o.class_id, o.name): o.id for o in db_map.query(db_map.object_sq)}
    checked = set()
    error_log = []
    to_add = []
    to_update = []
    for oc_name, name, *optionals in data:
        oc_id = object_class_ids.get(oc_name, None)
        if (oc_id, name) in checked:
            continue
        item = {"name": name, "class_id": oc_id, "type_id": db_map.object_entity_type}
        item.update(dict(zip(("description",), optionals)))
        o_id = object_ids.pop((oc_id, name), None)
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


def import_object_groups(db_map, data):
    """Imports list of object groups by name with associated object class name into given database mapping:
    Ignores duplicate and existing (group, member) tuples.

    Example::

            data = [
                ('object_class_name', 'object_group_name', ['member_name', 'another_member_name'])
            ]
            import_objects(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with object class name, group name,
            and list of member names

    Returns:
        (Int, List) Number of successful inserted objects, list of errors
    """
    to_add, _, error_log = _get_object_groups_for_import(db_map, data)
    added = db_map._add_entity_groups(*to_add)
    return len(added), error_log


def _get_object_groups_for_import(db_map, data):
    object_class_ids = {oc.name: oc.id for oc in db_map.query(db_map.object_class_sq)}
    object_ids = {(o.class_id, o.name): o.id for o in db_map.query(db_map.object_sq)}
    objects = {}
    for obj in db_map.query(db_map.object_sq):
        objects.setdefault(obj.class_id, dict())[obj.id] = obj._asdict()
    entity_groups = {(x.entity_id, x.member_id): x.id for x in db_map.query(db_map.entity_group_sq)}
    error_log = []
    to_add = []
    seen = set()
    for class_name, group_name, member_names in data:
        oc_id = object_class_ids.get(class_name)
        og_id = object_ids.get((oc_id, group_name))
        for member_name in member_names:
            mo_id = object_ids.get((oc_id, member_name))
            if (og_id, mo_id) in seen | entity_groups.keys():
                continue
            item = {"entity_class_id": oc_id, "entity_id": og_id, "member_id": mo_id}
            try:
                check_entity_group(item, entity_groups, objects)
                to_add.append(item)
                seen.add((og_id, mo_id))
            except SpineIntegrityError as e:
                error_log.append(
                    ImportErrorLogItem(
                        msg=f"Could not import object '{member_name}' into group '{group_name}': {e.msg}",
                        db_type="entity group",
                    )
                )
    return to_add, [], error_log


def import_relationships(db_map, data):
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
    to_add, _, error_log = _get_relationships_for_import(db_map, data)
    added = db_map._add_wide_relationships(*to_add)
    return len(added), error_log


def _get_relationships_for_import(db_map, data):
    relationships = {x.name: x for x in db_map.query(db_map.wide_relationship_sq)}
    relationship_ids_per_name = {(x.class_id, x.name): x.id for x in relationships.values()}
    relationship_ids_per_obj_lst = {(x.class_id, x.object_id_list): x.id for x in relationships.values()}
    relationship_classes = {
        x.id: {"object_class_id_list": [int(y) for y in x.object_class_id_list.split(",")], "name": x.name}
        for x in db_map.query(db_map.wide_relationship_class_sq)
    }
    objects = {x.id: {"class_id": x.class_id, "name": x.name} for x in db_map.query(db_map.object_sq)}
    object_ids = {(o["name"], o["class_id"]): o_id for o_id, o in objects.items()}
    relationship_class_ids = {rc["name"]: rc_id for rc_id, rc in relationship_classes.items()}
    object_class_id_lists = {rc_id: rc["object_class_id_list"] for rc_id, rc in relationship_classes.items()}
    error_log = []
    to_add = []
    seen = set()
    for class_name, object_names in data:
        rc_id = relationship_class_ids.get(class_name, None)
        oc_ids = object_class_id_lists.get(rc_id, [])
        if len(object_names) == len(oc_ids):
            o_ids = tuple(object_ids.get((name, oc_id), None) for name, oc_id in zip(object_names, oc_ids))
        else:
            o_ids = tuple(None for _ in object_names)
        if (rc_id, o_ids) in seen or (rc_id, ",".join(str(o) for o in o_ids)) in relationship_ids_per_obj_lst:
            continue
        object_names = [str(obj) for obj in object_names]
        item = {
            "name": class_name + "_" + "__".join(object_names),
            "class_id": rc_id,
            "object_id_list": list(o_ids),
            "object_class_id_list": oc_ids,
            "type_id": db_map.relationship_entity_type,
        }
        try:
            check_wide_relationship(
                item,
                relationship_ids_per_name,
                relationship_ids_per_obj_lst,
                relationship_classes,
                objects,
                db_map.relationship_entity_type,
            )
            to_add.append(item)
            seen.add((rc_id, o_ids))
        except SpineIntegrityError as e:
            error_log.append(
                ImportErrorLogItem(
                    msg=f"Could not import relationship with objects '{object_names}' into '{class_name}': {e.msg}",
                    db_type="relationship",
                )
            )
    return to_add, [], error_log


def import_object_parameters(db_map, data):
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
    to_add, to_update, error_log = _get_object_parameters_for_import(db_map, data)
    added = db_map._add_parameter_definitions(*to_add)
    updated = db_map._update_parameter_definitions(*to_update)
    return len(added) + len(updated), error_log


def _get_object_parameters_for_import(db_map, data):
    parameter_ids = {
        (x.object_class_id, x.name): x.id for x in db_map.query(db_map.parameter_definition_sq) if x.object_class_id
    }
    object_class_names = {x.id: x.name for x in db_map.query(db_map.object_class_sq)}
    object_class_ids = {oc_name: oc_id for oc_id, oc_name in object_class_names.items()}
    parameter_value_lists = {}
    parameter_value_list_ids = {}
    for x in db_map.query(db_map.wide_parameter_value_list_sq):
        parameter_value_lists[x.id] = x.value_list
        parameter_value_list_ids[x.name] = x.id
    checked = set()
    error_log = []
    to_add = []
    to_update = []
    functions = [to_database, parameter_value_list_ids.get, lambda x: x]
    for class_name, parameter_name, *optionals in data:
        oc_id = object_class_ids.get(class_name, None)
        checked_key = (oc_id, parameter_name)
        if checked_key in checked:
            continue
        item = {"name": parameter_name, "entity_class_id": oc_id}
        optionals = [f(x) for f, x in zip(functions, optionals)]
        item.update(dict(zip(("default_value", "parameter_value_list_id", "description"), optionals)))
        p_id = parameter_ids.pop((oc_id, parameter_name), None)
        try:
            check_parameter_definition(item, parameter_ids, object_class_names.keys(), parameter_value_lists)
        except SpineIntegrityError as e:
            # Object class doesn't exists
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


def import_relationship_parameters(db_map, data):
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
    to_add, to_update, error_log = _get_relationship_parameters_for_import(db_map, data)
    added = db_map._add_parameter_definitions(*to_add)
    updated = db_map._update_parameter_definitions(*to_update)
    return len(added) + len(updated), error_log


def _get_relationship_parameters_for_import(db_map, data):
    parameter_ids = {
        (x.relationship_class_id, x.name): x.id
        for x in db_map.query(db_map.parameter_definition_sq)
        if x.relationship_class_id
    }
    relationship_class_names = {x.id: x.name for x in db_map.query(db_map.wide_relationship_class_sq)}
    relationship_class_ids = {rc_name: rc_id for rc_id, rc_name in relationship_class_names.items()}
    parameter_value_lists = {}
    parameter_value_list_ids = {}
    for x in db_map.query(db_map.wide_parameter_value_list_sq):
        parameter_value_lists[x.id] = x.value_list
        parameter_value_list_ids[x.name] = x.id
    error_log = []
    to_add = []
    to_update = []
    checked = set()
    functions = [to_database, parameter_value_list_ids.get, lambda x: x]
    for class_name, parameter_name, *optionals in data:
        rc_id = relationship_class_ids.get(class_name, None)
        checked_key = (rc_id, parameter_name)
        if checked_key in checked:
            continue
        item = {"name": parameter_name, "entity_class_id": rc_id}
        optionals = [f(x) for f, x in zip(functions, optionals)]
        item.update(dict(zip(("default_value", "parameter_value_list_id", "description"), optionals)))
        p_id = parameter_ids.pop((rc_id, parameter_name), None)
        try:
            check_parameter_definition(item, parameter_ids, relationship_class_names.keys(), parameter_value_lists)
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


def import_object_parameter_values(db_map, data):
    """Imports object parameter values:

    Example::

            data = [('object_class_name', 'object_name', 'parameter_name', 123.4),
                    ('object_class_name', 'object_name', 'parameter_name2',
                        '{"type":"time_series", "data": [1,2,3]}')]
            import_object_parameter_values(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with
            object_class_name, object name, parameter name, (deserialized) parameter value

    Returns:
        (Int, List) Number of successful inserted objects, list of errors
    """
    to_add, to_update, error_log = _get_object_parameter_values_for_import(db_map, data)
    added = db_map._add_parameter_values(*to_add)
    updated = db_map._update_parameter_values(*to_update)
    return len(added) + len(updated), error_log


def _get_object_parameter_values_for_import(db_map, data):
    object_class_ids = {x.name: x.id for x in db_map.query(db_map.object_class_sq)}
    parameter_value_ids = {(x.object_id, x.parameter_id): x.id for x in db_map.query(db_map.object_parameter_value_sq)}
    parameters = {x.id: x._asdict() for x in db_map.query(db_map.parameter_definition_sq)}
    objects = {x.id: {"class_id": x.class_id, "name": x.name} for x in db_map.query(db_map.object_sq)}
    parameter_value_lists = {x.id: x.value_list for x in db_map.query(db_map.wide_parameter_value_list_sq)}
    object_ids = {(o["name"], o["class_id"]): o_id for o_id, o in objects.items()}
    parameter_ids = {(p["name"], p["entity_class_id"]): p_id for p_id, p in parameters.items()}
    error_log = []
    to_add = []
    to_update = []
    checked = set()
    for class_name, object_name, parameter_name, value in data:
        oc_id = object_class_ids.get(class_name, None)
        o_id = object_ids.get((object_name, oc_id), None)
        p_id = parameter_ids.get((parameter_name, oc_id), None)
        checked_key = (o_id, p_id)
        if checked_key in checked:
            error_log.append(
                ImportErrorLogItem(
                    msg="Could not import parameter value for '{0}', class '{1}', parameter '{2}': {3}".format(
                        object_name,
                        class_name,
                        parameter_name,
                        "Duplicate parameter value, only first value will be considered.",
                    ),
                    db_type="parameter value",
                )
            )
            continue
        item = {
            "parameter_definition_id": p_id,
            "entity_class_id": oc_id,
            "entity_id": o_id,
            "value": to_database(value),
        }
        pv_id = parameter_value_ids.pop((o_id, p_id), None)
        try:
            check_parameter_value(item, parameter_value_ids, parameters, objects, parameter_value_lists)
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
                parameter_value_ids[o_id, p_id] = pv_id
        checked.add(checked_key)
        if pv_id is not None:
            to_update.append({"id": pv_id, "value": item["value"]})
        else:
            to_add.append(item)
    return to_add, to_update, error_log


def import_relationship_parameter_values(db_map, data):
    """Imports relationship parameter values:

    Example::

            data = [['example_rel_class', ['example_object', 'other_object'], 'rel_parameter', 2.718]]
            import_relationship_parameter_values(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with
            relationship class name, list of object names, parameter name, (deserialized) parameter value

    Returns:
        (Int, List) Number of successful inserted objects, list of errors
    """
    to_add, to_update, error_log = _get_relationship_parameter_values_for_import(db_map, data)
    added = db_map._add_parameter_values(*to_add)
    updated = db_map._update_parameter_values(*to_update)
    return len(added) + len(updated), error_log


def _get_relationship_parameter_values_for_import(db_map, data):
    object_class_id_lists = {
        x.id: [int(id_) for id_ in x.object_class_id_list.split(",")]
        for x in db_map.query(db_map.wide_relationship_class_sq)
    }
    parameter_value_ids = {
        (x.relationship_id, x.parameter_id): x.id for x in db_map.query(db_map.relationship_parameter_value_sq)
    }
    parameters = {x.id: x._asdict() for x in db_map.query(db_map.parameter_definition_sq)}
    relationships = {
        x.id: {"class_id": x.class_id, "name": x.name, "object_id_list": [int(i) for i in x.object_id_list.split(",")]}
        for x in db_map.query(db_map.wide_relationship_sq)
    }
    parameter_value_lists = {x.id: x.value_list for x in db_map.query(db_map.wide_parameter_value_list_sq)}
    parameter_ids = {(p["entity_class_id"], p["name"]): p_id for p_id, p in parameters.items()}
    relationship_ids = {(r["class_id"], tuple(r["object_id_list"])): r_id for r_id, r in relationships.items()}
    object_ids = {(o.name, o.class_id): o.id for o in db_map.query(db_map.object_sq)}
    relationship_class_ids = {oc.name: oc.id for oc in db_map.query(db_map.wide_relationship_class_sq)}
    error_log = []
    to_add = []
    to_update = []
    checked = set()
    for class_name, object_names, parameter_name, value in data:
        rc_id = relationship_class_ids.get(class_name, None)
        oc_ids = object_class_id_lists.get(rc_id, [])
        if len(object_names) == len(oc_ids):
            o_ids = tuple(object_ids.get((name, oc_id), None) for name, oc_id in zip(object_names, oc_ids))
        else:
            o_ids = tuple(None for _ in object_names)
        r_id = relationship_ids.get((rc_id, o_ids), None)
        p_id = parameter_ids.get((rc_id, parameter_name), None)
        checked_key = (r_id, p_id)
        if checked_key in checked:
            error_log.append(
                ImportErrorLogItem(
                    msg="Could not import parameter value for '{0}', class '{1}', parameter '{2}': {3}".format(
                        object_names,
                        class_name,
                        parameter_name,
                        "Duplicate parameter value, only first value will be considered.",
                    ),
                    db_type="parameter value",
                )
            )
            continue
        item = {
            "parameter_definition_id": p_id,
            "entity_class_id": rc_id,
            "entity_id": r_id,
            "value": to_database(value),
        }
        pv_id = parameter_value_ids.pop((r_id, p_id), None)
        try:
            check_parameter_value(item, parameter_value_ids, parameters, relationships, parameter_value_lists)
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
                parameter_value_ids[r_id, p_id] = pv_id
        checked.add(checked_key)
        if pv_id is not None:
            to_update.append({"id": pv_id, "value": item["value"]})
        else:
            to_add.append(item)
    return to_add, to_update, error_log


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
    to_add, to_update, error_log = _get_parameter_value_lists_for_import(db_map, data)
    added = db_map._add_wide_parameter_value_lists(*to_add)
    updated = db_map.update_wide_parameter_value_lists(*to_update)
    return len(added) + len(updated), error_log


def _get_parameter_value_lists_for_import(db_map, data):
    parameter_value_list_ids = {x.name: x.id for x in db_map.query(db_map.wide_parameter_value_list_sq)}
    seen = set()
    error_log = []
    to_add = []
    to_update = []
    for name, value_list in data:
        if name in seen:
            error_log.append(
                ImportErrorLogItem(
                    msg=f"Could not import parameter value list '{name}': Duplicate list, only first will be considered",
                    db_type="parameter value list",
                )
            )
            continue
        item = {"name": name, "value_list": [to_database(value) for value in value_list]}
        pvl_id = parameter_value_list_ids.pop(name, None)
        try:
            check_wide_parameter_value_list(item, parameter_value_list_ids)
        except SpineIntegrityError as e:
            error_log.append(
                ImportErrorLogItem(
                    msg=f"Could not import parameter value list '{name}' with values '{value_list}': {e.msg}",
                    db_type="parameter value list",
                )
            )
            continue
        finally:
            if pvl_id is not None:
                parameter_value_list_ids[name] = pvl_id
        seen.add(name)
        if pvl_id is not None:
            item["id"] = pvl_id
            to_update.append(item)
        else:
            to_add.append(item)
    return to_add, to_update, error_log
