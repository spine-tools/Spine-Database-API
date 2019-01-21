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
Functions for importing values into spine database format when only name is known

:author: P. Vennström (VTT)
:date:   17.12.2018
"""

class ImportErrorLogItem():
    """Class to hold log data for import errors"""
    def __init__(self, msg='', db_type='', imported_from='', other=''):
        self.msg = msg
        self.db_type = db_type
        self.imported_from = imported_from
        self.other = other

def import_data(db_map, object_classes=[], relationship_classes=[], object_parameters=[],
                relationship_parameters=[], objects=[], relationships=[],
                object_parameter_values=[], relationship_parameter_values=[]):
    """Imports data without ids into spine database by identifying by name

    Ex:
        object_c = ['example_class', 'other_class']
        obj_parameters = [['example_class', 'example_parameter']]
        relationship_c = [['example_rel_class', ['example_class', 'other_class']]]
        rel_parameters = [['example_rel_class', 'rel_parameter']]
        objects = [['example_class', 'example_object'],
                   ['other_class', 'other_object']]
        object_p_values = [['example_object', 'example_parameter', 'value', 3.14]]
        relationships = [['example_rel_class', ['example_object', 'other_object']]]
        rel_p_values = [['example_rel_class', ['example_object', 'other_object'], 'rel_parameter', 'value', 2.718]]

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
        db_map (spinetoolbox_api.DiffDatabaseMapping): database mapping
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
        object_parameter_values (List[List[str, str, 'json'|'value', str|numeric]]):
            list of lists with object name, parameter name, field name, parameter value
        relationship_parameter_values (List[List[str, List(str), str, 'json'|'value', str|numeric]]):
            list of lists with relationship class name, list of object names, parameter name, field name,
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
    """Imports list of object class names into given database mapping.
    Skips duplicate names and existing names.
        ex:
            data = ['new_object']
            import_objects(db_map, data)

    Args:
        db (spinedatabase_api.DiffDatabaseMapping): mapping for database to insert into
        data (Iterable): list/set/iterable of object class names (strings) to import

    Returns:
        (Int, List) Number of succesfull inserted object classes, list of errors
    """
    new_classes = [{"name": o} for o in set(object_classes)]
    added, error_log = db_map.add_object_classes(*new_classes, raise_intgr_error=False)
    return added.count(), [ImportErrorLogItem(msg=msg, db_type="object class") for msg in error_log]


def import_objects(db_map, object_data):
    """Imports list of object names with object classes:
        ex:
            data = [('new_object', 'object_class_name')]
            import_objects(db_map, data)

    Args:
        db (spinedatabase_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with
                                 object name and object class name

    Returns:
        (Int, List) Number of succesfull inserted objects, list of errors
    """
    existing_classes = {oc.name: oc.id for oc in db_map.object_class_list()}
    # Check that class exists for each object we want to insert
    error_log = []
    new_objects = []
    for o in object_data:
        name = o[0]
        oc_name = o[1]
        try:
            oc_id = existing_classes[oc_name]
        except KeyError:
            # Class doesn't exists
            error_log.append("Object '{}' can't be inserted "
                             "because class '{}' doesn't exist in database".format(name, oc_name))
            continue
        new_objects.append({'name': name, 'class_id': oc_id})
    added, intgr_error_log = db_map.add_objects(*new_objects, raise_intgr_error=False)
    error_log.extend(intgr_error_log)
    return added.count(), [ImportErrorLogItem(msg=msg, db_type="object") for msg in error_log]


def import_relationship_classes(db_map, relationship_classes):
    """Imports list of relationship class names with object classes:
        ex:
            data = [('new_rel_class', ['object_class_1, object_class_2])]
            import_relationship_classes(db_map, data)

    Args:
        db (spinedatabase_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with
                                 relationship class names and list of object class names

    Returns:
        (Int, List) Number of succesfull inserted objects, list of errors
    """
    existing_classes = {oc.name: oc.id for oc in db_map.object_class_list()}
    error_log = []
    new_rc = []
    for rc in relationship_classes:
        name = rc[0]
        oc_names = rc[1]
        oc_ids = [existing_classes[oc] for oc in oc_names if oc in existing_classes]
        if len(oc_ids) != len(oc_names):
            # not all object_classes exists in database
            error_log.append("Relationship class '{}' can't be inserted because it contains object classes "
                             "that doesn't exist in db".format(name))
            continue
        # new relationship class
        new_rc.append({'name': name, 'object_class_id_list': oc_ids})
    added, intgr_error_log = db_map.add_wide_relationship_classes(*new_rc, raise_intgr_error=False)
    error_log.extend(intgr_error_log)
    return added.count(), [ImportErrorLogItem(msg=msg, db_type="relationship class") for msg in error_log]


def import_object_parameters(db_map, parameter_data):
    """Imports list of object class parameters:
        ex:
            data = [('new_parameter', 'object_class_1')]
            import_object_parameters(db_map, data)

    Args:
        db (spinedatabase_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with
                                 object class name and parameter name

    Returns:
        (Int, List) Number of succesfull inserted objects, list of errors
    """
    existing_classes = {oc.name: oc.id for oc in db_map.object_class_list()}
    error_log = []
    new_parameters = []
    for p in parameter_data:
        name = p[0]
        oc_name = p[1]
        try:
            oc_id = existing_classes[oc_name]
        except KeyError:
            # Object class doesn't exists
            error_log.append("Parameter '{}' can't be inserted "
                             "because object class '{}' doesn't exist in database".format(name, oc_name))
            continue
        # new parameter
        new_parameters.append({'name': name, 'object_class_id': oc_id})
    added, intgr_error_log = db_map.add_parameters(*new_parameters, raise_intgr_error=False)
    error_log.extend(intgr_error_log)
    return added.count(), [ImportErrorLogItem(msg=msg, db_type="parameter") for msg in error_log]


def import_relationship_parameters(db_map, parameter_data):
    """Imports list of relationship class parameters:
        ex:
            data = [('new_parameter', 'relationship_class_name')]
            import_object_parameters(db_map, data)

    Args:
        db (spinedatabase_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with
                                 relationship class name and parameter name

    Returns:
        (Int, List) Number of succesfull inserted objects, list of errors
    """
    existing_classes = {oc.name: oc.id for oc in db_map.wide_relationship_class_list()}
    error_log = []
    new_parameters = []
    for p in parameter_data:
        name = p[0]
        rc_name = p[1]
        try:
            rc_id = existing_classes[rc_name]
        except KeyError:
            # Relationship class doesn't exists
            error_log.append("Parameter '{}' can't be inserted "
                             "because relationship class '{}' doesn't exist in database".format(name, rc_name))
            continue
        # new parameter
        new_parameters.append({'name': name, 'relationship_class_id': rc_id})
    added, intgr_error_log = db_map.add_parameters(*new_parameters, raise_intgr_error=False)
    error_log.extend(intgr_error_log)
    return added.count(), [ImportErrorLogItem(msg=msg, db_type="parameter") for msg in error_log]


def import_relationships(db_map, relationship_data):
    """Imports list of relationships:
        ex:
            data = [('relationship_class_name', ('object_name1','object_name2'))]
            import_object_parameters(db_map, data)

    Args:
        db (spinedatabase_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with
                                 relationship class name and list of object names

    Returns:
        (Int, List) Number of succesfull inserted objects, list of errors
    """
    existing_objects = {o.name: o.id for o in db_map.object_list()}
    existing_relationship_classes = {oc.name: oc.id for oc in db_map.wide_relationship_class_list()}
    error_log = []
    new_relationships = []
    for r in relationship_data:
        o_names = r[1]
        rc_name = r[0]
        try:
            rc_id = existing_relationship_classes[rc_name]
        except KeyError:
            # Relationship class doesn't exist
            error_log.append("Relationship '{0}: {1}' can't be inserted because "
                             "relationship class '{0}' doesn't exist in database".format(rc_name, ','.join(o_names)))
            continue
        o_ids = tuple(existing_objects[n] for n in o_names if n in existing_objects)
        if len(o_ids) != len(o_names):
            # not all objects exist
            error_log.append("Relationship '{}: {}' can't be inserted because it contains objects "
                             "that don't exist in db".format(rc_name, ','.join(o_names)))
            continue
        new_relationships.append(
            {'name': rc_name + '_' + '__'.join(o_names), 'class_id': rc_id, 'object_id_list': o_ids})
    added, intgr_error_log = db_map.add_wide_relationships(*new_relationships, raise_intgr_error=False)
    error_log.extend(intgr_error_log)
    return added.count(), [ImportErrorLogItem(msg=msg, db_type="relationship") for msg in error_log]


def import_object_parameter_values(db_map, data):
    """Imports list of object parameter values:
        ex:
            data = [('object_name', 'parameter_name', 'value', 123.4),
                    ('object_name', 'parameter_name2', 'json', '{"timeseries": [1,2,3]}')]
            import_object_parameter_values(db_map, data)

    Args:
        db (spinedatabase_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with
                                 object name, parameter name, field name, parameter value

    Returns:
        (Int, List) Number of succesfull inserted objects, list of errors
    """
    existing_objects = {o.name: o.id for o in db_map.object_list()}
    existing_parameters = {p.name: p.id for p in db_map.parameter_list()}
    existing_parameter_values = {
        (pv.parameter_id, pv.object_id): pv.id for pv in db_map.object_parameter_value_list()}
    error_log = []
    new_values = []
    checked_new_values = set()
    for p in data:
        o_name = p[0]
        p_name = p[1]
        f_name = p[2].lower()
        if f_name not in ["value", "json"]:
            # invalid field name
            error_log.append("Parameter value for '{}: {}' can't be inserted; field name must be "
                             "'value' or 'json'".format(o_name, p_name))
            continue
        try:
            o_id = existing_objects[o_name]
        except KeyError:
            # object doesn't exist
            error_log.append("Parameter value for '{0}: {1}' can't be inserted because object '{0}' "
                             "doesn't exist".format(o_name, p_name))
            continue
        try:
            p_id = existing_parameters[p_name]
        except KeyError:
            # parameter doesn't exist
            error_log.append("Parameter value for '{0}: {1}' can't be inserted because parameter '{1}' "
                             "doesn't exist".format(o_name, p_name))
            continue
        checked_key = (p_id, o_id, f_name)
        if checked_key not in checked_new_values:
            # new values
            new_values.append({'parameter_id': p_id, 'object_id': o_id, f_name: p[3]})
            # add to check new values to avoid duplicates
            checked_new_values.add(checked_key)
        else:
            # duplicate new value
            error_log.append("Duplicate parameter value for '{}: {}', only first value "
                             "will be considered.".format(o_name, p_name))
    # Try and add everything
    added, intgr_error_log = db_map.add_parameter_values(*new_values, raise_intgr_error=False)
    error_log.extend(intgr_error_log)
    # Try and update whatever wasn't added
    added_keys = set((x.parameter_id, x.object_id) for x in added)
    updated_values = [
        {'id': existing_parameter_values[x['parameter_id'], x['object_id']], f_name: p[3]}
        for x in new_values if (x['parameter_id'], x['object_id']) not in added_keys]
    updated, intgr_error_log = db_map.update_parameter_values(*updated_values, raise_intgr_error=False)
    # NOTE: this second intgr_error_log can only contain already known information,
    # so it's fine to discard it
    rich_error_log = [ImportErrorLogItem(msg=msg, db_type="parameter value") for msg in error_log]
    return added.count() + updated.count(), rich_error_log


def import_relationship_parameter_values(db_map, data):
    """Imports list of object parameter values:
        ex:
            data = [['example_rel_class', ['example_object', 'other_object'], 'rel_parameter', 'value', 2.718]]
            import_relationship_parameter_values(db_map, data)

    Args:
        db (spinedatabase_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with
                                 relationship class name, list of object names, parameter name, field name,
                                 parameter value

    Returns:
        (Int, List) Number of succesfull inserted objects, list of errors
    """
    existing_relationship_classes = {oc.name: oc.id for oc in db_map.wide_relationship_class_list()}
    existing_objects = {o.name: o.id for o in db_map.object_list()}
    existing_parameters = {oc.name: oc.id for oc in db_map.parameter_list()}
    existing_relationships = {
        (r.class_id,) + tuple(map(int, r.object_id_list.split(','))): r.id
        for r in db_map.wide_relationship_list()
    }
    existing_parameter_values = {
        (pv.parameter_id, pv.relationship_id): pv.id for pv in db_map.relationship_parameter_value_list()}
    error_log = []
    new_values = []
    checked_new_values = set()
    for p in data:
        rc_name = p[0]
        o_names = p[1]
        p_name = p[2]
        f_name = p[3].lower()
        if f_name not in ["value", "json"]:
            # invalid field name
            error_log.append("Parameter value for '{0}: {1}: {2}' can't be inserted; field name must be "
                             "'value' or 'json'".format(rc_name, ','.join(o_names), p_name))
            continue
        try:
            # relationship class id
            rc_id = existing_relationship_classes[rc_name]
        except KeyError:
            # relationship class doesn't exist
            error_log.append("Parameter value for '{0}: {1}: {2}' can't be inserted because relationship class '{0}'"
                             "doesn't exist in db".format(rc_name, ','.join(o_names), p_name))
            continue
        # object ids
        o_ids = tuple(existing_objects[n] for n in o_names if n in existing_objects)
        if len(o_ids) != len(o_names):
            # not all objects exist
            error_log.append("Parameter value for '{0}: {1}: {2}' can't be inserted because it contains objects "
                             "that don't exist in db".format(rc_name, ','.join(o_names), p_name))
            continue
        rel_key = (rc_id,) + o_ids
        try:
            r_id = existing_relationships[rel_key]
        except KeyError:
            # relationship doesn't exist
            error_log.append("Parameter value for '{0}: {1}: {2}' can't be inserted because relationship "
                             "doesn't exist".format(rc_name, ','.join(o_names), p_name))
            continue
        try:
            p_id = existing_parameters[p_name]
        except KeyError:
            # parameter doesn't exist
            error_log.append("Parameter value for '{0}: {1}: {2}' can't be inserted because parameter '{2}' "
                             "doesn't exist".format(rc_name, ','.join(o_names), p_name))
            continue
        checked_key = (p_id, r_id, f_name)
        if checked_key not in checked_new_values:
            # new values
            new_values.append({'parameter_id': p_id, 'relationship_id': r_id, f_name: p[4]})
            # track new values to avoid inserting duplicates
            checked_new_values.add(checked_key)
        else:
            # duplicate new value
            error_log.append("Duplicate parameter value for '{0}: {1}: {2}', only first value "
                             "will be considered".format(rc_name, ','.join(o_names), p_name))
    # Try and add everything
    added, intgr_error_log = db_map.add_parameter_values(*new_values, raise_intgr_error=False)
    error_log.extend(intgr_error_log)
    # Try and update whatever wasn't added
    added_keys = set((x.parameter_id, x.relationship_id) for x in added)
    updated_values = [
        {'id': existing_parameter_values[x['parameter_id'], x['relationship_id']], f_name: p[4]}
        for x in new_values if (x['parameter_id'], x['relationship_id']) not in added_keys]
    updated, intgr_error_log = db_map.update_parameter_values(*updated_values, raise_intgr_error=False)
    # NOTE: this second intgr_error_log can only contain already known information,
    # so it's fine to discard it
    rich_error_log = [ImportErrorLogItem(msg=msg, db_type="parameter value") for msg in error_log]
    return added.count() + updated.count(), rich_error_log
