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
            list of lists with object name, parameter name, parameter type, parameter value
        relationship_parameter_values (List[List[str, List(str), str, 'json'|'value', str|numeric]]):
            list of lists with relationship class name, list of object names, parameter name, parameter type,
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
    # filter classes that don't already exist in db and duplicates
    existing_classes = set(oc.name for oc in db_map.object_class_list().all())
    new_classes = [{"name": o} for o in set(object_classes) if o not in existing_classes]
    if new_classes:
        db_map.add_object_classes(*new_classes)
    return len(new_classes), []


def import_objects(db_map, object_data):
    """Imports list of object names with object classes:
        ex:
            data = [('new_object', 'object_class_name')]
            import_objects(db_map, data)

    Args:
        db (spinedatabase_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterabel of lists/tuples with
                                 object name and object class name

    Returns:
        (Int, List) Number of succesfull inserted objects, list of errors
    """
    existing_classes = {oc.name: oc.id for oc in db_map.object_class_list().all()}
    existing_objects = {o.name: o.class_id for o in db_map.object_list().all()}

    # save new objects in set so we don't try to insert same object twice
    error_log = []
    new_objects = []
    for o in object_data:
        name = o[0]
        oc_name = o[1]
        if oc_name in existing_classes:
            oc_id = existing_classes[oc_name]
        else:
            # class doesn't exists
            error_log.append(ImportErrorLogItem(msg="Object with name '{}' can't be inserted because class with name: '{}' doesn't exist in database".format(name, oc_name), db_type="object"))
            continue
        if name in existing_objects:
            #object already exists
            if oc_id != existing_objects[name]:
                # existing object has different class
                error_log.append(ImportErrorLogItem(msg="Object with name '{}' can't be inserted because an object with that name and other object class exists".format(name, oc_name), db_type="object"))
        else:
            new_objects.append({'name': name, 'class_id': oc_id})
            # add to existing objects so we can catch duplicates with different class.
            existing_objects[name] = oc_id
    if new_objects:
        db_map.add_objects(*new_objects)
    return len(new_objects), error_log


def import_relationship_classes(db_map, relationship_classes):
    """Imports list of relationship class names with object classes:
        ex:
            data = [('new_rel_class', ['object_class_1, object_class_2])]
            import_relationship_classes(db_map, data)

    Args:
        db (spinedatabase_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterabel of lists/tuples with
                                 object name and object class name

    Returns:
        (Int, List) Number of succesfull inserted objects, list of errors
    """
    # get existing classes
    existing_classes = {oc.name: oc.id for oc in db_map.object_class_list().all()}
    existing_relationship_classes = {oc.name: list(map(int,oc.object_class_id_list.split(','))) for oc in db_map.wide_relationship_class_list().all()}
    error_log = []
    new_rc = []
    for rc in relationship_classes:
        name = rc[0]
        oc_names = rc[1]
        oc_ids = [existing_classes[oc] for oc in oc_names if oc in existing_classes]
        if len(oc_ids) != len(oc_names):
            # not all object_classes exists in database
            error_log.append(ImportErrorLogItem(msg="Relationship class: '{}' can't be inserted because it contains object classes that doesn't exist in db".format(name), db_type='relationship class'))
            continue
        if name in existing_relationship_classes:
            # relationship class exists
            if oc_ids != existing_relationship_classes[name]:
                # not same object classes
                error_log.append(ImportErrorLogItem(msg="Relationship class: '{}' can't be inserted because name already exists in database with different object class names", db_type='relationship class'))
        else:
            # new relationship class
            new_rc.append({'name': name, 'object_class_id_list': oc_ids})
            # add to existing_relationship_classes to avoid inserting duplicates
            existing_relationship_classes[name] = oc_ids
    if new_rc:
        db_map.add_wide_relationship_classes(*new_rc)
    return len(new_rc), error_log


def import_object_parameters(db_map, parameter_data):
    """Imports list of object class parameters:
        ex:
            data = [('new_parameter', 'object_class_1')]
            import_object_parameters(db_map, data)

    Args:
        db (spinedatabase_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterabel of lists/tuples with
                                 object name and object class name

    Returns:
        (Int, List) Number of succesfull inserted objects, list of errors
    """
    # get existing classes
    existing_classes = {oc.name: oc.id for oc in db_map.object_class_list().all()}
    existing_parameters = {oc.name: oc.object_class_id for oc in db_map.parameter_list().all()}
    error_log = []
    new_parameters = []
    for p in parameter_data:
        name = p[0]
        oc_name = p[1]
        if oc_name in existing_classes:
            oc_id = existing_classes[oc_name]
        else:
            # not all object_classes exists in database
            error_log.append(ImportErrorLogItem(msg="Parameter: '{}' can't be inserted because it contains object class that doesn't exist in db".format(name), db_type='parameter'))
            continue
        if name in existing_parameters:
            # relationship class exists
            if oc_id != existing_parameters[name]:
                # not same object classes
                error_log.append(ImportErrorLogItem(msg="Parameter: '{}' can't be inserted because name already exists in database with different object class", db_type='parameter'))
        else:
            # new relationship class
            new_parameters.append({'name': name, 'object_class_id': oc_id})
            # add to existing_relationship_classes to avoid inserting duplicates
            existing_parameters[name] = oc_id
    if new_parameters:
        db_map.add_parameters(*new_parameters)
    return len(new_parameters), error_log


def import_relationship_parameters(db_map, parameter_data):
    """Imports list of relationship class parameters:
        ex:
            data = [('new_parameter', 'relationship_class_name')]
            import_object_parameters(db_map, data)

    Args:
        db (spinedatabase_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterabel of lists/tuples with
                                 object name and object class name

    Returns:
        (Int, List) Number of succesfull inserted objects, list of errors
    """
    # get existing classes
    existing_classes = {oc.name: oc.id for oc in db_map.wide_relationship_class_list().all()}
    existing_parameters = {oc.name: oc.relationship_class_id for oc in db_map.parameter_list().all()}
    error_log = []
    new_parameters = []
    for p in parameter_data:
        name = p[0]
        rc_name = p[1]
        if rc_name in existing_classes:
            rc_id = existing_classes[rc_name]
        else:
            # not all object_classes exists in database
            error_log.append(ImportErrorLogItem(msg="Parameter: '{}' can't be inserted because it contains relationship class that doesn't exist in db".format(name), db_type='parameter'))
            continue
        if name in existing_parameters:
            # relationship class exists
            if rc_id != existing_parameters[name]:
                # not same object classes
                error_log.append(ImportErrorLogItem(msg="Parameter: '{}' can't be inserted because name already exists in database with different relatioship class", db_type='parameter'))
        else:
            # new relationship class
            new_parameters.append({'name': name, 'relationship_class_id': rc_id})
            # add to existing_relationship_classes to avoid inserting duplicates
            existing_parameters[name] = rc_id
    if new_parameters:
        db_map.add_parameters(*new_parameters)
    return len(new_parameters), error_log


def import_relationships(db_map, relationship_data):
    """Imports list of relationships:
        ex:
            data = [('relationship_class_name', ('object_name1','object_name2'))]
            import_object_parameters(db_map, data)

    Args:
        db (spinedatabase_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterabel of lists/tuples with
                                 object name and object class name

    Returns:
        (Int, List) Number of succesfull inserted objects, list of errors
    """
    # get existing classes
    existing_objects = {o.name: (o.id, o.class_id) for o in db_map.object_list().all()}
    existing_relationship_classes = {oc.name: (oc.id, list(map(int,oc.object_class_id_list.split(',')))) for oc in db_map.wide_relationship_class_list().all()}
    existing_relationships = {(oc.class_id,) + tuple(map(int,oc.object_id_list.split(','))) for oc in db_map.wide_relationship_list().all()}

    error_log = []
    new_relationships = []
    for r in relationship_data:
        o_names = r[1]
        rc_name = r[0]
        if rc_name in existing_relationship_classes:
            rc_id = existing_relationship_classes[rc_name][0]
            rc_oc_ids = existing_relationship_classes[rc_name][1]
        else:
            # not all object_classes exists in database
            error_log.append(ImportErrorLogItem(msg="Relationship: '{}: {}' can't be inserted because it contains relationship class that doesn't exist in db".format(rc_name,','.join(o_names)), db_type='relationship'))
            continue
        o_ids = tuple(existing_objects[n][0] for n in o_names if n in existing_objects)
        oc_ids = [existing_objects[n][1] for n in o_names if n in existing_objects]
        if len(o_ids) != len(o_names):
            # not all objects exits
            error_log.append(ImportErrorLogItem(msg="Relationship: '{}: {}' can't be inserted because not all objects exists in database".format(rc_name,','.join(o_names)), db_type='relationship'))
            continue
        if oc_ids != rc_oc_ids:
            # object classes doesn't match
            error_log.append(ImportErrorLogItem(msg="Relationship: '{}: {}' can't be inserted because object classes doesn't match".format(rc_name,','.join(o_names)), db_type='relationship'))
            continue
        rel_key = (rc_id,) + o_ids
        if not rel_key in existing_relationships:
            # relationship doesn't exists
            new_relationships.append({'name': rc_name + '__' + '_'.join(o_names), 'class_id': rc_id, 'object_id_list': o_ids})
            existing_relationships.add(rel_key)

    if new_relationships:
        db_map.add_wide_relationships(*new_relationships)
    return len(new_relationships), error_log


def import_object_parameter_values(db_map, data):
    """Imports list of object parameter values:
        ex:
            data = [('object_name', 'parameter_name', 'value', 123.4),
                    ('object_name', 'parameter_name2', 'json', '{"timeseries": [1,2,3]}')]
            import_object_parameter_values(db_map, data)

    Args:
        db (spinedatabase_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterabel of lists/tuples with
                                 object name and object class name

    Returns:
        (Int, List) Number of succesfull inserted objects, list of errors
    """
    # get existing classes
    existing_objects = {o.name: (o.id, o.class_id) for o in db_map.object_list().all()}
    existing_parameters = {oc.name: (oc.id, oc.object_class_id) for oc in db_map.parameter_list().all()}
    existing_parameter_values = {(oc.parameter_id, oc.object_id): oc.id for oc in db_map.object_parameter_value_list().all()}

    error_log = []
    new_values = []
    update_values = []
    checked_new_values = set()
    for p in data:
        o_name = p[0]
        p_name = p[1]
        f_name = p[2].lower()
        if not f_name in ["value", "json"]:
            # invalid field name
            error_log.append(ImportErrorLogItem(msg="Parameter value for: '{}: {}' can't be inserted field name must be 'value' or 'json'".format(o_name, p_name), db_type='parameter value value'))
            continue
        if o_name in existing_objects:
            oc_id = existing_objects[o_name][1]
            o_id = existing_objects[o_name][0]
        else:
            # object doesn't exist
            error_log.append(ImportErrorLogItem(msg="Parameter value for: '{}: {}' can't be inserted because object doesn't exist".format(o_name, p_name), db_type='parameter value'))
            continue
        if p_name in existing_parameters:
            p_id = existing_parameters[p_name][0]
            p_oc_id = existing_parameters[p_name][1]
        else:
            # not parameter doesn't exist
            error_log.append(ImportErrorLogItem(msg="Parameter value for: '{}: {}' can't be inserted because parameter doesn't exist".format(o_name, p_name), db_type='parameter value'))
            continue
        if oc_id != p_oc_id:
            # not parameter object class and given object class doesn't match
            error_log.append(ImportErrorLogItem(msg="Parameter value for: '{}: {}' can't be inserted because parameter object class doesn't match db".format(o_name, p_name), db_type='parameter value'))
            continue
        value_key = (p_id, o_id)
        checked_key = (p_id, o_id, f_name)
        if value_key in existing_parameter_values and checked_key not in checked_new_values:
            pv_id = existing_parameter_values[value_key]
            # value exists, update
            update_values.append({'id': pv_id, f_name: p[3]})
        elif checked_key not in checked_new_values:
            # new values
            new_values.append({'parameter_id': p_id, 'object_id': o_id, f_name: p[3]})
            # add to existing_relationship_classes to avoid inserting duplicates
            checked_new_values.add(checked_key)
        else:
            # duplicate new value
            error_log.append(ImportErrorLogItem(msg="Duplicate parameter value for: '{}: {}', only first value was inserted".format(o_name, p_name), db_type='parameter value'))

    if new_values:
        db_map.add_parameter_values(*new_values)
    if update_values:
        db_map.update_parameter_values(*update_values)
    return len(new_values) + len(update_values), error_log


def import_relationship_parameter_values(db_map, data):
    """Imports list of object parameter values:
        ex:
            data = [('object_name', 'parameter_name', 'value', 123.4),
                    ('object_name', 'parameter_name2', 'json', '{"timeseries": [1,2,3]}')]
            import_object_parameter_values(db_map, data)

    Args:
        db (spinedatabase_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterabel of lists/tuples with
                                 object name and object class name

    Returns:
        (Int, List) Number of succesfull inserted objects, list of errors
    """
    # get existing classes
    existing_objects = {o.name: (o.id, o.class_id) for o in db_map.object_list().all()}
    existing_parameters = {oc.name: (oc.id, oc.relationship_class_id) for oc in db_map.parameter_list().all()}
    existing_parameter_values = {(oc.parameter_id, oc.relationship_id): oc.id for oc in db_map.relationship_parameter_value_list().all()}
    existing_relationship_classes = {oc.name: (oc.id, list(map(int,oc.object_class_id_list.split(',')))) for oc in db_map.wide_relationship_class_list().all()}
    existing_relationships = {(oc.class_id,) + tuple(map(int,oc.object_id_list.split(','))): oc.id for oc in db_map.wide_relationship_list().all()}

    error_log = []
    new_values = []
    update_values = []
    checked_new_values = set()
    for p in data:
        rc_name = p[0]
        o_names = p[1]
        p_name = p[2]
        f_name = p[3].lower()
        if not f_name in ["value", "json"]:
            # invalid field name
            error_log.append(ImportErrorLogItem(msg="Parameter value for: '{}: {}' can't be inserted field name must be 'value' or 'json'".format(','.join(o_names), p_name), db_type='parameter value'))
            continue
        if rc_name in existing_relationship_classes:
            # relationship class ids
            rc_id = existing_relationship_classes[rc_name][0]
            rc_oc_ids = existing_relationship_classes[rc_name][1]
        else:
            # relationship class doesn't exist
            error_log.append(ImportErrorLogItem(msg="Parameter value for: '{}: {}' can't be inserted because it contains relationship class that doesn't exist in db".format(','.join(o_names), p_name), db_type='relationship'))
            continue
        #object ids
        o_ids = tuple(existing_objects[n][0] for n in o_names if n in existing_objects)
        oc_ids = [existing_objects[n][1] for n in o_names if n in existing_objects]
        if len(o_ids) != len(o_names):
            # not all objects exits
            error_log.append(ImportErrorLogItem(msg="Parameter value for: '{}: {}' can't be inserted field name must be 'value' or 'json'".format(','.join(o_names), p_name), db_type='parameter value'))
            continue
        if oc_ids != rc_oc_ids:
            # object classes doesn't match
            error_log.append(ImportErrorLogItem(msg="Parameter value for: '{}: {}' can't be inserted because object classes doesn't match".format(','.join(o_names), p_name), db_type='relationship'))
            continue
        rel_key = (rc_id,) + o_ids
        if rel_key in existing_relationships:
            r_id = existing_relationships[rel_key]
        else:
            # relationship doesn't exists
            error_log.append(ImportErrorLogItem(msg="Parameter value for: '{}: {}' can't be inserted because relationship doesn't exist".format(','.join(o_names), p_name), db_type='relationship'))
            continue
        if p_name in existing_parameters:
            p_id = existing_parameters[p_name][0]
            p_rc_id = existing_parameters[p_name][1]
        else:
            # parameter doesn't exist
            error_log.append(ImportErrorLogItem(msg="Parameter value for: '{}: {}' can't be inserted because parameter doesn't exist".format(','.join(o_names), p_name), db_type='parameter value'))
            continue
        if rc_id != p_rc_id:
            # parameter class and given class doesn't match
            error_log.append(ImportErrorLogItem(msg="Parameter value for: '{}: {}' can't be inserted because parameter relationship class doesn't match db".format(','.join(o_names), p_name), db_type='parameter value'))
            continue
        value_key = (p_id, r_id)
        checked_key = (p_id, r_id, f_name)
        if value_key in existing_parameter_values and checked_key not in checked_new_values:
            pv_id = existing_parameter_values[value_key]
            # value exists, update
            update_values.append({'id': pv_id, f_name: p[4]})
        elif checked_key not in checked_new_values:
            # new values
            new_values.append({'parameter_id': p_id, 'relationship_id': r_id, f_name: p[4]})
            # track new values to avoid inserting duplicates
            checked_new_values.add(checked_key)
        else:
            # duplicate new value
            error_log.append(ImportErrorLogItem(msg="Duplicate parameter value for: '{}: {}', only first value was inserted".format(','.join(o_names), p_name), db_type='parameter value'))

    if new_values:
        db_map.add_parameter_values(*new_values)
    if update_values:
        db_map.update_parameter_values(*update_values)
    return len(new_values) + len(update_values), error_log
