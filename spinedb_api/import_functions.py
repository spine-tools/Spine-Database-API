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

from .parameter_value import to_database, fix_conflict
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


def import_data(db_map, unparse_value=to_database, on_conflict="merge", **kwargs):
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
                        scenario_alternatives=scenario_alternatives)

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
    error_log = []
    num_imports = 0
    for tablename, (to_add, to_update, errors) in get_data_for_import(
        db_map, unparse_value=unparse_value, on_conflict=on_conflict, **kwargs
    ):
        updated, _ = db_map.update_items(tablename, *to_update, check=False)
        added, _ = db_map.add_items(tablename, *to_add, check=False)
        num_imports += len(added) + len(updated)
        error_log.extend(errors)
    return num_imports, error_log


def get_data_for_import(
    db_map,
    unparse_value=to_database,
    on_conflict="merge",
    entity_classes=(),
    entities=(),
    parameter_definitions=(),
    parameter_values=(),
    entity_groups=(),
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
    metadata=(),
    entity_metadata=(),
    parameter_value_metadata=(),
    object_metadata=(),
    relationship_metadata=(),
    object_parameter_value_metadata=(),
    relationship_parameter_value_metadata=(),
    # legacy
    tools=(),
    features=(),
    tool_features=(),
    tool_feature_methods=(),
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
    # NOTE: The order is important, because of references. E.g., we want to import alternatives before parameter_values
    if alternatives:
        yield ("alternative", _get_alternatives_for_import(db_map, alternatives))
    if scenarios:
        yield ("scenario", _get_scenarios_for_import(db_map, scenarios))
    if scenario_alternatives:
        if not scenarios:
            scenarios = list({item[0]: None for item in scenario_alternatives})
            yield ("scenario", _get_scenarios_for_import(db_map, scenarios))
        if not alternatives:
            alternatives = list({item[1]: None for item in scenario_alternatives})
            yield ("alternative", _get_alternatives_for_import(db_map, alternatives))
        yield ("scenario_alternative", _get_scenario_alternatives_for_import(db_map, scenario_alternatives))
    if entity_classes:
        yield ("entity_class", _get_entity_classes_for_import(db_map, entity_classes))
    if object_classes:
        yield ("object_class", _get_object_classes_for_import(db_map, object_classes))
    if relationship_classes:
        yield ("relationship_class", _get_entity_classes_for_import(db_map, relationship_classes))
    if parameter_value_lists:
        yield ("parameter_value_list", _get_parameter_value_lists_for_import(db_map, parameter_value_lists))
        yield ("list_value", _get_list_values_for_import(db_map, parameter_value_lists, unparse_value))
    if parameter_definitions:
        yield (
            "parameter_definition",
            _get_parameter_definitions_for_import(db_map, parameter_definitions, unparse_value),
        )
    if object_parameters:
        yield ("parameter_definition", _get_parameter_definitions_for_import(db_map, object_parameters, unparse_value))
    if relationship_parameters:
        yield (
            "parameter_definition",
            _get_parameter_definitions_for_import(db_map, relationship_parameters, unparse_value),
        )
    if entities:
        yield ("entity", _get_entities_for_import(db_map, entities))
    if objects:
        yield ("object", _get_entities_for_import(db_map, objects))
    if relationships:
        yield ("relationship", _get_entities_for_import(db_map, relationships))
    if entity_groups:
        yield ("entity_group", _get_entity_groups_for_import(db_map, entity_groups))
    if object_groups:
        yield ("entity_group", _get_entity_groups_for_import(db_map, object_groups))
    if parameter_values:
        yield (
            "parameter_value",
            _get_parameter_values_for_import(db_map, parameter_values, unparse_value, on_conflict),
        )
    if object_parameter_values:
        yield (
            "parameter_value",
            _get_parameter_values_for_import(db_map, object_parameter_values, unparse_value, on_conflict),
        )
    if relationship_parameter_values:
        yield (
            "parameter_value",
            _get_parameter_values_for_import(db_map, relationship_parameter_values, unparse_value, on_conflict),
        )
    if metadata:
        yield ("metadata", _get_metadata_for_import(db_map, metadata))
    if entity_metadata:
        yield ("metadata", _get_metadata_for_import(db_map, (metadata for _, _, metadata in entity_metadata)))
        yield ("entity_metadata", _get_entity_metadata_for_import(db_map, entity_metadata))
    if parameter_value_metadata:
        yield ("parameter_value_metadata", _get_parameter_value_metadata_for_import(db_map, parameter_value_metadata))
    if object_metadata:
        yield from get_data_for_import(db_map, entity_metadata=object_metadata)
    if relationship_metadata:
        yield from get_data_for_import(db_map, entity_metadata=relationship_metadata)
    if object_parameter_value_metadata:
        yield from get_data_for_import(db_map, parameter_value_metadata=object_parameter_value_metadata)
    if relationship_parameter_value_metadata:
        yield from get_data_for_import(db_map, parameter_value_metadata=relationship_parameter_value_metadata)


def import_entity_classes(db_map, data):
    """Imports entity classes.

    Example::

            data = [
                'new_class',
                ('another_class', 'description', 123456),
                ('multidimensional_class', 'description', 654321, ("new_class", "another_class"))
            ]
            import_entity_classes(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (Iterable): list/set/iterable of string entity class names,
            and optionally description, integer display icon reference, and lists/tuples with dimension names,

    Returns:
        tuple of int and list: Number of successfully inserted object classes, list of errors
    """
    return import_data(db_map, entity_classes=data)


def import_entities(db_map, data):
    """Imports entities.

    Example::

            data = [
                ('class_name1', 'entity_name1'),
                ('class_name2', 'entity_name2'),
                ('class_name3', ('entity_name1', 'entity_name2'))
            ]
            import_entities(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with entity class name
            and entity name or list/tuple of element names

    Returns:
        (Int, List) Number of successful inserted entities, list of errors
    """
    return import_data(db_map, entities=data)


def import_entity_groups(db_map, data):
    """Imports list of entity groups by name with associated class name into given database mapping:
    Ignores duplicate and existing (group, member) tuples.

    Example::

            data = [
                ('class_name', 'group_name', 'member_name'),
                ('class_name', 'group_name', 'another_member_name')
            ]
            import_entity_groups(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with entity class name, group name,
            and member name

    Returns:
        (Int, List) Number of successful inserted entity groups, list of errors
    """
    return import_data(db_map, entity_groups=data)


def import_parameter_definitions(db_map, data, unparse_value=to_database):
    """Imports list of parameter definitions:

    Example::

            data = [
                ('entity_class_1', 'new_parameter'),
                ('entity_class_2', 'other_parameter', 'default_value', 'value_list_name', 'description')
            ]
            import_parameter_definitions(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with entity class name, parameter name,
            and optionally default value, value list name, and description

    Returns:
        (Int, List) Number of successful inserted parameter definitions, list of errors
    """
    return import_data(db_map, parameter_definitions=data, unparse_value=unparse_value)


def import_parameter_values(db_map, data, unparse_value=to_database, on_conflict="merge"):
    """Imports parameter values:

    Example::

            data = [
                ['example_class2', 'example_entity', 'parameter', 5.5, 'alternative'],
                ['example_class1', ('example_entity', 'other_entity'), 'parameter', 2.718]
            ]
            import_parameter_values(db_map, data)

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): mapping for database to insert into
        data (List[List/Tuple]): list/set/iterable of lists/tuples with
            entity class name, entity name or list of element names, parameter name, (deserialized) parameter value,
            optional name of an alternative

    Returns:
        (Int, List) Number of successful inserted parameter values, list of errors
    """
    return import_data(db_map, parameter_values=data, unparse_value=unparse_value, on_conflict=on_conflict)


def import_alternatives(db_map, data):
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
    return import_data(db_map, alternatives=data)


def import_scenarios(db_map, data):
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
    return import_data(db_map, scenarios=data)


def import_scenario_alternatives(db_map, data):
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
    return import_data(db_map, scenario_alternatives=data)


def import_parameter_value_lists(db_map, data, unparse_value=to_database):
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
    return import_data(db_map, parameter_value_lists=data, unparse_value=unparse_value)


def import_metadata(db_map, data=None):
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
    return import_data(db_map, metadata=data)


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
        tuple of int and list: Number of successfully inserted object classes, list of errors
    """
    return import_data(db_map, object_classes=data)


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
    return import_data(db_map, relationship_classes=data)


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
    return import_data(db_map, objects=data)


def import_object_groups(db_map, data):
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
    return import_data(db_map, object_groups=data)


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
    return import_data(db_map, relationships=data)


def import_object_parameters(db_map, data, unparse_value=to_database):
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
    return import_data(db_map, object_parameters=data, unparse_value=unparse_value)


def import_relationship_parameters(db_map, data, unparse_value=to_database):
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
    return import_data(db_map, relationship_parameters=data, unparse_value=unparse_value)


def import_object_parameter_values(db_map, data, unparse_value=to_database, on_conflict="merge"):
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
    return import_data(db_map, object_parameter_values=data, unparse_value=unparse_value, on_conflict=on_conflict)


def import_relationship_parameter_values(db_map, data, unparse_value=to_database, on_conflict="merge"):
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
    return import_data(db_map, relationship_parameter_values=data, unparse_value=unparse_value, on_conflict=on_conflict)


def import_object_metadata(db_map, data):
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
    return import_data(db_map, object_metadata=data)


def import_relationship_metadata(db_map, data):
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
    return import_data(db_map, relationship_metadata=data)


def import_object_parameter_value_metadata(db_map, data):
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
    return import_data(db_map, object_parameter_value_metadata=data)


def import_relationship_parameter_value_metadata(db_map, data):
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
    return import_data(db_map, relationship_parameter_value_metadata=data)


def _get_items_for_import(db_map, item_type, data, skip_keys=()):
    table_cache = db_map.cache.table_cache(item_type)
    errors = []
    to_add = []
    to_update = []
    seen = {}
    for item in data:
        checked_item, add_error = table_cache.check_item(item, skip_keys=skip_keys)
        if not add_error:
            if not _check_unique(item_type, checked_item, seen, errors):
                continue
            to_add.append(checked_item)
            continue
        checked_item, update_error = table_cache.check_item(item, for_update=True, skip_keys=skip_keys)
        if not update_error:
            if checked_item:
                if not _check_unique(item_type, checked_item, seen, errors):
                    continue
                to_update.append(checked_item)
            continue
        errors.append(add_error)
    return to_add, to_update, errors


def _check_unique(item_type, checked_item, seen, errors):
    dupe_key = _add_to_seen(checked_item, seen)
    if not dupe_key:
        return True
    if item_type in ("parameter_value",):
        errors.append(f"attempting to import more than one {item_type} with {dupe_key} - only first will be considered")
    return False


def _add_to_seen(checked_item, seen):
    for key, value in checked_item.unique_values():
        if value in seen.get(key, set()):
            return dict(zip(key, value))
        seen.setdefault(key, set()).add(value)


def _get_entity_classes_for_import(db_map, data):
    key = ("name", "dimension_name_list", "description", "display_icon")
    return _get_items_for_import(
        db_map, "entity_class", ({"name": x} if isinstance(x, str) else dict(zip(key, x)) for x in data)
    )


def _get_entities_for_import(db_map, data):
    def _data_iterator():
        for class_name, name_or_element_name_list, *optionals in data:
            byname_key = "name" if isinstance(name_or_element_name_list, str) else "element_name_list"
            key = ("class_name", byname_key, "description")
            yield dict(zip(key, (class_name, name_or_element_name_list, *optionals)))

    return _get_items_for_import(db_map, "entity", _data_iterator())


def _get_entity_groups_for_import(db_map, data):
    key = ("class_name", "group_name", "member_name")
    return _get_items_for_import(db_map, "entity_group", (dict(zip(key, x)) for x in data))


def _get_parameter_definitions_for_import(db_map, data, unparse_value):
    def _data_iterator():
        for class_name, parameter_name, *optionals in data:
            if not optionals:
                yield class_name, parameter_name
                continue
            value = optionals.pop(0)
            value, type_ = unparse_value(value)
            yield class_name, parameter_name, value, type_, *optionals

    key = ("entity_class_name", "name", "default_value", "default_type", "parameter_value_list_name", "description")
    return _get_items_for_import(db_map, "parameter_definition", (dict(zip(key, x)) for x in _data_iterator()))


def _get_parameter_values_for_import(db_map, data, unparse_value, on_conflict):
    def _data_iterator():
        for class_name, entity_byname, parameter_name, value, *optionals in data:
            if isinstance(entity_byname, str):
                entity_byname = (entity_byname,)
            alternative_name = optionals[0] if optionals else db_map.get_import_alternative_name()
            value, type_ = unparse_value(value)
            item = {
                "entity_class_name": class_name,
                "entity_byname": entity_byname,
                "parameter_definition_name": parameter_name,
                "alternative_name": alternative_name,
                "value": None,
                "type": None,
            }
            pv = db_map.cache.table_cache("parameter_value").current_item(item)
            if pv is not None:
                value, type_ = fix_conflict((value, type_), (pv["value"], pv["type"]), on_conflict)
            item.update({"value": value, "type": type_})
            yield item

    return _get_items_for_import(db_map, "parameter_value", _data_iterator())


def _get_alternatives_for_import(db_map, data):
    key = ("name", "description")
    return _get_items_for_import(
        db_map, "alternative", ({"name": x} if isinstance(x, str) else dict(zip(key, x)) for x in data)
    )


def _get_scenarios_for_import(db_map, data):
    key = ("name", "active", "description")
    return _get_items_for_import(
        db_map, "scenario", ({"name": x} if isinstance(x, str) else dict(zip(key, x)) for x in data)
    )


def _get_scenario_alternatives_for_import(db_map, data):
    alt_name_list_by_scen_name, errors = {}, []
    for scen_name, alt_name, *optionals in data:
        scen = db_map.cache.table_cache("scenario").current_item({"name": scen_name})
        if scen is None:
            errors.append(f"no scenario with name {scen_name} to set alternatives for")
            continue
        alternative_name_list = alt_name_list_by_scen_name.setdefault(scen_name, scen["alternative_name_list"])
        if alt_name in alternative_name_list:
            alternative_name_list.remove(alt_name)
        before_alt_name = optionals[0] if optionals else None
        if before_alt_name is None:
            alternative_name_list.append(alt_name)
            continue
        if before_alt_name in alternative_name_list:
            pos = alternative_name_list.index(before_alt_name)
            alternative_name_list.insert(pos, alt_name)
        else:
            errors.append(f"{before_alt_name} is not in {scen_name}")

    def _data_iterator():
        for scen_name, alternative_name_list in alt_name_list_by_scen_name.items():
            for k, alt_name in enumerate(alternative_name_list):
                yield {"scenario_name": scen_name, "alternative_name": alt_name, "rank": k + 1}

    to_add, to_update, more_errors = _get_items_for_import(
        db_map, "scenario_alternative", _data_iterator(), skip_keys=(("scenario_name", "rank"),)
    )
    return to_add, to_update, errors + more_errors


def _get_parameter_value_lists_for_import(db_map, data):
    return _get_items_for_import(db_map, "parameter_value_list", ({"name": x} for x in {x[0]: None for x in data}))


def _get_list_values_for_import(db_map, data, unparse_value):
    def _data_iterator():
        index_by_list_name = {}
        for list_name, value in data:
            value, type_ = unparse_value(value)
            index = index_by_list_name.get(list_name)
            if index is None:
                current_list = db_map.cache.table_cache("parameter_value_list").current_item({"name": list_name})
                index = max(
                    (
                        x["index"]
                        for x in db_map.cache.get("list_value", {}).values()
                        if x["parameter_value_list_id"] == current_list["id"]
                    ),
                    default=-1,
                )
            index += 1
            index_by_list_name[list_name] = index
            yield {"parameter_value_list_name": list_name, "value": value, "type": type_, "index": index}

    return _get_items_for_import(db_map, "list_value", _data_iterator())


def _get_metadata_for_import(db_map, data):
    def _data_iterator():
        for metadata in data:
            for name, value in _parse_metadata(metadata):
                yield {"name": name, "value": value}

    return _get_items_for_import(db_map, "metadata", _data_iterator())


def _get_entity_metadata_for_import(db_map, data):
    def _data_iterator():
        for class_name, entity_byname, metadata in data:
            if isinstance(entity_byname, str):
                entity_byname = (entity_byname,)
            for name, value in _parse_metadata(metadata):
                yield (class_name, entity_byname, name, value)

    key = ("entity_class_name", "entity_byname", "metadata_name", "metadata_value")
    return _get_items_for_import(db_map, "entity_metadata", (dict(zip(key, x)) for x in _data_iterator()))


def _get_parameter_value_metadata_for_import(db_map, data):
    def _data_iterator():
        for class_name, entity_byname, parameter_name, metadata, *optionals in data:
            if isinstance(entity_byname, str):
                entity_byname = (entity_byname,)
            alternative_name = optionals[0] if optionals else db_map.get_import_alternative_name()
            for name, value in _parse_metadata(metadata):
                yield (class_name, entity_byname, parameter_name, name, value, alternative_name)

    key = (
        "entity_class_name",
        "entity_byname",
        "parameter_definition_name",
        "metadata_name",
        "metadata_value",
        "alternative_name",
    )
    return _get_items_for_import(db_map, "parameter_value_metadata", (dict(zip(key, x)) for x in _data_iterator()))


# Legacy
def _get_object_classes_for_import(db_map, data):
    def _data_iterator():
        for x in data:
            if isinstance(x, str):
                yield x
                continue
            name, *optionals = x
            yield name, (), *optionals

    return _get_entity_classes_for_import(db_map, _data_iterator())
