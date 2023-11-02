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
Functions for importing data into a Spine database in a standard format.
"""

from .parameter_value import to_database, fix_conflict
from .helpers import _parse_metadata


def import_data(db_map, unparse_value=to_database, on_conflict="merge", **kwargs):
    """Imports data into a Spine database using a standard format.

    Example::

            entity_classes = [
                ('example_class', ()), ('other_class', ()), ('multi_d_class', ('example_class', 'other_class'))
            ]
            alternatives = [('example_alternative', 'An example')]
            scenarios = [('example_scenario', 'An example')]
            scenario_alternatives = [
                ('example_scenario', 'example_alternative'), ('example_scenario', 'Base', 'example_alternative')
            ]
            parameter_value_lists = [("example_list", "value1"), ("example_list", "value2")]
            parameter_definitions = [('example_class', 'example_parameter'), ('multi_d_class', 'other_parameter')]
            entities = [
                ('example_class', 'example_entity'),
                ('example_class', 'example_group'),
                ('example_class', 'example_member'),
                ('other_class', 'other_entity'),
                ('multi_d_class', ('example_entity', 'other_entity')),
            ]
            entity_groups = [
                ('example_class', 'example_group', 'example_member'),
                ('example_class', 'example_group', 'example_entity'),
            ]
            parameter_values = [
                ('example_object_class', 'example_entity', 'example_parameter', 3.14),
                ('multi_d_class', ('example_entity', 'other_entity'), 'rel_parameter', 2.718),
            ]
            entity_alternatives = [
                ('example_class', 'example_entity', "example_alternative", True),
                ('example_class', 'example_entity', "example_alternative", False),
            ]
            import_data(
                db_map,
                entity_classes=entity_classes,
                alternatives=alternatives,
                scenarios=scenarios,
                scenario_alternatives=scenario_alternatives,
                parameter_value_lists=parameter_value_lists,
                parameter_definitions=parameter_definitions,
                entities=entities,
                entity_groups=entity_groups,
                parameter_values=parameter_values,
                entity_alternatives=entity_alternatives,
            )

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): database mapping
        on_conflict (str): Conflict resolution strategy for :func:`parameter_value.fix_conflict`
        entity_classes (list(tuple(str,tuple,str,int)): tuples of
            (name, dimension name tuple, description, display icon integer)
        parameter_definitions (list(tuple(str,str,str,str)):
            tuples of (class name, parameter name, default value, parameter value list name, description)
        entities: (list(tuple(str,str or tuple(str)): tuples of (class name, entity name or element name list)
        entity_alternatives: (list(tuple(str,str or tuple(str),str,bool): tuples of
            (class name, entity name or element name list, alternative name, activity)
        entity_groups (list(tuple(str,str,str))): tuples of (class name, group entity name, member entity name)
        parameter_values (list(tuple(str,str or tuple(str),str,str|numeric,str]):
            tuples of (class name, entity name or element name list, parameter name, value, alternative name)
        alternatives (list(str,str)): tuples of (name, description)
        scenarios (list(str,str)): tuples of (name, description)
        scenario_alternatives (list(str,str,str)): tuples of
            (scenario name, alternative name, preceeding alternative name)
        parameter_value_lists (list(str,str|numeric)): tuples of (list name, value)

    Returns:
        int: number of items imported
        list: errors
    """
    all_errors = []
    num_imports = 0
    for tablename, (to_add, to_update, errors) in get_data_for_import(
        db_map, unparse_value=unparse_value, on_conflict=on_conflict, **kwargs
    ):
        updated, _ = db_map.update_items(tablename, *to_update, check=False)
        added, _ = db_map.add_items(tablename, *to_add, check=False)
        num_imports += len(added) + len(updated)
        all_errors.extend(errors)
    return num_imports, all_errors


def get_data_for_import(
    db_map,
    unparse_value=to_database,
    on_conflict="merge",
    entity_classes=(),
    entities=(),
    entity_groups=(),
    entity_alternatives=(),  # TODO
    parameter_definitions=(),
    parameter_values=(),
    parameter_value_lists=(),
    alternatives=(),
    scenarios=(),
    scenario_alternatives=(),
    metadata=(),
    entity_metadata=(),
    parameter_value_metadata=(),
    superclass_subclasses=(),
    # legacy
    object_classes=(),
    relationship_classes=(),
    object_parameters=(),
    relationship_parameters=(),
    objects=(),
    relationships=(),
    object_groups=(),
    object_parameter_values=(),
    relationship_parameter_values=(),
    object_metadata=(),
    relationship_metadata=(),
    object_parameter_value_metadata=(),
    relationship_parameter_value_metadata=(),
    # removed
    tools=(),
    features=(),
    tool_features=(),
    tool_feature_methods=(),
):
    """Yields data to import into a Spine DB.

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): database mapping
        on_conflict (str): Conflict resolution strategy for :func:`~spinedb_api.parameter_value.fix_conflict`
        entity_classes (list(tuple(str,tuple,str,int)): tuples of
            (name, dimension name tuple, description, display icon integer)
        parameter_definitions (list(tuple(str,str,str,str)):
            tuples of (class name, parameter name, default value, parameter value list name)
        entities: (list(tuple(str,str or tuple(str)): tuples of (class name, entity name or element name list)
        entity_alternatives: (list(tuple(str,str or tuple(str),str,bool): tuples of
            (class name, entity name or element name list, alternative name, activity)
        entity_groups (list(tuple(str,str,str))): tuples of (class name, group entity name, member entity name)
        parameter_values (list(tuple(str,str or tuple(str),str,str|numeric,str]):
            tuples of (class name, entity name or element name list, parameter name, value, alternative name)
        alternatives (list(str,str)): tuples of (name, description)
        scenarios (list(str,str)): tuples of (name, description)
        scenario_alternatives (list(str,str,str)): tuples of
            (scenario name, alternative name, preceeding alternative name)
        parameter_value_lists (list(str,str|numeric)): tuples of (list name, value)

    Yields:
        str: item type
        tuple(list,list,list): tuple of (items to add, items to update, errors)
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
    if superclass_subclasses:
        yield ("superclass_subclass", _get_parameter_superclass_subclasses_for_import(db_map, superclass_subclasses))
    if entity_classes:
        for bucket in _get_entity_classes_for_import(db_map, entity_classes):
            yield ("entity_class", bucket)
    if entities:
        for bucket in _get_entities_for_import(db_map, entities):
            yield ("entity", bucket)
    if entity_alternatives:
        yield ("entity_alternative", _get_entity_alternatives_for_import(db_map, entity_alternatives))
    if entity_groups:
        yield ("entity_group", _get_entity_groups_for_import(db_map, entity_groups))
    if parameter_value_lists:
        yield ("parameter_value_list", _get_parameter_value_lists_for_import(db_map, parameter_value_lists))
        yield ("list_value", _get_list_values_for_import(db_map, parameter_value_lists, unparse_value))
    if parameter_definitions:
        yield (
            "parameter_definition",
            _get_parameter_definitions_for_import(db_map, parameter_definitions, unparse_value),
        )
    if parameter_values:
        yield (
            "parameter_value",
            _get_parameter_values_for_import(db_map, parameter_values, unparse_value, on_conflict),
        )
    if metadata:
        yield ("metadata", _get_metadata_for_import(db_map, metadata))
    if entity_metadata:
        yield ("metadata", _get_metadata_for_import(db_map, (metadata for _, _, metadata in entity_metadata)))
        yield ("entity_metadata", _get_entity_metadata_for_import(db_map, entity_metadata))
    if parameter_value_metadata:
        yield ("parameter_value_metadata", _get_parameter_value_metadata_for_import(db_map, parameter_value_metadata))
    # Legacy
    if object_classes:
        yield from get_data_for_import(db_map, entity_classes=_object_classes_to_entity_classes(object_classes))
    if relationship_classes:
        yield from get_data_for_import(db_map, entity_classes=relationship_classes)
    if object_parameters:
        yield from get_data_for_import(db_map, unparse_value=unparse_value, parameter_definitions=object_parameters)
    if relationship_parameters:
        yield from get_data_for_import(
            db_map, unparse_value=unparse_value, parameter_definitions=relationship_parameters
        )
    if objects:
        yield from get_data_for_import(db_map, entities=objects)
    if relationships:
        yield from get_data_for_import(db_map, entities=relationships)
    if object_groups:
        yield from get_data_for_import(db_map, entity_groups=object_groups)
    if object_parameter_values:
        yield from get_data_for_import(
            db_map, unparse_value=unparse_value, on_conflict=on_conflict, parameter_values=object_parameter_values
        )
    if relationship_parameter_values:
        yield from get_data_for_import(
            db_map, unparse_value=unparse_value, on_conflict=on_conflict, parameter_values=relationship_parameter_values
        )
    if object_metadata:
        yield from get_data_for_import(db_map, entity_metadata=object_metadata)
    if relationship_metadata:
        yield from get_data_for_import(db_map, entity_metadata=relationship_metadata)
    if object_parameter_value_metadata:
        yield from get_data_for_import(db_map, parameter_value_metadata=object_parameter_value_metadata)
    if relationship_parameter_value_metadata:
        yield from get_data_for_import(db_map, parameter_value_metadata=relationship_parameter_value_metadata)


def import_superclass_subclasses(db_map, data):
    """Imports superclass_subclasses into a Spine database using a standard format.

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): database mapping
        data (list(tuple(str,tuple,str,int)): tuples of (superclass name, subclass name)

    Returns:
        int: number of items imported
        list: errors
    """
    return import_data(db_map, superclass_subclasses=data)


def import_entity_classes(db_map, data):
    """Imports entity classes into a Spine database using a standard format.

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): database mapping
        data (list(tuple(str,tuple,str,int)): tuples of
            (name, dimension name tuple, description, display icon integer)

    Returns:
        int: number of items imported
        list: errors
    """
    return import_data(db_map, entity_classes=data)


def import_entities(db_map, data):
    """Imports entities into a Spine database using a standard format.

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): database mapping
        data: (list(tuple(str,str or tuple(str)): tuples of (class name, entity name or element name list)

    Returns:
        int: number of items imported
        list: errors
    """
    return import_data(db_map, entities=data)


def import_entity_alternatives(db_map, data):
    """Imports entity alternatives into a Spine database using a standard format.

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): database mapping
        data: (list(tuple(str,str or tuple(str),str,bool): tuples of
            (class name, entity name or element name list, alternative name, activity)

    Returns:
        int: number of items imported
        list: errors
    """
    return import_data(db_map, entity_alternatives=data)


def import_entity_groups(db_map, data):
    """Imports entity groups into a Spine database using a standard format.

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): database mapping
        data (list(tuple(str,str,str))): tuples of (class name, group entity name, member entity name)

    Returns:
        int: number of items imported
        list: errors
    """
    return import_data(db_map, entity_groups=data)


def import_parameter_definitions(db_map, data, unparse_value=to_database):
    """Imports parameter definitions into a Spine database using a standard format.

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): database mapping
        data (list(tuple(str,str,str,str)):
            tuples of (class name, parameter name, default value, parameter value list name)

    Returns:
        int: number of items imported
        list: errors
    """
    return import_data(db_map, parameter_definitions=data, unparse_value=unparse_value)


def import_parameter_values(db_map, data, unparse_value=to_database, on_conflict="merge"):
    """Imports parameter values into a Spine database using a standard format.

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): database mapping
        data (list(tuple(str,str or tuple(str),str,str|numeric,str]):
            tuples of (class name, entity name or element name list, parameter name, value, alternative name)
        on_conflict (str): Conflict resolution strategy for :func:`~spinedb_api.parameter_value.fix_conflict`

    Returns:
        int: number of items imported
        list: errors
    """
    return import_data(db_map, parameter_values=data, unparse_value=unparse_value, on_conflict=on_conflict)


def import_alternatives(db_map, data):
    """Imports alternatives into a Spine database using a standard format.

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): database mapping
        data (list(str,str)): tuples of (name, description)

    Returns:
        int: number of items imported
        list: errors
    """
    return import_data(db_map, alternatives=data)


def import_scenarios(db_map, data):
    """Imports scenarios into a Spine database using a standard format.

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): database mapping
        data (list(str, bool, str)): tuples of (name, <unused_bool>, description)

    Returns:
        int: number of items imported
        list: errors
    """
    return import_data(db_map, scenarios=data)


def import_scenario_alternatives(db_map, data):
    """Imports scenario alternatives into a Spine database using a standard format.

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): database mapping
        data (list(str,str,str)): tuples of (scenario name, alternative name, preceeding alternative name)

    Returns:
        int: number of items imported
        list: errors
    """
    return import_data(db_map, scenario_alternatives=data)


def import_parameter_value_lists(db_map, data, unparse_value=to_database):
    """Imports parameter value lists into a Spine database using a standard format.

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): database mapping
        data (list(str,str|numeric)): tuples of (list name, value)

    Returns:
        int: number of items imported
        list: errors
    """
    return import_data(db_map, parameter_value_lists=data, unparse_value=unparse_value)


def import_metadata(db_map, data):
    """Imports metadata into a Spine database using a standard format.

    Args:
        db_map (spinedb_api.DiffDatabaseMapping): database mapping
        data (list(tuple(str,str))): tuples of (entry name, value)

    Returns:
        int: number of items imported
        list: errors
    """
    return import_data(db_map, metadata=data)


def import_object_classes(db_map, data):
    return import_data(db_map, object_classes=data)


def import_relationship_classes(db_map, data):
    return import_data(db_map, relationship_classes=data)


def import_objects(db_map, data):
    return import_data(db_map, objects=data)


def import_object_groups(db_map, data):
    return import_data(db_map, object_groups=data)


def import_relationships(db_map, data):
    return import_data(db_map, relationships=data)


def import_object_parameters(db_map, data, unparse_value=to_database):
    return import_data(db_map, object_parameters=data, unparse_value=unparse_value)


def import_relationship_parameters(db_map, data, unparse_value=to_database):
    return import_data(db_map, relationship_parameters=data, unparse_value=unparse_value)


def import_object_parameter_values(db_map, data, unparse_value=to_database, on_conflict="merge"):
    return import_data(db_map, object_parameter_values=data, unparse_value=unparse_value, on_conflict=on_conflict)


def import_relationship_parameter_values(db_map, data, unparse_value=to_database, on_conflict="merge"):
    return import_data(db_map, relationship_parameter_values=data, unparse_value=unparse_value, on_conflict=on_conflict)


def import_object_metadata(db_map, data):
    return import_data(db_map, object_metadata=data)


def import_relationship_metadata(db_map, data):
    return import_data(db_map, relationship_metadata=data)


def import_object_parameter_value_metadata(db_map, data):
    return import_data(db_map, object_parameter_value_metadata=data)


def import_relationship_parameter_value_metadata(db_map, data):
    return import_data(db_map, relationship_parameter_value_metadata=data)


def _get_items_for_import(db_map, item_type, data, check_skip_keys=()):
    mapped_table = db_map.mapped_table(item_type)
    errors = []
    to_add = []
    to_update = []
    seen = {}
    for item in data:
        checked_item, add_error = mapped_table.checked_item_and_error(item, skip_keys=check_skip_keys)
        if not add_error:
            if not _check_unique(item_type, checked_item, seen, errors):
                continue
            to_add.append(checked_item)
            continue
        checked_item, update_error = mapped_table.checked_item_and_error(
            item, for_update=True, skip_keys=check_skip_keys
        )
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
    for key, value in checked_item.unique_key_values():
        if value in seen.get(key, set()):
            return dict(zip(key, value))
        seen.setdefault(key, set()).add(value)


def _get_parameter_superclass_subclasses_for_import(db_map, data):
    key = ("superclass_name", "subclass_name")
    return _get_items_for_import(db_map, "superclass_subclass", (dict(zip(key, x)) for x in data))


def _get_entity_classes_for_import(db_map, data):
    dim_name_list_by_name = {}
    items = []
    key = ("name", "dimension_name_list", "description", "display_icon")
    for x in data:
        if isinstance(x, str):
            x = x, ()
        name, *optionals = x
        dim_name_list = optionals.pop(0) if optionals else ()
        item = dict(zip(key, (name, dim_name_list, *optionals)))
        items.append(item)
        dim_name_list_by_name[name] = dim_name_list

    def _ref_count(name):
        dim_name_list = dim_name_list_by_name.get(name, ())
        return len(dim_name_list) + sum((_ref_count(dim_name) for dim_name in dim_name_list), start=0)

    items_by_ref_count = {}
    for item in items:
        items_by_ref_count.setdefault(_ref_count(item["name"]), []).append(item)
    return (
        _get_items_for_import(db_map, "entity_class", items_by_ref_count[ref_count])
        for ref_count in sorted(items_by_ref_count)
    )


def _get_entities_for_import(db_map, data):
    items_by_el_count = {}
    key = ("class_name", "name", "element_name_list", "description")
    for class_name, name_or_el_name_list, *optionals in data:
        if isinstance(name_or_el_name_list, (list, tuple)):
            name = None
            el_name_list = name_or_el_name_list
        else:
            name = name_or_el_name_list
            if optionals and isinstance(optionals[0], (list, tuple)):
                el_name_list = tuple(optionals.pop(0))
            else:
                el_name_list = ()
        item = dict(zip(key, (class_name, name, el_name_list, *optionals)))
        el_count = len(el_name_list)
        items_by_el_count.setdefault(el_count, []).append(item)
    return (
        _get_items_for_import(db_map, "entity", items_by_el_count[el_count]) for el_count in sorted(items_by_el_count)
    )


def _get_entity_alternatives_for_import(db_map, data):
    def _data_iterator():
        for class_name, entity_name_or_element_name_list, alternative, active in data:
            is_zero_dim = isinstance(entity_name_or_element_name_list, str)
            entity_byname = (entity_name_or_element_name_list,) if is_zero_dim else entity_name_or_element_name_list
            key = ("entity_class_name", "entity_byname", "alternative_name", "active")
            yield dict(zip(key, (class_name, entity_byname, alternative, active)))

    return _get_items_for_import(db_map, "entity_alternative", _data_iterator())


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
            pv = db_map.mapped_table("parameter_value").find_item(item)
            if pv:
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
        scen = db_map.mapped_table("scenario").find_item({"name": scen_name})
        if not scen:
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
        db_map, "scenario_alternative", _data_iterator(), check_skip_keys=(("scenario_name", "rank"),)
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
                current_list = db_map.mapped_table("parameter_value_list").find_item({"name": list_name})
                index = max(
                    (
                        x["index"]
                        for x in db_map.mapped_table("list_value").valid_values()
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
def _object_classes_to_entity_classes(data):
    for x in data:
        if isinstance(x, str):
            yield x, ()
        else:
            name, *optionals = x
            yield name, (), *optionals
