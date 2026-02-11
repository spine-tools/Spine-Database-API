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
Functions for importing data into a Spine database in a standard format.
This functionality is equivalent to the one provided by :meth:`.DatabaseMapping.add_update_item`,
but the syntax is a little more compact.
"""
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import suppress
from typing import Any, Optional, TypeAlias
from . import DatabaseMapping, SpineDBAPIError
from .helpers import DisplayStatus, ItemType, _parse_metadata
from .parameter_value import (
    ConflictResolution,
    ConflictResolutionCallable,
    Value,
    fancy_type_to_type_and_rank,
    get_conflict_fixer,
    to_database,
)

UnparseCallable: TypeAlias = Callable[[Value], tuple[bytes, Optional[str]]]
Alternative: TypeAlias = tuple[str] | tuple[str, str]
Scenario: TypeAlias = tuple[str] | tuple[str, bool] | tuple[str, bool, str]
ScenarioAlternative: TypeAlias = tuple[str, str] | tuple[str, str, str]
Location: TypeAlias = tuple[float, float, float, str, str]
Entity: TypeAlias = (
    tuple[str, str | Sequence[str]]
    | tuple[str, str | Sequence[str], str]
    | tuple[str, str | Sequence[str], str, Location]
)
EntityAlternative: TypeAlias = tuple[str, str | Sequence[str], str, bool]
EntityGroup: TypeAlias = tuple[str, str, str]
EntityClass: TypeAlias = (
    tuple[str, Sequence[str]]
    | tuple[str, Sequence[str], str]
    | tuple[str, Sequence[str], str, int]
    | tuple[str, Sequence[str], str, int, bool]
)
ParameterValueList: TypeAlias = tuple[str, Any]
ParameterValue: TypeAlias = tuple[str, str | Sequence[str], str, Any] | tuple[str, str | Sequence[str], str, Any, str]
ParameterGroup: TypeAlias = tuple[str, str, int]
ParameterType: TypeAlias = tuple[str, str, str] | tuple[str, str, str, str]
ParameterDefinition: TypeAlias = (
    tuple[str, str]
    | tuple[str, str, Any]
    | tuple[str, str, Any, str]
    | tuple[str, str, Any, str, str]
    | tuple[str, str, Any, str, str, str]
    | tuple[str, str, Any, str, str, str, str]
)
SuperclassSubclass: TypeAlias = tuple[str, str]
DisplayMode: TypeAlias = tuple[str] | tuple[str, str]
EntityClassDisplayMode: TypeAlias = (
    tuple[str, str, int]
    | tuple[str, str, int, DisplayStatus]
    | tuple[str, str, int, DisplayStatus, str]
    | tuple[str, str, int, DisplayStatus, str, str]
)
Metadata: TypeAlias = tuple[str, str]
EntityMetadata: TypeAlias = tuple[str, Sequence[str], str, str]
ParameterValueMetadata: TypeAlias = tuple[str, Sequence[str], str, str, str, str]


def import_data(
    db_map: DatabaseMapping,
    unparse_value: UnparseCallable = to_database,
    on_conflict: ConflictResolution = "merge",
    **kwargs,
) -> tuple[int, list[str]]:
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
        db_map: database mapping
        unparse_value: function to call to parse parameter values
        on_conflict: Conflict resolution strategy for :func:`parameter_value.fix_conflict`
        **kwargs: data to import

    Returns:
        number of items imported and list of errors
    """
    all_errors: list[str] = []
    num_imports = 0
    conflict_fixer = get_conflict_fixer(on_conflict)
    for item_type, items in get_data_for_import(
        db_map, all_errors, unparse_value=unparse_value, fix_value_conflict=conflict_fixer, **kwargs
    ):
        added, updated, errors = db_map.add_update_items(item_type, *items, strict=False)
        num_imports += len(added + updated)
        all_errors.extend(errors)
    return num_imports, all_errors


def get_data_for_import(
    db_map: DatabaseMapping,
    all_errors: list[str],
    unparse_value: UnparseCallable = to_database,
    fix_value_conflict: ConflictResolutionCallable = get_conflict_fixer("merge"),
    entity_classes: Iterable[EntityClass] = (),
    entities: Iterable[Entity] = (),
    entity_groups: Iterable[EntityGroup] = (),
    entity_alternatives: Iterable[EntityAlternative] = (),
    parameter_definitions: Iterable[ParameterDefinition] = (),
    parameter_types: Iterable[ParameterType] = (),
    parameter_values: Iterable[ParameterValue] = (),
    parameter_value_lists: Iterable[ParameterValueList] = (),
    parameter_groups: Iterable[ParameterGroup] = (),
    alternatives: Iterable[Alternative] = (),
    scenarios: Iterable[Scenario] = (),
    scenario_alternatives: Iterable[ScenarioAlternative] = (),
    metadata: Iterable[Metadata] = (),
    entity_metadata: Iterable[EntityMetadata] = (),
    parameter_value_metadata: Iterable[ParameterValueMetadata] = (),
    superclass_subclasses: Iterable[SuperclassSubclass] = (),
    display_modes: Iterable[DisplayMode] = (),
    entity_class_display_modes: Iterable[EntityClassDisplayMode] = (),
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
) -> Iterator[tuple[ItemType, Iterable[dict]]]:
    """Yields data to import into a Spine DB.

    Args:
        db_map: database mapping
        all_errors: errors encountered during import
        unparse_value: function to call when parsing parameter values
        fix_value_conflict: parameter value conflict resolution function
        entity_classes: entity class tuples
        parameter_definitions: tuples of parameter definitions
        parameter_types: tuples of parameter types
        parameter_groups: tuples of parameter groups
        entities: tuples of entities
        entity_alternatives: tuples of entity alternatives
        entity_groups: tuples of entity groups
        parameter_values: tuples of parameter values
        alternatives: tuples of alternatives
        scenarios: tuples of scenarios
        scenario_alternatives: tuples of scenario alternatives
        parameter_value_lists: tuples of parameter value lists
        metadata: tuples of metadata
        entity_metadata: tuples of entity metadata
        parameter_value_metadata: tuples of parameter value metadata
        superclass_subclasses: tuples of superclass subclasses
        display_modes: tuples of display modes
        entity_class_display_modes: tuples of entity class display modes

    Yields:
        tuple of (item type, item dicts)
    """
    # NOTE: The order is important, because of references. E.g., we want to import alternatives before parameter_values
    if alternatives:
        yield ("alternative", _get_alternatives_for_import(alternatives))
    if scenarios:
        yield ("scenario", _get_scenarios_for_import(scenarios))
    if scenario_alternatives:
        yield ("scenario_alternative", _get_scenario_alternatives_for_import(db_map, scenario_alternatives, all_errors))
    if entity_classes:
        for bucket in _get_entity_classes_for_import(entity_classes):
            yield ("entity_class", bucket)
    if object_classes:  # Legacy
        yield from get_data_for_import(
            db_map, all_errors, entity_classes=_object_classes_to_entity_classes(object_classes)
        )
    if relationship_classes:  # Legacy
        yield from get_data_for_import(db_map, all_errors, entity_classes=relationship_classes)
    if superclass_subclasses:
        yield ("superclass_subclass", _get_superclass_subclasses_for_import(superclass_subclasses))
    if entities:
        for bucket in _get_entities_for_import(entities):
            yield ("entity", bucket)
    if objects:  # Legacy
        yield from get_data_for_import(db_map, all_errors, entities=objects)
    if relationships:  # Legacy
        yield from get_data_for_import(db_map, all_errors, entities=relationships)
    if entity_alternatives:
        yield ("entity_alternative", _get_entity_alternatives_for_import(entity_alternatives))
    if entity_groups:
        yield ("entity_group", _get_entity_groups_for_import(entity_groups))
    if object_groups:  # Legacy
        yield from get_data_for_import(db_map, all_errors, entity_groups=object_groups)
    if parameter_value_lists:
        yield ("parameter_value_list", _get_parameter_value_lists_for_import(parameter_value_lists))
        yield ("list_value", _get_list_values_for_import(db_map, parameter_value_lists, unparse_value))
    if parameter_groups:
        yield ("parameter_group", _get_parameter_groups_for_import(parameter_groups))
    if parameter_definitions:
        yield (
            "parameter_definition",
            _get_parameter_definitions_for_import(parameter_definitions, unparse_value),
        )
    if object_parameters:  # Legacy
        yield from get_data_for_import(
            db_map, all_errors, unparse_value=unparse_value, parameter_definitions=object_parameters
        )
    if relationship_parameters:  # Legacy
        yield from get_data_for_import(
            db_map, all_errors, unparse_value=unparse_value, parameter_definitions=relationship_parameters
        )
    if parameter_types:
        yield ("parameter_type", _get_parameter_types_for_import(parameter_types, all_errors))
    if parameter_values:
        yield (
            "parameter_value",
            _get_parameter_values_for_import(db_map, parameter_values, all_errors, unparse_value, fix_value_conflict),
        )
    if object_parameter_values:  # Legacy
        yield from get_data_for_import(
            db_map,
            all_errors,
            unparse_value=unparse_value,
            fix_value_conflict=fix_value_conflict,
            parameter_values=object_parameter_values,
        )
    if relationship_parameter_values:  # Legacy
        yield from get_data_for_import(
            db_map,
            all_errors,
            unparse_value=unparse_value,
            fix_value_conflict=fix_value_conflict,
            parameter_values=relationship_parameter_values,
        )
    if metadata:
        yield ("metadata", _get_metadata_for_import(metadata))
    if entity_metadata:
        yield ("metadata", _get_metadata_for_import((ent_metadata[2] for ent_metadata in entity_metadata)))
        yield ("entity_metadata", _get_entity_metadata_for_import(entity_metadata))
    if parameter_value_metadata:
        yield (
            "metadata",
            _get_metadata_for_import((pval_metadata[3] for pval_metadata in parameter_value_metadata)),
        )
        yield ("parameter_value_metadata", _get_parameter_value_metadata_for_import(db_map, parameter_value_metadata))
    if object_metadata:  # Legacy
        yield from get_data_for_import(db_map, all_errors, entity_metadata=object_metadata)
    if relationship_metadata:  # Legacy
        yield from get_data_for_import(db_map, all_errors, entity_metadata=relationship_metadata)
    if object_parameter_value_metadata:  # Legacy
        yield from get_data_for_import(db_map, all_errors, parameter_value_metadata=object_parameter_value_metadata)
    if relationship_parameter_value_metadata:  # Legacy
        yield from get_data_for_import(
            db_map, all_errors, parameter_value_metadata=relationship_parameter_value_metadata
        )
    if display_modes:
        yield (
            "display_mode",
            _get_display_modes_for_import(display_modes),
        )
    if entity_class_display_modes:
        yield (
            "entity_class_display_mode",
            _get_entity_class_display_modes_for_import(entity_class_display_modes),
        )


def import_superclass_subclasses(db_map: DatabaseMapping, data: Iterable[SuperclassSubclass]) -> tuple[int, list[str]]:
    """Imports superclass_subclasses into a Spine database using a standard format.

    Args:
        db_map: database mapping
        data: tuples of (superclass name, subclass name)

    Returns:
        tuple of (number of items imported, list of errors)
    """
    return import_data(db_map, superclass_subclasses=data)


def import_entity_classes(db_map: DatabaseMapping, data: Iterable[EntityClass]) -> tuple[int, list[str]]:
    """Imports entity classes into a Spine database using a standard format.

    Args:
        db_map: database mapping
        data: tuples of (name, [(dimension 1 name, dimension 2 name ,...)], [description], [display icon integer], [active by default])

    Returns:
        tuple of (number of items imported, list of errors)
    """
    return import_data(db_map, entity_classes=data)


def import_entities(db_map: DatabaseMapping, data: Iterable[Entity]) -> tuple[int, list[str]]:
    """Imports entities into a Spine database using a standard format.

    Args:
        db_map: database mapping
        data: tuples of (class name, entity name or byname, [description], [location])
            where location is a tuple of (latitude, longitude, altitude, shape name, shape GEOJSON)

    Returns:
        tuple of (number of items imported, list of errors)
    """
    return import_data(db_map, entities=data)


def import_entity_alternatives(db_map: DatabaseMapping, data: Iterable[EntityAlternative]) -> tuple[int, list[str]]:
    """Imports entity alternatives into a Spine database using a standard format.

    Args:
        db_map: database mapping
        data: tuples of (class name, entity name or byname, alternative name, activity)

    Returns:
        tuple of (number of items imported, list of errors)
    """
    return import_data(db_map, entity_alternatives=data)


def import_entity_groups(db_map: DatabaseMapping, data: Iterable[EntityGroup]) -> tuple[int, list[str]]:
    """Imports entity groups into a Spine database using a standard format.

    Args:
        db_map: database mapping
        data: tuples of (class name, group entity name, member entity name)

    Returns:
        tuple of (number of items imported, list of errors)
    """
    return import_data(db_map, entity_groups=data)


def import_parameter_definitions(
    db_map: DatabaseMapping, data: Iterable[ParameterDefinition], unparse_value: UnparseCallable = to_database
) -> tuple[int, list[str]]:
    """Imports parameter definitions into a Spine database using a standard format.

    Args:
        db_map: database mapping
        data: tuples of (class name, parameter name, [default value], [default type], [parameter value list name], [description])
        unparse_value: function to parse parameter values

    Returns:
        tuple of (number of items imported, list of errors)
    """
    return import_data(db_map, parameter_definitions=data, unparse_value=unparse_value)


def import_parameter_types(
    db_map: DatabaseMapping, data: Iterable[ParameterType], unparse_value: UnparseCallable = to_database
) -> tuple[int, list[str]]:
    """Imports parameter types into a Spine database using a standard format.

    Args:
        db_map: database mapping
        data: tuple of (class name, parameter name, type, [succeeding type])
        unparse_value: function to parse parameter values

    Returns:
        tuple of (number of items imported, list of errors)
    """
    return import_data(db_map, parameter_types=data, unparse_value=unparse_value)


def import_parameter_values(
    db_map: DatabaseMapping,
    data: Iterable[ParameterValue],
    unparse_value: UnparseCallable = to_database,
    on_conflict: ConflictResolution = "merge",
) -> tuple[int, list[str]]:
    """Imports parameter values into a Spine database using a standard format.

    Args:
        db_map: database mapping
        data: tuples of (class name, entity name or byname, parameter definition name, value, [alternative_name])
        unparse_value: function to parse parameter values
        on_conflict: Conflict resolution strategy; options: "keep", "replace", "merge"

    Returns:
        tuple of (number of items imported, list of errors)
    """
    return import_data(db_map, parameter_values=data, unparse_value=unparse_value, on_conflict=on_conflict)


def import_alternatives(db_map: DatabaseMapping, data: Iterable[Alternative]) -> tuple[int, list[str]]:
    """Imports alternatives into a Spine database using a standard format.

    Args:
        db_map: database mapping
        data: tuples of (name, [description])

    Returns:
        tuple of (number of items imported, list of errors)
    """
    return import_data(db_map, alternatives=data)


def import_scenarios(db_map: DatabaseMapping, data: Iterable[Scenario]) -> tuple[int, list[str]]:
    """Imports scenarios into a Spine database using a standard format.

    Args:
        db_map: database mapping
        data: tuples of (name, [<unused bool>], [description])

    Returns:
        tuple of (number of items imported, list of errors)
    """
    return import_data(db_map, scenarios=data)


def import_display_modes(db_map: DatabaseMapping, data: Iterable[DisplayMode]) -> tuple[int, list[str]]:
    """Imports display modes into a Spine database using a standard format.

    Args:
        db_map: database mapping
        data: tuples of (name, [description])

    Returns:
        tuple of (number of items imported, list of errors)
    """
    return import_data(db_map, display_modes=data)


def import_entity_class_display_modes(
    db_map: DatabaseMapping, data: Iterable[EntityClassDisplayMode]
) -> tuple[int, list[str]]:
    """Imports entity class display modes into a Spine database using a standard format.

    Args:
        db_map: database mapping
        data: tuples of (display mode name, entity class name, display order)

    Returns:
        tuple of (number of items imported, list of errors)
    """
    return import_data(db_map, entity_class_display_modes=data)


def import_scenario_alternatives(db_map: DatabaseMapping, data: Iterable[ScenarioAlternative]) -> tuple[int, list[str]]:
    """Imports scenario alternatives into a Spine database using a standard format.

    Args:
        db_map: database mapping
        data: tuples of (scenario, alternative, [succeeding alternative])

    Returns:
        tuple of (number of items imported, list of errors)
    """
    return import_data(db_map, scenario_alternatives=data)


def import_parameter_value_lists(
    db_map: DatabaseMapping,
    data: Iterable[ParameterValueList],
    unparse_value: UnparseCallable = to_database,
) -> tuple[int, list[str]]:
    """Imports parameter value lists into a Spine database using a standard format.

    Args:
        db_map: database mapping
        data: tuples of (list name, value)
        unparse_value: function to parse parameter values

    Returns:
        tuple of (number of items imported, list of errors)
    """
    return import_data(db_map, parameter_value_lists=data, unparse_value=unparse_value)


def import_parameter_groups(db_map: DatabaseMapping, data: Iterable[ParameterGroup]) -> tuple[int, list[str]]:
    """Imports parameter groups into a Spine database using a standard format.

    Args:
        db_map: database mapping
        data: tuples of (group name, color as 6-digit HEX value)

    Returns:
        tuple of (number of groups imported, list of errors)
    """
    return import_data(db_map, parameter_groups=data)


def import_metadata(db_map: DatabaseMapping, data: Iterable[Metadata]) -> tuple[int, list[str]]:
    """Imports metadata into a Spine database using a standard format.

    Args:
        db_map: database mapping
        data: tuples of (entry name, value)

    Returns:
        tuple of (number of items imported, list of errors)
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


def _get_entity_classes_for_import(data: Iterable[EntityClass]) -> Iterable[dict]:
    dim_name_list_by_name = {}
    items = []
    key = ("name", "dimension_name_list", "description", "display_icon", "active_by_default")
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
    return (items_by_ref_count[ref_count] for ref_count in sorted(items_by_ref_count))


def _get_superclass_subclasses_for_import(data: Iterable[SuperclassSubclass]) -> Iterable[dict]:
    key = ("superclass_name", "subclass_name")
    return (dict(zip(key, x)) for x in data)


def _get_display_modes_for_import(data: Iterable[DisplayMode]) -> Iterable[dict]:
    key = ("name", "description")
    return ({"name": x} if isinstance(x, str) else dict(zip(key, x)) for x in data)


def _get_entity_class_display_modes_for_import(data: Iterable[EntityClassDisplayMode]) -> Iterator[dict]:
    key = (
        "display_mode_name",
        "entity_class_name",
        "display_order",
        "display_status",
        "display_font_color",
        "display_background_color",
    )
    for display_mode_name, entity_class_name, display_order, *optionals in data:
        yield dict(zip(key, (display_mode_name, entity_class_name, display_order, *optionals)))


def _get_entities_for_import(data: Iterable[Entity]) -> Iterable[list[dict]]:
    items_by_el_count: dict[int, list[dict]] = {}
    key = ("entity_class_name", "entity_byname", "description", "location")
    for class_name, name_or_el_name_list, *optionals in data:
        if isinstance(name_or_el_name_list, str):
            el_count = 0
            byname = (name_or_el_name_list,)
        else:
            el_count = len(name_or_el_name_list)
            byname = name_or_el_name_list
        item = dict(zip(key, (class_name, byname, *optionals)))
        if "location" in item:
            location = item.pop("location")
            if location is not None:
                with suppress(IndexError):
                    item["lat"] = location[0]
                    item["lon"] = location[1]
                    item["alt"] = location[2]
                    item["shape_name"] = location[3]
                    item["shape_blob"] = location[4]
        items_by_el_count.setdefault(el_count, []).append(item)
    return (items_by_el_count[el_count] for el_count in sorted(items_by_el_count))


def _get_entity_alternatives_for_import(data: Iterable[EntityAlternative]) -> Iterable[dict]:
    for class_name, entity_name_or_element_name_list, alternative, active in data:
        is_zero_dim = isinstance(entity_name_or_element_name_list, str)
        entity_byname = (entity_name_or_element_name_list,) if is_zero_dim else entity_name_or_element_name_list
        key = ("entity_class_name", "entity_byname", "alternative_name", "active")
        yield dict(zip(key, (class_name, entity_byname, alternative, active)))


def _get_entity_groups_for_import(data: Iterable[EntityGroup]) -> Iterable[dict]:
    key = ("entity_class_name", "group_name", "member_name")
    return (dict(zip(key, x)) for x in data)


def _get_parameter_definitions_for_import(
    data: Iterable[ParameterDefinition], unparse_value: UnparseCallable
) -> Iterator[dict]:
    key = (
        "entity_class_name",
        "name",
        "default_value",
        "default_type",
        "parameter_value_list_name",
        "description",
        "parameter_group_name",
    )
    for class_name, parameter_name, *optionals in data:
        if not optionals:
            yield dict(zip(key, (class_name, parameter_name)))
            continue
        value = optionals.pop(0)
        value, type_ = unparse_value(value)
        yield dict(zip(key, (class_name, parameter_name, value, type_, *optionals)))


def _get_parameter_values_for_import(
    db_map: DatabaseMapping,
    data: Iterable[ParameterValue],
    all_errors: list[str],
    unparse_value: UnparseCallable,
    fix_conflict: ConflictResolutionCallable,
) -> Iterator[dict]:
    seen = set()
    key = ("entity_class_name", "entity_byname", "parameter_definition_name", "alternative_name", "value", "type")
    parameter_value_table = db_map.mapped_table("parameter_value")
    for class_name, entity_byname, parameter_name, value, *optionals in data:
        if isinstance(entity_byname, str):
            entity_byname = (entity_byname,)
        else:
            entity_byname = tuple(entity_byname)
        alternative_name = optionals[0] if optionals else db_map.get_import_alternative_name()
        unique_values = (class_name, entity_byname, parameter_name, alternative_name)
        if unique_values in seen:
            dupe = dict(zip(key, unique_values))
            all_errors.append(
                f"attempting to import more than one parameter_value with {dupe} - only first will be considered"
            )
            continue
        seen.add(unique_values)
        value, type_ = unparse_value(value)
        item = dict(zip(key, unique_values + (None, None)))
        try:
            pv = parameter_value_table.find_item(item)
        except SpineDBAPIError:
            pass
        else:
            value, type_ = fix_conflict((value, type_), (pv["value"], pv["type"]))
        item.update({"value": value, "type": type_})
        yield item


def _get_alternatives_for_import(data: Iterable[Alternative]) -> Iterable[dict]:
    key = ("name", "description")
    return ({"name": x} if isinstance(x, str) else dict(zip(key, x)) for x in data)


def _get_scenarios_for_import(data: Iterable[Scenario]) -> Iterable[dict]:
    key = ("name", "active", "description")
    return ({"name": x} if isinstance(x, str) else dict(zip(key, x)) for x in data)


def _get_scenario_alternatives_for_import(
    db_map: DatabaseMapping, data: Iterable[ScenarioAlternative], all_errors: list[str]
) -> Iterable[dict]:
    # FIXME: maybe when updating, we only want to match by (scen_name, alt_name) and not by (scen_name, rank)
    alt_name_list_by_scen_name = {}
    succ_by_pred_by_scen_name = defaultdict(dict)
    for scen_name, predecessor, *optionals in data:
        successor = optionals[0] if optionals else None
        succ_by_pred_by_scen_name[scen_name][predecessor] = successor
    scenario_table = db_map.mapped_table("scenario")
    for scen_name, succ_by_pred in succ_by_pred_by_scen_name.items():
        try:
            scen = scenario_table.find_item({"name": scen_name})
        except SpineDBAPIError:
            alternative_name_list = []
        else:
            alternative_name_list = scen.get("alternative_name_list", [])
        alt_name_list_by_scen_name[scen_name] = alternative_name_list
        alternative_name_list.append(None)  # So alternatives where successor is None find their place at the tail
        while succ_by_pred:
            some_added = False
            for pred, succ in list(succ_by_pred.items()):
                if succ in alternative_name_list:
                    if pred in alternative_name_list:
                        alternative_name_list.remove(pred)
                    i = alternative_name_list.index(succ)
                    alternative_name_list.insert(i, pred)
                    del succ_by_pred[pred]
                    some_added = True
            if not some_added:
                break
        alternative_name_list.pop(-1)  # Remove the None
    all_errors += [
        f"can't insert alternative '{pred}' before '{succ}' because the latter is not in scenario '{scen}'"
        for scen, succ_by_pred in succ_by_pred_by_scen_name.items()
        for pred, succ in succ_by_pred.items()
    ]
    for scen_name, alternative_name_list in alt_name_list_by_scen_name.items():
        for k, alt_name in enumerate(alternative_name_list):
            yield {"scenario_name": scen_name, "alternative_name": alt_name, "rank": k + 1}


def _get_parameter_value_lists_for_import(data: Iterable[ParameterValueList]) -> Iterable[dict]:
    return ({"name": x} for x in {x[0]: None for x in data})


def _get_list_values_for_import(
    db_map: DatabaseMapping, data: Iterable[ParameterValueList], unparse_value: UnparseCallable
) -> Iterator[dict]:
    index_by_list_name = {}
    db_map.fetch_all("list_value")
    value_list_table = db_map.mapped_table("parameter_value_list")
    for list_name, value in data:
        value, type_ = unparse_value(value)
        index = index_by_list_name.get(list_name)
        if index is None:
            current_list = value_list_table.find_item({"name": list_name})
            current_list_id = current_list["id"]
            list_value_idx_by_val_typ = {
                (x["value"], x["type"]): x["index"]
                for x in db_map.mapped_table("list_value").valid_values()
                if x["parameter_value_list_id"] == current_list_id
            }
            if (value, type_) in list_value_idx_by_val_typ:
                continue
            index = max((idx for idx in list_value_idx_by_val_typ.values()), default=-1)
        index += 1
        index_by_list_name[list_name] = index
        yield {"parameter_value_list_name": list_name, "value": value, "type": type_, "index": index}


def _get_parameter_types_for_import(data: Iterable[ParameterType], all_errors: list[str]) -> Iterator[dict]:
    for class_name, definition_name, parameter_type, *optionals in data:
        if not optionals:
            if parameter_type == "map":
                all_errors.append(f"Missing rank for map type for parameter {definition_name} in class {class_name}")
                continue
            try:
                parameter_type, rank = fancy_type_to_type_and_rank(parameter_type)
            except ValueError:
                all_errors.append(
                    f"Failed to read rank from type '{parameter_type}' for parameter {definition_name} in class {class_name}"
                )
                continue
        else:
            rank = optionals[0]
        yield {
            "entity_class_name": class_name,
            "parameter_definition_name": definition_name,
            "type": parameter_type,
            "rank": rank,
        }


def _get_parameter_groups_for_import(data: Iterable[ParameterGroup]) -> Iterator[dict]:
    for parameter_group in data:
        yield {"name": parameter_group[0], "color": parameter_group[1], "priority": parameter_group[2]}


def _get_metadata_for_import(data: Iterable[Metadata]) -> Iterator[dict]:
    for metadata in data:
        for name, value in _parse_metadata(metadata):
            yield {"name": name, "value": value}


def _get_entity_metadata_for_import(data: Iterable[EntityMetadata]) -> Iterator[dict]:
    key = ("entity_class_name", "entity_byname", "metadata_name", "metadata_value")
    for class_name, entity_byname, metadata in data:
        if isinstance(entity_byname, str):
            entity_byname = (entity_byname,)
        for name, value in _parse_metadata(metadata):
            yield dict(zip(key, (class_name, entity_byname, name, value)))


def _get_parameter_value_metadata_for_import(
    db_map: DatabaseMapping, data: Iterable[ParameterValueMetadata]
) -> Iterator[dict]:
    key = (
        "entity_class_name",
        "entity_byname",
        "parameter_definition_name",
        "metadata_name",
        "metadata_value",
        "alternative_name",
    )
    for class_name, entity_byname, parameter_name, metadata, *optionals in data:
        if isinstance(entity_byname, str):
            entity_byname = (entity_byname,)
        alternative_name = optionals[0] if optionals else db_map.get_import_alternative_name()
        for name, value in _parse_metadata(metadata):
            yield dict(zip(key, (class_name, entity_byname, parameter_name, name, value, alternative_name)))


# Legacy
def _object_classes_to_entity_classes(data):
    for x in data:
        if isinstance(x, str):
            yield x, ()
        else:
            name, *optionals = x
            yield name, (), *optionals
