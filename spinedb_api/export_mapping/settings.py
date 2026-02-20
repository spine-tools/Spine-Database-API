######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Database API contributors
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
"""Contains convenience functions to set up different database export schemes."""
from itertools import takewhile
from ..mapping import unflatten
from .export_mapping import (
    AlternativeDescriptionMapping,
    AlternativeMapping,
    DefaultValueIndexNameMapping,
    DimensionMapping,
    ElementMapping,
    EntityClassDescriptionMapping,
    EntityClassMapping,
    EntityDescriptionMapping,
    EntityGroupEntityMapping,
    EntityGroupMapping,
    EntityMapping,
    EntityMetadataNameMapping,
    EntityMetadataValueMapping,
    ExpandedParameterDefaultValueMapping,
    ExpandedParameterValueMapping,
    ExportMapping,
    IndexNameMapping,
    MetadataNameMapping,
    MetadataValueMapping,
    ParameterDefaultValueIndexMapping,
    ParameterDefaultValueMapping,
    ParameterDefaultValueTypeMapping,
    ParameterDefinitionMapping,
    ParameterValueIndexMapping,
    ParameterValueListMapping,
    ParameterValueListValueMapping,
    ParameterValueMapping,
    ParameterValueMetadataNameMapping,
    ParameterValueMetadataValueMapping,
    ParameterValueTypeMapping,
    Position,
    ScenarioAlternativeMapping,
    ScenarioDescriptionMapping,
    ScenarioMapping,
)


def entity_group_export(
    entity_class_position=Position.hidden, group_position=Position.hidden, entity_position=Position.hidden
):
    """
    Sets up export mappings for exporting entity groups.

    Args:
        entity_class_position (int or Position): position of entity classes
        group_position (int or Position): position of groups
        entity_position (int or Position): position of entities

    Returns:
        ExportMapping: root mapping
    """
    class_ = EntityClassMapping(entity_class_position)
    group = EntityGroupMapping(group_position)
    entity = EntityGroupEntityMapping(entity_position)
    group.child = entity
    class_.child = group
    return class_


def entity_export(
    entity_class_position: Position | int = Position.hidden,
    entity_class_description_position: Position | int = Position.hidden,
    entity_position: Position | int = Position.hidden,
    entity_description_position: Position | int = Position.hidden,
    dimension_positions: list[Position | int] | None = None,
    element_positions: list[Position | int] | None = None,
) -> EntityClassMapping:
    """
    Sets up export items for exporting entities without parameters.

    Args:
        entity_class_position: position of entity classes in a table
        entity_class_description_position: position of entity class description.
        entity_position: position of entities in a table
        entity_description_position: position of entity description.
        dimension_positions: positions of dimension in a table
        element_positions: positions of element in a table

    Returns:
        root mapping
    """
    if dimension_positions is None:
        dimension_positions = []
    if element_positions is None:
        element_positions = []
    entity_class = EntityClassMapping(entity_class_position)
    entity_class_description = entity_class.child = EntityClassDescriptionMapping(entity_class_description_position)
    entity_class_description.set_ignorable(True)
    dimension = _generate_dimensions(entity_class_description, DimensionMapping, dimension_positions)
    entity = dimension.child = EntityMapping(entity_position)
    entity_description = entity.child = EntityDescriptionMapping(entity_description_position)
    entity_description.set_ignorable(True)
    _generate_dimensions(entity_description, ElementMapping, element_positions)
    return entity_class


def entity_parameter_default_value_export(
    entity_class_position: Position | int = Position.hidden,
    definition_position: Position | int = Position.hidden,
    value_type_position: Position | int = Position.hidden,
    value_position: Position | int = Position.hidden,
    index_name_positions: list[Position | int] | None = None,
    index_positions: list[Position | int] | None = None,
) -> EntityClassMapping:
    """
    Sets up export mappings for exporting entity classes and default parameter values.

    Args:
        entity_class_position: position of relationship classes
        definition_position: position of parameter definitions
        value_type_position: position of parameter value types
        value_position: position of parameter values
        index_name_positions: positions of index names
        index_positions: positions of parameter indexes

    Returns:
        root mapping
    """
    entity_class = EntityClassMapping(entity_class_position)
    definition = entity_class.child = ParameterDefinitionMapping(definition_position)
    _generate_default_value_mappings(
        definition, value_type_position, value_position, index_name_positions, index_positions
    )
    return entity_class


def entity_parameter_value_export(
    entity_class_position: Position | int = Position.hidden,
    definition_position: Position | int = Position.hidden,
    value_list_position: Position | int = Position.hidden,
    entity_position: Position | int = Position.hidden,
    dimension_positions: list[Position | int] | None = None,
    element_positions: list[Position | int] | None = None,
    alternative_position: Position | int = Position.hidden,
    value_type_position: Position | int = Position.hidden,
    value_position: Position | int = Position.hidden,
    index_name_positions: list[Position | int] | None = None,
    index_positions: list[Position | int] | None = None,
) -> EntityClassMapping:
    """
    Sets up export mappings for exporting entities and parameter values.

    Args:
        entity_class_position: position of entity classes
        definition_position: position of parameter definitions
        value_list_position: position of parameter value lists
        entity_position: position of entities
        dimension_positions: positions of dimensions
        element_positions: positions of elements
        alternative_position: positions of alternatives
        value_type_position: position of parameter value types
        value_position: position of parameter values
        index_name_positions: positions of index names
        index_positions: positions of parameter indexes

    Returns:
        root mapping
    """
    if dimension_positions is None:
        dimension_positions = []
    if element_positions is None:
        element_positions = []
    entity_class = EntityClassMapping(entity_class_position)
    dimension = _generate_dimensions(entity_class, DimensionMapping, dimension_positions)
    definition = dimension.child = ParameterDefinitionMapping(definition_position)
    value_list = definition.child = ParameterValueListMapping(value_list_position)
    value_list.set_ignorable(True)
    relationship = value_list.child = EntityMapping(entity_position)
    element = _generate_dimensions(relationship, ElementMapping, element_positions)
    _generate_parameter_value_mappings(
        element,
        alternative_position,
        value_type_position,
        value_position,
        index_name_positions,
        index_positions,
    )
    return entity_class


def entity_dimension_parameter_default_value_export(
    entity_class_position=Position.hidden,
    definition_position=Position.hidden,
    dimension_positions=None,
    value_type_position=Position.hidden,
    value_position=Position.hidden,
    index_name_positions=None,
    index_positions=None,
    highlight_position=0,
):
    """
    Sets up export mappings for exporting entity classes but with default dimension parameter values.

    Args:
        entity_class_position (int or Position): position of entity classes
        definition_position (int or Position): position of parameter definitions
        dimension_positions (list of int, optional): positions of dimensions
        value_type_position (int or Position): position of parameter value types
        value_position (int or Position): position of parameter values
        index_name_positions (list of int, optional): positions of index names
        index_positions (list of int, optional): positions of parameter indexes
        highlight_position (int): selected dimension

    Returns:
        ExportMapping: root mapping
    """
    root_mapping = unflatten(
        [
            EntityClassMapping(entity_class_position, highlight_position=highlight_position),
            ParameterDefinitionMapping(definition_position),
        ]
    )
    _generate_dimensions(root_mapping.tail_mapping(), DimensionMapping, dimension_positions)
    _generate_default_value_mappings(
        root_mapping.tail_mapping(), value_type_position, value_position, index_name_positions, index_positions
    )
    return root_mapping


def entity_dimension_parameter_value_export(
    entity_class_position: Position | int = Position.hidden,
    definition_position: Position | int = Position.hidden,
    value_list_position: Position | int = Position.hidden,
    entity_position: Position | int = Position.hidden,
    dimension_positions: list[Position | int] | None = None,
    element_positions: list[Position | int] | None = None,
    alternative_position: Position | int = Position.hidden,
    value_type_position: Position | int = Position.hidden,
    value_position: Position | int = Position.hidden,
    index_name_positions: list[Position | int] | None = None,
    index_positions: list[Position | int] | None = None,
    highlight_position: int | None = 0,
) -> EntityClassMapping:
    """
    Sets up export mappings for exporting entities and element parameter values.

    Args:
        entity_class_position: position of entity classes
        definition_position: position of parameter definitions
        value_list_position: position of parameter value lists
        entity_position: position of relationships
        dimension_positions: positions of object classes
        element_positions: positions of objects
        alternative_position: positions of alternatives
        value_type_position: position of parameter value types
        value_position: position of parameter values
        index_name_positions: positions of index names
        index_positions: positions of parameter indexes
        highlight_position: selected dimension position

    Returns:
        root mapping
    """
    if dimension_positions is None:
        dimension_positions = []
    if element_positions is None:
        element_positions = []
    entity_class = EntityClassMapping(entity_class_position, highlight_position=highlight_position)
    dimension = _generate_dimensions(entity_class, DimensionMapping, dimension_positions)
    value_list = ParameterValueListMapping(value_list_position)
    value_list.set_ignorable(True)
    definition = ParameterDefinitionMapping(definition_position)
    dimension.child = definition
    entity = EntityMapping(entity_position)
    definition.child = value_list
    value_list.child = entity
    element = _generate_dimensions(entity, ElementMapping, element_positions)
    _generate_parameter_value_mappings(
        element,
        alternative_position,
        value_type_position,
        value_position,
        index_name_positions,
        index_positions,
    )
    return entity_class


def set_entity_dimensions(entity_mapping: EntityClassMapping, dimensions: int) -> None:
    """
    Modifies given entity mapping's dimensions.

    Args:
        entity_mapping: an entity mapping
        dimensions: number of dimensions
    """
    mapping_list = entity_mapping.flatten()
    dimension_parent_class = (
        EntityClassDescriptionMapping
        if any(isinstance(mapping, EntityDescriptionMapping) for mapping in mapping_list)
        else EntityClassMapping
    )
    mapping_list = _change_amount_of_consecutive_mappings(
        mapping_list, dimension_parent_class, DimensionMapping, dimensions
    )
    if any(isinstance(m, EntityMapping) for m in mapping_list):
        element_parent_class = (
            EntityDescriptionMapping
            if any(isinstance(mapping, EntityDescriptionMapping) for mapping in mapping_list)
            else EntityMapping
        )
        mapping_list = _change_amount_of_consecutive_mappings(
            mapping_list, element_parent_class, ElementMapping, dimensions
        )
    unflatten(mapping_list)


def set_entity_elements(root_mapping: ExportMapping, dimensions: int) -> None:
    """
    Modifies given entity mapping's number of elements in place.

    Args:
        root_mapping: an entity mapping
        dimensions: number of dimensions
    """
    mapping_list = root_mapping.flatten()
    if any(isinstance(m, EntityMapping) for m in mapping_list):
        mapping_list = _change_amount_of_consecutive_mappings(mapping_list, EntityMapping, ElementMapping, dimensions)
    unflatten(mapping_list)


def alternative_export(alternative_position=Position.hidden, alternative_description_position=Position.hidden):
    """
    Sets up export mappings for exporting alternatives.

    Args:
        alternative_position (int or Position): position of alternatives
        alternative_description_position (int or Position): position of descriptions

    Returns:
        Mapping: root mapping
    """
    alt_mapping = AlternativeMapping(alternative_position)
    alt_mapping.child = AlternativeDescriptionMapping(alternative_description_position)
    return alt_mapping


def scenario_export(
    scenario_position=Position.hidden,
    scenario_description_position=Position.hidden,
):
    """
    Sets up export mappings for exporting scenarios.

    Args:
        scenario_position (int or Position): position of scenarios
        scenario_description_position (int or Position): position of descriptions

    Returns:
        Mapping: root mapping
    """
    scenario_mapping = ScenarioMapping(scenario_position)
    scenario_mapping.child = ScenarioDescriptionMapping(scenario_description_position)
    return scenario_mapping


def scenario_alternative_export(scenario_position=Position.hidden, alternative_position=Position.hidden):
    """
    Sets up export mappings for exporting scenario alternatives.

    Args:
        scenario_position (int or Position): position of scenarios
        alternative_position (int or Position): position of alternatives

    Returns:
        Mapping: root mapping
    """
    scenario_mapping = ScenarioMapping(scenario_position)
    scenario_mapping.child = ScenarioAlternativeMapping(alternative_position)
    return scenario_mapping


def parameter_value_list_export(value_list_position=Position.hidden, value_list_value_position=Position.hidden):
    """
    Sets up export mappings for exporting value lists.

    Args:
        value_list_position (int or Position): position of value lists
        value_list_value_position (int or Position): position of list values

    Returns:
        Mapping: root mapping
    """
    value_list_mapping = ParameterValueListMapping(value_list_position)
    value_mapping = ParameterValueListValueMapping(value_list_value_position)
    value_list_mapping.child = value_mapping
    return value_list_mapping


def metadata_export(
    name_position: int | Position = Position.hidden, value_position: int | Position = Position.hidden
) -> MetadataNameMapping:
    return unflatten([MetadataNameMapping(name_position), MetadataValueMapping(value_position)])


def entity_metadata_export(
    entity_class_position: int | Position = Position.hidden,
    entity_position: int | Position = Position.hidden,
    element_positions: list[int | Position] | None = None,
    metadata_name_position: int | Position = Position.hidden,
    metadata_value_position: int | Position = Position.hidden,
) -> EntityClassMapping:
    if element_positions is None:
        element_positions = []
    entity_class = EntityClassMapping(entity_class_position)
    entity = entity_class.child = EntityMapping(entity_position)
    last_element = _generate_dimensions(entity, ElementMapping, element_positions)
    metadata_name = last_element.child = EntityMetadataNameMapping(metadata_name_position)
    metadata_name.child = EntityMetadataValueMapping(metadata_value_position)
    return entity_class


def parameter_value_metadata_export(
    entity_class_position: int | Position = Position.hidden,
    entity_position: int | Position = Position.hidden,
    element_positions: list[int | Position] | None = None,
    definition_position: int | Position = Position.hidden,
    alternative_position: int | Position = Position.hidden,
    metadata_name_position: int | Position = Position.hidden,
    metadata_value_position: int | Position = Position.hidden,
) -> EntityClassMapping:
    if element_positions is None:
        element_positions = []
    entity_class = EntityClassMapping(entity_class_position)
    entity = entity_class.child = EntityMapping(entity_position)
    last_element = _generate_dimensions(entity, ElementMapping, element_positions)
    parameter_definition = last_element.child = ParameterDefinitionMapping(definition_position)
    alternative = parameter_definition.child = AlternativeMapping(alternative_position)
    metadata_name = alternative.child = ParameterValueMetadataNameMapping(metadata_name_position)
    metadata_name.child = ParameterValueMetadataValueMapping(metadata_value_position)
    return entity_class


def set_parameter_dimensions(mapping, dimensions):
    """
    Modifies given mapping's parameter dimensions (number of parameter indexes).

    Args:
        mapping (ExportMapping): a mapping (object or relationship mapping with parameters)
        dimensions (int): number of dimensions
    """
    _change_amount_of_dimensions(
        mapping,
        dimensions,
        ParameterValueMapping,
        ExpandedParameterValueMapping,
        ParameterValueIndexMapping,
        IndexNameMapping,
    )


def set_parameter_default_value_dimensions(mapping, dimensions):
    """
    Modifies given mapping's default dimensions (number of default value indexes).

    Args:
        mapping (ExportMapping): a mapping (object or relationship mapping with parameter default values)
        dimensions (int): number of dimensions
    """
    _change_amount_of_dimensions(
        mapping,
        dimensions,
        ParameterDefaultValueMapping,
        ExpandedParameterDefaultValueMapping,
        ParameterDefaultValueIndexMapping,
        DefaultValueIndexNameMapping,
    )


def _generate_dimensions(parent, cls, positions):
    """
    Nests mappings of same type as children of given ``parent``.

    Args:
        parent (ExportMapping): parent mapping
        cls (Type): mapping type
        positions (Iterable): list of child positions

    Returns:
        ExportMapping: final leaf mapping
    """
    if not positions:
        return parent
    mapping = cls(positions[0])
    parent.child = mapping
    if len(positions) == 1:
        return mapping
    return _generate_dimensions(mapping, cls, positions[1:])


def _generate_parameter_value_mappings(
    mapping, alternative_position, value_type_position, value_position, index_name_positions, index_positions
):
    """
    Appends alternative, value and (optionally) index mappings to given mapping.

    Note: does not append parameter definition mapping.

    Args:
        mapping (ExportMapping): mapping where to add parameter mappings
        alternative_position (int or Position): position of alternatives
        value_type_position (int or Position): position of parameter value types
        value_position (int or Position,): position of parameter values
        index_name_positions (list of int, optional): positions of index names
        index_positions (list of int, optional): positions of parameter indexes
    """
    alternative = AlternativeMapping(alternative_position)
    value_type = ParameterValueTypeMapping(value_type_position)
    alternative.child = value_type
    if not index_positions:
        value = ParameterValueMapping(value_position)
        value_type.child = value
    else:
        current = value_type
        for name_position, index_position in zip(index_name_positions, index_positions):
            name_mapping = IndexNameMapping(name_position)
            current.child = name_mapping
            index_mapping = ParameterValueIndexMapping(index_position)
            name_mapping.child = index_mapping
            current = index_mapping
        current.child = ExpandedParameterValueMapping(value_position)
    mapping.child = alternative


def _generate_default_value_mappings(
    mapping, value_type_position, value_position, index_name_positions, index_positions
):
    """
    Appends default value and (optionally) index mappings to given mapping.

    Note: does not append parameter definition mapping.

    Args:
        mapping (ExportMapping): mapping where to add default value mappings
        value_type_position (int or Position): position of parameter value types in a table
        value_position (int or Position): position of values
        index_name_positions (list of int, optional): positions of index names
        index_positions (list of int, optional): positions of indexes
    """
    type_mapping = ParameterDefaultValueTypeMapping(value_type_position)
    mapping.child = type_mapping
    if not index_positions:
        type_mapping.child = ParameterDefaultValueMapping(value_position)
    else:
        current = type_mapping
        for name_position, index_position in zip(index_name_positions, index_positions):
            name_mapping = DefaultValueIndexNameMapping(name_position)
            current.child = name_mapping
            index_mapping = ParameterDefaultValueIndexMapping(index_position)
            name_mapping.child = index_mapping
            current = index_mapping
        current.child = ExpandedParameterDefaultValueMapping(value_position)


def _change_amount_of_consecutive_mappings(mapping_list, after_class, new_class, count):
    """
    Inserts or removes mappings of same type from mapping list.

    Args:
        mapping_list (list of ExportMapping): flattened mappings
        after_class (Type): modified mappings are children of this type of mapping
        new_class (Type): if new mappings are needed, they will be of this type
        count (int): number of consecutive mappings after the operation

    Returns:
        list: modified flattened mappings
    """
    parent_index = None
    old_count = 0
    for i, mapping in enumerate(mapping_list):
        if isinstance(mapping, after_class):
            parent_index = i
        elif isinstance(mapping, new_class):
            old_count += 1
    new_count = max(count - old_count, 0)
    new_mappings = [new_class(Position.hidden) for _ in range(new_count)]
    first_consecutive_index = parent_index + 1
    last_consecutive_index = first_consecutive_index + old_count
    final_mapping_list = (
        mapping_list[: first_consecutive_index + min(count, old_count)]
        + new_mappings
        + mapping_list[last_consecutive_index:]
    )
    return final_mapping_list


def _change_amount_of_dimensions(
    mapping, dimensions, single_value_mapping, expanded_value_mapping, index_mapping, index_name_mapping
):
    """
    Changes the number of dimensions of parameter values or default values.

    Args:
        mapping (ExportMapping): root mapping
        dimensions (int): number of dimensions
        single_value_mapping (Type): single value mapping class
        expanded_value_mapping (Type): expanded value mapping class
        index_mapping (Type): index mapping class
        index_name_mapping (Type): index name mapping class
    """
    mapping_list = mapping.flatten()
    if dimensions == 0:
        if isinstance(mapping_list[-1], expanded_value_mapping):
            position = mapping_list[-1].position
            mapping_list = list(takewhile(lambda m: not isinstance(m, index_name_mapping), mapping_list))
            mapping_list.append(single_value_mapping(position))
            unflatten(mapping_list)
        return
    if isinstance(mapping_list[-1], single_value_mapping):
        position = mapping_list[-1].position
        mapping_list[-1] = expanded_value_mapping(position)
    existing_dimensions = len([m for m in mapping_list if isinstance(m, index_mapping)])
    if existing_dimensions < dimensions:
        n = dimensions - existing_dimensions
        name_mappings = [index_name_mapping(Position.hidden) for _ in range(n)]
        index_mappings = [index_mapping(Position.hidden) for _ in range(n)]
        mapping_list = (
            mapping_list[:-1] + [m for pair in zip(name_mappings, index_mappings) for m in pair] + mapping_list[-1:]
        )
        unflatten(mapping_list)
    elif existing_dimensions > dimensions:
        mapping_list = mapping_list[: -2 * (existing_dimensions - dimensions) - 1] + mapping_list[-1:]
        unflatten(mapping_list)
