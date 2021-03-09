######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
"""
Contains convenience functions to set up different database export schemes.

:author: A. Soininen (VTT)
:date:   10.12.2020
"""
from itertools import takewhile
from .item_export_mapping import (
    AlternativeMapping,
    AlternativeDescriptionMapping,
    ExpandedParameterDefaultValueMapping,
    ExpandedParameterValueMapping,
    FeatureEntityClassMapping,
    FeatureParameterDefinitionMapping,
    ObjectGroupMapping,
    ObjectMapping,
    ObjectClassMapping,
    ParameterDefaultValueMapping,
    ParameterDefaultValueIndexMapping,
    ParameterDefinitionMapping,
    ParameterValueIndexMapping,
    ParameterValueListMapping,
    ParameterValueListValueMapping,
    ParameterValueMapping,
    Position,
    RelationshipClassMapping,
    RelationshipClassObjectClassMapping,
    RelationshipMapping,
    RelationshipObjectMapping,
    ScenarioActiveFlagMapping,
    ScenarioAlternativeMapping,
    ScenarioBeforeAlternativeMapping,
    ScenarioMapping,
    ScenarioDescriptionMapping,
    ToolFeatureEntityClassMapping,
    ToolFeatureMethodMethodMapping,
    ToolFeatureMethodEntityClassMapping,
    ToolFeatureMethodParameterDefinitionMapping,
    ToolFeatureParameterDefinitionMapping,
    ToolFeatureRequiredFlagMapping,
    ToolMapping,
    unflatten,
)


def object_export(class_position=Position.hidden, object_position=Position.hidden):
    """
    Sets up export mappings for exporting objects without parameters.

    Args:
        class_position (int or Position): position of object classes in a table
        object_position (int or Position): position of objects in a table

    Returns:
        Mapping: root mapping
    """
    class_ = ObjectClassMapping(class_position)
    object_ = ObjectMapping(object_position)
    class_.child = object_
    return class_


def object_parameter_default_value_export(
    class_position=Position.hidden,
    definition_position=Position.hidden,
    value_position=Position.hidden,
    index_positions=None,
):
    """
    Sets up export mappings for exporting objects classes and default parameter values.

    Args:
        class_position (int or Position): position of object classes in a table
        definition_position (int or Position): position of parameter names in a table
        value_position (int or Position): position of parameter values in a table
        index_positions (list or int, optional): positions of parameter indexes in a table

    Returns:
        Mapping: root mapping
    """
    class_ = ObjectClassMapping(class_position)
    definition = ParameterDefinitionMapping(definition_position)
    _generate_default_value_mappings(definition, value_position, index_positions)
    class_.child = definition
    return class_


def object_parameter_export(
    class_position=Position.hidden,
    definition_position=Position.hidden,
    value_list_position=Position.hidden,
    object_position=Position.hidden,
    alternative_position=Position.hidden,
    value_position=Position.hidden,
    index_positions=None,
):
    """
    Sets up export mappings for exporting objects and object parameters.

    Args:
        class_position (int or Position): position of object classes in a table
        definition_position (int or Position): position of parameter names in a table
        value_list_position (int or Position): position of parameter value lists
        object_position (int or Position): position of objects in a table
        alternative_position (int or position): position of alternatives in a table
        value_position (int or Position): position of parameter values in a table
        index_positions (list or int, optional): positions of parameter indexes in a table

    Returns:
        Mapping: root mapping
    """
    class_ = ObjectClassMapping(class_position)
    definition = ParameterDefinitionMapping(definition_position)
    value_list = ParameterValueListMapping(value_list_position)
    value_list.set_ignorable()
    object_ = ObjectMapping(object_position)
    _generate_parameter_value_mappings(object_, alternative_position, value_position, index_positions)
    value_list.child = object_
    definition.child = value_list
    class_.child = definition
    return class_


def object_group_parameter_export(
    class_position=Position.hidden,
    definition_position=Position.hidden,
    value_list_position=Position.hidden,
    object_position=Position.hidden,
    group_position=Position.hidden,
    alternative_position=Position.hidden,
    value_position=Position.hidden,
    index_positions=None,
):
    """
    Sets up export mappings for exporting object groups and object parameters.

    Args:
        class_position (int or Position): position of object classes
        definition_position (int or Position): position of parameter names in a table
        value_list_position (int or Position): position of parameter value lists
        object_position (int or Position): position of objects
        group_position (int or Position): position of groups
        alternative_position (int or position): position of alternatives in a table
        value_position (int or Position): position of parameter values in a table
        index_positions (list or int, optional): positions of parameter indexes in a table

    Returns:
        Mapping: root mapping
    """
    class_ = ObjectClassMapping(class_position)
    definition = ParameterDefinitionMapping(definition_position)
    value_list = ParameterValueListMapping(value_list_position)
    value_list.set_ignorable()
    object_ = ObjectMapping(object_position)
    group = ObjectGroupMapping(group_position)
    object_.child = group
    _generate_parameter_value_mappings(group, alternative_position, value_position, index_positions)
    value_list.child = object_
    definition.child = value_list
    class_.child = definition
    return class_


def object_group_export(
    class_position=Position.hidden, object_position=Position.hidden, group_position=Position.hidden
):
    """
    Sets up export mappings for exporting object groups.

    Args:
        class_position (int or Position): position of object classes
        object_position (int or Position): position of objects
        group_position (int or Position): position of groups

    Returns:
        Mapping: root mapping
    """
    class_ = ObjectClassMapping(class_position)
    object_ = ObjectMapping(object_position)
    group = ObjectGroupMapping(group_position)
    object_.child = group
    class_.child = object_
    return class_


def relationship_export(
    relationship_class_position=Position.hidden,
    relationship_position=Position.hidden,
    object_class_positions=None,
    object_positions=None,
):
    """
    Sets up export items for exporting relationships without parameters.

    Args:
        relationship_class_position (int or Position): position of relationship classes in a table
        relationship_position (int or Position): position of relationships in a table
        object_class_positions (Iterable, optional): positions of object classes in a table
        object_positions (Iterable, optional): positions of object in a table

    Returns:
        Mapping: root mapping
    """
    if object_class_positions is None:
        object_class_positions = list()
    if object_positions is None:
        object_positions = list()
    relationship_class = RelationshipClassMapping(relationship_class_position)
    object_or_relationship_class = _generate_dimensions(
        relationship_class, RelationshipClassObjectClassMapping, object_class_positions
    )
    relationship = RelationshipMapping(relationship_position)
    object_or_relationship_class.child = relationship
    _generate_dimensions(relationship, RelationshipObjectMapping, object_positions)
    return relationship_class


def relationship_parameter_default_value_export(
    relationship_class_position=Position.hidden,
    definition_position=Position.hidden,
    value_position=Position.hidden,
    index_positions=None,
):
    """
    Sets up export mappings for exporting objects classes and default parameter values.

    Args:
        relationship_class_position (int or Position): position of relationship classes
        definition_position (int or Position): position of parameter definitions
        value_position (int or Position): position of parameter values
        index_positions (list of int, optional): positions of parameter indexes

    Returns:
        Mapping: root mapping
    """
    relationship_class = RelationshipClassMapping(relationship_class_position)
    definition = ParameterDefinitionMapping(definition_position)
    _generate_default_value_mappings(definition, value_position, index_positions)
    relationship_class.child = definition
    return relationship_class


def relationship_parameter_export(
    relationship_class_position=Position.hidden,
    definition_position=Position.hidden,
    value_list_position=Position.hidden,
    relationship_position=Position.hidden,
    object_class_positions=None,
    object_positions=None,
    alternative_position=Position.hidden,
    value_position=Position.hidden,
    index_positions=None,
):
    """
    Sets up export mappings for exporting relationships and relationship parameters.

    Args:
        relationship_class_position (int or Position): position of relationship classes
        definition_position (int or Position): position of parameter definitions
        value_list_position (int or Position): position of parameter value lists
        relationship_position (int or Position): position of relationships
        object_class_positions (list of int, optional): positions of object classes
        object_positions (list of int, optional): positions of objects
        alternative_position (int or Position): positions of alternatives
        value_position (int or Position): position of parameter values
        index_positions (list of int, optional): positions of parameter indexes

    Returns:
        Mapping: root mapping
    """
    if object_class_positions is None:
        object_class_positions = list()
    if object_positions is None:
        object_positions = list()
    relationship_class = RelationshipClassMapping(relationship_class_position)
    object_or_relationship_class = _generate_dimensions(
        relationship_class, RelationshipClassObjectClassMapping, object_class_positions
    )
    value_list = ParameterValueListMapping(value_list_position)
    value_list.set_ignorable()
    definition = ParameterDefinitionMapping(definition_position)
    object_or_relationship_class.child = definition
    relationship = RelationshipMapping(relationship_position)
    definition.child = value_list
    value_list.child = relationship
    object_or_relationship = _generate_dimensions(relationship, RelationshipObjectMapping, object_positions)
    _generate_parameter_value_mappings(object_or_relationship, alternative_position, value_position, index_positions)
    return relationship_class


def set_relationship_dimensions(relationship_mapping, dimensions):
    """
    Modifies given relationship mapping's dimensions (number of object classes and objects).

    Args:
        relationship_mapping (mapping): a relationship mapping
        dimensions (int): number of dimensions
    """
    mapping_list = relationship_mapping.flatten()
    mapping_list = _change_amount_of_consecutive_mappings(
        mapping_list, RelationshipClassMapping, RelationshipClassObjectClassMapping, dimensions
    )
    mapping_list = _change_amount_of_consecutive_mappings(
        mapping_list, RelationshipMapping, RelationshipObjectMapping, dimensions
    )
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
    scenario_active_flag_position=Position.hidden,
    scenario_description_position=Position.hidden,
):
    """
    Sets up export mappings for exporting scenarios.

    Args:
        scenario_position (int or Position): position of scenarios
        scenario_active_flag_position (int or Position): position of scenario active flags
        scenario_description_position (int or Position): position of descriptions

    Returns:
        Mapping: root mapping
    """
    scenario_mapping = ScenarioMapping(scenario_position)
    active_flag_mapping = scenario_mapping.child = ScenarioActiveFlagMapping(scenario_active_flag_position)
    active_flag_mapping.child = ScenarioDescriptionMapping(scenario_description_position)
    return scenario_mapping


def scenario_alternative_export(
    scenario_position=Position.hidden, alternative_position=Position.hidden, before_alternative_position=Position.hidden
):
    """
    Sets up export mappings for exporting scenario alternatives.

    Args:
        scenario_position (int or Position): position of scenarios
        alternative_position (int or Position): position of alternatives
        before_alternative_position (int or Position): position of 'before' alternatives
            (for each row, the 'alternative' goes *before* the 'before alternative' in the scenario rank)

    Returns:
        Mapping: root mapping
    """
    scenario_mapping = ScenarioMapping(scenario_position)
    alternative_mapping = scenario_mapping.child = ScenarioAlternativeMapping(alternative_position)
    alternative_mapping.child = ScenarioBeforeAlternativeMapping(before_alternative_position)
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


def set_parameter_dimensions(mapping, dimensions):
    """
    Modifies given mapping's parameter dimensions (number of parameter indexes).

    Args:
        mapping (mapping): a mapping (object or relationship mapping with parameters)
        dimensions (int): number of dimensions
    """
    _change_amount_of_dimensions(
        mapping, dimensions, ParameterValueMapping, ExpandedParameterValueMapping, ParameterValueIndexMapping
    )


def set_parameter_default_value_dimensions(mapping, dimensions):
    """
    Modifies given mapping's default dimensions (number of default value indexes).

    Args:
        mapping (mapping): a mapping (object or relationship mapping with parameter default values)
        dimensions (int): number of dimensions
    """
    _change_amount_of_dimensions(
        mapping,
        dimensions,
        ParameterDefaultValueMapping,
        ExpandedParameterDefaultValueMapping,
        ParameterDefaultValueIndexMapping,
    )


def feature_export(class_position=Position.hidden, definition_position=Position.hidden):
    """
    Sets up export mappings for exporting features.

    Args:
        class_position (int or Position): position of entity classes
        definition_position (int or Position): position of parameter definitions

    Returns:
        Mapping: root mapping
    """
    class_ = FeatureEntityClassMapping(class_position)
    definition = FeatureParameterDefinitionMapping(definition_position)
    class_.child = definition
    return class_


def tool_export(tool_position=Position.hidden):
    """
    Sets up export mappings for exporting tools.

    Args:
        tool_position (int or Position): position of tools

    Returns:
        Mapping: root mapping
    """
    return ToolMapping(tool_position)


def tool_feature_export(
    tool_position=Position.hidden,
    class_position=Position.hidden,
    definition_position=Position.hidden,
    required_flag_position=Position.hidden,
):
    """
    Sets up export mappings for exporting tool features.

    Args:
        tool_position (int or Position): position of tools
        class_position (int or Position): position of entity classes
        definition_position (int or Position): position of parameter definitions
        required_flag_position (int or Position): position of required flags

    Returns:
        Mapping: root mapping
    """
    tool = ToolMapping(tool_position)
    class_ = ToolFeatureEntityClassMapping(class_position)
    definition = ToolFeatureParameterDefinitionMapping(definition_position)
    required_flag = ToolFeatureRequiredFlagMapping(required_flag_position)
    definition.child = required_flag
    class_.child = definition
    tool.child = class_
    return tool


def tool_feature_method_export(
    tool_position=Position.hidden,
    class_position=Position.hidden,
    definition_position=Position.hidden,
    method_position=Position.hidden,
):
    """
    Sets up export mappings for exporting tool feature methods.

    Args:
        tool_position (int or Position): position of tools
        class_position (int or Position): position of entity classes
        definition_position (int or Position): position of parameter definitions
        method_position (int or Position): position of methods

    Returns:
        Mapping: root mapping
    """
    tool = ToolMapping(tool_position)
    class_ = ToolFeatureMethodEntityClassMapping(class_position)
    definition = ToolFeatureMethodParameterDefinitionMapping(definition_position)
    method = ToolFeatureMethodMethodMapping(method_position)
    definition.child = method
    class_.child = definition
    tool.child = class_
    return tool


def _generate_dimensions(parent, cls, positions):
    """
    Nests mappings of same type as children of given ``parent``.

    Args:
        parent (Mapping): parent mapping
        cls (Type): mapping type
        positions (Iterable): list of child positions

    Returns:
        Mapping: final leaf mapping
    """
    if not positions:
        return parent
    mapping = cls(positions[0])
    parent.child = mapping
    if len(positions) == 1:
        return mapping
    return _generate_dimensions(mapping, cls, positions[1:])


def _generate_parameter_value_mappings(mapping, alternative_position, value_position, index_positions):
    """
    Appends alternative, value and (optionally) index mappings to given mapping.

    Note: does not append parameter definition mapping.

    Args:
        mapping (Mapping): mapping where to add parameter mappings
        alternative_position (int or Position): position of alternatives
        value_position (int or Position,): position of parameter values
        index_positions (list of int, optional): positions of parameter indexes
    """
    alternative = AlternativeMapping(alternative_position)
    if not index_positions:
        value = ParameterValueMapping(value_position)
        alternative.child = value
    else:
        last_index = _generate_dimensions(alternative, ParameterValueIndexMapping, index_positions)
        last_index.child = ExpandedParameterValueMapping(value_position)
    mapping.child = alternative


def _generate_default_value_mappings(mapping, value_position, index_positions):
    """
    Appends default value and (optionally) index mappings to given mapping.

    Note: does not append parameter definition mapping.

    Args:
        mapping (Mapping): mapping where to add default value mappings
        value_position (int or Position): position of values
        index_positions (list of int, optional): positions of indexes
    """
    if not index_positions:
        mapping.child = ParameterDefaultValueMapping(value_position)
    else:
        last_index = _generate_dimensions(mapping, ParameterDefaultValueIndexMapping, index_positions)
        last_index.child = ExpandedParameterDefaultValueMapping(value_position)


def _change_amount_of_consecutive_mappings(mapping_list, after_class, new_class, count):
    """
    Inserts or removes mappings of same type from mapping list.

    Args:
        mapping_list (list of Mapping): flattened mappings
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


def _change_amount_of_dimensions(mapping, dimensions, single_value_mapping, expanded_value_mapping, index_mapping):
    """
    Changes the number of dimensions of parameter values or default values.

    Args:
        mapping (Mapping): root mapping
        dimensions (int): number of dimensions
        single_value_mapping (Type): single value mapping class
        expanded_value_mapping (Type): expanded value mapping class
        index_mapping (Type): index mapping class
    """
    mapping_list = mapping.flatten()
    if dimensions == 0:
        if type(mapping_list[-1]) == expanded_value_mapping:
            position = mapping_list[-1].position
            mapping_list = list(takewhile(lambda m: type(m) != index_mapping, mapping_list))
            mapping_list.append(single_value_mapping(position))
            unflatten(mapping_list)
        return
    if type(mapping_list[-1]) == single_value_mapping:
        position = mapping_list[-1].position
        mapping_list[-1] = expanded_value_mapping(position)
    existing_dimensions = len([m for m in mapping_list if type(m) == index_mapping])
    if existing_dimensions < dimensions:
        mapping_list = (
            mapping_list[:-1]
            + [index_mapping(Position.hidden) for _ in range(dimensions - existing_dimensions)]
            + mapping_list[-1:]
        )
        unflatten(mapping_list)
    elif existing_dimensions > dimensions:
        mapping_list = mapping_list[: -(existing_dimensions - dimensions + 1)] + mapping_list[-1:]
        unflatten(mapping_list)
