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
Classes for item import mappings.

:author: P. Vennström (VTT)
:date:   22.02.2018
"""
from .import_mapping import (
    Position,
    ObjectClassMapping,
    RelationshipClassMapping,
    RelationshipClassObjectClassMapping,
    ObjectMapping,
    RelationshipMapping,
    RelationshipObjectMapping,
    ParameterDefinitionMapping,
    ParameterDefaultValueMapping,
    ParameterValueMapping,
    ParameterValueTypeMapping,
    ParameterValueIndexMapping,
    ExpandedParameterValueMapping,
    AlternativeMapping,
    ScenarioMapping,
    ScenarioActiveFlagMapping,
    ScenarioAlternativeMapping,
    ScenarioBeforeAlternativeMapping,
    ToolMapping,
    FeatureEntityClassMapping,
    FeatureParameterDefinitionMapping,
    ToolFeatureEntityClassMapping,
    ToolFeatureParameterDefinitionMapping,
    ToolFeatureRequiredFlagMapping,
    ToolFeatureMethodEntityClassMapping,
    ToolFeatureMethodParameterDefinitionMapping,
    ToolFeatureMethodMethodMapping,
    ObjectGroupMapping,
    ParameterValueListMapping,
)


def import_mapping_from_dict(map_dict):
    """Creates Mapping object from a dict"""
    if not isinstance(map_dict, dict):
        raise TypeError(f"map_dict must be a dict, instead it was: {type(map_dict)}")
    map_type = map_dict.get("map_type")
    legacy_mapping_from_dict = {
        "ObjectClass": _object_class_mapping_from_dict,
        "RelationshipClass": _relationship_class_mapping_from_dict,
        "Alternative": _alternative_mapping_from_dict,
        "Scenario": _scenario_mapping_from_dict,
        "ScenarioAlternative": _scenario_alternative_mapping_from_dict,
        "Tool": _tool_mapping_from_dict,
        "Feature": _feature_mapping_from_dict,
        "ToolFeature": _tool_feature_mapping_from_dict,
        "ToolFeatureMethod": _tool_feature_method_mapping_from_dict,
        "ObjectGroup": _object_group_mapping_from_dict,
        # ParameterValueList,
    }
    from_dict = legacy_mapping_from_dict.get(map_type)
    if from_dict is not None:
        return from_dict(map_dict)
    raise ValueError(f'invalid "map_type" value, expected any of {", ".join(legacy_mapping_from_dict)}, got {map_type}')


def _alternative_mapping_from_dict(map_dict):
    name = map_dict.get("name")
    skip_columns = map_dict.get("skip_columns", [])
    read_start_row = map_dict.get("read_start_row", 0)
    root_mapping = AlternativeMapping(*_pos_and_val(name), skip_columns=skip_columns, read_start_row=read_start_row)
    return root_mapping


def _scenario_mapping_from_dict(map_dict):
    name = map_dict.get("name")
    skip_columns = map_dict.get("skip_columns", [])
    read_start_row = map_dict.get("read_start_row", 0)
    active = map_dict.get("active", "false")
    root_mapping = ScenarioMapping(*_pos_and_val(name), skip_columns=skip_columns, read_start_row=read_start_row)
    root_mapping.child = ScenarioActiveFlagMapping(*_pos_and_val(active))
    return root_mapping


def _scenario_alternative_mapping_from_dict(map_dict):
    scenario_name = map_dict.get("scenario_name")
    alternative_name = map_dict.get("alternative_name")
    before_alternative_name = map_dict.get("before_alternative_name")
    skip_columns = map_dict.get("skip_columns", [])
    read_start_row = map_dict.get("read_start_row", 0)
    root_mapping = ScenarioMapping(
        *_pos_and_val(scenario_name), skip_columns=skip_columns, read_start_row=read_start_row
    )
    scen_alt_mapping = root_mapping.child = ScenarioAlternativeMapping(*_pos_and_val(alternative_name))
    scen_alt_mapping.child = ScenarioBeforeAlternativeMapping(*_pos_and_val(before_alternative_name))
    return root_mapping


def _tool_mapping_from_dict(map_dict):
    name = map_dict.get("name")
    skip_columns = map_dict.get("skip_columns", [])
    read_start_row = map_dict.get("read_start_row", 0)
    root_mapping = ToolMapping(*_pos_and_val(name), skip_columns=skip_columns, read_start_row=read_start_row)
    return root_mapping


def _feature_mapping_from_dict(map_dict):
    entity_class_name = map_dict.get("entity_class_name")
    parameter_definition_name = map_dict.get("parameter_definition_name")
    skip_columns = map_dict.get("skip_columns", [])
    read_start_row = map_dict.get("read_start_row", 0)
    root_mapping = FeatureEntityClassMapping(
        *_pos_and_val(entity_class_name), skip_columns=skip_columns, read_start_row=read_start_row
    )
    root_mapping.child = FeatureParameterDefinitionMapping(*_pos_and_val(parameter_definition_name))
    return root_mapping


def _tool_feature_mapping_from_dict(map_dict):
    name = map_dict.get("name")
    entity_class_name = map_dict.get("entity_class_name")
    parameter_definition_name = map_dict.get("parameter_definition_name")
    required = map_dict.get("required", "false")
    skip_columns = map_dict.get("skip_columns", [])
    read_start_row = map_dict.get("read_start_row", 0)
    root_mapping = ToolMapping(*_pos_and_val(name), skip_columns=skip_columns, read_start_row=read_start_row)
    root_mapping.child = ent_class_mapping = ToolFeatureEntityClassMapping(*_pos_and_val(entity_class_name))
    ent_class_mapping.child = param_def_mapping = ToolFeatureParameterDefinitionMapping(
        *_pos_and_val(parameter_definition_name)
    )
    param_def_mapping.child = ToolFeatureRequiredFlagMapping(*_pos_and_val(required))
    return root_mapping


def _tool_feature_method_mapping_from_dict(map_dict):
    name = map_dict.get("name")
    entity_class_name = map_dict.get("entity_class_name")
    parameter_definition_name = map_dict.get("parameter_definition_name")
    method = map_dict.get("method")
    skip_columns = map_dict.get("skip_columns", [])
    read_start_row = map_dict.get("read_start_row", 0)
    root_mapping = ToolMapping(*_pos_and_val(name), skip_columns=skip_columns, read_start_row=read_start_row)
    root_mapping.child = ent_class_mapping = ToolFeatureMethodEntityClassMapping(*_pos_and_val(entity_class_name))
    ent_class_mapping.child = param_def_mapping = ToolFeatureMethodParameterDefinitionMapping(
        *_pos_and_val(parameter_definition_name)
    )
    param_def_mapping.child = ToolFeatureMethodMethodMapping(*_pos_and_val(method))
    return root_mapping


def _object_class_mapping_from_dict(map_dict):
    name = map_dict.get("name")
    objects = map_dict.get("objects", map_dict.get("object"))
    skip_columns = map_dict.get("skip_columns", [])
    read_start_row = map_dict.get("read_start_row", 0)
    root_mapping = ObjectClassMapping(*_pos_and_val(name), skip_columns=skip_columns, read_start_row=read_start_row)
    object_mapping = root_mapping.child = ObjectMapping(*_pos_and_val(objects))
    parameters = map_dict.get("parameters")
    object_mapping.child = _parameter_mapping_from_dict(parameters)
    return root_mapping
    # FIXME: We need to handle this below too:
    # object_metadata = map_dict.get("object_metadata", None)


def _object_group_mapping_from_dict(map_dict):
    name = map_dict.get("name")
    groups = map_dict.get("groups")
    members = map_dict.get("members")
    import_objects = map_dict.get("import_objects", False)
    skip_columns = map_dict.get("skip_columns", [])
    read_start_row = map_dict.get("read_start_row", 0)
    root_mapping = ObjectClassMapping(*_pos_and_val(name), skip_columns=skip_columns, read_start_row=read_start_row)
    object_mapping = root_mapping.child = ObjectMapping(*_pos_and_val(groups))
    group_mapping = object_mapping.child = ObjectGroupMapping(*_pos_and_val(members), import_objects=import_objects)
    parameters = map_dict.get("parameters")
    group_mapping.child = _parameter_mapping_from_dict(parameters)
    return root_mapping


def _relationship_class_mapping_from_dict(map_dict):
    name = map_dict.get("name")
    objects = map_dict.get("objects", [None])
    object_classes = map_dict.get("object_classes", [None])
    import_objects = map_dict.get("import_objects", False)
    skip_columns = map_dict.get("skip_columns", [])
    read_start_row = map_dict.get("read_start_row", 0)
    root_mapping = RelationshipClassMapping(
        *_pos_and_val(name), skip_columns=skip_columns, read_start_row=read_start_row
    )
    parent_mapping = root_mapping
    for klass in object_classes:
        class_mapping = RelationshipClassObjectClassMapping(*_pos_and_val(klass))
        parent_mapping.child = class_mapping
        parent_mapping = class_mapping
    relationship_mapping = parent_mapping.child = RelationshipMapping(Position.hidden, value="relationship")
    parent_mapping = relationship_mapping
    for obj in objects:
        object_mapping = RelationshipObjectMapping(*_pos_and_val(obj), import_objects=import_objects)
        parent_mapping.child = object_mapping
        parent_mapping = object_mapping
    parameters = map_dict.get("parameters")
    parent_mapping.child = _parameter_mapping_from_dict(parameters)
    return root_mapping
    # FIXME
    # relationship_metadata = map_dict.get("relationship_metadata", None)


def _parameter_mapping_from_dict(map_dict):
    if map_dict is None:
        return None
    map_type = map_dict.get("map_type")
    if map_type == "parameter" or "parameter_type" in map_dict:
        _fix_parameter_mapping_dict(map_dict)
    map_type = map_dict.get("map_type")
    if map_type == "None":
        return None
    param_def_mapping = ParameterDefinitionMapping(*_pos_and_val(map_dict["name"]))
    if map_type == "ParameterDefinition":
        default_value_dict = map_dict.get("default_value")
        value_list_name = map_dict.get("parameter_value_list_name")
        param_def_mapping.child = default_value_mapping = _parameter_default_value_mapping(default_value_dict)
        default_value_mapping.child = ParameterValueListMapping(*_pos_and_val(value_list_name))
        return param_def_mapping
    param_def_mapping.child = _parameter_value_mapping(map_dict["value"])
    return param_def_mapping


def _parameter_value_mapping(value_dict):
    value_type = value_dict["value_type"]
    if value_type == "single value":
        return ParameterValueMapping(*_pos_and_val(value_dict["main_value"]))
    extra_dimensions = value_dict.get("extra_dimensions", [None])
    compress = value_dict.get("compress", False)
    value_type = value_type.replace(" ", "_")
    root_mapping = ParameterValueTypeMapping(Position.hidden, value_type, compress=compress)
    parent_mapping = root_mapping
    for ed in extra_dimensions:
        mapping = ParameterValueIndexMapping(*_pos_and_val(ed))
        parent_mapping.child = mapping
        parent_mapping = mapping
    parent_mapping.child = ExpandedParameterValueMapping(*_pos_and_val(value_dict["main_value"]))
    return root_mapping


def _parameter_default_value_mapping(default_value_dict):
    if default_value_dict is None:
        return ParameterDefaultValueMapping(*_pos_and_val(None))
    value_type = default_value_dict["value_type"]
    if value_type == "single value":
        return ParameterDefaultValueMapping(*_pos_and_val(default_value_dict["main_value"]))
    extra_dimensions = default_value_dict.get("extra_dimensions", [None])
    compress = default_value_dict.get("compress", False)
    value_type = value_type.replace(" ", "_")
    root_mapping = ParameterValueTypeMapping(Position.hidden, value_type, compress=compress)
    parent_mapping = root_mapping
    for ed in extra_dimensions:
        mapping = ParameterValueIndexMapping(*_pos_and_val(ed))
        parent_mapping.child = mapping
        parent_mapping = mapping
    parent_mapping.child = ExpandedParameterValueMapping(*_pos_and_val(default_value_dict["main_value"]))
    return root_mapping


def _fix_parameter_mapping_dict(map_dict):
    # Even deeper legacy
    parameter_type = map_dict.pop("parameter_type", None)
    if parameter_type == "definition":
        map_dict["map_type"] = "ParameterDefinition"
    else:
        value_dict = map_dict.copy()
        value_dict.pop("name", None)
        value_dict["value_type"] = parameter_type if parameter_type else "single value"
        value_dict["main_value"] = value_dict.pop("value", None)
        map_dict["map_type"] = "ParameterValue"
        map_dict["value"] = value_dict


def _pos_and_val(x):
    if not isinstance(x, dict):
        map_type = "constant" if isinstance(x, str) else "column"
        map_dict = {"map_type": map_type, "reference": x}
    else:
        map_dict = x
    map_type = map_dict.get("map_type")
    ref = map_dict.get("reference", map_dict.get("value_reference"))
    if isinstance(ref, str) and not ref:
        ref = None
    # None, or invalid reference
    if map_type == "None" or ref is None:
        return Position.hidden, None  # This combination disables the mapping
    # Constant
    if map_type == "constant":
        if not isinstance(ref, str):
            raise TypeError(f"Constant reference must be str, instead got: {type(ref).__name__}")
        return Position.hidden, ref
    # Table name
    if map_type == "table_name":
        return Position.table_name, None
    # Row or column reference, including header
    if not isinstance(ref, (str, int)):
        raise TypeError(f"Row or column reference must be str or int, instead got: {type(ref).__name__}")
    # 1. Column header
    if map_type in ("column_name", "column_header"):
        if isinstance(ref, int) and ref < 0:
            ref = 0
        return Position.header, ref
    # 2. Data row or column
    try:
        ref = int(ref)
    except ValueError:
        pass
    # 2a. Column
    if map_type == "column":
        if isinstance(ref, int) and ref < 0:
            ref = 0
        return ref, None
    # 2b. Row
    if map_type == "row":
        if isinstance(ref, int):
            if ref == -1:
                return Position.header, None
            if ref < -1:
                ref = 0
            return -(ref + 1), None  # pylint: disable=invalid-unary-operand-type
        if ref.lower() == "header":
            return Position.header, None
        raise ValueError(f"If row reference is str, it must be 'header'. Instead got '{ref}'")
    # Fallback to invalid
    return Position.hidden, None
