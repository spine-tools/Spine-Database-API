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

""" Functions for creating import mappings from dicts. """
from .import_mapping import (
    Position,
    EntityClassMapping,
    DimensionMapping,
    EntityMapping,
    EntityMetadataMapping,
    ElementMapping,
    ParameterDefinitionMapping,
    ParameterDefaultValueMapping,
    ParameterDefaultValueTypeMapping,
    ParameterDefaultValueIndexMapping,
    ExpandedParameterDefaultValueMapping,
    ParameterValueMapping,
    ParameterValueTypeMapping,
    ParameterValueMetadataMapping,
    ParameterValueIndexMapping,
    ExpandedParameterValueMapping,
    AlternativeMapping,
    ScenarioMapping,
    ScenarioActiveFlagMapping,
    ScenarioAlternativeMapping,
    ScenarioBeforeAlternativeMapping,
    EntityGroupMapping,
    ParameterValueListMapping,
    ParameterValueListValueMapping,
    from_dict as mapping_from_dict,
    IndexNameMapping,
    DefaultValueIndexNameMapping,
)
from ..mapping import to_dict as import_mapping_to_dict


def parse_named_mapping_spec(named_mapping_spec):
    if len(named_mapping_spec) == 1:
        name, mapping_spec = next(iter(named_mapping_spec.items()))
        mapping = mapping_spec["mapping"]
    else:
        # Legacy
        name = named_mapping_spec.get("mapping_name", "")
        mapping = named_mapping_spec
    return name, import_mapping_from_dict(mapping)


def unparse_named_mapping_spec(name, root_mapping):
    return {name: {"mapping": import_mapping_to_dict(root_mapping)}}


def import_mapping_from_dict(map_dict):
    """Creates Mapping object from a dict"""
    if isinstance(map_dict, list):
        # New system, flattened mapping as list
        return mapping_from_dict(map_dict)
    # Compatibility system, plain dict
    if not isinstance(map_dict, dict):
        raise TypeError(f"map_dict must be a dict, instead it was: {type(map_dict)}")
    map_type = map_dict.get("map_type")
    legacy_mapping_from_dict = {
        "ObjectClass": _object_class_mapping_from_dict,
        "RelationshipClass": _relationship_class_mapping_from_dict,
        "Alternative": _alternative_mapping_from_dict,
        "Scenario": _scenario_mapping_from_dict,
        "ScenarioAlternative": _scenario_alternative_mapping_from_dict,
        "ObjectGroup": _object_group_mapping_from_dict,
        "ParameterValueList": _parameter_value_list_mapping_from_dict,
    }
    from_dict = legacy_mapping_from_dict.get(map_type)
    if from_dict is not None:
        return from_dict(map_dict)
    obsolete_types = ("Tool", "Feature", "ToolFeature", "ToolFeatureMethod")
    invalid = "obsolete" if map_type in obsolete_types else "unknown"
    raise ValueError(
        f'{invalid} "map_type" value, expected any of {", ".join(legacy_mapping_from_dict)}, got {map_type}'
    )


def _parameter_value_list_mapping_from_dict(map_dict):
    name = map_dict.get("name")
    value = map_dict.get("value")
    skip_columns = map_dict.get("skip_columns", [])
    read_start_row = map_dict.get("read_start_row", 0)
    root_mapping = ParameterValueListMapping(
        *_pos_and_val(name), skip_columns=skip_columns, read_start_row=read_start_row
    )
    root_mapping.child = ParameterValueListValueMapping(*_pos_and_val(value))
    return root_mapping


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


def _object_class_mapping_from_dict(map_dict):
    name = map_dict.get("name")
    objects = map_dict.get("objects", map_dict.get("object"))
    object_metadata = map_dict.get("object_metadata", None)
    parameters = map_dict.get("parameters")
    skip_columns = map_dict.get("skip_columns", [])
    read_start_row = map_dict.get("read_start_row", 0)
    root_mapping = EntityClassMapping(*_pos_and_val(name), skip_columns=skip_columns, read_start_row=read_start_row)
    object_mapping = root_mapping.child = EntityMapping(*_pos_and_val(objects))
    object_metadata_mapping = object_mapping.child = EntityMetadataMapping(*_pos_and_val(object_metadata))
    object_metadata_mapping.child = parameter_mapping_from_dict(parameters)
    return root_mapping


def _object_group_mapping_from_dict(map_dict):
    name = map_dict.get("name")
    groups = map_dict.get("groups")
    members = map_dict.get("members")
    import_entities = map_dict.get("import_objects", False)
    skip_columns = map_dict.get("skip_columns", [])
    read_start_row = map_dict.get("read_start_row", 0)
    root_mapping = EntityClassMapping(*_pos_and_val(name), skip_columns=skip_columns, read_start_row=read_start_row)
    object_mapping = root_mapping.child = EntityMapping(*_pos_and_val(groups))
    object_mapping.child = EntityGroupMapping(*_pos_and_val(members), import_entities=import_entities)
    return root_mapping


def _relationship_class_mapping_from_dict(map_dict):
    name = map_dict.get("name")
    objects = map_dict.get("objects")
    if objects is None:
        objects = [None]
    object_classes = map_dict.get("object_classes")
    if object_classes is None:
        object_classes = [None]
    relationship_metadata = map_dict.get("relationship_metadata")
    parameters = map_dict.get("parameters")
    import_entities = map_dict.get("import_objects", False)
    skip_columns = map_dict.get("skip_columns", [])
    read_start_row = map_dict.get("read_start_row", 0)
    root_mapping = EntityClassMapping(*_pos_and_val(name), skip_columns=skip_columns, read_start_row=read_start_row)
    parent_mapping = root_mapping
    for klass in object_classes:
        class_mapping = DimensionMapping(*_pos_and_val(klass))
        parent_mapping.child = class_mapping
        parent_mapping = class_mapping
    relationship_mapping = parent_mapping.child = EntityMapping(Position.hidden)
    parent_mapping = relationship_mapping
    for obj in objects:
        object_mapping = ElementMapping(*_pos_and_val(obj), import_entities=import_entities)
        parent_mapping.child = object_mapping
        parent_mapping = object_mapping
    relationship_metadata_mapping = parent_mapping.child = EntityMetadataMapping(*_pos_and_val(relationship_metadata))
    relationship_metadata_mapping.child = parameter_mapping_from_dict(parameters)
    return root_mapping


def parameter_mapping_from_dict(map_dict):
    if map_dict is None:
        return None
    map_type = map_dict.get("map_type")
    if map_type == "parameter" or "parameter_type" in map_dict:
        _fix_parameter_mapping_dict(map_dict)
    map_type = map_dict.get("map_type")
    if map_type == "None":
        return None
    param_def_mapping = ParameterDefinitionMapping(*_pos_and_val(map_dict.get("name")))
    if map_type == "ParameterDefinition":
        default_value_dict = map_dict.get("default_value")
        value_list_name = map_dict.get("parameter_value_list_name")
        param_def_mapping.child = value_list_mapping = ParameterValueListMapping(*_pos_and_val(value_list_name))
        value_list_mapping.child = parameter_default_value_mapping_from_dict(default_value_dict)
        return param_def_mapping
    alternative_name = map_dict.get("alternative_name")
    parameter_value_metadata = map_dict.get("parameter_value_metadata")
    param_def_mapping.child = alt_mapping = AlternativeMapping(*_pos_and_val(alternative_name))
    alt_mapping.child = param_val_metadata_mapping = ParameterValueMetadataMapping(
        *_pos_and_val(parameter_value_metadata)
    )
    param_val_metadata_mapping.child = parameter_value_mapping_from_dict(map_dict.get("value"))
    return param_def_mapping


def parameter_default_value_mapping_from_dict(default_value_dict):
    if default_value_dict is None:
        return ParameterDefaultValueMapping(*_pos_and_val(None))
    value_type = default_value_dict["value_type"].replace(" ", "_")
    main_value = default_value_dict.get("main_value")
    if value_type == "single_value":
        return ParameterDefaultValueMapping(*_pos_and_val(main_value))
    extra_dimensions = default_value_dict.get("extra_dimensions", [None])
    compress = default_value_dict.get("compress", False)
    options = default_value_dict.get("options", {})
    root_mapping = ParameterDefaultValueTypeMapping(Position.hidden, value_type, compress=compress, options=options)
    parent_mapping = root_mapping
    for ed in extra_dimensions:
        name_mapping = DefaultValueIndexNameMapping(Position.hidden, value=None)
        parent_mapping.child = name_mapping
        index_mapping = ParameterDefaultValueIndexMapping(*_pos_and_val(ed))
        name_mapping.child = index_mapping
        parent_mapping = index_mapping
    parent_mapping.child = ExpandedParameterDefaultValueMapping(*_pos_and_val(main_value))
    return root_mapping


def parameter_value_mapping_from_dict(value_dict):
    if value_dict is None:
        return ParameterValueMapping(*_pos_and_val(None))
    value_type = value_dict["value_type"].replace(" ", "_")
    main_value = value_dict.get("main_value")
    if value_type == "single_value":
        return ParameterValueMapping(*_pos_and_val(main_value))
    extra_dimensions = value_dict.get("extra_dimensions", [None])
    compress = value_dict.get("compress", False)
    options = value_dict.get("options", {})
    root_mapping = ParameterValueTypeMapping(Position.hidden, value_type, compress=compress, options=options)
    parent_mapping = root_mapping
    for ed in extra_dimensions:
        name_mapping = IndexNameMapping(Position.hidden, value=None)
        parent_mapping.child = name_mapping
        index_mapping = ParameterValueIndexMapping(*_pos_and_val(ed))
        name_mapping.child = index_mapping
        parent_mapping = index_mapping
    parent_mapping.child = ExpandedParameterValueMapping(*_pos_and_val(main_value))
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
