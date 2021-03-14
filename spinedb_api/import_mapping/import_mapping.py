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
Contains import mappings for database items such as entities, entity classes and parameter values.

:author: A. Soininen (VTT)
:date:   10.12.2020
"""

from distutils.util import strtobool
from enum import auto, Enum, unique
from spinedb_api.mapping import Mapping, Position, unflatten, is_pivoted
from spinedb_api.exception import InvalidMapping


@unique
class ImportKey(Enum):
    OBJECT_CLASS_NAME = auto()
    OBJECT_NAME = auto()
    PARAMETER_NAME = auto()
    PARAMETER_DEFINITION = auto()
    PARAMETER_DEFAULT_VALUES = auto()
    PARAMETER_DEFAULT_VALUE_INDEXES = auto()
    PARAMETER_VALUES = auto()
    PARAMETER_VALUE_INDEXES = auto()
    RELATIONSHIP_CLASS_NAME = auto()
    OBJECT_CLASS_NAMES = auto()
    OBJECT_NAMES = auto()
    ALTERNATIVE_NAME = auto()
    SCENARIO_NAME = auto()
    SCENARIO_ALTERNATIVE = auto()
    FEATURE = auto()
    TOOL_NAME = auto()
    TOOL_FEATURE = auto()
    TOOL_FEATURE_METHOD = auto()
    PARAMETER_VALUE_LIST_NAME = auto()


class ImportMapping(Mapping):
    def __init__(self, position, value=None, skip_columns=None, read_start_row=0):
        super().__init__(position, value=value)
        self._skip_columns = None
        self._read_start_row = None
        self.skip_columns = skip_columns
        self.read_start_row = read_start_row

    @property
    def skip_columns(self):
        return self._skip_columns

    @skip_columns.setter
    def skip_columns(self, skip_columns=None):
        if skip_columns is None:
            self._skip_columns = []
            return
        if isinstance(skip_columns, (str, int)):
            self._skip_columns = [skip_columns]
            return
        if isinstance(skip_columns, list):
            i = next(iter(i for i, column in enumerate(skip_columns) if not isinstance(column, (str, int))), None)
            if i is not None:
                raise TypeError(
                    "skip_columns must be str, int or list of str, int, "
                    f"instead got list with {type(skip_columns[i]).__name__} on index {i}"
                )
            self._skip_columns = skip_columns
            return
        raise TypeError(f"skip_columns must be str, int or list of str, int, instead got {type(skip_columns).__name__}")

    @property
    def read_start_row(self):
        return self._read_start_row

    @read_start_row.setter
    def read_start_row(self, row):
        if not isinstance(row, int):
            raise TypeError(f"row must be int, instead got {type(row).__name__}")
        if row < 0:
            raise ValueError(f"row must be >= 0 ({row})")
        self._read_start_row = row

    def polish(self, table_name, source_header):
        """Polishes the mapping before an import operation.
        'Expands' transient ``position`` and ``value`` attributes into their final value.

        Args:
            table_name (str)
            source_header (list(str))
        """
        # FIXME: Polish skip columns
        if self.child is not None:
            self.child.polish(table_name, source_header)
        if isinstance(self.position, str):
            # Column mapping with string position, we need to find the index in the header
            try:
                self.position = source_header.index(self.position)
                return
            except ValueError:
                raise InvalidMapping(f"'{self.position}' is not in '{source_header}'")
        if self.position == Position.table_name:
            # Table name mapping, we set the fixed value to the table name
            self.value = table_name
            return
        if self.position == Position.header:
            if self.value is None:
                # Row mapping from header, we handle this one separately
                return
            # Column header mapping, the value indicates which field
            if isinstance(self.value, str):
                # If the value is indeed in the header, we're good
                if self.value in source_header:
                    return
                try:
                    # Not in the header, maybe it's a stringified index?
                    self.value = int(self.value)
                except (ValueError, IndexError):
                    raise InvalidMapping(f"'{self.value}' is not in header '{source_header}'")
            # Integer value, we try and get the actual value from that index in the header
            try:
                self.value = source_header[self.value]
            except IndexError:
                raise InvalidMapping(f"'{self.value}' is not a valid index in header '{source_header}'")

    def import_row(self, source_row, state, mapped_data):
        if self.position != Position.hidden or self.value is not None:
            source_data = self._data(source_row)
            if source_data is not None:
                self._import_row(source_data, state, mapped_data)
        if self.child is not None:
            self.child.import_row(source_row, state, mapped_data)

    def _data(self, source_row):
        if source_row is None:
            return None
        return source_row[self.position]

    def _import_row(self, source_data, state, mapped_data):
        raise NotImplementedError()

    def is_constant(self):
        return self.position == Position.hidden and self.value is not None

    def is_none(self):
        return self.position == Position.hidden and self.value is None

    def is_pivoted(self):
        if is_pivoted(self.position):
            return True
        if self.position == Position.header and self.value is None:
            return True
        if self.child is None:
            return False
        return self.child.is_pivoted()

    @classmethod
    def reconstruct(cls, position, mapping_dict):
        """
        Reconstructs mapping.

        Args:
            position (int or Position, optional): mapping's position
            mapping_dict (dict): serialized mapping

        Returns:
            Mapping: reconstructed mapping
        """
        value = mapping_dict.get("value")
        skip_columns = mapping_dict.get("skip_columns")
        read_start_row = mapping_dict.get("read_start_row", 0)
        mapping = cls(position, value=value, skip_columns=skip_columns, read_start_row=read_start_row)
        return mapping


class ImportObjectsMixin:
    def __init__(self, position, value=None, skip_columns=None, read_start_row=0, import_objects=False):
        super().__init__(position, value, skip_columns, read_start_row)
        self.import_objects = import_objects

    def to_dict(self):
        d = super().to_dict()
        if self.import_objects:
            d["import_objects"] = True
        return d

    @classmethod
    def reconstruct(cls, position, mapping_dict):
        value = mapping_dict.get("value")
        skip_columns = mapping_dict.get("skip_columns")
        read_start_row = mapping_dict.get("read_start_row", 0)
        import_objects = mapping_dict.get("import_objects", False)
        mapping = cls(
            position,
            value=value,
            skip_columns=skip_columns,
            read_start_row=read_start_row,
            import_objects=import_objects,
        )
        return mapping


class IndexedValueMixin:
    def __init__(self, position, value=None, skip_columns=None, read_start_row=0, compress=False, options=None):
        super().__init__(position, value, skip_columns, read_start_row)
        if options is None:
            options = {}
        self.compress = compress
        self.options = options

    def to_dict(self):
        d = super().to_dict()
        if self.compress:
            d["compress"] = True
        if self.options:
            d["options"] = self.options
        return d

    @classmethod
    def reconstruct(cls, position, mapping_dict):
        value = mapping_dict.get("value")
        skip_columns = mapping_dict.get("skip_columns")
        read_start_row = mapping_dict.get("read_start_row", 0)
        compress = mapping_dict.get("compress", False)
        options = mapping_dict.get("options")
        mapping = cls(
            position,
            value=value,
            skip_columns=skip_columns,
            read_start_row=read_start_row,
            compress=compress,
            options=options,
        )
        return mapping


class ObjectClassMapping(ImportMapping):
    """Maps object classes.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "ObjectClass"

    def _import_row(self, source_data, state, mapped_data):
        object_class_name = state[ImportKey.OBJECT_CLASS_NAME] = source_data
        object_classes = mapped_data.setdefault("object_classes", list())
        object_classes.append(object_class_name)


class ObjectMapping(ImportMapping):
    """Maps objects.

    Cannot be used as the topmost mapping; one of the parents must be :class:`ObjectClassMapping`.
    """

    MAP_TYPE = "Object"

    def _import_row(self, source_data, state, mapped_data):
        object_class_name = state[ImportKey.OBJECT_CLASS_NAME]
        object_name = state[ImportKey.OBJECT_NAME] = source_data
        if not isinstance(self.child, ObjectGroupMapping):
            mapped_data.setdefault("objects", list()).append((object_class_name, object_name))


class ObjectMetadataMapping(ImportMapping):
    """Maps object metadata.

    Cannot be used as the topmost mapping; must have :class:`ObjectClassMapping` and :class:`ObjectMapping` as parents.
    """

    MAP_TYPE = "ObjectMetadata"

    def _import_row(self, source_data, state, mapped_data):
        pass


class ObjectGroupMapping(ImportObjectsMixin, ImportMapping):
    """Maps object groups.

    Cannot be used as the topmost mapping; must have :class:`ObjectClassMapping` and :class:`ObjectMapping` as parents.
    """

    MAP_TYPE = "ObjectGroup"

    def _import_row(self, source_data, state, mapped_data):
        object_class_name = state[ImportKey.OBJECT_CLASS_NAME]
        group_name = state[ImportKey.OBJECT_NAME]
        member_name = source_data
        mapped_data.setdefault("object_groups", list()).append((object_class_name, group_name, member_name))
        if self.import_objects:
            objects = [(object_class_name, group_name), (object_class_name, member_name)]
            mapped_data.setdefault("objects", list()).extend(objects)


class RelationshipClassMapping(ImportMapping):
    """Maps relationships classes.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "RelationshipClass"

    def _import_row(self, source_data, state, mapped_data):
        relationship_class_name = state[ImportKey.RELATIONSHIP_CLASS_NAME] = source_data
        object_class_names = state[ImportKey.OBJECT_CLASS_NAMES] = []
        relationship_classes = mapped_data.setdefault("relationship_classes", list())
        relationship_classes.append((relationship_class_name, object_class_names))


class RelationshipClassObjectClassMapping(ImportMapping):
    """Maps relationship class object classes.

    Cannot be used as the topmost mapping; one of the parents must be :class:`RelationshipClassMapping`.
    """

    MAP_TYPE = "RelationshipClassObjectClass"

    def _import_row(self, source_data, state, mapped_data):
        object_class_names = state[ImportKey.OBJECT_CLASS_NAMES]
        object_class_name = source_data
        object_class_names.append(object_class_name)


class RelationshipMapping(ImportMapping):
    """Maps relationships.

    Cannot be used as the topmost mapping; one of the parents must be :class:`RelationshipClassMapping`.
    """

    MAP_TYPE = "Relationship"

    def _import_row(self, source_data, state, mapped_data):
        relationship_class_name = state[ImportKey.RELATIONSHIP_CLASS_NAME]
        object_names = state[ImportKey.OBJECT_NAMES] = []
        relationships = mapped_data.setdefault("relationships", list())
        relationships.append((relationship_class_name, object_names))


class RelationshipObjectMapping(ImportObjectsMixin, ImportMapping):
    """Maps relationship's objects.

    Cannot be used as the topmost mapping; must have :class:`RelationshipClassMapping` and :class:`RelationshipMapping`
    as parents.
    """

    MAP_TYPE = "RelationshipObject"

    def _import_row(self, source_data, state, mapped_data):
        object_names = state[ImportKey.OBJECT_NAMES]
        object_name = source_data
        object_names.append(object_name)
        if self.import_objects:
            object_class_names = state[ImportKey.OBJECT_CLASS_NAMES]
            k = len(object_names) - 1
            object_class_name = object_class_names[k]
            mapped_data.setdefault("object_classes", list()).append(object_class_name)
            mapped_data.setdefault("objects", list()).append([object_class_name, object_name])


class RelationshipMetadataMapping(ImportMapping):
    """Maps relationship metadata.

    Cannot be used as the topmost mapping; must have :class:`RelationshipClassMapping`, a :class:`RelationshipMapping`
    and one or more :class:`RelationshipObjectMapping` as parents.
    """

    MAP_TYPE = "RelationshipMetadata"

    def _import_row(self, source_data, state, mapped_data):
        pass


class ParameterDefinitionMapping(ImportMapping):
    """Maps parameter definitions.

    Cannot be used as the topmost mapping; must have an entity class mapping as one of parents.
    """

    MAP_TYPE = "ParameterDefinition"

    def _import_row(self, source_data, state, mapped_data):
        object_class_name = state.get(ImportKey.OBJECT_CLASS_NAME)
        relationship_class_name = state.get(ImportKey.RELATIONSHIP_CLASS_NAME)
        if object_class_name:
            class_name, map_key = object_class_name, "object_parameters"
        elif relationship_class_name:
            class_name, map_key = relationship_class_name, "relationship_parameters"
        parameter_name = state[ImportKey.PARAMETER_NAME] = source_data
        parameter_definition = state[ImportKey.PARAMETER_DEFINITION] = [class_name, parameter_name]
        mapped_data.setdefault(map_key, list()).append(parameter_definition)


class ParameterDefaultValueMapping(ImportMapping):
    """Maps scalar (non-indexed) default values

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping` as parent.
    """

    MAP_TYPE = "ParameterDefaultValue"

    def _import_row(self, source_data, state, mapped_data):
        parameter_definition = state[ImportKey.PARAMETER_DEFINITION]
        parameter_definition.append(source_data)
        value_list_name = state.get(ImportKey.PARAMETER_VALUE_LIST_NAME)
        if value_list_name:
            parameter_definition.append(value_list_name)


class ParameterDefaultValueTypeMapping(IndexedValueMixin, ImportMapping):
    MAP_TYPE = "ParameterDefaultValueType"

    def _import_row(self, source_data, state, mapped_data):
        parameter_definition = state[ImportKey.PARAMETER_DEFINITION]
        default_values = state.setdefault(ImportKey.PARAMETER_DEFAULT_VALUES, {})
        key = tuple(parameter_definition)
        if key in default_values:
            return
        value_type = source_data
        default_value = default_values[key] = {"type": value_type}
        if self.compress and value_type == "map":
            default_value["compress"] = self.compress
        parameter_definition.append(default_value)
        value_list_name = state.get(ImportKey.PARAMETER_VALUE_LIST_NAME)
        if value_list_name:
            parameter_definition.append(value_list_name)


class ParameterDefaultValueIndexMapping(ImportMapping):
    """Maps default value indexes.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping` as parent.
    """

    MAP_TYPE = "ParameterDefaultValueIndex"

    def _import_row(self, source_data, state, mapped_data):
        index = source_data
        state.setdefault(ImportKey.PARAMETER_DEFAULT_VALUE_INDEXES, []).append(index)


class ExpandedParameterDefaultValueMapping(ImportMapping):
    """Maps indexed default values.

    Whenever this mapping is a child of :class:`ParameterDefaultValueIndexMapping`, it maps individual values of
    indexed parameters.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping` as parent.
    """

    MAP_TYPE = "ExpandedDefaultValue"

    def _import_row(self, source_data, state, mapped_data):
        object_class_name = state.get(ImportKey.OBJECT_CLASS_NAME)
        relationship_class_name = state.get(ImportKey.RELATIONSHIP_CLASS_NAME)
        if object_class_name:
            class_name = object_class_name
        elif relationship_class_name:
            class_name = relationship_class_name
        parameter_name = state[ImportKey.PARAMETER_NAME]
        key = (class_name, parameter_name)
        values = state.setdefault(ImportKey.PARAMETER_DEFAULT_VALUES, {})
        value = values[key]
        val = source_data
        data = value.setdefault("data", [])
        if value["type"] == "array":
            data.append(val)
            return
        indexes = state.pop(ImportKey.PARAMETER_DEFAULT_VALUE_INDEXES, None)
        data.append(indexes + [val])


class ParameterValueMapping(ImportMapping):
    """Maps scalar (non-indexed) parameter values.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping`, an entity mapping and
    an :class:`AlternativeMapping` as parents.
    """

    MAP_TYPE = "ParameterValue"

    def _import_row(self, source_data, state, mapped_data):
        object_class_name = state.get(ImportKey.OBJECT_CLASS_NAME)
        relationship_class_name = state.get(ImportKey.RELATIONSHIP_CLASS_NAME)
        if object_class_name:
            class_name, entity_name, map_key = (
                object_class_name,
                state[ImportKey.OBJECT_NAME],
                "object_parameter_values",
            )
        elif relationship_class_name:
            class_name, entity_name, map_key = (
                relationship_class_name,
                state[ImportKey.OBJECT_NAMES],
                "relationship_parameter_values",
            )
        parameter_name = state[ImportKey.PARAMETER_NAME]
        parameter_value = [class_name, entity_name, parameter_name, source_data]
        alternative_name = state.get(ImportKey.ALTERNATIVE_NAME)
        if alternative_name is not None:
            parameter_value.append(alternative_name)
        mapped_data.setdefault(map_key, list()).append(parameter_value)


class ParameterValueTypeMapping(IndexedValueMixin, ImportMapping):
    MAP_TYPE = "ParameterValueType"

    def _import_row(self, source_data, state, mapped_data):
        object_class_name = state.get(ImportKey.OBJECT_CLASS_NAME)
        relationship_class_name = state.get(ImportKey.RELATIONSHIP_CLASS_NAME)
        parameter_name = state[ImportKey.PARAMETER_NAME]
        values = state.setdefault(ImportKey.PARAMETER_VALUES, {})
        if object_class_name:
            class_name, entity_name, map_key = (
                object_class_name,
                state[ImportKey.OBJECT_NAME],
                "object_parameter_values",
            )
        elif relationship_class_name:
            class_name, entity_name, map_key = (
                relationship_class_name,
                tuple(state[ImportKey.OBJECT_NAMES]),
                "relationship_parameter_values",
            )
        key = (class_name, entity_name, parameter_name)
        if key in values:
            return
        value_type = source_data
        value = values[key] = {"type": value_type}
        if self.compress and value_type == "map":
            value["compress"] = self.compress
        parameter_value = [class_name, entity_name, parameter_name, value]
        alternative_name = state.get(ImportKey.ALTERNATIVE_NAME)
        if alternative_name is not None:
            parameter_value.append(alternative_name)
        mapped_data.setdefault(map_key, list()).append(parameter_value)


class ParameterValueMetadataMapping(ImportMapping):
    """Maps relationship metadata.

    Cannot be used as the topmost mapping; must have a :class:`ParameterValueMapping` or
    a :class:`ParameterValueTypeMapping` as parent.
    """

    MAP_TYPE = "ParameterValueMetadata"

    def _import_row(self, source_data, state, mapped_data):
        pass


class ParameterValueIndexMapping(ImportMapping):
    """Maps parameter value indexes.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping`, an entity mapping and
    an :class:`ParameterValueTypeMapping` as parents.
    """

    MAP_TYPE = "ParameterValueIndex"

    def _import_row(self, source_data, state, mapped_data):
        index = source_data
        state.setdefault(ImportKey.PARAMETER_VALUE_INDEXES, []).append(index)


class ExpandedParameterValueMapping(ImportMapping):
    """Maps parameter values.

    Whenever this mapping is a child of :class:`ParameterValueIndexMapping`, it maps individual values of indexed
    parameters.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping`, an entity mapping and
    an :class:`ParameterValueTypeMapping` as parents.
    """

    MAP_TYPE = "ExpandedValue"

    def _import_row(self, source_data, state, mapped_data):
        object_class_name = state.get(ImportKey.OBJECT_CLASS_NAME)
        relationship_class_name = state.get(ImportKey.RELATIONSHIP_CLASS_NAME)
        if object_class_name:
            class_name, entity_name = object_class_name, state[ImportKey.OBJECT_NAME]
        elif relationship_class_name:
            class_name, entity_name = relationship_class_name, tuple(state[ImportKey.OBJECT_NAMES])
        parameter_name = state[ImportKey.PARAMETER_NAME]
        values = state.setdefault(ImportKey.PARAMETER_VALUES, {})
        value = values[class_name, entity_name, parameter_name]
        val = source_data
        data = value.setdefault("data", [])
        if value["type"] == "array":
            data.append(val)
            return
        indexes = state.pop(ImportKey.PARAMETER_VALUE_INDEXES, None)
        data.append(indexes + [val])


class ParameterValueListMapping(ImportMapping):
    """Maps parameter value list names.

    Can be used as the topmost mapping; in case the mapping has a :class:`ParameterDefinitionMapping` as parent,
    yields value list name for that parameter definition.
    """

    MAP_TYPE = "ParameterValueList"

    def _import_row(self, source_data, state, mapped_data):
        state[ImportKey.PARAMETER_VALUE_LIST_NAME] = source_data


class ParameterValueListValueMapping(ImportMapping):
    """Maps parameter value list values.

    Cannot be used as the topmost mapping; must have a :class:`ParameterValueListMapping` as parent.

    """

    MAP_TYPE = "ParameterValueListValue"

    def _import_row(self, source_data, state, mapped_data):
        value_list_name = state[ImportKey.PARAMETER_VALUE_LIST_NAME]
        list_value = source_data
        mapped_data.setdefault("parameter_value_lists", list()).append([value_list_name, list_value])


class AlternativeMapping(ImportMapping):
    """Maps alternatives.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "Alternative"

    def _import_row(self, source_data, state, mapped_data):
        state[ImportKey.ALTERNATIVE_NAME] = source_data
        mapped_data.setdefault("alternatives", list()).append(source_data)


class ScenarioMapping(ImportMapping):
    """Maps scenarios.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "Scenario"

    def _import_row(self, source_data, state, mapped_data):
        state[ImportKey.SCENARIO_NAME] = source_data


class ScenarioActiveFlagMapping(ImportMapping):
    """Maps scenario active flags.

    Cannot be used as the topmost mapping; must have a :class:`ScenarioMapping` as parent.
    """

    MAP_TYPE = "ScenarioActiveFlag"

    def _import_row(self, source_data, state, mapped_data):
        scenario = state[ImportKey.SCENARIO_NAME]
        active = bool(strtobool(str(source_data)))
        mapped_data.setdefault("scenarios", list()).append([scenario, active])


class ScenarioAlternativeMapping(ImportMapping):
    """Maps scenario alternatives.

    Cannot be used as the topmost mapping; must have a :class:`ScenarioMapping` as parent.
    """

    MAP_TYPE = "ScenarioAlternative"

    def _import_row(self, source_data, state, mapped_data):
        scenario = state[ImportKey.SCENARIO_NAME]
        alternative = source_data
        scen_alt = state[ImportKey.SCENARIO_ALTERNATIVE] = [scenario, alternative]
        mapped_data.setdefault("scenario_alternatives", list()).append(scen_alt)


class ScenarioBeforeAlternativeMapping(ImportMapping):
    """Maps scenario 'before' alternatives.

    Cannot be used as the topmost mapping; must have a :class:`ScenarioAlternativeMapping` as parent.
    """

    MAP_TYPE = "ScenarioBeforeAlternative"

    def _import_row(self, source_data, state, mapped_data):
        scen_alt = state[ImportKey.SCENARIO_ALTERNATIVE]
        scen_alt.append(source_data)


class ToolMapping(ImportMapping):
    """Maps tools.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "Tool"

    def _import_row(self, source_data, state, mapped_data):
        state[ImportKey.TOOL_NAME] = source_data
        if self.child is None:
            mapped_data.setdefault("tools", list()).append(source_data)


class FeatureEntityClassMapping(ImportMapping):
    """Maps feature entity classes.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "FeatureEntityClass"

    def _import_row(self, source_data, state, mapped_data):
        state[ImportKey.FEATURE] = [source_data]


class FeatureParameterDefinitionMapping(ImportMapping):
    """Maps feature parameter definitions.

    Cannot be used as the topmost mapping; must have a :class:`FeatureEntityClassMapping` as parent.
    """

    MAP_TYPE = "FeatureParameterDefinition"

    def _import_row(self, source_data, state, mapped_data):
        feature = state[ImportKey.FEATURE]
        feature.append(source_data)
        mapped_data.setdefault("features", list()).append(feature)


class ToolFeatureEntityClassMapping(ImportMapping):
    """Maps tool feature entity classes.

    Cannot be used as the topmost mapping; must have :class:`ToolMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureEntityClass"

    def _import_row(self, source_data, state, mapped_data):
        tool_name = state[ImportKey.TOOL_NAME]
        tool_feature = [tool_name, source_data]
        state[ImportKey.TOOL_FEATURE] = tool_feature
        mapped_data.setdefault("tool_features", list()).append(tool_feature)


class ToolFeatureParameterDefinitionMapping(ImportMapping):
    """Maps tool feature parameter definitions.

    Cannot be used as the topmost mapping; must have :class:`ToolFeatureEntityClassMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureParameterDefinition"

    def _import_row(self, source_data, state, mapped_data):
        tool_feature = state[ImportKey.TOOL_FEATURE]
        tool_feature.append(source_data)


class ToolFeatureRequiredFlagMapping(ImportMapping):
    """Maps tool feature required flags.

    Cannot be used as the topmost mapping; must have :class:`ToolFeatureEntityClassMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureRequiredFlag"

    def _import_row(self, source_data, state, mapped_data):
        required = bool(strtobool(str(source_data)))
        tool_feature = state[ImportKey.TOOL_FEATURE]
        tool_feature.append(required)


class ToolFeatureMethodEntityClassMapping(ImportMapping):
    """Maps tool feature method entity classes.

    Cannot be used as the topmost mapping; must have :class:`ToolMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureMethodEntityClass"

    def _import_row(self, source_data, state, mapped_data):
        tool_name = state[ImportKey.TOOL_NAME]
        tool_feature_method = [tool_name, source_data]
        state[ImportKey.TOOL_FEATURE_METHOD] = tool_feature_method
        mapped_data.setdefault("tool_feature_methods", list()).append(tool_feature_method)


class ToolFeatureMethodParameterDefinitionMapping(ImportMapping):
    """Maps tool feature method parameter definitions.

    Cannot be used as the topmost mapping; must have :class:`ToolFeatureMethodEntityClassMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureMethodParameterDefinition"

    def _import_row(self, source_data, state, mapped_data):
        tool_feature_method = state[ImportKey.TOOL_FEATURE_METHOD]
        tool_feature_method.append(source_data)


class ToolFeatureMethodMethodMapping(ImportMapping):
    """Maps tool feature method methods.

    Cannot be used as the topmost mapping; must have :class:`ToolFeatureMethodEntityClassMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureMethodMethod"

    def _import_row(self, source_data, state, mapped_data):
        tool_feature_method = state[ImportKey.TOOL_FEATURE_METHOD]
        tool_feature_method.append(source_data)


def from_dict(serialized):
    """
    Deserializes mappings.

    Args:
        serialized (list): serialize mappings

    Returns:
        Mapping: root mapping
    """
    mappings = {
        klass.MAP_TYPE: klass
        for klass in (
            AlternativeMapping,
            ExpandedParameterValueMapping,
            FeatureEntityClassMapping,
            FeatureParameterDefinitionMapping,
            ObjectClassMapping,
            ObjectGroupMapping,
            ObjectMapping,
            ObjectMetadataMapping,
            ParameterDefinitionMapping,
            ParameterValueIndexMapping,
            ParameterValueListMapping,
            ParameterValueListValueMapping,
            ParameterValueMetadataMapping,
            ParameterValueMapping,
            RelationshipMapping,
            RelationshipClassMapping,
            RelationshipClassObjectClassMapping,
            RelationshipObjectMapping,
            RelationshipMetadataMapping,
            ScenarioActiveFlagMapping,
            ScenarioAlternativeMapping,
            ScenarioBeforeAlternativeMapping,
            ScenarioMapping,
            ToolMapping,
            ToolFeatureEntityClassMapping,
            ToolFeatureParameterDefinitionMapping,
            ToolFeatureRequiredFlagMapping,
            ToolFeatureMethodEntityClassMapping,
            ToolFeatureMethodParameterDefinitionMapping,
        )
    }
    # Legacy
    mappings["ParameterIndex"] = ParameterValueIndexMapping
    flattened = list()
    for mapping_dict in serialized:
        position = mapping_dict["position"]
        if isinstance(position, str):
            position = Position(position)
        flattened.append(mappings[mapping_dict["map_type"]].reconstruct(position, mapping_dict))
    return unflatten(flattened)
