######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
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

"""

from distutils.util import strtobool
from enum import auto, Enum, unique
from spinedb_api.mapping import Mapping, Position, unflatten, is_pivoted
from spinedb_api.exception import InvalidMappingComponent


@unique
class ImportKey(Enum):
    CLASS_NAME = auto()
    RELATIONSHIP_DIMENSION_COUNT = auto()
    OBJECT_CLASS_NAME = auto()
    OBJECT_NAME = auto()
    GROUP_NAME = auto()
    MEMBER_NAME = auto()
    PARAMETER_NAME = auto()
    PARAMETER_DEFINITION = auto()
    PARAMETER_DEFINITION_EXTRAS = auto()
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

    def __str__(self):
        name = {
            self.ALTERNATIVE_NAME.value: "Alternative names",
            self.CLASS_NAME.value: "Class names",
            self.OBJECT_CLASS_NAME.value: "Object class names",
            self.OBJECT_NAME.value: "Object names",
            self.GROUP_NAME.value: "Group names",
            self.MEMBER_NAME.value: "Member names",
            self.PARAMETER_NAME.value: "Parameter names",
            self.PARAMETER_DEFINITION.value: "Parameter names",
            self.PARAMETER_DEFAULT_VALUE_INDEXES.value: "Parameter indexes",
            self.PARAMETER_VALUE_INDEXES.value: "Parameter indexes",
            self.RELATIONSHIP_CLASS_NAME.value: "Relationship class names",
            self.OBJECT_CLASS_NAMES.value: "Object class names",
            self.OBJECT_NAMES.value: "Object names",
            self.PARAMETER_VALUE_LIST_NAME.value: "Parameter value lists",
            self.SCENARIO_NAME.value: "Scenario names",
            self.SCENARIO_ALTERNATIVE.value: "Alternative names",
            self.TOOL_NAME.value: "Tool names",
            self.FEATURE.value: "Entity class names",
            self.TOOL_FEATURE.value: "Entity class names",
            self.TOOL_FEATURE_METHOD.value: "Entity class names",
        }.get(self.value)
        if name is not None:
            return name
        return super().__str__()


class KeyFix(Exception):
    """Opposite of KeyError"""


def check_validity(root_mapping):
    class _DummySourceRow:
        def __getitem__(self, key):
            return "true"

    errors = []
    source_row = _DummySourceRow()
    root_mapping.import_row(source_row, {}, {}, errors)
    return errors


class ImportMapping(Mapping):
    """Base class for import mappings."""

    def __init__(self, position, value=None, skip_columns=None, read_start_row=0, filter_re=""):
        """
        Args:
            position (int or Position): what to map in the source table
            value (Any, optional): fixed value
            skip_columns (Iterable of int, optional): index of columns that should be skipped;
                useful when source is pivoted
            read_start_row (int): at which source row importing should start
            filter_re (str): regular expression for filtering
        """
        super().__init__(position, value, filter_re)
        self._skip_columns = None
        self._read_start_row = None
        self.skip_columns = skip_columns
        self.read_start_row = read_start_row
        self._has_filter_cached = None
        self._index = None

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
            bad_types = [
                f"{type(column).__name__} at index {i}"
                for i, column in enumerate(skip_columns)
                if not isinstance(column, (str, int))
            ]
            if bad_types:
                bad_types = ", ".join(bad_types)
                raise TypeError(f"skip_columns must be str, int or list of str, int, instead got list with {bad_types}")
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

    def polish(self, table_name, source_header, for_preview=False):
        """Polishes the mapping before an import operation.
        'Expands' transient ``position`` and ``value`` attributes into their final value.

        Args:
            table_name (str)
            source_header (list(str))
        """
        self._polish_for_import(table_name, source_header)
        if for_preview:
            self._polish_for_preview(source_header)

    def _polish_for_import(self, table_name, source_header):
        # FIXME: Polish skip columns
        if self.child is not None:
            self.child._polish_for_import(table_name, source_header)
        if isinstance(self.position, str):
            # Column mapping with string position, we need to find the index in the header
            try:
                self.position = source_header.index(self.position)
                return
            except ValueError:
                msg = f"'{self.position}' is not in '{source_header}'"
                raise InvalidMappingComponent(msg)
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
                except ValueError:
                    msg = f"'{self.value}' is not in header '{source_header}'"
                    raise InvalidMappingComponent(msg)
            # Integer value, we try and get the actual value from that index in the header
            try:
                self._index = self.value
                self.value = source_header[self.value]
            except IndexError:
                msg = f"'{self.value}' is not a valid index in header '{source_header}'"
                raise InvalidMappingComponent(msg)

    def _polish_for_preview(self, source_header):
        if self.position == Position.header and self.value is not None:
            self.value = self._index
        if self.child is not None:
            self.child._polish_for_preview(source_header)

    @property
    def rank(self):
        if self.parent is None:
            return 0
        return self.parent.rank + 1

    def _filter_accepts_row(self, source_row):
        """Whether or not the row passes the filter for this mapping."""
        if self.position == Position.hidden and self.value is None:
            return True
        if self._filter_re is None:
            return True
        source_data = self._data(source_row)
        return self._filter_re.search(str(source_data)) is not None

    def has_filter(self):
        """Whether mapping or one of its children has filter configured.

        Returns:
            bool: True if mapping or one of its children has filter configured , False otherwise
        """
        if self._has_filter_cached is None:
            child_has_filter = self._child.has_filter() if self._child is not None else False
            has_filter = (self.position != Position.hidden or self.value is not None) and self._filter_re is not None
            self._has_filter_cached = child_has_filter or has_filter
        return self._has_filter_cached

    def filter_accepts_row(self, source_row):
        """Whether or not the row passes the filter for all mappings in the hierarchy."""
        return self._filter_accepts_row(source_row) and (
            self.child is None or self.child.filter_accepts_row(source_row)
        )

    def import_row(self, source_row, state, mapped_data, errors=None):
        if self.has_filter() and not self.filter_accepts_row(source_row):
            return
        if errors is None:
            errors = []
        if not (self.position == Position.hidden and self.value is None):
            source_data = self._data(source_row)
            if source_data is None:
                self._skip_row(state)
            else:
                try:
                    self._import_row(source_data, state, mapped_data)
                except KeyError as err:
                    for key in err.args:
                        msg = f"Required key '{key}' is invalid"
                        error = InvalidMappingComponent(msg, self.rank, key)
                        errors.append(error)
                except KeyFix as fix:
                    indexes = set()
                    for key in fix.args:
                        indexes |= {k for k, err in enumerate(errors) if err.key == key}
                    for k in sorted(indexes, reverse=True):
                        errors.pop(k)
        if self.child is not None:
            self.child.import_row(source_row, state, mapped_data, errors=errors)

    def _data(self, source_row):  # pylint: disable=arguments-differ
        if source_row is None:
            return None
        return source_row[self.position]

    def _import_row(self, source_data, state, mapped_data):
        raise NotImplementedError()

    def _skip_row(self, state):
        """Called when the source data is None. Do necessary clean ups on state."""

    def is_constant(self):
        return self.position == Position.hidden and self.value is not None

    def is_pivoted(self):
        if is_pivoted(self.position):
            return True
        if self.position == Position.header and self.value is None and self.child is not None:
            return True
        if self.child is None:
            return False
        return self.child.is_pivoted()

    def to_dict(self):
        d = super().to_dict()
        if self.skip_columns:
            d["skip_columns"] = self.skip_columns
        if self.read_start_row:
            d["read_start_row"] = self.read_start_row
        return d

    @classmethod
    def reconstruct(cls, position, value, skip_columns, read_start_row, filter_re, mapping_dict):
        """
        Reconstructs mapping.

        Args:
            position (int or Position, optional): mapping's position
            value (Any): fixed value
            skip_columns (Iterable of Int, optional): skipped columns
            read_start_row (int): first source row to read
            filter_re (str): filter regular expression
            mapping_dict (dict): serialized mapping

        Returns:
            Mapping: reconstructed mapping
        """
        mapping = cls(position, value, skip_columns, read_start_row, filter_re)
        return mapping


class ImportObjectsMixin:
    def __init__(self, position, value=None, skip_columns=None, read_start_row=0, filter_re="", import_objects=False):
        super().__init__(position, value, skip_columns, read_start_row, filter_re)
        self.import_objects = import_objects

    def to_dict(self):
        d = super().to_dict()
        if self.import_objects:
            d["import_objects"] = True
        return d

    @classmethod
    def reconstruct(cls, position, value, skip_columns, read_start_row, filter_re, mapping_dict):
        import_objects = mapping_dict.get("import_objects", False)
        mapping = cls(position, value, skip_columns, read_start_row, filter_re, import_objects)
        return mapping


class IndexedValueMixin:
    def __init__(
        self, position, value=None, skip_columns=None, read_start_row=0, filter_re="", compress=False, options=None
    ):
        super().__init__(position, value, skip_columns, read_start_row, filter_re)
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
    def reconstruct(cls, position, value, skip_columns, read_start_row, filter_re, mapping_dict):
        compress = mapping_dict.get("compress", False)
        options = mapping_dict.get("options")
        mapping = cls(position, value, skip_columns, read_start_row, filter_re, compress, options)
        return mapping


class ObjectClassMapping(ImportMapping):
    """Maps object classes.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "ObjectClass"

    def _import_row(self, source_data, state, mapped_data):
        object_class_name = state[ImportKey.OBJECT_CLASS_NAME] = str(source_data)
        object_classes = mapped_data.setdefault("object_classes", set())
        object_classes.add(object_class_name)


class ObjectMapping(ImportMapping):
    """Maps objects.

    Cannot be used as the topmost mapping; one of the parents must be :class:`ObjectClassMapping`.
    """

    MAP_TYPE = "Object"

    def _import_row(self, source_data, state, mapped_data):
        object_class_name = state[ImportKey.OBJECT_CLASS_NAME]
        object_name = state[ImportKey.OBJECT_NAME] = str(source_data)
        if isinstance(self.child, ObjectGroupMapping):
            raise KeyError(ImportKey.MEMBER_NAME)
        mapped_data.setdefault("objects", set()).add((object_class_name, object_name))


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
        group_name = state.get(ImportKey.OBJECT_NAME)
        if group_name is None:
            raise KeyError(ImportKey.GROUP_NAME)
        member_name = str(source_data)
        mapped_data.setdefault("object_groups", set()).add((object_class_name, group_name, member_name))
        if self.import_objects:
            objects = (object_class_name, group_name), (object_class_name, member_name)
            mapped_data.setdefault("objects", set()).update(objects)
        raise KeyFix(ImportKey.MEMBER_NAME)


class RelationshipClassMapping(ImportMapping):
    """Maps relationships classes.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "RelationshipClass"

    def _import_row(self, source_data, state, mapped_data):
        dim_count = len([m for m in self.flatten() if isinstance(m, RelationshipClassObjectClassMapping)])
        state[ImportKey.RELATIONSHIP_DIMENSION_COUNT] = dim_count
        relationship_class_name = state[ImportKey.RELATIONSHIP_CLASS_NAME] = str(source_data)
        object_class_names = state[ImportKey.OBJECT_CLASS_NAMES] = []
        relationship_classes = mapped_data.setdefault("relationship_classes", dict())
        relationship_classes[relationship_class_name] = object_class_names
        raise KeyError(ImportKey.OBJECT_CLASS_NAMES)


class RelationshipClassObjectClassMapping(ImportMapping):
    """Maps relationship class object classes.

    Cannot be used as the topmost mapping; one of the parents must be :class:`RelationshipClassMapping`.
    """

    MAP_TYPE = "RelationshipClassObjectClass"

    def _import_row(self, source_data, state, mapped_data):
        _ = state[ImportKey.RELATIONSHIP_CLASS_NAME]
        object_class_names = state[ImportKey.OBJECT_CLASS_NAMES]
        object_class_name = str(source_data)
        object_class_names.append(object_class_name)
        if len(object_class_names) == state[ImportKey.RELATIONSHIP_DIMENSION_COUNT]:
            raise KeyFix(ImportKey.OBJECT_CLASS_NAMES)


class RelationshipMapping(ImportMapping):
    """Maps relationships.

    Cannot be used as the topmost mapping; one of the parents must be :class:`RelationshipClassMapping`.
    """

    MAP_TYPE = "Relationship"

    def _import_row(self, source_data, state, mapped_data):
        # Don't access state[ImportKey.RELATIONSHIP_CLASS_NAME], we don't want to catch errors here
        # because this one's invisible.
        state[ImportKey.OBJECT_NAMES] = []


class RelationshipObjectMapping(ImportObjectsMixin, ImportMapping):
    """Maps relationship's objects.

    Cannot be used as the topmost mapping; must have :class:`RelationshipClassMapping` and :class:`RelationshipMapping`
    as parents.
    """

    MAP_TYPE = "RelationshipObject"

    def _import_row(self, source_data, state, mapped_data):
        relationship_class_name = state[ImportKey.RELATIONSHIP_CLASS_NAME]
        object_class_names = state[ImportKey.OBJECT_CLASS_NAMES]
        if len(object_class_names) != state[ImportKey.RELATIONSHIP_DIMENSION_COUNT]:
            raise KeyError(ImportKey.OBJECT_CLASS_NAMES)
        object_names = state[ImportKey.OBJECT_NAMES]
        object_name = str(source_data)
        object_names.append(object_name)
        if self.import_objects:
            k = len(object_names) - 1
            object_class_name = object_class_names[k]
            mapped_data.setdefault("object_classes", set()).add(object_class_name)
            mapped_data.setdefault("objects", set()).add((object_class_name, object_name))
        if len(object_names) == state[ImportKey.RELATIONSHIP_DIMENSION_COUNT]:
            relationships = mapped_data.setdefault("relationships", set())
            relationships.add((relationship_class_name, tuple(object_names)))
            raise KeyFix(ImportKey.OBJECT_NAMES)
        raise KeyError(ImportKey.OBJECT_NAMES)


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
        if object_class_name is not None:
            class_name = object_class_name
            map_key = "object_parameters"
        else:
            relationship_class_name = state.get(ImportKey.RELATIONSHIP_CLASS_NAME)
            if relationship_class_name is not None:
                class_name = relationship_class_name
                map_key = "relationship_parameters"
            else:
                raise KeyError(ImportKey.CLASS_NAME)
        parameter_name = state[ImportKey.PARAMETER_NAME] = str(source_data)
        definition_extras = state[ImportKey.PARAMETER_DEFINITION_EXTRAS] = []
        parameter_definition_key = state[ImportKey.PARAMETER_DEFINITION] = class_name, parameter_name
        default_values = state.get(ImportKey.PARAMETER_DEFAULT_VALUES)
        if default_values is None or parameter_definition_key not in default_values:
            mapped_data.setdefault(map_key, dict())[parameter_definition_key] = definition_extras


class ParameterDefaultValueMapping(ImportMapping):
    """Maps scalar (non-indexed) default values

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping` as parent.
    """

    MAP_TYPE = "ParameterDefaultValue"

    def _import_row(self, source_data, state, mapped_data):
        default_value = source_data
        if default_value == "":
            return
        parameter_definition_extras = state[ImportKey.PARAMETER_DEFINITION_EXTRAS]
        parameter_definition_extras.append(default_value)
        value_list_name = state.get(ImportKey.PARAMETER_VALUE_LIST_NAME)
        if value_list_name is not None:
            parameter_definition_extras.append(value_list_name)


class ParameterDefaultValueTypeMapping(IndexedValueMixin, ImportMapping):
    MAP_TYPE = "ParameterDefaultValueType"

    def _import_row(self, source_data, state, mapped_data):
        parameter_definition = state.get(ImportKey.PARAMETER_DEFINITION)
        if parameter_definition is None:
            # Don't catch errors here, this one's invisible
            return
        default_values = state.setdefault(ImportKey.PARAMETER_DEFAULT_VALUES, {})
        if parameter_definition in default_values:
            return
        value_type = str(source_data)
        default_value = default_values[parameter_definition] = {"type": value_type}
        if self.compress and value_type == "map":
            default_value["compress"] = self.compress
        if self.options and value_type == "time_series":
            default_value["options"] = self.options
        parameter_definition_extras = state[ImportKey.PARAMETER_DEFINITION_EXTRAS]
        parameter_definition_extras.append(default_value)
        value_list_name = state.get(ImportKey.PARAMETER_VALUE_LIST_NAME)
        if value_list_name is not None:
            parameter_definition_extras.append(value_list_name)


class IndexNameMappingBase(ImportMapping):
    """Base class for index name mappings."""

    _STATE_KEY = NotImplemented

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._id = None

    def _value_key(self, state):
        raise NotImplementedError()

    def _import_row(self, source_data, state, mapped_data):
        values = state[self._STATE_KEY]
        value = values[self._value_key(state)]
        if self._id is None:
            self._id = 0
            current = self
            while True:
                if current.parent is None:
                    break
                current = current.parent
                if isinstance(current, type(self)):
                    self._id += 1
        value.setdefault("index_names", {})[self._id] = source_data


class DefaultValueIndexNameMapping(IndexNameMappingBase):
    """Maps default value index names.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefaultValueTypeMapping` as parent.
    """

    MAP_TYPE = "DefaultValueIndexName"
    _STATE_KEY = ImportKey.PARAMETER_DEFAULT_VALUES

    def _value_key(self, state):
        return _default_value_key(state)


class ParameterDefaultValueIndexMapping(ImportMapping):
    """Maps default value indexes.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping` as parent.
    """

    MAP_TYPE = "ParameterDefaultValueIndex"

    def _import_row(self, source_data, state, mapped_data):
        _ = state[ImportKey.PARAMETER_NAME]
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
        values = state.setdefault(ImportKey.PARAMETER_DEFAULT_VALUES, {})
        value = values[_default_value_key(state)]
        val = source_data
        data = value.setdefault("data", [])
        if value["type"] == "array":
            data.append(val)
            return
        indexes = state.pop(ImportKey.PARAMETER_DEFAULT_VALUE_INDEXES)
        data.append(indexes + [val])

    def _skip_row(self, state):
        state.pop(ImportKey.PARAMETER_DEFAULT_VALUE_INDEXES, None)


class ParameterValueMapping(ImportMapping):
    """Maps scalar (non-indexed) parameter values.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping`, an entity mapping and
    an :class:`AlternativeMapping` as parents.
    """

    MAP_TYPE = "ParameterValue"

    def _import_row(self, source_data, state, mapped_data):
        value = source_data
        if value == "":
            return
        object_class_name = state.get(ImportKey.OBJECT_CLASS_NAME)
        relationship_class_name = state.get(ImportKey.RELATIONSHIP_CLASS_NAME)
        if object_class_name is not None:
            class_name = object_class_name
            entity_name = state[ImportKey.OBJECT_NAME]
            map_key = "object_parameter_values"
        elif relationship_class_name is not None:
            object_names = state[ImportKey.OBJECT_NAMES]
            if len(object_names) != state[ImportKey.RELATIONSHIP_DIMENSION_COUNT]:
                raise KeyError(ImportKey.OBJECT_NAMES)
            class_name = relationship_class_name
            entity_name = object_names
            map_key = "relationship_parameter_values"
        else:
            raise KeyError(ImportKey.CLASS_NAME)
        parameter_name = state[ImportKey.PARAMETER_NAME]
        parameter_value = [class_name, entity_name, parameter_name, value]
        alternative_name = state.get(ImportKey.ALTERNATIVE_NAME)
        if alternative_name is not None:
            parameter_value.append(alternative_name)
        mapped_data.setdefault(map_key, list()).append(parameter_value)


class ParameterValueTypeMapping(IndexedValueMixin, ImportMapping):
    MAP_TYPE = "ParameterValueType"

    def _import_row(self, source_data, state, mapped_data):
        parameter_name = state.get(ImportKey.PARAMETER_NAME)
        if parameter_name is None:
            # Don't catch errors here, this one's invisible
            return
        object_class_name = state.get(ImportKey.OBJECT_CLASS_NAME)
        values = state.setdefault(ImportKey.PARAMETER_VALUES, {})
        if object_class_name is not None:
            class_name = object_class_name
            entity_name = state[ImportKey.OBJECT_NAME]
            map_key = "object_parameter_values"
        else:
            relationship_class_name = state.get(ImportKey.RELATIONSHIP_CLASS_NAME)
            if relationship_class_name is not None:
                class_name = relationship_class_name
                entity_name = tuple(state[ImportKey.OBJECT_NAMES])
                map_key = "relationship_parameter_values"
            else:
                raise KeyError(ImportKey.CLASS_NAME)
        alternative_name = state.get(ImportKey.ALTERNATIVE_NAME)
        key = (class_name, entity_name, parameter_name, alternative_name)
        if key in values:
            return
        value_type = str(source_data)
        value = values[key] = {"type": value_type}  # See import_mapping.generator._parameter_value_from_dict()
        if self.compress and value_type == "map":
            value["compress"] = self.compress
        if self.options and value_type == "time_series":
            value["options"] = self.options
        parameter_value = [class_name, entity_name, parameter_name, value]
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
        _ = state[ImportKey.PARAMETER_NAME]
        index = source_data
        state.setdefault(ImportKey.PARAMETER_VALUE_INDEXES, []).append(index)


class IndexNameMapping(IndexNameMappingBase):
    """Maps index names for indexed parameter values.

    Cannot be used as the topmost mapping; must have an :class:`ParameterValueTypeMapping` as a parent.
    """

    MAP_TYPE = "IndexName"
    _STATE_KEY = ImportKey.PARAMETER_VALUES

    def _value_key(self, state):
        return _parameter_value_key(state)


class ExpandedParameterValueMapping(ImportMapping):
    """Maps parameter values.

    Whenever this mapping is a child of :class:`ParameterValueIndexMapping`, it maps individual values of indexed
    parameters.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping`, an entity mapping and
    an :class:`ParameterValueTypeMapping` as parents.
    """

    MAP_TYPE = "ExpandedValue"

    def _import_row(self, source_data, state, mapped_data):
        values = state.setdefault(ImportKey.PARAMETER_VALUES, {})
        value = values[_parameter_value_key(state)]
        data = value.setdefault("data", [])
        if value["type"] == "array":
            data.append(source_data)
            return
        indexes = state.pop(ImportKey.PARAMETER_VALUE_INDEXES)
        data.append(indexes + [source_data])

    def _skip_row(self, state):
        state.pop(ImportKey.PARAMETER_VALUE_INDEXES, None)


class ParameterValueListMapping(ImportMapping):
    """Maps parameter value list names.

    Can be used as the topmost mapping; in case the mapping has a :class:`ParameterDefinitionMapping` as parent,
    yields value list name for that parameter definition.
    """

    MAP_TYPE = "ParameterValueList"

    def _import_row(self, source_data, state, mapped_data):
        if self.parent is not None:
            # Trigger a KeyError in case there's no parameter definition, so check_validity() registers the issue
            _ = state[ImportKey.PARAMETER_DEFINITION]
        state[ImportKey.PARAMETER_VALUE_LIST_NAME] = str(source_data)


class ParameterValueListValueMapping(ImportMapping):
    """Maps parameter value list values.

    Cannot be used as the topmost mapping; must have a :class:`ParameterValueListMapping` as parent.

    """

    MAP_TYPE = "ParameterValueListValue"

    def _import_row(self, source_data, state, mapped_data):
        list_value = source_data
        if list_value == "":
            return
        value_list_name = state[ImportKey.PARAMETER_VALUE_LIST_NAME]
        mapped_data.setdefault("parameter_value_lists", list()).append([value_list_name, list_value])


class AlternativeMapping(ImportMapping):
    """Maps alternatives.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "Alternative"

    def _import_row(self, source_data, state, mapped_data):
        alternative = state[ImportKey.ALTERNATIVE_NAME] = str(source_data)
        mapped_data.setdefault("alternatives", set()).add(alternative)


class ScenarioMapping(ImportMapping):
    """Maps scenarios.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "Scenario"

    def _import_row(self, source_data, state, mapped_data):
        state[ImportKey.SCENARIO_NAME] = str(source_data)


class ScenarioActiveFlagMapping(ImportMapping):
    """Maps scenario active flags.

    Cannot be used as the topmost mapping; must have a :class:`ScenarioMapping` as parent.
    """

    MAP_TYPE = "ScenarioActiveFlag"

    def _import_row(self, source_data, state, mapped_data):
        scenario = state[ImportKey.SCENARIO_NAME]
        active = bool(strtobool(str(source_data)))
        mapped_data.setdefault("scenarios", set()).add((scenario, active))


class ScenarioAlternativeMapping(ImportMapping):
    """Maps scenario alternatives.

    Cannot be used as the topmost mapping; must have a :class:`ScenarioMapping` as parent.
    """

    MAP_TYPE = "ScenarioAlternative"

    def _import_row(self, source_data, state, mapped_data):
        alternative = str(source_data)
        if not alternative:
            return
        scenario = state[ImportKey.SCENARIO_NAME]
        scen_alt = state[ImportKey.SCENARIO_ALTERNATIVE] = [scenario, alternative]
        mapped_data.setdefault("scenario_alternatives", list()).append(scen_alt)


class ScenarioBeforeAlternativeMapping(ImportMapping):
    """Maps scenario 'before' alternatives.

    Cannot be used as the topmost mapping; must have a :class:`ScenarioAlternativeMapping` as parent.
    """

    MAP_TYPE = "ScenarioBeforeAlternative"

    def _import_row(self, source_data, state, mapped_data):
        scen_alt = state[ImportKey.SCENARIO_ALTERNATIVE]
        alternative = str(source_data)
        scen_alt.append(alternative)


class ToolMapping(ImportMapping):
    """Maps tools.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "Tool"

    def _import_row(self, source_data, state, mapped_data):
        tool = state[ImportKey.TOOL_NAME] = str(source_data)
        if self.child is None:
            mapped_data.setdefault("tools", set()).add(tool)


class FeatureEntityClassMapping(ImportMapping):
    """Maps feature entity classes.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "FeatureEntityClass"

    def _import_row(self, source_data, state, mapped_data):
        entity_class = str(source_data)
        state[ImportKey.FEATURE] = [entity_class]


class FeatureParameterDefinitionMapping(ImportMapping):
    """Maps feature parameter definitions.

    Cannot be used as the topmost mapping; must have a :class:`FeatureEntityClassMapping` as parent.
    """

    MAP_TYPE = "FeatureParameterDefinition"

    def _import_row(self, source_data, state, mapped_data):
        feature = state[ImportKey.FEATURE]
        parameter = str(source_data)
        feature.append(parameter)
        mapped_data.setdefault("features", set()).add(tuple(feature))


class ToolFeatureEntityClassMapping(ImportMapping):
    """Maps tool feature entity classes.

    Cannot be used as the topmost mapping; must have :class:`ToolMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureEntityClass"

    def _import_row(self, source_data, state, mapped_data):
        tool = state[ImportKey.TOOL_NAME]
        entity_class = str(source_data)
        tool_feature = [tool, entity_class]
        state[ImportKey.TOOL_FEATURE] = tool_feature
        mapped_data.setdefault("tool_features", list()).append(tool_feature)


class ToolFeatureParameterDefinitionMapping(ImportMapping):
    """Maps tool feature parameter definitions.

    Cannot be used as the topmost mapping; must have :class:`ToolFeatureEntityClassMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureParameterDefinition"

    def _import_row(self, source_data, state, mapped_data):
        tool_feature = state[ImportKey.TOOL_FEATURE]
        parameter = str(source_data)
        tool_feature.append(parameter)


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
        entity_class = str(source_data)
        tool_feature_method = [tool_name, entity_class]
        state[ImportKey.TOOL_FEATURE_METHOD] = tool_feature_method
        mapped_data.setdefault("tool_feature_methods", list()).append(tool_feature_method)


class ToolFeatureMethodParameterDefinitionMapping(ImportMapping):
    """Maps tool feature method parameter definitions.

    Cannot be used as the topmost mapping; must have :class:`ToolFeatureMethodEntityClassMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureMethodParameterDefinition"

    def _import_row(self, source_data, state, mapped_data):
        tool_feature_method = state[ImportKey.TOOL_FEATURE_METHOD]
        parameter = str(source_data)
        tool_feature_method.append(parameter)


class ToolFeatureMethodMethodMapping(ImportMapping):
    """Maps tool feature method methods.

    Cannot be used as the topmost mapping; must have :class:`ToolFeatureMethodEntityClassMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureMethodMethod"

    def _import_row(self, source_data, state, mapped_data):
        method = source_data
        if method == "":
            return
        tool_feature_method = state[ImportKey.TOOL_FEATURE_METHOD]
        tool_feature_method.append(method)


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
            ObjectClassMapping,
            ObjectMapping,
            ObjectMetadataMapping,
            ObjectGroupMapping,
            RelationshipClassMapping,
            RelationshipClassObjectClassMapping,
            RelationshipMapping,
            RelationshipObjectMapping,
            RelationshipMetadataMapping,
            ParameterDefinitionMapping,
            ParameterDefaultValueMapping,
            ParameterDefaultValueTypeMapping,
            ParameterDefaultValueIndexMapping,
            ExpandedParameterDefaultValueMapping,
            ParameterValueMapping,
            ParameterValueTypeMapping,
            ParameterValueMetadataMapping,
            IndexNameMapping,
            ParameterValueIndexMapping,
            ExpandedParameterValueMapping,
            ParameterValueListMapping,
            ParameterValueListValueMapping,
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
        )
    }
    # Legacy
    mappings["ParameterIndex"] = ParameterValueIndexMapping
    flattened = list()
    for mapping_dict in serialized:
        position = mapping_dict["position"]
        value = mapping_dict.get("value")
        skip_columns = mapping_dict.get("skip_columns")
        read_start_row = mapping_dict.get("read_start_row", 0)
        filter_re = mapping_dict.get("filter_re", "")
        if isinstance(position, str):
            position = Position(position)
        flattened.append(
            mappings[mapping_dict["map_type"]].reconstruct(
                position, value, skip_columns, read_start_row, filter_re, mapping_dict
            )
        )
        if mapping_dict["map_type"] == "ObjectGroup":
            # Legacy: dropping parameter mappings from object groups
            break
    return unflatten(flattened)


def _parameter_value_key(state):
    """Creates parameter value's key from current state.

    Args:
        state (dict): import state

    Returns:
        tuple of str: class name, entity name and parameter name
    """
    object_class_name = state.get(ImportKey.OBJECT_CLASS_NAME)
    if object_class_name is not None:
        class_name = object_class_name
        entity_name = state[ImportKey.OBJECT_NAME]
    else:
        relationship_class_name = state.get(ImportKey.RELATIONSHIP_CLASS_NAME)
        if relationship_class_name is None:
            raise KeyError(ImportKey.CLASS_NAME)
        object_names = state[ImportKey.OBJECT_NAMES]
        if len(object_names) != state[ImportKey.RELATIONSHIP_DIMENSION_COUNT]:
            raise KeyError(ImportKey.OBJECT_NAMES)
        class_name = relationship_class_name
        entity_name = tuple(object_names)
    parameter_name = state[ImportKey.PARAMETER_NAME]
    alternative_name = state.get(ImportKey.ALTERNATIVE_NAME)
    return class_name, entity_name, parameter_name, alternative_name


def _default_value_key(state):
    """Creates parameter default value's key from current state.

    Args:
        state (dict): import state

    Returns:
        tuple of str: class name and parameter name
    """
    class_name = state.get(ImportKey.OBJECT_CLASS_NAME)
    if class_name is None:
        class_name = state.get(ImportKey.RELATIONSHIP_CLASS_NAME)
        if class_name is None:
            raise KeyError(ImportKey.CLASS_NAME)
    parameter_name = state[ImportKey.PARAMETER_NAME]
    return class_name, parameter_name
