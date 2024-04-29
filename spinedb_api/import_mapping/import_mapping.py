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
""" Contains import mappings for database items such as entities, entity classes and parameter values. """

from enum import auto, Enum, unique

from spinedb_api.helpers import string_to_bool
from spinedb_api.mapping import Mapping, Position, unflatten, is_pivoted
from spinedb_api.exception import InvalidMappingComponent


@unique
class ImportKey(Enum):
    DIMENSION_COUNT = auto()
    ENTITY_CLASS_NAME = auto()
    ENTITY_NAME = auto()
    GROUP_NAME = auto()
    MEMBER_NAME = auto()
    PARAMETER_NAME = auto()
    PARAMETER_DEFINITION = auto()
    PARAMETER_DEFINITION_EXTRAS = auto()
    PARAMETER_DEFAULT_VALUES = auto()
    PARAMETER_DEFAULT_VALUE_INDEXES = auto()
    PARAMETER_VALUES = auto()
    PARAMETER_VALUE_INDEXES = auto()
    DIMENSION_NAMES = auto()
    ELEMENT_NAMES = auto()
    ALTERNATIVE_NAME = auto()
    SCENARIO_NAME = auto()
    SCENARIO_ALTERNATIVE = auto()
    PARAMETER_VALUE_LIST_NAME = auto()

    def __str__(self):
        name = {
            self.ALTERNATIVE_NAME.value: "Alternative names",
            self.ENTITY_CLASS_NAME.value: "Entity class names",
            self.ENTITY_NAME.value: "Entity names",
            self.GROUP_NAME.value: "Group names",
            self.MEMBER_NAME.value: "Member names",
            self.PARAMETER_NAME.value: "Parameter names",
            self.PARAMETER_DEFINITION.value: "Parameter names",
            self.PARAMETER_DEFAULT_VALUE_INDEXES.value: "Parameter indexes",
            self.PARAMETER_VALUE_INDEXES.value: "Parameter indexes",
            self.DIMENSION_NAMES.value: "Dimension names",
            self.ELEMENT_NAMES.value: "Element names",
            self.PARAMETER_VALUE_LIST_NAME.value: "Parameter value lists",
            self.SCENARIO_NAME.value: "Scenario names",
            self.SCENARIO_ALTERNATIVE.value: "Alternative names",
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

    def check_for_invalid_column_refs(self, header, table_name):
        """Checks that the mappings column refs are not out of range for the source table

        Args:
            header (list): The header of the table as a list
            table_name (str): The name of the source table

        Returns:
            str: Error message if a column ref exceeds the column count of the source table,
            empty string otherwise
        """
        if self.child is not None:
            error = self.child.check_for_invalid_column_refs(header, table_name)
            if error:
                return error
        if isinstance(self.position, int) and self.position >= len(header) > 0:
            msg = f'Column ref {self.position + 1} is out of range for the source table "{table_name}"'
            return msg
        return ""

    def polish(self, table_name, source_header, column_count=0, for_preview=False):
        """Polishes the mapping before an import operation.
        'Expands' transient ``position`` and ``value`` attributes into their final value.

        Args:
            table_name (str)
            source_header (list(str))
            column_count (int, optional)
            for_preview (bool, optional)
        """
        self._polish_for_import(table_name, source_header, column_count)
        if for_preview:
            self._polish_for_preview(source_header)

    def _polish_for_import(self, table_name, source_header, column_count):
        # FIXME: Polish skip columns
        if self.child is not None:
            self.child._polish_for_import(table_name, source_header, column_count)
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
        if isinstance(self.position, int) and self.position >= column_count > 0:
            msg = f'Column ref {self.position + 1} is out of range for the source table "{table_name}"'
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

    def _data(self, source_row):  # pylint: disable=arguments-renamed
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


class ImportEntitiesMixin:
    def __init__(self, position, value=None, skip_columns=None, read_start_row=0, filter_re="", import_entities=False):
        super().__init__(position, value, skip_columns, read_start_row, filter_re)
        self.import_entities = import_entities

    def to_dict(self):
        d = super().to_dict()
        if self.import_entities:
            d["import_entities"] = True
        return d

    @classmethod
    def reconstruct(cls, position, value, skip_columns, read_start_row, filter_re, mapping_dict):
        import_entities = mapping_dict.get("import_entities", False)
        mapping = cls(position, value, skip_columns, read_start_row, filter_re, import_entities)
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


class EntityClassMapping(ImportMapping):
    """Maps entity classes.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "EntityClass"

    def _import_row(self, source_data, state, mapped_data):
        dim_count = len([m for m in self.flatten() if isinstance(m, DimensionMapping)])
        state[ImportKey.DIMENSION_COUNT] = dim_count
        entity_class_name = state[ImportKey.ENTITY_CLASS_NAME] = str(source_data)
        dimension_names = state[ImportKey.DIMENSION_NAMES] = []
        entity_classes = mapped_data.setdefault("entity_classes", {})
        entity_classes[entity_class_name] = dimension_names
        if dim_count:
            raise KeyError(ImportKey.DIMENSION_NAMES)


class EntityMapping(ImportMapping):
    """Maps entities.

    Cannot be used as the topmost mapping; one of the parents must be :class:`EntityClassMapping`.
    """

    MAP_TYPE = "Entity"

    def import_row(self, source_row, state, mapped_data, errors=None):
        state[ImportKey.ELEMENT_NAMES] = ()
        super().import_row(source_row, state, mapped_data, errors=errors)

    def _import_row(self, source_data, state, mapped_data):
        if state[ImportKey.DIMENSION_COUNT]:
            return
        entity_class_name = state[ImportKey.ENTITY_CLASS_NAME]
        entity_name = state[ImportKey.ENTITY_NAME] = str(source_data)
        if isinstance(self.child, EntityGroupMapping):
            raise KeyError(ImportKey.MEMBER_NAME)
        mapped_data.setdefault("entities", {})[entity_class_name, entity_name] = None


class EntityMetadataMapping(ImportMapping):
    """Maps entity metadata.

    Cannot be used as the topmost mapping; must have :class:`EntityClassMapping` and :class:`EntityMapping` as parents.
    """

    MAP_TYPE = "EntityMetadata"

    def _import_row(self, source_data, state, mapped_data):
        pass


class EntityGroupMapping(ImportEntitiesMixin, ImportMapping):
    """Maps entity groups.

    Cannot be used as the topmost mapping; must have :class:`EntityClassMapping` and :class:`EntityMapping` as parents.
    """

    MAP_TYPE = "EntityGroup"

    def _import_row(self, source_data, state, mapped_data):
        entity_class_name = state[ImportKey.ENTITY_CLASS_NAME]
        group_name = state.get(ImportKey.ENTITY_NAME)
        if group_name is None:
            raise KeyError(ImportKey.GROUP_NAME)
        member_name = str(source_data)
        mapped_data.setdefault("entity_groups", set()).add((entity_class_name, group_name, member_name))
        if self.import_entities:
            entities = mapped_data.setdefault("entities", {})
            entities[entity_class_name, group_name] = None
            entities[entity_class_name, member_name] = None
        raise KeyFix(ImportKey.MEMBER_NAME)


class DimensionMapping(ImportMapping):
    """Maps dimensions.

    Cannot be used as the topmost mapping; one of the parents must be :class:`EntityClassMapping`.
    """

    MAP_TYPE = "Dimension"

    def _import_row(self, source_data, state, mapped_data):
        _ = state[ImportKey.ENTITY_CLASS_NAME]
        dimension_name = str(source_data)
        state[ImportKey.DIMENSION_NAMES].append(dimension_name)
        dimension_names = state[ImportKey.DIMENSION_NAMES]
        if len(dimension_names) == state[ImportKey.DIMENSION_COUNT]:
            raise KeyFix(ImportKey.DIMENSION_NAMES)


class ElementMapping(ImportEntitiesMixin, ImportMapping):
    """Maps elements.

    Cannot be used as the topmost mapping; must have :class:`EntityClassMapping` and :class:`EntityMapping`
    as parents.
    """

    MAP_TYPE = "Element"

    def _import_row(self, source_data, state, mapped_data):
        entity_class_name = state[ImportKey.ENTITY_CLASS_NAME]
        dimension_names = state[ImportKey.DIMENSION_NAMES]
        if len(dimension_names) != state[ImportKey.DIMENSION_COUNT]:
            raise KeyError(ImportKey.DIMENSION_NAMES)
        element_name = str(source_data)
        element_names = state[ImportKey.ELEMENT_NAMES] = state[ImportKey.ELEMENT_NAMES] + (element_name,)
        if self.import_entities:
            k = len(element_names) - 1
            dimension_name = dimension_names[k]
            mapped_data.setdefault("entity_classes", {}).update({dimension_name: ()})
            mapped_data.setdefault("entities", {})[dimension_name, element_name] = None
        if len(element_names) == state[ImportKey.DIMENSION_COUNT]:
            mapped_data.setdefault("entities", {})[entity_class_name, tuple(element_names)] = None
            raise KeyFix(ImportKey.ELEMENT_NAMES)
        raise KeyError(ImportKey.ELEMENT_NAMES)


class ParameterDefinitionMapping(ImportMapping):
    """Maps parameter definitions.

    Cannot be used as the topmost mapping; must have an entity class mapping as one of parents.
    """

    MAP_TYPE = "ParameterDefinition"

    def _import_row(self, source_data, state, mapped_data):
        entity_class_name = state.get(ImportKey.ENTITY_CLASS_NAME)
        parameter_name = state[ImportKey.PARAMETER_NAME] = str(source_data)
        definition_extras = state[ImportKey.PARAMETER_DEFINITION_EXTRAS] = []
        parameter_definition_key = state[ImportKey.PARAMETER_DEFINITION] = entity_class_name, parameter_name
        default_values = state.get(ImportKey.PARAMETER_DEFAULT_VALUES)
        if default_values is None or parameter_definition_key not in default_values:
            mapped_data.setdefault("parameter_definitions", dict())[parameter_definition_key] = definition_extras


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
        entity_class_name, entity_byname, parameter_name, alternative_name = _parameter_value_key(state)
        parameter_value = [entity_class_name, entity_byname, parameter_name, value]
        if alternative_name is not None:
            parameter_value.append(alternative_name)
        mapped_data.setdefault("parameter_values", []).append(parameter_value)


class ParameterValueTypeMapping(IndexedValueMixin, ImportMapping):
    MAP_TYPE = "ParameterValueType"

    def _import_row(self, source_data, state, mapped_data):
        if ImportKey.PARAMETER_NAME not in state:
            # Don't catch errors here, this one's invisible
            return
        key = _parameter_value_key(state)
        values = state.setdefault(ImportKey.PARAMETER_VALUES, {})
        if key in values:
            return
        entity_class_name, entity_byname, parameter_name, alternative_name = key
        value_type = str(source_data)
        value = values[key] = {"type": value_type}  # See import_mapping.generator._parameter_value_from_dict()
        if self.compress and value_type == "map":
            value["compress"] = self.compress
        if self.options and value_type == "time_series":
            value["options"] = self.options
        parameter_value = [entity_class_name, entity_byname, parameter_name, value]
        if alternative_name is not None:
            parameter_value.append(alternative_name)
        mapped_data.setdefault("parameter_values", []).append(parameter_value)


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
        mapped_data.setdefault("parameter_value_lists", []).append([value_list_name, list_value])


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
        active = string_to_bool(str(source_data))
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
        mapped_data.setdefault("scenario_alternatives", []).append(scen_alt)


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


def default_import_mapping(map_type):
    """Creates default mappings for given map type.

    Args:
        map_type (str): map type

    Returns:
        ImportMapping: root mapping of desired type
    """
    make_root_mapping = {
        "EntityClass": _default_entity_class_mapping,
        "Alternative": _default_alternative_mapping,
        "Scenario": _default_scenario_mapping,
        "ScenarioAlternative": _default_scenario_alternative_mapping,
        "EntityGroup": _default_entity_group_mapping,
        "ParameterValueList": _default_parameter_value_list_mapping,
    }[map_type]
    return make_root_mapping()


def _default_entity_class_mapping():
    """Creates default entity class mappings.

    Returns:
        EntityClassMapping: root mapping
    """
    root_mapping = EntityClassMapping(Position.hidden)
    object_mapping = root_mapping.child = EntityMapping(Position.hidden)
    object_mapping.child = EntityMetadataMapping(Position.hidden)
    return root_mapping


def _default_alternative_mapping():
    """Creates default alternative mappings.

    Returns:
        AlternativeMapping: root mapping
    """
    root_mapping = AlternativeMapping(Position.hidden)
    return root_mapping


def _default_scenario_mapping():
    """Creates default scenario mappings.

    Returns:
        ScenarioMapping: root mapping
    """
    root_mapping = ScenarioMapping(Position.hidden)
    root_mapping.child = ScenarioActiveFlagMapping(Position.hidden)
    return root_mapping


def _default_scenario_alternative_mapping():
    """Creates default scenario alternative mappings.

    Returns:
        ScenarioAlternativeMapping: root mapping
    """
    root_mapping = ScenarioMapping(Position.hidden)
    scen_alt_mapping = root_mapping.child = ScenarioAlternativeMapping(Position.hidden)
    scen_alt_mapping.child = ScenarioBeforeAlternativeMapping(Position.hidden)
    return root_mapping


def _default_entity_group_mapping():
    """Creates default entity group mappings.

    Returns:
        EntityClassMapping: root mapping
    """
    root_mapping = EntityClassMapping(Position.hidden)
    object_mapping = root_mapping.child = EntityMapping(Position.hidden)
    object_mapping.child = EntityGroupMapping(Position.hidden)
    return root_mapping


def _default_parameter_value_list_mapping():
    """Creates default parameter value list mappings.

    Returns:
        ParameterValueListMapping: root mapping
    """
    root_mapping = ParameterValueListMapping(Position.hidden)
    root_mapping.child = ParameterValueListValueMapping(Position.hidden)
    return root_mapping


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
            EntityClassMapping,
            EntityMapping,
            EntityMetadataMapping,
            EntityGroupMapping,
            DimensionMapping,
            ElementMapping,
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
            # FIXME
            # FeatureEntityClassMapping,
            # FeatureParameterDefinitionMapping,
            # ToolFeatureEntityClassMapping,
            # ToolFeatureParameterDefinitionMapping,
            # ToolFeatureRequiredFlagMapping,
            # ToolFeatureMethodEntityClassMapping,
            # ToolFeatureMethodParameterDefinitionMapping,
            # ToolFeatureMethodMethodMapping,
        )
    }
    legacy_mappings = {
        "ParameterIndex": ParameterValueIndexMapping,
        "ObjectClass": EntityClassMapping,
        "Object": EntityMapping,
        "ObjectMetadata": EntityMetadataMapping,
        "ObjectGroup": EntityGroupMapping,
        "RelationshipClass": EntityClassMapping,
        "RelationshipClassObjectClass": DimensionMapping,
        "Relationship": EntityMapping,
        "RelationshipObject": ElementMapping,
        "RelationshipMetadata": EntityMetadataMapping,
    }
    mappings.update(legacy_mappings)
    flattened = []
    for mapping_dict in serialized:
        position = mapping_dict["position"]
        value = mapping_dict.get("value")
        skip_columns = mapping_dict.get("skip_columns")
        read_start_row = mapping_dict.get("read_start_row", 0)
        filter_re = mapping_dict.get("filter_re", "")
        if isinstance(position, str):
            position = Position(position)
        if "import_objects" in mapping_dict:
            # Legacy
            mapping_dict["import_entities"] = mapping_dict.pop("import_objects")
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
        tuple of str: class name, entity byname, parameter name, and alternative name
    """
    entity_class_name = state.get(ImportKey.ENTITY_CLASS_NAME)
    if state.get(ImportKey.DIMENSION_COUNT):
        element_names = state[ImportKey.ELEMENT_NAMES]
        if len(element_names) != state[ImportKey.DIMENSION_COUNT]:
            raise KeyError(ImportKey.ELEMENT_NAMES)
        entity_byname = element_names
    else:
        entity_byname = state[ImportKey.ENTITY_NAME]
    parameter_name = state[ImportKey.PARAMETER_NAME]
    alternative_name = state.get(ImportKey.ALTERNATIVE_NAME)
    return entity_class_name, entity_byname, parameter_name, alternative_name


def _default_value_key(state):
    """Creates parameter default value's key from current state.

    Args:
        state (dict): import state

    Returns:
        tuple of str: class name and parameter name
    """
    return state[ImportKey.ENTITY_CLASS_NAME], state[ImportKey.PARAMETER_NAME]
