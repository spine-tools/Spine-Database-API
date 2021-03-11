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
Contains export mappings for database items such as entities, entity classes and parameter values.

:author: A. Soininen (VTT)
:date:   10.12.2020
"""

from enum import auto, Enum, unique
from spinedb_api.spine_io.mapping import Mapping, Position
from spinedb_api.exception import InvalidMapping


@unique
class ImportKey(Enum):
    OBJECT_CLASS_NAME = auto()
    OBJECT_NAME = auto()
    PARAMETER_NAME = auto()
    PARAMETER_VALUE = auto()
    PARAMETER_VALUES = auto()
    PARAMETER_VALUE_INDEXES = auto()
    RELATIONSHIP_CLASS_NAME = auto()
    OBJECT_CLASS_NAMES = auto()
    OBJECT_NAMES = auto()


class ImportMapping(Mapping):
    def __init__(self, position, value=None, skip_columns=None, read_start_row=0):
        super().__init__(position, value)
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
        # TODO: Polish skip columns
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
            except ValueError:
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
        mapped_data.setdefault("objects", list()).append((object_class_name, object_name))


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


class RelationshipObjectMapping(ImportMapping):
    """Maps relationship's objects.

    Cannot be used as the topmost mapping; must have :class:`RelationshipClassMapping` and :class:`RelationshipMapping`
    as parents.
    """

    MAP_TYPE = "RelationshipObject"

    def __init__(self, position, value=None, skip_columns=None, read_start_row=0, import_objects=False):
        super().__init__(position, value, skip_columns, read_start_row)
        self.import_objects = import_objects

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
        mapped_data.setdefault(map_key, list()).append((class_name, parameter_name))


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
        value = state[ImportKey.PARAMETER_VALUE] = source_data
        mapped_data.setdefault(map_key, list()).append([class_name, entity_name, parameter_name, value])


class ParameterValueTypeMapping(ParameterValueMapping):
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
        value = values[key] = {"type": source_data}
        mapped_data.setdefault(map_key, list()).append([class_name, entity_name, parameter_name, value])


class ParameterValueIndexMapping(ImportMapping):
    """Maps parameter value indexes.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping`, an entity mapping and
    an :class:`AlternativeMapping` as parents.
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
    an :class:`AlternativeMapping` as parents.
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
        indexes = state.pop(ImportKey.PARAMETER_VALUE_INDEXES, None)
        val = source_data
        data = value.setdefault("data", [])
        if indexes is None:
            data.append(val)
            return
        data.append(indexes + [val])
