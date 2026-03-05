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
"""Contains import mappings for database items such as entities, entity classes and parameter values."""
from __future__ import annotations
from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import Enum, auto, unique
from typing import Any, ClassVar, Generic, Type, TypeAlias, TypeVar
from spinedb_api.exception import InvalidMapping, InvalidMappingComponent
from spinedb_api.mapping import Mapping, Position, is_pivoted, parse_fixed_position_value, unflatten


@unique
class ImportKey(Enum):
    ENTITY_CLASS_NAME = auto()
    ENTITY_NAME = auto()
    ELEMENT_NAMES = auto()
    GROUP_NAME = auto()
    MEMBER_NAME = auto()
    METADATA_NAME = auto()
    METADATA_VALUE = auto()
    PARAMETER_NAME = auto()
    PARAMETER_DEFAULT_VALUE_RECORD = auto()
    PARAMETER_DEFAULT_VALUE_INDEXES = auto()
    PARAMETER_DEFAULT_VALUE_INDEX_NAMES = auto()
    PARAMETER_VALUE_RECORD = auto()
    PARAMETER_VALUE_INDEXES = auto()
    PARAMETER_VALUE_INDEX_NAMES = auto()
    PARAMETER_VALUE_METADATA_NAME = auto()
    PARAMETER_VALUE_METADATA_VALUE = auto()
    ALTERNATIVE_NAME = auto()
    SCENARIO_NAME = auto()
    SCENARIO_ALTERNATIVE = auto()
    PARAMETER_VALUE_LIST_NAME = auto()
    ENTITY_METADATA_NAME = auto()
    ENTITY_METADATA_VALUE = auto()

    def __str__(self):
        name = {
            self.ALTERNATIVE_NAME.value: "Alternative names",
            self.ENTITY_CLASS_NAME.value: "Entity class names",
            self.ENTITY_NAME.value: "Entity names",
            self.GROUP_NAME.value: "Group names",
            self.MEMBER_NAME.value: "Member names",
            self.METADATA_NAME: "Metadata names",
            self.METADATA_VALUE: "Metadata values",
            self.PARAMETER_NAME.value: "Parameter names",
            self.PARAMETER_DEFAULT_VALUE_INDEXES.value: "Parameter indexes",
            self.PARAMETER_VALUE_INDEXES.value: "Parameter indexes",
            self.PARAMETER_VALUE_METADATA_NAME.value: "Metadata names",
            self.PARAMETER_VALUE_METADATA_VALUE.value: "Metadata values",
            self.PARAMETER_VALUE_LIST_NAME.value: "Parameter value lists",
            self.SCENARIO_NAME.value: "Scenario names",
            self.SCENARIO_ALTERNATIVE.value: "Alternative names",
            self.ENTITY_METADATA_NAME.value: "Metadata names",
            self.ENTITY_METADATA_VALUE: "Metadata values",
        }.get(self.value)
        if name is not None:
            return name
        return super().__str__()


State: TypeAlias = dict[ImportKey, Any]
SemiMappedData: TypeAlias = dict[str, Any]


def check_validity(root_mapping: ImportMapping) -> list[InvalidMappingComponent]:
    errors = []
    for rank, mapping in enumerate(root_mapping.flatten()):
        if mapping.position == Position.fixed:
            try:
                parse_fixed_position_value(mapping.value)
            except InvalidMapping as error:
                errors.append(InvalidMappingComponent(str(error), rank))
        elif mapping.position != Position.hidden or mapping.value is not None:
            try:
                mapping.check_validity()
            except InvalidMappingComponent as error:
                errors.append(error)
    errors += _check_dependent_pairs(root_mapping)
    return errors


def _check_dependent_pairs(root_mapping: ImportMapping) -> list[InvalidMappingComponent]:
    flattened = root_mapping.flatten()
    try:
        definition_mapping = next(m for m in flattened if isinstance(m, ParameterDefinitionMapping))
        value_list_mapping = next(m for m in flattened if isinstance(m, ParameterValueListMapping))
    except StopIteration:
        return []
    if (value_list_mapping.position is not Position.hidden or definition_mapping.value is not None) and (
        definition_mapping.position == Position.hidden and definition_mapping.value is None
    ):
        value_list_rank = next(n for n, m in enumerate(flattened) if isinstance(m, ParameterValueListMapping))
        return [InvalidMappingComponent("value list requires a parameter name", value_list_rank)]
    return []


class ImportMapping(Mapping):
    """Base class for import mappings."""

    ignorable: ClassVar[bool] = False

    def __init__(
        self,
        position: int | str | Position,
        value: Any = None,
        skip_columns: Iterable[int] | None = None,
        read_start_row: int = 0,
        filter_re: str = "",
    ):
        """
        Args:
            position: what to map in the source table
            value: fixed value
            skip_columns: index of columns that should be skipped; useful when source is pivoted
            read_start_row: at which source row importing should start
            filter_re: regular expression for filtering
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

    def check_validity(self) -> None:
        return

    def polish(self, table_name, source_header, mapping_name, column_count=0, for_preview=False):
        """Polishes the mapping before an import operation.
        'Expands' transient ``position`` and ``value`` attributes into their final value.

        Args:
            table_name (str)
            source_header (list(str))
            mapping_name (str)
            column_count (int, optional)
            for_preview (bool, optional)
        """
        self._polish_for_import(table_name, source_header, mapping_name, column_count)
        if for_preview:
            self._polish_for_preview(source_header)

    def _polish_for_import(self, table_name, source_header, mapping_name, column_count, pivoted=None):
        # FIXME: Polish skip columns
        if pivoted is None:
            pivoted = self.is_pivoted()
        if pivoted and self.parent and self.is_effective_leaf():
            return
        if self.child is not None:
            self.child._polish_for_import(table_name, source_header, mapping_name, column_count, pivoted)
        if isinstance(self.position, str):
            # Column mapping with string position, we need to find the index in the header
            try:
                self.position = source_header.index(self.position)
                return
            except ValueError as error:
                msg = f"'{self.position}' is not in '{source_header}'"
                raise InvalidMappingComponent(msg) from error
        if self.position == Position.table_name:
            # Table name mapping, we set the fixed value to the table name
            self.value = table_name
            return
        if self.position == Position.mapping_name:
            # Mapping name mapping, we set the fixed value to the mapping name
            self.value = mapping_name
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
                except ValueError as error:
                    msg = f"'{self.value}' is not in header '{source_header}'"
                    raise InvalidMappingComponent(msg) from error
            # Integer value, we try and get the actual value from that index in the header
            try:
                self._index = self.value
                self.value = source_header[self.value]
            except IndexError as error:
                msg = f"'{self.value}' is not a valid index in header '{source_header}'"
                raise InvalidMappingComponent(msg) from error
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
        if (self.position == Position.hidden and self.value is None) or self._filter_re is None:
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

    def import_row(self, source_row, state, mapped_data):
        if self.has_filter() and not self.filter_accepts_row(source_row):
            return
        if not (self.position == Position.hidden and self.value is None):
            source_data = self._data(source_row)
            if source_data is None:
                if not self.ignorable or self.child is None:
                    self._skip_row(state)
                    return
                self.child.import_row(source_row, state, mapped_data)
                return
            self._import_row(source_data, state, mapped_data)
        if self.child is not None:
            self.child.import_row(source_row, state, mapped_data)

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

    def _make_value_record(self, value_type: str) -> ValueRecord:
        match value_type:
            case "array":
                return ArrayValueRecord()
            case "map":
                return MapValueRecord(compress=self.compress)
            case "time_series":
                return TimeSeriesValueRecord(
                    ignore_year=self.options.get("ignore_year", False), repeat=self.options.get("repeat", False)
                )
            case "time_pattern":
                return TimePatternValueRecord()
            case _:
                raise InvalidMapping(f"unknown value type '{value_type}'")


@dataclass
class EntityClassRecord:
    dimensions: list[str] = field(default_factory=list)
    description: str | None = None


class EntityClassMapping(ImportMapping):
    """Maps entity classes."""

    MAP_TYPE = "EntityClass"

    def _import_row(self, source_data, state, mapped_data):
        entity_class_name = state[ImportKey.ENTITY_CLASS_NAME] = str(source_data)
        entity_classes = mapped_data.setdefault("entity_classes", {})
        entity_classes[entity_class_name] = EntityClassRecord()


def _require_parent(mapping: ImportMapping, parent_type: Type[ImportMapping]) -> None:
    parent = mapping.parent
    while parent is not None:
        if isinstance(parent, parent_type):
            if parent.position == Position.hidden and parent.value is None:
                raise InvalidMappingComponent(
                    f"{mapping.MAP_TYPE} requires {parent_type.MAP_TYPE} with position or constant value", mapping.rank
                )
            return
        parent = parent.parent
    raise InvalidMappingComponent(f"{mapping.MAP_TYPE} requires {parent_type.MAP_TYPE} as parent", mapping.rank)


def _require_one_of_parents(mapping: ImportMapping, parent_types: tuple[Type[ImportMapping], ...]) -> None:
    parent = mapping.parent
    while parent is not None:
        if isinstance(parent, parent_types) and (parent.position != Position.hidden or parent.value is not None):
            return
        parent = parent.parent
    display_types = " or ".join(m.MAP_TYPE for m in parent_types)
    raise InvalidMappingComponent(f"{mapping.MAP_TYPE} requires {display_types} as parent", mapping.rank)


def _require_enough_parents(mapping: ImportMapping, parent_type: Type[ImportMapping]) -> None:
    n_same_parent_type = 1
    parent = mapping.parent
    while parent is not None:
        if isinstance(parent, type(mapping)):
            n_same_parent_type += 1
        parent = parent.parent
    n_required_parent_type = 0
    parent = mapping.parent
    while parent is not None:
        if isinstance(parent, parent_type):
            n_required_parent_type += 1
            if n_required_parent_type == n_same_parent_type:
                return
        parent = parent.parent
    raise InvalidMappingComponent(
        f"the number of {mapping.MAP_TYPE} and {parent_type.MAP_TYPE} mappings do not match", mapping.rank
    )


class EntityClassDescriptionMapping(ImportMapping):
    """Maps entity class descriptions."""

    MAP_TYPE = "EntityClassDescription"
    ignorable = True

    def _import_row(self, source_data, state, mapped_data):
        description = str(source_data)
        if description:
            entity_class_name = state[ImportKey.ENTITY_CLASS_NAME]
            mapped_data["entity_classes"][entity_class_name].description = description

    def check_validity(self) -> None:
        _require_parent(self, EntityClassMapping)


@dataclass
class EntityRecord:
    elements: list[str] = field(default_factory=list)
    description: str | None = None


class EntityMapping(ImportMapping):
    """Maps entities."""

    MAP_TYPE = "Entity"

    def _import_row(self, source_data, state, mapped_data):
        if self.position == Position.hidden and isinstance(self._child, ElementMapping):
            return
        entity_class_name = state[ImportKey.ENTITY_CLASS_NAME]
        entity_name = state[ImportKey.ENTITY_NAME] = str(source_data)
        mapped_data.setdefault("entities", {})[entity_class_name, entity_name] = EntityRecord()

    def check_validity(self) -> None:
        _require_parent(self, EntityClassMapping)


class EntityDescriptionMapping(ImportMapping):
    """Maps entity descriptions."""

    MAP_TYPE = "EntityDescription"
    ignorable = True

    def _import_row(self, source_data, state, mapped_data):
        description = str(source_data)
        if description:
            entity_class_name = state[ImportKey.ENTITY_CLASS_NAME]
            entity_name = state[ImportKey.ENTITY_NAME]
            mapped_data["entities"][entity_class_name, entity_name].description = description

    def check_validity(self) -> None:
        _require_one_of_parents(self, (EntityMapping, ElementMapping))


class EntityMetadataNameMapping(ImportMapping):
    """Maps entity metadata names."""

    MAP_TYPE = "EntityMetadataName"
    ignorable = True

    def _import_row(self, source_data, state, mapped_data):
        state[ImportKey.ENTITY_METADATA_NAME] = source_data


class EntityMetadataValueMapping(ImportMapping):
    """Maps entity metadata names."""

    MAP_TYPE = "EntityMetadataValue"
    ignorable = True

    def _import_row(self, source_data, state, mapped_data):
        entity_class_name = state[ImportKey.ENTITY_CLASS_NAME]
        entity_byname = _byname_from_mapped_data(entity_class_name, state, mapped_data)
        metadata_name = state[ImportKey.ENTITY_METADATA_NAME]
        metadata_value = state[ImportKey.ENTITY_METADATA_VALUE] = source_data
        mapped_data.setdefault("entity_metadata", {})[
            entity_class_name, entity_byname, metadata_name, metadata_value
        ] = None

    def check_validity(self) -> None:
        _require_parent(self, EntityMetadataNameMapping)


class EntityGroupMapping(ImportEntitiesMixin, ImportMapping):
    """Maps entity groups."""

    MAP_TYPE = "EntityGroup"

    def _import_row(self, source_data, state, mapped_data):
        entity_class_name = state[ImportKey.ENTITY_CLASS_NAME]
        group_name = state[ImportKey.ENTITY_NAME]
        member_name = str(source_data)
        mapped_data.setdefault("entity_groups", set()).add((entity_class_name, group_name, member_name))
        if self.import_entities:
            mapped_data["entities"][entity_class_name, member_name] = EntityRecord()
        else:
            try:
                del mapped_data["entities"][entity_class_name, group_name]
            except KeyError:
                pass

    def check_validity(self) -> None:
        _require_parent(self, EntityMapping)


class EntityAlternativeActivityMapping(ImportMapping):
    """Maps activity flags for entity alternative."""

    MAP_TYPE = "EntityAlternativeActivity"
    ignorable = True

    def _import_row(self, source_data, state, mapped_data):
        if source_data == "":
            return
        entity_class_name = state[ImportKey.ENTITY_CLASS_NAME]
        entity_byname = _byname_from_mapped_data(entity_class_name, state, mapped_data)
        alternative_name = state[ImportKey.ALTERNATIVE_NAME]
        mapped_data.setdefault("entity_alternatives", {})[
            entity_class_name, entity_byname, alternative_name, source_data
        ] = None

    def check_validity(self) -> None:
        _require_parent(self, EntityMapping)
        _require_parent(self, AlternativeMapping)


class DimensionMapping(ImportMapping):
    """Maps dimensions."""

    MAP_TYPE = "Dimension"

    def _import_row(self, source_data, state, mapped_data):
        dimension_name = str(source_data)
        entity_class_name = state[ImportKey.ENTITY_CLASS_NAME]
        mapped_data["entity_classes"][entity_class_name].dimensions.append(dimension_name)

    def check_validity(self) -> None:
        _require_parent(self, EntityClassMapping)


class ElementMapping(ImportEntitiesMixin, ImportMapping):
    """Maps elements."""

    MAP_TYPE = "Element"

    def _import_row(self, source_data, state, mapped_data):
        element_name = str(source_data)
        if isinstance(self._child, ElementMapping):
            element_names = state.setdefault(ImportKey.ELEMENT_NAMES, [])
            element_names.append(element_name)
            return
        element_names = state.pop(ImportKey.ELEMENT_NAMES, [])
        element_names.append(element_name)
        entity_class_name = state[ImportKey.ENTITY_CLASS_NAME]
        if ImportKey.ENTITY_NAME in state:
            entity_name = state[ImportKey.ENTITY_NAME]
            try:
                record = mapped_data["entities"][entity_class_name, entity_name]
            except KeyError:
                pass
            else:
                if all(name == existing_name for name, existing_name in zip(element_names, record.elements)):
                    return
                del state[ImportKey.ENTITY_NAME]
        record = EntityRecord(element_names)
        byname = tuple(record.elements)
        mapped_entities = mapped_data.setdefault("entities", {})
        mapped_entities[entity_class_name, byname] = record
        state[ImportKey.ENTITY_NAME] = byname
        if self.import_entities:
            mapped_classes = mapped_data["entity_classes"]
            class_record = mapped_classes[entity_class_name]
            for element_name, dimension_name in zip(element_names, class_record.dimensions):
                if dimension_name not in mapped_classes:
                    mapped_classes[dimension_name] = EntityClassRecord()
                if (dimension_name, element_name) not in mapped_entities:
                    mapped_entities[dimension_name, element_name] = EntityRecord()

    def check_validity(self) -> None:
        _require_enough_parents(self, DimensionMapping)


class MetadataNameMapping(ImportMapping):
    """Maps metadata names."""

    MAP_TYPE = "MetadataName"

    def _import_row(self, source_data, state, mapped_data):
        state[ImportKey.METADATA_NAME] = str(source_data)


class MetadataValueMapping(ImportMapping):
    """Maps metadata values."""

    MAP_TYPE = "MetadataValue"

    def _import_row(self, source_data, state, mapped_data):
        metadata_name = state[ImportKey.METADATA_NAME]
        metadata_value = state[ImportKey.METADATA_VALUE] = str(source_data)
        mapped_data.setdefault("metadata", []).append((metadata_name, metadata_value))

    def check_validity(self) -> None:
        _require_parent(self, MetadataNameMapping)


T = TypeVar("T")


@dataclass
class ValueRecord(Generic[T]):
    index_names: list[str] = field(default_factory=list)
    indexes: list[list] = field(default_factory=list)
    values: list[T] = field(default_factory=list)

    def has_value(self) -> bool:
        return bool(self.values)


@dataclass
class ArrayValueRecord(ValueRecord[Any]):
    pass


@dataclass
class TimePatternValueRecord(ValueRecord[float]):
    pass


@dataclass
class MapValueRecord(ValueRecord[Any]):
    compress: bool = False


@dataclass
class TimeSeriesValueRecord(ValueRecord[float]):
    ignore_year: bool = False
    repeat: bool = False
    indexes: list = field(default_factory=list)


@dataclass
class ParameterDefinitionRecord:
    value_list_name: str | None = None
    default_value: ValueRecord | None = None
    description: str | None = None


class ParameterDefinitionMapping(ImportMapping):
    """Maps parameter definitions."""

    MAP_TYPE = "ParameterDefinition"

    def _import_row(self, source_data, state, mapped_data):
        entity_class_name = state.get(ImportKey.ENTITY_CLASS_NAME)
        parameter_name = state[ImportKey.PARAMETER_NAME] = str(source_data)
        parameter_definition_key = entity_class_name, parameter_name
        definitions = mapped_data.setdefault("parameter_definitions", {})
        if parameter_definition_key not in definitions:
            definitions[parameter_definition_key] = ParameterDefinitionRecord()

    def check_validity(self) -> None:
        _require_parent(self, EntityClassMapping)


class ParameterDefinitionDescriptionMapping(ImportMapping):
    """Maps parameter definition descriptions."""

    MAP_TYPE = "ParameterDefinitionDescription"
    ignorable = True

    def _import_row(self, source_data, state, mapped_data):
        description = str(source_data)
        if description:
            entity_class_name = state.get(ImportKey.ENTITY_CLASS_NAME)
            parameter_name = state[ImportKey.PARAMETER_NAME]
            mapped_data["parameter_definitions"][entity_class_name, parameter_name].description = description

    def check_validity(self) -> None:
        _require_parent(self, ParameterDefinitionMapping)


class ParameterTypeMapping(ImportMapping):
    """Maps parameter types."""

    MAP_TYPE = "ParameterType"

    def _import_row(self, source_data, state, mapped_data):
        parameter_type = str(source_data)
        if not parameter_type:
            return
        entity_class = state[ImportKey.ENTITY_CLASS_NAME]
        parameter = state[ImportKey.PARAMETER_NAME]
        mapped_data.setdefault("parameter_types", []).append((entity_class, parameter, parameter_type))

    def check_validity(self) -> None:
        _require_parent(self, ParameterDefinitionMapping)


class ParameterDefaultValueMapping(ImportMapping):
    """Maps scalar (non-indexed) default values."""

    MAP_TYPE = "ParameterDefaultValue"

    def _import_row(self, source_data, state, mapped_data):
        default_value = source_data
        if default_value == "":
            return
        entity_class_name = state[ImportKey.ENTITY_CLASS_NAME]
        parameter_name = state[ImportKey.PARAMETER_NAME]
        mapped_data["parameter_definitions"][entity_class_name, parameter_name].default_value = default_value

    def check_validity(self) -> None:
        _require_parent(self, ParameterDefinitionMapping)


class ParameterDefaultValueTypeMapping(IndexedValueMixin, ImportMapping):
    """Maps indexed default values."""

    MAP_TYPE = "ParameterDefaultValueType"

    def _import_row(self, source_data, state, mapped_data):
        entity_class_name = state[ImportKey.ENTITY_CLASS_NAME]
        parameter_name = state[ImportKey.PARAMETER_NAME]
        key = (entity_class_name, parameter_name)
        definition_record = mapped_data["parameter_definitions"][key]
        if definition_record.default_value is not None:
            state[ImportKey.PARAMETER_DEFAULT_VALUE_RECORD] = definition_record.default_value
            return
        record = self._make_value_record(source_data)
        definition_record.default_value = record
        state[ImportKey.PARAMETER_DEFAULT_VALUE_RECORD] = record

    def check_validity(self) -> None:
        _require_parent(self, ParameterDefinitionMapping)


class DefaultValueIndexNameMapping(ImportMapping):
    """Maps default value index names."""

    MAP_TYPE = "DefaultValueIndexName"

    def _import_row(self, source_data, state, mapped_data):
        if ImportKey.PARAMETER_DEFAULT_VALUE_INDEXES in state:
            i = len(state[ImportKey.PARAMETER_DEFAULT_VALUE_INDEXES])
            state.setdefault(ImportKey.PARAMETER_DEFAULT_VALUE_INDEX_NAMES, {})[i] = str(source_data)
        else:
            state[ImportKey.PARAMETER_DEFAULT_VALUE_INDEX_NAMES] = {0: str(source_data)}

    def check_validity(self) -> None:
        _require_parent(self, ParameterDefaultValueTypeMapping)


class ParameterDefaultValueIndexMapping(ImportMapping):
    """Maps default value indexes."""

    MAP_TYPE = "ParameterDefaultValueIndex"

    def _import_row(self, source_data, state, mapped_data):
        state.setdefault(ImportKey.PARAMETER_DEFAULT_VALUE_INDEXES, []).append(source_data)

    def check_validity(self) -> None:
        _require_parent(self, ParameterDefaultValueTypeMapping)
        _require_enough_parents(self, DefaultValueIndexNameMapping)


class ExpandedParameterDefaultValueMapping(ImportMapping):
    """Maps indexed default values.

    Whenever this mapping is a child of :class:`ParameterDefaultValueIndexMapping`, it maps individual values of
    indexed parameters.
    """

    MAP_TYPE = "ExpandedDefaultValue"

    def _import_row(self, source_data, state, mapped_data):
        record = state[ImportKey.PARAMETER_DEFAULT_VALUE_RECORD]
        record.values.append(source_data)
        try:
            record.indexes.append(state.pop(ImportKey.PARAMETER_DEFAULT_VALUE_INDEXES))
        except KeyError:
            pass
        try:
            index_names = state.pop(ImportKey.PARAMETER_DEFAULT_VALUE_INDEX_NAMES)
        except KeyError:
            pass
        else:
            if record.indexes:
                n_indexes = len(record.indexes[-1])
                if n_indexes == len(index_names):
                    record.index_names = list(index_names.values())
                else:
                    name_list = []
                    for i in range(n_indexes):
                        name_list.append(index_names.get(i))
                    record.index_names = name_list
            else:
                # Arrays
                record.index_names = [index_names[0]]

    def _skip_row(self, state):
        try:
            del state[ImportKey.PARAMETER_DEFAULT_VALUE_INDEXES]
        except KeyError:
            pass

    def check_validity(self) -> None:
        _require_parent(self, ParameterDefaultValueTypeMapping)


class ParameterValueMapping(ImportMapping):
    """Maps scalar (non-indexed) parameter values."""

    MAP_TYPE = "ParameterValue"

    def _import_row(self, source_data, state, mapped_data):
        if source_data == "":
            return
        entity_class_name = state.get(ImportKey.ENTITY_CLASS_NAME)
        entity_name = state[ImportKey.ENTITY_NAME]
        entity_byname = entity_name if isinstance(entity_name, tuple) else (entity_name,)
        parameter_name = state[ImportKey.PARAMETER_NAME]
        alternative_name = state.get(ImportKey.ALTERNATIVE_NAME)
        mapped_data.setdefault("parameter_values", {})[
            entity_class_name, entity_byname, parameter_name, alternative_name
        ] = source_data

    def check_validity(self) -> None:
        _require_parent(self, ParameterDefinitionMapping)
        _require_one_of_parents(self, (EntityMapping, ElementMapping))


class ParameterValueTypeMapping(IndexedValueMixin, ImportMapping):
    """Maps indexed parameter values."""

    MAP_TYPE = "ParameterValueType"

    def _import_row(self, source_data, state, mapped_data):
        entity_class_name = state.get(ImportKey.ENTITY_CLASS_NAME)
        entity_name = state[ImportKey.ENTITY_NAME]
        entity_byname = entity_name if isinstance(entity_name, tuple) else (entity_name,)
        parameter_name = state[ImportKey.PARAMETER_NAME]
        alternative_name = state.get(ImportKey.ALTERNATIVE_NAME)
        key = entity_class_name, entity_byname, parameter_name, alternative_name
        mapped_values = mapped_data.setdefault("parameter_values", {})
        if key in mapped_values:
            state[ImportKey.PARAMETER_VALUE_RECORD] = mapped_values[key]
            return
        record = self._make_value_record(source_data)
        mapped_values[key] = record
        state[ImportKey.PARAMETER_VALUE_RECORD] = record

    def check_validity(self) -> None:
        _require_parent(self, ParameterDefinitionMapping)
        _require_one_of_parents(self, (EntityMapping, ElementMapping))


class ParameterValueMetadataNameMapping(ImportMapping):
    """Maps parameter value metadata names."""

    MAP_TYPE = "ParameterValueMetadataName"
    ignorable = True

    def _import_row(self, source_data, state, mapped_data):
        state[ImportKey.PARAMETER_VALUE_METADATA_NAME] = str(source_data)


class ParameterValueMetadataValueMapping(ImportMapping):
    """Maps parameter value metadata values."""

    MAP_TYPE = "ParameterValueMetadataValue"
    ignorable = True

    def _import_row(self, source_data, state, mapped_data):
        entity_class_name = state[ImportKey.ENTITY_CLASS_NAME]
        entity_byname = _byname_from_mapped_data(entity_class_name, state, mapped_data)
        parameter_name = state[ImportKey.PARAMETER_NAME]
        alternative_name = state[ImportKey.ALTERNATIVE_NAME]
        metadata_name = state[ImportKey.PARAMETER_VALUE_METADATA_NAME]
        metadata_value = state[ImportKey.PARAMETER_VALUE_METADATA_VALUE] = str(source_data)
        mapped_data.setdefault("parameter_value_metadata", {})[
            entity_class_name, entity_byname, parameter_name, metadata_name, metadata_value, alternative_name
        ] = None

    def check_validity(self) -> None:
        _require_parent(self, ParameterValueMetadataNameMapping)


class ParameterValueIndexMapping(ImportMapping):
    """Maps parameter value indexes."""

    MAP_TYPE = "ParameterValueIndex"

    def _import_row(self, source_data, state, mapped_data):
        state.setdefault(ImportKey.PARAMETER_VALUE_INDEXES, []).append(source_data)

    def check_validity(self) -> None:
        _require_parent(self, ParameterValueTypeMapping)
        _require_enough_parents(self, IndexNameMapping)


class IndexNameMapping(ImportMapping):
    """Maps index names for indexed parameter values."""

    MAP_TYPE = "IndexName"

    def _import_row(self, source_data, state, mapped_data):
        if ImportKey.PARAMETER_VALUE_INDEXES in state:
            i = len(state[ImportKey.PARAMETER_VALUE_INDEXES])
            state.setdefault(ImportKey.PARAMETER_VALUE_INDEX_NAMES, {})[i] = str(source_data)
        else:
            state[ImportKey.PARAMETER_VALUE_INDEX_NAMES] = {0: str(source_data)}

    def check_validity(self) -> None:
        _require_parent(self, ParameterValueTypeMapping)


class ExpandedParameterValueMapping(ImportMapping):
    """Maps parameter values.

    Whenever this mapping is a child of :class:`ParameterValueIndexMapping`, it maps individual values of indexed
    parameters.
    """

    MAP_TYPE = "ExpandedValue"

    def _import_row(self, source_data, state, mapped_data):
        record = state[ImportKey.PARAMETER_VALUE_RECORD]
        record.values.append(source_data)
        try:
            record.indexes.append(state.pop(ImportKey.PARAMETER_VALUE_INDEXES))
        except KeyError:
            pass
        try:
            index_names = state.pop(ImportKey.PARAMETER_VALUE_INDEX_NAMES)
        except KeyError:
            pass
        else:
            if record.indexes:
                n_indexes = len(record.indexes[-1])
                if n_indexes == len(index_names):
                    record.index_names = list(index_names.values())
                else:
                    name_list = []
                    for i in range(n_indexes):
                        name_list.append(index_names.get(i))
                    record.index_names = name_list
            else:
                # Arrays
                record.index_names = [index_names[0]]

    def _skip_row(self, state):
        try:
            del state[ImportKey.PARAMETER_VALUE_INDEXES]
        except KeyError:
            pass

    def check_validity(self) -> None:
        _require_parent(self, ParameterValueTypeMapping)


class ParameterValueListMapping(ImportMapping):
    """Maps parameter value list names."""

    MAP_TYPE = "ParameterValueList"

    def _import_row(self, source_data, state, mapped_data):
        value_list_name = str(source_data)
        if not value_list_name:
            return
        state[ImportKey.PARAMETER_VALUE_LIST_NAME] = value_list_name
        if ImportKey.PARAMETER_NAME in state:
            parameter_name = state[ImportKey.PARAMETER_NAME]
            entity_class_name = state[ImportKey.ENTITY_CLASS_NAME]
            mapped_data["parameter_definitions"][entity_class_name, parameter_name].value_list_name = value_list_name


class ParameterValueListValueMapping(ImportMapping):
    """Maps parameter value list values."""

    MAP_TYPE = "ParameterValueListValue"

    def _import_row(self, source_data, state, mapped_data):
        list_value = source_data
        if list_value == "":
            return
        value_list_name = state[ImportKey.PARAMETER_VALUE_LIST_NAME]
        mapped_data.setdefault("parameter_value_lists", []).append([value_list_name, list_value])

    def check_validity(self) -> None:
        _require_parent(self, ParameterValueListMapping)


class AlternativeMapping(ImportMapping):
    """Maps alternatives."""

    MAP_TYPE = "Alternative"

    def _import_row(self, source_data, state, mapped_data):
        alternative = state[ImportKey.ALTERNATIVE_NAME] = str(source_data)
        mapped_data.setdefault("alternatives", set()).add(alternative)


class AlternativeDescriptionMapping(ImportMapping):
    """Maps alternative descriptions."""

    MAP_TYPE = "AlternativeDescription"
    ignorable = True

    def _import_row(self, source_data, state, mapped_data):
        description = str(source_data)
        if description:
            alternative = state[ImportKey.ALTERNATIVE_NAME]
            alternative_data = mapped_data["alternatives"]
            alternative_data.discard(alternative)
            alternative_data.add((alternative, description))

    def check_validity(self) -> None:
        _require_parent(self, AlternativeMapping)


class ScenarioMapping(ImportMapping):
    """Maps scenarios."""

    MAP_TYPE = "Scenario"

    def _import_row(self, source_data, state, mapped_data):
        scenario = str(source_data)
        state[ImportKey.SCENARIO_NAME] = scenario
        mapped_data.setdefault("scenarios", set()).add((scenario,))


class ScenarioAlternativeMapping(ImportMapping):
    """Maps scenario alternatives."""

    MAP_TYPE = "ScenarioAlternative"

    def _import_row(self, source_data, state, mapped_data):
        alternative = str(source_data)
        if not alternative:
            return
        scenario = state[ImportKey.SCENARIO_NAME]
        scen_alt = state[ImportKey.SCENARIO_ALTERNATIVE] = [scenario, alternative]
        mapped_data.setdefault("scenario_alternatives", []).append(scen_alt)

    def check_validity(self) -> None:
        _require_parent(self, ScenarioMapping)


class ScenarioBeforeAlternativeMapping(ImportMapping):
    """Maps scenario 'before' alternatives."""

    MAP_TYPE = "ScenarioBeforeAlternative"

    def _import_row(self, source_data, state, mapped_data):
        scen_alt = state[ImportKey.SCENARIO_ALTERNATIVE]
        alternative = str(source_data)
        scen_alt.append(alternative)

    def check_validity(self) -> None:
        _require_parent(self, ScenarioAlternativeMapping)


class ScenarioDescriptionMapping(ImportMapping):
    """Maps scenario descriptions."""

    MAP_TYPE = "ScenarioDescription"
    ignorable: ClassVar[bool] = True

    def _import_row(self, source_data, state, mapped_data):
        description = str(source_data)
        if description:
            scenario = state[ImportKey.SCENARIO_NAME]
            scenario_data = mapped_data["scenarios"]
            scenario_data.discard((scenario,))
            scenario_data.add((scenario, description))

    def check_validity(self) -> None:
        _require_parent(self, ScenarioMapping)


def default_import_mapping(map_type: str) -> ImportMapping:
    """Creates default mappings for given map type.

    Args:
        map_type: map type

    Returns:
        root mapping of desired type
    """
    make_root_mapping = {
        EntityClassMapping.MAP_TYPE: _default_entity_class_mapping,
        AlternativeMapping.MAP_TYPE: _default_alternative_mapping,
        ScenarioMapping.MAP_TYPE: _default_scenario_mapping,
        ScenarioAlternativeMapping.MAP_TYPE: _default_scenario_alternative_mapping,
        EntityGroupMapping.MAP_TYPE: _default_entity_group_mapping,
        ParameterValueListMapping.MAP_TYPE: _default_parameter_value_list_mapping,
        MetadataNameMapping.MAP_TYPE: _default_metadata_mapping,
        EntityMetadataNameMapping.MAP_TYPE: _default_entity_metadata_mapping,
        ParameterValueMetadataNameMapping.MAP_TYPE: _default_parameter_value_metadata_mapping,
    }[map_type]
    return make_root_mapping()


def _default_entity_class_mapping() -> EntityClassMapping:
    """Creates default entity class mappings.

    Returns:
        root mapping
    """
    root_mapping = EntityClassMapping(Position.hidden)
    description_mapping = root_mapping.child = EntityClassDescriptionMapping(Position.hidden)
    entity_mapping = description_mapping.child = EntityMapping(Position.hidden)
    entity_mapping.child = EntityDescriptionMapping(Position.hidden)
    return root_mapping


def _default_alternative_mapping() -> AlternativeMapping:
    """Creates default alternative mappings.

    Returns:
        root mapping
    """
    root_mapping = AlternativeMapping(Position.hidden)
    root_mapping.child = AlternativeDescriptionMapping(Position.hidden)
    return root_mapping


def _default_scenario_mapping() -> ScenarioMapping:
    """Creates default scenario mappings.

    Returns:
        root mapping
    """
    root_mapping = ScenarioMapping(Position.hidden)
    root_mapping.child = ScenarioDescriptionMapping(Position.hidden)
    return root_mapping


def _default_scenario_alternative_mapping() -> ScenarioMapping:
    """Creates default scenario alternative mappings.

    Returns:
        root mapping
    """
    root_mapping = ScenarioMapping(Position.hidden)
    root_mapping.child = ScenarioAlternativeMapping(Position.hidden)
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


def _default_metadata_mapping() -> MetadataNameMapping:
    root_mapping = MetadataNameMapping(Position.hidden)
    root_mapping.child = MetadataValueMapping(Position.hidden)
    return root_mapping


def _default_entity_metadata_mapping() -> EntityClassMapping:
    mappings = [
        EntityClassMapping(Position.hidden),
        EntityMapping(Position.hidden),
        EntityMetadataNameMapping(Position.hidden),
        EntityMetadataValueMapping(Position.hidden),
    ]
    return unflatten(mappings)


def _default_parameter_value_metadata_mapping() -> EntityClassMapping:
    mappings = [
        EntityClassMapping(Position.hidden),
        EntityMapping(Position.hidden),
        ParameterDefinitionMapping(Position.hidden),
        AlternativeMapping(Position.hidden),
        ParameterValueMetadataNameMapping(Position.hidden),
        ParameterValueMetadataValueMapping(Position.hidden),
    ]
    return unflatten(mappings)


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
            EntityClassDescriptionMapping,
            EntityMapping,
            EntityDescriptionMapping,
            EntityMetadataNameMapping,
            EntityMetadataValueMapping,
            EntityGroupMapping,
            DimensionMapping,
            ElementMapping,
            EntityAlternativeActivityMapping,
            ParameterDefinitionMapping,
            ParameterDefinitionDescriptionMapping,
            ParameterTypeMapping,
            ParameterDefaultValueMapping,
            ParameterDefaultValueTypeMapping,
            DefaultValueIndexNameMapping,
            ParameterDefaultValueIndexMapping,
            ExpandedParameterDefaultValueMapping,
            ParameterValueMapping,
            ParameterValueTypeMapping,
            ParameterValueMetadataNameMapping,
            ParameterValueMetadataValueMapping,
            IndexNameMapping,
            ParameterValueIndexMapping,
            ExpandedParameterValueMapping,
            ParameterValueListMapping,
            ParameterValueListValueMapping,
            MetadataNameMapping,
            MetadataValueMapping,
            AlternativeMapping,
            AlternativeDescriptionMapping,
            ScenarioMapping,
            ScenarioAlternativeMapping,
            ScenarioBeforeAlternativeMapping,
            ScenarioDescriptionMapping,
        )
    }
    legacy_mappings = {
        "ParameterIndex": ParameterValueIndexMapping,
        "ObjectClass": EntityClassMapping,
        "Object": EntityMapping,
        "ObjectGroup": EntityGroupMapping,
        "RelationshipClass": EntityClassMapping,
        "RelationshipClassObjectClass": DimensionMapping,
        "Relationship": EntityMapping,
        "RelationshipObject": ElementMapping,
    }
    mappings.update(legacy_mappings)
    flattened = []
    for mapping_dict in serialized:
        if mapping_dict["map_type"] in {
            "EntityMetadata",
            "ObjectMetadata",
            "RelationshipMetadata",
            "ParameterValueMetadata",
            "ScenarioActiveFlag",
        }:
            # We don't have JSON blob metadata nor active flag mappings anymore.
            continue
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


def _parameter_value_key(state: State, mapped_data: SemiMappedData) -> tuple[str, tuple[str, ...], str, str]:
    """Creates parameter value's key from current state.

    Args:
        state: import state

    Returns:
        class name, entity byname, parameter name, and alternative name
    """
    entity_class_name = state.get(ImportKey.ENTITY_CLASS_NAME)
    entity_byname = _byname_from_mapped_data(entity_class_name, state, mapped_data)
    parameter_name = state[ImportKey.PARAMETER_NAME]
    alternative_name = state.get(ImportKey.ALTERNATIVE_NAME)
    return entity_class_name, entity_byname, parameter_name, alternative_name


def _default_value_key(state: State) -> tuple[str, str]:
    """Creates parameter default value's key from current state.

    Args:
        state: import state

    Returns:
        class name and parameter name
    """
    return state[ImportKey.ENTITY_CLASS_NAME], state[ImportKey.PARAMETER_NAME]


def _byname_from_mapped_data(entity_class_name: str, state: State, mapped_data: SemiMappedData) -> tuple[str, ...]:
    entity_name = state[ImportKey.ENTITY_NAME]
    entity_record = mapped_data["entities"][entity_class_name, entity_name]
    return tuple(entity_record.elements) if entity_record.elements else (entity_name,)
