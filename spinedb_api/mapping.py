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

"""Base class for import and export mappings."""

from __future__ import annotations
from enum import Enum, unique
from itertools import takewhile
import re
from typing import Any, ClassVar, Optional
from spinedb_api import InvalidMapping

_TABLEFUL_FIXED_POSITION_RE = re.compile(r"^\s*(.+):\s*(\d+)\s*,\s*(\d+)\s*$")
_TABLELESS_FIXED_POSITION_RE = re.compile(r"^\s*(\d+)\s*,\s*(\d+)\s*$")


@unique
class Position(Enum):
    """Item positions when they are not in columns."""

    hidden = "hidden"
    table_name = "table_name"
    header = "header"
    mapping_name = "mapping_name"
    fixed = "fixed"


def is_pivoted(position: Position | int) -> bool:
    """Checks if position is pivoted.

    Args:
        position: position

    Returns:
        True if position is pivoted, False otherwise
    """
    return isinstance(position, int) and position < 0


def is_regular(position: Position | int) -> bool:
    """Checks if position is column index.

    Args:
        position: position

    Returns:
        True if position is a column index, False otherwise
    """
    return isinstance(position, int) and position >= 0


def parse_fixed_position_value(value: str) -> tuple[Optional[str], int, int]:
    """Parses mapping's value for ``Position.fixed`` to table name, row and column."""
    match = re.search(_TABLEFUL_FIXED_POSITION_RE, value)
    if match:
        return match.group(1), int(match.group(2)), int(match.group(3))
    match = re.search(_TABLELESS_FIXED_POSITION_RE, value)
    if match:
        return None, int(match.group(1)), int(match.group(2))
    raise InvalidMapping(f"failed to parse fixed position '{value}', expected 'table_name: row, column")


def unparse_fixed_position_value(table_name: Optional[str], row: int, column: int) -> str:
    return (f"{table_name}: " if table_name is not None else "") + f"{row}, {column}"


class Mapping:
    """Base class for import/export item mappings.

    Attributes:
        position: defines where the data is written/read in the output table.
            Nonnegative numbers are columns, negative numbers are pivot rows, and then there are some special cases
            in the Position enum.
        parent: Another mapping that's the 'parent' of this one.
            Used to determine if a mapping is root, in which case it needs to yield the header.
    """

    MAP_TYPE: ClassVar[str] = NotImplemented
    """Mapping type identifier for serialization."""

    def __init__(self, position: Position | int, value: Any = None, filter_re: str = ""):
        """
        Args:
            position: column index or Position
            value: fixed value
            filter_re: regular expression for filtering
        """
        self._child: Mapping | None = None
        self._value = None
        self._unfixed_value_data = self._data
        self._filter_re = None
        self.parent: Mapping | None = None
        self.position = position
        self.value = value
        self.filter_re = filter_re

    @property
    def child(self) -> Mapping | None:
        return self._child

    @child.setter
    def child(self, child: Mapping | None) -> None:
        self._child = child
        if isinstance(child, Mapping):
            child.parent = self

    @property
    def value(self) -> Any:
        """Fixed value."""
        return self._value

    @value.setter
    def value(self, value: Any) -> None:
        self._value = value
        self._set_fixed_value_data()

    @property
    def filter_re(self) -> str:
        return self._filter_re.pattern if self._filter_re is not None else ""

    @filter_re.setter
    def filter_re(self, filter_re):
        self._filter_re = re.compile(filter_re) if filter_re else None

    def _data(self, row: int) -> Any:
        raise NotImplementedError()

    def _fixed_value_data(self, _row: int) -> Any:
        return self._value

    def _set_fixed_value_data(self) -> None:
        if self._value is None:
            self._data = self._unfixed_value_data
            return
        self._data = self._fixed_value_data

    def __eq__(self, other):
        if not isinstance(other, Mapping):
            return NotImplemented
        return (
            self.MAP_TYPE == other.MAP_TYPE
            and self.position == other.position
            and self.child == other.child
            and self._filter_re == other._filter_re
        )

    def tail_mapping(self) -> Mapping:
        """Returns the last mapping in the chain.

        Returns:
            last child mapping
        """
        if self._child is None:
            return self
        return self._child.tail_mapping()

    def count_mappings(self) -> int:
        """
        Counts this and child mappings.

        Returns:
            number of mappings
        """
        return 1 + (self.child.count_mappings() if self.child is not None else 0)

    def flatten(self) -> list[Mapping]:
        """
        Flattens the mapping tree.

        Returns:
            mappings in parent-child-grand child-etc order
        """
        return [self] + (self.child.flatten() if self.child is not None else [])

    def is_effective_leaf(self) -> bool:
        """Tests if mapping is effectively the leaf mapping.

        Returns:
            True if mapping is effectively the last child, False otherwise
        """
        return self._child is None or all(
            child.position in (Position.hidden, Position.table_name) for child in self._child.flatten()[:-1]
        )

    def is_pivoted(self) -> bool:
        """
        Queries recursively if export items are pivoted.

        Returns:
            True if any of the items is pivoted, False otherwise
        """
        if is_pivoted(self.position):
            return True
        if self.child is None:
            return False
        return self.child.is_pivoted()

    def non_pivoted_width(self, parent_is_pivoted: bool = False) -> int:
        """
        Calculates columnar width of non-pivoted data.

        Args:
            parent_is_pivoted: True if a parent item is pivoted, False otherwise

        Returns:
            non-pivoted data width
        """
        if self.child is None:
            if is_regular(self.position) and not parent_is_pivoted:
                return self.position + 1
            return 0
        width = self.position + 1 if is_regular(self.position) else 0
        return max(width, self.child.non_pivoted_width(parent_is_pivoted or is_pivoted(self.position)))

    def non_pivoted_columns(self, parent_is_pivoted: bool = False) -> list[int]:
        """Gathers non-pivoted columns from mappings.

        Args:
            parent_is_pivoted: True if a parent item is pivoted, False otherwise

        Returns:
            indexes of non-pivoted columns
        """
        if self._child is None:
            if is_regular(self.position) and not parent_is_pivoted:
                return [self.position]
            return []
        pivoted = is_pivoted(self.position)
        return ([self.position] if is_regular(self.position) else []) + self._child.non_pivoted_columns(
            parent_is_pivoted or pivoted
        )

    def last_pivot_row(self) -> int:
        return max(
            (-(m.position + 1) for m in self.flatten() if isinstance(m.position, int) and m.position < 0), default=-1
        )

    def query_parents(self, what: str) -> Any:
        """Queries parent mapping for specific information.

        Args:
            what: query identifier

        Returns:
            query result or None if no parent recognized the identifier
        """
        if self.parent is None:
            return None
        return self.parent.query_parents(what)

    def to_dict(self) -> dict:
        """
        Serializes mapping into dict.

        Returns:
            serialized mapping
        """
        position = self.position.value if isinstance(self.position, Position) else self.position
        mapping_dict = {"map_type": self.MAP_TYPE, "position": position}
        if self.value is not None:
            mapping_dict["value"] = self.value
        if self._filter_re is not None:
            mapping_dict["filter_re"] = self._filter_re.pattern
        return mapping_dict


def unflatten(mappings: list[Mapping]) -> Mapping:
    """
    Builds a mapping hierarchy from flattened mappings.

    Args:
        mappings: flattened mappings

    Returns:
        root mapping
    """
    root = None
    current = None
    for mapping in mappings:
        if root is None:
            root = mapping
        else:
            current.child = mapping
        current = mapping
    current.child = None
    return root


def value_index(flattened_mappings: list[Mapping]) -> int:
    """
    Returns index of last non-hidden mapping in flattened mapping list.

    Args:
        flattened_mappings: flattened mappings

    Returns:
        value mapping index
    """
    return (
        len(flattened_mappings)
        - 1
        - len(list(takewhile(lambda m: m.position == Position.hidden, reversed(flattened_mappings))))
    )


def to_dict(root_mapping: Mapping) -> list[dict]:
    """
    Serializes mappings into JSON compatible data structure.

    Args:
        root_mapping: root mapping

    Returns:
        serialized mappings
    """
    return list(mapping.to_dict() for mapping in root_mapping.flatten())
