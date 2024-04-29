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

from enum import Enum, unique
from itertools import takewhile
import re


@unique
class Position(Enum):
    """Export item positions when they are not in columns."""

    hidden = "hidden"
    table_name = "table_name"
    header = "header"


def is_pivoted(position):
    """Checks if position is pivoted.

    Args:
        position (Position or int): position

    Returns:
        bool: True if position is pivoted, False otherwise
    """
    return isinstance(position, int) and position < 0


def is_regular(position):
    """Checks if position is column index.

    Args:
        position (Position or int): position

    Returns:
        bool: True if position is a column index, False otherwise
    """
    return isinstance(position, int) and position >= 0


class Mapping:
    """Base class for import/export item mappings.

    Attributes:
        position (int or Position): defines where the data is written/read in the output table.
            Nonnegative numbers are columns, negative numbers are pivot rows, and then there are some special cases
            in the Position enum.
        parent (Mapping or None): Another mapping that's the 'parent' of this one.
            Used to determine if a mapping is root, in which case it needs to yield the header.
    """

    MAP_TYPE = None
    """Mapping type identifier for serialization."""

    def __init__(self, position, value=None, filter_re=""):
        """
        Args:
            position (int or Position): column index or Position
            value (Any): fixed value
            filter_re (str): regular expression for filtering
        """
        self._child = None
        self._value = None
        self._unfixed_value_data = self._data
        self._filter_re = None
        self.parent = None
        self.position = position
        self.value = value
        self.filter_re = filter_re

    @property
    def child(self):
        return self._child

    @child.setter
    def child(self, child):
        self._child = child
        if isinstance(child, Mapping):
            child.parent = self

    @property
    def value(self):
        """Fixed value."""
        return self._value

    @value.setter
    def value(self, value):
        self._value = value
        self._set_fixed_value_data()

    @property
    def filter_re(self):
        return self._filter_re.pattern if self._filter_re is not None else ""

    @filter_re.setter
    def filter_re(self, filter_re):
        self._filter_re = re.compile(filter_re) if filter_re else None

    def _data(self, row):
        raise NotImplementedError()

    def _fixed_value_data(self, _row):
        return self._value

    def _set_fixed_value_data(self):
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

    def tail_mapping(self):
        """Returns the last mapping in the chain.

        Returns:
            Mapping: last child mapping
        """
        if self._child is None:
            return self
        return self._child.tail_mapping()

    def count_mappings(self):
        """
        Counts this and child mappings.

        Returns:
            int: number of mappings
        """
        return 1 + (self.child.count_mappings() if self.child is not None else 0)

    def flatten(self):
        """
        Flattens the mapping tree.

        Returns:
            list of Mapping: mappings in parent-child-grand child-etc order
        """
        return [self] + (self.child.flatten() if self.child is not None else [])

    def is_pivoted(self):
        """
        Queries recursively if export items are pivoted.

        Returns:
            bool: True if any of the items is pivoted, False otherwise
        """
        if is_pivoted(self.position):
            return True
        if self.child is None:
            return False
        return self.child.is_pivoted()

    def non_pivoted_width(self, parent_is_pivoted=False):
        """
        Calculates columnar width of non-pivoted data.

        Args:
            parent_is_pivoted (bool): True if a parent item is pivoted, False otherwise

        Returns:
            int: non-pivoted data width
        """
        if self.child is None:
            if is_regular(self.position) and not parent_is_pivoted:
                return self.position + 1
            return 0
        width = self.position + 1 if is_regular(self.position) else 0
        return max(width, self.child.non_pivoted_width(parent_is_pivoted or is_pivoted(self.position)))

    def non_pivoted_columns(self, parent_is_pivoted=False):
        """Gathers non-pivoted columns from mappings.

        Args:
            parent_is_pivoted (bool): True if a parent item is pivoted, False otherwise

        Returns:
            list of int: indexes of non-pivoted columns
        """
        if self._child is None:
            if is_regular(self.position) and not parent_is_pivoted:
                return [self.position]
            return []
        pivoted = is_pivoted(self.position)
        return ([self.position] if is_regular(self.position) else []) + self._child.non_pivoted_columns(
            parent_is_pivoted or pivoted
        )

    def last_pivot_row(self):
        return max(
            [-(m.position + 1) for m in self.flatten() if isinstance(m.position, int) and m.position < 0], default=-1
        )

    def query_parents(self, what):
        """Queries parent mapping for specific information.

        Args:
            what (str): query identifier

        Returns:
            Any: query result or None if no parent recognized the identifier
        """
        if self.parent is None:
            return None
        return self.parent.query_parents(what)

    def to_dict(self):
        """
        Serializes mapping into dict.

        Returns:
            dict: serialized mapping
        """
        position = self.position.value if isinstance(self.position, Position) else self.position
        mapping_dict = {"map_type": self.MAP_TYPE, "position": position}
        if self.value is not None:
            mapping_dict["value"] = self.value
        if self._filter_re is not None:
            mapping_dict["filter_re"] = self._filter_re.pattern
        return mapping_dict


def unflatten(mappings):
    """
    Builds a mapping hierarchy from flattened mappings.

    Args:
        mappings (Iterable of Mapping): flattened mappings

    Returns:
        Mapping: root mapping
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


def value_index(flattened_mappings):
    """
    Returns index of last non-hidden mapping in flattened mapping list.

    Args:
        flattened_mappings (list of Mapping): flattened mappings

    Returns:
        int: value mapping index
    """
    return (
        len(flattened_mappings)
        - 1
        - len(list(takewhile(lambda m: m.position == Position.hidden, reversed(flattened_mappings))))
    )


def to_dict(root_mapping):
    """
    Serializes mappings into JSON compatible data structure.

    Args:
        root_mapping (Mapping): root mapping

    Returns:
        list: serialized mappings
    """
    return list(mapping.to_dict() for mapping in root_mapping.flatten())
