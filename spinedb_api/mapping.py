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

from enum import Enum, unique


@unique
class Position(Enum):
    """Export item positions when they are not in columns."""

    hidden = "hidden"
    single_row = "single_row"
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
        value (any): fixed value
        parent (Mapping or None): Another mapping that's the 'parent' of this one.
            Used to determine if a mapping is root, in which case it needs to yield the header.
    """

    MAP_TYPE = None
    """Mapping type identifier for serialization."""

    def __init__(self, position, value=None):
        """
        Args:
            position (int or Position): column index or Position
            value (any): fixed value
        """
        self._child = None
        self._value = None
        self._unfixed_value_data = self._data
        self.parent = None
        self.position = position
        self.value = value

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
        return self._value

    @value.setter
    def value(self, value):
        self._value = value
        self._set_fixed_value_data()

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
        return self.MAP_TYPE == other.MAP_TYPE and self.position == other.position and self.child == other.child

    def can_drop(self):
        """
        Returns True if mapping is just dead weight and can be removed.

        Returns:
            bool: True if mapping is leaf mapping and has no position set
        """
        return self.child is None and self.position == Position.hidden

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
        if self.child is not None:
            if is_pivoted(self.position):
                return True
            return self.child.is_pivoted()
        return False

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

    def non_pivoted_columns(self):
        return [m.position for m in self.flatten() if isinstance(m.position, int) and m.position >= 0]

    def last_pivot_row(self):
        return max(
            [-(m.position + 1) for m in self.flatten() if isinstance(m.position, int) and m.position < 0], default=0
        )

    def to_dict(self):
        """
        Serializes mapping into dict.

        Returns:
            dict: serialized mapping
        """
        position = self.position.value if isinstance(self.position, Position) else self.position
        mapping_dict = {"map_type": self.MAP_TYPE, "position": position}
        if self.value:
            mapping_dict["value"] = self.value
        return mapping_dict

    @classmethod
    def reconstruct(cls, position, ignorable, mapping_dict):
        """
        Reconstructs mapping.

        Args:
            position (int or Position, optional): mapping's position
            ignorable (bool): ignorable flag
            mapping_dict (dict): serialized mapping

        Returns:
            Mapping: reconstructed mapping
        """
        value = mapping_dict.get("value")
        mapping = cls(position, value=value)
        if ignorable:
            mapping.set_ignorable()
        return mapping


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
