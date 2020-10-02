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
Contains functions and methods to turn a regular export table into a pivot table

:author: A. Soininen (VTT)
:date:   1.2.2021
"""
from copy import deepcopy
from .item_export_mapping import is_regular, is_pivoted, Position, unflatten


def make_pivot(table, value_column, regular_columns, hidden_columns, pivot_columns):
    """Turns a regular table into a pivot table.

    Args:
        table (list of list): table to convert
        value_column (int): index of data column in ``table``
        regular_columns (Iterable of int): indexes of non-pivoted columns in ``table``
        hidden_columns (Iterable of int): indexes of columns that will not show on the pivot table
        pivot_columns (Iterable of int): indexes of pivoted columns in ``table``

    Yields:
        list: pivoted table row
    """

    def leaf(nested, keys):
        """Returns leaf element from nested dict or None if not found

        Args:
            nested (dict): a nested dictionary
            keys (Sequence): dictionary keys that identify the leaf element

        Returns:
            Any: the leaf element, or None if not found
        """
        if not keys:
            return nested
        v = nested.get(keys[0])
        if v is None:
            return None
        if isinstance(v, dict):
            return leaf(v, keys[1:])
        return v

    def regular_key_rows_and_non_pivoted_keys():
        """Creates pivot table's 'left' side rows and non pivoted keys.

        Returns:
            tuple: regular key rows and non pivoted keys
        """
        column_keys_width = max(regular_columns) + 1 if regular_columns else 0
        keys = set()
        key_rows = dict()
        for row in table:
            hidden_key = tuple(row[c] for c in hidden_columns)
            regular_key = tuple(row[c] for c in regular_columns)
            key_row = column_keys_width * [None]
            for c, k in zip(regular_columns, regular_key):
                key_row[c] = k
            key = regular_key + hidden_key
            key_rows[key] = key_row
            keys.add(key)
        return key_rows, sorted(keys)

    def value_tree():
        """Indexes pivot values.

        Returns:
            dict: a nested dictionary mapping keys to pivot values
        """
        tree = dict()
        for row in table:
            branch = tree
            for c in non_pivot_columns + pivot_columns[:-1]:
                branch = branch.setdefault(row[c], dict())
            branch[row[pivot_columns[-1]]] = row[value_column]
        return tree

    def half_pivot():
        """Builds a 'half' pivot table that is missing the left columns.

        Yields:
            list: table row
        """
        for i in range(len(pivot_columns)):
            yield list(k[i] for k in pivot_keys)
        values = dict()
        for row in table:
            branch = values
            for c in pivot_columns[:-1]:
                branch = branch.setdefault(row[c], dict())
            branch.setdefault(row[pivot_columns[-1]], list()).append(row[value_column])
        height = max(len(leaf(values, key)) for key in pivot_keys)
        for i in range(height):
            row = list()
            for key in pivot_keys:
                v = leaf(values, key)
                if i < len(v):
                    row.append(v[i])
                else:
                    row.append(None)
            yield row

    pivot_keys = sorted({tuple(row[i] for i in pivot_columns) for row in table})
    non_pivot_columns = regular_columns + hidden_columns
    if non_pivot_columns:
        row_key_rows, non_pivot_keys = regular_key_rows_and_non_pivoted_keys()
        header_front_padding = max(regular_columns) + 1 if regular_columns else 0
        for i in range(len(pivot_columns)):
            yield header_front_padding * [None] + list(k[i] for k in pivot_keys)
        values = value_tree()
        for row_key in non_pivot_keys:
            pivot_branch = leaf(values, row_key)
            row = row_key_rows[row_key]
            row += [leaf(pivot_branch, column_key) for column_key in pivot_keys]
            yield row
    else:
        for row in half_pivot():
            yield row


def make_regular(root_mapping):
    """
    Makes a regular (non-pivoted) table out of pivoted mapping by giving column indexes to pivoted and hidden positions.

    Useful when preparing data for pivoting.

    Args:
        root_mapping (Mapping): root mapping

    Returns:
        tuple: non-pivoted root mapping, value column, regular columns, hidden columns, pivot columns
    """
    mappings = deepcopy(root_mapping).flatten()
    regular_columns = [m.position for m in mappings[:-1] if is_regular(m.position)]
    regular_column_count = max(regular_columns) + 1 if regular_columns else 0
    pivoted_positions = sorted((m.position for m in mappings[:-1] if is_pivoted(m.position)), reverse=True)
    pivot_position_to_row = {position: i for i, position in enumerate(pivoted_positions)}
    pivot_column_count = len(pivot_position_to_row)
    regularized = list()
    pivot_column_base = regular_column_count
    pivot_columns = list()
    hidden_column_base = pivot_column_base + pivot_column_count
    current_hidden_column = 0
    hidden_columns = list()
    for mapping in mappings[:-1]:
        position = mapping.position
        if is_regular(position):
            regularized.append(mapping)
        elif is_pivoted(position):
            mapping.position = pivot_column_base + pivot_position_to_row[mapping.position]
            pivot_columns.append(mapping.position)
        elif position == Position.hidden:
            column = hidden_column_base + current_hidden_column
            mapping.position = column
            hidden_columns.append(column)
            current_hidden_column += 1
    value_column = hidden_column_base + current_hidden_column + 1
    mappings[-1].position = value_column
    return unflatten(mappings), value_column, regular_columns, hidden_columns, sorted(pivot_columns)
