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
from .group_functions import from_str as group_function_from_str, NoGroup


def make_pivot(table, value_column, regular_columns, hidden_columns, pivot_columns, group_fn=None):
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

    def regular_rows():
        """Creates pivot table's 'left' side rows and non pivoted keys.

        Returns:
            dict: mapping non-pivoted keys to regular rows
        """
        regular_rows = dict()
        for row in table:
            regular_key = tuple(row[c] for c in key_columns)
            regular_row = [row[i] for i in range(regular_column_width)]
            regular_rows[regular_key] = regular_row
        return regular_rows

    def value_tree():
        """Indexes pivot values.

        Returns:
            dict: a nested dictionary mapping keys to pivot values
        """
        tree = dict()
        for row in table:
            branch = tree
            for c in key_columns + pivot_columns[:-1]:
                branch = branch.setdefault(row[c], dict())
            # If not grouping, the list below will have exactly one element
            # If grouping, it will have all the elements that need to be grouped
            values = branch.setdefault(row[pivot_columns[-1]], list())
            values.append(row[value_column])
        return tree

    def half_pivot():
        """Builds a 'half' pivot table that is missing the left columns.

        Yields:
            list: table row
        """
        for i in range(len(pivot_columns)):
            row = [pivot_header[i]] if any(pivot_header) else []
            row += list(k[i] for k in pivot_keys)
            yield row
        values = dict()
        for row in table:
            branch = values
            for c in pivot_columns[:-1]:
                branch = branch.setdefault(row[c], dict())
            branch.setdefault(row[pivot_columns[-1]], list()).append(row[value_column])
        height = max(len(leaf(values, key)) for key in pivot_keys)
        for i in range(height):
            row = [None] if any(pivot_header) else []
            for key in pivot_keys:
                v = leaf(values, key)
                if i < len(v):
                    row.append(v[i])
                else:
                    row.append(None)
            yield row

    def put_pivot_header(row, header):
        """Puts the given header into the given row.

        Args:
            row (list)
            header (str or None)
        """
        if row:
            if not row[-1]:
                row[-1] = header
        else:
            row.append(header)

    header = table.pop(0)
    if not table:
        return []
    pivot_keys = sorted({tuple(row[i] for i in pivot_columns) for row in table})
    pivot_header = tuple(header[i] for i in pivot_columns)
    if regular_columns or hidden_columns:
        regular_column_width = max(regular_columns) + 1 if regular_columns else 0
        regular_header = [header[i] for i in range(regular_column_width)]
        # If grouping, key columns are the 'visible' regular columns
        # If not grouping, we add the hidden columns
        key_columns = regular_columns
        group_fn = group_function_from_str(group_fn)
        if isinstance(group_fn, NoGroup):
            key_columns += hidden_columns
        # Yield pivot rows (all but last)
        for i in range(len(pivot_columns) - 1):
            row = regular_column_width * [None]
            if any(pivot_header):
                put_pivot_header(row, pivot_header[i])
            row += list(k[i] for k in pivot_keys)
            yield row
        # Yield last pivot row. This one has the regular header (if any) at the begining
        if pivot_columns:
            if any(regular_header):
                last_pivot_row = regular_header
            else:
                last_pivot_row = regular_column_width * [None]
            # Note that the last regular header and the last pivot header would end up in the same cell.
            # This is an arbitrary decision so the tables are more compact; otherwise we'd have an empty row or column
            # at the last header position.
            # To solve the conflict, we take the regular header if not None or empty, and the pivot header otherwise.
            if pivot_header[-1]:
                put_pivot_header(last_pivot_row, pivot_header[-1])
            last_pivot_row += list(k[-1] for k in pivot_keys)
            yield last_pivot_row
        # Yield regular rows
        regular_rows = regular_rows()
        values = value_tree()
        for row_key in sorted(regular_rows.keys()):
            pivot_branch = leaf(values, row_key)
            row = regular_rows[row_key]
            row += [group_fn(leaf(pivot_branch, column_key)) for column_key in pivot_keys]
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
    pivot_column_base = regular_column_count
    pivot_columns = list()
    hidden_column_base = pivot_column_base + pivot_column_count
    current_hidden_column = 0
    hidden_columns = list()
    for mapping in mappings[:-1]:
        position = mapping.position
        if is_pivoted(position):
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
