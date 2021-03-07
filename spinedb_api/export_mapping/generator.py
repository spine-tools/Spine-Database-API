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
Contains generator functions that convert a Spine database into rows of tabular data.

:author: A. Soininen (VTT)
:date:   10.12.2020
"""
from .item_export_mapping import Position
from .pivot import make_pivot, make_regular


def rows(root_mapping, db_map, fixed_key=None):
    """
    Generates table's rows.

    Args:
        root_mapping (Mapping): root export mapping
        db_map (DatabaseMappingBase): a database map
        fixed_key (Key, optional): a key that fixes items

    Yields:
        list: a list of row's cells
    """

    def row_iterator():
        """
        Yields non pivoted rows.

        Yields:
            row (list): a table row
        """

        def split_row(row):
            row.pop(Position.hidden, None)
            row.pop(Position.table_name, None)
            single_row = row.pop(Position.single_row, [])
            straight = (max(row) + 1) * [None] if row else []
            for index, data in row.items():
                straight[index] = data
            return straight, single_row

        row_iter = root_mapping.rows(db_map, dict(), fixed_key)
        # Yield header. Ignore the single row
        header = next(row_iter, {})
        straight, _ = split_row(header)
        yield straight
        # Yield normal rows
        for row in row_iter:
            straight, single_row = split_row(row)
            yield straight + single_row

    if fixed_key is None:
        fixed_key = dict()
    if root_mapping.is_pivoted():
        root_mapping, value_column, regular_columns, hidden_columns, pivot_columns = make_regular(root_mapping)
        regularized = list(row_iterator())
        for row in make_pivot(
            regularized, value_column, regular_columns, hidden_columns, pivot_columns, root_mapping.group_fn
        ):
            yield row
    else:
        row_iter = row_iterator()
        header = next(row_iter, [])
        if any(header):
            yield header
        yield from row_iter


def titles(root_mapping, db_map):
    """
    Generates titles.

    Args:
        root_mapping (Mapping): root export mapping
        db_map (DatabaseMappingBase): a database map

    Yield:
        tuple: title and title's fixed key
    """
    if not root_mapping.has_title():
        yield None, None
        return
    for title, title_key in root_mapping.title(db_map, dict()):
        yield title, title_key
