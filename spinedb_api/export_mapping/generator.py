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
from copy import deepcopy
from ..mapping import Position
from .pivot import make_pivot, make_regular
from .export_mapping import pair_header_buddies


def rows(root_mapping, db_map, fixed_state=None):
    """
    Generates table's rows.

    Args:
        root_mapping (Mapping): root export mapping
        db_map (DatabaseMappingBase): a database map
        fixed_state (dict, optional): mapping state that fixes items

    Yields:
        list: a list of row's cells
    """

    def listify_row(row):
        """Converts row dictionary to Python list representing the actual row.

        Args:
            row (dict): mapping from Position to cell data

        Returns:
            list: row as list
        """
        row.pop(Position.hidden, None)
        row.pop(Position.table_name, None)
        row.pop(Position.header, None)
        straight = (max(row) + 1) * [None] if row else []
        for index, data in row.items():
            straight[index] = data
        return straight

    def row_iterator():
        """
        Yields non pivoted rows.

        Yields:
            row (list): a table row
        """

        row_iter = root_mapping.rows(db_map, dict(), fixed_state)
        for row in row_iter:
            normal_row = listify_row(row)
            yield normal_row

    if fixed_state is None:
        fixed_state = dict()
    if root_mapping.is_pivoted():
        root_mapping, value_column, regular_columns, hidden_columns, pivot_columns = make_regular(root_mapping)
        if root_mapping.has_header():
            header_root = deepcopy(root_mapping)
            buddies = pair_header_buddies(header_root)
            header = listify_row(header_root.make_header(db_map, {}, fixed_state, buddies))
        else:
            header = None
        regularized = list(row_iterator())
        yield from make_pivot(
            regularized, header, value_column, regular_columns, hidden_columns, pivot_columns, root_mapping.group_fn
        )
    else:
        if root_mapping.has_header():
            header_root = deepcopy(root_mapping)
            buddies = pair_header_buddies(header_root)
            header = listify_row(header_root.make_header(db_map, {}, fixed_state, buddies))
            yield header
        yield from row_iterator()


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
