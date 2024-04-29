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
"""
Contains generator functions that convert a Spine database into rows of tabular data.

"""
from copy import deepcopy
from ..mapping import Position
from .pivot import make_pivot, make_regular
from .export_mapping import pair_header_buddies
from .group_functions import NoGroup


def rows(root_mapping, db_map, fixed_state=None, empty_data_header=True, group_fn=NoGroup.NAME):
    """
    Generates table's rows.

    Args:
        root_mapping (Mapping): root export mapping
        db_map (DatabaseMapping): a database map
        fixed_state (dict, optional): mapping state that fixes items
        empty_data_header (bool): True to yield at least header rows even if there is no data, False to yield nothing
        group_fn (str): group function name

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

    if fixed_state is None:
        fixed_state = dict()
    if root_mapping.is_pivoted():
        root_mapping, value_column, regular_columns, hidden_columns, pivot_columns = make_regular(root_mapping)
        if root_mapping.has_header():
            header_root = deepcopy(root_mapping)
            buddies = pair_header_buddies(header_root)
            header = listify_row(header_root.make_header(db_map, fixed_state, buddies))
        else:
            header = None
        mapping_rows = root_mapping.rows(db_map, fixed_state)
        regularized = list(map(listify_row, mapping_rows))
        yield from make_pivot(
            regularized,
            header,
            value_column,
            regular_columns,
            hidden_columns,
            pivot_columns,
            group_fn,
            empty_data_header,
        )
    else:
        mapping_rows = root_mapping.rows(db_map, fixed_state)
        row_iter = iter(map(listify_row, mapping_rows))
        try:
            peeked_row = next(row_iter)
        except StopIteration:
            if not empty_data_header:
                return
            peeked_row = None
        if root_mapping.has_header():
            buddies = pair_header_buddies(root_mapping)
            header = listify_row(root_mapping.make_header(db_map, fixed_state, buddies))
            yield header
        if peeked_row is None:
            return
        yield peeked_row
        yield from row_iter


def titles(root_mapping, db_map, limit=None):
    """
    Generates titles.

    Args:
        root_mapping (Mapping): root export mapping
        db_map (DatabaseMapping): a database map

    Yield:
        tuple: title and title's fixed key
    """
    if not root_mapping.has_titles():
        yield None, None
        return
    mapping_titles = root_mapping.titles(db_map, limit=limit)
    for title, title_key in mapping_titles:
        yield title, title_key
