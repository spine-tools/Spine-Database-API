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


def rows(root_item, db_map, fixed_key=None):
    """
    Generates table's rows.

    Args:
        root_item (ExportItem): root export item
        db_map (DatabaseMappingBase): a database map
        fixed_key (Key, optional): a key that fixes items

    Yields:
        list: a list of row's cells
    """

    def del_unused_positions(row):
        """
        Deletes columns that are not to be shown.

        Args:
            row (dict): a mapping from position to row data
        """
        try:
            del row[Position.hidden]
        except KeyError:
            pass
        try:
            del row[Position.table_name]
        except KeyError:
            pass

    if fixed_key is None:
        fixed_key = dict()
    if root_item.is_pivoted():
        root_item, value_column, regular_columns, hidden_columns, pivot_columns = make_regular(root_item)
        regularized = list(rows(root_item, db_map, fixed_key))
        for row in make_pivot(regularized, value_column, regular_columns, hidden_columns, pivot_columns):
            yield row
    else:
        for row in root_item.rows(db_map, dict(), fixed_key):
            del_unused_positions(row)
            single_row = row.pop(Position.single_row, [])
            straight = (max(row) + 1) * [None] if row else []
            for index, data in row.items():
                straight[index] = data
            yield straight + single_row


def titles(root_item, db_map):
    """
    Generates titles.

    Args:
        root_item (ExportItem): root export item
        db_map (DatabaseMappingBase): a database map

    Yield:
        tuple: title and title's fixed key
    """
    for title, title_key in root_item.title(db_map, dict()):
        yield title, title_key
