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
Contains `get_mapped_data()` that converts rows of tabular data into a dictionary for import to a Spine DB
using ``import_functions.import_data()``

:author: P. Vennström (VTT)
:date:   22.02.2018
"""

from copy import deepcopy
from ..parameter_value import from_dict, convert_leaf_maps_to_specialized_containers, Map
from ..import_mapping.import_mapping import ImportMapping, Position
from .import_mapping_compat import import_mapping_from_dict


def get_mapped_data(data_source, mappings, data_header=None, table_name="", column_types=None, row_types=None):
    if not isinstance(mappings, (list, tuple)):
        mappings = [mappings]
    # Sanitize mappings
    for k, mapping in enumerate(mappings):
        if isinstance(mapping, dict):
            mappings[k] = import_mapping_from_dict(mapping)
        elif isinstance(mapping, ImportMapping):
            pass
        else:
            raise TypeError(f"mapping must be a dict or ImportMapping subclass, instead got: {type(mapping).__name__}")
    mapped_data = {}
    errors = []
    read_state = {}
    rows = list(data_source)
    for mapping in mappings:
        mapping = deepcopy(mapping)
        mapping.polish(table_name, data_header)
        # Find pivoted and unpivoted mappings
        pivoted, non_pivoted, pivoted_from_header, last = _split_mapping(mapping)
        # If there are no pivoted mappings, we can just feed the rows to our mapping directly
        if not (pivoted or pivoted_from_header):
            for row in rows[mapping.read_start_row :]:
                mapping.import_row(row, read_state, mapped_data)
            continue
        # There are pivoted mappings. We will unpivot the table
        unpivoted_rows, last_pivoted_row_pos, last_non_pivoted_column_pos = _unpivot_rows(
            rows, data_header, pivoted, non_pivoted, pivoted_from_header, mapping.skip_columns
        )
        # If there are only pivoted mappings, we can just feed the unpivoted rows
        if not non_pivoted:
            # Reposition pivoted mappings:
            for k, m in enumerate(pivoted):
                m.position = k
            for row in unpivoted_rows:
                mapping.import_row(row, read_state, mapped_data)
            continue
        # There are both pivoted and unpivoted mappings
        # Reposition mappings:
        # - The last mapping (typically, parameter value) will read from the last position in the row
        # - The pivoted mappings will read from positions to the left of that
        last.position = -1
        for k, m in enumerate(reversed(pivoted)):
            m.position = -(k + 2)
        # Feed rows: To each regular row, we append each unpivoted row, plus the item at the intersection,
        # and feed that to the mapping
        start_pos = max(mapping.read_start_row, last_pivoted_row_pos)
        for row in rows[start_pos:]:
            regular_row = row[:last_non_pivoted_column_pos]
            for k, unpivoted_row in enumerate(unpivoted_rows):
                full_row = regular_row + unpivoted_row
                full_row.append(row[last_non_pivoted_column_pos + k])
                mapping.import_row(full_row, read_state, mapped_data)
    value_pos = -1  # from (class, entity, parameter, value)
    for key in ("object_parameter_values", "relationship_parameter_values"):
        for row in mapped_data.get(key, []):
            value = row[value_pos]
            if isinstance(value, dict):
                row[value_pos] = _parameter_value_from_dict(value)
    return mapped_data, errors


def _split_mapping(mapping):
    """Splits the given mapping into pivot components.

    Args:
        mapping (ImportMapping)

    Returns:
        list(ImportMapping): Pivoted mappings (reading from rows)
        list(ImportMapping): Non-pivoted mappings ('regular', reading from columns)
        ImportMapping,NoneType: Pivoted reading from header if any, None otherwise
        ImportMapping: last mapping (typically representing the parameter value)
    """
    flattened = mapping.flatten()
    pivoted = []
    non_pivoted = []
    pivoted_from_header = None
    for m in flattened:
        if m.position == Position.header and m.value is None:
            pivoted_from_header = m
        if not isinstance(m.position, int):
            continue
        if m.position < 0:
            pivoted.append(m)
        else:
            non_pivoted.append(m)
    return pivoted, non_pivoted, pivoted_from_header, flattened[-1]


def _unpivot_rows(rows, data_header, pivoted, non_pivoted, pivoted_from_header, skip_columns):
    """Upivots rows.

    Args:
        rows (list(list)): Source table rows
        data_header (list): Source table header
        pivoted (list(ImportMapping)): Pivoted mappings (reading from rows)
        non_pivoted (list(ImportMapping)): Non-pivoted mappings ('regular', reading from columns)
        pivoted_from_header (ImportMapping,NoneType): ImportMapping pivoted from header if any, None otherwise

    Returns:
        list(list): Unpivoted rows
        int: Position of last pivoted row
        int: Position of last non-pivoted row
    """
    # First we collect pivoted and unpivoted positions
    pivoted_pos = [-(m.position + 1) for m in pivoted]  # (-1) -> (0), (-2) -> (1), (-3) -> (2), etc.
    non_pivoted_pos = [m.position for m in non_pivoted]
    # Collect pivoted rows
    pivoted_rows = [rows[pos] for pos in pivoted_pos]
    # Prepend the header if needed
    if pivoted_from_header:
        pivoted.insert(0, pivoted_from_header)
        pivoted_rows.insert(0, data_header)
        pivoted_pos.append(-1)  # This is so ``last_pivoted_row_pos`` below gets the right value
    # Remove items in non pivoted and skipped positions from pivoted rows
    skip_pos = set(skip_columns) | set(non_pivoted_pos)
    skip_pos = sorted(skip_pos, reverse=True)
    for row in pivoted_rows:
        for j in skip_pos:
            row.pop(j)
    # Unpivot
    unpivoted_rows = [list(row) for row in zip(*pivoted_rows)]
    last_pivoted_row_pos = max(pivoted_pos, default=0) + 1
    last_non_pivoted_column_pos = max(non_pivoted_pos, default=0) + 1
    return unpivoted_rows, last_pivoted_row_pos, last_non_pivoted_column_pos


def _parameter_value_from_dict(d):
    if d["type"] == "map":
        return _table_to_map(d["data"], compress=d.get("compress", False))
    if d["type"] == "time_pattern":
        d["data"] = dict(d["data"])
    return from_dict(d)


def _table_to_map(table, compress=False):
    d = _table_to_dict(table)
    m = _dict_to_map_recursive(d)
    if compress:
        return convert_leaf_maps_to_specialized_containers(m)
    return m


def _table_to_dict(table):
    map_dict = dict()
    for row in table:
        row = [item for item in row if item]
        if len(row) < 2:
            continue
        d = map_dict
        for item in row[:-2]:
            d = d.setdefault(item, dict())
        d[row[-2]] = row[-1]
    return map_dict


def _dict_to_map_recursive(d):
    indexes = list()
    values = list()
    for key, value in d.items():
        if isinstance(value, dict):
            value = _dict_to_map_recursive(value)
        indexes.append(key)
        values.append(value)
    return Map(indexes, values)
