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
Contains `get_mapped_data()` that converts rows of tabular data into a dictionary for import to a Spine DB,
using ``import_functions.import_data()``

:author: P. Vennström (VTT)
:date:   22.02.2018
"""

from copy import deepcopy
from .import_mapping_compat import import_mapping_from_dict
from .import_mapping import ImportMapping, check_validity
from ..mapping import Position
from ..parameter_value import (
    convert_leaf_maps_to_specialized_containers,
    Map,
    TimeSeriesVariableResolution,
    TimePattern,
    Array,
    from_database,
)
from ..exception import ParameterValueFormatError


def get_mapped_data(
    data_source, mappings, data_header=None, table_name="", column_convert_fns=None, row_convert_fns=None
):
    """
    Args:
        data_source (Iterable): Yields rows (lists)
        mappings (list(ImportMapping)): Mappings from data rows into mapped data for ``import_data()``
        data_header (list, optional): table header
        table_name (str, optional): table name
        column_convert_fns (dict(int,function), optional): mapping from column number to convert function
        row_convert_fns (dict(int,function), optional): mapping from row number to convert function

    Returns:
        dict: Mapped data, ready for ``import_data()``
        list: Conversion errors
    """
    # Sanitize mappings
    for k, mapping in enumerate(mappings):
        if isinstance(mapping, (list, dict)):
            mappings[k] = import_mapping_from_dict(mapping)
        elif not isinstance(mapping, ImportMapping):
            raise TypeError(f"mapping must be a dict or ImportMapping subclass, instead got: {type(mapping).__name__}")
    if column_convert_fns is None:
        column_convert_fns = {}
    if row_convert_fns is None:
        row_convert_fns = {}
    mapped_data = {}
    errors = []
    read_state = {}
    rows = list(data_source)
    for mapping in mappings:
        mapping = deepcopy(mapping)
        mapping.polish(table_name, data_header)
        mapping_errors = check_validity(mapping)
        if mapping_errors:
            errors += mapping_errors
            continue
        # Find pivoted and unpivoted mappings
        pivoted, non_pivoted, pivoted_from_header, last = _split_mapping(mapping)
        # If there are no pivoted mappings, we can just feed the rows to our mapping directly
        if not (pivoted or pivoted_from_header):
            start_pos = mapping.read_start_row
            for k, row in enumerate(rows[mapping.read_start_row :]):
                row = _convert_row(row, column_convert_fns, start_pos + k, errors)
                mapping.import_row(row, read_state, mapped_data)
            continue
        # There are pivoted mappings. We will unpivot the table
        unpivoted_rows, last_pivoted_row_pos, last_non_pivoted_column_pos, unpivoted_column_pos = _unpivot_rows(
            rows, data_header, pivoted, non_pivoted, pivoted_from_header, mapping.skip_columns
        )
        # If there are only pivoted mappings, we can just feed the unpivoted rows
        if not non_pivoted:
            # Reposition pivoted mappings:
            for k, m in enumerate(pivoted):
                m.position = k
            for k, row in enumerate(unpivoted_rows):
                row = _convert_row(row, row_convert_fns, k, errors)
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
        for i, row in enumerate(rows[start_pos:]):
            regular_row = row[:last_non_pivoted_column_pos]
            regular_row = _convert_row(regular_row, column_convert_fns, start_pos + i, errors)
            for column_pos, unpivoted_row in zip(unpivoted_column_pos, unpivoted_rows):
                unpivoted_row = _convert_row(unpivoted_row, row_convert_fns, k, errors)
                full_row = regular_row + unpivoted_row
                full_row.append(row[column_pos])
                mapping.import_row(full_row, read_state, mapped_data)
    _make_parameter_values(mapped_data)
    return mapped_data, errors


def _convert_row(row, convert_fns, row_number, errors):
    if row is None:
        return None
    new_row = []
    for j, item in enumerate(row):
        convert_fn = convert_fns.get(j, lambda x: x)
        try:
            item = convert_fn(item)
        except (ValueError, ParameterValueFormatError):
            error = f"Could not convert '{item}' to type '{convert_fn.DISPLAY_NAME}' (near row {row_number})"
            errors.append(error)
        new_row.append(item)
    return new_row


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
        # FIXME MAYBE: Can there be multiple pivoted from header mappings?
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
        list(int): Columns positions corresponding to unpivoted rows
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
    # Collect non pivoted and skipped positions
    skip_pos = set(skip_columns) | set(non_pivoted_pos)
    # Remove items in those positions from pivoted rows
    pivoted_rows = [[item for k, item in enumerate(row) if k not in skip_pos] for row in pivoted_rows]
    # Unpivot
    unpivoted_rows = [list(row) for row in zip(*pivoted_rows)]
    last_pivoted_row_pos = max(pivoted_pos, default=0) + 1
    last_non_pivoted_column_pos = max(non_pivoted_pos, default=0) + 1
    unpivoted_column_pos = [k for k in range(len(rows[0])) if k not in skip_pos]
    return unpivoted_rows, last_pivoted_row_pos, last_non_pivoted_column_pos, unpivoted_column_pos


def _make_parameter_values(mapped_data):
    parameter_value_pos = {
        "object_parameter_values": 3,
        "relationship_parameter_values": 3,
        "object_parameters": 2,
        "relationship_parameters": 2,
    }
    for key, value_pos in parameter_value_pos.items():
        for row in mapped_data.get(key, []):
            try:
                value = row[value_pos]
            except IndexError:
                continue
            if isinstance(value, dict):
                row[value_pos] = _parameter_value_from_dict(value)
            if isinstance(value, str):
                try:
                    row[value_pos] = from_database(value)
                except ParameterValueFormatError:
                    pass


def _parameter_value_from_dict(d):
    if d["type"] == "map":
        return _table_to_map(d["data"], compress=d.get("compress", False))
    if d["type"] == "time_pattern":
        return TimePattern(*zip(*d["data"]))
    if d["type"] == "time_series":
        options = d.get("options", {})
        ignore_year = options.get("ignore_year", False)
        repeat = options.get("repeat", False)
        return TimeSeriesVariableResolution(*zip(*d["data"]), ignore_year, repeat)
    if d["type"] == "array":
        return Array(d["data"])


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
