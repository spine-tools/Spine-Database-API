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
Contains `get_mapped_data()` that converts rows of tabular data into a dictionary for import to a Spine DB,
using ``import_functions.import_data()``

"""

from copy import deepcopy
from operator import itemgetter
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
    split_value_and_type,
)
from ..exception import ParameterValueFormatError


_NO_VALUE = object()


def identity(x):
    """Returns argument unchanged.

    Args:
        x (Any): value to return

    Returns:
        Any: x
    """
    return x


def get_mapped_data(
    data_source,
    mappings,
    data_header=None,
    table_name="",
    column_convert_fns=None,
    default_column_convert_fn=None,
    row_convert_fns=None,
    unparse_value=identity,
):
    """
    Args:
        data_source (Iterable): Yields rows (lists)
        mappings (list(ImportMapping)): Mappings from data rows into mapped data for ``import_data()``
        data_header (list, optional): table header
        table_name (str, optional): table name
        column_convert_fns (dict(int,function), optional): mapping from column number to convert function
        default_column_convert_fn (Callable, optional): default convert function for surplus columns
        row_convert_fns (dict(int,function), optional): mapping from row number to convert function
        unparse_value (Callable): a callable that converts values to database format

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
    mapped_data = {}
    errors = []
    rows = list(data_source)
    if not rows:
        return mapped_data, errors
    column_count = len(max(rows, key=lambda x: len(x) if x else 0))
    if column_convert_fns is None:
        column_convert_fns = {}
    if row_convert_fns is None:
        row_convert_fns = {}
    if default_column_convert_fn is None:
        default_column_convert_fn = column_convert_fns[max(column_convert_fns)] if column_convert_fns else identity
    for mapping in mappings:
        read_state = {}
        mapping = deepcopy(mapping)
        mapping.polish(table_name, data_header, column_count)
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
                if not _is_valid_row(row):
                    continue
                row = _convert_row(row, column_convert_fns, start_pos + k, errors)
                mapping.import_row(row, read_state, mapped_data)
            continue
        # There are pivoted mappings. We unpivot the table
        unpivoted_rows, pivoted_pos, non_pivoted_pos, unpivoted_column_pos = _unpivot_rows(
            rows, data_header, pivoted, non_pivoted, pivoted_from_header, mapping.skip_columns
        )
        if not unpivoted_column_pos:
            continue
        # Reposition row convert functions
        row_convert_fns = {k: row_convert_fns[pos] for k, pos in enumerate(pivoted_pos) if pos in row_convert_fns}
        # If there are only pivoted mappings, we can just feed the unpivoted rows
        if not non_pivoted:
            # Reposition pivoted mappings:
            last.position = -1
            for k, m in enumerate(pivoted):
                m.position = k
            for k, row in enumerate(unpivoted_rows):
                if not _is_valid_row(row):
                    continue
                row = _convert_row(row, row_convert_fns, k, errors)
                mapping.import_row(row, read_state, mapped_data)
            continue
        # There are both pivoted and unpivoted mappings
        # Reposition mappings:
        # - The last mapping (typically, parameter value) will read from the last position in the row
        # - The pivoted mappings will read from positions to the left of that
        k = None
        last.position = -1
        for k, m in enumerate(reversed(pivoted)):
            m.position = -(k + 2)
        # Feed rows: To each regular row, we append each unpivoted row, plus the item at the intersection,
        # and feed that to the mapping
        last_pivoted_row_pos = max(pivoted_pos, default=0) + 1
        last_non_pivoted_column_pos = max(non_pivoted_pos, default=0) + 1
        start_pos = max(mapping.read_start_row, last_pivoted_row_pos)
        min_row_length = max(unpivoted_column_pos)
        for i, row in enumerate(rows[start_pos:]):
            if len(row) < min_row_length + 1:
                error = f"Could not process incomplete row {i + 1}"
                errors.append(error)
                continue
            if not _is_valid_row(row[:last_non_pivoted_column_pos]):
                continue
            row = _convert_row(row, column_convert_fns, start_pos + i, errors, default_column_convert_fn)
            non_pivoted_row = row[:last_non_pivoted_column_pos]
            for column_pos, unpivoted_row in zip(unpivoted_column_pos, unpivoted_rows):
                if not _is_valid_row(unpivoted_row):
                    continue
                unpivoted_row = _convert_row(unpivoted_row, row_convert_fns, k, errors)
                full_row = non_pivoted_row + unpivoted_row
                full_row.append(row[column_pos])
                mapping.import_row(full_row, read_state, mapped_data)
    _make_entity_classes(mapped_data)
    _make_entities(mapped_data)
    _make_parameter_values(mapped_data, unparse_value)
    return mapped_data, errors


def _is_valid_row(row):
    return row is not None and not all(i is None for i in row)


def _convert_row(row, convert_fns, row_number, errors, default_convert_fn=lambda x: x):
    new_row = []
    for j, item in enumerate(row):
        if item is None:
            new_row.append(item)
            continue
        convert_fn = convert_fns.get(j, default_convert_fn)
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
        list(ImportMapping): Pivoted from header mappings
        ImportMapping: last mapping (typically representing the parameter value)
    """
    flattened = mapping.flatten()
    pivoted = []
    non_pivoted = []
    pivoted_from_header = []
    for m in flattened:
        if (pivoted or pivoted_from_header) and m is flattened[-1]:
            # If any other mapping is pivoted, ignore last mapping's position
            break
        if m.position == Position.header and m.value is None:
            pivoted_from_header.append(m)
            continue
        if not isinstance(m.position, int):
            continue
        if m.position < 0:
            pivoted.append(m)
        else:
            non_pivoted.append(m)
    return pivoted, non_pivoted, pivoted_from_header, flattened[-1]


def _unpivot_rows(rows, data_header, pivoted, non_pivoted, pivoted_from_header, skip_columns):
    """Unpivots rows.

    Args:
        rows (list of list): Source table rows
        data_header (list): Source table header
        pivoted (list of ImportMapping): Pivoted mappings (reading from rows)
        non_pivoted (list of ImportMapping): Non-pivoted mappings ('regular', reading from columns)
        pivoted_from_header (list of ImportMapping): Mappings pivoted from header

    Returns:
        list of list: Unpivoted rows
        int: Position of last pivoted row
        int: Position of last non-pivoted row
        list of int: Columns positions corresponding to unpivoted rows
    """
    # First we collect pivoted and unpivoted positions
    pivoted_pos = [-(m.position + 1) for m in pivoted]  # (-1) -> (0), (-2) -> (1), (-3) -> (2), etc.
    non_pivoted_pos = [m.position for m in non_pivoted]
    # Collect pivoted rows
    pivoted_rows = [rows[pos] for pos in pivoted_pos] if non_pivoted_pos else rows
    # Prepend as many headers as needed
    for m in pivoted_from_header:
        pivoted.insert(0, m)
        pivoted_rows.insert(0, data_header)
    if pivoted_from_header:
        pivoted_pos.append(-1)  # This is so ``last_pivoted_row_pos`` below gets the right value
    # Collect non pivoted and skipped positions
    skip_pos = set(skip_columns) | set(non_pivoted_pos)
    # Remove items in those positions from pivoted rows
    if skip_pos:
        pivoted_rows = [[item for k, item in enumerate(row) if k not in skip_pos] for row in pivoted_rows]
    # Unpivot
    unpivoted_rows = [list(row) for row in zip(*pivoted_rows)]
    if not non_pivoted_pos:
        last_pivoted_position = max(pivoted_pos)
        if pivoted_from_header:
            last_pivoted_position += 1
        expanded_pivoted_rows = []
        for row in unpivoted_rows:
            head = row[: last_pivoted_position + 1]
            for data in row[last_pivoted_position + 1 :]:
                expanded_pivoted_rows.append(head + [data])
        unpivoted_rows = expanded_pivoted_rows
    unpivoted_column_pos = [k for k in range(len(rows[0])) if k not in skip_pos] if rows else []
    return unpivoted_rows, pivoted_pos, non_pivoted_pos, unpivoted_column_pos


def _make_entity_classes(mapped_data):
    rows = mapped_data.get("entity_classes")
    if rows is None:
        return
    rows = [(class_name, tuple(dimension_names)) for class_name, dimension_names in rows.items()]
    rows.sort(key=itemgetter(1))
    mapped_data["entity_classes"] = final_rows = []
    for class_name, dimension_names in rows:
        row = (class_name, tuple(dimension_names)) if dimension_names else (class_name,)
        final_rows.append(row)


def _make_entities(mapped_data):
    rows = mapped_data.get("entities")
    if rows is None:
        return
    mapped_data["entities"] = list(rows)


def _make_parameter_values(mapped_data, unparse_value):
    value_pos = 3
    key = "parameter_values"
    rows = mapped_data.get(key)
    if rows is not None:
        valued_rows = []
        for row in rows:
            raw_value = _make_value(row, value_pos)
            if raw_value is _NO_VALUE:
                continue
            value = unparse_value(raw_value)
            if value is not None:
                row[value_pos] = value
                valued_rows.append(row)
        mapped_data[key] = valued_rows
    value_pos = 0
    key = "parameter_definitions"
    rows = mapped_data.get(key)
    if rows is not None:
        full_rows = []
        for entity_definition, extras in rows.items():
            if extras:
                value = unparse_value(_make_value(extras, value_pos))
                if value is not None:
                    extras[value_pos] = value
                    full_rows.append(entity_definition + tuple(extras))
            else:
                full_rows.append(entity_definition)
        mapped_data[key] = full_rows


def _make_value(row, value_pos):
    try:
        value = row[value_pos]
    except IndexError:
        return None
    if isinstance(value, dict):
        if "data" not in value:
            return _NO_VALUE
        return _parameter_value_from_dict(value)
    if isinstance(value, str):
        try:
            return from_database(*split_value_and_type(value))
        except ParameterValueFormatError:
            pass
    return value


def _parameter_value_from_dict(d):
    mapped_index_names = d.get("index_names", {0: ""})
    index_names = (max(mapped_index_names) + 1) * [""]
    for i, name in mapped_index_names.items():
        index_names[i] = name
    if d["type"] == "map":
        map_ = _table_to_map(d["data"], compress=d.get("compress", False))
        if index_names != [""]:
            _apply_index_names(map_, index_names)
        return map_
    if d["type"] == "time_pattern":
        return TimePattern(*zip(*d["data"]), index_name=index_names[0])
    if d["type"] == "time_series":
        options = d.get("options", {})
        ignore_year = options.get("ignore_year", False)
        repeat = options.get("repeat", False)
        return TimeSeriesVariableResolution(*zip(*d["data"]), ignore_year, repeat, index_name=index_names[0])
    if d["type"] == "array":
        return Array(d["data"], index_name=index_names[0])


def _table_to_map(table, compress=False):
    d = _table_to_dict(table)
    m = _dict_to_map_recursive(d)
    if compress:
        return convert_leaf_maps_to_specialized_containers(m)
    return m


def _table_to_dict(table):
    map_dict = dict()
    for row in table:
        row = [item for item in row if item not in (None, "")]
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


def _apply_index_names(map_, index_names):
    """Applies index names to Map.

    Args:
        map_ (Map): target Map.
        index_names (Sequence of str): index names, one for each Map depth
    """
    name = index_names[0]
    if name:
        map_.index_name = index_names[0]
    if len(index_names) == 1:
        return
    for v in map_.values:
        if isinstance(v, Map):
            _apply_index_names(v, index_names[1:])
