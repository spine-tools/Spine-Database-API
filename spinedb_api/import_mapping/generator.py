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
from collections.abc import Callable
from copy import deepcopy
from itertools import dropwhile
from typing import Any, Optional
from ..exception import ParameterValueFormatError
from ..helpers import string_to_bool
from ..import_functions import UnparseCallable
from ..mapping import Position, is_pivoted
from ..parameter_value import (
    Array,
    IndexedValue,
    Map,
    TimePattern,
    TimeSeriesVariableResolution,
    convert_leaf_maps_to_specialized_containers,
    from_database,
    split_value_and_type,
)
from .import_mapping import (
    ArrayValueRecord,
    ImportMapping,
    MapValueRecord,
    SemiMappedData,
    TimePatternValueRecord,
    TimeSeriesValueRecord,
    ValueRecord,
    check_validity,
)
from .import_mapping_compat import import_mapping_from_dict

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
    mapping_names=None,
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
        mapping_names (list, optional): list of mapping names (order corresponds to order of mappings).

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
        default_column_convert_fn = _last_column_convert_function(column_convert_fns)
    if mapping_names is None:
        mapping_names = []
    _ensure_mapping_name_consistency(mappings, mapping_names)
    for mapping, mapping_name in zip(mappings, mapping_names):
        mapping = deepcopy(mapping)
        mapping.polish(table_name, data_header, mapping_name, column_count)
        mapping_errors = check_validity(mapping)
        if mapping_errors:
            errors += mapping_errors
            continue
        read_state = {}
        # Find pivoted and unpivoted mappings
        pivoted, non_pivoted, pivoted_from_header, last = _split_mapping(mapping)
        # If there are no pivoted mappings, we can just feed the rows to our mapping directly
        if not (pivoted or pivoted_from_header):
            start_pos = mapping.read_start_row
            for k, row in enumerate(rows[start_pos:]):
                if not _is_valid_row(row):
                    continue
                row = _convert_row(row, column_convert_fns, start_pos + k, errors)
                mapping.import_row(row, read_state, mapped_data)
            continue
        # There are pivoted mappings. We unpivot the table
        pivoted_by_leaf = all(
            not is_pivoted(m.position) and m.position != Position.header for m in mapping.flatten()[:-1]
        )
        unpivoted_rows, pivoted_pos, non_pivoted_pos, unpivoted_column_pos = _unpivot_rows(
            rows,
            data_header,
            pivoted,
            non_pivoted,
            pivoted_from_header,
            mapping.skip_columns,
            mapping.read_start_row,
            pivoted_by_leaf,
        )
        if not unpivoted_column_pos:
            continue
        last.position = -1
        # Reposition row convert functions
        row_convert_fns = {k: row_convert_fns[pos] for k, pos in enumerate(pivoted_pos) if pos in row_convert_fns}
        # If there are only pivoted mappings, we can just feed the unpivoted rows
        if not non_pivoted:
            # Reposition pivoted mappings:
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
    _make_entity_metadata(mapped_data)
    _make_entity_alternatives(mapped_data, errors)
    _make_parameter_definitions(mapped_data, unparse_value)
    _make_parameter_values(mapped_data, unparse_value)
    _make_parameter_value_metadata(mapped_data)
    return mapped_data, errors


def _last_column_convert_function(functions: Optional[dict]) -> Callable[[Any], Any]:
    return functions[max(functions)] if functions else identity


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


def _unpivot_rows(
    rows, data_header, pivoted, non_pivoted, pivoted_from_header, skip_columns, read_start_row, pivoted_by_leaf
):
    """Unpivots rows.

    Args:
        rows (list of list): Source table rows
        data_header (list): Source table header
        pivoted (list of ImportMapping): Pivoted mappings (reading from rows)
        non_pivoted (list of ImportMapping): Non-pivoted mappings ('regular', reading from columns)
        pivoted_from_header (list of ImportMapping): Mappings pivoted from header
        skip_columns (list of int): columns that should be skipped
        read_start_row (int): first row to include
        pivoted_by_leaf (bool): whether only the leaf mapping is pivoted

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
    if not pivoted_by_leaf:
        pivoted_rows = [rows[pos] for pos in pivoted_pos] if non_pivoted_pos else rows
    else:
        pivoted_rows = [rows[pos + read_start_row] for pos in pivoted_pos] if non_pivoted_pos else rows[read_start_row:]
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


def _make_entity_classes(mapped_data: dict) -> None:
    try:
        rows = mapped_data.pop("entity_classes")
    except KeyError:
        return
    final_rows = []
    for name, record in rows.items():
        item = [name, record.dimensions]
        if record.description:
            item.append(record.description)
        final_rows.append(item)
    if final_rows:
        mapped_data["entity_classes"] = final_rows


def _make_entities(mapped_data):
    try:
        rows = mapped_data.pop("entities")
    except KeyError:
        return
    final_rows = []
    for (class_name, name), record in rows.items():
        item = [class_name, name if not record.elements else record.elements]
        if record.description:
            item.append(record.description)
        final_rows.append(item)
    if final_rows:
        mapped_data["entities"] = final_rows


def _make_entity_alternatives(mapped_data, errors):
    if "entity_alternatives" not in mapped_data:
        return
    rows = []
    for item in mapped_data["entity_alternatives"]:
        entity_class, byname, alternative, activity = item
        try:
            activity = string_to_bool(activity) if isinstance(activity, str) else bool(activity)
        except ValueError:
            errors.append(
                f"Can't convert {activity} to entity alternative activity boolean "
                f"for '{byname}' in '{entity_class}' with alternative '{alternative}'"
            )
        else:
            rows.append((entity_class, byname, alternative, activity))
    mapped_data["entity_alternatives"] = rows


def _make_parameter_definitions(mapped_data: SemiMappedData, unparse_value: UnparseCallable) -> None:
    key = "parameter_definitions"
    try:
        rows = mapped_data.pop(key)
    except KeyError:
        return
    final_rows = []
    for (entity_class_name, parameter_name), record in rows.items():
        definition_data = [entity_class_name, parameter_name]
        default_value = record.default_value
        if isinstance(default_value, ValueRecord):
            if default_value.has_value():
                default_value = unparse_value(_make_value(default_value))
            else:
                default_value = None
        elif isinstance(default_value, str):
            try:
                default_value = from_database(*split_value_and_type(default_value))
            except ParameterValueFormatError:
                pass
        reversed_extras = [record.description, record.value_list_name, default_value]
        definition_data += reversed(list(dropwhile(lambda x: x is None, reversed_extras)))
        final_rows.append(definition_data)
    if final_rows:
        mapped_data[key] = final_rows


def _make_parameter_values(mapped_data, unparse_value):
    key = "parameter_values"
    try:
        rows = mapped_data.pop(key)
    except KeyError:
        return
    final_rows = []
    for (entity_class_name, entity_byname, parameter_name, alternative_name), value in rows.items():
        if isinstance(value, ValueRecord):
            if value.has_value():
                value = unparse_value(_make_value(value))
            else:
                value = None
        elif isinstance(value, str):
            try:
                value = from_database(*split_value_and_type(value))
            except ParameterValueFormatError:
                pass
        if value is None:
            continue
        value_data = [entity_class_name, entity_byname, parameter_name, value]
        if alternative_name is not None:
            value_data.append(alternative_name)
        final_rows.append(value_data)
    if final_rows:
        mapped_data[key] = final_rows


def _make_parameter_value_metadata(mapped_data):
    rows = mapped_data.get("parameter_value_metadata")
    if rows is None:
        return
    mapped_data["parameter_value_metadata"] = list(rows)


def _make_entity_metadata(mapped_data):
    rows = mapped_data.get("entity_metadata")
    if rows is None:
        return
    mapped_data["entity_metadata"] = list(rows)


def _make_value(record: ValueRecord) -> IndexedValue:
    match record:
        case ArrayValueRecord():
            index_name = record.index_names[0] if record.index_names else ""
            return Array(record.values, index_name=index_name)
        case TimePatternValueRecord():
            index_name = record.index_names[0] if record.index_names else ""
            indexes = [i[0] for i in record.indexes]
            return TimePattern(indexes, record.values, index_name)
        case TimeSeriesValueRecord():
            index_name = record.index_names[0] if record.index_names else ""
            indexes = [i[0] for i in record.indexes]
            return TimeSeriesVariableResolution(indexes, record.values, record.ignore_year, record.repeat, index_name)
        case MapValueRecord():
            map_value = _table_to_map(
                ([*indexes, values] for indexes, values in zip(record.indexes, record.values)), record.compress
            )
            if record.index_names:
                _apply_index_names(map_value, record.index_names)
            return map_value
        case _:
            raise RuntimeError(f"logic error: unknown record type '{type(record).__name__}'")


def _table_to_map(table, compress=False):
    d = _table_to_dict(table)
    m = _dict_to_map_recursive(d)
    if compress:
        return convert_leaf_maps_to_specialized_containers(m)
    return m


def _table_to_dict(table):
    map_dict = {}
    for row in table:
        row = [item for item in row if item not in (None, "")]
        if len(row) < 2:
            continue
        d = map_dict
        for item in row[:-2]:
            d = d.setdefault(item, {})
        d[row[-2]] = row[-1]
    return map_dict


def _dict_to_map_recursive(d):
    indexes = []
    values = []
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
        map_.index_name = name
    if len(index_names) == 1:
        return
    for v in map_.values:
        if isinstance(v, Map):
            _apply_index_names(v, index_names[1:])


def _ensure_mapping_name_consistency(mappings: list[ImportMapping], mapping_names: list[str]) -> None:
    """Makes sure that there are as many mapping names as actual mappings.

    Args:
        mappings: list of mappings
        mapping_names: list of mapping names
    """
    n_mappings = len(mappings)
    n_mapping_names = len(mapping_names)
    if n_mapping_names < n_mappings:
        mapping_names += [""] * (n_mappings - n_mapping_names)
