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
Classes for reading data with json mapping specifications

:author: P. Vennström (VTT)
:date:   22.02.2018
"""
import itertools
from copy import deepcopy
import math
from collections.abc import Iterable
from ..parameter_value import ParameterValueFormatError
from ..exception import InvalidMapping, TypeConversionError
from .item_import_mapping import ItemMappingBase, item_mapping_from_dict
from spinedb_api.spine_io.mapping import Mapping, Position
from .import_mapping_compat import import_mapping_from_dict


def type_class_list_from_spec(types, num_sections, skip_sections=None):
    if skip_sections is None:
        skip_sections = []
    type_conv_list = []
    for section in range(num_sections):
        type_class = types.get(section, None)
        if section in skip_sections or type_class is None:
            type_class = lambda x: x
        type_conv_list.append(type_class)
    return type_conv_list


def convert_value(value, type_converter):
    try:
        if isinstance(value, str) and not value:
            value = None
        if value is not None:
            value = type_converter(value)
        return value
    except (ValueError, ParameterValueFormatError):
        raise TypeConversionError(f"Could not convert value: '{value}' to type: '{type_converter.__name__}'")


def convert_function_from_spec(column_types, num_cols, skip_cols=None):
    """Creates a function that converts a list of data with length num_cols to the
    types in the column_types dict. If no type is given then the function returns the original value
    
    Arguments:
        column_types {dict} -- dict with column number as key and type constructor as value for each column
        num_cols {int} -- length of data that the function should convert
    
    Raises:
        ValueError: Raised if the column_types dict contains a unsupported class type
        TypeConversionError: [description]
    
    Returns:
        [function] -- A function that converts a row of data to the types given by column_types. 
    """
    if not column_types:
        return lambda x: x
    type_conv_list = type_class_list_from_spec(column_types, num_cols, skip_cols)
    return lambda row: [convert_value(row_item, col_type) for row_item, col_type in zip(row, type_conv_list)]


def mapping_non_pivoted_columns(mapping, num_cols, data_header=None):
    """Returns columns that are referenced but not pivoted given a header and number of columns
    
    Arguments:
        mapping {Mapping} -- mapping object
        num_cols {int} -- number of columns to check
    
    Keyword Arguments:
        data_header {list[str]} -- list of strings, headers (default: {None})
    
    Returns:
        [set] -- referenced columns in mapping that is not pivoted.
    """
    if data_header is None:
        data_header = []
    non_pivoted_columns = mapping.non_pivoted_columns()
    int_non_piv_cols = []
    for pc in non_pivoted_columns:
        if isinstance(pc, str):
            if pc not in data_header:
                # could not find reference
                continue
            pc = data_header.index(pc)
        if pc >= num_cols:
            continue
        int_non_piv_cols.append(pc)
    return set(int_non_piv_cols)


def get_pivoted_data(data_source, mapping, num_cols, data_header, row_types):
    pivoted_data = []
    errors = []

    # find used columns
    skip_columns = set(mapping_non_pivoted_columns(mapping, num_cols, data_header))
    if mapping.skip_columns is not None:
        skip_columns.update(set(mapping.skip_columns))
    used_columns = set(range(num_cols)).difference(skip_columns)

    # get data from iterator and convert to correct type.
    if mapping.is_pivoted():
        do_nothing = lambda x: x
        for row_number in range(mapping.last_pivot_row() + 1):
            # TODO: if data_source iterator ends before all pivoted rows are collected.
            type_converter = row_types.get(row_number, do_nothing)
            row_data = next(data_source)
            typed_row = []
            for col, value in enumerate(row_data):
                if col in used_columns:
                    try:
                        typed_row.append(convert_value(value, type_converter))
                    except TypeConversionError as e:
                        errors.append((row_number, e))
                        typed_row.append(None)
                else:
                    typed_row.append(value)
            pivoted_data.append(typed_row)
    return pivoted_data, errors


def _is_row_value_valid(row_value):
    if row_value is None:
        return False
    if not isinstance(row_value, Iterable):
        return True
    return all(v is not None for v in row_value)


def read_with_mapping(
    data_source, mappings, num_cols, data_header=None, table_name="", column_types=None, row_types=None
):
    if not isinstance(mappings, (list, tuple)):
        mappings = [mappings]
    # Sanitize mappings
    for k, mapping in enumerate(mappings):
        if isinstance(mapping, dict):
            mappings[k] = import_mapping_from_dict(mapping)
        elif isinstance(mapping, Mapping):
            pass
        else:
            raise TypeError(f"mapping must be a dict or Mapping subclass, instead got: {type(mapping).__name__}")
    mapped_data = {}
    errors = []
    read_state = {}
    rows = list(data_source)
    for mapping in mappings:
        mapping = deepcopy(mapping)
        mapping.polish(table_name, data_header)
        # Find pivoted and unpivoted mappings
        pivoted = []
        non_pivoted = []
        pivoted_from_header = None
        flattened = mapping.flatten()
        for m in flattened:
            if m.position == Position.header and m.value is None:
                pivoted_from_header = m
            if not isinstance(m.position, int):
                continue
            if m.position < 0:
                pivoted.append(m)
            else:
                non_pivoted.append(m)
        # If there are no pivoted mappings, we can just feed the rows to our mapping directly
        if not (pivoted or pivoted_from_header):
            for row in rows:
                mapping.import_row(row, read_state, mapped_data)
            continue
        # There are pivoted mappings
        # We will unpivot the table: first we collect pivoted and unpivoted positions
        pivoted_row_pos = [-(m.position + 1) for m in pivoted]  # (-1) -> (0), (-2) -> (1), (-3) -> (2), etc.
        non_pivoted_row_pos = sorted((m.position for m in non_pivoted), reverse=True)
        # Collect pivoted rows
        pivoted_rows = [rows[pos] for pos in pivoted_row_pos]
        # Prepend the header if needed
        if pivoted_from_header:
            pivoted.insert(0, pivoted_from_header)
            pivoted_rows.insert(0, data_header)
            pivoted_row_pos.append(-1)  # This is so ``last_pivoted_row_pos`` below gets the right value
        # Remove items in non pivoted positions from pivoted rows
        for row in pivoted_rows:
            for j in non_pivoted_row_pos:
                row.pop(j)
        # Unpivot
        unpivoted_rows = [list(row) for row in zip(*pivoted_rows)]
        # If there are only pivoted mappings, we can just feed the pivoted rows
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
        flattened[-1].position = -1
        for k, m in enumerate(reversed(pivoted)):
            m.position = -(k + 2)
        # Feed rows: To each regular row, we append each unpivoted row, plus the item at the intersection,
        # and feed that to the mapping
        last_pivoted_row_pos = max(pivoted_row_pos, default=0) + 1
        last_non_pivoted_row_pos = max(non_pivoted_row_pos, default=0) + 1
        for row in rows[last_pivoted_row_pos:]:
            regular_row = row[:last_non_pivoted_row_pos]
            for k, unpivoted_row in enumerate(unpivoted_rows):
                full_row = regular_row + unpivoted_row
                full_row.append(row[last_non_pivoted_row_pos + k])
                mapping.import_row(full_row, read_state, mapped_data)
    return mapped_data, errors


def old_read_with_mapping(data_source, mapping, num_cols, data_header=None, column_types=None, row_types=None):
    """Reads data_source line by line with supplied Mapping object or dict
    that can be translated into a Mapping object"""
    if row_types is None:
        row_types = {}

    errors = []
    mappings = []
    if not isinstance(mapping, (list, tuple)):
        mapping = [mapping]
    for map_ in mapping:
        if isinstance(map_, dict):
            mappings.append(item_mapping_from_dict(map_))
        elif isinstance(map_, ItemMappingBase):
            mappings = [map_]
        else:
            raise TypeError(
                "mapping must be a dict, ItemMappingBase subclass, or list of those, "
                f"instead got: {type(map_).__name__}"
            )

    for map_ in mappings:
        valid, message = map_.is_valid()
        if not valid:
            raise InvalidMapping(message)

    # find max pivot row since mapping can have different number of pivoted rows.
    last_pivot_row = -1
    has_pivot = False
    for map_ in mappings:
        if map_.is_pivoted():
            has_pivot = True
            last_pivot_row = max(last_pivot_row, map_.last_pivot_row())

    # get pivoted rows of data.
    raw_pivoted_data = []
    if has_pivot:
        for row_number in range(last_pivot_row + 1):
            raw_pivoted_data.append(next(data_source))

    # get a list of reader functions
    readers = []
    min_read_data_from_row = math.inf
    for map_index, m in enumerate(mappings):
        pivoted_data, pivot_type_errors = get_pivoted_data(iter(raw_pivoted_data), m, num_cols, data_header, row_types)
        errors.extend(pivot_type_errors)
        read_data_from_row = max(m.last_pivot_row() + 1, m.read_start_row)
        r = m.create_mapping_readers(num_cols, pivoted_data, data_header)
        readers.extend([((map_index, key), reader, reads_row, read_data_from_row) for key, reader, reads_row in r])
        min_read_data_from_row = min(min_read_data_from_row, read_data_from_row)
    data = dict()
    # run functions that read from header or pivoted area first
    # select only readers that actually need to read row data
    row_readers = []
    for key, func, reads_rows, read_data_from_row in readers:
        if key not in data:
            data[key] = []
        if reads_rows:
            row_readers.append((key, func, read_data_from_row))
        else:
            data[key].extend(func(None))

    # function that converts column in the row data to the types specified in column_types
    convert_row_types = convert_function_from_spec(column_types, num_cols)

    if raw_pivoted_data:
        data_source = itertools.chain(raw_pivoted_data, data_source)

    data_source = itertools.islice(data_source, min_read_data_from_row, None)
    skipped_rows = min_read_data_from_row

    # read each row in data source
    if row_readers:
        for row_number, row_data in enumerate(data_source):
            row_number = row_number + skipped_rows
            if not row_data:
                continue
            try:
                row_data = convert_row_types(row_data)
            except TypeConversionError as e:
                errors.append((row_number, e))
                continue
            try:
                # read the row with each reader
                for key, reader, read_data_from_row in row_readers:
                    if row_number >= read_data_from_row:
                        data[key].extend(
                            [row_value for row_value in reader(row_data) if _is_row_value_valid(row_value)]
                        )
            except IndexError as e:
                errors.append((row_number, e))
    # convert parameter values to right type and put all data in one dict
    merged_data = {}
    for key, v in data.items():
        map_i, k = key
        if "parameter_values" in k:
            current_mapping = mappings[map_i]
            merged_data.setdefault(k, []).extend(current_mapping.parameters.raw_data_to_type(v))
        else:
            merged_data.setdefault(k, []).extend(v)
    return merged_data, errors
