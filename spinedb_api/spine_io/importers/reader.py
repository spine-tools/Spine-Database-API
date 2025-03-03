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

""" Contains a base class for a data source readers used in importing. """

from dataclasses import dataclass, field
from itertools import islice
from typing import Any
from spinedb_api import DateTime, Duration, ParameterValueFormatError
from spinedb_api.exception import InvalidMappingComponent, ReaderError
from spinedb_api.import_mapping.generator import get_mapped_data, identity
from spinedb_api.import_mapping.import_mapping_compat import parse_named_mapping_spec
from spinedb_api.mapping import Position, parse_fixed_position_value

TYPE_STRING_TO_CLASS = {"string": str, "datetime": DateTime, "duration": Duration, "float": float, "boolean": bool}

TYPE_CLASS_TO_STRING = {type_class: string for string, type_class in TYPE_STRING_TO_CLASS.items()}


@dataclass
class TableProperties:
    options: dict = field(default_factory=dict)


class Reader:
    """A base class to read data."""

    # name of data source, ex: "Text/CSV"
    DISPLAY_NAME = "unnamed source"

    # dict with option specification for source.
    OPTIONS = {}
    BASE_OPTIONS = {
        "max_rows": {
            "type": int,
            "label": "Max rows",
            "Minimum": -1,
            "Maximum": 16777215,
            "SpecialValueText": "unrestricted",
            "default": -1,
        }
    }

    # File extensions for modal widget that that returns action (OK, CANCEL) and source object
    FILE_EXTENSIONS = NotImplemented

    def __init__(self, settings):
        """
        Args:
            settings (dict, optional): connector specific settings or None
        """

    def connect_to_source(self, source, **extras):
        """Connects to source, ex: connecting to a database where source is a connection string.

        Args:
            source (str): file path or URL to connect to
            **extras: additional source specific connection data
        """
        raise NotImplementedError()

    def disconnect(self):
        """Disconnect from connected source."""
        raise NotImplementedError()

    def get_tables_and_properties(self) -> dict[str, TableProperties]:
        """Returns table names and properties."""
        raise NotImplementedError()

    def get_data_iterator(self, table, options, max_rows=-1):
        """
        Returns a data iterator and data header.
        """
        raise NotImplementedError()

    def get_table_cell(self, table: str, row: int, column: int, options: dict) -> Any:
        """Returns data from a single table cell."""
        row_iter, _ = self.get_data_iterator(table, options)
        try:
            row_data = next(islice(row_iter, row, None))
        except StopIteration:
            raise ReaderError(f"{table} doesn't have row {row}")
        try:
            return row_data[column]
        except IndexError:
            raise ReaderError(f"{table} doesn't have column {column}")

    @staticmethod
    def _resolve_max_rows(options, max_rows=-1):
        options_max_rows = options.get("max_rows", -1)
        if options_max_rows == -1:
            return max_rows
        if max_rows == -1:
            return options_max_rows
        return min(max_rows, options_max_rows)

    def get_data(self, table, options, max_rows=-1, start=0):
        """
        Return data read from data source table in table. If max_rows is
        specified only that number of rows.
        """
        max_rows = self._resolve_max_rows(options, max_rows)
        data_iter, header = self.get_data_iterator(table, options, max_rows)
        data_iter = islice(data_iter, start, None)
        data = list(data_iter)
        return data, header

    def resolve_values_for_fixed_position_mappings(self, tables_mappings, table_options, reader_error_is_fatal):
        for table, named_mappings in tables_mappings.items():
            parsed_mappings = []
            for mapping_name, root_mapping in named_mappings:
                flattened_mappings = root_mapping.flatten()
                for mapping in flattened_mappings:
                    if mapping.position != Position.fixed:
                        continue
                    target_table, row, column = parse_fixed_position_value(mapping.value)
                    if target_table is None:
                        target_table = table
                    options = table_options.get(target_table, {})
                    try:
                        mapping.value = self.get_table_cell(target_table, row, column, options)
                    except ReaderError as error:
                        if reader_error_is_fatal:
                            raise error
                        mapping.value = None
                    mapping.position = Position.hidden
                parsed_mappings.append((mapping_name, root_mapping))
            tables_mappings[table] = parsed_mappings
        return tables_mappings

    def get_mapped_data(
        self,
        tables_mappings,
        table_options,
        table_column_convert_specs,
        table_default_column_convert_fns,
        table_row_convert_specs,
        unparse_value=identity,
        max_rows=-1,
    ):
        """
        Reads all mappings in dict tables_mappings, where key is name of table
        and value is the mappings for that table.

        Args:
            tables_mappings (dict): mapping from table name to list of import mappings
            table_options (dict): mapping from table name to table-specific import options
            table_column_convert_specs (dict): mapping from table name to column data type conversion settings
            table_default_column_convert_fns (dict): mapping from table name to
                default column data type converter
            table_row_convert_specs (dict): mapping from table name to row data type conversion settings
            unparse_value (Callable): callable that converts imported values to database representation
            max_rows (int): maximum number of source rows to map

        Returns:
            tuple: mapped data and a list of errors, if any
        """
        mapped_data = {}
        errors = []
        for table, named_mapping_specs in tables_mappings.items():
            column_convert_fns = table_column_convert_specs.get(table, {})
            default_column_convert_fn = table_default_column_convert_fns.get(table)
            row_convert_fns = table_row_convert_specs.get(table, {})
            options = table_options.get(table, {})
            table_max_rows = self._resolve_max_rows(options, max_rows)
            try:
                data_source, header = self.get_data_iterator(table, options, table_max_rows)
            except ReaderError as error:
                errors.append(str(error))
                continue
            mappings = []
            mapping_names = []
            for named_mapping_spec in named_mapping_specs:
                name, mapping = parse_named_mapping_spec(named_mapping_spec)
                mappings.append(mapping)
                mapping_names.append(name)
            try:
                data, t_errors = get_mapped_data(
                    data_source,
                    mappings,
                    header,
                    table,
                    column_convert_fns,
                    default_column_convert_fn,
                    row_convert_fns,
                    unparse_value,
                    mapping_names,
                )
            except (ReaderError, ParameterValueFormatError, InvalidMappingComponent) as error:
                errors.append(str(error))
                continue
            for key, value in data.items():
                mapped_data.setdefault(key, []).extend(value)
            errors.extend([(table, err) for err in t_errors])
        return mapped_data, errors
