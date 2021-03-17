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
Contains a class template for a data source connector used in import ui.

:author: P. Vennström (VTT)
:date:   1.6.2019
"""

from spinedb_api.import_mapping.generator import get_mapped_data
from spinedb_api.import_mapping.import_mapping_compat import parse_named_mapping_spec
from spinedb_api import DateTime, Duration, ParameterValueFormatError

TYPE_STRING_TO_CLASS = {"string": str, "datetime": DateTime, "duration": Duration, "float": float, "boolean": bool}

TYPE_CLASS_TO_STRING = {type_class: string for string, type_class in TYPE_STRING_TO_CLASS.items()}


class SourceConnection:
    """Template class to read data from another QThread."""

    # name of data source, ex: "Text/CSV"
    DISPLAY_NAME = "unnamed source"

    # dict with option specification for source.
    OPTIONS = {}

    # File extensions for modal widget that that returns action (OK, CANCEL) and source object
    FILE_EXTENSIONS = NotImplemented

    def __init__(self, settings):
        """
        Args:
            settings (dict, optional): connector specific settings or None
        """

    def connect_to_source(self, source):
        """Connects to source, ex: connecting to a database where source is a connection string.

        Arguments:
            source {} -- object with information on source to be connected to, ex: filepath string for a csv connection
        """
        raise NotImplementedError()

    def disconnect(self):
        """Disconnect from connected source.
        """
        raise NotImplementedError()

    def get_tables(self):
        """Method that should return a list of table names, list(str)

        Raises:
            NotImplementedError: [description]
        """
        raise NotImplementedError()

    def get_data_iterator(self, table, options, max_rows=-1):
        """
        Function that should return a data iterator and data header.
        """
        raise NotImplementedError()

    def get_data(self, table, options, max_rows=-1):
        """
        Return data read from data source table in table. If max_rows is
        specified only that number of rows.
        """
        data_iter, header = self.get_data_iterator(table, options, max_rows)
        data = list(data_iter)
        return data, header

    def get_mapped_data(
        self, tables_mappings, table_options, table_column_convert_specs, table_row_convert_specs, max_rows=-1
    ):
        """
        Reads all mappings in dict tables_mappings, where key is name of table
        and value is the mappings for that table.
        emits mapped data when ready.
        """
        mapped_data = {}
        errors = []
        for table, named_mapping_specs in tables_mappings.items():
            column_convert_fns = table_column_convert_specs.get(table, {})
            row_convert_fns = table_row_convert_specs.get(table, {})
            options = table_options.get(table, {})
            data_source, header = self.get_data_iterator(table, options, max_rows)
            mappings = []
            for named_mapping_spec in named_mapping_specs:
                _, mapping = parse_named_mapping_spec(named_mapping_spec)
                mappings.append(mapping)
            try:
                data, t_errors = get_mapped_data(
                    data_source, mappings, header, table, column_convert_fns, row_convert_fns
                )
            except ParameterValueFormatError as error:
                errors.append(str(error))
                continue
            for key, value in data.items():
                mapped_data.setdefault(key, []).extend(value)
            errors.extend([(table, err) for err in t_errors])

        return mapped_data, errors
