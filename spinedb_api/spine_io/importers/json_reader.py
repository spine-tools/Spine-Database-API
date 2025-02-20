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

""" Contains JSONReader class. """
import itertools
import os
import sys
import ijson
from ijson import IncompleteJSONError
from ijson.backends.python import UnexpectedSymbol
from ...exception import ReaderError
from .reader import Reader, TableProperties


class JSONReader(Reader):
    """A reader for JSON files."""

    DISPLAY_NAME = "JSON"

    # dict with option specification for source.
    OPTIONS = {"max_depth": {"type": int, "label": "Maximum depth", "default": 8}}

    # File extensions for modal widget that that returns source object and action (OK, CANCEL)
    FILE_EXTENSIONS = "*.json"

    def __init__(self, settings):
        super().__init__(settings)
        self._filename = None
        self._root_prefix = None

    def connect_to_source(self, source, **extras):
        """saves filepath

        Args:
            source (str): filepath
            **extras: ignored
        """
        self._filename = source
        self._root_prefix = os.path.splitext(os.path.basename(source))[0]

    def disconnect(self):
        """Disconnect from connected source."""

    def get_tables_and_properties(self):
        prefixes = {}
        with open(self._filename, encoding="utf-8") as f:
            for prefix, event, _ in ijson.parse(f):
                if event in ("start_map", "start_array"):
                    prefixes[".".join([self._root_prefix, prefix])] = None
        tables = {prefix: TableProperties() for prefix in itertools.islice(prefixes.keys(), 1, None)}
        tables[self._root_prefix] = TableProperties()
        return tables

    def file_iterator(self, table, options, max_rows=-1):
        if max_rows == -1:
            max_rows = sys.maxsize
        max_depth = options.get("max_depth", self.OPTIONS["max_depth"]["default"])
        prefix = ".".join(table.split(".")[1:])
        with open(self._filename, "rb") as f:
            row = 0
            try:
                for obj in ijson.items(f, prefix):
                    for x in _tabulize_json(obj):
                        if row > max_rows:
                            return
                        yield x[:max_depth]
                        row += 1
            except (IncompleteJSONError, UnexpectedSymbol) as error:
                raise ReaderError(f"failed to read JSON: {error}") from error

    def get_data_iterator(self, table, options, max_rows=-1):
        """
        Returns data read from data source table in table. If max_rows is
        specified only that number of rows.
        """
        return self.file_iterator(table, options, max_rows=max_rows), []


def _tabulize_json(obj):
    if isinstance(obj, dict):
        yield from _tabulize_json_object(obj)
    elif isinstance(obj, list):
        yield from _tabulize_json_array(obj)
    else:
        yield [obj]


def _tabulize_json_object(obj):
    for key, value in obj.items():
        for x in _tabulize_json(value):
            yield [key] + x


def _tabulize_json_array(arr):
    for i, item in enumerate(arr):
        for x in _tabulize_json(item):
            yield [i] + x
