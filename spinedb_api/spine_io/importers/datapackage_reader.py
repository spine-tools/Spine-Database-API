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

""" Contains DataPackageReader class. """
from itertools import chain
import threading
import frictionless
from ...exception import ReaderError
from .reader import Reader, TableProperties


class DatapackageReader(Reader):
    """A reader for datapackages."""

    # name of data source, ex: "Text/CSV"
    DISPLAY_NAME = "Datapackage"

    # dict with option specification for source.
    OPTIONS = {"has_header": {"type": bool, "label": "Has header", "default": True}}

    FILE_EXTENSIONS = "*.json"

    def __init__(self, settings):
        super().__init__(settings)
        self._filename = None
        self._datapackage = None
        self._resource_name_lock = threading.Lock()

    def __getstate__(self):
        """Builds a state that can be pickled.

        Returns:
            dict: picklable representation of the connector
        """
        state = self.__dict__.copy()
        del state["_resource_name_lock"]
        return state

    def __setstate__(self, state):
        """Restores connector from pickled state.

        Args:
            state (dict): pickled state
        """
        self.__dict__.update(state)
        self._resource_name_lock = threading.Lock()

    def connect_to_source(self, source, **extras):
        """Creates datapackage.

        Args:
            source (str): filepath of a datapackage.json file
            **extras: ignored
        """
        if source:
            self._datapackage = frictionless.Package(source)
            self._filename = source

    def disconnect(self):
        """Disconnect from connected source."""
        if self._datapackage:
            self._datapackage = None
        self._filename = None

    def get_tables_and_properties(self):
        """See base class."""
        if not self._datapackage:
            return {}
        tables = {}
        for resource in self._datapackage.resources:
            with self._resource_name_lock:
                if resource.name is None:
                    resource.infer()
            tables[resource.name] = TableProperties()
        return tables

    def get_data_iterator(self, table, options, max_rows=-1):
        """
        Return data read from data source table in table. If max_rows is
        specified only that number of rows.
        """
        if not self._datapackage:
            return iter([]), []
        max_rows = max_rows if max_rows >= 0 else None

        def iterator(resource, max_rows):
            with resource:
                try:
                    if max_rows is None:
                        for row in resource.row_stream:
                            yield row.to_list(json=True)
                    else:
                        for row_count, row in enumerate(resource.row_stream):
                            if row_count >= max_rows:
                                break
                            yield row.to_list(json=True)
                except frictionless.exception.FrictionlessException as error:
                    raise ReaderError(str(error)) from error

        has_header = options.get("has_header", True)
        for resource in self._datapackage.resources:
            with self._resource_name_lock:
                if resource.name is None:
                    resource.infer()
            if table == resource.name:
                i = iterator(resource, max_rows)
                with resource:
                    if has_header:
                        header = resource.header
                        return i, header
                    return chain([resource.header.labels], i), None
        raise ReaderError(f"no such table '{table}'")
