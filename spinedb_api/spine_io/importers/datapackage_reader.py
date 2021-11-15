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
Contains DataPackageConnector class.

:author: M. Marin (KTH)
:date:   15.11.2020
"""
import threading
from datapackage import Package
from .reader import SourceConnection


class DataPackageConnector(SourceConnection):
    """Template class to read data from another QThread."""

    # name of data source, ex: "Text/CSV"
    DISPLAY_NAME = "Datapackage"

    # dict with option specification for source.
    OPTIONS = {}  # FIXME?

    FILE_EXTENSIONS = "*.json"

    def __init__(self, settings):
        super().__init__(settings)
        self._filename = None
        self._datapackage = None
        self._resource_name_lock = threading.Lock()

    def connect_to_source(self, source):
        """Creates datapackage.

        Args:
            source (str): filepath of a datapackage.json file
        """
        if source:
            self._datapackage = Package(source)
            self._filename = source

    def disconnect(self):
        """Disconnect from connected source.
        """
        if self._datapackage:
            self._datapackage = None
        self._filename = None

    def get_tables(self):
        """Returns resources' mappings and their options.

        Returns:
            dict: key is resource name, value is mapping and options.
        """
        if not self._datapackage:
            return {}
        tables = {}
        for resource in self._datapackage.resources:
            with self._resource_name_lock:
                if resource.name is None:
                    resource.infer()
            tables[resource.name] = {"options": {}}  # FIXME?
        return tables

    def get_data_iterator(self, table, options, max_rows=-1):
        """
        Return data read from data source table in table. If max_rows is
        specified only that number of rows.
        """
        if not self._datapackage:
            return iter([]), []

        for resource in self._datapackage.resources:
            with self._resource_name_lock:
                if resource.name is None:
                    resource.infer()
            if table == resource.name:
                iterator = (item for row, item in enumerate(resource.iter(cast=False)) if row != max_rows)
                header = resource.schema.field_names
                return iterator, header
        # table not found
        return iter([]), []
