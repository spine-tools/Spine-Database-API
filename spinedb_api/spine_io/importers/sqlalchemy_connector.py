######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Contains SqlAlchemyConnector class.

:author: P. Vennström (VTT)
:date:   1.6.2019
"""


from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import Session
from ..io_api import SourceConnection


class SqlAlchemyConnector(SourceConnection):
    """Template class to read data from another QThread."""

    DISPLAY_NAME = "SqlAlchemy"
    """name of data source"""

    OPTIONS = {}
    """dict with option specification for source."""

    FILE_EXTENSIONS = "*.sqlite"

    def __init__(self, settings):
        super().__init__(settings)
        self._connection_string = None
        self._engine = None
        self._connection = None
        self._session = None
        self._metadata = MetaData()

    def connect_to_source(self, source):
        """saves filepath

        Args:
            source (str): filepath
        """
        self._connection_string = source
        self._engine = create_engine("sqlite:///" + source)
        self._connection = self._engine.connect()
        self._session = Session(self._engine)
        self._metadata.reflect(bind=self._engine)

    def disconnect(self):
        """Disconnect from connected source.
        """
        self._connection.close()
        self._connection_string = None
        self._engine = None
        self._connection = None
        self._session = None
        self._metadata = MetaData()

    def get_tables(self):
        """Method that should return a list of table names, list(str)

        Returns:
            list of str: Table names in list
        """
        tables = list(self._engine.table_names())
        return tables

    def get_data_iterator(self, table, options, max_rows=-1):
        """Creates a iterator for the file in self.filename

        Args:
            table (str): table name
            options (dict): dict with options, not used
            max_rows (int): how many rows of data to read, if -1 read all rows (default: {-1})

        Returns:
            tuple: iterator, header, column count
        """
        db_table = self._metadata.tables[table]
        header = [str(name) for name in db_table.columns.keys()]
        num_cols = len(header)

        query = self._session.query(db_table)
        if max_rows > 0:
            query = query.limit(max_rows)

        return query, header, num_cols
