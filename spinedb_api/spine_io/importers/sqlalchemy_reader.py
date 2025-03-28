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
""" Contains SQLAlchemyReader class. """
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import Session
from ...exception import ReaderError
from .reader import Reader, TableProperties


class SQLAlchemyReader(Reader):
    """A reader for SQL databases."""

    DISPLAY_NAME = "SQL database"
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
        self._schema = None
        self._metadata = None

    def connect_to_source(self, source, **extras):
        """Saves source.

        Args:
            source (str): url
            **extras: optional database schema
        """
        self._connection_string = source
        self._engine = create_engine(source, future=True)
        self._connection = self._engine.connect()
        self._session = Session(self._engine, future=True)
        self._schema = extras.get("schema")
        self._metadata = MetaData(schema=self._schema)
        self._metadata.reflect(bind=self._engine)

    def disconnect(self):
        """Disconnect from connected source."""
        self._metadata = None
        self._schema = None
        self._session.close()
        self._session = None
        self._connection.close()
        self._connection_string = None
        self._engine = None
        self._connection = None

    def get_tables_and_properties(self):
        """See base class"""
        return {table: TableProperties() for table in self._engine.table_names(schema=self._schema)}

    def get_data_iterator(self, table, options, max_rows=-1):
        """Creates an iterator for the database connection.

        Args:
            table (str): table name
            options (dict): dict with options, not used
            max_rows (int): how many rows of data to read, if -1 read all rows (default: {-1})

        Returns:
            tuple: iterator, header, column count
        """
        if self._schema is not None:
            table = self._schema + "." + table
        try:
            db_table = self._metadata.tables[table]
        except KeyError:
            raise ReaderError(f"no such table: '{table}'")
        header = [str(name) for name in db_table.columns.keys()]

        query = self._session.query(db_table)
        if max_rows > 0:
            query = query.limit(max_rows)

        return query, header
