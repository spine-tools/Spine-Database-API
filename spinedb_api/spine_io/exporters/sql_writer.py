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
Module contains an SQL writer implementation.

"""
from sqlalchemy import Boolean, Column, create_engine, Float, Integer, MetaData, String, Table, DateTime
from sqlalchemy.orm import Session
from spinedb_api import parameter_value
from .writer import Writer, WriterException


class SqlWriter(Writer):
    """Export writer that targets SQL databases."""

    def __init__(self, database, overwrite_existing):
        """
        Args:
            database (str): URL or path to output .sqlite file
            overwrite_existing (bool): if True, overwrites tables in existing database, otherwise appends to the tables
        """
        super().__init__()
        self._overwrite_existing = overwrite_existing
        if database.find("://") < 0:
            database = "sqlite:///" + database
        self._engine = create_engine(database)
        self._connection = self._engine.connect()
        self._metadata = MetaData()
        self._metadata.reflect(bind=self._engine)
        self._session = Session(self._engine)
        self._table_name = None
        self._column_names = None
        self._column_converters = None
        self._table = None
        self._finished_table_names = set()

    def finish(self):
        """Closes the database connection."""
        self._session.close()
        self._connection.close()

    def finish_table(self):
        """Commits current session."""
        if self._column_names and self._table is None:
            # Create an empty table if no rows were available in the database.
            columns = [Column(name, String) for name in self._column_names]
            self._table = Table(self._table_name, self._metadata, *columns)
            self._table.create(self._engine)
        self._finished_table_names.add(self._table_name)
        self._session.commit()

    def start_table(self, table_name, title_key):
        """See base class."""
        if not table_name:
            raise WriterException("Cannot create anonymous SQL tables.")
        self._table = self._metadata.tables.get(table_name)
        if self._overwrite_existing and self._table is not None and table_name not in self._finished_table_names:
            self._table.drop(self._engine)
            self._metadata.remove(self._table)
            self._table = None
        self._table_name = table_name
        self._column_names = None
        return True

    def write_row(self, row):
        """See base class."""
        if self._column_names is None:
            # Expecting first row to contain column names as headers.
            self._column_names = row
            return True
        if self._table is None:
            # Build columns using the second row.
            columns, self._column_converters = _database_columns_and_converters(self._column_names, row)
            self._table = Table(self._table_name, self._metadata, *columns)
            self._table.create(self._engine)
        elif self._column_converters is None:
            self._column_converters = _converters(row)
        row = [convert(x) for convert, x in zip(self._column_converters, row)]
        self._session.execute(self._table.insert().values(tuple(row)))
        return True


def _database_columns_and_converters(names, row):
    """Creates columns for a database table as well as converters to convert a row to correct types.

    Args:
        names (list of str): column names
        row (list): a data row for sniffing column types

    Returns:
        tuple: list of database columns and list of converter callables
    """
    types = []
    converters = []
    for x in row:
        if isinstance(x, float):
            types.append(Float)
            converters.append(float)
        elif isinstance(x, int):
            types.append(Integer)
            converters.append(int)
        elif isinstance(x, bool):
            types.append(Boolean)
            converters.append(bool)
        elif isinstance(x, parameter_value.DateTime):
            types.append(DateTime)
            converters.append(lambda v: v.value)
        elif isinstance(x, parameter_value.Duration):
            types.append(String)
            converters.append(lambda v: parameter_value.relativedelta_to_duration(v.value))
        else:
            types.append(String)
            converters.append(str)
    return [Column(name, type_, nullable=True) for name, type_ in zip(names, types)], converters


def _converters(row):
    """Creates converters to convert a row to correct types.

    Args:
        row (list): a data row for sniffing column types

    Returns:
        list of Callable: list of converter callables
    """
    converters = []
    for x in row:
        if isinstance(x, float):
            converters.append(float)
        elif isinstance(x, int):
            converters.append(int)
        elif isinstance(x, bool):
            converters.append(bool)
        elif isinstance(x, parameter_value.DateTime):
            converters.append(lambda v: v.value)
        elif isinstance(x, parameter_value.Duration):
            converters.append(lambda v: parameter_value.relativedelta_to_duration(v.value))
        else:
            converters.append(str)
    return converters
