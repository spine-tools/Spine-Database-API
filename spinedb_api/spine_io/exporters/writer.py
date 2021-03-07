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
Module contains the :class:`Writer` base class and functions to write tabular data.

:author: A. Soininen (VTT)
:date:   8.12.2020
"""
from contextlib import contextmanager
from spinedb_api.export_mapping import rows, titles


def write(db_map, writer, root_mapping):
    """
    Writes given mapping.

    Args:
        db_map (DatabaseMappingBase): database map
        writer (Writer): target writer
        root_mapping (Mapping): root mapping
    """
    with _new_write(writer):
        for title, title_key in titles(root_mapping, db_map):
            with _new_table(title, writer) as table_started:
                if not table_started:
                    break
                for row in rows(root_mapping, db_map, title_key):
                    write_more = writer.write_row(row)
                    if not write_more:
                        break


class Writer:
    def finish(self):
        """Finishes writing."""

    def finish_table(self):
        """Finishes writing the current table."""

    def start(self):
        """Prepares writer for writing."""

    def start_table(self, table_name):
        """
        Starts a new table.

        Args:
            table_name (str): table's name

        Returns:
            bool: True if the table was successfully started, False otherwise
        """
        raise NotImplementedError()

    def write_row(self, row):
        """
        Writes a row of data.

        Args:
            row (list): row elements

        Returns:
            bool: True if more rows can be written, False otherwise
        """
        raise NotImplementedError()


class WriterException(Exception):
    """Writer exception."""


@contextmanager
def _new_write(writer):
    """
    Manages writing contexts.

    Args:
        writer (Writer): a writer

    Yields:
        NoneType
    """
    try:
        writer.start()
        yield None
    finally:
        writer.finish()


@contextmanager
def _new_table(table_name, writer):
    """
    Manages table contexts.

    Args:
        table_name (str): table's name
        writer (Writer): a writer

    Yields:
        bool: whether or not the new table was successfully started
    """
    try:
        table_started = writer.start_table(table_name)
        yield table_started
    finally:
        writer.finish_table()
