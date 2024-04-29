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
Module contains the :class:`Writer` base class and functions to write tabular data.

"""
from contextlib import contextmanager
from copy import copy
from sqlalchemy.exc import OperationalError

from spinedb_api import SpineDBAPIError
from spinedb_api.export_mapping import rows, titles
from spinedb_api.export_mapping.export_mapping import drop_non_positioned_tail
from spinedb_api.export_mapping.group_functions import NoGroup


def write(db_map, writer, *mappings, empty_data_header=True, max_tables=None, max_rows=None, group_fns=NoGroup.NAME):
    """
    Writes given mapping.

    Args:
        db_map (DatabaseMapping): database map
        writer (Writer): target writer
        mappings (Mapping): root mappings
        empty_data_header (bool or Iterable of bool): True to write at least header rows even if there is no data,
            False to write nothing; a list of booleans applies to each mapping individually
        max_tables (int, optional): maximum number of tables to write
        max_rows (int, optional): maximum number of rows/table to write
        group_fns (str or Iterable of str): group function names for each mappings
    """
    if isinstance(empty_data_header, bool):
        empty_data_header = len(mappings) * [empty_data_header]
    if isinstance(group_fns, str):
        group_fns = len(mappings) * [group_fns]
    with _new_write(writer):
        for mapping, header_for_empty_data, group_fn in zip(mappings, empty_data_header, group_fns):
            mapping = drop_non_positioned_tail(copy(mapping))
            for title, title_key in titles(mapping, db_map, limit=max_tables):
                with _new_table(writer, title, title_key) as table_started:
                    if not table_started:
                        break
                    try:
                        if max_rows is None:
                            for row in rows(mapping, db_map, title_key, header_for_empty_data, group_fn=group_fn):
                                write_more = writer.write_row(row)
                                if not write_more:
                                    break
                        else:
                            for n, row in enumerate(
                                rows(mapping, db_map, title_key, header_for_empty_data, group_fn=group_fn)
                            ):
                                write_more = writer.write_row(row)
                                if not write_more or n + 1 == max_rows:
                                    break
                    except OperationalError as error:
                        raise SpineDBAPIError(str(error))


class Writer:
    def finish(self):
        """Finishes writing."""

    def finish_table(self):
        """Finishes writing the current table."""

    def start(self):
        """Prepares writer for writing."""

    def start_table(self, table_name, title_key):
        """
        Starts a new table.

        Args:
            table_name (str): table's name
            title_key (dict): table state dictionary

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
def _new_table(writer, table_name, title_key):
    """
    Manages table contexts.

    Args:
        writer (Writer): a writer
        table_name (str): table's name
        title_key (dict, optional)

    Yields:
        bool: whether or not the new table was successfully started
    """
    try:
        table_started = writer.start_table(table_name, title_key)
        yield table_started
    finally:
        writer.finish_table()
