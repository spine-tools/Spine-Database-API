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
Module contains a .csv writer implementation.

"""
import csv
import os
import os.path
from .writer import Writer


class CsvWriter(Writer):
    def __init__(self, path, backup_file_name):
        """
        Args:
            path (Path or str): path to output directory
            backup_file_name (str): output file name if no table name is provided by the mappings
        """
        super().__init__()
        self._path = path
        self._default_table_name = backup_file_name
        self._file = None
        self._out = None
        self._file_name = None
        self._finished_files = set()

    def finish_table(self):
        """See base class."""
        self._file.close()
        self._finished_files.add(self._file_name)
        self._file_name = None
        self._file = None
        self._out = None

    def output_files(self):
        """Returns absolute paths to files that have been written.

        Returns:
            set of str: file paths
        """
        return self._finished_files

    def start_table(self, table_name, title_key):
        """See base class."""
        if table_name is None:
            table_name = self._default_table_name
        else:
            table_name = table_name + ".csv"
        self._file_name = os.path.join(self._path, table_name)
        if self._file_name not in self._finished_files and os.path.exists(self._file_name):
            os.remove(self._file_name)
        self._file = open(self._file_name, "a", newline="")
        self._out = csv.writer(self._file)
        return True

    def write_row(self, row):
        """See base class."""
        self._out.writerow(row)
        return True
