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
Module contains a .csv writer implementation.

:author: A. Soininen (VTT)
:date:   9.12.2020
"""
import csv
import os.path
from .writer import Writer


class CsvWriter(Writer):
    def __init__(self, path, backup_file_name):
        """
        Args:
            path (str): path to output directory
            backup_file_name (str): output file name if no table name is provided by the mappings
        """
        super().__init__()
        self._path = path
        self._default_table_name = backup_file_name
        self._file = None
        self._out = None
        self._file_name = None
        self._finished_files = list()

    def finish_table(self):
        """See base class."""
        self._file.close()
        self._finished_files.append(self._file_name)
        self._file_name = None
        self._file = None
        self._out = None

    def output_files(self):
        """Returns absolute paths to files that have been written.

        Returns:
            list of str: list of file paths
        """
        return self._finished_files

    def start_table(self, table_name):
        """See base class."""
        if table_name is None:
            table_name = self._default_table_name
        else:
            table_name = table_name + ".csv"
        self._file_name = os.path.join(self._path, table_name)
        self._file = open(self._file_name, "w", newline="")
        self._out = csv.writer(self._file)
        return True

    def write_row(self, row):
        """See base class."""
        self._out.writerow(row)
        return True
