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
Module contains a .gdx writer implementation.

:author: A. Soininen (VTT)
:date:   9.12.2020
"""

from gdx2py import GAMSSet, GAMSScalar, GAMSParameter, GdxFile
from .writer import Writer, WriterException


class GdxWriter(Writer):
    def __init__(self, file_path, gams_directory):
        """
        Args:
            file_path (str): path ot output file
            gams_directory (str): GAMS directory
        """
        super().__init__()
        self._file_path = file_path
        self._gams_dir = gams_directory
        self._gdx_file = None
        self._current_table_name = None
        self._current_table = None
        self._dimensions = None

    def finish(self):
        if self._gdx_file is not None:
            self._gdx_file.close()

    def finish_table(self):
        first_row = self._current_table[0] if self._current_table else []
        if first_row:
            is_parameter = isinstance(self._current_table[-1][-1], (float, int))
            if is_parameter:
                if len(first_row) == 1:
                    set_ = GAMSScalar(first_row[0])
                else:
                    n_dimensions = len(first_row) - 1
                    data = {row[:-1]: row[-1] for row in self._current_table}
                    set_ = GAMSParameter(data, self._dimensions[:n_dimensions])
            else:
                try:
                    set_ = GAMSSet(self._current_table, self._dimensions)
                except ValueError as e:
                    raise WriterException(f"Error writing empty table '{self._current_table_name}': {e}")
        else:
            set_ = GAMSSet(self._current_table, self._dimensions)
        self._gdx_file[self._current_table_name] = set_

    def start(self):
        try:
            self._gdx_file = GdxFile(self._file_path, "w", self._gams_dir)
        except RuntimeError as e:
            raise WriterException(f"Could not open .gdx file : {e}")

    def start_table(self, table_name, title_key):
        if not table_name:
            raise WriterException("Gdx does not support anonymous tables.")
        if self._current_table_name in self._gdx_file:
            raise WriterException("Gdx does not support appending data to existing sets.")
        self._current_table_name = table_name
        self._current_table = list()
        self._dimensions = None
        return True

    def write_row(self, row):
        # First row should contain dimensions unless we are exporting a GAMS scalar.
        if self._dimensions is None and row and isinstance(row[0], str):
            self._dimensions = tuple(row)
            return True
        self._current_table.append(tuple(row))
        return True
