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
from .writer import Writer


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

    def finish(self):
        self._gdx_file.close()

    def finish_table(self):
        set_ = GAMSSet(self._current_table, "*")
        self._gdx_file[self._current_table_name] = set_

    def start(self):
        self._gdx_file = GdxFile(self._file_path, "w", self._gams_dir)

    def start_table(self, table_name):
        self._current_table_name = table_name
        self._current_table = list()
        return True

    def write_row(self, row):
        self._current_table.append(row)
        return True
