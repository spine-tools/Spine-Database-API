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
Module contains a .gdx writer implementation.

"""
import math
from gdx2py import GAMSSet, GAMSScalar, GAMSParameter, GdxFile
from gdx2py.gdxfile import EPS_VALUE
import gdxcc
from .writer import Writer, WriterException


SPECIAL_CONVERSIONS = {EPS_VALUE: gdxcc.GMS_SV_EPS, math.inf: gdxcc.GMS_SV_PINF, -math.inf: gdxcc.GMS_SV_MINF}


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
        self._tables = dict()
        self._table_dimensions = dict()
        self._current_table_name = None
        self._current_table = None
        self._dimensions_missing = True

    def finish(self):
        if self._gdx_file is not None:
            try:
                for table_name, table in self._tables.items():
                    _table_to_gdx(self._gdx_file, table, table_name, self._table_dimensions.get(table_name))
            finally:
                self._gdx_file.close()

    def finish_table(self):
        if self._current_table_name is None:
            return
        self._tables.setdefault(self._current_table_name, list()).extend(self._current_table)
        self._current_table_name = None

    def start(self):
        try:
            self._gdx_file = GdxFile(self._file_path, "w", self._gams_dir)
        except RuntimeError as e:
            raise WriterException(f"Could not open .gdx file : {e}")

    def start_table(self, table_name, title_key):
        if not table_name:
            raise WriterException("Gdx does not support anonymous tables.")
        if table_name in self._gdx_file:
            raise WriterException("Gdx does not support appending data to existing sets.")
        self._current_table_name = table_name
        self._current_table = list()
        self._dimensions_missing = True
        return True

    def write_row(self, row):
        # First row should contain dimensions unless we are exporting a GAMS scalar.
        if not self._current_table and self._dimensions_missing and row and isinstance(row[0], str):
            dimensions = tuple(row)
            previous_dimensions = self._table_dimensions.get(self._current_table_name)
            if previous_dimensions is not None:
                if dimensions != previous_dimensions:
                    raise WriterException(f"Cannot append to `{self._current_table_name}`: dimensions don't match.")
            else:
                self._table_dimensions[self._current_table_name] = dimensions
            self._dimensions_missing = False
            return True
        self._current_table.append(tuple(row))
        return True


def _table_to_gdx(gdx_file, table, table_name, dimensions):
    """Writes a table to .gdx file.

    Args:
        gdx_file (GdxFile): output file
        table (list of list): list of table rows
        table_name (str): output set's name
        dimensions (tuple of str): output set's dimensions
    """
    is_parameter = dimensions is not None and dimensions[-1] == ""
    first_row = table[0] if table else []
    if first_row:
        if len(first_row) == 1 and isinstance(first_row[0], (float, int)):
            set_ = GAMSScalar(first_row[0])
        elif is_parameter:
            n_dimensions = len(first_row) - 1
            data = {row[:-1]: _convert_to_gams(row[-1]) for row in table}
            set_ = GAMSParameter(data, dimensions[:n_dimensions])
        else:
            try:
                set_ = GAMSSet(table, dimensions)
            except ValueError as e:
                raise WriterException(f"Error writing empty table '{table_name}': {e}")
    else:
        set_ = GAMSParameter({}, dimensions[:-1]) if is_parameter else GAMSSet(table, dimensions)
    try:
        gdx_file[table_name] = set_
    except TypeError as e:
        if isinstance(set_, GAMSSet):
            raise WriterException(f"A column contains a mixture of numeric and non-numeric elements.")
        raise e
    except ValueError as e:
        if isinstance(set_, GAMSParameter):
            raise WriterException(f"Failed to create GAMS parameter in table '{table_name}': {e}")
        raise e


def _convert_to_gams(x):
    """Converts special float values to corresponding GAMS constants, otherwise returns x as is.

    Args:
        x (float): value to convert

    Returns:
        float: converted value
    """
    if not isinstance(x, float):
        return x
    if math.isnan(x):
        return gdxcc.GMS_SV_UNDEF
    return SPECIAL_CONVERSIONS.get(x, x)
