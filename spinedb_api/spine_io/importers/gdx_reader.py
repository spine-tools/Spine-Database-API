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

""" Contains GDXReader class and a help function. """
from gdx2py import GAMSParameter, GAMSScalar, GAMSSet, GdxFile
from spinedb_api.exception import ReaderError
from ..gdx_utils import find_gams_directory
from .reader import Reader, TableProperties


class GDXReader(Reader):
    """A reader for .gdx files."""

    DISPLAY_NAME = "GDX"
    """name of data source"""

    OPTIONS = {}
    """dict with option specification for source"""

    FILE_EXTENSIONS = "*.gdx"
    """File extensions for modal widget that returns source object and action (OK, CANCEL)."""

    def __init__(self, settings):
        """
        Args:
            settings (dict): a dict from "gams_directory" to GAMS path; if the argument is None
                or the path is empty or None, a default path is used
        """
        super().__init__(settings)
        self._filename = None
        self._gdx_file = None
        gams_directory = settings.get("gams_directory") if settings is not None else None
        if gams_directory is not None and gams_directory:
            self._gams_dir = gams_directory
        else:
            self._gams_dir = find_gams_directory()

    def __exit__(self, exc_type, exc_value, traceback):
        self.disconnect()

    def __del__(self):
        self.disconnect()

    def connect_to_source(self, source, **extras):
        """
        Connects to given .gdx file.

        Args:
            source (str): path to .gdx file.
            **extras: ignored
        """
        if self._gams_dir is None:
            raise IOError("Could not find GAMS directory. Make sure you have GAMS installed.")
        self._filename = source
        self._gdx_file = GdxFile(source, gams_dir=self._gams_dir)

    def disconnect(self):
        """Disconnects from connected source."""
        if self._gdx_file is not None:
            self._gdx_file.close()

    def get_tables_and_properties(self):
        """
        Returns table names and options.

        GAMS scalars are also regarded as tables.
        """
        return {symbol[0]: TableProperties() for symbol in self._gdx_file}

    def get_data_iterator(self, table, options, max_rows=-1):
        """See base class."""
        if table not in self._gdx_file:
            raise ReaderError(f"no symbol called '{table}'")
        symbol = self._gdx_file[table]
        if symbol is None:
            raise ReaderError(f"the type of '{table}' is not supported.")
        if isinstance(symbol, GAMSScalar):
            return iter([[float(symbol)]]), ["Value"]
        domains = symbol.domain if symbol.domain is not None else symbol.dimension * [None]
        header = [domain if domain is not None else f"dim{i}" for i, domain in enumerate(domains)]
        if isinstance(symbol, GAMSSet):
            if symbol.elements and isinstance(symbol.elements[0], str):
                return iter([[key] for key in symbol.elements]), header
            return iter(list(keys) for keys in symbol.elements), header
        if isinstance(symbol, GAMSParameter):
            header.append("Value")
            symbol_keys = list(symbol.keys())
            if symbol_keys and isinstance(symbol_keys[0], str):
                return iter([keys] + [value] for keys, value in zip(symbol_keys, symbol.values())), header
            return iter(list(keys) + [value] for keys, value in zip(symbol_keys, symbol.values())), header
        raise RuntimeError("Unknown GAMS symbol type.")
