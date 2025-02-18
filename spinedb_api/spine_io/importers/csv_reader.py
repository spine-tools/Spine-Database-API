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

""" Contains CSVReader class and helper functions. """

import csv
from itertools import islice
from typing import Any
import chardet
from ...exception import ReaderError
from .reader import Reader, TableProperties


class CSVReader(Reader):
    """A reader for CSV files."""

    DISPLAY_NAME = "CSV"
    """name of data source, ex: "Text/CSV""" ""

    _ENCODINGS = ["utf-8", "utf-16", "utf-32", "ascii", "iso-8859-1", "iso-8859-2"]
    """List of available text encodings"""

    OPTIONS = {
        "encoding": {"type": list, "label": "Encoding", "Items": _ENCODINGS, "default": _ENCODINGS[0]},
        "delimiter": {"type": list, "label": "Delimiter", "Items": [",", ";", "Tab"], "default": ","},
        "delimiter_custom": {"type": str, "label": "Custom Delimiter", "MaxLength": 1, "default": ""},
        "quotechar": {"type": str, "label": "Quotechar", "MaxLength": 1, "default": ""},
        "has_header": {"type": bool, "label": "Has header", "default": False},
        "skip": {"type": int, "label": "Skip rows", "Minimum": 0, "default": 0},
    }
    """dict with option specification for source."""

    FILE_EXTENSIONS = "*.csv"

    def __init__(self, settings):
        super().__init__(settings)
        self._filename = None

    def connect_to_source(self, source, **extras):
        """saves filepath

        Args:
            source (str): filepath
            **extras: ignored
        """
        self._filename = source

    def disconnect(self):
        """Disconnect from connected source."""

    def get_tables_and_properties(self):
        """
        Returns a mapping from file name to options.

        Returns:
            TableOptions
        """
        options = {"skip": 0}
        # try to find options for file
        with open(self._filename, "rb") as input_file:
            sniff_result = chardet.detect(input_file.read(1024))
        sniffed_encoding = sniff_result["encoding"]
        if sniffed_encoding is not None:
            sniffed_encoding = sniffed_encoding.lower()
        # The sniffed encoding is not always correct. We may still need to try other options too.
        if sniffed_encoding in self._ENCODINGS:
            try_encodings = [sniffed_encoding] + [
                encoding for encoding in self._ENCODINGS if encoding != sniffed_encoding
            ]
        else:
            try_encodings = self._ENCODINGS
        options["encoding"] = try_encodings[0]
        sniffer = csv.Sniffer()
        for encoding in try_encodings:
            with open(self._filename, encoding=encoding) as csvfile:
                sample = csvfile.read(1024)
                try:
                    dialect = sniffer.sniff(sample)
                    if dialect.delimiter in [",", ";"]:
                        options["delimiter"] = dialect.delimiter
                    elif dialect.delimiter == "\t":
                        options["delimiter"] = "Tab"
                    else:
                        options["delimiter_custom"] = dialect.delimiter
                    options.update({"quotechar": dialect.quotechar})
                except csv.Error:
                    pass
                except UnicodeDecodeError:
                    continue
                try:
                    options["has_header"] = sniffer.has_header(sample)
                except csv.Error:
                    pass
                options["encoding"] = encoding
                break
        return {"data": TableProperties(options)}

    @staticmethod
    def parse_options(options):
        """Parses options dict for file_iterator.

        Arguments:
            options (CSVOptions): reader options

        Returns:
            tuple(str, dict, bool, integer): tuple encoding
                                        dialect for CSV reader,
                                        header presence
                                        number of rows to skip
        """
        encoding = options.get("encoding", None)
        delimiter = options.get("delimiter_custom", "")
        if not delimiter:
            delimiter = options.get("delimiter", ",")
        if not delimiter:
            delimiter = ","
        elif delimiter == "Tab":
            delimiter = "\t"
        dialect = {"delimiter": delimiter}
        quotechar = options.get("quotechar", None)
        if quotechar:
            dialect.update({"quotechar": quotechar})
        has_header = options.get("has_header", False)
        skip = options.get("skip", 0)
        return encoding, dialect, has_header, skip

    def file_iterator(self, options, max_rows):
        """Creates an iterator that reads max_rows number of rows from text file.

        Arguments:
            options (CSVOptions): dict with options:
            max_rows (integer): max number of rows to read, if -1 then read all rows

        Returns:
            iterator: iterator of csv file
        """
        if not self._filename:
            return []
        encoding, dialect, has_header, skip = self.parse_options(options)
        if max_rows == -1:
            max_rows = None
        else:
            max_rows += skip + (1 if has_header else 0)
        with open(self._filename, encoding=encoding) as text_file:
            csv_reader = csv.reader(text_file, **dialect)
            csv_reader = islice(csv_reader, skip, max_rows)
            yield from csv_reader

    def get_data_iterator(self, table, options, max_rows=-1):
        """Creates an iterator for the file in self.filename.

        Arguments:
            table (string): ignored, used in abstract IOWorker class
            options (dict): dict with options
            max_rows (int): how many rows of data to read, if -1 read all rows (default: {-1})

        Returns:
            tuple:
        """
        csv_iter = self.file_iterator(options, max_rows)
        try:
            first_row = next(csv_iter)
        except StopIteration:
            return iter([]), []
        has_header = options.get("has_header", False)
        if has_header:
            # Very good, we already have the first row
            header = first_row
        else:
            header = []
            # reset iterator
            csv_iter = self.file_iterator(options, max_rows)
        return csv_iter, header

    def get_table_cell(self, table: str, row: int, column: int, options: dict) -> Any:
        """See base class."""
        single_row_options = options.copy()
        single_row_options["skip"] = options["skip"] + row
        csv_iter, _ = self.get_data_iterator(table, single_row_options, max_rows=1)
        try:
            row_data = next(csv_iter)
        except StopIteration:
            raise ReaderError(f"requested row {row} but file is too short")
        try:
            return row_data[column]
        except IndexError:
            raise ReaderError(f"requested column {column} but table is too narrow")
