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
Contains CSVConnector class and a help function.

"""


import csv
from itertools import islice
import chardet
from .reader import SourceConnection


class CSVConnector(SourceConnection):
    """Template class to read data from another QThread."""

    DISPLAY_NAME = "Text/CSV"
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

    def get_tables(self):
        """
        Returns a mapping from file name to options.

        Returns:
            dict
        """
        options = {}
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
        for encoding in try_encodings:
            with open(self._filename, encoding=encoding) as csvfile:
                try:
                    dialect = csv.Sniffer().sniff(csvfile.read(1024))
                    if dialect.delimiter in [",", ";"]:
                        options["delimiter"] = dialect.delimiter
                    elif dialect.delimiter == "\t":
                        options["delimiter"] = "Tab"
                    else:
                        options["delimiter_custom"] = dialect.delimiter
                    options.update({"quotechar": dialect.quotechar, "skip": 0})
                except csv.Error:
                    pass
                except UnicodeDecodeError:
                    continue
                try:
                    options["has_header"] = csv.Sniffer().has_header(csvfile.read(1024))
                except csv.Error:
                    pass
                options["encoding"] = encoding
                break
        return {"data": {"options": options}}

    @staticmethod
    def parse_options(options):
        """Parses options dict to dialect and quotechar options for csv.reader

        Arguments:
            options (dict): dict with options:
                "encoding": file text encoding
                "delimiter": file delimiter
                "quotechar": file quotechar
                "has_header": if first row should be treated as a header
                "skip": how many rows should be skipped

        Returns:
            tuple(dict, bool, integer): tuple dialect for csv.reader,
                                        quotechar for csv.reader and
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
        """creates an iterator that reads max_rows number of rows from text file

        Arguments:
            options (dict): dict with options:
            max_rows (integer): max number of rows to read, if -1 then read all rows

        Returns:
            iterator: iterator of csv file
        """
        if not self._filename:
            return []
        encoding, dialect, _has_header, skip = self.parse_options(options)
        if max_rows == -1:
            max_rows = None
        else:
            max_rows += skip
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
