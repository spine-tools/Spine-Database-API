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

import csv
import os.path
import pickle
from tempfile import TemporaryDirectory
import unittest
from spinedb_api.exception import ReaderError
from spinedb_api.spine_io.importers.csv_reader import CSVReader


class TestCSVReader(unittest.TestCase):
    @staticmethod
    def _write_basic_csv(file_name):
        with open(file_name, "w", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["1a", "1b", "1c"])
            writer.writerow(["2a", "2b", "2c"])

    @staticmethod
    def _write_csv_with_header(file_name):
        with open(file_name, "w", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["header_1", "header_2", "header_3"])
            writer.writerow([11, 12, 13])
            writer.writerow([21, 22, 23])

    def test_get_tables_and_properties(self):
        with TemporaryDirectory() as data_directory:
            file_name = os.path.join(data_directory, "test_get_tables.csv")
            self._write_basic_csv(file_name)
            reader = CSVReader(None)
            reader.connect_to_source(file_name)
            tables = reader.get_tables_and_properties()
            self.assertEqual(len(tables), 1)
            self.assertTrue("data" in tables)
            options = tables["data"].options
            self.assertEqual(options["encoding"], "ascii")
            self.assertEqual(options["delimiter"], ",")
            self.assertEqual(options["quotechar"], '"')
            self.assertEqual(options["skip"], 0)
            self.assertFalse(options["has_header"])

    def test_get_data_iterator(self):
        with TemporaryDirectory() as data_directory:
            file_name = os.path.join(data_directory, "test_get_data_iterator.csv")
            self._write_basic_csv(file_name)
            reader = CSVReader(None)
            reader.connect_to_source(file_name)
            tables = reader.get_tables_and_properties()
            options = tables["data"].options
            _, header = reader.get_data_iterator("", options)
            self.assertTrue(not header)

    def test_get_data(self):
        with TemporaryDirectory() as data_directory:
            file_name = os.path.join(data_directory, "test_get_data.csv")
            self._write_basic_csv(file_name)
            reader = CSVReader(None)
            reader.connect_to_source(file_name)
            tables = reader.get_tables_and_properties()
            options = tables["data"].options
            data, header = reader.get_data("", options)
            self.assertTrue(not header)
            self.assertEqual(len(data), 2)
            self.assertEqual(data[0], ["1a", "1b", "1c"])
            self.assertEqual(data[1], ["2a", "2b", "2c"])

    def test_get_data_with_skip(self):
        with TemporaryDirectory() as data_directory:
            file_name = os.path.join(data_directory, "test_get_data.csv")
            self._write_basic_csv(file_name)
            reader = CSVReader(None)
            reader.connect_to_source(file_name)
            tables = reader.get_tables_and_properties()
            options = tables["data"].options
            options["skip"] = 1
            data, header = reader.get_data("", options)
            self.assertTrue(not header)
            self.assertEqual(data, [["2a", "2b", "2c"]])

    def test_get_data_with_has_header(self):
        with TemporaryDirectory() as data_directory:
            file_name = os.path.join(data_directory, "test.csv")
            self._write_csv_with_header(file_name)
            reader = CSVReader(None)
            reader.connect_to_source(file_name)
            tables = reader.get_tables_and_properties()
            options = tables["data"].options
            self.assertTrue(options["has_header"])
            data, header = reader.get_data("", options)
            self.assertEqual(header, ["header_1", "header_2", "header_3"])
            self.assertEqual(len(data), 2)
            self.assertEqual(data[0], ["11", "12", "13"])
            self.assertEqual(data[1], ["21", "22", "23"])

    def test_get_data_with_has_header_and_max_rows(self):
        with TemporaryDirectory() as data_directory:
            file_name = os.path.join(data_directory, "test.csv")
            self._write_csv_with_header(file_name)
            reader = CSVReader(None)
            reader.connect_to_source(file_name)
            tables = reader.get_tables_and_properties()
            options = tables["data"].options
            self.assertTrue(options["has_header"])
            data, header = reader.get_data("", options, max_rows=1)
            self.assertEqual(header, ["header_1", "header_2", "header_3"])
            self.assertEqual(len(data), 1)
            self.assertEqual(data[0], ["11", "12", "13"])

    def test_get_table_cell(self):
        with TemporaryDirectory() as data_directory:
            file_name = os.path.join(data_directory, "test_get_data.csv")
            self._write_basic_csv(file_name)
            reader = CSVReader(None)
            reader.connect_to_source(file_name)
            tables = reader.get_tables_and_properties()
            options = tables["data"].options
            expected = [
                ["1a", "1b", "1c"],
                ["2a", "2b", "2c"],
            ]
            for row in range(2):
                for column in range(3):
                    cell_data = reader.get_table_cell("", row, column, options)
                    with self.subTest(row=row, column=column):
                        self.assertEqual(cell_data, expected[row][column])

    def test_get_table_cell_raises_when_row_too_great(self):
        with TemporaryDirectory() as data_directory:
            file_name = os.path.join(data_directory, "data.csv")
            self._write_basic_csv(file_name)
            reader = CSVReader(None)
            reader.connect_to_source(file_name)
            tables = reader.get_tables_and_properties()
            options = tables["data"].options
            with self.assertRaises(ReaderError):
                reader.get_table_cell("", 100, 0, options)

    def test_get_table_cell_raises_when_column_too_great(self):
        with TemporaryDirectory() as data_directory:
            file_name = os.path.join(data_directory, "data.csv")
            self._write_basic_csv(file_name)
            reader = CSVReader(None)
            reader.connect_to_source(file_name)
            tables = reader.get_tables_and_properties()
            options = tables["data"].options
            with self.assertRaises(ReaderError):
                reader.get_table_cell("", 0, 100, options)

    def test_get_table_cell_accounts_for_skipped_rows(self):
        with TemporaryDirectory() as data_directory:
            file_name = os.path.join(data_directory, "test_get_data.csv")
            self._write_basic_csv(file_name)
            reader = CSVReader(None)
            reader.connect_to_source(file_name)
            tables = reader.get_tables_and_properties()
            options = tables["data"].options
            options["skip"] = 1
            cell_data = reader.get_table_cell("data", 0, 2, options)
            self.assertEqual(cell_data, "2c")

    def test_get_table_cell_accounts_for_header(self):
        with TemporaryDirectory() as data_directory:
            file_name = os.path.join(data_directory, "test_get_data.csv")
            self._write_csv_with_header(file_name)
            reader = CSVReader(None)
            reader.connect_to_source(file_name)
            tables = reader.get_tables_and_properties()
            options = tables["data"].options
            self.assertTrue(options["has_header"])
            cell_data = reader.get_table_cell("data", 0, 2, options)
            self.assertEqual(cell_data, "13")

    def test_reader_is_picklable(self):
        reader = CSVReader(None)
        pickled = pickle.dumps(reader)
        self.assertTrue(pickled)


if __name__ == "__main__":
    unittest.main()
