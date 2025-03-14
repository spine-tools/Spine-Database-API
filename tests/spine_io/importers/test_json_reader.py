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

""" Contains unit tests for JSONReader. """
import json
from pathlib import Path
import pickle
from tempfile import TemporaryDirectory
import unittest
from spinedb_api.exception import ReaderError
from spinedb_api.spine_io.importers.json_reader import JSONReader


class TestJSONReader(unittest.TestCase):
    def test_reader_is_picklable(self):
        reader = JSONReader(None)
        pickled = pickle.dumps(reader)
        self.assertTrue(pickled)

    def test_file_iterator_works_with_empty_options(self):
        reader = JSONReader(None)
        data = {"a": 1, "b": {"c": 2}}
        with TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "test file.json"
            with open(file_path, "w") as out_file:
                json.dump(data, out_file)
            reader.connect_to_source(str(file_path))
            rows = list(reader.file_iterator("data", {}))
            reader.disconnect()
        self.assertEqual(rows, [["a", 1], ["b", "c", 2]])

    def test_invalid_json_raises_reader_error(self):
        reader = JSONReader(None)
        with TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "test file.json"
            with open(file_path, "w") as out_file:
                out_file.write("PK")
            reader.connect_to_source(str(file_path))
            with self.assertRaises(ReaderError):
                rows = list(reader.file_iterator("data", {}))
            reader.disconnect()

    def test_get_table_cell(self):
        reader = JSONReader(None)
        data = {"a": 1, "b": {"c": 2}}
        with TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "test file.json"
            with open(file_path, "w") as out_file:
                json.dump(data, out_file)
            reader.connect_to_source(str(file_path))
            cell_data = reader.get_table_cell("data", 1, 2, {})
            reader.disconnect()
        self.assertEqual(cell_data, 2)

    def test_get_table_cell_raises_when_row_is_out_of_bounds(self):
        reader = JSONReader(None)
        data = {"a": 1, "b": {"c": 2}}
        with TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "test file.json"
            with open(file_path, "w") as out_file:
                json.dump(data, out_file)
            reader.connect_to_source(str(file_path))
            with self.assertRaises(ReaderError):
                reader.get_table_cell("data", 2, 0, {})
            reader.disconnect()

    def test_get_table_cell_raises_when_column_is_out_of_bounds(self):
        reader = JSONReader(None)
        data = {"a": 1, "b": {"c": 2}}
        with TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "test file.json"
            with open(file_path, "w") as out_file:
                json.dump(data, out_file)
            reader.connect_to_source(str(file_path))
            with self.assertRaises(ReaderError):
                reader.get_table_cell("data", 0, 3, {})
            reader.disconnect()


if __name__ == "__main__":
    unittest.main()
