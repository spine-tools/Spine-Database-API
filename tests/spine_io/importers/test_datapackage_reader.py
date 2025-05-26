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
from contextlib import contextmanager
import csv
from pathlib import Path
import pickle
from tempfile import TemporaryDirectory
import unittest
from frictionless import Package, Resource
from spinedb_api.exception import ReaderError
from spinedb_api.spine_io.importers.datapackage_reader import DatapackageReader


class TestDatapackageReader(unittest.TestCase):
    def test_reader_is_picklable(self):
        reader = DatapackageReader(None)
        pickled = pickle.dumps(reader)
        self.assertTrue(pickled)

    def test_header_on(self):
        data = [["a", "b"], ["1.1", "2.2"]]
        with check_datapackage(data) as package_path:
            reader = DatapackageReader(None)
            reader.connect_to_source(str(package_path))
            data_iterator, header = reader.get_data_iterator("test_data", {"has_header": True})
            self.assertEqual(header, ["a", "b"])
            self.assertEqual(list(data_iterator), [["1.1", "2.2"]])

    def test_header_off(self):
        data = [["a", "b"], ["1.1", "2.2"]]
        with check_datapackage(data) as package_path:
            reader = DatapackageReader(None)
            reader.connect_to_source(str(package_path))
            data_iterator, header = reader.get_data_iterator("test_data", {"has_header": False})
            self.assertIsNone(header)
            self.assertEqual(list(data_iterator), data)

    def test_header_off_does_not_append_numbers_to_duplicate_cells(self):
        data = [["a", "a"]]
        with check_datapackage(data) as package_path:
            reader = DatapackageReader(None)
            reader.connect_to_source(str(package_path))
            data_iterator, header = reader.get_data_iterator("test_data", {"has_header": False})
            self.assertIsNone(header)
            self.assertEqual(list(data_iterator), data)

    def test_wrong_datapackage_encoding_raises_reader_error(self):
        broken_text = b"Slagn\xe4s"
        # Fool the datapackage sniffing algorithm by hiding the broken line behind a large number of UTF-8 lines.
        data = 1000 * [b"normal_text\n"] + [broken_text]
        with TemporaryDirectory() as temp_dir:
            csv_file_path = Path(temp_dir, "test_data.csv")
            with open(csv_file_path, "wb") as csv_file:
                for row in data:
                    csv_file.write(row)
            package = Package(basepath=temp_dir)
            package.add_resource(Resource(path=str(csv_file_path.relative_to(temp_dir))))
            package_path = Path(temp_dir, "datapackage.json")
            package.to_json(package_path)
            reader = DatapackageReader(None)
            reader.connect_to_source(str(package_path))
            data_iterator, header = reader.get_data_iterator("test_data", {"has_header": False})
            self.assertIsNone(header)
            self.assertRaises(ReaderError, list, data_iterator)

    def test_get_table_cell(self):
        data = [["11", "12", "13"], ["21", "22", "23"]]
        with check_datapackage(data) as package_path:
            reader = DatapackageReader(None)
            reader.connect_to_source(str(package_path))
            cell_data = reader.get_table_cell("test_data", 1, 2, {"has_header": False})
            self.assertEqual(cell_data, 23)

    def test_get_table_cell_with_header(self):
        data = [["header 1", "header 2", "header 3"], ["11", "12", "13"], ["21", "22", "23"]]
        with check_datapackage(data) as package_path:
            reader = DatapackageReader(None)
            reader.connect_to_source(str(package_path))
            cell_data = reader.get_table_cell("test_data", 1, 2, {"has_header": True})
            self.assertEqual(cell_data, 23)

    def test_get_table_cell_with_row_out_of_bound(self):
        data = [["11", "12", "13"], ["21", "22", "23"]]
        with check_datapackage(data) as package_path:
            reader = DatapackageReader(None)
            reader.connect_to_source(str(package_path))
            with self.assertRaisesRegex(ReaderError, "test_data doesn't have row 3"):
                reader.get_table_cell("test_data", 3, 0, {"has_header": False})

    def test_get_table_cell_with_column_out_of_bound(self):
        data = [["11", "12", "13"], ["21", "22", "23"]]
        with check_datapackage(data) as package_path:
            reader = DatapackageReader(None)
            reader.connect_to_source(str(package_path))
            with self.assertRaisesRegex(ReaderError, "test_data doesn't have column 4"):
                reader.get_table_cell("test_data", 0, 4, {"has_header": False})

    def test_get_table_cell_raises_when_table_doesnt_exist(self):
        data = [["11", "12", "13"], ["21", "22", "23"]]
        with check_datapackage(data) as package_path:
            reader = DatapackageReader(None)
            reader.connect_to_source(str(package_path))
            with self.assertRaisesRegex(ReaderError, "no such table 'non-table'"):
                reader.get_table_cell("non-table", 0, 0, {"has_header": False})

    def test_get_large_data_does_not_raise_operation_on_closed_file(self):
        header = ["header 1 ", "header 2", "header 3"]
        data = [header] + 1000 * [["21", "22", "23"]]
        with check_datapackage(data) as package_path:
            reader = DatapackageReader(None)
            reader.connect_to_source(str(package_path))
            data, header = reader.get_data("test_data", {})
            self.assertEqual(header, ["header 1", "header 2", "header 3"])
            self.assertEqual(data, 1000 * [[21, 22, 23]])


@contextmanager
def check_datapackage(rows):
    with TemporaryDirectory() as temp_dir:
        csv_file_path = Path(temp_dir, "test_data.csv")
        with open(csv_file_path, "w", newline="") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerows(rows)
        package = Package(basepath=temp_dir)
        package.add_resource(Resource(path=str(csv_file_path.relative_to(temp_dir))))
        package_path = Path(temp_dir, "datapackage.json")
        package.to_json(package_path)
        yield package_path


if __name__ == "__main__":
    unittest.main()
