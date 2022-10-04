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
Contains unit tests for DatapackageConnector.

:author: A. Soininen (VTT)
:date:   30.12.2021
"""
import csv
import unittest
from pathlib import Path
import pickle
from tempfile import TemporaryDirectory
from datapackage import Package
from spinedb_api.spine_io.importers.datapackage_reader import DataPackageConnector


class TestDatapackageConnector(unittest.TestCase):
    def test_connector_is_picklable(self):
        reader = DataPackageConnector(None)
        pickled = pickle.dumps(reader)
        self.assertTrue(pickled)

    def test_header_on(self):
        with TemporaryDirectory() as temp_dir:
            data = [["a", "b"], ["1.1", "2.2"]]
            csv_file_path = Path(temp_dir, "test_data.csv")
            with open(csv_file_path, "w", newline="") as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerows(data)
            package = Package(base_path=temp_dir)
            package.add_resource({"path": str(csv_file_path.relative_to(temp_dir))})
            package_path = Path(temp_dir, "datapackage.json")
            package.save(package_path)
            reader = DataPackageConnector(None)
            reader.connect_to_source(str(package_path))
            data_iterator, header = reader.get_data_iterator("test_data", {"has_header": True})
            self.assertEqual(header, ["a", "b"])
            self.assertEqual(list(data_iterator), [["1.1", "2.2"]])

    def test_header_off(self):
        with TemporaryDirectory() as temp_dir:
            data = [["a", "b"], ["1.1", "2.2"]]
            csv_file_path = Path(temp_dir, "test_data.csv")
            with open(csv_file_path, "w", newline="") as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerows(data)
            package = Package(base_path=temp_dir)
            package.add_resource({"path": str(csv_file_path.relative_to(temp_dir))})
            package_path = Path(temp_dir, "datapackage.json")
            package.save(package_path)
            reader = DataPackageConnector(None)
            reader.connect_to_source(str(package_path))
            data_iterator, header = reader.get_data_iterator("test_data", {"has_header": False})
            self.assertIsNone(header)
            self.assertEqual(list(data_iterator), data)


if __name__ == '__main__':
    unittest.main()
