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
""" Unit tests for csv writer. """
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from spinedb_api import DatabaseMapping, import_object_classes, import_objects
from spinedb_api.export_mapping import entity_export
from spinedb_api.mapping import Position
from spinedb_api.spine_io.exporters.csv_writer import CsvWriter
from spinedb_api.spine_io.exporters.writer import write
from tests.mock_helpers import AssertSuccessTestCase


class TestCsvWriter(AssertSuccessTestCase):
    def test_write_empty_database(self):
        with TemporaryDirectory() as temp_dir:
            with DatabaseMapping("sqlite://", create=True) as db_map:
                root_mapping = entity_export(0, 1)
                out_path = Path(temp_dir, "out.csv")
                writer = CsvWriter(out_path.parent, out_path.name)
                write(db_map, writer, root_mapping)
                self.assertTrue(out_path.exists())
                with open(out_path) as out_file:
                    self.assertEqual(out_file.readlines(), [])

    def test_write_single_object_class_and_object(self):
        with TemporaryDirectory() as temp_dir:
            with DatabaseMapping("sqlite://", create=True) as db_map:
                self._assert_imports(import_object_classes(db_map, ("oc",)))
                self._assert_imports(import_objects(db_map, (("oc", "o1"),)))
                db_map.commit_session("Add test data.")
                root_mapping = entity_export(0, 1)
                out_path = Path(temp_dir, "out.csv")
                writer = CsvWriter(out_path.parent, out_path.name)
                write(db_map, writer, root_mapping)
                self.assertTrue(out_path.exists())
                with open(out_path) as out_file:
                    self.assertEqual(out_file.readlines(), ["oc,o1\n"])

    def test_tables_are_written_to_separate_files(self):
        with TemporaryDirectory() as temp_dir:
            with DatabaseMapping("sqlite://", create=True) as db_map:
                self._assert_imports(import_object_classes(db_map, ("oc1", "oc2")))
                self._assert_imports(import_objects(db_map, (("oc1", "o1"), ("oc2", "o2"))))
                db_map.commit_session("Add test data.")
                root_mapping = entity_export(Position.table_name, 0)
                out_path = Path(temp_dir, "out.csv")
                writer = CsvWriter(out_path.parent, out_path.name)
                write(db_map, writer, root_mapping)
                self.assertFalse(out_path.exists())
                out_files = []
                for real_out_path in Path(temp_dir).iterdir():
                    out_files.append(real_out_path.name)
                    expected = None
                    if real_out_path.name == "oc1.csv":
                        expected = ["o1\n"]
                    elif real_out_path.name == "oc2.csv":
                        expected = ["o2\n"]
                    with open(real_out_path) as out_file:
                        self.assertEqual(out_file.readlines(), expected)
                self.assertEqual(len(out_files), 2)
                self.assertEqual(set(out_files), {"oc1.csv", "oc2.csv"})

    def test_append_to_table(self):
        with TemporaryDirectory() as temp_dir:
            with DatabaseMapping("sqlite://", create=True) as db_map:
                self._assert_imports(import_object_classes(db_map, ("oc",)))
                self._assert_imports(import_objects(db_map, (("oc", "o1"),)))
                db_map.commit_session("Add test data.")
                root_mapping1 = entity_export(0, 1)
                root_mapping2 = entity_export(0, 1)
                out_path = Path(temp_dir, "out.csv")
                writer = CsvWriter(out_path.parent, out_path.name)
                write(db_map, writer, root_mapping1, root_mapping2)
                self.assertTrue(out_path.exists())
                with open(out_path) as out_file:
                    self.assertEqual(out_file.readlines(), ["oc,o1\n", "oc,o1\n"])


if __name__ == "__main__":
    unittest.main()
