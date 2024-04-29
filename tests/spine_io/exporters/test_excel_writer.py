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
Unit tests for Excel writer.

"""
import os.path
from tempfile import TemporaryDirectory
import unittest
from openpyxl import load_workbook
from spinedb_api import DatabaseMapping, import_object_classes, import_objects
from spinedb_api.mapping import Position
from spinedb_api.export_mapping import entity_export
from spinedb_api.spine_io.exporters.writer import write
from spinedb_api.spine_io.exporters.excel_writer import ExcelWriter


class TestExcelWriter(unittest.TestCase):
    def setUp(self):
        self._temp_dir = TemporaryDirectory()

    def tearDown(self):
        self._temp_dir.cleanup()

    def test_write_empty_database(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        root_mapping = entity_export(0, 1)
        path = os.path.join(self._temp_dir.name, "test.xlsx")
        writer = ExcelWriter(path)
        write(db_map, writer, root_mapping)
        workbook = load_workbook(path, read_only=True)
        self.assertEqual(workbook.sheetnames, ["Sheet1"])
        sheet = workbook["Sheet1"]
        self.assertEqual(sheet.calculate_dimension(), "A1:A1")
        workbook.close()
        db_map.close()

    def test_write_single_object_class_and_object(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_objects(db_map, (("oc", "o1"),))
        db_map.commit_session("Add test data.")
        root_mapping = entity_export(0, 1)
        path = os.path.join(self._temp_dir.name, "test.xlsx")
        writer = ExcelWriter(path)
        write(db_map, writer, root_mapping)
        workbook = load_workbook(path, read_only=True)
        self.assertEqual(workbook.sheetnames, ["Sheet1"])
        expected = [["oc", "o1"]]
        self.check_sheet(workbook, "Sheet1", expected)
        workbook.close()
        db_map.close()

    def test_write_to_existing_sheet(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("Sheet1",))
        import_objects(db_map, (("Sheet1", "o1"), ("Sheet1", "o2")))
        db_map.commit_session("Add test data.")
        root_mapping = entity_export(Position.table_name, 0)
        path = os.path.join(self._temp_dir.name, "test.xlsx")
        writer = ExcelWriter(path)
        write(db_map, writer, root_mapping)
        workbook = load_workbook(path, read_only=True)
        self.assertEqual(workbook.sheetnames, ["Sheet1"])
        expected = [["o1"], ["o2"]]
        self.check_sheet(workbook, "Sheet1", expected)
        workbook.close()
        db_map.close()

    def test_write_to_named_sheets(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc1", ("oc2")))
        import_objects(db_map, (("oc1", "o11"), ("oc1", "o12"), ("oc2", "o21")))
        db_map.commit_session("Add test data.")
        root_mapping = entity_export(Position.table_name, 1)
        path = os.path.join(self._temp_dir.name, "test.xlsx")
        writer = ExcelWriter(path)
        write(db_map, writer, root_mapping)
        workbook = load_workbook(path, read_only=True)
        self.assertEqual(workbook.sheetnames, ["oc1", "oc2"])
        expected = [[None, "o11"], [None, "o12"]]
        self.check_sheet(workbook, "oc1", expected)
        expected = [[None, "o21"]]
        self.check_sheet(workbook, "oc2", expected)
        workbook.close()
        db_map.close()

    def test_append_to_anonymous_table(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_objects(db_map, (("oc", "o1"),))
        db_map.commit_session("Add test data.")
        root_mapping1 = entity_export(0, 1)
        root_mapping2 = entity_export(0, 1)
        path = os.path.join(self._temp_dir.name, "test.xlsx")
        writer = ExcelWriter(path)
        write(db_map, writer, root_mapping1, root_mapping2)
        workbook = load_workbook(path, read_only=True)
        self.assertEqual(workbook.sheetnames, ["Sheet1"])
        expected = [["oc", "o1"], ["oc", "o1"]]
        self.check_sheet(workbook, "Sheet1", expected)
        workbook.close()
        db_map.close()

    def test_append_to_named_table(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_objects(db_map, (("oc", "o1"),))
        db_map.commit_session("Add test data.")
        root_mapping1 = entity_export(Position.table_name, 0)
        root_mapping2 = entity_export(Position.table_name, 0)
        path = os.path.join(self._temp_dir.name, "test.xlsx")
        writer = ExcelWriter(path)
        write(db_map, writer, root_mapping1, root_mapping2)
        workbook = load_workbook(path, read_only=True)
        self.assertEqual(workbook.sheetnames, ["oc"])
        expected = [["o1"], ["o1"]]
        self.check_sheet(workbook, "oc", expected)
        workbook.close()
        db_map.close()

    def check_sheet(self, workbook, sheet_name, expected):
        """
        Args:
            workbook (Workbook): a workbook to check
            sheet_name (str): sheet name
            expected (list): expected rows
        """
        sheet = workbook[sheet_name]
        for row, expected_row in zip(sheet.iter_rows(), expected):
            values = [cell.value for cell in row]
            self.assertEqual(values, expected_row)


if __name__ == "__main__":
    unittest.main()
