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
Unit tests for gdx writer.

:author: A. Soininen (VTT)
:date:   10.12.2020
"""
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from gdx2py import GdxFile
from spinedb_api.spine_io.gdx_utils import find_gams_directory
from spinedb_api.spine_io.exporters.gdx_writer import GdxWriter
from spinedb_api.spine_io.exporters.writer import write
from spinedb_api import DiffDatabaseMapping, import_object_classes, import_objects
from spinedb_api.export_mapping import object_export, Position


class TestGdxWriter(unittest.TestCase):
    _gams_dir = find_gams_directory()

    @unittest.skipIf(_gams_dir is None, "No working GAMS installation found.")
    def test_write_empty_database(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        settings = object_export(class_position=Position.table_name, object_position=0)
        with TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir, "test_write_empty_database.gdx")
            writer = GdxWriter(str(file_path), self._gams_dir)
            write(db_map, writer, settings)
            with GdxFile(str(file_path), "r", self._gams_dir) as gdx_file:
                self.assertEqual(len(gdx_file), 0)

    @unittest.skipIf(_gams_dir is None, "No working GAMS installation found.")
    def test_write_single_object_class_and_object(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_objects(db_map, (("oc", "o1"),))
        db_map.commit_session("Add test data.")
        settings = object_export(Position.table_name, 0)
        with TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir, "test_write_empty_database.gdx")
            writer = GdxWriter(str(file_path), self._gams_dir)
            write(db_map, writer, settings)
            with GdxFile(str(file_path), "r", self._gams_dir) as gdx_file:
                self.assertEqual(len(gdx_file), 1)
                gams_set = gdx_file["oc"]
                self.assertEqual(gams_set.elements, ["o1"])


if __name__ == '__main__':
    unittest.main()
