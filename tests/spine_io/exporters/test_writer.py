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
Unit tests for ``writer`` module.

"""
import unittest
from spinedb_api import DatabaseMapping, import_object_classes, import_objects
from spinedb_api.spine_io.exporters.writer import Writer, write
from spinedb_api.export_mapping.settings import entity_export


class _TableWriter(Writer):
    def __init__(self):
        self._tables = dict()
        self._current_table = None

    def finish_table(self):
        self._current_table = None

    def start_table(self, table_name, title_key):
        self._current_table = self._tables.setdefault(table_name, list())
        return True

    @property
    def tables(self):
        return self._tables

    def write_row(self, row):
        self._current_table.append(row)
        return True


class TestWrite(unittest.TestCase):
    def setUp(self):
        self._db_map = DatabaseMapping("sqlite://", create=True)

    def tearDown(self):
        self._db_map.close()

    def test_max_rows(self):
        import_object_classes(self._db_map, ("class1", "class2"))
        import_objects(
            self._db_map,
            (
                ("class1", "obj1"),
                ("class1", "obj2"),
                ("class1", "obj3"),
                ("class2", "obj4"),
                ("class2", "obj5"),
                ("class2", "obj6"),
            ),
        )
        self._db_map.commit_session("Add test data.")
        writer = _TableWriter()
        root_mapping = entity_export(0, 1)
        write(self._db_map, writer, root_mapping, max_rows=2)
        self.assertEqual(writer.tables, {None: [["class1", "obj1"], ["class1", "obj2"]]})

    def test_max_rows_with_filter(self):
        import_object_classes(self._db_map, ("class1", "class2"))
        import_objects(
            self._db_map,
            (
                ("class1", "obj1"),
                ("class1", "obj2"),
                ("class1", "obj3"),
                ("class2", "obj4"),
                ("class2", "obj5"),
                ("class2", "obj6"),
            ),
        )
        self._db_map.commit_session("Add test data.")
        writer = _TableWriter()
        root_mapping = entity_export(0, 1)
        root_mapping.child.filter_re = "obj6"
        write(self._db_map, writer, root_mapping, max_rows=1)
        self.assertEqual(writer.tables, {None: [["class2", "obj6"]]})


if __name__ == "__main__":
    unittest.main()
