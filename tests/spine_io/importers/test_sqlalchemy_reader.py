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

import pathlib
import pickle
from tempfile import TemporaryDirectory
import unittest
from sqlalchemy import create_engine, text
from spinedb_api.exception import ReaderError
from spinedb_api.spine_io.importers.sqlalchemy_reader import SQLAlchemyReader


class TestSQLAlchemyReader(unittest.TestCase):
    def test_reader_is_picklable(self):
        reader = SQLAlchemyReader(None)
        pickled = pickle.dumps(reader)
        self.assertTrue(pickled)

    def test_get_data(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + str(pathlib.Path(temp_dir) / "test_db.sqlite")
            engine = create_engine(url, future=True)
            self._make_xyz_int_table(engine, "data_table", [[11, 12, 13], [21, 22, 23]])
            reader = SQLAlchemyReader(None)
            reader.connect_to_source(url)
            data, header = reader.get_data("data_table", {})
            reader.disconnect()
        self.assertEqual(header, ["x", "y", "z"])
        self.assertEqual(data, [(11, 12, 13), (21, 22, 23)])

    def test_get_data_with_max_rows(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + str(pathlib.Path(temp_dir) / "test_db.sqlite")
            engine = create_engine(url, future=True)
            self._make_xyz_int_table(engine, "data_table", [[11, 12, 13], [21, 22, 23]])
            reader = SQLAlchemyReader(None)
            reader.connect_to_source(url)
            data, header = reader.get_data("data_table", {}, max_rows=1)
            reader.disconnect()
        self.assertEqual(header, ["x", "y", "z"])
        self.assertEqual(data, [(11, 12, 13)])

    def test_get_table_cell(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + str(pathlib.Path(temp_dir) / "test_db.sqlite")
            engine = create_engine(url, future=True)
            self._make_xyz_int_table(engine, "data_table", [[11, 12, 13], [21, 22, 23]])
            reader = SQLAlchemyReader(None)
            reader.connect_to_source(url)
            cell_data = reader.get_table_cell("data_table", 1, 2, {})
            reader.disconnect()
        self.assertEqual(cell_data, 23)

    def test_get_table_cell_raises_when_row_is_out_of_bounds(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + str(pathlib.Path(temp_dir) / "test_db.sqlite")
            engine = create_engine(url, future=True)
            self._make_xyz_int_table(engine, "data_table", [[11, 12, 13], [21, 22, 23]])
            reader = SQLAlchemyReader(None)
            reader.connect_to_source(url)
            with self.assertRaisesRegex(ReaderError, "data_table doesn't have row 2"):
                reader.get_table_cell("data_table", 2, 0, {})
            reader.disconnect()

    def test_get_table_cell_raises_when_column_is_out_of_bounds(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + str(pathlib.Path(temp_dir) / "test_db.sqlite")
            engine = create_engine(url, future=True)
            self._make_xyz_int_table(engine, "data_table", [[11, 12, 13], [21, 22, 23]])
            reader = SQLAlchemyReader(None)
            reader.connect_to_source(url)
            with self.assertRaisesRegex(ReaderError, "data_table doesn't have column 3"):
                reader.get_table_cell("data_table", 0, 3, {})
            reader.disconnect()

    def test_get_table_cell_raises_when_table_doesnt_exist(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + str(pathlib.Path(temp_dir) / "test_db.sqlite")
            engine = create_engine(url, future=True)
            self._make_xyz_int_table(engine, "data_table", [[11, 12, 13], [21, 22, 23]])
            reader = SQLAlchemyReader(None)
            reader.connect_to_source(url)
            with self.assertRaisesRegex(ReaderError, "no such table: 'non-table'"):
                reader.get_table_cell("non-table", 0, 0, {})
            reader.disconnect()

    @staticmethod
    def _make_xyz_int_table(engine, table_name, rows):
        with engine.begin() as connection:
            connection.execute(text(f"CREATE TABLE {table_name} (x int, y int, z int)"))
            insert_statement = text("INSERT INTO data_table (x, y, z) VALUES (:x, :y, :z)")
            for x, y, z in rows:
                connection.execute(insert_statement, {"x": x, "y": y, "z": z})


if __name__ == "__main__":
    unittest.main()
