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
Unit tests for SQL writer.

:author: A. Soininen (VTT)
:date:   7.4.2021
"""
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import Session
from spinedb_api import (
    DateTime,
    DiffDatabaseMapping,
    Duration,
    import_object_classes,
    import_objects,
    import_object_parameters,
    import_object_parameter_values,
)
from spinedb_api.mapping import Position, unflatten
from spinedb_api.export_mapping import object_export
from spinedb_api.export_mapping.export_mapping import (
    AlternativeMapping,
    FixedValueMapping,
    ObjectClassMapping,
    ObjectMapping,
    ParameterDefinitionMapping,
    ParameterValueMapping,
)
from spinedb_api.spine_io.exporters.writer import write
from spinedb_api.spine_io.exporters.sql_writer import SqlWriter


class TestSqlWriter(unittest.TestCase):
    def setUp(self):
        self._temp_dir = TemporaryDirectory()

    def tearDown(self):
        self._temp_dir.cleanup()

    def test_write_empty_database(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        settings = FixedValueMapping(Position.table_name, "table 1")
        out_path = Path(self._temp_dir.name, "out.sqlite")
        writer = SqlWriter(str(out_path))
        write(db_map, writer, settings)
        db_map.connection.close()
        self.assertTrue(out_path.exists())

    def test_write_header_only(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        db_map.commit_session("Add test data.")
        root_mapping = unflatten(
            [
                FixedValueMapping(Position.table_name, "table 1"),
                ObjectClassMapping(0, header="classes"),
                ObjectMapping(1, header="objects"),
            ]
        )
        out_path = Path(self._temp_dir.name, "out.sqlite")
        writer = SqlWriter(str(out_path))
        write(db_map, writer, root_mapping)
        db_map.connection.close()
        self.assertTrue(out_path.exists())
        engine = create_engine("sqlite:///" + str(out_path))
        connection = engine.connect()
        try:
            metadata = MetaData()
            metadata.reflect(bind=engine)
            session = Session(engine)
            self.assertIn("table 1", metadata.tables)
            table = metadata.tables["table 1"]
            column_names = [str(c) for c in table.c]
            self.assertEqual(column_names, ["table 1.classes", "table 1.objects"])
            self.assertEqual(len(session.query(table).all()), 0)
            session.close()
        finally:
            connection.close()

    def test_write_single_object_class_and_object(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_objects(db_map, (("oc", "o1"),))
        db_map.commit_session("Add test data.")
        root_mapping = unflatten(
            [
                FixedValueMapping(Position.table_name, "table 1"),
                ObjectClassMapping(0, header="classes"),
                ObjectMapping(1, header="objects"),
            ]
        )
        out_path = Path(self._temp_dir.name, "out.sqlite")
        writer = SqlWriter(str(out_path))
        write(db_map, writer, root_mapping)
        db_map.connection.close()
        self.assertTrue(out_path.exists())
        engine = create_engine("sqlite:///" + str(out_path))
        connection = engine.connect()
        try:
            metadata = MetaData()
            metadata.reflect(bind=engine)
            session = Session(engine)
            self.assertIn("table 1", metadata.tables)
            table = metadata.tables["table 1"]
            column_names = [str(c) for c in table.c]
            self.assertEqual(column_names, ["table 1.classes", "table 1.objects"])
            for class_ in session.query(table):
                self.assertEqual(class_, ("oc", "o1"))
            session.close()
        finally:
            connection.close()

    def test_write_datetime_value(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p"),))
        import_objects(db_map, (("oc", "o1"),))
        dt = DateTime("2021-04-08T08:00")
        import_object_parameter_values(db_map, (("oc", "o1", "p", DateTime("2021-04-08T08:00")),))
        db_map.commit_session("Add test data.")
        root_mapping = unflatten(
            [
                FixedValueMapping(Position.table_name, "table 1"),
                ObjectClassMapping(0, header="classes"),
                ObjectMapping(1, header="objects"),
                ParameterDefinitionMapping(2, header="parameters"),
                AlternativeMapping(Position.hidden),
                ParameterValueMapping(3, header="values"),
            ]
        )
        out_path = Path(self._temp_dir.name, "out.sqlite")
        writer = SqlWriter(str(out_path))
        write(db_map, writer, root_mapping)
        db_map.connection.close()
        self.assertTrue(out_path.exists())
        engine = create_engine("sqlite:///" + str(out_path))
        connection = engine.connect()
        try:
            metadata = MetaData()
            metadata.reflect(bind=engine)
            session = Session(engine)
            self.assertIn("table 1", metadata.tables)
            table = metadata.tables["table 1"]
            column_names = [str(c) for c in table.c]
            self.assertEqual(
                column_names, ["table 1.classes", "table 1.objects", "table 1.parameters", "table 1.values"]
            )
            for class_ in session.query(table):
                self.assertEqual(class_, ("oc", "o1", "p", str(dt)))
            session.close()
        finally:
            connection.close()

    def test_write_duration_value(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_object_parameters(db_map, (("oc", "p"),))
        import_objects(db_map, (("oc", "o1"),))
        import_object_parameter_values(db_map, (("oc", "o1", "p", Duration("3h")),))
        db_map.commit_session("Add test data.")
        root_mapping = unflatten(
            [
                FixedValueMapping(Position.table_name, "table 1"),
                ObjectClassMapping(0, header="classes"),
                ObjectMapping(1, header="objects"),
                ParameterDefinitionMapping(2, header="parameters"),
                AlternativeMapping(Position.hidden),
                ParameterValueMapping(3, header="values"),
            ]
        )
        out_path = Path(self._temp_dir.name, "out.sqlite")
        writer = SqlWriter(str(out_path))
        write(db_map, writer, root_mapping)
        db_map.connection.close()
        self.assertTrue(out_path.exists())
        engine = create_engine("sqlite:///" + str(out_path))
        connection = engine.connect()
        try:
            metadata = MetaData()
            metadata.reflect(bind=engine)
            session = Session(engine)
            self.assertIn("table 1", metadata.tables)
            table = metadata.tables["table 1"]
            column_names = [str(c) for c in table.c]
            self.assertEqual(
                column_names, ["table 1.classes", "table 1.objects", "table 1.parameters", "table 1.values"]
            )
            for class_ in session.query(table):
                self.assertEqual(class_, ("oc", "o1", "p", "3h"))
            session.close()
        finally:
            connection.close()

    def test_append_to_table(self):
        db_map = DiffDatabaseMapping("sqlite://", create=True)
        import_object_classes(db_map, ("oc",))
        import_objects(db_map, (("oc", "o1"), ("oc", "q1")))
        db_map.commit_session("Add test data.")
        root_mapping1 = object_export(Position.table_name, 0)
        root_mapping1.child.header = "objects"
        root_mapping1.child.filter_re = "o1"
        root_mapping2 = object_export(Position.table_name, 0)
        root_mapping2.child.header = "objects"
        root_mapping2.child.filter_re = "q1"
        out_path = Path(self._temp_dir.name, "out.sqlite")
        writer = SqlWriter(str(out_path))
        write(db_map, writer, root_mapping1)
        write(db_map, writer, root_mapping2)
        db_map.connection.close()
        self.assertTrue(out_path.exists())
        engine = create_engine("sqlite:///" + str(out_path))
        connection = engine.connect()
        try:
            metadata = MetaData()
            metadata.reflect(bind=engine)
            session = Session(engine)
            self.assertIn("oc", metadata.tables)
            table = metadata.tables["oc"]
            column_names = [str(c) for c in table.c]
            self.assertEqual(column_names, ["oc.objects"])
            expected_rows = (("o1",), ("q1",))
            rows = session.query(table).all()
            self.assertEqual(len(rows), len(expected_rows))
            for row, expected in zip(rows, expected_rows):
                self.assertEqual(row, expected)
            session.close()
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
