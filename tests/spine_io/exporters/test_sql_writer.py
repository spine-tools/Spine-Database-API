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
""" Unit tests for SQL writer. """
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from sqlalchemy import Column, MetaData, String, Table, create_engine
from sqlalchemy.orm import Session
from spinedb_api import (
    DatabaseMapping,
    DateTime,
    Duration,
    import_object_classes,
    import_object_parameter_values,
    import_object_parameters,
    import_objects,
)
from spinedb_api.export_mapping import entity_export
from spinedb_api.export_mapping.export_mapping import (
    AlternativeMapping,
    EntityClassMapping,
    EntityMapping,
    FixedValueMapping,
    ParameterDefinitionMapping,
    ParameterValueMapping,
)
from spinedb_api.mapping import Position, unflatten
from spinedb_api.spine_io.exporters.sql_writer import SqlWriter
from spinedb_api.spine_io.exporters.writer import write
from tests.mock_helpers import AssertSuccessTestCase


class TestSqlWriter(AssertSuccessTestCase):
    def test_write_empty_database(self):
        with TemporaryDirectory() as temp_dir:
            with DatabaseMapping("sqlite://", create=True) as db_map:
                settings = FixedValueMapping(Position.table_name, "table 1")
                out_path = Path(temp_dir, "out.sqlite")
                writer = SqlWriter(str(out_path), overwrite_existing=True)
                write(db_map, writer, settings)
                self.assertTrue(out_path.exists())

    def test_write_header_only(self):
        with TemporaryDirectory() as temp_dir:
            with DatabaseMapping("sqlite://", create=True) as db_map:
                self._assert_imports(import_object_classes(db_map, ("oc",)))
                db_map.commit_session("Add test data.")
                root_mapping = unflatten(
                    [
                        FixedValueMapping(Position.table_name, "table 1"),
                        EntityClassMapping(0, header="classes"),
                        EntityMapping(1, header="objects"),
                    ]
                )
                out_path = Path(temp_dir, "out.sqlite")
                writer = SqlWriter(str(out_path), overwrite_existing=True)
                write(db_map, writer, root_mapping)
                self.assertTrue(out_path.exists())
                engine = create_engine("sqlite:///" + str(out_path), future=True)
                with engine.begin():
                    metadata = MetaData()
                    metadata.reflect(bind=engine)
                    session = Session(engine, future=True)
                    self.assertIn("table 1", metadata.tables)
                    table = metadata.tables["table 1"]
                    column_names = [str(c) for c in table.c]
                    self.assertEqual(column_names, ["table 1.classes", "table 1.objects"])
                    self.assertEqual(len(session.query(table).all()), 0)
                    session.close()

    def test_write_single_object_class_and_object(self):
        with TemporaryDirectory() as temp_dir:
            with DatabaseMapping("sqlite://", create=True) as db_map:
                self._assert_imports(import_object_classes(db_map, ("oc",)))
                self._assert_imports(import_objects(db_map, (("oc", "o1"),)))
                db_map.commit_session("Add test data.")
                root_mapping = unflatten(
                    [
                        FixedValueMapping(Position.table_name, "table 1"),
                        EntityClassMapping(0, header="classes"),
                        EntityMapping(1, header="objects"),
                    ]
                )
                out_path = Path(temp_dir, "out.sqlite")
                writer = SqlWriter(str(out_path), overwrite_existing=True)
                write(db_map, writer, root_mapping)
                self.assertTrue(out_path.exists())
                engine = create_engine("sqlite:///" + str(out_path), future=True)
                with engine.begin():
                    metadata = MetaData()
                    metadata.reflect(bind=engine)
                    session = Session(engine, future=True)
                    self.assertIn("table 1", metadata.tables)
                    table = metadata.tables["table 1"]
                    column_names = [str(c) for c in table.c]
                    self.assertEqual(column_names, ["table 1.classes", "table 1.objects"])
                    for class_ in session.query(table):
                        self.assertEqual(class_, ("oc", "o1"))
                    session.close()

    def test_write_datetime_value(self):
        with TemporaryDirectory() as temp_dir:
            with DatabaseMapping("sqlite://", create=True) as db_map:
                self._assert_imports(import_object_classes(db_map, ("oc",)))
                self._assert_imports(import_object_parameters(db_map, (("oc", "p"),)))
                self._assert_imports(import_objects(db_map, (("oc", "o1"),)))
                dt = DateTime("2021-04-08T08:00")
                self._assert_imports(
                    import_object_parameter_values(db_map, (("oc", "o1", "p", DateTime("2021-04-08T08:00")),))
                )
                db_map.commit_session("Add test data.")
                root_mapping = unflatten(
                    [
                        FixedValueMapping(Position.table_name, "table 1"),
                        EntityClassMapping(0, header="classes"),
                        EntityMapping(1, header="objects"),
                        ParameterDefinitionMapping(2, header="parameters"),
                        AlternativeMapping(Position.hidden),
                        ParameterValueMapping(3, header="values"),
                    ]
                )
                out_path = Path(temp_dir, "out.sqlite")
                writer = SqlWriter(str(out_path), overwrite_existing=True)
                write(db_map, writer, root_mapping)
                self.assertTrue(out_path.exists())
                engine = create_engine("sqlite:///" + str(out_path), future=True)
                with engine.begin():
                    metadata = MetaData()
                    metadata.reflect(bind=engine)
                    session = Session(engine, future=True)
                    self.assertIn("table 1", metadata.tables)
                    table = metadata.tables["table 1"]
                    column_names = [str(c) for c in table.c]
                    self.assertEqual(
                        column_names, ["table 1.classes", "table 1.objects", "table 1.parameters", "table 1.values"]
                    )
                    for class_ in session.query(table):
                        self.assertEqual(class_, ("oc", "o1", "p", dt.value))
                    session.close()

    def test_write_duration_value(self):
        with TemporaryDirectory() as temp_dir:
            with DatabaseMapping("sqlite://", create=True) as db_map:
                self._assert_imports(import_object_classes(db_map, ("oc",)))
                self._assert_imports(import_object_parameters(db_map, (("oc", "p"),)))
                self._assert_imports(import_objects(db_map, (("oc", "o1"),)))
                self._assert_imports(import_object_parameter_values(db_map, (("oc", "o1", "p", Duration("3h")),)))
                db_map.commit_session("Add test data.")
                root_mapping = unflatten(
                    [
                        FixedValueMapping(Position.table_name, "table 1"),
                        EntityClassMapping(0, header="classes"),
                        EntityMapping(1, header="objects"),
                        ParameterDefinitionMapping(2, header="parameters"),
                        AlternativeMapping(Position.hidden),
                        ParameterValueMapping(3, header="values"),
                    ]
                )
                out_path = Path(temp_dir, "out.sqlite")
                writer = SqlWriter(str(out_path), overwrite_existing=True)
                write(db_map, writer, root_mapping)
                self.assertTrue(out_path.exists())
                engine = create_engine("sqlite:///" + str(out_path), future=True)
                with engine.begin():
                    metadata = MetaData()
                    metadata.reflect(bind=engine)
                    session = Session(engine, future=True)
                    self.assertIn("table 1", metadata.tables)
                    table = metadata.tables["table 1"]
                    column_names = [str(c) for c in table.c]
                    self.assertEqual(
                        column_names, ["table 1.classes", "table 1.objects", "table 1.parameters", "table 1.values"]
                    )
                    for class_ in session.query(table):
                        self.assertEqual(class_, ("oc", "o1", "p", "3h"))
                    session.close()

    def test_append_to_table(self):
        with TemporaryDirectory() as temp_dir:
            with DatabaseMapping("sqlite://", create=True) as db_map:
                self._assert_imports(import_object_classes(db_map, ("oc",)))
                self._assert_imports(import_objects(db_map, (("oc", "o1"), ("oc", "q1"))))
                db_map.commit_session("Add test data.")
                root_mapping1 = entity_export(Position.table_name, 0)
                root_mapping1.child.header = "objects"
                root_mapping1.child.filter_re = "o1"
                root_mapping2 = entity_export(Position.table_name, 0)
                root_mapping2.child.header = "objects"
                root_mapping2.child.filter_re = "q1"
                out_path = Path(temp_dir, "out.sqlite")
                writer = SqlWriter(str(out_path), overwrite_existing=True)
                write(db_map, writer, root_mapping1)
                write(db_map, writer, root_mapping2)
                self.assertTrue(out_path.exists())
                engine = create_engine("sqlite:///" + str(out_path), future=True)
                with engine.begin():
                    metadata = MetaData()
                    metadata.reflect(bind=engine)
                    session = Session(engine, future=True)
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

    def test_appending_to_table_in_existing_database(self):
        with TemporaryDirectory() as temp_dir:
            with DatabaseMapping("sqlite://", create=True) as db_map:
                self._assert_imports(import_object_classes(db_map, ("oc",)))
                self._assert_imports(import_objects(db_map, (("oc", "o1"),)))
                db_map.commit_session("Add test data.")
                out_path = Path(temp_dir, "out.sqlite")
                out_engine = create_engine("sqlite:///" + str(out_path))
                with out_engine.begin() as out_connection:
                    metadata = MetaData()
                    object_table = Table("oc", metadata, Column("objects", String))
                    metadata.create_all(out_engine)
                    out_connection.execute(object_table.insert(), {"objects": "initial_object"})
                root_mapping = entity_export(Position.table_name, 0)
                root_mapping.child.header = "objects"
                writer = SqlWriter(str(out_path), overwrite_existing=False)
                write(db_map, writer, root_mapping)
                self.assertTrue(out_path.exists())
                engine = create_engine("sqlite:///" + str(out_path), future=True)
                with engine.begin():
                    metadata = MetaData()
                    metadata.reflect(bind=engine)
                    session = Session(engine, future=True)
                    self.assertIn("oc", metadata.tables)
                    table = metadata.tables["oc"]
                    column_names = [str(c) for c in table.c]
                    self.assertEqual(column_names, ["oc.objects"])
                    expected_rows = (("initial_object",), ("o1",))
                    rows = session.query(table).all()
                    self.assertEqual(len(rows), len(expected_rows))
                    for row, expected in zip(rows, expected_rows):
                        self.assertEqual(row, expected)
                    session.close()


if __name__ == "__main__":
    unittest.main()
