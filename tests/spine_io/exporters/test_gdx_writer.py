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
""" Unit tests for gdx writer. """
import math
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from gdx2py import GAMSParameter, GdxFile
from spinedb_api import (
    DatabaseMapping,
    Map,
    import_object_classes,
    import_object_parameter_values,
    import_object_parameters,
    import_objects,
    import_relationship_classes,
    import_relationships,
)
from spinedb_api.export_mapping import entity_export, entity_parameter_value_export
from spinedb_api.export_mapping.export_mapping import FixedValueMapping
from spinedb_api.mapping import Position, unflatten
from spinedb_api.spine_io.exporters.gdx_writer import GdxWriter
from spinedb_api.spine_io.exporters.writer import WriterException, write
from spinedb_api.spine_io.gdx_utils import find_gams_directory
from tests.mock_helpers import AssertSuccessTestCase


class TestGdxWriter(AssertSuccessTestCase):
    _gams_dir = find_gams_directory()

    @unittest.skipIf(_gams_dir is None, "No working GAMS installation found.")
    def test_write_empty_database(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            root_mapping = entity_export(entity_class_position=Position.table_name, entity_position=0)
            root_mapping.child.header = "*"
            with TemporaryDirectory() as temp_dir:
                file_path = Path(temp_dir, "test_write_empty_database.gdx")
                writer = GdxWriter(str(file_path), self._gams_dir)
                write(db_map, writer, root_mapping)
                with GdxFile(str(file_path), "r", self._gams_dir) as gdx_file:
                    self.assertEqual(len(gdx_file), 0)

    @unittest.skipIf(_gams_dir is None, "No working GAMS installation found.")
    def test_write_single_object_class_and_object(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_object_classes(db_map, ("oc",)))
            self._assert_imports(import_objects(db_map, (("oc", "o1"),)))
            db_map.commit_session("Add test data.")
            root_mapping = entity_export(Position.table_name, 0)
            root_mapping.child.header = "*"
            with TemporaryDirectory() as temp_dir:
                file_path = Path(temp_dir, "test_write_single_object_class_and_object.gdx")
                writer = GdxWriter(str(file_path), self._gams_dir)
                write(db_map, writer, root_mapping)
                with GdxFile(str(file_path), "r", self._gams_dir) as gdx_file:
                    self.assertEqual(len(gdx_file), 1)
                    gams_set = gdx_file["oc"]
                    self.assertIsNone(gams_set.domain)
                    self.assertEqual(gams_set.elements, ["o1"])

    @unittest.skipIf(_gams_dir is None, "No working GAMS installation found.")
    def test_write_2D_relationship(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_object_classes(db_map, ("oc1", "oc2")))
            self._assert_imports(import_objects(db_map, (("oc1", "o1"), ("oc2", "o2"))))
            self._assert_imports(import_relationship_classes(db_map, (("rel", ("oc1", "oc2")),)))
            self._assert_imports(import_relationships(db_map, (("rel", ("o1", "o2")),)))
            db_map.commit_session("Add test data.")
            root_mapping = entity_export(
                Position.table_name, Position.hidden, [Position.header, Position.header], [0, 1]
            )
            with TemporaryDirectory() as temp_dir:
                file_path = Path(temp_dir, "test_write_2D_relationship.gdx")
                writer = GdxWriter(str(file_path), self._gams_dir)
                write(db_map, writer, root_mapping)
                with GdxFile(str(file_path), "r", self._gams_dir) as gdx_file:
                    self.assertEqual(len(gdx_file), 1)
                    gams_set = gdx_file["rel"]
                    self.assertEqual(gams_set.domain, ["oc1", "oc2"])
                    self.assertEqual(gams_set.elements, [("o1", "o2")])

    @unittest.skipIf(_gams_dir is None, "No working GAMS installation found.")
    def test_write_parameters(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_object_classes(db_map, ("oc",)))
            self._assert_imports(import_object_parameters(db_map, (("oc", "p"),)))
            self._assert_imports(import_objects(db_map, (("oc", "o1"),)))
            self._assert_imports(import_object_parameter_values(db_map, (("oc", "o1", "p", 2.3),)))
            db_map.commit_session("Add test data.")
            root_mapping = entity_parameter_value_export(
                entity_class_position=Position.table_name, entity_position=0, value_position=1
            )
            mappings = root_mapping.flatten()
            mappings[3].header = "*"
            with TemporaryDirectory() as temp_dir:
                file_path = Path(temp_dir, "test_write_parameters.gdx")
                writer = GdxWriter(str(file_path), self._gams_dir)
                write(db_map, writer, root_mapping)
                with GdxFile(str(file_path), "r", self._gams_dir) as gdx_file:
                    self.assertEqual(len(gdx_file), 1)
                    gams_parameter = gdx_file["oc"]
                    self.assertEqual(len(gams_parameter), 1)
                    self.assertEqual(gams_parameter["o1"], 2.3)

    @unittest.skipIf(_gams_dir is None, "No working GAMS installation found.")
    def test_non_numerical_parameter_value_raises_writer_expection(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_object_classes(db_map, ("oc",)))
            self._assert_imports(import_object_parameters(db_map, (("oc", "p"),)))
            self._assert_imports(import_objects(db_map, (("oc", "o1"),)))
            self._assert_imports(import_object_parameter_values(db_map, (("oc", "o1", "p", "text"),)))
            db_map.commit_session("Add test data.")
            root_mapping = entity_parameter_value_export(
                entity_class_position=Position.table_name, entity_position=0, value_position=1
            )
            mappings = root_mapping.flatten()
            mappings[3].header = "*"
            with TemporaryDirectory() as temp_dir:
                file_path = Path(temp_dir, "test_write_parameters.gdx")
                writer = GdxWriter(str(file_path), self._gams_dir)
                self.assertRaises(WriterException, write, db_map, writer, root_mapping)

    @unittest.skipIf(_gams_dir is None, "No working GAMS installation found.")
    def test_empty_parameter(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_object_classes(db_map, ("oc",)))
            self._assert_imports(import_object_parameters(db_map, (("oc", "p"),)))
            self._assert_imports(import_objects(db_map, (("oc", "o1"),)))
            self._assert_imports(import_object_parameter_values(db_map, (("oc", "o1", "p", Map([], [], str)),)))
            db_map.commit_session("Add test data.")
            root_mapping = entity_parameter_value_export(
                entity_class_position=Position.table_name, entity_position=0, value_position=1
            )
            mappings = root_mapping.flatten()
            mappings[3].header = "*"
            mappings[-1].filter_re = "single_value"
            with TemporaryDirectory() as temp_dir:
                file_path = Path(temp_dir, "test_write_parameters.gdx")
                writer = GdxWriter(str(file_path), self._gams_dir)
                write(db_map, writer, root_mapping)
                with GdxFile(str(file_path), "r", self._gams_dir) as gdx_file:
                    self.assertEqual(len(gdx_file), 1)
                    gams_parameter = gdx_file["oc"]
                    self.assertIsInstance(gams_parameter, GAMSParameter)
                    self.assertEqual(len(gams_parameter), 0)

    @unittest.skipIf(_gams_dir is None, "No working GAMS installation found.")
    def test_write_scalars(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_object_classes(db_map, ("oc",)))
            self._assert_imports(import_object_parameters(db_map, (("oc", "p"),)))
            self._assert_imports(import_objects(db_map, (("oc", "o1"),)))
            self._assert_imports(import_object_parameter_values(db_map, (("oc", "o1", "p", 2.3),)))
            db_map.commit_session("Add test data.")
            root_mapping = entity_parameter_value_export(entity_class_position=Position.table_name, value_position=0)
            with TemporaryDirectory() as temp_dir:
                file_path = Path(temp_dir, "test_write_scalars.gdx")
                writer = GdxWriter(str(file_path), self._gams_dir)
                write(db_map, writer, root_mapping)
                with GdxFile(str(file_path), "r", self._gams_dir) as gdx_file:
                    self.assertEqual(len(gdx_file), 1)
                    gams_scalar = gdx_file["oc"]
                    self.assertEqual(float(gams_scalar), 2.3)

    @unittest.skipIf(_gams_dir is None, "No working GAMS installation found.")
    def test_two_tables(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_object_classes(db_map, ("oc1", "oc2")))
            self._assert_imports(import_objects(db_map, (("oc1", "o"), ("oc2", "p"))))
            self._assert_imports(db_map.commit_session("Add test data."))
            root_mapping = entity_export(entity_class_position=Position.table_name, entity_position=0)
            root_mapping.child.header = "*"
            with TemporaryDirectory() as temp_dir:
                file_path = Path(temp_dir, "test_two_tables.gdx")
                writer = GdxWriter(str(file_path), self._gams_dir)
                write(db_map, writer, root_mapping)
                with GdxFile(str(file_path), "r", self._gams_dir) as gdx_file:
                    self.assertEqual(len(gdx_file), 2)
                    gams_set = gdx_file["oc1"]
                    self.assertIsNone(gams_set.domain)
                    self.assertEqual(gams_set.elements, ["o"])
                    gams_set = gdx_file["oc2"]
                    self.assertIsNone(gams_set.domain)
                    self.assertEqual(gams_set.elements, ["p"])

    @unittest.skipIf(_gams_dir is None, "No working GAMS installation found.")
    def test_append_to_table(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_object_classes(db_map, ("oc1", "oc2")))
            self._assert_imports(import_objects(db_map, (("oc1", "o"), ("oc2", "p"))))
            db_map.commit_session("Add test data.")
            root_mapping1 = unflatten(
                [FixedValueMapping(Position.table_name, value="set_X")] + entity_export(entity_position=0).flatten()
            )
            root_mapping1.child.filter_re = "oc1"
            root_mapping1.child.child.header = "*"
            root_mapping2 = unflatten(
                [FixedValueMapping(Position.table_name, value="set_X")] + entity_export(entity_position=0).flatten()
            )
            root_mapping2.child.filter_re = "oc2"
            root_mapping2.child.child.header = "*"
            with TemporaryDirectory() as temp_dir:
                file_path = Path(temp_dir, "test_two_tables.gdx")
                writer = GdxWriter(str(file_path), self._gams_dir)
                write(db_map, writer, root_mapping1, root_mapping2)
                with GdxFile(str(file_path), "r", self._gams_dir) as gdx_file:
                    self.assertEqual(len(gdx_file), 1)
                    gams_set = gdx_file["set_X"]
                    self.assertIsNone(gams_set.domain)
                    self.assertEqual(gams_set.elements, ["o", "p"])

    @unittest.skipIf(_gams_dir is None, "No working GAMS installation found.")
    def test_parameter_value_non_convertible_to_float_raises_WriterException(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_object_classes(db_map, ("oc",)))
            self._assert_imports(import_object_parameters(db_map, (("oc", "param"),)))
            self._assert_imports(import_objects(db_map, (("oc", "o"), ("oc", "p"))))
            self._assert_imports(
                import_object_parameter_values(db_map, (("oc", "o", "param", "text"), ("oc", "p", "param", 2.3)))
            )
            db_map.commit_session("Add test data.")
            root_mapping = entity_parameter_value_export(
                entity_class_position=Position.hidden,
                definition_position=Position.table_name,
                entity_position=0,
                value_position=1,
            )
            root_mapping.child.child.child.header = "*"
            with TemporaryDirectory() as temp_dir:
                file_path = Path(temp_dir, "test_two_tables.gdx")
                writer = GdxWriter(str(file_path), self._gams_dir)
                self.assertRaises(WriterException, write, db_map, writer, root_mapping)

    @unittest.skipIf(_gams_dir is None, "No working GAMS installation found.")
    def test_non_string_set_element_raises_WriterException(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_object_classes(db_map, ("oc",)))
            self._assert_imports(import_object_parameters(db_map, (("oc", "param"),)))
            self._assert_imports(import_objects(db_map, (("oc", "o"), ("oc", "p"))))
            self._assert_imports(
                import_object_parameter_values(db_map, (("oc", "o", "param", 2.3), ("oc", "p", "param", "text")))
            )
            db_map.commit_session("Add test data.")
            root_mapping = entity_parameter_value_export(
                entity_class_position=Position.hidden,
                definition_position=Position.table_name,
                entity_position=0,
                value_position=1,
            )
            root_mapping.child.child.child.header = "*"
            with TemporaryDirectory() as temp_dir:
                file_path = Path(temp_dir, "test_two_tables.gdx")
                writer = GdxWriter(str(file_path), self._gams_dir)
                self.assertRaises(WriterException, write, db_map, writer, root_mapping)

    @unittest.skipIf(_gams_dir is None, "No working GAMS installation found.")
    def test_special_value_conversions(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_imports(import_object_classes(db_map, ("oc",)))
            self._assert_imports(
                import_object_parameters(
                    db_map,
                    (
                        ("oc", "epsilon1"),
                        ("oc", "epsilon2"),
                        ("oc", "epsilon3"),
                        ("oc", "epsilon4"),
                        ("oc", "infinity"),
                        ("oc", "negative_infinity"),
                        ("oc", "nan"),
                    ),
                )
            )
            self._assert_imports(import_objects(db_map, (("oc", "o1"),)))
            self._assert_imports(
                import_object_parameter_values(
                    db_map,
                    (
                        ("oc", "o1", "epsilon1", sys.float_info.min),
                        ("oc", "o1", "epsilon2", 2.2250738585072014e-308),
                        ("oc", "o1", "epsilon3", 1e-10),
                        ("oc", "o1", "epsilon4", "EPS"),
                        ("oc", "o1", "infinity", math.inf),
                        ("oc", "o1", "negative_infinity", -math.inf),
                        ("oc", "o1", "nan", math.nan),
                    ),
                )
            )
            db_map.commit_session("Add test data.")
            root_mapping = entity_parameter_value_export(
                entity_class_position=Position.table_name, entity_position=0, definition_position=1, value_position=2
            )
            mappings = root_mapping.flatten()
            mappings[1].header = mappings[3].header = "*"
            with TemporaryDirectory() as temp_dir:
                file_path = Path(temp_dir, "test_special_value_conversions.gdx")
                writer = GdxWriter(str(file_path), self._gams_dir)
                write(db_map, writer, root_mapping)
                with GdxFile(str(file_path), "r", self._gams_dir) as gdx_file:
                    self.assertEqual(len(gdx_file), 1)
                    gams_parameter = gdx_file["oc"]
                    self.assertEqual(len(gams_parameter), 7)
                    self.assertEqual(gams_parameter[("o1", "epsilon1")], sys.float_info.min)
                    self.assertEqual(gams_parameter[("o1", "epsilon2")], sys.float_info.min)
                    self.assertEqual(gams_parameter[("o1", "epsilon3")], sys.float_info.min)
                    self.assertEqual(gams_parameter[("o1", "epsilon4")], sys.float_info.min)
                    self.assertEqual(gams_parameter[("o1", "infinity")], math.inf)
                    self.assertEqual(gams_parameter[("o1", "negative_infinity")], -math.inf)
                    self.assertTrue(math.isnan(gams_parameter[("o1", "nan")]))


if __name__ == "__main__":
    unittest.main()
