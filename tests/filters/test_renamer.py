######################################################################################################################
# Copyright (C) 2017 - 2020 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Unit tests for ``renamer`` module.

:author: Antti Soininen (VTT)
:date:   2.10.2020
"""
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from sqlalchemy.engine.url import URL
from spinedb_api import (
    apply_renaming_to_entity_class_sq,
    create_new_spine_database,
    DatabaseMapping,
    DiffDatabaseMapping,
    import_alternatives,
    import_object_classes,
    import_object_parameter_values,
    import_object_parameters,
    import_objects,
    import_relationship_classes,
    import_relationship_parameter_values,
    import_relationship_parameters,
    import_relationships,
    import_scenario_alternatives,
    import_scenarios,
)


class TestRenamer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._temp_dir = TemporaryDirectory()
        cls._db_url = URL("sqlite", database=Path(cls._temp_dir.name, "test_scenario_filter_mapping.sqlite").as_posix())

    def setUp(self):
        create_new_spine_database(self._db_url)
        self._out_map = DiffDatabaseMapping(self._db_url)
        self._db_map = DatabaseMapping(self._db_url)

    def tearDown(self):
        self._out_map.connection.close()
        self._db_map.connection.close()

    def test_renaming_empty_database(self):
        apply_renaming_to_entity_class_sq(self._db_map, {"some_name": "another_name"})
        classes = list(self._db_map.query(self._db_map.entity_class_sq).all())
        self.assertEqual(classes, [])

    def test_renaming_singe_entity_class(self):
        import_object_classes(self._out_map, ("old_name",))
        self._out_map.commit_session("Add test data")
        apply_renaming_to_entity_class_sq(self._db_map, {"old_name": "new_name"})
        classes = list(self._db_map.query(self._db_map.entity_class_sq).all())
        self.assertEqual(len(classes), 1)
        class_row = classes[0]
        keys = tuple(class_row.keys())
        expected_keys = ("id", "type_id", "name", "description", "display_order", "display_icon", "hidden", "commit_id")
        self.assertEqual(len(keys), len(expected_keys))
        for expected_key in expected_keys:
            self.assertIn(expected_key, keys)
        self.assertEqual(class_row.name, "new_name")

    def test_renaming_singe_relationship_class(self):
        import_object_classes(self._out_map, ("object_class",))
        import_relationship_classes(self._out_map, (("old_name", ("object_class",)),))
        self._out_map.commit_session("Add test data")
        apply_renaming_to_entity_class_sq(self._db_map, {"old_name": "new_name"})
        classes = list(self._db_map.query(self._db_map.relationship_class_sq).all())
        self.assertEqual(len(classes), 1)
        self.assertEqual(classes[0].name, "new_name")

    def test_renaming_multiple_entity_classes(self):
        import_object_classes(self._out_map, ("object_class1", "object_class2"))
        import_relationship_classes(
            self._out_map,
            (
                ("relationship_class1", ("object_class1", "object_class2")),
                ("relationship_class2", ("object_class2", "object_class1")),
            ),
        )
        self._out_map.commit_session("Add test data")
        apply_renaming_to_entity_class_sq(
            self._db_map, {"object_class1": "new_object_class", "relationship_class1": "new_relationship_class"}
        )
        object_classes = list(self._db_map.query(self._db_map.object_class_sq).all())
        self.assertEqual(len(object_classes), 2)
        names = [row.name for row in object_classes]
        for expected_name in ["new_object_class", "object_class2"]:
            self.assertIn(expected_name, names)
        relationship_classes = list(self._db_map.query(self._db_map.wide_relationship_class_sq).all())
        self.assertEqual(len(relationship_classes), 2)
        names = [row.name for row in relationship_classes]
        for expected_name in ["new_relationship_class", "relationship_class2"]:
            self.assertIn(expected_name, names)
        object_class_names = [row.object_class_name_list for row in relationship_classes]
        for expected_names in ["new_object_class,object_class2", "object_class2,new_object_class"]:
            self.assertIn(expected_names, object_class_names)


if __name__ == "__main__":
    unittest.main()
