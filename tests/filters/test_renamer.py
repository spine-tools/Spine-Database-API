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
""" Unit tests for ``renamer`` module. """
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from sqlalchemy.engine.url import URL
from spinedb_api import (
    apply_renaming_to_parameter_definition_sq,
    apply_renaming_to_entity_class_sq,
    create_new_spine_database,
    DatabaseMapping,
    DatabaseMapping,
    import_object_classes,
    import_object_parameters,
    import_relationship_classes,
)
from spinedb_api.filters.renamer import (
    entity_class_renamer_config,
    entity_class_renamer_config_to_shorthand,
    entity_class_renamer_from_dict,
    entity_class_renamer_shorthand_to_config,
    parameter_renamer_config,
    parameter_renamer_config_to_shorthand,
    parameter_renamer_from_dict,
    parameter_renamer_shorthand_to_config,
)


class TestEntityClassRenamer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._temp_dir = TemporaryDirectory()
        cls._db_url = URL("sqlite", database=Path(cls._temp_dir.name, "test_entity_class_renamer.sqlite").as_posix())

    def setUp(self):
        create_new_spine_database(self._db_url)
        self._out_db_map = DatabaseMapping(self._db_url)
        self._db_map = DatabaseMapping(self._db_url)

    def tearDown(self):
        self._out_db_map.close()
        self._db_map.close()

    def test_renaming_empty_database(self):
        apply_renaming_to_entity_class_sq(self._db_map, {"some_name": "another_name"})
        classes = list(self._db_map.query(self._db_map.entity_class_sq).all())
        self.assertEqual(classes, [])

    def test_renaming_singe_entity_class(self):
        import_object_classes(self._out_db_map, ("old_name",))
        self._out_db_map.commit_session("Add test data")
        apply_renaming_to_entity_class_sq(self._db_map, {"old_name": "new_name"})
        classes = list(self._db_map.query(self._db_map.entity_class_sq).all())
        self.assertEqual(len(classes), 1)
        class_row = classes[0]
        keys = tuple(class_row.keys())
        expected_keys = ("id", "name", "description", "display_order", "display_icon", "hidden", "active_by_default")
        self.assertEqual(len(keys), len(expected_keys))
        for expected_key in expected_keys:
            self.assertIn(expected_key, keys)
        self.assertEqual(class_row.name, "new_name")

    def test_renaming_singe_relationship_class(self):
        import_object_classes(self._out_db_map, ("object_class",))
        import_relationship_classes(self._out_db_map, (("old_name", ("object_class",)),))
        self._out_db_map.commit_session("Add test data")
        apply_renaming_to_entity_class_sq(self._db_map, {"old_name": "new_name"})
        classes = list(self._db_map.query(self._db_map.relationship_class_sq).all())
        self.assertEqual(len(classes), 1)
        self.assertEqual(classes[0].name, "new_name")

    def test_renaming_multiple_entity_classes(self):
        import_object_classes(self._out_db_map, ("object_class1", "object_class2"))
        import_relationship_classes(
            self._out_db_map,
            (
                ("relationship_class1", ("object_class1", "object_class2")),
                ("relationship_class2", ("object_class2", "object_class1")),
            ),
        )
        self._out_db_map.commit_session("Add test data")
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

    def test_entity_class_renamer_config(self):
        config = entity_class_renamer_config(class1="renamed1", class2="renamed2")
        self.assertEqual(
            config, {"type": "entity_class_renamer", "name_map": {"class1": "renamed1", "class2": "renamed2"}}
        )

    def test_entity_class_renamer_from_dict(self):
        import_object_classes(self._out_db_map, ("old_name",))
        self._out_db_map.commit_session("Add test data")
        config = entity_class_renamer_config(old_name="new_name")
        entity_class_renamer_from_dict(self._db_map, config)
        classes = list(self._db_map.query(self._db_map.entity_class_sq).all())
        self.assertEqual(len(classes), 1)
        class_row = classes[0]
        keys = tuple(class_row.keys())
        expected_keys = ("id", "name", "description", "display_order", "display_icon", "hidden", "active_by_default")
        self.assertEqual(len(keys), len(expected_keys))
        for expected_key in expected_keys:
            self.assertIn(expected_key, keys)
        self.assertEqual(class_row.name, "new_name")


class TestEntityClassRenamerWithoutDatabase(unittest.TestCase):
    def test_entity_class_renamer_config_to_shorthand(self):
        config = entity_class_renamer_config(class1="renamed1", class2="renamed2")
        shorthand = entity_class_renamer_config_to_shorthand(config)
        self.assertEqual(shorthand, "entity_class_rename:class1:renamed1:class2:renamed2")

    def test_entity_class_renamer_shorthand_to_config(self):
        config = entity_class_renamer_shorthand_to_config("entity_class_rename:class1:renamed1:class2:renamed2")
        self.assertEqual(
            config, {"type": "entity_class_renamer", "name_map": {"class1": "renamed1", "class2": "renamed2"}}
        )


class TestParameterRenamer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._temp_dir = TemporaryDirectory()
        cls._db_url = URL("sqlite", database=Path(cls._temp_dir.name, "test_parameter_renamer.sqlite").as_posix())

    def setUp(self):
        create_new_spine_database(self._db_url)
        self._out_db_map = DatabaseMapping(self._db_url)
        self._db_map = DatabaseMapping(self._db_url)

    def tearDown(self):
        self._out_db_map.close()
        self._db_map.close()

    def test_renaming_empty_database(self):
        apply_renaming_to_parameter_definition_sq(self._db_map, {"some_name": "another_name"})
        classes = list(self._db_map.query(self._db_map.parameter_definition_sq).all())
        self.assertEqual(classes, [])

    def test_renaming_single_parameter(self):
        import_object_classes(self._out_db_map, ("object_class",))
        import_object_parameters(self._out_db_map, (("object_class", "old_name"),))
        self._out_db_map.commit_session("Add test data")
        apply_renaming_to_parameter_definition_sq(self._db_map, {"object_class": {"old_name": "new_name"}})
        parameters = list(self._db_map.query(self._db_map.parameter_definition_sq).all())
        self.assertEqual(len(parameters), 1)
        parameter_row = parameters[0]
        keys = tuple(parameter_row.keys())
        expected_keys = (
            "id",
            "name",
            "description",
            "entity_class_id",
            "default_value",
            "default_type",
            "list_value_id",
            "commit_id",
            "parameter_value_list_id",
        )
        self.assertEqual(len(keys), len(expected_keys))
        for expected_key in expected_keys:
            self.assertIn(expected_key, keys)
        self.assertEqual(parameter_row.name, "new_name")

    def test_renaming_applies_to_correct_parameter(self):
        import_object_classes(self._out_db_map, ("oc1", "oc2"))
        import_object_parameters(self._out_db_map, (("oc1", "param"), ("oc2", "param")))
        self._out_db_map.commit_session("Add test data")
        apply_renaming_to_parameter_definition_sq(self._db_map, {"oc2": {"param": "new_name"}})
        parameters = list(self._db_map.query(self._db_map.entity_parameter_definition_sq).all())
        self.assertEqual(len(parameters), 2)
        for parameter_row in parameters:
            if parameter_row.entity_class_name == "oc2":
                self.assertEqual(parameter_row.parameter_name, "new_name")
            else:
                self.assertEqual(parameter_row.parameter_name, "param")

    def test_parameter_renamer_config(self):
        config = parameter_renamer_config({"class": {"parameter1": "renamed1", "parameter2": "renamed2"}})
        self.assertEqual(
            config,
            {"type": "parameter_renamer", "name_map": {"class": {"parameter1": "renamed1", "parameter2": "renamed2"}}},
        )

    def test_parameter_renamer_from_dict(self):
        import_object_classes(self._out_db_map, ("object_class",))
        import_object_parameters(self._out_db_map, (("object_class", "old_name"),))
        self._out_db_map.commit_session("Add test data")
        config = parameter_renamer_config({"object_class": {"old_name": "new_name"}})
        parameter_renamer_from_dict(self._db_map, config)
        parameters = list(self._db_map.query(self._db_map.parameter_definition_sq).all())
        self.assertEqual(len(parameters), 1)
        parameter_row = parameters[0]
        keys = tuple(parameter_row.keys())
        expected_keys = (
            "id",
            "name",
            "description",
            "entity_class_id",
            "default_value",
            "default_type",
            "list_value_id",
            "commit_id",
            "parameter_value_list_id",
        )
        self.assertEqual(len(keys), len(expected_keys))
        for expected_key in expected_keys:
            self.assertIn(expected_key, keys)
        self.assertEqual(parameter_row.name, "new_name")


class TestParameterRenamerWithoutDatabase(unittest.TestCase):
    def test_parameter_renamer_config_to_shorthand(self):
        config = parameter_renamer_config({"class": {"parameter1": "renamed1", "parameter2": "renamed2"}})
        shorthand = parameter_renamer_config_to_shorthand(config)
        self.assertEqual(shorthand, "parameter_rename:class:parameter1:renamed1:class:parameter2:renamed2")

    def test_parameter_renamer_shorthand_to_config(self):
        config = parameter_renamer_shorthand_to_config(
            "parameter_rename:class:parameter1:renamed1:class:parameter2:renamed2"
        )
        self.assertEqual(
            config,
            {"type": "parameter_renamer", "name_map": {"class": {"parameter1": "renamed1", "parameter2": "renamed2"}}},
        )


if __name__ == "__main__":
    unittest.main()
