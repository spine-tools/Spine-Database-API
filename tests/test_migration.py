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

""" Unit tests for migration scripts. """
import os.path
from tempfile import TemporaryDirectory
import unittest
from sqlalchemy import inspect, text
from sqlalchemy.engine.url import URL
from spinedb_api import DatabaseMapping
from spinedb_api.helpers import _create_first_spine_database, create_new_spine_database, is_head_engine, schema_dict


class TestMigration(unittest.TestCase):
    def test_upgrade_content(self):
        """Tests that the upgrade scripts when applied on a db that has some contents
        persist that content entirely.
        """
        with TemporaryDirectory() as temp_dir:
            db_url = URL.create("sqlite", database=os.path.join(temp_dir, "test_upgrade_content.sqlite"))
            engine = _create_first_spine_database(db_url)
            # Insert basic stuff
            with engine.begin() as connection:
                connection.execute(text("INSERT INTO object_class (id, name) VALUES (1, 'dog')"))
                connection.execute(text("INSERT INTO object_class (id, name) VALUES (2, 'fish')"))
                connection.execute(text("INSERT INTO object (id, class_id, name) VALUES (1, 1, 'pluto')"))
                connection.execute(text("INSERT INTO object (id, class_id, name) VALUES (2, 1, 'scooby')"))
                connection.execute(text("INSERT INTO object (id, class_id, name) VALUES (3, 2, 'nemo')"))
                connection.execute(
                    text(
                        "INSERT INTO relationship_class (id, name, dimension, object_class_id) VALUES (1, 'dog__fish', 0, 1)"
                    )
                )
                connection.execute(
                    text(
                        "INSERT INTO relationship_class (id, name, dimension, object_class_id) VALUES (1, 'dog__fish', 1, 2)"
                    )
                )
                connection.execute(
                    text(
                        "INSERT INTO relationship (id, class_id, name, dimension, object_id) VALUES (1, 1, 'pluto__nemo', 0, 1)"
                    )
                )
                connection.execute(
                    text(
                        "INSERT INTO relationship (id, class_id, name, dimension, object_id) VALUES (1, 1, 'pluto__nemo', 1, 3)"
                    )
                )
                connection.execute(
                    text(
                        "INSERT INTO relationship (id, class_id, name, dimension, object_id) VALUES (2, 1, 'scooby__nemo', 0, 2)"
                    )
                )
                connection.execute(
                    text(
                        "INSERT INTO relationship (id, class_id, name, dimension, object_id) VALUES (2, 1, 'scooby__nemo', 1, 3)"
                    )
                )
                connection.execute(text("INSERT INTO parameter (id, object_class_id, name) VALUES (1, 1, 'breed')"))
                connection.execute(text("INSERT INTO parameter (id, object_class_id, name) VALUES (2, 2, 'water')"))
                connection.execute(
                    text("INSERT INTO parameter (id, relationship_class_id, name) VALUES (3, 1, 'relative_speed')")
                )
                connection.execute(
                    text("INSERT INTO parameter_value (parameter_id, object_id, value) VALUES (1, 1, '\"labrador\"')")
                )
                connection.execute(
                    text("INSERT INTO parameter_value (parameter_id, object_id, value) VALUES (1, 2, '\"big dane\"')")
                )
                connection.execute(
                    text("INSERT INTO parameter_value (parameter_id, relationship_id, value) VALUES (3, 1, '100')")
                )
                connection.execute(
                    text("INSERT INTO parameter_value (parameter_id, relationship_id, value) VALUES (3, 2, '-1')")
                )
            # Upgrade the db and check that our stuff is still there
            with DatabaseMapping(db_url, upgrade=True) as db_map:
                object_classes = {x.id: x.name for x in db_map.query(db_map.object_class_sq)}
                objects = {x.id: (object_classes[x.class_id], x.name) for x in db_map.query(db_map.object_sq)}
                rel_clss = {
                    x.id: (x.name, x.object_class_name_list) for x in db_map.query(db_map.wide_relationship_class_sq)
                }
                rels = {
                    x.id: (rel_clss[x.class_id][0], x.name, x.object_name_list)
                    for x in db_map.query(db_map.wide_relationship_sq)
                }
                obj_par_defs = {
                    x.id: (object_classes[x.object_class_id], x.parameter_name)
                    for x in db_map.query(db_map.object_parameter_definition_sq)
                }
                rel_par_defs = {
                    x.id: (rel_clss[x.relationship_class_id][0], x.parameter_name)
                    for x in db_map.query(db_map.relationship_parameter_definition_sq)
                }
                obj_par_vals = {
                    (obj_par_defs[x.parameter_id][1], objects[x.object_id][1], x.value)
                    for x in db_map.query(db_map.object_parameter_value_sq)
                }
                rel_par_vals = {
                    (rel_par_defs[x.parameter_id][1], rels[x.relationship_id][1], x.value)
                    for x in db_map.query(db_map.relationship_parameter_value_sq)
                }
                self.assertTrue(len(object_classes), 2)
                self.assertTrue(len(objects), 3)
                self.assertTrue(len(rel_clss), 1)
                self.assertTrue(len(rels), 2)
                self.assertTrue(len(obj_par_defs), 2)
                self.assertTrue(len(rel_par_defs), 1)
                self.assertTrue(len(obj_par_vals), 2)
                self.assertTrue(len(rel_par_vals), 2)
                self.assertTrue("dog" in object_classes.values())
                self.assertTrue("fish" in object_classes.values())
                self.assertTrue(("dog", "pluto") in objects.values())
                self.assertTrue(("dog", "scooby") in objects.values())
                self.assertTrue(("fish", "nemo") in objects.values())
                self.assertTrue(("dog__fish", "dog,fish") in rel_clss.values())
                self.assertTrue(("dog__fish", "pluto__nemo", "pluto,nemo") in rels.values())
                self.assertTrue(("dog__fish", "scooby__nemo", "scooby,nemo") in rels.values())
                self.assertTrue(("dog", "breed") in obj_par_defs.values())
                self.assertTrue(("fish", "water") in obj_par_defs.values())
                self.assertTrue(("dog__fish", "relative_speed") in rel_par_defs.values())
                self.assertTrue(("breed", "scooby", b'"big dane"') in obj_par_vals)
                self.assertTrue(("breed", "pluto", b'"labrador"') in obj_par_vals)
                self.assertTrue(("relative_speed", "pluto__nemo", b"100") in rel_par_vals)
                self.assertTrue(("relative_speed", "scooby__nemo", b"-1") in rel_par_vals)
