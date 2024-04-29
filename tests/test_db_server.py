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
""" Unit tests for spine_db_server module. """
import os
import unittest
import threading
from tempfile import TemporaryDirectory
from spinedb_api.spine_db_server import db_server_manager, closing_spine_db_server
from spinedb_api.spine_db_client import SpineDBClient
from spinedb_api.db_mapping import DatabaseMapping


class TestDBServer(unittest.TestCase):
    def test_use_id_from_server_response(self):
        with TemporaryDirectory() as temp_dir:
            db_url = "sqlite:///" + os.path.join(temp_dir, "database.sqlite")
            with DatabaseMapping(db_url, create=True) as db_map:
                db_map.add_entity_class_item(name="fish")
                db_map.commit_session("Fishing")
            with closing_spine_db_server(db_url) as server_url:
                client = SpineDBClient.from_server_url(server_url)
                fish = client.call_method("get_entity_class_item", name="fish")["result"]
                mouse = client.call_method("update_entity_class_item", id=fish["id"], name="mouse")
                client.call_method("commit_session", "Mousing")
            with DatabaseMapping(db_url) as db_map:
                fish = db_map.get_entity_class_item(name="fish")
                mouse = db_map.get_entity_class_item(name="mouse")
                self.assertFalse(fish)
                self.assertTrue(mouse)
                self.assertEqual(mouse["name"], "mouse")

    def test_ordering(self):
        def _import_entity_class(server_url, class_name):
            client = SpineDBClient.from_server_url(server_url)
            client.db_checkin()
            _answer = client.import_data({"entity_classes": [(class_name, ())]}, f"Import {class_name}")
            client.db_checkout()

        with TemporaryDirectory() as temp_dir:
            db_url = "sqlite:///" + os.path.join(temp_dir, "database.sqlite")
            with db_server_manager() as mngr_queue:
                first_ordering = {
                    "id": "second_before_first",
                    "current": "first",
                    "precursors": {"second"},
                    "part_count": 1,
                }
                second_ordering = {
                    "id": "second_before_first",
                    "current": "second",
                    "precursors": set(),
                    "part_count": 1,
                }
                with closing_spine_db_server(
                    db_url, server_manager_queue=mngr_queue, ordering=first_ordering
                ) as first_server_url:
                    with closing_spine_db_server(
                        db_url, server_manager_queue=mngr_queue, ordering=second_ordering
                    ) as second_server_url:
                        t1 = threading.Thread(target=_import_entity_class, args=(first_server_url, "monkey"))
                        t2 = threading.Thread(target=_import_entity_class, args=(second_server_url, "donkey"))
                        t1.start()
                        with DatabaseMapping(db_url) as db_map:
                            assert db_map.get_items("entity_class") == []  # Nothing written yet
                        t2.start()
                        t1.join()
                        t2.join()
            with DatabaseMapping(db_url) as db_map:
                self.assertEqual([x["name"] for x in db_map.get_items("entity_class")], ["donkey", "monkey"])


if __name__ == "__main__":
    unittest.main()
