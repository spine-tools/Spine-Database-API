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
from tempfile import TemporaryDirectory
import threading
import unittest
from spinedb_api import Array, DateTime, Duration, Map, TimePattern, TimeSeriesVariableResolution, to_database
from spinedb_api.db_mapping import DatabaseMapping
from spinedb_api.spine_db_client import SpineDBClient
from spinedb_api.spine_db_server import DBHandler, closing_spine_db_server, db_server_manager


class TestDBServer(unittest.TestCase):
    def _assert_import(self, result):
        self.assertIn("result", result)
        count, errors = result["result"]
        self.assertEqual(errors, [])
        return count

    def test_use_id_from_server_response(self):
        with TemporaryDirectory() as temp_dir:
            db_url = "sqlite:///" + os.path.join(temp_dir, "database.sqlite")
            with DatabaseMapping(db_url, create=True) as db_map:
                db_map.add_entity_class_item(name="fish")
                db_map.commit_session("Fishing")
            db_map.engine.dispose()
            with closing_spine_db_server(db_url) as server_url:
                client = SpineDBClient.from_server_url(server_url)
                fish = client.call_method("get_entity_class_item", name="fish")["result"]
                client.call_method("update_entity_class_item", id=fish["id"], name="mouse")
                client.call_method("commit_session", "Mousing")
            with DatabaseMapping(db_url) as db_map:
                fish = db_map.get_entity_class_item(name="fish")
                mouse = db_map.get_entity_class_item(name="mouse")
                self.assertFalse(fish)
                self.assertTrue(mouse)
                self.assertEqual(mouse["name"], "mouse")
            db_map.engine.dispose()

    def test_ordering(self):
        def _import_entity_class(server_url, class_name):
            client = SpineDBClient.from_server_url(server_url)
            client.db_checkin()
            self._assert_import(client.import_data({"entity_classes": [(class_name, ())]}, f"Import {class_name}"))
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
                        db_map.engine.dispose()
                        t2.start()
                        t1.join()
                        t2.join()
            with DatabaseMapping(db_url) as db_map:
                self.assertEqual([x["name"] for x in db_map.get_items("entity_class")], ["donkey", "monkey"])
            db_map.engine.dispose()

    def test_in_memory_database(self):
        url = "sqlite://"
        with closing_spine_db_server(url):
            handler = DBHandler(url)
            try:
                response = handler.call_method("add_entity_class_item", name="Object")
                _, error = response["result"]
                self.assertIsNone(error)
                response = handler.call_method("add_parameter_definition_item", name="Y", entity_class_name="Object")
                definition, error = response["result"]
                self.assertIsNone(error)
                definition_id = definition["id"]
                handler.call_method("commit_session", "Add test data.")
                response = handler.call_method("get_parameter_definition_item", id=definition_id)
                self.assertEqual(response["result"]["parameter_type_list"], ())
            finally:
                handler.close_db_map()

    def test_query_with_data(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            class_id = None
            with DatabaseMapping(url, create=True) as db_map:
                entity_class = db_map.add_entity_class(name="Object")
                db_map.commit_session("Add test data.")
                class_id = entity_class["id"].db_id
            db_map.engine.dispose()
            with closing_spine_db_server(url) as server_url:
                client = SpineDBClient.from_server_url(server_url)
                result = client.query("entity_class_sq")
                self.assertEqual(
                    result,
                    {
                        "result": {
                            "entity_class_sq": [
                                {
                                    "id": class_id,
                                    "name": "Object",
                                    "description": None,
                                    "active_by_default": True,
                                    "display_icon": None,
                                    "display_order": 99,
                                    "hidden": 0,
                                }
                            ]
                        }
                    },
                )

    def test_export_parameter_values(self):
        with closing_spine_db_server("sqlite://") as server_url:
            client = SpineDBClient.from_server_url(server_url)
            self._assert_import(
                client.import_data(
                    {"entity_classes": [("Object",)], "parameter_definitions": [("Object", "X")]},
                    "Import basic structure.",
                )
            )
            self._assert_import(
                client.import_data(
                    {
                        "entities": [
                            ("Object", "float"),
                            ("Object", "string"),
                            ("Object", "boolean"),
                            ("Object", "none"),
                            ("Object", "date time"),
                            ("Object", "duration"),
                            ("Object", "array"),
                            ("Object", "time pattern"),
                            ("Object", "time series"),
                            ("Object", "map"),
                        ]
                    },
                    "Import entities.",
                )
            )
            self._assert_import(
                client.import_data(
                    {
                        "parameter_values": [
                            ("Object", ("float",), "X", to_database(2.3)),
                            ("Object", ("string",), "X", to_database("oh my")),
                            ("Object", ("boolean",), "X", to_database(False)),
                            ("Object", ("none",), "X", to_database(None)),
                            ("Object", ("date time",), "X", to_database(DateTime("2025-09-02T13:45"))),
                            ("Object", ("duration",), "X", to_database(Duration("33m"))),
                            ("Object", ("array",), "X", to_database(Array([2.3]))),
                            ("Object", ("time pattern",), "X", to_database(TimePattern(["M1-12"], [2.3]))),
                            (
                                "Object",
                                ("time series",),
                                "X",
                                to_database(
                                    TimeSeriesVariableResolution(
                                        ["2025-09-02T13:50"], [2.3], ignore_year=False, repeat=True
                                    )
                                ),
                            ),
                            ("Object", ("map",), "X", to_database(Map([DateTime("2025-09-02T13:50")], [2.3]))),
                        ]
                    },
                    "Import values.",
                )
            )
            result = client.export_data()
            self.assertEqual(len(result), 1)
            result_data = result["result"]
            self.assertEqual(len(result_data), 5)
            self.assertEqual(result_data["alternatives"], [["Base", "Base alternative"]])
            self.assertEqual(result_data["entity_classes"], [["Object", [], None, None, True]])
            self.assertEqual(result_data["parameter_definitions"], [["Object", "X", [None, None], None, None]])
            self.assertCountEqual(
                result_data["entities"],
                [
                    ["Object", "float", None],
                    ["Object", "string", None],
                    ["Object", "boolean", None],
                    ["Object", "none", None],
                    ["Object", "date time", None],
                    ["Object", "duration", None],
                    ["Object", "array", None],
                    ["Object", "time pattern", None],
                    ["Object", "time series", None],
                    ["Object", "map", None],
                ],
            )
            self.assertCountEqual(
                result_data["parameter_values"],
                [
                    ["Object", "float", "X", list(to_database(2.3)), "Base"],
                    ["Object", "string", "X", list(to_database("oh my")), "Base"],
                    ["Object", "boolean", "X", list(to_database(False)), "Base"],
                    ["Object", "none", "X", list(to_database(None)), "Base"],
                    ["Object", "date time", "X", list(to_database(DateTime("2025-09-02T13:45"))), "Base"],
                    ["Object", "duration", "X", list(to_database(Duration("33m"))), "Base"],
                    ["Object", "array", "X", list(to_database(Array([2.3]))), "Base"],
                    ["Object", "time pattern", "X", list(to_database(TimePattern(["M1-12"], [2.3]))), "Base"],
                    [
                        "Object",
                        "time series",
                        "X",
                        list(
                            to_database(
                                TimeSeriesVariableResolution(
                                    ["2025-09-02T13:50"], [2.3], ignore_year=False, repeat=True
                                )
                            )
                        ),
                        "Base",
                    ],
                    ["Object", "map", "X", list(to_database(Map([DateTime("2025-09-02T13:50")], [2.3]))), "Base"],
                ],
            )
