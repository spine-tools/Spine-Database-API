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

import json
from numbers import Number
import unittest

from spinedb_api import DatabaseMapping
from spinedb_api.parameter_value import to_database


def _val_dict(val):
    keys = ("value", "type")
    values = to_database(val)
    return dict(zip(keys, values))


class TestCheckIntegrity(unittest.TestCase):
    def setUp(self):
        self.data = [
            (bool, (b'"TRUE"', b'"FALSE"', b'"T"', b'"True"', b'"False"'), (b"true", b"false")),
            (int, (b"32", b"3.14"), (b"42", b"-2")),
            (str, (b'"FOO"', b'"bar"'), (b'"foo"', b'"Bar"', b'"BAZ"')),
        ]
        self.value_type = {bool: 1, int: 2, str: 3}
        self.db_map = DatabaseMapping("sqlite://", create=True)
        self.db_map.add_items("entity_class", {"id": 1, "name": "cat"})
        self.db_map.add_items(
            "entity",
            {"id": 1, "name": "Tom", "class_id": 1},
            {"id": 2, "name": "Felix", "class_id": 1},
            {"id": 3, "name": "Jansson", "class_id": 1},
        )
        self.db_map.add_items(
            "parameter_value_list", {"id": 1, "name": "list1"}, {"id": 2, "name": "list2"}, {"id": 3, "name": "list3"}
        )
        self.db_map.add_items(
            "list_value",
            {"id": 1, **_val_dict(True), "index": 0, "parameter_value_list_id": 1},
            {"id": 2, **_val_dict(False), "index": 1, "parameter_value_list_id": 1},
            {"id": 3, **_val_dict(42), "index": 0, "parameter_value_list_id": 2},
            {"id": 4, **_val_dict(-2), "index": 1, "parameter_value_list_id": 2},
            {"id": 5, **_val_dict("foo"), "index": 0, "parameter_value_list_id": 3},
            {"id": 6, **_val_dict("Bar"), "index": 1, "parameter_value_list_id": 3},
            {"id": 7, **_val_dict("BAZ"), "index": 2, "parameter_value_list_id": 3},
        )
        self.db_map.add_items(
            "parameter_definition",
            {"id": 1, "name": "par1", "entity_class_id": 1, "parameter_value_list_id": 1},
            {"id": 2, "name": "par2", "entity_class_id": 1, "parameter_value_list_id": 2},
            {"id": 3, "name": "par3", "entity_class_id": 1, "parameter_value_list_id": 3},
        )

    @staticmethod
    def get_item(id_: int, val: bytes, entity_id: int):
        return {
            "id": 1,
            "parameter_definition_id": id_,
            "entity_class_id": 1,
            "entity_id": entity_id,
            "value": val,
            "type": None,
            "alternative_id": 1,
        }

    def test_parameter_values_and_default_values_with_list_references(self):
        # regression test for spine-tools/Spine-Toolbox#1878
        for type_, fail, pass_ in self.data:
            id_ = self.value_type[type_]  # setup: parameter definition/value list ids are equal
            for k, value in enumerate(fail):
                with self.subTest(type=type_, value=value):
                    item = self.get_item(id_, value, 1)
                    _, errors = self.db_map.add_items("parameter_value", item)
                    self.assertEqual(len(errors), 1)
                    parsed_value = json.loads(value.decode("utf8"))
                    if isinstance(parsed_value, Number):
                        parsed_value = float(parsed_value)
                    self.assertEqual(errors[0], f"value {parsed_value} of par{id_} for ('Tom',) is not in list{id_}")
            for k, value in enumerate(pass_):
                with self.subTest(type=type_, value=value):
                    item = self.get_item(id_, value, k + 1)
                    _, errors = self.db_map.add_items("parameter_value", item)
                    self.assertEqual(errors, [])
