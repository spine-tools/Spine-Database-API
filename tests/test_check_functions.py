######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
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

from spinedb_api.db_cache import DBCache, ParameterValueItem
from spinedb_api.exception import SpineIntegrityError

from spinedb_api.check_functions import replace_parameter_values_with_list_references


class TestCheckFunctions(unittest.TestCase):
    def setUp(self):
        self.data = [
            (bool, (b'"TRUE"', b'"FALSE"', b'"T"', b'"True"', b'"False"'), (b'true', b'false')),
            (int, (b'32', b'3.14'), (b'42', b'-2')),
            (str, (b'"FOO"', b'"bar"'), (b'"foo"', b'"Bar"', b'"BAZ"')),
        ]
        self.parameter_definitions = {
            1: {'name': 'par1', 'entity_class_id': 1, 'parameter_value_list_id': 1},
            2: {'name': 'par2', 'entity_class_id': 1, 'parameter_value_list_id': 2},
            3: {'name': 'par2', 'entity_class_id': 1, 'parameter_value_list_id': 3},
        }
        self.value_type = {bool: 1, int: 2, str: 3}
        self.parameter_value_lists = {1: (1, 2), 2: (3, 4), 3: (5, 6, 7)}
        self.list_values = {1: True, 2: False, 3: 42, 4: -2, 5: 'foo', 6: 'Bar', 7: 'BAZ'}

    def get_item(self, _type: type, val: bytes):
        _id = self.value_type[_type]  # setup: parameter definition/value list ids are equal
        kwargs = {
            'id': 1,
            'parameter_definition_id': _id,
            'entity_class_id': 1,
            'entity_id': 1,
            'object_class_id': 1,
            'object_id': 1,
            'value': val,
            'commit_id': 3,
            'alternative_id': 1,
            'object_class_name': 'test_objcls',
            'alternative_name': 'Base',
            'object_name': 'obj1',
        }
        return ParameterValueItem(DBCache(lambda *_, **__: None), item_type="value", **kwargs)

    def test_replace_parameter_or_default_values_with_list_references(self):
        # regression test for spine-tools/Spine-Toolbox#1878
        for _type, _fail, _pass in self.data:
            for data in _fail:
                with self.subTest(_type=_type, data=data):
                    expect_in = json.loads(data.decode('utf8'))
                    if isinstance(expect_in, Number):
                        expect_in = float(expect_in)
                    ref = [self.list_values[i] for i in self.parameter_value_lists[self.value_type[_type]]]
                    expect_ref = ", ".join(f"{json.dumps(i)!r}" for i in ref)
                    self.assertRaisesRegex(
                        SpineIntegrityError,
                        fr"{expect_in!r}.+{expect_ref}",
                        replace_parameter_values_with_list_references,
                        self.get_item(_type, data),
                        self.parameter_definitions,
                        self.parameter_value_lists,
                        self.list_values,
                    )

            for data in _pass:
                with self.subTest(_type=_type, data=data):
                    replace_parameter_values_with_list_references(
                        self.get_item(_type, data),
                        self.parameter_definitions,
                        self.parameter_value_lists,
                        self.list_values,
                    )
