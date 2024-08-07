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
import unittest
from spinedb_api import (
    Array,
    DatabaseMapping,
    DateTime,
    Duration,
    Map,
    TimePattern,
    TimeSeriesFixedResolution,
    to_database,
)
from spinedb_api.db_mapping_helpers import is_parameter_type_valid, type_check_args
from tests.mock_helpers import AssertSuccessTestCase


class TestTypeCheckArgs(AssertSuccessTestCase):
    def test_none_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Star"))
            definition = self._assert_success(
                db_map.add_parameter_definition_item(name="mass", entity_class_name="Star")
            )
            args = type_check_args(definition)
            self.assertEqual(args, ((), None, None, None))
            self.assertTrue(is_parameter_type_valid(*args))
            self._assert_success(db_map.add_entity_item(name="Vega", entity_class_name="Star"))
            x, value_type = to_database(None)
            value = self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="Star",
                    entity_byname=("Vega",),
                    parameter_definition_name="mass",
                    alternative_name="Base",
                    value=x,
                    type=value_type,
                )
            )
            args = type_check_args(value)
            self.assertEqual(args, ((), b"null", None, None))
            self.assertTrue(is_parameter_type_valid(*args))

    def test_non_empty_parameter_type_list(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Star"))
            definition = self._assert_success(
                db_map.add_parameter_definition_item(
                    name="mass", entity_class_name="Star", parameter_type_list=("bool", "2d_map")
                )
            )
            args = type_check_args(definition)
            self.assertEqual(args, ((("bool", 0), ("map", 2)), None, None, None))
            self.assertTrue(is_parameter_type_valid(*args))
            self._assert_success(db_map.add_entity_item(name="Vega", entity_class_name="Star"))
            x, value_type = to_database(None)
            value = self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="Star",
                    entity_byname=("Vega",),
                    parameter_definition_name="mass",
                    alternative_name="Base",
                    value=x,
                    type=value_type,
                )
            )
            self.assertEqual(type_check_args(value), ((("bool", 0), ("map", 2)), b"null", None, None))
            self.assertTrue(is_parameter_type_valid(*args))

    def test_with_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Star"))
            x, value_type = to_database(2.3)
            definition = self._assert_success(
                db_map.add_parameter_definition_item(
                    name="mass", entity_class_name="Star", default_value=x, default_type=value_type
                )
            )
            args = type_check_args(definition)
            self.assertEqual(args, ((), x, None, value_type))
            self.assertEqual(definition["parsed_value"], 2.3)
            args = type_check_args(definition)
            self.assertEqual(args, ((), x, 2.3, value_type))
            self.assertTrue(is_parameter_type_valid(*args))
            self._assert_success(db_map.add_entity_item(name="Vega", entity_class_name="Star"))
            value = self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="Star",
                    entity_byname=("Vega",),
                    parameter_definition_name="mass",
                    alternative_name="Base",
                    value=x,
                    type=value_type,
                )
            )
            args = type_check_args(value)
            self.assertEqual(args, ((), x, None, value_type))
            self.assertEqual(value["parsed_value"], 2.3)
            args = type_check_args(value)
            self.assertEqual(args, ((), x, 2.3, value_type))
            self.assertTrue(is_parameter_type_valid(*args))


class TestIsParameterTypeValid(AssertSuccessTestCase):
    def test_none_is_always_valid(self):
        self.assertTrue(is_parameter_type_valid((), None, None, None))
        value, value_type = to_database(None)
        self.assertTrue(is_parameter_type_valid((), value, None, value_type))
        self.assertTrue(is_parameter_type_valid(("duration", "time_series"), value, None, value_type))

    def test_valid_types_are_valid(self):
        def assert_test(value, rank):
            db_value, value_type = to_database(value)
            self.assertTrue(is_parameter_type_valid(((value_type, rank),), db_value, None, value_type))
            self.assertTrue(is_parameter_type_valid(((value_type, rank),), db_value, value, value_type))

        values = {
            0: (2.3, "I'm a string", True, Duration("12M"), DateTime("2024-07-25T12:00:00")),
            1: (
                Array([1.0]),
                TimePattern(["M1-12"], [1.0]),
                TimeSeriesFixedResolution("2024-07-25T12:00:00", "1h", [1.0], False, False),
            ),
            2: (Map(["a", "b"], [1.0, Map(["c"], [2.0])]),),
        }
        for rank, xs in values.items():
            for x in xs:
                with self.subTest(value=x):
                    assert_test(x, rank)

    def test_invalid_types_are_invalid(self):
        def assert_test(value, rank):
            db_value, value_type = to_database(value)
            test_type = "float" if value_type != "float" else "bool"
            self.assertFalse(is_parameter_type_valid(((test_type, rank),), db_value, None, value_type))
            if value_type == "map":
                test_rank = rank + 1
                self.assertFalse(is_parameter_type_valid(((value_type, test_rank),), db_value, None, value_type))
            self.assertFalse(is_parameter_type_valid(((test_type, rank),), db_value, value, value_type))

        values = {
            0: (2.3, "I'm a string", True, Duration("12M"), DateTime("2024-07-25T12:00:00")),
            1: (
                Array([1.0]),
                TimePattern(["M1-12"], [1.0]),
                TimeSeriesFixedResolution("2024-07-25T12:00:00", "1h", [1.0], False, False),
            ),
            2: (Map(["a", "b"], [1.0, Map(["c"], [2.0])]),),
        }
        for rank, xs in values.items():
            for x in xs:
                with self.subTest(value=x):
                    assert_test(x, rank)


if __name__ == "__main__":
    unittest.main()
