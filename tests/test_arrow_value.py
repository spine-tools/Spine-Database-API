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
""" Unit tests for the `arrow_value` module. """
import datetime
import unittest
import pyarrow
from spinedb_api import arrow_value, parameter_value


class DatabaseUsingTest(unittest.TestCase):
    def _assert_success(self, result):
        item, error = result
        self.assertIsNone(error)
        return item


class TestFromDatabaseForArrays(unittest.TestCase):
    def test_empty_array(self):
        value, value_type = parameter_value.to_database(parameter_value.Array([]))
        array = arrow_value.from_database(value, value_type)
        self.assertEqual(len(array), 0)
        self.assertEqual(array.type.num_fields, 2)
        self.assertEqual([field.name for field in array.type], ["i", "y"])
        self.assertEqual(array.type["i"].type, pyarrow.int64())
        self.assertEqual(array.type["y"].type, pyarrow.float64())

    def test_floats_with_index_name(self):
        value, value_type = parameter_value.to_database(parameter_value.Array([2.3], index_name="my index"))
        array = arrow_value.from_database(value, value_type)
        self.assertEqual(len(array), 1)
        self.assertEqual([field.name for field in array.type], ["my index", "y"])
        indices = array.field("my index")
        self.assertEqual(indices.type, pyarrow.int64())
        self.assertEqual(indices, pyarrow.array([0]))
        ys = array.field("y")
        self.assertEqual(ys.type, pyarrow.float64())
        self.assertEqual(ys, pyarrow.array([2.3]))

    def test_date_times_with_index_name(self):
        value, value_type = parameter_value.to_database(
            parameter_value.Array([parameter_value.DateTime("2024-09-02T05:51:00")], index_name="my index")
        )
        array = arrow_value.from_database(value, value_type)
        self.assertEqual(len(array), 1)
        self.assertEqual([field.name for field in array.type], ["my index", "y"])
        indices = array.field("my index")
        self.assertEqual(indices.type, pyarrow.int64())
        self.assertEqual(indices, pyarrow.array([0]))
        ys = array.field("y")
        self.assertEqual(ys.type, pyarrow.timestamp("s"))
        self.assertEqual(ys.tolist(), [datetime.datetime(2024, 9, 2, 5, 51)])


class TestFromDatabaseForMaps(unittest.TestCase):
    def test_empty_map(self):
        value, value_type = parameter_value.to_database(parameter_value.Map([], [], str))
        map_ = arrow_value.from_database(value, value_type)
        self.assertEqual(len(map_), 0)
        self.assertEqual([field.name for field in map_.type], ["x", "y"])
        self.assertEqual(map_.field("x").type, pyarrow.string())
        self.assertEqual(map_.field("y").type, pyarrow.null())

    def test_string_to_string_map_with_index_name(self):
        value, value_type = parameter_value.to_database(parameter_value.Map(["key"], ["value"], index_name="Keys"))
        map_ = arrow_value.from_database(value, value_type)
        self.assertEqual(len(map_), 1)
        self.assertEqual([field.name for field in map_.type], ["Keys", "y"])
        self.assertEqual(map_.field("Keys").type, pyarrow.string())
        self.assertEqual(map_.field("Keys")[0].as_py(), "key")
        self.assertEqual(map_.field("y").type, pyarrow.string())
        self.assertEqual(map_.field("y")[0].as_py(), "value")

    def test_date_time_to_different_simple_types_map_with_index_name(self):
        value, value_type = parameter_value.to_database(
            parameter_value.Map(
                [parameter_value.DateTime("2024-02-09T10:00"), parameter_value.DateTime("2024-02-09T11:00")],
                ["value", 2.3],
                index_name="timestamps",
            )
        )
        map_ = arrow_value.from_database(value, value_type)
        self.assertEqual(len(map_), 2)
        self.assertEqual([field.name for field in map_.type], ["timestamps", "y"])
        self.assertEqual(map_.field("timestamps").type, pyarrow.timestamp("s"))
        self.assertEqual(
            map_.field("timestamps").to_pylist(), [datetime.datetime(2024, 2, 9, 10), datetime.datetime(2024, 2, 9, 11)]
        )
        self.assertEqual(
            map_.field("y").type,
            pyarrow.dense_union([pyarrow.field("str", pyarrow.string()), pyarrow.field("float", pyarrow.float64())]),
        )
        self.assertEqual(map_.field("y").to_pylist(), ["value", 2.3])

    def test_nested_maps(self):
        string_map = parameter_value.Map([11.0], ["value"], index_name="nested index")
        float_map = parameter_value.Map(["key"], [22.0], index_name="nested index")
        value, value_type = parameter_value.to_database(
            parameter_value.Map(["strings", "floats"], [string_map, float_map], index_name="main index")
        )
        map_ = arrow_value.from_database(value, value_type)
        self.assertEqual(len(map_), 2)
        self.assertEqual([field.name for field in map_.type], ["main index", "nested index", "y"])
        self.assertEqual(map_.field("main index").type, pyarrow.string())
        self.assertEqual(map_.field("main index").to_pylist(), ["strings", "floats"])
        self.assertEqual(
            map_.field("nested index").type,
            pyarrow.dense_union([pyarrow.field("float", pyarrow.float64()), pyarrow.field("str", pyarrow.string())]),
        )
        self.assertEqual(map_.field("nested index").to_pylist(), [11.0, "key"])
        self.assertEqual(
            map_.field("y").type,
            pyarrow.dense_union([pyarrow.field("str", pyarrow.string()), pyarrow.field("float", pyarrow.float64())]),
        )
        self.assertEqual(map_.field("y").to_pylist(), ["value", 22.0])

    def test_unevenly_nested_map_with_fixed_resolution_time_series(self):
        string_map = parameter_value.Map([11.0], ["value"], index_name="nested index")
        float_map = parameter_value.Map(["key"], [22.0], index_name="nested index")
        time_series = parameter_value.TimeSeriesFixedResolution(
            "2025-02-26T09:00:00", "1h", [2.3, 23.0], ignore_year=False, repeat=False
        )
        time_series_map = parameter_value.Map([parameter_value.DateTime("2024-02-26T16:45:00")], [time_series])
        nested_time_series_map = parameter_value.Map(
            ["ts", "no ts"], [time_series_map, "empty"], index_name="nested index"
        )
        value, value_type = parameter_value.to_database(
            parameter_value.Map(
                ["not nested", "strings", "time series", "floats"],
                ["none", string_map, nested_time_series_map, float_map],
                index_name="main index",
            )
        )
        map_ = arrow_value.from_database(value, value_type)
        self.assertEqual(len(map_), 6)
        self.assertEqual([field.name for field in map_.type], ["main index", "nested index", "x", "t", "y"])
        self.assertEqual(map_.field("main index").type, pyarrow.string())
        self.assertEqual(
            map_.field("main index").to_pylist(),
            ["not nested", "strings", "time series", "time series", "time series", "floats"],
        )
        self.assertEqual(map_.field("nested index").to_pylist(), [None, 11.0, "ts", "ts", "no ts", "key"])
        self.assertEqual(
            map_.field("x").to_pylist(),
            [
                None,
                None,
                datetime.datetime.fromisoformat("2024-02-26T16:45:00"),
                datetime.datetime.fromisoformat("2024-02-26T16:45:00"),
                None,
                None,
            ],
        )
        self.assertEqual(
            map_.field("t").to_pylist(),
            [
                None,
                None,
                datetime.datetime.fromisoformat("2025-02-26T09:00:00"),
                datetime.datetime.fromisoformat("2025-02-26T10:00:00"),
                None,
                None,
            ],
        )
        self.assertEqual(map_.field("y").to_pylist(), ["none", "value", 2.3, 23.0, "empty", 22.0])

    def test_unevenly_nested_map(self):
        string_map = parameter_value.Map([11.0], ["value"], index_name="nested index")
        float_map = parameter_value.Map(["key"], [22.0], index_name="nested index")
        datetime_map = parameter_value.Map(["time of my life"], [parameter_value.DateTime("2024-02-26T16:45:00")])
        another_string_map = parameter_value.Map([parameter_value.DateTime("2024-02-26T17:45:00")], ["future"])
        nested_map = parameter_value.Map(
            ["date time", "more date time", "non nested"],
            [datetime_map, another_string_map, "empty"],
            index_name="nested index",
        )
        value, value_type = parameter_value.to_database(
            parameter_value.Map(
                ["not nested", "strings", "date times", "floats"],
                ["none", string_map, nested_map, float_map],
                index_name="main index",
            )
        )
        map_ = arrow_value.from_database(value, value_type)
        self.assertEqual(len(map_), 6)
        self.assertEqual([field.name for field in map_.type], ["main index", "nested index", "x", "y"])
        self.assertEqual(map_.field("main index").type, pyarrow.string())
        self.assertEqual(
            map_.field("main index").to_pylist(),
            ["not nested", "strings", "date times", "date times", "date times", "floats"],
        )
        self.assertEqual(
            map_.field("nested index").to_pylist(), [None, 11.0, "date time", "more date time", "non nested", "key"]
        )
        self.assertEqual(
            map_.field("x").to_pylist(),
            [None, None, "time of my life", datetime.datetime.fromisoformat("2024-02-26T17:45:00"), None, None],
        )
        self.assertEqual(
            map_.field("y").to_pylist(),
            ["none", "value", datetime.datetime.fromisoformat("2024-02-26T16:45:00"), "future", "empty", 22.0],
        )


if __name__ == "__main__":
    unittest.main()
