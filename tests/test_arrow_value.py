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
import json
import unittest
import pyarrow
import pytest
from spinedb_api import SpineDBAPIError, arrow_value, parameter_value
from spinedb_api.arrow_value import _month_day_nano_interval_to_duration, with_column_as_time_period
from spinedb_api.compat.converters import parse_duration


class TestFromDatabaseForArrays(unittest.TestCase):
    def test_empty_array(self):
        value, value_type = parameter_value.to_database(parameter_value.Array([]))
        record_batch = arrow_value.from_database(value, value_type)
        self.assertEqual(len(record_batch), 0)
        self.assertEqual(record_batch.column_names, ["i", "value"])
        self.assertEqual(record_batch.column("i").type, pyarrow.int64())
        self.assertEqual(record_batch.column("value").type, pyarrow.float64())

    def test_floats_with_index_name(self):
        value, value_type = parameter_value.to_database(parameter_value.Array([2.3], index_name="my index"))
        record_batch = arrow_value.from_database(value, value_type)
        self.assertEqual(len(record_batch), 1)
        self.assertEqual(record_batch.column_names, ["my index", "value"])
        indices = record_batch.column("my index")
        self.assertEqual(indices.type, pyarrow.int64())
        self.assertEqual(indices, pyarrow.array([0]))
        ys = record_batch.column("value")
        self.assertEqual(ys.type, pyarrow.float64())
        self.assertEqual(ys, pyarrow.array([2.3]))

    def test_date_times_with_index_name(self):
        value, value_type = parameter_value.to_database(
            parameter_value.Array([parameter_value.DateTime("2024-09-02T05:51:00")], index_name="my index")
        )
        record_batch = arrow_value.from_database(value, value_type)
        self.assertEqual(len(record_batch), 1)
        self.assertEqual(record_batch.column_names, ["my index", "value"])
        indices = record_batch.column("my index")
        self.assertEqual(indices.type, pyarrow.int64())
        self.assertEqual(indices, pyarrow.array([0]))
        ys = record_batch.column("value")
        self.assertEqual(ys.type, pyarrow.timestamp("s"))
        self.assertEqual(ys.tolist(), [datetime.datetime(2024, 9, 2, 5, 51)])


class TestFromDatabaseForMaps(unittest.TestCase):
    def test_empty_map(self):
        value, value_type = parameter_value.to_database(parameter_value.Map([], [], str))
        map_ = arrow_value.from_database(value, value_type)
        self.assertEqual(len(map_), 0)
        self.assertEqual(map_.column_names, ["col_1", "value"])
        self.assertEqual(map_.column("col_1").type, pyarrow.string())
        self.assertEqual(map_.column("value").type, pyarrow.null())

    def test_string_to_string_map_with_index_name(self):
        value, value_type = parameter_value.to_database(parameter_value.Map(["key"], ["value"], index_name="Keys"))
        map_ = arrow_value.from_database(value, value_type)
        self.assertEqual(len(map_), 1)
        self.assertEqual(map_.column_names, ["Keys", "value"])
        self.assertEqual(map_.column("Keys").type, pyarrow.string())
        self.assertEqual(map_.column("Keys")[0].as_py(), "key")
        self.assertEqual(map_.column("value").type, pyarrow.string())
        self.assertEqual(map_.column("value")[0].as_py(), "value")

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
        self.assertEqual(map_.column_names, ["timestamps", "value"])
        self.assertEqual(map_.column("timestamps").type, pyarrow.timestamp("s"))
        self.assertEqual(
            map_.column("timestamps").to_pylist(),
            [datetime.datetime(2024, 2, 9, 10), datetime.datetime(2024, 2, 9, 11)],
        )
        self.assertEqual(
            map_.column("value").type,
            pyarrow.dense_union([pyarrow.field("str", pyarrow.string()), pyarrow.field("float", pyarrow.float64())]),
        )
        self.assertEqual(map_.column("value").to_pylist(), ["value", 2.3])

    def test_nested_maps(self):
        string_map = parameter_value.Map([11.0], ["value"], index_name="nested index")
        float_map = parameter_value.Map(["key"], [22.0], index_name="nested index")
        value, value_type = parameter_value.to_database(
            parameter_value.Map(["strings", "floats"], [string_map, float_map], index_name="main index")
        )
        map_ = arrow_value.from_database(value, value_type)
        self.assertEqual(len(map_), 2)
        self.assertEqual(map_.column_names, ["main index", "nested index", "value"])
        self.assertEqual(map_.column("main index").type, pyarrow.string())
        self.assertEqual(map_.column("main index").to_pylist(), ["strings", "floats"])
        self.assertEqual(
            map_.column("nested index").type,
            pyarrow.dense_union([pyarrow.field("float", pyarrow.float64()), pyarrow.field("str", pyarrow.string())]),
        )
        self.assertEqual(map_.column("nested index").to_pylist(), [11.0, "key"])
        self.assertEqual(
            map_.column("value").type,
            pyarrow.dense_union([pyarrow.field("str", pyarrow.string()), pyarrow.field("float", pyarrow.float64())]),
        )
        self.assertEqual(map_.column("value").to_pylist(), ["value", 22.0])

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
        self.assertEqual(map_.column_names, ["main index", "nested index", "col_3", "t", "value"])
        self.assertEqual(map_.column("main index").type, pyarrow.string())
        self.assertEqual(
            map_.column("main index").to_pylist(),
            ["not nested", "strings", "time series", "time series", "time series", "floats"],
        )
        self.assertEqual(map_.column("nested index").to_pylist(), [None, 11.0, "ts", "ts", "no ts", "key"])
        self.assertEqual(
            map_.column("col_3").to_pylist(),
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
            map_.column("t").to_pylist(),
            [
                None,
                None,
                datetime.datetime.fromisoformat("2025-02-26T09:00:00"),
                datetime.datetime.fromisoformat("2025-02-26T10:00:00"),
                None,
                None,
            ],
        )
        self.assertEqual(map_.column("value").to_pylist(), ["none", "value", 2.3, 23.0, "empty", 22.0])

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
        self.assertEqual(map_.column_names, ["main index", "nested index", "col_3", "value"])
        self.assertEqual(map_.column("main index").type, pyarrow.string())
        self.assertEqual(
            map_.column("main index").to_pylist(),
            ["not nested", "strings", "date times", "date times", "date times", "floats"],
        )
        self.assertEqual(
            map_.column("nested index").to_pylist(), [None, 11.0, "date time", "more date time", "non nested", "key"]
        )
        self.assertEqual(
            map_.column("col_3").to_pylist(),
            [None, None, "time of my life", datetime.datetime.fromisoformat("2024-02-26T17:45:00"), None, None],
        )
        self.assertEqual(
            map_.column("value").to_pylist(),
            ["none", "value", datetime.datetime.fromisoformat("2024-02-26T16:45:00"), "future", "empty", 22.0],
        )


class TestFromDatabaseForTimeSeries(unittest.TestCase):
    def test_fixed_resolution_series(self):
        value, value_type = parameter_value.to_database(
            parameter_value.TimeSeriesFixedResolution(
                "2025-02-05T09:59", "15m", [1.1, 1.2], ignore_year=False, repeat=False
            )
        )
        fixed_resolution = arrow_value.from_database(value, value_type)
        self.assertEqual(fixed_resolution.column_names, ["t", "value"])
        self.assertEqual(
            fixed_resolution.column("t").to_pylist(),
            [datetime.datetime(2025, 2, 5, 9, 59), datetime.datetime(2025, 2, 5, 10, 14)],
        )
        self.assertEqual(fixed_resolution.schema.field("t").metadata, {b"ignore_year": b"false", b"repeat": b"false"})
        self.assertEqual(fixed_resolution.column("value").to_pylist(), [1.1, 1.2])

    def test_ignore_year(self):
        value, value_type = parameter_value.to_database(
            parameter_value.TimeSeriesFixedResolution(
                "2025-02-05T09:59", "15m", [1.1, 1.2], ignore_year=True, repeat=False
            )
        )
        fixed_resolution = arrow_value.from_database(value, value_type)
        self.assertEqual(fixed_resolution.schema.field("t").metadata, {b"ignore_year": b"true", b"repeat": b"false"})

    def test_repeat(self):
        value, value_type = parameter_value.to_database(
            parameter_value.TimeSeriesFixedResolution(
                "2025-02-05T09:59", "15m", [1.1, 1.2], ignore_year=False, repeat=True
            )
        )
        fixed_resolution = arrow_value.from_database(value, value_type)
        self.assertEqual(fixed_resolution.schema.field("t").metadata, {b"ignore_year": b"false", b"repeat": b"true"})

    def test_variable_resolution_series(self):
        value, value_type = parameter_value.to_database(
            parameter_value.TimeSeriesVariableResolution(
                ["2025-02-05T09:59", "2025-02-05T10:14", "2025-02-05T11:31"],
                [1.1, 1.2, 1.3],
                ignore_year=False,
                repeat=False,
            )
        )
        fixed_resolution = arrow_value.from_database(value, value_type)
        self.assertEqual(fixed_resolution.column_names, ["t", "value"])
        self.assertEqual(
            fixed_resolution.column("t").to_pylist(),
            [
                datetime.datetime(2025, 2, 5, 9, 59),
                datetime.datetime(2025, 2, 5, 10, 14),
                datetime.datetime(2025, 2, 5, 11, 31),
            ],
        )
        self.assertEqual(fixed_resolution.schema.field("t").metadata, {b"ignore_year": b"false", b"repeat": b"false"})
        self.assertEqual(fixed_resolution.column("value").to_pylist(), [1.1, 1.2, 1.3])


class TestToDatabaseForRecordBatches:
    def test_strings_as_run_end_encoded(self):
        index_array = pyarrow.RunEndEncodedArray.from_arrays([3, 5], ["A", "B"])
        value_array = pyarrow.RunEndEncodedArray.from_arrays([2, 5], ["a", "b"])
        record_batch = pyarrow.RecordBatch.from_arrays([index_array, value_array], ["Indexes", "Values"])
        blob, value_type = arrow_value.to_database(record_batch)
        deserialized = arrow_value.from_database(blob, value_type)
        assert deserialized == record_batch
        assert deserialized.schema.metadata == record_batch.schema.metadata

    def test_date_times_as_run_end_encoded_index(self):
        index_array = pyarrow.RunEndEncodedArray.from_arrays(
            [3, 5],
            [
                datetime.datetime(year=2025, month=7, day=24, hour=11, minute=41),
                datetime.datetime(year=2025, month=7, day=24, hour=18, minute=41),
            ],
        )
        value_array = pyarrow.RunEndEncodedArray.from_arrays([2, 5], ["a", "b"])
        record_batch = pyarrow.RecordBatch.from_arrays([index_array, value_array], ["Indexes", "Values"])
        blob, value_type = arrow_value.to_database(record_batch)
        deserialized = arrow_value.from_database(blob, value_type)
        assert deserialized == record_batch
        assert deserialized.schema.metadata == record_batch.schema.metadata

    def test_duration_as_run_end_encoded(self):
        index_array = pyarrow.RunEndEncodedArray.from_arrays([3, 5], [parse_duration("PT30M"), parse_duration("PT45M")])
        value_array = pyarrow.RunEndEncodedArray.from_arrays([2, 5], [parse_duration("P3Y"), parse_duration("P2Y")])
        record_batch = pyarrow.RecordBatch.from_arrays([index_array, value_array], ["Indexes", "Values"])
        blob, value_type = arrow_value.to_database(record_batch)
        deserialized = arrow_value.from_database(blob, value_type)
        assert deserialized == record_batch
        assert deserialized.schema.metadata == record_batch.schema.metadata

    def test_dictionary(self):
        index_array = pyarrow.DictionaryArray.from_arrays([0, 1, 0], ["A", "B"])
        value_array = pyarrow.DictionaryArray.from_arrays([1, 0, 2], [2.3, 3.2, -2.3])
        record_batch = pyarrow.RecordBatch.from_arrays([index_array, value_array], ["Indexes", "Values"])
        blob, value_type = arrow_value.to_database(record_batch)
        deserialized = arrow_value.from_database(blob, value_type)
        assert deserialized == record_batch
        assert deserialized.schema.metadata == record_batch.schema.metadata

    def test_date_times_in_dictionary(self):
        index_array = pyarrow.DictionaryArray.from_arrays(
            [0, 1, 0],
            [
                datetime.datetime(year=2025, month=7, day=24, hour=13, minute=20),
                datetime.datetime(year=2025, month=7, day=24, hour=13, minute=20),
            ],
        )
        value_array = pyarrow.DictionaryArray.from_arrays([1, 0, 1], [True, False])
        record_batch = pyarrow.RecordBatch.from_arrays([index_array, value_array], ["Indexes", "Values"])
        blob, value_type = arrow_value.to_database(record_batch)
        deserialized = arrow_value.from_database(blob, value_type)
        assert deserialized == record_batch
        assert deserialized.schema.metadata == record_batch.schema.metadata

    def test_durations_in_dictionary(self):
        index_array = pyarrow.DictionaryArray.from_arrays([0, 1, 0], [parse_duration("P23D"), parse_duration("P5M")])
        value_array = pyarrow.DictionaryArray.from_arrays([2, 1, 0], ["a", "b", "c"])
        record_batch = pyarrow.RecordBatch.from_arrays([index_array, value_array], ["Indexes", "Values"])
        blob, value_type = arrow_value.to_database(record_batch)
        deserialized = arrow_value.from_database(blob, value_type)
        assert deserialized == record_batch
        assert deserialized.schema.metadata == record_batch.schema.metadata

    def test_union(self):
        index_array = pyarrow.array(["integer", "float_generic", "float_int_like", "string", "boolean", "duration"])
        int_array = pyarrow.array([23])
        float_array = pyarrow.array([2.3, 5.0])
        str_array = pyarrow.array(["A"])
        boolean_array = pyarrow.array([True])
        duration_array = pyarrow.array([parse_duration("PT5H")])
        value_type_array = pyarrow.array([0, 1, 1, 2, 3, 4], type=pyarrow.int8())
        value_index_array = pyarrow.array([0, 0, 1, 0, 0, 0], type=pyarrow.int32())
        value_array = pyarrow.UnionArray.from_dense(
            value_type_array, value_index_array, [int_array, float_array, str_array, boolean_array, duration_array]
        )
        record_batch = pyarrow.RecordBatch.from_arrays([index_array, value_array], ["Indexes", "Values"])
        blob, value_type = arrow_value.to_database(record_batch)
        deserialized = arrow_value.from_database(blob, value_type)
        assert deserialized == record_batch
        assert deserialized.schema.metadata == record_batch.schema.metadata

    def test_union_as_index_raises(self):
        int_array = pyarrow.array([23])
        value_type_array = pyarrow.array([0], type=pyarrow.int8())
        value_index_array = pyarrow.array([0], type=pyarrow.int32())
        value_array = pyarrow.UnionArray.from_dense(value_type_array, value_index_array, [int_array])
        record_batch = pyarrow.RecordBatch.from_arrays([value_array, value_array], ["Indexes", "Values"])
        with pytest.raises(SpineDBAPIError, match="union array cannot be index"):
            arrow_value.to_database(record_batch)

    def test_float(self):
        index_array = pyarrow.array([1.1, 2.2])
        value_array = pyarrow.array([2.3, 3.2])
        record_batch = pyarrow.RecordBatch.from_arrays([index_array, value_array], ["Indexes", "Values"])
        blob, value_type = arrow_value.to_database(record_batch)
        deserialized = arrow_value.from_database(blob, value_type)
        assert deserialized == record_batch
        assert deserialized.schema.metadata == record_batch.schema.metadata

    def test_str(self):
        index_array = pyarrow.array(["T01", "T02"])
        value_array = pyarrow.array(["high", "low"])
        record_batch = pyarrow.RecordBatch.from_arrays([index_array, value_array], ["Indexes", "Values"])
        blob, value_type = arrow_value.to_database(record_batch)
        deserialized = arrow_value.from_database(blob, value_type)
        assert deserialized == record_batch
        assert deserialized.schema.metadata == record_batch.schema.metadata

    def test_int(self):
        index_array = pyarrow.array([23, 55])
        value_array = pyarrow.array([-2, -4])
        record_batch = pyarrow.RecordBatch.from_arrays([index_array, value_array], ["Indexes", "Values"])
        blob, value_type = arrow_value.to_database(record_batch)
        deserialized = arrow_value.from_database(blob, value_type)
        assert deserialized == record_batch
        assert deserialized.schema.metadata == record_batch.schema.metadata

    def test_date_time(self):
        index_array = pyarrow.array([datetime.datetime(year=2025, month=7, day=21, hour=15, minute=30)])
        value_array = pyarrow.array([2.3])
        record_batch = pyarrow.RecordBatch.from_arrays([index_array, value_array], ["Indexes", "Values"])
        blob, value_type = arrow_value.to_database(record_batch)
        deserialized = arrow_value.from_database(blob, value_type)
        assert deserialized == record_batch
        assert deserialized.schema.metadata == record_batch.schema.metadata

    def test_duration(self):
        index_array = pyarrow.array([parse_duration("P3D")])
        value_array = pyarrow.array([parse_duration("PT5H")])
        record_batch = pyarrow.RecordBatch.from_arrays([index_array, value_array], ["Indexes", "Values"])
        blob, value_type = arrow_value.to_database(record_batch)
        deserialized = arrow_value.from_database(blob, value_type)
        assert deserialized == record_batch
        assert deserialized.schema.metadata == record_batch.schema.metadata

    def test_time_pattern(self):
        index_array = pyarrow.array(["M1-4,M9-12", "M5-8"])
        value_array = pyarrow.array([3.0, -2.0])
        record_batch = pyarrow.RecordBatch.from_arrays([index_array, value_array], ["Indexes", "Values"])
        record_batch = with_column_as_time_period(record_batch, "Indexes")
        blob, value_type = arrow_value.to_database(record_batch)
        deserialized = arrow_value.from_database(blob, value_type)
        assert deserialized == record_batch
        assert deserialized.schema.metadata == record_batch.schema.metadata

    def test_bool(self):
        index_array = pyarrow.array(["T001", "T002"])
        value_array = pyarrow.array([False, True])
        record_batch = pyarrow.RecordBatch.from_arrays([index_array, value_array], ["Indexes", "Values"])
        blob, value_type = arrow_value.to_database(record_batch)
        deserialized = arrow_value.from_database(blob, value_type)
        assert deserialized == record_batch
        assert deserialized.schema.metadata == record_batch.schema.metadata

    def test_bool_as_index_raises(self):
        index_array = pyarrow.array([True, False])
        value_array = pyarrow.array([False, True])
        record_batch = pyarrow.RecordBatch.from_arrays([index_array, value_array], ["Indexes", "Values"])
        with pytest.raises(SpineDBAPIError, match="boolean array cannot be index"):
            arrow_value.to_database(record_batch)


class TestToDatabaseForMaps:
    def test_basic_map(self):
        map_value = parameter_value.Map(["A", "B"], [2.3, 3.2])
        blob, value_type = arrow_value.to_database(map_value)
        deserialized = arrow_value.from_database(blob, value_type)
        index_array = pyarrow.array(["A", "B"])
        value_array = pyarrow.array([2.3, 3.2])
        expected = pyarrow.RecordBatch.from_arrays([index_array, value_array], ["col_0", "value"])
        assert deserialized.to_pydict() == expected.to_pydict()


class TestWithColumnAsTimePeriod:
    def test_column_given_by_name(self):
        column = pyarrow.array(["M1-4,M9-12", "M5-8"])
        record_batch = pyarrow.record_batch({"data": column})
        as_time_period = with_column_as_time_period(record_batch, "data")
        column_metadata = as_time_period.schema.metadata["data".encode()]
        assert json.loads(column_metadata) == {"format": "time_period"}

    def test_column_given_by_index(self):
        column = pyarrow.array(["M1-4,M9-12", "M5-8"])
        record_batch = pyarrow.record_batch({"data": column})
        as_time_period = with_column_as_time_period(record_batch, 0)
        column_metadata = as_time_period.schema.metadata["data".encode()]
        assert json.loads(column_metadata) == {"format": "time_period"}

    def test_raises_when_column_data_is_invalid(self):
        column = pyarrow.array(["gibberish"])
        record_batch = pyarrow.record_batch({"data": column})
        with pytest.raises(
            SpineDBAPIError, match="^Invalid interval gibberish, it should start with either Y, M, D, WD, h, m, or s.$"
        ):
            with_column_as_time_period(record_batch, 0)


class TestMonthDayNanoIntervalToDuration:
    def test_seconds(self):
        durations = ["PT0S", "PT23S", "PT120S", "PT145S", "PT7200S", "PT7310S", "PT86400S", "PT86460S"]
        intervals = pyarrow.array([parse_duration(d) for d in durations])
        converted = [_month_day_nano_interval_to_duration(dt) for dt in intervals]
        assert converted == ["P0D", "PT23S", "PT2M", "PT2M25S", "PT2H", "PT2H1M50S", "P1D", "P1DT1M"]

    def test_days(self):
        durations = ["P0D", "P12D", "P1DT4H"]
        intervals = pyarrow.array([parse_duration(d) for d in durations])
        converted = [_month_day_nano_interval_to_duration(dt) for dt in intervals]
        assert converted == ["P0D", "P12D", "P1DT4H"]

    def test_months(self):
        durations = ["P0M", "P5M", "P12M", "P17M"]
        intervals = pyarrow.array([parse_duration(d) for d in durations])
        converted = [_month_day_nano_interval_to_duration(dt) for dt in intervals]
        assert converted == ["P0D", "P5M", "P1Y", "P1Y5M"]
