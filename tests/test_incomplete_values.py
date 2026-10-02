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
from datetime import datetime
import json
from dateutil.relativedelta import relativedelta
import pyarrow
from spinedb_api import (
    Array,
    DateTime,
    Duration,
    Map,
    TimePattern,
    TimeSeriesFixedResolution,
    TimeSeriesVariableResolution,
    duration_to_relativedelta,
    to_database,
)
from spinedb_api.arrow_value import with_column_as_time_period, with_column_as_time_stamps
from spinedb_api.incomplete_values import (
    dump_db_value,
    from_database_to_dimension_count,
    join_value_and_type,
    split_value_and_type,
    try_parse_json,
)
from spinedb_api.parameter_value import (
    BOOLEAN_VALUE_TYPE,
    FLOAT_VALUE_TYPE,
    STRING_VALUE_TYPE,
    TimeSeries,
    from_database,
    to_list,
)


class TestDumpDbValue:
    def test_legacy_values(self):
        values = [
            Duration("23 minutes"),
            DateTime("2025-09-04T15:57"),
            Array([Duration("5Y")]),
            TimePattern(["M1-12"], [2.3]),
            TimeSeriesFixedResolution("2025-09-04T15:59", "6h", [2.3], ignore_year=False, repeat=True),
            TimeSeriesVariableResolution(
                ["2025-09-04T15:59", "2025-09-04T16:00"], [2.3, 3.2], ignore_year=True, repeat=False
            ),
            Map(["A", "B"], [2.3, 3.2]),
        ]
        for value in values:
            value_dict = value.to_dict()
            value_dict["type"] = value.TYPE
            assert dump_db_value(value_dict) == to_database(value)

    def test_scalars(self):
        values = ["a string", False, 2.3, 5.0, 23, None]
        types = ["str", "bool", "float", "float", "int", None]
        for value, value_type in zip(values, types):
            assert dump_db_value(value) == to_database(value)

    def test_record_batch(self):
        index_array = pyarrow.array(["a", "b"])
        value_array = pyarrow.array([2.3, 3.2])
        value = pyarrow.record_batch({"col_1": index_array, "value": value_array})
        assert dump_db_value(to_list(value)) == to_database(value)


class TestFromDatabaseToDimensionCount:
    def test_zero_dimensional_types(self):
        assert from_database_to_dimension_count(*to_database(None)) == 0
        assert from_database_to_dimension_count(*to_database("a string")) == 0
        assert from_database_to_dimension_count(*to_database(5)) == 0
        assert from_database_to_dimension_count(*to_database(2.3)) == 0
        assert from_database_to_dimension_count(*to_database(True)) == 0
        assert (
            from_database_to_dimension_count(*to_database(datetime(year=2025, month=8, day=25, hour=15, minute=15)))
            == 0
        )
        assert from_database_to_dimension_count(*to_database(duration_to_relativedelta("5 years"))) == 0

    def test_one_dimensional_types(self):
        assert from_database_to_dimension_count(*to_database(Array([2.3, 3.2]))) == 1
        assert from_database_to_dimension_count(*to_database(TimePattern(["WD1-7"], [23.0]))) == 1
        assert (
            from_database_to_dimension_count(
                *to_database(
                    TimeSeriesFixedResolution("2025-08-25T15:15", "1h", [2.3], ignore_year=False, repeat=False)
                )
            )
            == 1
        )
        assert (
            from_database_to_dimension_count(
                *to_database(
                    TimeSeriesVariableResolution(
                        ["2025-08-25T15:15", "2025-08-25T16:15"], [2.3, 3.2], ignore_year=False, repeat=False
                    )
                )
            )
            == 1
        )

    def test_variable_dimensional_types(self):
        assert from_database_to_dimension_count(*to_database(Map(["a"], [2.3]))) == 1
        assert from_database_to_dimension_count(*to_database(Map(["a"], [Map(["A"], [2.3])]))) == 2
        indexes_1 = pyarrow.array(["A", "B"])
        values = pyarrow.array([2.3, 3.3])
        record_batch = pyarrow.record_batch({"category": indexes_1, "value": values})
        assert from_database_to_dimension_count(*to_database(record_batch)) == 1
        indexes_2 = pyarrow.array(["a", "a"])
        record_batch = pyarrow.record_batch({"category": indexes_1, "subcategory": indexes_2, "value": values})
        assert from_database_to_dimension_count(*to_database(record_batch)) == 2


class TestJoinValueAndType:
    def test_correctness(self):
        blob, value_type = to_database(2.3)
        assert json.loads(join_value_and_type(*to_database(2.3))) == [2.3, value_type]
        blob, value_type = to_database("I'm just a string")
        assert json.loads(join_value_and_type(*to_database("I'm just a string"))) == ["I'm just a string", value_type]
        blob, value_type = to_database(datetime(year=2026, month=9, day=30, hour=14, minute=1))
        assert join_value_and_type(blob, value_type) == json.dumps(["2026-09-30T14:01:00", "date_time"])
        assert from_database(*split_value_and_type(join_value_and_type(*to_database(2.3)))) == 2.3
        assert (
            from_database(*split_value_and_type(join_value_and_type(*to_database("I'm just a string"))))
            == "I'm just a string"
        )
        assert from_database(
            *split_value_and_type(
                join_value_and_type(*to_database(datetime(year=2026, month=9, day=30, hour=14, minute=1)))
            )
        ) == DateTime("2026-09-30T14:01")


class TestSplitValueAndType:
    def test_with_join_value_and_type(self):
        blob, value_type = to_database(2.3)
        assert split_value_and_type(join_value_and_type(blob, value_type)) == to_database(2.3)
        blob, value_type = to_database(DateTime("2025-09-04T16:20"))
        assert split_value_and_type(join_value_and_type(blob, value_type)) == (blob, value_type)


class TestTryParseJson:
    def test_things_that_end_up_as_strings(self):
        assert try_parse_json("a random string") == (json.dumps("a random string").encode(), STRING_VALUE_TYPE)
        random_json_dict = json.dumps({"some": "data"})
        assert try_parse_json(random_json_dict) == (json.dumps(random_json_dict).encode(), STRING_VALUE_TYPE)
        random_json_list = json.dumps([1, 2, 3])
        assert try_parse_json(random_json_list) == (json.dumps(random_json_list).encode(), STRING_VALUE_TYPE)

    def test_legacy_parameter_values(self):
        assert try_parse_json(json.dumps("a string")) == (json.dumps("a string").encode(), STRING_VALUE_TYPE)
        assert try_parse_json(json.dumps(2.3)) == (json.dumps(2.3).encode(), FLOAT_VALUE_TYPE)
        assert try_parse_json(json.dumps(False)) == ("false".encode(), BOOLEAN_VALUE_TYPE)
        date_time = DateTime("2026-10-02T11:11").to_dict()
        date_time["type"] = DateTime.TYPE
        assert try_parse_json(json.dumps(date_time)) == (b'"2026-10-02T11:11:00"', DateTime.TYPE)
        duration = Duration("13h").to_dict()
        duration["type"] = Duration.TYPE
        assert try_parse_json(json.dumps(duration)) == (b'"PT13H"', Duration.TYPE)
        time_pattern = TimePattern(["D1-7"], [2.3]).to_dict()
        time_pattern["type"] = TimePattern.TYPE
        expected_value = with_column_as_time_period(
            pyarrow.record_batch(
                {
                    "p": pyarrow.array(["D1-7"]),
                    "value": pyarrow.array([2.3]),
                }
            ),
            0,
        )
        assert try_parse_json(json.dumps(time_pattern)) == (to_database(expected_value)[0], TimePattern.TYPE)
        array = Array([1.1, 2.2]).to_dict()
        array["type"] = Array.TYPE
        expected_value = pyarrow.record_batch(
            {
                "i": [1, 2],
                "value": [1.1, 2.2],
            }
        )
        assert try_parse_json(json.dumps(array)) == (to_database(expected_value)[0], Array.TYPE)
        time_series = TimeSeriesFixedResolution(
            "2026-10-02T12:30", "2h", [1.1, 2.2], ignore_year=False, repeat=False
        ).to_dict()
        time_series["type"] = TimeSeries.TYPE
        expected_value = with_column_as_time_stamps(
            pyarrow.record_batch(
                {
                    "t": [
                        datetime(year=2026, month=10, day=2, hour=12, minute=30),
                        datetime(year=2026, month=10, day=2, hour=14, minute=30),
                    ],
                    "value": [1.1, 2.2],
                }
            ),
            0,
            ignore_year=False,
            repeat=False,
        )
        assert try_parse_json(json.dumps(time_series)) == (to_database(expected_value)[0], TimeSeries.TYPE)
        map_value = Map(["a", "b"], [1.1, 2.2]).to_dict()
        map_value["type"] = Map.TYPE
        expected_value = pyarrow.record_batch(
            {
                "col_1": ["a", "b"],
                "value": [1.1, 2.2],
            }
        )
        assert try_parse_json(json.dumps(map_value)) == (to_database(expected_value)[0], Map.TYPE)
