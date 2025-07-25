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
import datetime
import pyarrow
import pytest
from spinedb_api import TimePattern, TimeSeriesFixedResolution, arrow_value
from spinedb_api.compat.data_transition import transition_data


class TestTransitionData:
    @pytest.mark.skip("data_transition needs to be fixed")
    def test_standard_time_pattern(self):
        value = TimePattern(["M1-3,M7-12", "M4-6"], [2.3, 3.2])
        old_blob, value_type = value.to_database()
        new_blob = transition_data(old_blob, value_type)
        deserialized = arrow_value.from_database(new_blob, value_type)
        time_period_array = pyarrow.array(["M1-3,M7-12", "M4-6"])
        value_array = pyarrow.array([2.3, 3.2])
        expected_table = pyarrow.record_batch({"col_1": time_period_array, "value": value_array})
        expected_table = arrow_value.with_column_as_time_period(expected_table, "col_1")
        assert deserialized == expected_table
        assert deserialized.schema.metadata == expected_table.schema.metadata

    @pytest.mark.skip("data_transition needs to be fixed")
    def test_time_pattern_with_index_name(self):
        value = TimePattern(["WD1-3"], [2.3], index_name="Seasons")
        old_blob, value_type = value.to_database()
        new_blob = transition_data(old_blob, value_type)
        deserialized = arrow_value.from_database(new_blob, value_type)
        time_period_array = pyarrow.array(["WD1-3"])
        value_array = pyarrow.array([2.3])
        expected_table = pyarrow.record_batch({"Seasons": time_period_array, "value": value_array})
        expected_table = arrow_value.with_column_as_time_period(expected_table, "Seasons")
        assert deserialized == expected_table
        assert deserialized.schema.metadata == expected_table.schema.metadata

    @pytest.mark.skip("data_transition needs to be fixed")
    def test_time_series_fixed_resolution(self):
        value = TimeSeriesFixedResolution("2025-07-25T09:15", "3h", [2.3, 5.0], ignore_year=True, repeat=True)
        old_blob, value_type = value.to_database()
        new_blob = transition_data(old_blob, value_type)
        deserialized = arrow_value.from_database(new_blob, value_type)
        time_stamp_array = pyarrow.array(
            [
                datetime.datetime(year=2025, month=7, day=25, hour=9, minute=15),
                datetime.datetime(year=2025, month=7, day=25, hour=12, minute=15),
            ]
        )
        value_array = pyarrow.array([2.3, 5.0])
        expected_table = pyarrow.record_batch({"col_1": time_stamp_array, "value": value_array})
        expected_table = arrow_value.with_column_as_time_stamps(expected_table, "col_1", True, True)
        assert deserialized == expected_table
        assert deserialized.schema.metadata == expected_table.schema.metadata
