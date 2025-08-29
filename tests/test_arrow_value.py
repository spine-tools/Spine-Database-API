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
import pyarrow
import pytest
from spinedb_api import SpineDBAPIError
from spinedb_api.arrow_value import load_field_metadata, with_column_as_time_period, with_column_as_time_stamps


class TestWithColumnAsTimePeriod:
    def test_column_given_by_name(self):
        column = pyarrow.array(["M1-4,M9-12", "M5-8"])
        record_batch = pyarrow.record_batch({"data": column})
        as_time_period = with_column_as_time_period(record_batch, "data")
        column_metadata = load_field_metadata(as_time_period.field("data"))
        assert column_metadata == {"format": "time_period"}

    def test_column_given_by_index(self):
        column = pyarrow.array(["M1-4,M9-12", "M5-8"])
        record_batch = pyarrow.record_batch({"data": column})
        as_time_period = with_column_as_time_period(record_batch, 0)
        column_metadata = load_field_metadata(as_time_period.field("data"))
        assert column_metadata == {"format": "time_period"}

    def test_raises_when_column_data_is_invalid(self):
        column = pyarrow.array(["gibberish"])
        record_batch = pyarrow.record_batch({"data": column})
        with pytest.raises(
            SpineDBAPIError, match="^Invalid interval gibberish, it should start with either Y, M, D, WD, h, m, or s.$"
        ):
            with_column_as_time_period(record_batch, 0)


class TestWithColumnAsTimeStamps:
    def test_column_given_by_name(self):
        column = pyarrow.array([datetime.datetime(year=2025, month=7, day=25, hour=9, minute=48)])
        record_batch = pyarrow.record_batch({"stamps": column})
        as_time_stamps_with_year_ignored = with_column_as_time_stamps(record_batch, "stamps", True, False)
        as_time_stamps_with_repeat = with_column_as_time_stamps(record_batch, "stamps", False, True)
        assert load_field_metadata(as_time_stamps_with_year_ignored.field("stamps")) == {
            "ignore_year": True,
            "repeat": False,
        }
        assert load_field_metadata(as_time_stamps_with_repeat.field("stamps")) == {
            "ignore_year": False,
            "repeat": True,
        }

    def test_column_given_by_index(self):
        column = pyarrow.array([datetime.datetime(year=2025, month=7, day=25, hour=9, minute=48)])
        record_batch = pyarrow.record_batch({"stamps": column})
        as_time_stamps = with_column_as_time_stamps(record_batch, 0, True, True)
        assert load_field_metadata(as_time_stamps.field("stamps")) == {"ignore_year": True, "repeat": True}

    def test_raises_when_column_type_is_wrong(self):
        column = pyarrow.array(["A"])
        record_batch = pyarrow.record_batch({"stamps": column})
        with pytest.raises(SpineDBAPIError, match="^column is not time stamp column$"):
            with_column_as_time_stamps(record_batch, 0, False, False)
