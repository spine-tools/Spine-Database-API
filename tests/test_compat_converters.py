from datetime import timedelta

from dateutil.relativedelta import relativedelta
import pandas as pd
import pyarrow as pa
import pytest

from spinedb_api.compat.converters import parse_duration, to_dateoffset, to_duration, to_relativedelta

durations = [
    ("P2M1DT2H3M", relativedelta(months=2, days=1, hours=2, minutes=3)),
    ("P3WT1H", relativedelta(weeks=3)),
    ("PT1H2M3S", relativedelta(hours=1, minutes=2, seconds=3)),
]


@pytest.mark.parametrize("json_str,expect", durations)
def test_parse_duration(json_str, expect):
    rd = parse_duration(json_str)
    assert rd == expect


bad_durations = [("foo", ValueError, "unable to parse as duration")]


@pytest.mark.parametrize("json_str,exc,msg", bad_durations)
def test_parse_duration_err(json_str, exc, msg):
    with pytest.raises(exc, match=msg):
        parse_duration(json_str)


deltas = [
    "P2M1DT2H3M",
    pd.DateOffset(months=2, hours=1),
    timedelta(days=7, hours=1),
    pa.MonthDayNano([1, 2, 3]),
    relativedelta(months=1, days=3, seconds=5),
]

rel_deltas = [
    relativedelta(months=2, days=1, hours=2, minutes=3),
    relativedelta(months=2, hours=1),
    relativedelta(days=7, hours=1),
    relativedelta(months=1, days=2, seconds=0),
    relativedelta(months=1, days=3, seconds=5),
]


@pytest.mark.parametrize("data,expect", list(zip(deltas, rel_deltas)))
def test_to_relativedelta(data, expect):
    assert to_relativedelta(data) == expect


pd_offsets = [
    pd.DateOffset(months=2, days=1, hours=2, minutes=3),
    pd.DateOffset(months=2, hours=1),
    pd.DateOffset(days=7, hours=1),
    pd.DateOffset(months=1, days=2),
    pd.DateOffset(months=1, days=3, seconds=5),
]


@pytest.mark.parametrize("data,expect", list(zip(deltas, pd_offsets)))
def test_to_dateoffset(data, expect):
    assert to_dateoffset(data) == expect


json_durations = ["P2M1DT2H3M", "P2MT1H", "P7DT1H", "P1M2D", "P1M3DT5S"]


@pytest.mark.parametrize("data,expect", list(zip(deltas, json_durations)))
def test_to_durations(data, expect):
    assert to_duration(data) == expect
