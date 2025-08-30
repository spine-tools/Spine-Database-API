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
from datetime import timedelta
import re

from dateutil.relativedelta import relativedelta
import pandas as pd

# Regex pattern to identify a number encoded as a string
freq = r"([0-9]+)"
# Regex patterns that matches partial duration strings
DATE_PAT = re.compile(r"".join(rf"({freq}{unit})?" for unit in "YMD"))
TIME_PAT = re.compile(r"".join(rf"({freq}{unit})?" for unit in "HMS"))
WEEK_PAT = re.compile(rf"{freq}W")


def parse_duration(value: str) -> relativedelta:
    """Parse a ISO 8601 duration format string to a `relativedelta`."""
    value = value.lstrip("P")
    if m0 := WEEK_PAT.match(value):
        weeks = m0.groups()[0]
        return relativedelta(weeks=int(weeks))

    # unpack to variable number of args to handle absence of timestamp
    date, *_time = value.split("T")
    time = _time[0] if _time else ""
    delta = relativedelta()

    def parse_num(token: str) -> int:
        return int(token) if token else 0

    if m1 := DATE_PAT.match(date):
        years = parse_num(m1.groups()[1])
        months = parse_num(m1.groups()[3])
        days = parse_num(m1.groups()[5])
        delta += relativedelta(years=years, months=months, days=days)

    if m2 := TIME_PAT.match(time):
        hours = parse_num(m2.groups()[1])
        minutes = parse_num(m2.groups()[3])
        seconds = parse_num(m2.groups()[5])
        delta += relativedelta(hours=hours, minutes=minutes, seconds=seconds)

    return delta


def _delta_as_dict(delta: relativedelta | pd.DateOffset | timedelta) -> dict:
    match delta:
        case timedelta():
            return dict(days=delta.days, seconds=delta.seconds, microseconds=delta.microseconds)
        case relativedelta() | pd.DateOffset():
            return {k: v for k, v in vars(delta).items() if not k.startswith("_") and k.endswith("s") and v}
        case _:
            raise TypeError(f"{delta}: unknown type {type(delta)}")


def to_relativedelta(offset: str | pd.DateOffset | timedelta) -> relativedelta:
    """Convert various compatible time offset formats to `relativedelta`.

    Compatible formats:
    - JSON string in "duration" format
    - `pandas.DateOffset`
    - `datetime.timedelta`

    Everyone should use this instead of trying to convert themselves.

    """
    match offset:
        case str():
            return parse_duration(offset)
        case _:
            return relativedelta(**_delta_as_dict(offset))


def to_dateoffset(delta: relativedelta) -> pd.DateOffset:
    """Convert `relativedelta` to `pandas.DateOffset`."""
    return pd.DateOffset(**_delta_as_dict(delta))


_duration_abbrevs = {
    "years": "Y",
    "months": "M",
    "days": "D",
    "sentinel": "T",
    "hours": "H",
    "minutes": "M",
    "seconds": "S",
}


def to_duration(delta: relativedelta | pd.DateOffset | timedelta) -> str:
    """Convert various compatible time offset objects to JSON string
    in "duration" format.

    Compatible formats:
    - `relativedelta`
    - `pandas.DateOffset`
    - `datetime.timedelta`

    Use this for any kind of serialisation.

    """
    kwargs = _delta_as_dict(delta)
    duration = "P"
    for unit, abbrev in _duration_abbrevs.items():
        match unit, kwargs.get(unit):
            case "sentinel", _:
                duration += abbrev
            case _, None:
                pass
            case _, num:
                duration += f"{num}{abbrev}"
    return duration.rstrip("T")
