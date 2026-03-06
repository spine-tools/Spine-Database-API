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

""" Type conversion functions. """
from __future__ import annotations
from collections.abc import Callable
from datetime import datetime
import re
from typing import Any, ClassVar, Generic, Literal, TypeAlias, TypedDict, TypeVar
from dateutil.relativedelta import relativedelta
from typing_extensions import NotRequired
from spinedb_api.helpers import string_to_bool
from spinedb_api.parameter_value import DateTime, Duration, ParameterValueFormatError


class ConvertSpecDict(TypedDict):
    name: str
    start_datetime: NotRequired[str]
    duration: NotRequired[str]
    start_int: NotRequired[int]

ConvertSpecValue: TypeAlias = Literal["datetime", "duration", "float", "string", "boolean"]

def value_to_convert_spec(value: ConvertSpec | ConvertSpecValue | ConvertSpecDict):
    if isinstance(value, ConvertSpec):
        return value
    if isinstance(value, str):
        spec = {
            "datetime": DateTimeConvertSpec,
            "duration": DurationConvertSpec,
            "float": FloatConvertSpec,
            "string": StringConvertSpec,
            "boolean": BooleanConvertSpec,
        }[value]
        return spec()
    if isinstance(value, dict):
        start_datetime = DateTime(value.get("start_datetime"))
        duration = Duration(value.get("duration"))
        start_int = value.get("start_int")
        return IntegerSequenceDateTimeConvertSpec(start_datetime, start_int, duration)
    raise TypeError(f"value must be str or dict instead got {type(value).__name__}")


T = TypeVar("T")

class ConvertSpec(Generic[T]):
    DISPLAY_NAME: ClassVar[str] = NotImplemented
    RETURN_TYPE: Callable[[Any], T] = NotImplemented

    def __call__(self, value: Any) -> T | None:
        try:
            return self.RETURN_TYPE(value)
        except ValueError as error:
            if not value:
                return None
            raise error

    def to_json_value(self) -> str | ConvertSpecDict:
        return self.DISPLAY_NAME


class DateTimeConvertSpec(ConvertSpec[DateTime]):
    DISPLAY_NAME = "datetime"
    RETURN_TYPE = DateTime


class DurationConvertSpec(ConvertSpec[Duration]):
    DISPLAY_NAME = "duration"
    RETURN_TYPE = Duration


class FloatConvertSpec(ConvertSpec[float]):
    DISPLAY_NAME = "float"
    RETURN_TYPE = float


class StringConvertSpec(ConvertSpec[str]):
    DISPLAY_NAME = "string"
    RETURN_TYPE = str


class BooleanConvertSpec(ConvertSpec[bool]):
    DISPLAY_NAME = "boolean"
    RETURN_TYPE = bool

    def __call__(self, value):
        return self.RETURN_TYPE(string_to_bool(str(value)))


class IntegerSequenceDateTimeConvertSpec(ConvertSpec[DateTime]):
    DISPLAY_NAME = "integer sequence datetime"
    RETURN_TYPE = DateTime

    def __init__(self, start_datetime: str | DateTime | datetime, start_int: int, duration: str | relativedelta | Duration):
        if not isinstance(start_datetime, DateTime):
            start_datetime = DateTime(start_datetime)
        if not isinstance(duration, Duration):
            duration = Duration(duration)
        self.start_datetime = start_datetime
        self.start_int = start_int
        self.duration = duration
        self.pattern = re.compile(r"[0-9]+|$")

    def __call__(self, value):
        start_datetime = self.start_datetime.value
        duration = self.duration.value
        start_int = self.start_int
        pattern = self.pattern
        try:
            int_str = pattern.search(str(value)).group()
            int_value = int(int_str) - start_int
            return DateTime(start_datetime + int_value * duration)
        except (ValueError, ParameterValueFormatError) as error:
            raise ValueError(f"Could not convert '{value}' to a DateTime") from error

    def to_json_value(self):
        return {
            "name": self.DISPLAY_NAME,
            "start_datetime": self.start_datetime.value.isoformat(),
            "duration": str(self.duration),
            "start_int": self.start_int,
        }
