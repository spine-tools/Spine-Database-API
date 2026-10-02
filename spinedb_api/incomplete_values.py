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
"""This module contains utilities that deal with value blobs or JSON representations."""

import json
from typing import Optional
from . import ParameterValueFormatError
from .arrow_value import to_record_batch
from .models import to_json
from .parameter_value import RANK_1_TYPES, STRING_VALUE_TYPE, TABLE_TYPE, Map, from_dict, to_database, type_for_scalar
from .value_support import JSONValue, load_db_value


def dump_db_value(parsed_value: JSONValue) -> tuple[bytes, str | None]:
    """
    Unparses a JSON object into a binary blob and type string.

    If the given object is a dict, extracts the "type" property from it.

    :meta private:

    Args:
        parsed_value: A JSON object.

    Returns:
        database representation (value and type).
    """
    if isinstance(parsed_value, dict):
        value_type = parsed_value.pop("type")
        value = from_dict(parsed_value, value_type)
        return to_database(value)
    if isinstance(parsed_value, list):
        value_type = TABLE_TYPE
        db_value = to_json(parsed_value).encode("UTF8")
    else:
        value_type = type_for_scalar(parsed_value)
        db_value = json.dumps(parsed_value).encode("UTF8")
    return db_value, value_type


def from_database_to_dimension_count(database_value: bytes, value_type: Optional[str]) -> int:
    """
    Counts the dimensions in a database representation of a parameter value (value and type).

    :meta private:

    Args:
        database_value: the database value
        value_type: the value type

    Returns:
        number of dimensions
    """
    if value_type in RANK_1_TYPES:
        return 1
    if value_type == Map.TYPE or value_type == TABLE_TYPE:
        parsed = load_db_value(database_value, value_type)
        return len(parsed) - 1
    return 0


def join_value_and_type(db_value: bytes, db_type: str | None) -> str:
    """Joins value blob and type into list and dumps it into JSON string.

    Args:
        db_value: database value
        db_type: value type

    Returns:
        JSON string.
    """
    return json.dumps([json.loads(db_value), db_type])


def split_value_and_type(value_and_type: str) -> tuple[bytes, str | None]:
    """Splits the given JSON string into value blob and type.

    Args:
        value_and_type: a string joining value and type, as obtained by calling :func:`join_value_and_type`.

    Returns:
        value blob and type.
    """
    parsed = json.loads(value_and_type)
    if isinstance(parsed, dict):
        # legacy
        value_dict = json.loads(value_and_type)
        return to_database(from_dict(value_dict, value_dict["type"]))
    return json.dumps(parsed[0]).encode(), parsed[1]


def try_parse_json(json_string: str) -> tuple[bytes, str | None]:
    """Tries to interpret user input as parameter value JSON."""
    try:
        parsed = json.loads(json_string)
    except json.JSONDecodeError:
        return json.dumps(json_string).encode("UTF8"), STRING_VALUE_TYPE
    try:
        if isinstance(parsed, dict):
            value_type = parsed.pop("type")
            value = from_dict(parsed, value_type)
            return to_database(value)
        if isinstance(parsed, list):
            value = to_record_batch(parsed)
            return to_database(value)
        value_type = type_for_scalar(parsed)
        return json.dumps(parsed).encode("UTF8"), value_type
    except (TypeError, KeyError, ParameterValueFormatError):
        return json.dumps(json_string).encode("UTF8"), STRING_VALUE_TYPE
