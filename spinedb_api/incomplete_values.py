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

from .models import to_json
from .parameter_value import RANK_1_TYPES, TABLE_TYPE, Map, from_dict, to_database, type_for_scalar
from .value_support import JSONValue, load_db_value


def dump_db_value(parsed_value: JSONValue) -> tuple[bytes, str | None]:
    """
    Unparses a JSON object into a binary blob and type string.

    If the given object is a dict, extracts the "type" property from it.

    :meta private:

    Args:
        parsed_value: A JSON object, typically obtained by calling :func:`load_db_value`.

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
        parsed = load_db_value(database_value)
        return len(parsed) - 1
    return 0


def join_value_and_type(db_value: bytes, db_type: Optional[str]) -> str:
    """Joins value blob and type into list and dumps it into JSON string.

    Args:
        db_value: database value
        db_type: value type

    Returns:
        JSON string.
    """
    return json.dumps([db_value.decode(), db_type])


def split_value_and_type(value_and_type: str) -> tuple[bytes, str]:
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
    return parsed[0].encode(), parsed[1]
