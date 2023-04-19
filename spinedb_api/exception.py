######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Classes to handle exceptions while using the Spine database API.

"""


class SpineDBAPIError(Exception):
    """Basic exception for errors raised by the API."""

    def __init__(self, msg=None):
        super().__init__(msg)
        self.msg = msg

    def __str__(self):
        return self.msg


class SpineIntegrityError(SpineDBAPIError):
    """Database integrity error while inserting/updating records.

    Attributes:
        msg (str): the message to be displayed
        id (int): the id the instance that caused a unique violation
    """

    def __init__(self, msg=None, id=None):
        super().__init__(msg)
        self.id = id


class SpineDBVersionError(SpineDBAPIError):
    """Database version error."""

    def __init__(self, url=None, current=None, expected=None, upgrade_available=True):
        super().__init__(msg="The database at '{}' is not the expected version.".format(url))
        self.url = url
        self.current = current
        self.expected = expected
        self.upgrade_available = upgrade_available


class SpineTableNotFoundError(SpineDBAPIError):
    """Can't find one of the tables."""

    def __init__(self, table, url=None):
        super().__init__(msg="Table(s) '{}' couldn't be mapped from the database at '{}'.".format(table, url))
        self.table = table


class RecordNotFoundError(SpineDBAPIError):
    """Can't find one record in one of the tables."""

    def __init__(self, table, name=None, id=None):
        super().__init__(msg="Unable to find item in table '{}'.".format(table))
        self.table = table
        self.name = name
        self.id = id


class ParameterValueError(SpineDBAPIError):
    """The value given for a parameter does not fit the datatype."""

    def __init__(self, value, data_type):
        super().__init__(msg="The value {} does not fit the datatype '{}'.".format(value, data_type))
        self.value = value
        self.data_type = data_type


class ParameterValueFormatError(SpineDBAPIError):
    """
    Failure in encoding/decoding a parameter value.

    Attributes:
        msg (str): an error message
    """

    def __init__(self, msg):
        super().__init__(msg)


class InvalidMapping(SpineDBAPIError):
    """
    Failure in import/export mapping
    """

    def __init__(self, msg):
        super().__init__(msg)


class InvalidMappingComponent(InvalidMapping):
    def __init__(self, msg, rank=None, key=None):
        super().__init__(msg)
        self.rank = rank
        self.key = key
