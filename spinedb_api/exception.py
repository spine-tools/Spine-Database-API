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

"""
Spine DB API exceptions.
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
    Failure in import/export mapping.
    """

    def __init__(self, msg):
        super().__init__(msg)


class InvalidMappingComponent(InvalidMapping):
    def __init__(self, msg, rank=None, key=None):
        super().__init__(msg)
        self.rank = rank
        self.key = key


class ConnectorError(SpineDBAPIError):
    """Failure in import/export connector."""
