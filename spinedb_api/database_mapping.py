#############################################################################
# Copyright (C) 2017 - 2018 VTT Technical Research Centre of Finland
#
# This file is part of Spine Database API.
#
# Spine Spine Database API is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#############################################################################

"""
Provides :class:`.DatabaseMapping`.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""

from .database_mapping_query_mixin import DatabaseMappingQueryMixin
from .database_mapping_base import DatabaseMappingBase


class DatabaseMapping(DatabaseMappingQueryMixin, DatabaseMappingBase):
    """The standard database mapping class.

    :param str db_url: A database URL in RFC-1738 format, used for creating the Spine object relational mapping.
    :param str username: A user name. If omitted, the string ``"anon"`` is used.
    :param bool upgrade: Whether or not the db at the given URL should be upgraded to the most recent version.
    """

    def __init__(self, db_url, username=None, upgrade=False):
        """Initialize class."""
        super().__init__(db_url, username=username, upgrade=upgrade)
