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
Classes to handle the Spine database object relational mapping.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""

from .database_mapping_query import _DatabaseMappingQuery
from .database_mapping_check import _DatabaseMappingCheck
from .diff_database_mapping_base import _DiffDatabaseMappingBase
from .diff_database_mapping_add import _DiffDatabaseMappingAdd
from .diff_database_mapping_update import _DiffDatabaseMappingUpdate
from .diff_database_mapping_remove import _DiffDatabaseMappingRemove
from .diff_database_mapping_commit import _DiffDatabaseMappingCommit


class DiffDatabaseMapping(
    _DiffDatabaseMappingBase,
    _DatabaseMappingQuery,
    _DatabaseMappingCheck,
    _DiffDatabaseMappingAdd,
    _DiffDatabaseMappingUpdate,
    _DiffDatabaseMappingRemove,
    _DiffDatabaseMappingCommit,
):
    """A class to create a 'diff' ORM from a Spine db, query it, make changes to it, and
    commit those changes.
    """

    def __init__(self, db_url, username=None, upgrade=False):
        """Initialize class."""
        super().__init__(db_url, username=username, upgrade=upgrade)
        self.create_subqueries()
        self.create_special_subqueries()
