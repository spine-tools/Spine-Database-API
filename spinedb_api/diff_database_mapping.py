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

from .database_mapping_query_mixin import DatabaseMappingQueryMixin
from .database_mapping_check_mixin import DatabaseMappingCheckMixin
from .diff_database_mapping_add_mixin import DiffDatabaseMappingAddMixin
from .diff_database_mapping_update_mixin import DiffDatabaseMappingUpdateMixin
from .diff_database_mapping_remove_mixin import DiffDatabaseMappingRemoveMixin
from .diff_database_mapping_commit_mixin import DiffDatabaseMappingCommitMixin
from .diff_database_mapping_base import DiffDatabaseMappingBase


class DiffDatabaseMapping(
    DatabaseMappingQueryMixin,
    DatabaseMappingCheckMixin,
    DiffDatabaseMappingAddMixin,
    DiffDatabaseMappingUpdateMixin,
    DiffDatabaseMappingRemoveMixin,
    DiffDatabaseMappingCommitMixin,
    DiffDatabaseMappingBase,
):
    """A class to create a 'diff' ORM from a Spine db, query it, make changes to it, and
    commit those changes.

    Attributes:
        db_url (str): The database url formatted according to sqlalchemy rules
        username (str): The user name
        upgrade (Bool): Whether or not the given url should be automatically upgraded to the latest version
    """

    def __init__(self, db_url, username=None, upgrade=False):
        """Initialize class."""
        super().__init__(db_url, username=username, upgrade=upgrade)
