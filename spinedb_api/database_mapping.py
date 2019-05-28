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
A class to create an object relational mapping from a Spine database and query it.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""

import warnings
from .database_mapping_base import _DatabaseMappingBase
from .database_mapping_query import _DatabaseMappingQuery
from sqlalchemy import (
    create_engine,
    false,
    distinct,
    func,
    MetaData,
    event,
    or_,
    inspect,
)
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy.exc import NoSuchTableError, DBAPIError, DatabaseError
from alembic.migration import MigrationContext
from alembic.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from alembic.config import Config
from .exception import SpineDBAPIError, SpineDBVersionError, SpineTableNotFoundError

# TODO: Consider returning lists of dict (with _asdict()) rather than queries,
# to better support platforms that cannot handle queries efficiently (such as Julia)
# TODO: At some point DatabaseMapping attributes such as session, engine, and all the tables should be made 'private'
# so as to prevent hacking into the database.
# TODO: SELECT queries should also be checked for errors


class DatabaseMapping(_DatabaseMappingBase, _DatabaseMappingQuery):
    """A class to create an object relational mapping from a Spine database and query it.

    Attributes:
        db_url (str): The database url formatted according to sqlalchemy rules
        username (str): The user name
        upgrade (Bool): Whether or not the given url should be automatically upgraded to the latest version
    """

    def __init__(self, db_url, username=None, upgrade=False):
        """Initialize class."""
        super().__init__(db_url, username=username, upgrade=upgrade)
        self.create_subqueries()
        self.create_special_subqueries()
