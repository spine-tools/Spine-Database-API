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
Provides :class:`.DiffDatabaseMappingBase`.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""

from .database_mapping_base import DatabaseMappingBase
from sqlalchemy import MetaData, Table, inspect
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.sql.expression import Alias
from .exception import SpineTableNotFoundError
from .helpers import forward_sweep
from datetime import datetime, timezone
from sqlalchemy.orm.util import AliasedInsp

# TODO: improve docstrings


class DiffDatabaseMappingBase(DatabaseMappingBase):
    """Base class for the *difference* database mapping.

    This is a special mapping designed to *stage* temporary changes to the database
    so they can be committed in batch. All subquery properties return results
    *as if* the changes were already committed.
    """

    # NOTE: It works by creating and mapping a set of
    # temporary 'diff' tables to hold staged changes until the moment of commit.

    def __init__(self, db_url, username=None, upgrade=False):
        """Initialize class."""
        super().__init__(db_url, username=username, upgrade=upgrade)
        self.diff_prefix = None
        # Diff classes
        self.DiffCommit = None
        self.DiffObjectClass = None
        self.DiffObject = None
        self.DiffRelationshipClass = None
        self.DiffRelationship = None
        self.DiffParameterDefinition = None
        self.DiffParameterValue = None
        self.DiffParameterTag = None
        self.DiffParameterDefinitionTag = None
        self.DiffParameterValueList = None
        # Diff dictionaries
        self.added_item_id = {}
        self.updated_item_id = {}
        self.removed_item_id = {}
        self.dirty_item_id = {}
        # Initialize stuff
        self._init_diff_dicts()
        self._create_diff_tables_and_mapping()

    def _init_diff_dicts(self):
        """Initialize dictionaries that help keeping track of the differences."""
        self.added_item_id = {x: set() for x in self.table_to_class}
        self.updated_item_id = {x: set() for x in self.table_to_class}
        self.removed_item_id = {x: set() for x in self.table_to_class}
        self.dirty_item_id = {x: set() for x in self.table_to_class}

    def _create_diff_tables_and_mapping(self):
        """Create diff tables and ORM."""
        # Create tables...
        diff_name_prefix = "diff_" + self.username
        self.diff_prefix = diff_name_prefix + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + "_"
        metadata = MetaData(self.engine)
        metadata.reflect()
        diff_metadata = MetaData()
        diff_tables = list()
        for t in metadata.sorted_tables:
            if t.name.startswith(diff_name_prefix) or t.name == "next_id":
                continue
            diff_columns = [c.copy() for c in t.columns]
            diff_table = Table(self.diff_prefix + t.name, diff_metadata, *diff_columns, prefixes=["TEMPORARY"])
        diff_metadata.drop_all(self.engine)
        # NOTE: Using `self.connection` below allows `self.session` to see the temp tables
        diff_metadata.create_all(self.connection)
        # Create mapping...
        DiffBase = automap_base(metadata=diff_metadata)
        DiffBase.prepare()
        not_found = []
        for tablename, classname in self.table_to_class.items():
            try:
                setattr(self, "Diff" + classname, getattr(DiffBase.classes, self.diff_prefix + tablename))
            except (NoSuchTableError, AttributeError):
                not_found.append(tablename)
        if not_found:
            raise SpineTableNotFoundError(not_found, self.db_url)

    def _mark_as_dirty(self, tablename, ids):
        """Mark items as dirty, which means the corresponding records from the original tables
        are no longer valid, and they should be queried from the diff tables instead."""
        self.dirty_item_id[tablename].update(ids)
        # Set subquery attributes involving the affected table to `None`
        # (This forces the subqueries to be refreshed when accessing the corresponding property)
        for attr, val in self.__dict__.items():
            if not isinstance(val, Alias):
                continue
            tables = []
            func = lambda x: isinstance(x, Table) and tables.append(x.name)
            forward_sweep(val, func)
            if tablename in tables:
                setattr(self, attr, None)

    def subquery(self, tablename):
        """Return an (:class:`~sqlalchemy.sql.expression.Alias` object) derived from:: sql

            SELECT * FROM tablename

        If this mapping has staged changes, the result includes those changes 'as if'
        they were already committed.
        """
        # NOTE: Overriden method to (i) filter dirty items from original tables, and
        # (ii) also bring data from diff tables:
        #     SELECT * FROM orig_table WHERE id NOT IN dirty_ids
        #     UNION ALL
        #     SELECT * FROM diff_table
        classname = self.table_to_class[tablename]
        orig_class = getattr(self, classname)
        diff_class = getattr(self, "Diff" + classname)
        return (
            self.query(*[c.label(c.name) for c in inspect(orig_class).mapper.columns])
            .filter(~orig_class.id.in_(self.dirty_item_id[tablename]))
            .union_all(self.query(*inspect(diff_class).mapper.columns))
            .subquery()
        )

    def has_pending_changes(self):
        """True if this mapping has uncommitted changes."""
        if any([v for v in self.added_item_id.values()]):
            return True
        if any([v for v in self.dirty_item_id.values()]):
            return True
        return False

    def _reset_diff_mapping(self):
        """Delete all records from diff tables (but don't drop the tables)."""
        self.query(self.DiffObjectClass).delete()
        self.query(self.DiffObject).delete()
        self.query(self.DiffRelationshipClass).delete()
        self.query(self.DiffRelationship).delete()
        self.query(self.DiffParameterDefinition).delete()
        self.query(self.DiffParameterValue).delete()
        self.query(self.DiffParameterTag).delete()
        self.query(self.DiffParameterDefinitionTag).delete()
        self.query(self.DiffParameterValueList).delete()
