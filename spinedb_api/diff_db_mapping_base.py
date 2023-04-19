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
Provides :class:`.DiffDatabaseMappingBase`.

"""

from datetime import datetime, timezone
from sqlalchemy import select
from sqlalchemy.sql.expression import literal, union_all
from .db_mapping_base import DatabaseMappingBase
from .helpers import labelled_columns

# TODO: improve docstrings


class DiffDatabaseMappingBase(DatabaseMappingBase):
    """Base class for the read-write database mapping.

    :param str db_url: A URL in RFC-1738 format pointing to the database to be mapped.
    :param str username: A user name. If ``None``, it gets replaced by the string ``"anon"``.
    :param bool upgrade: Whether or not the db at the given URL should be upgraded to the most recent version.
    """

    # NOTE: It works by creating and mapping a set of
    # temporary 'diff' tables, where temporary changes are staged until the moment of commit.

    _session_kwargs = dict(autocommit=True)

    def __init__(self, *args, **kwargs):
        """Initialize class."""
        super().__init__(*args, **kwargs)
        self.diff_prefix = None
        # Diff dictionaries
        self.added_item_id = {}
        self.updated_item_id = {}
        self.removed_item_id = {}
        self.dirty_item_id = {}
        # Initialize stuff
        self._init_diff_dicts()
        self._create_diff_tables()

    def _init_diff_dicts(self):
        """Initialize dictionaries that help keeping track of the differences."""
        self.added_item_id = {x: set() for x in self._tablenames}
        self.updated_item_id = {x: set() for x in self._tablenames}
        self.removed_item_id = {x: set() for x in self._tablenames}
        self.dirty_item_id = {x: set() for x in self._tablenames}

    def _reset_diff_dicts(self):
        self._init_diff_dicts()
        self._clear_subqueries(*self._tablenames)

    def _create_diff_tables(self):
        """Create diff tables."""
        diff_name_prefix = "diff_" + self.username
        self.diff_prefix = diff_name_prefix + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + "_"
        for tablename in self._tablenames:
            table = self._metadata.tables[tablename]
            diff_columns = [c.copy() for c in table.columns]
            self.make_temporary_table(self.diff_prefix + tablename, *diff_columns)

    def _mark_as_dirty(self, tablename, ids):
        """Mark items as dirty, which means the corresponding records from the original tables
        are no longer valid, and they should be queried from the diff tables instead."""
        self.dirty_item_id[tablename].update(ids)
        self._clear_subqueries(tablename)

    def _subquery(self, tablename):
        """Overriden method to
            (i) filter dirty items from original tables, and
            (ii) also bring data from diff tables:
        Roughly equivalent to:
            SELECT * FROM orig_table WHERE id NOT IN dirty_ids
            UNION ALL
            SELECT * FROM diff_table
        """
        orig_table = self._metadata.tables[tablename]
        table_id = self.table_ids.get(tablename, "id")
        qry = self.query(*labelled_columns(orig_table)).filter(
            ~self.in_(getattr(orig_table.c, table_id), self.dirty_item_id[tablename])
        )
        if self.added_item_id[tablename] or self.updated_item_id[tablename]:
            diff_table = self._diff_table(tablename)
            if self.sa_url.drivername.startswith("mysql"):
                # Work around the "can't reopen <temporary table>" error in MySQL.
                # (This happens whenever a temporary table is used more than once in a query.)
                # Basically what we do here, is dump the contents of the diff table into a
                # `SELECT first row UNION ALL SELECT second row ... UNION ALL SELECT last row` statement,
                # and use it as a replacement.
                diff_row_selects = [
                    select([literal(v).label(k) for k, v in row._asdict().items()]) for row in self.query(diff_table)
                ]
                diff_table = union_all(*diff_row_selects).alias()
            qry = qry.union_all(self.query(*labelled_columns(diff_table)))
        return qry.subquery()

    def _orig_subquery(self, tablename):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM tablename

        :param str tablename: A string indicating the table to be queried.
        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        table = self._metadata.tables[tablename]
        return self.query(table).subquery()

    def _diff_subquery(self, tablename):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM tablename

        :param str tablename: A string indicating the table to be queried.
        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        return self.query(self._diff_table(tablename)).subquery()

    def diff_ids(self):
        return {x: self.added_item_id[x] | self.updated_item_id[x] for x in self._tablenames}

    def _diff_table(self, tablename):
        return self._metadata.tables.get(self.diff_prefix + tablename)

    def _reset_diff_mapping(self):
        """Delete all records from diff tables (but don't drop the tables)."""
        for tablename in self._tablenames:
            table = self._diff_table(tablename)
            if table is not None:
                self.connection.execute(table.delete())
