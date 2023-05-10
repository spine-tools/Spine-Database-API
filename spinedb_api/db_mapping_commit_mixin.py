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
Provides :class:`.QuickDatabaseMappingBase`.

"""

from datetime import datetime, timezone
from .exception import SpineDBAPIError


class DatabaseMappingCommitMixin:
    """Provides methods to commit or rollback pending changes onto a Spine database."""

    def __init__(self, *args, **kwargs):
        """Initialize class."""
        super().__init__(*args, **kwargs)
        self._commit_id = None

    def has_pending_changes(self):
        # FIXME
        return True

    def _make_commit_id(self):
        if self._commit_id is None:
            with self.engine.begin() as connection:
                self._commit_id = self._do_make_commit_id(connection)
        return self._commit_id

    def _do_make_commit_id(self, connection):
        user = self.username
        date = datetime.now(timezone.utc)
        ins = self._metadata.tables["commit"].insert()
        return connection.execute(ins, {"user": user, "date": date, "comment": "uncomplete"}).inserted_primary_key[0]

    def commit_session(self, comment):
        """Commits current session to the database.

        Args:
            comment (str): commit message
        """
        self._check_commit(comment)
        commit = self._metadata.tables["commit"]
        user = self.username
        date = datetime.now(timezone.utc)
        upd = commit.update().where(commit.c.id == self._make_commit_id())
        self._checked_execute(upd, dict(user=user, date=date, comment=comment))
        to_add, to_update, to_remove = self.cache.to_change()
        for tablename, items in to_add.items():
            self._do_add_items(tablename, *items)
        for tablename, items in to_update.items():
            self._do_update_items(tablename, *items)
        self._do_remove_items(**to_remove)
        self._commit_id = None
        if self._memory:
            self._memory_dirty = True

    def rollback_session(self):
        if not self.has_pending_changes():
            raise SpineDBAPIError("Nothing to rollback.")
        self.cache.reset_queries()
        self._commit_id = None
