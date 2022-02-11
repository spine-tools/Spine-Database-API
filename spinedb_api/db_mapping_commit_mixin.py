######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
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

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""

from datetime import datetime, timezone
from sqlalchemy import event
from .exception import SpineDBAPIError


class DatabaseMappingCommitMixin:
    """Provides methods to commit or rollback pending changes onto a Spine database.
    Unlike Diff..., there's no "staging area", i.e., all changes are applied directly on the 'original' tables.
    So no regrets. But it's much faster than maintaining the staging area and diff tables,
    so ideal for, e.g., Spine Toolbox's Importer that operates 'in one go'.
    """

    def __init__(self, *args, **kwargs):
        """Initialize class."""
        super().__init__(*args, **kwargs)
        self._transaction = None
        self._commit_id = None
        event.listen(self.session, 'after_begin', self._receive_after_begin)

    def _receive_after_begin(self, session, transaction, connection):
        if self._commit_id is None:
            session.commit()
            self.make_commit_id()

    def has_pending_changes(self):
        return self._commit_id is not None

    def make_commit_id(self):
        if self._commit_id is None:
            self._transaction = self.connection.begin()
            user = self.username
            date = datetime.now(timezone.utc)
            ins = self._metadata.tables["commit"].insert()
            self._commit_id = self._checked_execute(
                ins, {"user": user, "date": date, "comment": ""}
            ).inserted_primary_key[0]
        return self._commit_id

    def commit_session(self, comment):
        """Commits current session to the database.

        Args:
            comment (str): commit message
        """
        self._pre_commit(comment)
        commit = self._metadata.tables["commit"]
        user = self.username
        date = datetime.now(timezone.utc)
        upd = commit.update().where(commit.c.id == self.make_commit_id())
        self._checked_execute(upd, dict(user=user, date=date, comment=comment))
        self._transaction.commit()
        self._commit_id = None

    def rollback_session(self):
        if not self.has_pending_changes():
            raise SpineDBAPIError("Nothing to rollback.")
        self._transaction.rollback()
        self._commit_id = None
