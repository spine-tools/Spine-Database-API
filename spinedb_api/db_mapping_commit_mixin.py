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

    def commit_session(self, comment):
        """Commits current session to the database.

        Args:
            comment (str): commit message
        """
        if not comment:
            raise SpineDBAPIError("Commit message cannot be empty.")
        dirty_items = self.cache.dirty_items()
        if not dirty_items:
            raise SpineDBAPIError("Nothing to commit.")
        user = self.username
        date = datetime.now(timezone.utc)
        ins = self._metadata.tables["commit"].insert()
        with self.engine.begin() as connection:
            commit_id = connection.execute(ins, dict(user=user, date=date, comment=comment)).inserted_primary_key[0]
            for tablename, (to_add, to_update, to_remove) in dirty_items:
                for item in to_add + to_update + to_remove:
                    item.commit(commit_id)
                self._do_add_items(connection, tablename, *to_add)
                self._do_update_items(connection, tablename, *to_update)
                self._do_remove_items(connection, tablename, *{x["id"] for x in to_remove})
        if self._memory:
            self._memory_dirty = True

    def rollback_session(self):
        if not self.cache.dirty_items():
            raise SpineDBAPIError("Nothing to rollback.")
        self.cache.reset_queries()
