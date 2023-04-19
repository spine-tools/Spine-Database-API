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
Provides :class:`DiffDatabaseMappingCommitMixin`.

"""

from datetime import datetime, timezone
from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql.expression import bindparam
from .exception import SpineDBAPIError


class DiffDatabaseMappingCommitMixin:
    """Provides methods to commit or rollback staged changes onto a Spine database."""

    def commit_session(self, comment):
        """Commit staged changes to the database.

        Args:
            comment (str): An informative comment explaining the nature of the commit.
        """
        self._check_commit(comment)
        transaction = self.connection.begin()
        try:
            user = self.username
            date = datetime.now(timezone.utc)
            ins = self._metadata.tables["commit"].insert().values(user=user, date=date, comment=comment)
            commit_id = self.connection.execute(ins).inserted_primary_key[0]
            # NOTE: Remove first, so `scenario_alternative.rank`s become 'free'.
            # Remove
            for tablename, ids in self.removed_item_id.items():
                if not ids:
                    continue
                table = self._metadata.tables[tablename]
                id_col = self.table_ids.get(tablename, "id")
                self.query(table).filter(self.in_(getattr(table.c, id_col), ids)).delete(synchronize_session=False)
            # Update
            for tablename, ids in self.updated_item_id.items():
                if not ids:
                    continue
                id_col = self.table_ids.get(tablename, "id")
                orig_table = self._metadata.tables[tablename]
                diff_table = self._diff_table(tablename)
                updated_items = []
                for item in self.query(diff_table).filter(self.in_(getattr(diff_table.c, id_col), ids)):
                    kwargs = item._asdict()
                    kwargs["commit_id"] = commit_id
                    updated_items.append(kwargs)
                upd = orig_table.update()
                for k in self._get_primary_key(tablename):
                    upd = upd.where(getattr(orig_table.c, k) == bindparam(k))
                upd = upd.values({key: bindparam(key) for key in orig_table.columns.keys()})
                self._checked_execute(upd, updated_items)
            # Add
            for tablename, ids in self.added_item_id.items():
                if not ids:
                    continue
                id_col = self.table_ids.get(tablename, "id")
                orig_table = self._metadata.tables[tablename]
                diff_table = self._diff_table(tablename)
                new_items = []
                for item in self.query(diff_table).filter(self.in_(getattr(diff_table.c, id_col), ids)):
                    kwargs = item._asdict()
                    kwargs["commit_id"] = commit_id
                    new_items.append(kwargs)
                self._checked_execute(orig_table.insert(), new_items)
            self._reset_diff_mapping()
            transaction.commit()
            self._reset_diff_dicts()
            if self._memory:
                self._memory_dirty = True
        except DBAPIError as e:
            msg = "DBAPIError while committing changes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg) from None

    def rollback_session(self):
        """Discard all staged changes."""
        if not self.has_pending_changes():
            raise SpineDBAPIError("Nothing to rollback.")
        self.reset_session()

    def reset_session(self):
        transaction = self.connection.begin()
        try:
            self._reset_diff_mapping()
            transaction.commit()
            self._reset_diff_dicts()
        except DBAPIError as e:
            msg = "DBAPIError while rolling back changes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg) from None

    def has_pending_changes(self):
        """True if this mapping has any staged changes."""
        return any(self.added_item_id.values()) or any(self.dirty_item_id.values())
