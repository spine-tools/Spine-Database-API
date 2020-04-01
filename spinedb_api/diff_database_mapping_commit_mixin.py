######################################################################################################################
# Copyright (C) 2017 - 2019 Spine project consortium
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

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""

from datetime import datetime, timezone
from sqlalchemy.exc import DBAPIError
from .exception import SpineDBAPIError
from .helpers import attr_dict


# TODO: improve docstrings


class DiffDatabaseMappingCommitMixin:
    """Provides methods to commit or rollback staged changes onto a Spine database."""

    def commit_session(self, comment):
        """Commit staged changes to the database.

        :param str comment: An informative comment explaining the nature of the commit.
        """
        if not self.has_pending_changes():
            raise SpineDBAPIError("Nothing to commit.")

        try:
            user = self.username
            date = datetime.now(timezone.utc)
            commit = self.Commit(comment=comment, date=date, user=user)
            self.session.add(commit)
            self.session.flush()
            n = 499  # Maximum number of sql variables
            # Remove
            for tablename, ids in self.removed_item_id.items():
                classname = self.table_to_class[tablename]
                id_col = self.table_ids.get(tablename, "id")
                orig_class = getattr(self, classname)
                removed_ids = list(ids)
                for i in range(0, len(removed_ids), n):
                    self.query(orig_class).filter(getattr(orig_class, id_col).in_(removed_ids[i : i + n])).delete(
                        synchronize_session=False
                    )

            # Update
            for tablename, ids in self.updated_item_id.items():
                classname = self.table_to_class[tablename]
                id_col = self.table_ids.get(tablename, "id")
                orig_class = getattr(self, classname)
                diff_class = getattr(self, "Diff" + classname)
                dirty_ids = list(ids)
                updated_items = []
                for i in range(0, len(dirty_ids), n):
                    for item in self.query(diff_class).filter(getattr(diff_class, id_col).in_(dirty_ids[i : i + n])):
                        kwargs = attr_dict(item)
                        kwargs["commit_id"] = commit.id
                        updated_items.append(kwargs)
                self.session.bulk_update_mappings(orig_class, updated_items)
            # Add
            for tablename, ids in self.added_item_id.items():
                classname = self.table_to_class[tablename]
                id_col = self.table_ids.get(tablename, "id")
                orig_class = getattr(self, classname)
                diff_class = getattr(self, "Diff" + classname)
                new_ids = list(ids)
                new_items = []
                for i in range(0, len(new_ids), n):
                    for item in self.query(diff_class).filter(getattr(diff_class, id_col).in_(new_ids[i : i + n])):
                        kwargs = attr_dict(item)
                        kwargs["commit_id"] = commit.id
                        new_items.append(kwargs)
                self.session.bulk_insert_mappings(orig_class, new_items)
            self._reset_diff_mapping()
            self.session.commit()
            self._init_diff_dicts()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while commiting changes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def rollback_session(self):
        """Discard all staged changes.
        """
        if not self.has_pending_changes():
            raise SpineDBAPIError("Nothing to rollback.")
        try:
            self._reset_diff_mapping()
            self.session.commit()
            self._init_diff_dicts()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while rolling back changes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def has_pending_changes(self):
        """True if this mapping has any staged changes."""
        if any(self.updated_item_id.values()):
            return True
        if (
            any(self.added_item_id.values()) or any(self.removed_item_id.values())
        ) and self.added_item_id != self.removed_item_id:
            return True
        return False
