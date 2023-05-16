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

"""Provides :class:`.DiffDatabaseMappingRemoveMixin`.

"""

from sqlalchemy.exc import DBAPIError
from .exception import SpineDBAPIError

# TODO: improve docstrings


class DatabaseMappingRemoveMixin:
    """Provides methods to perform ``REMOVE`` operations over a Spine db."""

    def restore_items(self, tablename, *ids):
        if not ids:
            return []
        tablename = self._real_tablename(tablename)
        table_cache = self.cache.get(tablename)
        if not table_cache:
            return []
        return [table_cache.restore_item(id_) for id_ in ids]

    def remove_items(self, tablename, *ids):
        if not ids:
            return []
        tablename = self._real_tablename(tablename)
        table_cache = self.cache.get(tablename)
        if not table_cache:
            return []
        ids = set(ids)
        if tablename == "alternative":
            # Do not remove the Base alternative
            ids -= {1}
        return [table_cache.remove_item(id_) for id_ in ids]

    def _do_remove_items(self, connection, **kwargs):
        """Removes items from the db.

        Args:
            **kwargs: keyword is table name, argument is list of ids to remove
        """
        for tablename, ids in kwargs.items():
            tablename = self._real_tablename(tablename)
            if tablename == "alternative":
                # Do not remove the Base alternative
                ids -= {1}
            if not ids:
                continue
            id_field = self._id_fields.get(tablename, "id")
            table = self._metadata.tables[tablename]
            delete = table.delete().where(self.in_(getattr(table.c, id_field), ids))
            try:
                connection.execute(delete)
            except DBAPIError as e:
                msg = f"DBAPIError while removing {tablename} items: {e.orig.args}"
                raise SpineDBAPIError(msg) from e

    def _get_metadata_ids_to_remove(self):
        used_metadata_ids = set()
        for x in self.cache.get("entity_metadata", {}).values():
            used_metadata_ids.add(x["metadata_id"])
        for x in self.cache.get("parameter_value_metadata", {}).values():
            used_metadata_ids.add(x["metadata_id"])
        return {x["id"] for x in self.cache.get("metadata", {}).values()} - used_metadata_ids
