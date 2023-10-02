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


from sqlalchemy import and_, or_
from sqlalchemy.exc import DBAPIError
from .exception import SpineDBAPIError
from .helpers import Asterisk, group_consecutive
from .temp_id import resolve

# TODO: improve docstrings


class DatabaseMappingRemoveMixin:
    """Provides methods to perform ``REMOVE`` operations over a Spine db."""

    def remove_items(self, tablename, *ids):
        """Removes items from the DB.

        Args:
            tablename (str): Target database table name
            *ids (int): Ids of items to be removed.

        Returns:
            set: ids or items successfully updated
            list(SpineIntegrityError): found violations
        """
        if not ids:
            return []
        tablename = self._real_tablename(tablename)
        table_cache = self.cache.table_cache(tablename)
        if Asterisk in ids:
            self.cache.fetch_all(tablename)
            ids = table_cache
        ids = set(ids)
        if tablename == "alternative":
            # Do not remove the Base alternative
            ids.discard(1)
        return [table_cache.remove_item(id_) for id_ in ids]

    def restore_items(self, tablename, *ids):
        if not ids:
            return []
        tablename = self._real_tablename(tablename)
        table_cache = self.cache.table_cache(tablename)
        return [table_cache.restore_item(id_) for id_ in ids]

    def remove_item(self, tablename, id_):
        tablename = self._real_tablename(tablename)
        table_cache = self.cache.table_cache(tablename)
        return table_cache.remove_item(id_)

    def restore_item(self, tablename, id_):
        tablename = self._real_tablename(tablename)
        table_cache = self.cache.table_cache(tablename)
        return table_cache.restore_item(id_)

    def purge_items(self, tablename):
        """Removes all items from given table.

        Args:
            tablename (str): name of table

        Returns:
            bool: True if operation was successful, False otherwise
        """
        return self.remove_items(tablename, Asterisk)

    def _do_remove_items(self, connection, tablename, *ids):
        """Removes items from the db.

        Args:
            *ids: ids to remove
        """
        tablenames = [self._real_tablename(tablename)]
        ids = {resolve(id_) for id_ in ids}
        if tablenames[0] == "alternative":
            # Do not remove the Base alternative
            ids.discard(1)
        if not ids:
            return
        if tablenames[0] == "entity_class":
            # Also remove the items corresponding to the id in entity_class_dimension
            tablenames.append("entity_class_dimension")
        elif tablenames[0] == "entity":
            # Also remove the items corresponding to the id in entity_element
            tablenames.append("entity_element")
        for tablename in tablenames:
            table = self._metadata.tables[tablename]
            id_field = self._id_fields.get(tablename, "id")
            id_column = getattr(table.c, id_field)
            cond = or_(*(and_(id_column >= first, id_column <= last) for first, last in group_consecutive(ids)))
            delete = table.delete().where(cond)
            try:
                connection.execute(delete)
            except DBAPIError as e:
                msg = f"DBAPIError while removing {tablename} items: {e.orig.args}"
                raise SpineDBAPIError(msg) from e

    def remove_unused_metadata(self):
        used_metadata_ids = set()
        for x in self.cache.table_cache("entity_metadata").valid_values():
            used_metadata_ids.add(x["metadata_id"])
        for x in self.cache.table_cache("parameter_value_metadata").valid_values():
            used_metadata_ids.add(x["metadata_id"])
        unused_metadata_ids = {x["id"] for x in self.cache.table_cache("metadata").valid_values()} - used_metadata_ids
        self.remove_items("metadata", *unused_metadata_ids)
