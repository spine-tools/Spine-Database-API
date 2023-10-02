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
This module defines the :class:`.DatabaseMapping` class.
"""

import sqlalchemy.exc
from .db_mapping_base import DatabaseMappingBase
from .db_mapping_add_mixin import DatabaseMappingAddMixin
from .db_mapping_update_mixin import DatabaseMappingUpdateMixin
from .db_mapping_remove_mixin import DatabaseMappingRemoveMixin
from .db_mapping_commit_mixin import DatabaseMappingCommitMixin


class DatabaseMapping(
    DatabaseMappingAddMixin,
    DatabaseMappingUpdateMixin,
    DatabaseMappingRemoveMixin,
    DatabaseMappingCommitMixin,
    DatabaseMappingBase,
):
    """Enables communication with a Spine DB.

    An in-memory clone (ORM) of the DB is incrementally formed as data is requested/modified.

    Data is typically retrieved using :meth:`get_item` or :meth:`get_items`.
    If the requested data is already in the in-memory clone, it is returned from there;
    otherwise it is fetched from the DB, stored in the clone, and then returned.
    In other words, the data is fetched from the DB exactly once.

    Data is added via :meth:`add_item` or :meth:`add_items`;
    updated via :meth:`update_item` or :meth:`update_items`;
    removed via :meth:`remove_item` or :meth:`remove_items`;
    and restored via :meth:`restore_item` or :meth:`restore_items`.
    All the above methods modify the in-memory clone (not the DB itself).
    These methods also fetch data from the DB into the in-memory clone to perform the necessary integrity checks
    (unique constraints, foreign key constraints) as needed.

    Modifications to the in-memory clone are committed (written) to the DB via :meth:`commit_session`,
    or rolled back (discarded) via :meth:`rollback_session`.

    The in-memory clone is reset via :meth:`refresh_session`.

    You can also control the fetching process via :meth:`fetch_more` and/or :meth:`fetch_all`.
    These methods are especially useful to be called asynchronously.

    Data can also be retreived using :meth:`query` in combination with one of the multiple subquery properties
    documented below.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for item_type in self.ITEM_TYPES:
            setattr(self, "get_" + item_type, self._make_getter(item_type))

    def _make_getter(self, item_type):
        def _get_item(self, **kwargs):
            return self.get_item(item_type, **kwargs)

        return _get_item

    def get_item(self, tablename, **kwargs):
        tablename = self._real_tablename(tablename)
        cache_item = self.cache.table_cache(tablename).find_item(kwargs)
        if not cache_item:
            return None
        return PublicItem(self, cache_item)

    def get_items(self, tablename, fetch=True, valid_only=True):
        tablename = self._real_tablename(tablename)
        if fetch and tablename not in self.cache.fetched_item_types:
            self.fetch_all(tablename)
        if valid_only:
            return [PublicItem(self, x) for x in self.cache.table_cache(tablename).valid_values()]
        return [PublicItem(self, x) for x in self.cache.table_cache(tablename).values()]

    def can_fetch_more(self, tablename):
        return tablename not in self.cache.fetched_item_types

    def fetch_more(self, tablename, limit):
        """Fetches items from the DB into memory, incrementally.

        Args:
            tablename (str): The table to fetch.
            limit (int): The maximum number of items to fetch. Successive calls to this function
                will start from the point where the last one left.
                In other words, each item is fetched from the DB exactly once.

        Returns:
            list(PublicItem): The items fetched.
        """
        tablename = self._real_tablename(tablename)
        return self.cache.fetch_more(tablename, limit=limit)

    def fetch_all(self, *tablenames):
        """Fetches items from the DB into memory. Unlike :meth:`fetch_more`, this method fetches entire tables.

        Args:
            *tablenames (str): The tables to fetch. If none given, then the entire DB is fecthed.
        """
        tablenames = set(self.ITEM_TYPES) if not tablenames else set(tablenames) & set(self.ITEM_TYPES)
        for tablename in tablenames:
            tablename = self._real_tablename(tablename)
            self.cache.fetch_all(tablename)

    def add_item(self, tablename, **kwargs):
        tablename = self._real_tablename(tablename)
        table_cache = self.cache.table_cache(tablename)
        self._convert_legacy(tablename, kwargs)
        checked_item, error = table_cache.check_item(kwargs)
        if error:
            return None, error
        return table_cache.add_item(checked_item, new=True), None


class PublicItem:
    def __init__(self, db_map, cache_item):
        self._db_map = db_map
        self._cache_item = cache_item

    @property
    def item_type(self):
        return self._cache_item.item_type

    def __getitem__(self, key):
        return self._cache_item[key]

    def __eq__(self, other):
        if isinstance(other, dict):
            return self._cache_item == other
        return super().__eq__(other)

    def __repr__(self):
        return repr(self._cache_item)

    def __str__(self):
        return str(self._cache_item)

    def get(self, key, default=None):
        return self._cache_item.get(key, default)

    def is_valid(self):
        return self._cache_item.is_valid()

    def is_committed(self):
        return self._cache_item.is_committed()

    def _asdict(self):
        return self._cache_item._asdict()

    def update(self, **kwargs):
        self._db_map.update_item(self.item_type, id=self["id"], **kwargs)

    def remove(self):
        return self._db_map.remove_item(self.item_type, self["id"])

    def restore(self):
        return self._db_map.restore_item(self.item_type, self["id"])

    def add_update_callback(self, callback):
        self._cache_item.update_callbacks.add(callback)

    def add_remove_callback(self, callback):
        self._cache_item.remove_callbacks.add(callback)

    def add_restore_callback(self, callback):
        self._cache_item.restore_callbacks.add(callback)
