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
from .db_cache_impl import DBCache


class DatabaseMapping(
    DatabaseMappingAddMixin,
    DatabaseMappingUpdateMixin,
    DatabaseMappingRemoveMixin,
    DatabaseMappingCommitMixin,
    DatabaseMappingBase,
):
    """Enables communication with a Spine DB.

    A mapping of the DB is incrementally created in memory as data is requested/modified.

    Data is typically retrieved using :meth:`get_item` or :meth:`get_items`.
    If the requested data is already in memory, it is returned from there;
    otherwise it is fetched from the DB, stored in memory, and then returned.
    In other words, the data is fetched from the DB exactly once.

    Data is added via :meth:`add_item`;
    updated via :meth:`update_item`;
    removed via :meth:`remove_item`;
    and restored via :meth:`restore_item`.
    All the above methods modify the in-memory mapping (not the DB itself).
    These methods also fetch data from the DB into the in-memory mapping to perform the necessary integrity checks
    (unique and foreign key constraints).

    To retrieve an item or to manipulate it, you typically need to specify certain fields.
    The :meth:`describe_item_type` method is provided to help you identify these fields.

    Modifications to the in-memory mapping are committed (written) to the DB via :meth:`commit_session`,
    or rolled back (discarded) via :meth:`rollback_session`.

    The DB fetch status is reset via :meth:`refresh_session`.
    This causes new items in the DB to be merged into the memory mapping as data is further requested/modified.

    You can also control the fetching process via :meth:`fetch_more` and/or :meth:`fetch_all`.
    For example, a UI application might want to fetch data in the background so the UI is not blocked in the process.
    In that case they can call e.g. :meth:`fetch_more` asynchronously as the user scrolls or expands the views.

    The :meth:`query` method is also provided as an alternative way to retrieve data from the DB
    while bypassing the in-memory mapping entirely.
    """

    def get_item(self, item_type, fetch=True, skip_removed=True, **kwargs):
        """Finds and returns and item matching the arguments, or None if none found.

        Args:
            item_type (str): The type of the item.
            fetch (bool, optional): Whether to fetch the DB in case the item is not found in memory.
            skip_removed (bool, optional): Whether to ignore removed items.
            **kwargs: Fields of one of the item type's unique keys and their values for the requested item.

        Returns:
            :class:`PublicItem` or None
        """
        item_type = self._real_tablename(item_type)
        cache_item = self.cache.table_cache(item_type).find_item(kwargs, fetch=fetch)
        if not cache_item:
            return None
        if skip_removed and not cache_item.is_valid():
            return None
        return PublicItem(self, cache_item)

    def get_items(self, item_type, fetch=True, skip_removed=True):
        """Finds and returns and item matching the arguments, or None if none found.

        Args:
            item_type (str): The type of items to get.
            fetch (bool, optional): Whether to fetch the DB before returning the items.
            skip_removed (bool, optional): Whether to ignore removed items.

        Returns:
            :class:`PublicItem` or None
        """
        item_type = self._real_tablename(item_type)
        if fetch and item_type not in self.cache.fetched_item_types:
            self.fetch_all(item_type)
        table_cache = self.cache.table_cache(item_type)
        get_items = table_cache.valid_values if skip_removed else table_cache.values
        return [PublicItem(self, x) for x in get_items()]

    def add_item(self, item_type, check=True, **kwargs):
        """Adds an item to the in-memory mapping.

        Example::

                with DatabaseMapping(url) as db_map:
                    db_map.add_item("entity", class_name="dog", name="Pete")


        Args:
            item_type (str): The type of the item.
            check (bool, optional): Whether to carry out integrity checks.
            **kwargs: Mandatory fields for the item type and their values.

        Returns:
            tuple(:class:`PublicItem` or None, str): The added item and any errors.
        """
        item_type = self._real_tablename(item_type)
        table_cache = self.cache.table_cache(item_type)
        self._convert_legacy(item_type, kwargs)
        if not check:
            return table_cache.add_item(kwargs, new=True), None
        checked_item, error = table_cache.check_item(kwargs)
        return (
            PublicItem(self, table_cache.add_item(checked_item, new=True)) if checked_item and not error else None,
            error,
        )

    def update_item(self, item_type, check=True, **kwargs):
        """Updates an item in the in-memory mapping.

        Example::

                with DatabaseMapping(url) as db_map:
                    my_dog = db_map.get_item("entity", class_name="dog", name="Pete")
                    db_map.update_item("entity", id=my_dog["id], name="Pluto")

        Args:
            item_type (str): The type of the item.
            check (bool, optional): Whether to carry out integrity checks.
            id (int): The id of the item to update.
            **kwargs: Fields to update and their new values.

        Returns:
            tuple(:class:`PublicItem` or None, str): The added item and any errors.
        """
        item_type = self._real_tablename(item_type)
        table_cache = self.cache.table_cache(item_type)
        self._convert_legacy(item_type, kwargs)
        if not check:
            return table_cache.update_item(kwargs), None
        checked_item, error = table_cache.check_item(kwargs, for_update=True)
        return (PublicItem(self, table_cache.update_item(checked_item._asdict())) if checked_item else None, error)

    def remove_item(self, item_type, id_):
        """Removes an item from the in-memory mapping.

        Example::

                with DatabaseMapping(url) as db_map:
                    my_dog = db_map.get_item("entity", class_name="dog", name="Pluto")
                    db_map.remove_item("entity", my_dog["id])


        Args:
            item_type (str): The type of the item.
            id (int): The id of the item to remove.

        Returns:
            tuple(:class:`PublicItem` or None, str): The removed item if any.
        """
        item_type = self._real_tablename(item_type)
        table_cache = self.cache.table_cache(item_type)
        return PublicItem(self, table_cache.remove_item(id_))

    def restore_item(self, item_type, id_):
        """Restores a previously removed item into the in-memory mapping.

        Example::

                with DatabaseMapping(url) as db_map:
                    my_dog = db_map.get_item("entity", skip_removed=False, class_name="dog", name="Pluto")
                    db_map.restore_item("entity", my_dog["id])

        Args:
            item_type (str): The type of the item.
            id (int): The id of the item to restore.

        Returns:
            tuple(:class:`PublicItem` or None, str): The restored item if any.
        """
        item_type = self._real_tablename(item_type)
        table_cache = self.cache.table_cache(item_type)
        return PublicItem(self, table_cache.restore_item(id_))

    def can_fetch_more(self, item_type):
        """Whether or not more data can be fetched from the DB for the given item type.

        Args:
            item_type (str): The item type (table) to check.

        Returns:
            bool
        """
        return item_type not in self.cache.fetched_item_types

    def fetch_more(self, item_type, limit):
        """Fetches items from the DB into the in-memory mapping, incrementally.

        Args:
            item_type (str): The item type (table) to fetch.
            limit (int): The maximum number of items to fetch. Successive calls to this function
                will start from the point where the last one left.
                In other words, each item is fetched from the DB exactly once.

        Returns:
            list(PublicItem): The items fetched.
        """
        item_type = self._real_tablename(item_type)
        return self.cache.fetch_more(item_type, limit=limit)

    def fetch_all(self, *item_types):
        """Fetches items from the DB into the in-memory mapping.
        Unlike :meth:`fetch_more`, this method fetches entire tables.

        Args:
            *item_types (str): The item types (tables) to fetch. If none given, then the entire DB is fetched.
        """
        item_types = set(self.ITEM_TYPES) if not item_types else set(item_types) & set(self.ITEM_TYPES)
        for item_type in item_types:
            item_type = self._real_tablename(item_type)
            self.cache.fetch_all(item_type)

    @staticmethod
    def describe_item_type(item_type):
        """Prints a synopsis of the given item type to the stdout.

        Args:
            item_type (str): The type of item to describe.
        """
        factory = DBCache.item_factory(item_type)
        sections = ("Fields:", "Unique keys:")
        width = max(len(s) for s in sections) + 4
        print()
        print(item_type)
        print("-" * len(item_type))
        section = sections[0]
        field_iter = (f"{field} ({type_}) - {description}" for field, (type_, description) in factory._fields.items())
        _print_section(section, width, field_iter)
        print()
        section = sections[1]
        unique_key_iter = ("(" + ", ".join(key) + ")" for key in factory._unique_keys)
        _print_section(section, width, unique_key_iter)
        print()


def _print_section(section, width, iterator):
    row = next(iterator)
    bullet = "- "
    print(f"{section:<{width}}" + bullet + row)
    for row in iterator:
        print(" " * width + bullet + row)


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
