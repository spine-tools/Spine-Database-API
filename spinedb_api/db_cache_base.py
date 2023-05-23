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
DB cache base.

"""
from contextlib import suppress
from enum import Enum, unique, auto
from functools import cmp_to_key
from .temp_id import TempIdDict, TempId

# TODO: Implement CacheItem.pop() to do lookup?


@unique
class Status(Enum):
    """Cache item status."""

    committed = auto()
    to_add = auto()
    to_update = auto()
    to_remove = auto()


class DBCacheBase(dict):
    """A dictionary that maps table names to ids to items. Used to store and retrieve database contents."""

    def __init__(self, chunk_size=None):
        super().__init__()
        self._updated_items = {}
        self._removed_items = {}
        self._offsets = {}
        self._fetched_item_types = set()
        self._chunk_size = chunk_size

    @property
    def fetched_item_types(self):
        return self._fetched_item_types

    def _item_factory(self, item_type):
        raise NotImplementedError()

    def _query(self, item_type):
        raise NotImplementedError()

    def make_item(self, item_type, **item):
        factory = self._item_factory(item_type)
        return factory(self, item_type, **item)

    def _cmp_item_type(self, a, b):
        if a in self._item_factory(b).ref_types():
            # a should come before b
            return -1
        if b in self._item_factory(a).ref_types():
            # a should come after b
            return 1
        return 0

    def _sorted_item_types(self):
        sorted(self, key=cmp_to_key(self._cmp_item_type))

    def dirty_items(self):
        """Returns a list of tuples of the form (item_type, (to_add, to_update, to_remove)) corresponding to
        items that have been modified but not yet committed.

        Returns:
            list
        """
        dirty_items = []
        for item_type in sorted(self, key=cmp_to_key(self._cmp_item_type)):
            table_cache = self[item_type]
            to_add = []
            to_update = []
            to_remove = []
            for item in dict.values(table_cache):
                _ = item.is_valid()
                if item.status == Status.to_add:
                    to_add.append(item)
                elif item.status == Status.to_update:
                    to_update.append(item)
                elif item.status == Status.to_remove:
                    to_remove.append(item)
                if to_remove:
                    # Fetch descendants, so that they are validated in next iterations of the loop.
                    # This ensures cascade removal.
                    # FIXME: We should also fetch the current item type because of multi-dimensional entities and
                    # classes which also depend on no-dimensional ones
                    for x in self:
                        if self._cmp_item_type(item_type, x) < 0:
                            self.fetch_all(x)
            if to_add or to_update or to_remove:
                dirty_items.append((item_type, (to_add, to_update, to_remove)))
        return dirty_items

    def rollback(self):
        """Discards uncommitted changes.

        Namely, removes all the added items, resets all the updated items, and restores all the removed items.

        Returns:
            bool: False if there is no uncommitted items, True if successful.
        """
        dirty_items = self.dirty_items()
        if not dirty_items:
            return False
        to_add_by_type = []
        to_update_by_type = []
        to_remove_by_type = []
        for item_type, (to_add, to_update, to_remove) in reversed(dirty_items):
            to_add_by_type.append((item_type, to_add))
            to_update_by_type.append((item_type, to_update))
            to_remove_by_type.append((item_type, to_remove))
        for item_type, to_remove in to_remove_by_type:
            table_cache = self.table_cache(item_type)
            for item in to_remove:
                table_cache.restore_item(item["id"])
        for item_type, to_update in to_update_by_type:
            table_cache = self.table_cache(item_type)
            for item in to_update:
                table_cache.update_item(item.backup)
        for item_type, to_add in to_add_by_type:
            table_cache = self.table_cache(item_type)
            for item in to_add:
                if table_cache.remove_item(item["id"]) is not None:
                    del item["id"]
        return True

    def refresh(self):
        """Stores dirty items in internal dictionaries and clears the cache, so the DB can be fetched again.
        Conflicts between new contents of the DB and dirty items are solved in favor of the latter
        (See ``advance_query`` where we resolve those conflicts as consuming the queries).
        """
        dirty_items = self.dirty_items()  # Get dirty items before clearing
        self.clear()
        self._updated_items.clear()
        self._removed_items.clear()
        for item_type, (to_add, to_update, to_remove) in dirty_items:
            # Add new items directly
            table_cache = self.table_cache(item_type)
            for item in to_add:
                table_cache.add_item(item, new=True)
            # Store updated and removed so we can take the proper action
            # when we see their equivalents comming from the DB
            self._updated_items[item_type] = {x["id"]: x for x in to_update}
            self._removed_items[item_type] = {x["id"]: x for x in to_remove}
        self._offsets.clear()
        self._fetched_item_types.clear()

    def _get_next_chunk(self, item_type):
        qry = self._query(item_type)
        if not self._chunk_size:
            self._fetched_item_types.add(item_type)
            return [dict(x) for x in qry]
        offset = self._offsets.setdefault(item_type, 0)
        chunk = [dict(x) for x in qry.limit(self._chunk_size).offset(offset)]
        self._offsets[item_type] += len(chunk)
        return chunk

    def advance_query(self, item_type):
        """Advances the DB query that fetches items of given type and caches the results.

        Args:
            item_type (str)

        Returns:
            list: items fetched from the DB
        """
        chunk = self._get_next_chunk(item_type)
        if not chunk:
            self._fetched_item_types.add(item_type)
            return []
        table_cache = self.table_cache(item_type)
        updated_items = self._updated_items.get(item_type, {})
        removed_items = self._removed_items.get(item_type, {})
        for item in chunk:
            updated_item = updated_items.get(item["id"])
            if updated_item:
                table_cache.persist_item(updated_item)
                continue
            removed_item = removed_items.get(item["id"])
            if removed_item:
                table_cache.persist_item(removed_item, removed=True)
                continue
            table_cache.add_item(item)
        return chunk

    def table_cache(self, item_type):
        return self.setdefault(item_type, _TableCache(self, item_type))

    def get_item(self, item_type, id_):
        table_cache = self.get(item_type, {})
        item = table_cache.get(id_)
        if item is None:
            return {}
        return item

    def fetch_more(self, item_type):
        if item_type in self._fetched_item_types:
            return False
        return bool(self.advance_query(item_type))

    def fetch_all(self, item_type):
        while self.fetch_more(item_type):
            pass

    def fetch_value(self, item_type, return_fn):
        while self.fetch_more(item_type):
            return_value = return_fn()
            if return_value:
                return return_value
        return return_fn()

    def fetch_ref(self, item_type, id_):
        while self.fetch_more(item_type):
            with suppress(KeyError):
                return self[item_type][id_]
        # It is possible that fetching was completed between deciding to call this function
        # and starting the while loop above resulting in self.fetch_more() to return False immediately.
        # Therefore, we should try one last time if the ref is available.
        with suppress(KeyError):
            return self[item_type][id_]
        return None


class _TableCache(TempIdDict):
    def __init__(self, db_cache, item_type, *args, **kwargs):
        """
        Args:
            db_cache (DBCache): the DB cache where this table cache belongs.
            item_type (str): the item type, equal to a table name
        """
        super().__init__(*args, **kwargs)
        self._db_cache = db_cache
        self._item_type = item_type
        self._id_by_unique_key_value = {}

    def _new_id(self):
        return TempId(self._item_type)

    def unique_key_value_to_id(self, key, value, strict=False):
        """Returns the id that has the given value for the given unique key, or None.

        Args:
            key (tuple)
            value (tuple)

        Returns:
            int
        """
        id_by_unique_value = self._id_by_unique_key_value.get(key, {})
        if not id_by_unique_value:
            id_by_unique_value = self._db_cache.fetch_value(
                self._item_type, lambda: self._id_by_unique_key_value.get(key, {})
            )
        value = tuple(tuple(x) if isinstance(x, list) else x for x in value)
        if strict:
            return id_by_unique_value[value]
        return id_by_unique_value.get(value)

    def _unique_key_value_to_item(self, key, value, strict=False):
        return self.get(self.unique_key_value_to_id(key, value))

    def values(self):
        return (x for x in super().values() if x.is_valid())

    def _make_item(self, item):
        """Returns a cache item.

        Args:
            item (dict): the 'db item' to use as base

        Returns:
            CacheItem
        """
        return self._db_cache.make_item(self._item_type, **item)

    def current_item(self, item, skip_keys=()):
        """Returns a CacheItemBase that matches the given dictionary-item.

        Args:
            item (dict)

        Returns:
            CacheItemBase or None
        """
        id_ = item.get("id")
        if isinstance(id_, int):
            # id is an int, easy
            return self.get(id_) or self._db_cache.fetch_ref(self._item_type, id_)
        if isinstance(id_, dict):
            # id is a dict specifying the values for one of the unique constraints
            key, value = zip(*id_.items())
            return self._unique_key_value_to_item(key, value)
        if id_ is None:
            # No id. Try to locate the item by the value of one of the unique keys.
            # Used by import_data (and more...)
            cache_item = self._make_item(item)
            error = cache_item.resolve_inverse_references(item.keys())
            if error:
                return None
            error = cache_item.polish()
            if error:
                return None
            for key, value in cache_item.unique_values(skip_keys=skip_keys):
                current_item = self._unique_key_value_to_item(key, value)
                if current_item:
                    return current_item

    def check_item(self, item, for_update=False, skip_keys=()):
        # FIXME: The only use-case for skip_keys at the moment is that of importing scenario alternatives,
        # where we only want to match by (scen_name, alt_name) and not by (scen_name, rank)
        if for_update:
            current_item = self.current_item(item, skip_keys=skip_keys)
            if current_item is None:
                return None, f"no {self._item_type} matching {item} to update"
            full_item, merge_error = current_item.merge(item)
            if full_item is None:
                return None, merge_error
        else:
            current_item = None
            full_item, merge_error = item, None
        candidate_item = self._make_item(full_item)
        error = candidate_item.resolve_inverse_references(skip_keys=item.keys())
        if error:
            return None, error
        error = candidate_item.polish()
        if error:
            return None, error
        invalid_ref = candidate_item.invalid_ref()
        if invalid_ref:
            return None, f"invalid {invalid_ref} for {self._item_type}"
        try:
            for key, value in candidate_item.unique_values(skip_keys=skip_keys):
                empty = {k for k, v in zip(key, value) if v == ""}
                if empty:
                    return None, f"invalid empty keys {empty} for {self._item_type}"
                unique_item = self._unique_key_value_to_item(key, value)
                if unique_item not in (None, current_item) and unique_item.is_valid():
                    return None, f"there's already a {self._item_type} with {dict(zip(key, value))}"
        except KeyError as e:
            return None, f"missing {e} for {self._item_type}"
        if "id" not in candidate_item:
            candidate_item["id"] = self._new_id()
        return candidate_item, merge_error

    def _add_unique(self, item):
        for key, value in item.unique_values():
            self._id_by_unique_key_value.setdefault(key, TempIdDict())[value] = item["id"]

    def _remove_unique(self, item):
        for key, value in item.unique_values():
            self._id_by_unique_key_value.get(key, {}).pop(value, None)

    def persist_item(self, item, removed=False):
        self[item["id"]] = item
        if not removed:
            self._add_unique(item)

    def add_item(self, item, new=False):
        if "id" not in item:
            item["id"] = self._new_id()
        self[item["id"]] = new_item = self._make_item(item)
        self._add_unique(new_item)
        if new:
            new_item.status = Status.to_add
        return new_item

    def update_item(self, item):
        current_item = self.current_item(item)
        self._remove_unique(current_item)
        current_item.update(item)
        self._add_unique(current_item)
        current_item.cascade_update()
        return current_item

    def remove_item(self, id_):
        current_item = self.current_item({"id": id_})
        if current_item is not None:
            self._remove_unique(current_item)
            current_item.cascade_remove()
        return current_item

    def restore_item(self, id_):
        current_item = self.get(id_)
        if current_item is not None:
            self._add_unique(current_item)
            current_item.cascade_restore()
        return current_item


class CacheItemBase(TempIdDict):
    """A dictionary that represents an db item."""

    _defaults = {}
    _unique_keys = ()
    _references = {}
    _inverse_references = {}

    def __init__(self, db_cache, item_type, **kwargs):
        """
        Args:
            db_cache (DBCache): the DB cache where this item belongs.
        """
        super().__init__(**kwargs)
        self._db_cache = db_cache
        self._item_type = item_type
        self._referrers = TempIdDict()
        self._weak_referrers = TempIdDict()
        self.restore_callbacks = set()
        self.update_callbacks = set()
        self.remove_callbacks = set()
        self._to_remove = False
        self._removed = False
        self._corrupted = False
        self._valid = None
        self._status = Status.committed
        self._backup = None

    @classmethod
    def ref_types(cls):
        return set(ref_type for _src_key, (ref_type, _ref_key) in cls._references.values())

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        self._status = status

    @property
    def backup(self):
        return self._backup

    @property
    def removed(self):
        return self._removed

    @property
    def item_type(self):
        return self._item_type

    @property
    def key(self):
        id_ = dict.get(self, "id")
        if id_ is None:
            return None
        return (self._item_type, id_)

    def __repr__(self):
        return f"{self._item_type}{self._extended()}"

    def __getattr__(self, name):
        """Overridden method to return the dictionary key named after the attribute, or None if it doesn't exist."""
        # FIXME: We should try and get rid of this one
        return self.get(name)

    def __getitem__(self, key):
        ref = self._references.get(key)
        if ref:
            src_key, (ref_type, ref_key) = ref
            ref_id = self[src_key]
            if isinstance(ref_id, tuple):
                return tuple(self._get_ref(ref_type, x).get(ref_key) for x in ref_id)
            return self._get_ref(ref_type, ref_id).get(ref_key)
        return super().__getitem__(key)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def update(self, other):
        if self._status == Status.committed:
            self._status = Status.to_update
            self._backup = self._asdict()
        for src_key, (ref_type, _ref_key) in self._references.values():
            ref_id = self[src_key]
            if src_key in other and other[src_key] != ref_id:
                # Forget references
                if isinstance(ref_id, tuple):
                    for x in ref_id:
                        self._forget_ref(ref_type, x)
                else:
                    self._forget_ref(ref_type, ref_id)
        super().update(other)
        if self._asdict() == self._backup:
            self._status = Status.committed

    def merge(self, other):
        if all(self.get(key) == value for key, value in other.items()):
            return None, ""
        merged = {**self._extended(), **other}
        merged["id"] = self["id"]
        return merged, ""

    def polish(self):
        """Polishes this item once all it's references are resolved. Returns any errors.

        Returns:
            str or None
        """
        for key, default_value in self._defaults.items():
            self.setdefault(key, default_value)
        return ""

    def resolve_inverse_references(self, skip_keys=()):
        for src_key, (id_key, (ref_type, ref_key)) in self._inverse_references.items():
            if src_key in skip_keys:
                continue
            id_value = tuple(dict.pop(self, k, None) or self.get(k) for k in id_key)
            if None in id_value:
                continue
            table_cache = self._db_cache.table_cache(ref_type)
            try:
                self[src_key] = (
                    tuple(table_cache.unique_key_value_to_id(ref_key, v, strict=True) for v in zip(*id_value))
                    if all(isinstance(v, (tuple, list)) for v in id_value)
                    else table_cache.unique_key_value_to_id(ref_key, id_value, strict=True)
                )
            except KeyError as err:
                # Happens at unique_key_value_to_id(..., strict=True)
                return f"can't find {ref_type} with {dict(zip(ref_key, err.args[0]))}"

    def invalid_ref(self):
        for src_key, (ref_type, _ref_key) in self._references.values():
            try:
                ref_id = self[src_key]
            except KeyError:
                return src_key
            if isinstance(ref_id, tuple):
                for x in ref_id:
                    if not self._get_ref(ref_type, x):
                        return src_key
            elif not self._get_ref(ref_type, ref_id):
                return src_key

    def unique_values(self, skip_keys=()):
        for key in self._unique_keys:
            if key not in skip_keys:
                yield key, tuple(self.get(k) for k in key)

    def _get_ref(self, ref_type, ref_id, strong=True):
        ref = self._db_cache.get_item(ref_type, ref_id)
        if not ref:
            if not strong:
                return {}
            ref = self._db_cache.fetch_ref(ref_type, ref_id)
            if not ref:
                self._corrupted = True
                return {}
        return self._handle_ref(ref, strong)

    def _handle_ref(self, ref, strong):
        if strong:
            ref.add_referrer(self)
            if ref.removed:
                self._to_remove = True
        else:
            ref.add_weak_referrer(self)
            if ref.removed:
                return {}
        return ref

    def _forget_ref(self, ref_type, ref_id):
        ref = self._db_cache.get_item(ref_type, ref_id)
        ref.remove_referrer(self)

    def is_valid(self):
        if self._valid is not None:
            return self._valid
        if self._removed or self._corrupted:
            return False
        self._to_remove = False
        self._corrupted = False
        for key in self._references:
            _ = self[key]
        if self._to_remove:
            self.cascade_remove()
        self._valid = not self._removed and not self._corrupted
        return self._valid

    def add_referrer(self, referrer):
        if referrer.key is None:
            return
        self._referrers[referrer.key] = self._weak_referrers.pop(referrer.key, referrer)

    def remove_referrer(self, referrer):
        if referrer.key is None:
            return
        self._referrers.pop(referrer.key, None)

    def add_weak_referrer(self, referrer):
        if referrer.key is None:
            return
        if referrer.key not in self._referrers:
            self._weak_referrers[referrer.key] = referrer

    def _update_weak_referrers(self):
        for weak_referrer in self._weak_referrers.values():
            weak_referrer.call_update_callbacks()

    def cascade_restore(self):
        if not self._removed:
            return
        if self._status == Status.committed:
            self._status = Status.to_add
        else:
            self._status = Status.committed
        self._removed = False
        for referrer in self._referrers.values():
            referrer.cascade_restore()
        self._update_weak_referrers()
        obsolete = set()
        for callback in self.restore_callbacks:
            if not callback(self):
                obsolete.add(callback)
        self.restore_callbacks -= obsolete

    def cascade_remove(self):
        if self._removed:
            return
        if self._status == Status.committed:
            self._status = Status.to_remove
        else:
            self._status = Status.committed
        self._removed = True
        self._to_remove = False
        self._valid = None
        obsolete = set()
        for callback in self.remove_callbacks:
            if not callback(self):
                obsolete.add(callback)
        self.remove_callbacks -= obsolete
        for referrer in self._referrers.values():
            referrer.cascade_remove()
        self._update_weak_referrers()

    def cascade_update(self):
        self.call_update_callbacks()
        for referrer in self._referrers.values():
            referrer.cascade_update()
        self._update_weak_referrers()

    def call_update_callbacks(self):
        obsolete = set()
        for callback in self.update_callbacks:
            if not callback(self):
                obsolete.add(callback)
        self.update_callbacks -= obsolete

    def _extended(self):
        return {**self, **{key: self[key] for key in self._references}}

    def _asdict(self):
        return dict(self)

    def is_committed(self):
        return self._status == Status.committed

    def commit(self, commit_id):
        self._status = Status.committed
        if commit_id:
            self["commit_id"] = commit_id
