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

import threading
from enum import Enum, unique, auto
from difflib import SequenceMatcher
from .temp_id import TempId
from .exception import SpineDBAPIError

# TODO: Implement MappedItem.pop() to do lookup?

_LIMIT = 10000


@unique
class Status(Enum):
    """Mapped item status."""

    committed = auto()
    to_add = auto()
    to_update = auto()
    to_remove = auto()
    added_and_removed = auto()


class DatabaseMappingBase:
    """An in-memory mapping of a DB, mapping item types (table names), to numeric ids, to items.

    This class is not meant to be used directly. Instead, you should subclass it to fit your particular DB schema.

    When subclassing, you need to implement :meth:`item_types`, :meth:`_item_factory`, and :meth:`_make_query`.
    """

    def __init__(self):
        self._mapped_tables = {}
        self._offsets = {}
        self._offset_lock = threading.Lock()
        self._fetched_item_types = set()
        item_types = self.item_types()
        self._sorted_item_types = []
        while item_types:
            item_type = item_types.pop(0)
            if self._item_factory(item_type).ref_types() & set(item_types):
                item_types.append(item_type)
            else:
                self._sorted_item_types.append(item_type)

    @property
    def fetched_item_types(self):
        """Returns a set with the item types that are already fetched.

        Returns:
            set
        """
        return self._fetched_item_types

    @staticmethod
    def item_types():
        """Returns a list of item types from the DB mapping schema (equivalent to the table names).

        Returns:
            list(str)
        """
        raise NotImplementedError()

    @staticmethod
    def _item_factory(item_type):
        """Returns a subclass of :class:`.MappedItemBase` to make items of given type.

        Args:
            item_type (str)

        Returns:
            function
        """
        raise NotImplementedError()

    def _make_query(self, item_type):
        """Returns a :class:`~spinedb_api.query.Query` object to fecth items of given type.

        Args:
            item_type (str)

        Returns:
            :class:`~spinedb_api.query.Query`
        """
        raise NotImplementedError()

    def make_item(self, item_type, **item):
        factory = self._item_factory(item_type)
        return factory(self, item_type, **item)

    def dirty_ids(self, item_type):
        return {
            item["id"]
            for item in self.mapped_table(item_type).valid_values()
            if item.status in (Status.to_add, Status.to_update)
        }

    def _dirty_items(self):
        """Returns a list of tuples of the form (item_type, (to_add, to_update, to_remove)) corresponding to
        items that have been modified but not yet committed.

        Returns:
            list
        """
        dirty_items = []
        for item_type in self._sorted_item_types:
            mapped_table = self.get(item_type)
            if mapped_table is None:
                continue
            to_add = []
            to_update = []
            to_remove = []
            for item in mapped_table.values():
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
                    # classes which also depend on zero-dimensional ones
                    for other_item_type in self.item_types():
                        if (
                            other_item_type not in self.fetched_item_types
                            and item_type in self._item_factory(other_item_type).ref_types()
                        ):
                            self.fetch_all(other_item_type)
            if to_add or to_update or to_remove:
                dirty_items.append((item_type, (to_add, to_update, to_remove)))
        return dirty_items

    def _rollback(self):
        """Discards uncommitted changes.

        Namely, removes all the added items, resets all the updated items, and restores all the removed items.

        Returns:
            bool: False if there is no uncommitted items, True if successful.
        """
        dirty_items = self._dirty_items()
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
            mapped_table = self.mapped_table(item_type)
            for item in to_remove:
                mapped_table.restore_item(item["id"])
        for item_type, to_update in to_update_by_type:
            mapped_table = self.mapped_table(item_type)
            for item in to_update:
                mapped_table.update_item(item.backup)
        for item_type, to_add in to_add_by_type:
            mapped_table = self.mapped_table(item_type)
            for item in to_add:
                if mapped_table.remove_item(item["id"]) is not None:
                    item.invalidate_id()
        return True

    def _refresh(self):
        """Clears fetch progress, so the DB is queried again."""
        self._offsets.clear()
        self._fetched_item_types.clear()

    def _get_next_chunk(self, item_type, limit):
        qry = self._make_query(item_type)
        if not qry:
            return []
        if not limit:
            self._fetched_item_types.add(item_type)
            return [dict(x) for x in qry]
        with self._offset_lock:
            offset = self._offsets.setdefault(item_type, 0)
            chunk = [dict(x) for x in qry.limit(limit).offset(offset)]
            self._offsets[item_type] += len(chunk)
        return chunk

    def _advance_query(self, item_type, limit):
        """Advances the DB query that fetches items of given type
        and adds the results to the corresponding mapped table.

        Args:
            item_type (str)

        Returns:
            list: items fetched from the DB
        """
        chunk = self._get_next_chunk(item_type, limit)
        if not chunk:
            self._fetched_item_types.add(item_type)
            return []
        mapped_table = self.mapped_table(item_type)
        return list(filter(lambda i: i is not None, (mapped_table.add_item(item) for item in chunk)))

    def _check_item_type(self, item_type):
        if item_type not in self.item_types():
            candidate = max(self.item_types(), key=lambda x: SequenceMatcher(None, item_type, x).ratio())
            # FIXME: raise SpineDBAPIError(f"Invalid item type '{item_type}' - maybe you meant '{candidate}'?")

    def mapped_table(self, item_type):
        self._check_item_type(item_type)
        return self._mapped_tables.setdefault(item_type, _MappedTable(self, item_type))

    def get(self, item_type, default=None):
        self._check_item_type(item_type)
        return self._mapped_tables.get(item_type, default)

    def pop(self, item_type, default):
        self._check_item_type(item_type)
        return self._mapped_tables.pop(item_type, default)

    def clear(self):
        self._mapped_tables.clear()

    def get_mapped_item(self, item_type, id_):
        mapped_table = self.mapped_table(item_type)
        item = mapped_table.get(id_)
        if item is None:
            return {}
        return item

    def do_fetch_more(self, item_type, limit=_LIMIT):
        if item_type in self._fetched_item_types:
            return []
        return self._advance_query(item_type, limit)

    def do_fetch_all(self, item_type):
        while self.do_fetch_more(item_type):
            pass

    def fetch_value(self, item_type, return_fn):
        while self.do_fetch_more(item_type):
            return_value = return_fn()
            if return_value:
                return return_value
        return return_fn()

    def fetch_ref(self, item_type, id_):
        while self.do_fetch_more(item_type):
            ref = self.get_mapped_item(item_type, id_)
            if ref:
                return ref
        # It is possible that fetching was completed between deciding to call this function
        # and starting the while loop above resulting in self.do_fetch_more() to return False immediately.
        # Therefore, we should try one last time if the ref is available.
        ref = self.get_mapped_item(item_type, id_)
        if ref:
            return ref


class _MappedTable(dict):
    def __init__(self, db_map, item_type, *args, **kwargs):
        """
        Args:
            db_map (DatabaseMappingBase): the DB mapping where this mapped table belongs.
            item_type (str): the item type, equal to a table name
        """
        super().__init__(*args, **kwargs)
        self._db_map = db_map
        self._item_type = item_type
        self._id_by_unique_key_value = {}
        self._temp_id_by_db_id = {}

    def get(self, id_, default=None):
        id_ = self._temp_id_by_db_id.get(id_, id_)
        return super().get(id_, default)

    def _new_id(self):
        temp_id = TempId(self._item_type)

        def _callback(db_id):
            self._temp_id_by_db_id[db_id] = temp_id

        temp_id.add_resolve_callback(_callback)
        return temp_id

    def unique_key_value_to_id(self, key, value, strict=False, fetch=True):
        """Returns the id that has the given value for the given unique key, or None if not found.

        Args:
            key (tuple)
            value (tuple)
            strict (bool): if True, raise a KeyError if id is not found
            fetch (bool): whether to fetch the DB until found.

        Returns:
            int
        """
        id_by_unique_value = self._id_by_unique_key_value.get(key, {})
        if not id_by_unique_value and fetch:
            id_by_unique_value = self._db_map.fetch_value(
                self._item_type, lambda: self._id_by_unique_key_value.get(key, {})
            )
        value = tuple(tuple(x) if isinstance(x, list) else x for x in value)
        if strict:
            return id_by_unique_value[value]
        return id_by_unique_value.get(value)

    def _unique_key_value_to_item(self, key, value, fetch=True):
        return self.get(self.unique_key_value_to_id(key, value, fetch=fetch))

    def valid_values(self):
        return (x for x in self.values() if x.is_valid())

    def _make_item(self, item):
        """Returns a mapped item.

        Args:
            item (dict): the 'db item' to use as base

        Returns:
            MappedItem
        """
        return self._db_map.make_item(self._item_type, **item)

    def find_item(self, item, skip_keys=(), fetch=True):
        """Returns a MappedItemBase that matches the given dictionary-item.

        Args:
            item (dict)

        Returns:
            MappedItemBase or None
        """
        id_ = item.get("id")
        if id_ is not None:
            # id is given, easy
            item = self.get(id_)
            if item or not fetch:
                return item
            return self._db_map.fetch_ref(self._item_type, id_)
        # No id. Try to locate the item by the value of one of the unique keys.
        # Used by import_data (and more...)
        # FIXME: Do we really need to make the MappedItem here?
        # Can't we just obtain the unique_values directly from item?
        # I guess it's needed in case the user specifies stuff like 'class_id', as tests do,
        # but that should be a corner case...
        mapped_item = self._make_item(item)
        error = mapped_item.resolve_inverse_references(item.keys())
        if error:
            return None
        error = mapped_item.polish()
        if error:
            return None
        for key, value in mapped_item.unique_values(skip_keys=skip_keys):
            current_item = self._unique_key_value_to_item(key, value, fetch=fetch)
            if current_item:
                return current_item

    def check_item(self, item, for_update=False, skip_keys=()):
        # FIXME: The only use-case for skip_keys at the moment is that of importing scenario alternatives,
        # where we only want to match by (scen_name, alt_name) and not by (scen_name, rank)
        if for_update:
            current_item = self.find_item(item, skip_keys=skip_keys)
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
        first_invalid_key = candidate_item.first_invalid_key()
        if first_invalid_key:
            return None, f"invalid {first_invalid_key} for {self._item_type}"
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

    def add_unique(self, item):
        id_ = item["id"]
        for key, value in item.unique_values():
            self._id_by_unique_key_value.setdefault(key, {})[value] = id_

    def remove_unique(self, item):
        id_ = item["id"]
        for key, value in item.unique_values():
            id_by_value = self._id_by_unique_key_value.get(key, {})
            if id_by_value.get(value) == id_:
                del id_by_value[value]

    def add_item(self, item, new=False):
        if not isinstance(item, MappedItemBase):
            item = self._make_item(item)
            item.polish()
        if not new:
            # Item comes from the DB
            id_ = item["id"]
            if id_ in self or id_ in self._temp_id_by_db_id:
                # The item is already in the mapping
                return
            if any(value in self._id_by_unique_key_value.get(key, {}) for key, value in item.unique_values()):
                # An item with the same unique key is already in the mapping
                return
        else:
            item.status = Status.to_add
        if "id" not in item or not item.is_id_valid:
            item["id"] = self._new_id()
        self[item["id"]] = item
        self.add_unique(item)
        return item

    def update_item(self, item):
        current_item = self.find_item(item)
        current_item.cascade_remove_unique()
        current_item.update(item)
        current_item.cascade_add_unique()
        current_item.cascade_update()
        return current_item

    def remove_item(self, id_):
        current_item = self.find_item({"id": id_})
        if current_item is not None:
            self.remove_unique(current_item)
            current_item.cascade_remove()
        return current_item

    def restore_item(self, id_):
        current_item = self.find_item({"id": id_})
        if current_item is not None:
            self.add_unique(current_item)
            current_item.cascade_restore()
        return current_item


class MappedItemBase(dict):
    """A dictionary that represents a db item."""

    fields = {}
    """A dictionary mapping fields to a tuple of (type, value description)"""
    _defaults = {}
    """A dictionary mapping keys to their default values"""
    _unique_keys = ()
    """A tuple where each element is itself a tuple of keys that are unique"""
    _references = {}
    """A dictionary mapping keys that are not in the original dictionary,
    to a recipe for finding the field they reference in another item.

    The recipe is a tuple of the form (original_field, (ref_item_type, ref_field)),
    to be interpreted as follows:
        1. take the value from the original_field of this item, which should be an id,
        2. locate the item of type ref_item_type that has that id,
        3. return the value from the ref_field of that item.
    """
    _inverse_references = {}
    """Another dictionary mapping keys that are not in the original dictionary,
    to a recipe for finding the field they reference in another item.
    Used only for creating new items, when the user provides names and we want to find the ids.

    The recipe is a tuple of the form (src_unique_key, (ref_item_type, ref_unique_key)),
    to be interpreted as follows:
        1. take the values from the src_unique_key of this item, to form a tuple,
        2. locate the item of type ref_item_type where the ref_unique_key is exactly that tuple of values,
        3. return the id of that item.
    """

    def __init__(self, db_map, item_type, **kwargs):
        """
        Args:
            db_map (DatabaseMappingBase): the DB where this item belongs.
        """
        super().__init__(**kwargs)
        self._db_map = db_map
        self._item_type = item_type
        self._referrers = {}
        self._weak_referrers = {}
        self.restore_callbacks = set()
        self.update_callbacks = set()
        self.remove_callbacks = set()
        self._is_id_valid = True
        self._to_remove = False
        self._removed = False
        self._corrupted = False
        self._valid = None
        self._status = Status.committed
        self._removal_source = None
        self._status_when_removed = None
        self._backup = None
        self.public_item = PublicItem(self._db_map, self)

    @classmethod
    def ref_types(cls):
        """Returns a set of item types that this class refers.

        Returns:
            set(str)
        """
        return set(ref_type for _src_key, (ref_type, _ref_key) in cls._references.values())

    @property
    def status(self):
        """Returns the status of this item.

        Returns:
            Status
        """
        return self._status

    @status.setter
    def status(self, status):
        """Sets the status of this item.

        Args:
            status (Status)
        """
        self._status = status

    @property
    def backup(self):
        """Returns the committed version of this item.

        Returns:
            dict or None
        """
        return self._backup

    @property
    def removed(self):
        """Returns whether or not this item has been removed.

        Returns:
            bool
        """
        return self._removed

    @property
    def item_type(self):
        """Returns this item's type

        Returns:
            str
        """
        return self._item_type

    @property
    def key(self):
        """Returns a tuple (item_type, id) for convenience, or None if this item doesn't yet have an id.
        TODO: When does the latter happen?

        Returns:
            tuple(str,int) or None
        """
        id_ = dict.get(self, "id")
        if id_ is None:
            return None
        return (self._item_type, id_)

    @property
    def is_id_valid(self):
        return self._is_id_valid

    def invalidate_id(self):
        """Sets id as invalid."""
        self._is_id_valid = False

    def _extended(self):
        """Returns a dict from this item's original fields plus all the references resolved statically.

        Returns:
            dict
        """
        d = self._asdict()
        d.update({key: self[key] for key in self._references})
        return d

    def _asdict(self):
        """Returns a dict from this item's original fields.

        Returns:
            dict
        """
        return dict(self)

    def merge(self, other):
        """Merges this item with another and returns the merged item together with any errors.
        Used for updating items.

        Args:
            other (dict): the item to merge into this.

        Returns:
            dict: merged item.
            str: error description if any.
        """
        if all(self.get(key) == value for key, value in other.items()):
            return None, ""
        merged = {**self._extended(), **other}
        if not isinstance(merged["id"], int):
            merged["id"] = self["id"]
        return merged, ""

    def first_invalid_key(self):
        """Goes through the ``_references`` class attribute and returns the key of the first one
        that cannot be resolved.

        Returns:
            str or None: unresolved reference's key if any.
        """
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
        """Yields tuples of unique keys and their values.

        Args:
            skip_keys: Don't yield these keys

        Yields:
            tuple(tuple,tuple): the first element is the unique key, the second is the values.
        """
        for key in self._unique_keys:
            if key not in skip_keys:
                yield key, tuple(self.get(k) for k in key)

    def resolve_inverse_references(self, skip_keys=()):
        """Goes through the ``_inverse_references`` class attribute and updates this item
        by resolving those references.
        Returns any error.

        Args:
            skip_keys (tuple): don't resolve references for these keys.

        Returns:
            str or None: error description if any.
        """
        for src_key, (id_key, (ref_type, ref_key)) in self._inverse_references.items():
            if src_key in skip_keys:
                continue
            id_value = tuple(dict.pop(self, k, None) or self.get(k) for k in id_key)
            if None in id_value:
                continue
            mapped_table = self._db_map.mapped_table(ref_type)
            try:
                self[src_key] = (
                    tuple(mapped_table.unique_key_value_to_id(ref_key, v, strict=True) for v in zip(*id_value))
                    if all(isinstance(v, (tuple, list)) for v in id_value)
                    else mapped_table.unique_key_value_to_id(ref_key, id_value, strict=True)
                )
            except KeyError as err:
                # Happens at unique_key_value_to_id(..., strict=True)
                return f"can't find {ref_type} with {dict(zip(ref_key, err.args[0]))}"

    def polish(self):
        """Polishes this item once all it's references have been resolved. Returns any error.

        The base implementation sets defaults but subclasses can do more work if needed.

        Returns:
            str or None: error description if any.
        """
        for key, default_value in self._defaults.items():
            self.setdefault(key, default_value)
        return ""

    def _get_ref(self, ref_type, ref_id, strong=True):
        """Collects a reference from the in-memory mapping.
        Adds this item to the reference's list of referrers if strong is True;
        or weak referrers if strong is False.
        If the reference is not found, sets some flags.

        Args:
            ref_type (str): The reference's type
            ref_id (int): The reference's id
            strong (bool): True if the reference corresponds to a foreign key, False otherwise

        Returns:
            MappedItemBase or dict
        """
        ref = self._db_map.get_mapped_item(ref_type, ref_id)
        if not ref:
            if not strong:
                return {}
            ref = self._db_map.fetch_ref(ref_type, ref_id)
            if not ref:
                self._corrupted = True
                return {}
        # Here we have a ref
        if strong:
            ref.add_referrer(self)
            if ref.removed:
                self._to_remove = True
        else:
            ref.add_weak_referrer(self)
            if ref.removed:
                return {}
        return ref

    def _invalidate_ref(self, ref_type, ref_id):
        """Invalidates a reference previously collected from the in-memory mapping.

        Args:
            ref_type (str): The reference's type
            ref_id (int): The reference's id
        """
        ref = self._db_map.get_mapped_item(ref_type, ref_id)
        ref.remove_referrer(self)

    def is_valid(self):
        """Checks if this item has all its references.
        Removes the item from the in-memory mapping if not valid by calling ``cascade_remove``.

        Returns:
            bool
        """
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
        """Adds a strong referrer to this item. Strong referrers are removed, updated and restored
        in cascade with this item.

        Args:
            referrer (MappedItemBase)
        """
        if referrer.key is None:
            return
        self._referrers[referrer.key] = self._weak_referrers.pop(referrer.key, referrer)

    def remove_referrer(self, referrer):
        """Removes a strong referrer.

        Args:
            referrer (MappedItemBase)
        """
        if referrer.key is None:
            return
        self._referrers.pop(referrer.key, None)

    def add_weak_referrer(self, referrer):
        """Adds a weak referrer to this item.
        Weak referrers' update callbacks are called whenever this item changes.

        Args:
            referrer (MappedItemBase)
        """
        if referrer.key is None:
            return
        if referrer.key not in self._referrers:
            self._weak_referrers[referrer.key] = referrer

    def _update_weak_referrers(self):
        for weak_referrer in self._weak_referrers.values():
            weak_referrer.call_update_callbacks()

    def cascade_restore(self, source=None):
        """Restores this item (if removed) and all its referrers in cascade.
        Also, updates items' status and calls their restore callbacks.
        """
        if not self._removed:
            return
        if source is not self._removal_source:
            return
        if self.status in (Status.added_and_removed, Status.to_remove):
            self._status = self._status_when_removed
        elif self.status == Status.committed:
            self._status = Status.to_add
        else:
            raise RuntimeError("invalid status for item being restored")
        self._removed = False
        # First restore this, then referrers
        obsolete = set()
        for callback in list(self.restore_callbacks):
            if not callback(self):
                obsolete.add(callback)
        self.restore_callbacks -= obsolete
        for referrer in self._referrers.values():
            referrer.cascade_restore(source=self)
        self._update_weak_referrers()

    def cascade_remove(self, source=None):
        """Removes this item and all its referrers in cascade.
        Also, updates items' status and calls their remove callbacks.
        """
        if self._removed:
            return
        self._status_when_removed = self._status
        if self._status == Status.to_add:
            self._status = Status.added_and_removed
        elif self._status in (Status.committed, Status.to_update):
            self._status = Status.to_remove
        else:
            raise RuntimeError("invalid status for item being removed")
        self._removal_source = source
        self._removed = True
        self._to_remove = False
        self._valid = None
        # First remove referrers, then this
        for referrer in self._referrers.values():
            referrer.cascade_remove(source=self)
        self._update_weak_referrers()
        obsolete = set()
        for callback in list(self.remove_callbacks):
            if not callback(self):
                obsolete.add(callback)
        self.remove_callbacks -= obsolete

    def cascade_update(self):
        """Updates this item and all its referrers in cascade.
        Also, calls items' update callbacks.
        """
        self.call_update_callbacks()
        for referrer in self._referrers.values():
            referrer.cascade_update()
        self._update_weak_referrers()

    def call_update_callbacks(self):
        obsolete = set()
        for callback in list(self.update_callbacks):
            if not callback(self):
                obsolete.add(callback)
        self.update_callbacks -= obsolete

    def cascade_add_unique(self):
        """Removes item and all its referrers unique keys and ids in cascade."""
        mapped_table = self._db_map.mapped_table(self._item_type)
        mapped_table.add_unique(self)
        for referrer in self._referrers.values():
            referrer.cascade_add_unique()

    def cascade_remove_unique(self):
        """Removes item and all its referrers unique keys and ids in cascade."""
        mapped_table = self._db_map.mapped_table(self._item_type)
        mapped_table.remove_unique(self)
        for referrer in self._referrers.values():
            referrer.cascade_remove_unique()

    def is_committed(self):
        """Returns whether or not this item is committed to the DB.

        Returns:
            bool
        """
        return self._status == Status.committed

    def commit(self, commit_id):
        """Sets this item as committed with the given commit id."""
        self._status = Status.committed
        if commit_id:
            self["commit_id"] = commit_id

    def __repr__(self):
        """Overridden to return a more verbose representation."""
        return f"{self._item_type}{self._extended()}"

    def __getattr__(self, name):
        """Overridden to return the dictionary key named after the attribute, or None if it doesn't exist."""
        # FIXME: We should try and get rid of this one
        return self.get(name)

    def __getitem__(self, key):
        """Overridden to return references."""
        ref = self._references.get(key)
        if ref:
            src_key, (ref_type, ref_key) = ref
            ref_id = self[src_key]
            if isinstance(ref_id, tuple):
                return tuple(self._get_ref(ref_type, x).get(ref_key) for x in ref_id)
            return self._get_ref(ref_type, ref_id).get(ref_key)
        return super().__getitem__(key)

    def __setitem__(self, key, value):
        """Sets id valid if key is 'id'."""
        if key == "id":
            self._is_id_valid = True
        super().__setitem__(key, value)

    def get(self, key, default=None):
        """Overridden to return references."""
        try:
            return self[key]
        except KeyError:
            return default

    def update(self, other):
        """Overridden to update the item status and also to invalidate references that become obsolete."""
        if self._status == Status.committed:
            self._status = Status.to_update
            self._backup = self._asdict()
        elif self._status in (Status.to_remove, Status.added_and_removed):
            raise RuntimeError("invalid status of item being updated")
        for src_key, (ref_type, _ref_key) in self._references.values():
            ref_id = self[src_key]
            if src_key in other and other[src_key] != ref_id:
                # Invalidate references
                if isinstance(ref_id, tuple):
                    for x in ref_id:
                        self._invalidate_ref(ref_type, x)
                else:
                    self._invalidate_ref(ref_type, ref_id)
        super().update(other)
        if self._asdict() == self._backup:
            self._status = Status.committed


class PublicItem:
    def __init__(self, db_map, mapped_item):
        self._db_map = db_map
        self._mapped_item = mapped_item

    @property
    def item_type(self):
        return self._mapped_item.item_type

    def __getitem__(self, key):
        return self._mapped_item[key]

    def __eq__(self, other):
        if isinstance(other, dict):
            return self._mapped_item == other
        return super().__eq__(other)

    def __repr__(self):
        return repr(self._mapped_item)

    def __str__(self):
        return str(self._mapped_item)

    def get(self, key, default=None):
        return self._mapped_item.get(key, default)

    def is_valid(self):
        return self._mapped_item.is_valid()

    def is_committed(self):
        return self._mapped_item.is_committed()

    def _asdict(self):
        return self._mapped_item._asdict()

    def _extended(self):
        return self._mapped_item._extended()

    def update(self, **kwargs):
        self._db_map.update_item(self.item_type, id=self["id"], **kwargs)

    def remove(self):
        return self._db_map.remove_item(self.item_type, self["id"])

    def restore(self):
        return self._db_map.restore_item(self.item_type, self["id"])

    def add_update_callback(self, callback):
        self._mapped_item.update_callbacks.add(callback)

    def add_remove_callback(self, callback):
        self._mapped_item.remove_callbacks.add(callback)

    def add_restore_callback(self, callback):
        self._mapped_item.restore_callbacks.add(callback)
