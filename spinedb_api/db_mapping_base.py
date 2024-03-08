######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Database API contributors
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

from multiprocessing import RLock
from enum import Enum, unique, auto
from difflib import SequenceMatcher
from .temp_id import TempId, resolve
from .exception import SpineDBAPIError
from .helpers import Asterisk

# TODO: Implement MappedItem.pop() to do lookup?


@unique
class Status(Enum):
    """Mapped item status."""

    committed = auto()
    to_add = auto()
    to_update = auto()
    to_remove = auto()
    added_and_removed = auto()
    compromised = auto()


class DatabaseMappingBase:
    """An in-memory mapping of a DB, mapping item types (table names), to numeric ids, to items.

    This class is not meant to be used directly. Instead, you should subclass it to fit your particular DB schema.

    When subclassing, you need to implement :meth:`item_types`, :meth:`item_factory`, :meth:`_make_sq`,
    and :meth:`_query_commit_count`.
    """

    def __init__(self):
        self.closed = False
        self._mapped_tables = {}
        self._fetched = {}
        self._locks = {}
        self._commit_count = None
        item_types = self.item_types()
        self._sorted_item_types = []
        while item_types:
            item_type = item_types.pop(0)
            if self.item_factory(item_type).ref_types() & set(item_types):
                item_types.append(item_type)
            else:
                self._sorted_item_types.append(item_type)

    @staticmethod
    def item_types():
        """Returns a list of public item types from the DB mapping schema (equivalent to the table names).

        :meta private:

        Returns:
            list(str)
        """
        raise NotImplementedError()

    @staticmethod
    def all_item_types():
        """Returns a list of all item types from the DB mapping schema (equivalent to the table names).

        :meta private:

        Returns:
            list(str)
        """
        raise NotImplementedError()

    @staticmethod
    def item_factory(item_type):
        """Returns a subclass of :class:`.MappedItemBase` to make items of given type.

        :meta private:

        Args:
            item_type (str)

        Returns:
            function
        """
        raise NotImplementedError()

    def _make_query(self, item_type, **kwargs):
        """Returns a :class:`~spinedb_api.query.Query` object to fetch items of given type.

        Args:
            item_type (str)
            **kwargs: query filters

        Returns:
            :class:`~spinedb_api.query.Query` or None if the mapping is closed.
        """
        if self.closed:
            return None
        sq = self._make_sq(item_type)
        qry = self.query(sq)
        for key, value in kwargs.items():
            if isinstance(value, tuple):
                continue
            value = resolve(value)
            if hasattr(sq.c, key):
                qry = qry.filter(getattr(sq.c, key) == value)
            elif key in self.item_factory(item_type)._external_fields:
                src_key, key = self.item_factory(item_type)._external_fields[key]
                ref_type, ref_key = self.item_factory(item_type)._references[src_key]
                ref_sq = self._make_sq(ref_type)
                try:
                    qry = qry.filter(
                        getattr(sq.c, src_key) == getattr(ref_sq.c, ref_key), getattr(ref_sq.c, key) == value
                    )
                except AttributeError:
                    pass
        return qry

    def _make_sq(self, item_type):
        """Returns a :class:`~sqlalchemy.sql.expression.Alias` object representing a subquery
        to collect items of given type.

        Args:
            item_type (str)

        Returns:
            :class:`~sqlalchemy.sql.expression.Alias`
        """
        raise NotImplementedError()

    def _query_commit_count(self):
        """Returns the number of rows in the commit table in the DB.

        Returns:
            int
        """
        raise NotImplementedError()

    def make_item(self, item_type, **item):
        factory = self.item_factory(item_type)
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
        real_commit_count = self._query_commit_count()
        dirty_items = []
        purged_item_types = {x for x in self.item_types() if self.mapped_table(x).purged}
        self._add_descendants(purged_item_types)
        for item_type in self._sorted_item_types:
            self.do_fetch_all(item_type, commit_count=real_commit_count)  # To fix conflicts in add_item_from_db
            mapped_table = self.mapped_table(item_type)
            to_add = []
            to_update = []
            to_remove = []
            for item in mapped_table.valid_values():
                if item.status == Status.to_add:
                    to_add.append(item)
                elif item.status == Status.to_update:
                    to_update.append(item)
            if item_type in purged_item_types:
                to_remove.append(mapped_table.wildcard_item)
                to_remove.extend(mapped_table.values())
            else:
                for item in mapped_table.values():
                    item.validate()
                    if item.status == Status.to_remove:
                        to_remove.append(item)
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
                if mapped_table.remove_item(item) is not None:
                    item.invalidate_id()
        return True

    def _refresh(self):
        """Clears fetch progress, so the DB is queried again."""
        if self._commit_count == self._query_commit_count():
            return
        self._fetched.clear()
        for item_type in self.item_types():
            mapped_table = self.mapped_table(item_type)
            for item in mapped_table.values():
                item.handle_refresh()

    def _check_item_type(self, item_type):
        if item_type not in self.all_item_types():
            candidate = max(self.all_item_types(), key=lambda x: SequenceMatcher(None, item_type, x).ratio())
            raise SpineDBAPIError(f"Invalid item type '{item_type}' - maybe you meant '{candidate}'?")

    def mapped_table(self, item_type):
        if item_type not in self._mapped_tables:
            self._check_item_type(item_type)
            self._mapped_tables[item_type] = _MappedTable(self, item_type)
        return self._mapped_tables[item_type]

    def reset(self, *item_types):
        """Resets the mapping for given item types as if nothing was fetched from the DB or modified in the mapping.
        Any modifications in the mapping that aren't committed to the DB are lost after this.
        """
        item_types = set(self.item_types()) if not item_types else set(item_types) & set(self.item_types())
        self._add_descendants(item_types)
        for item_type in item_types:
            self._mapped_tables.pop(item_type, None)
            self._fetched.pop(item_type, None)

    def reset_purging(self):
        """Resets purging status for all item types.

        Fetching items of an item type that has been purged will automatically mark those items removed.
        Resetting the purge status lets fetched items to be added unmodified.
        """
        for mapped_table in self._mapped_tables.values():
            mapped_table.wildcard_item.status = Status.committed

    def _add_descendants(self, item_types):
        while True:
            changed = False
            for item_type in set(self.item_types()) - item_types:
                if self.item_factory(item_type).ref_types() & item_types:
                    item_types.add(item_type)
                    changed = True
            if not changed:
                break

    def get_mapped_item(self, item_type, id_, fetch=True):
        mapped_table = self.mapped_table(item_type)
        return mapped_table.find_item_by_id(id_, fetch=fetch) or {}

    def _get_next_chunk(self, item_type, offset, limit, **kwargs):
        """Gets chunk of items from the DB.

        Returns:
            list(dict): list of dictionary items.
        """
        qry = self._make_query(item_type, **kwargs)
        if not qry:
            return []
        if not limit:
            return [dict(x) for x in qry]
        return [dict(x) for x in qry.limit(limit).offset(offset)]

    def do_fetch_more(self, item_type, offset=0, limit=None, **kwargs):
        """Fetches items from the DB and adds them to the mapping.

        Args:
            item_type (str)

        Returns:
            list(MappedItem): items fetched from the DB.
        """
        chunk = self._get_next_chunk(item_type, offset, limit, **kwargs)
        if not chunk:
            return []
        real_commit_count = self._query_commit_count()
        is_db_dirty = self._get_commit_count() != real_commit_count
        if is_db_dirty:
            # We need to fetch the most recent references because their ids might have changed in the DB
            for ref_type in self.item_factory(item_type).ref_types():
                if ref_type != item_type:
                    self.do_fetch_all(ref_type, commit_count=real_commit_count)
        mapped_table = self.mapped_table(item_type)
        items = []
        new_items = []
        # Add items first
        for x in chunk:
            item, new = mapped_table.add_item_from_db(x, not is_db_dirty)
            if new:
                new_items.append(item)
            else:
                item.handle_refetch()
            items.append(item)
        # Once all items are added, add the unique key values
        # Otherwise items that refer to other items that come later in the query will be seen as corrupted
        for item in new_items:
            mapped_table.add_unique(item)
        return items

    def _get_commit_count(self):
        """Returns current commit count.

        Returns:
            int
        """
        if self._commit_count is None:
            self._commit_count = self._query_commit_count()
        return self._commit_count

    def do_fetch_all(self, item_type, commit_count=None):
        """Fetches all items of given type, but only once for each commit_count.
        In other words, the second time this method is called with the same commit_count, it does nothing.
        If not specified, commit_count defaults to the result of self._get_commit_count().

        Args:
            item_type (str)
            commit_count (int,optional)
        """
        if commit_count is None:
            commit_count = self._get_commit_count()
        with self._locks.setdefault(item_type, RLock()):
            if self._fetched.get(item_type, -1) < commit_count:
                self._fetched[item_type] = commit_count
                self.do_fetch_more(item_type, offset=0, limit=None)


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
        self._ids_by_unique_key_value = {}
        self._temp_id_lookup = {}
        self.wildcard_item = MappedItemBase(self._db_map, self._item_type, id=Asterisk)

    @property
    def purged(self):
        return self.wildcard_item.status == Status.to_remove

    @purged.setter
    def purged(self, purged):
        self.wildcard_item.status = Status.to_remove if purged else Status.committed

    def get(self, id_, default=None):
        id_ = self._temp_id_lookup.get(id_, id_)
        return super().get(id_, default)

    def _new_id(self):
        return TempId.new_unique(self._item_type, self._temp_id_lookup)

    def _unique_key_value_to_id(self, key, value, fetch=True):
        """Returns the id that has the given value for the given unique key, or None.

        Args:
            key (tuple)
            value (tuple)
            fetch (bool): whether to fetch the DB until found.

        Returns:
            int or None
        """
        value = tuple(tuple(x) if isinstance(x, list) else x for x in value)
        ids = self._ids_by_unique_key_value.get(key, {}).get(value, [])
        if not ids and fetch:
            self._db_map.do_fetch_all(self._item_type)
            ids = self._ids_by_unique_key_value.get(key, {}).get(value, [])
        return None if not ids else ids[-1]

    def _unique_key_value_to_item(self, key, value, fetch=True, valid_only=True):
        id_ = self._unique_key_value_to_id(key, value, fetch=fetch)
        mapped_item = self.get(id_)
        if mapped_item is None:
            return None
        if valid_only and not mapped_item.is_valid():
            return None
        return mapped_item

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
            return self.find_item_by_id(id_, fetch=fetch)
        return self._find_item_by_unique_key(item, skip_keys=skip_keys, fetch=fetch)

    def find_item_by_id(self, id_, fetch=True):
        current_item = self.get(id_, {})
        if not current_item and fetch:
            self._db_map.do_fetch_all(self._item_type)
            current_item = self.get(id_, {})
        return current_item

    def _find_item_by_unique_key(self, item, skip_keys=(), fetch=True, valid_only=True):
        for key, value in self._db_map.item_factory(self._item_type).unique_values_for_item(item, skip_keys=skip_keys):
            current_item = self._unique_key_value_to_item(key, value, fetch=fetch, valid_only=valid_only)
            if current_item:
                return current_item
        # Maybe item is missing some key stuff, so try with a resolved and polished MappedItem too...
        mapped_item = self._make_item(item)
        error = mapped_item.resolve_internal_fields(skip_keys=item.keys())
        if error:
            return {}
        error = mapped_item.polish()
        if error:
            return {}
        for key, value in mapped_item.unique_key_values(skip_keys=skip_keys):
            current_item = self._unique_key_value_to_item(key, value, fetch=fetch, valid_only=valid_only)
            if current_item:
                return current_item
        return {}

    def checked_item_and_error(self, item, for_update=False):
        if for_update:
            current_item = self.find_item(item)
            if not current_item:
                return None, f"no {self._item_type} matching {item} to update"
            full_item, merge_error = current_item.merge(item)
            if full_item is None:
                return None, merge_error
        else:
            current_item = None
            full_item, merge_error = item, None
        candidate_item = self._make_item(full_item)
        error = self._prepare_item(candidate_item, current_item, item)
        if error:
            return None, error
        valid_types = (type(None),) if for_update else ()
        self.check_fields(candidate_item._asdict(), valid_types=valid_types)
        return candidate_item, merge_error

    def _prepare_item(self, candidate_item, current_item, original_item):
        """Prepares item for insertion or update, returns any errors.

        Args:
            candidate_item (MappedItem)
            current_item (MappedItem)
            original_item (dict)

        Returns:
            str or None: errors if any.
        """
        error = candidate_item.resolve_internal_fields(skip_keys=original_item.keys())
        if error:
            return error
        error = candidate_item.check_mutability()
        if error:
            return error
        error = candidate_item.polish()
        if error:
            return error
        first_invalid_key = candidate_item.first_invalid_key()
        if first_invalid_key:
            return f"invalid {first_invalid_key} for {self._item_type}"
        try:
            for key, value in candidate_item.unique_key_values():
                empty = {k for k, v in zip(key, value) if v == ""}
                if empty:
                    return f"invalid empty keys {empty} for {self._item_type}"
                unique_item = self._unique_key_value_to_item(key, value)
                if unique_item not in (None, current_item) and unique_item.is_valid():
                    return f"there's already a {self._item_type} with {dict(zip(key, value))}"
        except KeyError as e:
            return f"missing {e} for {self._item_type}"

    def item_to_remove_and_error(self, id_):
        if id_ is Asterisk:
            return self.wildcard_item, None
        current_item = self.find_item({"id": id_})
        if not current_item:
            return None, None
        return current_item, current_item.check_mutability()

    def add_unique(self, item):
        id_ = item["id"]
        for key, value in item.unique_key_values():
            self._ids_by_unique_key_value.setdefault(key, {}).setdefault(value, []).append(id_)

    def remove_unique(self, item):
        id_ = item["id"]
        for key, value in item.unique_key_values():
            ids = self._ids_by_unique_key_value.get(key, {}).get(value, [])
            if id_ in ids:
                ids.remove(id_)

    def _make_and_add_item(self, item):
        if not isinstance(item, MappedItemBase):
            item = self._make_item(item)
            item.polish()
        db_id = item.pop("id", None) if item.has_valid_id else None
        item["id"] = new_id = self._new_id()
        if db_id is not None:
            new_id.resolve(db_id)
        self[new_id] = item
        return item

    def add_item_from_db(self, item, is_db_clean):
        """Adds an item fetched from the DB.

        Args:
            item (dict): item from the DB.
            is_db_clean (Bool)

        Returns:
            tuple(MappedItem,bool): A mapped item and whether it needs to be added to the unique key values dict.
        """
        mapped_item = self._find_item_by_unique_key(item, fetch=False, valid_only=False)
        if mapped_item and (is_db_clean or self._same_item(mapped_item, item)):
            mapped_item.force_id(item["id"])
            return mapped_item, False
        mapped_item = self.get(item["id"])
        if mapped_item and (is_db_clean or self._same_item(mapped_item.db_equivalent(), item)):
            return mapped_item, False
        conflicting_item = self.get(item["id"])
        if conflicting_item is not None:
            conflicting_item.handle_id_steal()
        mapped_item = self._make_and_add_item(item)
        if self.purged:
            # Lazy purge: instead of fetching all at purge time, we purge stuff as it comes.
            mapped_item.cascade_remove()
        return mapped_item, True

    def _same_item(self, mapped_item, db_item):
        """Whether the two given items have the same unique keys.

        Args:
            mapped_item (MappedItemBase): an item in the in-memory mapping
            db_item (dict): an item just fetched from the DB
        """
        db_item = self._db_map.make_item(self._item_type, **db_item)
        db_item.polish()
        return dict(mapped_item.unique_key_values()) == dict(db_item.unique_key_values())

    def check_fields(self, item, valid_types=()):
        factory = self._db_map.item_factory(self._item_type)

        def _error(key, value, valid_types):
            if key in set(factory._internal_fields) | set(factory._external_fields) | factory._private_fields | {
                "id",
                "commit_id",
            }:
                # The user seems to know what they're doing
                return
            f_dict = factory.fields.get(key)
            if f_dict is None:
                valid_args = ", ".join(factory.fields)
                return f"invalid keyword argument '{key}' for '{self._item_type}' - valid arguments are {valid_args}."
            valid_types = valid_types + (f_dict["type"],)
            if f_dict.get("optional", False):
                valid_types = valid_types + (type(None),)
            if not isinstance(value, valid_types):
                return (
                    f"invalid type for '{key}' of '{self._item_type}' - "
                    f"got {type(value).__name__}, expected {f_dict['type'].__name__}."
                )

        errors = list(filter(lambda x: x is not None, (_error(key, value, valid_types) for key, value in item.items())))
        if errors:
            raise SpineDBAPIError("\n".join(errors))

    def add_item(self, item):
        item = self._make_and_add_item(item)
        self.add_unique(item)
        item.status = Status.to_add
        return item

    def update_item(self, item):
        current_item = self.find_item(item)
        current_item.cascade_remove_unique()
        current_item.update(item)
        current_item.cascade_add_unique()
        current_item.cascade_update()
        return current_item

    def remove_item(self, item):
        if not item:
            return None
        if item is self.wildcard_item:
            self.purged = True
            for current_item in self.valid_values():
                current_item.cascade_remove()
            return self.wildcard_item
        item.cascade_remove()
        return item

    def restore_item(self, id_):
        if id_ is Asterisk:
            self.purged = False
            for current_item in self.values():
                current_item.cascade_restore()
            return self.wildcard_item
        current_item = self.find_item({"id": id_})
        if current_item:
            current_item.cascade_restore()
        return current_item


class MappedItemBase(dict):
    """A dictionary that represents a db item."""

    fields = {}
    """A dictionary mapping fields to a another dict mapping "type" to a Python type,
    "value" to a description of the value for the key, and "optional" to a bool."""
    _defaults = {}
    """A dictionary mapping fields to their default values"""
    _unique_keys = ()
    """A tuple where each element is itself a tuple of fields corresponding to a unique key"""
    _references = {}
    """A dictionary mapping source fields, to a tuple of reference item type and reference field.
    Used to access external fields.
    """
    _external_fields = {}
    """A dictionary mapping fields that are not in the original dictionary, to a tuple of source field
    and target field.
    When accessing fields in _external_fields, we first find the reference pointed at by the source field,
    and then return the target field of that reference.
    """
    _alt_references = {}
    """A dictionary mapping source fields, to a tuple of reference item type and reference fields.
    Used only to resolve internal fields at item creation.
    """
    _internal_fields = {}
    """A dictionary mapping fields that are not in the original dictionary, to a tuple of source field
    and target field.
    When resolving fields in _internal_fields, we first find the alt_reference pointed at by the source field,
    and then use the target field of that reference.
    """
    _private_fields = set()
    """A set with fields that should be ignored in validations."""
    is_protected = False

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
        self._has_valid_id = True
        self._removed = False
        self._valid = None
        self._status = Status.committed
        self._removal_source = None
        self._status_when_removed = None
        self._status_when_committed = None
        self._backup = None
        self.public_item = PublicItem(self._db_map, self)

    def handle_refetch(self):
        """Called when an equivalent item is fetched from the DB.

        1. If this item is compromised, then mark it as committed.
        2. If this item is committed, then assume the one from the DB is newer and reset the state.
           Otherwise assume *this* is newer and do nothing.
        """
        if self.status == Status.compromised:
            self.status = Status.committed
        if self.is_committed():
            self._removed = False
            self._valid = None

    def handle_refresh(self):
        """Called when the mapping is refreshed.

        If this item is committed, then set it as compromised.
        """
        if self.status == Status.committed:
            self.status = Status.compromised

    @classmethod
    def ref_types(cls):
        """Returns a set of item types that this class refers.

        Returns:
            set(str)
        """
        return set(ref_type for ref_type, _ref_key in cls._references.values())

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
    def has_valid_id(self):
        return self._has_valid_id

    def invalidate_id(self):
        """Sets id as invalid."""
        self._has_valid_id = False

    def _extended(self):
        """Returns a dict from this item's original fields plus all the references resolved statically.

        Returns:
            dict
        """
        d = self._asdict()
        d.update({key: self[key] for key in self._external_fields})
        return d

    def _asdict(self):
        """Returns a dict from this item's original fields.

        Returns:
            dict
        """
        return dict(self)

    def resolve(self):
        return {k: resolve(v) for k, v in self._asdict().items()}

    def merge(self, other):
        """Merges this item with another and returns the merged item together with any errors.
        Used for updating items.

        Args:
            other (dict): the item to merge into this.

        Returns:
            dict: merged item.
            str: error description if any.
        """
        if not self._something_to_update(other):
            # Nothing to update, that's fine
            return None, ""
        merged = {**self._extended(), **other}
        if not isinstance(merged["id"], int):
            merged["id"] = self["id"]
        return merged, ""

    def _something_to_update(self, other):
        def _convert(x):
            if isinstance(x, list):
                x = tuple(x)
            return resolve(x)

        return not all(
            _convert(self.get(key)) == _convert(value)
            for key, value in other.items()
            if value is not None
            or self.fields.get(key, {}).get("optional", False)  # Ignore mandatory fields that are None
        )

    def db_equivalent(self):
        """The equivalent of this item in the DB.

        Returns:
            MappedItemBase
        """
        if self.status == Status.to_update:
            db_item = self._db_map.make_item(self._item_type, **self.backup)
            db_item.polish()
            return db_item
        return self

    def first_invalid_key(self):
        """Goes through the ``_references`` class attribute and returns the key of the first reference
        that cannot be resolved.

        Returns:
            str or None: unresolved reference's key if any.
        """
        return next((src_key for src_key, ref in self._resolve_refs() if not ref), None)

    def _resolve_refs(self):
        """Goes through the ``_references`` class attribute and tries to resolve them.
        If successful, replace source fields referring to db-ids with the reference's TempId.

        Yields:
            tuple(str,MappedItem or None): the source field and resolved ref.
        """
        for src_key, (ref_type, ref_key) in self._references.items():
            ref = self._get_full_ref(src_key, ref_type, ref_key)
            if isinstance(ref, tuple):
                for r in ref:
                    yield src_key, r
            else:
                yield src_key, ref

    def _get_full_ref(self, src_key, ref_type, ref_key, strong=True):
        try:
            src_val = self[src_key]
        except KeyError:
            return {}
        if isinstance(src_val, tuple):
            ref = tuple(self._get_ref(ref_type, {ref_key: x}, strong=strong) for x in src_val)
            if all(ref) and ref_key == "id":
                self[src_key] = tuple(r["id"] for r in ref)
            return ref
        ref = self._get_ref(ref_type, {ref_key: src_val}, strong=strong)
        if ref and ref_key == "id":
            self[src_key] = ref["id"]
        return ref

    @classmethod
    def unique_values_for_item(cls, item, skip_keys=()):
        for key in cls._unique_keys:
            if key not in skip_keys:
                value = tuple(item.get(k) for k in key)
                if None not in value:
                    yield key, value

    def unique_key_values(self, skip_keys=()):
        """Yields tuples of unique keys and their values.

        Args:
            skip_keys: Don't yield these keys

        Yields:
            tuple(tuple,tuple): the first element is the unique key, the second is the values.
        """
        yield from self.unique_values_for_item(self, skip_keys=skip_keys)

    def resolve_internal_fields(self, skip_keys=()):
        """Goes through the ``_internal_fields`` class attribute and updates this item
        by resolving those references.
        Returns any error.

        Args:
            skip_keys (tuple): don't resolve references for these keys.

        Returns:
            str or None: error description if any.
        """
        for key in self._internal_fields:
            if key in skip_keys:
                continue
            error = self._do_resolve_internal_field(key)
            if error:
                return error

    def _do_resolve_internal_field(self, key):
        src_key, target_key = self._internal_fields[key]
        src_val = tuple(dict.pop(self, k, None) or self.get(k) for k in src_key)
        if None in src_val:
            return
        ref_type, ref_key = self._alt_references[src_key]
        mapped_table = self._db_map.mapped_table(ref_type)
        if all(isinstance(v, (tuple, list)) for v in src_val):
            refs = []
            for v in zip(*src_val):
                ref = mapped_table.find_item(dict(zip(ref_key, v)))
                if not ref:
                    return f"can't find {ref_type} with {dict(zip(ref_key, v))}"
                refs.append(ref)
            self[key] = tuple(ref[target_key] for ref in refs)
        else:
            ref = mapped_table.find_item(dict(zip(ref_key, src_val)))
            if not ref:
                return f"can't find {ref_type} with {dict(zip(ref_key, src_val))}"
            self[key] = ref[target_key]

    def polish(self):
        """Polishes this item once all it's references have been resolved. Returns any error.

        The base implementation sets defaults but subclasses can do more work if needed.

        Returns:
            str or None: error description if any.
        """
        for key, default_value in self._defaults.items():
            self.setdefault(key, default_value)
        return ""

    def check_mutability(self):
        """Called before adding, updating, or removing this item. Returns any errors that prevent that.

        Returns:
            str or None: error description if any.
        """
        return ""

    def _get_ref(self, ref_type, key_val, strong=True):
        """Collects a reference from the in-memory mapping.
        Adds this item to the reference's list of referrers if strong is True;
        or weak referrers if strong is False.

        Args:
            ref_type (str): The reference's type
            key_val (dict): The reference's key and value to match
            strong (bool): True if the reference corresponds to a foreign key, False otherwise

        Returns:
            MappedItemBase or dict
        """
        mapped_table = self._db_map.mapped_table(ref_type)
        ref = mapped_table.find_item(key_val, fetch=True)
        if not ref:
            return {}
        if strong:
            ref.add_referrer(self)
        else:
            ref.add_weak_referrer(self)
            if ref.removed:
                return {}
        return ref

    def _invalidate_ref(self, ref_type, key_val):
        """Invalidates a reference previously collected from the in-memory mapping.

        Args:
            ref_type (str): The reference's type
            key_val (dict): The reference's key and value to match
        """
        mapped_table = self._db_map.mapped_table(ref_type)
        ref = mapped_table.find_item(key_val)
        ref.remove_referrer(self)

    def is_valid(self):
        """Checks if this item has all its references.
        Removes the item from the in-memory mapping if not valid by calling ``cascade_remove``.

        Returns:
            bool
        """
        if self.status == Status.compromised:
            return False
        self.validate()
        return self._valid

    def validate(self):
        """Resolves all references and checks if the item is valid.
        The item is valid if it's not removed, has all of its references, and none of them is removed."""
        if self._valid is not None:
            return
        refs = [ref for _, ref in self._resolve_refs()]
        self._valid = not self._removed and all(ref and not ref.removed for ref in refs)
        if not self._valid:
            self.cascade_remove()

    def add_referrer(self, referrer):
        """Adds a strong referrer to this item. Strong referrers are removed, updated and restored
        in cascade with this item.

        Args:
            referrer (MappedItemBase)
        """
        key = referrer.key
        if key is None:
            return
        self._referrers[key] = self._weak_referrers.pop(key, referrer)

    def remove_referrer(self, referrer):
        """Removes a strong referrer.

        Args:
            referrer (MappedItemBase)
        """
        key = referrer.key
        if key is not None:
            self._referrers.pop(key, None)

    def add_weak_referrer(self, referrer):
        """Adds a weak referrer to this item.
        Weak referrers' update callbacks are called whenever this item changes.

        Args:
            referrer (MappedItemBase)
        """
        key = referrer.key
        if key is None:
            return
        if key not in self._referrers:
            self._weak_referrers[key] = referrer

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
        self._valid = None
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
        if self._removed:
            return
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
        """Adds item and all its referrers unique keys and ids in cascade."""
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
        self._status_when_committed = self._status
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
        source_and_target_key = self._external_fields.get(key)
        if source_and_target_key:
            source_key, target_key = source_and_target_key
            ref_type, ref_key = self._references[source_key]
            ref = self._get_full_ref(source_key, ref_type, ref_key)
            if isinstance(ref, tuple):
                return tuple(r.get(target_key) for r in ref)
            return ref.get(target_key)
        return super().__getitem__(key)

    def __setitem__(self, key, value):
        """Sets id valid if key is 'id'."""
        if key == "id":
            self._has_valid_id = True
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
        for src_key, (ref_type, ref_key) in self._references.items():
            src_val = self[src_key]
            if src_key in other and other[src_key] != src_val:
                # Invalidate references
                if isinstance(src_val, tuple):
                    for x in src_val:
                        self._invalidate_ref(ref_type, {ref_key: x})
                else:
                    self._invalidate_ref(ref_type, {ref_key: src_val})
        id_ = self["id"]
        super().update(other)
        self["id"] = id_
        if self._asdict() == self._backup:
            self._status = Status.committed

    def force_id(self, id_):
        """Makes sure this item's has the given id_, corresponding to the new id of the item
        in the DB after some external changes.

        Args:
            id_ (int): The most recent id_ of the item as fetched from the DB.
        """
        mapped_id = self["id"]
        if mapped_id == id_:
            return
        # Resolve the TempId to the new db id (and commit the item if pending)
        mapped_id.resolve(id_)
        if self.status == Status.to_add:
            self.status = Status.committed

    def handle_id_steal(self):
        """Called when a new item is fetched from the DB with this item's id."""
        self["id"].unresolve()
        # TODO: Test if the below works...
        if self.is_committed():
            self._status = self._status_when_committed
        if self._status == Status.to_update:
            self._status = Status.to_add
        elif self._status == Status.to_remove:
            self._status = Status.committed
            self._status_when_removed = Status.to_add


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

    def validate(self):
        self._mapped_item.validate()

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

    def resolve(self):
        return self._mapped_item.resolve()
