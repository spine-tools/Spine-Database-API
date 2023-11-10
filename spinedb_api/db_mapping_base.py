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


class DatabaseMappingBase:
    """An in-memory mapping of a DB, mapping item types (table names), to numeric ids, to items.

    This class is not meant to be used directly. Instead, you should subclass it to fit your particular DB schema.

    When subclassing, you need to implement :meth:`item_types`, :meth:`item_factory`, and :meth:`_make_query`.
    """

    def __init__(self):
        self._mapped_tables = {}
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
            if hasattr(sq.c, key):
                qry = qry.filter(getattr(sq.c, key) == value)
            elif key in self.item_factory(item_type)._external_fields:
                src_key, key = self.item_factory(item_type)._external_fields[key]
                ref_type, ref_key = self.item_factory(item_type)._references[src_key]
                ref_sq = self._make_sq(ref_type)
                qry = qry.filter(getattr(sq.c, src_key) == getattr(ref_sq.c, ref_key), getattr(ref_sq.c, key) == value)
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
        dirty_items = []
        purged_item_types = {x for x in self.item_types() if self.mapped_table(x).purged}
        self._add_descendants(purged_item_types)
        for item_type in self._sorted_item_types:
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
            else:
                for item in mapped_table.values():
                    _ = item.is_valid()
                    if item.status == Status.to_remove:
                        to_remove.append(item)
                if to_remove:
                    # Fetch descendants, so that they are validated in next iterations of the loop.
                    # This ensures cascade removal.
                    # FIXME: We should also fetch the current item type because of multi-dimensional entities and
                    # classes which also depend on zero-dimensional ones
                    for other_item_type in self.item_types():
                        if item_type in self.item_factory(other_item_type).ref_types():
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
                if mapped_table.remove_item(item) is not None:
                    item.invalidate_id()
        return True

    def _refresh(self):
        """Clears fetch progress, so the DB is queried again."""

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
        mapped_table = self.mapped_table(item_type)
        items = []
        new_items = []
        # Add items first
        for x in chunk:
            item, new = mapped_table.add_item_from_db(x)
            if new:
                new_items.append(item)
            items.append(item)
        # Once all items are added, add the unique key values
        # Otherwise items that refer to other items that come later in the query will be seen as corrupted
        for item in new_items:
            mapped_table.add_unique(item)
        return items

    def do_fetch_all(self, item_type, **kwargs):
        self.do_fetch_more(item_type, offset=0, limit=None, **kwargs)


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
        self.wildcard_item = MappedItemBase(self._db_map, self._item_type, id=Asterisk)

    @property
    def purged(self):
        return self.wildcard_item.status == Status.to_remove

    @purged.setter
    def purged(self, purged):
        self.wildcard_item.status = Status.to_remove if purged else Status.committed

    def get(self, id_, default=None):
        id_ = self._temp_id_by_db_id.get(id_, id_)
        return super().get(id_, default)

    def _new_id(self):
        temp_id = TempId(self._item_type)

        def _callback(db_id):
            self._temp_id_by_db_id[db_id] = temp_id

        temp_id.add_resolve_callback(_callback)
        return temp_id

    def _unique_key_value_to_id(self, key, value, fetch=True):
        """Returns the id that has the given value for the given unique key, or None if not found.

        Args:
            key (tuple)
            value (tuple)
            fetch (bool): whether to fetch the DB until found.

        Returns:
            int
        """
        id_by_unique_value = self._id_by_unique_key_value.get(key, {})
        if not id_by_unique_value and fetch:
            self._db_map.do_fetch_all(self._item_type)
            id_by_unique_value = self._id_by_unique_key_value.get(key, {})
        value = tuple(tuple(x) if isinstance(x, list) else x for x in value)
        return id_by_unique_value.get(value)

    def _unique_key_value_to_item(self, key, value, fetch=True):
        return self.get(self._unique_key_value_to_id(key, value, fetch=fetch))

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
        return self.find_item_by_unique_key(item, skip_keys=skip_keys, fetch=fetch)

    def find_item_by_id(self, id_, fetch=True):
        current_item = self.get(id_, {})
        if not current_item and fetch:
            self._db_map.do_fetch_all(self._item_type)
            current_item = self.get(id_, {})
        return current_item

    def find_item_by_unique_key(self, item, skip_keys=(), fetch=True, complete=True):
        for key, value in self._db_map.item_factory(self._item_type).unique_values_for_item(item, skip_keys=skip_keys):
            current_item = self._unique_key_value_to_item(key, value, fetch=fetch)
            if current_item:
                return current_item
        if complete:
            # Maybe item is missing some key stuff, so try with a resolved and polished MappedItem too...
            mapped_item = self._make_item(item)
            error = mapped_item.resolve_internal_fields(item.keys())
            if error:
                return {}
            error = mapped_item.polish()
            if error:
                return {}
            for key, value in mapped_item.unique_key_values(skip_keys=skip_keys):
                current_item = self._unique_key_value_to_item(key, value, fetch=fetch)
                if current_item:
                    return current_item
        return {}

    def checked_item_and_error(self, item, for_update=False, skip_keys=()):
        # FIXME: The only use-case for skip_keys at the moment is that of importing scenario alternatives,
        # where we only want to match by (scen_name, alt_name) and not by (scen_name, rank)
        if for_update:
            current_item = self.find_item(item, skip_keys=skip_keys)
            if not current_item:
                return None, f"no {self._item_type} matching {item} to update"
            full_item, merge_error = current_item.merge(item)
            if full_item is None:
                return None, merge_error
        else:
            current_item = None
            full_item, merge_error = item, None
        candidate_item = self._make_item(full_item)
        error = self._prepare_item(candidate_item, current_item, item, skip_keys)
        if error:
            return None, error
        return candidate_item, merge_error

    def _prepare_item(self, candidate_item, current_item, original_item, skip_keys):
        """Prepares item for insertion or update, returns any errors.

        Args:
            candidate_item (MappedItem)
            current_item (MappedItem)
            original_item (dict)
            skip_keys (optional, tuple)

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
            for key, value in candidate_item.unique_key_values(skip_keys=skip_keys):
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
            self._id_by_unique_key_value.setdefault(key, {})[value] = id_

    def remove_unique(self, item):
        id_ = item["id"]
        for key, value in item.unique_key_values():
            id_by_value = self._id_by_unique_key_value.get(key, {})
            if id_by_value.get(value) == id_:
                del id_by_value[value]

    def _make_and_add_item(self, item):
        if not isinstance(item, MappedItemBase):
            item = self._make_item(item)
            item.polish()
        if "id" not in item or not item.is_id_valid:
            item["id"] = self._new_id()
        self[item["id"]] = item
        return item

    def add_item_from_db(self, item):
        """Adds an item fetched from the DB.

        Args:
            item (dict): item from the DB.

        Returns:
            tuple(MappedItem,bool): The mapped item and whether it hadn't been added before.
        """
        current = self.find_item_by_id(item["id"], fetch=False) or self.find_item_by_unique_key(
            item, fetch=False, complete=False
        )
        if current:
            return current, False
        item = self._make_and_add_item(item)
        if self.purged:
            # Lazy purge: instead of fetching all at purge time, we purge stuff as it comes.
            item.cascade_remove(source=self.wildcard_item)
        return item, True

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
                self.remove_unique(current_item)
                current_item.cascade_remove(source=self.wildcard_item)
            return self.wildcard_item
        self.remove_unique(item)
        item.cascade_remove()
        return item

    def restore_item(self, id_):
        if id_ is Asterisk:
            self.purged = False
            for current_item in self.values():
                self.add_unique(current_item)
                current_item.cascade_restore(source=self.wildcard_item)
            return self.wildcard_item
        current_item = self.find_item({"id": id_})
        if current_item:
            self.add_unique(current_item)
            current_item.cascade_restore()
        return current_item


class MappedItemBase(dict):
    """A dictionary that represents a db item."""

    fields = {}
    """A dictionary mapping keys to a tuple of (type, value description)"""
    _defaults = {}
    """A dictionary mapping keys to their default values"""
    _unique_keys = ()
    """A tuple where each element is itself a tuple of keys corresponding to a unique constraint"""
    _references = {}
    """A dictionary mapping source keys, to a tuple of reference item type and reference key.
    Used to access external fields.
    """
    _external_fields = {}
    """A dictionary mapping keys that are not in the original dictionary, to a tuple of source key and reference key.
    Keys in _external_fields are accessed via the reference key of the reference pointed at by the source key.
    """
    _alt_references = {}
    """A dictionary mapping source keys, to a tuple of reference item type and reference key.
    Used only to resolve internal fields at item creation.
    """
    _internal_fields = {}
    """A dictionary mapping keys that are not in the original dictionary, to a tuple of source key and reference key.
    Keys in _internal_fields are resolved to the reference key of the alternative reference pointed at by the
    source key.
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
            return tuple(x) if isinstance(x, list) else x

        return not all(_convert(self.get(key)) == _convert(value) for key, value in other.items())

    def first_invalid_key(self):
        """Goes through the ``_references`` class attribute and returns the key of the first reference
        that cannot be resolved.

        Returns:
            str or None: unresolved reference's key if any.
        """
        return next(self._invalid_keys(), None)

    def _invalid_keys(self):
        """Goes through the ``_references`` class attribute and returns the keys of the ones
        that cannot be resolved.

        Yields:
            str: unresolved keys if any.
        """
        for src_key, (ref_type, ref_key) in self._references.items():
            try:
                src_val = self[src_key]
            except KeyError:
                yield src_key
            else:
                if isinstance(src_val, tuple):
                    for x in src_val:
                        if not self._get_ref(ref_type, {ref_key: x}):
                            yield src_key
                elif not self._get_ref(ref_type, {ref_key: src_val}):
                    yield src_key

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
        for key, (src_key, target_key) in self._internal_fields.items():
            if key in skip_keys:
                continue
            error = self._do_resolve_internal_field(key, src_key, target_key)
            if error:
                return error

    def _do_resolve_internal_field(self, key, src_key, target_key):
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
        If the reference is not found, sets some flags.

        Args:
            ref_type (str): The reference's type
            key_val (dict): The reference's key and value to match
            strong (bool): True if the reference corresponds to a foreign key, False otherwise

        Returns:
            MappedItemBase or dict
        """
        mapped_table = self._db_map.mapped_table(ref_type)
        ref = mapped_table.find_item(key_val, fetch=False)
        if not ref:
            ref = mapped_table.find_item(key_val, fetch=True)
            if not ref:
                if strong:
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
        if self._valid is not None:
            return self._valid
        if self._removed or self._corrupted:
            return False
        self._to_remove = False
        self._corrupted = False
        for _ in self._invalid_keys():  # This sets self._to_remove and self._corrupted
            pass
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
        ext_val = self._external_fields.get(key)
        if ext_val:
            src_key, key = ext_val
            ref_type, ref_key = self._references[src_key]
            src_val = self[src_key]
            if isinstance(src_val, tuple):
                return tuple(self._get_ref(ref_type, {ref_key: x}).get(key) for x in src_val)
            return self._get_ref(ref_type, {ref_key: src_val}).get(key)
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
        for src_key, (ref_type, ref_key) in self._references.items():
            src_val = self[src_key]
            if src_key in other and other[src_key] != src_val:
                # Invalidate references
                if isinstance(src_val, tuple):
                    for x in src_val:
                        self._invalidate_ref(ref_type, {ref_key: x})
                else:
                    self._invalidate_ref(ref_type, {ref_key: src_val})
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
