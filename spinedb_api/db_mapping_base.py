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
from __future__ import annotations
from collections.abc import Callable, Iterable, Iterator
from contextlib import suppress
from difflib import SequenceMatcher
from typing import Any, ClassVar, Optional, Type, TypedDict, Union
from .exception import SpineDBAPIError
from .helpers import Asterisk
from .mapped_item_status import Status
from .temp_id import TempId, resolve


class DatabaseMappingBase:
    """An in-memory mapping of a DB, mapping item types (table names), to numeric ids, to items.

    This class is not meant to be used directly. Instead, you should subclass it to fit your particular DB schema.

    When subclassing, you need to implement :meth:`item_types`, :meth:`item_factory`, :meth:`_make_sq`,
    and :meth:`_query_commit_count`.
    """

    def __init__(self):
        self._closed = False
        self._context_open_count = 0
        self._mapped_tables = {item_type: MappedTable(self, item_type) for item_type in self.all_item_types()}
        self._fetched = {}
        self._commit_count = None
        item_types = self.item_types()
        self._sorted_item_types = []
        while item_types:
            item_type = item_types.pop(0)
            if not self.item_factory(item_type).ref_types().isdisjoint(item_types):
                item_types.append(item_type)
            else:
                self._sorted_item_types.append(item_type)

    def __del__(self):
        self.close()

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self):
        """Closes this DB mapping."""
        self._closed = True

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

    def make_item(self, item_type: str, **item) -> MappedItemBase:
        raise NotImplementedError

    def dirty_ids(self, item_type):
        return {
            item["id"]
            for item in self._mapped_tables[item_type].valid_values()
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
        purged_item_types = {x for x in self.item_types() if self._mapped_tables[x].purged}
        self._add_descendants(purged_item_types)
        for item_type in self._sorted_item_types:
            mapped_table = self._mapped_tables[item_type]
            self.do_fetch_all(mapped_table, commit_count=real_commit_count)  # To fix conflicts in add_item_from_db
            to_add = []
            to_update = []
            to_remove = []
            for item in mapped_table.valid_values():
                if item.status == Status.to_add:
                    to_add.append(item)
                elif item.status == Status.to_update:
                    to_update.append(item)
                if item.replaced_item_waiting_for_removal is not None:
                    to_remove.append(item.replaced_item_waiting_for_removal)
                    item.replaced_item_waiting_for_removal = None
            if item_type in purged_item_types:
                to_remove.append(mapped_table.wildcard_item)
                to_remove.extend(mapped_table.values())
            else:
                for item in mapped_table.values():
                    item.validate()
                    if item.status == Status.to_remove and item.has_valid_id:
                        to_remove.append(item)
                    if item.status == Status.added_and_removed and item.replaced_item_waiting_for_removal is not None:
                        to_remove.append(item.replaced_item_waiting_for_removal)
                        item.replaced_item_waiting_for_removal = None
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
            mapped_table = self._mapped_tables[item_type]
            for item in to_remove:
                mapped_table.restore_item(item["id"])
        for item_type, to_update in to_update_by_type:
            mapped_table = self._mapped_tables[item_type]
            for item in to_update:
                merged_item, updated_fields = item.merge(item.backup)
                mapped_table.update_item(merged_item, item, updated_fields)
        for item_type, to_add in to_add_by_type:
            mapped_table = self._mapped_tables[item_type]
            for item in to_add:
                if mapped_table.remove_item(item) is not None:
                    item.invalidate_id()
        return True

    def refresh_session(self):
        """Clears fetch progress, so the DB is queried again, and committed, unchanged items."""
        changed_statuses = {Status.to_add, Status.to_update, Status.to_remove}
        for item_type in self.item_types():
            mapped_table = self._mapped_tables[item_type]
            for item in list(mapped_table.values()):
                if item.status not in changed_statuses:
                    mapped_table.remove_unique(item)
                    del mapped_table[dict.__getitem__(item, "id")]
        self._commit_count = None
        self._fetched.clear()

    def mapped_table(self, item_type: str) -> MappedTable:
        """Returns mapped table for given item type."""
        try:
            return self._mapped_tables[item_type]
        except KeyError as error:
            candidate = max(self.all_item_types(), key=lambda x: SequenceMatcher(None, item_type, x).ratio())
            raise SpineDBAPIError(f"Invalid item type '{item_type}' - maybe you meant '{candidate}'?") from error

    def reset(self, *item_types):
        """Resets the mapping for given item types as if nothing was fetched from the DB or modified in the mapping.
        Any modifications in the mapping that aren't committed to the DB are lost after this.
        """
        if not item_types:
            self._commit_count = None
            item_types = set(self.item_types())
        else:
            item_types = set(item_types) & set(self.item_types())
            self._add_descendants(item_types)
        for item_type in item_types:
            self._mapped_tables[item_type].reset()
            with suppress(KeyError):
                del self._fetched[item_type]

    def reset_purging(self):
        """Resets purging status for all item types.

        Fetching items of an item type that has been purged will automatically mark those items removed.
        Resetting the purge status lets fetched items to be added unmodified.
        """
        for mapped_table in self._mapped_tables.values():
            mapped_table.reset_purging()

    def _add_descendants(self, item_types):
        while True:
            changed = False
            for item_type in set(self.item_types()) - item_types:
                if not self.item_factory(item_type).ref_types().isdisjoint(item_types):
                    item_types.add(item_type)
                    changed = True
            if not changed:
                break

    def _get_commit_count(self) -> int:
        """Returns current commit count.

        Returns:
            int
        """
        if self._commit_count is None:
            self._commit_count = self._query_commit_count()
        return self._commit_count

    def _do_fetch_more(
        self, mapped_table: MappedTable, offset: int, limit: Optional[int], real_commit_count: Optional[int], **kwargs
    ) -> list[MappedItemBase]:
        """Fetches items from the DB and adds them to the mapping."""
        raise NotImplementedError()

    def do_fetch_all(self, mapped_table: MappedTable, commit_count: Optional[int] = None) -> list[MappedItemBase]:
        """Fetches all items of given type, but only once for each commit_count.
        In other words, the second time this method is called with the same commit_count, it does nothing.
        If not specified, commit_count defaults to the result of self._get_commit_count().
        """
        if commit_count is None:
            commit_count = self._get_commit_count()
        if self._fetched.get(mapped_table.item_type, -1) < commit_count:
            self._fetched[mapped_table.item_type] = commit_count
            return self._do_fetch_more(mapped_table, offset=0, limit=None, real_commit_count=commit_count)
        return []

    def item(self, mapped_table: MappedTable, **kwargs) -> PublicItem:
        raise NotImplementedError()


class MappedTable(dict):
    def __init__(self, db_map: DatabaseMappingBase, item_type: str, *args, **kwargs):
        """
        Args:
            db_map: the DB mapping where this mapped table belongs.
            item_type: the item type, equal to a table name
        """
        super().__init__(*args, **kwargs)
        self._db_map = db_map
        self.item_type = item_type
        self._ids_by_unique_key_value: dict[tuple[str, ...], dict[tuple[str, ...], list[TempId]]] = {}
        self._temp_id_lookup: dict[int, TempId] = {}
        self.wildcard_item = MappedItemBase(self._db_map, id=Asterisk)
        self.wildcard_item.item_type = self.item_type

    @property
    def purged(self) -> bool:
        return self.wildcard_item.status == Status.to_remove

    @purged.setter
    def purged(self, purged: bool) -> None:
        self.wildcard_item.status = Status.to_remove if purged else Status.committed

    def get(self, id_, default=None):
        id_ = self._temp_id_lookup.get(id_, id_)
        return super().get(id_, default)

    def _unique_key_value_to_item(
        self, key: tuple[str, ...], value: Any, fetch: bool = True
    ) -> Optional[MappedItemBase]:
        value = tuple(tuple(x) if isinstance(x, list) else x for x in value)
        try:
            ids = self._ids_by_unique_key_value[key][value]
        except KeyError:
            if fetch:
                self._db_map.do_fetch_all(self)
                return self._unique_key_value_to_item(key, value, fetch=False)
            ids = None
        if not ids:
            erroneous_values = dict(zip(key, value))
            raise SpineDBAPIError(f"no {self.item_type} matching {erroneous_values}")
        return self[ids[-1]]

    def valid_values(self) -> Iterator[MappedItemBase]:
        return (x for x in self.values() if x.is_valid())

    def find_item(self, item: dict, fetch: bool = True) -> MappedItemBase:
        """Returns a MappedItemBase that matches the given dictionary-item.

        Args:
            item: item's unique keys
            fetch: if True, fetch db if item is not found in-memory

        Returns:
            item
        """
        id_ = item.get("id")
        if id_ is not None:
            return self.find_item_by_id(id_, fetch=fetch)
        return self.find_item_by_unique_key(item, fetch=fetch)

    def find_item_by_id(self, id_: TempId, fetch: bool = True) -> MappedItemBase:
        current_item = self.get(id_)
        if current_item is None and fetch:
            self._db_map.do_fetch_all(self)
            current_item = self.get(id_)
        if current_item is None:
            raise SpineDBAPIError(f"no {self.item_type} with id {id_}")
        return current_item

    def _find_fully_qualified_item_by_unique_key(self, item: dict, fetch: bool) -> Optional[MappedItemBase]:
        for key, value in self._db_map.item_factory(self.item_type).unique_values_for_item(item):
            try:
                return self._unique_key_value_to_item(key, value, fetch=fetch)
            except SpineDBAPIError:
                continue
        raise SpineDBAPIError(f"no {self.item_type} matching {item}")

    def find_item_by_unique_key(self, item: dict, fetch: bool = True) -> MappedItemBase:
        try:
            return self._find_fully_qualified_item_by_unique_key(item, fetch=fetch)
        except SpineDBAPIError as error:
            # Maybe item is missing some key stuff, so try with a resolved MappedItem too...
            mapped_item = self._db_map.make_item(self.item_type, **item)
            mapped_item.resolve_internal_fields(skip_keys=tuple(item.keys()))
            for key, value in mapped_item.unique_values_for_item(mapped_item):
                try:
                    return self._unique_key_value_to_item(key, value, fetch=fetch)
                except SpineDBAPIError:
                    continue
            raise error

    def make_candidate_item(self, item: dict) -> Optional[MappedItemBase]:
        candidate_item = self._db_map.make_item(self.item_type, **item)
        self._check_required_keys(candidate_item)
        self._prepare_item(candidate_item, None, item)
        self.check_fields(candidate_item._asdict(), valid_types=())
        return candidate_item

    def check_merged_item(self, merged_item: dict, current_item: MappedItemBase, original_update: dict) -> dict:
        candidate_item = self._db_map.make_item(self.item_type, **merged_item)
        self._prepare_item(candidate_item, current_item, original_update)
        checked_item = candidate_item._asdict()
        self.check_fields(checked_item, valid_types=(type(None),))
        return checked_item

    def _check_required_keys(self, item: MappedItemBase) -> None:
        """Checks that required keys are set in given item for addition.

        Args:
            item (MappedItemBase): item to check
        """
        for required_key_set in item.required_key_combinations:
            if not any(key in item for key in required_key_set):
                raise SpineDBAPIError(f"missing {' or '.join(required_key_set)}")

    def _prepare_item(
        self, candidate_item: MappedItemBase, current_item: Optional[MappedItemBase], original_item: dict
    ) -> None:
        """Prepares item for insertion or update, raises SpineDBAPIError on error."""
        candidate_item.resolve_internal_fields(skip_keys=tuple(original_item.keys()))
        candidate_item.check_mutability()
        candidate_item.polish()
        first_invalid_key = candidate_item.first_invalid_key()
        if first_invalid_key:
            raise SpineDBAPIError(f"invalid {first_invalid_key} for {self.item_type}")
        for key, value in candidate_item.unique_values_for_item(candidate_item):
            empty = {k for k, v in zip(key, value) if v == ""}
            if empty:
                raise SpineDBAPIError(f"invalid empty keys {empty} for {self.item_type}")
            try:
                unique_item = self._unique_key_value_to_item(key, value)
            except SpineDBAPIError:
                continue
            if unique_item is not current_item and unique_item.is_valid():
                raise SpineDBAPIError(f"there's already a {self.item_type} with {dict(zip(key, value))}")

    def item_to_remove(self, id_: TempId) -> MappedItemBase:
        if id_ is Asterisk:
            return self.wildcard_item
        return self.find_item_by_id(id_)

    def add_unique(self, item: MappedItemBase) -> None:
        id_ = item["id"]
        for key, value in item.unique_values_for_item(item):
            self._ids_by_unique_key_value.setdefault(key, {}).setdefault(value, []).append(id_)

    def remove_unique(self, item: MappedItemBase) -> None:
        id_ = dict.__getitem__(item, "id")
        for key, value in item.unique_values_for_item(item):
            ids = self._ids_by_unique_key_value.get(key, {}).get(value, [])
            try:
                ids.remove(id_)
            except ValueError:
                pass

    def _make_and_add_item(self, item: Union[dict, MappedItemBase], ignore_polishing_errors: bool) -> MappedItemBase:
        if not isinstance(item, MappedItemBase):
            item = self._db_map.make_item(self.item_type, **item)
            try:
                item.polish()
            except SpineDBAPIError as error:
                if not ignore_polishing_errors:
                    raise error
        db_id = item.pop("id", None) if item.has_valid_id else None
        item["id"] = new_id = TempId.new_unique(self.item_type, self._temp_id_lookup)
        if db_id is not None:
            new_id.resolve(db_id)
        self[new_id] = item
        return item

    def add_item_from_db(self, item: dict, is_db_clean: bool) -> tuple[MappedItemBase, bool]:
        """Adds an item fetched from the DB."""
        try:
            mapped_item = self._find_fully_qualified_item_by_unique_key(item, fetch=False)
        except SpineDBAPIError:
            mapped_item = self.get(item["id"])
            if mapped_item:
                if is_db_clean or self._same_item(mapped_item.db_equivalent(), item):
                    return mapped_item, False
                mapped_item.handle_id_steal()
            mapped_item = self._make_and_add_item(item, ignore_polishing_errors=True)
            if self.purged:
                # Lazy purge: instead of fetching all at purge time, we purge stuff as it comes.
                mapped_item.cascade_remove()
            return mapped_item, True
        if is_db_clean or self._same_item(mapped_item, item):
            mapped_item.force_id(item["id"])
            if mapped_item.status == Status.to_add and (
                mapped_item.replaced_item_waiting_for_removal is None
                or mapped_item.replaced_item_waiting_for_removal["id"].db_id != item["id"]
            ):
                # We could test if the non-unique fields of mapped_item and db item are equal
                # and set status to Status.committed
                # but that is potentially complex operation (for e.g. large parameter values)
                # so we take a shortcut here and assume that mapped_item always contains modified data.
                mapped_item.status = Status.to_update
            return mapped_item, False

    def _same_item(self, mapped_item: MappedItemBase, db_item: dict) -> bool:
        """Whether the two given items have the same unique keys.

        Args:
            mapped_item: an item in the in-memory mapping
            db_item: an item just fetched from the DB
        """
        db_item = self._db_map.make_item(self.item_type, **db_item)
        db_item.polish()
        return dict(mapped_item.unique_values_for_item(mapped_item)) == dict(db_item.unique_values_for_item(db_item))

    def check_fields(self, item: dict, valid_types: tuple[Type, ...] = ()):
        factory = self._db_map.item_factory(self.item_type)
        field_union = factory.internal_external_private_fields() | {
            "id",
            "commit_id",
        }

        def _error(key: str, value: Any, valid_types: tuple[Type, ...]) -> Optional[str]:
            if key in field_union:
                # The user seems to know what they're doing
                return
            f_dict = factory.fields.get(key)
            if f_dict is None:
                valid_args = ", ".join(factory.fields)
                return f"invalid keyword argument '{key}' for '{self.item_type}' - valid arguments are {valid_args}."
            valid_types = valid_types + (f_dict["type"],)
            if f_dict.get("optional", False):
                valid_types = valid_types + (type(None),)
            if not isinstance(value, valid_types):
                return (
                    f"invalid type for '{key}' of '{self.item_type}' - "
                    f"got {type(value).__name__}, expected {f_dict['type'].__name__}"
                )

        errors = list(filter(lambda x: x is not None, (_error(key, value, valid_types) for key, value in item.items())))
        if errors:
            raise SpineDBAPIError("\n".join(errors))

    def add_item(self, item: dict) -> MappedItemBase:
        item = self._make_and_add_item(item, ignore_polishing_errors=False)
        self.add_unique(item)
        item.become_referrer()
        item.status = Status.to_add
        item.added_to_mapped_table()
        return item

    def update_item(self, item: dict, target_item: MappedItemBase, updated_fields: set[str]) -> None:
        target_item.cascade_remove_unique()
        target_item.update(item)
        target_item.cascade_add_unique()
        update_referrers = not updated_fields.issubset(target_item.fields_not_requiring_cascade_update)
        target_item.cascade_update(update_referrers)

    def remove_item(self, item: Optional[MappedItemBase]) -> Optional[MappedItemBase]:
        if not item:
            return None
        if item is self.wildcard_item:
            self.purged = True
            for current_item in self.valid_values():
                current_item.cascade_remove()
            return self.wildcard_item
        item.cascade_remove()
        return item

    def restore_item(self, id_: TempId) -> Optional[MappedItemBase]:
        if id_ is Asterisk:
            self.purged = False
            for current_item in self.values():
                current_item.cascade_restore()
            return self.wildcard_item
        try:
            current_item = self.find_item_by_id(id_)
        except SpineDBAPIError:
            return None
        current_item.cascade_restore()
        return current_item

    def reset_purging(self) -> None:
        self.wildcard_item.status = Status.committed

    def reset(self) -> None:
        self._ids_by_unique_key_value.clear()
        self._temp_id_lookup.clear()
        self.wildcard_item.status = Status.committed
        self.clear()


class FieldDict(TypedDict):
    type: Type
    value: str
    optional: Optional[bool]


class MappedItemBase(dict):
    """A dictionary that represents a db item."""

    item_type: ClassVar[str] = "not implemented"
    fields: ClassVar[dict[str:FieldDict]] = {}
    """A dictionary mapping fields to a another dict mapping "type" to a Python type,
    "value" to a description of the value for the key, and "optional" to a bool."""
    _defaults: ClassVar[dict[str, Any]] = {}
    """A dictionary mapping fields to their default values."""
    unique_keys: ClassVar[tuple[tuple[str, str], ...]] = ()
    """A tuple where each element is itself a tuple of fields corresponding to a unique key."""
    required_key_combinations: ClassVar[tuple[tuple[str, ...], ...]] = ()
    """Tuple containing tuples of required keys and their possible alternatives."""
    _references: ClassVar[dict[str, str]] = {}
    """A dictionary mapping source fields to reference item type.
    Used to access external fields.
    """
    _weak_references: ClassVar[dict[str, str]] = {}
    """A dictionary mapping source field, to a tuple of reference item type.
    Used to access external fields that may be None."""
    _soft_references: ClassVar[set[str]] = set()
    """A set of reference source fields that are OK to have no external field value."""
    _external_fields: ClassVar[dict[str, tuple[str, str]]] = {}
    """A dictionary mapping fields that are not in the original dictionary, to a tuple of source field
    and target field.
    When accessing fields in _external_fields, we first find the reference pointed at by the source field,
    and then return the target field of that reference.
    """
    _alt_references: ClassVar[dict[tuple[str, ...], tuple[str, tuple[str, ...]]]] = {}
    """A dictionary mapping source fields, to a tuple of reference item type and reference fields.
    Used only to resolve internal fields at item creation.
    """
    _internal_fields: ClassVar[dict[str, tuple[tuple[str, ...], str]]] = {}
    """A dictionary mapping fields that are not in the original dictionary, to a tuple of source field
    and target field.
    When resolving fields in _internal_fields, we first find the alt_reference pointed at by the source field,
    and then use the target field of that reference.
    """
    _private_fields: ClassVar[set[str]] = set()
    """A set with fields that should be ignored in validations."""
    _internal_external_private_fields: ClassVar[Optional[set[str]]] = None
    """Cache for the union of internal and external private fields."""
    fields_not_requiring_cascade_update: ClassVar[set[str]] = set()
    is_protected: ClassVar[bool] = False

    def __init__(self, db_map: DatabaseMappingBase, **kwargs):
        """
        Args:
            db_map: the DB where this item belongs.
            **kwargs: parameter passed to dict constructor
        """
        super().__init__(**kwargs)
        self.db_map = db_map
        self._referrers: dict[TempId, MappedItemBase] = {}
        self._weak_referrers: dict[TempId, MappedItemBase] = {}
        self.restore_callbacks: set[Callable[[MappedItemBase], bool]] = set()
        self.update_callbacks: set[Callable[[MappedItemBase], bool]] = set()
        self.remove_callbacks: set[Callable[[MappedItemBase], bool]] = set()
        self._has_valid_id = True
        self._removed = False
        self._valid: Optional[bool] = None
        self.status = Status.committed
        self._removal_source = None
        self._status_when_removed: Optional[Status] = None
        self._status_when_committed: Optional[Status] = None
        self.replaced_item_waiting_for_removal: Optional[MappedItemBase] = None
        self._backup: Optional[dict] = None
        self._referenced_value_cache = {}
        self.public_item = PublicItem(self)

    def handle_refetch(self) -> None:
        """Called when an equivalent item is fetched from the DB.

        1. If this item is committed, then assume the one from the DB is newer and reset the state.
           Otherwise, assume *this* is newer and do nothing.
        """
        if self.is_committed():
            self._removed = False
            self._valid = None

    @classmethod
    def ref_types(cls) -> set[str]:
        """Returns a set of item types that this class refers."""
        return set(cls._references.values())

    @classmethod
    def internal_external_private_fields(cls) -> set[str]:
        """Returns a union of internal, external and private fields."""
        if cls._internal_external_private_fields is None:
            cls._internal_external_private_fields = (
                set(cls._internal_fields) | set(cls._external_fields) | cls._private_fields
            )
        return cls._internal_external_private_fields

    @property
    def backup(self) -> Optional[dict]:
        """Returns the committed version of this item."""
        return self._backup

    @property
    def removed(self) -> bool:
        """Returns whether this item has been removed."""
        return self._removed

    @property
    def has_valid_id(self) -> bool:
        return self._has_valid_id

    def invalidate_id(self) -> None:
        """Sets id as invalid."""
        self._has_valid_id = False

    def validate_id(self) -> None:
        """Sets id as valid"""
        self._has_valid_id = True

    def extended(self) -> dict:
        """Returns a dict from this item's original fields plus all the references resolved statically."""
        d = self._asdict()
        d.update({key: self[key] for key in self._external_fields})
        return d

    def _asdict(self) -> dict:
        """Returns a dict from this item's original fields."""
        return dict(self)

    def resolve(self) -> dict:
        return {k: resolve(v) for k, v in self._asdict().items()}

    def merge(self, other: dict) -> tuple[Optional[dict], set[str]]:
        """Merges this item with another and returns the merged item together with any errors.
        Used for updating items.

        Args:
            other: the item to merge into this.

        Returns:
            merged item or None if there was nothing to merge and set of updated fields
        """
        other = self._strip_equal_fields(other)
        if not other:
            return None, set()
        merged = {**self.extended(), **other}
        return merged, set(other)

    def _strip_equal_fields(self, other: dict) -> dict:
        def _resolved(x):
            if isinstance(x, list):
                x = tuple(x)
            return resolve(x)

        return {
            key: value
            for key, value in other.items()
            if (value is not None or self.fields.get(key, {}).get("optional", False))
            and _resolved(self.get(key)) != _resolved(value)
        }

    def db_equivalent(self) -> MappedItemBase:
        """The equivalent of this item in the DB."""
        if self.status == Status.to_update:
            db_item = self.db_map.make_item(self.item_type, **self.backup)
            db_item.polish()
            return db_item
        return self

    def first_invalid_key(self) -> str:
        """Goes through the ``_references`` class attribute and returns the key of the first reference
        that cannot be resolved.

        Returns:
            str or None: unresolved reference's key if any.
        """
        return next((src_key for src_key, ref in self._resolve_refs() if not ref), None)

    def _resolve_refs(self) -> Iterator[tuple[str, Optional[MappedItemBase]]]:
        """Goes through the ``_references`` class attribute and tries to resolve them.
        If successful, replace source fields referring to db-ids with the reference's TempId.

        Yields:
            the source field and resolved ref.
        """
        for src_key, ref_type in self._references.items():
            ref = self._get_full_ref(src_key, ref_type)
            if not ref and src_key in self._soft_references:
                continue
            if isinstance(ref, tuple):
                for r in ref:
                    yield src_key, r
            else:
                yield src_key, ref

    def _get_full_ref(self, src_key, ref_type) -> Optional[Union[tuple[MappedItemBase, ...], MappedItemBase]]:
        try:
            src_val = self[src_key]
        except KeyError:
            return None
        if src_val is None:
            return None
        find_by_id = self.db_map.mapped_table(ref_type).find_item_by_id
        if isinstance(src_val, tuple):
            try:
                ref = tuple(find_by_id(x) for x in src_val)
            except SpineDBAPIError:
                return None
            self[src_key] = tuple(dict.__getitem__(r, "id") for r in ref)
            return ref
        try:
            ref = find_by_id(src_val)
        except SpineDBAPIError:
            return None
        self[src_key] = dict.__getitem__(ref, "id")
        return ref

    @classmethod
    def unique_values_for_item(cls, item: MappedItemBase) -> Iterable[tuple[tuple[str, ...], tuple[str, ...]]]:
        for key_set in cls.unique_keys:
            try:
                yield key_set, tuple(item[key] for key in key_set)
            except KeyError:
                continue

    def resolve_internal_fields(self, skip_keys: tuple[str, ...] = ()) -> None:
        """Goes through the ``_internal_fields`` class attribute and updates this item
        by resolving those references.
        Returns any error.

        Args:
            skip_keys: don't resolve references for these keys.
        """
        for key in self._internal_fields:
            if key in skip_keys:
                continue
            self._do_resolve_internal_field(key)
        self._referenced_value_cache.clear()

    def _do_resolve_internal_field(self, key: str) -> None:
        src_key, target_key = self._internal_fields[key]
        src_val = tuple(dict.pop(self, k, None) or self.get(k) for k in src_key)
        if None in src_val:
            return
        ref_type, ref_key = self._alt_references[src_key]
        mapped_table = self.db_map.mapped_table(ref_type)
        if all(isinstance(v, (tuple, list)) for v in src_val):
            refs = []
            for v in zip(*src_val):
                ref = mapped_table.find_item(dict(zip(ref_key, v)))
                refs.append(ref)
            self[key] = tuple(ref[target_key] for ref in refs)
        else:
            ref = mapped_table.find_item(dict(zip(ref_key, src_val)))
            self[key] = ref[target_key]

    def polish(self) -> None:
        """Polishes this item once all it's references have been resolved.

        Raises any errors.

        The base implementation sets defaults but subclasses can do more work if needed.
        """
        for key, default_value in self._defaults.items():
            self.setdefault(key, default_value)

    def check_mutability(self) -> None:
        """Called before adding or updating this item.

        Raises any errors that prevent the operation."""

    def is_valid(self) -> bool:
        """Checks if this item has all its references.

        Removes the item from the in-memory mapping if not valid by calling ``cascade_remove``.
        """
        self.validate()
        return self._valid

    def validate(self) -> None:
        """Resolves all references and checks if the item is valid.

        Removes the item from the in-memory mapping if not valid by calling ``cascade_remove``.

        The item is valid if it or any of its references have not been removed."""
        if self._valid is not None:
            return
        refs = [ref for _, ref in self._resolve_refs() if ref]
        self._valid = not self._removed and all(not ref.removed for ref in refs)
        if not self._valid:
            self.cascade_remove()

    def add_referrer(self, referrer: MappedItemBase) -> None:
        """Adds a strong referrer to this item.

        Strong referrers are removed, updated and restored in cascade with this item.
        """
        try:
            id_ = dict.__getitem__(referrer, "id")
        except KeyError as error:
            raise RuntimeError("referrer is missing id") from error
        self._referrers[id_] = referrer

    def remove_referrer(self, referrer: MappedItemBase) -> None:
        """Removes a strong referrer."""
        try:
            id_ = dict.__getitem__(referrer, "id")
        except KeyError:
            return
        self._referrers.pop(id_, None)

    def add_weak_referrer(self, referrer: MappedItemBase) -> None:
        """Adds a weak referrer to this item.
        Weak referrers' update callbacks are called whenever this item changes.
        """
        try:
            id_ = dict.__getitem__(referrer, "id")
        except KeyError as error:
            raise RuntimeError("weak referrer is missing id") from error
        self._weak_referrers[(referrer.item_type, id_)] = referrer

    def _update_weak_referrers(self) -> None:
        for weak_referrer in self._weak_referrers.values():
            weak_referrer.call_update_callbacks()

    def become_referrer(self) -> None:
        def add_self_as_referrer(ref_id):
            ref = mapped_table.get(ref_id)
            if ref is None:
                raise RuntimeError(f"Reference id {ref_id} in '{ref_table}' table not found")
            ref.add_referrer(self)

        for field, ref_table in self._references.items():
            mapped_table = self.db_map.mapped_table(ref_table)
            field_value = self[field]
            if not field_value:
                return
            if isinstance(field_value, tuple):
                for id_ in field_value:
                    add_self_as_referrer(id_)
            else:
                add_self_as_referrer(field_value)
        for field, ref_table in self._weak_references.items():
            try:
                id_ = self[field]
            except KeyError:
                continue
            if not id_:
                continue
            try:
                ref = self.db_map.mapped_table(ref_table)[id_]
            except KeyError:
                continue
            ref.add_weak_referrer(self)

    def cascade_restore(self, source: Optional[object] = None) -> None:
        """Restores this item (if removed) and all its referrers in cascade.
        Also, updates items' status and calls their restore callbacks.
        """
        self._referenced_value_cache.clear()
        if not self._removed:
            return
        if source is not self._removal_source:
            return
        if self.status in (Status.added_and_removed, Status.to_remove):
            self.status = self._status_when_removed
        elif self.status == Status.committed:
            self.status = Status.to_add
        else:
            raise RuntimeError("invalid status for item being restored")
        self._removed = False
        self._valid = None
        # First restore this, then referrers
        obsolete = set()
        for callback in self.restore_callbacks:
            if not callback(self):
                obsolete.add(callback)
        self.restore_callbacks -= obsolete
        for referrer in self._referrers.values():
            referrer.cascade_restore(source=self)
        self._update_weak_referrers()

    def cascade_remove(self, source: Optional[object] = None) -> None:
        """Removes this item and all its referrers in cascade.
        Also, updates items' status and calls their remove callbacks.
        """
        if self._removed:
            return
        self._status_when_removed = self.status
        if self.status == Status.to_add:
            self.status = Status.added_and_removed
        elif self.status in (Status.committed, Status.to_update):
            self.status = Status.to_remove
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
        for callback in self.remove_callbacks:
            if not callback(self):
                obsolete.add(callback)
        self.remove_callbacks -= obsolete

    def cascade_update(self, update_referrers: bool) -> None:
        """Updates this item and optionally all its referrers in cascade.
        Also, calls items' update callbacks.
        """
        if self._removed:
            return
        self._referenced_value_cache.clear()
        self.call_update_callbacks()
        if update_referrers:
            for referrer in self._referrers.values():
                referrer.cascade_update(True)
            self._update_weak_referrers()

    def call_update_callbacks(self) -> None:
        obsolete = set()
        for callback in self.update_callbacks:
            if not callback(self):
                obsolete.add(callback)
        self.update_callbacks -= obsolete

    def cascade_add_unique(self) -> None:
        """Adds item and all its referrers unique keys and ids in cascade."""
        self._referenced_value_cache.clear()
        mapped_table = self.db_map.mapped_table(self.item_type)
        mapped_table.add_unique(self)
        for referrer in self._referrers.values():
            referrer.cascade_add_unique()

    def cascade_remove_unique(self) -> None:
        """Removes item and all its referrers unique keys and ids in cascade."""
        mapped_table = self.db_map.mapped_table(self.item_type)
        mapped_table.remove_unique(self)
        for referrer in self._referrers.values():
            referrer.cascade_remove_unique()

    def is_committed(self) -> bool:
        """Returns whether this item is committed to the DB."""
        return self.status == Status.committed

    def commit(self, commit_id) -> None:
        """Sets this item as committed with the given commit id."""
        self._status_when_committed = self.status
        self.status = Status.committed
        if commit_id:
            self["commit_id"] = commit_id

    def __repr__(self):
        """Overridden to return a more verbose representation."""
        return f"{self.item_type}{self.extended()}"

    def __getitem__(self, key):
        """Overridden to return references."""
        source_and_target_key = self._external_fields.get(key)
        if source_and_target_key:
            if source_and_target_key in self._referenced_value_cache:
                return self._referenced_value_cache[source_and_target_key]
            source_key, target_key = source_and_target_key
            ref_type = self._references[source_key]
            ref = self._get_full_ref(source_key, ref_type)
            if isinstance(ref, tuple):
                value = tuple(r[target_key] for r in ref)
                self._referenced_value_cache[source_and_target_key] = value
                return value
            value = ref[target_key] if ref is not None else None
            self._referenced_value_cache[source_and_target_key] = value
            return value
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

        def invalidate_ref(ref_id):
            ref = find_by_id(ref_id)
            ref.remove_referrer(self)

        if self.status == Status.committed:
            self.status = Status.to_update
            self._backup = self._asdict()
        elif self.status in (Status.to_remove, Status.added_and_removed):
            raise RuntimeError("invalid status of item being updated")
        for src_key, ref_type in self._references.items():
            find_by_id = self.db_map.mapped_table(ref_type).find_item_by_id
            src_val = self[src_key]
            if src_val is None and src_key in self._soft_references:
                continue
            if src_key in other and other[src_key] != src_val:
                # Invalidate references
                if isinstance(src_val, tuple):
                    for id_ in src_val:
                        invalidate_ref(id_)
                else:
                    invalidate_ref(src_val)
        del other["id"]
        super().update(other)
        if self._backup is not None:
            backup = self._backup
            as_dict = self._asdict()
            if as_dict is not None and all(as_dict[key] == backup[key] for key in backup):
                self.status = Status.committed

    def force_id(self, id_: TempId) -> None:
        """Makes sure this item's has the given id_, corresponding to the new id of the item
        in the DB after some external changes.

        Args:
            id_: The most recent id_ of the item as fetched from the DB.
        """
        mapped_id = dict.__getitem__(self, "id")
        if mapped_id == id_:
            return
        mapped_id.resolve(id_)

    def handle_id_steal(self) -> None:
        """Called when a new item is fetched from the DB with this item's id."""
        dict.__getitem__(self, "id").unresolve()
        # TODO: Test if the below works...
        if self.is_committed():
            self.status = self._status_when_committed
        if self.status == Status.to_update:
            self.status = Status.to_add
        elif self.status == Status.to_remove:
            self.status = Status.committed
            self._status_when_removed = Status.to_add

    def added_to_mapped_table(self) -> None:
        """Called after the item has been added to a mapped table as a new item (not after fetching etc.)."""


class PublicItem:
    def __init__(self, mapped_item: MappedItemBase):
        self._mapped_item = mapped_item

    @property
    def item_type(self) -> str:
        return self._mapped_item.item_type

    @property
    def mapped_item(self) -> MappedItemBase:
        return self._mapped_item

    @property
    def db_map(self) -> DatabaseMappingBase:
        return self._mapped_item.db_map

    def __getitem__(self, key):
        return self._mapped_item[key]

    def __contains__(self, item):
        return self._mapped_item.extended().__contains__(item)

    def __eq__(self, other):
        if isinstance(other, dict):
            return self._mapped_item == other
        return super().__eq__(other)

    def __repr__(self):
        return repr(self._mapped_item)

    def __str__(self):
        return str(self._mapped_item)

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        return self._mapped_item.get(key, default)

    def validate(self) -> None:
        self._mapped_item.validate()

    def is_valid(self) -> bool:
        return self._mapped_item.is_valid()

    def is_committed(self) -> bool:
        return self._mapped_item.is_committed()

    def _asdict(self) -> dict:
        return self._mapped_item._asdict()

    def extended(self) -> dict:
        return self._mapped_item.extended()

    def update(self, **kwargs) -> Optional[PublicItem]:
        mapped_table = self._mapped_item.db_map.mapped_table(self._mapped_item.item_type)
        return self._mapped_item.db_map.update(mapped_table, id=dict.__getitem__(self._mapped_item, "id"), **kwargs)

    def remove(self) -> None:
        db_map = self._mapped_item.db_map
        db_map.remove(db_map.mapped_table(self.item_type), id=dict.__getitem__(self._mapped_item, "id"))

    def restore(self) -> PublicItem:
        mapped_table = self._mapped_item.db_map.mapped_table(self.item_type)
        if not self._mapped_item.has_valid_id:
            try:
                existing_item = mapped_table.find_item_by_unique_key(self._mapped_item, fetch=False)
            except SpineDBAPIError:
                pass
            else:
                if not existing_item.removed:
                    raise SpineDBAPIError("restoring would create a conflict with another item with same unique values")
                existing_item.invalidate_id()
                mapped_table.remove_unique(existing_item)
                self._mapped_item.validate_id()
                mapped_table.add_unique(self._mapped_item)
        return self._mapped_item.db_map.restore(mapped_table, id=dict.__getitem__(self._mapped_item, "id"))

    def add_update_callback(self, callback: Callable[[MappedItemBase], bool]) -> None:
        self._mapped_item.update_callbacks.add(callback)

    def add_remove_callback(self, callback: Callable[[MappedItemBase], bool]) -> None:
        self._mapped_item.remove_callbacks.add(callback)

    def add_restore_callback(self, callback: Callable[[MappedItemBase], bool]) -> None:
        self._mapped_item.restore_callbacks.add(callback)

    def resolve(self) -> dict:
        return self._mapped_item.resolve()
