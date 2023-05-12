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
DB cache utility.

"""
import uuid
from contextlib import suppress
from operator import itemgetter
from enum import Enum, unique, auto
from .parameter_value import from_database

# TODO: Implement CacheItem.pop() to do lookup?


@unique
class Status(Enum):
    """Cache item status."""

    committed = auto()
    to_add = auto()
    to_update = auto()
    to_remove = auto()


class DBCache(dict):
    """A dictionary that maps table names to ids to items. Used to store and retrieve database contents."""

    def __init__(self, db_map, chunk_size=None):
        """
        Args:
            db_map (DatabaseMapping)
        """
        super().__init__()
        self._db_map = db_map
        self._offsets = {}
        self._fetched_item_types = set()
        self._chunk_size = chunk_size

    def commit(self):
        to_add = {}
        to_update = {}
        to_remove = {}
        for item_type, table_cache in self.items():
            for item in dict.values(table_cache):
                if item.status == Status.to_add:
                    to_add.setdefault(item_type, []).append(item)
                elif item.status == Status.to_update:
                    to_update.setdefault(item_type, []).append(item)
                elif item.status == Status.to_remove:
                    to_remove.setdefault(item_type, set()).add(item["id"])
                item.status = Status.committed
        # FIXME: When computing to_remove, we could at the same time fetch all tables where items should be removed
        # in cascade. This could be nice. So we would visit the tables in order, collect removed items,
        # and if we find some then we would fetch all the descendant tables and validate items in them.
        # This would set the removed flag, and then we would be able to collect those items
        # in subsequent iterations.
        # This might solve the issue when the user removes, commits, and then undoes the removal.
        # My impression is since committing the removal action would fetch all the referrers, then it would
        # be possible to properly undo it. Maybe that is the case already because `cascading_ids()`
        # also fetches all the descendant tablenams into cache.
        # Actually, it looks like all we're missing is setting the new attribute for restored items too??!!
        # Ok so when you restore and item whose removal was committed, you need to set new to True

        # Another option would be to build a list of fetched ids in a fully independent dictionary.
        # Then we could compare contents of the cache with this list and easily find out which items need
        # to be added, updated and removed.
        # To add: Those that are valid in the cache but not in fetched id
        # To update: Those that are both valid in the cache and in fetched id
        # To remove: Those that are in fetched id but not valid in the cache.
        # But this would require fetching the entire DB before committing or something like that... To think about it.
        return to_add, to_update, to_remove

    @property
    def fetched_item_types(self):
        return self._fetched_item_types

    def reset_queries(self):
        """Resets queries and clears caches."""
        self._offsets.clear()
        self._fetched_item_types.clear()

    def advance_query(self, item_type):
        """Schedules an advance of the DB query that fetches items of given type.

        Args:
            item_type (str)

        Returns:
            Future
        """
        return self._db_map.executor.submit(self.do_advance_query, item_type)

    def _get_next_chunk(self, item_type):
        try:
            sq_name = {
                "entity_class": "wide_entity_class_sq",
                "entity": "wide_entity_sq",
                "parameter_value_list": "parameter_value_list_sq",
                "list_value": "list_value_sq",
                "alternative": "alternative_sq",
                "scenario": "scenario_sq",
                "scenario_alternative": "scenario_alternative_sq",
                "entity_group": "entity_group_sq",
                "parameter_definition": "parameter_definition_sq",
                "parameter_value": "parameter_value_sq",
                "metadata": "metadata_sq",
                "entity_metadata": "entity_metadata_sq",
                "parameter_value_metadata": "parameter_value_metadata_sq",
                "commit": "commit_sq",
            }[item_type]
            qry = self._db_map.query(getattr(self._db_map, sq_name))
        except KeyError:
            return []
        if not self._chunk_size:
            self._fetched_item_types.add(item_type)
            return [dict(x) for x in qry]
        offset = self._offsets.setdefault(item_type, 0)
        chunk = [dict(x) for x in qry.limit(self._chunk_size).offset(offset)]
        self._offsets[item_type] += len(chunk)
        return chunk

    def do_advance_query(self, item_type):
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
        for item in chunk:
            # FIXME: This will overwrite working changes after a refresh
            table_cache.add_item(item)
        return chunk

    def table_cache(self, item_type):
        return self.setdefault(item_type, TableCache(self, item_type))

    def get_item(self, item_type, id_):
        table_cache = self.get(item_type, {})
        item = table_cache.get(id_)
        if item is None:
            return {}
        return item

    def fetch_more(self, item_type):
        if item_type in self._fetched_item_types:
            return False
        return bool(self.do_advance_query(item_type))

    def fetch_all(self, item_type):
        while self.fetch_more(item_type):
            pass

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


class TableCache(dict):
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

    def unique_key_value_to_id(self, key, value, strict=False):
        """Returns the id that has the given value for the given unique key, or None.

        Args:
            key (tuple)
            value (tuple)

        Returns:
            int
        """
        value = tuple(tuple(x) if isinstance(x, list) else x for x in value)
        self._db_cache.fetch_all(self._item_type)
        id_by_unique_value = self._id_by_unique_key_value.get(key, {})
        if strict:
            return id_by_unique_value[value]
        return id_by_unique_value.get(value)

    def _unique_key_value_to_item(self, key, value, strict=False):
        return self.get(self.unique_key_value_to_id(key, value))

    def values(self):
        return (x for x in super().values() if x.is_valid())

    @property
    def _item_factory(self):
        return {
            "entity_class": EntityClassItem,
            "entity": EntityItem,
            "entity_group": EntityGroupItem,
            "parameter_definition": ParameterDefinitionItem,
            "parameter_value": ParameterValueItem,
            "list_value": ListValueItem,
            "alternative": AlternativeItem,
            "scenario": ScenarioItem,
            "scenario_alternative": ScenarioAlternativeItem,
            "metadata": MetadataItem,
            "entity_metadata": EntityMetadataItem,
            "parameter_value_metadata": ParameterValueMetadataItem,
        }.get(self._item_type, CacheItem)

    def _make_item(self, item):
        """Returns a cache item.

        Args:
            item (dict): the 'db item' to use as base

        Returns:
            CacheItem
        """
        return self._item_factory(self._db_cache, self._item_type, **item)

    def current_item(self, item, skip_keys=()):
        id_ = item.get("id")
        if isinstance(id_, int):
            # id is an int, easy
            return self.get(id_)
        if isinstance(id_, dict):
            # id is a dict specifying the values for one of the unique constraints
            key, value = zip(*id_.items())
            return self._unique_key_value_to_item(key, value)
        if id_ is None:
            # No id. Try to locate the item by the value of one of the unique keys. Used by import_data.
            item = self._make_item(item)
            error = item.resolve_inverse_references()
            if error:
                return None
            error = item.polish()
            if error:
                return None
            for key, value in item.unique_values(skip_keys=skip_keys):
                current_item = self._unique_key_value_to_item(key, value)
                if current_item:
                    return current_item

    def check_item(self, item, for_update=False, skip_keys=()):
        if for_update:
            current_item = self.current_item(item, skip_keys=skip_keys)
            if current_item is None:
                return None, f"no {self._item_type} matching {item} to update"
            item = {**current_item, **item}
            item["id"] = current_item["id"]
        else:
            current_item = None
        candidate_item = self._make_item(item)
        error = candidate_item.resolve_inverse_references()
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
        return candidate_item, None

    def _add_unique(self, item):
        for key, value in item.unique_values():
            self._id_by_unique_key_value.setdefault(key, {})[value] = item["id"]

    def _remove_unique(self, item):
        for key, value in item.unique_values():
            self._id_by_unique_key_value.get(key, {}).pop(value, None)

    def add_item(self, item, new=False):
        self[item["id"]] = new_item = self._make_item(item)
        self._add_unique(new_item)
        if new:
            new_item.status = Status.to_add
        return new_item

    def update_item(self, item):
        current_item = self[item["id"]]
        self._remove_unique(current_item)
        current_item.update(item)
        self._add_unique(current_item)
        current_item.cascade_update()
        if current_item.status != Status.to_add:
            current_item.status = Status.to_update
        return current_item

    def remove_item(self, id_):
        current_item = self.get(id_)
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


class CacheItem(dict):
    """A dictionary that represents an db item."""

    _defaults = {}
    _unique_keys = (("name",),)
    _references = {}
    _inverse_references = {}

    def __init__(self, db_cache, item_type, *args, **kwargs):
        """
        Args:
            db_cache (DBCache): the DB cache where this item belongs.
        """
        super().__init__(*args, **kwargs)
        self._db_cache = db_cache
        self._item_type = item_type
        self._referrers = {}
        self._weak_referrers = {}
        self.restore_callbacks = set()
        self.update_callbacks = set()
        self.remove_callbacks = set()
        self._to_remove = False
        self._removed = False
        self._corrupted = False
        self._valid = None
        self.status = Status.committed

    def is_committed(self):
        return self.status == Status.committed

    def polish(self):
        """Polishes this item once all it's references are resolved. Returns any errors.

        Returns:
            str or None
        """
        for key, default_value in self._defaults.items():
            self.setdefault(key, default_value)
        return ""

    def resolve_inverse_references(self):
        for src_key, (id_key, (ref_type, ref_key)) in self._inverse_references.items():
            if dict.get(self, src_key):
                # When updating items, the user might update the id keys while leaving the name keys intact.
                # In this case we shouldn't overwrite the updated id keys from the obsolete name keys.
                # FIXME: It feels that this is our fault, though, like it is us who keep the obsolete name keys around.
                continue
            id_value = tuple(dict.get(self, k) or self.get(k) for k in id_key)
            if None in id_value:
                continue
            table_cache = self._db_cache.table_cache(ref_type)
            try:
                src_value = (
                    tuple(table_cache.unique_key_value_to_id(ref_key, v, strict=True) for v in zip(*id_value))
                    if all(isinstance(v, (tuple, list)) for v in id_value)
                    else table_cache.unique_key_value_to_id(ref_key, id_value, strict=True)
                )
                self[src_key] = src_value
            except KeyError as err:
                # Happens at unique_key_value_to_id(..., strict=True)
                return f"can't find {ref_type} with {dict(zip(ref_key, err.args[0]))}"

    def invalid_ref(self):
        for key, (ref_type, _ref_key) in self._references.values():
            try:
                ref_id = self[key]
            except KeyError:
                return key
            if isinstance(ref_id, tuple):
                for x in ref_id:
                    if not self._get_ref(ref_type, x):
                        return key
            elif not self._get_ref(ref_type, ref_id):
                return key

    def unique_values(self, skip_keys=()):
        for key in self._unique_keys:
            if key not in skip_keys:
                yield key, tuple(self.get(k) for k in key)

    @property
    def removed(self):
        return self._removed

    @property
    def item_type(self):
        return self._item_type

    @property
    def key(self):
        if dict.get(self, "id") is None:
            return None
        return (self._item_type, self["id"])

    def __getattr__(self, name):
        """Overridden method to return the dictionary key named after the attribute, or None if it doesn't exist."""
        return self.get(name)

    def __repr__(self):
        return f"{self._item_type}{self._extended()}"

    def _extended(self):
        return {**self, **{key: self[key] for key in self._references}}

    def _asdict(self):
        return dict(**self)

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

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

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

    def add_weak_referrer(self, referrer):
        if referrer.key is None:
            return
        if referrer.key not in self._referrers:
            self._weak_referrers[referrer.key] = referrer

    def cascade_restore(self):
        if self.status == Status.committed:
            self.status = Status.to_add
        if not self._removed:
            return
        self._removed = False
        for referrer in self._referrers.values():
            referrer.cascade_restore()
        for weak_referrer in self._weak_referrers.values():
            weak_referrer.call_update_callbacks()
        obsolete = set()
        for callback in self.restore_callbacks:
            if not callback(self):
                obsolete.add(callback)
        self.restore_callbacks -= obsolete

    def cascade_remove(self):
        self.status = Status.to_remove
        if self._removed:
            return
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
        for weak_referrer in self._weak_referrers.values():
            weak_referrer.call_update_callbacks()

    def cascade_update(self):
        self.call_update_callbacks()
        for weak_referrer in self._weak_referrers.values():
            weak_referrer.call_update_callbacks()
        for referrer in self._referrers.values():
            referrer.cascade_update()

    def call_update_callbacks(self):
        self.pop("parsed_value", None)
        obsolete = set()
        for callback in self.update_callbacks:
            if not callback(self):
                obsolete.add(callback)
        self.update_callbacks -= obsolete

    def __getitem__(self, key):
        ref = self._references.get(key)
        if ref:
            key, (ref_type, ref_key) = ref
            ref_id = self[key]
            if isinstance(ref_id, tuple):
                return tuple(self._get_ref(ref_type, x).get(ref_key) for x in ref_id)
            return self._get_ref(ref_type, ref_id).get(ref_key)
        return super().__getitem__(key)


class EntityClassItem(CacheItem):
    _defaults = {"description": None, "display_icon": None}
    _references = {"dimension_name_list": ("dimension_id_list", ("entity_class", "name"))}
    _inverse_references = {"dimension_id_list": (("dimension_name_list",), ("entity_class", ("name",)))}

    def __init__(self, *args, **kwargs):
        dimension_id_list = kwargs.get("dimension_id_list")
        if dimension_id_list is None:
            dimension_id_list = ()
        if isinstance(dimension_id_list, str):
            dimension_id_list = (int(id_) for id_ in dimension_id_list.split(","))
        kwargs["dimension_id_list"] = tuple(dimension_id_list)
        super().__init__(*args, **kwargs)


class EntityItem(CacheItem):
    _defaults = {"description": None}
    _unique_keys = (("class_name", "name"), ("class_name", "byname"))
    _references = {
        "class_name": ("class_id", ("entity_class", "name")),
        "dimension_id_list": ("class_id", ("entity_class", "dimension_id_list")),
        "dimension_name_list": ("class_id", ("entity_class", "dimension_name_list")),
        "element_name_list": ("element_id_list", ("entity", "name")),
    }
    _inverse_references = {
        "class_id": (("class_name",), ("entity_class", ("name",))),
        "element_id_list": (("dimension_name_list", "element_name_list"), ("entity", ("class_name", "name"))),
    }

    def __init__(self, *args, **kwargs):
        element_id_list = kwargs.get("element_id_list")
        if element_id_list is None:
            element_id_list = ()
        if isinstance(element_id_list, str):
            element_id_list = (int(id_) for id_ in element_id_list.split(","))
        kwargs["element_id_list"] = tuple(element_id_list)
        super().__init__(*args, **kwargs)

    def __getitem__(self, key):
        if key == "byname":
            return self["element_name_list"] or (self["name"],)
        return super().__getitem__(key)

    def polish(self):
        error = super().polish()
        if error:
            return error
        if "name" in self:
            return
        base_name = self["class_name"] + "_" + "__".join(self["element_name_list"])
        name = base_name
        table_cache = self._db_cache.table_cache(self._item_type)
        while table_cache.unique_key_value_to_id(("class_name", "name"), (self["class_name"], name)) is not None:
            name = base_name + "_" + uuid.uuid4().hex
        self["name"] = name


class EntityGroupItem(CacheItem):
    _unique_keys = (("group_name", "member_name"),)
    _references = {
        "class_name": ("entity_class_id", ("entity_class", "name")),
        "group_name": ("entity_id", ("entity", "name")),
        "member_name": ("member_id", ("entity", "name")),
        "dimension_id_list": ("entity_class_id", ("entity_class", "dimension_id_list")),
    }
    _inverse_references = {
        "entity_class_id": (("class_name",), ("entity_class", ("name",))),
        "entity_id": (("class_name", "group_name"), ("entity", ("class_name", "name"))),
        "member_id": (("class_name", "member_name"), ("entity", ("class_name", "name"))),
    }

    def __getitem__(self, key):
        if key == "class_id":
            return self["entity_class_id"]
        if key == "group_id":
            return self["entity_id"]
        return super().__getitem__(key)


class ParameterDefinitionItem(CacheItem):
    _defaults = {"description": None, "default_value": None, "default_type": None, "parameter_value_list_id": None}
    _unique_keys = (("entity_class_name", "name"),)
    _references = {
        "entity_class_name": ("entity_class_id", ("entity_class", "name")),
        "dimension_id_list": ("entity_class_id", ("entity_class", "dimension_id_list")),
        "dimension_name_list": ("entity_class_id", ("entity_class", "dimension_name_list")),
    }
    _inverse_references = {
        "entity_class_id": (("entity_class_name",), ("entity_class", ("name",))),
        "parameter_value_list_id": (("parameter_value_list_name",), ("parameter_value_list", ("name",))),
    }

    @property
    def list_value_id(self):
        if dict.get(self, "default_type") == "list_value_ref":
            return int(dict.__getitem__(self, "default_value"))
        return None

    def __getitem__(self, key):
        if key == "parameter_name":
            return super().__getitem__("name")
        if key == "value_list_id":
            return super().__getitem__("parameter_value_list_id")
        if key == "parameter_value_list_id":
            return dict.get(self, key)
        if key == "parameter_value_list_name":
            return self._get_ref("parameter_value_list", self["parameter_value_list_id"], strong=False).get("name")
        if key in ("default_value", "default_type"):
            list_value_id = self.list_value_id
            if list_value_id is not None:
                list_value_key = {"default_value": "value", "default_type": "type"}[key]
                return self._get_ref("list_value", list_value_id, strong=False).get(list_value_key)
            return dict.get(self, key)
        if key == "list_value_id":
            return self.list_value_id
        return super().__getitem__(key)

    def polish(self):
        error = super().polish()
        if error:
            return error
        default_type = self["default_type"]
        default_value = self["default_value"]
        list_name = self["parameter_value_list_name"]
        if list_name is None:
            return
        if default_type == "list_value_ref":
            return
        parsed_value = from_database(default_value, default_type)
        if parsed_value is None:
            return
        list_value_id = self._db_cache.table_cache("list_value").unique_key_value_to_id(
            ("parameter_value_list_name", "value", "type"), (list_name, default_value, default_type)
        )
        if list_value_id is None:
            return f"default value {parsed_value} of {self['name']} is not in {list_name}"
        self["default_value"] = str(list_value_id).encode()
        self["default_type"] = "list_value_ref"


class ParameterValueItem(CacheItem):
    _unique_keys = (("parameter_definition_name", "entity_byname", "alternative_name"),)
    _references = {
        "entity_class_name": ("entity_class_id", ("entity_class", "name")),
        "dimension_id_list": ("entity_class_id", ("entity_class", "dimension_id_list")),
        "dimension_name_list": ("entity_class_id", ("entity_class", "dimension_name_list")),
        "parameter_definition_name": ("parameter_definition_id", ("parameter_definition", "name")),
        "parameter_value_list_id": ("parameter_definition_id", ("parameter_definition", "parameter_value_list_id")),
        "parameter_value_list_name": ("parameter_definition_id", ("parameter_definition", "parameter_value_list_name")),
        "entity_name": ("entity_id", ("entity", "name")),
        "entity_byname": ("entity_id", ("entity", "byname")),
        "element_id_list": ("entity_id", ("entity", "element_id_list")),
        "element_name_list": ("entity_id", ("entity", "element_name_list")),
        "alternative_name": ("alternative_id", ("alternative", "name")),
    }
    _inverse_references = {
        "entity_class_id": (("entity_class_name",), ("entity_class", ("name",))),
        "parameter_definition_id": (
            ("entity_class_name", "parameter_definition_name"),
            ("parameter_definition", ("entity_class_name", "name")),
        ),
        "entity_id": (("entity_class_name", "entity_byname"), ("entity", ("class_name", "byname"))),
        "alternative_id": (("alternative_name",), ("alternative", ("name",))),
    }

    @property
    def list_value_id(self):
        if dict.__getitem__(self, "type") == "list_value_ref":
            return int(dict.__getitem__(self, "value"))
        return None

    def __getitem__(self, key):
        if key == "parameter_id":
            return super().__getitem__("parameter_definition_id")
        if key == "parameter_name":
            return super().__getitem__("parameter_definition_name")
        if key in ("value", "type"):
            list_value_id = self.list_value_id
            if list_value_id:
                return self._get_ref("list_value", list_value_id, strong=False).get(key)
        if key == "list_value_id":
            return self.list_value_id
        return super().__getitem__(key)

    def polish(self):
        list_name = self["parameter_value_list_name"]
        if list_name is None:
            return
        type_ = self["type"]
        if type_ == "list_value_ref":
            return
        value = self["value"]
        parsed_value = from_database(value, type_)
        if parsed_value is None:
            return
        list_value_id = self._db_cache.table_cache("list_value").unique_key_value_to_id(
            ("parameter_value_list_name", "value", "type"), (list_name, value, type_)
        )
        if list_value_id is None:
            return (
                f"value {parsed_value} of {self['parameter_definition_name']} for {self['entity_byname']} "
                "is not in {list_name}"
            )
        self["value"] = str(list_value_id).encode()
        self["type"] = "list_value_ref"


class ListValueItem(CacheItem):
    _unique_keys = (("parameter_value_list_name", "value", "type"), ("parameter_value_list_name", "index"))
    _references = {"parameter_value_list_name": ("parameter_value_list_id", ("parameter_value_list", "name"))}
    _inverse_references = {
        "parameter_value_list_id": (("parameter_value_list_name",), ("parameter_value_list", ("name",))),
    }


class AlternativeItem(CacheItem):
    _defaults = {"description": None}


class ScenarioItem(CacheItem):
    _defaults = {"active": False, "description": None}

    @property
    def sorted_alternatives(self):
        self._db_cache.fetch_all("scenario_alternative")
        return sorted(
            (x for x in self._db_cache.get("scenario_alternative", {}).values() if x["scenario_id"] == self["id"]),
            key=itemgetter("rank"),
        )

    def __getitem__(self, key):
        if key == "alternative_id_list":
            return [x["alternative_id"] for x in self.sorted_alternatives]
        if key == "alternative_name_list":
            return [x["alternative_name"] for x in self.sorted_alternatives]
        return super().__getitem__(key)


class ScenarioAlternativeItem(CacheItem):
    _unique_keys = (("scenario_name", "alternative_name"), ("scenario_name", "rank"))
    _references = {
        "scenario_name": ("scenario_id", ("scenario", "name")),
        "alternative_name": ("alternative_id", ("alternative", "name")),
    }
    _inverse_references = {
        "scenario_id": (("scenario_name",), ("scenario", ("name",))),
        "alternative_id": (("alternative_name",), ("alternative", ("name",))),
    }

    def __getitem__(self, key):
        # The 'before' is to be interpreted as, this scenario alternative goes *before* the before_alternative.
        # Since ranks go from 1 to the alternative count, the first alternative will have the second as the 'before',
        # the second will have the third, etc, and the last will have None.
        # Note that alternatives with higher ranks overwrite the values of those with lower ranks.
        if key == "before_alternative_name":
            return self._get_ref("alternative", self["before_alternative_id"], strong=False).get("name")
        if key == "before_alternative_id":
            scenario = self._get_ref("scenario", self["scenario_id"], strong=False)
            try:
                return scenario["alternative_id_list"][self["rank"]]
            except IndexError:
                return None
        return super().__getitem__(key)


class MetadataItem(CacheItem):
    _unique_keys = (("name", "value"),)


class EntityMetadataItem(CacheItem):
    _unique_keys = (("entity_name", "metadata_name"),)
    _references = {
        "entity_name": ("entity_id", ("entity", "name")),
        "metadata_name": ("metadata_id", ("metadata", "name")),
        "metadata_value": ("metadata_id", ("metadata", "value")),
    }
    _inverse_references = {
        "entity_id": (("entity_class_name", "entity_byname"), ("entity", ("class_name", "byname"))),
        "metadata_id": (("metadata_name", "metadata_value"), ("metadata", ("name", "value"))),
    }


class ParameterValueMetadataItem(CacheItem):
    _unique_keys = (("parameter_definition_name", "entity_byname", "alternative_name", "metadata_name"),)
    _references = {
        "parameter_definition_name": ("parameter_value_id", ("parameter_value", "parameter_definition_name")),
        "entity_byname": ("parameter_value_id", ("parameter_value", "entity_byname")),
        "alternative_name": ("parameter_value_id", ("parameter_value", "alternative_name")),
        "metadata_name": ("metadata_id", ("metadata", "name")),
        "metadata_value": ("metadata_id", ("metadata", "value")),
    }
    _inverse_references = {
        "parameter_value_id": (
            ("parameter_definition_name", "entity_byname", "alternative_name"),
            ("parameter_value", ("parameter_definition_name", "entity_byname", "alternative_name")),
        ),
        "metadata_id": (("metadata_name", "metadata_value"), ("metadata", ("name", "value"))),
    }
