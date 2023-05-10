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
from sqlalchemy.exc import ProgrammingError

# TODO: Implement CacheItem.pop() to do lookup?


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

    def to_change(self):
        to_add = {}
        to_update = {}
        to_remove = {}
        for item_type, table_cache in self.items():
            new = [x for x in table_cache.values() if x.new]
            dirty = [x for x in table_cache.values() if x.dirty and not x.new]
            removed = {x.id for x in dict.values(table_cache) if x.removed}
            if new:
                to_add[item_type] = new
            if dirty:
                to_update[item_type] = dirty
            if removed:
                to_remove[item_type] = removed
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
            return qry.all()
        offset = self._offsets.setdefault(item_type, 0)
        chunk = qry.limit(self._chunk_size).offset(offset).all()
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
        self._existing = {}

    def existing(self, key, value):
        """Returns the CacheItem that has the given value for the given unique constraint key, or None.

        Args:
            key (tuple)
            value (tuple)

        Returns:
            CacheItem
        """
        self._db_cache.fetch_all(self._item_type)
        return self._existing.get(key, {}).get(value)

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

    def _current_item(self, item):
        id_ = item.get("id")
        if isinstance(id_, int):
            # id is an int, easy
            return self.get(id_)
        if isinstance(id_, dict):
            # id is a dict specifying the values for one of the unique constraints
            return self._current_item_from_dict_id(id_)
        if id_ is None:
            # No id. Try to build the dict id from the item itself. Used by import_data.
            for key in self._item_factory.unique_constraint:
                dict_id = {k: item.get(k) for k in key}
                current_item = self._current_item_from_dict_id(dict_id)
                if current_item:
                    return current_item

    def _current_item_from_dict_id(self, dict_id):
        key, value = zip(*dict_id.items())
        return self.existing(key, value)

    def check_item(self, item, for_update=False):
        if for_update:
            current_item = self._current_item(item)
            if current_item is None:
                return None, f"no {self._item_type} matching {item} to update"
            item = {**current_item, **item}
            item["id"] = current_item["id"]
        else:
            current_item = None
        candidate_item = self._make_item(item)
        candidate_item.resolve_inverse_references()
        missing_ref = candidate_item.missing_ref()
        if missing_ref:
            return None, f"missing {missing_ref} for {self._item_type}"
        try:
            for key, value in candidate_item.unique_values():
                existing_item = self.existing(key, value)
                if existing_item not in (None, current_item) and existing_item.is_valid():
                    kv_parts = [f"{k} '{', '.join(v) if isinstance(v, tuple) else v}'" for k, v in zip(key, value)]
                    head, tail = kv_parts[:-1], kv_parts[-1]
                    head_str = ", ".join(head)
                    main_parts = [head_str, tail] if head_str else [tail]
                    key_val = " and ".join(main_parts)
                    return None, f"there's already a {self._item_type} with {key_val}"
        except KeyError as e:
            return None, f"missing {e} for {self._item_type}"
        return candidate_item._asdict(), None

    def _add_to_existing(self, item):
        for key, value in item.unique_values():
            self._existing.setdefault(key, {})[value] = item

    def _remove_from_existing(self, item):
        for key, value in item.unique_values():
            self._existing.get(key, {}).pop(value, None)

    def add_item(self, item, new=False):
        self[item["id"]] = new_item = self._make_item(item)
        self._add_to_existing(new_item)
        new_item.new = new
        return new_item

    def update_item(self, item):
        current_item = self[item["id"]]
        self._remove_from_existing(current_item)
        current_item.dirty = True
        current_item.update(item)
        self._add_to_existing(current_item)
        current_item.cascade_update()

    def remove_item(self, id_):
        current_item = self.get(id_)
        if current_item is not None:
            self._remove_from_existing(current_item)
            current_item.cascade_remove()
        return current_item

    def restore_item(self, id_):
        current_item = self.get(id_)
        if current_item is not None:
            self._add_to_existing(current_item)
            current_item.cascade_restore()
        return current_item


class CacheItem(dict):
    """A dictionary that represents an db item."""

    unique_constraint = (("name",),)
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
        self.new = False
        self.dirty = False

    def missing_ref(self):
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

    def unique_values(self):
        for key in self.unique_constraint:
            yield key, tuple(self[k] for k in key)

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

    def resolve_inverse_references(self):
        for src_key, (id_key, (ref_type, ref_key)) in self._inverse_references.items():
            id_value = tuple(dict.get(self, k) or self.get(k) for k in id_key)
            if None in id_value:
                continue
            table_cache = self._db_cache.table_cache(ref_type)
            with suppress(AttributeError):  # NoneType has no attribute id, happens when existing() returns None
                self[src_key] = (
                    tuple(table_cache.existing(ref_key, v).id for v in zip(*id_value))
                    if all(isinstance(v, tuple) for v in id_value)
                    else table_cache.existing(ref_key, id_value).id
                )
                # FIXME: Do we need to catch the AttributeError and give it to the user instead??


class DisplayIconMixin:
    def __getitem__(self, key):
        if key == "display_icon":
            return dict.get(self, "display_icon")
        return super().__getitem__(key)


class DescriptionMixin:
    def __getitem__(self, key):
        if key == "description":
            return dict.get(self, "description")
        return super().__getitem__(key)


class EntityClassItem(DisplayIconMixin, DescriptionMixin, CacheItem):
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


class EntityItem(DescriptionMixin, CacheItem):
    unique_constraint = (("class_name", "name"), ("class_name", "byname"))
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

    def resolve_inverse_references(self):
        super().resolve_inverse_references()
        self._fill_name()

    def _fill_name(self):
        if "name" in self:
            return
        base_name = self["class_name"] + "_" + "__".join(self["element_name_list"])
        name = base_name
        table_cache = self._db_cache.table_cache(self._item_type)
        while table_cache.existing(("class_name", "name"), (self["class_name"], name)) is not None:
            name = base_name + uuid.uuid4().hex
        self["name"] = name


class EntityGroupItem(CacheItem):
    unique_constraint = (("group_name", "member_name"),)
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


class ParameterDefinitionItem(DescriptionMixin, CacheItem):
    unique_constraint = (("entity_class_name", "name"),)
    _references = {
        "entity_class_name": ("entity_class_id", ("entity_class", "name")),
        "dimension_id_list": ("entity_class_id", ("entity_class", "dimension_id_list")),
        "dimension_name_list": ("entity_class_id", ("entity_class", "dimension_name_list")),
    }
    _inverse_references = {
        "entity_class_id": (("entity_class_name",), ("entity_class", ("name",))),
        "parameter_value_list_id": (("parameter_value_list_name",), ("parameter_value_list", ("name",))),
    }

    def __init__(self, *args, **kwargs):
        if kwargs.get("list_value_id") is None:
            kwargs["list_value_id"] = (
                int(kwargs["default_value"]) if kwargs.get("default_type") == "list_value_ref" else None
            )
        super().__init__(*args, **kwargs)

    def __getitem__(self, key):
        if key == "parameter_name":
            return super().__getitem__("name")
        if key == "value_list_id":
            return super().__getitem__("parameter_value_list_id")
        if key == "parameter_value_list_id":
            return dict.get(self, key)
        if key == "value_list_name":
            return self._get_ref("parameter_value_list", self["value_list_id"], strong=False).get("name")
        if key in ("default_value", "default_type"):
            if self["list_value_id"] is not None:
                return self._get_ref("list_value", self["list_value_id"], strong=False).get(key.split("_")[1])
            return dict.get(self, key)
        return super().__getitem__(key)


class ParameterValueItem(CacheItem):
    unique_constraint = (("parameter_definition_name", "entity_byname", "alternative_name"),)
    _references = {
        "entity_class_name": ("entity_class_id", ("entity_class", "name")),
        "dimension_id_list": ("entity_class_id", ("entity_class", "dimension_id_list")),
        "dimension_name_list": ("entity_class_id", ("entity_class", "dimension_name_list")),
        "parameter_definition_name": ("parameter_definition_id", ("parameter_definition", "name")),
        "parameter_value_list_id": ("parameter_definition_id", ("parameter_definition", "parameter_value_list_id")),
        "entity_name": ("entity_id", ("entity", "name")),
        "entity_byname": ("entity_id", ("entity", "byname")),
        "element_id_list": ("entity_id", ("entity", "element_id_list")),
        "element_name_list": ("entity_id", ("entity", "element_name_list")),
        "alternative_name": ("alternative_id", ("alternative", "name")),
    }
    _inverse_references = {
        "parameter_definition_id": (
            ("entity_class_name", "parameter_definition_name"),
            ("parameter_definition", ("entity_class_name", "name")),
        ),
        "entity_id": (("entity_class_name", "entity_byname"), ("entity", ("class_name", "byname"))),
        "alternative_id": (("alternative_name",), ("alternative", ("name",))),
    }

    def __init__(self, *args, **kwargs):
        if kwargs.get("list_value_id") is None:
            kwargs["list_value_id"] = int(kwargs["value"]) if kwargs.get("type") == "list_value_ref" else None
        super().__init__(*args, **kwargs)

    def __getitem__(self, key):
        if key == "parameter_id":
            return super().__getitem__("parameter_definition_id")
        if key in ("value", "type") and self["list_value_id"] is not None:
            return self._get_ref("list_value", self["list_value_id"], strong=False).get(key)
        return super().__getitem__(key)


class ListValueItem(CacheItem):
    unique_constraint = (("parameter_value_list_name", "value"), ("parameter_value_list_name", "index"))
    _references = {"parameter_value_list_name": ("parameter_value_list_id", ("parameter_value_list", "name"))}
    _inverse_references = {
        "parameter_value_list_id": (("parameter_value_list_name",), ("parameter_value_list", ("name",))),
    }


class ScenarioItem(CacheItem):
    @property
    def sorted_scenario_alternatives(self):
        self._db_cache.fetch_all("scenario_alternative")
        return sorted(
            (x for x in self._db_cache.get("scenario_alternative", {}).values() if x["scenario_id"] == self["id"]),
            key=itemgetter("rank"),
        )

    def __getitem__(self, key):
        if key == "alternative_id_list":
            return [x["alternative_id"] for x in self.sorted_scenario_alternatives]
        if key == "alternative_name_list":
            return [x["alternative_name"] for x in self.sorted_scenario_alternatives]
        return super().__getitem__(key)


class ScenarioAlternativeItem(CacheItem):
    unique_constraint = (("scenario_name", "alternative_name"), ("scenario_name", "rank"))
    _references = {
        "scenario_name": ("scenario_id", ("scenario", "name")),
        "alternative_name": ("alternative_id", ("alternative", "name")),
    }
    _inverse_references = {
        "scenario_id": (("scenario_name",), ("scenario", ("name",))),
        "alternative_id": (("alternative_name",), ("alternative", ("name",))),
        "before_alternative_id": (("before_alternative_name",), ("alternative", ("name",))),
    }

    def __getitem__(self, key):
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
    unique_constraint = (("name", "value"),)


class EntityMetadataItem(CacheItem):
    unique_constraint = (("entity_name", "metadata_name"),)
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
    unique_constraint = (("parameter_definition_name", "entity_byname", "alternative_name", "metadata_name"),)
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
