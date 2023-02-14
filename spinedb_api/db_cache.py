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

:author: Manuel Marin (ER)
:date:   22.11.2022
"""
from operator import itemgetter


class DBCache(dict):
    def __init__(self, advance_query, *args, **kwargs):
        """
        A dictionary that maps table names to ids to items. Used to store and retreive database contents.

        Args:
            advance_query (function): A function to call when references aren't found.
                It receives a table name (a.k.a item type) and should bring more items of that type into this cache.
        """
        super().__init__(*args, **kwargs)
        self._advance_query = advance_query

    def table_cache(self, item_type):
        return self.setdefault(item_type, TableCache(self, item_type))

    def get_item(self, item_type, id_):
        table_cache = self.get(item_type, {})
        item = table_cache.get(id_)
        if item is None:
            return {}
        return item

    def fetch_ref(self, item_type, id_):
        while self._advance_query(item_type):
            ref = self.get(item_type, {}).get(id_)
            if ref:
                return ref

    def make_item(self, item_type, item):
        """Returns a cache item.

        Args:
            item_type (str): the item type, equal to a table name
            item (dict): the 'db item' to use as base

        Returns:
            CacheItem
        """
        factory = {
            "entity_class": EntityClassItem,
            "entity": EntityItem,
            "parameter_definition": ParameterDefinitionItem,
            "parameter_value": ParameterValueItem,
            "entity_group": EntityGroupItem,
            "scenario": ScenarioItem,
            "scenario_alternative": ScenarioAlternativeItem,
            "feature": FeatureItem,
            "tool_feature": ToolFeatureItem,
            "tool_feature_method": ToolFeatureMethodItem,
            "parameter_value_list": ParameterValueListItem,
        }.get(item_type, CacheItem)
        return factory(self, item_type, **item)


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

    def values(self):
        return (x for x in super().values() if x.is_valid())

    def add_item(self, item):
        self[item["id"]] = new_item = self._db_cache.make_item(self._item_type, item)
        return new_item

    def update_item(self, item):
        current_item = self[item["id"]]
        current_item.update(item)
        current_item.cascade_update()

    def remove_item(self, id_):
        current_item = self.get(id_)
        if current_item:
            current_item.cascade_remove()
        return current_item


class CacheItem(dict):
    """A dictionary that behaves kinda like a row from a query result.

    It is used to store items in a cache, so we can access them as if they were rows from a query result.
    This is mainly because we want to use the cache as a replacement for db queries in some methods.
    """

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
        self.readd_callbacks = set()
        self.update_callbacks = set()
        self.remove_callbacks = set()
        self._to_remove = False
        self._removed = False
        self._corrupted = False
        self._valid = None

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
        return {**self, **{key: self[key] for key in self._reference_keys()}}

    def _asdict(self):
        return dict(**self)

    def _reference_keys(self):
        return ()

    def _get_ref(self, ref_type, ref_id, source_key):
        ref = self._db_cache.get_item(ref_type, ref_id)
        if not ref:
            if source_key not in self._reference_keys():
                return {}
            ref = self._db_cache.fetch_ref(ref_type, ref_id)
            if not ref:
                self._corrupted = True
                return {}
        return self._handle_ref(ref, source_key)

    def _handle_ref(self, ref, source_key):
        if source_key in self._reference_keys():
            ref.add_referrer(self)
            if ref.is_removed():
                self._to_remove = True
        else:
            ref.add_weak_referrer(self)
            if ref.is_removed():
                return {}
        return ref

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def copy(self):
        return type(self)(self._db_cache, self._item_type, **self)

    def updated(self, other):
        return type(self)(self._db_cache, self._item_type, **{**self, **other})

    def is_valid(self):
        if self._valid is not None:
            return self._valid
        if self._removed or self._corrupted:
            return False
        self._to_remove = False
        self._corrupted = False
        for key in self._reference_keys():
            _ = self[key]
        if self._to_remove:
            self.cascade_remove()
        self._valid = not self._removed and not self._corrupted
        return self._valid

    def is_removed(self):
        return self._removed

    def add_referrer(self, referrer):
        if referrer.key is None:
            return
        self._referrers[referrer.key] = self._weak_referrers.pop(referrer.key, referrer)

    def add_weak_referrer(self, referrer):
        if referrer.key is None:
            return
        if referrer.key not in self._referrers:
            self._weak_referrers[referrer.key] = referrer

    def cascade_readd(self):
        if not self._removed:
            return
        self._removed = False
        for referrer in self._referrers.values():
            referrer.cascade_readd()
        for weak_referrer in self._weak_referrers.values():
            weak_referrer.call_update_callbacks()
        obsolete = set()
        for callback in self.readd_callbacks:
            if not callback(self):
                obsolete.add(callback)
        self.readd_callbacks -= obsolete

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
    def __init__(self, *args, **kwargs):
        dimension_id_list = kwargs["dimension_id_list"]
        if dimension_id_list is None:
            dimension_id_list = ()
        if isinstance(dimension_id_list, str):
            dimension_id_list = (int(id_) for id_ in dimension_id_list.split(","))
        kwargs["dimension_id_list"] = tuple(dimension_id_list)
        super().__init__(*args, **kwargs)

    def __getitem__(self, key):
        if key == "dimension_name_list":
            return tuple(self._get_ref("entity_class", id_, key).get("name") for id_ in self["dimension_id_list"])
        return super().__getitem__(key)

    def _reference_keys(self):
        return super()._reference_keys() + ("dimension_name_list",)


class EntityItem(DescriptionMixin, CacheItem):
    def __init__(self, *args, **kwargs):
        element_id_list = kwargs["element_id_list"]
        if element_id_list is None:
            element_id_list = ()
        if isinstance(element_id_list, str):
            element_id_list = (int(id_) for id_ in element_id_list.split(","))
        kwargs["element_id_list"] = tuple(element_id_list)
        super().__init__(*args, **kwargs)

    def __getitem__(self, key):
        if key == "class_name":
            return self._get_ref("entity_class", self["class_id"], key).get("name")
        if key == "dimension_id_list":
            return self._get_ref("entity_class", self["class_id"], key).get("dimension_id_list")
        if key == "dimension_name_list":
            return self._get_ref("entity_class", self["class_id"], key).get("dimension_name_list")
        if key == "element_name_list":
            return tuple(self._get_ref("entity", id_, key).get("name") for id_ in self["element_id_list"])
        return super().__getitem__(key)

    def _reference_keys(self):
        return super()._reference_keys() + (
            "class_name",
            "dimension_id_list",
            "dimension_name_list",
            "element_name_list",
        )


class ParameterMixin:
    def __getitem__(self, key):
        if key in ("dimension_id_list", "dimension_name_list"):
            return self._get_ref("entity_class", self["entity_class_id"], key)[key]
        if key == "entity_class_name":
            return self._get_ref("entity_class", self["entity_class_id"], key)["name"]
        if key == "parameter_value_list_id":
            return dict.get(self, key)
        return super().__getitem__(key)

    def _reference_keys(self):
        return super()._reference_keys() + ("entity_class_name", "dimension_id_list", "dimension_name_list")


class ParameterDefinitionItem(DescriptionMixin, ParameterMixin, CacheItem):
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
        if key == "value_list_name":
            return self._get_ref("parameter_value_list", self["value_list_id"], key).get("name")
        if key in ("default_value", "default_type"):
            if self["list_value_id"] is not None:
                return self._get_ref("list_value", self["list_value_id"], key).get(key.split("_")[1])
            return dict.get(self, key)
        return super().__getitem__(key)


class ParameterValueItem(ParameterMixin, CacheItem):
    def __init__(self, *args, **kwargs):
        if kwargs.get("list_value_id") is None:
            kwargs["list_value_id"] = int(kwargs["value"]) if kwargs.get("type") == "list_value_ref" else None
        super().__init__(*args, **kwargs)

    def __getitem__(self, key):
        if key == "parameter_id":
            return super().__getitem__("parameter_definition_id")
        if key == "parameter_name":
            return self._get_ref("parameter_definition", self["parameter_definition_id"], key).get("name")
        if key == "entity_name":
            return self._get_ref("entity", self["entity_id"], key)["name"]
        if key in ("element_id_list", "element_name_list"):
            return self._get_ref("entity", self["entity_id"], key)[key]
        if key == "alternative_name":
            return self._get_ref("alternative", self["alternative_id"], key).get("name")
        if key in ("value", "type") and self["list_value_id"] is not None:
            return self._get_ref("list_value", self["list_value_id"], key).get(key)
        return super().__getitem__(key)

    def _reference_keys(self):
        return super()._reference_keys() + (
            "parameter_name",
            "alternative_name",
            "entity_name",
            "element_id_list",
            "element_name_list",
        )


class EntityGroupItem(CacheItem):
    def __getitem__(self, key):
        if key == "class_id":
            return self["entity_class_id"]
        if key == "group_id":
            return self["entity_id"]
        if key == "class_name":
            return self._get_ref("entity_class", self["entity_class_id"], key)["name"]
        if key == "group_name":
            return self._get_ref("entity", self["entity_id"], key)["name"]
        if key == "member_name":
            return self._get_ref("entity", self["member_id"], key)["name"]
        if key == "dimension_id_list":
            return self._get_ref("entity_class", self["entity_class_id"], key)["dimension_id_list"]
        return super().__getitem__(key)

    def _reference_keys(self):
        return super()._reference_keys() + ("class_name", "group_name", "member_name", "dimension_id_list")


class ScenarioItem(CacheItem):
    @property
    def _sorted_scen_alts(self):
        return sorted(
            (x for x in self._db_cache.get("scenario_alternative", {}).values() if x["scenario_id"] == self["id"]),
            key=itemgetter("rank"),
        )

    def __getitem__(self, key):
        if key == "active":
            return dict.get(self, "active", False)
        if key == "alternative_id_list":
            return tuple(x.get("alternative_id") for x in self._sorted_scen_alts)
        if key == "alternative_name_list":
            return tuple(x.get("alternative_name") for x in self._sorted_scen_alts)
        return super().__getitem__(key)


class ScenarioAlternativeItem(CacheItem):
    def __getitem__(self, key):
        if key == "scenario_name":
            return self._get_ref("scenario", self["scenario_id"], key).get("name")
        if key == "alternative_name":
            return self._get_ref("alternative", self["alternative_id"], key).get("name")
        scen_key = {
            "before_alternative_id": "alternative_id_list",
            "before_alternative_name": "alternative_name_list",
        }.get(key)
        if scen_key is not None:
            scenario = self._get_ref("scenario", self["scenario_id"], key)
            try:
                return scenario[scen_key][self["rank"]]
            except IndexError:
                return None
        return super().__getitem__(key)

    def _reference_keys(self):
        return super()._reference_keys() + ("scenario_name", "alternative_name")


class FeatureItem(CacheItem):
    def __getitem__(self, key):
        if key == "parameter_definition_name":
            return self._get_ref("parameter_definition", self["parameter_definition_id"], key).get("name")
        if key in ("entity_class_id", "entity_class_name"):
            return self._get_ref("parameter_definition", self["parameter_definition_id"], key).get(key)
        if key == "parameter_value_list_name":
            return self._get_ref("parameter_value_list", self["parameter_value_list_id"], key).get("name")
        return super().__getitem__(key)

    def _reference_keys(self):
        return super()._reference_keys() + (
            "entity_class_id",
            "entity_class_name",
            "parameter_definition_name",
            "parameter_value_list_name",
        )


class ToolFeatureItem(CacheItem):
    def __getitem__(self, key):
        if key in ("entity_class_id", "entity_class_name", "parameter_definition_id", "parameter_definition_name"):
            return self._get_ref("feature", self["feature_id"], key).get(key)
        if key == "tool_name":
            return self._get_ref("tool", self["tool_id"], key).get("name")
        if key == "parameter_value_list_name":
            return self._get_ref("parameter_value_list", self["parameter_value_list_id"], key).get("name")
        if key == "required":
            return dict.get(self, "required", False)
        return super().__getitem__(key)

    def _reference_keys(self):
        return super()._reference_keys() + (
            "tool_name",
            "entity_class_id",
            "entity_class_name",
            "parameter_definition_id",
            "parameter_definition_name",
            "parameter_value_list_name",
        )


class ToolFeatureMethodItem(CacheItem):
    def __getitem__(self, key):
        if key in (
            "tool_id",
            "tool_name",
            "feature_id",
            "entity_class_id",
            "entity_class_name",
            "parameter_definition_id",
            "parameter_definition_name",
            "parameter_value_list_id",
            "parameter_value_list_name",
        ):
            return self._get_ref("tool_feature", self["tool_feature_id"], key).get(key)
        if key == "method":
            value_list = self._get_ref("parameter_value_list", self["parameter_value_list_id"], key)
            if not value_list:
                return None
            try:
                list_value_id = value_list["value_id_list"][self["method_index"]]
                return self._get_ref("list_value", list_value_id, key).get("value")
            except IndexError:
                return None
        return super().__getitem__(key)

    def _reference_keys(self):
        return super()._reference_keys() + (
            "tool_id",
            "tool_name",
            "feature_id",
            "entity_class_id",
            "entity_class_name",
            "parameter_definition_id",
            "parameter_definition_name",
            "parameter_value_list_id",
            "parameter_value_list_name",
            "method",
        )


class ParameterValueListItem(CacheItem):
    def _sorted_list_values(self, key):
        return sorted(
            (
                self._get_ref("list_value", x["id"], key)
                for x in self._db_cache.get("list_value", {}).values()
                if x["parameter_value_list_id"] == self["id"]
            ),
            key=itemgetter("index"),
        )

    def __getitem__(self, key):
        if key == "value_index_list":
            return tuple(x.get("index") for x in self._sorted_list_values(key))
        if key == "value_id_list":
            return tuple(x.get("id") for x in self._sorted_list_values(key))
        return super().__getitem__(key)
