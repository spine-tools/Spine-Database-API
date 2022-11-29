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

# TODO:
# - description
# - when to pop parsed_value?


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

    def _get_table_cache(self, item_type):
        return self.get(item_type, {})

    def get_item_by_key_value(self, item_type, key, value):
        table_cache = self._get_table_cache(item_type)
        item = next((x for x in table_cache.values() if x.get(key) == value), None)
        if item is None:
            self._advance_query(item_type)
            return {}
        return item

    def get_item(self, item_type, id_, referrer=None):
        table_cache = self._get_table_cache(item_type)
        item = table_cache.get(id_)
        if item is None:
            self._advance_query(item_type)
            return {}
        if referrer is not None:
            item.add_referrer(referrer)
        return item

    def make_item(self, item_type, item):
        """Returns a cache item.

        Args:
            item_type (str): the item type, equal to a table name
            item (dict): the 'db item' to use as base

        Returns:
            CacheItem
        """
        factory = {
            "object_class": ObjectClassItem,
            "object": ObjectItem,
            "relationship_class": RelationshipClassItem,
            "relationship": RelationshipItem,
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

    def add_item(self, item):
        if isinstance(item, CacheItem) and item["id"] in self:
            item.cascade_readd()
            return
        self[item["id"]] = self._db_cache.make_item(self._item_type, item)

    def update_item(self, item):
        current_item = self[item["id"]]
        current_item.update(item)
        current_item.cascade_update()

    def remove_item(self, id_):
        current_item = self.get(id_)
        if current_item:
            current_item.cascade_remove()
        return current_item


def _is_null(value):
    if isinstance(value, tuple):
        return None in value
    return value is None


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
        self._referrers = {}
        self.readd_callbacks = set()
        self.update_callbacks = set()
        self.remove_callbacks = set()
        self._item_type = item_type
        self._to_remove = False
        self._removed = False

    def is_valid(self):
        if self._removed:
            return False
        for key in self._reference_keys():
            _ = self[key]
            if self._to_remove:
                self.remove()
                break
        return not self._removed

    def is_removed(self):
        return self._removed

    def _reference_keys(self):
        return ()

    @property
    def key(self):
        return (self._item_type, self["id"])

    def __getattr__(self, name):
        """Overridden method to return the dictionary key named after the attribute, or None if it doesn't exist."""
        return self.get(name)

    def __repr__(self):
        return f"{type(self).__name__}{self.extended()}"

    def extended(self):
        return {**self, **{key: self[key] for key in self._reference_keys()}}

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def _asdict(self):
        return dict(**self)

    def _get_item(self, item_type, id_):
        item = self._db_cache.get_item(item_type, id_, self)
        if item and item.is_removed():
            self._to_remove = True
        return item

    def add_referrer(self, referrer):
        self._referrers[referrer.key] = referrer

    def readd(self):
        if not self._removed:
            return
        self._removed = False
        self._db_cache[self._item_type].add_item(self)

    def remove(self):
        if self._removed:
            return
        self._removed = True
        self._db_cache[self._item_type].remove_item(self["id"])
        self._to_remove = False

    def cascade_readd(self):
        for referrer in self._referrers.values():
            referrer.readd()
        for callback in self.readd_callbacks:
            callback()

    def cascade_remove(self):
        for referrer in self._referrers.values():
            referrer.remove()
        for callback in self.remove_callbacks:
            callback()

    def cascade_update(self):
        for referrer in self._referrers.values():
            referrer.cascade_update()
        for callback in self.update_callbacks:
            callback()


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


class ObjectClassItem(DisplayIconMixin, DescriptionMixin, CacheItem):
    pass


class ObjectItem(DescriptionMixin, CacheItem):
    def __getitem__(self, key):
        if key == "class_name":
            return self._get_item("object_class", self["class_id"]).get("name")
        if key == "group_id":
            return self._db_cache.get_item_by_key_value("entity_group", "entity_id", self["id"]).get("entity_id")
        return super().__getitem__(key)

    def _reference_keys(self):
        return super()._reference_keys() + ("class_name",)


class ObjectClassIdListMixin:
    def __init__(self, db_cache, *args, **kwargs):
        object_class_id_list = kwargs["object_class_id_list"]
        if isinstance(object_class_id_list, str):
            object_class_id_list = (int(id_) for id_ in object_class_id_list.split(","))
        kwargs["object_class_id_list"] = tuple(object_class_id_list)
        super().__init__(db_cache, *args, **kwargs)

    def __getitem__(self, key):
        if key == "object_class_name_list":
            return tuple(self._get_item("object_class", id_).get("name") for id_ in self["object_class_id_list"])
        return super().__getitem__(key)

    def _reference_keys(self):
        return super()._reference_keys() + ("object_class_name_list",)


class RelationshipClassItem(DisplayIconMixin, ObjectClassIdListMixin, DescriptionMixin, CacheItem):
    pass


class RelationshipItem(ObjectClassIdListMixin, CacheItem):
    def __init__(self, db_cache, *args, **kwargs):
        if "object_class_id_list" not in kwargs:
            kwargs["object_class_id_list"] = db_cache.get_item("relationship_class", kwargs["class_id"]).get(
                "object_class_id_list"
            )
        object_id_list = kwargs["object_id_list"]
        if isinstance(object_id_list, str):
            object_id_list = (int(id_) for id_ in object_id_list.split(","))
        kwargs["object_id_list"] = tuple(object_id_list)
        super().__init__(db_cache, *args, **kwargs)

    def __getitem__(self, key):
        if key == "class_name":
            return self._get_item("relationship_class", self["class_id"]).get("name")
        if key == "object_name_list":
            return tuple(self._get_item("object", id_).get("name") for id_ in self["object_id_list"])
        return super().__getitem__(key)

    def _reference_keys(self):
        return super()._reference_keys() + ("class_name", "object_name_list")


class ParameterMixin:
    def __init__(self, *args, **kwargs):
        if "entity_class_id" not in kwargs:
            kwargs["entity_class_id"] = kwargs.get("object_class_id") or kwargs.get("relationship_class_id")
        super().__init__(*args, **kwargs)

    def __getitem__(self, key):
        if key == "object_class_id":
            return self._get_item("object_class", self["entity_class_id"]).get("id")
        if key == "relationship_class_id":
            return self._get_item("relationship_class", self["entity_class_id"]).get("id")
        if key == "object_class_name":
            if self["object_class_id"] is None:
                return None
            return self._get_item("object_class", self["object_class_id"]).get("name")
        if key == "relationship_class_name":
            if self["relationship_class_id"] is None:
                return None
            return self._get_item("relationship_class", self["relationship_class_id"]).get("name")
        if key in ("object_class_id_list", "object_class_name_list"):
            if self["relationship_class_id"] is None:
                return None
            return self._get_item("relationship_class", self["relationship_class_id"]).get(key)
        if key == "entity_class_name":
            return self["relationship_class_name"] if self["object_class_id"] is None else self["object_class_name"]
        if key == "parameter_value_list_id":
            return dict.get(self, key)
        return super().__getitem__(key)

    def _reference_keys(self):
        return super()._reference_keys() + ("object_class_name", "relationship_class_name")


class ParameterDefinitionItem(DescriptionMixin, ParameterMixin, CacheItem):
    def __init__(self, db_cache, *args, **kwargs):
        kwargs["list_value_id"] = (
            int(kwargs["default_value"]) if kwargs.get("default_type") == "list_value_ref" else None
        )
        super().__init__(db_cache, *args, **kwargs)

    def __getitem__(self, key):
        if key == "parameter_name":
            return super().__getitem__("name")
        if key == "value_list_id":
            return super().__getitem__("parameter_value_list_id")
        if key == "value_list_name":
            return self._get_item("parameter_value_list", self["value_list_id"]).get("name")
        if key in ("default_value", "default_type"):
            if self["list_value_id"] is not None:
                return self._get_item("list_value", self["list_value_id"]).get(key.split("_")[1])
            return dict.get(self, key)
        return super().__getitem__(key)


class ParameterValueItem(ParameterMixin, CacheItem):
    def __init__(self, db_cache, *args, **kwargs):
        if "entity_id" not in kwargs:
            kwargs["entity_id"] = kwargs.get("object_id") or kwargs.get("relationship_id")
        kwargs["list_value_id"] = int(kwargs["value"]) if kwargs["type"] == "list_value_ref" else None
        super().__init__(db_cache, *args, **kwargs)

    def __getitem__(self, key):
        if key == "object_id":
            return self._get_item("object", self["entity_id"]).get("id")
        if key == "relationship_id":
            return self._get_item("relationship", self["entity_id"]).get("id")
        if key == "parameter_id":
            return super().__getitem__("parameter_definition_id")
        if key == "parameter_name":
            return self._get_item("parameter_definition", self["parameter_definition_id"]).get("name")
        if key == "object_name":
            if dict.get(self, "object_id") is None:
                return None
            return self._get_item("object", self["object_id"]).get("name")
        if key in ("object_id_list", "object_name_list"):
            if dict.get(self, "relationship_id") is None:
                return None
            return self._get_item("relationship", self["relationship_id"]).get(key)
        if key == "alternative_name":
            return self._get_item("alternative", self["alternative_id"]).get("name")
        if key in ("value", "type") and self["list_value_id"] is not None:
            return self._get_item("list_value", self["list_value_id"]).get(key)
        return super().__getitem__(key)

    def _reference_keys(self):
        return super()._reference_keys() + ("parameter_name", "alternative_name", "object_name", "object_name_list")


class EntityGroupItem(CacheItem):
    def __getitem__(self, key):
        if key == "class_id":
            return self["entity_class_id"]
        if key == "group_id":
            return self["entity_id"]
        if key == "class_name":
            return (
                self._get_item("object_class", self["entity_class_id"])
                or self._get_item("relationship_class", self["entity_class_id"])
            ).get("name")
        if key == "group_name":
            return (
                self._get_item("object", self["entity_id"]) or self._get_item("relationship", self["entity_id"])
            ).get("name")
        if key == "member_name":
            return (
                self._get_item("object", self["member_id"]) or self._get_item("relationship", self["member_id"])
            ).get("name")
        if key == "object_class_id":
            return self._get_item("object_class", self["entity_class_id"]).get("id")
        if key == "relationship_class_id":
            return self._get_item("relationship_class", self["entity_class_id"]).get("id")
        return super().__getitem__(key)


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
            return self._get_item("scenario", self["scenario_id"]).get("name")
        if key == "alternative_name":
            return self._get_item("alternative", self["alternative_id"]).get("name")
        scen_key = {
            "before_alternative_id": "alternative_id_list",
            "before_alternative_name": "alternative_name_list",
        }.get(key)
        if scen_key is not None:
            scenario = self._get_item("scenario", self["scenario_id"])
            try:
                return scenario[scen_key][self["rank"]]
            except IndexError:
                return None
        return super().__getitem__(key)


class FeatureItem(CacheItem):
    def __getitem__(self, key):
        if key == "parameter_definition_name":
            return self._get_item("parameter_definition", self["parameter_definition_id"]).get("name")
        if key in ("entity_class_id", "entity_class_name"):
            return self._get_item("parameter_definition", self["parameter_definition_id"]).get(key)
        if key == "parameter_value_list_name":
            return self._get_item("parameter_value_list", self["parameter_value_list_id"]).get("name")
        return super().__getitem__(key)


class ToolFeatureItem(CacheItem):
    def __getitem__(self, key):
        if key in ("entity_class_id", "entity_class_name", "parameter_definition_id", "parameter_definition_name"):
            return self._get_item("feature", self["feature_id"]).get(key)
        if key == "tool_name":
            return self._get_item("tool", self["tool_id"]).get("name")
        if key == "parameter_value_list_name":
            return self._get_item("parameter_value_list", self["parameter_value_list_id"]).get("name")
        if key == "required":
            return dict.get(self, "required", False)
        return super().__getitem__(key)


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
            return self._get_item("tool_feature", self["tool_feature_id"]).get(key)
        if key == "method":
            value_list = self._get_item("parameter_value_list", self["parameter_value_list_id"])
            if not value_list:
                return None
            list_value_id = value_list["value_id_list"][self["method_index"]]
            return self._get_item("list_value", list_value_id).get("value")
        return super().__getitem__(key)


class ParameterValueListItem(CacheItem):
    @property
    def _sorted_list_values(self):
        return sorted(
            (x for x in self._db_cache.get("list_value", {}).values() if x["parameter_value_list_id"] == self["id"]),
            key=itemgetter("index"),
        )

    def __getitem__(self, key):
        if key == "value_index_list":
            return tuple(x.get("index") for x in self._sorted_list_values)
        if key == "value_id_list":
            return tuple(x.get("id") for x in self._sorted_list_values)
        return super().__getitem__(key)
