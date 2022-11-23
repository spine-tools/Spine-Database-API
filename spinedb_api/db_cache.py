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
    def __init__(self, worker, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._worker = worker

    def table_cache(self, item_type):
        return self.setdefault(item_type, TableCache(self, item_type))

    def _get_table_cache(self, item_type):
        return self.get(item_type, {})

    def get_item_by_key_value(self, item_type, key, value):
        table_cache = self._get_table_cache(item_type)
        item = next((x for x in table_cache.values() if x.get(key) == value), None)
        if item is None:
            self._worker.do_advance_query(item_type)
            return {}
        return item

    def get_item(self, item_type, id_):
        table_cache = self._get_table_cache(item_type)
        item = table_cache.get(id_)
        if item is None and self._worker is not None:
            self._worker.do_advance_query(item_type)
            return {}
        return item


class TableCache(dict):
    def __init__(self, db_cache, item_type, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._db_cache = db_cache
        self._item_type = item_type
        self._make_item = {
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
            "parameter_value_list": ParameterValueListItem,
        }.get(self._item_type, CacheItem)

    def __setitem__(self, id_, item):
        new_item = self._make_item(self._db_cache, **item)
        super().__setitem__(id_, new_item)


class CacheItem(dict):
    """A dictionary that behaves kinda like a row from a query result.

    It is used to store items in a cache, so we can access them as if they were rows from a query result.
    This is mainly because we want to use the cache as a replacement for db queries in some methods.
    """

    def __init__(self, cache, *args, **kwargs):
        kwargs.pop("parsed_value", None)
        super().__init__(*args, **kwargs)
        self._cache = cache

    def __getattr__(self, name):
        """Overridden method to return the dictionary key named after the attribute, or None if it doesn't exist."""
        return self.get(name)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def _asdict(self):
        return dict(**self)

    def _get_item(self, item_type, id_):
        return self._cache.get_item(item_type, id_)


class ClassItem(CacheItem):
    def __getitem__(self, key):
        if key == "display_icon":
            return dict.get(self, "display_icon")
        return super().__getitem__(key)


class ObjectClassItem(ClassItem):
    pass


class ObjectItem(CacheItem):
    def __getitem__(self, key):
        if key == "class_name":
            return self._get_item("object_class", self["class_id"]).get("name")
        if key == "group_id":
            return self._cache.get_item_by_key_value("entity_group", "entity_id", self["id"]).get("entity_id")
        return super().__getitem__(key)


class ObjectClassIdListItem(CacheItem):
    def __init__(self, cache, *args, **kwargs):
        object_class_id_list = kwargs["object_class_id_list"]
        if isinstance(object_class_id_list, str):
            object_class_id_list = (int(id_) for id_ in object_class_id_list.split(","))
        kwargs["object_class_id_list"] = tuple(object_class_id_list)
        super().__init__(cache, *args, **kwargs)

    def __getitem__(self, key):
        if key == "object_class_name_list":
            return tuple(self._get_item("object_class", id_).get("name") for id_ in self["object_class_id_list"])
        return super().__getitem__(key)


class RelationshipClassItem(ObjectClassIdListItem, ClassItem):
    pass


class RelationshipItem(ObjectClassIdListItem):
    def __init__(self, cache, *args, **kwargs):
        if "object_class_id_list" not in kwargs:
            kwargs["object_class_id_list"] = cache.get_item("relationship_class", kwargs["class_id"]).get(
                "object_class_id_list"
            )
        object_id_list = kwargs["object_id_list"]
        if isinstance(object_id_list, str):
            object_id_list = (int(id_) for id_ in object_id_list.split(","))
        kwargs["object_id_list"] = tuple(object_id_list)
        super().__init__(cache, *args, **kwargs)

    def __getitem__(self, key):
        if key == "class_name":
            return self._get_item("relationship_class", self["class_id"]).get("name")
        if key == "object_name_list":
            return tuple(self._get_item("object", id_).get("name") for id_ in self["object_id_list"])
        return super().__getitem__(key)


class ParameterItem(CacheItem):
    def __getitem__(self, key):
        if key in ("object_class_id", "relationship_class_id"):
            return dict.get(self, key)
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
        return super().__getitem__(key)


class ParameterDefinitionItem(ParameterItem):
    def __init__(self, cache, *args, **kwargs):
        kwargs["list_value_id"] = (
            int(kwargs["default_value"]) if kwargs.get("default_type") == "list_value_ref" else None
        )
        super().__init__(cache, *args, **kwargs)

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


class ParameterValueItem(ParameterItem):
    def __init__(self, cache, *args, **kwargs):
        kwargs["list_value_id"] = int(kwargs["value"]) if kwargs["type"] == "list_value_ref" else None
        super().__init__(cache, *args, **kwargs)

    def __getitem__(self, key):
        if key == "parameter_id":
            return super().__getitem__("parameter_definition_id")
        if key == "parameter_name":
            return self._get_item("parameter_definition", self["parameter_definition_id"]).get("name")
        if key == "object_name":
            if self["object_id"] is None:
                return None
            return self._get_item("object", self["object_id"]).get("name")
        if key in ("object_id_list", "object_name_list"):
            if self["relationship_id"] is None:
                return None
            return self._get_item("relationship", self["relationship_id"]).get(key)
        if key == "alternative_name":
            return self._get_item("alternative", self["alternative_id"]).get("name")
        if key in ("value", "type") and self["list_value_id"] is not None:
            return self._get_item("list_value", self["list_value_id"]).get(key)
        return super().__getitem__(key)


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
            (x for x in self._cache.get("scenario_alternative", {}).values() if x["scenario_id"] == self["id"]),
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
        # TODO: before alternative...
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


class ParameterValueListItem(CacheItem):
    @property
    def _sorted_list_values(self):
        return sorted(
            (x for x in self._cache.get("list_value", {}).values() if x["parameter_value_list_id"] == self["id"]),
            key=itemgetter("index"),
        )

    def __getitem__(self, key):
        if key == "value_index_list":
            return tuple(x.get("index") for x in self._sorted_list_values)
        if key == "value_id_list":
            return tuple(x.get("id") for x in self._sorted_list_values)
        return super().__getitem__(key)
