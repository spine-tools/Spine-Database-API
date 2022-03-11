######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""Provides :class:`.DatabaseMappingCheckMixin`.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""
# TODO: Review docstrings, they are almost good

from contextlib import contextmanager
from itertools import chain
from .exception import SpineIntegrityError
from .check_functions import (
    check_alternative,
    check_scenario,
    check_scenario_alternative,
    check_object_class,
    check_object,
    check_wide_relationship_class,
    check_wide_relationship,
    check_entity_group,
    check_parameter_definition,
    check_parameter_value,
    check_parameter_value_list,
    check_list_value,
    check_feature,
    check_tool,
    check_tool_feature,
    check_tool_feature_method,
)
from .parameter_value import from_database
from .helpers import CacheItem


# NOTE: To check for an update we remove the current instance from our lookup dictionary,
# check for an insert of the updated instance,
# and finally reinsert the instance to the dictionary
class DatabaseMappingCheckMixin:
    """Provides methods to check whether insert and update operations violate Spine db integrity constraints."""

    def check_items(self, tablename, *items, for_update=False, strict=False, cache=None):
        return {
            "alternative": self.check_alternatives,
            "scenario": self.check_scenarios,
            "scenario_alternative": self.check_scenario_alternatives,
            "object": self.check_objects,
            "object_class": self.check_object_classes,
            "relationship_class": self.check_wide_relationship_classes,
            "relationship": self.check_wide_relationships,
            "entity_group": self.check_entity_groups,
            "parameter_definition": self.check_parameter_definitions,
            "parameter_value": self.check_parameter_values,
            "parameter_value_list": self.check_parameter_value_lists,
            "list_value": self.check_list_values,
            "feature": self.check_features,
            "tool": self.check_tools,
            "tool_feature": self.check_tool_features,
            "tool_feature_method": self.check_tool_feature_methods,
        }[tablename](*items, for_update=for_update, strict=strict, cache=cache)

    def check_features(self, *items, for_update=False, strict=False, cache=None):
        """Check whether features passed as argument respect integrity constraints.

        Args:
            items (Iterable): One or more Python :class:`dict` objects representing the items to be checked.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if one of the items violates an integrity constraint.

        Returns
            list: items that passed the check.
            list: :exc:`~.exception.SpineIntegrityError` instances corresponding to found violations.
        """
        if cache is None:
            cache = self.make_cache({"feature"}, include_ancestors=True)
        intgr_error_log = []
        checked_items = list()
        feature_ids = {x.parameter_definition_id: x.id for x in cache.get("feature", {}).values()}
        parameter_definitions = {
            x.id: {
                "name": x.parameter_name,
                "entity_class_id": x.entity_class_id,
                "parameter_value_list_id": x.value_list_id,
            }
            for x in cache.get("parameter_definition", {}).values()
        }
        for item in items:
            try:
                with _manage_stocks(
                    item, {("parameter_definition_id",): feature_ids}, "feature", for_update, cache
                ) as item:
                    check_feature(item, feature_ids, parameter_definitions)
                    checked_items.append(item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_tools(self, *items, for_update=False, strict=False, cache=None):
        """Check whether tools passed as argument respect integrity constraints.

        Args:
            items (Iterable): One or more Python :class:`dict` objects representing the items to be checked.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if one of the items violates an integrity constraint.

        Returns
            list: items that passed the check.
            list: :exc:`~.exception.SpineIntegrityError` instances corresponding to found violations.
        """
        if cache is None:
            cache = self.make_cache({"tool"}, include_ancestors=True)
        intgr_error_log = []
        checked_items = list()
        tool_ids = {x.name: x.id for x in cache.get("tool", {}).values()}
        for item in items:
            try:
                with _manage_stocks(item, {("name",): tool_ids}, "tool", for_update, cache) as item:
                    check_tool(item, tool_ids)
                    checked_items.append(item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_tool_features(self, *items, for_update=False, strict=False, cache=None):
        """Check whether tool features passed as argument respect integrity constraints.

        Args:
            items (Iterable): One or more Python :class:`dict` objects representing the items to be checked.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if one of the items violates an integrity constraint.

        Returns
            list: items that passed the check.
            list: :exc:`~.exception.SpineIntegrityError` instances corresponding to found violations.
        """
        if cache is None:
            cache = self.make_cache({"tool_feature"}, include_ancestors=True)
        intgr_error_log = []
        checked_items = list()
        tool_feature_ids = {(x.tool_id, x.feature_id): x.id for x in cache.get("tool_feature", {}).values()}
        tools = {x.id: x._asdict() for x in cache.get("tool", {}).values()}
        features = {
            x.id: {
                "name": x.entity_class_name + "/" + x.parameter_definition_name,
                "parameter_value_list_id": x.parameter_value_list_id,
            }
            for x in cache.get("feature", {}).values()
        }
        for item in items:
            try:
                with _manage_stocks(
                    item, {("tool_id", "feature_id"): tool_feature_ids}, "tool_feature", for_update, cache
                ) as item:
                    check_tool_feature(item, tool_feature_ids, tools, features)
                    checked_items.append(item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_tool_feature_methods(self, *items, for_update=False, strict=False, cache=None):
        """Check whether tool feature methods passed as argument respect integrity constraints.

        Args:
            items (Iterable): One or more Python :class:`dict` objects representing the items to be checked.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if one of the items violates an integrity constraint.

        Returns
            list: items that passed the check.
            list: :exc:`~.exception.SpineIntegrityError` instances corresponding to found violations.
        """
        if cache is None:
            cache = self.make_cache({"tool_feature_method"}, include_ancestors=True)
        intgr_error_log = []
        checked_items = list()
        tool_feature_method_ids = {
            (x.tool_feature_id, x.method_index): x.id for x in cache.get("tool_feature_method", {}).values()
        }
        tool_features = {x.id: x._asdict() for x in cache.get("tool_feature", {}).values()}
        parameter_value_lists = {
            x.id: {"name": x.name, "value_index_list": [int(idx) for idx in x.value_index_list.split(",")]}
            for x in cache.get("parameter_value_list", {}).values()
        }
        for item in items:
            try:
                with _manage_stocks(
                    item,
                    {("tool_feature_id", "method_index"): tool_feature_method_ids},
                    "tool_feature_method",
                    for_update,
                    cache,
                ) as item:
                    check_tool_feature_method(item, tool_feature_method_ids, tool_features, parameter_value_lists)
                    checked_items.append(item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_alternatives(self, *items, for_update=False, strict=False, cache=None):
        """Check whether alternatives passed as argument respect integrity constraints.

        Args:
            items (Iterable): One or more Python :class:`dict` objects representing the items to be checked.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if one of the items violates an integrity constraint.

        Returns
            list: items that passed the check.
            list: :exc:`~.exception.SpineIntegrityError` instances corresponding to found violations.
        """
        if cache is None:
            cache = self.make_cache({"alternative"}, include_ancestors=True)
        intgr_error_log = []
        checked_items = list()
        alternative_ids = {x.name: x.id for x in cache.get("alternative", {}).values()}
        for item in items:
            try:
                with _manage_stocks(item, {("name",): alternative_ids}, "alternative", for_update, cache) as item:
                    check_alternative(item, alternative_ids)
                    checked_items.append(item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_scenarios(self, *items, for_update=False, strict=False, cache=None):
        """Check whether scenarios passed as argument respect integrity constraints.

        Args:
            items (Iterable): One or more Python :class:`dict` objects representing the items to be checked.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if one of the items violates an integrity constraint.

        Returns
            list: items that passed the check.
            list: :exc:`~.exception.SpineIntegrityError` instances corresponding to found violations.
        """
        if cache is None:
            cache = self.make_cache({"scenario"}, include_ancestors=True)
        intgr_error_log = []
        checked_items = list()
        scenario_ids = {x.name: x.id for x in cache.get("scenario", {}).values()}
        for item in items:
            try:
                with _manage_stocks(item, {("name",): scenario_ids}, "scenario", for_update, cache) as item:
                    check_scenario(item, scenario_ids)
                    checked_items.append(item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_scenario_alternatives(self, *items, for_update=False, strict=False, cache=None):
        """Check whether scenario alternatives passed as argument respect integrity constraints.

        Args:
            items (Iterable): One or more Python :class:`dict` objects representing the items to be checked.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if one of the items violates an integrity constraint.

        Returns
            list: items that passed the check.
            list: :exc:`~.exception.SpineIntegrityError` instances corresponding to found violations.
        """
        if cache is None:
            cache = self.make_cache({"scenario_alternative"}, include_ancestors=True)
        intgr_error_log = []
        checked_items = list()
        ids_by_alt_id = {}
        ids_by_rank = {}
        for item in cache.get("scenario_alternative", {}).values():
            ids_by_alt_id[item.scenario_id, item.alternative_id] = item.id
            ids_by_rank[item.scenario_id, item.rank] = item.id
        scenario_names = {s.id: s.name for s in cache.get("scenario", {}).values()}
        alternative_names = {s.id: s.name for s in cache.get("alternative", {}).values()}
        for item in items:
            try:
                with _manage_stocks(
                    item,
                    {
                        ("scenario_id", "alternative_id"): ids_by_alt_id,
                        ("scenario_id", "rank"): ids_by_rank,
                    },
                    "scenario_alternative",
                    for_update,
                    cache,
                ) as item:
                    check_scenario_alternative(item, ids_by_alt_id, ids_by_rank, scenario_names, alternative_names)
                    checked_items.append(item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_object_classes(self, *items, for_update=False, strict=False, cache=None):
        """Check whether object classes passed as argument respect integrity constraints.

        Args:
            items (Iterable): One or more Python :class:`dict` objects representing the items to be checked.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if one of the items violates an integrity constraint.

        Returns
            list: items that passed the check.
            list: :exc:`~.exception.SpineIntegrityError` instances corresponding to found violations.
        """
        if cache is None:
            cache = self.make_cache({"object_class"}, include_ancestors=True)
        intgr_error_log = []
        checked_items = list()
        object_class_ids = {x.name: x.id for x in cache.get("object_class", {}).values()}
        for item in items:
            try:
                with _manage_stocks(item, {("name",): object_class_ids}, "object_class", for_update, cache) as item:
                    check_object_class(item, object_class_ids, self.object_class_type)
                    checked_items.append(item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_objects(self, *items, for_update=False, strict=False, cache=None):
        """Check whether objects passed as argument respect integrity constraints.
        Args:
            items (Iterable): One or more Python :class:`dict` objects representing the items to be checked.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if one of the items violates an integrity constraint.

        Returns
            list: items that passed the check.
            list: :exc:`~.exception.SpineIntegrityError` instances corresponding to found violations.
        """
        if cache is None:
            cache = self.make_cache({"object"}, include_ancestors=True)
        intgr_error_log = []
        checked_items = list()
        object_ids = {(x.class_id, x.name): x.id for x in cache.get("object", {}).values()}
        object_class_ids = [x.id for x in cache.get("object_class", {}).values()]
        for item in items:
            try:
                with _manage_stocks(item, {("class_id", "name"): object_ids}, "object", for_update, cache) as item:
                    check_object(item, object_ids, object_class_ids, self.object_entity_type)
                    checked_items.append(item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_wide_relationship_classes(self, *wide_items, for_update=False, strict=False, cache=None):
        """Check whether relationship classes passed as argument respect integrity constraints.

        Args:
            items (Iterable): One or more Python :class:`dict` objects representing the items to be checked.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if one of the items violates an integrity constraint.

        Returns
            list: items that passed the check.
            list: :exc:`~.exception.SpineIntegrityError` instances corresponding to found violations.
        """
        if cache is None:
            cache = self.make_cache({"relationship_class"}, include_ancestors=True)
        intgr_error_log = []
        checked_wide_items = list()
        relationship_class_ids = {x.name: x.id for x in cache.get("relationship_class", {}).values()}
        object_class_ids = [x.id for x in cache.get("object_class", {}).values()]
        for wide_item in wide_items:
            try:
                with _manage_stocks(
                    wide_item, {("name",): relationship_class_ids}, "relationship_class", for_update, cache
                ) as wide_item:
                    check_wide_relationship_class(
                        wide_item, relationship_class_ids, object_class_ids, self.relationship_class_type
                    )
                    checked_wide_items.append(wide_item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_wide_items, intgr_error_log

    def check_wide_relationships(self, *wide_items, for_update=False, strict=False, cache=None):
        """Check whether relationships passed as argument respect integrity constraints.

        Args:
            items (Iterable): One or more Python :class:`dict` objects representing the items to be checked.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if one of the items violates an integrity constraint.

        Returns
            list: items that passed the check.
            list: :exc:`~.exception.SpineIntegrityError` instances corresponding to found violations.
        """
        if cache is None:
            cache = self.make_cache({"relationship"}, include_ancestors=True)
        intgr_error_log = []
        checked_wide_items = list()
        relationship_ids_by_name = {(x.class_id, x.name): x.id for x in cache.get("relationship", {}).values()}
        relationship_ids_by_obj_lst = {
            (x.class_id, x.object_id_list): x.id for x in cache.get("relationship", {}).values()
        }
        relationship_classes = {
            x.id: {"object_class_id_list": [int(y) for y in x.object_class_id_list.split(",")], "name": x.name}
            for x in cache.get("relationship_class", {}).values()
        }
        objects = {x.id: {"class_id": x.class_id, "name": x.name} for x in cache.get("object", {}).values()}
        for wide_item in wide_items:
            try:
                with _manage_stocks(
                    wide_item,
                    {
                        ("class_id", "name"): relationship_ids_by_name,
                        ("class_id", "object_id_list"): relationship_ids_by_obj_lst,
                    },
                    "relationship",
                    for_update,
                    cache,
                ) as wide_item:
                    check_wide_relationship(
                        wide_item,
                        relationship_ids_by_name,
                        relationship_ids_by_obj_lst,
                        relationship_classes,
                        objects,
                        self.relationship_entity_type,
                    )
                    wide_item["object_class_id_list"] = [
                        objects[id_]["class_id"] for id_ in wide_item["object_id_list"]
                    ]
                    checked_wide_items.append(wide_item)
                # FIXME
                # join_object_id_list = ",".join([str(x) for x in wide_item["object_id_list"]])
                # relationship_ids_by_obj_lst[wide_item["class_id"], join_object_id_list] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_wide_items, intgr_error_log

    def check_entity_groups(self, *items, for_update=False, strict=False, cache=None):
        """Check whether entity groups passed as argument respect integrity constraints.

        Args:
            items (Iterable): One or more Python :class:`dict` objects representing the items to be checked.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if one of the items violates an integrity constraint.

        Returns
            list: items that passed the check.
            list: :exc:`~.exception.SpineIntegrityError` instances corresponding to found violations.
        """
        if cache is None:
            cache = self.make_cache({"entity_group"}, include_ancestors=True)
        intgr_error_log = list()
        checked_items = list()
        current_ids = {(x.group_id, x.member_id): x.id for x in cache.get("entity_group", {}).values()}
        entities = {}
        for entity in chain(cache.get("object", {}).values(), cache.get("relationship", {}).values()):
            entities.setdefault(entity.class_id, dict())[entity.id] = entity._asdict()
        for item in items:
            try:
                with _manage_stocks(
                    item, {("entity_id", "member_id"): current_ids}, "entity_group", for_update, cache
                ) as item:
                    check_entity_group(item, current_ids, entities)
                    checked_items.append(item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_parameter_definitions(self, *items, for_update=False, strict=False, cache=None):
        """Check whether parameter definitions passed as argument respect integrity constraints.

        Args:
            items (Iterable): One or more Python :class:`dict` objects representing the items to be checked.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if one of the items violates an integrity constraint.

        Returns
            list: items that passed the check.
            list: :exc:`~.exception.SpineIntegrityError` instances corresponding to found violations.
        """
        if cache is None:
            cache = self.make_cache({"parameter_definition"}, include_ancestors=True)
        intgr_error_log = []
        checked_items = list()
        parameter_definition_ids = {
            (x.entity_class_id, x.parameter_name): x.id for x in cache.get("parameter_definition", {}).values()
        }
        object_class_ids = {x.id for x in cache.get("object_class", {}).values()}
        relationship_class_ids = {x.id for x in cache.get("relationship_class", {}).values()}
        entity_class_ids = object_class_ids | relationship_class_ids
        parameter_value_lists = {x.id: x.value_id_list for x in cache.get("parameter_value_list", {}).values()}
        list_values = {x.id: from_database(x.value, x.type) for x in cache.get("list_value", {}).values()}
        for item in items:
            object_class_id = item.get("object_class_id")
            relationship_class_id = item.get("relationship_class_id")
            if object_class_id and relationship_class_id:
                e = SpineIntegrityError("Can't associate a parameter to both an object and a relationship class.")
                if strict:
                    raise e
                intgr_error_log.append(e)
                continue
            if object_class_id:
                class_ids = object_class_ids
            elif relationship_class_id:
                class_ids = relationship_class_ids
            else:
                class_ids = entity_class_ids
            item["entity_class_id"] = object_class_id or relationship_class_id or item.get("entity_class_id")
            try:
                with _manage_stocks(
                    item,
                    {("entity_class_id", "name"): parameter_definition_ids},
                    "parameter_definition",
                    for_update,
                    cache,
                ) as item:
                    check_parameter_definition(
                        item, parameter_definition_ids, class_ids, parameter_value_lists, list_values
                    )
                    checked_items.append(item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_parameter_values(self, *items, for_update=False, strict=False, cache=None):
        """Check whether parameter values passed as argument respect integrity constraints.

        Args:
            items (Iterable): One or more Python :class:`dict` objects representing the items to be checked.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if one of the items violates an integrity constraint.

        Returns
            list: items that passed the check.
            list: :exc:`~.exception.SpineIntegrityError` instances corresponding to found violations.
        """
        if cache is None:
            cache = self.make_cache({"parameter_value"}, include_ancestors=True)
        intgr_error_log = []
        checked_items = list()
        parameter_value_ids = {
            (x.entity_id, x.parameter_id, x.alternative_id): x.id for x in cache.get("parameter_value", {}).values()
        }
        parameter_definitions = {
            x.id: {
                "name": x.parameter_name,
                "entity_class_id": x.entity_class_id,
                "parameter_value_list_id": x.value_list_id,
            }
            for x in cache.get("parameter_definition", {}).values()
        }
        entities = {
            x.id: {"class_id": x.class_id, "name": x.name}
            for x in chain(cache.get("object", {}).values(), cache.get("relationship", {}).values())
        }
        parameter_value_lists = {x.id: x.value_id_list for x in cache.get("parameter_value_list", {}).values()}
        list_values = {x.id: from_database(x.value, x.type) for x in cache.get("list_value", {}).values()}
        alternatives = set(a.id for a in cache.get("alternative", {}).values())
        for item in items:
            item["entity_class_id"] = (
                item.get("object_class_id") or item.get("relationship_class_id") or item.get("entity_class_id")
            )
            item["entity_id"] = item.get("object_id") or item.get("relationship_id") or item.get("entity_id")
            item["alternative_id"] = item.get("alternative_id")
            try:
                with _manage_stocks(
                    item,
                    {("entity_id", "parameter_definition_id", "alternative_id"): parameter_value_ids},
                    "parameter_value",
                    for_update,
                    cache,
                ) as item:
                    check_parameter_value(
                        item,
                        parameter_value_ids,
                        parameter_definitions,
                        entities,
                        parameter_value_lists,
                        list_values,
                        alternatives,
                    )
                    checked_items.append(item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_parameter_value_lists(self, *items, for_update=False, strict=False, cache=None):
        """Check whether parameter value-lists passed as argument respect integrity constraints.

        Args:
            items (Iterable): One or more Python :class:`dict` objects representing the items to be checked.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if one of the items violates an integrity constraint.

        Returns
            list: items that passed the check.
            list: :exc:`~.exception.SpineIntegrityError` instances corresponding to found violations.
        """
        if cache is None:
            cache = self.make_cache({"parameter_value_list"}, include_ancestors=True)
        intgr_error_log = []
        checked_items = list()
        parameter_value_list_ids = {x.name: x.id for x in cache.get("parameter_value_list", {}).values()}
        for item in items:
            try:
                with _manage_stocks(
                    item, {("name"): parameter_value_list_ids}, "parameter_value_list", for_update, cache
                ) as item:
                    check_parameter_value_list(item, parameter_value_list_ids)
                    checked_items.append(item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_list_values(self, *items, for_update=False, strict=False, cache=None):
        """Check whether list values passed as argument respect integrity constraints.

        Args:
            items (Iterable): One or more Python :class:`dict` objects representing the items to be checked.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if one of the items violates an integrity constraint.

        Returns
            list: items that passed the check.
            list: :exc:`~.exception.SpineIntegrityError` instances corresponding to found violations.
        """
        if cache is None:
            cache = self.make_cache({"list_value"}, include_ancestors=True)
        intgr_error_log = []
        checked_items = list()
        list_value_ids_by_index = {
            (x.parameter_value_list_id, x.index): x.id for x in cache.get("list_value", {}).values()
        }
        list_value_ids_by_value = {
            (x.parameter_value_list_id, x.type, x.value): x.id for x in cache.get("list_value", {}).values()
        }
        list_names_by_id = {x.id: x.name for x in cache.get("parameter_value_list", {}).values()}
        for item in items:
            try:
                with _manage_stocks(
                    item,
                    {
                        ("parameter_value_list_id", "index"): list_value_ids_by_index,
                        ("parameter_value_list_id", "type", "value"): list_value_ids_by_value,
                    },
                    "list_value",
                    for_update,
                    cache,
                ) as item:
                    check_list_value(item, list_names_by_id, list_value_ids_by_index, list_value_ids_by_value)
                    checked_items.append(item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log


@contextmanager
def _manage_stocks(item, existing_ids_by_key_fields, item_type, for_update, cache):
    if for_update:
        try:
            id_ = item["id"]
        except KeyError:
            raise SpineIntegrityError(f"Missing {item_type} identifier.") from None
        try:
            cache_item = cache.get(item_type, {})[id_]
        except KeyError:
            raise SpineIntegrityError(f"{item_type} not found.") from None
        full_item = cache_item._asdict()
    else:
        id_ = None
        full_item = item
    try:
        existing_ids_by_key = {
            _get_key(full_item, key_fields): existing_ids
            for key_fields, existing_ids in existing_ids_by_key_fields.items()
        }
    except KeyError as e:
        raise SpineIntegrityError(f"Missing key field {e} for {item_type}.") from None
    if for_update:
        try:
            # Remove from existing
            for key, existing_ids in existing_ids_by_key.items():
                del existing_ids[key]
        except KeyError:
            raise SpineIntegrityError(f"{item_type} not found.") from None
        full_item.update(item)
    try:
        yield full_item
        # Check is performed at this point
    except SpineIntegrityError:  # pylint: disable=try-except-raise
        # Check didn't pass, so reraise
        raise
    else:
        # Check passed, so add to existing
        for key, existing_ids in existing_ids_by_key.items():
            existing_ids[key] = id_
        if for_update:
            cache.get(item_type, {})[id_] = CacheItem(**full_item)


def _get_key(item, key_fields):
    return tuple(item[field] for field in key_fields) if len(key_fields) > 1 else item[key_fields[0]]


def _fix_immutable_fields(current_item, item, immutable_fields):
    fixed = []
    for field in immutable_fields:
        if current_item[field] is None:
            continue
        if field in item and item[field] != current_item[field]:
            fixed.append(field)
        item[field] = current_item[field]
    if fixed:
        fixed = ', '.join([f"'{field}'" for field in fixed])
        return [SpineIntegrityError(f"Can't update fields {fixed}")]
    return []
