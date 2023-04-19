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

"""Provides :class:`.DatabaseMappingCheckMixin`.

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
    check_entity_metadata,
    check_metadata,
    check_parameter_value_metadata,
)
from .parameter_value import from_database


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
            "metadata": self.check_metadata,
            "entity_metadata": self.check_entity_metadata,
            "parameter_value_metadata": self.check_parameter_value_metadata,
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
                with self._manage_stocks(
                    "feature", item, {("parameter_definition_id",): feature_ids}, for_update, cache, intgr_error_log
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
                with self._manage_stocks(
                    "tool", item, {("name",): tool_ids}, for_update, cache, intgr_error_log
                ) as item:
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
                with self._manage_stocks(
                    "tool_feature",
                    item,
                    {("tool_id", "feature_id"): tool_feature_ids},
                    for_update,
                    cache,
                    intgr_error_log,
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
            x.id: {"name": x.name, "value_index_list": x.value_index_list}
            for x in cache.get("parameter_value_list", {}).values()
        }
        for item in items:
            try:
                with self._manage_stocks(
                    "tool_feature_method",
                    item,
                    {("tool_feature_id", "method_index"): tool_feature_method_ids},
                    for_update,
                    cache,
                    intgr_error_log,
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
                with self._manage_stocks(
                    "alternative", item, {("name",): alternative_ids}, for_update, cache, intgr_error_log
                ) as item:
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
                with self._manage_stocks(
                    "scenario", item, {("name",): scenario_ids}, for_update, cache, intgr_error_log
                ) as item:
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
                with self._manage_stocks(
                    "scenario_alternative",
                    item,
                    {("scenario_id", "alternative_id"): ids_by_alt_id, ("scenario_id", "rank"): ids_by_rank},
                    for_update,
                    cache,
                    intgr_error_log,
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
                with self._manage_stocks(
                    "object_class", item, {("name",): object_class_ids}, for_update, cache, intgr_error_log
                ) as item:
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
                with self._manage_stocks(
                    "object", item, {("class_id", "name"): object_ids}, for_update, cache, intgr_error_log
                ) as item:
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
                with self._manage_stocks(
                    "relationship_class",
                    wide_item,
                    {("name",): relationship_class_ids},
                    for_update,
                    cache,
                    intgr_error_log,
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
            x.id: {"object_class_id_list": x.object_class_id_list, "name": x.name}
            for x in cache.get("relationship_class", {}).values()
        }
        objects = {x.id: {"class_id": x.class_id, "name": x.name} for x in cache.get("object", {}).values()}
        for wide_item in wide_items:
            try:
                with self._manage_stocks(
                    "relationship",
                    wide_item,
                    {
                        ("class_id", "name"): relationship_ids_by_name,
                        ("class_id", "object_id_list"): relationship_ids_by_obj_lst,
                    },
                    for_update,
                    cache,
                    intgr_error_log,
                ) as wide_item:
                    check_wide_relationship(
                        wide_item,
                        relationship_ids_by_name,
                        relationship_ids_by_obj_lst,
                        relationship_classes,
                        objects,
                        self.relationship_entity_type,
                    )
                    checked_wide_items.append(wide_item)
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
                with self._manage_stocks(
                    "entity_group", item, {("entity_id", "member_id"): current_ids}, for_update, cache, intgr_error_log
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
            strict (bool): Whether the method should raise :exc:`~.exception.SpineIntegrityError`
                if one of the items violates an integrity constraint.

        Returns:
            list: items that passed the check.
            list: :exc:`~.exception.SpineIntegrityError` instances corresponding to found violations.
        """
        if cache is None:
            cache = self.make_cache({"parameter_definition", "parameter_value"}, include_ancestors=True)
        parameter_definition_ids_with_values = {
            value.parameter_id for value in cache.get("parameter_value", {}).values()
        }
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
            entity_class_id = object_class_id or relationship_class_id
            if entity_class_id is not None:
                item["entity_class_id"] = entity_class_id
            try:
                if (
                    for_update
                    and item["id"] in parameter_definition_ids_with_values
                    and item["parameter_value_list_id"] != cache["parameter_definition"][item["id"]].value_list_id
                ):
                    raise SpineIntegrityError(
                        f"Can't change value list on parameter {item['name']} because it has parameter values."
                    )
                with self._manage_stocks(
                    "parameter_definition",
                    item,
                    {("entity_class_id", "name"): parameter_definition_ids},
                    for_update,
                    cache,
                    intgr_error_log,
                ) as full_item:
                    check_parameter_definition(
                        full_item, parameter_definition_ids, class_ids, parameter_value_lists, list_values
                    )
                    checked_items.append(full_item)
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
            entity_id = item.get("object_id") or item.get("relationship_id")
            if entity_id is not None:
                item["entity_id"] = entity_id
            try:
                with self._manage_stocks(
                    "parameter_value",
                    item,
                    {("entity_id", "parameter_definition_id", "alternative_id"): parameter_value_ids},
                    for_update,
                    cache,
                    intgr_error_log,
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
                with self._manage_stocks(
                    "parameter_value_list",
                    item,
                    {("name",): parameter_value_list_ids},
                    for_update,
                    cache,
                    intgr_error_log,
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
                with self._manage_stocks(
                    "list_value",
                    item,
                    {
                        ("parameter_value_list_id", "index"): list_value_ids_by_index,
                        ("parameter_value_list_id", "type", "value"): list_value_ids_by_value,
                    },
                    for_update,
                    cache,
                    intgr_error_log,
                ) as item:
                    check_list_value(item, list_names_by_id, list_value_ids_by_index, list_value_ids_by_value)
                    checked_items.append(item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_metadata(self, *items, for_update=False, strict=False, cache=None):
        """Checks whether metadata respects integrity constraints.

        Args:
            *items: One or more Python :class:`dict` objects representing the items to be checked.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if one of the items violates an integrity constraint.
            cache (dict, optional): Database cache

        Returns
            list: items that passed the check.
            list: :exc:`~.exception.SpineIntegrityError` instances corresponding to found violations.
        """
        if cache is None:
            cache = self.make_cache({"metadata"})
        intgr_error_log = []
        checked_items = list()
        metadata = {(x.name, x.value): x.id for x in cache.get("metadata", {}).values()}
        for item in items:
            try:
                with self._manage_stocks(
                    "metadata", item, {("name", "value"): metadata}, for_update, cache, intgr_error_log
                ) as item:
                    check_metadata(item, metadata)
                    if (item["name"], item["value"]) not in metadata:
                        checked_items.append(item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_entity_metadata(self, *items, for_update=False, strict=False, cache=None):
        """Checks whether entity metadata respects integrity constraints.

        Args:
            *items: One or more Python :class:`dict` objects representing the items to be checked.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if one of the items violates an integrity constraint.
            cache (dict, optional): Database cache

        Returns
            list: items that passed the check.
            list: :exc:`~.exception.SpineIntegrityError` instances corresponding to found violations.
        """
        if cache is None:
            cache = self.make_cache({"entity_metadata"}, include_ancestors=True)
        intgr_error_log = []
        checked_items = list()
        entities = {x.id for x in cache.get("object", {}).values()}
        entities |= {x.id for x in cache.get("relationship", {}).values()}
        metadata = {x.id for x in cache.get("metadata", {}).values()}
        for item in items:
            try:
                with self._manage_stocks("entity_metadata", item, {}, for_update, cache, intgr_error_log) as item:
                    check_entity_metadata(item, entities, metadata)
                    checked_items.append(item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_parameter_value_metadata(self, *items, for_update=False, strict=False, cache=None):
        """Checks whether parameter value metadata respects integrity constraints.

        Args:
            *items: One or more Python :class:`dict` objects representing the items to be checked.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if one of the items violates an integrity constraint.
            cache (dict, optional): Database cache

        Returns
            list: items that passed the check.
            list: :exc:`~.exception.SpineIntegrityError` instances corresponding to found violations.
        """
        if cache is None:
            cache = self.make_cache({"parameter_value_metadata"}, include_ancestors=True)
        intgr_error_log = []
        checked_items = list()
        values = {x.id for x in cache.get("parameter_value", {}).values()}
        metadata = {x.id for x in cache.get("metadata", {}).values()}
        for item in items:
            try:
                with self._manage_stocks(
                    "parameter_value_metadata", item, {}, for_update, cache, intgr_error_log
                ) as item:
                    check_parameter_value_metadata(item, values, metadata)
                    checked_items.append(item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    @contextmanager
    def _manage_stocks(self, item_type, item, existing_ids_by_pk, for_update, cache, intgr_error_log):
        if for_update:
            try:
                id_ = item["id"]
            except KeyError:
                raise SpineIntegrityError(f"Missing {item_type} identifier.") from None
            try:
                full_item = cache.get(item_type, {})[id_]
            except KeyError:
                raise SpineIntegrityError(f"{item_type} not found.") from None
        else:
            id_ = None
            full_item = cache.make_item(item_type, item)
        try:
            existing_ids_by_key = {
                _get_key(full_item, pk): existing_ids for pk, existing_ids in existing_ids_by_pk.items()
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
            intgr_error_log += _fix_immutable_fields(item_type, full_item, item)
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
                cache.get(item_type, {})[id_] = full_item


def _get_key_values(item, pk):
    for field in pk:
        value = item[field]
        if isinstance(value, list):
            value = tuple(value)
        yield value


def _get_key(item, pk):
    key = tuple(_get_key_values(item, pk))
    if len(key) > 1:
        return key
    return key[0]


def _fix_immutable_fields(item_type, current_item, item):
    immutable_fields = {
        "object": ("class_id",),
        "relationship_class": ("object_class_id_list",),
        "relationship": ("class_id",),
        "parameter_definition": ("entity_class_id", "object_class_id", "relationship_class_id"),
        "parameter_value": ("entity_class_id", "object_class_id", "relationship_class_id"),
    }.get(item_type, ())
    fixed = []
    for field in immutable_fields:
        if current_item.get(field) is None:
            continue
        if field in item and item[field] != current_item[field]:
            fixed.append(field)
        item[field] = current_item[field]
    if fixed:
        fixed = ', '.join([f"'{field}'" for field in fixed])
        return [SpineIntegrityError(f"Can't update fixed fields {fixed}")]
    return []
