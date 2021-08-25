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
    check_wide_parameter_value_list,
    check_feature,
    check_tool,
    check_tool_feature,
    check_tool_feature_method,
)


# NOTE: To check for an update we remove the current instance from our lookup dictionary,
# check for an insert of the updated instance,
# and finally reinsert the instance to the dictionary
class DatabaseMappingCheckMixin:
    """Provides methods to check whether insert and update operations violate Spine db integrity constraints.
    """

    @staticmethod
    def check_immutable_fields(current_item, item, immutable_fields):
        for field in immutable_fields:
            if field not in item:
                continue
            value = item[field]
            current_value = current_item[field]
            if value != current_value:
                raise SpineIntegrityError("Cannot change field {0} from {1} to {2}".format(field, current_value, value))

    def check_items_for_insert(self, tablename, *items, strict=False, cache=None):
        return {
            "alternative": self.check_alternatives_for_insert,
            "scenario": self.check_scenarios_for_insert,
            "scenario_alternative": self.check_scenario_alternatives_for_insert,
            "object": self.check_objects_for_insert,
            "object_class": self.check_object_classes_for_insert,
            "relationship_class": self.check_wide_relationship_classes_for_insert,
            "relationship": self.check_wide_relationships_for_insert,
            "entity_group": self.check_entity_groups_for_insert,
            "parameter_definition": self.check_parameter_definitions_for_insert,
            "parameter_value": self.check_parameter_values_for_insert,
            "parameter_value_list": self.check_wide_parameter_value_lists_for_insert,
            "feature": self.check_features_for_insert,
            "tool": self.check_tools_for_insert,
            "tool_feature": self.check_tool_features_for_insert,
            "tool_feature_method": self.check_tool_feature_methods_for_insert,
        }[tablename](*items, strict=strict, cache=cache)

    def check_items_for_update(self, tablename, *items, strict=False, cache=None):
        return {
            "alternative": self.check_alternatives_for_update,
            "scenario": self.check_scenarios_for_update,
            "scenario_alternative": self.check_scenario_alternatives_for_update,
            "object": self.check_objects_for_update,
            "object_class": self.check_object_classes_for_update,
            "relationship_class": self.check_wide_relationship_classes_for_update,
            "relationship": self.check_wide_relationships_for_update,
            "parameter_definition": self.check_parameter_definitions_for_update,
            "parameter_value": self.check_parameter_values_for_update,
            "parameter_value_list": self.check_wide_parameter_value_lists_for_update,
            "feature": self.check_features_for_update,
            "tool": self.check_tools_for_update,
            "tool_feature": self.check_tool_features_for_update,
            "tool_feature_method": self.check_tool_feature_methods_for_update,
        }[tablename](*items, strict=strict, cache=cache)

    def check_features_for_insert(self, *items, strict=False, cache=None):
        """Check whether features passed as argument respect integrity constraints
        for an insert operation.

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
                check_feature(item, feature_ids, parameter_definitions)
                checked_items.append(item)
                feature_ids[item["parameter_definition_id"]] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_features_for_update(self, *items, strict=False, cache=None):
        """Check whether features passed as argument respect integrity constraints
        for an update operation.

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
        features = {x.id: x._asdict() for x in cache.get("feature", {}).values()}
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
                id_ = item["id"]
            except KeyError:
                msg = "Missing feature identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                # Simulate removal of current instance
                updated_item = features.pop(id_)
                del feature_ids[updated_item["parameter_definition_id"]]
            except KeyError:
                msg = "feature not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            # Check for an insert of the updated instance
            try:
                updated_item.update(item)
                check_feature(updated_item, feature_ids, parameter_definitions)
                checked_items.append(item)
                # If the check passes, reinject the updated instance for next iteration.
                features[id_] = updated_item
                feature_ids[updated_item["parameter_definition_id"]] = id_
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_tools_for_insert(self, *items, strict=False, cache=None):
        """Check whether tools passed as argument respect integrity constraints
        for an insert operation.

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
                check_tool(item, tool_ids)
                checked_items.append(item)
                # If the check passes, append item to `object_class_names` for next iteration.
                tool_ids[item["name"]] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_tools_for_update(self, *items, strict=False, cache=None):
        """Check whether tools passed as argument respect integrity constraints
        for an update operation.

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
        tools = {x.id: {"name": x.name} for x in cache.get("tool", {}).values()}
        tool_ids = {x.name: x.id for x in cache.get("tool", {}).values()}
        for item in items:
            try:
                id_ = item["id"]
            except KeyError:
                msg = "Missing tool identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                # Simulate removal of current instance
                updated_item = tools.pop(id_)
                del tool_ids[updated_item["name"]]
            except KeyError:
                msg = "tool not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            # Check for an insert of the updated instance
            try:
                updated_item.update(item)
                check_tool(updated_item, tool_ids)
                checked_items.append(item)
                # If the check passes, reinject the updated instance for next iteration.
                tools[id_] = updated_item
                tool_ids[updated_item["name"]] = id_
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_tool_features_for_insert(self, *items, strict=False, cache=None):
        """Check whether tool features passed as argument respect integrity constraints
        for an insert operation.

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
                check_tool_feature(item, tool_feature_ids, tools, features)
                checked_items.append(item)
                tool_feature_ids[item["tool_id"], item["feature_id"]] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_tool_features_for_update(self, *items, strict=False, cache=None):
        """Check whether tool_features passed as argument respect integrity constraints
        for an update operation.

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
        tool_features = {x.id: x._asdict() for x in cache.get("tool_feature", {}).values()}
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
                id_ = item["id"]
            except KeyError:
                msg = "Missing tool feature identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                # Simulate removal of current instance
                updated_item = tool_features.pop(id_)
                del tool_feature_ids[updated_item["tool_id"], updated_item["feature_id"]]
            except KeyError:
                msg = "tool feature not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            # Check for an insert of the updated instance
            try:
                updated_item.update(item)
                check_tool_feature(updated_item, tool_feature_ids, tools, features)
                checked_items.append(item)
                # If the check passes, reinject the updated instance for next iteration.
                tool_features[id_] = updated_item
                tool_feature_ids[updated_item["tool_id"], updated_item["feature_id"]] = id_
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_tool_feature_methods_for_insert(self, *items, strict=False, cache=None):
        """Check whether tool feature methods passed as argument respect integrity constraints
        for an insert operation.

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
            x.id: {"name": x.name, "value_index_list": [int(idx) for idx in x.value_index_list.split(";")]}
            for x in cache.get("parameter_value_list", {}).values()
        }
        for item in items:
            try:
                check_tool_feature_method(item, tool_feature_method_ids, tool_features, parameter_value_lists)
                checked_items.append(item)
                tool_feature_method_ids[item["tool_feature_id"], item["method_index"]] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_tool_feature_methods_for_update(self, *items, strict=False, cache=None):
        """Check whether tool_feature_methods passed as argument respect integrity constraints
        for an update operation.

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
        tool_feature_methods = {x.id: x._asdict() for x in cache.get("tool_feature_method", {}).values()}
        tool_feature_method_ids = {
            (x.tool_feature_id, x.method_index): x.id for x in cache.get("tool_feature_method", {}).values()
        }
        tool_features = {x.id: x._asdict() for x in cache.get("tool_feature", {}).values()}
        parameter_value_lists = {
            x.id: {"name": x.name, "value_index_list": [int(idx) for idx in x.value_index_list.split(";")]}
            for x in cache.get("parameter_value_list", {}).values()
        }
        for item in items:
            try:
                id_ = item["id"]
            except KeyError:
                msg = "Missing tool feature method identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                # Simulate removal of current instance
                updated_item = tool_feature_methods.pop(id_)
                del tool_feature_method_ids[updated_item["tool_feature_id"], updated_item["method_index"]]
            except KeyError:
                msg = "tool feature method not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            # Check for an insert of the updated instance
            try:
                updated_item.update(item)
                check_tool_feature_method(updated_item, tool_feature_method_ids, tool_features, parameter_value_lists)
                checked_items.append(item)
                # If the check passes, reinject the updated instance for next iteration.
                tool_feature_methods[id_] = updated_item
                tool_feature_method_ids[updated_item["tool_feature_id"], updated_item["method_index"]] = id_
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_alternatives_for_insert(self, *items, strict=False, cache=None):
        """Check whether alternatives passed as argument respect integrity constraints
        for an insert operation.

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
                check_alternative(item, alternative_ids)
                checked_items.append(item)
                # If the check passes, append item to `object_class_names` for next iteration.
                alternative_ids[item["name"]] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_alternatives_for_update(self, *items, strict=False, cache=None):
        """Check whether alternatives passed as argument respect integrity constraints
        for an update operation.

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
        alternatives = {x.id: {"name": x.name} for x in cache.get("alternative", {}).values()}
        alternative_ids = {x.name: x.id for x in cache.get("alternative", {}).values()}
        for item in items:
            try:
                id_ = item["id"]
            except KeyError:
                msg = "Missing alternative identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                # Simulate removal of current instance
                updated_item = alternatives.pop(id_)
                del alternative_ids[updated_item["name"]]
            except KeyError:
                msg = "alternative not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            # Check for an insert of the updated instance
            try:
                updated_item.update(item)
                check_alternative(updated_item, alternative_ids)
                checked_items.append(item)
                # If the check passes, reinject the updated instance for next iteration.
                alternatives[id_] = updated_item
                alternative_ids[updated_item["name"]] = id_
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_scenarios_for_insert(self, *items, strict=False, cache=None):
        """Check whether scenarios passed as argument respect integrity constraints
        for an insert operation.

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
                check_scenario(item, scenario_ids)
                checked_items.append(item)
                scenario_ids[item["name"]] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_scenarios_for_update(self, *items, strict=False, cache=None):
        """Check whether scenarios passed as argument respect integrity constraints
        for an update operation.

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
        scenarios = {x.id: {"name": x.name} for x in cache.get("scenario", {}).values()}
        scenario_ids = {x.name: x.id for x in cache.get("scenario", {}).values()}
        for item in items:
            try:
                id_ = item["id"]
            except KeyError:
                msg = "Missing scenario identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                # Simulate removal of current instance
                updated_item = scenarios.pop(id_)
                del scenario_ids[updated_item["name"]]
            except KeyError:
                msg = "alternative not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            # Check for an insert of the updated instance
            try:
                updated_item.update(item)
                check_scenario(updated_item, scenario_ids)
                checked_items.append(item)
                # If the check passes, reinject the updated instance for next iteration.
                scenarios[id_] = updated_item
                scenario_ids[updated_item["name"]] = id_
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_scenario_alternatives_for_insert(self, *items, strict=False, cache=None):
        """Check whether scenario alternatives passed as argument respect integrity constraints
        for an insert operation.

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
                check_scenario_alternative(item, ids_by_alt_id, ids_by_rank, scenario_names, alternative_names)
                checked_items.append(item)
                ids_by_alt_id[item["scenario_id"], item["alternative_id"]] = None
                ids_by_rank[item["scenario_id"], item["rank"]] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_scenario_alternatives_for_update(self, *items, strict=False, cache=None):
        """Check whether scenario alternatives passed as argument respect integrity constraints
        for an insert operation.

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
        scenario_alternatives = {sa.id: sa._asdict() for sa in cache.get("scenario_alternative", {}).values()}
        scenario_names = {s.id: s.name for s in cache.get("scenario", {}).values()}
        alternative_names = {s.id: s.name for s in cache.get("alternative", {}).values()}
        for item in items:
            try:
                id_ = item["id"]
            except KeyError:
                msg = "Missing scenario alternative identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                updated_item = scenario_alternatives.pop(id_)
                del ids_by_alt_id[updated_item["scenario_id"], updated_item["alternative_id"]]
                del ids_by_rank[updated_item["scenario_id"], updated_item["rank"]]
            except KeyError:
                msg = "Scenario alternative not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                updated_item.update(item)
                check_scenario_alternative(updated_item, ids_by_alt_id, ids_by_rank, scenario_names, alternative_names)
                checked_items.append(item)
                ids_by_alt_id[updated_item["scenario_id"], updated_item["alternative_id"]] = id_
                ids_by_rank[updated_item["scenario_id"], updated_item["rank"]] = id_
                scenario_alternatives[id_] = updated_item
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_object_classes_for_insert(self, *items, strict=False, cache=None):
        """Check whether object classes passed as argument respect integrity constraints
        for an insert operation.

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
                check_object_class(item, object_class_ids, self.object_class_type)
                checked_items.append(item)
                # If the check passes, append item to `object_class_ids` for next iteration.
                object_class_ids[item["name"]] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_object_classes_for_update(self, *items, strict=False, cache=None):
        """Check whether object classes passed as argument respect integrity constraints
        for an update operation.

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
        object_classes = {x.id: {"name": x.name} for x in cache.get("object_class", {}).values()}
        object_class_ids = {x.name: x.id for x in cache.get("object_class", {}).values()}
        for item in items:
            try:
                id_ = item["id"]
            except KeyError:
                msg = "Missing object class identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                # Simulate removal of current instance
                updated_item = object_classes.pop(id_)
                del object_class_ids[updated_item["name"]]
            except KeyError:
                msg = "Object class not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            # Check for an insert of the updated instance
            try:
                updated_item.update(item)
                check_object_class(updated_item, object_class_ids, self.object_class_type)
                checked_items.append(item)
                # If the check passes, reinject the updated instance for next iteration.
                object_classes[id_] = updated_item
                object_class_ids[updated_item["name"]] = id_
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_objects_for_insert(self, *items, strict=False, cache=None):
        """Check whether objects passed as argument respect integrity constraints
        for an insert operation.

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
                check_object(item, object_ids, object_class_ids, self.object_entity_type)
                checked_items.append(item)
                object_ids[item["class_id"], item["name"]] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_objects_for_update(self, *items, strict=False, cache=None):
        """Check whether objects passed as argument respect integrity constraints
        for an update operation.

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
        objects = {x.id: {"name": x.name, "class_id": x.class_id} for x in cache.get("object", {}).values()}
        object_class_ids = [x.id for x in cache.get("object_class", {}).values()]
        for item in items:
            try:
                id_ = item["id"]
            except KeyError:
                msg = "Missing object identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                updated_item = objects.pop(id_)
                del object_ids[updated_item["class_id"], updated_item["name"]]
            except KeyError:
                msg = "Object not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                self.check_immutable_fields(updated_item, item, ("class_id",))
                updated_item.update(item)
                check_object(updated_item, object_ids, object_class_ids, self.object_entity_type)
                checked_items.append(item)
                objects[id_] = updated_item
                object_ids[updated_item["class_id"], updated_item["name"]] = id_
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_wide_relationship_classes_for_insert(self, *wide_items, strict=False, cache=None):
        """Check whether relationship classes passed as argument respect integrity constraints
        for an insert operation.

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
                check_wide_relationship_class(
                    wide_item, relationship_class_ids, object_class_ids, self.relationship_class_type
                )
                checked_wide_items.append(wide_item)
                relationship_class_ids[wide_item["name"]] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_wide_items, intgr_error_log

    def check_wide_relationship_classes_for_update(self, *wide_items, strict=False, cache=None):
        """Check whether relationship classes passed as argument respect integrity constraints
        for an update operation.

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
        relationship_classes = {
            x.id: {"name": x.name, "object_class_id_list": [int(y) for y in x.object_class_id_list.split(",")]}
            for x in cache.get("relationship_class", {}).values()
        }
        object_class_ids = [x.id for x in cache.get("object_class", {}).values()]
        for wide_item in wide_items:
            try:
                id_ = wide_item["id"]
            except KeyError:
                msg = "Missing relationship class identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                updated_wide_item = relationship_classes.pop(id_)
                del relationship_class_ids[updated_wide_item["name"]]
            except KeyError:
                msg = "Relationship class not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                self.check_immutable_fields(updated_wide_item, wide_item, ("object_class_id_list",))
                updated_wide_item.update(wide_item)
                check_wide_relationship_class(
                    updated_wide_item, relationship_class_ids, object_class_ids, self.relationship_class_type
                )
                checked_wide_items.append(wide_item)
                relationship_classes[id_] = updated_wide_item
                relationship_class_ids[updated_wide_item["name"]] = id_
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_wide_items, intgr_error_log

    def check_wide_relationships_for_insert(self, *wide_items, strict=False, cache=None):
        """Check whether relationships passed as argument respect integrity constraints
        for an insert operation.

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
                check_wide_relationship(
                    wide_item,
                    relationship_ids_by_name,
                    relationship_ids_by_obj_lst,
                    relationship_classes,
                    objects,
                    self.relationship_entity_type,
                )
                wide_item["object_class_id_list"] = [objects[id_]["class_id"] for id_ in wide_item["object_id_list"]]
                checked_wide_items.append(wide_item)
                relationship_ids_by_name[wide_item["class_id"], wide_item["name"]] = None
                join_object_id_list = ",".join([str(x) for x in wide_item["object_id_list"]])
                relationship_ids_by_obj_lst[wide_item["class_id"], join_object_id_list] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_wide_items, intgr_error_log

    def check_wide_relationships_for_update(self, *wide_items, strict=False, cache=None):
        """Check whether relationships passed as argument respect integrity constraints
        for an update operation.

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
        relationships = {
            x.id: {
                "class_id": x.class_id,
                "name": x.name,
                "object_id_list": [int(y) for y in x.object_id_list.split(",")],
            }
            for x in cache.get("relationship", {}).values()
        }
        relationship_classes = {
            x.id: {"object_class_id_list": [int(y) for y in x.object_class_id_list.split(",")], "name": x.name}
            for x in cache.get("relationship_class", {}).values()
        }
        objects = {x.id: {"class_id": x.class_id, "name": x.name} for x in cache.get("object", {}).values()}
        for wide_item in wide_items:
            try:
                id_ = wide_item["id"]
            except KeyError:
                msg = "Missing relationship identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                updated_wide_item = relationships.pop(id_)
                del relationship_ids_by_name[updated_wide_item["class_id"], updated_wide_item["name"]]
                join_object_id_list = ",".join([str(x) for x in updated_wide_item["object_id_list"]])
                del relationship_ids_by_obj_lst[updated_wide_item["class_id"], join_object_id_list]
            except KeyError:
                msg = "Relationship not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                self.check_immutable_fields(updated_wide_item, wide_item, ("class_id",))
                updated_wide_item.update(wide_item)
                check_wide_relationship(
                    updated_wide_item,
                    relationship_ids_by_name,
                    relationship_ids_by_obj_lst,
                    relationship_classes,
                    objects,
                    self.relationship_entity_type,
                )
                wide_item["object_class_id_list"] = [objects[id_]["class_id"] for id_ in wide_item["object_id_list"]]
                checked_wide_items.append(wide_item)
                relationships[id_] = updated_wide_item
                relationship_ids_by_name[updated_wide_item["class_id"], updated_wide_item["name"]] = id_
                join_object_id_list = ",".join([str(x) for x in updated_wide_item["object_id_list"]])
                relationship_ids_by_obj_lst[updated_wide_item["class_id"], join_object_id_list] = id_
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_wide_items, intgr_error_log

    def check_entity_groups_for_insert(self, *items, strict=False, cache=None):
        """Check whether entity groups passed as argument respect integrity constraints
        for an insert operation.

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
        current_items = {(x.group_id, x.member_id): None for x in cache.get("entity_group", {}).values()}
        entities = {}
        for entity in chain(cache.get("object", {}).values(), cache.get("relationship", {}).values()):
            entities.setdefault(entity.class_id, dict())[entity.id] = entity._asdict()
        for item in items:
            try:
                check_entity_group(item, current_items, entities)
                checked_items.append(item)
                current_items[item["entity_id"], item["member_id"]] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_parameter_definitions_for_insert(self, *items, strict=False, cache=None):
        """Check whether parameter definitions passed as argument respect integrity constraints
        for an insert operation.

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
        parameter_value_lists = {x.id: x.value_list for x in cache.get("parameter_value_list", {}).values()}
        for item in items:
            checked_item = item.copy()
            object_class_id = checked_item.pop("object_class_id", None)
            relationship_class_id = checked_item.pop("relationship_class_id", None)
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
            entity_class_id = checked_item["entity_class_id"] = (
                object_class_id or relationship_class_id or checked_item.get("entity_class_id")
            )
            try:
                check_parameter_definition(checked_item, parameter_definition_ids, class_ids, parameter_value_lists)
                parameter_definition_ids[entity_class_id, checked_item["name"]] = None
                checked_items.append(checked_item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_parameter_definitions_for_update(self, *items, strict=False, cache=None):
        """Check whether parameter definitions passed as argument respect integrity constraints
        for an update operation.

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
        parameter_definitions = {
            x.id: {
                "name": x.parameter_name,
                "entity_class_id": x.entity_class_id,
                "object_class_id": x.object_class_id,
                "relationship_class_id": x.relationship_class_id,
                "parameter_value_list_id": x.value_list_id,
                "default_value": x.default_value,
            }
            for x in cache.get("parameter_definition", {}).values()
        }
        entity_class_ids = {
            x.id for x in chain(cache.get("object_class", {}).values(), cache.get("relationship_class", {}).values())
        }
        parameter_value_lists = {x.id: x.value_list for x in cache.get("parameter_value_list", {}).values()}
        for item in items:
            try:
                id_ = item["id"]
            except KeyError:
                msg = "Missing parameter definition identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                updated_item = parameter_definitions.pop(id_)
                del parameter_definition_ids[updated_item["entity_class_id"], updated_item["name"]]
            except KeyError:
                msg = "Parameter not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                self.check_immutable_fields(
                    updated_item, item, ("entity_class_id", "object_class_id", "relationship_class_id")
                )
                updated_item.update(item)
                check_parameter_definition(
                    updated_item, parameter_definition_ids, entity_class_ids, parameter_value_lists
                )
                parameter_definition_ids[updated_item["entity_class_id"], updated_item["name"]] = id_
                parameter_definitions[id_] = updated_item
                checked_items.append(item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_parameter_values_for_insert(self, *items, strict=False, cache=None):
        """Check whether parameter values passed as argument respect integrity constraints
        for an insert operation.

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
        parameter_value_lists = {x.id: x.value_list for x in cache.get("parameter_value_list", {}).values()}
        alternatives = set(a.id for a in cache.get("alternative", {}).values())
        for item in items:
            checked_item = item.copy()
            checked_item["entity_class_id"] = (
                checked_item.pop("object_class_id", None)
                or checked_item.pop("relationship_class_id", None)
                or checked_item.get("entity_class_id")
            )
            entity_id = checked_item["entity_id"] = (
                checked_item.pop("object_id", None)
                or checked_item.pop("relationship_id", None)
                or checked_item.get("entity_id")
            )
            alt_id = checked_item.get("alternative_id", None)
            try:
                check_parameter_value(
                    checked_item,
                    parameter_value_ids,
                    parameter_definitions,
                    entities,
                    parameter_value_lists,
                    alternatives,
                )
                parameter_value_ids[entity_id, checked_item["parameter_definition_id"], alt_id] = None
                checked_items.append(checked_item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_parameter_values_for_update(self, *items, strict=False, cache=None):
        """Check whether parameter values passed as argument respect integrity constraints
        for an update operation.

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
        parameter_values = {
            x.id: {
                "parameter_definition_id": x.parameter_id,
                "entity_id": x.entity_id,
                "object_id": x.object_id,
                "relationship_id": x.relationship_id,
                "entity_class_id": x.entity_class_id,
                "object_class_id": x.object_class_id,
                "relationship_class_id": x.relationship_class_id,
                "alternative_id": x.alternative_id,
            }
            for x in cache.get("parameter_value", {}).values()
        }
        parameter_value_ids = {
            (x.entity_id, x.parameter_id, x.alternative_id): x.id for x in cache.get("parameter_value", {}).values()
        }
        parameter_definitions = {
            x.id: {
                "name": x.parameter_name,
                "entity_class_id": x.entity_class_id,
                "object_class_id": x.object_class_id,
                "relationship_class_id": x.relationship_class_id,
                "parameter_value_list_id": x.value_list_id,
            }
            for x in cache.get("parameter_definition", {}).values()
        }
        entities = {x.id: {"class_id": x.class_id, "name": x.name} for x in cache.get("object", {}).values()}
        entities.update(
            {x.id: {"class_id": x.class_id, "name": x.name} for x in cache.get("relationship", {}).values()}
        )
        parameter_value_lists = {x.id: x.value_list for x in cache.get("parameter_value_list", {}).values()}
        alternatives = set(a.id for a in cache.get("alternative", {}).values())
        for item in items:
            try:
                id_ = item["id"]
            except KeyError:
                msg = "Missing parameter value identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                updated_item = parameter_values.pop(id_)
                del parameter_value_ids[
                    updated_item["entity_id"], updated_item["parameter_definition_id"], updated_item["alternative_id"]
                ]
            except KeyError:
                msg = "Parameter value not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                self.check_immutable_fields(
                    updated_item, item, ("entity_class_id", "object_class_id", "relationship_class_id")
                )
                updated_item.update(item)
                check_parameter_value(
                    updated_item, parameter_values, parameter_definitions, entities, parameter_value_lists, alternatives
                )
                parameter_values[id_] = updated_item
                parameter_value_ids[
                    updated_item["entity_id"], updated_item["parameter_definition_id"], updated_item["alternative_id"]
                ] = id_
                checked_items.append(item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_wide_parameter_value_lists_for_insert(self, *wide_items, strict=False, cache=None):
        """Check whether parameter value-lists passed as argument respect integrity constraints
        for an insert operation.

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
        checked_wide_items = list()
        parameter_value_list_ids = {x.name: x.id for x in cache.get("parameter_value_list", {}).values()}
        for wide_item in wide_items:
            try:
                check_wide_parameter_value_list(wide_item, parameter_value_list_ids)
                checked_wide_items.append(wide_item)
                parameter_value_list_ids[wide_item["name"]] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_wide_items, intgr_error_log

    def check_wide_parameter_value_lists_for_update(self, *wide_items, strict=False, cache=None):
        """Check whether parameter value-lists passed as argument respect integrity constraints
        for an update operation.

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
        checked_wide_items = list()
        parameter_value_lists = {
            x.id: {"name": x.name, "value_list": x.value_list.split(";")}
            for x in cache.get("parameter_value_list", {}).values()
        }
        parameter_value_list_ids = {x.name: x.id for x in cache.get("parameter_value_list", {}).values()}
        for wide_item in wide_items:
            try:
                id_ = wide_item["id"]
            except KeyError:
                msg = "Missing parameter value list identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                # 'Remove' current instance
                updated_wide_item = parameter_value_lists.pop(id_)
                del parameter_value_list_ids[updated_wide_item["name"]]
            except KeyError:
                msg = "Parameter value list not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            # Check for an insert of the updated instance
            try:
                updated_wide_item.update(wide_item)
                check_wide_parameter_value_list(updated_wide_item, parameter_value_list_ids)
                checked_wide_items.append(wide_item)
                parameter_value_lists[id_] = updated_wide_item
                parameter_value_list_ids[updated_wide_item["name"]] = id_
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_wide_items, intgr_error_log
