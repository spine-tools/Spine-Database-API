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

"""Provides :class:`.DiffDatabaseMappingUpdateMixin`.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""

from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql.expression import bindparam
from .exception import SpineDBAPIError


class DatabaseMappingUpdateMixin:
    """Provides methods to perform ``UPDATE`` operations over a Spine db."""

    def _items_to_update_and_ids(self, *items):
        items_to_update = []
        ids = []
        append_item = items_to_update.append
        append_id = ids.append
        for item in items:
            item["commit_id"] = self.make_commit_id()
            append_item(item)
            append_id(item["id"])
        return items_to_update, ids

    def _update_items(self, tablename, *items):
        if not items:
            return set()
        real_tablename = {
            "object_class": "entity_class",
            "relationship_class": "entity_class",
            "object": "entity",
            "relationship": "entity",
        }.get(tablename, tablename)
        item = items[0]
        table = self._metadata.tables[real_tablename]
        items, ids = self._items_to_update_and_ids(*items)
        upd = table.update()
        for k in self._get_primary_key(real_tablename):
            upd = upd.where(getattr(table.c, k) == bindparam(k))
        upd = upd.values({key: bindparam(key) for key in table.columns.keys() & item.keys()})
        try:
            self._checked_execute(upd, items)
        except DBAPIError as e:
            msg = f"DBAPIError while updating '{tablename}' items: {e.orig.args}"
            raise SpineDBAPIError(msg)
        return set(ids)

    def update_items(self, tablename, *items, strict=False):
        """Update items."""
        checked_items, intgr_error_log = self.check_items_for_update(tablename, *items, strict=strict)
        updated_ids = self._update_items(tablename, *checked_items)
        return updated_ids, intgr_error_log

    def update_alternatives(self, *items, strict=False):
        return self.update_items("alternative", *items, strict=strict)

    def _update_alternatives(self, *items):
        return self._update_items("alternative", *items)

    def update_scenarios(self, *items, strict=False):
        return self.update_items("scenario", *items, strict=strict)

    def _update_scenarios(self, *items):
        return self._update_items("scenario", *items)

    def update_scenario_alternatives(self, *items, strict=False):
        return self.update_items("scenario_alternative", *items, strict=strict)

    def _update_scenario_alternatives(self, *items):
        return self._update_items("scenario_alternative", *items)

    def update_object_classes(self, *items, strict=False):
        return self.update_items("object_class", *items, strict=strict)

    def _update_object_classes(self, *items):
        return self._update_items("object_class", *items)

    def update_objects(self, *items, strict=False):
        return self.update_items("object", *items, strict=strict)

    def _update_objects(self, *items):
        return self._update_items("object", *items)

    def update_wide_relationship_classes(self, *items, strict=False):
        return self.update_items("relationship_class", *items, strict=strict)

    def _update_wide_relationship_classes(self, *items):
        return self._update_items("relationship_class", *items)

    def update_wide_relationships(self, *items, strict=False):
        return self.update_items("relationship_class", *items, strict=strict)

    def _update_wide_relationships(self, *items):
        return self._update_items("entity", *items)

    def update_parameter_definitions(self, *items, strict=False):
        return self.update_items("parameter_definition", *items, strict=strict)

    def _update_parameter_definitions(self, *items):
        return self._update_items("parameter_definition", *items)

    def update_parameter_values(self, *items, strict=False):
        return self.update_items("parameter_value", *items, strict=strict)

    def update_checked_parameter_values(self, *items, strict=False):
        return self._update_items("parameter_value", *items), []

    def _update_parameter_values(self, *items):
        return self._update_items("parameter_value", *items)

    def update_parameter_tags(self, *items, strict=False):
        return self.update_items("parameter_tag", *items, strict=strict)

    def _update_parameter_tags(self, *items):
        return self._update_items("parameter_tag", *items)

    def update_features(self, *items, strict=False):
        return self.update_items("feature", *items, strict=strict)

    def _update_features(self, *items):
        return self._update_items("feature", *items)

    def update_tools(self, *items, strict=False):
        return self.update_items("tool", *items, strict=strict)

    def _update_tools(self, *items):
        return self._update_items("tool", *items)

    def update_tool_features(self, *items, strict=False):
        return self.update_items("tool_feature", *items, strict=strict)

    def _update_tool_features(self, *items):
        return self._update_items("tool_feature", *items)

    def update_tool_feature_methods(self, *items, strict=False):
        return self.update_items("tool_feature_method", *items, strict=strict)

    def _update_tool_feature_methods(self, *items):
        return self._update_items("tool_feature_method", *items)

    def update_wide_parameter_value_lists(self, *items, strict=False):
        checked_items, intgr_error_log = self.check_wide_parameter_value_lists_for_update(*items, strict=strict)
        updated_ids = self._update_wide_parameter_value_lists(*checked_items)
        return updated_ids, intgr_error_log

    def _update_wide_parameter_value_lists(self, *checked_items, strict=False):
        wide_parameter_value_lists = {x.id: x._asdict() for x in self.query(self.wide_parameter_value_list_sq)}
        updated_items = list()
        updated_ids = set()
        for item in checked_items:
            id_ = item.get("id")
            if "id" is None:
                continue
            if list(item.keys()) == ["id"]:
                continue
            updated_item = wide_parameter_value_lists.get(id_)
            if updated_item is None:
                continue
            updated_item["value_list"] = updated_item["value_list"].split(";")
            if all(updated_item[k] == item[k] for k in updated_item.keys() & item.keys()):
                continue
            updated_item.update(item)
            updated_items.append(updated_item)
            updated_ids.add(id_)
        try:
            self.remove_items(parameter_value_list=updated_ids)
            self.readd_wide_parameter_value_lists(*updated_items)
            return updated_ids
        except SpineDBAPIError as e:
            msg = "DBAPIError while updating parameter value lists: {}".format(e.msg)
            raise SpineDBAPIError(msg)

    def get_data_to_set_scenario_alternatives(self, *items):
        """Returns data to add, update, and remove, in order to set wide scenario alternatives.

        Args:
            items (Iterable): One or more wide scenario_alternative :class:`dict` objects to set.
                Each item must include the following keys:
                    "id": integer scenario id
                    "alternative_id_list": string comma separated list of alternative ids for that scenario

        Returns
            list: narrow scenario_alternative :class:`dict` objects to add.
            list: narrow scenario_alternative :class:`dict` objects to update.
            set: integer scenario_alternative ids to remove
        """
        current_alternative_id_lists = {x.id: x.alternative_id_list for x in self.query(self.wide_scenario_sq)}
        scenario_alternative_ids = {
            (x.scenario_id, x.alternative_id): x.id for x in self.query(self.scenario_alternative_sq)
        }
        items_to_add = list()
        items_to_update = list()
        ids_to_remove = set()
        for item in items:
            scenario_id = item["id"]
            alternative_id_list = item["alternative_id_list"]
            alternative_id_list = [int(x) for x in alternative_id_list.split(",")] if alternative_id_list else []
            current_alternative_id_list = current_alternative_id_lists[scenario_id]
            current_alternative_id_list = (
                [int(x) for x in current_alternative_id_list.split(",")] if current_alternative_id_list else []
            )
            for k, alternative_id in enumerate(alternative_id_list):
                scen_alt_id = scenario_alternative_ids.get((scenario_id, alternative_id))
                if scen_alt_id is None:
                    item_to_add = {"scenario_id": scenario_id, "alternative_id": alternative_id, "rank": k + 1}
                    items_to_add.append(item_to_add)
                else:
                    item_to_update = {"id": scen_alt_id, "rank": k + 1}
                    items_to_update.append(item_to_update)
            for alternative_id in current_alternative_id_list:
                if alternative_id not in alternative_id_list:
                    ids_to_remove.add(scenario_alternative_ids[scenario_id, alternative_id])
        return items_to_add, items_to_update, ids_to_remove

    def get_data_to_set_parameter_definition_tags(self, *items):
        """Returns data to add, and remove, in order to set wide parameter definition tags.

        Args:
            items (Iterable): One or more wide parameter_definition_tag :class:`dict` objects to set.
                Each item must include the following keys:
                    "id": parameter definition id
                    "parameter_tag_id_list": string comma separated list of tag ids for that definition

        Returns
            list: narrow parameter_definition_tag :class:`dict` objects to add.
            set: integer parameter_definition_tag ids to remove
        """
        current_tag_id_lists = {
            x.id: x.parameter_tag_id_list for x in self.query(self.wide_parameter_definition_tag_sq)
        }
        definition_tag_ids = {
            (x.parameter_definition_id, x.parameter_tag_id): x.id for x in self.query(self.parameter_definition_tag_sq)
        }
        items_to_add = list()
        ids_to_remove = set()
        for item in items:
            param_def_id = item["id"]
            tag_id_list = item["parameter_tag_id_list"]
            tag_id_list = [int(x) for x in tag_id_list.split(",")] if tag_id_list else []
            current_tag_id_list = current_tag_id_lists[param_def_id]
            current_tag_id_list = [int(x) for x in current_tag_id_list.split(",")] if current_tag_id_list else []
            for tag_id in tag_id_list:
                if (param_def_id, tag_id) not in definition_tag_ids:
                    item_to_add = {"parameter_definition_id": param_def_id, "parameter_tag_id": tag_id}
                    items_to_add.append(item_to_add)
            for tag_id in current_tag_id_list:
                if tag_id not in tag_id_list:
                    ids_to_remove.add(definition_tag_ids[param_def_id, tag_id])
        return items_to_add, ids_to_remove
