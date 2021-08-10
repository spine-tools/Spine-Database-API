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

"""Provides :class:`DatabaseMappingUpdateMixin`.

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
        table = self._metadata.tables[real_tablename]
        items = self._items_with_type_id(tablename, *items)
        items, ids = self._items_to_update_and_ids(*items)
        upd = table.update()
        for k in self._get_primary_key(real_tablename):
            upd = upd.where(getattr(table.c, k) == bindparam(k))
        upd = upd.values({key: bindparam(key) for key in table.columns.keys() & items[0].keys()})
        try:
            self._checked_execute(upd, items)
        except DBAPIError as e:
            msg = f"DBAPIError while updating '{tablename}' items: {e.orig.args}"
            raise SpineDBAPIError(msg)
        return set(ids)

    def update_items(self, tablename, *items, check=True, strict=False, return_items=False, cache=None):
        """Update items.

        Args:
            tablename (str)
            items (Iterable): One or more Python :class:`dict` objects representing the items to be inserted.
            check (bool): Whether or not to check integrity
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if the insertion of one of the items violates an integrity constraint.
            return_items (bool): Return full items rather than just ids
            cache (dict): A dict mapping table names to a list of dictionary items, to use as db replacement
                for queries

        Returns:
            set: ids or items succesfully updated
            list(SpineIntegrityError): found violations
        """
        if check:
            checked_items, intgr_error_log = self.check_items_for_update(tablename, *items, strict=strict, cache=cache)
        else:
            checked_items, intgr_error_log = items, []
        updated_ids = self._update_items(tablename, *checked_items)
        return checked_items if return_items else updated_ids, intgr_error_log

    def update_alternatives(self, *items, **kwargs):
        return self.update_items("alternative", *items, **kwargs)

    def _update_alternatives(self, *items):
        return self._update_items("alternative", *items)

    def update_scenarios(self, *items, **kwargs):
        return self.update_items("scenario", *items, **kwargs)

    def _update_scenarios(self, *items):
        return self._update_items("scenario", *items)

    def update_scenario_alternatives(self, *items, **kwargs):
        return self.update_items("scenario_alternative", *items, **kwargs)

    def _update_scenario_alternatives(self, *items):
        return self._update_items("scenario_alternative", *items)

    def update_object_classes(self, *items, **kwargs):
        return self.update_items("object_class", *items, **kwargs)

    def _update_object_classes(self, *items):
        return self._update_items("object_class", *items)

    def update_objects(self, *items, **kwargs):
        return self.update_items("object", *items, **kwargs)

    def _update_objects(self, *items):
        return self._update_items("object", *items)

    def update_wide_relationship_classes(self, *items, **kwargs):
        return self.update_items("relationship_class", *items, **kwargs)

    def _update_wide_relationship_classes(self, *items):
        return self._update_items("relationship_class", *items)

    def update_wide_relationships(self, *items, **kwargs):
        return self.update_items("relationship", *items, **kwargs)

    def _update_wide_relationships(self, *items):
        return self._update_items("relationship", *items)

    def update_parameter_definitions(self, *items, **kwargs):
        return self.update_items("parameter_definition", *items, **kwargs)

    def _update_parameter_definitions(self, *items):
        return self._update_items("parameter_definition", *items)

    def update_parameter_values(self, *items, **kwargs):
        return self.update_items("parameter_value", *items, **kwargs)

    def _update_parameter_values(self, *items):
        return self._update_items("parameter_value", *items)

    def update_features(self, *items, **kwargs):
        return self.update_items("feature", *items, **kwargs)

    def _update_features(self, *items):
        return self._update_items("feature", *items)

    def update_tools(self, *items, **kwargs):
        return self.update_items("tool", *items, **kwargs)

    def _update_tools(self, *items):
        return self._update_items("tool", *items)

    def update_tool_features(self, *items, **kwargs):
        return self.update_items("tool_feature", *items, **kwargs)

    def _update_tool_features(self, *items):
        return self._update_items("tool_feature", *items)

    def update_tool_feature_methods(self, *items, **kwargs):
        return self.update_items("tool_feature_method", *items, **kwargs)

    def _update_tool_feature_methods(self, *items):
        return self._update_items("tool_feature_method", *items)

    def update_wide_parameter_value_lists(self, *items, strict=False, return_items=False, cache=None):
        checked_items, intgr_error_log = self.check_wide_parameter_value_lists_for_update(
            *items, strict=strict, cache=cache
        )
        updated_ids = self._update_wide_parameter_value_lists(*checked_items)
        return checked_items if return_items else updated_ids, intgr_error_log

    def _update_wide_parameter_value_lists(self, *checked_items):
        checked_items = list(checked_items)
        ids = {item["id"] for item in checked_items}
        try:
            self.remove_items(parameter_value_list=ids)
            self.add_wide_parameter_value_lists(*checked_items, readd=True)
            return ids
        except SpineDBAPIError as e:
            msg = "DBAPIError while updating parameter value lists: {}".format(e.msg)
            raise SpineDBAPIError(msg)

    def get_data_to_set_scenario_alternatives(self, *items, cache=None):
        """Returns data to add and remove, in order to set wide scenario alternatives.

        Args:
            items (Iterable): One or more wide scenario_alternative :class:`dict` objects to set.
                Each item must include the following keys:

                - "id": integer scenario id
                - "alternative_id_list": string comma separated list of alternative ids for that scenario

        Returns
            list: narrow scenario_alternative :class:`dict` objects to add.
            set: integer scenario_alternative ids to remove
        """
        if cache is None:
            cache = self.make_cache("scenario_alternative", "scenario")
        current_alternative_id_lists = {x.id: x.alternative_id_list for x in cache.get("scenario", {}).values()}
        scenario_alternative_ids = {
            (x.scenario_id, x.alternative_id): x.id for x in cache.get("scenario_alternative", {}).values()
        }
        items_to_add = list()
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
                item_to_add = {"scenario_id": scenario_id, "alternative_id": alternative_id, "rank": k + 1}
                items_to_add.append(item_to_add)
            for alternative_id in current_alternative_id_list:
                ids_to_remove.add(scenario_alternative_ids[scenario_id, alternative_id])
        return items_to_add, ids_to_remove
