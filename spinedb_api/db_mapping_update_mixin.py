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

"""Provides :class:`DatabaseMappingUpdateMixin`.

"""
from collections import Counter
from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql.expression import bindparam
from .exception import SpineDBAPIError


class DatabaseMappingUpdateMixin:
    """Provides methods to perform ``UPDATE`` operations over a Spine db."""

    def _add_commit_id(self, *items):
        for item in items:
            item["commit_id"] = self._make_commit_id()

    def _update_items(self, tablename, *items, dry_run=False):
        if not items:
            return set()
        if dry_run:
            return {x["id"] for x in items}
        # Special cases
        if tablename == "entity":
            return self._do_update_entities(*items)
        if tablename == "scenario":
            return self._do_update_scenarios(*items)
        if tablename == "object":
            return self._do_update_objects(*items)
        if tablename == "relationship":
            return self._do_update_wide_relationships(*items)
        real_tablename = self._real_tablename(tablename)
        self._do_update_items(real_tablename, *items)

    def _do_update_entities(self, *items):
        entity_items = []
        entity_element_items = []
        entity_alternative_items = []
        for item in items:
            entity_id = item["id"]
            class_id = item["class_id"]
            dimension_id_list = item["dimension_id_list"]
            element_id_list = item["element_id_list"]
            entity_items.append(
                {"id": entity_id, "class_id": class_id, "name": item["name"], "description": item.get("description")}
            )
            entity_element_items.extend(
                [
                    {
                        "entity_class_id": class_id,
                        "entity_id": entity_id,
                        "position": position,
                        "dimension_id": dimension_id,
                        "element_id": element_id,
                    }
                    for position, (dimension_id, element_id) in enumerate(zip(dimension_id_list, element_id_list))
                ]
            )
            entity_alternative_items.extend(
                [
                    {"entity_id": entity_id, "alternative_id": alt_id, "active": True}
                    for alt_id in item["active_alternative_id_list"]
                ]
                + [
                    {"entity_id": entity_id, "alternative_id": alt_id, "active": False}
                    for alt_id in item["inactive_alternative_id_list"]
                ]
            )
        self._do_update_items("entity", *entity_items)
        self._do_update_items("entity_element", *entity_element_items)
        self._do_update_items("entity_alternative", *entity_alternative_items)
        return {x["id"] for x in entity_items}

    def _do_update_scenarios(self, *items):
        """Returns data to add and remove, in order to set wide scenario alternatives.

        Args:
            *items: One or more wide scenario :class:`dict` objects to set.
                Each item must include the following keys:

                - "id": integer scenario id
                - "alternative_id_list": list of alternative ids for that scenario

        Returns
            list: narrow scenario_alternative :class:`dict` objects to add.
            set: integer scenario_alternative ids to remove
        """
        self.fetch_all({"scenario_alternative", "scenario"})
        cache = self.cache
        current_alternative_id_lists = {x.id: x.alternative_id_list for x in cache.get("scenario", {}).values()}
        scenario_alternative_ids = {
            (x.scenario_id, x.alternative_id): x.id for x in cache.get("scenario_alternative", {}).values()
        }
        scen_alts_to_add = []
        scen_alt_ids_to_remove = set()
        for item in items:
            scenario_id = item["id"]
            alternative_id_list = item["alternative_id_list"]
            current_alternative_id_list = current_alternative_id_lists[scenario_id]
            for k, alternative_id in enumerate(alternative_id_list):
                item_to_add = {"scenario_id": scenario_id, "alternative_id": alternative_id, "rank": k + 1}
                scen_alts_to_add.append(item_to_add)
            for alternative_id in current_alternative_id_list:
                scen_alt_ids_to_remove.add(scenario_alternative_ids[scenario_id, alternative_id])
        self.remove_items(scenario_alternative=scen_alt_ids_to_remove)
        self.add_items("scenario_alternative", *scen_alts_to_add)
        return self._do_update_items("scenario", *items)

    def _do_update_objects(self, *items):
        entity_items = []
        entity_alternative_items = []
        for item in items:
            entity_id = item["id"]
            class_id = item["class_id"]
            entity_items.append(
                {"id": entity_id, "class_id": class_id, "name": item["name"], "description": item.get("description")}
            )
            entity_alternative_items.extend(
                [
                    {"entity_id": entity_id, "alternative_id": alt_id, "active": True}
                    for alt_id in item["active_alternative_id_list"]
                ]
                + [
                    {"entity_id": entity_id, "alternative_id": alt_id, "active": False}
                    for alt_id in item["inactive_alternative_id_list"]
                ]
            )
        self._do_update_items("entity", *entity_items)
        self._do_update_items("entity_alternative", *entity_alternative_items)
        return {x["id"] for x in entity_items}

    def _do_update_wide_relationships(self, *items):
        entity_items = []
        entity_element_items = []
        entity_alternative_items = []
        for item in items:
            entity_id = item["id"]
            class_id = item["class_id"]
            object_class_id_list = item["object_class_id_list"]
            object_id_list = item["object_id_list"]
            entity_items.append(
                {"id": entity_id, "class_id": class_id, "name": item["name"], "description": item.get("description")}
            )
            entity_element_items.extend(
                [
                    {
                        "entity_class_id": class_id,
                        "entity_id": entity_id,
                        "position": position,
                        "dimension_id": dimension_id,
                        "element_id": element_id,
                    }
                    for position, (dimension_id, element_id) in enumerate(zip(object_class_id_list, object_id_list))
                ]
            )
            entity_alternative_items.extend(
                [
                    {"entity_id": entity_id, "alternative_id": alt_id, "active": True}
                    for alt_id in item["active_alternative_id_list"]
                ]
                + [
                    {"entity_id": entity_id, "alternative_id": alt_id, "active": False}
                    for alt_id in item["inactive_alternative_id_list"]
                ]
            )
        self._do_update_items("entity", *entity_items)
        self._do_update_items("entity_element", *entity_element_items)
        self._do_update_items("entity_alternative", *entity_alternative_items)
        return {x["id"] for x in entity_items}

    def _do_update_items(self, tablename, *items):
        if not items:
            return
        self._add_commit_id(*items)
        table = self._metadata.tables[tablename]
        upd = table.update()
        for k in self._get_primary_key(tablename):
            upd = upd.where(getattr(table.c, k) == bindparam(k))
        upd = upd.values({key: bindparam(key) for key in table.columns.keys() & items[0].keys()})
        try:
            self._checked_execute(upd, [{**item} for item in items])
        except DBAPIError as e:
            msg = f"DBAPIError while updating '{tablename}' items: {e.orig.args}"
            raise SpineDBAPIError(msg) from e
        else:
            self._has_pending_changes = True

    def update_items(self, tablename, *items, check=True, strict=False, return_items=False, dry_run=False):
        """Updates items.

        Args:
            tablename (str): Target database table name
            *items: One or more Python :class:`dict` objects representing the items to be inserted.
            check (bool): Whether or not to check integrity
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if the insertion of one of the items violates an integrity constraint.
            return_items (bool): Return full items rather than just ids

        Returns:
            set: ids or items successfully updated
            list(SpineIntegrityError): found violations
        """
        if check:
            checked_items, intgr_error_log = self.check_items(tablename, *items, for_update=True, strict=strict)
        else:
            checked_items, intgr_error_log = list(items), []
        updated_ids = self._update_items(tablename, *checked_items, dry_run=dry_run)
        if return_items:
            return checked_items, intgr_error_log
        return updated_ids, intgr_error_log

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

    def update_entity_classes(self, *items, **kwargs):
        return self.update_items("entity_class", *items, **kwargs)

    def _update_entity_classes(self, *items):
        return self._update_items("entity_class", *items)

    def update_entities(self, *items, **kwargs):
        return self.update_items("entity", *items, **kwargs)

    def _update_entities(self, *items):
        return self._update_items("entity", *items)

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

    def update_parameter_value_lists(self, *items, **kwargs):
        return self.update_items("parameter_value_list", *items, **kwargs)

    def _update_parameter_value_lists(self, *items):
        return self._update_items("parameter_value_list", *items)

    def update_list_values(self, *items, **kwargs):
        return self.update_items("list_value", *items, **kwargs)

    def _update_list_values(self, *items):
        return self._update_items("list_value", *items)

    def update_metadata(self, *items, **kwargs):
        return self.update_items("metadata", *items, **kwargs)

    def _update_metadata(self, *items):
        return self._update_items("metadata", *items)

    def update_ext_entity_metadata(self, *items, check=True, strict=False, return_items=False, dry_run=False):
        updated_items, errors = self._update_ext_item_metadata(
            "entity_metadata", *items, check=check, strict=strict, dry_run=dry_run
        )
        if return_items:
            return updated_items, errors
        return {i["id"] for i in updated_items}, errors

    def update_ext_parameter_value_metadata(self, *items, check=True, strict=False, return_items=False, dry_run=False):
        updated_items, errors = self._update_ext_item_metadata(
            "parameter_value_metadata", *items, check=check, strict=strict, dry_run=dry_run
        )
        if return_items:
            return updated_items, errors
        return {i["id"] for i in updated_items}, errors

    def _update_ext_item_metadata(self, metadata_table, *items, check=True, strict=False, dry_run=False):
        self.fetch_all({"entity_metadata", "parameter_value_metadata", "metadata"})
        cache = self.cache
        metadata_ids = {}
        for entry in cache.get("metadata", {}).values():
            metadata_ids.setdefault(entry.name, {})[entry.value] = entry.id
        item_metadata_cache = cache[metadata_table]
        metadata_usage_counts = self._metadata_usage_counts()
        updatable_items = []
        homeless_items = []
        for item in items:
            metadata_name = item["metadata_name"]
            metadata_value = item["metadata_value"]
            metadata_id = metadata_ids.get(metadata_name, {}).get(metadata_value)
            if metadata_id is None:
                homeless_items.append(item)
                continue
            item["metadata_id"] = metadata_id
            previous_metadata_id = item_metadata_cache[item["id"]]["metadata_id"]
            metadata_usage_counts[previous_metadata_id] -= 1
            metadata_usage_counts[metadata_id] += 1
            updatable_items.append(item)
        homeless_item_metadata_usage_counts = Counter()
        for item in homeless_items:
            homeless_item_metadata_usage_counts[item_metadata_cache[item["id"]].metadata_id] += 1
        updatable_metadata_items = []
        future_metadata_ids = {}
        for metadata_id, count in homeless_item_metadata_usage_counts.items():
            if count == metadata_usage_counts[metadata_id]:
                for cached_item in item_metadata_cache.values():
                    if cached_item["metadata_id"] == metadata_id:
                        found = False
                        for item in homeless_items:
                            if item["id"] == cached_item["id"]:
                                metadata_name = item["metadata_name"]
                                metadata_value = item["metadata_value"]
                                updatable_metadata_items.append(
                                    {"id": metadata_id, "name": metadata_name, "value": metadata_value}
                                )
                                future_metadata_ids.setdefault(metadata_name, {})[metadata_value] = metadata_id
                                metadata_usage_counts[metadata_id] = 0
                                found = True
                                break
                        if found:
                            break
        items_needing_new_metadata = []
        for item in homeless_items:
            metadata_name = item["metadata_name"]
            metadata_value = item["metadata_value"]
            metadata_id = future_metadata_ids.get(metadata_name, {}).get(metadata_value)
            if metadata_id is None:
                items_needing_new_metadata.append(item)
                continue
            if item_metadata_cache[item["id"]]["metadata_id"] == metadata_id:
                continue
            item["metadata_id"] = metadata_id
            updatable_items.append(item)
        all_items = []
        errors = []
        if updatable_metadata_items:
            updated_metadata, errors = self.update_metadata(
                *updatable_metadata_items, check=False, strict=strict, return_items=True, dry_run=dry_run
            )
            all_items += updated_metadata
            if errors:
                return all_items, errors
        addable_metadata = [
            {"name": i["metadata_name"], "value": i["metadata_value"]} for i in items_needing_new_metadata
        ]
        added_metadata = []
        if addable_metadata:
            added_metadata, metadata_add_errors = self.add_metadata(
                *addable_metadata, check=False, strict=strict, return_items=True
            )
            all_items += added_metadata
            errors += metadata_add_errors
            if errors:
                return all_items, errors
        added_metadata_ids = {}
        for item in added_metadata:
            added_metadata_ids.setdefault(item["name"], {})[item["value"]] = item["id"]
        for item in items_needing_new_metadata:
            item["metadata_id"] = added_metadata_ids[item["metadata_name"]][item["metadata_value"]]
            updatable_items.append(item)
        if updatable_items:
            # FIXME: Force-clear cache before updating item metadata to ensure that added/updated metadata is found.
            updated_item_metadata, item_metadata_errors = self.update_items(
                metadata_table, *updatable_items, check=check, strict=strict, return_items=True
            )
            all_items += updated_item_metadata
            errors += item_metadata_errors
        return all_items, errors
