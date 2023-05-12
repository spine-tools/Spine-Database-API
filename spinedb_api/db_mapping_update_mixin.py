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
from .exception import SpineIntegrityError
from .helpers import convert_legacy


class DatabaseMappingUpdateMixin:
    """Provides methods to perform ``UPDATE`` operations over a Spine db."""

    def _do_update_items(self, tablename, *items_to_update):
        """Update items in DB without checking integrity."""
        try:
            for tablename_, items_to_update_ in self._items_to_update_per_table(tablename, items_to_update):
                if not items_to_update_:
                    continue
                table = self._metadata.tables[self._real_tablename(tablename_)]
                upd = table.update()
                for k in self._get_primary_key(tablename_):
                    upd = upd.where(getattr(table.c, k) == bindparam(k))
                upd = upd.values({key: bindparam(key) for key in table.columns.keys() & items_to_update_[0].keys()})
                self.connection_execute(upd, [dict(item) for item in items_to_update_])
        except DBAPIError as e:
            msg = f"DBAPIError while updating '{tablename}' items: {e.orig.args}"
            raise SpineIntegrityError(msg) from e

    @staticmethod
    def _items_to_update_per_table(tablename, items_to_update):
        """
        Yields tuples of string tablename, list of items to update. Needed because some update queries
        actually need to update records in more than one table.

        Args:
            tablename (str): target database table name
            items_to_update (list): items to update

        Yields:
            tuple: database table name, items to update
        """
        yield (tablename, items_to_update)
        if tablename == "entity":
            ee_items_to_update = [
                {
                    "entity_id": item["id"],
                    "entity_class_id": item["class_id"],
                    "position": position,
                    "element_id": element_id,
                    "dimension_id": dimension_id,
                }
                for item in items_to_update
                for position, (element_id, dimension_id) in enumerate(
                    zip(item["element_id_list"], item["dimension_id_list"])
                )
            ]
            yield ("entity_element", ee_items_to_update)

    def update_items(self, tablename, *items, check=True, strict=False):
        """Updates items in cache.

        Args:
            tablename (str): Target database table name
            *items: One or more Python :class:`dict` objects representing the items to be inserted.
            check (bool): Whether or not to check integrity
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if the insertion of one of the items violates an integrity constraint.

        Returns:
            set: ids or items successfully updated
            list(SpineIntegrityError): found violations
        """
        updated, errors = [], []
        tablename = self._real_tablename(tablename)
        table_cache = self.cache.table_cache(tablename)
        if not check:
            for item in items:
                convert_legacy(tablename, item)
                updated.append(table_cache.update_item(item)._asdict())
        else:
            for item in items:
                convert_legacy(tablename, item)
                checked_item, error = table_cache.check_item(item, for_update=True)
                if error:
                    if strict:
                        raise SpineIntegrityError(error)
                    errors.append(error)
                    continue
                item = checked_item._asdict()
                updated.append(table_cache.update_item(item)._asdict())
        return updated, errors

    def update_alternatives(self, *items, **kwargs):
        return self.update_items("alternative", *items, **kwargs)

    def update_scenarios(self, *items, **kwargs):
        return self.update_items("scenario", *items, **kwargs)

    def update_scenario_alternatives(self, *items, **kwargs):
        return self.update_items("scenario_alternative", *items, **kwargs)

    def update_entity_classes(self, *items, **kwargs):
        return self.update_items("entity_class", *items, **kwargs)

    def update_entities(self, *items, **kwargs):
        return self.update_items("entity", *items, **kwargs)

    def update_object_classes(self, *items, **kwargs):
        return self.update_items("object_class", *items, **kwargs)

    def update_objects(self, *items, **kwargs):
        return self.update_items("object", *items, **kwargs)

    def update_wide_relationship_classes(self, *items, **kwargs):
        return self.update_items("relationship_class", *items, **kwargs)

    def update_wide_relationships(self, *items, **kwargs):
        return self.update_items("relationship", *items, **kwargs)

    def update_parameter_definitions(self, *items, **kwargs):
        return self.update_items("parameter_definition", *items, **kwargs)

    def update_parameter_values(self, *items, **kwargs):
        return self.update_items("parameter_value", *items, **kwargs)

    def update_parameter_value_lists(self, *items, **kwargs):
        return self.update_items("parameter_value_list", *items, **kwargs)

    def update_list_values(self, *items, **kwargs):
        return self.update_items("list_value", *items, **kwargs)

    def update_metadata(self, *items, **kwargs):
        return self.update_items("metadata", *items, **kwargs)

    def update_ext_entity_metadata(self, *items, check=True, strict=False):
        updated_items, errors = self._update_ext_item_metadata("entity_metadata", *items, check=check, strict=strict)
        return updated_items, errors

    def update_ext_parameter_value_metadata(self, *items, check=True, strict=False):
        updated_items, errors = self._update_ext_item_metadata(
            "parameter_value_metadata", *items, check=check, strict=strict
        )
        return updated_items, errors

    def _update_ext_item_metadata(self, metadata_table, *items, check=True, strict=False):
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
            updated_metadata, errors = self.update_metadata(*updatable_metadata_items, check=False, strict=strict)
            all_items += updated_metadata
            if errors:
                return all_items, errors
        addable_metadata = [
            {"name": i["metadata_name"], "value": i["metadata_value"]} for i in items_needing_new_metadata
        ]
        added_metadata = []
        if addable_metadata:
            added_metadata, metadata_add_errors = self.add_metadata(*addable_metadata, check=False, strict=strict)
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
                metadata_table, *updatable_items, check=check, strict=strict
            )
            all_items += updated_item_metadata
            errors += item_metadata_errors
        return all_items, errors

    def get_data_to_set_scenario_alternatives(self, *scenarios, strict=True):
        """Returns data to add and remove, in order to set wide scenario alternatives.

        Args:
            *scenarios: One or more wide scenario :class:`dict` objects to set.
                Each item must include the following keys:

                - "id": integer scenario id
                - "alternative_id_list": list of alternative ids for that scenario

        Returns
            list: scenario_alternative :class:`dict` objects to add.
            set: integer scenario_alternative ids to remove
        """
        scen_alts_to_add = []
        scen_alt_ids_to_remove = set()
        errors = []
        for scen in scenarios:
            current_scen = self.cache.table_cache("scenario").current_item(scen)
            if current_scen is None:
                error = f"no scenario matching {scen} to set alternatives for"
                if strict:
                    raise SpineIntegrityError(error)
                errors.append(error)
                continue
            for k, alternative_id in enumerate(scen.get("alternative_id_list", ())):
                item_to_add = {"scenario_id": current_scen["id"], "alternative_id": alternative_id, "rank": k + 1}
                scen_alts_to_add.append(item_to_add)
            for k, alternative_name in enumerate(scen.get("alternative_name_list", ())):
                item_to_add = {"scenario_id": current_scen["id"], "alternative_name": alternative_name, "rank": k + 1}
                scen_alts_to_add.append(item_to_add)
            for alternative_id in current_scen["alternative_id_list"]:
                scen_alt = {"scenario_id": current_scen["id"], "alternative_id": alternative_id}
                current_scen_alt = self.cache.table_cache("scenario_alternative").current_item(scen_alt)
                scen_alt_ids_to_remove.add(current_scen_alt["id"])
        return scen_alts_to_add, scen_alt_ids_to_remove, errors
