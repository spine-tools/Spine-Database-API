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
from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql.expression import bindparam
from .exception import SpineIntegrityError, SpineDBAPIError
from .temp_id import resolve


class DatabaseMappingUpdateMixin:
    """Provides methods to perform ``UPDATE`` operations over a Spine db."""

    def _make_update_stmt(self, tablename, keys):
        table = self._metadata.tables[self._real_tablename(tablename)]
        upd = table.update()
        for k in self._get_primary_key(tablename):
            upd = upd.where(getattr(table.c, k) == bindparam(k))
        return upd.values({key: bindparam(key) for key in table.columns.keys() & keys})

    def _do_update_items(self, connection, tablename, *items_to_update):
        """Update items in DB without checking integrity."""
        if not items_to_update:
            return
        try:
            upd = self._make_update_stmt(tablename, items_to_update[0].keys())
            connection.execute(upd, [resolve(item._asdict()) for item in items_to_update])
            for tablename_, items_to_update_ in self._extra_items_to_update_per_table(tablename, items_to_update):
                if not items_to_update_:
                    continue
                upd = self._make_update_stmt(tablename_, items_to_update_[0].keys())
                connection.execute(upd, [resolve(x) for x in items_to_update_])
        except DBAPIError as e:
            msg = f"DBAPIError while updating '{tablename}' items: {e.orig.args}"
            raise SpineDBAPIError(msg) from e

    @staticmethod
    def _extra_items_to_update_per_table(tablename, items_to_update):
        """
        Yields tuples of string tablename, list of items to update. Needed because some update queries
        actually need to update records in more than one table.

        Args:
            tablename (str): target database table name
            items_to_update (list): items to update

        Yields:
            tuple: database table name, items to update
        """
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
                self._convert_legacy(tablename, item)
                updated.append(table_cache.update_item(item))
        else:
            for item in items:
                self._convert_legacy(tablename, item)
                checked_item, error = table_cache.check_item(item, for_update=True)
                if error:
                    if strict:
                        raise SpineIntegrityError(error)
                    errors.append(error)
                if checked_item:
                    item = checked_item._asdict()
                    updated.append(table_cache.update_item(item))
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

    def update_entity_metadata(self, *items, **kwargs):
        return self.update_items("entity_metadata", *items, **kwargs)

    def update_parameter_value_metadata(self, *items, **kwargs):
        return self.update_items("parameter_value_metadata", *items, **kwargs)

    def _update_ext_item_metadata(self, tablename, *items, **kwargs):
        metadata_items = self.get_metadata_to_add_with_item_metadata_items(*items)
        added, errors = self.add_items("metadata", *metadata_items, **kwargs)
        updated, more_errors = self.update_items(tablename, *items, **kwargs)
        return added + updated, errors + more_errors

    def update_ext_entity_metadata(self, *items, **kwargs):
        return self._update_ext_item_metadata("entity_metadata", *items, **kwargs)

    def update_ext_parameter_value_metadata(self, *items, **kwargs):
        return self._update_ext_item_metadata("parameter_value_metadata", *items, **kwargs)

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
        scen_alt_ids_to_remove = {}
        errors = []
        for scen in scenarios:
            current_scen = self.cache.table_cache("scenario").find_item(scen)
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
                current_scen_alt = self.cache.table_cache("scenario_alternative").find_item(scen_alt)
                scen_alt_ids_to_remove[current_scen_alt["id"]] = current_scen_alt
        # Remove items that are both to add and to remove
        for id_, to_rm in list(scen_alt_ids_to_remove.items()):
            i = next((i for i, to_add in enumerate(scen_alts_to_add) if _is_equal(to_add, to_rm)), None)
            if i is not None:
                del scen_alts_to_add[i]
                del scen_alt_ids_to_remove[id_]
        return scen_alts_to_add, set(scen_alt_ids_to_remove), errors


def _is_equal(to_add, to_rm):
    return all(to_rm[k] == v for k, v in to_add.items())
