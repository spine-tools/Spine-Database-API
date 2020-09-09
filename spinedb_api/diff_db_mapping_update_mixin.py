######################################################################################################################
# Copyright (C) 2017 - 2019 Spine project consortium
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
from .exception import SpineDBAPIError

# TODO: improve docstrings


class DiffDatabaseMappingUpdateMixin:
    """Provides methods to stage ``UPDATE`` operations over a Spine db.
    """

    def _get_items_for_update_and_insert(self, tablename, checked_items):
        """Return lists of items for update and insert.
        Items found in the diff classes should be updated, whereas items found in the orig classes
        should be marked as dirty and inserted into the corresponding diff class."""
        orig_sq = self._orig_subquery(tablename)
        diff_sq = self._diff_subquery(tablename)
        items_for_update = list()
        items_for_insert = dict()
        dirty_ids = set()
        updated_ids = set()
        table_id = self.table_ids.get(tablename, "id")
        pk = self.composite_pks.get(tablename)
        if pk is None:
            pk = (table_id,)
        for item in checked_items:
            try:
                filter_expr = {k: item[k] for k in pk}
            except KeyError:
                continue
            if len(filter_expr) == len(item):
                continue
            diff_item = self.query(diff_sq).filter_by(**filter_expr).one_or_none()
            if diff_item is not None:
                updated_item = diff_item._asdict()
                # updated_item = attr_dict(diff_item)
                if all(updated_item[k] == item[k] for k in updated_item.keys() & item.keys()):
                    continue
                updated_item.update(item)
                items_for_update.append(updated_item)
                updated_ids.add(updated_item[table_id])
                continue
            orig_item = self.query(orig_sq).filter_by(**filter_expr).one_or_none()
            if orig_item is not None:
                updated_item = orig_item._asdict()
                if all(updated_item[k] == item[k] for k in updated_item.keys() & item.keys()):
                    continue
                updated_item.update(item)
                key = tuple(item[k] for k in pk)
                items_for_insert[key] = updated_item
                updated_id = updated_item[table_id]
                dirty_ids.add(updated_id)
                updated_ids.add(updated_id)
        # Handle tables where a single id spans multiple rows, notably relationship_entity_class and relationship_entity
        # Basically we need to collect all rows having dirty ids into all_items_for_insert,
        # even if only one of those rows was updated.
        all_items_for_insert = {}
        for orig_item in self.query(orig_sq).filter(self.in_(getattr(orig_sq.c, table_id), dirty_ids)):
            dirty_item = orig_item._asdict()
            key = tuple(dirty_item[k] for k in pk)
            all_items_for_insert[key] = dirty_item
        all_items_for_insert.update(items_for_insert)
        return items_for_update, list(all_items_for_insert.values()), dirty_ids, updated_ids

    def update_alternatives(self, *kwargs_list, strict=False):
        """Update alternatives."""
        checked_kwargs_list, intgr_error_log = self.check_alternatives_for_update(*kwargs_list, strict=strict)
        updated_ids = self._update_alternatives(*checked_kwargs_list)
        return updated_ids, intgr_error_log

    def _update_alternatives(self, *checked_kwargs_list, strict=False):
        """Update alternatives without checking integrity."""
        try:
            items_for_update, items_for_insert, dirty_ids, updated_ids = self._get_items_for_update_and_insert(
                "alternative", checked_kwargs_list
            )
            self.session.bulk_update_mappings(self.DiffAlternative, items_for_update)
            self.session.bulk_insert_mappings(self.DiffAlternative, items_for_insert)
            self.session.commit()
            self._mark_as_dirty("alternative", dirty_ids)
            self.updated_item_id["alternative"].update(dirty_ids)
            return updated_ids
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating alternatives: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_scenarios(self, *kwargs_list, strict=False):
        """Update scenarios."""
        checked_kwargs_list, intgr_error_log = self.check_scenarios_for_update(*kwargs_list, strict=strict)
        updated_ids = self._update_scenarios(*checked_kwargs_list)
        return updated_ids, intgr_error_log

    def _update_scenarios(self, *checked_kwargs_list, strict=False):
        """Update scenarios without checking integrity."""

        try:
            items_for_update, items_for_insert, dirty_ids, updated_ids = self._get_items_for_update_and_insert(
                "scenario", checked_kwargs_list
            )
            self.session.bulk_update_mappings(self.DiffScenario, items_for_update)
            self.session.bulk_insert_mappings(self.DiffScenario, items_for_insert)
            self.session.commit()
            self._mark_as_dirty("scenario", dirty_ids)
            self.updated_item_id["scenario"].update(dirty_ids)
            return updated_ids
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating scenarios: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_scenario_alternatives(self, *kwargs_list, strict=False):
        """Update scenario_alternatives."""
        checked_kwargs_list, intgr_error_log = self.check_scenario_alternatives_for_update(*kwargs_list, strict=strict)
        updated_ids = self._update_scenario_alternatives(*checked_kwargs_list)
        return updated_ids, intgr_error_log

    def _update_scenario_alternatives(self, *checked_kwargs_list, strict=False):
        """Update scenario_alternatives without checking integrity."""
        try:
            items_for_update, items_for_insert, dirty_ids, updated_ids = self._get_items_for_update_and_insert(
                "scenario_alternative", checked_kwargs_list
            )
            self.session.bulk_update_mappings(self.DiffScenarioAlternative, items_for_update)
            self.session.bulk_insert_mappings(self.DiffScenarioAlternative, items_for_insert)
            self.session.commit()
            self._mark_as_dirty("scenario_alternative", dirty_ids)
            self.updated_item_id["scenario_alternative"].update(dirty_ids)
            return updated_ids
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating scenario alternatives: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_object_classes(self, *items, strict=False):
        """Update parameter values."""
        checked_items, intgr_error_log = self.check_object_classes_for_update(*items, strict=strict)
        updated_ids = self._update_object_classes(*checked_items)
        return updated_ids, intgr_error_log

    def _update_object_classes(self, *checked_items, strict=False):
        """Update object classes without checking integrity."""
        try:
            items_for_update, items_for_insert, dirty_ids, updated_ids = self._get_items_for_update_and_insert(
                "entity_class", checked_items
            )
            self.session.bulk_update_mappings(self.DiffEntityClass, items_for_update)
            self.session.bulk_insert_mappings(self.DiffEntityClass, items_for_insert)
            self.session.commit()
            self._mark_as_dirty("entity_class", dirty_ids)
            self.updated_item_id["entity_class"].update(dirty_ids)
            return updated_ids
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating object classes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_objects(self, *items, strict=False):
        """Update objects."""
        checked_items, intgr_error_log = self.check_objects_for_update(*items, strict=strict)
        updated_ids = self._update_objects(*checked_items)
        return updated_ids, intgr_error_log

    def _update_objects(self, *checked_items):
        """Update objects without checking integrity."""
        try:
            items_for_update, items_for_insert, dirty_ids, updated_ids = self._get_items_for_update_and_insert(
                "entity", checked_items
            )
            self.session.bulk_update_mappings(self.DiffEntity, items_for_update)
            self.session.bulk_insert_mappings(self.DiffEntity, items_for_insert)
            self.session.commit()
            self._mark_as_dirty("entity", dirty_ids)
            self.updated_item_id["entity"].update(dirty_ids)
            return updated_ids
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating objects: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_wide_relationship_classes(self, *wide_items, strict=False):
        """Update relationship classes."""
        checked_wide_items, intgr_error_log = self.check_wide_relationship_classes_for_update(
            *wide_items, strict=strict
        )
        updated_ids = self._update_wide_relationship_classes(*checked_wide_items)
        return updated_ids, intgr_error_log

    def _update_wide_relationship_classes(self, *checked_wide_items):
        """Update relationship classes without checking integrity."""
        try:
            items_for_update, items_for_insert, dirty_ids, updated_ids = self._get_items_for_update_and_insert(
                "entity_class", checked_wide_items
            )
            self.session.bulk_update_mappings(self.DiffEntityClass, items_for_update)
            self.session.bulk_insert_mappings(self.DiffEntityClass, items_for_insert)
            self.session.commit()
            self._mark_as_dirty("entity_class", dirty_ids)
            self.updated_item_id["entity_class"].update(dirty_ids)
            return updated_ids
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating relationship classes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_wide_relationships(self, *wide_items, strict=False):
        """Update relationships."""
        checked_wide_items, intgr_error_log = self.check_wide_relationships_for_update(*wide_items, strict=strict)
        updated_ids = self._update_wide_relationships(*checked_wide_items)
        return updated_ids, intgr_error_log

    def _update_wide_relationships(self, *checked_wide_items):
        """Update relationships without checking integrity."""
        ent_items = []
        rel_ent_items = []
        for wide_item in checked_wide_items:
            ent_item = dict(wide_item)
            object_id_list = ent_item.pop("object_id_list", [])
            ent_items.append(ent_item)
            for dimension, member_id in enumerate(object_id_list):
                rel_ent_item = dict(ent_item)
                rel_ent_item["entity_id"] = rel_ent_item.pop("id", None)
                rel_ent_item["dimension"] = dimension
                rel_ent_item["member_id"] = member_id
                rel_ent_items.append(rel_ent_item)
        try:
            ents_for_update, ents_for_insert, dirty_ent_ids, updated_ent_ids = self._get_items_for_update_and_insert(
                "entity", ent_items
            )
            (
                rel_ents_for_update,
                rel_ents_for_insert,
                dirty_rel_ent_ids,
                updated_rel_ent_ids,
            ) = self._get_items_for_update_and_insert("relationship_entity", rel_ent_items)
            self.session.bulk_update_mappings(self.DiffEntity, ents_for_update)
            self.session.bulk_insert_mappings(self.DiffEntity, ents_for_insert)
            self.session.bulk_update_mappings(self.DiffRelationshipEntity, rel_ents_for_update)
            self.session.bulk_insert_mappings(self.DiffRelationshipEntity, rel_ents_for_insert)
            self.session.commit()
            self._mark_as_dirty("entity", dirty_ent_ids)
            self.updated_item_id["entity"].update(dirty_ent_ids)
            self._mark_as_dirty("relationship_entity", dirty_rel_ent_ids)
            self.updated_item_id["relationship_entity"].update(dirty_rel_ent_ids)
            return updated_ent_ids.union(updated_rel_ent_ids)
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating relationships: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_parameter_definitions(self, *items, strict=False):
        """Update parameter definitions."""
        checked_items, intgr_error_log = self.check_parameter_definitions_for_update(*items, strict=strict)
        updated_ids = self._update_parameter_definitions(*checked_items)
        return updated_ids, intgr_error_log

    def _update_parameter_definitions(self, *checked_items):
        """Update parameter definitions without checking integrity."""
        try:
            items_for_update, items_for_insert, dirty_ids, updated_ids = self._get_items_for_update_and_insert(
                "parameter_definition", checked_items
            )
            self.session.bulk_update_mappings(self.DiffParameterDefinition, items_for_update)
            self.session.bulk_insert_mappings(self.DiffParameterDefinition, items_for_insert)
            self.session.commit()
            self._mark_as_dirty("parameter_definition", dirty_ids)
            self.updated_item_id["parameter_definition"].update(dirty_ids)
            return updated_ids
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating parameter definitions: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_parameter_values(self, *items, strict=False):
        """Update parameter values."""
        checked_items, intgr_error_log = self.check_parameter_values_for_update(*items, strict=strict)
        updated_ids = self._update_parameter_values(*checked_items)
        return updated_ids, intgr_error_log

    def update_checked_parameter_values(self, *checked_items):
        """Update checked parameter values."""
        updated_ids = self._update_parameter_values(*checked_items)
        return updated_ids, []

    def _update_parameter_values(self, *checked_items):
        """Update parameter values.

        Returns:
            updated_ids (set): updated instances' ids
        """
        try:
            items_for_update, items_for_insert, dirty_ids, updated_ids = self._get_items_for_update_and_insert(
                "parameter_value", checked_items
            )
            self.session.bulk_update_mappings(self.DiffParameterValue, items_for_update)
            self.session.bulk_insert_mappings(self.DiffParameterValue, items_for_insert)
            self.session.commit()
            self._mark_as_dirty("parameter_value", dirty_ids)
            self.updated_item_id["parameter_value"].update(dirty_ids)
            return updated_ids
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating parameter values: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_parameter_tags(self, *items, strict=False):
        """Update parameter tags."""
        checked_items, intgr_error_log = self.check_parameter_tags_for_update(*items, strict=strict)
        updated_ids = self._update_parameter_tags(*checked_items)
        return updated_ids, intgr_error_log

    def _update_parameter_tags(self, *checked_items):
        """Update parameter tags without checking integrity."""
        try:
            items_for_update, items_for_insert, dirty_ids, updated_ids = self._get_items_for_update_and_insert(
                "parameter_tag", checked_items
            )
            self.session.bulk_update_mappings(self.DiffParameterTag, items_for_update)
            self.session.bulk_insert_mappings(self.DiffParameterTag, items_for_insert)
            self.session.commit()
            self._mark_as_dirty("parameter_tag", dirty_ids)
            self.updated_item_id["parameter_tag"].update(dirty_ids)
            return updated_ids
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating parameter tags: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_wide_parameter_value_lists(self, *wide_items, strict=False):
        """Update parameter value lists."""
        checked_wide_items, intgr_error_log = self.check_wide_parameter_value_lists_for_update(
            *wide_items, strict=strict
        )
        updated_ids = self._update_wide_parameter_value_lists(*checked_wide_items)
        return updated_ids, intgr_error_log

    def _update_wide_parameter_value_lists(self, *checked_wide_items, strict=False):
        """Update parameter value lists without checking integrity."""
        wide_parameter_value_lists = {x.id: x._asdict() for x in self.query(self.wide_parameter_value_list_sq)}
        updated_wide_items = list()
        updated_ids = set()
        for wide_item in checked_wide_items:
            id_ = wide_item.get("id")
            if "id" is None:
                continue
            if list(wide_item.keys()) == ["id"]:
                continue
            updated_wide_item = wide_parameter_value_lists.get(id_)
            if updated_wide_item is None:
                continue
            updated_wide_item["value_list"] = updated_wide_item["value_list"].split(";")
            if all(updated_wide_item[k] == wide_item[k] for k in updated_wide_item.keys() & wide_item.keys()):
                continue
            updated_wide_item.update(wide_item)
            updated_wide_items.append(updated_wide_item)
            updated_ids.add(id_)
        try:
            self.remove_items(parameter_value_list=updated_ids)
            self.readd_wide_parameter_value_lists(*updated_wide_items)
            return updated_ids
        except SpineDBAPIError as e:
            msg = "DBAPIError while updating parameter value lists: {}".format(e.msg)
            raise SpineDBAPIError(msg)

    def update_tools(self, *kwargs_list, strict=False):
        """Update tools."""
        checked_kwargs_list, intgr_error_log = self.check_tools_for_update(*kwargs_list, strict=strict)
        updated_ids = self._update_tools(*checked_kwargs_list)
        return updated_ids, intgr_error_log

    def _update_tools(self, *checked_kwargs_list, strict=False):
        """Update tools without checking integrity."""
        try:
            items_for_update, items_for_insert, dirty_ids, updated_ids = self._get_items_for_update_and_insert(
                "tool", checked_kwargs_list
            )
            self.session.bulk_update_mappings(self.DiffTool, items_for_update)
            self.session.bulk_insert_mappings(self.DiffTool, items_for_insert)
            self.session.commit()
            self._mark_as_dirty("tool", dirty_ids)
            self.updated_item_id["tool"].update(dirty_ids)
            return updated_ids
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating tools: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def get_data_to_set_scenario_alternatives(self, *items):
        """Returns data to add, update, and remove, in order to set wide scenario alternatives.

        :param Iterable items: One or more wide scenario_alternative :class:`dict` objects to set.
            Each item must include the following keys:
                "id": integer scenario id
                "alternative_id_list": string comma separated list of alternative ids for that scenario

        :returns:
            - **items_to_add** -- A list of narrow scenario_alternative :class:`dict` objects to add.

            - **items_to_update** --  A list of narrow scenario_alternative :class:`dict` objects to update.

            - **ids_to_remove** -- A set of integer scenario_alternative ids to remove
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

        :param Iterable items: One or more wide parameter_definition_tag :class:`dict` objects to set.
            Each item must include the following keys:
                "id": parameter definition id
                "parameter_tag_id_list": string comma separated list of tag ids for that definition

        :returns:
            - **items_to_add** -- A list of narrow parameter_definition_tag :class:`dict` objects to add.

            - **ids_to_remove** -- A set of integer parameter_definition_tag ids to remove
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
