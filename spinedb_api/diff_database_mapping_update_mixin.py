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
from .helpers import attr_dict

# TODO: improve docstrings


class DiffDatabaseMappingUpdateMixin:
    """Provides methods to stage ``UPDATE`` operations over a Spine db.
    """

    def _get_items_for_update_and_insert(self, tablename, checked_items):
        """Return lists of items for update and insert.
        Items found in the diff classes should be updated, whereas items found in the orig classes
        should be marked as dirty and inserted into the corresponding diff class."""
        classname = self.table_to_class[tablename]
        orig_class = getattr(self, classname)
        diff_class = getattr(self, "Diff" + classname)
        items_for_update = list()
        items_for_insert = list()
        dirty_ids = set()
        updated_ids = set()
        pk = self.composite_pks.get(tablename)
        if pk is None:
            id_column = self.table_ids.get(tablename, "id")
            pk = (id_column,)
            _get_id = lambda updated_item: updated_item[id_column]
        else:
            _get_id = lambda updated_item: tuple(updated_item[field] for field in pk)
        for item in checked_items:
            try:
                filter_ = {k: item[k] for k in pk}
            except KeyError:
                continue
            if len(filter_) == len(item):
                continue
            diff_query = self.query(diff_class).filter_by(**filter_)
            for diff_item in diff_query:
                updated_item = attr_dict(diff_item)
                if all(updated_item[k] == item[k] for k in updated_item.keys() & item.keys()):
                    continue
                updated_item.update(item)
                items_for_update.append(updated_item)
                updated_ids.add(_get_id(updated_item))
            if diff_query.count() > 0:
                # Don't look in orig_class if found in diff_class
                continue
            for orig_item in self.query(orig_class).filter_by(**filter_):
                updated_item = attr_dict(orig_item)
                if all(updated_item[k] == item[k] for k in updated_item.keys() & item.keys()):
                    continue
                updated_item.update(item)
                items_for_insert.append(updated_item)
                updated_id = _get_id(updated_item)
                dirty_ids.add(updated_id)
                updated_ids.add(updated_id)
        return items_for_update, items_for_insert, dirty_ids, updated_ids

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
            rel_ents_for_update, rel_ents_for_insert, dirty_rel_ent_ids, updated_rel_ent_ids = self._get_items_for_update_and_insert(
                "relationship_entity", rel_ent_items
            )
            self.session.bulk_update_mappings(self.DiffEntity, ents_for_update)
            self.session.bulk_insert_mappings(self.DiffEntity, ents_for_insert)
            self.session.bulk_update_mappings(self.DiffRelationshipEntity, rel_ents_for_update)
            self.session.bulk_insert_mappings(self.DiffRelationshipEntity, rel_ents_for_insert)
            self.session.commit()
            self._mark_as_dirty("entity", dirty_ent_ids)
            self.updated_item_id["entity"].update(updated_ent_ids)
            self._mark_as_dirty("relationship_entity", dirty_rel_ent_ids)
            self.updated_item_id["relationship_entity"].update(x[0] for x in updated_rel_ent_ids)
            return updated_ent_ids.union(x[0] for x in updated_rel_ent_ids)
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

    def set_parameter_definition_tags(self, *items, strict=False):
        """Set tags for parameter definitions."""
        current_tag_id_lists = {
            x.id: x.parameter_tag_id_list for x in self.query(self.wide_parameter_definition_tag_sq)
        }
        definition_tag_id_dict = {
            (x.parameter_definition_id, x.parameter_tag_id): x.id for x in self.query(self.parameter_definition_tag_sq)
        }
        new_items = list()
        deleted_ids = set()
        definition_ids = set()
        for item in items:
            definition_id = item["parameter_definition_id"]
            definition_ids.add(definition_id)
            tag_id_list = item["parameter_tag_id_list"]
            tag_id_list = [int(x) for x in tag_id_list.split(",")] if tag_id_list else []
            current_tag_id_list = current_tag_id_lists[definition_id]
            current_tag_id_list = [int(x) for x in current_tag_id_list.split(",")] if current_tag_id_list else []
            for tag_id in tag_id_list:
                if tag_id not in current_tag_id_list:
                    item = {"parameter_definition_id": definition_id, "parameter_tag_id": tag_id}
                    new_items.append(item)
            for tag_id in current_tag_id_list:
                if tag_id not in tag_id_list:
                    deleted_ids.add(definition_tag_id_dict[definition_id, tag_id])
        self.remove_items(parameter_definition_tag_ids=deleted_ids)
        _, error_log = self.add_parameter_definition_tags(*new_items, strict=strict)
        return definition_ids, error_log

    def update_wide_parameter_value_lists(self, *wide_items, strict=False):
        """Update parameter value_lists.
        """
        # NOTE: Since the value list can actually change size, we proceed by removing the entire list and then
        # inserting the new one to avoid unnecessary headaches
        checked_wide_items, intgr_error_log = self.check_wide_parameter_value_lists_for_update(
            *wide_items, strict=strict
        )
        wide_parameter_value_lists = {x.id: x._asdict() for x in self.query(self.wide_parameter_value_list_sq)}
        updated_ids = set()
        items = list()
        for wide_item in checked_wide_items:
            if "id" not in wide_item:
                continue
            if len(wide_item) == 1:
                continue
            id_ = wide_item["id"]
            if id_ not in wide_parameter_value_lists:
                continue
            updated_ids.add(id_)
            updated_wide_item = wide_parameter_value_lists[id_]
            updated_wide_item["value_list"] = updated_wide_item["value_list"].split(",")
            updated_wide_item.update(wide_item)
            for k, value in enumerate(updated_wide_item["value_list"]):
                narrow_item = {"id": id_, "name": updated_wide_item["name"], "value_index": k, "value": value}
                items.append(narrow_item)
        try:
            self.query(self.DiffParameterValueList).filter(self.DiffParameterValueList.id.in_(updated_ids)).delete(
                synchronize_session=False
            )
            self.session.bulk_insert_mappings(self.DiffParameterValueList, items)
            self.session.commit()
            self.added_item_id["parameter_value_list"].update(updated_ids)
            self.removed_item_id["parameter_value_list"].update(updated_ids)
            self._mark_as_dirty("parameter_value_list", updated_ids)
            return updated_ids, intgr_error_log
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating parameter value lists: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)
