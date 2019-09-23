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

import warnings
from sqlalchemy.exc import DBAPIError
from .exception import SpineDBAPIError
from .helpers import attr_dict

# TODO: improve docstrings


class DiffDatabaseMappingUpdateMixin:
    """Provides methods to stage ``UPDATE`` operations over a Spine db.
    """

    def __init__(self, *args, **kwargs):
        """Initialize class."""
        super().__init__(*args, **kwargs)

    def _handle_items(self, tablename, checked_kwargs_list, filter_key=("id",), skip_fields=()):
        """Return lists of items for update and insert.
        Items found in the diff classes should be updated,
        whereas items found in the orig classes should be marked as dirty and
        inserted into the corresponding diff class."""
        classname = self.table_to_class[tablename]
        orig_class = getattr(self, classname)
        diff_class = getattr(self, "Diff" + classname)
        primary_id = self.table_ids.get(tablename, "id")
        items_for_update = list()
        items_for_insert = list()
        dirty_ids = set()
        updated_ids = set()
        for kwargs in checked_kwargs_list:
            if any(k not in kwargs for k in filter_key):
                continue
            filter_ = {k: kwargs.pop(k) for k in filter_key}
            if not kwargs:
                continue
            if any(x in kwargs for x in skip_fields):
                continue
            diff_query = self.query(diff_class).filter_by(**filter_)
            for diff_item in diff_query:
                updated_kwargs = attr_dict(diff_item)
                updated_kwargs.update(kwargs)
                items_for_update.append(updated_kwargs)
                updated_ids.add(updated_kwargs[primary_id])
            if diff_query.count() > 0:
                # Don't look in orig_class if found in diff_class
                continue
            for orig_item in self.query(orig_class).filter_by(**filter_):
                updated_kwargs = attr_dict(orig_item)
                updated_kwargs.update(kwargs)
                items_for_insert.append(updated_kwargs)
                dirty_ids.add(updated_kwargs[primary_id])
                updated_ids.add(updated_kwargs[primary_id])
        return items_for_update, items_for_insert, dirty_ids, updated_ids

    def update_object_classes(self, *kwargs_list, strict=False):
        """Update parameter values."""
        checked_kwargs_list, intgr_error_log = self.check_object_classes_for_update(*kwargs_list, strict=strict)
        updated_ids = self._update_object_classes(*checked_kwargs_list)
        updated_item_list = self.query(self.object_class_sq).filter(self.object_class_sq.c.id.in_(updated_ids))
        return updated_item_list, intgr_error_log

    def _update_object_classes(self, *checked_kwargs_list, strict=False):
        """Update object classes without checking integrity."""
        try:
            items_for_update, items_for_insert, dirty_ids, updated_ids = self._handle_items(
                "entity_class", checked_kwargs_list
            )
            self.session.bulk_update_mappings(self.DiffEntityClass, items_for_update)
            self.session.bulk_insert_mappings(self.DiffEntityClass, items_for_insert)
            self.session.commit()
            self._mark_as_dirty("entity_class", dirty_ids)
            self._mark_as_dirty("object_class", dirty_ids)
            self.updated_item_id["entity_class"].update(dirty_ids)
            self.updated_item_id["object_class"].update(dirty_ids)
            return updated_ids
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating object classes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_objects(self, *kwargs_list, strict=False):
        """Update objects."""
        checked_kwargs_list, intgr_error_log = self.check_objects_for_update(*kwargs_list, strict=strict)
        updated_ids = self._update_objects(*checked_kwargs_list)
        updated_item_list = self.query(self.object_sq).filter(self.object_sq.c.id.in_(updated_ids))
        return updated_item_list, intgr_error_log

    def _update_objects(self, *checked_kwargs_list):
        """Update objects without checking integrity."""
        try:
            items_for_update, items_for_insert, dirty_ids, updated_ids = self._handle_items(
                "entity", checked_kwargs_list, skip_fields=("class_id",)
            )
            self.session.bulk_update_mappings(self.DiffEntity, items_for_update)
            self.session.bulk_insert_mappings(self.DiffEntity, items_for_insert)
            self.session.commit()
            self._mark_as_dirty("entity", dirty_ids)
            self._mark_as_dirty("object", dirty_ids)
            self.updated_item_id["entity"].update(dirty_ids)
            self.updated_item_id["object"].update(dirty_ids)
            return updated_ids
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating objects: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_wide_relationship_classes(self, *wide_kwargs_list, strict=False):
        """Update relationship classes."""
        checked_wide_kwargs_list, intgr_error_log = self.check_wide_relationship_classes_for_update(
            *wide_kwargs_list, strict=strict
        )
        updated_ids = self._update_wide_relationship_classes(*checked_wide_kwargs_list)
        updated_item_list = self.query(self.wide_relationship_class_sq).filter(
            self.wide_relationship_class_sq.c.id.in_(updated_ids)
        )
        return updated_item_list, intgr_error_log

    def _update_wide_relationship_classes(self, *checked_wide_kwargs_list):
        """Update relationship classes without checking integrity."""
        try:
            items_for_update, items_for_insert, dirty_ids, updated_ids = self._handle_items(
                "entity_class", checked_wide_kwargs_list, skip_fields=("object_class_id_list",)
            )
            self.session.bulk_update_mappings(self.DiffEntityClass, items_for_update)
            self.session.bulk_insert_mappings(self.DiffEntityClass, items_for_insert)
            self.session.commit()
            self._mark_as_dirty("entity_class", dirty_ids)
            self._mark_as_dirty("relationship_class", dirty_ids)
            self._mark_as_dirty("relationship_entity_class", dirty_ids)
            self.updated_item_id["entity_class"].update(dirty_ids)
            self.updated_item_id["relationship_class"].update(dirty_ids)
            self.updated_item_id["relationship_entity_class"].update(dirty_ids)
            return updated_ids
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating relationship classes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_wide_relationships(self, *wide_kwargs_list, strict=False):
        """Update relationships."""
        checked_wide_kwargs_list, intgr_error_log = self.check_wide_relationships_for_update(
            *wide_kwargs_list, strict=strict
        )
        updated_ids = self._update_wide_relationships(*checked_wide_kwargs_list)
        updated_item_list = self.query(self.wide_relationship_sq).filter(
            self.wide_relationship_sq.c.id.in_(updated_ids)
        )
        return updated_item_list, intgr_error_log

    def _update_wide_relationships(self, *checked_wide_kwargs_list):
        """Update relationships without checking integrity."""
        ent_kwargs_list = []
        rel_ent_kwargs_list = []
        for wide_kwargs in checked_wide_kwargs_list:
            object_id_list = wide_kwargs.pop("object_id_list", [])
            ent_kwargs_list.append(wide_kwargs)
            for dimension, member_id in enumerate(object_id_list):
                rel_ent_kwargs = dict(wide_kwargs)
                rel_ent_kwargs["entity_id"] = rel_ent_kwargs.pop("id", None)
                rel_ent_kwargs["dimension"] = dimension
                rel_ent_kwargs["member_id"] = member_id
                rel_ent_kwargs_list.append(rel_ent_kwargs)
        try:
            ents_for_update, ents_for_insert, dirty_ent_ids, updated_ent_ids = self._handle_items(
                "entity", ent_kwargs_list, filter_key=("id",), skip_fields=("class_id",)
            )
            rel_ents_for_update, rel_ents_for_insert, dirty_rel_ent_ids, updated_rel_ent_ids = self._handle_items(
                "relationship_entity",
                rel_ent_kwargs_list,
                filter_key=("entity_id", "dimension"),
                skip_fields=("entity_class_id",),
            )
            self.session.bulk_update_mappings(self.DiffEntity, ents_for_update)
            self.session.bulk_insert_mappings(self.DiffEntity, ents_for_insert)
            self.session.bulk_update_mappings(self.DiffRelationshipEntity, rel_ents_for_update)
            self.session.bulk_insert_mappings(self.DiffRelationshipEntity, rel_ents_for_insert)
            self.session.commit()
            dirty_ids = dirty_ent_ids.union(dirty_rel_ent_ids)
            updated_ids = updated_ent_ids.union(updated_rel_ent_ids)
            self._mark_as_dirty("entity", dirty_ids)
            self._mark_as_dirty("relationship", dirty_ids)
            self._mark_as_dirty("relationship_entity", dirty_ids)
            self.updated_item_id["entity"].update(dirty_ids)
            self.updated_item_id["relationship"].update(dirty_ids)
            self.updated_item_id["relationship_entity"].update(dirty_ids)
            return updated_ids
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating relationships: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_parameter_definitions(self, *kwargs_list, strict=False):
        """Update parameter definitions."""
        checked_kwargs_list, intgr_error_log = self.check_parameter_definitions_for_update(*kwargs_list, strict=strict)
        updated_ids = self._update_parameters(*checked_kwargs_list)
        updated_item_list = self.query(self.parameter_definition_sq).filter(
            self.parameter_definition_sq.c.id.in_(updated_ids)
        )
        return updated_item_list, intgr_error_log

    def _update_parameters(self, *checked_kwargs_list):
        """Update parameter definitions without checking integrity."""
        try:
            items_for_update, items_for_insert, dirty_ids, updated_ids = self._handle_items(
                "parameter_definition", checked_kwargs_list, skip_fields=("object_class_id", "relationship_class_id")
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

    def update_parameters(self, *kwargs_list, strict=False):
        warnings.warn("update_parameters is deprecated, use update_parameter_definitions instead", DeprecationWarning)
        return self.update_parameter_definitions(*kwargs_list, strict=strict)

    def update_parameter_values(self, *kwargs_list, strict=False):
        """Update parameter values."""
        checked_kwargs_list, intgr_error_log = self.check_parameter_values_for_update(*kwargs_list, strict=strict)
        updated_ids = self._update_parameter_values(*checked_kwargs_list)
        updated_item_list = self.query(self.parameter_value_sq).filter(self.parameter_value_sq.c.id.in_(updated_ids))
        return updated_item_list, intgr_error_log

    def _update_parameter_values(self, *checked_kwargs_list):
        """Update parameter values.

        Returns:
            updated_ids (set): updated instances' ids
        """
        try:
            items_for_update, items_for_insert, dirty_ids, updated_ids = self._handle_items(
                "parameter_value",
                checked_kwargs_list,
                skip_fields=("object_id", "relationship_id", "parameter_definition_id"),
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

    def update_parameter_tags(self, *kwargs_list, strict=False):
        """Update parameter tags."""
        checked_kwargs_list, intgr_error_log = self.check_parameter_tags_for_update(*kwargs_list, strict=strict)
        updated_ids = self._update_parameter_tags(*checked_kwargs_list)
        updated_item_list = self.query(self.parameter_tag_sq).filter(self.parameter_tag_sq.c.id.in_(updated_ids))
        return updated_item_list, intgr_error_log

    def _update_parameter_tags(self, *checked_kwargs_list):
        """Update parameter tags without checking integrity."""
        try:
            items_for_update, items_for_insert, dirty_ids, updated_ids = self._handle_items(
                "parameter_tag", checked_kwargs_list
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

    def set_parameter_definition_tags(self, tag_dict, strict=False):
        """Set tags for parameter definitions."""
        tag_id_lists = {
            x.parameter_definition_id: [int(y) for y in x.parameter_tag_id_list.split(",")]
            for x in self.wide_parameter_definition_tag_list()
        }
        definition_tag_id_dict = {
            (x.parameter_definition_id, x.parameter_tag_id): x.id for x in self.parameter_definition_tag_list()
        }
        items_to_insert = list()
        ids_to_delete = set()
        for definition_id, tag_id_list in tag_dict.items():
            target_tag_id_list = [int(x) for x in tag_id_list.split(",")] if tag_id_list else []
            current_tag_id_list = tag_id_lists.get(definition_id, [])
            for tag_id in target_tag_id_list:
                if tag_id not in current_tag_id_list:
                    item = {"parameter_definition_id": definition_id, "parameter_tag_id": tag_id}
                    items_to_insert.append(item)
            for tag_id in current_tag_id_list:
                if tag_id not in target_tag_id_list:
                    ids_to_delete.add(definition_tag_id_dict[definition_id, tag_id])
        deleted_items = self.parameter_definition_tag_list(id_list=ids_to_delete)
        self.remove_items(parameter_definition_tag_ids=ids_to_delete)
        added_items, error_log = self.add_parameter_definition_tags(*items_to_insert, strict=strict)
        return added_items.all() + deleted_items.all(), error_log

    def update_wide_parameter_value_lists(self, *wide_kwargs_list, strict=False):
        """Update parameter value_lists.
        NOTE: It's too difficult to do it the usual way, so we just remove and then add.
        """
        checked_wide_kwargs_list, intgr_error_log = self.check_wide_parameter_value_lists_for_update(
            *wide_kwargs_list, strict=strict
        )
        wide_parameter_value_list_dict = {x.id: x._asdict() for x in self.wide_parameter_value_list_list()}
        updated_ids = set()
        item_list = list()
        for wide_kwargs in checked_wide_kwargs_list:
            id = wide_kwargs.pop("id")
            if not id or not wide_kwargs:
                continue
            updated_ids.add(id)
            try:
                updated_wide_kwargs = wide_parameter_value_list_dict[id]
            except KeyError:
                continue
            # Split value_list so it's actually a list
            updated_wide_kwargs["value_list"] = updated_wide_kwargs["value_list"].split(",")
            updated_wide_kwargs.update(wide_kwargs)
            for k, value in enumerate(updated_wide_kwargs["value_list"]):
                narrow_kwargs = {"id": id, "name": updated_wide_kwargs["name"], "value_index": k, "value": value}
                item_list.append(narrow_kwargs)
        try:
            self.query(self.DiffParameterValueList).filter(self.DiffParameterValueList.id.in_(updated_ids)).delete(
                synchronize_session=False
            )
            self.session.bulk_insert_mappings(self.DiffParameterValueList, item_list)
            self.session.commit()
            self.added_item_id["parameter_value_list"].update(updated_ids)
            self.removed_item_id["parameter_value_list"].update(updated_ids)
            self._mark_as_dirty("parameter_value_list", updated_ids)
            updated_item_list = self.wide_parameter_value_list_list(id_list=updated_ids)
            return updated_item_list, intgr_error_log
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating parameter value lists: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)
