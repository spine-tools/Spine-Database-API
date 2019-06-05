#############################################################################
# Copyright (C) 2017 - 2018 VTT Technical Research Centre of Finland
#
# This file is part of Spine Database API.
#
# Spine Spine Database API is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#############################################################################

"""
A class to handle UPDATE operations onto a Spine db 'diff' ORM.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""

import warnings
from sqlalchemy.exc import DBAPIError
from .exception import SpineDBAPIError
from .helpers import attr_dict

# TODO: improve docstrings


class DiffDatabaseMappingUpdateMixin:
    """A mixin to handle UPDATE operations onto a Spine db 'diff' ORM."""

    def __init__(self, *args, **kwargs):
        """Initialize class."""
        super().__init__(*args, **kwargs)

    def handle_items(self, orig_class, diff_class, checked_kwargs_list, filter_key=("id",), unhandled_fields=()):
        """Return lists of items for update and insert.
        Items found in the diff classes should be updated,
        whereas items found in the orig classes should be marked as dirty and
        inserted into the corresponding diff class."""
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
            if any(x in kwargs for x in unhandled_fields):
                continue
            diff_query = self.query(diff_class).filter_by(**filter_)
            for diff_item in diff_query:
                updated_kwargs = attr_dict(diff_item)
                updated_kwargs.update(kwargs)
                items_for_update.append(updated_kwargs)
                updated_ids.add(updated_kwargs["id"])
            if diff_query.count() > 0:
                # Don't look in orig_class if found in diff_class
                continue
            for orig_item in self.query(orig_class).filter_by(**filter_):
                updated_kwargs = attr_dict(orig_item)
                updated_kwargs.update(kwargs)
                items_for_insert.append(updated_kwargs)
                dirty_ids.add(updated_kwargs["id"])
                updated_ids.add(updated_kwargs["id"])
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
            items_for_update, items_for_insert, dirty_ids, updated_ids = self.handle_items(
                self.ObjectClass, self.DiffObjectClass, checked_kwargs_list
            )
            self.session.bulk_update_mappings(self.DiffObjectClass, items_for_update)
            self.session.bulk_insert_mappings(self.DiffObjectClass, items_for_insert)
            self.session.commit()
            self._mark_as_dirty("object_class", dirty_ids)
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
            items_for_update, items_for_insert, dirty_ids, updated_ids = self.handle_items(
                self.Object, self.DiffObject, checked_kwargs_list, unhandled_fields=("class_id",)
            )
            self.session.bulk_update_mappings(self.DiffObject, items_for_update)
            self.session.bulk_insert_mappings(self.DiffObject, items_for_insert)
            self.session.commit()
            self._mark_as_dirty("object", dirty_ids)
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
            items_for_update, items_for_insert, dirty_ids, updated_ids = self.handle_items(
                self.RelationshipClass,
                self.DiffRelationshipClass,
                checked_wide_kwargs_list,
                unhandled_fields=("object_class_id_list",),
            )
            self.session.bulk_update_mappings(self.DiffRelationshipClass, items_for_update)
            self.session.bulk_insert_mappings(self.DiffRelationshipClass, items_for_insert)
            self.session.commit()
            self._mark_as_dirty("relationship_class", dirty_ids)
            self.updated_item_id["relationship_class"].update(dirty_ids)
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
        id_kwargs_list = []
        id_dim_kwargs_list = []
        for wide_kwargs in checked_wide_kwargs_list:
            object_id_list = wide_kwargs.pop("object_id_list", None)
            if object_id_list is None:
                id_kwargs_list.append(wide_kwargs)
                continue
            for dimension, object_id in enumerate(object_id_list):
                narrow_kwargs = dict(wide_kwargs)
                narrow_kwargs["dimension"] = dimension
                narrow_kwargs["object_id"] = object_id
                id_dim_kwargs_list.append(narrow_kwargs)
        try:
            items_for_update, items_for_insert, dirty_ids, updated_ids = self.handle_items(
                self.Relationship,
                self.DiffRelationship,
                id_kwargs_list,
                filter_key=("id",),
                unhandled_fields=("class_id",),
            )
            items_for_update_, items_for_insert_, dirty_ids_, updated_ids_ = self.handle_items(
                self.Relationship,
                self.DiffRelationship,
                id_dim_kwargs_list,
                filter_key=("id", "dimension"),
                unhandled_fields=("class_id",),
            )
            self.session.bulk_update_mappings(self.DiffRelationship, items_for_update + items_for_update_)
            self.session.bulk_insert_mappings(self.DiffRelationship, items_for_insert + items_for_insert_)
            self.session.commit()
            self._mark_as_dirty("relationship", dirty_ids.union(dirty_ids_))
            self.updated_item_id["relationship"].update(dirty_ids.union(dirty_ids_))
            return updated_ids.union(updated_ids_)
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
            items_for_update, items_for_insert, dirty_ids, updated_ids = self.handle_items(
                self.ParameterDefinition,
                self.DiffParameterDefinition,
                checked_kwargs_list,
                unhandled_fields=("object_class_id", "relationship_class_id"),
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
            items_for_update, items_for_insert, dirty_ids, updated_ids = self.handle_items(
                self.ParameterValue,
                self.DiffParameterValue,
                checked_kwargs_list,
                unhandled_fields=("object_id", "relationship_id", "parameter_id"),
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
            items_for_update, items_for_insert, dirty_ids, updated_ids = self.handle_items(
                self.ParameterTag, self.DiffParameterTag, checked_kwargs_list
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
