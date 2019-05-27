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
Classes to handle the Spine database object relational mapping.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""

from sqlalchemy.exc import DBAPIError
from .exception import SpineDBAPIError
from .helpers import attr_dict
from ._add_diff_database_mapping import _AddDiffDatabaseMapping


# TODO: improve docstrings


class _UpdateDiffDatabaseMapping(_AddDiffDatabaseMapping):
    def __init__(self, db_url, username=None, create_all=True, upgrade=False):
        """Initialize class."""
        super().__init__(
            db_url, username=username, create_all=create_all, upgrade=upgrade
        )

    # TODO: split all update_ methods like update_parameter_values...

    def update_object_classes(self, *kwargs_list, strict=False):
        """Update object classes."""
        checked_kwargs_list, intgr_error_log = self.check_object_classes_for_update(
            *kwargs_list, strict=strict
        )
        try:
            items_for_update = list()
            items_for_insert = list()
            new_dirty_ids = set()
            updated_ids = set()
            for kwargs in checked_kwargs_list:
                id = kwargs.pop("id")
                if not id or not kwargs:
                    continue
                diff_item = (
                    self.session.query(self.DiffObjectClass)
                    .filter_by(id=id)
                    .one_or_none()
                )
                if diff_item:
                    updated_kwargs = attr_dict(diff_item)
                    updated_kwargs.update(kwargs)
                    items_for_update.append(updated_kwargs)
                    updated_ids.add(id)
                else:
                    item = (
                        self.session.query(self.ObjectClass)
                        .filter_by(id=id)
                        .one_or_none()
                    )
                    if item:
                        updated_kwargs = attr_dict(item)
                        updated_kwargs.update(kwargs)
                        items_for_insert.append(updated_kwargs)
                        new_dirty_ids.add(id)
                        updated_ids.add(id)
            self.session.bulk_update_mappings(self.DiffObjectClass, items_for_update)
            self.session.bulk_insert_mappings(self.DiffObjectClass, items_for_insert)
            self.session.commit()
            self.touched_item_id["object_class"].update(new_dirty_ids)
            self.dirty_item_id["object_class"].update(new_dirty_ids)
            updated_item_list = self.object_class_list(id_list=updated_ids)
            return updated_item_list, intgr_error_log
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating object classes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_objects(self, *kwargs_list, strict=False):
        """Update objects."""
        checked_kwargs_list, intgr_error_log = self.check_objects_for_update(
            *kwargs_list, strict=strict
        )
        try:
            items_for_update = list()
            items_for_insert = list()
            new_dirty_ids = set()
            updated_ids = set()
            for kwargs in checked_kwargs_list:
                if "class_id" in kwargs:
                    continue
                id = kwargs.pop("id")
                if not id or not kwargs:
                    continue
                diff_item = (
                    self.session.query(self.DiffObject).filter_by(id=id).one_or_none()
                )
                if diff_item:
                    updated_kwargs = attr_dict(diff_item)
                    updated_kwargs.update(kwargs)
                    items_for_update.append(updated_kwargs)
                    updated_ids.add(id)
                else:
                    item = (
                        self.session.query(self.Object).filter_by(id=id).one_or_none()
                    )
                    if item:
                        updated_kwargs = attr_dict(item)
                        updated_kwargs.update(kwargs)
                        items_for_insert.append(updated_kwargs)
                        new_dirty_ids.add(id)
                        updated_ids.add(id)
            self.session.bulk_update_mappings(self.DiffObject, items_for_update)
            self.session.bulk_insert_mappings(self.DiffObject, items_for_insert)
            self.session.commit()
            self.touched_item_id["object"].update(new_dirty_ids)
            self.dirty_item_id["object"].update(new_dirty_ids)
            updated_item_list = self.object_list(id_list=updated_ids)
            return updated_item_list, intgr_error_log
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating objects: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_wide_relationship_classes(self, *wide_kwargs_list, strict=False):
        """Update relationship classes."""
        checked_wide_kwargs_list, intgr_error_log = self.check_wide_relationship_classes_for_update(
            *wide_kwargs_list, strict=strict
        )
        try:
            items_for_update = list()
            items_for_insert = list()
            new_dirty_ids = set()
            updated_ids = set()
            for wide_kwargs in checked_wide_kwargs_list:
                # Don't update object_class_id for now (even though below we handle it)
                if "object_class_id_list" in wide_kwargs:
                    continue
                id = wide_kwargs.pop("id")
                if not id or not wide_kwargs:
                    continue
                object_class_id_list = wide_kwargs.pop("object_class_id_list", list())
                diff_item_list = self.session.query(
                    self.DiffRelationshipClass
                ).filter_by(id=id)
                if diff_item_list.count():
                    for dimension, diff_item in enumerate(diff_item_list):
                        narrow_kwargs = wide_kwargs
                        try:
                            narrow_kwargs.update(
                                {"object_class_id": object_class_id_list[dimension]}
                            )
                        except IndexError:
                            pass
                        updated_kwargs = attr_dict(diff_item)
                        updated_kwargs.update(narrow_kwargs)
                        items_for_update.append(updated_kwargs)
                    updated_ids.add(id)
                else:
                    item_list = self.session.query(self.RelationshipClass).filter_by(
                        id=id
                    )
                    if item_list.count():
                        for dimension, item in enumerate(item_list):
                            narrow_kwargs = wide_kwargs
                            try:
                                narrow_kwargs.update(
                                    {"object_class_id": object_class_id_list[dimension]}
                                )
                            except IndexError:
                                pass
                            updated_kwargs = attr_dict(item)
                            updated_kwargs.update(narrow_kwargs)
                            items_for_insert.append(updated_kwargs)
                        new_dirty_ids.add(id)
                        updated_ids.add(id)
            self.session.bulk_update_mappings(
                self.DiffRelationshipClass, items_for_update
            )
            self.session.bulk_insert_mappings(
                self.DiffRelationshipClass, items_for_insert
            )
            self.session.commit()
            self.touched_item_id["relationship_class"].update(new_dirty_ids)
            self.dirty_item_id["relationship_class"].update(new_dirty_ids)
            updated_item_list = self.wide_relationship_class_list(id_list=updated_ids)
            return updated_item_list, intgr_error_log
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating relationship classes: {}".format(
                e.orig.args
            )
            raise SpineDBAPIError(msg)

    def update_wide_relationships(self, *wide_kwargs_list, strict=False):
        """Update relationships."""
        checked_wide_kwargs_list, intgr_error_log = self.check_wide_relationships_for_update(
            *wide_kwargs_list, strict=strict
        )
        try:
            items_for_update = list()
            items_for_insert = list()
            new_dirty_ids = set()
            updated_ids = set()
            for wide_kwargs in checked_wide_kwargs_list:
                if "class_id" in wide_kwargs:
                    continue
                id = wide_kwargs.pop("id")
                if not id or not wide_kwargs:
                    continue
                object_id_list = wide_kwargs.pop("object_id_list", list())
                diff_item_list = (
                    self.session.query(self.DiffRelationship)
                    .filter_by(id=id)
                    .order_by(self.DiffRelationship.dimension)
                )
                if diff_item_list.count():
                    for dimension, diff_item in enumerate(diff_item_list):
                        narrow_kwargs = wide_kwargs
                        try:
                            narrow_kwargs.update(
                                {"object_id": object_id_list[dimension]}
                            )
                        except IndexError:
                            pass
                        updated_kwargs = attr_dict(diff_item)
                        updated_kwargs.update(narrow_kwargs)
                        items_for_update.append(updated_kwargs)
                    updated_ids.add(id)
                else:
                    item_list = self.session.query(self.Relationship).filter_by(id=id)
                    if item_list.count():
                        for dimension, item in enumerate(item_list):
                            narrow_kwargs = wide_kwargs
                            try:
                                narrow_kwargs.update(
                                    {"object_id": object_id_list[dimension]}
                                )
                            except IndexError:
                                pass
                            updated_kwargs = attr_dict(item)
                            updated_kwargs.update(narrow_kwargs)
                            items_for_insert.append(updated_kwargs)
                        new_dirty_ids.add(id)
                        updated_ids.add(id)
            self.session.bulk_update_mappings(self.DiffRelationship, items_for_update)
            self.session.bulk_insert_mappings(self.DiffRelationship, items_for_insert)
            self.session.commit()
            self.touched_item_id["relationship"].update(new_dirty_ids)
            self.dirty_item_id["relationship"].update(new_dirty_ids)
            updated_item_list = self.wide_relationship_list(id_list=updated_ids)
            return updated_item_list, intgr_error_log
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating relationships: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_parameters(self, *kwargs_list, strict=False):
        """Update parameters."""
        checked_kwargs_list, intgr_error_log = self.check_parameter_definitions_for_update(
            *kwargs_list, strict=strict
        )
        try:
            items_for_update = list()
            items_for_insert = list()
            new_dirty_ids = set()
            updated_ids = set()
            for kwargs in checked_kwargs_list:
                if "object_class_id" in kwargs or "relationship_class_id" in kwargs:
                    continue
                id = kwargs.pop("id")
                if not id or not kwargs:
                    continue
                diff_item = (
                    self.session.query(self.DiffParameterDefinition)
                    .filter_by(id=id)
                    .one_or_none()
                )
                if diff_item:
                    updated_kwargs = attr_dict(diff_item)
                    updated_kwargs.update(kwargs)
                    items_for_update.append(updated_kwargs)
                    updated_ids.add(id)
                else:
                    item = (
                        self.session.query(self.ParameterDefinition)
                        .filter_by(id=id)
                        .one_or_none()
                    )
                    if item:
                        updated_kwargs = attr_dict(item)
                        updated_kwargs.update(kwargs)
                        items_for_insert.append(updated_kwargs)
                        new_dirty_ids.add(id)
                        updated_ids.add(id)
            self.session.bulk_update_mappings(
                self.DiffParameterDefinition, items_for_update
            )
            self.session.bulk_insert_mappings(
                self.DiffParameterDefinition, items_for_insert
            )
            self.session.commit()
            self.touched_item_id["parameter_definition"].update(new_dirty_ids)
            self.dirty_item_id["parameter_definition"].update(new_dirty_ids)
            updated_item_list = self.parameter_list(id_list=updated_ids)
            return updated_item_list, intgr_error_log
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating parameters: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_parameter_values(self, *kwargs_list, strict=False):
        """Update parameter values."""
        checked_kwargs_list, intgr_error_log = self.check_parameter_values_for_update(
            *kwargs_list, strict=strict
        )
        updated_ids = self._update_parameter_values(*checked_kwargs_list)
        updated_item_list = self.parameter_value_list(id_list=updated_ids)
        return updated_item_list, intgr_error_log

    def _update_parameter_values(self, *kwargs_list):
        """Update parameter values.

        Returns:
            updated_ids (set): updated instances' ids
        """
        try:
            items_for_update = list()
            items_for_insert = list()
            new_dirty_ids = set()
            updated_ids = set()
            for kwargs in kwargs_list:
                if (
                    "object_id" in kwargs
                    or "relationship_id" in kwargs
                    or "parameter_id" in kwargs
                ):
                    continue
                id = kwargs.pop("id")
                if not id or not kwargs:
                    continue
                diff_item = (
                    self.session.query(self.DiffParameterValue)
                    .filter_by(id=id)
                    .one_or_none()
                )
                if diff_item:
                    updated_kwargs = attr_dict(diff_item)
                    updated_kwargs.update(kwargs)
                    items_for_update.append(updated_kwargs)
                    updated_ids.add(id)
                else:
                    item = (
                        self.session.query(self.ParameterValue)
                        .filter_by(id=id)
                        .one_or_none()
                    )
                    if item:
                        updated_kwargs = attr_dict(item)
                        updated_kwargs.update(kwargs)
                        items_for_insert.append(updated_kwargs)
                        new_dirty_ids.add(id)
                        updated_ids.add(id)
            self.session.bulk_update_mappings(self.DiffParameterValue, items_for_update)
            self.session.bulk_insert_mappings(self.DiffParameterValue, items_for_insert)
            self.session.commit()
            self.touched_item_id["parameter_value"].update(new_dirty_ids)
            self.dirty_item_id["parameter_value"].update(new_dirty_ids)
            return updated_ids
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating parameter values: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_parameter_tags(self, *kwargs_list, strict=False):
        """Update parameter tags."""
        checked_kwargs_list, intgr_error_log = self.check_parameter_tags_for_update(
            *kwargs_list, strict=strict
        )
        try:
            items_for_update = list()
            items_for_insert = list()
            new_dirty_ids = set()
            updated_ids = set()
            for kwargs in checked_kwargs_list:
                try:
                    id = kwargs["id"]
                except KeyError:
                    continue
                diff_item = (
                    self.session.query(self.DiffParameterTag)
                    .filter_by(id=id)
                    .one_or_none()
                )
                if diff_item:
                    updated_kwargs = attr_dict(diff_item)
                    updated_kwargs.update(kwargs)
                    items_for_update.append(updated_kwargs)
                    updated_ids.add(id)
                else:
                    item = (
                        self.session.query(self.ParameterTag)
                        .filter_by(id=id)
                        .one_or_none()
                    )
                    if item:
                        updated_kwargs = attr_dict(item)
                        updated_kwargs.update(kwargs)
                        items_for_insert.append(updated_kwargs)
                        new_dirty_ids.add(id)
                        updated_ids.add(id)
            self.session.bulk_update_mappings(self.DiffParameterTag, items_for_update)
            self.session.bulk_insert_mappings(self.DiffParameterTag, items_for_insert)
            self.session.commit()
            self.touched_item_id["parameter_tag"].update(new_dirty_ids)
            self.dirty_item_id["parameter_tag"].update(new_dirty_ids)
            updated_item_list = self.parameter_tag_list(id_list=updated_ids)
            return updated_item_list, intgr_error_log
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating parameter tags: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def set_parameter_definition_tags(self, tag_dict, strict=False):
        """Set tags for parameter definitions."""
        tag_id_lists = {
            x.parameter_definition_id: [
                int(y) for y in x.parameter_tag_id_list.split(",")
            ]
            for x in self.wide_parameter_definition_tag_list()
        }
        definition_tag_id_dict = {
            (x.parameter_definition_id, x.parameter_tag_id): x.id
            for x in self.parameter_definition_tag_list()
        }
        items_to_insert = list()
        ids_to_delete = set()
        for definition_id, tag_id_list in tag_dict.items():
            target_tag_id_list = (
                [int(x) for x in tag_id_list.split(",")] if tag_id_list else []
            )
            current_tag_id_list = tag_id_lists.get(definition_id, [])
            for tag_id in target_tag_id_list:
                if tag_id not in current_tag_id_list:
                    item = {
                        "parameter_definition_id": definition_id,
                        "parameter_tag_id": tag_id,
                    }
                    items_to_insert.append(item)
            for tag_id in current_tag_id_list:
                if tag_id not in target_tag_id_list:
                    ids_to_delete.add(definition_tag_id_dict[definition_id, tag_id])
        deleted_items = self.parameter_definition_tag_list(id_list=ids_to_delete).all()
        self.remove_items(parameter_definition_tag_ids=ids_to_delete)
        added_items, error_log = self.add_parameter_definition_tags(
            *items_to_insert, strict=strict
        )
        return added_items.all() + deleted_items, error_log

    def update_wide_parameter_value_lists(self, *wide_kwargs_list, strict=False):
        """Update parameter value_lists.
        NOTE: It's too difficult to do it cleanly, so we just remove and then add.
        """
        checked_wide_kwargs_list, intgr_error_log = self.check_wide_parameter_value_lists_for_update(
            *wide_kwargs_list, strict=strict
        )
        wide_parameter_value_list_dict = {
            x.id: x._asdict() for x in self.wide_parameter_value_list_list()
        }
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
            updated_wide_kwargs["value_list"] = updated_wide_kwargs["value_list"].split(
                ","
            )
            updated_wide_kwargs.update(wide_kwargs)
            for k, value in enumerate(updated_wide_kwargs["value_list"]):
                narrow_kwargs = {
                    "id": id,
                    "name": updated_wide_kwargs["name"],
                    "value_index": k,
                    "value": value,
                }
                item_list.append(narrow_kwargs)
        try:
            self.session.query(self.DiffParameterValueList).filter(
                self.DiffParameterValueList.id.in_(updated_ids)
            ).delete(synchronize_session=False)
            self.session.bulk_insert_mappings(self.DiffParameterValueList, item_list)
            self.session.commit()
            self.new_item_id["parameter_value_list"].update(updated_ids)
            self.removed_item_id["parameter_value_list"].update(updated_ids)
            self.touched_item_id["parameter_value_list"].update(updated_ids)
            updated_item_list = self.wide_parameter_value_list_list(id_list=updated_ids)
            return updated_item_list, intgr_error_log
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating parameter value lists: {}".format(
                e.orig.args
            )
            raise SpineDBAPIError(msg)
