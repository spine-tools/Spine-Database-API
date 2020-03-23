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

    def _handle_items(self, tablename, checked_kwargs_list, filter_key=("id",)):
        """Return lists of items for update and insert.
        Items found in the diff classes should be updated, whereas items found in the orig classes
        should be marked as dirty and inserted into the corresponding diff class."""
        classname = self.table_to_class[tablename]
        orig_class = getattr(self, classname)
        diff_class = getattr(self, "Diff" + classname)
        primary_id = self.table_ids.get(tablename, "id")
        items_for_update = list()
        items_for_insert = list()
        dirty_ids = set()
        updated_ids = set()
        for kwargs in checked_kwargs_list:
            try:
                filter_ = {k: kwargs[k] for k in filter_key}
            except KeyError:
                continue
            if len(filter_) == len(kwargs):
                continue
            diff_query = self.query(diff_class).filter_by(**filter_)
            for diff_item in diff_query:
                updated_kwargs = attr_dict(diff_item)
                if all(updated_kwargs[k] == kwargs[k] for k in updated_kwargs.keys() & kwargs.keys()):
                    continue
                updated_kwargs.update(kwargs)
                items_for_update.append(updated_kwargs)
                updated_ids.add(updated_kwargs[primary_id])
            if diff_query.count() > 0:
                # Don't look in orig_class if found in diff_class
                continue
            for orig_item in self.query(orig_class).filter_by(**filter_):
                updated_kwargs = attr_dict(orig_item)
                if all(updated_kwargs[k] == kwargs[k] for k in updated_kwargs.keys() & kwargs.keys()):
                    continue
                updated_kwargs.update(kwargs)
                items_for_insert.append(updated_kwargs)
                dirty_ids.add(updated_kwargs[primary_id])
                updated_ids.add(updated_kwargs[primary_id])
        return items_for_update, items_for_insert, dirty_ids, updated_ids

    def update_alternatives(self, *kwargs_list, strict=False):
        """Update alternatives."""
        checked_kwargs_list, intgr_error_log = self.check_alternatives_for_update(*kwargs_list, strict=strict)
        updated_ids = self._update_alternatives(*checked_kwargs_list)
        updated_item_list = self.query(self.alternative_sq).filter(self.alternative_sq.c.id.in_(updated_ids))
        return updated_item_list, intgr_error_log

    def _update_alternatives(self, *checked_kwargs_list, strict=False):
        """Update object classes without checking integrity."""
        try:
            items_for_update, items_for_insert, dirty_ids, updated_ids = self._handle_items(
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
        updated_item_list = self.query(self.scenario_sq).filter(self.scenario_sq.c.id.in_(updated_ids))
        return updated_item_list, intgr_error_log

    def _update_scenarios(self, *checked_kwargs_list, strict=False):
        """Update scenarios without checking integrity."""

        try:
            items_for_update, items_for_insert, dirty_ids, updated_ids = self._handle_items(
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
        updated_item_list = self.query(self.scenario_alternatives_sq).filter(
            self.scenario_alternatives_sq.c.id.in_(updated_ids)
        )
        return updated_item_list, intgr_error_log

    def _update_scenario_alternatives(self, *checked_kwargs_list, strict=False):
        """Update scenario_alternatives without checking integrity."""

        try:
            items_for_update, items_for_insert, dirty_ids, updated_ids = self._handle_items(
                "scenario_alternatives", checked_kwargs_list
            )
            self.session.bulk_update_mappings(self.DiffScenarioAlternatives, items_for_update)
            self.session.bulk_insert_mappings(self.DiffScenarioAlternatives, items_for_insert)
            self.session.commit()
            self._mark_as_dirty("scenario_alternatives", dirty_ids)
            self.updated_item_id["scenario_alternatives"].update(dirty_ids)
            return updated_ids
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating scenario_alternatives: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

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
            self.updated_item_id["entity_class"].update(dirty_ids)
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
                "entity", checked_kwargs_list
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
                "entity_class", checked_wide_kwargs_list
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
            ent_kwargs = dict(wide_kwargs)
            object_id_list = ent_kwargs.pop("object_id_list", [])
            ent_kwargs_list.append(ent_kwargs)
            for dimension, member_id in enumerate(object_id_list):
                rel_ent_kwargs = dict(ent_kwargs)
                rel_ent_kwargs["entity_id"] = rel_ent_kwargs.pop("id", None)
                rel_ent_kwargs["dimension"] = dimension
                rel_ent_kwargs["member_id"] = member_id
                rel_ent_kwargs_list.append(rel_ent_kwargs)
        try:
            ents_for_update, ents_for_insert, dirty_ent_ids, updated_ent_ids = self._handle_items(
                "entity", ent_kwargs_list, filter_key=("id",)
            )
            rel_ents_for_update, rel_ents_for_insert, dirty_rel_ent_ids, updated_rel_ent_ids = self._handle_items(
                "relationship_entity", rel_ent_kwargs_list, filter_key=("entity_id", "dimension")
            )
            self.session.bulk_update_mappings(self.DiffEntity, ents_for_update)
            self.session.bulk_insert_mappings(self.DiffEntity, ents_for_insert)
            self.session.bulk_update_mappings(self.DiffRelationshipEntity, rel_ents_for_update)
            self.session.bulk_insert_mappings(self.DiffRelationshipEntity, rel_ents_for_insert)
            self.session.commit()
            dirty_ids = dirty_ent_ids.union(dirty_rel_ent_ids)
            updated_ids = updated_ent_ids.union(updated_rel_ent_ids)
            self._mark_as_dirty("entity", dirty_ids)
            self._mark_as_dirty("relationship_entity", dirty_ids)
            self.updated_item_id["entity"].update(dirty_ids)
            self.updated_item_id["relationship_entity"].update(dirty_ids)
            return updated_ids
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating relationships: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_parameter_definitions(self, *kwargs_list, strict=False):
        """Update parameter definitions."""
        checked_kwargs_list, intgr_error_log = self.check_parameter_definitions_for_update(*kwargs_list, strict=strict)
        updated_ids = self._update_parameter_definitions(*checked_kwargs_list)
        updated_item_list = self.query(self.parameter_definition_sq).filter(
            self.parameter_definition_sq.c.id.in_(updated_ids)
        )
        return updated_item_list, intgr_error_log

    def _update_parameter_definitions(self, *checked_kwargs_list):
        """Update parameter definitions without checking integrity."""
        try:
            items_for_update, items_for_insert, dirty_ids, updated_ids = self._handle_items(
                "parameter_definition", checked_kwargs_list
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

    def update_parameter_values(self, *kwargs_list, strict=False):
        """Update parameter values."""
        checked_kwargs_list, intgr_error_log = self.check_parameter_values_for_update(*kwargs_list, strict=strict)
        updated_ids = self._update_parameter_values(*checked_kwargs_list)
        updated_item_list = self.query(self.parameter_value_sq).filter(self.parameter_value_sq.c.id.in_(updated_ids))
        return updated_item_list, intgr_error_log

    def update_checked_parameter_values(self, *checked_kwargs_list):
        """Update checked parameter values."""
        updated_ids = self._update_parameter_values(*checked_kwargs_list)
        updated_item_list = self.query(self.parameter_value_sq).filter(self.parameter_value_sq.c.id.in_(updated_ids))
        return updated_item_list, []

    def _update_parameter_values(self, *checked_kwargs_list):
        """Update parameter values.

        Returns:
            updated_ids (set): updated instances' ids
        """
        try:
            items_for_update, items_for_insert, dirty_ids, updated_ids = self._handle_items(
                "parameter_value", checked_kwargs_list
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

    def set_parameter_definition_tags(self, *items, strict=False):
        """Set tags for parameter definitions."""
        current_tag_id_lists = {
            x.parameter_definition_id: x.parameter_tag_id_list
            for x in self.query(self.wide_parameter_definition_tag_sq)
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
        sq = self.wide_parameter_definition_tag_sq
        return self.query(sq).filter(sq.c.parameter_definition_id.in_(definition_ids)), error_log

    def update_wide_parameter_value_lists(self, *wide_kwargs_list, strict=False):
        """Update parameter value_lists.
        """
        # NOTE: Since the value list can actually change size, we proceed by removing the entire list and then
        # inserting the new one to avoid unnecessary headaches
        checked_wide_kwargs_list, intgr_error_log = self.check_wide_parameter_value_lists_for_update(
            *wide_kwargs_list, strict=strict
        )
        wide_parameter_value_lists = {x.id: x._asdict() for x in self.query(self.wide_parameter_value_list_sq)}
        updated_ids = set()
        item_list = list()
        for wide_kwargs in checked_wide_kwargs_list:
            if "id" not in wide_kwargs:
                continue
            if len(wide_kwargs) == 1:
                continue
            id_ = wide_kwargs["id"]
            if id_ not in wide_parameter_value_lists:
                continue
            updated_ids.add(id_)
            updated_wide_kwargs = wide_parameter_value_lists[id_]
            updated_wide_kwargs["value_list"] = updated_wide_kwargs["value_list"].split(",")
            updated_wide_kwargs.update(wide_kwargs)
            for k, value in enumerate(updated_wide_kwargs["value_list"]):
                narrow_kwargs = {"id": id_, "name": updated_wide_kwargs["name"], "value_index": k, "value": value}
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
            sq = self.wide_parameter_value_list_sq
            updated_item_list = self.query(sq).filter(sq.c.id.in_(updated_ids))
            return updated_item_list, intgr_error_log
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating parameter value lists: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)
