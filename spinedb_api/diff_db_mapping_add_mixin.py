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

"""Provides :class:`.DiffDatabaseMappingAddMixin`.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""
# TODO: improve docstrings

from datetime import datetime
from sqlalchemy import func, Column, Integer, String, null
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import DBAPIError
from .exception import SpineDBAPIError
from .helpers import get_relationship_entity_class_items, get_relationship_entity_items, get_parameter_value_list_items


class DiffDatabaseMappingAddMixin:
    """Provides methods to stage ``INSERT`` operations over a Spine db.
    """

    class NextId(declarative_base()):
        __tablename__ = "next_id"

        user = Column(String(155), primary_key=True)
        date = Column(String(155), primary_key=True)
        entity_id = Column(Integer, server_default=null())
        entity_class_id = Column(Integer, server_default=null())
        entity_group_id = Column(Integer, server_default=null())
        parameter_definition_id = Column(Integer, server_default=null())
        parameter_value_id = Column(Integer, server_default=null())
        parameter_tag_id = Column(Integer, server_default=null())
        parameter_value_list_id = Column(Integer, server_default=null())
        parameter_definition_tag_id = Column(Integer, server_default=null())
        alternative_id = Column(Integer, server_default=null())
        scenario_id = Column(Integer, server_default=null())
        scenario_alternative_id = Column(Integer, server_default=null())
        tool_id = Column(Integer, server_default=null())
        feature_id = Column(Integer, server_default=null())
        tool_feature_id = Column(Integer, server_default=null())
        tool_feature_method_id = Column(Integer, server_default=null())
        metadata_id = Column(Integer, server_default=null())
        parameter_value_metadata_id = Column(Integer, server_default=null())
        entity_metadata_id = Column(Integer, server_default=null())

    def __init__(self, *args, **kwargs):
        """Initialize class."""
        super().__init__(*args, **kwargs)
        self.NextId.__table__.create(self.connection, checkfirst=True)

    def _next_id_row(self):
        """Returns the next_id row."""
        next_id = self.query(self.NextId).one_or_none()
        if next_id:
            next_id.user = self.username
            next_id.date = datetime.utcnow()
        else:
            next_id = self.NextId(user=self.username, date=datetime.utcnow())
            self.session.add(next_id)
        try:
            # TODO: This flush is supposed to lock the record, so no one can steal our ids.... does it work?
            self.session.flush()
        except DBAPIError as e:
            # TODO: Find a way to try this again, or wait till unlocked
            # Maybe listen for an event?
            self.session.rollback()
            raise SpineDBAPIError("Unable to get next id: {}".format(e.orig.args))
        return self.query(self.NextId).one_or_none()

    def _next_id(self, tablename, next_id_candidate=None):
        table = self._metadata.tables[tablename]
        id_col = self.table_ids.get(tablename, "id")
        max_id = self.query(func.max(getattr(table.c, id_col))).scalar()
        next_id = max_id + 1 if max_id else 1
        if next_id_candidate is None:
            return next_id
        return max(next_id_candidate, next_id)

    def _items_and_ids(self, tablename, *items):
        if not items:
            return [], set()
        fieldname = {
            "object_class": "entity_class_id",
            "object": "entity_id",
            "relationship_class": "entity_class_id",
            "relationship": "entity_id",
            "entity_group": "entity_group_id",
            "parameter_definition": "parameter_definition_id",
            "parameter_value": "parameter_value_id",
            "parameter_tag": "parameter_tag_id",
            "parameter_value_list": "parameter_value_list_id",
            "parameter_definition_tag": "parameter_definition_tag_id",
            "alternative": "alternative_id",
            "scenario": "scenario_id",
            "scenario_alternative": "scenario_alternative_id",
            "tool": "tool_id",
            "feature": "feature_id",
            "tool_feature": "tool_feature_id",
            "tool_feature_method": "tool_feature_method_id",
            "metadata": "metadata_id",
            "parameter_value_metadata": "parameter_value_metadata_id",
            "entity_metadata": "entity_metadata_id",
        }[tablename]
        next_id_row = self._next_id_row()
        next_id = getattr(next_id_row, fieldname)
        next_id = self._next_id(tablename, next_id)
        ids = list(range(next_id, next_id + len(items)))
        items_to_add = list()
        append_item = items_to_add.append
        for id_, item in zip(ids, items):
            item["id"] = id_
            append_item(item)
        next_id = ids[-1] + 1
        setattr(next_id_row, fieldname, next_id)
        return items_to_add, set(ids)

    def add_items(self, tablename, *items, strict=False, return_dups=False):
        """Stage items items for insertion.

        Args:
            tablename (str)
            items (Iterable): One or more Python :class:`dict` objects representing the items to be inserted.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if the insertion of one of the items violates an integrity constraint.
            return_dups (bool): Whether or not already existing and duplicated entries should also be returned.

        Returns:
            set: ids succesfully staged
            list(SpineIntegrityError): found violations
        """
        checked_items, intgr_error_log = self.check_items_for_insert(tablename, *items, strict=strict)
        ids = self._add_items(tablename, *checked_items)
        if return_dups:
            ids.update(set(x.id for x in intgr_error_log if x.id))
        return ids, intgr_error_log

    def _add_items(self, tablename, *items):
        """Add object classes to database without checking integrity.

        Args:
            tablename (str)
            items (Iterable): list of dictionaries which correspond to the instances to add
            strict (bool): if True SpineIntegrityError are raised. Otherwise
                they are catched and returned as a log

        Returns:
            ids (set): added instances' ids
        """
        items_to_add, ids = self._items_and_ids(tablename, *items)
        self._do_add_items(tablename, *items_to_add)
        self.added_item_id[tablename].update(ids)
        return ids

    def _do_add_items(self, tablename, *items_to_add):
        table = self._diff_table(tablename)
        try:
            self._checked_execute(table.insert(), items_to_add)
        except DBAPIError as e:
            msg = f"DBAPIError while inserting {tablename} items: {e.orig.args}"
            raise SpineDBAPIError(msg)

    def readd_items(self, tablename, *items):
        """Add known items to database.
        """
        self._do_add_items(tablename, *items)
        ids = set(x["id"] for x in items)
        self.added_item_id[tablename].update(ids)
        return ids, []

    def add_features(self, *items, strict=False, return_dups=False):
        return self.add_items("feature", *items, strict=strict, return_dups=return_dups)

    def add_tools(self, *items, strict=False, return_dups=False):
        return self.add_items("tool", *items, strict=strict, return_dups=return_dups)

    def add_tool_features(self, *items, strict=False, return_dups=False):
        return self.add_items("tool_feature", *items, strict=strict, return_dups=return_dups)

    def add_tool_feature_methods(self, *items, strict=False, return_dups=False):
        return self.add_items("tool_feature_method", *items, strict=strict, return_dups=return_dups)

    def add_alternatives(self, *items, strict=False, return_dups=False):
        return self.add_items("alternative", *items, strict=strict, return_dups=return_dups)

    def add_scenarios(self, *items, strict=False, return_dups=False):
        return self.add_items("scenario", *items, strict=strict, return_dups=return_dups)

    def add_scenario_alternatives(self, *items, strict=False, return_dups=False):
        return self.add_items("scenario_alternative", *items, strict=strict, return_dups=return_dups)

    def add_entity_groups(self, *items, strict=False, return_dups=False):
        return self.add_items("entity_group", *items, strict=strict, return_dups=return_dups)

    def add_parameter_tags(self, *items, strict=False, return_dups=False):
        return self.add_items("parameter_tag", *items, strict=strict, return_dups=return_dups)

    def add_parameter_definition_tags(self, *items, strict=False, return_dups=False):
        return self.add_items("parameter_definition_tag", *items, strict=strict, return_dups=return_dups)

    def _add_features(self, *items):
        return self._add_items("feature", *items)

    def _add_tools(self, *items):
        return self._add_items("tool", *items)

    def _add_tool_features(self, *items):
        return self._add_items("tool_feature", *items)

    def _add_tool_feature_methods(self, *items):
        return self._add_items("tool_feature_method", *items)

    def _add_alternatives(self, *items):
        return self._add_items("alternative", *items)

    def _add_scenarios(self, *items):
        return self._add_items("scenario", *items)

    def _add_scenario_alternatives(self, *items):
        return self._add_items("scenario_alternative", *items)

    def _add_entity_groups(self, *items):
        return self._add_items("entity_group", *items)

    def _add_parameter_tags(self, *items):
        return self._add_items("parameter_tag", *items)

    def _add_parameter_definition_tags(self, *items):
        return self._add_items("parameter_definition_tag", *items)

    def _add_metadata(self, *items):
        return self._add_items("metadata", *items)

    def _add_parameter_value_metadata(self, *items):
        return self._add_items("parameter_value_metadata", *items)

    def _add_entity_metadata(self, *items):
        return self._add_items("entity_metadata", *items)

    def _do_add_features(self, *items_to_add):
        self._do_add_items("feature", *items_to_add)

    def _do_add_tools(self, *items_to_add):
        self._do_add_items("tool", *items_to_add)

    def _do_add_tool_features(self, *items_to_add):
        self._do_add_items("tool_feature", *items_to_add)

    def _do_add_tool_feature_methods(self, *items_to_add):
        self._do_add_items("tool_feature_method", *items_to_add)

    def _do_add_alternatives(self, *items_to_add):
        self._do_add_items("alternative", *items_to_add)

    def _do_add_scenarios(self, *items_to_add):
        self._do_add_items("scenario", *items_to_add)

    def _do_add_scenario_alternatives(self, *items_to_add):
        self._do_add_items("scenario_alternative", *items_to_add)

    def _do_add_entity_groups(self, *items_to_add):
        self._do_add_items("entity_group", *items_to_add)

    def _do_add_parameter_tags(self, *items_to_add):
        self._do_add_items("parameter_tag", *items_to_add)

    def _do_add_parameter_definition_tags(self, *items_to_add):
        self._do_add_items("parameter_definition_tag", *items_to_add)

    def _do_add_metadata(self, *items_to_add):
        self._do_add_items("metadata", *items_to_add)

    def _do_add_parameter_value_metadata(self, *items_to_add):
        self._do_add_items("parameter_value_metadata", *items_to_add)

    def _do_add_entity_metadata(self, *items_to_add):
        self._do_add_items("entity_metadata", *items_to_add)

    def readd_features(self, *items):
        return self.readd_items("feature", *items)

    def readd_tools(self, *items):
        return self.readd_items("tool", *items)

    def readd_tool_features(self, *items):
        return self.readd_items("tool_feature", *items)

    def readd_tool_feature_methods(self, *items):
        return self.readd_items("tool_feature_method", *items)

    def readd_alternatives(self, *items):
        return self.readd_items("alternative", *items)

    def readd_scenarios(self, *items):
        return self.readd_items("scenario", *items)

    def readd_scenario_alternatives(self, *items):
        return self.readd_items("scenario_alternative", *items)

    def readd_entity_groups(self, *items):
        return self.readd_items("entity_group", *items)

    def readd_parameter_tags(self, *items):
        return self.readd_items("parameter_tag", *items)

    def readd_parameter_definition_tags(self, *items):
        return self.readd_items("parameter_definition_tag", *items)

    def add_object_classes(self, *items, strict=False, return_dups=False):
        checked_items, intgr_error_log = self.check_object_classes_for_insert(*items, strict=strict)
        ids = self._add_object_classes(*checked_items)
        if return_dups:
            ids.update(set(x.id for x in intgr_error_log if x.id))
        return ids, intgr_error_log

    def _add_object_classes(self, *items):
        items_to_add, ids = self._items_and_ids("object_class", *items)
        self._do_add_object_classes(*items_to_add)
        self.added_item_id["entity_class"].update(ids)
        self.added_item_id["object_class"].update(ids)
        return ids

    def _do_add_object_classes(self, *items_to_add):
        oc_items_to_add = list()
        append_oc_items_to_add = oc_items_to_add.append
        for item in items_to_add:
            item["type_id"] = self.object_class_type
            append_oc_items_to_add({"entity_class_id": item["id"], "type_id": self.object_class_type})
        try:
            self._checked_execute(self._diff_table("entity_class").insert(), items_to_add)
            self._checked_execute(self._diff_table("object_class").insert(), oc_items_to_add)
        except DBAPIError as e:
            msg = "DBAPIError while inserting object classes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def readd_object_classes(self, *items):
        self._do_add_object_classes(*items)
        ids = set(x["id"] for x in items)
        self.added_item_id["entity_class"].update(ids)
        self.added_item_id["object_class"].update(ids)
        return ids, []

    def add_objects(self, *items, strict=False, return_dups=False):
        checked_items, intgr_error_log = self.check_objects_for_insert(*items, strict=strict)
        ids = self._add_objects(*checked_items)
        if return_dups:
            ids.update(set(x.id for x in intgr_error_log if x.id))
        return ids, intgr_error_log

    def _add_objects(self, *items):
        items_to_add, ids = self._items_and_ids("object", *items)
        self._do_add_objects(*items_to_add)
        self.added_item_id["entity"].update(ids)
        self.added_item_id["object"].update(ids)
        return ids

    def _do_add_objects(self, *items_to_add):
        o_items_to_add = list()
        append_o_items_to_add = o_items_to_add.append
        for item in items_to_add:
            item["type_id"] = self.object_entity_type
            append_o_items_to_add({"entity_id": item["id"], "type_id": item["type_id"]})
        try:
            self._checked_execute(self._diff_table("entity").insert(), items_to_add)
            self._checked_execute(self._diff_table("object").insert(), o_items_to_add)
        except DBAPIError as e:
            msg = "DBAPIError while inserting objects: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def readd_objects(self, *items):
        self._do_add_objects(*items)
        ids = set(x["id"] for x in items)
        self.added_item_id["entity"].update(ids)
        self.added_item_id["object"].update(ids)
        return ids, []

    def add_wide_relationship_classes(self, *wide_items, strict=False, return_dups=False):
        checked_wide_items, intgr_error_log = self.check_wide_relationship_classes_for_insert(
            *wide_items, strict=strict
        )
        ids = self._add_wide_relationship_classes(*checked_wide_items)
        if return_dups:
            ids.update(set(x.id for x in intgr_error_log if x.id))
        return ids, intgr_error_log

    def _add_wide_relationship_classes(self, *wide_items):
        wide_items_to_add, ids = self._items_and_ids("relationship_class", *wide_items)
        self._do_add_wide_relationship_classes(*wide_items_to_add)
        self.added_item_id["entity_class"].update(ids)
        self.added_item_id["relationship_class"].update(ids)
        self.added_item_id["relationship_entity_class"].update(ids)
        return ids

    def _do_add_wide_relationship_classes(self, *wide_items_to_add):
        rc_items_to_add = list()
        rec_items_to_add = list()
        for wide_item in wide_items_to_add:
            wide_item["type_id"] = self.relationship_class_type
            rc_items_to_add.append({"entity_class_id": wide_item["id"], "type_id": self.relationship_class_type})
            rec_items_to_add += get_relationship_entity_class_items(wide_item, self.object_class_type)
        try:
            self._checked_execute(self._diff_table("entity_class").insert(), wide_items_to_add)
            self._checked_execute(self._diff_table("relationship_class").insert(), rc_items_to_add)
            self._checked_execute(self._diff_table("relationship_entity_class").insert(), rec_items_to_add)
        except DBAPIError as e:
            msg = "DBAPIError while inserting relationship classes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def readd_wide_relationship_classes(self, *items):
        self._do_add_wide_relationship_classes(*items)
        ids = set(x["id"] for x in items)
        self.added_item_id["entity_class"].update(ids)
        self.added_item_id["relationship_class"].update(ids)
        self.added_item_id["relationship_entity_class"].update(ids)
        return ids, []

    def add_wide_relationships(self, *wide_items, strict=False, return_dups=False):
        checked_wide_items, intgr_error_log = self.check_wide_relationships_for_insert(*wide_items, strict=strict)
        ids = self._add_wide_relationships(*checked_wide_items)
        if return_dups:
            ids.update(set(x.id for x in intgr_error_log if x.id))
        return ids, intgr_error_log

    def _add_wide_relationships(self, *wide_items):
        wide_items_to_add, ids = self._items_and_ids("relationship", *wide_items)
        self._do_add_wide_relationships(*wide_items_to_add)
        self.added_item_id["entity"].update(ids)
        self.added_item_id["relationship"].update(ids)
        self.added_item_id["relationship_entity"].update(ids)
        return ids

    def _do_add_wide_relationships(self, *wide_items_to_add):
        re_items_to_add = list()
        r_items_to_add = list()
        for wide_item in wide_items_to_add:
            wide_item["type_id"] = self.relationship_entity_type
            r_items_to_add.append(
                {
                    "entity_id": wide_item["id"],
                    "entity_class_id": wide_item["class_id"],
                    "type_id": self.relationship_entity_type,
                }
            )
            re_items_to_add += get_relationship_entity_items(
                wide_item, self.relationship_entity_type, self.object_entity_type
            )
        try:
            self._checked_execute(self._diff_table("entity").insert(), wide_items_to_add)
            self._checked_execute(self._diff_table("relationship").insert(), r_items_to_add)
            self._checked_execute(self._diff_table("relationship_entity").insert(), re_items_to_add)
        except DBAPIError as e:
            msg = "DBAPIError while inserting relationships: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def readd_wide_relationships(self, *items):
        """Add known relationships to database.
        """
        self._do_add_wide_relationships(*items)
        ids = set(x["id"] for x in items)
        self.added_item_id["entity"].update(ids)
        self.added_item_id["relationship"].update(ids)
        self.added_item_id["relationship_entity"].update(ids)
        return ids, []

    def add_parameter_definitions(self, *items, strict=False, return_dups=False):
        checked_items, intgr_error_log = self.check_parameter_definitions_for_insert(*items, strict=strict)
        ids = self._add_parameter_definitions(*checked_items)
        if return_dups:
            ids.update(set(x.id for x in intgr_error_log if x.id))
        return ids, intgr_error_log

    def _add_parameter_definitions(self, *items):
        items_to_add, ids = self._items_and_ids("parameter_definition", *items)
        self._do_add_parameter_definitions(*items_to_add)
        self.added_item_id["parameter_definition"].update(ids)
        return ids

    def _do_add_parameter_definitions(self, *items_to_add):
        for item in items_to_add:
            item["entity_class_id"] = (
                item.pop("object_class_id", None)
                or item.pop("relationship_class_id", None)
                or item.get("entity_class_id")
            )
        try:
            self._checked_execute(self._diff_table("parameter_definition").insert(), items_to_add)
        except DBAPIError as e:
            msg = "DBAPIError while inserting parameters: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def readd_parameter_definitions(self, *items):
        """Add known parameter definitions to database.
        """
        self._do_add_parameter_definitions(*items)
        ids = set(x["id"] for x in items)
        self.added_item_id["parameter_definition"].update(ids)
        return ids, []

    def add_parameter_values(self, *items, strict=False, return_dups=False):
        checked_items, intgr_error_log = self.check_parameter_values_for_insert(*items, strict=strict)
        ids = self._add_parameter_values(*checked_items)
        if return_dups:
            ids.update(set(x.id for x in intgr_error_log if x.id))
        return ids, intgr_error_log

    def add_checked_parameter_values(self, *checked_items):
        ids = self._add_parameter_values(*checked_items)
        return ids, []

    def _add_parameter_values(self, *items):
        items_to_add, ids = self._items_and_ids("parameter_value", *items)
        self._do_add_parameter_values(*items_to_add)
        self.added_item_id["parameter_value"].update(ids)
        return ids

    def _do_add_parameter_values(self, *items_to_add):
        for item in items_to_add:
            item["entity_id"] = (
                item.pop("object_id", None) or item.pop("relationship_id", None) or item.get("entity_id")
            )
            item["entity_class_id"] = (
                item.pop("object_class_id", None)
                or item.pop("relationship_class_id", None)
                or item.get("entity_class_id")
            )
        try:
            self._checked_execute(self._diff_table("parameter_value").insert(), items_to_add)
        except DBAPIError as e:
            msg = "DBAPIError while inserting parameter values: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def readd_parameter_values(self, *items):
        """Add known parameter values to database.
        """
        self._do_add_parameter_values(*items)
        ids = set(x["id"] for x in items)
        self.added_item_id["parameter_value"].update(ids)
        return ids, []

    def add_wide_parameter_value_lists(self, *wide_items, strict=False, return_dups=False):
        checked_wide_items, intgr_error_log = self.check_wide_parameter_value_lists_for_insert(
            *wide_items, strict=strict
        )
        ids = self._add_wide_parameter_value_lists(*checked_wide_items)
        if return_dups:
            ids.update(set(x.id for x in intgr_error_log if x.id))
        return ids, intgr_error_log

    def _add_wide_parameter_value_lists(self, *wide_items):
        wide_items_to_add, ids = self._items_and_ids("parameter_value_list", *wide_items)
        self._do_add_wide_parameter_value_lists(*wide_items_to_add)
        self.added_item_id["parameter_value_list"].update(ids)
        return ids

    def _do_add_wide_parameter_value_lists(self, *wide_items_to_add):
        items_to_add = list()
        for wide_item in wide_items_to_add:
            items_to_add += get_parameter_value_list_items(wide_item)
        try:
            self._checked_execute(self._diff_table("parameter_value_list").insert(), items_to_add)
        except DBAPIError as e:
            msg = "DBAPIError while inserting parameter value lists: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def readd_wide_parameter_value_lists(self, *wide_items):
        self._do_add_wide_parameter_value_lists(*wide_items)
        ids = set(x["id"] for x in wide_items)
        self.added_item_id["parameter_value_list"].update(ids)
        return ids, []

    def add_object_class(self, **kwargs):
        """Stage an object class item for insertion.

        :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.

        :returns:
            - **new_item** -- The item succesfully staged for insertion.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        sq = self.object_class_sq
        ids, _ = self.add_object_classes(kwargs, strict=True)
        return self.query(sq).filter(sq.c.id.in_(ids)).one_or_none()

    def add_object(self, **kwargs):
        """Stage an object item for insertion.

        :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.

        :returns:
            - **new_item** -- The item succesfully staged for insertion.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        sq = self.object_sq
        ids, _ = self.add_objects(kwargs, strict=True)
        return self.query(sq).filter(sq.c.id.in_(ids)).one_or_none()

    def add_wide_relationship_class(self, **kwargs):
        """Stage a relationship class item for insertion.

        :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.

        :returns:
            - **new_item** -- The item succesfully staged for insertion.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        sq = self.wide_relationship_class_sq
        ids, _ = self.add_wide_relationship_classes(kwargs, strict=True)
        return self.query(sq).filter(sq.c.id.in_(ids)).one_or_none()

    def add_wide_relationship(self, **kwargs):
        """Stage a relationship item for insertion.

        :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.

        :returns:
            - **new_item** -- The item succesfully staged for insertion.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        sq = self.wide_relationship_sq
        ids, _ = self.add_wide_relationships(kwargs, strict=True)
        return self.query(sq).filter(sq.c.id.in_(ids)).one_or_none()

    def add_parameter_definition(self, **kwargs):
        """Stage a parameter definition item for insertion.

        :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.

        :returns:
            - **new_item** -- The item succesfully staged for insertion.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        sq = self.parameter_definition_sq
        ids, _ = self.add_parameter_definitions(kwargs, strict=True)
        return self.query(sq).filter(sq.c.id.in_(ids)).one_or_none()

    def add_parameter_value(self, **kwargs):
        """Stage a parameter value item for insertion.

        :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.

        :returns:
            - **new_item** -- The item succesfully staged for insertion.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        sq = self.parameter_value_sq
        ids, _ = self.add_parameter_values(kwargs, strict=True)
        return self.query(sq).filter(sq.c.id.in_(ids)).one_or_none()

    def get_or_add_object_class(self, **kwargs):
        """Stage an object class item for insertion if it doesn't already exists in the db.

        :returns:
            - **item** -- The item succesfully staged for insertion or already existing.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        sq = self.object_class_sq
        ids, _ = self.add_object_classes(kwargs, return_dups=True)
        return self.query(sq).filter(sq.c.id.in_(ids)).one_or_none()

    def get_or_add_object(self, **kwargs):
        """Stage an object item for insertion if it doesn't already exists in the db.

        :returns:
            - **item** -- The item succesfully staged for insertion or already existing.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        sq = self.object_sq
        ids, _ = self.add_objects(kwargs, return_dups=True)
        return self.query(sq).filter(sq.c.id.in_(ids)).one_or_none()

    def get_or_add_parameter_definition(self, **kwargs):
        """Stage a parameter definition item for insertion if it doesn't already exists in the db.

        :returns:
            - **item** -- The item succesfully staged for insertion or already existing.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        sq = self.parameter_definition_sq
        ids, _ = self.add_parameter_definitions(kwargs, return_dups=True)
        return self.query(sq).filter(sq.c.id.in_(ids)).one_or_none()
