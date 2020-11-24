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

"""Provides :class:`.DatabaseMappingAddMixin`.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""
# TODO: improve docstrings

from datetime import datetime
from sqlalchemy import func, Table, Column, Integer, String, null
from sqlalchemy.exc import DBAPIError
from .exception import SpineDBAPIError
from .helpers import get_relationship_entity_class_items, get_relationship_entity_items, get_parameter_value_list_items


class DatabaseMappingAddMixin:
    """Provides methods to perform ``INSERT`` operations over a Spine db.
    """

    def __init__(self, *args, **kwargs):
        """Initialize class."""
        super().__init__(*args, **kwargs)
        next_id = self._metadata.tables.get("next_id")
        if next_id is None:
            next_id = Table(
                "next_id",
                self._metadata,
                Column("user", String(155), primary_key=True),
                Column("date", String(155), primary_key=True),
                Column("entity_id", Integer, server_default=null()),
                Column("entity_class_id", Integer, server_default=null()),
                Column("entity_group_id", Integer, server_default=null()),
                Column("parameter_definition_id", Integer, server_default=null()),
                Column("parameter_value_id", Integer, server_default=null()),
                Column("parameter_tag_id", Integer, server_default=null()),
                Column("parameter_value_list_id", Integer, server_default=null()),
                Column("parameter_definition_tag_id", Integer, server_default=null()),
                Column("alternative_id", Integer, server_default=null()),
                Column("scenario_id", Integer, server_default=null()),
                Column("scenario_alternative_id", Integer, server_default=null()),
                Column("tool_id", Integer, server_default=null()),
                Column("feature_id", Integer, server_default=null()),
                Column("tool_feature_id", Integer, server_default=null()),
                Column("tool_feature_method_id", Integer, server_default=null()),
                Column("metadata_id", Integer, server_default=null()),
                Column("parameter_value_metadata_id", Integer, server_default=null()),
                Column("entity_metadata_id", Integer, server_default=null()),
            )
            next_id.create(self.connection, checkfirst=True)
        self._next_id = next_id

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
        with self.connection.begin():
            next_id_row = self.query(self._next_id).one_or_none()
            if next_id_row is None:
                next_id = None
                stmt = self._next_id.insert()
            else:
                next_id = getattr(next_id_row, fieldname)
                stmt = self._next_id.update()
            if next_id is None:
                table = self._metadata.tables[tablename]
                id_col = self.table_ids.get(tablename, "id")
                max_id = self.query(func.max(getattr(table.c, id_col))).scalar()
                next_id = max_id + 1 if max_id else 1
            new_next_id = next_id + len(items)
            self.connection.execute(stmt, {"user": self.username, "date": datetime.utcnow(), fieldname: new_next_id})
        ids = list(range(next_id, new_next_id))
        items_to_add = list()
        append_item = items_to_add.append
        for id_, item in zip(ids, items):
            item["commit_id"] = self.make_commit_id()
            item["id"] = id_
            append_item(item)
        return items_to_add, set(ids)

    def add_items(self, tablename, *items, strict=False, return_dups=False):
        """Add items to db.

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
        """Add items to database without checking integrity.

        Args:
            tablename (str)
            items (Iterable): list of dictionaries which correspond to the instances to add
            strict (bool): if True SpineIntegrityError are raised. Otherwise
                they are catched and returned as a log

        Returns:
            ids (set): added instances' ids
        """
        items_to_add, ids = self._items_and_ids(tablename, *items)
        for _ in self._do_add_items(tablename, *items_to_add):
            pass
        return ids

    def _get_table_for_insert(self, tablename):
        """
        Returns the Table object to perform the insertion. Subclasses can override this method to insert
        to another table instead (e.g., diff...)

        Args:
            tablename (str)
        Returns:
            Table
        """
        return self._metadata.tables[tablename]

    def _do_add_items(self, tablename, *items_to_add):
        try:
            for tablename_, items_to_add_ in self._items_to_add_per_table(tablename, items_to_add):
                table = self._get_table_for_insert(tablename_)
                self._checked_execute(table.insert(), items_to_add_)
                yield tablename_
        except DBAPIError as e:
            msg = f"DBAPIError while inserting {tablename} items: {e.orig.args}"
            raise SpineDBAPIError(msg)

    def readd_items(self, tablename, *items):
        """Add known items to database."""
        ids = set(x["id"] for x in items)
        self._do_add_items(tablename, *items)
        return ids, []

    def _items_to_add_per_table(self, tablename, items_to_add):
        """
        Yields tuples of string tablename, list of items to insert. Needed because some insert queries
        actually need to insert records to more than one table.

        Args:
            tablename (str):
            items_to_add (list)

        Returns:
            Generator
        """
        if tablename == "object_class":
            oc_items_to_add = list()
            append_oc_items_to_add = oc_items_to_add.append
            for item in items_to_add:
                item["type_id"] = self.object_class_type
                append_oc_items_to_add({"entity_class_id": item["id"], "type_id": self.object_class_type})
            yield ("entity_class", items_to_add)
            yield ("object_class", oc_items_to_add)
        elif tablename == "object":
            o_items_to_add = list()
            append_o_items_to_add = o_items_to_add.append
            for item in items_to_add:
                item["type_id"] = self.object_entity_type
                append_o_items_to_add({"entity_id": item["id"], "type_id": item["type_id"]})
            yield ("entity", items_to_add)
            yield ("object", o_items_to_add)
        elif tablename == "relationship_class":
            rc_items_to_add = list()
            rec_items_to_add = list()
            for item in items_to_add:
                item["type_id"] = self.relationship_class_type
                rc_items_to_add.append({"entity_class_id": item["id"], "type_id": self.relationship_class_type})
                rec_items_to_add += get_relationship_entity_class_items(item, self.object_class_type)
            yield ("entity_class", items_to_add)
            yield ("relationship_class", rc_items_to_add)
            yield ("relationship_entity_class", rec_items_to_add)
        elif tablename == "relationship":
            re_items_to_add = list()
            r_items_to_add = list()
            for item in items_to_add:
                item["type_id"] = self.relationship_entity_type
                r_items_to_add.append(
                    {
                        "entity_id": item["id"],
                        "entity_class_id": item["class_id"],
                        "type_id": self.relationship_entity_type,
                    }
                )
                re_items_to_add += get_relationship_entity_items(
                    item, self.relationship_entity_type, self.object_entity_type
                )
            yield ("entity", items_to_add)
            yield ("relationship", r_items_to_add)
            yield ("relationship_entity", re_items_to_add)
        elif tablename == "parameter_definition":
            for item in items_to_add:
                item["entity_class_id"] = (
                    item.pop("object_class_id", None)
                    or item.pop("relationship_class_id", None)
                    or item.get("entity_class_id")
                )
            yield ("parameter_definition", items_to_add)
        elif tablename == "parameter_value":
            for item in items_to_add:
                item["entity_id"] = (
                    item.pop("object_id", None) or item.pop("relationship_id", None) or item.get("entity_id")
                )
                item["entity_class_id"] = (
                    item.pop("object_class_id", None)
                    or item.pop("relationship_class_id", None)
                    or item.get("entity_class_id")
                )
            yield ("parameter_value", items_to_add)
        elif tablename == "parameter_value_list":
            items_to_add_ = list()
            for item in items_to_add:
                items_to_add_ += get_parameter_value_list_items(item)
            yield ("parameter_value_list", items_to_add_)
        else:
            yield (tablename, items_to_add)

    def add_object_classes(self, *items, strict=False, return_dups=False):
        return self.add_items("object_class", *items, strict=strict, return_dups=return_dups)

    def add_objects(self, *items, strict=False, return_dups=False):
        return self.add_items("object", *items, strict=strict, return_dups=return_dups)

    def add_wide_relationship_classes(self, *items, strict=False, return_dups=False):
        return self.add_items("relationship_class", *items, strict=strict, return_dups=return_dups)

    def add_wide_relationships(self, *items, strict=False, return_dups=False):
        return self.add_items("relationship", *items, strict=strict, return_dups=return_dups)

    def add_parameter_definitions(self, *items, strict=False, return_dups=False):
        return self.add_items("parameter_definition", *items, strict=strict, return_dups=return_dups)

    def add_parameter_values(self, *items, strict=False, return_dups=False):
        return self.add_items("parameter_value", *items, strict=strict, return_dups=return_dups)

    def add_checked_parameter_values(self, *checked_items):
        ids = self._add_parameter_values(*checked_items)
        return ids, []

    def add_wide_parameter_value_lists(self, *items, strict=False, return_dups=False):
        return self.add_items("parameter_value_list", *items, strict=strict, return_dups=return_dups)

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

    def _add_object_classes(self, *items):
        return self._add_items("object_class", *items)

    def _add_objects(self, *items):
        return self._add_items("object", *items)

    def _add_wide_relationship_classes(self, *items):
        return self._add_items("relationship_class", *items)

    def _add_wide_relationships(self, *items):
        return self._add_items("relationship", *items)

    def _add_parameter_definitions(self, *items):
        return self._add_items("parameter_definition", *items)

    def _add_parameter_values(self, *items):
        return self._add_items("parameter_value", *items)

    def _add_wide_parameter_value_lists(self, *items):
        return self._add_items("parameter_value_list", *items)

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

    def readd_object_classes(self, *items):
        return self.readd_items("object_class", *items)

    def readd_objects(self, *items):
        return self.readd_items("object", *items)

    def readd_wide_relationship_classes(self, *items):
        return self.readd_items("relationship_class", *items)

    def readd_wide_relationships(self, *items):
        return self.readd_items("relationship", *items)

    def readd_parameter_definitions(self, *items):
        return self.readd_items("parameter_definition", *items)

    def readd_parameter_values(self, *items):
        return self.readd_items("parameter_value", *items)

    def readd_wide_parameter_value_lists(self, *items):
        return self.readd_items("parameter_value_list", *items)

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
