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

"""Provides :class:`.DatabaseMappingAddMixin`.

"""
# TODO: improve docstrings

from datetime import datetime
from sqlalchemy import func, Table, Column, Integer, String, null, select
from sqlalchemy.exc import DBAPIError
from .exception import SpineDBAPIError, SpineIntegrityError
from .helpers import get_relationship_entity_class_items, get_relationship_entity_items


class DatabaseMappingAddMixin:
    """Provides methods to perform ``INSERT`` operations over a Spine db."""

    def __init__(self, *args, **kwargs):
        """Initialize class."""
        super().__init__(*args, **kwargs)
        self._next_id = self._metadata.tables.get("next_id")
        if self._next_id is None:
            self._next_id = Table(
                "next_id",
                self._metadata,
                Column("user", String(155), primary_key=True),
                Column("date", String(155), primary_key=True),
                Column("entity_id", Integer, server_default=null()),
                Column("entity_class_id", Integer, server_default=null()),
                Column("entity_group_id", Integer, server_default=null()),
                Column("parameter_definition_id", Integer, server_default=null()),
                Column("parameter_value_id", Integer, server_default=null()),
                Column("parameter_value_list_id", Integer, server_default=null()),
                Column("list_value_id", Integer, server_default=null()),
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
            try:
                self._next_id.create(self.connection)
            except DBAPIError:
                # Some other concurrent process must have beaten us to create the table
                self._next_id = Table("next_id", self._metadata, autoload=True)

    def _add_commit_id_and_ids(self, tablename, *items):
        if not items:
            return [], set()
        ids = self._reserve_ids(tablename, len(items))
        commit_id = self._make_commit_id()
        for id_, item in zip(ids, items):
            item["commit_id"] = commit_id
            item["id"] = id_

    def _reserve_ids(self, tablename, count):
        if self.committing:
            return self._do_reserve_ids(self.connection, tablename, count)
        with self.engine.begin() as connection:
            return self._do_reserve_ids(connection, tablename, count)

    def _do_reserve_ids(self, connection, tablename, count):
        fieldname = {
            "object_class": "entity_class_id",
            "object": "entity_id",
            "relationship_class": "entity_class_id",
            "relationship": "entity_id",
            "entity_group": "entity_group_id",
            "parameter_definition": "parameter_definition_id",
            "parameter_value": "parameter_value_id",
            "parameter_value_list": "parameter_value_list_id",
            "list_value": "list_value_id",
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
        select_next_id = select([self._next_id])
        next_id_row = connection.execute(select_next_id).first()
        if next_id_row is None:
            next_id = None
            stmt = self._next_id.insert()
        else:
            next_id = getattr(next_id_row, fieldname)
            stmt = self._next_id.update()
        if next_id is None:
            table = self._metadata.tables[tablename]
            id_col = self.table_ids.get(tablename, "id")
            select_max_id = select([func.max(getattr(table.c, id_col))])
            max_id = connection.execute(select_max_id).scalar()
            next_id = max_id + 1 if max_id else 1
        new_next_id = next_id + count
        connection.execute(stmt, {"user": self.username, "date": datetime.utcnow(), fieldname: new_next_id})
        return range(next_id, new_next_id)

    def _readd_items(self, tablename, *items):
        """Add known items to database."""
        self._make_commit_id()
        for _ in self._do_add_items(tablename, *items):
            pass

    def add_items(
        self,
        tablename,
        *items,
        check=True,
        strict=False,
        return_dups=False,
        return_items=False,
        cache=None,
        readd=False,
    ):
        """Add items to db.

        Args:
            tablename (str)
            items (Iterable): One or more Python :class:`dict` objects representing the items to be inserted.
            check (bool): Whether or not to check integrity
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if the insertion of one of the items violates an integrity constraint.
            return_dups (bool): Whether or not already existing and duplicated entries should also be returned.
            return_items (bool): Return full items rather than just ids
            cache (dict, optional): A dict mapping table names to a list of dictionary items, to use as db replacement
                for queries
            readd (bool): Readds items directly

        Returns:
            set: ids or items successfully added
            list(SpineIntegrityError): found violations
        """
        if readd:
            try:
                self._readd_items(tablename, *items)
                return items if return_items else {x["id"] for x in items}, []
            except SpineDBAPIError as e:
                return set(), [e]

        if check:
            checked_items, intgr_error_log = self.check_items(
                tablename, *items, for_update=False, strict=strict, cache=cache
            )
        else:
            checked_items, intgr_error_log = list(items), []
        try:
            ids = self._add_items(tablename, *checked_items)
        except DBAPIError as e:
            intgr_error_log.append(SpineIntegrityError(f"Fail to add items: {e.orig.args}"))
            return set(), intgr_error_log
        if return_items:
            return checked_items, intgr_error_log
        if return_dups:
            ids.update(set(x.id for x in intgr_error_log if x.id))
        return ids, intgr_error_log

    def _add_items(self, tablename, *items):
        """Add items to database without checking integrity.

        Args:
            tablename (str)
            items (Iterable): list of dictionaries which correspond to the instances to add
            strict (bool): if True SpineIntegrityError are raised. Otherwise
                they are caught and returned as a log

        Returns:
            ids (set): added instances' ids
        """
        self._add_commit_id_and_ids(tablename, *items)
        for _ in self._do_add_items(tablename, *items):
            pass
        return {item["id"] for item in items}

    def _get_table_for_insert(self, tablename):
        """
        Returns the table name where to perform insertion.

        Subclasses can override this method to insert to another table instead (e.g., diff...)

        Args:
            tablename (str): target database table name

        Yields:
            str: database table name
        """
        return self._metadata.tables[tablename]

    def _do_add_items(self, tablename, *items_to_add):
        if not self.committing:
            return
        items_to_add = tuple(self._items_with_type_id(tablename, *items_to_add))
        try:
            for tablename_, items_to_add_ in self._items_to_add_per_table(tablename, items_to_add):
                table = self._get_table_for_insert(tablename_)
                self._checked_execute(table.insert(), [{**item} for item in items_to_add_])
                yield tablename_
        except DBAPIError as e:
            msg = f"DBAPIError while inserting {tablename} items: {e.orig.args}"
            raise SpineDBAPIError(msg) from e

    def _items_to_add_per_table(self, tablename, items_to_add):
        """
        Yields tuples of string tablename, list of items to insert. Needed because some insert queries
        actually need to insert records to more than one table.

        Args:
            tablename (str): target database table name
            items_to_add (list): items to add

        Yields:
            tuple: database table name, items to add
        """
        if tablename == "object_class":
            oc_items_to_add = list()
            append_oc_items_to_add = oc_items_to_add.append
            for item in items_to_add:
                append_oc_items_to_add({"entity_class_id": item["id"], "type_id": self.object_class_type})
            yield ("entity_class", items_to_add)
            yield ("object_class", oc_items_to_add)
        elif tablename == "object":
            o_items_to_add = list()
            append_o_items_to_add = o_items_to_add.append
            for item in items_to_add:
                append_o_items_to_add({"entity_id": item["id"], "type_id": item["type_id"]})
            yield ("entity", items_to_add)
            yield ("object", o_items_to_add)
        elif tablename == "relationship_class":
            rc_items_to_add = list()
            rec_items_to_add = list()
            for item in items_to_add:
                rc_items_to_add.append({"entity_class_id": item["id"], "type_id": self.relationship_class_type})
                rec_items_to_add += get_relationship_entity_class_items(item, self.object_class_type)
            yield ("entity_class", items_to_add)
            yield ("relationship_class", rc_items_to_add)
            yield ("relationship_entity_class", rec_items_to_add)
        elif tablename == "relationship":
            re_items_to_add = list()
            r_items_to_add = list()
            for item in items_to_add:
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
                    item.get("object_class_id") or item.get("relationship_class_id") or item.get("entity_class_id")
                )
            yield ("parameter_definition", items_to_add)
        elif tablename == "parameter_value":
            for item in items_to_add:
                item["entity_id"] = item.get("object_id") or item.get("relationship_id") or item.get("entity_id")
                item["entity_class_id"] = (
                    item.get("object_class_id") or item.get("relationship_class_id") or item.get("entity_class_id")
                )
            yield ("parameter_value", items_to_add)
        else:
            yield (tablename, items_to_add)

    def add_object_classes(self, *items, **kwargs):
        return self.add_items("object_class", *items, **kwargs)

    def add_objects(self, *items, **kwargs):
        return self.add_items("object", *items, **kwargs)

    def add_wide_relationship_classes(self, *items, **kwargs):
        return self.add_items("relationship_class", *items, **kwargs)

    def add_wide_relationships(self, *items, **kwargs):
        return self.add_items("relationship", *items, **kwargs)

    def add_parameter_definitions(self, *items, **kwargs):
        return self.add_items("parameter_definition", *items, **kwargs)

    def add_parameter_values(self, *items, **kwargs):
        return self.add_items("parameter_value", *items, **kwargs)

    def add_parameter_value_lists(self, *items, **kwargs):
        return self.add_items("parameter_value_list", *items, **kwargs)

    def add_list_values(self, *items, **kwargs):
        return self.add_items("list_value", *items, **kwargs)

    def add_features(self, *items, **kwargs):
        return self.add_items("feature", *items, **kwargs)

    def add_tools(self, *items, **kwargs):
        return self.add_items("tool", *items, **kwargs)

    def add_tool_features(self, *items, **kwargs):
        return self.add_items("tool_feature", *items, **kwargs)

    def add_tool_feature_methods(self, *items, **kwargs):
        return self.add_items("tool_feature_method", *items, **kwargs)

    def add_alternatives(self, *items, **kwargs):
        return self.add_items("alternative", *items, **kwargs)

    def add_scenarios(self, *items, **kwargs):
        return self.add_items("scenario", *items, **kwargs)

    def add_scenario_alternatives(self, *items, **kwargs):
        return self.add_items("scenario_alternative", *items, **kwargs)

    def add_entity_groups(self, *items, **kwargs):
        return self.add_items("entity_group", *items, **kwargs)

    def add_metadata(self, *items, **kwargs):
        return self.add_items("metadata", *items, **kwargs)

    def add_entity_metadata(self, *items, **kwargs):
        return self.add_items("entity_metadata", *items, **kwargs)

    def add_parameter_value_metadata(self, *items, **kwargs):
        return self.add_items("parameter_value_metadata", *items, **kwargs)

    def _get_or_add_metadata_ids_for_items(self, *items, check, strict, cache):
        metadata_ids = {}
        for entry in cache.get("metadata", {}).values():
            metadata_ids.setdefault(entry.name, {})[entry.value] = entry.id
        metadata_to_add = []
        items_missing_metadata_ids = {}
        for item in items:
            existing_values = metadata_ids.get(item["metadata_name"])
            existing_id = existing_values.get(item["metadata_value"]) if existing_values is not None else None
            if existing_values is None or existing_id is None:
                metadata_to_add.append({"name": item["metadata_name"], "value": item["metadata_value"]})
                items_missing_metadata_ids.setdefault(item["metadata_name"], {})[item["metadata_value"]] = item
            else:
                item["metadata_id"] = existing_id
        added_metadata, errors = self.add_items(
            "metadata", *metadata_to_add, check=check, strict=strict, return_items=True, cache=cache
        )
        for x in added_metadata:
            cache.table_cache("metadata").add_item(x)
        if errors:
            return added_metadata, errors
        new_metadata_ids = {}
        for added in added_metadata:
            new_metadata_ids.setdefault(added["name"], {})[added["value"]] = added["id"]
        for metadata_name, value_to_item in items_missing_metadata_ids.items():
            for metadata_value, item in value_to_item.items():
                item["metadata_id"] = new_metadata_ids[metadata_name][metadata_value]
        return added_metadata, errors

    def _add_ext_item_metadata(self, table_name, *items, check=True, strict=False, return_items=False, cache=None):
        # Note, that even though return_items can be False, it doesn't make much sense here because we'll be mixing
        # metadata and entity metadata ids.
        if cache is None:
            cache = self.make_cache({table_name}, include_ancestors=True)
        added_metadata, metadata_errors = self._get_or_add_metadata_ids_for_items(
            *items, check=check, strict=strict, cache=cache
        )
        if metadata_errors:
            if not return_items:
                return added_metadata, metadata_errors
            return {i["id"] for i in added_metadata}, metadata_errors
        added_item_metadata, item_errors = self.add_items(
            table_name, *items, check=check, strict=strict, return_items=True, cache=cache
        )
        errors = metadata_errors + item_errors
        if not return_items:
            return {i["id"] for i in added_metadata + added_item_metadata}, errors
        return added_metadata + added_item_metadata, errors

    def add_ext_entity_metadata(self, *items, check=True, strict=False, return_items=False, cache=None, readd=False):
        return self._add_ext_item_metadata(
            "entity_metadata", *items, check=check, strict=strict, return_items=return_items, cache=cache
        )

    def add_ext_parameter_value_metadata(
        self, *items, check=True, strict=False, return_items=False, cache=None, readd=False
    ):
        return self._add_ext_item_metadata(
            "parameter_value_metadata", *items, check=check, strict=strict, return_items=return_items, cache=cache
        )

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

    def _add_parameter_value_lists(self, *items):
        return self._add_items("parameter_value_list", *items)

    def _add_list_values(self, *items):
        return self._add_items("list_value", *items)

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

    def _add_metadata(self, *items):
        return self._add_items("metadata", *items)

    def _add_parameter_value_metadata(self, *items):
        return self._add_items("parameter_value_metadata", *items)

    def _add_entity_metadata(self, *items):
        return self._add_items("entity_metadata", *items)

    def add_object_class(self, **kwargs):
        """Stage an object class item for insertion.

        :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.

        :returns:
            - **new_item** -- The item successfully staged for insertion.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        sq = self.object_class_sq
        ids, _ = self.add_object_classes(kwargs, strict=True)
        return self.query(sq).filter(sq.c.id.in_(ids)).one_or_none()

    def add_object(self, **kwargs):
        """Stage an object item for insertion.

        :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.

        :returns:
            - **new_item** -- The item successfully staged for insertion.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        sq = self.object_sq
        ids, _ = self.add_objects(kwargs, strict=True)
        return self.query(sq).filter(sq.c.id.in_(ids)).one_or_none()

    def add_wide_relationship_class(self, **kwargs):
        """Stage a relationship class item for insertion.

        :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.

        :returns:
            - **new_item** -- The item successfully staged for insertion.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        sq = self.wide_relationship_class_sq
        ids, _ = self.add_wide_relationship_classes(kwargs, strict=True)
        return self.query(sq).filter(sq.c.id.in_(ids)).one_or_none()

    def add_wide_relationship(self, **kwargs):
        """Stage a relationship item for insertion.

        :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.

        :returns:
            - **new_item** -- The item successfully staged for insertion.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        sq = self.wide_relationship_sq
        ids, _ = self.add_wide_relationships(kwargs, strict=True)
        return self.query(sq).filter(sq.c.id.in_(ids)).one_or_none()

    def add_parameter_definition(self, **kwargs):
        """Stage a parameter definition item for insertion.

        :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.

        :returns:
            - **new_item** -- The item successfully staged for insertion.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        sq = self.parameter_definition_sq
        ids, _ = self.add_parameter_definitions(kwargs, strict=True)
        return self.query(sq).filter(sq.c.id.in_(ids)).one_or_none()

    def add_parameter_value(self, **kwargs):
        """Stage a parameter value item for insertion.

        :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.

        :returns:
            - **new_item** -- The item successfully staged for insertion.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        sq = self.parameter_value_sq
        ids, _ = self.add_parameter_values(kwargs, strict=True)
        return self.query(sq).filter(sq.c.id.in_(ids)).one_or_none()

    def get_or_add_object_class(self, **kwargs):
        """Stage an object class item for insertion if it doesn't already exists in the db.

        :returns:
            - **item** -- The item successfully staged for insertion or already existing.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        sq = self.object_class_sq
        ids, _ = self.add_object_classes(kwargs, return_dups=True)
        return self.query(sq).filter(sq.c.id.in_(ids)).one_or_none()

    def get_or_add_object(self, **kwargs):
        """Stage an object item for insertion if it doesn't already exists in the db.

        :returns:
            - **item** -- The item successfully staged for insertion or already existing.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        sq = self.object_sq
        ids, _ = self.add_objects(kwargs, return_dups=True)
        return self.query(sq).filter(sq.c.id.in_(ids)).one_or_none()

    def get_or_add_parameter_definition(self, **kwargs):
        """Stage a parameter definition item for insertion if it doesn't already exists in the db.

        :returns:
            - **item** -- The item successfully staged for insertion or already existing.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        sq = self.parameter_definition_sq
        ids, _ = self.add_parameter_definitions(kwargs, return_dups=True)
        return self.query(sq).filter(sq.c.id.in_(ids)).one_or_none()
