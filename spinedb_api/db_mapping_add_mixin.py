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
from contextlib import contextmanager
from sqlalchemy import func, Table, Column, Integer, String, null, select
from sqlalchemy.exc import DBAPIError
from .exception import SpineDBAPIError


class DatabaseMappingAddMixin:
    """Provides methods to perform ``INSERT`` operations over a Spine db."""

    class _IdGenerator:
        def __init__(self, next_id):
            self._next_id = next_id

        @property
        def next_id(self):
            return self._next_id

        def __call__(self):
            try:
                return self._next_id
            finally:
                self._next_id += 1

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
                Column("metadata_id", Integer, server_default=null()),
                Column("parameter_value_metadata_id", Integer, server_default=null()),
                Column("entity_metadata_id", Integer, server_default=null()),
            )
            try:
                self._next_id.create(self.connection)
            except DBAPIError:
                # Some other concurrent process must have beaten us to create the table
                self._next_id = Table("next_id", self._metadata, autoload=True)

    @contextmanager
    def generate_ids(self, tablename):
        """Manages id generation for new items to be added to the db.

        Args:
            tablename (str): the table to which items will be added

        Yields:
            self._IdGenerator: an object that generates a new id every time it is called.
        """
        fieldname = {
            "entity_class": "entity_class_id",
            "object_class": "entity_class_id",
            "relationship_class": "entity_class_id",
            "entity": "entity_id",
            "object": "entity_id",
            "relationship": "entity_id",
            "entity_group": "entity_group_id",
            "parameter_definition": "parameter_definition_id",
            "parameter_value": "parameter_value_id",
            "parameter_value_list": "parameter_value_list_id",
            "list_value": "list_value_id",
            "alternative": "alternative_id",
            "scenario": "scenario_id",
            "scenario_alternative": "scenario_alternative_id",
            "metadata": "metadata_id",
            "parameter_value_metadata": "parameter_value_metadata_id",
            "entity_metadata": "entity_metadata_id",
        }[tablename]
        with self.engine.begin() as connection:
            select_next_id = select([self._next_id])
            next_id_row = connection.execute(select_next_id).first()
            if next_id_row is None:
                next_id = None
                stmt = self._next_id.insert()
            else:
                next_id = getattr(next_id_row, fieldname)
                stmt = self._next_id.update()
            if next_id is None:
                real_tablename = self._real_tablename(tablename)
                table = self._metadata.tables[real_tablename]
                id_field = self._id_fields.get(real_tablename, "id")
                select_max_id = select([func.max(getattr(table.c, id_field))])
                max_id = connection.execute(select_max_id).scalar()
                next_id = max_id + 1 if max_id else 1
            gen = self._IdGenerator(next_id)
            try:
                yield gen
            finally:
                connection.execute(stmt, {"user": self.username, "date": datetime.utcnow(), fieldname: gen.next_id})

    def _add_commit_id_and_ids(self, tablename, *items):
        if not items:
            return [], set()
        commit_id = self._make_commit_id()
        with self.generate_ids(tablename) as new_id:
            for item in items:
                item["commit_id"] = commit_id
                if "id" not in item:
                    item["id"] = new_id()

    def add_items(self, tablename, *items, check=True, strict=False):
        """Add items to cache.

        Args:
            tablename (str)
            items (Iterable): One or more Python :class:`dict` objects representing the items to be inserted.
            check (bool): Whether or not to check integrity
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if the insertion of one of the items violates an integrity constraint.

        Returns:
            set: ids or items successfully added
            list(str): found violations
        """
        if check:
            checked_items, errors = self.check_items(tablename, *items)
        else:
            checked_items, errors = list(items), []
        if errors and strict:
            raise SpineDBAPIError(", ".join(errors))
        _ = self._add_items(tablename, *checked_items)
        return checked_items, errors

    def _add_items(self, tablename, *items):
        """Add items to cache without checking integrity.

        Args:
            tablename (str)
            items (Iterable): list of dictionaries which correspond to the instances to add
            strict (bool): if True SpineIntegrityError are raised. Otherwise
                they are caught and returned as a log

        Returns:
            ids (set): added instances' ids
        """
        self._add_commit_id_and_ids(tablename, *items)
        tablename = self._real_tablename(tablename)
        table_cache = self.cache.table_cache(tablename)
        for item in items:
            table_cache.add_item(item, new=True)
        return {item["id"] for item in items}

    def _do_add_items(self, tablename, *items_to_add):
        """Add items to DB without checking integrity."""
        try:
            for tablename_, items_to_add_ in self._items_to_add_per_table(tablename, items_to_add):
                table = self._metadata.tables[self._real_tablename(tablename_)]
                self._checked_execute(table.insert(), [{**item} for item in items_to_add_])
        except DBAPIError as e:
            msg = f"DBAPIError while inserting {tablename} items: {e.orig.args}"
            raise SpineDBAPIError(msg) from e

    @staticmethod
    def _items_to_add_per_table(tablename, items_to_add):
        """
        Yields tuples of string tablename, list of items to insert. Needed because some insert queries
        actually need to insert records to more than one table.

        Args:
            tablename (str): target database table name
            items_to_add (list): items to add

        Yields:
            tuple: database table name, items to add
        """
        if tablename == "entity_class":
            ecd_items_to_add = [
                {"entity_class_id": item["id"], "position": position, "dimension_id": dimension_id}
                for item in items_to_add
                for position, dimension_id in enumerate(item["dimension_id_list"])
            ]
            yield ("entity_class", items_to_add)
            yield ("entity_class_dimension", ecd_items_to_add)
        elif tablename == "entity":
            ee_items_to_add = [
                {
                    "entity_id": item["id"],
                    "entity_class_id": item["class_id"],
                    "position": position,
                    "element_id": element_id,
                    "dimension_id": dimension_id,
                }
                for item in items_to_add
                for position, (element_id, dimension_id) in enumerate(
                    zip(item["element_id_list"], item["dimension_id_list"])
                )
            ]
            ea_items_to_add = [
                {"entity_id": item["id"], "alternative_id": alternative_id, "active": True}
                for item in items_to_add
                for alternative_id in item["active_alternative_id_list"]
            ] + [
                {"entity_id": item["id"], "alternative_id": alternative_id, "active": False}
                for item in items_to_add
                for alternative_id in item["inactive_alternative_id_list"]
            ]
            yield ("entity", items_to_add)
            yield ("entity_element", ee_items_to_add)
            yield ("entity_alternative", ea_items_to_add)
        elif tablename == "relationship_class":
            ecd_items_to_add = [
                {"entity_class_id": item["id"], "position": position, "dimension_id": dimension_id}
                for item in items_to_add
                for position, dimension_id in enumerate(item["object_class_id_list"])
            ]
            yield ("entity_class", items_to_add)
            yield ("entity_class_dimension", ecd_items_to_add)
        elif tablename == "object":
            ea_items_to_add = [
                {"entity_id": item["id"], "alternative_id": alternative_id, "active": True}
                for item in items_to_add
                for alternative_id in item["active_alternative_id_list"]
            ] + [
                {"entity_id": item["id"], "alternative_id": alternative_id, "active": False}
                for item in items_to_add
                for alternative_id in item["inactive_alternative_id_list"]
            ]
            yield ("entity", items_to_add)
            yield ("entity_alternative", ea_items_to_add)
        elif tablename == "relationship":
            ee_items_to_add = [
                {
                    "entity_id": item["id"],
                    "entity_class_id": item["class_id"],
                    "position": position,
                    "element_id": element_id,
                    "dimension_id": dimension_id,
                }
                for item in items_to_add
                for position, (element_id, dimension_id) in enumerate(
                    zip(item["object_id_list"], item["object_class_id_list"])
                )
            ]
            ea_items_to_add = [
                {"entity_id": item["id"], "alternative_id": alternative_id, "active": True}
                for item in items_to_add
                for alternative_id in item["active_alternative_id_list"]
            ] + [
                {"entity_id": item["id"], "alternative_id": alternative_id, "active": False}
                for item in items_to_add
                for alternative_id in item["inactive_alternative_id_list"]
            ]
            yield ("entity", items_to_add)
            yield ("entity_element", ee_items_to_add)
            yield ("entity_alternative", ea_items_to_add)
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

    def add_entity_classes(self, *items, **kwargs):
        return self.add_items("entity_class", *items, **kwargs)

    def add_entities(self, *items, **kwargs):
        return self.add_items("entity", *items, **kwargs)

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

    def _get_or_add_metadata_ids_for_items(self, *items, check, strict):
        cache = self.cache
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
        added_metadata, errors = self.add_items("metadata", *metadata_to_add, check=check, strict=strict)
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

    def _add_ext_item_metadata(self, table_name, *items, check=True, strict=False):
        self.fetch_all({table_name}, include_ancestors=True)
        added_metadata, metadata_errors = self._get_or_add_metadata_ids_for_items(*items, check=check, strict=strict)
        if metadata_errors:
            return added_metadata, metadata_errors
        added_item_metadata, item_errors = self.add_items(table_name, *items, check=check, strict=strict)
        errors = metadata_errors + item_errors
        return added_metadata + added_item_metadata, errors

    def add_ext_entity_metadata(self, *items, check=True, strict=False):
        return self._add_ext_item_metadata("entity_metadata", *items, check=check, strict=strict)

    def add_ext_parameter_value_metadata(self, *items, check=True, strict=False):
        return self._add_ext_item_metadata("parameter_value_metadata", *items, check=check, strict=strict)
