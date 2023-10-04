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

# TODO: improve docstrings

from sqlalchemy.exc import DBAPIError
from .exception import SpineIntegrityError, SpineDBAPIError
from .temp_id import TempId, resolve


class DatabaseMappingAddMixin:
    """Provides methods to perform ``INSERT`` operations over a Spine db."""

    def add_items(self, tablename, *items, check=True, strict=False):
        """Add items to the in-memory mapping.

        Args:
            tablename (str): The table where items are inserted.
            items (Iterable): One or more :class:`dict` objects representing the items to be inserted.
            check (bool): Whether or not to run integrity checks.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if the insertion of one of the items violates an integrity constraint.

        Returns:
            tuple(list(dict),list(str)): items successfully added and found violations.
        """
        added, errors = [], []
        for item in items:
            item, error = self.add_item(tablename, check, **item)
            if error:
                if strict:
                    raise SpineIntegrityError(error)
                errors.append(error)
                continue
            added.append(item)
        return added, errors

    def _do_add_items(self, connection, tablename, *items_to_add):
        """Add items to DB without checking integrity."""
        if not items_to_add:
            return
        try:
            table = self._metadata.tables[self._real_tablename(tablename)]
            id_items, temp_id_items = [], []
            for item in items_to_add:
                if isinstance(item["id"], TempId):
                    temp_id_items.append(item)
                else:
                    id_items.append(item)
            if id_items:
                connection.execute(table.insert(), [resolve(x._asdict()) for x in id_items])
            if temp_id_items:
                current_ids = {x["id"] for x in connection.execute(table.select())}
                next_id = max(current_ids, default=0) + 1
                available_ids = set(range(1, next_id)) - current_ids
                required_id_count = len(temp_id_items) - len(available_ids)
                new_ids = set(range(next_id, next_id + required_id_count))
                ids = sorted(available_ids | new_ids)
                for id_, item in zip(ids, temp_id_items):
                    temp_id = item["id"]
                    temp_id.resolve(id_)
                connection.execute(table.insert(), [resolve(x._asdict()) for x in temp_id_items])
            for tablename_, items_to_add_ in self._extra_items_to_add_per_table(tablename, items_to_add):
                if not items_to_add_:
                    continue
                table = self._metadata.tables[self._real_tablename(tablename_)]
                connection.execute(table.insert(), [resolve(x) for x in items_to_add_])
        except DBAPIError as e:
            msg = f"DBAPIError while inserting {tablename} items: {e.orig.args}"
            raise SpineDBAPIError(msg) from e

    @staticmethod
    def _extra_items_to_add_per_table(tablename, items_to_add):
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
            yield ("entity_element", ee_items_to_add)

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

    def add_ext_entity_metadata(self, *items, **kwargs):
        metadata_items = self.get_metadata_to_add_with_item_metadata_items(*items)
        self.add_items("metadata", *metadata_items, **kwargs)
        return self.add_items("entity_metadata", *items, **kwargs)

    def add_ext_parameter_value_metadata(self, *items, **kwargs):
        metadata_items = self.get_metadata_to_add_with_item_metadata_items(*items)
        self.add_items("metadata", *metadata_items, **kwargs)
        return self.add_items("parameter_value_metadata", *items, **kwargs)

    def get_metadata_to_add_with_item_metadata_items(self, *items):
        metadata_items = ({"name": item["metadata_name"], "value": item["metadata_value"]} for item in items)
        return [x for x in metadata_items if not self.mapped_table("metadata").find_item(x)]
