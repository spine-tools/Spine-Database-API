######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Database API contributors
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

from sqlalchemy import and_, or_
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.exc import DBAPIError
from .exception import SpineDBAPIError
from .temp_id import TempId, resolve
from .helpers import group_consecutive, Asterisk


class DatabaseMappingCommitMixin:
    _id_fields = {
        "entity_class_dimension": "entity_class_id",
        "entity_element": "entity_id",
        "object_class": "entity_class_id",
        "relationship_class": "entity_class_id",
        "object": "entity_id",
        "relationship": "entity_id",
    }
    composite_pks = {
        "entity_element": ("entity_id", "position"),
        "entity_class_dimension": ("entity_class_id", "position"),
    }

    def _do_add_items(self, connection, tablename, *items_to_add):
        """Add items to DB without checking integrity."""
        if not items_to_add:
            return
        try:
            table = self._metadata.tables[self.real_item_type(tablename)]
            id_items, temp_id_items = [], []
            for item in items_to_add:
                if isinstance(item["id"], TempId):
                    temp_id_items.append(item)
                else:
                    id_items.append(item)
            if id_items:
                connection.execute(table.insert(), [x.resolve() for x in id_items])
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
                connection.execute(table.insert(), [x.resolve() for x in temp_id_items])
            for tablename_, items_to_add_ in self._extra_items_to_add_per_table(tablename, items_to_add):
                if not items_to_add_:
                    continue
                table = self._metadata.tables[self.real_item_type(tablename_)]
                connection.execute(table.insert(), [resolve(x) for x in items_to_add_])
        except DBAPIError as e:
            msg = f"DBAPIError while inserting {tablename} items: {e.orig.args}"
            raise SpineDBAPIError(msg) from e

    @staticmethod
    def _dimensions_for_classes(classes):
        return [
            {"entity_class_id": x["id"], "position": position, "dimension_id": dimension_id}
            for x in classes
            for position, dimension_id in enumerate(x["dimension_id_list"])
        ]

    @staticmethod
    def _elements_for_entities(entities):
        return [
            {
                "entity_id": x["id"],
                "entity_class_id": x["class_id"],
                "position": position,
                "element_id": element_id,
                "dimension_id": dimension_id,
            }
            for x in entities
            for position, (element_id, dimension_id) in enumerate(zip(x["element_id_list"], x["dimension_id_list"]))
        ]

    def _extra_items_to_add_per_table(self, tablename, items_to_add):
        if tablename == "entity_class":
            yield ("entity_class_dimension", self._dimensions_for_classes(items_to_add))
        elif tablename == "entity":
            yield ("entity_element", self._elements_for_entities(items_to_add))

    def _extra_items_to_update_per_table(self, tablename, items_to_update):
        if tablename == "entity":
            yield ("entity_element", self._elements_for_entities(items_to_update))

    def _get_primary_key(self, tablename):
        pk = self.composite_pks.get(tablename)
        if pk is None:
            id_field = self._id_fields.get(tablename, "id")
            pk = (id_field,)
        return pk

    def _make_update_stmt(self, tablename, keys):
        table = self._metadata.tables[self.real_item_type(tablename)]
        upd = table.update()
        for k in self._get_primary_key(tablename):
            upd = upd.where(getattr(table.c, k) == bindparam(k))
        return upd.values({key: bindparam(key) for key in table.columns.keys() & keys})

    def _do_update_items(self, connection, tablename, *items_to_update):
        """Update items in DB without checking integrity."""
        if not items_to_update:
            return
        try:
            upd = self._make_update_stmt(tablename, items_to_update[0].keys())
            connection.execute(upd, [x.resolve() for x in items_to_update])
            for tablename_, items_to_update_ in self._extra_items_to_update_per_table(tablename, items_to_update):
                if not items_to_update_:
                    continue
                upd = self._make_update_stmt(tablename_, items_to_update_[0].keys())
                connection.execute(upd, [resolve(x) for x in items_to_update_])
        except DBAPIError as e:
            msg = f"DBAPIError while updating '{tablename}' items: {e.orig.args}"
            raise SpineDBAPIError(msg) from e

    def _do_remove_items(self, connection, tablename, *ids):
        """Removes items from the db.

        Args:
            *ids: ids to remove
        """
        tablename = self.real_item_type(tablename)
        ids = {resolve(id_) for id_ in ids}
        if tablename == "alternative":
            # Do not remove the Base alternative
            ids.discard(1)
        if not ids:
            return
        tablenames = [tablename]
        if tablename == "entity_class":
            # Also remove the items corresponding to the id in entity_class_dimension
            tablenames.append("entity_class_dimension")
        elif tablename == "entity":
            # Also remove the items corresponding to the id in entity_element
            tablenames.append("entity_element")
        for tablename_ in tablenames:
            table = self._metadata.tables[tablename_]
            delete = table.delete()
            if Asterisk not in ids:
                id_field = self._id_fields.get(tablename_, "id")
                id_column = getattr(table.c, id_field)
                cond = or_(*(and_(id_column >= first, id_column <= last) for first, last in group_consecutive(ids)))
                delete = delete.where(cond)
            try:
                connection.execute(delete)
            except DBAPIError as e:
                msg = f"DBAPIError while removing {tablename_} items: {e.orig.args}"
                raise SpineDBAPIError(msg) from e
