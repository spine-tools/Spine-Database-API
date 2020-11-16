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

"""
Provides :class:`.QuickDatabaseMappingBase`.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""

from datetime import datetime, timezone
from sqlalchemy import func
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.exc import DBAPIError
from .db_mapping_base import DatabaseMappingBase
from .helpers import (
    get_relationship_entity_class_items,
    get_relationship_entity_items,
    get_parameter_value_list_items,
)
from .exception import SpineDBAPIError


class QuickDatabaseMappingBase(DatabaseMappingBase):
    """Base class for a quick read-write database mapping.
    Unlike DatabaseMappingBase, this class doesn't use sqlalchemy's ORM.
    It instead does everything using the Core expression language, for performance.

    :param str db_url: A URL in RFC-1738 format pointing to the database to be mapped.
    :param str username: A user name. If ``None``, it gets replaced by the string ``"anon"``.
    :param bool upgrade: Whether or not the db at the given URL should be upgraded to the most recent version.
    """

    def __init__(self, *args, **kwargs):
        """Initialize class."""
        super().__init__(*args, **kwargs)
        self._transaction = self.connection.begin()
        user = self.username
        date = datetime.now(timezone.utc)
        ins = self._metadata.tables["commit"].insert().values(user=user, date=date, comment="")
        self._commit_id = self.connection.execute(ins).inserted_primary_key[0]

    def __del__(self):
        self.rollback_session()

    def reconnect(self):
        super().reconnect()
        self._transaction = self.connection.begin()

    def commit_session(self, comment):
        commit = self._metadata.tables["commit"]
        user = self.username
        date = datetime.now(timezone.utc)
        upd = commit.update().where(commit.c.id == self._commit_id).values(user=user, date=date, comment=comment)
        self.connection.execute(upd)
        self._transaction.commit()
        self.connection.close()

    def rollback_session(self):
        self._transaction.rollback()
        self.connection.close()

    def _next_id(self, tablename):
        tablename = {
            "object_class": "entity_class",
            "relationship_class": "entity_class",
            "object": "entity",
            "relationship": "entity",
        }.get(tablename, tablename)
        table = self._metadata.tables[tablename]
        max_id = self.query(func.max(table.c.id)).scalar()
        return max_id + 1 if max_id else 1

    def _items_to_add_and_ids(self, next_id, *items):
        ids = list(range(next_id, next_id + len(items)))
        items_to_add = list()
        append_item = items_to_add.append
        for id_, item in zip(ids, items):
            item["id"] = id_
            item["commit_id"] = self._commit_id
            append_item(item)
        return items_to_add, ids

    def _add_items(self, tablename, *items):
        next_id = self._next_id(tablename)
        items, ids = self._items_to_add_and_ids(next_id, *items)
        self._do_add_items(tablename, *items)
        return set(ids)

    def _do_add_items(self, tablename, *items):
        if not items:
            return
        table = self._metadata.tables[tablename]
        ins = table.insert()
        try:
            self.connection.execute(ins, items)
        except DBAPIError as e:
            msg = f"DBAPIError while inserting '{tablename}' items: {e.orig.args}"
            raise SpineDBAPIError(msg)

    def _add_object_classes(self, *items):
        for item in items:
            item["type_id"] = self.object_class_type
        ids = self._add_items("entity_class", *items)
        items = [{"entity_class_id": id_, "type_id": self.object_class_type} for id_ in ids]
        self._do_add_items("object_class", *items)
        return set(ids)

    def _add_objects(self, *items):
        for item in items:
            item["type_id"] = self.object_entity_type
        ids = self._add_items("entity", *items)
        items = [{"entity_id": id_, "type_id": self.object_entity_type} for id_ in ids]
        self._do_add_items("object", *items)
        return set(ids)

    def _add_wide_relationship_classes(self, *wide_items):
        for wide_item in wide_items:
            wide_item["type_id"] = self.relationship_class_type
        ids = self._add_items("entity_class", *wide_items)
        items = [{"entity_class_id": id_, "type_id": self.relationship_class_type} for id_ in ids]
        self._do_add_items("relationship_class", *items)
        items = []
        for wide_item in wide_items:
            items += get_relationship_entity_class_items(wide_item, self.object_class_type)
        self._do_add_items("relationship_entity_class", *items)
        return set(ids)

    def _add_wide_relationships(self, *wide_items):
        for wide_item in wide_items:
            wide_item["type_id"] = self.relationship_entity_type
        ids = self._add_items("entity", *wide_items)
        items = [{"entity_id": id_, "type_id": self.relationship_entity_type} for id_ in ids]
        self._do_add_items("relationship", *items)
        items = []
        for wide_item in wide_items:
            items += get_relationship_entity_items(wide_item, self.relationship_entity_type, self.object_entity_type)
        self._do_add_items("relationship_entity", *items)
        return set(ids)

    def _add_wide_parameter_value_lists(self, *wide_items):
        tablename = "parameter_value_list"
        next_id = self._next_id(tablename)
        wide_items, ids = self._items_to_add_and_ids(next_id, *wide_items)
        items = []
        for wide_item in wide_items:
            items += get_parameter_value_list_items(wide_item)
        self._do_add_items(tablename, *items)
        return set(ids)

    def _add_alternatives(self, *items):
        return self._add_items("alternative", *items)

    def _add_scenarios(self, *items):
        return self._add_items("scenario", *items)

    def _add_scenario_alternatives(self, *items):
        return self._add_items("scenario_alternative", *items)

    def _add_features(self, *items):
        return self._add_items("feature", *items)

    def _add_tools(self, *items):
        return self._add_items("tool", *items)

    def _add_tool_features(self, *items):
        return self._add_items("tool_feature", *items)

    def _add_tool_feature_methods(self, *items):
        return self._add_items("tool_feature_method", *items)

    def _add_entity_groups(self, *items):
        return self._add_items("entity_group", *items)

    def _add_parameter_definitions(self, *items):
        return self._add_items("parameter_definition", *items)

    def _add_parameter_values(self, *items):
        return self._add_items("parameter_value", *items)

    def _add_metadata(self, *items):
        return self._add_items("metadata", *items)

    def _add_parameter_value_metadata(self, *items):
        return self._add_items("parameter_value_metadata", *items)

    def _add_entity_metadata(self, *items):
        return self._add_items("entity_metadata", *items)

    def _items_to_update_and_ids(self, *items):
        items_to_update = []
        ids = []
        append_item = items_to_update.append
        append_id = ids.append
        for item in items:
            item["commit_id"] = self._commit_id
            append_item(item)
            append_id(item["id"])
        return items_to_update, ids

    def _update_items(self, tablename, *items):
        if not items:
            return set()
        item = items[0]
        table = self._metadata.tables[tablename]
        items, ids = self._items_to_update_and_ids(*items)
        upd = (
            table.update()
            .where(table.c.id == bindparam('id'))
            .values({key: bindparam(key) for key in table.columns.keys() & item.keys()})
        )
        try:
            self.connection.execute(upd, items)
        except DBAPIError as e:
            msg = f"DBAPIError while updating '{tablename}' items: {e.orig.args}"
            raise SpineDBAPIError(msg)
        return set(ids)

    def _update_object_classes(self, *items):
        return self._update_items("entity_class", *items)

    def _update_objects(self, *items):
        return self._update_items("entity", *items)

    def _update_wide_relationship_classes(self, *wide_items):
        # TODO: Update member classes if needed
        return self._update_items("entity_class", *wide_items)

    def _update_wide_relationships(self, *wide_items):
        return self._update_items("entity", *wide_items)

    def _update_wide_parameter_value_lists(self, *wide_items):
        self._remove_items("parameter_value_list")
        return self._add_wide_parameter_value_lists(*wide_items)

    def _update_alternatives(self, *items):
        return self._update_items("alternative", *items)

    def _update_scenarios(self, *items):
        return self._update_items("scenario", *items)

    def _update_scenario_alternatives(self, *items):
        return self._update_items("scenario_alternative", *items)

    def _update_features(self, *items):
        return self._update_items("feature", *items)

    def _update_tools(self, *items):
        return self._update_items("tool", *items)

    def _update_tool_features(self, *items):
        return self._update_items("tool_feature", *items)

    def _update_tool_feature_methods(self, *items):
        return self._update_items("tool_feature_method", *items)

    def _update_entity_groups(self, *items):
        return self._update_items("entity_group", *items)

    def _update_parameter_definitions(self, *items):
        return self._update_items("parameter_definition", *items)

    def _update_parameter_values(self, *items):
        return self._update_items("parameter_value", *items)

    def _update_metadata(self, *items):
        return self._update_items("metadata", *items)

    def _update_parameter_value_metadata(self, *items):
        return self._update_items("parameter_value_metadata", *items)

    def _update_entity_metadata(self, *items):
        return self._update_items("entity_metadata", *items)

    def _remove_items(self, tablename, *ids):
        table = self._metadata.tables[tablename]
        delete = table.delete().where(self.in_(table.c.id, ids))
        try:
            self.connection.execute(delete)
        except DBAPIError as e:
            msg = f"DBAPIError while removing '{tablename}' items: {e.orig.args}"
            raise SpineDBAPIError(msg)
