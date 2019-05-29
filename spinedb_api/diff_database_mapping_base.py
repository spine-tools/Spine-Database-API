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

from .database_mapping_base import DatabaseMappingBase
from sqlalchemy import MetaData, Table, Column, Integer, String, inspect
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import NoSuchTableError
from .exception import SpineTableNotFoundError
from datetime import datetime, timezone


# TODO: improve docstrings


class DiffDatabaseMappingBase(DatabaseMappingBase):
    """A class to create a 'diff' ORM from a Spine db. It works by creating and mapping a set of
    temporary 'diff' tables, where changes made by the user can be staged until committed.
    """

    def __init__(self, db_url, username=None, upgrade=False):
        """Initialize class."""
        super().__init__(db_url, username=username, upgrade=upgrade)
        # Diff meta, Base and tables
        self.diff_prefix = None
        self.DiffCommit = None
        self.DiffObjectClass = None
        self.DiffObject = None
        self.DiffRelationshipClass = None
        self.DiffRelationship = None
        self.DiffParameterDefinition = None
        self.DiffParameterValue = None
        self.DiffParameterTag = None
        self.DiffParameterDefinitionTag = None
        self.DiffParameterValueList = None
        self.NextId = None
        # Diff dictionaries
        self.new_item_id = {}
        self.dirty_item_id = {}
        self.removed_item_id = {}
        self.touched_item_id = {}
        # Initialize stuff
        self.init_diff_dicts()
        self.create_diff_tables_and_mapping()
        self.init_next_id()

    def touch_items(self, tablename, ids):
        """Mark items as touched, which means the corresponding records from the original tables are not valid,
        and they should always be queried from the diff tables."""
        self.touched_item_id[tablename].update(ids)
        # Force subquery to be refreshed
        setattr(self, "_" + tablename + "_sq", None)
        # Force special subqueries to be refreshed
        self._ext_relationship_class_sq = None
        self._wide_relationship_class_sq = None
        self._ext_relationship_sq = None
        self._wide_relationship_sq = None
        self._ext_parameter_definition_tag_sq = None
        self._wide_parameter_definition_tag_sq = None
        self._wide_parameter_value_list_sq = None

    def has_pending_changes(self):
        """Return `True` if there are uncommitted changes. Otherwise return `False`."""
        if any([v for v in self.new_item_id.values()]):
            return True
        if any([v for v in self.touched_item_id.values()]):
            return True
        return False

    def init_diff_dicts(self):
        """Initialize dictionaries holding the difference ids."""
        self.new_item_id = {x: set() for x in self.table_to_class}
        self.dirty_item_id = {x: set() for x in self.table_to_class}
        self.removed_item_id = {x: set() for x in self.table_to_class}
        self.touched_item_id = {
            x: set() for x in self.table_to_class
        }  # Either dirty, or removed

    def create_diff_tables_and_mapping(self):
        """Create tables to hold differences and the corresponding mapping."""
        # Create tables...
        diff_name_prefix = "diff_"
        diff_name_prefix += self.username if self.username else "anon"
        self.diff_prefix = (
            diff_name_prefix + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + "_"
        )
        metadata = MetaData(self.engine)
        metadata.reflect()
        diff_metadata = MetaData()
        diff_tables = list()
        for t in metadata.sorted_tables:
            if t.name.startswith(diff_name_prefix) or t.name == "next_id":
                continue
            # Copy columns
            diff_columns = [c.copy() for c in t.columns]
            # Create table
            diff_table = Table(
                self.diff_prefix + t.name,
                diff_metadata,
                *diff_columns,
                prefixes=["TEMPORARY"]
            )
        diff_metadata.drop_all(self.engine)
        # NOTE: Using `self.connection` below allows `self.session` to see the temp tables
        diff_metadata.create_all(self.connection)
        # Create mapping...
        DiffBase = automap_base(metadata=diff_metadata)
        DiffBase.prepare()
        not_found = []
        for tablename, classname in self.table_to_class.items():
            try:
                setattr(
                    self,
                    "Diff" + classname,
                    getattr(DiffBase.classes, self.diff_prefix + tablename),
                )
            except (NoSuchTableError, AttributeError):
                not_found.append(tablename)
        if not_found:
            raise SpineTableNotFoundError(not_found, self.db_url)

    def init_next_id(self):
        """Create `next_id` table if not exists and map it."""
        # TODO: Does this work? What happens if there's already a next_id table with a different definition?
        # Create table
        metadata = MetaData()
        next_id_table = Table(
            "next_id",
            metadata,
            Column("user", String, primary_key=True),
            Column("date", String, primary_key=True),
            Column("object_class_id", Integer),
            Column("object_id", Integer),
            Column("relationship_class_id", Integer),
            Column("relationship_id", Integer),
            Column("parameter_definition_id", Integer),
            Column("parameter_value_id", Integer),
            Column("parameter_tag_id", Integer),
            Column("parameter_value_list_id", Integer),
            Column("parameter_definition_tag_id", Integer),
        )
        next_id_table.create(self.engine, checkfirst=True)
        # Create mapping...
        Base = automap_base(metadata=metadata)
        Base.prepare()
        try:
            self.NextId = Base.classes.next_id
        except (AttributeError, NoSuchTableError):
            raise SpineTableNotFoundError("next_id", self.db_url)

    def subquery(self, tablename):
        """
        Overriden method to bring items from both original and diff tables,
        while filtering touched items from the former:
            SELECT * FROM orig_table WHERE id NOT IN touched_ids
            UNION ALL
            SELECT * FROM diff_table
        """
        classname = self.table_to_class[tablename]
        orig_class = getattr(self, classname)
        diff_class = getattr(self, "Diff" + classname)
        return (
            self.query(*[c.label(c.name) for c in inspect(orig_class).mapper.columns])
            .filter(~orig_class.id.in_(self.touched_item_id[tablename]))
            .union_all(self.query(*inspect(diff_class).mapper.columns))
            .subquery()
        )

    def reset_diff_mapping(self):
        """Delete all records from diff tables (but don't drop the tables)."""
        self.query(self.DiffObjectClass).delete()
        self.query(self.DiffObject).delete()
        self.query(self.DiffRelationshipClass).delete()
        self.query(self.DiffRelationship).delete()
        self.query(self.DiffParameterDefinition).delete()
        self.query(self.DiffParameterValue).delete()
        self.query(self.DiffParameterTag).delete()
        self.query(self.DiffParameterDefinitionTag).delete()
        self.query(self.DiffParameterValueList).delete()
