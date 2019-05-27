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

import time
import logging
import json
import warnings
from .database_mapping import DatabaseMapping
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    func,
    or_,
    and_,
    event,
    inspect,
)
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import NoSuchTableError, DBAPIError
from sqlalchemy.sql.schema import (
    UniqueConstraint,
    PrimaryKeyConstraint,
    ForeignKeyConstraint,
    CheckConstraint,
)
from .exception import SpineDBAPIError, SpineTableNotFoundError, SpineIntegrityError
from .helpers import custom_generate_relationship, attr_dict
from datetime import datetime, timezone


# TODO: improve docstrings


class _BaseDiffDatabaseMapping(DatabaseMapping):
    def __init__(self, db_url, username=None, create_all=True, upgrade=False):
        """Initialize class."""
        super().__init__(db_url, username=username, create_all=False, upgrade=upgrade)
        # Diff meta, Base and tables
        self.diff_prefix = None
        self.diff_metadata = None
        self.DiffBase = None
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
        # Subqueries that combine orig and diff into one result set
        self.object_class = None
        self.object = None
        self.relationship_class = None
        self.relationship = None
        self.parameter_definition = None
        self.parameter_value = None
        self.parameter_tag = None
        self.parameter_definition_tag = None
        self.parameter_value_list = None
        # Initialize stuff
        self.init_diff_dicts()
        if create_all:
            self.create_engine_and_session()
            self.check_db_version(upgrade=upgrade)
            self.create_mapping()
            self.create_diff_tables_and_mapping()
            self.init_next_id()
            self.create_subqueries()

    def has_pending_changes(self):
        """Return True if there are uncommitted changes. Otherwise return False."""
        if any([v for v in self.new_item_id.values()]):
            return True
        if any([v for v in self.touched_item_id.values()]):
            return True
        return False

    def init_diff_dicts(self):
        """Initialize dictionaries holding the differences."""
        self.new_item_id = {
            "object_class": set(),
            "object": set(),
            "relationship_class": set(),
            "relationship": set(),
            "parameter_definition": set(),
            "parameter_value": set(),
            "parameter_tag": set(),
            "parameter_definition_tag": set(),
            "parameter_value_list": set(),
        }
        self.dirty_item_id = {
            "object_class": set(),
            "object": set(),
            "relationship_class": set(),
            "relationship": set(),
            "parameter_definition": set(),
            "parameter_value": set(),
            "parameter_tag": set(),
            "parameter_definition_tag": set(),
            "parameter_value_list": set(),
        }
        self.removed_item_id = {
            "object_class": set(),
            "object": set(),
            "relationship_class": set(),
            "relationship": set(),
            "parameter_definition": set(),
            "parameter_value": set(),
            "parameter_tag": set(),
            "parameter_definition_tag": set(),
            "parameter_value_list": set(),
        }
        # Items that we don't want to read from the original tables (either dirty, or removed)
        self.touched_item_id = {
            "object_class": set(),
            "object": set(),
            "relationship_class": set(),
            "relationship": set(),
            "parameter_definition": set(),
            "parameter_value": set(),
            "parameter_tag": set(),
            "parameter_definition_tag": set(),
            "parameter_value_list": set(),
        }

    def create_diff_tables_and_mapping(self):
        """Create tables to hold differences and the corresponding mapping using an automap_base."""
        # Tables...
        self.diff_prefix = "diff_"
        self.diff_prefix += self.username if self.username else "anon"
        self.diff_prefix += datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + "_"
        self.diff_metadata = MetaData()
        diff_tables = list()
        for t in self.Base.metadata.sorted_tables:
            if t.name.startswith("diff_" + self.username):
                continue
            if t.name == "next_id":
                continue
            # Copy columns
            diff_columns = [c.copy() for c in t.columns]
            # Create table
            diff_table = Table(
                self.diff_prefix + t.name,
                self.diff_metadata,
                *diff_columns,
                prefixes=["TEMPORARY"]
            )
        self.diff_metadata.drop_all(self.engine)
        self.diff_metadata.create_all(
            self.connection
        )  # Using self.connection allows self.session to see the temp tables
        # Mapping...
        self.DiffBase = automap_base(metadata=self.diff_metadata)
        self.DiffBase.prepare(generate_relationship=custom_generate_relationship)
        try:
            self.DiffCommit = getattr(
                self.DiffBase.classes, self.diff_prefix + "commit"
            )
            self.DiffObjectClass = getattr(
                self.DiffBase.classes, self.diff_prefix + "object_class"
            )
            self.DiffObject = getattr(
                self.DiffBase.classes, self.diff_prefix + "object"
            )
            self.DiffRelationshipClass = getattr(
                self.DiffBase.classes, self.diff_prefix + "relationship_class"
            )
            self.DiffRelationship = getattr(
                self.DiffBase.classes, self.diff_prefix + "relationship"
            )
            self.DiffParameterDefinition = getattr(
                self.DiffBase.classes, self.diff_prefix + "parameter_definition"
            )
            self.DiffParameterValue = getattr(
                self.DiffBase.classes, self.diff_prefix + "parameter_value"
            )
            self.DiffParameterTag = getattr(
                self.DiffBase.classes, self.diff_prefix + "parameter_tag"
            )
            self.DiffParameterDefinitionTag = getattr(
                self.DiffBase.classes, self.diff_prefix + "parameter_definition_tag"
            )
            self.DiffParameterValueList = getattr(
                self.DiffBase.classes, self.diff_prefix + "parameter_value_list"
            )
        except NoSuchTableError as table:
            raise SpineTableNotFoundError(table, self.db_url)
        except AttributeError as table:
            raise SpineTableNotFoundError(table, self.db_url)

    def init_next_id(self):
        """Create next_id table if not exists and map it."""
        # TODO: Does this work? What happens if there's already a next_id table with a different definition?
        # Next id table
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
        # Mapping...
        Base = automap_base(metadata=metadata)
        Base.prepare()
        try:
            self.NextId = Base.classes.next_id
        except NoSuchTableError as table:
            raise SpineTableNotFoundError(table, self.db_url)
        except AttributeError as table:
            raise SpineTableNotFoundError(table, self.db_url)

    def create_subqueries(self):
        """Create subqueries that combine the original and difference tables.
        These subqueries should be used in all queries instead of the original classes,
        e.g., `self.session.query(self.object_class.c.id)` rather than `self.session.query(self.ObjectClass.id)`
        """
        table_to_class = {
            "object_class": "ObjectClass",
            "object": "Object",
            "relationship_class": "RelationshipClass",
            "relationship": "Relationship",
            "parameter_definition": "ParameterDefinition",
            "parameter_value": "ParameterValue",
            "parameter_tag": "ParameterTag",
            "parameter_definition_tag": "ParameterDefinitionTag",
            "parameter_value_list": "ParameterValueList",
        }
        for tablename, classname in table_to_class.items():
            orig_class = getattr(self, classname)
            diff_class = getattr(self, "Diff" + classname)
            setattr(
                self,
                tablename,
                self.session.query(
                    *[c.label(c.name) for c in inspect(orig_class).mapper.columns]
                )
                .filter(~orig_class.id.in_(self.touched_item_id[tablename]))
                .union_all(self.session.query(*inspect(diff_class).mapper.columns))
                .subquery(with_labels=False),
            )

    def reset_diff_mapping(self):
        """Delete all records from diff tables (but don't drop the tables)."""
        self.session.query(self.DiffObjectClass).delete()
        self.session.query(self.DiffObject).delete()
        self.session.query(self.DiffRelationshipClass).delete()
        self.session.query(self.DiffRelationship).delete()
        self.session.query(self.DiffParameterDefinition).delete()
        self.session.query(self.DiffParameterValue).delete()
        self.session.query(self.DiffParameterTag).delete()
        self.session.query(self.DiffParameterDefinitionTag).delete()
        self.session.query(self.DiffParameterValueList).delete()
