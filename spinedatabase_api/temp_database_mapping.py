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

:author: Manuel Marin <manuelma@kth.se>
:date:   11.8.2018
"""

import time
import logging
import sqlite3
from .database_mapping import DatabaseMapping
from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy.orm.session import make_transient
from sqlalchemy.pool import StaticPool
from sqlalchemy.exc import NoSuchTableError, DBAPIError, DatabaseError
from .exception import SpineDBAPIError, TableNotFoundError
from datetime import datetime, timezone


class TempDatabaseMapping(DatabaseMapping):
    """A mapping to a temporary copy of a db."""
    def __init__(self, db_url, username=None):
        """Initialize class."""
        super().__init__(db_url, username)
        self.new_object_class_id = set()
        self.new_object_id = set()
        self.new_relationship_class_id = set()
        self.new_relationship_id = set()
        self.new_parameter_id = set()
        self.new_parameter_value_id = set()
        self.dirty_object_class_id = set()
        self.dirty_object_id = set()
        self.dirty_relationship_class_id = set()
        self.dirty_relationship_id = set()
        self.dirty_parameter_id = set()
        self.dirty_parameter_value_id = set()
        self.removed_object_class_id = set()
        self.removed_object_id = set()
        self.removed_relationship_class_id = set()
        self.removed_relationship_id = set()
        self.removed_parameter_id = set()
        self.removed_parameter_value_id = set()

    def create_engine_and_session(self):
        """Create engine and session."""
        source_engine = create_engine(self.db_url)
        # TODO: Confirm that this below creates an engine connected to a *temporary* sqlite database.
        # These are not the same as *memory* sqlite databases. Temporary lives in memory, but gets flush to disk when
        # there's too much pressure. See https://www.sqlite.org/inmemorydb.html
        def connect():
            return sqlite3.connect('')
        self.engine = create_engine(
            'sqlite://', creator=connect, connect_args={'check_same_thread':False}, poolclass=StaticPool)
        tic = time.clock()
        meta = MetaData()
        meta.reflect(source_engine)
        meta.create_all(self.engine)
        source_meta = MetaData(bind=source_engine)
        dest_meta = MetaData(bind=self.engine)
        for t in meta.sorted_tables:
            source_table = Table(t, source_meta, autoload=True)
            dest_table = Table(t, dest_meta, autoload=True)
            sel = select([source_table])
            result = source_engine.execute(sel)
            values = [row for row in result]
            if values:
                ins = dest_table.insert()
                self.engine.execute(ins, values)
        toc = time.clock()
        logging.debug("Temporary database created in {} seconds".format(toc - tic))
        self.session = Session(self.engine)

    def init_base(self):
        """Create base and reflect tables."""
        try:
            self.Base = automap_base()
            self.Base.prepare(self.engine, reflect=True)
            self.ObjectClass = self.Base.classes.object_class
            self.Object = self.Base.classes.object
            self.RelationshipClass = self.Base.classes.relationship_class
            self.Relationship = self.Base.classes.relationship
            self.Parameter = self.Base.classes.parameter
            self.ParameterValue = self.Base.classes.parameter_value
            self.Commit = self.Base.classes.commit
        except NoSuchTableError as table:
            raise TableNotFoundError(table)
        except AttributeError as table:
            raise TableNotFoundError(table)

    def add_object_class(self, **kwargs):
        """Add object class to database.

        Returns:
            object_class (KeyedTuple): the object class now with the id
        """
        try:
            item = super().add_object_class(**kwargs)
            self.new_object_class_id.add(item.id)
            return item
        except SpineDBAPIError as e:
            raise e

    def add_object(self, **kwargs):
        """Add object to database.

        Returns:
            object_ (KeyedTuple): the object now with the id
        """
        try:
            item = super().add_object(**kwargs)
            self.new_object_id.add(item.id)
            return item
        except SpineDBAPIError as e:
            raise e

    def add_wide_relationship_class(self, **kwargs):
        """Add relationship class to database.

        Args:
            kwargs (dict): the relationship class in wide format

        Returns:
            wide_relationship_class (KeyedTuple): the relationship class now with the id
        """
        try:
            item = super().add_wide_relationship_class(**kwargs)
            self.new_relationship_class_id.add(item.id)
            return item
        except SpineDBAPIError as e:
            raise e

    def add_wide_relationship(self, **kwargs):
        """Add relationship to database.

        Args:
            kwargs (dict): the relationship in wide format

        Returns:
            wide_relationship (KeyedTuple): the relationship now with the id
        """
        try:
            item = super().add_wide_relationship(**kwargs)
            self.new_relationship_id.add(item.id)
            return item
        except SpineDBAPIError as e:
            raise e

    def add_parameter(self, **kwargs):
        """Add parameter to database.

        Returns:
            An instance of self.Parameter if successful, None otherwise
        """
        try:
            item = super().add_parameter(**kwargs)
            self.new_parameter_id.add(item.id)
            return item
        except SpineDBAPIError as e:
            raise e

    def add_parameter_value(self, **kwargs):
        """Add parameter value to database.

        Returns:
            An instance of self.ParameterValue if successful, None otherwise
        """
        try:
            item = super().add_parameter_value(**kwargs)
            self.new_parameter_value_id.add(item.id)
            return item
        except SpineDBAPIError as e:
            raise e

    def rename_object_class(self, id, new_name):
        """Rename object class."""
        try:
            item = super().rename_object_class(id, new_name)
            self.dirty_object_class_id.add(id)
            return item
        except SpineDBAPIError as e:
            raise e

    def rename_object(self, id, new_name):
        """Rename object."""
        try:
            item = super().rename_object(id, new_name)
            self.dirty_object_id.add(id)
            return item
        except SpineDBAPIError as e:
            raise e

    def rename_relationship_class(self, id, new_name):
        """Rename relationship class."""
        try:
            item = super().rename_relationship_class(id, new_name)
            self.dirty_relationship_class_id.add(id)
            return item
        except SpineDBAPIError as e:
            raise e

    def rename_relationship(self, id, new_name):
        """Rename relationship."""
        try:
            item = super().rename_relationship(id, new_name)
            self.dirty_relationship_id.add(id)
            return item
        except SpineDBAPIError as e:
            raise e

    def update_parameter(self, id, field_name, new_value):
        """Update parameter."""
        try:
            item = super().update_parameter(id, field_name, new_value)
            self.dirty_parameter_id.add(id)
            return item
        except SpineDBAPIError as e:
            raise e

    def update_parameter_value(self, id, field_name, new_value):
        """Update parameter value."""
        try:
            item = super().update_parameter_value(id, field_name, new_value)
            self.dirty_parameter_value_id.add(id)
            return item
        except SpineDBAPIError as e:
            raise e

    def remove_object_class(self, id):
        """Remove object class."""
        try:
            item = super().remove_object_class(id)
            self.removed_object_class_id.add(id)
        except SpineDBAPIError as e:
            raise e

    def remove_object(self, id):
        """Remove object."""
        try:
            item = super().remove_object(id)
            self.removed_object_id.add(id)
        except SpineDBAPIError as e:
            raise e

    def remove_relationship_class(self, id):
        """Remove relationship class."""
        try:
            item = super().remove_relationship_class(id)
            self.removed_relationship_class_id.add(id)
        except SpineDBAPIError as e:
            raise e

    def remove_relationship(self, id):
        """Remove relationship."""
        try:
            item = super().remove_relationship(id)
            self.removed_relationship_id.add(id)
        except SpineDBAPIError as e:
            raise e

    def remove_parameter(self, id):
        """Remove parameter."""
        try:
            item = super().remove_parameter(id)
            self.removed_parameter_id.add(id)
        except SpineDBAPIError as e:
            raise e

    def remove_parameter_value(self, id):
        """Remove parameter value."""
        try:
            item = super().remove_parameter_value(id)
            self.removed_parameter_value_id.add(id)
        except SpineDBAPIError as e:
            raise e

    def commit_session(self, comment):
        """Commit changes to source database."""
        super().commit_session(comment)
        try:
            source_engine = create_engine(self.db_url)
            source_engine.connect()
            source_session = Session(source_engine)
            SourceBase = automap_base()
            SourceBase.prepare(source_engine, reflect=True)
            user = self.username
            date = datetime.now(timezone.utc)
            commit = SourceBase.classes.commit(comment=comment, date=date, user=user)
            source_session.add(commit)
            source_session.flush()
            # Add new
            new_items = list()
            for id in self.new_object_class_id:
                item = self.session.query(self.ObjectClass).filter_by(id=id).one_or_none()
                item.commit_id = commit.id
                new_items.append(item)
            for id in self.new_object_id:
                item = self.session.query(self.Object).filter_by(id=id).one_or_none()
                item.commit_id = commit.id
                new_items.append(item)
            for id in self.new_relationship_class_id:
                for item in self.session.query(self.RelationshipClass).filter_by(id=id):
                    item.commit_id = commit.id
                    new_items.append(item)
            for id in self.new_relationship_id:
                for item in self.session.query(self.Relationship).filter_by(id=id):
                    item.commit_id = commit.id
                    new_items.append(item)
            for id in self.new_parameter_id:
                item = self.session.query(self.Parameter).filter_by(id=id).one_or_none()
                item.commit_id = commit.id
                new_items.append(item)
            for id in self.new_parameter_value_id:
                item = self.session.query(self.ParameterValue).filter_by(id=id).one_or_none()
                item.commit_id = commit.id
                new_items.append(item)
            for item in new_items:
                make_transient(item)
            source_session.add_all(new_items)
            # Merge dirty
            dirty_items = list()
            for id in self.dirty_object_class_id:
                item = self.session.query(self.ObjectClass).filter_by(id=id).one_or_none()
                item.commit_id = commit.id
                dirty_items.append(item)
            for id in self.dirty_object_id:
                item = self.session.query(self.Object).filter_by(id=id).one_or_none()
                item.commit_id = commit.id
                dirty_items.append(item)
            for id in self.dirty_relationship_class_id:
                for item in self.session.query(self.RelationshipClass).filter_by(id=id):
                    item.commit_id = commit.id
                    dirty_items.append(item)
            for id in self.dirty_relationship_id:
                for item in self.session.query(self.Relationship).filter_by(id=id):
                    item.commit_id = commit.id
                    dirty_items.append(item)
            for id in self.dirty_parameter_id:
                item = self.session.query(self.Parameter).filter_by(id=id).one_or_none()
                item.commit_id = commit.id
                dirty_items.append(item)
            for id in self.dirty_parameter_value_id:
                item = self.session.query(self.ParameterValue).filter_by(id=id).one_or_none()
                item.commit_id = commit.id
                dirty_items.append(item)
            self.session.flush()
            for item in dirty_items:
                source_session.merge(item)
            # Remove removed
            removed_items = list()
            for id in self.removed_object_class_id:
                item = source_session.query(SourceBase.classes.object_class).filter_by(id=id).one_or_none()
                removed_items.append(item)
            for id in self.removed_object_id:
                item = source_session.query(SourceBase.classes.object).filter_by(id=id).one_or_none()
                removed_items.append(item)
            for id in self.removed_relationship_class_id:
                for item in source_session.query(SourceBase.classes.relationship_class).filter_by(id=id):
                    removed_items.append(item)
            for id in self.removed_relationship_id:
                for item in source_session.query(SourceBase.classes.relationship).filter_by(id=id):
                    removed_items.append(item)
            for id in self.removed_parameter_id:
                item = source_session.query(SourceBase.classes.parameter).filter_by(id=id).one_or_none()
                removed_items.append(item)
            for id in self.removed_parameter_value_id:
                item = source_session.query(SourceBase.classes.parameter_value).filter_by(id=id).one_or_none()
                removed_items.append(item)
            for item in removed_items:
                source_session.delete(item)
            source_session.commit()
        except DBAPIError as e:
            self.source_session.rollback()
            msg = "DBAPIError while commiting changes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)
