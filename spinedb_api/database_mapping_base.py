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
Class to create an object relational mapping from a Spine db.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""

from sqlalchemy import create_engine, inspect, func
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy.exc import NoSuchTableError, DBAPIError, DatabaseError
from alembic.migration import MigrationContext
from alembic.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from alembic.config import Config
from .exception import SpineDBAPIError, SpineDBVersionError, SpineTableNotFoundError


class DatabaseMappingBase(object):
    """A class to create an object relational mapping from a Spine db.

    Attributes:
        db_url (str): The database url formatted according to sqlalchemy rules
        username (str): The user name
    """

    def __init__(self, db_url, username=None, upgrade=False):
        """Initialize class."""
        self.db_url = db_url
        self.username = username
        self.engine = None
        self.connection = None
        self.session = None
        self.query = None
        self.Commit = None
        self.ObjectClass = None
        self.Object = None
        self.RelationshipClass = None
        self.Relationship = None
        self.ParameterDefinition = None
        self.ParameterValue = None
        self.ParameterTag = None
        self.ParameterDefinitionTag = None
        self.ParameterValueList = None
        # Subqueries that select everything from each table
        self._object_class_sq = None
        self._object_sq = None
        self._relationship_class_sq = None
        self._relationship_sq = None
        self._parameter_definition_sq = None
        self._parameter_value_sq = None
        self._parameter_tag_sq = None
        self._parameter_definition_tag_sq = None
        self._parameter_value_list_sq = None
        # Special convenience subqueries that join different tables
        self._ext_relationship_class_sq = None
        self._wide_relationship_class_sq = None
        self._ext_relationship_sq = None
        self._wide_relationship_sq = None
        self._ext_parameter_definition_tag_sq = None
        self._wide_parameter_definition_tag_sq = None
        self._wide_parameter_value_list_sq = None
        # Table to class dict
        self.table_to_class = {
            "commit": "Commit",
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
        self.create_engine_and_session()
        self.check_db_version(upgrade=upgrade)
        self.create_mapping()

    def create_engine_and_session(self):
        """Create engine, session and connection."""
        try:
            self.engine = create_engine(self.db_url)
            with self.engine.connect():
                pass
        except DatabaseError as e:
            raise SpineDBAPIError(
                "Could not connect to '{}': {}".format(self.db_url, e.orig.args)
            )
        try:
            # Quickly check if `commit` table is there...
            self.engine.execute("SELECT * from 'commit';")
        except DBAPIError as e:
            raise SpineDBAPIError("Table 'commit' not found. Not a Spine database?")
        if self.db_url.startswith("sqlite"):
            try:
                self.engine.execute("pragma quick_check;")
            except DatabaseError as e:
                msg = "Could not open '{}' as a SQLite database: {}".format(
                    self.db_url, e.orig.args
                )
                raise SpineDBAPIError(msg)
            # try:
            #     self.engine.execute('BEGIN IMMEDIATE')
            # except DatabaseError as e:
            #     msg = "Could not open '{}', seems to be locked: {}".format(self.db_url, e.orig.args)
            #     raise SpineDBAPIError(msg)
        self.connection = self.engine.connect()
        self.session = Session(self.connection, autoflush=False)
        self.query = self.session.query

    def check_db_version(self, upgrade=False):
        """Check if database is the latest version and raise a `SpineDBVersionError` if not.
        If upgrade is True, then don't raise the error and upgrade the database instead.
        """
        config = Config()
        config.set_main_option("script_location", "spinedb_api:alembic")
        script = ScriptDirectory.from_config(config)
        head = script.get_current_head()
        with self.engine.connect() as connection:
            migration_context = MigrationContext.configure(connection)
            current = migration_context.get_current_revision()
            if current == head:
                return
            if not upgrade:
                raise SpineDBVersionError(
                    url=self.db_url, current=current, expected=head
                )
            # Upgrade function
            def upgrade_to_head(rev, context):
                return script._upgrade_revs("head", rev)

            with EnvironmentContext(
                config,
                script,
                fn=upgrade_to_head,
                as_sql=False,
                starting_rev=None,
                destination_rev="head",
                tag=None,
            ) as environment_context:
                environment_context.configure(
                    connection=connection, target_metadata=None
                )
                with environment_context.begin_transaction():
                    environment_context.run_migrations()

    def create_mapping(self):
        """Create ORM."""
        Base = automap_base()
        Base.prepare(self.engine, reflect=True)
        not_found = []
        for tablename, classname in self.table_to_class.items():
            try:
                setattr(self, classname, getattr(Base.classes, tablename))
            except (NoSuchTableError, AttributeError):
                not_found.append(tablename)
        if not_found:
            raise SpineTableNotFoundError(not_found, self.db_url)

    def subquery(self, tablename):
        """SELECT * FROM table"""
        classname = self.table_to_class[tablename]
        class_ = getattr(self, classname)
        return self.query(
            *[c.label(c.name) for c in inspect(class_).mapper.columns]
        ).subquery()

    @property
    def object_class_sq(self):
        """SELECT * FROM object_class"""
        if self._object_class_sq is None:
            self._object_class_sq = self.subquery("object_class")
        return self._object_class_sq

    @property
    def object_sq(self):
        """SELECT * FROM object"""
        if self._object_sq is None:
            self._object_sq = self.subquery("object")
        return self._object_sq

    @property
    def relationship_class_sq(self):
        """SELECT * FROM relationship_class"""
        if self._relationship_class_sq is None:
            self._relationship_class_sq = self.subquery("relationship_class")
        return self._relationship_class_sq

    @property
    def relationship_sq(self):
        """SELECT * FROM relationship"""
        if self._relationship_sq is None:
            self._relationship_sq = self.subquery("relationship")
        return self._relationship_sq

    @property
    def parameter_definition_sq(self):
        """SELECT * FROM parameter_definition"""
        if self._parameter_definition_sq is None:
            self._parameter_definition_sq = self.subquery("parameter_definition")
        return self._parameter_definition_sq

    @property
    def parameter_value_sq(self):
        """SELECT * FROM parameter_value"""
        if self._parameter_value_sq is None:
            self._parameter_value_sq = self.subquery("parameter_value")
        return self._parameter_value_sq

    @property
    def parameter_tag_sq(self):
        """SELECT * FROM parameter_tag"""
        if self._parameter_tag_sq is None:
            self._parameter_tag_sq = self.subquery("parameter_tag")
        return self._parameter_tag_sq

    @property
    def parameter_definition_tag_sq(self):
        """SELECT * FROM parameter_definition_tag"""
        if self._parameter_definition_tag_sq is None:
            self._parameter_definition_tag_sq = self.subquery(
                "parameter_definition_tag"
            )
        return self._parameter_definition_tag_sq

    @property
    def parameter_value_list_sq(self):
        """SELECT * FROM parameter_value_list"""
        if self._parameter_value_list_sq is None:
            self._parameter_value_list_sq = self.subquery("parameter_value_list")
        return self._parameter_value_list_sq

    @property
    def ext_relationship_class_sq(self):
        """."""
        if self._ext_relationship_class_sq is None:
            self._ext_relationship_class_sq = (
                self.query(
                    self.relationship_class_sq.c.id.label("id"),
                    self.relationship_class_sq.c.object_class_id.label(
                        "object_class_id"
                    ),
                    self.object_class_sq.c.name.label("object_class_name"),
                    self.relationship_class_sq.c.name.label("name"),
                )
                .filter(
                    self.relationship_class_sq.c.object_class_id
                    == self.object_class_sq.c.id
                )
                .order_by(
                    self.relationship_class_sq.c.id,
                    self.relationship_class_sq.c.dimension,
                )
                .subquery()
            )
        return self._ext_relationship_class_sq

    @property
    def wide_relationship_class_sq(self):
        if self._wide_relationship_class_sq is None:
            self._wide_relationship_class_sq = (
                self.query(
                    self.ext_relationship_class_sq.c.id,
                    func.group_concat(
                        self.ext_relationship_class_sq.c.object_class_id
                    ).label("object_class_id_list"),
                    func.group_concat(
                        self.ext_relationship_class_sq.c.object_class_name
                    ).label("object_class_name_list"),
                    self.ext_relationship_class_sq.c.name,
                )
                .group_by(self.ext_relationship_class_sq.c.id)
                .subquery()
            )
        return self._wide_relationship_class_sq

    @property
    def ext_relationship_sq(self):
        if self._ext_relationship_sq is None:
            self._ext_relationship_sq = (
                self.query(
                    self.relationship_sq.c.id.label("id"),
                    self.relationship_sq.c.class_id.label("class_id"),
                    self.relationship_sq.c.object_id.label("object_id"),
                    self.object_sq.c.name.label("object_name"),
                    self.relationship_sq.c.name.label("name"),
                )
                .filter(self.relationship_sq.c.object_id == self.object_sq.c.id)
                .order_by(self.relationship_sq.c.id, self.relationship_sq.c.dimension)
                .subquery()
            )
        return self._ext_relationship_sq

    @property
    def wide_relationship_sq(self):
        if self._wide_relationship_sq is None:
            self._wide_relationship_sq = (
                self.query(
                    self.ext_relationship_sq.c.id,
                    self.ext_relationship_sq.c.class_id,
                    func.group_concat(self.ext_relationship_sq.c.object_id).label(
                        "object_id_list"
                    ),
                    func.group_concat(self.ext_relationship_sq.c.object_name).label(
                        "object_name_list"
                    ),
                    self.ext_relationship_sq.c.name,
                )
                .group_by(self.ext_relationship_sq.c.id)
                .subquery()
            )
        return self._wide_relationship_sq

    @property
    def ext_parameter_definition_tag_sq(self):
        if self._ext_parameter_definition_tag_sq is None:
            self._ext_parameter_definition_tag_sq = (
                self.query(
                    self.parameter_definition_tag_sq.c.parameter_definition_id.label(
                        "parameter_definition_id"
                    ),
                    self.parameter_definition_tag_sq.c.parameter_tag_id.label(
                        "parameter_tag_id"
                    ),
                    self.parameter_tag_sq.c.tag.label("parameter_tag"),
                )
                .filter(
                    self.parameter_definition_tag_sq.c.parameter_tag_id
                    == self.parameter_tag_sq.c.id
                )
                .subquery()
            )
        return self._ext_parameter_definition_tag_sq

    @property
    def wide_parameter_definition_tag_sq(self):
        if self._wide_parameter_definition_tag_sq is None:
            self._wide_parameter_definition_tag_sq = (
                self.query(
                    self.ext_parameter_definition_tag_sq.c.parameter_definition_id,
                    func.group_concat(
                        self.ext_parameter_definition_tag_sq.c.parameter_tag_id
                    ).label("parameter_tag_id_list"),
                    func.group_concat(
                        self.ext_parameter_definition_tag_sq.c.parameter_tag
                    ).label("parameter_tag_list"),
                )
                .group_by(
                    self.ext_parameter_definition_tag_sq.c.parameter_definition_id
                )
                .subquery()
            )
        return self._wide_parameter_definition_tag_sq

    @property
    def wide_parameter_value_list_sq(self):
        if self._wide_parameter_value_list_sq is None:
            self._wide_parameter_value_list_sq = (
                self.query(
                    self.parameter_value_list_sq.c.id,
                    self.parameter_value_list_sq.c.name,
                    func.group_concat(self.parameter_value_list_sq.c.value).label(
                        "value_list"
                    ),
                )
                .order_by(
                    self.parameter_value_list_sq.c.id,
                    self.parameter_value_list_sq.c.value_index,
                )
                .group_by(self.parameter_value_list_sq.c.id)
            ).subquery()
        return self._wide_parameter_value_list_sq

    def reset_mapping(self):
        """Delete all records from all tables (but don't drop the tables)."""
        self.query(self.ObjectClass).delete(synchronize_session=False)
        self.query(self.Object).delete(synchronize_session=False)
        self.query(self.RelationshipClass).delete(synchronize_session=False)
        self.query(self.Relationship).delete(synchronize_session=False)
        self.query(self.ParameterDefinition).delete(synchronize_session=False)
        self.query(self.ParameterValue).delete(synchronize_session=False)
        self.query(self.ParameterTag).delete(synchronize_session=False)
        self.query(self.ParameterDefinitionTag).delete(synchronize_session=False)
        self.query(self.ParameterValueList).delete(synchronize_session=False)
        self.query(self.Commit).delete(synchronize_session=False)
