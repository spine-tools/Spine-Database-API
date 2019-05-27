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

import logging
import warnings
from sqlalchemy import create_engine, false, distinct, func, MetaData, event, or_
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, aliased
from sqlalchemy.pool import StaticPool
from sqlalchemy.exc import NoSuchTableError, DBAPIError, DatabaseError
from alembic.migration import MigrationContext
from alembic.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from alembic.config import Config
from .exception import (
    SpineDBAPIError,
    SpineDBVersionError,
    SpineTableNotFoundError,
    RecordNotFoundError,
    ParameterValueError,
)
from .helpers import custom_generate_relationship, attr_dict
from datetime import datetime, timezone

# TODO: Consider returning lists of dict (with _asdict()) rather than queries,
# to better support platforms that cannot handle queries efficiently (such as Julia)
# TODO: At some point DatabaseMapping attributes such as session, engine, and all the tables should be made 'private'
# so as to prevent hacking into the database.
# TODO: SELECT queries should also be checked for errors


class DatabaseMapping(object):
    """A class to query a Spine database using object relational mapping.

    Attributes:
        db_url (str): The database url formatted according to sqlalchemy rules
        username (str): The user name
    """

    def __init__(self, db_url, username=None, create_all=True, upgrade=False):
        """Initialize class."""
        self.db_url = db_url
        self.username = username
        self.engine = None
        self.connection = None
        self.session = None
        self.Base = None
        self.ObjectClass = None
        self.Object = None
        self.RelationshipClass = None
        self.Relationship = None
        self.ParameterDefinition = None
        self.ParameterValue = None
        self.ParameterTag = None
        self.ParameterDefinitionTag = None
        self.ParameterValueList = None
        self.Commit = None
        if create_all:
            self.create_engine_and_session()
            self.check_db_version(upgrade=upgrade)
            self.create_mapping()
            self.create_subqueries()

    def create_engine_and_session(self):
        """Create engine connected to self.db_url and corresponding session."""
        try:
            self.engine = create_engine(self.db_url)
            with self.engine.connect():
                pass
        except DatabaseError as e:
            raise SpineDBAPIError(
                "Could not connect to '{}': {}".format(self.db_url, e.orig.args)
            )
        try:
            # Quickly check if at least object_class is there...
            self.engine.execute("SELECT * from object_class;")
        except DBAPIError as e:
            raise SpineDBAPIError(
                "Table 'object_class' not found. Not a Spine database?"
            )
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

    def check_db_version(self, upgrade=False):
        """Check if database is the latest version and raise a SpineDBVersionError if not.
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
        # NOTE: Should we include all missing tables in the error message, rather than only the first one?
        try:
            self.Base = automap_base()
            self.Base.prepare(self.engine, reflect=True)
            self.ObjectClass = self.Base.classes.object_class
            self.Object = self.Base.classes.object
            self.RelationshipClass = self.Base.classes.relationship_class
            self.Relationship = self.Base.classes.relationship
            self.ParameterDefinition = self.Base.classes.parameter_definition
            self.Parameter = self.ParameterDefinition  # FIXME
            self.ParameterValue = self.Base.classes.parameter_value
            self.ParameterTag = self.Base.classes.parameter_tag
            self.ParameterDefinitionTag = self.Base.classes.parameter_definition_tag
            self.ParameterValueList = self.Base.classes.parameter_value_list
            self.Commit = self.Base.classes.commit
        except (NoSuchTableError, AttributeError) as table:
            raise SpineTableNotFoundError(table, self.db_url)

    def create_subqueries(self):
        """Create subqueries that select everything from each table.
        These subqueries are used in all queries below (instead of the classes,
        e.g., `self.session.query(self.object_class.c.id)` rather than `self.session.query(self.ObjectClass.id)`).
        The idea is that subclasses can override the subquery attributes to provide custom functionality (see
        `DiffDatabaseMapping`)
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
            class_ = getattr(self, classname)
            setattr(
                self,
                tablename,
                self.session.query(
                    *[c.label(c.name) for c in inspect(class_).mapper.columns]
                ).subquery(with_labels=False),
            )

    def single_object_class(self, id=None, name=None):
        """Return a single object class given the id or name."""
        qry = self.object_class_list()
        if id:
            return qry.filter(self.object_class.id == id)
        if name:
            return qry.filter(self.object_class.name == name)
        return self.empty_list()

    def single_object(self, id=None, name=None):
        """Return a single object given the id or name."""
        qry = self.object_list()
        if id:
            return qry.filter(self.object.id == id)
        if name:
            return qry.filter(self.object.name == name)
        return self.empty_list()

    def single_wide_relationship_class(self, id=None, name=None):
        """Return a single relationship class in wide format given the id or name."""
        subqry = self.wide_relationship_class_list().subquery()
        qry = self.session.query(
            subqry.c.id,
            subqry.c.object_class_id_list,
            subqry.c.object_class_name_list,
            subqry.c.name,
        )
        if id:
            return qry.filter(subqry.c.id == id)
        if name:
            return qry.filter(subqry.c.name == name)
        return self.empty_list()

    def single_wide_relationship(
        self,
        id=None,
        name=None,
        class_id=None,
        object_id_list=None,
        object_name_list=None,
    ):
        """Return a single relationship in wide format given the id or name."""
        subqry = self.wide_relationship_list().subquery()
        qry = self.session.query(
            subqry.c.id,
            subqry.c.class_id,
            subqry.c.object_id_list,
            subqry.c.object_name_list,
            subqry.c.name,
        )
        if id:
            return qry.filter(subqry.c.id == id)
        if name:
            return qry.filter(subqry.c.name == name)
        if class_id:
            qry = qry.filter(subqry.c.class_id == class_id)
            if object_id_list:
                return qry.filter(subqry.c.object_id_list == object_id_list)
            if object_name_list:
                return qry.filter(subqry.c.object_name_list == object_name_list)
        return self.empty_list()

    def single_parameter_definition(self, id=None, name=None):
        """Return parameter corresponding to id."""
        qry = self.parameter_definition_list()
        if id:
            return qry.filter(self.parameter_definition.c.id == id)
        if name:
            return qry.filter(self.parameter_definition.c.name == name)
        return self.empty_list()

    def single_object_parameter_definition(self, id):
        """Return object class and the parameter corresponding to id."""
        return self.object_parameter_definition_list().filter(
            self.parameter_definition.c.id == id
        )

    def single_relationship_parameter_definition(self, id):
        """Return relationship class and the parameter corresponding to id."""
        return self.relationship_parameter_definition_list().filter(
            self.parameter_definition.c.id == id
        )

    def single_parameter(self, id=None, name=None):
        warnings.warn(
            "single_parameter is deprecated, use single_parameter_definition instead",
            DeprecationWarning,
        )
        return self.single_parameter_definition(id=id, name=name)

    def single_object_parameter(self, id):
        warnings.warn(
            "single_object_parameter is deprecated, use single_object_parameter_definition instead",
            DeprecationWarning,
        )
        return self.single_object_parameter_definition(id)

    def single_relationship_parameter(self, id):
        warnings.warn(
            "single_relationship_parameter is deprecated, use single_relationship_parameter_definition instead",
            DeprecationWarning,
        )
        return self.single_relationship_parameter_definition(id)

    def single_parameter_value(self, id=None):
        """Return parameter value corresponding to id."""
        if id:
            return self.parameter_value_list().filter(self.parameter_value.c.id == id)
        return self.empty_list()

    def single_object_parameter_value(
        self, id=None, parameter_id=None, parameter_definition_id=None, object_id=None
    ):
        """Return object and the parameter value, either corresponding to id,
        or to parameter_id and object_id.
        """
        if parameter_definition_id is None and parameter_id is not None:
            parameter_definition_id = parameter_id
            warnings.warn(
                "the parameter_id argument is deprecated, use parameter_definition_id instead",
                DeprecationWarning,
            )
        qry = self.object_parameter_value_list()
        if id:
            return qry.filter(self.parameter_value.c.id == id)
        if parameter_definition_id and object_id:
            return qry.filter(
                self.parameter_value.c.parameter_definition_id
                == parameter_definition_id
            ).filter(self.parameter_value.c.object_id == object_id)
        return self.empty_list()

    def single_relationship_parameter_value(self, id):
        """Return relationship and the parameter value corresponding to id."""
        return self.relationship_parameter_value_list().filter(
            self.parameter_value.c.id == id
        )

    def object_class_list(self, id_list=None, ordered=True):
        """Return object classes ordered by display order."""
        qry = self.session.query(self.object_class)
        if id_list is not None:
            qry = qry.filter(self.object_class.c.id.in_(id_list))
        if ordered:
            qry = qry.order_by(self.object_class.c.display_order)
        return qry

    def object_list(self, id_list=None, class_id=None):
        """Return objects, optionally filtered by class id."""
        qry = self.session.query(self.object)
        if id_list is not None:
            qry = qry.filter(self.object.c.id.in_(id_list))
        if class_id:
            qry = qry.filter(self.object.c.class_id == class_id)
        return qry

    def relationship_class_list(self, id=None, ordered=True):
        """Return all relationship classes optionally filtered by id."""
        qry = self.session.query(self.relationship_class)
        if id:
            qry = qry.filter(self.relationship_class.c.id == id)
        if ordered:
            qry = qry.order_by(
                self.relationship_class.c.id, self.relationship_class.c.dimension
            )
        return qry

    def wide_relationship_class_list(self, id_list=None, object_class_id=None):
        """Return list of relationship classes in wide format involving a given object class."""
        qry = self.session.query(
            self.relationship_class.c.id.label("id"),
            self.relationship_class.c.object_class_id.label("object_class_id"),
            self.object_class.c.name.label("object_class_name"),
            self.relationship_class.c.name.label("name"),
        ).filter(self.relationship_class.c.object_class_id == self.object_class.c.id)
        if id_list is not None:
            qry = qry.filter(self.relationship_class.c.id.in_(id_list))
        if object_class_id:
            qry = qry.filter(
                self.relationship_class.c.id.in_(
                    self.session.query(self.relationship_class.c.id)
                    .filter(
                        self.relationship_class.c.object_class_id == object_class_id
                    )
                    .distinct()
                )
            )
        subqry = qry.subquery()
        return self.session.query(
            subqry.c.id,
            func.group_concat(subqry.c.object_class_id).label("object_class_id_list"),
            func.group_concat(subqry.c.object_class_name).label(
                "object_class_name_list"
            ),
            subqry.c.name,
        ).group_by(subqry.c.id)

    def relationship_list(self, id=None):
        """Return relationships, optionally filtered by id."""
        qry = self.session.query(self.relationship).order_by(
            self.relationship.c.id, self.relationship.c.dimension
        )
        if id:
            qry = qry.filter(self.relationship.c.id == id)
        return qry

    def wide_relationship_list(self, id_list=None, class_id=None, object_id=None):
        """Return list of relationships in wide format involving a given relationship class and object."""
        object_list = self.object_list().subquery()
        qry = self.session.query(
            self.relationship.c.id.label("id"),
            self.relationship.c.class_id.label("class_id"),
            self.relationship.c.object_id.label("object_id"),
            object_list.c.name.label("object_name"),
            self.relationship.c.name.label("name"),
        ).filter(self.relationship.c.object_id == object_list.c.id)
        if id_list is not None:
            qry = qry.filter(self.relationship.c.id.in_(id_list))
        if class_id:
            qry = qry.filter(
                self.relationship.c.id.in_(
                    self.session.query(self.relationship.c.id)
                    .filter(self.relationship.c.class_id == class_id)
                    .distinct()
                )
            )
        if object_id:
            qry = qry.filter(
                self.relationship.c.id.in_(
                    self.session.query(self.relationship.c.id)
                    .filter(self.relationship.c.object_id == object_id)
                    .distinct()
                )
            )
        subqry = qry.subquery()
        return self.session.query(
            subqry.c.id,
            subqry.c.class_id,
            func.group_concat(subqry.c.object_id).label("object_id_list"),
            func.group_concat(subqry.c.object_name).label("object_name_list"),
            subqry.c.name,
        ).group_by(subqry.c.id)

    def parameter_definition_list(
        self, id_list=None, object_class_id=None, relationship_class_id=None
    ):
        """Return parameter definitions."""
        qry = self.session.query(
            self.parameter_definition.c.id.label("id"),
            self.parameter_definition.c.name.label("name"),
            self.parameter_definition.c.relationship_class_id.label(
                "relationship_class_id"
            ),
            self.parameter_definition.c.object_class_id.label("object_class_id"),
            self.parameter_definition.c.parameter_value_list_id.label(
                "parameter_value_list_id"
            ),
            self.parameter_definition.c.default_value.label("default_value"),
        )
        if id_list is not None:
            qry = qry.filter(self.parameter_definition.c.id.in_(id_list))
        if object_class_id:
            qry = qry.filter(
                self.parameter_definition.c.object_class_id == object_class_id
            )
        if relationship_class_id:
            qry = qry.filter(
                self.parameter_definition.c.relationship_class_id
                == relationship_class_id
            )
        return qry

    def object_parameter_definition_list(
        self, object_class_id=None, parameter_id=None, parameter_definition_id=None
    ):
        """Return object classes and their parameters."""
        if parameter_definition_id is None and parameter_id is not None:
            parameter_definition_id = parameter_id
            warnings.warn(
                "the parameter_id argument is deprecated, use parameter_definition_id instead",
                DeprecationWarning,
            )
        object_class_list = self.object_class_list().subquery()
        wide_parameter_definition_tag_list = (
            self.wide_parameter_definition_tag_list().subquery()
        )
        wide_parameter_value_list_list = (
            self.wide_parameter_value_list_list().subquery()
        )
        qry = (
            self.session.query(
                self.parameter_definition.c.id.label("id"),
                object_class_list.c.id.label("object_class_id"),
                object_class_list.c.name.label("object_class_name"),
                self.parameter_definition.c.name.label("parameter_name"),
                self.parameter_definition.c.parameter_value_list_id.label(
                    "value_list_id"
                ),
                wide_parameter_value_list_list.c.name.label("value_list_name"),
                wide_parameter_definition_tag_list.c.parameter_tag_id_list,
                wide_parameter_definition_tag_list.c.parameter_tag_list,
                self.parameter_definition.c.default_value,
            )
            .filter(
                object_class_list.c.id == self.parameter_definition.c.object_class_id
            )
            .outerjoin(
                wide_parameter_definition_tag_list,
                wide_parameter_definition_tag_list.c.parameter_definition_id
                == self.parameter_definition.c.id,
            )
            .outerjoin(
                wide_parameter_value_list_list,
                wide_parameter_value_list_list.c.id
                == self.parameter_definition.c.parameter_value_list_id,
            )
        )
        if object_class_id:
            qry = qry.filter(
                self.parameter_definition.c.object_class_id == object_class_id
            )
        if parameter_id:
            qry = qry.filter(self.parameter_definition.c.id == parameter_id)
        return qry

    def relationship_parameter_definition_list(
        self,
        relationship_class_id=None,
        parameter_id=None,
        parameter_definition_id=None,
    ):
        """Return relationship classes and their parameters."""
        if parameter_definition_id is None and parameter_id is not None:
            parameter_definition_id = parameter_id
            warnings.warn(
                "the parameter_id argument is deprecated, use parameter_definition_id instead",
                DeprecationWarning,
            )
        wide_relationship_class_list = self.wide_relationship_class_list().subquery()
        wide_parameter_definition_tag_list = (
            self.wide_parameter_definition_tag_list().subquery()
        )
        wide_parameter_value_list_list = (
            self.wide_parameter_value_list_list().subquery()
        )
        qry = (
            self.session.query(
                self.parameter_definition.c.id.label("id"),
                wide_relationship_class_list.c.id.label("relationship_class_id"),
                wide_relationship_class_list.c.name.label("relationship_class_name"),
                wide_relationship_class_list.c.object_class_id_list,
                wide_relationship_class_list.c.object_class_name_list,
                self.parameter_definition.c.name.label("parameter_name"),
                self.parameter_definition.c.parameter_value_list_id.label(
                    "value_list_id"
                ),
                wide_parameter_value_list_list.c.name.label("value_list_name"),
                wide_parameter_definition_tag_list.c.parameter_tag_id_list,
                wide_parameter_definition_tag_list.c.parameter_tag_list,
                self.parameter_definition.c.default_value,
            )
            .filter(
                self.parameter_definition.c.relationship_class_id
                == wide_relationship_class_list.c.id
            )
            .outerjoin(
                wide_parameter_definition_tag_list,
                wide_parameter_definition_tag_list.c.parameter_definition_id
                == self.parameter_definition.c.id,
            )
            .outerjoin(
                wide_parameter_value_list_list,
                wide_parameter_value_list_list.c.id
                == self.parameter_definition.c.parameter_value_list_id,
            )
        )
        if relationship_class_id:
            qry = qry.filter(
                self.parameter_definition.c.relationship_class_id
                == relationship_class_id
            )
        if parameter_id:
            qry = qry.filter(self.parameter_definition.c.id == parameter_id)
        return qry

    def parameter_list(
        self, id_list=None, object_class_id=None, relationship_class_id=None
    ):
        warnings.warn(
            "parameter_list is deprecated, use parameter_definition_list instead",
            DeprecationWarning,
        )
        return self.parameter_definition_list(
            id_list=id_list,
            object_class_id=object_class_id,
            relationship_class_id=relationship_class_id,
        )

    def object_parameter_list(self, object_class_id=None, parameter_id=None):
        warnings.warn(
            "object_parameter_list is deprecated, use object_parameter_definition_list instead",
            DeprecationWarning,
        )
        return self.object_parameter_definition_list(
            object_class_id=object_class_id, parameter_id=parameter_id
        )

    def relationship_parameter_list(
        self, relationship_class_id=None, parameter_id=None
    ):
        warnings.warn(
            "relationship_parameter_list is deprecated, use relationship_parameter_definition_list instead",
            DeprecationWarning,
        )
        return self.relationship_parameter_definition_list(
            relationship_class_id=relationship_class_id, parameter_id=parameter_id
        )

    def wide_object_parameter_definition_list(
        self, object_class_id_list=None, parameter_definition_id_list=None
    ):
        """Return object classes and their parameter definitions in wide format."""
        parameter_definition_list = self.parameter_definition_list().subquery()
        qry = self.session.query(
            self.object_class.c.id.label("object_class_id"),
            self.object_class.c.name.label("object_class_name"),
            parameter_definition_list.c.id.label("parameter_definition_id"),
            parameter_definition_list.c.name.label("parameter_name"),
        ).filter(self.object_class.c.id == parameter_definition_list.c.object_class_id)
        if object_class_id_list is not None:
            qry = qry.filter(self.object_class.c.id.in_(object_class_id_list))
        if parameter_definition_id_list is not None:
            qry = qry.filter(
                parameter_definition_list.c.id.in_(parameter_definition_id_list)
            )
        subqry = qry.subquery()
        return self.session.query(
            subqry.c.object_class_id,
            subqry.c.object_class_name,
            func.group_concat(subqry.c.parameter_definition_id).label(
                "parameter_definition_id_list"
            ),
            func.group_concat(subqry.c.parameter_name).label("parameter_name_list"),
        ).group_by(subqry.c.object_class_id)

    def wide_relationship_parameter_definition_list(
        self, relationship_class_id_list=None, parameter_definition_id_list=None
    ):
        """Return relationship classes and their parameter definitions in wide format."""
        parameter_definition_list = self.parameter_definition_list().subquery()
        qry = self.session.query(
            self.RelationshipClass.id.label("relationship_class_id"),
            self.RelationshipClass.name.label("relationship_class_name"),
            parameter_definition_list.c.id.label("parameter_definition_id"),
            parameter_definition_list.c.name.label("parameter_name"),
        ).filter(
            self.RelationshipClass.id
            == parameter_definition_list.c.relationship_class_id
        )
        if relationship_class_id_list is not None:
            qry = qry.filter(self.RelationshipClass.id.in_(relationship_class_id_list))
        if parameter_definition_id_list is not None:
            qry = qry.filter(
                parameter_definition_list.c.id.in_(parameter_definition_id_list)
            )
        subqry = qry.subquery()
        return self.session.query(
            subqry.c.relationship_class_id,
            subqry.c.relationship_class_name,
            func.group_concat(subqry.c.parameter_definition_id).label(
                "parameter_definition_id_list"
            ),
            func.group_concat(subqry.c.parameter_name).label("parameter_name_list"),
        ).group_by(subqry.c.relationship_class_id)

    def parameter_value_list(self, id_list=None, object_id=None, relationship_id=None):
        """Return parameter values."""
        qry = self.session.query(
            self.parameter_value.c.id,
            self.parameter_value.c.parameter_definition_id,
            self.parameter_value.c.object_id,
            self.parameter_value.c.relationship_id,
            self.parameter_value.c.value,
        )
        if id_list is not None:
            qry = qry.filter(self.parameter_value.c.id.in_(id_list))
        if object_id:
            qry = qry.filter(self.parameter_value.c.object_id == object_id)
        if relationship_id:
            qry = qry.filter(self.parameter_value.c.relationship_id == relationship_id)
        return qry

    # TODO: This should be updated so it also brings value_list and tag_list
    def object_parameter_value_list(self, parameter_name=None):
        """Return objects and their parameter values."""
        parameter_list = self.parameter_list().subquery()
        object_class_list = self.object_class_list().subquery()
        object_list = self.object_list().subquery()
        qry = (
            self.session.query(
                self.parameter_value.c.id.label("id"),
                object_class_list.c.id.label("object_class_id"),
                object_class_list.c.name.label("object_class_name"),
                object_list.c.id.label("object_id"),
                object_list.c.name.label("object_name"),
                parameter_list.c.id.label("parameter_id"),
                parameter_list.c.name.label("parameter_name"),
                self.parameter_value.c.value,
            )
            .filter(
                parameter_list.c.id == self.parameter_value.c.parameter_definition_id
            )
            .filter(self.parameter_value.c.object_id == object_list.c.id)
            .filter(parameter_list.c.object_class_id == object_class_list.c.id)
        )
        if parameter_name:
            qry = qry.filter(parameter_list.c.name == parameter_name)
        return qry

    # TODO: This should be updated so it also brings value_list and tag_list
    def relationship_parameter_value_list(self, parameter_name=None):
        """Return relationships and their parameter values."""
        parameter_list = self.parameter_list().subquery()
        wide_relationship_class_list = self.wide_relationship_class_list().subquery()
        wide_relationship_list = self.wide_relationship_list().subquery()
        qry = (
            self.session.query(
                self.parameter_value.c.id.label("id"),
                wide_relationship_class_list.c.id.label("relationship_class_id"),
                wide_relationship_class_list.c.name.label("relationship_class_name"),
                wide_relationship_class_list.c.object_class_id_list,
                wide_relationship_class_list.c.object_class_name_list,
                wide_relationship_list.c.id.label("relationship_id"),
                wide_relationship_list.c.object_id_list,
                wide_relationship_list.c.object_name_list,
                parameter_list.c.id.label("parameter_id"),
                parameter_list.c.name.label("parameter_name"),
                self.parameter_value.c.value,
            )
            .filter(
                parameter_list.c.id == self.parameter_value.c.parameter_definition_id
            )
            .filter(
                self.parameter_value.c.relationship_id == wide_relationship_list.c.id
            )
            .filter(
                parameter_list.c.relationship_class_id
                == wide_relationship_class_list.c.id
            )
        )
        if parameter_name:
            qry = qry.filter(parameter_list.c.name == parameter_name)
        return qry

    def all_object_parameter_value_list(self, parameter_id=None):
        """TODO: Is this needed?
        Return all object parameter values, even those that don't have a value."""
        qry = (
            self.session.query(
                self.parameter_definition.c.id.label("parameter_id"),
                self.object.c.name.label("object_name"),
                self.parameter_value.c.id.label("parameter_value_id"),
                self.parameter_definition.c.name.label("parameter_name"),
                self.parameter_value.c.value,
            )
            .filter(self.parameter_value.c.object_id == self.object.c.id)
            .outerjoin(self.ParameterValue)
            .filter(
                self.parameter_definition.c.id
                == self.parameter_value.c.parameter_definition_id
            )
        )
        if parameter_id:
            qry = qry.filter(self.parameter_definition.c.id == parameter_id)
        return qry

    # NOTE: maybe these unvalued... are obsolete
    def unvalued_object_parameter_list(self, object_id):
        """Return parameters that do not have a value for given object."""
        object_ = self.single_object(id=object_id).one_or_none()
        if not object_:
            return self.empty_list()
        valued_parameter_ids = self.session.query(
            self.parameter_value.c.parameter_definition_id
        ).filter(self.parameter_value.c.object_id == object_id)
        return self.parameter_definition_list(object_class_id=object_.class_id).filter(
            ~self.parameter_definition.c.id.in_(valued_parameter_ids)
        )

    def unvalued_object_list(self, parameter_id):
        """Return objects for which given parameter does not have a value."""
        parameter = self.single_parameter(parameter_id).one_or_none()
        if not parameter:
            return self.empty_list()
        valued_object_ids = self.session.query(self.parameter_value.c.object_id).filter(
            self.parameter_value.c.parameter_definition_id == parameter_id
        )
        return (
            self.object_list()
            .filter(self.object.c.class_id == parameter.object_class_id)
            .filter(~self.object.c.id.in_(valued_object_ids))
        )

    def unvalued_relationship_parameter_list(self, relationship_id):
        """Return parameters that do not have a value for given relationship."""
        relationship = self.single_wide_relationship(id=relationship_id).one_or_none()
        if not relationship:
            return self.empty_list()
        valued_parameter_ids = self.session.query(
            self.parameter_value.c.parameter_definition_id
        ).filter(self.parameter_value.relationship_id == relationship_id)
        return self.parameter_definition_list(
            relationship_class_id=relationship.class_id
        ).filter(~self.parameter_definition.c.id.in_(valued_parameter_ids))

    def unvalued_relationship_list(self, parameter_id):
        """Return relationships for which given parameter does not have a value."""
        parameter = self.single_parameter(parameter_id).one_or_none()
        if not parameter:
            return self.empty_list()
        valued_relationship_ids = self.session.query(
            self.parameter_value.c.relationship_id
        ).filter(self.parameter_value.c.parameter_definition_id == parameter_id)
        return (
            self.wide_relationship_list()
            .filter(self.relationship.c.class_id == parameter.relationship_class_id)
            .filter(~self.relationship.c.id.in_(valued_relationship_ids))
        )

    def parameter_tag_list(self, id_list=None, tag_list=None):
        """Return list of parameter tags."""
        qry = self.session.query(
            self.parameter_tag.c.id.label("id"),
            self.parameter_tag.c.tag.label("tag"),
            self.parameter_tag.c.description.label("description"),
        )
        if id_list is not None:
            qry = qry.filter(self.parameter_tag.c.id.in_(id_list))
        if tag_list is not None:
            qry = qry.filter(self.parameter_tag.c.tag.in_(tag_list))
        return qry

    def parameter_definition_tag_list(self, id_list=None):
        """Return list of parameter definition tags."""
        qry = self.session.query(
            self.parameter_definition_tag.c.id.label("id"),
            self.parameter_definition_tag.c.parameter_definition_id.label(
                "parameter_definition_id"
            ),
            self.parameter_definition_tag.c.parameter_tag_id.label("parameter_tag_id"),
        )
        if id_list is not None:
            qry = qry.filter(self.parameter_definition_tag.c.id.in_(id_list))
        return qry

    def wide_parameter_definition_tag_list(self, parameter_definition_id=None):
        """Return list of parameter tags in wide format for a given parameter definition."""
        qry = self.session.query(
            self.parameter_definition_tag.c.parameter_definition_id.label(
                "parameter_definition_id"
            ),
            self.parameter_definition_tag.c.parameter_tag_id.label("parameter_tag_id"),
            self.parameter_tag.c.tag.label("parameter_tag"),
        ).filter(
            self.parameter_definition_tag.c.parameter_tag_id == self.parameter_tag.c.id
        )
        if parameter_definition_id:
            qry = qry.filter(
                self.parameter_definition_tag.c.parameter_definition_id
                == parameter_definition_id
            )
        subqry = qry.subquery()
        return self.session.query(
            subqry.c.parameter_definition_id,
            func.group_concat(subqry.c.parameter_tag_id).label("parameter_tag_id_list"),
            func.group_concat(subqry.c.parameter_tag).label("parameter_tag_list"),
        ).group_by(subqry.c.parameter_definition_id)

    def wide_parameter_tag_definition_list(self, parameter_tag_id=None):
        """Return list of parameter tags (including the NULL tag) and their definitions in wide format.
        """
        parameter_definition_tag_list = self.parameter_definition_tag_list().subquery()
        qry = self.session.query(
            self.parameter_definition.c.id.label("parameter_definition_id"),
            parameter_definition_tag_list.c.parameter_tag_id.label("parameter_tag_id"),
        ).outerjoin(
            parameter_definition_tag_list,
            self.parameter_definition.c.id
            == parameter_definition_tag_list.c.parameter_definition_id,
        )
        if parameter_tag_id:
            qry = qry.filter(
                parameter_definition_tag_list.c.parameter_tag_id == parameter_tag_id
            )
        subqry = qry.subquery()
        return self.session.query(
            subqry.c.parameter_tag_id,
            func.group_concat(subqry.c.parameter_definition_id).label(
                "parameter_definition_id_list"
            ),
        ).group_by(subqry.c.parameter_tag_id)

    def parameter_value_list_list(self, id_list=None):
        """Return list of parameter value_lists."""
        qry = self.session.query(
            self.parameter_value_list.c.id.label("id"),
            self.parameter_value_list.c.name.label("name"),
            self.parameter_value_list.c.value_index.label("value_index"),
            self.parameter_value_list.c.value.label("value"),
        )
        if id_list is not None:
            qry = qry.filter(self.parameter_value_list.c.id.in_(id_list))
        return qry

    def wide_parameter_value_list_list(self, id_list=None):
        """Return list of parameter value_lists and their elements in wide format."""
        subqry = self.parameter_value_list_list(id_list=id_list).subquery()
        return (
            self.session.query(
                subqry.c.id,
                subqry.c.name,
                func.group_concat(subqry.c.value).label("value_list"),
            )
            .order_by(subqry.c.id, subqry.c.value_index)
            .group_by(subqry.c.id)
        )

    def object_parameter_fields(self):
        """Return object parameter fields."""
        return [x["name"] for x in self.object_parameter_list().column_descriptions]

    def relationship_parameter_fields(self):
        """Return relationship parameter fields."""
        return [
            x["name"] for x in self.relationship_parameter_list().column_descriptions
        ]

    def object_parameter_value_fields(self):
        """Return object parameter value fields."""
        return [
            x["name"] for x in self.object_parameter_value_list().column_descriptions
        ]

    def relationship_parameter_value_fields(self):
        """Return relationship parameter value fields."""
        return [
            x["name"]
            for x in self.relationship_parameter_value_list().column_descriptions
        ]

    def empty_list(self):
        return self.session.query(false()).filter(false())

    def reset_mapping(self):
        """Delete all records from all tables (but don't drop the tables)."""
        self.session.query(self.ObjectClass).delete(synchronize_session=False)
        self.session.query(self.Object).delete(synchronize_session=False)
        self.session.query(self.RelationshipClass).delete(synchronize_session=False)
        self.session.query(self.Relationship).delete(synchronize_session=False)
        self.session.query(self.ParameterDefinition).delete(synchronize_session=False)
        self.session.query(self.ParameterValue).delete(synchronize_session=False)
        self.session.query(self.ParameterTag).delete(synchronize_session=False)
        self.session.query(self.ParameterDefinitionTag).delete(
            synchronize_session=False
        )
        self.session.query(self.ParameterValueList).delete(synchronize_session=False)
        self.session.query(self.Commit).delete(synchronize_session=False)
