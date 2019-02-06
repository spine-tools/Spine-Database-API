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
from sqlalchemy import create_engine, false, distinct, func, MetaData, event
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, aliased
from sqlalchemy.pool import StaticPool
from sqlalchemy.exc import NoSuchTableError, DBAPIError, DatabaseError
from .exception import SpineDBAPIError, SpineTableNotFoundError, RecordNotFoundError, ParameterValueError
from .helpers import custom_generate_relationship, attr_dict, upgrade_to_head
from datetime import datetime, timezone

# TODO: Consider returning lists of dict (with _asdict()) rather than queries,
# to better support platforms that cannot handle queries efficiently (such as Julia)
# TODO: At some point DatabaseMapping attributes such as session, engine, and all the tables should be made 'private'
# so as to prevent hacking into the database.
# TODO: SELECT queries should also be checked for errors


class DatabaseMapping(object):
    """A class to manipulate the Spine database object relational mapping.

    Attributes:
        db_url (str): The database url formatted according to sqlalchemy rules
        username (str): The user name
    """
    def __init__(self, db_url, username=None, create_all=True):
        """Initialize class."""
        self.db_url = db_url
        self.username = username
        self.engine = None
        self.session = None
        self._commit = None
        self.transactions = list()
        self.Base = None
        self.ObjectClass = None
        self.Object = None
        self.RelationshipClass = None
        self.Relationship = None
        self.ParameterDefinition = None
        self.ParameterValue = None
        self.ParameterTag = None
        self.ParameterDefinitionTag = None
        self.ParameterEnum = None
        self.Commit = None
        upgrade_to_head(db_url)
        if create_all:
            self.create_engine_and_session()
            self.create_mapping()
            # self.create_triggers()

    def create_engine_and_session(self):
        """Create engine connected to self.db_url and session."""
        try:
            self.engine = create_engine(self.db_url)
            self.engine.connect()
        except DatabaseError as e:
            raise SpineDBAPIError("Could not connect to '{}': {}".format(self.db_url, e.orig.args))
        if self.db_url.startswith('sqlite'):
            try:
                self.engine.execute('pragma quick_check;')
            except DatabaseError as e:
                msg = "Could not open '{}' as a SQLite database: {}".format(self.db_url, e.orig.args)
                raise SpineDBAPIError(msg)
            # try:
            #     self.engine.execute('BEGIN IMMEDIATE')
            # except DatabaseError as e:
            #     msg = "Could not open '{}', seems to be locked: {}".format(self.db_url, e.orig.args)
            #     raise SpineDBAPIError(msg)
        self.session = Session(self.engine, autoflush=False)

    def create_mapping(self):
        """Create ORM."""
        try:
            self.Base = automap_base()
            self.Base.prepare(self.engine, reflect=True, generate_relationship=custom_generate_relationship)
            self.ObjectClass = self.Base.classes.object_class
            self.Object = self.Base.classes.object
            self.RelationshipClass = self.Base.classes.relationship_class
            self.Relationship = self.Base.classes.relationship
            self.ParameterDefinition = self.Base.classes.parameter_definition
            self.Parameter = self.ParameterDefinition  # FIXME
            self.ParameterValue = self.Base.classes.parameter_value
            self.ParameterTag = self.Base.classes.parameter_tag
            self.ParameterDefinitionTag = self.Base.classes.parameter_definition_tag
            self.ParameterEnum = self.Base.classes.parameter_enum
            self.Commit = self.Base.classes.commit
        except NoSuchTableError as table:
            self.close()
            raise SpineTableNotFoundError(table)
        except AttributeError as table:
            self.close()
            raise SpineTableNotFoundError(table)

    def create_triggers(self):
        """Create ad-hoc triggers.
        NOTE: Not in use at the moment
        TODO: is there a way to synch this with our CREATE TRIGGER statements
        from `helpers.create_new_spine_database`?
        """
        @event.listens_for(self.ObjectClass, 'after_delete')
        def receive_after_object_class_delete(mapper, connection, object_class):
            @event.listens_for(self.session, "after_flush", once=True)
            def receive_after_flush(session, context):
                id_list = session.query(self.RelationshipClass.id).\
                    filter_by(object_class_id=object_class.id).distinct()
                item_list = session.query(self.RelationshipClass).filter(self.RelationshipClass.id.in_(id_list))
                for item in item_list:
                    session.delete(item)
        @event.listens_for(self.Object, 'after_delete')
        def receive_after_object_delete(mapper, connection, object_):
            @event.listens_for(self.session, "after_flush", once=True)
            def receive_after_flush(session, context):
                id_list = session.query(self.Relationship.id).filter_by(object_id=object_.id).distinct()
                item_list = session.query(self.Relationship).filter(self.Relationship.id.in_(id_list))
                for item in item_list:
                    session.delete(item)

    def add_working_commit(self):
        """Add working commit item."""
        if self._commit:
            return
        comment = 'In progress...'
        user = self.username
        date = datetime.now(timezone.utc)
        self._commit = self.Commit(comment=comment, date=date, user=user)
        try:
            self.session.add(self._commit)
            self.session.flush()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting new commit: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def commit_session(self, comment):
        """Commit changes to source database."""
        if not self.session:
            raise SpineDBAPIError("Unable to retrieve current session.")
        if not self._commit:
            raise SpineDBAPIError("There's nothing to commit.")
        try:
            self._commit.comment = comment
            self._commit.date = datetime.now(timezone.utc)
            for i in reversed(range(len(self.transactions))):
                self.session.commit()
                del self.transactions[i]
            self.session.commit()  # also commit main transaction
        except DBAPIError as e:
            self._commit.comment = None
            self._commit.date = None
            msg = "DBAPIError while commiting changes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def rollback_session(self):
        if not self.session:
            raise SpineDBAPIError("No session!")
        try:
            for i in reversed(range(len(self.transactions))):
                self.session.rollback()
                del self.transactions[i]
            self.session.rollback()  # also rollback main transaction
        except DBAPIError:
            msg = "DBAPIError while rolling back changes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def single_object_class(self, id=None, name=None):
        """Return a single object class given the id or name."""
        qry = self.object_class_list()
        if id:
            return qry.filter_by(id=id)
        if name:
            return qry.filter_by(name=name)
        return self.empty_list()

    def single_object(self, id=None, name=None):
        """Return a single object given the id or name."""
        qry = self.object_list()
        if id:
            return qry.filter_by(id=id)
        if name:
            return qry.filter_by(name=name)
        return self.empty_list()

    def single_wide_relationship_class(self, id=None, name=None):
        """Return a single relationship class in wide format given the id or name."""
        subqry = self.wide_relationship_class_list().subquery()
        qry = self.session.query(
            subqry.c.id,
            subqry.c.object_class_id_list,
            subqry.c.object_class_name_list,
            subqry.c.name
        )
        if id:
            return qry.filter(subqry.c.id == id)
        if name:
            return qry.filter(subqry.c.name == name)
        return self.empty_list()

    def single_wide_relationship(self, id=None, name=None, class_id=None, object_id_list=None, object_name_list=None):
        """Return a single relationship in wide format given the id or name."""
        subqry = self.wide_relationship_list().subquery()
        qry = self.session.query(
            subqry.c.id,
            subqry.c.class_id,
            subqry.c.object_id_list,
            subqry.c.object_name_list,
            subqry.c.name
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

    def single_parameter(self, id=None, name=None):
        """Return parameter corresponding to id."""
        qry = self.parameter_list()
        if id:
            return qry.filter_by(id=id)
        if name:
            return qry.filter_by(name=name)
        return self.empty_list()

    def single_object_parameter(self, id):
        """Return object class and the parameter corresponding to id."""
        return self.object_parameter_list().filter(self.ParameterDefinition.id == id)

    def single_relationship_parameter(self, id):
        """Return relationship class and the parameter corresponding to id."""
        return self.relationship_parameter_list().filter(self.ParameterDefinition.id == id)

    def single_parameter_value(self, id=None):
        """Return parameter value corresponding to id."""
        if id:
            return self.parameter_value_list().filter_by(id=id)
        return self.empty_list()

    def single_object_parameter_value(self, id=None, parameter_id=None, object_id=None):
        """Return object and the parameter value, either corresponding to id,
        or to parameter_id and object_id.
        """
        qry = self.object_parameter_value_list()
        if id:
            return qry.filter(self.ParameterValue.id == id)
        if parameter_id and object_id:
            return qry.filter(self.ParameterValue.parameter_definition_id == parameter_id).\
                filter(self.ParameterValue.object_id == object_id)
        return self.empty_list()

    def single_relationship_parameter_value(self, id):
        """Return relationship and the parameter value corresponding to id."""
        return self.relationship_parameter_value_list().filter(self.ParameterValue.id == id)

    def object_class_list(self, id_list=None, ordered=True):
        """Return object classes ordered by display order."""
        qry = self.session.query(
            self.ObjectClass.id.label("id"),
            self.ObjectClass.name.label("name"),
            self.ObjectClass.display_order.label("display_order"),
            self.ObjectClass.description.label("description"))
        if id_list is not None:
            qry = qry.filter(self.ObjectClass.id.in_(id_list))
        if ordered:
            qry = qry.order_by(self.ObjectClass.display_order)
        return qry

    def object_list(self, id_list=None, class_id=None):
        """Return objects, optionally filtered by class id."""
        qry = self.session.query(
            self.Object.id.label('id'),
            self.Object.class_id.label('class_id'),
            self.Object.name.label('name'),
            self.Object.description.label("description"))
        if id_list is not None:
            qry = qry.filter(self.Object.id.in_(id_list))
        if class_id:
            qry = qry.filter_by(class_id=class_id)
        return qry

    def relationship_class_list(self, id=None, ordered=True):
        """Return all relationship classes optionally filtered by id."""
        qry = self.session.query(
            self.RelationshipClass.id.label('id'),
            self.RelationshipClass.dimension.label('dimension'),
            self.RelationshipClass.object_class_id.label('object_class_id'),
            self.RelationshipClass.name.label('name')
        )
        if id:
            qry = qry.filter_by(id=id)
        if ordered:
            qry = qry.order_by(self.RelationshipClass.id, self.RelationshipClass.dimension)
        return qry

    def wide_relationship_class_list(self, id_list=None, object_class_id=None):
        """Return list of relationship classes in wide format involving a given object class."""
        object_class_list = self.object_class_list(ordered=False).subquery()
        qry = self.session.query(
            self.RelationshipClass.id.label('id'),
            self.RelationshipClass.object_class_id.label('object_class_id'),
            object_class_list.c.name.label('object_class_name'),
            self.RelationshipClass.name.label('name')
        ).filter(self.RelationshipClass.object_class_id == object_class_list.c.id)
        if id_list is not None:
            qry = qry.filter(self.RelationshipClass.id.in_(id_list))
        if object_class_id:
            qry = qry.filter(self.RelationshipClass.id.in_(
                self.session.query(self.RelationshipClass.id).\
                    filter_by(object_class_id=object_class_id).distinct()))
        subqry = qry.subquery()
        return self.session.query(
            subqry.c.id,
            func.group_concat(subqry.c.object_class_id).label('object_class_id_list'),
            func.group_concat(subqry.c.object_class_name).label('object_class_name_list'),
            subqry.c.name
        ).group_by(subqry.c.id)

    def relationship_list(self, id=None):
        """Return relationships, optionally filtered by id."""
        qry = self.session.query(
            self.Relationship.id,
            self.Relationship.dimension,
            self.Relationship.object_id,
            self.Relationship.class_id,
            self.Relationship.name
        ).order_by(self.Relationship.id, self.Relationship.dimension)
        if id:
            qry = qry.filter_by(id=id)
        return qry

    def wide_relationship_list(self, id_list=None, class_id=None, object_id=None):
        """Return list of relationships in wide format involving a given relationship class and object."""
        object_list = self.object_list().subquery()
        qry = self.session.query(
            self.Relationship.id.label('id'),
            self.Relationship.class_id.label('class_id'),
            self.Relationship.object_id.label('object_id'),
            object_list.c.name.label('object_name'),
            self.Relationship.name.label('name')
        ).filter(self.Relationship.object_id == object_list.c.id)
        if id_list is not None:
            qry = qry.filter(self.Relationship.id.in_(id_list))
        if class_id:
            qry = qry.filter(self.Relationship.id.in_(
                self.session.query(self.Relationship.id).filter_by(class_id=class_id).distinct()))
        if object_id:
            qry = qry.filter(self.Relationship.id.in_(
                self.session.query(self.Relationship.id).filter_by(object_id=object_id).distinct()))
        subqry = qry.subquery()
        return self.session.query(
            subqry.c.id,
            subqry.c.class_id,
            func.group_concat(subqry.c.object_id).label('object_id_list'),
            func.group_concat(subqry.c.object_name).label('object_name_list'),
            subqry.c.name
        ).group_by(subqry.c.id)

    def parameter_list(self, id_list=None, object_class_id=None, relationship_class_id=None):
        """Return parameters."""
        qry = self.session.query(
            self.ParameterDefinition.id.label('id'),
            self.ParameterDefinition.name.label('name'),
            self.ParameterDefinition.relationship_class_id.label('relationship_class_id'),
            self.ParameterDefinition.object_class_id.label('object_class_id'),
            self.ParameterDefinition.can_have_time_series.label('can_have_time_series'),
            self.ParameterDefinition.can_have_time_pattern.label('can_have_time_pattern'),
            self.ParameterDefinition.can_be_stochastic.label('can_be_stochastic'),
            self.ParameterDefinition.default_value.label('default_value'),
            self.ParameterDefinition.is_mandatory.label('is_mandatory'),
            self.ParameterDefinition.precision.label('precision'),
            self.ParameterDefinition.minimum_value.label('minimum_value'),
            self.ParameterDefinition.maximum_value.label('maximum_value'))
        if id_list is not None:
            qry = qry.filter(self.ParameterDefinition.id.in_(id_list))
        if object_class_id:
            qry = qry.filter_by(object_class_id=object_class_id)
        if relationship_class_id:
            qry = qry.filter_by(relationship_class_id=relationship_class_id)
        return qry

    def object_parameter_list(self, object_class_id=None, parameter_id=None):
        """Return object classes and their parameters."""
        object_class_list = self.object_class_list().subquery()
        wide_parameter_definition_tag_list = self.wide_parameter_definition_tag_list().subquery()
        qry = self.session.query(
            self.ParameterDefinition.id.label('id'),
            object_class_list.c.id.label('object_class_id'),
            object_class_list.c.name.label('object_class_name'),
            self.ParameterDefinition.name.label('parameter_name'),
            wide_parameter_definition_tag_list.c.name.label('parameter_tag_list'),
            self.ParameterDefinition.can_have_time_series,
            self.ParameterDefinition.can_have_time_pattern,
            self.ParameterDefinition.can_be_stochastic,
            self.ParameterDefinition.default_value,
            self.ParameterDefinition.is_mandatory,
            self.ParameterDefinition.precision,
            self.ParameterDefinition.minimum_value,
            self.ParameterDefinition.maximum_value
        ).filter(object_class_list.c.id == self.ParameterDefinition.object_class_id).\
        outerjoin(
            wide_parameter_definition_tag_list,
            wide_parameter_definition_tag_list.c.parameter_definition_id == self.ParameterDefinition.id)
        if object_class_id:
            qry = qry.filter(self.ParameterDefinition.object_class_id == object_class_id)
        if parameter_id:
            qry = qry.filter(self.ParameterDefinition.id == parameter_id)
        return qry

    def relationship_parameter_list(self, relationship_class_id=None, parameter_id=None):
        """Return relationship classes and their parameters."""
        wide_relationship_class_list = self.wide_relationship_class_list().subquery()
        qry = self.session.query(
            self.ParameterDefinition.id.label('id'),
            wide_relationship_class_list.c.id.label('relationship_class_id'),
            wide_relationship_class_list.c.name.label('relationship_class_name'),
            wide_relationship_class_list.c.object_class_id_list,
            wide_relationship_class_list.c.object_class_name_list,
            self.ParameterDefinition.name.label('parameter_name'),
            self.ParameterDefinition.can_have_time_series,
            self.ParameterDefinition.can_have_time_pattern,
            self.ParameterDefinition.can_be_stochastic,
            self.ParameterDefinition.default_value,
            self.ParameterDefinition.is_mandatory,
            self.ParameterDefinition.precision,
            self.ParameterDefinition.minimum_value,
            self.ParameterDefinition.maximum_value
        ).filter(self.ParameterDefinition.relationship_class_id == wide_relationship_class_list.c.id)
        if relationship_class_id:
            qry = qry.filter(self.ParameterDefinition.relationship_class_id == relationship_class_id)
        if parameter_id:
            qry = qry.filter(self.ParameterDefinition.id == parameter_id)
        return qry

    def parameter_value_list(self, id_list=None, object_id=None, relationship_id=None):
        """Return parameter values."""
        qry = self.session.query(
            self.ParameterValue.id,
            self.ParameterValue.parameter_definition_id,
            self.ParameterValue.object_id,
            self.ParameterValue.relationship_id,
            self.ParameterValue.index,
            self.ParameterValue.value,
            self.ParameterValue.json,
            self.ParameterValue.expression,
            self.ParameterValue.time_pattern,
            self.ParameterValue.time_series_id,
            self.ParameterValue.stochastic_model_id)
        if id_list is not None:
            qry = qry.filter(self.ParameterValue.id.in_(id_list))
        if object_id:
            qry = qry.filter_by(object_id=object_id)
        if relationship_id:
            qry = qry.filter_by(relationship_id=relationship_id)
        return qry

    # TODO: This should be updated so it also brings enum and tag_list
    def object_parameter_value_list(self, parameter_name=None):
        """Return objects and their parameter values."""
        parameter_list = self.parameter_list().subquery()
        object_class_list = self.object_class_list().subquery()
        object_list = self.object_list().subquery()
        qry = self.session.query(
            self.ParameterValue.id.label('id'),
            object_class_list.c.id.label('object_class_id'),
            object_class_list.c.name.label('object_class_name'),
            object_list.c.id.label('object_id'),
            object_list.c.name.label('object_name'),
            parameter_list.c.id.label('parameter_id'),
            parameter_list.c.name.label('parameter_name'),
            self.ParameterValue.index,
            self.ParameterValue.value,
            self.ParameterValue.json,
            self.ParameterValue.expression,
            self.ParameterValue.time_pattern,
            self.ParameterValue.time_series_id,
            self.ParameterValue.stochastic_model_id
        ).filter(parameter_list.c.id == self.ParameterValue.parameter_definition_id).\
        filter(self.ParameterValue.object_id == object_list.c.id).\
        filter(parameter_list.c.object_class_id == object_class_list.c.id)
        if parameter_name:
            qry = qry.filter(parameter_list.c.name == parameter_name)
        return qry

    # TODO: This should be updated so it also brings enum and tag_list
    def relationship_parameter_value_list(self, parameter_name=None):
        """Return relationships and their parameter values."""
        parameter_list = self.parameter_list().subquery()
        wide_relationship_class_list = self.wide_relationship_class_list().subquery()
        wide_relationship_list = self.wide_relationship_list().subquery()
        qry = self.session.query(
            self.ParameterValue.id.label('id'),
            wide_relationship_class_list.c.id.label('relationship_class_id'),
            wide_relationship_class_list.c.name.label('relationship_class_name'),
            wide_relationship_class_list.c.object_class_id_list,
            wide_relationship_class_list.c.object_class_name_list,
            wide_relationship_list.c.object_id_list,
            wide_relationship_list.c.object_name_list,
            parameter_list.c.id.label('parameter_id'),
            parameter_list.c.name.label('parameter_name'),
            self.ParameterValue.index,
            self.ParameterValue.value,
            self.ParameterValue.json,
            self.ParameterValue.expression,
            self.ParameterValue.time_pattern,
            self.ParameterValue.time_series_id,
            self.ParameterValue.stochastic_model_id
        ).filter(parameter_list.c.id == self.ParameterValue.parameter_definition_id).\
        filter(self.ParameterValue.relationship_id == wide_relationship_list.c.id).\
        filter(parameter_list.c.relationship_class_id == wide_relationship_class_list.c.id)
        if parameter_name:
            qry = qry.filter(parameter_list.c.name == parameter_name)
        return qry

    def all_object_parameter_value_list(self, parameter_id=None):
        """TODO: Is this needed?
        Return all object parameter values, even those that don't have a value."""
        qry = self.session.query(
            self.ParameterDefinition.id.label('parameter_id'),
            self.Object.name.label('object_name'),
            self.ParameterValue.id.label('parameter_value_id'),
            self.ParameterDefinition.name.label('parameter_name'),
            self.ParameterValue.index,
            self.ParameterValue.value,
            self.ParameterValue.json,
            self.ParameterValue.expression,
            self.ParameterValue.time_pattern,
            self.ParameterValue.time_series_id,
            self.ParameterValue.stochastic_model_id
        ).filter(self.ParameterValue.object_id == self.Object.id).\
        outerjoin(self.ParameterValue).\
        filter(self.ParameterDefinition.id == self.ParameterValue.parameter_definition_id)
        if parameter_id:
            qry = qry.filter(self.ParameterDefinition.id == parameter_id)
        return qry

    # NOTE: maybe these unvalued... are obsolete
    def unvalued_object_parameter_list(self, object_id):
        """Return parameters that do not have a value for given object."""
        object_ = self.single_object(id=object_id).one_or_none()
        if not object_:
            return self.empty_list()
        valued_parameter_ids = self.session.query(self.ParameterValue.parameter_definition_id).\
            filter_by(object_id=object_id)
        return self.parameter_list(object_class_id=object_.class_id).\
            filter(~self.ParameterDefinition.id.in_(valued_parameter_ids))

    def unvalued_object_list(self, parameter_id):
        """Return objects for which given parameter does not have a value."""
        parameter = self.single_parameter(parameter_id).one_or_none()
        if not parameter:
            return self.empty_list()
        valued_object_ids = self.session.query(self.ParameterValue.object_id).\
            filter_by(parameter_id=parameter_id)
        return self.object_list().filter_by(class_id=parameter.object_class_id).\
            filter(~self.Object.id.in_(valued_object_ids))

    def unvalued_relationship_parameter_list(self, relationship_id):
        """Return parameters that do not have a value for given relationship."""
        relationship = self.single_wide_relationship(id=relationship_id).one_or_none()
        if not relationship:
            return self.empty_list()
        valued_parameter_ids = self.session.query(self.ParameterValue.parameter_definition_id).\
            filter_by(relationship_id=relationship_id)
        return self.parameter_list().filter_by(relationship_class_id=relationship.class_id).\
            filter(~self.ParameterDefinition.id.in_(valued_parameter_ids))

    def unvalued_relationship_list(self, parameter_id):
        """Return relationships for which given parameter does not have a value."""
        parameter = self.single_parameter(parameter_id).one_or_none()
        if not parameter:
            return self.empty_list()
        valued_relationship_ids = self.session.query(self.ParameterValue.relationship_id).\
            filter_by(parameter_id=parameter_id)
        return self.wide_relationship_list().filter_by(class_id=parameter.relationship_class_id).\
            filter(~self.Relationship.id.in_(valued_relationship_ids))

    def parameter_tag_list(self, id_list=None):
        """Return list of parameter tags."""
        qry = self.session.query(
            self.ParameterTag.id.label("id"),
            self.ParameterTag.tag.label("tag"),
            self.ParameterTag.description.label("description"))
        if id_list is not None:
            qry = qry.filter(self.ParameterTag.id.in_(id_list))
        return qry

    def parameter_definition_tag_list(self, id_list=None):
        """Return list of parameter definition tags."""
        qry = self.session.query(
            self.ParameterDefinitionTag.id.label('id'),
            self.ParameterDefinitionTag.parameter_definition_id.label('parameter_definition_id'),
            self.ParameterDefinitionTag.parameter_tag_id.label('parameter_tag_id'))
        if id_list is not None:
            qry = qry.filter(self.ParameterDefinitionTag.id.in_(id_list))
        return qry

    def wide_parameter_definition_tag_list(self, parameter_definition_id=None):
        """Return list of parameter tags in wide format for a given parameter definition."""
        qry = self.session.query(
            self.ParameterDefinitionTag.parameter_definition_id.label('parameter_definition_id'),
            self.ParameterDefinitionTag.parameter_tag_id.label('parameter_tag_id'),
            self.ParameterTag.tag.label('parameter_tag')
        ).filter(self.ParameterDefinitionTag.parameter_tag_id == self.ParameterTag.id)
        if parameter_definition_id:
            qry = qry.filter(self.ParameterDefinitionTag.parameter_definition_id == parameter_definition_id)
        subqry = qry.subquery()
        return self.session.query(
            subqry.c.parameter_definition_id,
            func.group_concat(subqry.c.parameter_tag_id).label('parameter_tag_id_list'),
            func.group_concat(subqry.c.parameter_tag).label('parameter_tag_list')
        ).group_by(subqry.c.parameter_definition_id)

    def parameter_enum_list(self, id_list=None):
        """Return list of parameter enums."""
        qry = self.session.query(
            self.ParameterEnum.id.label("id"),
            self.ParameterEnum.name.label("name"),
            self.ParameterEnum.element_index.label("element_index"),
            self.ParameterEnum.element.label("element"),
            self.ParameterEnum.value.label("value"))
        if id_list is not None:
            qry = qry.filter(self.ParameterEnum.id.in_(id_list))
        return qry

    def wide_parameter_enum_list(self, id_list=None):
        """Return list of parameter enums and their elements in wide format."""
        subqry = self.parameter_enum_list(id_list=id_list).subquery()
        return self.session.query(
            subqry.c.id,
            subqry.c.name,
            func.group_concat(subqry.c.element).label('element_list'),
            func.group_concat(subqry.c.value).label('value_list')
        ).order_by(subqry.c.id, subqry.c.element_index).group_by(subqry.c.id)

    def object_parameter_fields(self):
        """Return object parameter fields."""
        return [x['name'] for x in self.object_parameter_list().column_descriptions]

    def relationship_parameter_fields(self):
        """Return relationship parameter fields."""
        return [x['name'] for x in self.relationship_parameter_list().column_descriptions]

    def object_parameter_value_fields(self):
        """Return object parameter value fields."""
        return [x['name'] for x in self.object_parameter_value_list().column_descriptions]

    def relationship_parameter_value_fields(self):
        """Return relationship parameter value fields."""
        return [x['name'] for x in self.relationship_parameter_value_list().column_descriptions]

    def add_object_class(self, **kwargs):
        """Add object class to database.

        Returns:
            object_class (dict): the object class now with the id
        """
        self.add_working_commit()
        object_class = self.ObjectClass(commit_id=self._commit.id, **kwargs)
        try:
            self.transactions.append(self.session.begin_nested())
            self.session.add(object_class)
            self.session.flush()
            return self.single_object_class(id=object_class.id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting object class '{}': {}".format(object_class.name, e.orig.args)
            raise SpineDBAPIError(msg)

    def add_object(self, **kwargs):
        """Add object to database.

        Returns:
            object_ (dict): the object now with the id
        """
        self.add_working_commit()
        object_ = self.Object(commit_id=self._commit.id, **kwargs)
        try:
            self.transactions.append(self.session.begin_nested())
            self.session.add(object_)
            self.session.flush()
            return self.single_object(id=object_.id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting object '{}': {}".format(object_.name, e.orig.args)
            raise SpineDBAPIError(msg)

    def add_wide_relationship_class(self, **kwargs):
        """Add relationship class to database.

        Args:
            kwargs (dict): the relationship class in wide format

        Returns:
            wide_relationship_class (dict): the relationship class now with the id
        """
        self.add_working_commit()
        id = self.session.query(func.max(self.RelationshipClass.id)).scalar()
        if not id:
            id = 0
        id += 1
        relationship_class_list = list()
        for dimension, object_class_id in enumerate(kwargs['object_class_id_list']):
            kwargs = {
                'id': id,
                'dimension': dimension,
                'object_class_id': object_class_id,
                'name': kwargs['name'],
                'commit_id': self._commit.id
            }
            relationship_class = self.RelationshipClass(**kwargs)
            relationship_class_list.append(relationship_class)
        try:
            self.session.add_all(relationship_class_list)
            self.session.flush()
            return self.single_wide_relationship_class(id=id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting relationship class '{}': {}".\
                format(kwargs['name'], e.orig.args)
            raise SpineDBAPIError(msg)

    def add_wide_relationship(self, **kwargs):
        """Add relationship to database.

        Args:
            kwargs (dict): the relationship in wide format

        Returns:
            wide_relationship (dict): the relationship now with the id
        """
        # If relationship already exists (same class, same object_id_list), raise an error
        object_id_str = ",".join([str(x) for x in kwargs['object_id_list']])
        wide_relationship = self.single_wide_relationship(
            class_id=kwargs['class_id'],
            object_id_list=object_id_str).one_or_none()
        if wide_relationship:
            err = "A relationship between these objects already exists in this class."
            msg = "DBAPIError while inserting relationship '{}': {}".format(kwargs['name'], err)
            raise SpineDBAPIError(msg)
        self.add_working_commit()
        id = self.session.query(func.max(self.Relationship.id)).scalar()
        if not id:
            id = 0
        id += 1
        relationship_list = list()
        for dimension, object_id in enumerate(kwargs['object_id_list']):
            kwargs = {
                'id': id,
                'dimension': dimension,
                'object_id': object_id,
                'name': kwargs['name'],
                'class_id': kwargs['class_id'],
                'commit_id': self._commit.id
            }
            relationship = self.Relationship(**kwargs)
            relationship_list.append(relationship)
        try:
            self.session.add_all(relationship_list)
            self.session.flush()
            return self.single_wide_relationship(id=id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting relationship '{}': {}".format(kwargs['name'], e.orig.args)
            raise SpineDBAPIError(msg)

    def add_parameter(self, **kwargs):
        """Add parameter to database.

        Returns:
            An instance of self.ParameterDefinition if successful, None otherwise
        """
        self.add_working_commit()
        parameter = self.ParameterDefinition(commit_id=self._commit.id, **kwargs)
        try:
            self.session.add(parameter)
            self.session.flush()
            return self.single_parameter(id=parameter.id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting parameter '{}': {}".format(parameter.name, e.orig.args)
            raise SpineDBAPIError(msg)

    def add_parameter_value(self, **kwargs):
        """Add parameter value to database.

        Returns:
            An instance of self.ParameterValue if successful, None otherwise
        """
        self.add_working_commit()
        parameter_value = self.ParameterValue(commit_id=self._commit.id, **kwargs)
        try:
            self.session.add(parameter_value)
            self.session.flush()
            return self.single_parameter_value(id=parameter_value.id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting parameter value: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def get_or_add_object_class(self, **kwargs):
        """Add object class to database if not exists.

        Returns:
            A dict if successful, None otherwise
        """
        if "name" not in kwargs:
            return None
        object_class = self.single_object_class(name=kwargs["name"]).one_or_none()
        if object_class:
            return object_class
        return self.add_object_class(**kwargs)

    def get_or_add_wide_relationship_class(self, **kwargs):
        """Add relationship class to database if not exists.

        Returns:
            A dict if successful, None otherwise
        """
        if "name" not in kwargs or "object_class_id_list" not in kwargs:
            return None
        wide_relationship_class = self.single_wide_relationship_class(name=kwargs["name"]).one_or_none()
        if not wide_relationship_class:
            return self.add_wide_relationship_class(**kwargs)
        given_object_class_id_list = [int(x) for x in kwargs["object_class_id_list"]]
        found_object_class_id_list = [int(x) for x in wide_relationship_class.object_class_id_list.split(",")]
        if given_object_class_id_list != found_object_class_id_list:
            return None  # TODO: should we raise an error here?
        return wide_relationship_class

    def get_or_add_parameter(self, **kwargs):
        """Add parameter to database if not exists.

        Returns:
            A dict if successful, None otherwise
        """
        if "name" not in kwargs:
            return None
        parameter = self.single_parameter(name=kwargs["name"]).one_or_none()
        if parameter:
            return parameter
        return self.add_parameter(**kwargs)

    def rename_object_class(self, id, new_name):
        """Rename object class."""
        self.add_working_commit()
        object_class = self.session.query(self.ObjectClass).filter_by(id=id).one_or_none()
        if not object_class:
            raise RecordNotFoundError('object_class', name=new_name)
        try:
            self.transactions.append(self.session.begin_nested())
            object_class.name = new_name
            object_class.commit_id = self._commit.id
            self.session.flush()
            return self.single_object_class(id=object_class.id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while renaming object class '{}': {}".format(object_class.name, e.orig.args)
            raise SpineDBAPIError(msg)

    def rename_object(self, id, new_name):
        """Rename object."""
        self.add_working_commit()
        object_ = self.session.query(self.Object).filter_by(id=id).one_or_none()
        if not object_:
            raise RecordNotFoundError('object', name=new_name)
        try:
            self.transactions.append(self.session.begin_nested())
            object_.name = new_name
            object_.commit_id = self._commit.id
            self.session.flush()
            return self.single_object(id=object_.id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while renaming object '{}': {}".format(object_.name, e.orig.args)
            raise SpineDBAPIError(msg)

    def rename_relationship_class(self, id, new_name):
        """Rename relationship class."""
        self.add_working_commit()
        relationship_class_list = self.session.query(self.RelationshipClass).filter_by(id=id)
        if not relationship_class_list.count():
            raise RecordNotFoundError('relationship_class', name=new_name)
        try:
            self.transactions.append(self.session.begin_nested())
            for relationship_class in relationship_class_list:
                relationship_class.name = new_name
                relationship_class.commit_id = self._commit.id
            self.session.flush()
            return relationship_class_list.first()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while renaming relationship class '{}': {}".format(relationship_class.name, e.orig.args)
            raise SpineDBAPIError(msg)

    def rename_relationship(self, id, new_name):
        """Rename relationship."""
        self.add_working_commit()
        relationship_list = self.session.query(self.Relationship).filter_by(id=id)
        if not relationship_list.count():
            raise RecordNotFoundError('relationship', name=new_name)
        try:
            self.transactions.append(self.session.begin_nested())
            for relationship in relationship_list:
                relationship.name = new_name
                relationship.commit_id = self._commit.id
            self.session.flush()
            return relationship_list.first()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while renaming relationship '{}': {}".format(relationship.name, e.orig.args)
            raise SpineDBAPIError(msg)

    def update_parameter(self, id, field_name, new_value):
        """Update parameter."""
        self.add_working_commit()
        parameter = self.session.query(self.ParameterDefinition).filter_by(id=id).one_or_none()
        if not parameter:
            raise RecordNotFoundError('parameter', id=id)
        value = getattr(parameter, field_name)
        data_type = type(value)
        try:
            new_casted_value = data_type(new_value)
        except TypeError:
            new_casted_value = new_value
        except ValueError:
            raise ParameterValueError(new_value, data_type)
        if value == new_casted_value:
            return None
        try:
            self.transactions.append(self.session.begin_nested())
            setattr(parameter, field_name, new_value)
            parameter.commit_id = self._commit.id
            self.session.flush()
            return self.single_parameter(id=parameter.id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating parameter '{}': {}".format(parameter.name, e.orig.args)
            raise SpineDBAPIError(msg)

    def update_parameter_value(self, id, field_name, new_value):
        """Update parameter value."""
        self.add_working_commit()
        parameter_value = self.session.query(self.ParameterValue).filter_by(id=id).one_or_none()
        if not parameter_value:
            raise RecordNotFoundError('parameter_value', id=id)
        value = getattr(parameter_value, field_name)
        data_type = type(value)
        try:
            new_casted_value = data_type(new_value)
        except TypeError:
            new_casted_value = new_value
        except ValueError:
            raise ParameterValueError(new_value, data_type)
        if value == new_casted_value:
            return None
        try:
            self.transactions.append(self.session.begin_nested())
            setattr(parameter_value, field_name, new_casted_value)
            parameter_value.commit_id = self._commit.id
            self.session.flush()
            return self.single_parameter_value(id=parameter_value.id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating parameter value: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def remove_object_class(self, id):
        """Remove object class."""
        self.add_working_commit()
        object_class = self.session.query(self.ObjectClass).filter_by(id=id).one_or_none()
        if not object_class:
            raise RecordNotFoundError('object_class', id=id)
        try:
            self.transactions.append(self.session.begin_nested())
            self.session.delete(object_class)
            self.session.flush()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while removing object class: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def remove_object(self, id):
        """Remove object."""
        self.add_working_commit()
        object_ = self.session.query(self.Object).filter_by(id=id).one_or_none()
        if not object_:
            raise RecordNotFoundError('object', id=id)
        try:
            self.transactions.append(self.session.begin_nested())
            self.session.delete(object_)
            self.session.flush()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while removing object: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def remove_relationship_class(self, id):
        """Remove relationship class."""
        self.add_working_commit()
        relationship_class_list = self.session.query(self.RelationshipClass).filter_by(id=id)
        if not relationship_class_list.count():
            raise RecordNotFoundError('relationship_class', id=id)
        try:
            self.transactions.append(self.session.begin_nested())
            for relationship_class in relationship_class_list:
                self.session.delete(relationship_class)
            self.session.flush()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while removing relationship class: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def remove_relationship(self, id):
        """Remove relationship."""
        self.add_working_commit()
        relationship_list = self.session.query(self.Relationship).filter_by(id=id)
        if not relationship_list.count():
            raise RecordNotFoundError('relationship', id=id)
        try:
            self.transactions.append(self.session.begin_nested())
            for relationship in relationship_list:
                self.session.delete(relationship)
            self.session.flush()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while removing relationship: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def remove_parameter(self, id):
        """Remove parameter."""
        self.add_working_commit()
        parameter = self.session.query(self.ParameterDefinition).filter_by(id=id).one_or_none()
        if not parameter:
            raise RecordNotFoundError('parameter', id=id)
        try:
            self.transactions.append(self.session.begin_nested())
            self.session.delete(parameter)
            self.session.flush()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while removing parameter: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def remove_parameter_value(self, id):
        """Remove parameter value."""
        self.add_working_commit()
        parameter_value = self.session.query(self.ParameterValue).filter_by(id=id).one_or_none()
        if not parameter_value:
            raise RecordNotFoundError('parameter_value', id=id)
        try:
            self.transactions.append(self.session.begin_nested())
            self.session.delete(parameter_value)
            self.session.flush()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while removing parameter value: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

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
        self.session.query(self.ParameterDefinitionTag).delete(synchronize_session=False)
        self.session.query(self.ParameterEnum).delete(synchronize_session=False)
        self.session.query(self.Commit).delete(synchronize_session=False)

    def close(self):
        if self.session:
            self.session.rollback()
            self.session.close()
        if self.engine:
            self.engine.dispose()
